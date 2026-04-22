from __future__ import annotations

import json
import os
import smtplib
import socket
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any

DEFAULT_ALERT_CONFIG_PATH = Path("output/alert_notifier_config.json")
DEFAULT_ALERT_STATUS_PATH = Path("output/alert_notifier_status.json")
DEFAULT_DIRECT_MX_BY_DOMAIN: dict[str, list[str]] = {
    "qq.com": ["mx1.qq.com", "mx2.qq.com", "mx3.qq.com", "mx4.qq.com"],
}
DEFAULT_ALERT_SOURCE_LABELS: dict[str, str] = {
    "VM-0-16-ubuntu": "114",
    "VM-0-2-ubuntu": "150",
}


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_dict(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_alert_notifier_config(path: Path | None = None) -> dict[str, Any]:
    config_path = Path(path or DEFAULT_ALERT_CONFIG_PATH)
    file_config = _read_json_dict(config_path)
    env_to = os.environ.get("GRID_ALERT_EMAIL_TO", "").strip()
    env_from = os.environ.get("GRID_ALERT_EMAIL_FROM", "").strip()
    env_mode = os.environ.get("GRID_ALERT_EMAIL_MODE", "").strip()
    env_smtp_host = os.environ.get("GRID_ALERT_SMTP_HOST", "").strip()
    env_smtp_port = os.environ.get("GRID_ALERT_SMTP_PORT", "").strip()
    env_smtp_username = os.environ.get("GRID_ALERT_SMTP_USERNAME", "").strip()
    env_smtp_password = os.environ.get("GRID_ALERT_SMTP_PASSWORD", "").strip()
    env_smtp_tls = os.environ.get("GRID_ALERT_SMTP_STARTTLS", "").strip()

    email_to = file_config.get("email_to") or []
    if isinstance(email_to, str):
        email_to = [part.strip() for part in email_to.split(",") if part.strip()]
    if env_to:
        email_to = [part.strip() for part in env_to.split(",") if part.strip()]

    email_from = env_from or str(file_config.get("email_from") or "").strip()
    if not email_from:
        hostname = socket.gethostname().strip() or "grid-alert"
        email_from = f"grid-alert@{hostname}"

    direct_mx_by_domain = dict(DEFAULT_DIRECT_MX_BY_DOMAIN)
    raw_direct = file_config.get("direct_mx_by_domain") or {}
    if isinstance(raw_direct, dict):
        for domain, hosts in raw_direct.items():
            if isinstance(hosts, list):
                direct_mx_by_domain[str(domain).strip().lower()] = [
                    str(item).strip() for item in hosts if str(item).strip()
                ]

    return {
        "enabled": bool(email_to),
        "email_to": email_to,
        "email_from": email_from,
        "mode": env_mode or str(file_config.get("mode") or "direct_mx").strip() or "direct_mx",
        "smtp_host": env_smtp_host or str(file_config.get("smtp_host") or "").strip(),
        "smtp_port": int(env_smtp_port or file_config.get("smtp_port") or 587),
        "smtp_username": env_smtp_username or str(file_config.get("smtp_username") or "").strip(),
        "smtp_password": env_smtp_password or str(file_config.get("smtp_password") or "").strip(),
        "smtp_starttls": str(env_smtp_tls or file_config.get("smtp_starttls") or "true").strip().lower()
        not in {"0", "false", "no"},
        "timeout_seconds": float(file_config.get("timeout_seconds") or 4.0),
        "direct_mx_by_domain": direct_mx_by_domain,
        "status_path": str(file_config.get("status_path") or DEFAULT_ALERT_STATUS_PATH),
    }


def alert_source_label() -> str:
    explicit = os.environ.get("GRID_ALERT_SOURCE_LABEL", "").strip()
    if explicit:
        return explicit
    hostname = socket.gethostname().strip()
    if hostname in DEFAULT_ALERT_SOURCE_LABELS:
        return DEFAULT_ALERT_SOURCE_LABELS[hostname]
    return hostname or "unknown"


def _send_via_smtp(config: dict[str, Any], message: EmailMessage) -> None:
    host = str(config.get("smtp_host") or "").strip()
    if not host:
        raise RuntimeError("missing smtp_host")
    port = int(config.get("smtp_port") or 587)
    timeout_seconds = float(config.get("timeout_seconds") or 4.0)
    username = str(config.get("smtp_username") or "").strip()
    password = str(config.get("smtp_password") or "").strip()
    with smtplib.SMTP(host, port, timeout=timeout_seconds) as server:
        server.ehlo()
        if config.get("smtp_starttls", True):
            server.starttls()
            server.ehlo()
        if username or password:
            server.login(username, password)
        server.send_message(message)


def _send_via_direct_mx(config: dict[str, Any], message: EmailMessage) -> None:
    recipients = [str(item).strip() for item in list(message.get_all("To", [])) if str(item).strip()]
    if not recipients:
        raise RuntimeError("missing recipients")
    timeout_seconds = float(config.get("timeout_seconds") or 4.0)
    direct_mx_by_domain = dict(config.get("direct_mx_by_domain") or {})
    recipient = recipients[0]
    if "@" not in recipient:
        raise RuntimeError(f"invalid recipient: {recipient}")
    domain = recipient.rsplit("@", 1)[-1].strip().lower()
    mx_hosts = list(direct_mx_by_domain.get(domain) or [])
    if not mx_hosts:
        raise RuntimeError(f"unsupported direct_mx domain: {domain}")
    last_error: Exception | None = None
    for host in mx_hosts:
        try:
            with smtplib.SMTP(host, 25, timeout=timeout_seconds) as server:
                server.ehlo()
                server.sendmail(message["From"], recipients, message.as_string())
            return
        except Exception as exc:  # pragma: no cover - network-dependent
            last_error = exc
    raise RuntimeError(f"direct_mx delivery failed for {domain}: {last_error}")


def send_alert_email(
    *,
    subject: str,
    body: str,
    config_path: Path | None = None,
) -> dict[str, Any]:
    config = load_alert_notifier_config(config_path)
    status_path = Path(str(config.get("status_path") or DEFAULT_ALERT_STATUS_PATH))
    result: dict[str, Any] = {
        "sent": False,
        "mode": config.get("mode"),
        "email_to": list(config.get("email_to") or []),
        "email_from": config.get("email_from"),
        "subject": subject,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }
    if not config.get("enabled"):
        result["error"] = "alert_email_disabled"
        _write_json_dict(status_path, result)
        return result

    message = EmailMessage()
    message["From"] = str(config.get("email_from") or "").strip()
    message["To"] = ", ".join(str(item).strip() for item in list(config.get("email_to") or []) if str(item).strip())
    message["Subject"] = subject
    message.set_content(body)

    try:
        mode = str(config.get("mode") or "direct_mx").strip().lower()
        if mode == "smtp":
            _send_via_smtp(config, message)
        else:
            _send_via_direct_mx(config, message)
        result["sent"] = True
    except Exception as exc:  # pragma: no cover - network-dependent
        result["error"] = f"{type(exc).__name__}: {exc}"

    _write_json_dict(status_path, result)
    return result
