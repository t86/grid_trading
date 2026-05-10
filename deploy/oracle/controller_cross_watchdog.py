#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from grid_optimizer.notifications import send_alert_email


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or "").strip()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_payload(url: str, username: str, password: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url)
    if username or password:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    return payload if isinstance(payload, dict) else {}


def _build_failure_summary(payload: dict[str, Any]) -> tuple[str, list[str]]:
    warnings = [str(item).strip() for item in list(payload.get("warnings") or []) if str(item).strip()]
    bad_servers: list[str] = []
    for item in list(payload.get("servers") or []):
        if not isinstance(item, dict):
            continue
        if item.get("ok") is True:
            continue
        label = str(item.get("label") or item.get("id") or "unknown").strip() or "unknown"
        error = str(item.get("error") or "").strip()
        if error:
            bad_servers.append(f"{label}: {error}")
        else:
            bad_servers.append(label)
    if warnings or bad_servers:
        summary = "; ".join(warnings + bad_servers)
        return summary or "cross status unhealthy", warnings + bad_servers
    return "", []


def _send_alert(subject: str, body: str) -> dict[str, Any]:
    return send_alert_email(subject=subject, body=body)


def _format_alert_body(
    *,
    host_label: str,
    now: str,
    controller_url: str,
    summary: str,
    details: list[str],
    consecutive_failures: int,
    failure_threshold: int,
) -> str:
    lines = [
        f"结论: Controller {host_label} 聚合状态异常",
        f"时间: {now}",
        f"连续失败: {consecutive_failures}/{failure_threshold}",
        f"检查地址: {controller_url}",
        "",
        "异常摘要",
        f"- {summary}",
        "",
        "异常节点 / 详情",
    ]
    if details:
        lines.extend(f"- {item}" for item in details)
    else:
        lines.append("- 无")
    lines.extend(
        [
            "",
            "建议动作",
            "- 先看 110 的 /api/running_status?scope=cross 是否仍有 warnings",
            "- 再看异常节点本机 /api/health 和 /api/running_status?scope=local",
            "- 若本机接口超时或 5xx，优先重启对应 web service",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    controller_url = _env("CONTROLLER_CROSS_STATUS_URL", "http://127.0.0.1:8787/api/running_status?scope=cross")
    auth_username = _env("AUTH_USERNAME")
    auth_password = _env("AUTH_PASSWORD")
    timeout_seconds = float(_env("TIMEOUT_SECONDS", "10") or "10")
    failure_threshold = int(_env("FAILURE_THRESHOLD", "2") or "2")
    state_path = Path(_env("STATE_PATH", "/var/tmp/grid-controller-cross-watchdog/state.json"))
    host_label = _env("HOST_LABEL", "110")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    state = _read_json(state_path)
    previous_summary = str(state.get("summary") or "").strip()
    previous_status = str(state.get("status") or "unknown").strip()
    consecutive_failures = int(state.get("consecutive_failures") or 0)

    try:
      payload = _fetch_payload(controller_url, auth_username, auth_password, timeout_seconds)
      summary, details = _build_failure_summary(payload)
      ok = not summary
    except Exception as exc:
      payload = {}
      summary = f"{type(exc).__name__}: {exc}"
      details = [summary]
      ok = False

    now = datetime.now(timezone.utc).isoformat()
    if ok:
        next_state = {
            "status": "ok",
            "summary": "",
            "consecutive_failures": 0,
            "checked_at": now,
            "controller_url": controller_url,
        }
        _write_json(state_path, next_state)
        if previous_status != "ok":
            logging.info("controller cross status recovered")
        return 0

    consecutive_failures += 1
    next_state = {
        "status": "alert",
        "summary": summary,
        "consecutive_failures": consecutive_failures,
        "checked_at": now,
        "controller_url": controller_url,
        "details": details,
    }
    _write_json(state_path, next_state)
    logging.warning("controller cross status unhealthy (%s/%s): %s", consecutive_failures, failure_threshold, summary)

    should_alert = consecutive_failures >= failure_threshold and (
        previous_status != "alert" or previous_summary != summary or int(state.get("consecutive_failures") or 0) < failure_threshold
    )
    if not should_alert:
        return 1

    result = _send_alert(
        subject=f"[grid][{host_label}] controller cross status unhealthy",
        body=_format_alert_body(
            host_label=host_label,
            now=now,
            controller_url=controller_url,
            summary=summary,
            details=details,
            consecutive_failures=consecutive_failures,
            failure_threshold=failure_threshold,
        ),
    )
    logging.info("alert send result: sent=%s error=%s", result.get("sent"), result.get("error"))
    return 1


if __name__ == "__main__":
    sys.exit(main())
