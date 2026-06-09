from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .notifications import alert_source_label, send_alert_email


DEFAULT_BARK_URL = "https://api.day.app"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tail_jsonl(path: Path, *, max_lines: int) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max(int(max_lines), 1))
    try:
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return list(rows)


def _event_points(rows: list[dict[str, Any]]) -> list[tuple[datetime, float]]:
    points: list[tuple[datetime, float]] = []
    for row in rows:
        ts = _parse_time(row.get("ts"))
        if ts is None:
            continue
        gross = _safe_float(row.get("cumulative_gross_notional"))
        if gross <= 0:
            gross = _safe_float(row.get("gross_notional"))
        points.append((ts, gross))
    return sorted(points, key=lambda item: item[0])


def _cumulative_at(points: list[tuple[datetime, float]], target: datetime) -> float | None:
    value: float | None = None
    for ts, cumulative in points:
        if ts > target:
            break
        value = cumulative
    return value


def summarize_event_volume(
    *,
    rows: list[dict[str, Any]],
    now: datetime,
    current_window_seconds: float,
    baseline_window_seconds: float,
) -> dict[str, Any]:
    points = _event_points(rows)
    if not points:
        return {
            "available": False,
            "reason": "missing_events",
            "current_volume": 0.0,
            "baseline_volume": 0.0,
            "baseline_per_minute": 0.0,
        }

    current_start = now.timestamp() - max(float(current_window_seconds), 1.0)
    baseline_start = current_start - max(float(baseline_window_seconds), 1.0)
    current_start_at = datetime.fromtimestamp(current_start, tz=timezone.utc)
    baseline_start_at = datetime.fromtimestamp(baseline_start, tz=timezone.utc)

    latest = _cumulative_at(points, now)
    current_start_value = _cumulative_at(points, current_start_at)
    baseline_start_value = _cumulative_at(points, baseline_start_at)
    if latest is None or current_start_value is None:
        return {
            "available": False,
            "reason": "insufficient_current_window",
            "current_volume": 0.0,
            "baseline_volume": 0.0,
            "baseline_per_minute": 0.0,
            "latest_event_at": points[-1][0].isoformat(),
        }

    if baseline_start_value is None:
        baseline_start_value = points[0][1]

    current_volume = max(0.0, latest - current_start_value)
    baseline_volume = max(0.0, current_start_value - baseline_start_value)
    baseline_minutes = max(float(baseline_window_seconds) / 60.0, 1.0 / 60.0)
    return {
        "available": True,
        "current_volume": current_volume,
        "baseline_volume": baseline_volume,
        "baseline_per_minute": baseline_volume / baseline_minutes,
        "current_window_seconds": float(current_window_seconds),
        "baseline_window_seconds": float(baseline_window_seconds),
        "latest_event_at": points[-1][0].isoformat(),
        "latest_cumulative": latest,
    }


def _load_bark_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"enabled": False}
    config = _read_json(path)
    endpoint = str(config.get("bark_endpoint") or config.get("endpoint") or "").strip()
    return {
        "enabled": bool(endpoint),
        "bark_endpoint": endpoint,
        "bark_base_url": str(config.get("bark_base_url") or DEFAULT_BARK_URL).strip() or DEFAULT_BARK_URL,
        "bark_level": str(config.get("bark_level") or "critical").strip() or "critical",
        "bark_sound": str(config.get("bark_sound") or "alarm").strip() or "alarm",
        "timeout_seconds": float(config.get("timeout_seconds") or 5.0),
    }


def _extract_bark_key(value: str) -> str:
    text = value.strip().rstrip("/")
    if not text:
        return ""
    parsed = urllib.parse.urlparse(text)
    if parsed.scheme and parsed.netloc:
        parts = [part for part in parsed.path.split("/") if part]
        return parts[0] if parts else ""
    return text


def send_bark_alert(*, title: str, body: str, config_path: Path | None) -> dict[str, Any]:
    config = _load_bark_config(config_path)
    result = {"sent": False, "error": None}
    if not config.get("enabled"):
        result["error"] = "bark_disabled"
        return result
    key = _extract_bark_key(str(config.get("bark_endpoint") or ""))
    if not key:
        result["error"] = "missing_bark_key"
        return result
    base_url = str(config.get("bark_base_url") or DEFAULT_BARK_URL).rstrip("/")
    url = f"{base_url}/{urllib.parse.quote(key, safe='')}/{urllib.parse.quote(title, safe='')}"
    payload = json.dumps(
        {
            "body": body,
            "level": config.get("bark_level") or "critical",
            "sound": config.get("bark_sound") or "alarm",
        },
        ensure_ascii=False,
    ).encode("utf-8")
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=float(config.get("timeout_seconds") or 5.0)) as response:
            result["status"] = response.status
            result["sent"] = 200 <= int(response.status) < 300
    except Exception as exc:  # pragma: no cover - network-dependent
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _format_body(symbol: str, summary: dict[str, Any], threshold: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"{symbol} 分钟成交量突增。",
            "",
            f"最近1分钟成交额: {summary['current_volume']:.4f} USDT",
            f"前置均量: {summary['baseline_per_minute']:.4f} USDT/分钟",
            f"触发阈值: >= {threshold['min_spike_notional']:.4f} USDT 且 >= {threshold['multiplier']:.2f}x",
            f"累计成交额: {summary.get('latest_cumulative', 0.0):.4f} USDT",
            f"最后事件时间: {summary.get('latest_event_at')}",
        ]
    )


def check_symbol(
    *,
    symbol: str,
    output_dir: Path,
    state: dict[str, Any],
    now: datetime,
    current_window_seconds: float,
    baseline_window_seconds: float,
    min_spike_notional: float,
    multiplier: float,
    cooldown_seconds: float,
    alert_config_path: Path | None,
    bark_config_path: Path | None,
    send_notifications: bool = True,
) -> dict[str, Any]:
    normalized = symbol.upper().strip()
    event_path = output_dir / f"{normalized.lower()}_spot_events.jsonl"
    summary = summarize_event_volume(
        rows=_tail_jsonl(event_path, max_lines=2000),
        now=now,
        current_window_seconds=current_window_seconds,
        baseline_window_seconds=baseline_window_seconds,
    )
    threshold = {"min_spike_notional": float(min_spike_notional), "multiplier": float(multiplier)}
    baseline_trigger = max(float(min_spike_notional), float(summary.get("baseline_per_minute") or 0.0) * float(multiplier))
    spike = bool(summary.get("available")) and float(summary.get("current_volume") or 0.0) >= baseline_trigger

    item = dict(state.get(normalized) or {})
    last_alert_at = _parse_time(item.get("last_alert_at"))
    cooldown_elapsed = last_alert_at is None or (now - last_alert_at).total_seconds() >= max(float(cooldown_seconds), 0.0)
    should_alert = spike and cooldown_elapsed
    email_result = None
    bark_result = None

    if should_alert:
        item["last_alert_at"] = now.isoformat()
        if send_notifications:
            subject = f"[grid][{alert_source_label()}] {normalized} 1m volume spike {summary['current_volume']:.0f}U"
            body = _format_body(normalized, summary, threshold)
            email_result = send_alert_email(subject=subject, body=body, config_path=alert_config_path)
            bark_result = send_bark_alert(title=subject, body=body, config_path=bark_config_path)

    item.update(
        {
            "status": "spike" if spike else "normal",
            "checked_at": now.isoformat(),
            "last_summary": summary,
            "threshold": threshold,
            "cooldown_seconds": float(cooldown_seconds),
        }
    )
    state[normalized] = item
    return {
        "symbol": normalized,
        "event_path": str(event_path),
        "summary": summary,
        "spike": spike,
        "should_alert": should_alert,
        "email": email_result,
        "bark": bark_result,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alert when spot volume jumps in a one-minute window.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. STRAXUSDT,OPGUSDT")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--state-path", default="output/volume_spike_monitor_state.json")
    parser.add_argument("--alert-config-path", default="output/alert_notifier_config.json")
    parser.add_argument("--bark-config-path", default="")
    parser.add_argument("--current-window-seconds", type=float, default=60.0)
    parser.add_argument("--baseline-window-seconds", type=float, default=600.0)
    parser.add_argument("--min-spike-notional", type=float, default=1000.0)
    parser.add_argument("--multiplier", type=float, default=3.0)
    parser.add_argument("--cooldown-seconds", type=float, default=900.0)
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    state_path = Path(args.state_path)
    state = _read_json(state_path)
    results = [
        check_symbol(
            symbol=symbol,
            output_dir=Path(args.output_dir),
            state=state,
            now=now,
            current_window_seconds=args.current_window_seconds,
            baseline_window_seconds=args.baseline_window_seconds,
            min_spike_notional=args.min_spike_notional,
            multiplier=args.multiplier,
            cooldown_seconds=args.cooldown_seconds,
            alert_config_path=Path(args.alert_config_path) if str(args.alert_config_path).strip() else None,
            bark_config_path=Path(args.bark_config_path) if str(args.bark_config_path).strip() else None,
        )
        for symbol in [part.strip().upper() for part in args.symbols.split(",") if part.strip()]
    ]
    _write_json(state_path, state)
    print(json.dumps({"ok": True, "checked_at": now.isoformat(), "results": results}, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
