from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .notifications import alert_source_label, send_alert_email


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


def _trade_time(row: dict[str, Any]) -> datetime | None:
    time_ms = _safe_float(row.get("time"))
    if time_ms > 0:
        return datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)
    return _parse_time(row.get("audit_synced_at") or row.get("ts") or row.get("transactTime"))


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def summarize_recent_volume(*, rows: list[dict[str, Any]], now: datetime, window_seconds: float) -> dict[str, Any]:
    window_start = now.timestamp() - max(float(window_seconds), 1.0)
    gross_notional = 0.0
    trade_count = 0
    latest_trade_at: datetime | None = None
    for row in rows:
        trade_at = _trade_time(row)
        if trade_at is None or trade_at.timestamp() < window_start or trade_at > now:
            continue
        notional = _safe_float(row.get("quoteQty") or row.get("quote_qty") or row.get("notional"))
        if notional <= 0:
            notional = _safe_float(row.get("price")) * _safe_float(row.get("qty") or row.get("quantity"))
        if notional <= 0:
            continue
        gross_notional += abs(notional)
        trade_count += 1
        latest_trade_at = trade_at if latest_trade_at is None or trade_at > latest_trade_at else latest_trade_at
    return {
        "gross_notional": gross_notional,
        "trade_count": trade_count,
        "window_seconds": max(float(window_seconds), 1.0),
        "latest_trade_at": latest_trade_at.isoformat() if latest_trade_at else None,
    }


def _format_alert_body(
    *,
    symbol: str,
    volume_summary: dict[str, Any],
    min_volume_notional: float,
    elapsed_seconds: float,
) -> str:
    return "\n".join(
        [
            f"{symbol} 最近 {volume_summary['window_seconds'] / 60:.1f} 分钟成交额低于阈值。",
            "",
            f"当前成交额: {volume_summary['gross_notional']:.4f} USDT",
            f"阈值: {min_volume_notional:.4f} USDT",
            f"持续时间: {elapsed_seconds / 60:.1f} 分钟",
            f"成交笔数: {volume_summary['trade_count']}",
            f"最后成交时间: {volume_summary.get('latest_trade_at')}",
        ]
    )


def check_symbol(
    *,
    symbol: str,
    output_dir: Path,
    state: dict[str, Any],
    now: datetime,
    window_seconds: float,
    min_volume_notional: float,
    threshold_seconds: float,
    alert_config_path: Path | None,
    send_email: bool = True,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    prefix = normalized_symbol.lower()
    trade_path = output_dir / f"{prefix}_loop_trade_audit.jsonl"
    volume_summary = summarize_recent_volume(
        rows=_iter_jsonl(trade_path),
        now=now,
        window_seconds=window_seconds,
    )
    gross = float(volume_summary["gross_notional"])
    item = dict(state.get(normalized_symbol) or {})
    previous_summary = item.get("last_summary") if isinstance(item.get("last_summary"), dict) else {}
    config_changed = (
        _safe_float(previous_summary.get("window_seconds")) != float(volume_summary["window_seconds"])
        or _safe_float(item.get("min_volume_notional")) != float(min_volume_notional)
    )
    if config_changed:
        item["alert_sent"] = False
    should_alert = False
    alert_result = None

    if gross >= min_volume_notional:
        item = {
            "status": "normal",
            "last_normal_at": now.isoformat(),
            "last_summary": volume_summary,
            "alert_sent": False,
        }
    else:
        first_low = _parse_time(item.get("first_low_volume_at")) or now
        elapsed = (now - first_low).total_seconds()
        should_alert = elapsed >= max(float(threshold_seconds), 0.0) and not bool(item.get("alert_sent"))
        item.update(
            {
                "status": "low_volume",
                "first_low_volume_at": first_low.isoformat(),
                "last_low_volume_at": now.isoformat(),
                "elapsed_seconds": elapsed,
                "min_volume_notional": min_volume_notional,
                "last_summary": volume_summary,
            }
        )
        if should_alert:
            item["alert_sent"] = True
            item["last_alert_at"] = now.isoformat()
            if send_email:
                alert_result = send_alert_email(
                    subject=(
                        f"[grid][{alert_source_label()}] {normalized_symbol} "
                        f"{volume_summary['window_seconds'] / 60:g}m volume below {min_volume_notional:g}"
                    ),
                    body=_format_alert_body(
                        symbol=normalized_symbol,
                        volume_summary=volume_summary,
                        min_volume_notional=min_volume_notional,
                        elapsed_seconds=elapsed,
                    ),
                    config_path=alert_config_path,
                )

    state[normalized_symbol] = item
    return {
        "symbol": normalized_symbol,
        "trade_path": str(trade_path),
        "volume_summary": volume_summary,
        "min_volume_notional": min_volume_notional,
        "low_volume": gross < min_volume_notional,
        "should_alert": should_alert,
        "alert": alert_result,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alert when recent runner trade volume falls below a threshold.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. BILLUSDT,AIGENSYNUSDT")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--state-path", default="output/low_volume_monitor_state.json")
    parser.add_argument("--alert-config-path", default="output/low_volume_alert_config.json")
    parser.add_argument("--window-seconds", type=float, default=3600.0)
    parser.add_argument("--min-volume-notional", type=float, default=1000.0)
    parser.add_argument("--threshold-seconds", type=float, default=0.0)
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
            window_seconds=args.window_seconds,
            min_volume_notional=args.min_volume_notional,
            threshold_seconds=args.threshold_seconds,
            alert_config_path=Path(args.alert_config_path) if args.alert_config_path else None,
        )
        for symbol in [part.strip().upper() for part in args.symbols.split(",") if part.strip()]
    ]
    _write_json(state_path, state)
    print(json.dumps({"ok": True, "checked_at": now.isoformat(), "results": results}, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
