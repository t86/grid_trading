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


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _order_side_set(orders: list[Any]) -> set[str]:
    sides: set[str] = set()
    for item in orders:
        if not isinstance(item, dict):
            continue
        side = str(item.get("side") or "").upper().strip()
        if side in {"BUY", "SELL"}:
            sides.add(side)
    return sides


def assess_recovery_stall(
    *,
    symbol: str,
    plan: dict[str, Any],
    submit: dict[str, Any],
    now: datetime,
    plan_stale_seconds: float = 120.0,
) -> dict[str, Any]:
    plan_at = _parse_time(plan.get("generated_at"))
    submit_at = _parse_time(submit.get("generated_at"))
    reasons: list[str] = []
    details: dict[str, Any] = {
        "symbol": symbol,
        "plan_at": plan.get("generated_at"),
        "submit_at": submit.get("generated_at"),
    }
    if plan_at is None or (now - plan_at).total_seconds() > plan_stale_seconds:
        reasons.append("latest_plan_stale")
    if submit_at is None or (now - submit_at).total_seconds() > plan_stale_seconds:
        reasons.append("latest_submit_stale")

    bq = plan.get("best_quote_maker_volume") if isinstance(plan.get("best_quote_maker_volume"), dict) else {}
    regime = str(bq.get("regime") or "").strip()
    if regime and regime != "normal":
        reasons.append(f"best_quote_regime:{regime}")
    details["best_quote_regime"] = regime or None
    details["best_quote_reasons"] = list(bq.get("reasons") or [])

    vol = plan.get("volatility_entry_pause") if isinstance(plan.get("volatility_entry_pause"), dict) else {}
    if bool(vol.get("active")):
        reasons.append("volatility_entry_pause")
    details["volatility_entry_pause_active"] = bool(vol.get("active"))
    details["volatility_entry_pause_reason"] = vol.get("reason")

    plan_sides = _order_side_set(list(plan.get("buy_orders") or []) + list(plan.get("sell_orders") or []))
    actions = submit.get("validation", {}).get("actions", {}) if isinstance(submit.get("validation"), dict) else {}
    submit_sides = _order_side_set(list(actions.get("place_orders") or [])) if isinstance(actions, dict) else set()
    if not ({"BUY", "SELL"} <= plan_sides or {"BUY", "SELL"} <= submit_sides):
        reasons.append("not_bilateral_orders")
    details["plan_sides"] = sorted(plan_sides)
    details["submit_sides"] = sorted(submit_sides)

    guard = actions.get("loss_inventory_no_cross_entry_guard") if isinstance(actions, dict) else {}
    if isinstance(guard, dict) and int(guard.get("dropped_order_count") or 0) > 0:
        reasons.append("loss_inventory_guard_dropped_orders")
    details["loss_inventory_guard"] = guard if isinstance(guard, dict) else None

    if isinstance(actions, dict):
        reduce_cap = actions.get("reduce_only_position_cap")
        if isinstance(reduce_cap, dict):
            details["reduce_only_position_cap"] = reduce_cap

    return {
        "normal_bilateral": not reasons,
        "reasons": reasons,
        "details": details,
    }


def update_stall_state(
    *,
    symbol: str,
    assessment: dict[str, Any],
    state: dict[str, Any],
    now: datetime,
    threshold_seconds: float,
) -> tuple[dict[str, Any], bool]:
    item = dict(state.get(symbol) or {})
    if assessment.get("normal_bilateral"):
        item = {
            "status": "normal",
            "last_normal_at": now.isoformat(),
            "last_assessment": assessment,
            "alert_sent": False,
        }
        state[symbol] = item
        return state, False

    first_seen = _parse_time(item.get("first_non_normal_at")) or now
    previous_reasons = list(item.get("reasons") or [])
    reasons = list(assessment.get("reasons") or [])
    if previous_reasons != reasons:
        first_seen = now
        item["alert_sent"] = False
    elapsed = (now - first_seen).total_seconds()
    should_alert = elapsed >= threshold_seconds and not bool(item.get("alert_sent"))
    item.update(
        {
            "status": "non_normal",
            "first_non_normal_at": first_seen.isoformat(),
            "last_non_normal_at": now.isoformat(),
            "elapsed_seconds": elapsed,
            "reasons": reasons,
            "last_assessment": assessment,
        }
    )
    if should_alert:
        item["alert_sent"] = True
        item["last_alert_at"] = now.isoformat()
    state[symbol] = item
    return state, should_alert


def _format_alert_body(symbol: str, assessment: dict[str, Any], elapsed_seconds: float) -> str:
    details = dict(assessment.get("details") or {})
    return "\n".join(
        [
            f"{symbol} 已连续 {elapsed_seconds / 60:.1f} 分钟未恢复正常双边交易。",
            "",
            f"原因: {', '.join(assessment.get('reasons') or [])}",
            f"best_quote_regime: {details.get('best_quote_regime')}",
            f"volatility_entry_pause: {details.get('volatility_entry_pause_active')} {details.get('volatility_entry_pause_reason') or ''}",
            f"plan_sides: {details.get('plan_sides')}",
            f"submit_sides: {details.get('submit_sides')}",
            f"plan_at: {details.get('plan_at')}",
            f"submit_at: {details.get('submit_at')}",
        ]
    )


def check_symbol(
    *,
    symbol: str,
    output_dir: Path,
    state: dict[str, Any],
    threshold_seconds: float,
    alert_config_path: Path | None,
    now: datetime,
) -> dict[str, Any]:
    prefix = symbol.lower()
    plan = _read_json(output_dir / f"{prefix}_loop_latest_plan.json")
    submit = _read_json(output_dir / f"{prefix}_loop_latest_submit.json")
    assessment = assess_recovery_stall(symbol=symbol, plan=plan, submit=submit, now=now)
    state, should_alert = update_stall_state(
        symbol=symbol,
        assessment=assessment,
        state=state,
        now=now,
        threshold_seconds=threshold_seconds,
    )
    result = {"symbol": symbol, "assessment": assessment, "alert": None}
    if should_alert:
        elapsed = float(state.get(symbol, {}).get("elapsed_seconds") or threshold_seconds)
        result["alert"] = send_alert_email(
            subject=f"[grid][{alert_source_label()}] {symbol} normal bilateral stalled >10m",
            body=_format_alert_body(symbol, assessment, elapsed),
            config_path=alert_config_path,
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alert when saved runners fail to recover normal bilateral trading.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. BILLUSDT,AIGENSYNUSDT")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--state-path", default="output/recovery_stall_monitor_state.json")
    parser.add_argument("--alert-config-path", default="output/recovery_stall_alert_config.json")
    parser.add_argument("--threshold-seconds", type=float, default=600.0)
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    output_dir = Path(args.output_dir)
    state_path = Path(args.state_path)
    state = _read_json(state_path)
    results = []
    for symbol in [part.strip().upper() for part in args.symbols.split(",") if part.strip()]:
        results.append(
            check_symbol(
                symbol=symbol,
                output_dir=output_dir,
                state=state,
                threshold_seconds=max(float(args.threshold_seconds), 1.0),
                alert_config_path=Path(args.alert_config_path) if args.alert_config_path else None,
                now=now,
            )
        )
    _write_json(state_path, state)
    print(json.dumps({"ok": True, "checked_at": now.isoformat(), "results": results}, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
