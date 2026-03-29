from __future__ import annotations

import argparse
import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data import (
    delete_futures_order,
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    load_binance_api_credentials,
    post_futures_change_initial_leverage,
    post_futures_change_margin_type,
    post_futures_order,
)
from .dry_run import _round_order_price
from .live_check import extract_symbol_position


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _price(value: float) -> str:
    return f"{value:.7f}"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_iso_ts(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _build_client_order_id(*, symbol: str, role: str, index: int) -> str:
    compact_symbol = symbol.lower().replace("usdt", "u")
    compact_role = role.lower().replace("_", "")[:8]
    suffix = str(int(time.time() * 1000))[-8:]
    return f"gx-{compact_symbol}-{compact_role}-{index}-{suffix}"[:36]


def adjust_post_only_price(
    *,
    desired_price: float,
    side: str,
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
) -> float:
    normalized_side = side.upper().strip()
    if normalized_side == "BUY":
        if tick_size and tick_size > 0:
            resting_ceiling = max(live_ask_price - tick_size, 0.0)
            return _round_order_price(min(desired_price, resting_ceiling), tick_size, normalized_side)
        return min(desired_price, live_bid_price)
    if tick_size and tick_size > 0:
        resting_floor = live_bid_price + tick_size
        return _round_order_price(max(desired_price, resting_floor), tick_size, normalized_side)
    return max(desired_price, live_ask_price)


def prepare_post_only_order_request(
    *,
    order: dict[str, Any],
    side: str,
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    qty = _safe_float(order.get("qty"))
    desired_price = _safe_float(order.get("price"))
    submit_price = adjust_post_only_price(
        desired_price=desired_price,
        side=side,
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        tick_size=tick_size,
    )
    submit_notional = qty * submit_price
    if qty <= 0:
        return None, {
            "reason": "non_positive_qty",
            "qty": qty,
            "desired_price": desired_price,
            "submitted_price": submit_price,
            "submitted_notional": submit_notional,
        }
    if min_qty is not None and qty < min_qty:
        return None, {
            "reason": "qty_below_min_qty",
            "qty": qty,
            "min_qty": float(min_qty),
            "desired_price": desired_price,
            "submitted_price": submit_price,
            "submitted_notional": submit_notional,
        }
    if min_notional is not None and submit_notional < min_notional:
        return None, {
            "reason": "submitted_notional_below_min_notional",
            "qty": qty,
            "min_notional": float(min_notional),
            "desired_price": desired_price,
            "submitted_price": submit_price,
            "submitted_notional": submit_notional,
        }
    return {
        "qty": qty,
        "desired_price": desired_price,
        "submitted_price": submit_price,
        "submitted_notional": submit_notional,
    }, None


def build_execution_actions(plan_report: dict[str, Any]) -> dict[str, Any]:
    bootstrap_orders = [
        item for item in plan_report.get("bootstrap_orders", []) if isinstance(item, dict)
    ]
    missing_orders = [
        item for item in plan_report.get("missing_orders", []) if isinstance(item, dict)
    ]
    stale_orders = [
        item for item in plan_report.get("stale_orders", []) if isinstance(item, dict)
    ]
    place_orders = [*bootstrap_orders, *missing_orders]
    place_notional = sum(_safe_float(item.get("notional")) for item in place_orders)
    return {
        "place_orders": place_orders,
        "cancel_orders": stale_orders,
        "place_count": len(place_orders),
        "cancel_count": len(stale_orders),
        "place_notional": place_notional,
    }


def estimate_mid_drift_steps(
    *,
    report_mid_price: float,
    live_bid_price: float,
    live_ask_price: float,
    step_price: float,
) -> float:
    if step_price <= 0:
        return 0.0
    live_mid_price = (live_bid_price + live_ask_price) / 2.0
    return abs(live_mid_price - report_mid_price) / step_price


def validate_plan_report(
    *,
    plan_report: dict[str, Any],
    allow_symbol: str,
    max_new_orders: int,
    max_total_notional: float,
    cancel_stale: bool,
    max_plan_age_seconds: int,
    now: datetime,
    allow_dual_side_position: bool = False,
) -> dict[str, Any]:
    errors: list[str] = []
    actions = build_execution_actions(plan_report)
    symbol = str(plan_report.get("symbol", "")).upper().strip()

    if not symbol:
        errors.append("plan report is missing symbol")
    if symbol != allow_symbol.upper().strip():
        errors.append(f"symbol {symbol or '<empty>'} is not allowed; expected {allow_symbol.upper().strip()}")
    if _truthy(plan_report.get("dual_side_position")) and not allow_dual_side_position:
        errors.append("account is in hedge mode; only one-way mode is supported")
    if actions["cancel_count"] > 0 and not cancel_stale:
        errors.append("plan contains stale orders; rerun with --cancel-stale or regenerate the plan")
    if actions["place_count"] <= 0 and actions["cancel_count"] <= 0:
        errors.append("plan contains no actions to execute")
    if actions["place_count"] > max_new_orders:
        errors.append(f"plan places {actions['place_count']} new orders, above max_new_orders={max_new_orders}")
    if actions["place_notional"] > max_total_notional:
        errors.append(
            f"plan places total notional {_float(actions['place_notional'])}, above max_total_notional={_float(max_total_notional)}"
        )

    generated_at = _parse_iso_ts(plan_report.get("generated_at"))
    if generated_at is None:
        errors.append("plan report is missing a valid generated_at timestamp")
        plan_age_seconds = None
    else:
        plan_age_seconds = max((now - generated_at.astimezone(timezone.utc)).total_seconds(), 0.0)
        if plan_age_seconds > max_plan_age_seconds:
            errors.append(
                f"plan age is {plan_age_seconds:.1f}s, above max_plan_age_seconds={max_plan_age_seconds}"
            )

    return {
        "ok": not errors,
        "errors": errors,
        "actions": actions,
        "plan_age_seconds": plan_age_seconds,
    }


def _ignore_noop_error(exc: RuntimeError, allowed_markers: tuple[str, ...]) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in allowed_markers)


def _print_preview(
    *,
    plan_report: dict[str, Any],
    validation: dict[str, Any],
    drift_steps: float | None,
) -> None:
    actions = validation["actions"]
    print("=== Submit Plan ===")
    print(f"Symbol: {plan_report['symbol']}")
    print(
        f"Plan bid/ask/mid: {_price(_safe_float(plan_report.get('bid_price')))} / "
        f"{_price(_safe_float(plan_report.get('ask_price')))} / {_price(_safe_float(plan_report.get('mid_price')))}"
    )
    if validation["plan_age_seconds"] is not None:
        print(f"Plan age: {validation['plan_age_seconds']:.1f}s")
    if drift_steps is not None:
        print(f"Current mid drift: {drift_steps:.2f} steps")
    print(
        f"Orders to place / cancel / total notional: "
        f"{actions['place_count']} / {actions['cancel_count']} / {_float(actions['place_notional'])}"
    )
    if validation["errors"]:
        print("Errors:")
        for item in validation["errors"]:
            print(f"  - {item}")
    print("Place orders:")
    for order in actions["place_orders"]:
        print(
            f"  {str(order.get('role', 'entry'))} {str(order.get('side', '')).upper()} "
            f"qty={_float(_safe_float(order.get('qty')))} "
            f"price={_price(_safe_float(order.get('price')))} "
            f"notional={_float(_safe_float(order.get('notional')))}"
        )
    if actions["cancel_orders"]:
        print("Cancel orders:")
        for order in actions["cancel_orders"]:
            print(
                f"  {str(order.get('side', '')).upper()} "
                f"qty={_float(_safe_float(order.get('origQty', order.get('qty'))))} "
                f"price={_price(_safe_float(order.get('price')))}"
            )
    print("")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit a previously generated NIGHT semi-auto plan as real Binance USD-M futures orders."
    )
    parser.add_argument("--plan-json", type=str, default="output/night_semi_auto_plan.json")
    parser.add_argument("--allow-symbol", type=str, default="NIGHTUSDT")
    parser.add_argument("--margin-type", type=str, default="KEEP")
    parser.add_argument("--leverage", type=int, default=2)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--max-plan-age-seconds", type=int, default=60)
    parser.add_argument("--max-mid-drift-steps", type=float, default=4.0)
    parser.add_argument("--max-new-orders", type=int, default=24)
    parser.add_argument("--max-total-notional", type=float, default=500.0)
    parser.add_argument("--maker-retries", type=int, default=2)
    parser.add_argument("--cancel-stale", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report-json", type=str, default="output/night_submit_report.json")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.leverage <= 0:
        raise SystemExit("--leverage must be > 0")
    if args.max_plan_age_seconds <= 0:
        raise SystemExit("--max-plan-age-seconds must be > 0")
    if args.max_mid_drift_steps < 0:
        raise SystemExit("--max-mid-drift-steps must be >= 0")
    if args.max_new_orders <= 0:
        raise SystemExit("--max-new-orders must be > 0")
    if args.max_total_notional <= 0:
        raise SystemExit("--max-total-notional must be > 0")
    if args.maker_retries < 0:
        raise SystemExit("--maker-retries must be >= 0")

    plan_path = Path(args.plan_json)
    if not plan_path.exists():
        raise SystemExit(f"plan file not found: {plan_path}")
    plan_report = json.loads(plan_path.read_text(encoding="utf-8"))
    validation = validate_plan_report(
        plan_report=plan_report,
        allow_symbol=args.allow_symbol,
        max_new_orders=args.max_new_orders,
        max_total_notional=args.max_total_notional,
        cancel_stale=args.cancel_stale,
        max_plan_age_seconds=args.max_plan_age_seconds,
        now=datetime.now(timezone.utc),
    )

    symbol = str(plan_report.get("symbol", "")).upper().strip()
    step_price = _safe_float(plan_report.get("step_price"))
    book_rows = fetch_futures_book_tickers(symbol=symbol)
    if not book_rows:
        raise SystemExit(f"no book ticker returned for {symbol}")
    live_book = book_rows[0]
    live_bid_price = _safe_float(live_book.get("bid_price"))
    live_ask_price = _safe_float(live_book.get("ask_price"))
    drift_steps = estimate_mid_drift_steps(
        report_mid_price=_safe_float(plan_report.get("mid_price")),
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        step_price=step_price,
    )
    if drift_steps > args.max_mid_drift_steps:
        validation["errors"].append(
            f"live mid drift is {drift_steps:.2f} steps, above max_mid_drift_steps={args.max_mid_drift_steps:.2f}"
        )
        validation["ok"] = False

    _print_preview(plan_report=plan_report, validation=validation, drift_steps=drift_steps)

    report = {
        "plan_json": str(plan_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "apply_requested": bool(args.apply),
        "validation": validation,
        "live_book": {
            "bid_price": live_bid_price,
            "ask_price": live_ask_price,
        },
        "executed": False,
        "canceled_orders": [],
        "placed_orders": [],
        "skipped_orders": [],
        "configuration": {
            "allow_symbol": args.allow_symbol.upper().strip(),
            "margin_type": str(args.margin_type).upper().strip(),
            "leverage": args.leverage,
            "cancel_stale": bool(args.cancel_stale),
        },
        "error": None,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.apply:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Preview only. Re-run with --apply to send real orders.")
        print(f"JSON report saved: {report_path}")
        return

    if not validation["ok"]:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise SystemExit("Refusing to place orders because validation failed")

    try:
        credentials = load_binance_api_credentials()
        if credentials is None:
            raise SystemExit("请先在本机环境变量中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
        api_key, api_secret = credentials

        position_mode = fetch_futures_position_mode(api_key, api_secret, recv_window=args.recv_window)
        if _truthy(position_mode.get("dualSidePosition")):
            raise SystemExit("账户当前是双向持仓模式，当前提交器只支持单向模式")

        account_info = fetch_futures_account_info_v3(api_key, api_secret, recv_window=args.recv_window)
        multi_assets_margin = _truthy(account_info.get("multiAssetsMargin"))
        requested_margin_type = str(args.margin_type).upper().strip()
        report["account_mode"] = {
            "multi_assets_margin": multi_assets_margin,
            "requested_margin_type": requested_margin_type,
        }
        if multi_assets_margin and requested_margin_type == "ISOLATED":
            raise SystemExit(
                "账户当前启用了 Multi-Assets Mode，Binance 不允许在该模式下切到 ISOLATED。"
                "请先关闭 Multi-Assets Mode，或改用 --margin-type CROSSED。"
            )
        current_open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=args.recv_window)
        current_position = extract_symbol_position(account_info, symbol)
        current_long_qty = max(_safe_float(current_position.get("positionAmt")), 0.0)
        expected_open_order_count = int(plan_report.get("open_order_count", 0) or 0)
        expected_long_qty = _safe_float(plan_report.get("current_long_qty"))
        if len(current_open_orders) != expected_open_order_count:
            raise SystemExit("当前未成交委托数量与计划生成时不一致，请先重新生成计划")
        if abs(current_long_qty - expected_long_qty) > 1e-9:
            raise SystemExit("当前持仓与计划生成时不一致，请先重新生成计划")

        margin_response: dict[str, Any] | None = None
        leverage_response: dict[str, Any] | None = None
        if requested_margin_type != "KEEP":
            try:
                margin_response = post_futures_change_margin_type(
                    symbol=symbol,
                    margin_type=requested_margin_type,
                    api_key=api_key,
                    api_secret=api_secret,
                    recv_window=args.recv_window,
                )
            except RuntimeError as exc:
                if not _ignore_noop_error(exc, ("no need to change margin type",)):
                    raise
                margin_response = {"ignored_error": str(exc)}
        else:
            margin_response = {"skipped": True, "reason": "KEEP"}
        try:
            leverage_response = post_futures_change_initial_leverage(
                symbol=symbol,
                leverage=args.leverage,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=args.recv_window,
            )
        except RuntimeError as exc:
            if not _ignore_noop_error(exc, ("no need to change leverage", "same leverage",)):
                raise
            leverage_response = {"ignored_error": str(exc)}

        report["margin_response"] = margin_response
        report["leverage_response"] = leverage_response

        if args.cancel_stale:
            for stale_order in validation["actions"]["cancel_orders"]:
                cancel_response = delete_futures_order(
                    symbol=symbol,
                    order_id=int(stale_order["orderId"]) if str(stale_order.get("orderId", "")).strip() else None,
                    orig_client_order_id=str(stale_order.get("clientOrderId", "")).strip() or None,
                    api_key=api_key,
                    api_secret=api_secret,
                    recv_window=args.recv_window,
                )
                report["canceled_orders"].append(cancel_response)

        tick_size = _safe_float((plan_report.get("symbol_info") or {}).get("tick_size"))
        min_qty = (plan_report.get("symbol_info") or {}).get("min_qty")
        min_notional = (plan_report.get("symbol_info") or {}).get("min_notional")
        for index, order in enumerate(validation["actions"]["place_orders"], start=1):
            role = str(order.get("role", "entry"))
            side = str(order.get("side", "")).upper().strip()
            reduce_only = True if side == "SELL" and role == "take_profit" else None
            last_exc: RuntimeError | None = None
            for attempt in range(args.maker_retries + 1):
                live_book = fetch_futures_book_tickers(symbol=symbol)[0]
                live_bid = _safe_float(live_book.get("bid_price"))
                live_ask = _safe_float(live_book.get("ask_price"))
                prepared_order, skip_reason = prepare_post_only_order_request(
                    order=order,
                    side=side,
                    live_bid_price=live_bid,
                    live_ask_price=live_ask,
                    tick_size=tick_size,
                    min_qty=min_qty,
                    min_notional=min_notional,
                )
                if prepared_order is None:
                    report["skipped_orders"].append(
                        {
                            "request": {
                                "role": role,
                                "side": side,
                                "qty": _safe_float(order.get("qty")),
                                "desired_price": _safe_float(order.get("price")),
                                "attempt": attempt + 1,
                            },
                            "reason": skip_reason or {},
                        }
                    )
                    last_exc = None
                    break
                try:
                    place_response = post_futures_order(
                        symbol=symbol,
                        side=side,
                        quantity=_safe_float(prepared_order.get("qty")),
                        price=_safe_float(prepared_order.get("submitted_price")),
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=args.recv_window,
                        time_in_force="GTX",
                        new_client_order_id=_build_client_order_id(symbol=symbol, role=role, index=index),
                        reduce_only=reduce_only,
                    )
                    report["placed_orders"].append(
                        {
                            "request": {
                                "role": role,
                                "side": side,
                                "qty": _safe_float(prepared_order.get("qty")),
                                "desired_price": _safe_float(prepared_order.get("desired_price")),
                                "submitted_price": _safe_float(prepared_order.get("submitted_price")),
                                "submitted_notional": _safe_float(prepared_order.get("submitted_notional")),
                                "attempt": attempt + 1,
                            },
                            "response": place_response,
                        }
                    )
                    last_exc = None
                    break
                except RuntimeError as exc:
                    last_exc = exc
                    message = str(exc).lower()
                    if "-5022" not in message and "post only order will be rejected" not in message:
                        raise
                    if attempt >= args.maker_retries:
                        raise
            if last_exc is not None:
                raise last_exc

        report["executed"] = True
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Submitted {len(report['placed_orders'])} orders.")
        print(f"JSON report saved: {report_path}")
    except (Exception, SystemExit) as exc:
        report["error"] = {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise


if __name__ == "__main__":
    main()
