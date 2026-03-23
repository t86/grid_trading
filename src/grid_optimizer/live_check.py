from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from .backtest import (
    supported_allocation_modes,
    supported_grid_level_modes,
    supported_strategy_directions,
)
from .data import (
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    fetch_futures_premium_index,
    fetch_futures_symbol_config,
    load_binance_api_credentials,
    post_futures_test_order,
)
from .dry_run import build_paper_grid_legs


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def extract_symbol_position(account_info: dict[str, Any], symbol: str, position_side: str | None = None) -> dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    normalized_position_side = str(position_side or "").upper().strip()
    positions = account_info.get("positions", [])
    if not isinstance(positions, list):
        return {}
    for item in positions:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol", "")).upper().strip() == normalized_symbol:
            if normalized_position_side and str(item.get("positionSide", "")).upper().strip() != normalized_position_side:
                continue
            return item
    return {}


def build_live_entry_orders(
    *,
    legs: list[dict[str, Any]],
    bid_price: float,
    ask_price: float,
) -> list[dict[str, Any]]:
    active_orders: list[dict[str, Any]] = []
    for leg in legs:
        if leg.get("state") == "disabled":
            continue
        if str(leg.get("position_state", "flat")).lower() != "flat":
            continue
        side = str(leg.get("entry_order_side", "")).upper()
        price = float(leg.get("entry_price", 0.0))
        qty = float(leg.get("qty", 0.0))
        if qty <= 0 or price <= 0:
            continue
        is_resting = (side == "BUY" and price < ask_price) or (side == "SELL" and price > bid_price)
        if not is_resting:
            continue
        active_orders.append(
            {
                "grid_index": int(leg["grid_index"]),
                "side": side,
                "price": price,
                "qty": qty,
                "notional": price * qty,
            }
        )
    return active_orders


def evaluate_live_readiness(
    *,
    symbol: str,
    total_buy_notional: float,
    leverage: int,
    strategy_direction: str,
    market_status: str,
    funding_rate: float | None,
    account_info: dict[str, Any],
    position_mode: dict[str, Any],
    open_orders: list[dict[str, Any]],
    entry_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    symbol_position = extract_symbol_position(account_info, symbol)
    position_amt = _safe_float(symbol_position.get("positionAmt"))
    available_balance = _safe_float(account_info.get("availableBalance"))
    wallet_balance = _safe_float(account_info.get("totalWalletBalance"))
    estimated_initial_margin = total_buy_notional / max(leverage, 1)
    dual_side_position = _truthy(position_mode.get("dualSidePosition"))

    if dual_side_position:
        errors.append("账户当前是双向持仓模式，当前执行器只支持单向模式")
    if open_orders:
        errors.append(f"{symbol} 当前已有 {len(open_orders)} 笔未成交委托，请先清空")
    if abs(position_amt) > 1e-12:
        errors.append(f"{symbol} 当前已有持仓，positionAmt={position_amt}")
    if market_status != "in_range":
        errors.append("当前价格已经离开策略区间，需要先重定带宽")
    if not entry_orders:
        errors.append("当前盘口下没有可挂的 maker 入场单")

    if available_balance < estimated_initial_margin:
        warnings.append(
            f"availableBalance={_float(available_balance)} 低于预估初始保证金 {_float(estimated_initial_margin)}"
        )
    if leverage > 2:
        warnings.append("当前建议杠杆上限是 2x，再高会明显放大尾部风险")
    if funding_rate is not None and strategy_direction == "long" and funding_rate > 0:
        warnings.append("当前 funding 为正，多头要付资金费")
    if wallet_balance <= 0:
        warnings.append("账户总钱包余额为 0 或读取异常，请先核对账户类型")

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "dual_side_position": dual_side_position,
        "available_balance": available_balance,
        "wallet_balance": wallet_balance,
        "estimated_initial_margin": estimated_initial_margin,
        "symbol_position": symbol_position,
        "position_amt": position_amt,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate a live Binance USD-M futures grid launch without placing real orders."
    )
    parser.add_argument("--symbol", type=str, default="ENSOUSDT")
    parser.add_argument("--min-price", type=float, default=1.1148)
    parser.add_argument("--max-price", type=float, default=1.1838)
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--total-buy-notional", type=float, default=100.0)
    parser.add_argument(
        "--grid-level-mode",
        type=str,
        default="arithmetic",
        choices=supported_grid_level_modes(),
    )
    parser.add_argument(
        "--allocation-mode",
        type=str,
        default="linear_reverse",
        choices=supported_allocation_modes(),
    )
    parser.add_argument(
        "--strategy-direction",
        type=str,
        default="long",
        choices=supported_strategy_directions(),
    )
    parser.add_argument("--neutral-anchor-price", type=float, default=None)
    parser.add_argument("--leverage", type=int, default=2)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument(
        "--report-json",
        type=str,
        default="output/enso_live_competition_check.json",
    )
    parser.add_argument(
        "--skip-test-orders",
        action="store_true",
        help="Skip Binance /order/test validation for the first batch of maker orders",
    )
    return parser


def _print_summary(report: dict[str, Any]) -> None:
    print("=== Live Check ===")
    print(f"Symbol: {report['symbol']}")
    print(
        f"Bid/Ask: {_float(report['bid_price'])} / {_float(report['ask_price'])}"
    )
    print(f"Market status: {report['market_status']}")
    if report["funding_rate"] is not None:
        print(f"Funding rate: {_pct(report['funding_rate'])}")
    print(
        f"Wallet / Available / Est. Margin: "
        f"{_float(report['wallet_balance'])} / {_float(report['available_balance'])} / {_float(report['estimated_initial_margin'])}"
    )
    print(
        f"Enabled legs / Active entry orders: {report['enabled_leg_count']} / {len(report['entry_orders'])}"
    )
    print(f"Position mode one-way: {'yes' if not report['dual_side_position'] else 'no'}")
    print(f"Current position amt: {_float(report['position_amt'])}")
    print(f"Existing open orders: {report['open_order_count']}")
    if report["warnings"]:
        print("Warnings:")
        for item in report["warnings"]:
            print(f"  - {item}")
    if report["errors"]:
        print("Errors:")
        for item in report["errors"]:
            print(f"  - {item}")
    if report["test_results"]:
        ok_count = sum(1 for item in report["test_results"] if item.get("ok"))
        print(f"Test orders passed: {ok_count}/{len(report['test_results'])}")
    print("")
    print("First batch entry orders:")
    for order in report["entry_orders"]:
        print(
            f"  grid {order['grid_index'] + 1} {order['side']} "
            f"qty={order['qty']:.4f} price={order['price']:.4f} notional={order['notional']:.4f}"
        )
    print("")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.min_price >= args.max_price:
        raise SystemExit("--min-price must be < --max-price")
    if args.n <= 0:
        raise SystemExit("--n must be > 0")
    if args.total_buy_notional <= 0:
        raise SystemExit("--total-buy-notional must be > 0")
    if args.leverage <= 0:
        raise SystemExit("--leverage must be > 0")
    if args.neutral_anchor_price is not None and args.neutral_anchor_price <= 0:
        raise SystemExit("--neutral-anchor-price must be > 0")

    credentials = load_binance_api_credentials()
    if credentials is None:
        raise SystemExit("请先在本机环境变量中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
    api_key, api_secret = credentials

    symbol = args.symbol.upper().strip()
    symbol_info = fetch_futures_symbol_config(symbol)
    account_info = fetch_futures_account_info_v3(
        api_key,
        api_secret,
        recv_window=args.recv_window,
    )
    position_mode = fetch_futures_position_mode(
        api_key,
        api_secret,
        recv_window=args.recv_window,
    )
    open_orders = fetch_futures_open_orders(
        symbol,
        api_key,
        api_secret,
        recv_window=args.recv_window,
    )
    book_rows = fetch_futures_book_tickers(symbol=symbol)
    premium_rows = fetch_futures_premium_index(symbol=symbol)
    if not book_rows:
        raise SystemExit(f"No book ticker returned for {symbol}")
    if not premium_rows:
        raise SystemExit(f"No premium index returned for {symbol}")
    book = book_rows[0]
    premium = premium_rows[0]
    bid_price = float(book["bid_price"])
    ask_price = float(book["ask_price"])

    if ask_price < args.min_price:
        market_status = "below_range"
    elif bid_price > args.max_price:
        market_status = "above_range"
    else:
        market_status = "in_range"

    legs = build_paper_grid_legs(
        min_price=args.min_price,
        max_price=args.max_price,
        n=args.n,
        total_buy_notional=args.total_buy_notional,
        grid_level_mode=args.grid_level_mode,
        allocation_mode=args.allocation_mode,
        strategy_direction=args.strategy_direction,
        neutral_anchor_price=args.neutral_anchor_price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
    )
    enabled_leg_count = sum(1 for leg in legs if leg.get("state") != "disabled")
    entry_orders = build_live_entry_orders(legs=legs, bid_price=bid_price, ask_price=ask_price)
    funding_rate = premium.get("funding_rate")
    readiness = evaluate_live_readiness(
        symbol=symbol,
        total_buy_notional=args.total_buy_notional,
        leverage=args.leverage,
        strategy_direction=args.strategy_direction,
        market_status=market_status,
        funding_rate=float(funding_rate) if funding_rate is not None else None,
        account_info=account_info,
        position_mode=position_mode,
        open_orders=open_orders,
        entry_orders=entry_orders,
    )

    test_results: list[dict[str, Any]] = []
    if readiness["ok"] and not args.skip_test_orders:
        ts_ms = int(time.time() * 1000)
        for offset, order in enumerate(entry_orders):
            client_order_id = f"gchk{ts_ms % 100000000}{offset + 1}"
            try:
                post_futures_test_order(
                    symbol=symbol,
                    side=order["side"],
                    quantity=float(order["qty"]),
                    price=float(order["price"]),
                    api_key=api_key,
                    api_secret=api_secret,
                    new_client_order_id=client_order_id,
                    recv_window=args.recv_window,
                )
                test_results.append(
                    {
                        "grid_index": order["grid_index"],
                        "side": order["side"],
                        "qty": order["qty"],
                        "price": order["price"],
                        "ok": True,
                    }
                )
            except Exception as exc:
                readiness["errors"].append(f"测试单失败 grid {order['grid_index'] + 1}: {exc}")
                test_results.append(
                    {
                        "grid_index": order["grid_index"],
                        "side": order["side"],
                        "qty": order["qty"],
                        "price": order["price"],
                        "ok": False,
                        "error": str(exc),
                    }
                )
        readiness["ok"] = not readiness["errors"]

    report = {
        "symbol": symbol,
        "min_price": args.min_price,
        "max_price": args.max_price,
        "n": args.n,
        "total_buy_notional": args.total_buy_notional,
        "grid_level_mode": args.grid_level_mode,
        "allocation_mode": args.allocation_mode,
        "strategy_direction": args.strategy_direction,
        "leverage": args.leverage,
        "symbol_info": symbol_info,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "funding_rate": float(funding_rate) if funding_rate is not None else None,
        "market_status": market_status,
        "enabled_leg_count": enabled_leg_count,
        "entry_orders": entry_orders,
        "open_order_count": len(open_orders),
        "test_results": test_results,
        **readiness,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _print_summary(report)
    print(f"JSON report saved: {report_path}")
    if not report["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
