"""冲刺刷量执行层。

竞赛最后一天用：taker IOC 自回转，短时间确定性爆出大量成交清门槛。
默认 dry-run（用真实盘口模拟、不下单）；--apply 才真实下单，且建议先小额 canary。

可靠性要点：
- IOC 限价单（不挂在盘上、立即成交否则撤），带价格保护防薄盘滑点。
- 买卖等量瞬时回平；每轮核对净持仓，残留超阈值即暂停告警（不让方向敞口累积）。
- 深度感知定量 + 磨损预算闸门 + 目标感知（决策核 sprint_volume_engine）。
- kill switch：到目标量 / 超磨损预算 / 残留持仓 / 最大轮数，任一触发即停。

凭证沿用 BINANCE_API_KEY/SECRET。成交 client_order_id 前缀 gs-<sym>-sprint。
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from typing import Any

from .data import (
    _http_signed_request_json,
    fetch_spot_book_tickers,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
    post_spot_order,
)
from .sprint_volume_engine import SprintConfig, resolve_sprint_action


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def fetch_book(symbol: str) -> dict[str, Any]:
    """盘口快照：最优买卖价、价差、顶档名义额。"""
    rows = fetch_spot_book_tickers(symbol)
    row = rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else {})
    bid = _safe_float(row.get("bid_price") or row.get("bidPrice"))
    ask = _safe_float(row.get("ask_price") or row.get("askPrice"))
    bid_qty = _safe_float(row.get("bid_qty") or row.get("bidQty"))
    ask_qty = _safe_float(row.get("ask_qty") or row.get("askQty"))
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
    spread_bps = ((ask - bid) / mid * 10_000.0) if mid > 0 else 0.0
    return {
        "bid": bid, "ask": ask, "mid": mid, "spread_bps": spread_bps,
        "top_bid_notional": bid * bid_qty, "top_ask_notional": ask * ask_qty,
    }


def _quantize(value: float, increment: float, *, up: bool = False) -> float:
    """用 Decimal 精确量化到 increment 的整数倍，避免浮点残差导致交易所 -1111 精度报错。"""
    if increment <= 0:
        return float(value)
    inc = Decimal(str(increment))
    q = (Decimal(str(value)) / inc).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN) * inc
    return float(q)


def _round_step(qty: float, step: float) -> float:
    return _quantize(qty, step, up=False)


def _sprint_client_order_id(symbol: str, side: str) -> str:
    compact = symbol.lower().replace("usdt", "u").replace("usdc", "c")
    return f"gs-{compact}-sprint-{side[0].lower()}{str(int(time.time() * 1000))[-8:]}"[:36]


def flatten_all_base(
    *,
    symbol: str,
    qty_step: float,
    min_order_notional: float,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    """撤销本交易对挂单并卖出可成交的全部 base，返回绝对中性状态。"""
    base_asset = symbol.upper()
    for quote_asset in ("USDT", "USDC", "FDUSD", "BUSD", "TUSD"):
        if base_asset.endswith(quote_asset):
            base_asset = base_asset[: -len(quote_asset)]
            break

    result: dict[str, Any] = {}
    try:
        _http_signed_request_json(
            "https://api.binance.com/api/v3/openOrders",
            {"symbol": symbol},
            api_key,
            api_secret,
            method="DELETE",
        )
        result["canceled"] = True
    except Exception:
        result["canceled"] = False

    try:
        account = _http_signed_request_json(
            "https://api.binance.com/api/v3/account",
            {},
            api_key,
            api_secret,
            method="GET",
        )
        free = next(
            (
                _safe_float(balance.get("free"))
                for balance in account.get("balances", [])
                if balance.get("asset") == base_asset
            ),
            0.0,
        )
        qty = _round_step(free, qty_step)
        if qty <= 0:
            result["flattened_base"] = 0.0
            return result

        book = fetch_book(symbol)
        if qty * max(book["mid"], 0.0) < max(min_order_notional, 0.0):
            result["flattened_base"] = 0.0
            result["dust_base"] = qty
            return result

        sell = post_spot_order(
            symbol=symbol,
            side="SELL",
            quantity=qty,
            price=0.0,
            api_key=api_key,
            api_secret=api_secret,
            order_type="MARKET",
            new_client_order_id=_sprint_client_order_id(symbol, "FLAT"),
        )
        result["flattened_base"] = _safe_float(sell.get("executedQty"))
        result["recovered_quote"] = _safe_float(sell.get("cummulativeQuoteQty"))
    except Exception as exc:
        result["error"] = str(exc)[:150]
    return result


def execute_roundtrip(
    *,
    symbol: str,
    notional: float,
    book: dict[str, Any],
    qty_step: float,
    tick_size: float,
    api_key: str,
    api_secret: str,
    dry_run: bool,
    slippage_bps: float = 5.0,
    order_type: str = "market",
) -> dict[str, Any]:
    """一次 taker 自回转：买入 notional，立即卖出买到的量。返回成交额/手续费/残留。

    order_type:
    - "market"（默认，推荐）：MARKET 市价两腿都**保证成交**，价格就算跑了几tick也不会留单边；
      代价是无价格上限（靠紧价差护栏+深度感知控成本）。
    - "ioc"：IOC 限价带 slippage 缓冲，有价格上限，但价格跑超缓冲时第二腿可能挂不上→残留（由flatten兜底）。
    """
    bid, ask, mid = book["bid"], book["ask"], book["mid"]
    qty = _round_step(notional / ask if ask > 0 else 0.0, qty_step)
    if qty <= 0:
        return {"ok": False, "reason": "qty_zero", "volume": 0.0, "residual_base": 0.0}

    if dry_run:
        # 模拟：假设在顶档全额成交，估算价差成本（买在 ask、卖在 bid）
        buy_quote = qty * ask
        sell_quote = qty * bid
        return {
            "ok": True, "dry_run": True,
            "buy_quote": buy_quote, "sell_quote": sell_quote, "base_bought": qty, "base_sold": qty,
            "residual_base": 0.0, "volume": buy_quote + sell_quote,
            "spread_cost": buy_quote - sell_quote,
        }

    use_market = str(order_type).lower() == "market"
    if use_market:
        buy = post_spot_order(
            symbol=symbol, side="BUY", quantity=qty, price=0.0,
            api_key=api_key, api_secret=api_secret, order_type="MARKET",
            new_client_order_id=_sprint_client_order_id(symbol, "BUY"),
        )
    else:
        # IOC 限价：买价对齐 tick 上浮（吃顶档、不追深）
        buy_price = _quantize(ask * (1.0 + slippage_bps / 10_000.0), tick_size, up=True)
        buy = post_spot_order(
            symbol=symbol, side="BUY", quantity=qty, price=buy_price,
            api_key=api_key, api_secret=api_secret, order_type="LIMIT", time_in_force="IOC",
            new_client_order_id=_sprint_client_order_id(symbol, "BUY"),
        )
    base_bought = _safe_float(buy.get("executedQty"))
    buy_quote = _safe_float(buy.get("cummulativeQuoteQty"))
    if base_bought <= 0:
        return {"ok": False, "reason": "buy_unfilled", "volume": 0.0, "residual_base": 0.0, "buy": buy}

    sell_qty = _round_step(base_bought, qty_step)
    try:
        if use_market:
            sell = post_spot_order(
                symbol=symbol, side="SELL", quantity=sell_qty, price=0.0,
                api_key=api_key, api_secret=api_secret, order_type="MARKET",
                new_client_order_id=_sprint_client_order_id(symbol, "SELL"),
            )
        else:
            sell_price = _quantize(bid * (1.0 - slippage_bps / 10_000.0), tick_size, up=False)
            sell = post_spot_order(
                symbol=symbol, side="SELL", quantity=sell_qty, price=sell_price,
                api_key=api_key, api_secret=api_secret, order_type="LIMIT", time_in_force="IOC",
                new_client_order_id=_sprint_client_order_id(symbol, "SELL"),
            )
    except Exception as exc:
        # 买腿已经成交时不能丢失敞口信息；交给调用方立即 flatten。
        return {
            "ok": True,
            "dry_run": False,
            "reason": "sell_error",
            "error": str(exc)[:150],
            "buy_quote": buy_quote,
            "sell_quote": 0.0,
            "base_bought": base_bought,
            "base_sold": 0.0,
            "residual_base": base_bought,
            "volume": buy_quote,
            "spread_cost": 0.0,
        }
    base_sold = _safe_float(sell.get("executedQty"))
    sell_quote = _safe_float(sell.get("cummulativeQuoteQty"))
    residual = base_bought - base_sold
    return {
        "ok": True, "dry_run": False,
        "buy_quote": buy_quote, "sell_quote": sell_quote,
        "base_bought": base_bought, "base_sold": base_sold,
        "residual_base": residual, "volume": buy_quote + sell_quote,
        "spread_cost": buy_quote - sell_quote,
    }


def flatten_residual(
    *, symbol: str, residual_base: float, book: dict[str, Any], qty_step: float, tick_size: float,
    min_order_notional: float, api_key: str, api_secret: str, dry_run: bool,
) -> dict[str, Any]:
    """主动平掉残留底仓（卖单 IOC 没全成时留下的方向敞口），保持持仓中性。

    残留在现货下恒为多头（买到没卖完）→ 市价/IOC 卖出。残留小于最小名义额则保留（无法成交），
    由调用方的累计残留 kill 兜底。返回卖出的 base 量（用于回冲 net_residual）。
    """
    residual = max(_safe_float(residual_base), 0.0)
    notional = residual * max(book["mid"], 0.0)
    if notional < max(_safe_float(min_order_notional), 0.0):
        return {"flattened_base": 0.0, "reason": "below_min"}
    qty = _round_step(residual, qty_step)
    if qty <= 0:
        return {"flattened_base": 0.0, "reason": "qty_zero"}
    if dry_run:
        return {"flattened_base": qty, "dry_run": True}
    try:
        sell = post_spot_order(
            symbol=symbol, side="SELL", quantity=qty, price=_quantize(book["bid"] * (1.0 - 0.001), tick_size, up=False),
            api_key=api_key, api_secret=api_secret, order_type="LIMIT", time_in_force="IOC",
            new_client_order_id=_sprint_client_order_id(symbol, "FLAT"),
        )
        return {"flattened_base": _safe_float(sell.get("executedQty")), "order": sell}
    except Exception as exc:
        return {"flattened_base": 0.0, "error": str(exc)[:150]}


def precheck_funds(*, symbol: str, base_asset_quote: str, burst_volume: float, api_key: str, api_secret: str) -> dict[str, Any]:
    """冲刺前资金预检：报告 USDT(买入用) 和 BNB(手续费) 余额，估算够不够。失败不阻断（返回 unknown）。"""
    try:
        from .data import _http_signed_request_json

        acct = _http_signed_request_json("https://api.binance.com/api/v3/account", {}, api_key, api_secret, method="GET")
        bals = {b["asset"]: _safe_float(b.get("free")) for b in acct.get("balances", [])}
        usdt = bals.get("USDT", 0.0)
        bnb = bals.get("BNB", 0.0)
        # 估算：手续费 ~ burst_volume × 7.5bps；USDT 需求 ~ 单笔在途（小，因即买即卖回收）
        fee_need_usdt = burst_volume * 7.5 / 10_000.0
        return {"ok": True, "usdt_free": usdt, "bnb_free": bnb, "est_fee_usdt": fee_need_usdt,
                "bnb_low": bnb * 600 < fee_need_usdt}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:150]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="冲刺刷量引擎（taker IOC 自回转，竞赛末日爆量）")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--burst-volume", type=float, required=True, help="本次要补的成交量（USDT）")
    parser.add_argument("--depth-fraction", type=float, default=0.25)
    parser.add_argument("--max-order-notional", type=float, default=300.0)
    parser.add_argument("--min-order-notional", type=float, default=10.0)
    parser.add_argument("--abnormal-spread-bps", type=float, default=40.0, help="仅防极端异常价差的短暂重试，不是效率闸门")
    parser.add_argument("--max-take-spread-bps", type=float, default=0.0,
                        help="紧价差护栏：只在价差≤此值时take，宽于此等收窄。0=不限(XAUT)；薄盘/变宽价差币(NIGHT)设1-2tick对应bps控成本")
    parser.add_argument("--order-type", choices=["market", "ioc"], default="market",
                        help="market(默认,推荐)=市价两腿保证成交不留单边；ioc=限价有价格上限但价格跑超缓冲会留残留")
    parser.add_argument("--taker-fee-bps", type=float, default=7.5, help="taker 费率，仅用于磨损监控上报")
    parser.add_argument("--max-total-loss-usdt", type=float, default=400.0,
                        help="灾难性 sanity 兜底：累计亏损超此值即停（应设为低于清掉该档解锁的奖励额，绝非万8效率线）")
    parser.add_argument("--max-residual-base-notional", type=float, default=80.0, help="残留持仓超此名义额即停（自动平之后仍超才停）")
    parser.add_argument("--max-roundtrips", type=int, default=1000000, help="kill switch：最大回转轮数")
    parser.add_argument("--sleep-seconds", type=float, default=0.3)
    parser.add_argument("--status-json", default=None)
    parser.add_argument("--flatten-on-start", action="store_true", help="启动先撤单并卖掉残留 base")
    parser.add_argument("--apply", action="store_true", help="真实下单；缺省 dry-run 用真实盘口模拟")
    args = parser.parse_args(argv)

    creds = load_binance_api_credentials()
    if not creds:
        raise SystemExit("Binance API credentials required")
    api_key, api_secret = creds
    symbol = args.symbol.upper().strip()
    sc = fetch_spot_symbol_config(symbol)
    qty_step = _safe_float(sc.get("step_size")) or 1.0
    tick_size = _safe_float(sc.get("tick_size")) or 0.0
    exchange_min_notional = _safe_float(sc.get("min_notional")) or args.min_order_notional
    status_path = Path(args.status_json) if args.status_json else Path("output") / f"{symbol.lower()}_sprint_status.json"

    def exit_flatten() -> dict[str, Any] | None:
        if not args.apply:
            return None
        return flatten_all_base(
            symbol=symbol,
            qty_step=qty_step,
            min_order_notional=exchange_min_notional,
            api_key=api_key,
            api_secret=api_secret,
        )

    if args.apply:
        import signal

        def _graceful(signum, _frame):
            result = exit_flatten()
            print(json.dumps({"shutdown_flatten": result, "signal": signum}, ensure_ascii=False), flush=True)
            raise SystemExit(0)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _graceful)
            except Exception:
                pass

    if args.flatten_on_start and args.apply:
        print(json.dumps({"flatten_on_start": exit_flatten()}, ensure_ascii=False))

    if args.apply:
        funds = precheck_funds(symbol=symbol, base_asset_quote="USDT", burst_volume=args.burst_volume,
                               api_key=api_key, api_secret=api_secret)
        print(json.dumps({"precheck_funds": funds}, ensure_ascii=False))
        if funds.get("bnb_low"):
            print(json.dumps({"warning": "BNB 可能不足以覆盖整轮手续费，建议先补 BNB"}, ensure_ascii=False))

    config = SprintConfig(
        enabled=True, target_total_volume=args.burst_volume, depth_fraction=args.depth_fraction,
        max_order_notional=args.max_order_notional, min_order_notional=args.min_order_notional,
        abnormal_spread_bps=args.abnormal_spread_bps, max_take_spread_bps=args.max_take_spread_bps,
    )
    done_volume = 0.0
    total_wear = 0.0
    net_residual_base = 0.0
    roundtrips = 0
    consecutive_errors = 0
    started = datetime.now(timezone.utc).isoformat()

    def emit(reason: str, extra: dict[str, Any] | None = None) -> None:
        wear_p10 = (total_wear / done_volume * 10_000.0) if done_volume > 0 else 0.0
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "started": started, "symbol": symbol,
               "dry_run": not args.apply, "reason": reason, "done_volume": round(done_volume, 1),
               "target": args.burst_volume, "wear_per10k": round(wear_p10, 3), "total_loss_usdt": round(total_wear, 2),
               "roundtrips": roundtrips, "net_residual_base": net_residual_base, **(extra or {})}
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({k: rec[k] for k in ("reason", "done_volume", "target", "wear_per10k", "total_loss_usdt", "roundtrips", "net_residual_base")}, ensure_ascii=False))

    while True:
        try:
            book = fetch_book(symbol)
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors > 20:
                emit("book_fetch_failed_kill", {"error": str(exc)[:150], "exit_flatten": exit_flatten()})
                return 1
            time.sleep(min(0.5 * consecutive_errors, 5.0))  # 限频/网络退避
            continue
        consecutive_errors = 0

        # 灾难性兜底（不是磨损预算！只防失控）
        if total_wear > args.max_total_loss_usdt:
            emit("max_total_loss_kill", {"exit_flatten": exit_flatten()})
            return 0
        residual_notional = abs(net_residual_base) * max(book["mid"], 0.0)
        if residual_notional > args.max_residual_base_notional:
            emit("residual_position_kill", {"exit_flatten": exit_flatten()})
            return 0
        if roundtrips >= args.max_roundtrips:
            emit("max_roundtrips_kill", {"exit_flatten": exit_flatten()})
            return 0

        action = resolve_sprint_action(
            config=config, current_volume=done_volume, mid_price=book["mid"], spread_bps=book["spread_bps"],
            top_bid_notional=book["top_bid_notional"], top_ask_notional=book["top_ask_notional"],
        )
        if action["action"] in {"stop", "disabled"}:
            emit(action["reason"], {"exit_flatten": exit_flatten()})
            return 0
        if action["action"] == "pause":
            emit(action["reason"])
            time.sleep(max(args.sleep_seconds, 1.0))  # 极端价差/深度薄/无价：等待重试，不放弃
            continue

        # roundtrip
        try:
            rt = execute_roundtrip(
                symbol=symbol, notional=action["roundtrip_notional"], book=book, qty_step=qty_step,
                tick_size=tick_size, api_key=api_key, api_secret=api_secret, dry_run=not args.apply,
                order_type=args.order_type,
            )
        except Exception as exc:
            consecutive_errors += 1
            emit("roundtrip_error", {"error": str(exc)[:150]})
            time.sleep(min(0.5 * consecutive_errors, 5.0))
            continue
        if rt.get("ok"):
            done_volume += rt["volume"]
            total_wear += max(rt.get("spread_cost", 0.0), 0.0) + rt["volume"] * args.taker_fee_bps / 10_000.0
            net_residual_base += rt.get("residual_base", 0.0)
            roundtrips += 1
            if rt.get("reason") == "sell_error":
                flattened = exit_flatten()
                if flattened:
                    net_residual_base -= _safe_float(flattened.get("flattened_base"))
                emit(
                    "sell_error_kill",
                    {"error": rt.get("error"), "exit_flatten": flattened},
                )
                return 1
            # 残留自动平：每轮把卖单没全成留下的多头平掉，保持中性
            if net_residual_base * max(book["mid"], 0.0) >= args.min_order_notional:
                fl = flatten_residual(symbol=symbol, residual_base=net_residual_base, book=book, qty_step=qty_step,
                                      tick_size=tick_size, min_order_notional=args.min_order_notional,
                                      api_key=api_key, api_secret=api_secret, dry_run=not args.apply)
                net_residual_base -= _safe_float(fl.get("flattened_base"))
            emit("burst", {"last_roundtrip_volume": round(rt["volume"], 1)})
            time.sleep(max(args.sleep_seconds, 0.0))
        else:
            emit("roundtrip_unfilled", {"detail": rt.get("reason")})
            time.sleep(max(args.sleep_seconds, 0.3))


if __name__ == "__main__":
    raise SystemExit(main())
