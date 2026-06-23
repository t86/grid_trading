"""混合 Maker-入场 / Taker-平腿 刷量执行层。

现货交易赛刷量:每回合在买一/卖一**双边各挂一个 LIMIT_MAKER**(post-only,低费/返佣),
轮询哪边先被吃 → **撤掉另一边** → 用 **MARKET(Taker)反向平掉**成交腿回到中性 → 重新双挂。

相比:
- 纯 Maker 网格:单向行情挂单不被吃会空转(0/h);本工具同样靠 Maker 入场,但平腿用 Taker 保证回中性。
- 纯 Taker 冲刺(sprint_volume_runner):两腿都 taker(贵);本工具入场腿省成 Maker,单回合磨损更低。

设计要点:
- **死盘不退化为 Taker**:双边 Maker 挂着没被吃,就撤了按最新盘口追价重挂,绝不转 Taker。
- LIMIT_MAKER 被拒(-2010,会立即成交)→ 该回合 maker_rejected,调用方贴价重试。
- 每回合净敞口归零(撤另一边 + Taker 平),不破坏底舱中性。
- 复用 sprint 的盘口/量化/残留兜底/闸门基础设施,保持风格一致。

默认 dry-run;--apply 才真实下单。凭证沿用 BINANCE_API_KEY/SECRET。
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from typing import Any, Callable

from .data import (
    delete_spot_order,
    fetch_spot_book_tickers,
    fetch_spot_order,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
    post_spot_order,
)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def fetch_book(symbol: str) -> dict[str, Any]:
    """盘口快照:最优买卖价、价差、顶档名义额。"""
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
    """用 Decimal 精确量化到 increment 的整数倍,避免浮点残差触发交易所 -1111 精度报错。"""
    if increment <= 0:
        return float(value)
    inc = Decimal(str(increment))
    q = (Decimal(str(value)) / inc).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN) * inc
    return float(q)


def _round_step(qty: float, step: float) -> float:
    return _quantize(qty, step, up=False)


def _hybrid_client_order_id(symbol: str, tag: str) -> str:
    compact = symbol.lower().replace("usdt", "u").replace("usdc", "c")
    return f"gh-{compact}-{tag}-{str(int(time.time() * 1000))[-8:]}"[:36]


def _is_filled(state: dict[str, Any]) -> float:
    """返回成交的 base 量(executedQty>0 即视为该腿被吃)。"""
    return _safe_float(state.get("executedQty"))


def execute_hybrid_roundtrip(
    *,
    symbol: str,
    notional: float,
    book: dict[str, Any],
    qty_step: float,
    tick_size: float,
    poll_max_cycles: int,
    place_maker: Callable[..., str],
    query: Callable[..., dict[str, Any]],
    cancel: Callable[..., None],
    place_taker: Callable[..., dict[str, Any]],
    poll_interval: float = 0.0,
    maker_offset_ticks: int = 1,
    dry_run: bool = False,
) -> dict[str, Any]:
    """一次混合自回转:双边挂 Maker → 先成交边撤另一边 + Taker 反向平。

    通过注入的 place_maker/query/cancel/place_taker 回调与交易所交互(便于测试)。
    返回 {ok, volume(两腿成交额和), maker_quote, taker_quote, residual_base, reason?}。
    """
    bid, ask, mid = book["bid"], book["ask"], book["mid"]
    qty = _round_step(notional / ask if ask > 0 else 0.0, qty_step)
    if qty <= 0:
        return {"ok": False, "reason": "qty_zero", "volume": 0.0, "residual_base": 0.0}

    if dry_run:
        # 模拟:假设 Maker 买腿在 bid 成交、Taker 卖腿吃 bid;两腿名义都计入量。
        maker_quote = qty * bid
        taker_quote = qty * bid
        return {
            "ok": True, "dry_run": True,
            "volume": maker_quote + taker_quote, "maker_quote": maker_quote, "taker_quote": taker_quote,
            "residual_base": 0.0, "spread_cost": (ask - bid) * qty,
        }

    # 1) 双边挂 LIMIT_MAKER(post-only)。任一边被拒(-2010 立即成交)→ 撤已挂的、回报 maker_rejected。
    placed: dict[str, str] = {}
    try:
        # 退 maker_offset_ticks 档挂单:买价压低、卖价抬高,保证 post-only 不会立即成交被拒
        # (超窄价差盘口如 WLD 价差仅 1 tick 时,贴 bid/ask 挂常被 -2010 拒)
        offset = max(maker_offset_ticks, 0) * tick_size
        buy_price = _quantize(bid - offset, tick_size)
        sell_price = _quantize(ask + offset, tick_size, up=True)
        placed["BUY"] = place_maker(side="BUY", price=buy_price, qty=qty)
        placed["SELL"] = place_maker(side="SELL", price=sell_price, qty=qty)
    except Exception as exc:
        for coid in placed.values():
            try:
                cancel(coid=coid)
            except Exception:
                pass
        return {"ok": False, "reason": "maker_rejected", "volume": 0.0, "residual_base": 0.0,
                "detail": str(exc)[:150]}

    # 2) 轮询哪边先成交
    filled_side = None
    filled_state: dict[str, Any] = {}
    for _ in range(max(poll_max_cycles, 1)):
        buy_state = query(coid=placed["BUY"])
        if _is_filled(buy_state) > 0:
            filled_side, filled_state = "BUY", buy_state
            break
        sell_state = query(coid=placed["SELL"])
        if _is_filled(sell_state) > 0:
            filled_side, filled_state = "SELL", sell_state
            break
        if poll_interval > 0:
            time.sleep(poll_interval)

    # 3a) 双边都没成交 → 撤双边,不下任何 Taker(死盘不退化)
    if filled_side is None:
        for coid in placed.values():
            try:
                cancel(coid=coid)
            except Exception:
                pass
        return {"ok": False, "reason": "maker_unfilled", "volume": 0.0, "residual_base": 0.0}

    # 3b) 撤掉另一边未成交的 Maker
    other_side = "SELL" if filled_side == "BUY" else "BUY"
    try:
        cancel(coid=placed[other_side])
    except Exception:
        pass

    maker_base = _is_filled(filled_state)
    maker_quote = _safe_float(filled_state.get("cummulativeQuoteQty"))

    # 4) Taker 反向平掉成交腿:BUY 被吃 → 市价 SELL;SELL 被吃 → 市价 BUY
    taker_side = "SELL" if filled_side == "BUY" else "BUY"
    taker_qty = _round_step(maker_base, qty_step)
    taker = place_taker(side=taker_side, qty=taker_qty)
    taker_base = _safe_float(taker.get("executedQty"))
    taker_quote = _safe_float(taker.get("cummulativeQuoteQty"))

    # 残留:现货买腿被吃后卖不净 → 多头残留;卖腿被吃后买不回 → 空头残留(以正名义量上报)
    residual = maker_base - taker_base
    return {
        "ok": True, "dry_run": False, "filled_side": filled_side,
        "volume": maker_quote + taker_quote, "maker_quote": maker_quote, "taker_quote": taker_quote,
        "residual_base": residual, "spread_cost": abs(maker_quote - taker_quote),
    }


def flatten_residual(
    *, symbol: str, residual_base: float, book: dict[str, Any], qty_step: float, tick_size: float,
    min_order_notional: float, api_key: str, api_secret: str, dry_run: bool,
) -> dict[str, Any]:
    """主动平掉残留底仓(Taker 平腿没全成留下的方向敞口),保持持仓中性。

    残留为正(多头买到没卖完)→ 卖出;残留小于最小名义额则保留,由调用方累计残留 kill 兜底。
    """
    residual = _safe_float(residual_base)
    side = "SELL" if residual > 0 else "BUY"
    notional = abs(residual) * max(book["mid"], 0.0)
    if notional < max(_safe_float(min_order_notional), 0.0):
        return {"flattened_base": 0.0, "reason": "below_min"}
    qty = _round_step(abs(residual), qty_step)
    if qty <= 0:
        return {"flattened_base": 0.0, "reason": "qty_zero"}
    if dry_run:
        return {"flattened_base": qty if side == "SELL" else -qty, "dry_run": True}
    try:
        order = post_spot_order(
            symbol=symbol, side=side, quantity=qty, price=0.0,
            api_key=api_key, api_secret=api_secret, order_type="MARKET",
            new_client_order_id=_hybrid_client_order_id(symbol, "flat"),
        )
        flat = _safe_float(order.get("executedQty"))
        return {"flattened_base": flat if side == "SELL" else -flat, "order": order}
    except Exception as exc:
        return {"flattened_base": 0.0, "error": str(exc)[:150]}


def precheck_funds(*, symbol: str, burst_volume: float, maker_fee_bps: float, taker_fee_bps: float,
                   api_key: str, api_secret: str) -> dict[str, Any]:
    """资金预检:报告 USDT/BNB 余额,估算手续费够不够。失败不阻断。"""
    try:
        from .data import _http_signed_request_json

        acct = _http_signed_request_json("https://api.binance.com/api/v3/account", {}, api_key, api_secret, method="GET")
        bals = {b["asset"]: _safe_float(b.get("free")) for b in acct.get("balances", [])}
        usdt = bals.get("USDT", 0.0)
        bnb = bals.get("BNB", 0.0)
        # 混合:Maker 腿 + Taker 腿各约 burst_volume/2 名义
        fee_need_usdt = burst_volume / 2.0 * (maker_fee_bps + taker_fee_bps) / 10_000.0
        return {"ok": True, "usdt_free": usdt, "bnb_free": bnb, "est_fee_usdt": fee_need_usdt,
                "bnb_low": bnb * 600 < fee_need_usdt}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:150]}


def _make_exchange_callbacks(symbol: str, api_key: str, api_secret: str):
    """生产环境回调:绑定真实交易所下单/查单/撤单。"""

    def place_maker(*, side, price, qty):
        order = post_spot_order(
            symbol=symbol, side=side, quantity=qty, price=price,
            api_key=api_key, api_secret=api_secret, order_type="LIMIT_MAKER",
            new_client_order_id=_hybrid_client_order_id(symbol, side[0].lower()),
        )
        return str(order.get("clientOrderId") or order.get("orderId"))

    def query(*, coid):
        return fetch_spot_order(symbol=symbol, api_key=api_key, api_secret=api_secret,
                                orig_client_order_id=coid)

    def cancel(*, coid):
        delete_spot_order(symbol=symbol, api_key=api_key, api_secret=api_secret,
                          orig_client_order_id=coid)

    def place_taker(*, side, qty):
        return post_spot_order(
            symbol=symbol, side=side, quantity=qty, price=0.0,
            api_key=api_key, api_secret=api_secret, order_type="MARKET",
            new_client_order_id=_hybrid_client_order_id(symbol, f"t{side[0].lower()}"),
        )

    return place_maker, query, cancel, place_taker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="混合刷量引擎(Maker 入场 + Taker 平腿,现货交易赛)")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--burst-volume", type=float, required=True, help="本次要补的成交量(USDT)")
    parser.add_argument("--max-order-notional", type=float, default=300.0)
    parser.add_argument("--min-order-notional", type=float, default=10.0)
    parser.add_argument("--per-order-notional", type=float, default=200.0, help="每回合单腿挂单名义额")
    parser.add_argument("--max-take-spread-bps", type=float, default=0.0,
                        help="紧价差护栏:价差宽于此则等待收窄(0=不限,XAUT 盘口好可 0)")
    parser.add_argument("--poll-interval", type=float, default=0.3, help="Maker 挂单成交轮询间隔(秒)")
    parser.add_argument("--poll-max-cycles", type=int, default=10, help="每回合最多轮询多少次,耗尽未成交则追价重挂")
    parser.add_argument("--maker-offset-ticks", type=int, default=1, help="Maker 挂单退几档(买压低/卖抬高),避免超窄价差被-2010拒;0=贴盘口")
    parser.add_argument("--maker-fee-bps", type=float, default=4.0, help="Maker 净费率(返佣后),磨损上报用")
    parser.add_argument("--taker-fee-bps", type=float, default=5.4, help="Taker 净费率(返佣后),磨损上报用")
    parser.add_argument("--max-total-loss-usdt", type=float, default=400.0,
                        help="灾难性兜底:累计磨损超此值即停(设为低于清掉该档解锁的奖励额)")
    parser.add_argument("--max-residual-base-notional", type=float, default=80.0, help="残留持仓超此名义额即停")
    parser.add_argument("--max-roundtrips", type=int, default=1000000, help="kill switch:最大回转轮数")
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--status-json", default=None)
    parser.add_argument("--apply", action="store_true", help="真实下单;缺省 dry-run 用真实盘口模拟")
    args = parser.parse_args(argv)

    creds = load_binance_api_credentials()
    if not creds:
        raise SystemExit("Binance API credentials required")
    api_key, api_secret = creds
    symbol = args.symbol.upper().strip()
    sc = fetch_spot_symbol_config(symbol)
    qty_step = _safe_float(sc.get("step_size")) or 1.0
    tick_size = _safe_float(sc.get("tick_size")) or 0.0
    status_path = Path(args.status_json) if args.status_json else Path("output") / f"{symbol.lower()}_hybrid_status.json"

    if args.apply:
        funds = precheck_funds(symbol=symbol, burst_volume=args.burst_volume,
                               maker_fee_bps=args.maker_fee_bps, taker_fee_bps=args.taker_fee_bps,
                               api_key=api_key, api_secret=api_secret)
        print(json.dumps({"precheck_funds": funds}, ensure_ascii=False))
        if funds.get("bnb_low"):
            print(json.dumps({"warning": "BNB 可能不足以覆盖整轮手续费,建议先补 BNB"}, ensure_ascii=False))

    place_maker, query, cancel, place_taker = _make_exchange_callbacks(symbol, api_key, api_secret)

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
        if done_volume >= args.burst_volume:
            emit("target_reached")
            return 0
        try:
            book = fetch_book(symbol)
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors > 20:
                emit("book_fetch_failed_kill", {"error": str(exc)[:150]})
                return 1
            time.sleep(min(0.5 * consecutive_errors, 5.0))
            continue
        consecutive_errors = 0

        if total_wear > args.max_total_loss_usdt:
            emit("max_total_loss_kill")
            return 0
        residual_notional = abs(net_residual_base) * max(book["mid"], 0.0)
        if residual_notional > args.max_residual_base_notional:
            emit("residual_position_kill")
            return 0
        if roundtrips >= args.max_roundtrips:
            emit("max_roundtrips_kill")
            return 0

        # 价差护栏:盘口太宽则等待收窄(不放弃)
        if args.max_take_spread_bps > 0 and book["spread_bps"] > args.max_take_spread_bps:
            emit("spread_wide_wait", {"spread_bps": round(book["spread_bps"], 2)})
            time.sleep(max(args.sleep_seconds, 0.5))
            continue

        notional = min(args.per_order_notional, args.max_order_notional, args.burst_volume - done_volume)
        if notional < args.min_order_notional:
            notional = args.min_order_notional

        try:
            rt = execute_hybrid_roundtrip(
                symbol=symbol, notional=notional, book=book, qty_step=qty_step, tick_size=tick_size,
                poll_max_cycles=args.poll_max_cycles, poll_interval=args.poll_interval,
                maker_offset_ticks=args.maker_offset_ticks,
                place_maker=place_maker, query=query, cancel=cancel, place_taker=place_taker,
                dry_run=not args.apply,
            )
        except Exception as exc:
            consecutive_errors += 1
            emit("roundtrip_error", {"error": str(exc)[:150]})
            time.sleep(min(0.5 * consecutive_errors, 5.0))
            continue

        if rt.get("ok"):
            done_volume += rt["volume"]
            total_wear += (
                _safe_float(rt.get("maker_quote")) * args.maker_fee_bps / 10_000.0
                + _safe_float(rt.get("taker_quote")) * args.taker_fee_bps / 10_000.0
                + max(rt.get("spread_cost", 0.0), 0.0)
            )
            net_residual_base += rt.get("residual_base", 0.0)
            roundtrips += 1
            if abs(net_residual_base) * max(book["mid"], 0.0) >= args.min_order_notional:
                fl = flatten_residual(symbol=symbol, residual_base=net_residual_base, book=book, qty_step=qty_step,
                                      tick_size=tick_size, min_order_notional=args.min_order_notional,
                                      api_key=api_key, api_secret=api_secret, dry_run=not args.apply)
                net_residual_base -= _safe_float(fl.get("flattened_base"))
            emit("burst", {"last_roundtrip_volume": round(rt["volume"], 1)})
            time.sleep(max(args.sleep_seconds, 0.0))
        else:
            # maker_unfilled(死盘)/maker_rejected(贴价太紧)→ 不退化,短暂等待后按最新盘口重挂
            emit(rt.get("reason", "roundtrip_unfilled"), {"detail": rt.get("detail")})
            time.sleep(max(args.sleep_seconds, 0.3))


if __name__ == "__main__":
    raise SystemExit(main())
