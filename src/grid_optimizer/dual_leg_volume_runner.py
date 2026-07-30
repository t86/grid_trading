"""通用 make+take 双腿刷量执行层。

每轮:在 bid 挂一条 maker 买单 → 轮询成交 → 成交后立刻 taker 市价卖出回平。
买腿走 maker 省掉跨价差,卖腿 taker 保证瞬时中性,只付一条 maker 费+一条 taker 费。
价差太窄/紧迫则退化纯 taker(复用冲刺引擎的 execute_roundtrip)。

默认 dry-run(用真实盘口模拟,不下单);--apply 才真实下单,建议先小额 canary。

成本双基准:gross(全额手续费+价差,用于 $ 预算硬顶)/ effective(手续费×(1-返费)+价差,对标 万6/万8 目标)。
与冲刺不同:本工具用于**有成本约束、无 deadline 压力**的常态刷量,gross 累计亏损撞 --max-total-loss-usdt
(=预算上限,如 $200)即停,**保证不超预算**;紧迫保量场景才把 deadline 传进来退化 taker。

凭证沿用 BINANCE_API_KEY/SECRET。client_order_id 前缀 gd-<sym>-dual。
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data import (
    delete_spot_order,
    fetch_spot_order,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
    post_spot_order,
)
from .dual_leg_volume_engine import (
    DualLegConfig,
    decide_entry,
    decide_pending,
    maker_buy_price,
    should_switch_to_taker,
)
from .sprint_volume_runner import (
    _quantize,
    _round_step,
    _safe_float,
    execute_roundtrip,
    fetch_book,
    flatten_all_base,
    flatten_residual,
    precheck_funds,
)


def _dual_client_order_id(symbol: str, tag: str) -> str:
    compact = symbol.lower().replace("usdt", "u").replace("usdc", "c")
    return f"gd-{compact}-dual-{tag}{str(int(time.time() * 1000))[-8:]}"[:36]


def place_maker_buy(
    *, symbol: str, price: float, qty: float, api_key: str, api_secret: str,
) -> dict[str, Any]:
    """挂一条 LIMIT_MAKER 买单(post-only:若会越价成 taker,交易所直接拒单)。返回下单响应或 error。"""
    try:
        order = post_spot_order(
            symbol=symbol, side="BUY", quantity=qty, price=price,
            api_key=api_key, api_secret=api_secret, order_type="LIMIT_MAKER",
            new_client_order_id=_dual_client_order_id(symbol, "b"),
        )
        return {"ok": True, "order": order}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:160]}


def taker_sell_base(
    *, symbol: str, base_qty: float, qty_step: float, api_key: str, api_secret: str,
) -> dict[str, Any]:
    """MARKET 卖出持有的 base,瞬时回平(保证成交、不留单边)。"""
    qty = _round_step(base_qty, qty_step)
    if qty <= 0:
        return {"ok": False, "reason": "qty_zero", "sell_quote": 0.0, "base_sold": 0.0}
    sell = post_spot_order(
        symbol=symbol, side="SELL", quantity=qty, price=0.0,
        api_key=api_key, api_secret=api_secret, order_type="MARKET",
        new_client_order_id=_dual_client_order_id(symbol, "s"),
    )
    return {"ok": True, "sell_quote": _safe_float(sell.get("cummulativeQuoteQty")),
            "base_sold": _safe_float(sell.get("executedQty"))}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="通用 make+take 双腿刷量(maker优先省价差,taker收口保中性)")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--burst-volume", type=float, required=True, help="本次要刷的成交量(USDT,注意扣已有量)")
    parser.add_argument("--depth-fraction", type=float, default=0.25)
    parser.add_argument("--max-order-notional", type=float, default=200.0)
    parser.add_argument("--min-order-notional", type=float, default=10.0)
    parser.add_argument("--maker-wait-seconds", type=float, default=6.0, help="maker 挂单耐心;超时撤单转重挂/taker")
    parser.add_argument("--maker-improve-ticks", type=int, default=0, help="maker 买价在 bid 上加几tick抢队列(仍<ask),0=贴bid")
    parser.add_argument("--min-maker-spread-bps", type=float, default=2.5, help="价差窄于此直接走taker(maker省不了/挂不动)")
    parser.add_argument("--abnormal-spread-bps", type=float, default=60.0)
    parser.add_argument("--maker-fee-bps", type=float, default=6.0, help="maker 费率(BNB折后),用于成本核算")
    parser.add_argument("--taker-fee-bps", type=float, default=7.5, help="taker 费率(BNB折后),用于成本核算")
    parser.add_argument("--rebate-fraction", type=float, default=0.4, help="手续费返还比例,用于 effective 成本")
    parser.add_argument("--max-total-loss-usdt", type=float, default=200.0,
                        help="gross 累计亏损(价差+全额手续费)硬顶=预算上限,撞到即停,保证不超预算")
    parser.add_argument("--max-wear-per10k", type=float, default=0.0,
                        help="effective 磨损/万U 软闸:有量后超此值即停(0=关)。BABY 目标设8、最好6")
    parser.add_argument("--wear-gate-min-volume", type=float, default=20000.0, help="软闸生效的最小已刷量(滤噪)")
    parser.add_argument(
        "--switch-to-taker-price-wear-per10k",
        type=float,
        default=0.0,
        help="仅价格磨损/万U超过此值后永久切纯taker；0=关闭",
    )
    parser.add_argument(
        "--switch-to-taker-min-volume",
        type=float,
        default=1000.0,
        help="maker→taker价格磨损闸生效前的最小已刷量",
    )
    parser.add_argument("--max-residual-base-notional", type=float, default=60.0, help="残留持仓超此名义额即停")
    parser.add_argument("--prefer-taker", action="store_true", help="强制纯taker(紧迫保量,跳过maker优先)")
    parser.add_argument("--taker-on-maker-timeout", dest="taker_on_maker_timeout", action="store_true", default=True,
                        help="maker挂不上时这轮直接taker保速度,下轮继续试maker(持续混合,默认开)")
    parser.add_argument("--no-taker-on-maker-timeout", dest="taker_on_maker_timeout", action="store_false",
                        help="maker挂不上只重试maker、不退taker(最便宜但最慢)")
    parser.add_argument("--poll-seconds", type=float, default=0.6, help="maker挂单成交轮询间隔")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="每轮间歇")
    parser.add_argument("--status-json", default=None)
    parser.add_argument("--flatten-on-start", action="store_true", help="启动先撤单+平掉残留 base 再开始(干净起步)")
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
    exchange_min_notional = _safe_float(sc.get("min_notional")) or args.min_order_notional
    status_path = Path(args.status_json) if args.status_json else Path("output") / f"{symbol.lower()}_dual_status.json"

    # 优雅退出:收到 SIGTERM/SIGINT 时撤单+平底仓再退,杜绝"中途被杀留底仓"
    if args.apply:
        import signal

        def _graceful(signum, _frame):
            res = flatten_all_base(
                symbol=symbol,
                qty_step=qty_step,
                min_order_notional=exchange_min_notional,
                api_key=api_key,
                api_secret=api_secret,
            )
            print(json.dumps({"shutdown_flatten": res, "signal": signum}, ensure_ascii=False), flush=True)
            raise SystemExit(0)

        for _sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(_sig, _graceful)
            except Exception:
                pass

    if args.flatten_on_start and args.apply:
        pre = flatten_all_base(
            symbol=symbol,
            qty_step=qty_step,
            min_order_notional=exchange_min_notional,
            api_key=api_key,
            api_secret=api_secret,
        )
        print(json.dumps({"flatten_on_start": pre}, ensure_ascii=False))

    if args.apply:
        funds = precheck_funds(symbol=symbol, base_asset_quote="USDT", burst_volume=args.burst_volume,
                               api_key=api_key, api_secret=api_secret)
        print(json.dumps({"precheck_funds": funds}, ensure_ascii=False))
        if funds.get("bnb_low"):
            print(json.dumps({"warning": "BNB 可能不足以覆盖整轮手续费,建议先补 BNB"}, ensure_ascii=False))

    config = DualLegConfig(
        enabled=True, target_total_volume=args.burst_volume, depth_fraction=args.depth_fraction,
        max_order_notional=args.max_order_notional, min_order_notional=args.min_order_notional,
        maker_wait_seconds=args.maker_wait_seconds, maker_improve_ticks=args.maker_improve_ticks,
        min_maker_spread_bps=args.min_maker_spread_bps, abnormal_spread_bps=args.abnormal_spread_bps,
    )
    done_volume = 0.0
    total_spread = 0.0
    total_fee_gross = 0.0
    net_residual_base = 0.0
    roundtrips = 0
    maker_fills = 0
    taker_fills = 0
    forced_taker = bool(args.prefer_taker)
    consecutive_errors = 0
    consecutive_unfilled = 0
    started = datetime.now(timezone.utc).isoformat()
    reb = max(0.0, min(1.0, _safe_float(args.rebate_fraction)))

    def gross_wear() -> float:
        return total_spread + total_fee_gross

    def eff_wear() -> float:
        return total_spread + total_fee_gross * (1.0 - reb)

    def emit(reason: str, extra: dict[str, Any] | None = None) -> None:
        price_p10 = (total_spread / done_volume * 10_000.0) if done_volume > 0 else 0.0
        gross_p10 = (gross_wear() / done_volume * 10_000.0) if done_volume > 0 else 0.0
        eff_p10 = (eff_wear() / done_volume * 10_000.0) if done_volume > 0 else 0.0
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "started": started, "symbol": symbol,
               "dry_run": not args.apply, "reason": reason, "done_volume": round(done_volume, 1),
               "target": args.burst_volume,
               "price_wear_per10k": round(price_p10, 3),
               "gross_wear_per10k": round(gross_p10, 3), "eff_wear_per10k": round(eff_p10, 3),
               "gross_loss_usdt": round(gross_wear(), 2), "eff_loss_usdt": round(eff_wear(), 2),
               "roundtrips": roundtrips, "maker_fills": maker_fills, "taker_fills": taker_fills,
               "forced_taker": forced_taker,
               "net_residual_base": round(net_residual_base, 4), **(extra or {})}
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({k: rec[k] for k in ("reason", "done_volume", "target", "price_wear_per10k",
              "gross_wear_per10k",
              "eff_wear_per10k", "gross_loss_usdt", "roundtrips", "maker_fills", "taker_fills",
              "forced_taker", "net_residual_base")}, ensure_ascii=False))

    def account_roundtrip(*, buy_quote: float, sell_quote: float, base_bought: float, base_sold: float, maker_buy: bool) -> None:
        nonlocal done_volume, total_spread, total_fee_gross, net_residual_base, roundtrips
        vol = buy_quote + sell_quote
        done_volume += vol
        total_spread += (buy_quote - sell_quote)
        buy_fee_bps = args.maker_fee_bps if maker_buy else args.taker_fee_bps
        total_fee_gross += buy_quote * buy_fee_bps / 10_000.0 + sell_quote * args.taker_fee_bps / 10_000.0
        net_residual_base += (base_bought - base_sold)
        roundtrips += 1

    def maybe_flatten(book: dict[str, Any]) -> None:
        nonlocal net_residual_base
        if net_residual_base * max(book["mid"], 0.0) >= args.min_order_notional:
            fl = flatten_residual(symbol=symbol, residual_base=net_residual_base, book=book, qty_step=qty_step,
                                  tick_size=tick_size, min_order_notional=args.min_order_notional,
                                  api_key=api_key, api_secret=api_secret, dry_run=not args.apply)
            net_residual_base -= _safe_float(fl.get("flattened_base"))

    def safety_kill(book: dict[str, Any]) -> str | None:
        if gross_wear() > args.max_total_loss_usdt:
            return "budget_cap_reached"  # 撞预算上限,停(保证不超预算)
        if abs(net_residual_base) * max(book["mid"], 0.0) > args.max_residual_base_notional:
            return "residual_position_kill"
        if (args.max_wear_per10k > 0 and done_volume >= args.wear_gate_min_volume
                and eff_wear() / done_volume * 10_000.0 > args.max_wear_per10k):
            return "wear_gate_exceeded"  # effective 磨损超目标软闸,停下复盘
        return None

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

    def maybe_switch_to_taker() -> bool:
        nonlocal forced_taker
        if forced_taker:
            return False
        if not should_switch_to_taker(
            total_spread_loss=total_spread,
            done_volume=done_volume,
            threshold_per10k=args.switch_to_taker_price_wear_per10k,
            min_volume=args.switch_to_taker_min_volume,
        ):
            return False
        forced_taker = True
        return True

    state = "idle"
    maker_ctx: dict[str, Any] = {}

    while True:
        try:
            book = fetch_book(symbol)
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors > 20:
                emit("book_fetch_failed_kill", {"error": str(exc)[:150], "exit_flatten": exit_flatten()})
                return 1
            time.sleep(min(0.5 * consecutive_errors, 5.0))
            continue
        consecutive_errors = 0

        kill = safety_kill(book)
        if kill:
            emit(kill, {"exit_flatten": exit_flatten()})
            return 0
        if maybe_switch_to_taker():
            emit(
                "switch_to_taker_price_wear",
                {
                    "switch_threshold": args.switch_to_taker_price_wear_per10k,
                    "switch_min_volume": args.switch_to_taker_min_volume,
                },
            )

        if state == "idle":
            action = decide_entry(
                config=config, current_volume=done_volume, mid_price=book["mid"], spread_bps=book["spread_bps"],
                top_bid_notional=book["top_bid_notional"], top_ask_notional=book["top_ask_notional"],
                prefer_taker=forced_taker,
            )
            act = action["action"]
            if act in {"stop", "disabled"}:
                emit(action["reason"], {"exit_flatten": exit_flatten()})
                return 0
            if act == "pause":
                emit(action["reason"])
                time.sleep(max(args.sleep_seconds, 1.0))
                continue

            if act == "taker_roundtrip":
                # 退化纯 taker(价差太窄/紧迫):复用冲刺引擎的 MARKET 自回转
                try:
                    rt = execute_roundtrip(
                        symbol=symbol, notional=action["leg_notional"], book=book, qty_step=qty_step,
                        tick_size=tick_size, api_key=api_key, api_secret=api_secret, dry_run=not args.apply,
                        order_type="market",
                    )
                except Exception as exc:
                    emit("taker_roundtrip_error", {"error": str(exc)[:150]})
                    time.sleep(max(args.sleep_seconds, 0.5))
                    continue
                if rt.get("ok"):
                    account_roundtrip(buy_quote=rt["buy_quote"], sell_quote=rt["sell_quote"],
                                      base_bought=rt["base_bought"], base_sold=rt["base_sold"], maker_buy=False)
                    taker_fills += 1
                    maybe_flatten(book)
                    emit("taker", {"last_volume": round(rt["volume"], 1)})
                time.sleep(max(args.sleep_seconds, 0.0))
                continue

            # post_maker
            price = maker_buy_price(bid=book["bid"], ask=book["ask"], tick_size=tick_size,
                                    improve_ticks=args.maker_improve_ticks)
            price = _quantize(price, tick_size, up=False) if tick_size > 0 else price
            qty = _round_step(action["leg_notional"] / price if price > 0 else 0.0, qty_step)
            if qty <= 0 or price <= 0:
                emit("maker_size_zero")
                time.sleep(max(args.sleep_seconds, 0.5))
                continue

            if not args.apply:
                # dry-run:模拟 maker 在 bid 成交、taker 在 bid 卖出(价差≈0,只付费)
                buy_quote = qty * price
                sell_quote = qty * book["bid"]
                account_roundtrip(buy_quote=buy_quote, sell_quote=sell_quote, base_bought=qty, base_sold=qty, maker_buy=True)
                maker_fills += 1
                emit("maker_dry", {"last_volume": round(buy_quote + sell_quote, 1)})
                time.sleep(max(args.sleep_seconds, 0.0))
                continue

            res = place_maker_buy(symbol=symbol, price=price, qty=qty, api_key=api_key, api_secret=api_secret)
            if not res.get("ok"):
                # LIMIT_MAKER 被拒(多半瞬时越价)→ 短暂退避重试;连续失败则本轮退化 taker
                consecutive_unfilled += 1
                emit("maker_post_rejected", {"error": res.get("error"), "consecutive": consecutive_unfilled})
                time.sleep(max(args.sleep_seconds, 0.4))
                continue
            order = res["order"]
            maker_ctx = {
                "client_id": order.get("clientOrderId"),
                "order_id": order.get("orderId"),
                "price": price, "qty": qty, "order_notional": qty * price,
                "t0": time.time(),
            }
            state = "maker_pending"
            time.sleep(max(args.poll_seconds, 0.1))
            continue

        # ---- state == maker_pending ----
        try:
            od = fetch_spot_order(symbol=symbol, api_key=api_key, api_secret=api_secret,
                                  order_id=maker_ctx.get("order_id"),
                                  orig_client_order_id=maker_ctx.get("client_id"))
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors > 20:
                emit("order_query_failed_kill", {"error": str(exc)[:150], "exit_flatten": exit_flatten()})
                return 1
            time.sleep(min(0.5 * consecutive_errors, 3.0))
            continue
        consecutive_errors = 0

        filled_quote = _safe_float(od.get("cummulativeQuoteQty"))
        filled_base = _safe_float(od.get("executedQty"))
        elapsed = time.time() - _safe_float(maker_ctx.get("t0"))
        p = decide_pending(config=config, filled_notional=filled_quote,
                           order_notional=maker_ctx.get("order_notional", 0.0), elapsed_seconds=elapsed)
        pa = p["action"]

        if pa == "wait":
            time.sleep(max(args.poll_seconds, 0.1))
            continue

        # 需要收口或撤单:除"complete"全成外,先撤掉残余挂单(防 taker 卖腿与自己买单自成交)
        if pa in {"cancel_and_complete", "cancel_unfilled"}:
            try:
                delete_spot_order(symbol=symbol, api_key=api_key, api_secret=api_secret,
                                  order_id=maker_ctx.get("order_id"), orig_client_order_id=maker_ctx.get("client_id"))
            except Exception:
                pass  # 已成交/已不存在
            # 撤单后再查一次最终成交量(撤单瞬间可能又成交了一点)
            try:
                od2 = fetch_spot_order(symbol=symbol, api_key=api_key, api_secret=api_secret,
                                       order_id=maker_ctx.get("order_id"), orig_client_order_id=maker_ctx.get("client_id"))
                filled_quote = _safe_float(od2.get("cummulativeQuoteQty"))
                filled_base = _safe_float(od2.get("executedQty"))
            except Exception:
                pass

        if pa == "cancel_unfilled" and filled_base <= 0:
            consecutive_unfilled += 1
            state = "idle"
            # 挂不上(厚队列/快市):这轮直接 taker 保速度,下轮继续试 maker——持续混合,绝不永久放弃 maker
            if args.taker_on_maker_timeout and not args.prefer_taker:
                try:
                    rt = execute_roundtrip(
                        symbol=symbol, notional=maker_ctx.get("order_notional", args.min_order_notional),
                        book=book, qty_step=qty_step, tick_size=tick_size, api_key=api_key, api_secret=api_secret,
                        dry_run=not args.apply, order_type="market",
                    )
                    if rt.get("ok"):
                        account_roundtrip(buy_quote=rt["buy_quote"], sell_quote=rt["sell_quote"],
                                          base_bought=rt["base_bought"], base_sold=rt["base_sold"], maker_buy=False)
                        taker_fills += 1
                        maybe_flatten(book)
                        emit("taker_after_maker_timeout", {"consecutive_unfilled": consecutive_unfilled})
                except Exception as exc:
                    emit("taker_fallback_error", {"error": str(exc)[:150]})
            else:
                emit("maker_unfilled", {"consecutive": consecutive_unfilled, "elapsed": round(elapsed, 1)})
            time.sleep(max(args.sleep_seconds, 0.0))
            continue

        # complete / cancel_and_complete:把已买到的 base 用 taker 市价卖出回平
        consecutive_unfilled = 0
        buy_quote = filled_quote
        base_bought = filled_base
        if base_bought <= 0:
            state = "idle"
            emit("maker_zero_fill")
            continue
        try:
            ts = taker_sell_base(symbol=symbol, base_qty=base_bought, qty_step=qty_step,
                                 api_key=api_key, api_secret=api_secret)
        except Exception as exc:
            # 卖腿失败时立即停止并清仓，禁止带着残留继续开下一轮。
            net_residual_base += base_bought
            state = "idle"
            emit(
                "taker_sell_error",
                {
                    "error": str(exc)[:150],
                    "stuck_base": base_bought,
                    "exit_flatten": exit_flatten(),
                },
            )
            return 1
        account_roundtrip(buy_quote=buy_quote, sell_quote=ts.get("sell_quote", 0.0),
                          base_bought=base_bought, base_sold=ts.get("base_sold", 0.0), maker_buy=True)
        maker_fills += 1
        maybe_flatten(book)
        state = "idle"
        emit("maker_roundtrip", {"last_volume": round(buy_quote + ts.get("sell_quote", 0.0), 1),
                                 "maker_fill_frac": round(base_bought / max(maker_ctx.get("qty", 1.0), 1e-9), 2)})
        time.sleep(max(args.sleep_seconds, 0.0))


if __name__ == "__main__":
    raise SystemExit(main())
