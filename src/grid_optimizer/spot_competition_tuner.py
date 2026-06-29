from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .data import fetch_spot_agg_trades, fetch_spot_book_tickers, fetch_spot_klines, fetch_spot_symbol_config
from .types import Candle


EPSILON = 1e-12
_BACKTEST_INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
}


@dataclass(frozen=True)
class SpotCompetitionTuningInputs:
    symbol: str
    budget_quote: float = 1000.0
    risk_level: str = "balanced"
    window_minutes: int = 180
    interval: str = "1m"
    target_mode: str = "inventory_grid"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number else default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low), high)


def _round_up_to_step(value: float, step: float | None) -> float:
    if not step or step <= EPSILON:
        return value
    units = int((value + step - EPSILON) / step)
    return max(units * step, step)


def _parse_utc_datetime(value: Any, field_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO datetime") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _downsample_points(rows: list[dict[str, Any]], limit: int = 240) -> list[dict[str, Any]]:
    if len(rows) <= limit:
        return rows
    step = max(int(len(rows) / limit), 1)
    sampled = rows[::step]
    if sampled[-1] != rows[-1]:
        sampled.append(rows[-1])
    return sampled


def _trade_notional(trade: dict[str, Any]) -> float:
    return max(_safe_float(trade.get("p")), 0.0) * max(_safe_float(trade.get("q")), 0.0)


def _trade_time_ms(trade: dict[str, Any]) -> int:
    return max(_safe_int(trade.get("T")), 0)


def _fetch_spot_agg_trades_window(
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    max_pages: int = 8,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    for _ in range(max(max_pages, 1)):
        if cursor >= end_ms:
            break
        page = fetch_spot_agg_trades(symbol, cursor, end_ms)
        if not page:
            break
        rows.extend(page)
        last_time = max(_trade_time_ms(trade) for trade in page)
        next_cursor = last_time + 1
        if next_cursor <= cursor or len(page) < 1000:
            break
        cursor = next_cursor
    return rows


def _market_metrics(
    *,
    candles: list[Candle],
    agg_trades: list[dict[str, Any]],
    bid_price: float,
    ask_price: float,
    bid_qty: float,
    ask_qty: float,
) -> dict[str, Any]:
    if not candles:
        raise ValueError("candles is required")
    first = candles[0]
    last = candles[-1]
    mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else last.close
    returns: list[float] = []
    amplitudes: list[float] = []
    previous_close = first.open
    for candle in candles:
        if previous_close > EPSILON:
            returns.append((candle.close - previous_close) / previous_close)
        if candle.open > EPSILON:
            amplitudes.append((candle.high - candle.low) / candle.open)
        previous_close = candle.close
    quote_volume = sum(_trade_notional(trade) for trade in agg_trades)
    realized_volatility = (sum(item * item for item in returns) / max(len(returns), 1)) ** 0.5
    avg_amplitude = sum(amplitudes) / max(len(amplitudes), 1)
    trend_return = (last.close - first.open) / first.open if first.open > EPSILON else 0.0
    spread_ratio = (ask_price - bid_price) / mid_price if mid_price > EPSILON else 0.0
    book_depth_notional = (bid_qty * bid_price) + (ask_qty * ask_price)
    imbalance = ((bid_qty * bid_price) - (ask_qty * ask_price)) / max(book_depth_notional, EPSILON)
    return {
        "mid_price": mid_price,
        "first_open": first.open,
        "last_close": last.close,
        "candle_count": len(candles),
        "quote_volume": quote_volume,
        "quote_volume_per_minute": quote_volume / max(len(candles), 1),
        "realized_volatility": realized_volatility,
        "avg_amplitude_ratio": avg_amplitude,
        "trend_return_ratio": trend_return,
        "spread_ratio": max(spread_ratio, 0.0),
        "book_depth_notional": book_depth_notional,
        "orderbook_imbalance": imbalance,
    }


def _classify_market(metrics: dict[str, Any]) -> dict[str, str]:
    vol = _safe_float(metrics.get("avg_amplitude_ratio"))
    volume_per_min = _safe_float(metrics.get("quote_volume_per_minute"))
    spread = _safe_float(metrics.get("spread_ratio"))
    trend = abs(_safe_float(metrics.get("trend_return_ratio")))

    volatility_bucket = "low" if vol < 0.0015 else "mid" if vol < 0.0045 else "high"
    liquidity_bucket = "high" if volume_per_min >= 250_000 and spread <= 0.0008 else "mid" if volume_per_min >= 25_000 else "low"
    trend_bucket = "flat" if trend < 0.003 else "directional" if trend < 0.012 else "strong"
    if liquidity_bucket == "high" and volatility_bucket == "low":
        regime = "tight_liquid"
    elif liquidity_bucket in {"high", "mid"} and volatility_bucket == "high":
        regime = "active_volatile"
    elif liquidity_bucket == "low" and volatility_bucket == "high":
        regime = "thin_volatile"
    elif liquidity_bucket == "low":
        regime = "thin_quiet"
    else:
        regime = "balanced"
    return {
        "regime": regime,
        "volatility_bucket": volatility_bucket,
        "liquidity_bucket": liquidity_bucket,
        "trend_bucket": trend_bucket,
    }


def _risk_scales(risk_level: str) -> dict[str, float]:
    normalized = str(risk_level or "balanced").strip().lower()
    if normalized in {"conservative", "safe", "low"}:
        return {"budget": 0.70, "step": 1.35, "inventory": 0.70, "orders": 0.80}
    if normalized in {"aggressive", "high"}:
        return {"budget": 1.25, "step": 0.80, "inventory": 1.20, "orders": 1.20}
    return {"budget": 1.0, "step": 1.0, "inventory": 1.0, "orders": 1.0}


def recommend_spot_competition_config(
    *,
    inputs: SpotCompetitionTuningInputs,
    metrics: dict[str, Any],
    symbol_config: dict[str, Any],
) -> dict[str, Any]:
    symbol = inputs.symbol.upper().strip()
    budget = max(float(inputs.budget_quote), 0.0)
    mid_price = max(_safe_float(metrics.get("mid_price")), EPSILON)
    tick_size = _safe_float(symbol_config.get("tick_size")) or None
    min_notional = _safe_float(symbol_config.get("min_notional"), 5.0) or 5.0
    classification = _classify_market(metrics)
    regime = classification["regime"]
    scales = _risk_scales(inputs.risk_level)

    base_step_ratio = {
        "tight_liquid": 0.00055,
        "balanced": 0.0012,
        "active_volatile": 0.0024,
        "thin_quiet": 0.0018,
        "thin_volatile": 0.0045,
    }[regime]
    vol_extra = _clamp(_safe_float(metrics.get("avg_amplitude_ratio")) * 0.45, 0.0, 0.006)
    step_price = _round_up_to_step(mid_price * (base_step_ratio + vol_extra) * scales["step"], tick_size)

    budget_scale = {
        "tight_liquid": 0.050,
        "balanced": 0.030,
        "active_volatile": 0.018,
        "thin_quiet": 0.012,
        "thin_volatile": 0.007,
    }[regime]
    per_order_notional = max(min_notional * 1.15, budget * budget_scale * scales["budget"])
    max_single_order = max(budget * 0.08, min_notional * 1.15)
    per_order_notional = min(per_order_notional, max_single_order)

    level_base = {
        "tight_liquid": (10, 10),
        "balanced": (8, 8),
        "active_volatile": (6, 7),
        "thin_quiet": (5, 5),
        "thin_volatile": (3, 4),
    }[regime]
    buy_levels = max(1, int(round(level_base[0] * scales["orders"])))
    sell_levels = max(1, int(round(level_base[1] * scales["orders"])))

    hard_ratio = {
        "tight_liquid": 0.55,
        "balanced": 0.42,
        "active_volatile": 0.28,
        "thin_quiet": 0.22,
        "thin_volatile": 0.12,
    }[regime] * scales["inventory"]
    hard_limit = max(per_order_notional * 2.0, budget * _clamp(hard_ratio, 0.05, 0.65))
    soft_limit = max(per_order_notional * 1.5, hard_limit * 0.62)
    threshold_position = max(per_order_notional * 1.5, soft_limit * 0.45)
    max_order_position = max(per_order_notional * 2.0, hard_limit * 0.78)

    trend_abs = abs(_safe_float(metrics.get("trend_return_ratio")))
    fast_stop_ratio = _clamp(max(_safe_float(metrics.get("avg_amplitude_ratio")) * 2.2, 0.003), 0.003, 0.025)
    slow_trend_enabled = trend_abs >= 0.004 or classification["volatility_bucket"] == "high"

    synthetic_mode = str(inputs.target_mode or "").strip().lower() in {"synthetic", "synthetic_neutral", "neutral"}
    strategy_mode = (
        "spot_competition_synthetic_neutral_grid"
        if synthetic_mode
        else "spot_competition_inventory_grid"
    )
    neutral_base_qty = (hard_limit * 0.5 / mid_price) if synthetic_mode else 0.0
    config = {
        "market_type": "spot",
        "strategy_mode": strategy_mode,
        "symbol": symbol,
        "grid_level_mode": "arithmetic",
        "min_price": mid_price * 0.97,
        "max_price": mid_price * 1.03,
        "n": 30,
        "total_quote_budget": budget,
        "step_price": step_price,
        "per_order_notional": per_order_notional,
        "first_order_multiplier": 1.0 if regime in {"active_volatile", "thin_volatile"} else 1.5,
        "threshold_position_notional": threshold_position,
        "threshold_reduce_target_notional": max(per_order_notional, threshold_position * 0.55),
        "warmup_position_notional": 0.0,
        "require_non_loss_exit": False,
        "spot_taker_exit_enabled": False,
        "spot_fast_stop_enabled": True,
        "spot_fast_stop_down_only": False,
        "spot_fast_stop_10s_abs_return_ratio": fast_stop_ratio,
        "spot_fast_stop_10s_amplitude_ratio": fast_stop_ratio * 1.4,
        "spot_fast_stop_30s_abs_return_ratio": fast_stop_ratio * 1.4,
        "spot_fast_stop_30s_amplitude_ratio": fast_stop_ratio * 1.9,
        "spot_fast_stop_freeze_position_notional": max(per_order_notional, soft_limit * 0.5),
        "spot_fast_stop_exit_position_notional": 0.0,
        "spot_fast_stop_reduce_target_notional": max(per_order_notional, threshold_position * 0.5),
        "spot_app_loss_guard_enabled": True,
        "spot_app_loss_recovery_reduce_only_enabled": True,
        "spot_slow_trend_step_enabled": slow_trend_enabled,
        "spot_slow_trend_step_5m_return_ratio": max(0.002, trend_abs * 0.35),
        "spot_slow_trend_step_15m_return_ratio": max(0.004, trend_abs * 0.75),
        "spot_slow_trend_step_5m_amplitude_ratio": max(0.004, _safe_float(metrics.get("avg_amplitude_ratio")) * 2.0),
        "spot_slow_trend_step_15m_amplitude_ratio": max(0.008, _safe_float(metrics.get("avg_amplitude_ratio")) * 3.5),
        "spot_slow_trend_step_scale": 1.4 if regime in {"active_volatile", "thin_volatile"} else 1.2,
        "max_order_position_notional": max_order_position,
        "max_position_notional": hard_limit,
        "neutral_base_qty": neutral_base_qty,
        "max_short_position_notional": hard_limit if strategy_mode.endswith("synthetic_neutral_grid") else 0.0,
        "elastic_volume_enabled": True,
        "sleep_seconds": 3.0 if regime in {"tight_liquid", "active_volatile"} else 5.0,
        "cancel_stale": True,
        "apply": True,
        "reset_state": False,
        "inventory_soft_limit_notional": soft_limit,
        "inventory_hard_limit_notional": hard_limit,
        "max_single_cycle_new_orders": min(max(buy_levels + sell_levels, 4), 16),
    }
    notes = [
        "仅生成 maker 竞赛配置草案；保存不会启动策略。",
        "默认关闭 taker exit，保留 LIMIT_MAKER 执行路径。",
    ]
    if regime == "thin_volatile":
        notes.append("盘口薄且波动高，建议小仓位试跑并严格观察库存。")
    if classification["trend_bucket"] != "flat":
        notes.append("趋势较明显，已打开慢趋势步长放大。")
    if synthetic_mode:
        notes.append("合成中性模式已按预算估算 neutral_base_qty，启动前请核对现货 Base 余额。")
    return {
        "ok": True,
        "symbol": symbol,
        "classification": classification,
        "metrics": metrics,
        "recommended_config": config,
        "notes": notes,
    }


def build_spot_competition_recommendation(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    symbol = str(payload.get("symbol") or "BTCUSDT").upper().strip()
    if not symbol:
        raise ValueError("symbol is required")
    window_minutes = max(30, min(_safe_int(payload.get("window_minutes"), 180), 24 * 60))
    interval = str(payload.get("interval") or "1m").strip() or "1m"
    budget = max(_safe_float(payload.get("budget_quote"), 1000.0), 0.0)
    risk_level = str(payload.get("risk_level") or "balanced").strip().lower() or "balanced"
    target_mode = str(payload.get("target_mode") or "inventory_grid").strip().lower()
    resolved_now = now or datetime.now(timezone.utc)
    start = resolved_now - timedelta(minutes=window_minutes)
    symbol_config = fetch_spot_symbol_config(symbol)
    book_rows = fetch_spot_book_tickers(symbol)
    if not book_rows:
        raise RuntimeError(f"missing spot book ticker for {symbol}")
    book = book_rows[0]
    candles = fetch_spot_klines(
        symbol,
        interval,
        int(start.timestamp() * 1000),
        int(resolved_now.timestamp() * 1000),
    )
    if len(candles) < 5:
        raise RuntimeError(f"not enough spot klines for {symbol}: {len(candles)}")
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(resolved_now.timestamp() * 1000)
    agg_trades = _fetch_spot_agg_trades_window(symbol=symbol, start_ms=start_ms, end_ms=end_ms)
    metrics = _market_metrics(
        candles=candles,
        agg_trades=agg_trades,
        bid_price=_safe_float(book.get("bid_price")),
        ask_price=_safe_float(book.get("ask_price")),
        bid_qty=_safe_float(book.get("bid_qty")),
        ask_qty=_safe_float(book.get("ask_qty")),
    )
    return recommend_spot_competition_config(
        inputs=SpotCompetitionTuningInputs(
            symbol=symbol,
            budget_quote=budget,
            risk_level=risk_level,
            window_minutes=window_minutes,
            interval=interval,
            target_mode=target_mode,
        ),
        metrics=metrics,
        symbol_config=symbol_config,
    )


def _simulate_spot_competition_backtest(
    *,
    symbol: str,
    candles: list[Candle],
    config: dict[str, Any],
    interval: str,
    start_time: datetime,
    end_time: datetime,
    maker_fee_rate: float,
) -> dict[str, Any]:
    if len(candles) < 2:
        raise RuntimeError(f"not enough spot klines for {symbol}: {len(candles)}")

    budget = max(_safe_float(config.get("total_quote_budget"), 0.0), 0.0)
    if budget <= EPSILON:
        raise ValueError("config.total_quote_budget must be > 0")
    per_order_notional = max(_safe_float(config.get("per_order_notional"), 0.0), 0.0)
    if per_order_notional <= EPSILON:
        raise ValueError("config.per_order_notional must be > 0")
    step_price = max(_safe_float(config.get("step_price"), 0.0), 0.0)
    if step_price <= EPSILON:
        raise ValueError("config.step_price must be > 0")

    max_position_notional = max(
        _safe_float(config.get("max_position_notional"), budget * 0.5),
        per_order_notional,
    )
    max_cycle_orders = max(_safe_int(config.get("max_single_cycle_new_orders"), 8), 2)
    buy_levels = max(1, min(max_cycle_orders // 2, 12))

    cash = budget
    lots: list[dict[str, float]] = []
    buy_count = 0
    sell_count = 0
    gross_trade_notional = 0.0
    total_fees = 0.0
    realized_gross_pnl = 0.0
    max_inventory_notional = 0.0
    inventory_notional_sum = 0.0
    peak_equity = budget
    max_drawdown = 0.0
    equity_points: list[dict[str, Any]] = []
    trade_samples: list[dict[str, Any]] = []

    def inventory_qty() -> float:
        return sum(lot["qty"] for lot in lots)

    def inventory_cost() -> float:
        return sum(lot["cost"] for lot in lots)

    def record_trade(row: dict[str, Any]) -> None:
        if len(trade_samples) < 80:
            trade_samples.append(row)

    previous_close = candles[0].open
    for candle in candles:
        reference_price = previous_close if previous_close > EPSILON else candle.open

        remaining_lots: list[dict[str, float]] = []
        for lot in lots:
            sell_price = lot["price"] + step_price
            if candle.high + EPSILON >= sell_price:
                notional = lot["qty"] * sell_price
                fee = notional * maker_fee_rate
                cash += notional - fee
                gross_trade_notional += notional
                total_fees += fee
                sell_count += 1
                realized = notional - lot["cost"]
                realized_gross_pnl += realized
                record_trade(
                    {
                        "time": candle.open_time.isoformat(),
                        "side": "SELL",
                        "price": sell_price,
                        "qty": lot["qty"],
                        "notional": notional,
                        "gross_pnl": realized,
                    }
                )
            else:
                remaining_lots.append(lot)
        lots = remaining_lots

        current_inventory_notional = inventory_qty() * candle.close
        for level in range(1, buy_levels + 1):
            buy_price = reference_price - step_price * level
            if buy_price <= EPSILON or candle.low > buy_price + EPSILON:
                continue
            if current_inventory_notional + per_order_notional > max_position_notional + EPSILON:
                break
            fee = per_order_notional * maker_fee_rate
            required_cash = per_order_notional + fee
            if cash + EPSILON < required_cash:
                break
            qty = per_order_notional / buy_price
            cash -= required_cash
            lots.append({"qty": qty, "price": buy_price, "cost": per_order_notional})
            current_inventory_notional += qty * candle.close
            gross_trade_notional += per_order_notional
            total_fees += fee
            buy_count += 1
            record_trade(
                {
                    "time": candle.open_time.isoformat(),
                    "side": "BUY",
                    "price": buy_price,
                    "qty": qty,
                    "notional": per_order_notional,
                    "gross_pnl": None,
                }
            )

        final_inventory_qty = inventory_qty()
        inventory_value = final_inventory_qty * candle.close
        inventory_notional_sum += inventory_value
        max_inventory_notional = max(max_inventory_notional, inventory_value)
        equity = cash + inventory_value
        peak_equity = max(peak_equity, equity)
        if peak_equity > EPSILON:
            max_drawdown = max(max_drawdown, (peak_equity - equity) / peak_equity)
        equity_points.append(
            {
                "time": candle.close_time.isoformat(),
                "price": candle.close,
                "equity": equity,
                "inventory_notional": inventory_value,
            }
        )
        previous_close = candle.close

    end_price = candles[-1].close
    final_inventory_qty = inventory_qty()
    final_inventory_notional = final_inventory_qty * end_price
    final_inventory_cost = inventory_cost()
    unrealized_gross_pnl = final_inventory_notional - final_inventory_cost
    equity_end = cash + final_inventory_notional
    net_pnl = equity_end - budget
    trade_count = buy_count + sell_count
    duration_days = max((candles[-1].close_time - candles[0].open_time).total_seconds() / 86400.0, EPSILON)

    return {
        "ok": True,
        "symbol": symbol,
        "interval": interval,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "candle_count": len(candles),
        "summary": {
            "budget_quote": budget,
            "maker_fee_rate": maker_fee_rate,
            "start_price": candles[0].open,
            "end_price": end_price,
            "underlying_return_ratio": (end_price - candles[0].open) / candles[0].open if candles[0].open > EPSILON else 0.0,
            "gross_trade_notional": gross_trade_notional,
            "turnover_multiple": gross_trade_notional / max(budget, EPSILON),
            "annualized_turnover_multiple": (gross_trade_notional / max(budget, EPSILON)) / duration_days * 365.0,
            "trade_count": trade_count,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "realized_gross_pnl": realized_gross_pnl,
            "unrealized_gross_pnl": unrealized_gross_pnl,
            "total_fees": total_fees,
            "net_pnl": net_pnl,
            "return_ratio": net_pnl / max(budget, EPSILON),
            "max_drawdown_ratio": max_drawdown,
            "final_inventory_qty": final_inventory_qty,
            "final_inventory_notional": final_inventory_notional,
            "max_inventory_notional": max_inventory_notional,
            "avg_inventory_notional": inventory_notional_sum / max(len(candles), 1),
            "cash_end": cash,
            "equity_end": equity_end,
        },
        "equity_points": _downsample_points(equity_points),
        "trade_samples": trade_samples,
        "notes": [
            "回测只使用公开 K 线，不会读取账户、不提交订单。",
            "成交按 K 线 high/low 触碰挂单价估算，未模拟盘口排队、撤单延迟和同根 K 线真实路径。",
            "卖出按买入价加一个 step_price 的 maker 网格近似处理，用于比较参数稳健性而非精确复盘。",
        ],
    }


def build_spot_competition_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    config = payload.get("config")
    if not isinstance(config, dict):
        recommendation = build_spot_competition_recommendation(payload)
        config = recommendation.get("recommended_config")
    if not isinstance(config, dict):
        raise ValueError("config is required")

    symbol = str(payload.get("symbol") or config.get("symbol") or "BTCUSDT").upper().strip()
    if not symbol:
        raise ValueError("symbol is required")
    interval = str(payload.get("backtest_interval") or payload.get("interval") or "1m").strip() or "1m"
    if interval not in _BACKTEST_INTERVAL_MS:
        raise ValueError(f"unsupported backtest interval: {interval}")
    start_time = _parse_utc_datetime(payload.get("start_time"), "start_time")
    end_time = _parse_utc_datetime(payload.get("end_time"), "end_time")
    if start_time >= end_time:
        raise ValueError("start_time must be before end_time")

    interval_ms = _BACKTEST_INTERVAL_MS[interval]
    estimated_candles = int((end_time - start_time).total_seconds() * 1000 / interval_ms)
    max_candles = 20_000
    if estimated_candles > max_candles:
        max_days = max_candles * interval_ms / 86_400_000
        raise ValueError(f"backtest range is too large for {interval}; max about {max_days:.1f} days")

    maker_fee_rate = _clamp(_safe_float(payload.get("maker_fee_rate"), 0.0002), 0.0, 0.005)
    candles = fetch_spot_klines(
        symbol,
        interval,
        int(start_time.timestamp() * 1000),
        int(end_time.timestamp() * 1000),
    )
    candles = [candle for candle in candles if candle.open_time >= start_time and candle.open_time < end_time]
    return _simulate_spot_competition_backtest(
        symbol=symbol,
        candles=candles,
        config=config,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        maker_fee_rate=maker_fee_rate,
    )
