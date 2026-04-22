from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .backtest import run_backtest
from .data import load_or_fetch_candles, load_or_fetch_funding_rates
from .symbol_lists import get_symbol_list
from .types import BacktestResult, Candle, FundingRate

COMPETITION_SYMBOLS: tuple[str, ...] = (
    "SOONUSDT",
    "BTCUSDC",
    "ETHUSDC",
    "XAUUSDT",
    "XAGUSDT",
    "CLUSDT",
    "BZUSDT",
    "ORDIUSDC",
)

COMPETITION_PROFILE_PRESETS: dict[str, dict[str, Any]] = {
    "conservative": {
        "price_band_ratio": 0.03,
        "n": 20,
        "grid_level_mode": "arithmetic",
        "allocation_mode": "linear_reverse",
        "strategy_direction": "long",
        "total_buy_notional": 10000.0,
        "fee_rate": 0.0002,
        "slippage": 0.0,
    },
    "aggressive": {
        "price_band_ratio": 0.04,
        "n": 25,
        "grid_level_mode": "arithmetic",
        "allocation_mode": "linear_reverse",
        "strategy_direction": "long",
        "total_buy_notional": 10000.0,
        "fee_rate": 0.0002,
        "slippage": 0.0,
    },
}


def competition_symbols() -> list[str]:
    return get_symbol_list("competition")


def competition_profile_keys() -> list[str]:
    return list(COMPETITION_PROFILE_PRESETS.keys())


def build_competition_strategy(reference_price: float, profile_key: str) -> dict[str, Any]:
    if reference_price <= 0:
        raise ValueError("reference_price must be > 0")
    normalized_key = str(profile_key).strip().lower()
    preset = COMPETITION_PROFILE_PRESETS.get(normalized_key)
    if preset is None:
        raise ValueError(f"Unknown competition profile: {profile_key}")
    band_ratio = float(preset["price_band_ratio"])
    return {
        "profile": normalized_key,
        "min_price": reference_price * (1.0 - band_ratio),
        "max_price": reference_price * (1.0 + band_ratio),
        "n": int(preset["n"]),
        "grid_level_mode": str(preset["grid_level_mode"]),
        "allocation_mode": str(preset["allocation_mode"]),
        "strategy_direction": str(preset["strategy_direction"]),
        "total_buy_notional": float(preset["total_buy_notional"]),
        "fee_rate": float(preset["fee_rate"]),
        "slippage": float(preset["slippage"]),
    }


def summarize_backtest_result(result: BacktestResult) -> dict[str, Any]:
    return {
        "gross_trade_notional": result.gross_trade_notional,
        "turnover_multiple": result.turnover_multiple,
        "net_profit": result.net_profit,
        "total_fees": result.total_fees,
        "funding_pnl": result.funding_pnl,
        "trade_count": result.trade_count,
        "max_drawdown": result.max_drawdown,
        "avg_capital_usage": result.avg_capital_usage,
        "start_time": result.start_time.isoformat(),
        "end_time": result.end_time.isoformat(),
    }


def _slice_candles(candles: list[Candle], start_time: datetime, end_time: datetime) -> list[Candle]:
    return [item for item in candles if start_time <= item.open_time < end_time]


def _slice_funding_rates(
    funding_rates: list[FundingRate],
    start_time: datetime,
    end_time: datetime,
) -> list[FundingRate]:
    return [item for item in funding_rates if start_time <= item.ts < end_time]


def _window_range(candles: list[Candle]) -> dict[str, float]:
    return {
        "low": min(item.low for item in candles),
        "high": max(item.high for item in candles),
    }


def build_competition_symbol_report(
    *,
    symbol: str,
    window_days: list[int],
    profile_keys: list[str],
    interval: str = "1m",
    cache_dir: str = "data",
    refresh: bool = False,
    end_time: datetime | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    supported_symbols = competition_symbols()
    if normalized_symbol not in supported_symbols:
        raise ValueError(
            f"Unsupported competition symbol: {symbol}. "
            f"Supported: {', '.join(supported_symbols)}"
        )
    normalized_windows = sorted({int(item) for item in window_days if int(item) > 0}, reverse=True)
    if not normalized_windows:
        raise ValueError("window_days must contain at least one positive integer")
    normalized_profiles = [str(item).strip().lower() for item in profile_keys if str(item).strip()]
    if not normalized_profiles:
        raise ValueError("profile_keys cannot be empty")

    end_dt = (end_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start_dt = end_dt - timedelta(days=max(normalized_windows))
    candles = load_or_fetch_candles(
        symbol=normalized_symbol,
        interval=interval,
        start_time=start_dt,
        end_time=end_dt,
        cache_dir=cache_dir,
        refresh=refresh,
    )
    funding_rates = load_or_fetch_funding_rates(
        symbol=normalized_symbol,
        start_time=start_dt,
        end_time=end_dt,
        cache_dir=cache_dir,
        refresh=refresh,
    )
    reference_price = candles[-1].close
    market_ranges: dict[str, dict[str, float]] = {}
    strategies: dict[str, Any] = {}
    for days in normalized_windows:
        window_start = end_dt - timedelta(days=days)
        sliced = _slice_candles(candles, window_start, end_dt)
        if not sliced:
            raise RuntimeError(f"No candle data available for {normalized_symbol} window {days}d")
        market_ranges[f"{days}d"] = _window_range(sliced)

    for profile_key in normalized_profiles:
        strategy = build_competition_strategy(reference_price=reference_price, profile_key=profile_key)
        metrics: dict[str, Any] = {}
        for days in normalized_windows:
            window_start = end_dt - timedelta(days=days)
            sliced_candles = _slice_candles(candles, window_start, end_dt)
            sliced_funding = _slice_funding_rates(funding_rates, window_start, end_dt)
            result = run_backtest(
                candles=sliced_candles,
                min_price=strategy["min_price"],
                max_price=strategy["max_price"],
                n=strategy["n"],
                total_buy_notional=strategy["total_buy_notional"],
                grid_level_mode=strategy["grid_level_mode"],
                allocation_mode=strategy["allocation_mode"],
                strategy_direction=strategy["strategy_direction"],
                fee_rate=strategy["fee_rate"],
                slippage=strategy["slippage"],
                funding_rates=sliced_funding,
            )
            metrics[f"{days}d"] = summarize_backtest_result(result)
        strategies[profile_key] = {
            "min_price": strategy["min_price"],
            "max_price": strategy["max_price"],
            "n": strategy["n"],
            "grid_level_mode": strategy["grid_level_mode"],
            "allocation_mode": strategy["allocation_mode"],
            "strategy_direction": strategy["strategy_direction"],
            "total_buy_notional": strategy["total_buy_notional"],
            "fee_rate": strategy["fee_rate"],
            "slippage": strategy["slippage"],
            "window_metrics": metrics,
        }

    return {
        "symbol": normalized_symbol,
        "generated_at_utc": end_dt.isoformat(),
        "market_snapshot": {
            "last_close": reference_price,
            "ranges": market_ranges,
        },
        "strategies": strategies,
    }
