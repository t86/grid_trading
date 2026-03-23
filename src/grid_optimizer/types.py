from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Candle:
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class Trade:
    ts: datetime
    side: str
    grid_index: int
    price: float
    qty: float
    notional: float
    fee: float


@dataclass(frozen=True)
class FundingRate:
    ts: datetime
    rate: float


@dataclass(frozen=True)
class FundingEvent:
    ts: datetime
    rate: float
    mark_price: float
    position_notional: float
    pnl: float
    cumulative_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    total_fees: float
    net_pnl: float
    account_equity: float


@dataclass
class BacktestResult:
    n: int
    allocation_mode: str
    strategy_direction: str
    neutral_anchor_price: float | None
    grid_levels: list[float]
    grid_sides: list[str]
    per_grid_notionals: list[float]
    per_grid_qty: list[float]
    capital_base_notional: float
    gross_trade_notional: float
    turnover_multiple: float
    net_profit: float
    total_fees: float
    funding_pnl: float
    funding_event_count: int
    total_return: float
    annualized_return: float
    max_drawdown: float
    calmar: float
    score: float
    trade_count: int
    win_rate: float
    avg_capital_usage: float
    max_capital_usage: float
    realized_pnl: float
    unrealized_pnl: float
    final_position_qty: float
    final_position_notional: float
    start_time: datetime
    end_time: datetime
    start_price: float
    end_price: float
    underlying_return: float
    period_low: float
    period_high: float
    period_amplitude: float
    equity_curve: list[float] | None = None
    capital_usage_curve: list[float] | None = None
    trades: list[Trade] | None = None
    funding_events: list[FundingEvent] | None = None


@dataclass
class OptimizationResult:
    best: BacktestResult | None
    top_results: list[BacktestResult]
    skipped_by_cost: int
    tested: int
