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


@dataclass
class BacktestResult:
    n: int
    allocation_mode: str
    grid_levels: list[float]
    per_grid_notionals: list[float]
    per_grid_qty: list[float]
    net_profit: float
    total_fees: float
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
    start_time: datetime
    end_time: datetime
    start_price: float
    end_price: float
    underlying_return: float
    equity_curve: list[float] | None = None
    capital_usage_curve: list[float] | None = None
    trades: list[Trade] | None = None


@dataclass
class OptimizationResult:
    best: BacktestResult | None
    top_results: list[BacktestResult]
    skipped_by_cost: int
    tested: int
