from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.backtest import run_backtest
from grid_optimizer.optimize import optimize_grid_count
from grid_optimizer.types import Candle


def _make_candles(ohlc: list[tuple[float, float, float, float]]) -> list[Candle]:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    candles = []
    for i, item in enumerate(ohlc):
        open_price, high_price, low_price, close_price = item
        open_time = start + timedelta(hours=i)
        close_time = open_time + timedelta(hours=1)
        candles.append(
            Candle(
                open_time=open_time,
                close_time=close_time,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
        )
    return candles


class BacktestTests(unittest.TestCase):
    def test_run_backtest_basic_cycle(self) -> None:
        candles = _make_candles(
            [
                (100, 101, 98, 99),
                (99, 100, 88, 90),
                (90, 102, 89, 101),
                (101, 103, 97, 98),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="equal",
            fee_rate=0.0002,
            slippage=0.0,
        )
        self.assertGreater(result.trade_count, 0)
        self.assertGreater(result.total_fees, 0)
        self.assertEqual(len(result.per_grid_qty), 4)
        self.assertAlmostEqual(sum(result.per_grid_notionals), 1000, places=6)

    def test_linear_allocation_more_size_at_lower_price(self) -> None:
        candles = _make_candles(
            [
                (100, 101, 98, 99),
                (99, 100, 95, 96),
                (96, 99, 94, 98),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="linear",
            fee_rate=0.0002,
            slippage=0.0,
        )
        self.assertGreater(result.per_grid_notionals[0], result.per_grid_notionals[-1])
        self.assertAlmostEqual(sum(result.per_grid_notionals), 1000, places=6)

    def test_optimize_grid_count_has_candidate(self) -> None:
        candles = _make_candles(
            [
                (100, 101, 99, 100),
                (100, 101, 95, 96),
                (96, 97, 92, 93),
                (93, 100, 92, 99),
                (99, 104, 98, 103),
                (103, 104, 98, 100),
            ]
        )
        opt = optimize_grid_count(
            candles=candles,
            min_price=90,
            max_price=110,
            total_buy_notional=1000,
            n_min=2,
            n_max=10,
            fee_rate=0.0002,
            slippage=0.0,
            allocation_modes=["equal", "linear"],
            top_k=3,
        )
        self.assertIsNotNone(opt.best)
        self.assertGreater(len(opt.top_results), 0)
        self.assertIn(opt.best.allocation_mode, {"equal", "linear"})


if __name__ == "__main__":
    unittest.main()
