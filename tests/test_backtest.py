from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.backtest import build_per_grid_notionals, run_backtest
from grid_optimizer.data import parse_interval_ms
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
    def test_all_allocation_modes_sum_to_total(self) -> None:
        modes = [
            "equal",
            "equal_qty",
            "linear",
            "linear_reverse",
            "quadratic",
            "quadratic_reverse",
            "geometric",
            "geometric_reverse",
            "center_heavy",
            "edge_heavy",
        ]
        price_levels = [90 + i * 2 for i in range(8)]
        for mode in modes:
            with self.subTest(mode=mode):
                notionals = build_per_grid_notionals(1000, 7, mode, price_levels=price_levels)
                self.assertEqual(len(notionals), 7)
                self.assertAlmostEqual(sum(notionals), 1000, places=6)

    def test_equal_qty_has_constant_base_qty(self) -> None:
        levels = [100, 110, 120, 130, 140]
        notionals = build_per_grid_notionals(1000, 4, "equal_qty", price_levels=levels)
        qty = [notionals[i] / levels[i] for i in range(4)]
        self.assertAlmostEqual(qty[0], qty[1], places=8)
        self.assertAlmostEqual(qty[1], qty[2], places=8)
        self.assertAlmostEqual(qty[2], qty[3], places=8)

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
        self.assertGreater(result.trade_volume, 0)
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

    def test_reverse_allocation_more_size_at_higher_price(self) -> None:
        notionals = build_per_grid_notionals(1000, 5, "linear_reverse")
        self.assertLess(notionals[0], notionals[-1])

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

    def test_competition_objective_prioritizes_trade_volume(self) -> None:
        candles = _make_candles(
            [
                (100, 120, 80, 110),
                (110, 120, 80, 90),
                (90, 120, 80, 110),
                (110, 120, 80, 90),
                (90, 120, 80, 110),
                (110, 120, 80, 90),
            ]
        )
        opt = optimize_grid_count(
            candles=candles,
            min_price=80,
            max_price=120,
            total_buy_notional=1000,
            n_min=2,
            n_max=12,
            fee_rate=0.0002,
            slippage=0.0,
            allocation_modes=["equal"],
            objective="competition_volume",
            target_trade_volume=100_000_000,
            top_k=5,
        )
        self.assertIsNotNone(opt.best)
        top_volumes = [x.trade_volume for x in opt.top_results]
        self.assertAlmostEqual(opt.best.trade_volume, max(top_volumes), places=8)

    def test_optimize_with_discrete_n_values(self) -> None:
        candles = _make_candles(
            [
                (100, 103, 97, 101),
                (101, 104, 98, 102),
                (102, 104, 99, 100),
                (100, 103, 96, 97),
                (97, 101, 95, 100),
                (100, 105, 98, 104),
            ]
        )
        n_values = [3, 7, 11]
        opt = optimize_grid_count(
            candles=candles,
            min_price=90,
            max_price=110,
            total_buy_notional=1000,
            n_min=2,
            n_max=20,
            n_values=n_values,
            fee_rate=0.0002,
            slippage=0.0,
            allocation_modes=["equal"],
            objective="net_profit",
            top_k=2,
        )
        self.assertIsNotNone(opt.best)
        self.assertIn(opt.best.n, n_values)
        self.assertEqual(opt.tested, len(n_values))

    def test_parse_interval_supports_1s(self) -> None:
        self.assertEqual(parse_interval_ms("1s"), 1000)


if __name__ == "__main__":
    unittest.main()
