from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.backtest import build_grid_levels, build_per_grid_notionals, run_backtest
from grid_optimizer.optimize import objective_value, optimize_grid_count
from grid_optimizer.types import Candle, FundingRate


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
    def test_competition_volume_objective_value_uses_target_coverage(self) -> None:
        result = type(
            "Result",
            (),
            {
                "calmar": 0.0,
                "annualized_return": 0.0,
                "net_profit": -10.0,
                "total_return": -0.01,
                "gross_trade_notional": 7500.0,
            },
        )()
        self.assertAlmostEqual(
            objective_value(result, "competition_volume", target_trade_volume=10000.0),
            0.75,
            places=8,
        )

    def test_build_grid_levels_geometric_ratios_are_consistent(self) -> None:
        levels = build_grid_levels(1000, 2000, 4, grid_level_mode="geometric")
        self.assertEqual(len(levels), 5)
        self.assertAlmostEqual(levels[0], 1000.0, places=8)
        self.assertAlmostEqual(levels[-1], 2000.0, places=8)
        self.assertTrue(all(levels[i + 1] > levels[i] for i in range(4)))
        ratios = [levels[i + 1] / levels[i] for i in range(4)]
        self.assertAlmostEqual(ratios[0], ratios[1], places=10)
        self.assertAlmostEqual(ratios[1], ratios[2], places=10)
        self.assertAlmostEqual(ratios[2], ratios[3], places=10)

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
        self.assertGreater(result.total_fees, 0)
        self.assertGreater(result.gross_trade_notional, 0)
        self.assertAlmostEqual(
            result.turnover_multiple,
            result.gross_trade_notional / result.capital_base_notional,
            places=8,
        )
        self.assertEqual(len(result.per_grid_qty), 4)
        self.assertAlmostEqual(result.capital_base_notional, 1000, places=6)
        self.assertAlmostEqual(sum(result.per_grid_qty) * 90.0, 1000.0, places=6)
        self.assertAlmostEqual(result.period_low, 88.0, places=8)
        self.assertAlmostEqual(result.period_high, 103.0, places=8)
        self.assertAlmostEqual(result.period_amplitude, (103.0 - 88.0) / 88.0, places=8)

    def test_run_backtest_geometric_grid_levels(self) -> None:
        candles = _make_candles(
            [
                (100, 102, 98, 101),
                (101, 104, 96, 97),
                (97, 103, 95, 102),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=90,
            max_price=160,
            n=4,
            total_buy_notional=1000,
            grid_level_mode="geometric",
            allocation_mode="equal",
            fee_rate=0.0002,
            slippage=0.0,
        )
        levels = result.grid_levels
        ratios = [levels[i + 1] / levels[i] for i in range(4)]
        self.assertAlmostEqual(levels[0], 90.0, places=8)
        self.assertAlmostEqual(levels[-1], 160.0, places=8)
        self.assertAlmostEqual(ratios[0], ratios[1], places=10)
        self.assertAlmostEqual(ratios[1], ratios[2], places=10)
        self.assertAlmostEqual(ratios[2], ratios[3], places=10)

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
        self.assertAlmostEqual(result.capital_base_notional, 1000, places=6)
        self.assertAlmostEqual(sum(result.per_grid_qty) * 90.0, 1000.0, places=6)

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

    def test_optimize_grid_count_geometric_has_candidate(self) -> None:
        candles = _make_candles(
            [
                (100, 101, 99, 100),
                (100, 104, 95, 96),
                (96, 102, 92, 101),
                (101, 106, 97, 104),
            ]
        )
        opt = optimize_grid_count(
            candles=candles,
            min_price=90,
            max_price=150,
            total_buy_notional=1000,
            n_min=2,
            n_max=12,
            grid_level_mode="geometric",
            fee_rate=0.0002,
            slippage=0.0,
            allocation_modes=["equal"],
            top_k=3,
        )
        self.assertIsNotNone(opt.best)
        self.assertGreater(len(opt.top_results), 0)

    def test_optimize_grid_count_volume_objective_uses_gross_trade_notional(self) -> None:
        candles = _make_candles(
            [
                (100, 103, 97, 102),
                (102, 105, 96, 98),
                (98, 104, 95, 103),
                (103, 106, 99, 100),
            ]
        )
        opt = optimize_grid_count(
            candles=candles,
            min_price=90,
            max_price=110,
            total_buy_notional=1000,
            n_min=2,
            n_max=8,
            fee_rate=0.0002,
            slippage=0.0,
            allocation_modes=["equal"],
            objective="gross_trade_notional",
            top_k=3,
        )
        self.assertIsNotNone(opt.best)
        assert opt.best is not None
        self.assertAlmostEqual(opt.best.score, opt.best.gross_trade_notional, places=8)

    def test_optimize_grid_count_competition_objective_uses_target_trade_volume(self) -> None:
        candles = _make_candles(
            [
                (100, 103, 97, 102),
                (102, 105, 96, 98),
                (98, 104, 95, 103),
                (103, 106, 99, 100),
            ]
        )
        opt = optimize_grid_count(
            candles=candles,
            min_price=90,
            max_price=110,
            total_buy_notional=1000,
            n_min=2,
            n_max=8,
            fee_rate=0.0002,
            slippage=0.0,
            allocation_modes=["equal"],
            objective="competition_volume",
            target_trade_volume=10000.0,
            top_k=3,
        )
        self.assertIsNotNone(opt.best)
        assert opt.best is not None
        self.assertAlmostEqual(
            opt.best.score,
            opt.best.gross_trade_notional / 10000.0,
            places=8,
        )

    def test_optimize_grid_count_neutral_anchor_price_controls_side_split(self) -> None:
        candles = _make_candles(
            [
                (150, 151, 149, 150),
                (150, 151, 89, 90),
                (90, 111, 89, 110),
            ]
        )
        opt = optimize_grid_count(
            candles=candles,
            min_price=90,
            max_price=110,
            total_buy_notional=1000,
            n_min=4,
            n_max=4,
            fee_rate=0.0002,
            slippage=0.0,
            strategy_direction="neutral",
            neutral_anchor_price=100.0,
            allocation_modes=["equal"],
            top_k=1,
        )
        self.assertIsNotNone(opt.best)
        assert opt.best is not None
        self.assertIn("long", opt.best.grid_sides)
        self.assertIn("short", opt.best.grid_sides)
        self.assertEqual(opt.best.neutral_anchor_price, 100.0)

    def test_run_backtest_with_funding_rates(self) -> None:
        candles = _make_candles(
            [
                (100, 100, 95, 96),
                (96, 98, 95, 97),
            ]
        )
        funding = [
            FundingRate(ts=candles[0].close_time, rate=0.01),  # long pays funding
        ]
        base = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="equal",
            fee_rate=0.0,
            slippage=0.0,
        )
        with_funding = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="equal",
            fee_rate=0.0,
            slippage=0.0,
            funding_rates=funding,
        )
        self.assertEqual(with_funding.funding_event_count, 1)
        self.assertLess(with_funding.net_profit, base.net_profit)
        self.assertLess(with_funding.funding_pnl, 0)

    def test_run_backtest_fixed_per_grid_qty_mode(self) -> None:
        candles = _make_candles(
            [
                (100, 102, 90, 95),
                (95, 99, 92, 97),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=80,
            max_price=120,
            n=4,
            total_buy_notional=1,  # ignored when fixed_per_grid_qty is set
            allocation_mode="equal",
            fee_rate=0.0,
            slippage=0.0,
            fixed_per_grid_qty=2,
        )
        self.assertEqual(result.per_grid_qty, [2.0, 2.0, 2.0, 2.0])
        self.assertAlmostEqual(result.capital_base_notional, sum(result.per_grid_notionals), places=8)
        self.assertLess(result.per_grid_notionals[0], result.per_grid_notionals[-1])

    def test_run_backtest_short_can_profit_when_price_reverts_down(self) -> None:
        candles = _make_candles(
            [
                (100, 107, 99, 106),
                (106, 107, 92, 93),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="equal",
            strategy_direction="short",
            fee_rate=0.0,
            slippage=0.0,
        )
        self.assertEqual(result.strategy_direction, "short")
        self.assertTrue(all(x == "short" for x in result.grid_sides))
        self.assertGreater(result.trade_count, 0)
        self.assertGreater(result.net_profit, 0)
        self.assertAlmostEqual(result.capital_base_notional, 1000, places=6)
        self.assertAlmostEqual(sum(result.per_grid_qty) * 110.0, 1000.0, places=6)

    def test_run_backtest_neutral_uses_start_price_as_anchor(self) -> None:
        candles = _make_candles(
            [
                (100, 103, 97, 101),
                (101, 104, 96, 99),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="equal",
            strategy_direction="neutral",
            fee_rate=0.0,
            slippage=0.0,
        )
        self.assertEqual(result.strategy_direction, "neutral")
        self.assertAlmostEqual(result.neutral_anchor_price or 0.0, 100.0, places=8)
        self.assertIn("long", result.grid_sides)
        self.assertIn("short", result.grid_sides)

    def test_run_backtest_bootstrap_long_can_trade_first_up_leg(self) -> None:
        candles = _make_candles(
            [
                (100, 106, 99, 105),
            ]
        )
        result = run_backtest(
            candles=candles,
            min_price=90,
            max_price=110,
            n=4,
            total_buy_notional=1000,
            allocation_mode="equal",
            strategy_direction="long",
            fee_rate=0.0,
            slippage=0.0,
            capture_trades=True,
        )
        self.assertGreaterEqual(result.trade_count, 3)
        self.assertGreater(result.realized_pnl, 0.0)


if __name__ == "__main__":
    unittest.main()
