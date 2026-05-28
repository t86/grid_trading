from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime
from grid_optimizer.types import Candle
from grid_optimizer.spot_loop_runner import (
    _assess_spot_fast_stop_guard,
    _auto_flatten_on_runtime_guard,
    _build_parser,
    _build_spot_competition_inventory_grid_orders,
    _load_state,
    _normalize_commission_quote,
    _resolve_spot_competition_runtime,
    _spot_order_meets_exchange_mins,
    _sync_synthetic_neutral_trades,
    _sync_volume_shift_trades,
)


class SpotLoopRunnerTests(unittest.TestCase):
    def test_parser_accepts_spot_competition_inventory_grid(self) -> None:
        args = _build_parser().parse_args(
            [
                "--symbol",
                "BTCUSDT",
                "--strategy-mode",
                "spot_competition_inventory_grid",
                "--total-quote-budget",
                "1000",
                "--client-order-prefix",
                "spotgrid",
                "--state-path",
                "/tmp/spot-state.json",
                "--summary-jsonl",
                "/tmp/spot-summary.jsonl",
                "--first-order-multiplier",
                "1.5",
                "--threshold-position-notional",
                "200",
                "--warmup-position-notional",
                "30",
                "--require-non-loss-exit",
                "--spot-taker-exit-enabled",
                "--spot-taker-exit-fee-ratio",
                "0.001",
                "--spot-taker-exit-min-profit-ratio",
                "0.0002",
                "--spot-fast-stop-enabled",
                "--spot-fast-stop-10s-abs-return-ratio",
                "0.003",
                "--spot-fast-stop-30s-amplitude-ratio",
                "0.006",
                "--spot-fast-stop-freeze-position-notional",
                "120",
                "--spot-fast-stop-exit-position-notional",
                "240",
                "--spot-fast-stop-reduce-target-notional",
                "80",
                "--spot-slow-trend-step-enabled",
                "--spot-slow-trend-step-5m-return-ratio",
                "0.0045",
                "--spot-slow-trend-step-15m-return-ratio",
                "0.008",
                "--spot-slow-trend-step-5m-amplitude-ratio",
                "0.005",
                "--spot-slow-trend-step-15m-amplitude-ratio",
                "0.01",
                "--spot-slow-trend-step-scale",
                "1.8",
                "--runtime-guard-stats-start-time",
                "2026-04-05T00:00:00+00:00",
                "--max-order-position-notional",
                "300",
                "--max-position-notional",
                "400",
            ]
        )

        self.assertEqual(args.strategy_mode, "spot_competition_inventory_grid")
        self.assertEqual(args.first_order_multiplier, 1.5)
        self.assertEqual(args.threshold_position_notional, 200.0)
        self.assertEqual(args.warmup_position_notional, 30.0)
        self.assertTrue(args.require_non_loss_exit)
        self.assertTrue(args.spot_taker_exit_enabled)
        self.assertEqual(args.spot_taker_exit_fee_ratio, 0.001)
        self.assertEqual(args.spot_taker_exit_min_profit_ratio, 0.0002)
        self.assertTrue(args.spot_fast_stop_enabled)
        self.assertEqual(args.spot_fast_stop_10s_abs_return_ratio, 0.003)
        self.assertEqual(args.spot_fast_stop_30s_amplitude_ratio, 0.006)
        self.assertEqual(args.spot_fast_stop_freeze_position_notional, 120.0)
        self.assertEqual(args.spot_fast_stop_exit_position_notional, 240.0)
        self.assertEqual(args.spot_fast_stop_reduce_target_notional, 80.0)
        self.assertTrue(args.spot_slow_trend_step_enabled)
        self.assertEqual(args.spot_slow_trend_step_5m_return_ratio, 0.0045)
        self.assertEqual(args.spot_slow_trend_step_15m_return_ratio, 0.008)
        self.assertEqual(args.spot_slow_trend_step_5m_amplitude_ratio, 0.005)
        self.assertEqual(args.spot_slow_trend_step_15m_amplitude_ratio, 0.01)
        self.assertEqual(args.spot_slow_trend_step_scale, 1.8)
        self.assertEqual(args.runtime_guard_stats_start_time, "2026-04-05T00:00:00+00:00")
        self.assertEqual(args.max_order_position_notional, 300.0)
        self.assertEqual(args.max_position_notional, 400.0)

    def test_parser_accepts_spot_competition_synthetic_neutral_grid(self) -> None:
        args = _build_parser().parse_args(
            [
                "--symbol",
                "SPKUSDT",
                "--strategy-mode",
                "spot_competition_synthetic_neutral_grid",
                "--total-quote-budget",
                "1500",
                "--client-order-prefix",
                "spotgrid",
                "--state-path",
                "/tmp/spot-state.json",
                "--summary-jsonl",
                "/tmp/spot-summary.jsonl",
                "--neutral-base-qty",
                "30000",
                "--max-short-position-notional",
                "650",
                "--synthetic-freeze-enabled",
                "--synthetic-freeze-loss-ratio",
                "0.0015",
                "--synthetic-freeze-min-notional",
                "12",
                "--synthetic-freeze-max-side-notional",
                "1000",
                "--synthetic-freeze-release-profit-ratio",
                "0.0002",
                "--no-synthetic-freeze-pair-release-enabled",
                "--synthetic-freeze-manual-release-side",
                "short",
                "--synthetic-freeze-manual-release-qty",
                "3000",
                "--synthetic-freeze-manual-release-price",
                "0.031",
            ]
        )

        self.assertEqual(args.strategy_mode, "spot_competition_synthetic_neutral_grid")
        self.assertEqual(args.neutral_base_qty, 30000.0)
        self.assertEqual(args.max_short_position_notional, 650.0)
        self.assertTrue(args.synthetic_freeze_enabled)
        self.assertEqual(args.synthetic_freeze_loss_ratio, 0.0015)
        self.assertEqual(args.synthetic_freeze_min_notional, 12.0)
        self.assertEqual(args.synthetic_freeze_max_side_notional, 1000.0)
        self.assertEqual(args.synthetic_freeze_release_profit_ratio, 0.0002)
        self.assertFalse(args.synthetic_freeze_pair_release_enabled)
        self.assertEqual(args.synthetic_freeze_manual_release_side, "short")
        self.assertEqual(args.synthetic_freeze_manual_release_qty, 3000.0)
        self.assertEqual(args.synthetic_freeze_manual_release_price, 0.031)

    def test_runtime_guard_does_not_auto_flatten_spot_competition_inventory_grid(self) -> None:
        self.assertFalse(_auto_flatten_on_runtime_guard("spot_competition_inventory_grid"))
        self.assertFalse(_auto_flatten_on_runtime_guard("spot_competition_synthetic_neutral_grid"))
        self.assertTrue(_auto_flatten_on_runtime_guard("spot_one_way_long"))

    def test_fast_stop_guard_freezes_before_exit_threshold(self) -> None:
        def fake_window(symbol: str, *, seconds: int, now=None):
            return {
                "seconds": seconds,
                "available": True,
                "return_ratio": -0.004 if seconds == 10 else -0.003,
                "amplitude_ratio": 0.005,
            }

        with patch("grid_optimizer.spot_loop_runner._spot_trade_window_move", side_effect=fake_window):
            guard = _assess_spot_fast_stop_guard(
                symbol="ETHU",
                current_long_notional=180.0,
                freeze_position_notional=100.0,
                exit_position_notional=300.0,
                trigger_10s_abs_return_ratio=0.003,
                trigger_10s_amplitude_ratio=0.0,
                trigger_30s_abs_return_ratio=0.006,
                trigger_30s_amplitude_ratio=0.0,
            )

        self.assertTrue(guard["freeze_buy"])
        self.assertFalse(guard["force_exit"])
        self.assertTrue(guard["reasons"])

    def test_fast_stop_guard_exits_above_exit_threshold(self) -> None:
        def fake_window(symbol: str, *, seconds: int, now=None):
            return {
                "seconds": seconds,
                "available": True,
                "return_ratio": -0.008,
                "amplitude_ratio": 0.01,
            }

        with patch("grid_optimizer.spot_loop_runner._spot_trade_window_move", side_effect=fake_window):
            guard = _assess_spot_fast_stop_guard(
                symbol="BNBU",
                current_long_notional=650.0,
                freeze_position_notional=100.0,
                exit_position_notional=600.0,
                trigger_10s_abs_return_ratio=0.003,
                trigger_10s_amplitude_ratio=0.0,
                trigger_30s_abs_return_ratio=0.006,
                trigger_30s_amplitude_ratio=0.0,
            )

        self.assertTrue(guard["freeze_buy"])
        self.assertTrue(guard["force_exit"])

    def test_build_spot_competition_inventory_grid_orders_places_only_buy_bootstrap_when_flat(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=5.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.5,
            per_order_notional=20.0,
            threshold_position_notional=200.0,
            max_order_position_notional=300.0,
            max_position_notional=400.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY"])
        self.assertEqual(desired_orders[0]["role"], "bootstrap_entry")
        self.assertEqual(controls["direction_state"], "flat")
        self.assertEqual(controls["risk_state"], "normal")

    def test_build_spot_competition_synthetic_neutral_grid_orders_opens_both_sides_from_base(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=0.099,
            ask_price=0.101,
            step_price=0.002,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=60.0,
            max_order_position_notional=80.0,
            max_position_notional=100.0,
            tick_size=0.000001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1000.0,
            max_short_position_notional=80.0,
            actual_base_qty=1000.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "SELL", "SELL"])
        self.assertEqual(controls["mode"], "competition_synthetic_neutral_grid")
        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["neutral_base_qty"], 1000.0)
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertAlmostEqual(controls["current_long_notional"], 0.0)
        self.assertAlmostEqual(controls["current_short_notional"], 0.0)
        self.assertAlmostEqual(controls["max_short_position_notional"], 80.0)

    def test_build_spot_competition_synthetic_neutral_grid_excludes_frozen_short_from_brushing_net(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "short_active"
        runtime["position_lots"] = [
            {
                "lot_id": "active_short",
                "side": "short",
                "qty": 3000.0,
                "entry_price": 0.206,
                "opened_at_ms": 1,
                "source_role": "synthetic_recovery",
            }
        ]
        runtime["frozen_position_lots"] = [
            {
                "lot_id": "frozen_short",
                "source_lot_id": "active_short",
                "side": "short",
                "qty": 3000.0,
                "entry_price": 0.203,
                "frozen_at_ms": 2,
                "freeze_reason": "threshold_reduce_loss",
            }
        ]
        state = {
            "known_orders": {},
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.2055,
            ask_price=0.2056,
            step_price=0.0002,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=80.0,
            threshold_position_notional=500.0,
            threshold_reduce_target_notional=400.0,
            max_order_position_notional=1600.0,
            max_position_notional=2000.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=5000.0,
            max_short_position_notional=1000.0,
            actual_base_qty=2000.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_pair_release_enabled=True,
            synthetic_freeze_release_profit_ratio=0.01,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "SELL", "SELL"])
        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertAlmostEqual(controls["current_short_notional"], 0.0)
        self.assertAlmostEqual(controls["synthetic_frozen_short_qty"], 3000.0)
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["position_lots"], [])
        self.assertAlmostEqual(cached_runtime["frozen_position_lots"][0]["qty"], 3000.0)

    def test_build_spot_competition_synthetic_neutral_grid_honors_zero_side_levels(self) -> None:
        desired_orders, _ = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=0.099,
            ask_price=0.101,
            step_price=0.002,
            buy_levels=0,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=60.0,
            max_order_position_notional=80.0,
            max_position_notional=100.0,
            tick_size=0.000001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1000.0,
            max_short_position_notional=80.0,
            actual_base_qty=1000.0,
        )

        self.assertEqual({order["side"] for order in desired_orders}, {"SELL"})

    def test_slow_trend_step_guard_widens_synthetic_neutral_ladder(self) -> None:
        now = datetime(2026, 5, 15, 6, 15, tzinfo=timezone.utc)
        candles = []
        for idx in range(15):
            open_price = 676.0 + idx * 0.45
            close_price = open_price + 0.45
            candles.append(
                Candle(
                    open_time=now - timedelta(minutes=15 - idx),
                    close_time=now - timedelta(minutes=14 - idx),
                    open=open_price,
                    high=close_price + 0.05,
                    low=open_price - 0.05,
                    close=close_price,
                )
            )

        with patch("grid_optimizer.spot_loop_runner.fetch_spot_klines", return_value=candles):
            desired_orders, controls = _build_spot_competition_inventory_grid_orders(
                state={
                    "known_orders": {},
                    "inventory_lots": [],
                },
                trades=[],
                bid_price=682.7,
                ask_price=682.8,
                step_price=0.06,
                buy_levels=3,
                sell_levels=3,
                first_order_multiplier=1.0,
                per_order_notional=420.0,
                threshold_position_notional=1000.0,
                max_order_position_notional=1600.0,
                max_position_notional=2000.0,
                tick_size=0.01,
                step_size=0.001,
                min_qty=0.001,
                min_notional=5.0,
                synthetic_neutral=True,
                neutral_base_qty=10.0,
                max_short_position_notional=1600.0,
                actual_base_qty=10.0,
                symbol="BNBU",
                slow_trend_step_enabled=True,
                slow_trend_step_5m_return_ratio=0.003,
                slow_trend_step_15m_return_ratio=0.006,
                slow_trend_step_5m_amplitude_ratio=0.0,
                slow_trend_step_15m_amplitude_ratio=0.0,
                slow_trend_step_scale=2.0,
                now=now,
            )

        buy_prices = [order["price"] for order in desired_orders if order["side"] == "BUY"]
        sell_prices = [order["price"] for order in desired_orders if order["side"] == "SELL"]
        self.assertEqual(buy_prices, [682.7, 682.58, 682.46])
        self.assertEqual(sell_prices, [682.8, 682.92, 683.04])
        self.assertAlmostEqual(controls["base_step_price"], 0.06)
        self.assertAlmostEqual(controls["effective_step_price"], 0.12)
        self.assertTrue(controls["slow_trend_step"]["active"])
        self.assertEqual(controls["slow_trend_step"]["scale"], 2.0)
        self.assertGreater(controls["slow_trend_step"]["window_5m"]["return_ratio"], 0.003)

    def test_build_spot_competition_synthetic_neutral_grid_ignores_dust_residual(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["direction_state"] = "short_active"
        runtime["grid_anchor_price"] = 0.034487
        runtime["position_lots"] = [
            {
                "lot_id": "dust",
                "side": "short",
                "qty": 69.0,
                "entry_price": 0.034487,
                "opened_at_ms": 1000,
                "source_role": "bootstrap_entry",
            }
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.034532,
            ask_price=0.034541,
            step_price=0.00004,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=35.0,
            threshold_position_notional=520.0,
            max_order_position_notional=700.0,
            max_position_notional=900.0,
            tick_size=0.000001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=30000.0,
            max_short_position_notional=650.0,
            actual_base_qty=29931.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "SELL", "SELL"])
        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertAlmostEqual(controls["actual_base_qty"], 29931.0)
        self.assertAlmostEqual(controls["synthetic_dust_qty"], -69.0)
        self.assertAlmostEqual(controls["synthetic_dust_notional"], 69.0 * 0.0345365)

    def test_build_spot_competition_synthetic_neutral_grid_does_not_replay_dust_history(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {
                "123": {
                    "side": "SELL",
                    "role": "bootstrap_entry",
                    "created_at_ms": 1000,
                }
            },
            "inventory_lots": [],
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": new_inventory_grid_runtime(market_type="futures"),
                "applied_trade_keys": [],
            },
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[
                {
                    "id": 1,
                    "orderId": 123,
                    "price": "0.034487",
                    "qty": "69",
                    "time": 1000,
                }
            ],
            bid_price=0.034532,
            ask_price=0.034541,
            step_price=0.00004,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=35.0,
            threshold_position_notional=520.0,
            max_order_position_notional=700.0,
            max_position_notional=900.0,
            tick_size=0.000001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=30000.0,
            max_short_position_notional=650.0,
            actual_base_qty=29931.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "SELL", "SELL"])
        self.assertEqual(controls["direction_state"], "flat")
        self.assertEqual(controls["risk_state"], "normal")
        self.assertEqual(
            state["spot_competition_synthetic_neutral_grid_runtime_cache"]["applied_trade_keys"],
            ["1000:1:123"],
        )

    def test_build_spot_competition_synthetic_neutral_grid_keeps_mismatched_short_cost_unknown(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "known_orders": {
                    "123": {
                        "side": "SELL",
                        "role": "bootstrap_entry",
                        "created_at_ms": 1000,
                    }
                },
                "inventory_lots": [],
            },
            trades=[
                {
                    "id": 1,
                    "orderId": 123,
                    "price": "0.034487",
                    "qty": "1000",
                    "time": 1000,
                }
            ],
            bid_price=0.034532,
            ask_price=0.034541,
            step_price=0.00004,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=35.0,
            threshold_position_notional=520.0,
            max_order_position_notional=700.0,
            max_position_notional=900.0,
            tick_size=0.000001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=30000.0,
            max_short_position_notional=650.0,
            actual_base_qty=28922.0,
        )

        self.assertEqual(controls["direction_state"], "short_active")
        self.assertEqual(controls["risk_state"], "hard_reduce_only")
        self.assertAlmostEqual(controls["synthetic_net_qty"], -1078.0)
        self.assertEqual(desired_orders, [])
        self.assertTrue(controls["synthetic_cost_unknown"])
        self.assertEqual(controls["recovery_mode"], "conservative_reduce_only")

    def test_build_spot_competition_synthetic_neutral_grid_persists_frozen_reduce_lots(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "long_active"
        runtime["grid_anchor_price"] = 0.10
        runtime["position_lots"] = [
            {
                "lot_id": "held",
                "side": "long",
                "qty": 4000.0,
                "entry_price": 0.10,
                "opened_at_ms": 1000,
                "source_role": "grid_entry",
            }
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.0799,
            ask_price=0.0801,
            step_price=0.0005,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=40.0,
            threshold_position_notional=300.0,
            threshold_reduce_target_notional=250.0,
            max_order_position_notional=500.0,
            max_position_notional=600.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=100000.0,
            max_short_position_notional=500.0,
            actual_base_qty=104000.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.001,
            synthetic_freeze_min_notional=5.0,
        )

        self.assertFalse(any(order["role"] == "forced_reduce" for order in desired_orders))
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertGreater(cached_runtime["frozen_position_lots"][0]["qty"], 0.0)
        self.assertGreater(controls["synthetic_frozen_position"]["long_qty"], 0.0)
        self.assertGreater(controls["synthetic_freeze_last"]["freeze_qty"], 0.0)

    def test_synthetic_neutral_runtime_tolerates_frozen_fill_float_dust(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "long_active"
        runtime["grid_anchor_price"] = 0.1827
        runtime["position_lots"] = [
            {
                "lot_id": "active",
                "side": "long",
                "qty": 2736.029079999999,
                "entry_price": 0.1882,
                "opened_at_ms": 1000,
                "source_role": "grid_entry",
            }
        ]
        runtime["frozen_position_lots"] = [
            {
                "lot_id": "frozen",
                "source_lot_id": "held",
                "side": "long",
                "qty": 1105.2,
                "entry_price": 0.1884,
                "opened_at_ms": 900,
                "source_role": "grid_entry",
                "frozen_at_ms": 1200,
                "freeze_reason": "threshold_reduce_loss",
            }
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {
                "1": {"role": "grid_entry", "side": "BUY"},
                "2": {"role": "grid_entry", "side": "BUY"},
            },
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": ["1000:10:1"],
            },
        }
        trades = [
            {"time": 1000, "id": 10, "orderId": 1, "price": "0.1852", "qty": "971.9"},
            {"time": 2000, "id": 11, "orderId": 2, "price": "0.1825", "qty": "986.3"},
        ]

        resolved = _resolve_spot_competition_runtime(
            state=state,
            trades=trades,
            order_refs=state["known_orders"],
            step_price=0.0002,
            current_position_qty=4827.52908,
            synthetic_neutral=True,
        )

        self.assertEqual(resolved["recovery_mode"], "live")
        self.assertEqual(resolved["recovery_errors"], [])
        self.assertAlmostEqual(
            sum(item["qty"] for item in resolved["position_lots"]),
            3722.329079999999,
        )
        self.assertAlmostEqual(
            sum(item["qty"] for item in resolved["frozen_position_lots"]),
            1105.2,
        )

    def test_synthetic_neutral_runtime_tolerates_frozen_only_residual_dust(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "flat"
        runtime["grid_anchor_price"] = 0.1822
        runtime["frozen_position_lots"] = [
            {
                "lot_id": "frozen",
                "source_lot_id": "held",
                "side": "long",
                "qty": 3841.22908,
                "entry_price": 0.1884,
                "opened_at_ms": 1000,
                "source_role": "grid_entry",
                "frozen_at_ms": 2000,
                "freeze_reason": "threshold_reduce_loss",
            }
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        _desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.1821,
            ask_price=0.1823,
            step_price=0.0002,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=180.0,
            threshold_position_notional=600.0,
            threshold_reduce_target_notional=500.0,
            max_order_position_notional=7000.0,
            max_position_notional=8200.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=10000.0,
            max_short_position_notional=1200.0,
            actual_base_qty=13841.32908,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.01,
            synthetic_freeze_min_notional=20.0,
        )

        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertEqual(controls["recovery_mode"], "live")
        self.assertAlmostEqual(cached_runtime["frozen_position_lots"][0]["qty"], 3841.22908)

    def test_build_spot_competition_synthetic_neutral_grid_caps_freeze_and_reduces_excess(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "long_active"
        runtime["grid_anchor_price"] = 0.10
        runtime["position_lots"] = [
            {
                "lot_id": "held",
                "side": "long",
                "qty": 15000.0,
                "entry_price": 0.10,
                "opened_at_ms": 1000,
                "source_role": "grid_entry",
            }
        ]
        runtime["frozen_position_lots"] = [
            {
                "lot_id": "already_frozen",
                "source_lot_id": "old",
                "side": "long",
                "qty": 11250.0,
                "entry_price": 0.10,
                "frozen_at_ms": 900,
                "freeze_reason": "threshold_reduce_loss",
            }
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.0799,
            ask_price=0.0801,
            step_price=0.0005,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=40.0,
            threshold_position_notional=300.0,
            threshold_reduce_target_notional=250.0,
            max_order_position_notional=500.0,
            max_position_notional=3000.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=100000.0,
            max_short_position_notional=500.0,
            actual_base_qty=126250.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.001,
            synthetic_freeze_min_notional=5.0,
            synthetic_freeze_max_side_notional=1000.0,
        )

        self.assertTrue(any(order["role"] == "forced_reduce" for order in desired_orders))
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        frozen_qty = sum(
            item["qty"]
            for item in cached_runtime["frozen_position_lots"]
            if item["side"] == "long"
        )
        self.assertLessEqual(frozen_qty * 0.08, 1000.0)
        self.assertAlmostEqual(controls["synthetic_freeze_last"]["freeze_notional"], 100.0, places=4)

    def test_build_spot_competition_synthetic_neutral_grid_pairs_opposite_frozen_lots(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "flat"
        runtime["frozen_position_lots"] = [
            {
                "lot_id": "frozen_long",
                "source_lot_id": "long",
                "side": "long",
                "qty": 1000.0,
                "entry_price": 0.10,
                "frozen_at_ms": 1000,
                "freeze_reason": "threshold_reduce_loss",
            },
            {
                "lot_id": "frozen_short",
                "source_lot_id": "short",
                "side": "short",
                "qty": 600.0,
                "entry_price": 0.12,
                "frozen_at_ms": 2000,
                "freeze_reason": "threshold_reduce_loss",
            },
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.0999,
            ask_price=0.1001,
            step_price=0.0005,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=40.0,
            threshold_position_notional=300.0,
            threshold_reduce_target_notional=250.0,
            max_order_position_notional=500.0,
            max_position_notional=600.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=100000.0,
            max_short_position_notional=500.0,
            actual_base_qty=100400.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_pair_release_enabled=True,
            synthetic_freeze_release_profit_ratio=0.01,
        )

        self.assertEqual(
            [order["role"] for order in desired_orders],
            [
                "bootstrap_entry",
                "grid_entry",
                "grid_entry",
                "bootstrap_entry",
                "grid_entry",
                "grid_entry",
            ],
        )
        self.assertNotIn("synthetic_frozen_release", [order["role"] for order in desired_orders])
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["frozen_position_lots"][0]["side"], "long")
        self.assertAlmostEqual(cached_runtime["frozen_position_lots"][0]["qty"], 400.0)
        self.assertAlmostEqual(controls["synthetic_pair_release_last"]["release_qty"], 600.0)
        self.assertAlmostEqual(controls["synthetic_frozen_position"]["pair_eligible_qty"], 0.0)

    def test_build_spot_competition_inventory_grid_orders_ignores_unrelated_trades_when_flat(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[
                {
                    "id": 1,
                    "orderId": 999999,
                    "price": "100.0",
                    "qty": "0.2",
                    "time": 1234567890,
                }
            ],
            bid_price=99.0,
            ask_price=101.0,
            step_price=5.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.5,
            per_order_notional=20.0,
            threshold_position_notional=200.0,
            max_order_position_notional=300.0,
            max_position_notional=400.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY"])
        self.assertEqual(desired_orders[0]["role"], "bootstrap_entry")
        self.assertEqual(controls["direction_state"], "flat")

    def test_sync_volume_shift_trades_keeps_old_known_orders_for_competition_mode(self) -> None:
        state = {
            "strategy_mode": "spot_competition_inventory_grid",
            "seen_trade_ids": [],
            "known_orders": {
                "123": {
                    "side": "BUY",
                    "role": "bootstrap_entry",
                    "created_at_ms": 1,
                }
            },
            "inventory_lots": [],
            "metrics": {},
            "last_trade_time_ms": 0,
        }

        applied = _sync_volume_shift_trades(
            state=state,
            trades=[],
            base_asset="BTC",
            quote_asset="USDT",
        )

        self.assertEqual(applied, 0)
        self.assertIn("123", state["known_orders"])

    def test_sync_synthetic_neutral_trades_tracks_volume_without_mutating_manual_base_lots(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "seen_trade_ids": [],
            "known_orders": {
                "123": {
                    "side": "SELL",
                    "role": "bootstrap_entry",
                    "created_at_ms": 1,
                }
            },
            "inventory_lots": [
                {
                    "qty": 1000.0,
                    "cost_quote": 100.0,
                    "buy_time_ms": 1,
                    "tag": "manual_base",
                }
            ],
            "metrics": {},
            "last_trade_time_ms": 0,
        }

        applied = _sync_synthetic_neutral_trades(
            state=state,
            trades=[
                {
                    "id": 1,
                    "orderId": 123,
                    "price": "0.10",
                    "qty": "100",
                    "commission": "0.01",
                    "commissionAsset": "USDT",
                    "time": 1000,
                }
            ],
            base_asset="SPK",
            quote_asset="USDT",
        )

        self.assertEqual(applied, 1)
        self.assertEqual(state["inventory_lots"][0]["qty"], 1000.0)
        self.assertAlmostEqual(state["metrics"]["gross_notional"], 10.0)
        self.assertAlmostEqual(state["metrics"]["realized_pnl"], -0.01)
        self.assertIn("123", state["known_orders"])

    def test_load_state_resets_foreign_spot_mode_state_for_competition_mode_bootstrap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "strategy_mode": "spot_volume_shift_long",
                        "known_orders": {
                            "123": {
                                "side": "BUY",
                                "role": "inventory_build",
                                "created_at_ms": 1,
                            }
                        },
                        "inventory_lots": [
                            {
                                "qty": 0.2,
                                "cost_quote": 20.0,
                                "buy_time_ms": 1234567890,
                                "tag": "foreign",
                            }
                        ],
                        "seen_trade_ids": [1],
                        "last_trade_time_ms": 1234567890,
                        "spot_competition_inventory_grid_runtime_cache": {
                            "strategy_mode": "spot_competition_inventory_grid",
                            "market_type": "spot",
                            "runtime": {"direction_state": "long_active"},
                            "applied_trade_keys": ["old-trade"],
                        },
                    }
                ),
                encoding="utf-8",
            )

            state = _load_state(
                state_path,
                symbol="BTCUSDT",
                strategy_mode="spot_competition_inventory_grid",
                cell_count=0,
            )

        self.assertNotIn("spot_competition_inventory_grid_runtime_cache", state)
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=5.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.5,
            per_order_notional=20.0,
            threshold_position_notional=200.0,
            max_order_position_notional=300.0,
            max_position_notional=400.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY"])
        self.assertEqual(desired_orders[0]["role"], "bootstrap_entry")
        self.assertEqual(controls["direction_state"], "flat")

    def test_load_state_resets_competition_state_when_symbol_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "symbol": "BTCUSDT",
                        "strategy_mode": "spot_competition_inventory_grid",
                        "known_orders": {
                            "123": {
                                "side": "BUY",
                                "role": "bootstrap_entry",
                                "created_at_ms": 1,
                            }
                        },
                        "inventory_lots": [
                            {
                                "qty": 1.5,
                                "cost_quote": 150.0,
                                "buy_time_ms": 1234567890,
                                "tag": "stale",
                            }
                        ],
                        "seen_trade_ids": [1],
                        "last_trade_time_ms": 1234567890,
                        "metrics": {"realized_pnl": 1.0},
                        "spot_competition_inventory_grid_runtime_cache": {
                            "strategy_mode": "spot_competition_inventory_grid",
                            "market_type": "spot",
                            "runtime": {"direction_state": "long_active"},
                            "applied_trade_keys": ["old-trade"],
                        },
                    }
                ),
                encoding="utf-8",
            )

            state = _load_state(
                state_path,
                symbol="ETHUSDT",
                strategy_mode="spot_competition_inventory_grid",
                cell_count=0,
            )

        self.assertEqual(state["symbol"], "ETHUSDT")
        self.assertEqual(state["known_orders"], {})
        self.assertEqual(state["inventory_lots"], [])
        self.assertNotIn("spot_competition_inventory_grid_runtime_cache", state)

    def test_build_spot_competition_inventory_grid_orders_uses_cached_runtime_for_older_inventory(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="spot")
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side="BUY",
            price=100.0,
            qty=2.0,
            fill_time_ms=1000,
            step_price=5.0,
        )

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "strategy_mode": "spot_competition_inventory_grid",
                "known_orders": {
                    "200": {
                        "side": "BUY",
                        "role": "grid_entry",
                        "created_at_ms": 2000,
                    }
                },
                "inventory_lots": [
                    {
                        "qty": 2.5,
                        "cost_quote": 250.0,
                        "buy_time_ms": 1000,
                        "tag": "older",
                    }
                ],
                "spot_competition_inventory_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_inventory_grid",
                    "market_type": "spot",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
            },
            trades=[
                {
                    "id": 1,
                    "orderId": 200,
                    "price": "100.0",
                    "qty": "0.5",
                    "time": 2000,
                }
            ],
            bid_price=109.0,
            ask_price=111.0,
            step_price=5.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=110.0,
            threshold_position_notional=500.0,
            max_order_position_notional=700.0,
            max_position_notional=900.0,
            tick_size=0.1,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
        )

        sell_orders = [order for order in desired_orders if str(order.get("side", "")).upper() == "SELL"]
        self.assertEqual(controls["direction_state"], "long_active")
        self.assertEqual(controls["risk_state"], "normal")
        self.assertEqual(len(sell_orders), 1)
        self.assertAlmostEqual(sell_orders[0]["price"], 111.0, places=8)
        self.assertAlmostEqual(sell_orders[0]["qty"], 0.9, places=8)

    def test_synthetic_neutral_keeps_cost_unknown_when_recovery_lacks_trade_history(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.1886,
            ask_price=0.1887,
            step_price=0.0002,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=180.0,
            threshold_position_notional=600.0,
            threshold_reduce_target_notional=500.0,
            max_order_position_notional=7000.0,
            max_position_notional=8200.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=10000.0,
            actual_base_qty=11320.0,
            max_short_position_notional=1200.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.006,
            synthetic_freeze_min_notional=20.0,
        )

        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(desired_orders, [])
        self.assertEqual(cached_runtime["position_lots"], [])
        self.assertEqual(cached_runtime["recovery_mode"], "conservative_reduce_only")
        self.assertIn("missing_strategy_trade_history", cached_runtime["recovery_errors"])
        self.assertTrue(controls["synthetic_cost_unknown"])
        self.assertEqual(controls["recovery_mode"], "conservative_reduce_only")

    def test_synthetic_neutral_treats_cached_synthetic_recovered_lot_as_cost_unknown(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": {
                    "market_type": "futures",
                    "direction_state": "long_active",
                    "risk_state": "hard_reduce_only",
                    "position_lots": [
                        {
                            "lot_id": "synthetic_recovered",
                            "side": "long",
                            "qty": 1320.0,
                            "entry_price": 0.18865,
                            "opened_at_ms": 0,
                            "source_role": "synthetic_recovery",
                        }
                    ],
                    "recovery_mode": "live",
                    "recovery_errors": [],
                },
                "applied_trade_keys": [],
            },
        }

        _, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.1886,
            ask_price=0.1887,
            step_price=0.0002,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=180.0,
            threshold_position_notional=600.0,
            threshold_reduce_target_notional=500.0,
            max_order_position_notional=7000.0,
            max_position_notional=8200.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=10000.0,
            actual_base_qty=11320.0,
            max_short_position_notional=1200.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.006,
            synthetic_freeze_min_notional=20.0,
        )

        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["position_lots"], [])
        self.assertEqual(cached_runtime["recovery_mode"], "conservative_reduce_only")
        self.assertIn("synthetic_recovered_cost_unknown", cached_runtime["recovery_errors"])
        self.assertTrue(controls["synthetic_cost_unknown"])

    def test_build_spot_competition_inventory_grid_orders_supports_multiple_buy_levels(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=100.0,
            ask_price=100.2,
            step_price=0.7,
            buy_levels=3,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=20.0,
            threshold_position_notional=200.0,
            max_order_position_notional=300.0,
            max_position_notional=400.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "BUY"])
        self.assertEqual([order["role"] for order in desired_orders], ["bootstrap_entry", "grid_entry", "grid_entry"])
        self.assertEqual([order["price"] for order in desired_orders], [100.0, 99.3, 98.6])
        self.assertEqual(controls["effective_buy_levels"], 3)

    def test_build_spot_competition_inventory_grid_orders_supports_multiple_sell_levels(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="spot")
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side="BUY",
            price=100.0,
            qty=1.0,
            fill_time_ms=1000,
            step_price=0.7,
        )

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "strategy_mode": "spot_competition_inventory_grid",
                "known_orders": {},
                "inventory_lots": [
                    {
                        "qty": 1.0,
                        "cost_quote": 100.0,
                        "buy_time_ms": 1000,
                        "tag": "held",
                    }
                ],
                "spot_competition_inventory_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_inventory_grid",
                    "market_type": "spot",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
            },
            trades=[],
            bid_price=99.9,
            ask_price=100.1,
            step_price=0.7,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=20.0,
            threshold_position_notional=200.0,
            max_order_position_notional=300.0,
            max_position_notional=400.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
        )

        sell_orders = [order for order in desired_orders if str(order.get("side", "")).upper() == "SELL"]
        self.assertEqual([order["price"] for order in sell_orders], [100.7, 101.4, 102.1])
        self.assertEqual(controls["effective_sell_levels"], 3)

    def test_build_spot_competition_inventory_grid_orders_reduces_to_target_without_pair_credit(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="spot")
        runtime["direction_state"] = "long_active"
        runtime["grid_anchor_price"] = 100.0
        runtime["pair_credit_steps"] = 0
        runtime["position_lots"] = [
            {
                "lot_id": "held",
                "side": "long",
                "qty": 4.0,
                "entry_price": 101.0,
                "opened_at_ms": 1000,
                "source_role": "bootstrap_entry",
            }
        ]

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "strategy_mode": "spot_competition_inventory_grid",
                "known_orders": {},
                "inventory_lots": [
                    {
                        "qty": 4.0,
                        "cost_quote": 404.0,
                        "buy_time_ms": 1000,
                        "tag": "held",
                    }
                ],
                "spot_competition_inventory_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_inventory_grid",
                    "market_type": "spot",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
            },
            trades=[],
            bid_price=99.9,
            ask_price=100.1,
            step_price=1.0,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.0,
            per_order_notional=100.0,
            threshold_position_notional=300.0,
            threshold_reduce_target_notional=250.0,
            max_order_position_notional=500.0,
            max_position_notional=500.0,
            tick_size=0.1,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
        )

        sell_orders = [order for order in desired_orders if str(order.get("side", "")).upper() == "SELL"]
        self.assertEqual([order["role"] for order in sell_orders], ["grid_exit", "forced_reduce"])
        self.assertEqual(controls["risk_state"], "threshold_reduce_only")
        self.assertEqual(controls["inventory_soft_limit_notional"], 250.0)
        self.assertEqual(controls["threshold_position_notional"], 300.0)

    def test_build_spot_competition_inventory_grid_orders_supports_warmup_and_non_loss_controls(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=0.33,
            ask_price=0.331,
            step_price=0.0005,
            buy_levels=5,
            sell_levels=5,
            first_order_multiplier=1.0,
            per_order_notional=30.0,
            threshold_position_notional=200.0,
            warmup_position_notional=30.0,
            require_non_loss_exit=True,
            max_order_position_notional=300.0,
            max_position_notional=300.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY"])
        self.assertEqual(controls["warmup_position_notional"], 30.0)
        self.assertTrue(controls["require_non_loss_exit"])

    def test_normalize_commission_quote_handles_quote_asset(self) -> None:
        fee = _normalize_commission_quote(
            commission=0.015,
            commission_asset="USDT",
            price=0.0269,
            base_asset="SAHARA",
            quote_asset="USDT",
        )
        self.assertAlmostEqual(fee, 0.015, places=10)

    def test_spot_order_meets_exchange_mins_rejects_notusdt_dust_sell(self) -> None:
        self.assertFalse(
            _spot_order_meets_exchange_mins(
                qty=76.0,
                price=0.00062,
                min_qty=1.0,
                min_notional=5.0,
            )
        )
        self.assertTrue(
            _spot_order_meets_exchange_mins(
                qty=50000.0,
                price=0.00062,
                min_qty=1.0,
                min_notional=5.0,
            )
        )

    def test_normalize_commission_quote_handles_base_asset(self) -> None:
        fee = _normalize_commission_quote(
            commission=1.5,
            commission_asset="SAHARA",
            price=0.0269,
            base_asset="SAHARA",
            quote_asset="USDT",
        )
        self.assertAlmostEqual(fee, 1.5 * 0.0269, places=10)

    @patch("grid_optimizer.spot_loop_runner.fetch_spot_latest_price")
    def test_normalize_commission_quote_converts_bnb_to_quote(self, mock_latest_price) -> None:
        mock_latest_price.return_value = 600.0
        fee = _normalize_commission_quote(
            commission=0.01,
            commission_asset="BNB",
            price=0.0269,
            base_asset="SAHARA",
            quote_asset="USDT",
        )
        self.assertAlmostEqual(fee, 6.0, places=10)


if __name__ == "__main__":
    unittest.main()
