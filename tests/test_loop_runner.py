from __future__ import annotations

from argparse import Namespace
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.audit import build_audit_paths
from grid_optimizer.loop_runner import (
    AUDIT_SYNC_MIN_INTERVAL_SECONDS,
    _should_sync_account_audit,
    _apply_synthetic_trade_fill,
    _current_check_bucket,
    _custom_grid_levels_above_current,
    _read_custom_grid_trade_count,
    _resolve_custom_grid_roll,
    _shift_custom_grid_bounds,
    StartupProtectionError,
    apply_excess_inventory_reduce_only,
    apply_hedge_position_controls,
    apply_hedge_position_notional_caps,
    apply_inventory_tiering,
    apply_max_position_notional_cap,
    apply_position_controls,
    apply_short_cover_pause,
    apply_warm_start_bootstrap_guard,
    assess_flat_start_guard,
    apply_xaut_reduce_only_pruning,
    assess_auto_regime,
    assess_market_guard,
    assess_xaut_adaptive_regime,
    build_effective_runner_args,
    build_xaut_adaptive_runner_args,
    generate_plan_report,
    execute_plan_report,
    resolve_neutral_hourly_scale,
    resolve_market_bias_entry_pause,
    resolve_market_bias_offsets,
    resolve_market_bias_regime_switch,
    resolve_adaptive_step_price,
    resolve_auto_regime_profile,
    resolve_xaut_adaptive_state,
    update_synthetic_order_refs,
)
from grid_optimizer.semi_auto_plan import build_static_binance_grid_plan
from grid_optimizer.types import Candle


class LoopRunnerTests(unittest.TestCase):
    def test_current_check_bucket_rounds_down_to_interval(self) -> None:
        bucket = _current_check_bucket(
            datetime(2026, 3, 31, 14, 27, 12, tzinfo=timezone.utc),
            interval_minutes=5,
        )
        self.assertEqual(bucket, "2026-03-31T14:25Z")

    def test_custom_grid_levels_above_current_counts_remaining_complete_ladders(self) -> None:
        levels = [10.0, 11.0, 12.0, 13.0, 14.0]
        self.assertEqual(_custom_grid_levels_above_current(levels, 12.0), 2)
        self.assertEqual(_custom_grid_levels_above_current(levels, 11.99), 2)
        self.assertEqual(_custom_grid_levels_above_current(levels, 9.5), 4)
        self.assertEqual(_custom_grid_levels_above_current(levels, 14.5), 0)

    def test_shift_custom_grid_bounds_supports_arithmetic_and_geometric(self) -> None:
        arithmetic = _shift_custom_grid_bounds(
            min_price=10.0,
            max_price=14.0,
            n=4,
            grid_level_mode="arithmetic",
            shift_levels=1,
        )
        self.assertAlmostEqual(arithmetic["min_price"], 9.0, places=8)
        self.assertAlmostEqual(arithmetic["max_price"], 13.0, places=8)

        geometric = _shift_custom_grid_bounds(
            min_price=10.0,
            max_price=160.0,
            n=4,
            grid_level_mode="geometric",
            shift_levels=1,
        )
        self.assertAlmostEqual(geometric["min_price"], 5.0, places=8)
        self.assertAlmostEqual(geometric["max_price"], 80.0, places=8)

    def test_read_custom_grid_trade_count_uses_trade_audit_path(self) -> None:
        with TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "opnusdt_loop_events.jsonl"
            trade_path = build_audit_paths(summary_path)["trade_audit"]
            trade_path.write_text('{"id":1}\n{"id":2}\n{"id":3}\n', encoding="utf-8")

            self.assertEqual(_read_custom_grid_trade_count(summary_path), 3)

    def test_resolve_custom_grid_roll_triggers_and_resets_trade_baseline(self) -> None:
        with TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "opnusdt_loop_events.jsonl"
            trade_path = build_audit_paths(summary_path)["trade_audit"]
            trade_path.write_text('{"id":1}\n{"id":2}\n{"id":3}\n', encoding="utf-8")
            state: dict[str, object] = {}

            new_min, new_max, roll = _resolve_custom_grid_roll(
                state=state,
                summary_path=summary_path,
                now=datetime(2026, 3, 31, 14, 27, 12, tzinfo=timezone.utc),
                current_price=12.0,
                min_price=10.0,
                max_price=14.0,
                n=4,
                grid_level_mode="arithmetic",
                direction="long",
                enabled=True,
                interval_minutes=5,
                trade_threshold=3,
                upper_distance_ratio=0.50,
                shift_levels=1,
            )

            self.assertAlmostEqual(new_min, 9.0, places=8)
            self.assertAlmostEqual(new_max, 13.0, places=8)
            self.assertTrue(roll["checked"])
            self.assertTrue(roll["triggered"])
            self.assertEqual(roll["reason"], "triggered")
            self.assertEqual(roll["levels_above_current"], 2)
            self.assertEqual(roll["required_levels_above"], 2)
            self.assertEqual(roll["current_trade_count"], 3)
            self.assertEqual(roll["trades_since_last_roll"], 0)
            self.assertEqual(state["custom_grid_roll_last_check_bucket"], "2026-03-31T14:25Z")
            self.assertEqual(state["custom_grid_roll_trade_baseline"], 3)
            self.assertEqual(state["custom_grid_roll_trades_since_last_roll"], 0)
            self.assertAlmostEqual(state["custom_grid_runtime_min_price"], 9.0, places=8)
            self.assertAlmostEqual(state["custom_grid_runtime_max_price"], 13.0, places=8)
            self.assertEqual(state["custom_grid_roll_last_applied_at"], "2026-03-31T14:27:12+00:00")
            self.assertAlmostEqual(float(state["custom_grid_roll_last_applied_price"]), 12.0, places=8)

    def test_resolve_custom_grid_roll_skips_when_bucket_already_checked(self) -> None:
        with TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "opnusdt_loop_events.jsonl"
            trade_path = build_audit_paths(summary_path)["trade_audit"]
            trade_path.write_text('{"id":1}\n{"id":2}\n{"id":3}\n{"id":4}\n', encoding="utf-8")
            state: dict[str, object] = {
                "custom_grid_roll_last_check_bucket": "2026-03-31T14:25Z",
                "custom_grid_roll_trade_baseline": 1,
            }

            new_min, new_max, roll = _resolve_custom_grid_roll(
                state=state,
                summary_path=summary_path,
                now=datetime(2026, 3, 31, 14, 29, 0, tzinfo=timezone.utc),
                current_price=12.0,
                min_price=10.0,
                max_price=14.0,
                n=4,
                grid_level_mode="arithmetic",
                direction="long",
                enabled=True,
                interval_minutes=5,
                trade_threshold=3,
                upper_distance_ratio=0.50,
                shift_levels=1,
            )

            self.assertAlmostEqual(new_min, 10.0, places=8)
            self.assertAlmostEqual(new_max, 14.0, places=8)
            self.assertFalse(roll["checked"])
            self.assertFalse(roll["triggered"])
            self.assertEqual(roll["reason"], "already_checked_bucket")
            self.assertEqual(roll["trades_since_last_roll"], 3)
            self.assertEqual(state["custom_grid_roll_last_check_bucket"], "2026-03-31T14:25Z")
            self.assertEqual(state["custom_grid_roll_trades_since_last_roll"], 3)

    def test_should_sync_account_audit_when_updated_at_missing_or_stale(self) -> None:
        now = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        self.assertTrue(_should_sync_account_audit({}, now=now))
        self.assertTrue(
            _should_sync_account_audit(
                {"updated_at": (now - timedelta(seconds=AUDIT_SYNC_MIN_INTERVAL_SECONDS + 1)).isoformat()},
                now=now,
            )
        )

    def test_should_not_sync_account_audit_when_recently_updated(self) -> None:
        now = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        self.assertFalse(
            _should_sync_account_audit(
                {"updated_at": (now - timedelta(seconds=AUDIT_SYNC_MIN_INTERVAL_SECONDS - 1)).isoformat()},
                now=now,
            )
        )

    def test_build_static_binance_grid_plan_splits_orders_by_current_price(self) -> None:
        bootstrap = build_static_binance_grid_plan(
            strategy_direction="long",
            grid_level_mode="arithmetic",
            min_price=10.0,
            max_price=14.0,
            n=4,
            total_grid_notional=400.0,
            neutral_anchor_price=None,
            bid_price=11.99,
            ask_price=12.01,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
        )
        target_qty = float(bootstrap["target_base_qty"])
        self.assertGreater(target_qty, 0.0)

        live = build_static_binance_grid_plan(
            strategy_direction="long",
            grid_level_mode="arithmetic",
            min_price=10.0,
            max_price=14.0,
            n=4,
            total_grid_notional=400.0,
            neutral_anchor_price=None,
            bid_price=11.99,
            ask_price=12.01,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            current_long_qty=target_qty,
            current_short_qty=0.0,
        )

        self.assertGreater(len(live["buy_orders"]), 0)
        self.assertGreater(len(live["sell_orders"]), 0)
        nearest_sell = min(item["price"] for item in live["sell_orders"])
        self.assertAlmostEqual(nearest_sell, 13.0, places=8)

    def test_build_static_binance_grid_plan_can_disable_base_bootstrap(self) -> None:
        plan = build_static_binance_grid_plan(
            strategy_direction="long",
            grid_level_mode="arithmetic",
            min_price=10.0,
            max_price=14.0,
            n=4,
            total_grid_notional=400.0,
            neutral_anchor_price=None,
            bid_price=11.99,
            ask_price=12.01,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
            bootstrap_positions=False,
        )

        self.assertGreater(len(plan["buy_orders"]), 0)
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["target_base_qty"], 0.0)
        self.assertEqual(plan["bootstrap_qty"], 0.0)
        self.assertEqual(plan["target_long_base_qty"], 0.0)
        self.assertEqual(plan["bootstrap_long_qty"], 0.0)

    def _base_one_way_long_args(self, tmpdir: str, **overrides: object) -> Namespace:
        payload: dict[str, object] = {
            "symbol": "BARDUSDT",
            "strategy_mode": "one_way_long",
            "strategy_profile": "bard_volume_long_v2",
            "step_price": 0.0002,
            "buy_levels": 5,
            "sell_levels": 11,
            "per_order_notional": 40.0,
            "base_position_notional": 120.0,
            "center_price": None,
            "flat_start_enabled": True,
            "warm_start_enabled": True,
            "fixed_center_enabled": False,
            "fixed_center_roll_enabled": False,
            "fixed_center_roll_trigger_steps": 1.0,
            "fixed_center_roll_confirm_cycles": 3,
            "fixed_center_roll_shift_steps": 1,
            "custom_grid_enabled": False,
            "custom_grid_direction": None,
            "custom_grid_level_mode": "arithmetic",
            "custom_grid_min_price": None,
            "custom_grid_max_price": None,
            "custom_grid_n": None,
            "custom_grid_total_notional": None,
            "custom_grid_neutral_anchor_price": None,
            "custom_grid_roll_enabled": False,
            "custom_grid_roll_interval_minutes": 5,
            "custom_grid_roll_trade_threshold": 100,
            "custom_grid_roll_upper_distance_ratio": 0.30,
            "custom_grid_roll_shift_levels": 1,
            "down_trigger_steps": 2,
            "up_trigger_steps": 2,
            "shift_steps": 2,
            "neutral_center_interval_minutes": 3,
            "neutral_band1_offset_ratio": 0.005,
            "neutral_band2_offset_ratio": 0.01,
            "neutral_band3_offset_ratio": 0.02,
            "neutral_band1_target_ratio": 0.20,
            "neutral_band2_target_ratio": 0.50,
            "neutral_band3_target_ratio": 1.00,
            "neutral_hourly_scale_enabled": False,
            "neutral_hourly_scale_stable": 1.0,
            "neutral_hourly_scale_transition": 0.85,
            "neutral_hourly_scale_defensive": 0.65,
            "max_position_notional": 560.0,
            "max_short_position_notional": None,
            "pause_buy_position_notional": 420.0,
            "pause_short_position_notional": None,
            "min_mid_price_for_buys": None,
            "excess_inventory_reduce_only_enabled": True,
            "auto_regime_enabled": False,
            "auto_regime_confirm_cycles": 2,
            "auto_regime_stable_15m_max_amplitude_ratio": 0.02,
            "auto_regime_stable_60m_max_amplitude_ratio": 0.05,
            "auto_regime_stable_60m_return_floor_ratio": -0.01,
            "auto_regime_defensive_15m_amplitude_ratio": 0.035,
            "auto_regime_defensive_60m_amplitude_ratio": 0.08,
            "auto_regime_defensive_15m_return_ratio": -0.015,
            "auto_regime_defensive_60m_return_ratio": -0.03,
            "buy_pause_amp_trigger_ratio": 0.0048,
            "buy_pause_down_return_trigger_ratio": -0.002,
            "short_cover_pause_amp_trigger_ratio": None,
            "short_cover_pause_down_return_trigger_ratio": None,
            "freeze_shift_abs_return_trigger_ratio": 0.004,
            "inventory_tier_start_notional": 260.0,
            "inventory_tier_end_notional": 380.0,
            "inventory_tier_buy_levels": 2,
            "inventory_tier_sell_levels": 14,
            "inventory_tier_per_order_notional": 35.0,
            "inventory_tier_base_position_notional": 80.0,
            "recv_window": 5000,
            "reset_state": True,
            "state_path": str(Path(tmpdir) / "bardusdt_state.json"),
            "summary_jsonl": str(Path(tmpdir) / "bardusdt_events.jsonl"),
        }
        payload.update(overrides)
        return Namespace(**payload)

    @patch("grid_optimizer.loop_runner._resolve_custom_grid_roll")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_custom_grid_long_ignores_base_inventory_gate(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_resolve_custom_grid_roll,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.01,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "11.99", "ask_price": "12.01"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "KATUSDT", "positionAmt": "60", "entryPrice": "11.50"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "shift_frozen": False,
        }
        mock_resolve_custom_grid_roll.return_value = (
            10.0,
            14.0,
            {"enabled": False, "checked": False, "triggered": False, "reason": "disabled"},
        )

        with TemporaryDirectory() as tmpdir:
            args = Namespace(
                symbol="KATUSDT",
                strategy_mode="one_way_long",
                strategy_profile="custom_grid_katusdt_kat",
                step_price=0.01,
                buy_levels=95,
                sell_levels=5,
                per_order_notional=10.0,
                base_position_notional=30.0,
                center_price=12.0,
                fixed_center_enabled=False,
                fixed_center_roll_enabled=False,
                fixed_center_roll_trigger_steps=1.0,
                fixed_center_roll_confirm_cycles=3,
                fixed_center_roll_shift_steps=1,
                custom_grid_enabled=True,
                custom_grid_direction="long",
                custom_grid_level_mode="arithmetic",
                custom_grid_min_price=10.0,
                custom_grid_max_price=14.0,
                custom_grid_n=4,
                custom_grid_total_notional=400.0,
                custom_grid_neutral_anchor_price=None,
                custom_grid_roll_enabled=False,
                custom_grid_roll_interval_minutes=5,
                custom_grid_roll_trade_threshold=100,
                custom_grid_roll_upper_distance_ratio=0.30,
                custom_grid_roll_shift_levels=1,
                down_trigger_steps=1,
                up_trigger_steps=1,
                shift_steps=1,
                neutral_center_interval_minutes=3,
                neutral_band1_offset_ratio=0.005,
                neutral_band2_offset_ratio=0.01,
                neutral_band3_offset_ratio=0.02,
                neutral_band1_target_ratio=0.20,
                neutral_band2_target_ratio=0.50,
                neutral_band3_target_ratio=1.00,
                neutral_hourly_scale_enabled=False,
                neutral_hourly_scale_stable=1.0,
                neutral_hourly_scale_transition=0.85,
                neutral_hourly_scale_defensive=0.65,
                max_position_notional=2000.0,
                max_short_position_notional=None,
                pause_buy_position_notional=None,
                pause_short_position_notional=None,
                min_mid_price_for_buys=None,
                excess_inventory_reduce_only_enabled=True,
                auto_regime_enabled=False,
                auto_regime_confirm_cycles=2,
                auto_regime_stable_15m_max_amplitude_ratio=0.02,
                auto_regime_stable_60m_max_amplitude_ratio=0.05,
                auto_regime_stable_60m_return_floor_ratio=-0.01,
                auto_regime_defensive_15m_amplitude_ratio=0.035,
                auto_regime_defensive_60m_amplitude_ratio=0.08,
                auto_regime_defensive_15m_return_ratio=-0.015,
                auto_regime_defensive_60m_return_ratio=-0.03,
                buy_pause_amp_trigger_ratio=None,
                buy_pause_down_return_trigger_ratio=None,
                freeze_shift_abs_return_trigger_ratio=None,
                recv_window=5000,
                reset_state=True,
                state_path=str(Path(tmpdir) / "katusdt_state.json"),
                summary_jsonl=str(Path(tmpdir) / "katusdt_events.jsonl"),
            )

            report = generate_plan_report(args)

        self.assertFalse(report["buy_paused"])
        self.assertFalse(report["excess_inventory_gate"]["active"])
        self.assertEqual(report["target_base_qty"], 0.0)
        self.assertEqual(report["bootstrap_qty"], 0.0)
        self.assertEqual(report["bootstrap_orders"], [])
        self.assertGreater(len(report["buy_orders"]), 0)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_blocks_flat_start_when_open_orders_exist(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.3120", "ask_price": "0.3122"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "BARDUSDT", "positionAmt": "0", "entryPrice": "0"},
            ],
        }
        mock_open_orders.return_value = [{"orderId": 12345}]
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(tmpdir)
            with self.assertRaises(StartupProtectionError):
                generate_plan_report(args)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_warm_starts_existing_long_without_bootstrap(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.3120", "ask_price": "0.3122"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "BARDUSDT", "positionAmt": "300", "entryPrice": "0.3000"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(tmpdir)
            report = generate_plan_report(args)
            state = json.loads(Path(args.state_path).read_text(encoding="utf-8"))

        self.assertTrue(report["startup_pending"])
        self.assertTrue(report["warm_start"]["active"])
        self.assertEqual(report["bootstrap_orders"], [])
        self.assertEqual(report["bootstrap_qty"], 0.0)
        self.assertGreater(report["target_base_qty"], report["current_long_qty"])
        self.assertFalse(state["startup_pending"])
        self.assertIsNotNone(state["startup_completed_at"])

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_market_bias_regime_switch_uses_directional_short_inventory(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.0700", "ask_price": "0.0702"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "BASEDUSDT", "positionAmt": "-3200", "entryPrice": "0.0710"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "return_ratio": -0.004,
            "amplitude_ratio": 0.003,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BASEDUSDT",
                strategy_profile="volume_neutral_ping_pong_v1",
                strategy_mode="synthetic_neutral",
                base_position_notional=0.0,
                buy_levels=4,
                sell_levels=4,
                per_order_notional=45.0,
                startup_entry_multiplier=4.0,
                step_price=0.0002,
                market_bias_enabled=True,
                market_bias_max_shift_steps=0.75,
                market_bias_signal_steps=2.0,
                market_bias_drift_weight=0.65,
                market_bias_return_weight=0.35,
                market_bias_weak_buy_pause_enabled=True,
                market_bias_weak_buy_pause_threshold=0.15,
                market_bias_strong_short_pause_enabled=True,
                market_bias_strong_short_pause_threshold=0.15,
                market_bias_regime_switch_enabled=True,
                market_bias_regime_switch_confirm_cycles=1,
                market_bias_regime_switch_weak_threshold=0.15,
                market_bias_regime_switch_strong_threshold=0.15,
                pause_buy_position_notional=500.0,
                pause_short_position_notional=500.0,
                max_position_notional=600.0,
                max_short_position_notional=600.0,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["requested_strategy_mode"], "synthetic_neutral")
        self.assertEqual(report["strategy_mode"], "one_way_short")
        self.assertEqual(report["market_bias"]["regime"], "weak")
        self.assertEqual(report["market_bias_regime_switch"]["active_mode"], "one_way_short")
        self.assertAlmostEqual(report["current_long_qty"], 0.0)
        self.assertAlmostEqual(report["current_short_qty"], 3200.0)
        self.assertIsNone(report["synthetic_ledger"])

    def test_apply_synthetic_trade_fill_tracks_virtual_long_and_short_books(self) -> None:
        ledger = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "100", "price": "1.20"},
                order_ref={"role": "entry_long"},
            )
        )
        self.assertAlmostEqual(ledger["virtual_long_qty"], 100.0)
        self.assertAlmostEqual(ledger["virtual_long_avg_price"], 1.20)

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "40", "price": "1.25"},
                order_ref={"role": "entry_short"},
            )
        )
        self.assertAlmostEqual(ledger["virtual_short_qty"], 40.0)
        self.assertAlmostEqual(ledger["virtual_short_avg_price"], 1.25)

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "30", "price": "1.28"},
                order_ref={"role": "take_profit_long"},
            )
        )
        self.assertAlmostEqual(ledger["virtual_long_qty"], 70.0)

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "10", "price": "1.18"},
                order_ref={"role": "take_profit_short"},
            )
        )
        self.assertAlmostEqual(ledger["virtual_short_qty"], 30.0)

    def test_update_synthetic_order_refs_persists_placed_orders(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            update_synthetic_order_refs(
                state_path=state_path,
                strategy_mode="synthetic_neutral",
                submit_report={
                    "placed_orders": [
                        {
                            "request": {
                                "role": "entry_short",
                                "side": "SELL",
                                "position_side": "BOTH",
                            },
                            "response": {
                                "orderId": 12345,
                                "clientOrderId": "gx-test",
                            },
                        }
                    ]
                },
            )
            payload = state_path.read_text(encoding="utf-8")
            self.assertIn("synthetic_order_refs", payload)
            self.assertIn("12345", payload)

    def test_apply_hedge_position_controls_can_pause_long_or_short_side_independently(self) -> None:
        plan = {
            "bootstrap_orders": [
                {"side": "BUY", "price": 1.20, "qty": 10, "notional": 12.0, "role": "bootstrap_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.21, "qty": 10, "notional": 12.1, "role": "bootstrap_short", "position_side": "SHORT"},
            ],
            "buy_orders": [
                {"side": "BUY", "price": 1.19, "qty": 10, "notional": 11.9, "role": "entry_long", "position_side": "LONG"},
                {"side": "BUY", "price": 1.18, "qty": 10, "notional": 11.8, "role": "take_profit_short", "position_side": "SHORT"},
            ],
            "sell_orders": [
                {"side": "SELL", "price": 1.22, "qty": 10, "notional": 12.2, "role": "take_profit_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.23, "qty": 10, "notional": 12.3, "role": "entry_short", "position_side": "SHORT"},
            ],
        }

        result = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=500,
            current_short_qty=450,
            mid_price=1.0,
            pause_long_position_notional=400.0,
            pause_short_position_notional=400.0,
            min_mid_price_for_buys=None,
        )

        self.assertTrue(result["long_paused"])
        self.assertTrue(result["short_paused"])
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(plan["buy_orders"][0]["role"], "take_profit_short")
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit_long")

    def test_apply_hedge_position_controls_respects_external_short_pause(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "SELL", "price": 1.21, "qty": 10, "notional": 12.1, "role": "bootstrap_short", "position_side": "SHORT"}],
            "buy_orders": [{"side": "BUY", "price": 1.18, "qty": 10, "notional": 11.8, "role": "take_profit_short", "position_side": "SHORT"}],
            "sell_orders": [
                {"side": "SELL", "price": 1.22, "qty": 10, "notional": 12.2, "role": "take_profit_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.23, "qty": 10, "notional": 12.3, "role": "entry_short", "position_side": "SHORT"},
            ],
        }

        result = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=0.0,
            mid_price=1.0,
            pause_long_position_notional=None,
            pause_short_position_notional=None,
            min_mid_price_for_buys=None,
            external_short_pause=True,
            external_short_pause_reasons=["market_bias_strong_short_pause regime=strong score=+0.24 >= +0.15"],
        )

        self.assertFalse(result["long_paused"])
        self.assertTrue(result["short_paused"])
        self.assertEqual(result["short_pause_reasons"], ["market_bias_strong_short_pause regime=strong score=+0.24 >= +0.15"])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit_long")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(plan["buy_orders"][0]["role"], "take_profit_short")

    def test_apply_hedge_position_notional_caps_trim_long_and_short_entries(self) -> None:
        plan = {
            "bootstrap_orders": [
                {"side": "BUY", "price": 1.0, "qty": 50, "notional": 50.0, "role": "bootstrap_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.0, "qty": 50, "notional": 50.0, "role": "bootstrap_short", "position_side": "SHORT"},
            ],
            "buy_orders": [
                {"side": "BUY", "price": 0.99, "qty": 50, "notional": 49.5, "role": "entry_long", "position_side": "LONG"},
            ],
            "sell_orders": [
                {"side": "SELL", "price": 1.01, "qty": 50, "notional": 50.5, "role": "entry_short", "position_side": "SHORT"},
            ],
        }

        result = apply_hedge_position_notional_caps(
            plan=plan,
            current_long_notional=470.0,
            current_short_notional=480.0,
            max_long_position_notional=500.0,
            max_short_position_notional=500.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
        )

        self.assertTrue(result["buy_cap_applied"])
        self.assertTrue(result["short_cap_applied"])
        self.assertAlmostEqual(result["buy_budget_notional"], 30.0)
        self.assertAlmostEqual(result["short_budget_notional"], 20.0)
        self.assertAlmostEqual(result["planned_buy_notional"], 30.0)
        self.assertAlmostEqual(result["planned_short_notional"], 20.0)
        self.assertEqual(len(plan["buy_orders"]), 0)
        self.assertEqual(len(plan["sell_orders"]), 0)
        self.assertAlmostEqual(plan["bootstrap_orders"][0]["notional"], 30.0)
        self.assertAlmostEqual(plan["bootstrap_orders"][1]["notional"], 20.0)

    def test_apply_position_controls_pauses_buys_when_position_notional_is_too_large(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 100, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 100, "role": "entry"}],
            "sell_orders": [{"side": "SELL", "price": 0.051, "qty": 100, "role": "take_profit"}],
        }

        result = apply_position_controls(
            plan=plan,
            current_long_qty=4000,
            mid_price=0.05,
            pause_buy_position_notional=180.0,
            min_mid_price_for_buys=None,
        )

        self.assertTrue(result["buy_paused"])
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)

    def test_apply_position_controls_pauses_buys_below_floor_price(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 100, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 100, "role": "entry"}],
            "sell_orders": [],
        }

        result = apply_position_controls(
            plan=plan,
            current_long_qty=0,
            mid_price=0.0495,
            pause_buy_position_notional=None,
            min_mid_price_for_buys=0.0500,
        )

        self.assertTrue(result["buy_paused"])
        self.assertEqual(len(result["pause_reasons"]), 1)
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])

    def test_apply_position_controls_keeps_buys_when_within_limits(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 100, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 100, "role": "entry"}],
            "sell_orders": [],
        }

        result = apply_position_controls(
            plan=plan,
            current_long_qty=1000,
            mid_price=0.05,
            pause_buy_position_notional=180.0,
            min_mid_price_for_buys=0.0490,
        )

        self.assertFalse(result["buy_paused"])
        self.assertEqual(len(plan["bootstrap_orders"]), 1)
        self.assertEqual(len(plan["buy_orders"]), 1)

    def test_apply_excess_inventory_reduce_only_pauses_long_entries_when_over_target_base(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 100, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 100, "role": "entry"}],
            "sell_orders": [{"side": "SELL", "price": 0.051, "qty": 100, "role": "take_profit"}],
        }

        result = apply_excess_inventory_reduce_only(
            plan=plan,
            strategy_mode="one_way_long",
            current_long_qty=1200,
            current_short_qty=0,
            target_base_qty=1000,
            enabled=True,
        )

        self.assertTrue(result["active"])
        self.assertEqual(result["direction"], "long")
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)

    def test_assess_flat_start_guard_blocks_reverse_short_for_long_startup(self) -> None:
        result = assess_flat_start_guard(
            strategy_mode="one_way_long",
            actual_net_qty=-1573.0,
            open_orders=[],
            enabled=True,
        )

        self.assertTrue(result["blocked"])
        self.assertIn("清空反向空仓", str(result["reason"]))
        self.assertAlmostEqual(result["reverse_qty"], 1573.0)

    def test_assess_flat_start_guard_blocks_lingering_open_orders(self) -> None:
        result = assess_flat_start_guard(
            strategy_mode="one_way_short",
            actual_net_qty=0.0,
            open_orders=[{"orderId": 1}, {"orderId": 2}],
            enabled=True,
        )

        self.assertTrue(result["blocked"])
        self.assertIn("撤掉遗留挂单", str(result["reason"]))
        self.assertEqual(result["open_order_count"], 2)

    def test_apply_warm_start_bootstrap_guard_clears_long_bootstrap_when_inventory_exists(self) -> None:
        plan = {
            "bootstrap_qty": 180.0,
            "bootstrap_long_qty": 180.0,
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 3600, "notional": 180.0, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 1000, "notional": 49.0, "role": "entry"}],
            "sell_orders": [{"side": "SELL", "price": 0.051, "qty": 1000, "notional": 51.0, "role": "take_profit"}],
        }

        result = apply_warm_start_bootstrap_guard(
            plan=plan,
            strategy_mode="one_way_long",
            current_long_qty=300.0,
            current_short_qty=0.0,
            startup_pending=True,
            enabled=True,
        )

        self.assertTrue(result["active"])
        self.assertEqual(result["direction"], "long")
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["bootstrap_qty"], 0.0)
        self.assertEqual(plan["bootstrap_long_qty"], 0.0)
        self.assertEqual(len(plan["buy_orders"]), 1)

    def test_apply_position_controls_respects_external_market_guard_pause(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 100, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 100, "role": "entry"}],
            "sell_orders": [{"side": "SELL", "price": 0.051, "qty": 100, "role": "take_profit"}],
        }

        result = apply_position_controls(
            plan=plan,
            current_long_qty=0,
            mid_price=0.05,
            pause_buy_position_notional=180.0,
            min_mid_price_for_buys=None,
            external_buy_pause=True,
            external_pause_reasons=["1m_extreme_down_candle amp=0.80% ret=-0.40%"],
        )

        self.assertTrue(result["buy_paused"])
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(result["pause_reasons"], ["1m_extreme_down_candle amp=0.80% ret=-0.40%"])

    def test_apply_short_cover_pause_clears_buy_orders_when_active(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "SELL", "price": 0.051, "qty": 100, "role": "bootstrap_short"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 100, "role": "take_profit_short"}],
            "sell_orders": [{"side": "SELL", "price": 0.052, "qty": 100, "role": "entry_short"}],
        }

        result = apply_short_cover_pause(
            plan=plan,
            active=True,
            pause_reasons=["1m_short_cover_pause amp=0.80% ret=-0.40%"],
        )

        self.assertTrue(result["short_cover_paused"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(result["short_cover_pause_reasons"], ["1m_short_cover_pause amp=0.80% ret=-0.40%"])

    @patch("grid_optimizer.loop_runner.assess_auto_regime")
    def test_resolve_neutral_hourly_scale_caches_per_hour_bucket(self, mock_assess_auto_regime) -> None:
        now = datetime(2026, 3, 19, 10, 25, tzinfo=timezone.utc)
        mock_assess_auto_regime.return_value = {
            "available": True,
            "regime": "defensive",
            "reason": "60m ret=-3.5%",
            "metrics": {"window_60m": {"return_ratio": -0.035}},
        }
        state: dict[str, object] = {}

        first = resolve_neutral_hourly_scale(
            state=state,
            symbol="OPNUSDT",
            enabled=True,
            stable_scale=1.0,
            transition_scale=0.85,
            defensive_scale=0.65,
            stable_15m_max_amplitude_ratio=0.02,
            stable_60m_max_amplitude_ratio=0.05,
            stable_60m_return_floor_ratio=-0.01,
            defensive_15m_amplitude_ratio=0.035,
            defensive_60m_amplitude_ratio=0.08,
            defensive_15m_return_ratio=-0.015,
            defensive_60m_return_ratio=-0.03,
            now=now,
        )
        second = resolve_neutral_hourly_scale(
            state=state,
            symbol="OPNUSDT",
            enabled=True,
            stable_scale=1.0,
            transition_scale=0.85,
            defensive_scale=0.65,
            stable_15m_max_amplitude_ratio=0.02,
            stable_60m_max_amplitude_ratio=0.05,
            stable_60m_return_floor_ratio=-0.01,
            defensive_15m_amplitude_ratio=0.035,
            defensive_60m_amplitude_ratio=0.08,
            defensive_15m_return_ratio=-0.015,
            defensive_60m_return_ratio=-0.03,
            now=now + timedelta(minutes=10),
        )

        self.assertAlmostEqual(first["scale"], 0.65)
        self.assertEqual(first["regime"], "defensive")
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(mock_assess_auto_regime.call_count, 1)

    def test_apply_max_position_notional_cap_trims_buy_side_to_remaining_budget(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 2000, "notional": 100.0, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 1000, "notional": 49.0, "role": "entry"}],
            "sell_orders": [{"side": "SELL", "price": 0.051, "qty": 1000, "notional": 51.0, "role": "take_profit"}],
        }

        result = apply_max_position_notional_cap(
            plan=plan,
            current_long_notional=910.0,
            max_position_notional=1000.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
        )

        self.assertTrue(result["cap_applied"])
        self.assertAlmostEqual(result["buy_budget_notional"], 90.0)
        self.assertAlmostEqual(result["planned_buy_notional"], 90.0)
        self.assertEqual(len(plan["bootstrap_orders"]), 1)
        self.assertAlmostEqual(plan["bootstrap_orders"][0]["qty"], 1800.0)
        self.assertAlmostEqual(plan["bootstrap_orders"][0]["notional"], 90.0)
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)

    def test_apply_max_position_notional_cap_keeps_orders_when_under_cap(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 1000, "notional": 50.0, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 1000, "notional": 49.0, "role": "entry"}],
            "sell_orders": [],
        }

        result = apply_max_position_notional_cap(
            plan=plan,
            current_long_notional=800.0,
            max_position_notional=1000.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
        )

        self.assertFalse(result["cap_applied"])
        self.assertAlmostEqual(result["buy_budget_notional"], 200.0)
        self.assertAlmostEqual(result["planned_buy_notional"], 99.0)
        self.assertEqual(len(plan["bootstrap_orders"]), 1)
        self.assertEqual(len(plan["buy_orders"]), 1)

    def test_apply_max_position_notional_cap_drops_buy_orders_when_budget_is_below_exchange_minimum(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "price": 0.05, "qty": 1000, "notional": 50.0, "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "price": 0.049, "qty": 1000, "notional": 49.0, "role": "entry"}],
            "sell_orders": [{"side": "SELL", "price": 0.051, "qty": 1000, "notional": 51.0, "role": "take_profit"}],
        }

        result = apply_max_position_notional_cap(
            plan=plan,
            current_long_notional=998.0,
            max_position_notional=1000.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
        )

        self.assertTrue(result["cap_applied"])
        self.assertAlmostEqual(result["buy_budget_notional"], 2.0)
        self.assertAlmostEqual(result["planned_buy_notional"], 0.0)
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)

    def test_apply_inventory_tiering_keeps_base_profile_below_threshold(self) -> None:
        result = apply_inventory_tiering(
            current_long_notional=580.0,
            buy_levels=8,
            sell_levels=8,
            per_order_notional=70.0,
            base_position_notional=420.0,
            tier_start_notional=600.0,
            tier_end_notional=750.0,
            tier_buy_levels=4,
            tier_sell_levels=12,
            tier_per_order_notional=70.0,
            tier_base_position_notional=280.0,
        )

        self.assertTrue(result["enabled"])
        self.assertFalse(result["active"])
        self.assertEqual(result["effective_buy_levels"], 8)
        self.assertEqual(result["effective_sell_levels"], 8)
        self.assertAlmostEqual(result["effective_base_position_notional"], 420.0)

    def test_apply_inventory_tiering_interpolates_between_base_and_tier_targets(self) -> None:
        result = apply_inventory_tiering(
            current_long_notional=675.0,
            buy_levels=8,
            sell_levels=8,
            per_order_notional=70.0,
            base_position_notional=420.0,
            tier_start_notional=600.0,
            tier_end_notional=750.0,
            tier_buy_levels=4,
            tier_sell_levels=12,
            tier_per_order_notional=70.0,
            tier_base_position_notional=280.0,
        )

        self.assertTrue(result["active"])
        self.assertAlmostEqual(result["ratio"], 0.5)
        self.assertEqual(result["effective_buy_levels"], 6)
        self.assertEqual(result["effective_sell_levels"], 10)
        self.assertAlmostEqual(result["effective_per_order_notional"], 70.0)
        self.assertAlmostEqual(result["effective_base_position_notional"], 350.0)

    def test_apply_inventory_tiering_hits_tier_profile_at_upper_bound(self) -> None:
        result = apply_inventory_tiering(
            current_long_notional=760.0,
            buy_levels=8,
            sell_levels=8,
            per_order_notional=70.0,
            base_position_notional=420.0,
            tier_start_notional=600.0,
            tier_end_notional=750.0,
            tier_buy_levels=4,
            tier_sell_levels=12,
            tier_per_order_notional=70.0,
            tier_base_position_notional=280.0,
        )

        self.assertTrue(result["active"])
        self.assertAlmostEqual(result["ratio"], 1.0)
        self.assertEqual(result["effective_buy_levels"], 4)
        self.assertEqual(result["effective_sell_levels"], 12)
        self.assertAlmostEqual(result["effective_base_position_notional"], 280.0)

    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_handles_optional_synthetic_fields(self, mock_validate_plan_report, mock_book_tickers) -> None:
        mock_validate_plan_report.return_value = {
            "ok": False,
            "errors": ["plan contains no actions to execute"],
            "actions": {"place_count": 0, "cancel_count": 0},
        }
        mock_book_tickers.return_value = [{"bid_price": "1.0000", "ask_price": "1.0002"}]

        args = Namespace(
            symbol="OPNUSDT",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=True,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/opnusdt_loop_latest_plan.json",
            apply=False,
            margin_type="KEEP",
            leverage=2,
            maker_retries=2,
        )
        plan_report = {
            "symbol": "OPNUSDT",
            "strategy_mode": "one_way_long",
            "mid_price": 1.0001,
            "step_price": 0.0001,
            "inventory_tier": None,
            "synthetic_ledger": None,
        }

        report = execute_plan_report(args, plan_report)

        self.assertTrue(report["idle"])
        self.assertEqual(report["plan_snapshot"]["inventory_tier"], {})
        self.assertEqual(report["plan_snapshot"]["synthetic_ledger"], {})

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_skips_order_below_min_notional_after_post_only_adjustment(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 9.9, "price": 0.51},
                ],
            },
        }
        mock_book_tickers.return_value = [{"bid_price": "0.49", "ask_price": "0.51"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {"multiAssetsMargin": False, "positions": [{"symbol": "KATUSDT", "positionAmt": "0", "entryPrice": "0"}]}
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}

        args = Namespace(
            symbol="KATUSDT",
            strategy_mode="one_way_long",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/katusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=2,
            maker_retries=2,
            recv_window=5000,
            state_path="output/katusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "KATUSDT",
            "strategy_mode": "one_way_long",
            "mid_price": 0.50,
            "step_price": 0.01,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 0.0,
            "actual_net_qty": 0.0,
            "symbol_info": {
                "tick_size": 0.01,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertTrue(report["executed"])
        self.assertEqual(report["placed_orders"], [])
        self.assertEqual(len(report["skipped_orders"]), 1)
        self.assertEqual(
            report["skipped_orders"][0]["reason"]["reason"],
            "submitted_notional_below_min_notional",
        )
        mock_post_order.assert_not_called()
        mock_update_refs.assert_called_once()

    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    def test_assess_market_guard_flags_extreme_down_candle(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
        mock_fetch_futures_klines.return_value = [
            Candle(
                open_time=now - timedelta(minutes=2),
                close_time=now - timedelta(minutes=1, seconds=1),
                open=0.0500,
                high=0.0502,
                low=0.0497,
                close=0.0498,
            ),
            Candle(
                open_time=now - timedelta(minutes=1),
                close_time=now - timedelta(seconds=1),
                open=0.0500,
                high=0.05025,
                low=0.04970,
                close=0.04980,
            ),
        ]

        result = assess_market_guard(
            symbol="NIGHTUSDT",
            buy_pause_amp_trigger_ratio=0.0075,
            buy_pause_down_return_trigger_ratio=-0.0035,
            short_cover_pause_amp_trigger_ratio=None,
            short_cover_pause_down_return_trigger_ratio=None,
            freeze_shift_abs_return_trigger_ratio=0.0035,
            now=now,
        )

        self.assertTrue(result["enabled"])
        self.assertTrue(result["available"])
        self.assertTrue(result["buy_pause_active"])
        self.assertTrue(result["shift_frozen"])
        self.assertAlmostEqual(result["return_ratio"], -0.004)
        self.assertGreater(result["amplitude_ratio"], 0.01)

    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    def test_assess_market_guard_ignores_small_candle(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
        mock_fetch_futures_klines.return_value = [
            Candle(
                open_time=now - timedelta(minutes=1),
                close_time=now - timedelta(seconds=1),
                open=0.0500,
                high=0.0501,
                low=0.04995,
                close=0.05002,
            ),
        ]

        result = assess_market_guard(
            symbol="NIGHTUSDT",
            buy_pause_amp_trigger_ratio=0.0075,
            buy_pause_down_return_trigger_ratio=-0.0035,
            short_cover_pause_amp_trigger_ratio=None,
            short_cover_pause_down_return_trigger_ratio=None,
            freeze_shift_abs_return_trigger_ratio=0.005,
            now=now,
        )

        self.assertTrue(result["available"])
        self.assertFalse(result["buy_pause_active"])
        self.assertFalse(result["short_cover_pause_active"])
        self.assertFalse(result["shift_frozen"])

    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    def test_assess_market_guard_flags_short_cover_pause(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
        mock_fetch_futures_klines.return_value = [
            Candle(
                open_time=now - timedelta(minutes=1),
                close_time=now - timedelta(seconds=1),
                open=0.0500,
                high=0.05025,
                low=0.04970,
                close=0.04980,
            ),
        ]

        result = assess_market_guard(
            symbol="NIGHTUSDT",
            buy_pause_amp_trigger_ratio=None,
            buy_pause_down_return_trigger_ratio=None,
            short_cover_pause_amp_trigger_ratio=0.004,
            short_cover_pause_down_return_trigger_ratio=-0.0018,
            freeze_shift_abs_return_trigger_ratio=None,
            now=now,
        )

        self.assertTrue(result["enabled"])
        self.assertTrue(result["available"])
        self.assertFalse(result["buy_pause_active"])
        self.assertTrue(result["short_cover_pause_active"])
        self.assertFalse(result["shift_frozen"])

    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    def test_assess_auto_regime_prefers_defensive_when_recent_drop_and_amp_expand(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
        candles: list[Candle] = []
        base = now - timedelta(minutes=60)
        prices = [
            (1.00, 1.005, 0.998, 1.002),
            (1.002, 1.004, 0.996, 0.999),
            (0.999, 1.000, 0.992, 0.994),
            (0.994, 0.996, 0.987, 0.989),
            (0.989, 0.992, 0.983, 0.985),
            (0.985, 0.987, 0.977, 0.979),
            (0.979, 0.982, 0.972, 0.975),
            (0.975, 0.978, 0.969, 0.972),
            (0.972, 0.975, 0.968, 0.970),
            (0.970, 0.972, 0.965, 0.968),
            (0.968, 0.971, 0.963, 0.966),
            (0.966, 0.969, 0.960, 0.962),
        ]
        for index, (open_, high, low, close) in enumerate(prices):
            open_time = base + timedelta(minutes=5 * index)
            candles.append(
                Candle(
                    open_time=open_time,
                    close_time=open_time + timedelta(minutes=5),
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                )
            )
        mock_fetch_futures_klines.return_value = candles

        result = assess_auto_regime(
            symbol="OPNUSDT",
            stable_15m_max_amplitude_ratio=0.02,
            stable_60m_max_amplitude_ratio=0.05,
            stable_60m_return_floor_ratio=-0.01,
            defensive_15m_amplitude_ratio=0.035,
            defensive_60m_amplitude_ratio=0.08,
            defensive_15m_return_ratio=-0.015,
            defensive_60m_return_ratio=-0.03,
            now=now,
        )

        self.assertTrue(result["available"])
        self.assertEqual(result["regime"], "defensive")
        self.assertEqual(result["candidate_profile"], "volatility_defensive_v1")

    def test_resolve_auto_regime_profile_switches_after_confirm_cycles(self) -> None:
        state = {}
        first = resolve_auto_regime_profile(
            state=state,
            regime_report={"regime": "defensive", "candidate_profile": "volatility_defensive_v1", "reason": "drop"},
            confirm_cycles=2,
            now=datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(first["active_profile"], "volume_long_v4")
        self.assertEqual(first["pending_count"], 1)
        second = resolve_auto_regime_profile(
            state=state,
            regime_report={"regime": "defensive", "candidate_profile": "volatility_defensive_v1", "reason": "drop"},
            confirm_cycles=2,
            now=datetime(2026, 3, 18, 10, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(second["active_profile"], "volatility_defensive_v1")
        self.assertTrue(second["switched"])

    def test_build_effective_runner_args_applies_defensive_profile_and_retunes_step(self) -> None:
        args = Namespace(
            strategy_profile="adaptive_volatility_v1",
            strategy_mode="one_way_long",
            symbol="OPNUSDT",
            step_price=0.0002,
            buy_levels=8,
            sell_levels=8,
            per_order_notional=70.0,
            base_position_notional=420.0,
            up_trigger_steps=6,
            down_trigger_steps=4,
            shift_steps=4,
            pause_buy_position_notional=750.0,
            max_position_notional=900.0,
            buy_pause_amp_trigger_ratio=0.0075,
            buy_pause_down_return_trigger_ratio=-0.0035,
            freeze_shift_abs_return_trigger_ratio=0.005,
            inventory_tier_start_notional=600.0,
            inventory_tier_end_notional=750.0,
            inventory_tier_buy_levels=4,
            inventory_tier_sell_levels=12,
            inventory_tier_per_order_notional=70.0,
            inventory_tier_base_position_notional=280.0,
        )

        effective = build_effective_runner_args(
            args=args,
            active_profile="volatility_defensive_v1",
            symbol_info={"tick_size": 0.0001},
            bid_price=0.2668,
            ask_price=0.2670,
            mid_price=0.2669,
        )

        self.assertEqual(effective.buy_levels, 4)
        self.assertEqual(effective.sell_levels, 12)
        self.assertEqual(effective.max_position_notional, 420.0)
        self.assertGreaterEqual(effective.step_price, 0.0004)

    def test_resolve_market_bias_offsets_pushes_sell_side_closer_in_weak_market(self) -> None:
        report = resolve_market_bias_offsets(
            enabled=True,
            center_price=1.0,
            mid_price=0.99,
            step_price=0.01,
            market_guard_return_ratio=-0.008,
            max_shift_steps=0.75,
            signal_steps=2.0,
            drift_weight=0.65,
            return_weight=0.35,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["regime"], "weak")
        self.assertGreater(report["buy_offset_steps"], 0.0)
        self.assertLess(report["sell_offset_steps"], 0.0)

    def test_resolve_market_bias_offsets_pushes_buy_side_closer_in_strong_market(self) -> None:
        report = resolve_market_bias_offsets(
            enabled=True,
            center_price=1.0,
            mid_price=1.01,
            step_price=0.01,
            market_guard_return_ratio=0.008,
            max_shift_steps=0.75,
            signal_steps=2.0,
            drift_weight=0.65,
            return_weight=0.35,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["regime"], "strong")
        self.assertLess(report["buy_offset_steps"], 0.0)
        self.assertGreater(report["sell_offset_steps"], 0.0)

    def test_resolve_market_bias_entry_pause_activates_in_weak_market(self) -> None:
        report = resolve_market_bias_entry_pause(
            buy_pause_enabled=True,
            short_pause_enabled=True,
            market_bias={"regime": "weak", "bias_score": -0.23},
            weak_buy_pause_threshold=0.15,
            strong_short_pause_threshold=0.15,
        )

        self.assertTrue(report["buy_pause_active"])
        self.assertEqual(report["buy_pause_threshold"], 0.15)
        self.assertEqual(len(report["buy_pause_reasons"]), 1)
        self.assertFalse(report["short_pause_active"])

    def test_resolve_market_bias_entry_pause_stays_inactive_above_threshold(self) -> None:
        report = resolve_market_bias_entry_pause(
            buy_pause_enabled=True,
            short_pause_enabled=True,
            market_bias={"regime": "weak", "bias_score": -0.12},
            weak_buy_pause_threshold=0.15,
            strong_short_pause_threshold=0.15,
        )

        self.assertFalse(report["buy_pause_active"])
        self.assertEqual(report["buy_pause_reasons"], [])
        self.assertFalse(report["short_pause_active"])

    def test_resolve_market_bias_entry_pause_activates_short_pause_in_strong_market(self) -> None:
        report = resolve_market_bias_entry_pause(
            buy_pause_enabled=True,
            short_pause_enabled=True,
            market_bias={"regime": "strong", "bias_score": 0.27},
            weak_buy_pause_threshold=0.15,
            strong_short_pause_threshold=0.15,
        )

        self.assertFalse(report["buy_pause_active"])
        self.assertTrue(report["short_pause_active"])
        self.assertEqual(report["short_pause_threshold"], 0.15)
        self.assertEqual(len(report["short_pause_reasons"]), 1)

    def test_resolve_market_bias_entry_pause_keeps_buy_side_off_when_only_short_pause_enabled(self) -> None:
        report = resolve_market_bias_entry_pause(
            buy_pause_enabled=False,
            short_pause_enabled=True,
            market_bias={"regime": "weak", "bias_score": -0.30},
            weak_buy_pause_threshold=0.15,
            strong_short_pause_threshold=0.15,
        )

        self.assertFalse(report["buy_pause_active"])
        self.assertFalse(report["short_pause_active"])

    def test_resolve_market_bias_regime_switch_requires_confirm_cycles(self) -> None:
        state: dict[str, object] = {}
        first = resolve_market_bias_regime_switch(
            state=state,
            enabled=True,
            requested_strategy_mode="synthetic_neutral",
            market_bias={"regime": "weak", "bias_score": -0.28},
            weak_threshold=0.15,
            strong_threshold=0.15,
            confirm_cycles=2,
            now=datetime(2026, 4, 4, 4, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(first["active_mode"], "synthetic_neutral")
        self.assertEqual(first["candidate_mode"], "one_way_short")
        self.assertEqual(first["pending_count"], 1)

        second = resolve_market_bias_regime_switch(
            state=state,
            enabled=True,
            requested_strategy_mode="synthetic_neutral",
            market_bias={"regime": "weak", "bias_score": -0.31},
            weak_threshold=0.15,
            strong_threshold=0.15,
            confirm_cycles=2,
            now=datetime(2026, 4, 4, 4, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(second["active_mode"], "one_way_short")
        self.assertTrue(second["switched"])

        third = resolve_market_bias_regime_switch(
            state=state,
            enabled=True,
            requested_strategy_mode="synthetic_neutral",
            market_bias={"regime": "neutral", "bias_score": 0.02},
            weak_threshold=0.15,
            strong_threshold=0.15,
            confirm_cycles=2,
            now=datetime(2026, 4, 4, 4, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(third["active_mode"], "one_way_short")
        self.assertEqual(third["candidate_mode"], "synthetic_neutral")
        self.assertEqual(third["pending_count"], 1)

        fourth = resolve_market_bias_regime_switch(
            state=state,
            enabled=True,
            requested_strategy_mode="synthetic_neutral",
            market_bias={"regime": "neutral", "bias_score": 0.01},
            weak_threshold=0.15,
            strong_threshold=0.15,
            confirm_cycles=2,
            now=datetime(2026, 4, 4, 4, 15, tzinfo=timezone.utc),
        )
        self.assertEqual(fourth["active_mode"], "synthetic_neutral")
        self.assertTrue(fourth["switched"])

    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    def test_assess_xaut_adaptive_regime_prefers_reduce_only_for_long_extreme_drop(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
        mock_fetch_futures_klines.side_effect = [
            [
                Candle(
                    open_time=now - timedelta(minutes=15),
                    close_time=now - timedelta(seconds=1),
                    open=100.0,
                    high=100.2,
                    low=99.1,
                    close=99.2,
                )
            ],
            [
                Candle(
                    open_time=now - timedelta(hours=1),
                    close_time=now - timedelta(seconds=1),
                    open=100.0,
                    high=100.4,
                    low=98.4,
                    close=98.7,
                )
            ],
        ]

        result = assess_xaut_adaptive_regime(
            symbol="XAUTUSDT",
            strategy_mode="one_way_long",
            now=now,
        )

        self.assertTrue(result["available"])
        self.assertEqual(result["candidate_state"], "reduce_only")

    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    def test_assess_xaut_adaptive_regime_prefers_reduce_only_for_short_extreme_rally(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
        mock_fetch_futures_klines.side_effect = [
            [
                Candle(
                    open_time=now - timedelta(minutes=15),
                    close_time=now - timedelta(seconds=1),
                    open=100.0,
                    high=101.0,
                    low=99.8,
                    close=100.8,
                )
            ],
            [
                Candle(
                    open_time=now - timedelta(hours=1),
                    close_time=now - timedelta(seconds=1),
                    open=100.0,
                    high=101.7,
                    low=99.9,
                    close=101.3,
                )
            ],
        ]

        result = assess_xaut_adaptive_regime(
            symbol="XAUTUSDT",
            strategy_mode="one_way_short",
            now=now,
        )

        self.assertTrue(result["available"])
        self.assertEqual(result["candidate_state"], "reduce_only")

    @patch("grid_optimizer.loop_runner.fetch_futures_klines", autospec=True)
    def test_assess_xaut_adaptive_regime_fetches_recent_klines_with_time_bounds(self, mock_fetch_futures_klines) -> None:
        now = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
        mock_fetch_futures_klines.side_effect = [
            [
                Candle(
                    open_time=now - timedelta(minutes=15),
                    close_time=now - timedelta(seconds=1),
                    open=100.0,
                    high=100.5,
                    low=99.8,
                    close=100.1,
                )
            ],
            [
                Candle(
                    open_time=now - timedelta(hours=1),
                    close_time=now - timedelta(seconds=1),
                    open=100.0,
                    high=100.6,
                    low=99.7,
                    close=100.2,
                )
            ],
        ]

        result = assess_xaut_adaptive_regime(
            symbol="XAUTUSDT",
            strategy_mode="one_way_short",
            now=now,
        )

        self.assertTrue(result["available"])
        self.assertIsNone(result["warning"])
        self.assertEqual(len(mock_fetch_futures_klines.call_args_list), 2)

        first_call = mock_fetch_futures_klines.call_args_list[0].kwargs
        self.assertEqual(first_call["symbol"], "XAUTUSDT")
        self.assertEqual(first_call["interval"], "15m")
        self.assertEqual(first_call["limit"], 8)
        self.assertEqual(first_call["end_ms"], int(now.timestamp() * 1000))
        self.assertEqual(
            first_call["start_ms"],
            int((now - timedelta(minutes=15 * 8)).timestamp() * 1000),
        )

        second_call = mock_fetch_futures_klines.call_args_list[1].kwargs
        self.assertEqual(second_call["symbol"], "XAUTUSDT")
        self.assertEqual(second_call["interval"], "1h")
        self.assertEqual(second_call["limit"], 8)
        self.assertEqual(second_call["end_ms"], int(now.timestamp() * 1000))
        self.assertEqual(
            second_call["start_ms"],
            int((now - timedelta(hours=8)).timestamp() * 1000),
        )

    def test_resolve_xaut_adaptive_state_enters_reduce_only_immediately_from_normal(self) -> None:
        state: dict[str, object] = {}

        resolved = resolve_xaut_adaptive_state(
            state=state,
            regime_report={"candidate_state": "reduce_only", "reason": "extreme"},
            strategy_mode="one_way_long",
            now=datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(resolved["active_state"], "reduce_only")
        self.assertTrue(resolved["switched"])

    def test_resolve_xaut_adaptive_state_requires_reduce_only_to_pass_through_defensive(self) -> None:
        state: dict[str, object] = {
            "xaut_adaptive_state": {
                "active_state": "reduce_only",
                "pending_state": None,
                "pending_count": 0,
            }
        }

        resolved = resolve_xaut_adaptive_state(
            state=state,
            regime_report={"candidate_state": "normal", "reason": "stable"},
            strategy_mode="one_way_long",
            now=datetime(2026, 4, 1, 0, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(resolved["active_state"], "reduce_only")
        self.assertEqual(resolved["candidate_state"], "defensive")
        self.assertEqual(resolved["pending_state"], "defensive")
        self.assertEqual(resolved["pending_count"], 1)

    def test_apply_xaut_reduce_only_pruning_drops_long_opening_orders(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "BUY", "role": "bootstrap"}],
            "buy_orders": [{"side": "BUY", "role": "entry"}],
            "sell_orders": [{"side": "SELL", "role": "take_profit"}],
        }

        apply_xaut_reduce_only_pruning(plan=plan, strategy_mode="one_way_long")

        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)

    def test_apply_xaut_reduce_only_pruning_drops_short_opening_orders(self) -> None:
        plan = {
            "bootstrap_orders": [{"side": "SELL", "role": "bootstrap_short"}],
            "buy_orders": [{"side": "BUY", "role": "take_profit_short"}],
            "sell_orders": [{"side": "SELL", "role": "entry_short"}],
        }

        apply_xaut_reduce_only_pruning(plan=plan, strategy_mode="one_way_short")

        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(len(plan["buy_orders"]), 1)

    def test_build_xaut_adaptive_runner_args_applies_defensive_profile(self) -> None:
        args = Namespace(
            strategy_profile="xaut_long_adaptive_v1",
            strategy_mode="one_way_long",
            symbol="XAUTUSDT",
            step_price=7.5,
            buy_levels=6,
            sell_levels=10,
            per_order_notional=80.0,
            base_position_notional=320.0,
            up_trigger_steps=5,
            down_trigger_steps=4,
            shift_steps=3,
            pause_buy_position_notional=520.0,
            max_position_notional=680.0,
            buy_pause_amp_trigger_ratio=0.0060,
            buy_pause_down_return_trigger_ratio=-0.0045,
            freeze_shift_abs_return_trigger_ratio=0.0048,
            inventory_tier_start_notional=420.0,
            inventory_tier_end_notional=520.0,
            inventory_tier_buy_levels=3,
            inventory_tier_sell_levels=12,
            inventory_tier_per_order_notional=70.0,
            inventory_tier_base_position_notional=160.0,
        )

        effective = build_xaut_adaptive_runner_args(
            args=args,
            active_state="defensive",
        )

        self.assertEqual(effective.buy_levels, 2)
        self.assertEqual(effective.sell_levels, 14)
        self.assertEqual(effective.max_position_notional, 260.0)
        self.assertEqual(effective.step_price, 12.0)

    def test_build_xaut_adaptive_runner_args_scales_step_down_in_calm_regime(self) -> None:
        args = Namespace(
            strategy_profile="xaut_long_adaptive_v1",
            strategy_mode="one_way_long",
            symbol="XAUTUSDT",
            step_price=7.5,
            buy_levels=6,
            sell_levels=10,
            per_order_notional=80.0,
            base_position_notional=320.0,
            up_trigger_steps=5,
            down_trigger_steps=4,
            shift_steps=3,
            pause_buy_position_notional=520.0,
            max_position_notional=680.0,
            buy_pause_amp_trigger_ratio=0.0060,
            buy_pause_down_return_trigger_ratio=-0.0045,
            freeze_shift_abs_return_trigger_ratio=0.0048,
            inventory_tier_start_notional=420.0,
            inventory_tier_end_notional=520.0,
            inventory_tier_buy_levels=3,
            inventory_tier_sell_levels=12,
            inventory_tier_per_order_notional=70.0,
            inventory_tier_base_position_notional=160.0,
        )

        effective = build_xaut_adaptive_runner_args(
            args=args,
            active_state="normal",
            regime_metrics={
                "window_15m": {"amplitude_ratio": 0.00022},
                "window_60m": {"amplitude_ratio": 0.00086},
            },
            bid_price=4629.37,
            ask_price=4629.38,
            mid_price=4629.38,
            tick_size=0.01,
        )

        self.assertLess(effective.step_price, 7.5)
        self.assertAlmostEqual(effective.step_price, 2.86, places=2)

    def test_build_xaut_adaptive_runner_args_scales_step_up_gradually_before_reduce_only(self) -> None:
        args = Namespace(
            strategy_profile="xaut_long_adaptive_v1",
            strategy_mode="one_way_long",
            symbol="XAUTUSDT",
            step_price=7.5,
            buy_levels=6,
            sell_levels=10,
            per_order_notional=80.0,
            base_position_notional=320.0,
            up_trigger_steps=5,
            down_trigger_steps=4,
            shift_steps=3,
            pause_buy_position_notional=520.0,
            max_position_notional=680.0,
            buy_pause_amp_trigger_ratio=0.0060,
            buy_pause_down_return_trigger_ratio=-0.0045,
            freeze_shift_abs_return_trigger_ratio=0.0048,
            inventory_tier_start_notional=420.0,
            inventory_tier_end_notional=520.0,
            inventory_tier_buy_levels=3,
            inventory_tier_sell_levels=12,
            inventory_tier_per_order_notional=70.0,
            inventory_tier_base_position_notional=160.0,
        )

        effective = build_xaut_adaptive_runner_args(
            args=args,
            active_state="defensive",
            regime_metrics={
                "window_15m": {"amplitude_ratio": 0.0050},
                "window_60m": {"amplitude_ratio": 0.0100},
            },
            bid_price=4629.30,
            ask_price=4629.40,
            mid_price=4629.35,
            tick_size=0.01,
        )

        self.assertGreater(effective.step_price, 7.5)
        self.assertLess(effective.step_price, 12.0)
        self.assertAlmostEqual(effective.step_price, 8.83, places=2)

    def test_build_xaut_adaptive_runner_args_applies_short_normal_profile(self) -> None:
        args = Namespace(
            strategy_profile="xaut_short_adaptive_v1",
            strategy_mode="one_way_short",
            symbol="XAUTUSDT",
            step_price=7.4,
            buy_levels=10,
            sell_levels=6,
            per_order_notional=60.0,
            base_position_notional=220.0,
            up_trigger_steps=4,
            down_trigger_steps=5,
            shift_steps=3,
            pause_short_position_notional=620.0,
            max_short_position_notional=720.0,
            inventory_tier_start_notional=320.0,
            inventory_tier_end_notional=520.0,
            inventory_tier_buy_levels=14,
            inventory_tier_sell_levels=3,
            inventory_tier_per_order_notional=45.0,
            inventory_tier_base_position_notional=100.0,
            short_cover_pause_amp_trigger_ratio=0.0060,
            short_cover_pause_down_return_trigger_ratio=-0.0045,
        )

        effective = build_xaut_adaptive_runner_args(
            args=args,
            active_state="normal",
        )

        self.assertEqual(effective.step_price, 7.4)
        self.assertEqual(effective.per_order_notional, 60.0)
        self.assertEqual(effective.base_position_notional, 220.0)
        self.assertEqual(effective.pause_short_position_notional, 620.0)
        self.assertEqual(effective.max_short_position_notional, 720.0)
        self.assertEqual(effective.inventory_tier_start_notional, 320.0)
        self.assertEqual(effective.inventory_tier_end_notional, 520.0)
        self.assertEqual(effective.inventory_tier_buy_levels, 14)
        self.assertEqual(effective.inventory_tier_sell_levels, 3)
        self.assertEqual(effective.inventory_tier_per_order_notional, 45.0)
        self.assertEqual(effective.inventory_tier_base_position_notional, 100.0)

    def test_build_xaut_adaptive_runner_args_applies_short_defensive_profile_with_small_entry_budget(self) -> None:
        args = Namespace(
            strategy_profile="xaut_short_adaptive_v1",
            strategy_mode="one_way_short",
            symbol="XAUTUSDT",
            step_price=7.4,
            buy_levels=10,
            sell_levels=6,
            per_order_notional=60.0,
            base_position_notional=220.0,
            up_trigger_steps=4,
            down_trigger_steps=5,
            shift_steps=3,
            pause_short_position_notional=620.0,
            max_short_position_notional=720.0,
            inventory_tier_start_notional=320.0,
            inventory_tier_end_notional=520.0,
            inventory_tier_buy_levels=14,
            inventory_tier_sell_levels=3,
            inventory_tier_per_order_notional=45.0,
            inventory_tier_base_position_notional=100.0,
            short_cover_pause_amp_trigger_ratio=0.0060,
            short_cover_pause_down_return_trigger_ratio=-0.0045,
        )

        effective = build_xaut_adaptive_runner_args(
            args=args,
            active_state="defensive",
        )

        self.assertEqual(effective.step_price, 10.5)
        self.assertEqual(effective.buy_levels, 14)
        self.assertEqual(effective.sell_levels, 2)
        self.assertEqual(effective.per_order_notional, 35.0)
        self.assertEqual(effective.base_position_notional, 70.0)
        self.assertEqual(effective.pause_short_position_notional, 620.0)
        self.assertEqual(effective.max_short_position_notional, 720.0)
        self.assertEqual(effective.inventory_tier_start_notional, 180.0)
        self.assertEqual(effective.inventory_tier_end_notional, 300.0)
        self.assertEqual(effective.inventory_tier_buy_levels, 16)
        self.assertEqual(effective.inventory_tier_sell_levels, 2)
        self.assertEqual(effective.inventory_tier_per_order_notional, 30.0)
        self.assertEqual(effective.inventory_tier_base_position_notional, 50.0)

    def test_resolve_adaptive_step_price_widens_step_for_fast_move(self) -> None:
        state = {
            "adaptive_step_history": [
                {"ts": "2026-04-05T16:06:49+00:00", "mid_price": 0.06665},
                {"ts": "2026-04-05T16:06:56+00:00", "mid_price": 0.06685},
                {"ts": "2026-04-05T16:07:03+00:00", "mid_price": 0.06655},
                {"ts": "2026-04-05T16:07:10+00:00", "mid_price": 0.06635},
                {"ts": "2026-04-05T16:07:17+00:00", "mid_price": 0.06625},
            ]
        }

        result = resolve_adaptive_step_price(
            state=state,
            now=datetime(2026, 4, 5, 16, 7, 19, tzinfo=timezone.utc),
            mid_price=0.06625,
            base_step_price=0.0001,
            tick_size=0.00001,
            enabled=True,
            window_30s_abs_return_ratio=0.0028,
            window_30s_amplitude_ratio=0.0035,
            window_1m_abs_return_ratio=0.0045,
            window_1m_amplitude_ratio=0.0065,
            window_3m_abs_return_ratio=0.0100,
            window_5m_abs_return_ratio=0.0140,
            max_scale=3.0,
            base_per_order_notional=100.0,
            base_pause_buy_position_notional=2000.0,
            base_max_position_notional=2400.0,
            min_per_order_scale=0.35,
            min_position_limit_scale=0.45,
        )

        self.assertTrue(result["active"])
        self.assertTrue(result["controls_active"])
        self.assertEqual(result["dominant_window"], "window_30s")
        self.assertEqual(result["dominant_metric"], "amplitude_ratio")
        self.assertGreater(result["scale"], 2.0)
        self.assertAlmostEqual(result["effective_step_price"], 0.00026, places=8)
        self.assertAlmostEqual(result["effective_per_order_notional"], 38.64583333, places=6)
        self.assertAlmostEqual(result["effective_pause_buy_position_notional"], 900.0, places=6)
        self.assertAlmostEqual(result["effective_max_position_notional"], 1080.0, places=6)

    def test_resolve_adaptive_step_price_keeps_widened_step_during_sustained_trend(self) -> None:
        state = {
            "adaptive_step_history": [
                {"ts": "2026-04-05T16:00:00+00:00", "mid_price": 0.06000},
                {"ts": "2026-04-05T16:01:00+00:00", "mid_price": 0.06024},
                {"ts": "2026-04-05T16:02:00+00:00", "mid_price": 0.06045},
                {"ts": "2026-04-05T16:03:00+00:00", "mid_price": 0.06058},
                {"ts": "2026-04-05T16:04:00+00:00", "mid_price": 0.06078},
                {"ts": "2026-04-05T16:04:45+00:00", "mid_price": 0.06082},
            ]
        }

        result = resolve_adaptive_step_price(
            state=state,
            now=datetime(2026, 4, 5, 16, 4, 55, tzinfo=timezone.utc),
            mid_price=0.06084,
            base_step_price=0.0001,
            tick_size=0.00001,
            enabled=True,
            window_30s_abs_return_ratio=0.0028,
            window_30s_amplitude_ratio=0.0035,
            window_1m_abs_return_ratio=0.0045,
            window_1m_amplitude_ratio=0.0065,
            window_3m_abs_return_ratio=0.0100,
            window_5m_abs_return_ratio=0.0140,
            max_scale=3.0,
            base_per_order_notional=100.0,
            base_pause_buy_position_notional=2000.0,
            min_per_order_scale=0.35,
            min_position_limit_scale=0.45,
        )

        self.assertTrue(result["active"])
        self.assertTrue(result["controls_active"])
        self.assertIn(result["dominant_window"], {"window_3m", "window_5m"})
        self.assertEqual(result["dominant_metric"], "abs_return_ratio")
        self.assertGreater(result["effective_step_price"], 0.0001)
        self.assertLess(result["effective_per_order_notional"], 100.0)
        self.assertLess(result["effective_pause_buy_position_notional"], 2000.0)

    def test_resolve_adaptive_step_price_returns_to_base_when_market_calms(self) -> None:
        state = {
            "adaptive_step_history": [
                {"ts": "2026-04-05T16:20:00+00:00", "mid_price": 0.06620},
                {"ts": "2026-04-05T16:20:15+00:00", "mid_price": 0.06621},
                {"ts": "2026-04-05T16:20:30+00:00", "mid_price": 0.06622},
                {"ts": "2026-04-05T16:20:45+00:00", "mid_price": 0.06622},
            ]
        }

        result = resolve_adaptive_step_price(
            state=state,
            now=datetime(2026, 4, 5, 16, 20, 55, tzinfo=timezone.utc),
            mid_price=0.06621,
            base_step_price=0.0001,
            tick_size=0.00001,
            enabled=True,
            window_30s_abs_return_ratio=0.0028,
            window_30s_amplitude_ratio=0.0035,
            window_1m_abs_return_ratio=0.0045,
            window_1m_amplitude_ratio=0.0065,
            window_3m_abs_return_ratio=0.0100,
            window_5m_abs_return_ratio=0.0140,
            max_scale=3.0,
            base_per_order_notional=100.0,
            base_pause_buy_position_notional=2000.0,
            min_per_order_scale=0.35,
            min_position_limit_scale=0.45,
        )

        self.assertFalse(result["active"])
        self.assertFalse(result["controls_active"])
        self.assertAlmostEqual(result["scale"], 1.0, places=8)
        self.assertAlmostEqual(result["effective_step_price"], 0.0001, places=8)
        self.assertAlmostEqual(result["effective_per_order_notional"], 100.0, places=8)
        self.assertAlmostEqual(result["effective_pause_buy_position_notional"], 2000.0, places=8)


if __name__ == "__main__":
    unittest.main()
