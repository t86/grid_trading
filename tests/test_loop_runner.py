from __future__ import annotations

from argparse import Namespace
import json
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from grid_optimizer.audit import build_audit_paths
from grid_optimizer.best_quote_maker_volume import (
    BestQuoteMakerVolumeConfig,
    BestQuoteMakerVolumeInputs,
    build_best_quote_maker_volume_plan,
)
from grid_optimizer.inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime
from grid_optimizer.loop_runner import (
    AUTO_REGIME_PROFILE_LABELS,
    AUDIT_SYNC_MIN_INTERVAL_SECONDS,
    _build_parser,
    _validate_multi_timeframe_bias_args,
    _should_sync_account_audit,
    _uses_entry_price_cost_basis,
    _uses_synthetic_lot_cost_guard,
    _uses_volume_long_v4_staged_delever,
    _apply_synthetic_trade_fill,
    _decorate_synthetic_open_orders,
    _load_synthetic_ledger,
    _current_check_bucket,
    _custom_grid_levels_above_current,
    _generate_competition_inventory_grid_plan,
    _filter_futures_strategy_orders,
    _elastic_volume_config,
    _elastic_volume_state_snapshot,
    _regime_budget_entry_reuse_tolerance,
    _wait_for_runner_market_stream_snapshot,
    _is_binance_rate_limit_error,
    _infer_synthetic_order_ref_from_trade,
    _read_custom_grid_trade_count,
    _resolve_custom_grid_roll,
    _resolve_best_quote_dynamic_offsets,
    _resolve_synthetic_resync_price,
    _resolve_hard_loss_reduce_target_notional,
    _best_quote_reduce_freeze_report,
    _apply_best_quote_reduce_freeze,
    _cap_best_quote_reduce_orders_to_managed_inventory,
    apply_best_quote_frozen_inventory_manual_reduce,
    apply_best_quote_frozen_inventory_pair_release,
    _position_unrealized_or_estimate,
    _is_long_exit_order,
    _is_short_exit_order,
    _shift_custom_grid_bounds,
    _run_periodic_reconcile,
    _cap_best_quote_profitable_inventory_exit_offset,
    _separate_paired_best_quote_reduce_orders,
    prioritize_inventory_reducing_place_orders,
    apply_execution_request_budget_to_actions,
    StartupProtectionError,
    _update_inventory_grid_order_refs,
    apply_excess_inventory_reduce_only,
    apply_active_delever_short,
    apply_active_delever_long,
    apply_volume_long_v4_staged_delever,
    apply_volume_long_v4_flow_sleeve,
    apply_hard_loss_forced_reduce,
    apply_inventory_unlock_release,
    _resolve_inventory_unlock_pause_notional,
    _is_best_quote_maker_volume_mode,
    resolve_loss_recovery_brush,
    resolve_hard_loss_forced_reduce_episode,
    prime_exposure_escalation_on_market_guard,
    resolve_exposure_escalation_buy_pause,
    resolve_exposure_escalation,
    apply_synthetic_inventory_exit_priority,
    apply_adverse_inventory_reduce,
    apply_hedge_position_controls,
    apply_hedge_position_notional_caps,
    apply_inventory_tiering,
    apply_max_position_notional_cap,
    apply_position_controls,
    apply_take_profit_profit_guard,
    apply_synthetic_flow_sleeve,
    apply_short_cover_pause,
    apply_warm_start_bootstrap_guard,
    assess_synthetic_tp_only_watchdog,
    assess_adverse_inventory_reduce,
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
    resolve_volatility_entry_pause,
    apply_volatility_entry_pause_controls,
    resolve_interval_locked_center_price,
    resolve_inventory_pause_timeout_state,
    resolve_short_threshold_timeout_state,
    resolve_synthetic_trend_follow,
    resolve_auto_regime_profile,
    resolve_xaut_adaptive_state,
    resolve_anti_chase_entry_guard,
    resolve_loss_reduce_reentry_guard,
    apply_synthetic_trend_follow_guard,
    apply_entry_permission_gate,
    assess_unrealized_loss_entry_guard,
    remove_take_profit_exit_orders,
    sync_synthetic_ledger,
    update_synthetic_order_refs,
)
from grid_optimizer.submit_plan import (
    apply_loss_inventory_no_cross_entry_guard_to_actions,
    apply_loss_reduce_reentry_guard_to_actions,
)
from grid_optimizer.semi_auto_plan import (
    build_hedge_micro_grid_plan,
    build_maker_volatility_inventory_plan,
    build_static_binance_grid_plan,
    load_or_initialize_state,
)
from grid_optimizer.types import Candle


class LoopRunnerTests(unittest.TestCase):
    def test_reset_state_preserves_best_quote_frozen_inventory(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            frozen_ledger = {
                "long_qty": 352.0,
                "long_entry_price": 0.6442504916363,
                "long_frozen_at": "2026-05-22T05:30:52.196376+00:00",
                "short_qty": 77.0,
                "short_entry_price": 0.6421485022341,
                "short_frozen_at": "2026-05-22T05:47:48.487386+00:00",
                "updated_at": "2026-05-22T05:51:23.848287+00:00",
            }
            manual_reduce = {
                "long": {
                    "requested": True,
                    "requested_at": "2026-05-22T05:55:00+00:00",
                    "source": "running_status_ui",
                }
            }
            pair_release = {
                "requested": True,
                "requested_at": "2026-05-22T06:05:00+00:00",
                "source": "running_status_ui",
            }
            state_path.write_text(
                json.dumps(
                    {
                        "version": "old",
                        "best_quote_frozen_inventory": frozen_ledger,
                        "best_quote_frozen_inventory_manual_reduce": manual_reduce,
                        "best_quote_frozen_inventory_pair_release": pair_release,
                        "runtime_guard_loss_recovery": {"cooldown": True},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            state = load_or_initialize_state(
                state_path=state_path,
                args=Namespace(
                    symbol="PHAROSUSDT",
                    strategy_mode="hedge_best_quote_maker_volume_v1",
                    step_price=0.00025,
                    buy_levels=1,
                    sell_levels=1,
                    per_order_notional=10.0,
                    base_position_notional=0.0,
                    center_price=None,
                    down_trigger_steps=10,
                    up_trigger_steps=10,
                    shift_steps=1,
                ),
                symbol_info={
                    "tick_size": 0.0001,
                    "step_size": 1.0,
                    "min_qty": 1.0,
                    "min_notional": 5.0,
                },
                mid_price=0.65,
                reset_state=True,
            )

        self.assertEqual(state["best_quote_frozen_inventory"], frozen_ledger)
        self.assertEqual(state["best_quote_frozen_inventory_manual_reduce"], manual_reduce)
        self.assertEqual(state["best_quote_frozen_inventory_pair_release"], pair_release)
        self.assertNotIn("runtime_guard_loss_recovery", state)
        self.assertTrue(state["startup_pending"])

    def test_resolve_volatility_entry_pause_holds_for_min_observation_window(self) -> None:
        trigger_now = datetime(2026, 5, 16, 4, 7, tzinfo=timezone.utc)
        triggered = resolve_volatility_entry_pause(
            adaptive_step={"metrics": {"window_10s": {"return_ratio": -0.005, "amplitude_ratio": 0.005}}},
            state={},
            enabled=True,
            window_10s_abs_return_ratio=0.004,
            window_10s_amplitude_ratio=0.006,
            window_30s_abs_return_ratio=0.0,
            window_30s_amplitude_ratio=0.0,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.0,
            recover_confirm_cycles=2,
            now=trigger_now,
            min_observation_seconds=180.0,
            current_long_notional=700.0,
            current_short_notional=0.0,
            inventory_recover_ratio=0.75,
        )
        recovering = resolve_volatility_entry_pause(
            adaptive_step={"metrics": {}},
            state={"volatility_entry_pause_state": triggered["state"]},
            enabled=True,
            window_10s_abs_return_ratio=0.004,
            window_10s_amplitude_ratio=0.006,
            window_30s_abs_return_ratio=0.0,
            window_30s_amplitude_ratio=0.0,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.0,
            recover_confirm_cycles=2,
            now=trigger_now + timedelta(seconds=30),
            min_observation_seconds=180.0,
            current_long_notional=500.0,
            current_short_notional=0.0,
            inventory_recover_ratio=0.75,
        )

        self.assertTrue(recovering["active"])
        self.assertTrue(recovering["recovering"])
        self.assertGreater(recovering["observation_remaining_seconds"], 0.0)
        self.assertIn("observing", recovering["reason"])

    def test_resolve_volatility_entry_pause_holds_until_inventory_recovers(self) -> None:
        trigger_now = datetime(2026, 5, 16, 4, 7, tzinfo=timezone.utc)
        triggered = resolve_volatility_entry_pause(
            adaptive_step={"metrics": {"window_30s": {"return_ratio": -0.008, "amplitude_ratio": 0.010}}},
            state={},
            enabled=True,
            window_10s_abs_return_ratio=0.0,
            window_10s_amplitude_ratio=0.0,
            window_30s_abs_return_ratio=0.007,
            window_30s_amplitude_ratio=0.012,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.0,
            recover_confirm_cycles=2,
            now=trigger_now,
            min_observation_seconds=180.0,
            current_long_notional=800.0,
            current_short_notional=0.0,
            inventory_recover_ratio=0.75,
        )
        still_blocked = resolve_volatility_entry_pause(
            adaptive_step={"metrics": {}},
            state={"volatility_entry_pause_state": triggered["state"]},
            enabled=True,
            window_10s_abs_return_ratio=0.0,
            window_10s_amplitude_ratio=0.0,
            window_30s_abs_return_ratio=0.007,
            window_30s_amplitude_ratio=0.012,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.0,
            recover_confirm_cycles=2,
            now=trigger_now + timedelta(seconds=240),
            min_observation_seconds=180.0,
            current_long_notional=650.0,
            current_short_notional=0.0,
            inventory_recover_ratio=0.75,
        )

        self.assertTrue(still_blocked["active"])
        self.assertTrue(still_blocked["inventory_gate_active"])
        self.assertIn("inventory", still_blocked["reason"])

    def test_resolve_volatility_entry_pause_ignores_tiny_inventory_recovery_gate(self) -> None:
        trigger_now = datetime(2026, 5, 20, 1, 30, tzinfo=timezone.utc)
        triggered = resolve_volatility_entry_pause(
            adaptive_step={"metrics": {"window_10s": {"return_ratio": -0.0049, "amplitude_ratio": 0.0049}}},
            state={},
            enabled=True,
            window_10s_abs_return_ratio=0.004,
            window_10s_amplitude_ratio=0.006,
            window_30s_abs_return_ratio=0.0,
            window_30s_amplitude_ratio=0.0,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.0,
            recover_confirm_cycles=1,
            now=trigger_now,
            min_observation_seconds=0.0,
            current_long_notional=8.0,
            current_short_notional=0.0,
            inventory_recover_ratio=0.75,
        )
        recovered = resolve_volatility_entry_pause(
            adaptive_step={"metrics": {}},
            state={"volatility_entry_pause_state": triggered["state"]},
            enabled=True,
            window_10s_abs_return_ratio=0.004,
            window_10s_amplitude_ratio=0.006,
            window_30s_abs_return_ratio=0.0,
            window_30s_amplitude_ratio=0.0,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.0,
            recover_confirm_cycles=1,
            now=trigger_now + timedelta(seconds=5),
            min_observation_seconds=0.0,
            current_long_notional=6.2,
            current_short_notional=0.0,
            inventory_recover_ratio=0.75,
            tiny_inventory_ignore_notional=10.0,
        )

        self.assertFalse(recovered["active"])
        self.assertFalse(recovered["inventory_gate_active"])
        self.assertTrue(recovered["tiny_inventory_ignored"])

    def test_apply_volatility_entry_pause_allows_loss_recovery_brush_reduce_side(self) -> None:
        controls = {
            "buy_paused": False,
            "short_paused": False,
            "pause_reasons": [],
            "short_pause_reasons": [],
        }
        volatility_entry_pause = {"active": True, "reason": "fast move"}

        apply_volatility_entry_pause_controls(
            controls=controls,
            volatility_entry_pause=volatility_entry_pause,
            loss_recovery_brush={"active": True, "side": "short"},
            elastic_volume={},
        )

        self.assertFalse(controls["buy_paused"])
        self.assertTrue(controls["short_paused"])
        self.assertEqual(controls["pause_reasons"], [])
        self.assertEqual(controls["short_pause_reasons"], ["volatility_entry_pause: fast move"])
        self.assertEqual(volatility_entry_pause["loss_recovery_brush_bypass_side"], "BUY")

    def test_volatility_entry_pause_classifies_best_quote_reduces_as_exits(self) -> None:
        self.assertTrue(_is_long_exit_order({"role": "best_quote_reduce_long"}))
        self.assertTrue(_is_short_exit_order({"role": "best_quote_reduce_short"}))

    def test_remove_take_profit_exit_orders_keeps_active_delever(self) -> None:
        plan = {
            "buy_orders": [
                {"side": "BUY", "price": 0.99, "qty": 10, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.98, "qty": 10, "role": "active_delever_short"},
                {"side": "BUY", "price": 0.97, "qty": 10, "role": "entry_long"},
            ],
            "sell_orders": [
                {"side": "SELL", "price": 1.01, "qty": 10, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.02, "qty": 10, "role": "active_delever_long"},
                {"side": "SELL", "price": 1.03, "qty": 10, "role": "entry_short"},
            ],
        }

        report = remove_take_profit_exit_orders(plan)

        self.assertEqual(report, {"dropped_sell_orders": 1, "dropped_buy_orders": 1})
        self.assertEqual([item["role"] for item in plan["buy_orders"]], ["active_delever_short", "entry_long"])
        self.assertEqual([item["role"] for item in plan["sell_orders"]], ["active_delever_long", "entry_short"])

    def test_chip_profile_uses_synthetic_lot_cost_guard(self) -> None:
        self.assertTrue(_uses_synthetic_lot_cost_guard("chip_low_wear_guarded_v1"))

    def _maker_plan(self, **overrides):
        params = {
            "bid_price": 100.0,
            "ask_price": 100.2,
            "current_net_qty": 0.0,
            "window_open": 100.0,
            "window_high": 100.25,
            "window_low": 99.95,
            "window_close": 100.1,
            "base_spread_bps": 4.0,
            "wide_spread_bps": 12.0,
            "order_notional": 30.0,
            "max_long_notional": 300.0,
            "max_short_notional": 300.0,
            "inventory_soft_ratio": 0.7,
            "volatility_wide_threshold": 0.006,
            "extreme_volatility_threshold": 0.012,
            "directional_move_threshold": 0.004,
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        params.update(overrides)
        return build_maker_volatility_inventory_plan(**params)

    def test_maker_volatility_inventory_normal_generates_two_sided_orders(self) -> None:
        plan = self._maker_plan()

        self.assertEqual(plan["maker_state"]["regime"], "normal")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["buy_orders"][0]["role"], "maker_entry_long")
        self.assertEqual(plan["sell_orders"][0]["role"], "maker_entry_short")
        self.assertLess(plan["buy_orders"][0]["price"], 100.0)
        self.assertGreater(plan["sell_orders"][0]["price"], 100.2)

    def test_maker_volatility_inventory_high_volatility_widens_quotes(self) -> None:
        normal = self._maker_plan()
        wide = self._maker_plan(window_high=100.7, window_low=99.8, window_close=100.4)

        self.assertEqual(wide["maker_state"]["regime"], "volatility_wide")
        self.assertLess(wide["buy_orders"][0]["price"], normal["buy_orders"][0]["price"])
        self.assertGreater(wide["sell_orders"][0]["price"], normal["sell_orders"][0]["price"])

    def test_maker_volatility_inventory_extreme_volatility_enters_cooldown(self) -> None:
        plan = self._maker_plan(window_high=102.0, window_low=99.5, window_close=101.5)

        self.assertEqual(plan["maker_state"]["regime"], "cooldown")
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(plan["maker_state"]["blocked_reason"], "extreme_volatility")

    def test_maker_volatility_inventory_soft_long_inventory_suppresses_buy(self) -> None:
        plan = self._maker_plan(current_net_qty=2.2)

        self.assertEqual(plan["maker_state"]["regime"], "long_inventory_reduce")
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "maker_reduce_long")

    def test_maker_volatility_inventory_soft_short_inventory_suppresses_sell(self) -> None:
        plan = self._maker_plan(current_net_qty=-2.2)

        self.assertEqual(plan["maker_state"]["regime"], "short_inventory_reduce")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(plan["buy_orders"][0]["role"], "maker_reduce_short")

    def test_maker_volatility_inventory_hard_long_inventory_reduce_only(self) -> None:
        plan = self._maker_plan(current_net_qty=3.1)

        self.assertEqual(plan["maker_state"]["regime"], "hard_reduce_only")
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "maker_reduce_long")

    def test_maker_volatility_inventory_hard_short_inventory_reduce_only(self) -> None:
        plan = self._maker_plan(current_net_qty=-3.1)

        self.assertEqual(plan["maker_state"]["regime"], "hard_reduce_only")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(plan["buy_orders"][0]["role"], "maker_reduce_short")

    def test_maker_volatility_inventory_fast_selloff_with_long_inventory_reduce_only(self) -> None:
        plan = self._maker_plan(current_net_qty=2.2, window_high=100.4, window_low=99.5, window_close=99.5)

        self.assertEqual(plan["maker_state"]["regime"], "long_inventory_reduce")
        self.assertIn("fast_down_move", plan["maker_state"]["reasons"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"][0]["role"], "maker_reduce_long")

    def test_maker_volatility_inventory_fast_rally_with_short_inventory_reduce_only(self) -> None:
        plan = self._maker_plan(current_net_qty=-2.2, window_high=100.7, window_low=99.8, window_close=100.5)

        self.assertEqual(plan["maker_state"]["regime"], "short_inventory_reduce")
        self.assertIn("fast_up_move", plan["maker_state"]["reasons"])
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(plan["buy_orders"][0]["role"], "maker_reduce_short")

    def test_maker_volatility_inventory_invalid_market_data_blocks_orders(self) -> None:
        plan = self._maker_plan(bid_price=0.0)

        self.assertEqual(plan["maker_state"]["regime"], "blocked")
        self.assertEqual(plan["maker_state"]["blocked_reason"], "invalid_market_data")
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [])

    def test_best_quote_dynamic_offsets_widen_in_low_volatility(self) -> None:
        result = _resolve_best_quote_dynamic_offsets(
            adaptive_step={"enabled": True, "raw_scale": 0.4, "dominant_window": "window_1m"},
            quote_offset_ticks=3,
            defensive_offset_ticks=6,
        )

        self.assertTrue(result["dynamic_quote_offset_applied"])
        self.assertEqual(result["quote_offset_ticks"], 8)
        self.assertEqual(result["defensive_offset_ticks"], 6)
        self.assertEqual(result["configured_quote_offset_ticks"], 3)

    def test_best_quote_dynamic_offsets_keep_configured_offset_when_volatility_is_high(self) -> None:
        result = _resolve_best_quote_dynamic_offsets(
            adaptive_step={"enabled": True, "raw_scale": 1.3, "active": True},
            quote_offset_ticks=3,
            defensive_offset_ticks=6,
        )

        self.assertFalse(result["dynamic_quote_offset_applied"])
        self.assertEqual(result["quote_offset_ticks"], 3)
        self.assertEqual(result["defensive_offset_ticks"], 6)

    def test_best_quote_profitable_long_exit_offset_cap_uses_configured_distance(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {
                    "side": "SELL",
                    "price": 0.14626,
                    "qty": 888.0,
                    "notional": 129.87888,
                    "role": "best_quote_entry_short",
                }
            ],
        }

        report = _cap_best_quote_profitable_inventory_exit_offset(
            plan=plan,
            current_long_qty=5430.0,
            current_short_qty=0.0,
            current_long_avg_price=0.1436006879299,
            current_short_avg_price=0.0,
            bid_price=0.14530,
            ask_price=0.14532,
            tick_size=0.00001,
            configured_quote_offset_ticks=20,
            min_profit_ratio=0.0003,
        )

        self.assertEqual(report["adjusted_sell_orders"], 1)
        self.assertEqual(report["reason"], "profitable_inventory_exit_uses_configured_quote_offset")
        self.assertEqual(plan["sell_orders"][0]["price"], 0.14552)
        self.assertGreater(plan["sell_orders"][0]["price"], 0.14365)
        self.assertEqual(plan["sell_orders"][0]["profitable_inventory_exit_offset_cap"]["max_price"], 0.14552)

    def test_best_quote_profitable_short_exit_cap_skips_paired_reduce(self) -> None:
        plan = {
            "buy_orders": [
                {
                    "side": "BUY",
                    "price": 0.6012,
                    "qty": 13.0,
                    "notional": 7.8156,
                    "role": "best_quote_reduce_short",
                    "paired_entry_reduce": True,
                }
            ],
            "sell_orders": [],
        }

        report = _cap_best_quote_profitable_inventory_exit_offset(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=900.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.6017,
            bid_price=0.6027,
            ask_price=0.6028,
            tick_size=0.0001,
            configured_quote_offset_ticks=3,
            min_profit_ratio=0.00008,
        )

        self.assertEqual(report["adjusted_buy_orders"], 0)
        self.assertEqual(plan["buy_orders"][0]["price"], 0.6012)
        self.assertNotIn("profitable_inventory_exit_offset_cap", plan["buy_orders"][0])

    def test_paired_best_quote_reduce_is_separated_from_main_reduce_bucket(self) -> None:
        plan = {
            "buy_orders": [
                {
                    "side": "BUY",
                    "price": 0.6045,
                    "qty": 39.0,
                    "notional": 23.5755,
                    "role": "best_quote_reduce_short",
                    "position_side": "SHORT",
                },
                {
                    "side": "BUY",
                    "price": 0.6045,
                    "qty": 9.0,
                    "notional": 5.4405,
                    "role": "best_quote_reduce_short",
                    "position_side": "SHORT",
                    "paired_entry_reduce": True,
                },
            ],
            "sell_orders": [],
        }

        report = _separate_paired_best_quote_reduce_orders(plan=plan, tick_size=0.0001)

        self.assertEqual(report["adjusted_buy_orders"], 1)
        self.assertEqual(plan["buy_orders"][0]["price"], 0.6045)
        self.assertEqual(plan["buy_orders"][1]["price"], 0.6044)
        self.assertTrue(plan["buy_orders"][1]["paired_reduce_price_separated"])
        self.assertAlmostEqual(plan["buy_orders"][1]["notional"], 9.0 * 0.6044)

    @patch("grid_optimizer.loop_runner.resolve_adaptive_step_price")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_reports_dynamic_low_volatility_widen_offset(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_adaptive_step,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0", "entryPrice": "0"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }
        mock_adaptive_step.return_value = {
            "enabled": True,
            "active": False,
            "controls_active": False,
            "base_step_price": 1.0,
            "effective_step_price": 1.0,
            "scale": 1.0,
            "raw_scale": 0.4,
            "metrics": {},
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_quote_offset_ticks=3,
                best_quote_maker_volume_defensive_offset_ticks=6,
                best_quote_maker_volume_max_long_notional=1_500.0,
                best_quote_maker_volume_max_short_notional=1_500.0,
                reset_state=True,
            )

            report = generate_plan_report(args)

        dynamic_offsets = report["best_quote_maker_volume"]["dynamic_offsets"]
        self.assertTrue(dynamic_offsets["dynamic_quote_offset_applied"])
        self.assertEqual(dynamic_offsets["configured_quote_offset_ticks"], 3)
        self.assertEqual(dynamic_offsets["quote_offset_ticks"], 8)
        self.assertEqual(report["buy_orders"][0]["price"], 80399.2)
        self.assertEqual(report["sell_orders"][0]["price"], 80400.9)

    def test_best_quote_maker_volume_quotes_best_bid_ask(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True),
            inputs=BestQuoteMakerVolumeInputs(
                bid_price=80400.0,
                ask_price=80400.1,
                mid_price=80400.05,
                current_net_qty=0.0,
                cycle_budget_notional=400.0,
                loss_per_10k_15m=0.2,
                target_volume_remaining=10_000.0,
                tick_size=0.1,
                step_size=0.001,
                min_qty=0.001,
                min_notional=5.0,
            ),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertEqual(plan["buy_orders"][0]["price"], 80400.0)
        self.assertEqual(plan["sell_orders"][0]["price"], 80400.1)
        self.assertTrue(plan["buy_orders"][0]["post_only"])

    def test_best_quote_maker_volume_net_loss_reduce_blocks_entries(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                net_loss_reduce_enabled=True,
                net_loss_reduce_min_loss=2.0,
                net_loss_reduce_ratio=0.005,
            ),
            inputs=BestQuoteMakerVolumeInputs(
                bid_price=100.0,
                ask_price=100.1,
                mid_price=100.05,
                current_net_qty=0.0,
                current_long_qty=1.0,
                current_short_qty=1.0,
                position_side_mode="hedge",
                cycle_budget_notional=40.0,
                loss_per_10k_15m=0.2,
                target_volume_remaining=10_000.0,
                unrealized_pnl=-3.5,
                recent_realized_pnl=0.0,
                tick_size=0.1,
                step_size=0.001,
                min_qty=0.001,
                min_notional=5.0,
            ),
        )

        guard = plan["metrics"]["net_loss_reduce"]
        self.assertTrue(guard["active"])
        self.assertIn("net_loss_reduce", plan["reasons"])
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_reduce_long")
        self.assertTrue(plan["buy_orders"][0]["force_reduce_only"])
        self.assertTrue(plan["sell_orders"][0]["force_reduce_only"])

    def test_best_quote_reduce_freeze_marks_losing_reduce_inventory_unmanaged(self) -> None:
        state: dict[str, object] = {}
        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=100.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_short_avg_price=0.0,
            mid_price=0.989,
        )

        report = _apply_best_quote_reduce_freeze(
            state=state,
            plan={"sell_orders": [{"role": "best_quote_reduce_long"}], "buy_orders": []},
            report=report,
            enabled=True,
            threshold_loss_ratio=0.01,
            min_notional=10.0,
            current_long_qty=100.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_short_avg_price=0.0,
            mid_price=0.989,
        )

        self.assertTrue(report["applied"])
        self.assertEqual(report["frozen_long_qty"], 100.0)
        self.assertEqual(report["managed_long_qty"], 0.0)
        self.assertEqual(report["offset_qty"], 0.0)
        ledger = state["best_quote_frozen_inventory"]
        self.assertEqual(ledger["long_qty"], 100.0)

    def test_best_quote_reduce_freeze_can_trigger_below_soft_inventory(self) -> None:
        state: dict[str, object] = {}
        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=0.0,
            current_short_qty=300.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.6600,
            mid_price=0.6680,
        )

        report = _apply_best_quote_reduce_freeze(
            state=state,
            plan={"sell_orders": [], "buy_orders": [{"role": "best_quote_reduce_short"}]},
            report=report,
            enabled=True,
            threshold_loss_ratio=0.01,
            min_notional=10.0,
            current_long_qty=0.0,
            current_short_qty=300.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.6600,
            mid_price=0.6680,
        )

        self.assertTrue(report["applied"])
        self.assertEqual(report["frozen_short_qty"], 300.0)
        self.assertEqual(report["managed_short_qty"], 0.0)
        ledger = state["best_quote_frozen_inventory"]
        self.assertEqual(ledger["short_qty"], 300.0)

    def test_best_quote_reduce_freeze_can_add_opposite_side_lot(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {
                "long_qty": 352.0,
                "long_entry_price": 0.6442,
                "long_frozen_at": "2026-05-22T05:30:52+00:00",
            }
        }
        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=352.0,
            current_short_qty=110.0,
            current_long_avg_price=0.6442,
            current_short_avg_price=0.6449,
            mid_price=0.6514,
        )

        report = _apply_best_quote_reduce_freeze(
            state=state,
            plan={"sell_orders": [], "buy_orders": [{"role": "best_quote_reduce_short"}]},
            report=report,
            enabled=True,
            threshold_loss_ratio=0.01,
            min_notional=10.0,
            current_long_qty=352.0,
            current_short_qty=110.0,
            current_long_avg_price=0.6442,
            current_short_avg_price=0.6449,
            mid_price=0.6514,
        )

        ledger = state["best_quote_frozen_inventory"]
        self.assertEqual(ledger["long_qty"], 352.0)
        self.assertEqual(ledger["short_qty"], 110.0)
        self.assertEqual(len(ledger["short_lots"]), 1)
        self.assertEqual(ledger["short_lots"][0]["qty"], 110.0)
        self.assertTrue(report["applied"])

    def test_best_quote_reduce_freeze_appends_new_lot_for_unfrozen_managed_qty(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {
                "long_lots": [
                    {
                        "qty": 100.0,
                        "entry_price": 1.0,
                        "frozen_at": "2026-05-22T05:30:52+00:00",
                    }
                ],
            }
        }
        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=150.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_short_avg_price=0.0,
            mid_price=0.989,
        )

        report = _apply_best_quote_reduce_freeze(
            state=state,
            plan={"sell_orders": [{"role": "best_quote_reduce_long"}], "buy_orders": []},
            report=report,
            enabled=True,
            threshold_loss_ratio=0.01,
            min_notional=10.0,
            current_long_qty=150.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_short_avg_price=0.0,
            mid_price=0.989,
        )

        ledger = state["best_quote_frozen_inventory"]
        self.assertTrue(report["applied"])
        self.assertEqual(ledger["long_qty"], 150.0)
        self.assertEqual(len(ledger["long_lots"]), 2)
        self.assertEqual(ledger["long_lots"][0]["qty"], 100.0)
        self.assertEqual(ledger["long_lots"][1]["qty"], 50.0)
        self.assertEqual(report["managed_long_qty"], 0.0)

    def test_best_quote_reduce_freeze_reports_frozen_side_offset(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {
                "long_qty": 120.0,
                "short_qty": 80.0,
            }
        }

        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=120.0,
            current_short_qty=100.0,
            current_long_avg_price=1.0,
            current_short_avg_price=1.0,
            mid_price=0.5,
        )

        self.assertEqual(report["offset_qty"], 80.0)
        self.assertEqual(report["offset_notional"], 40.0)
        self.assertEqual(report["managed_long_qty"], 0.0)
        self.assertEqual(report["managed_short_qty"], 20.0)

    def test_best_quote_reduce_freeze_isolates_frozen_unrealized_loss(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {
                "long_qty": 60.0,
                "long_entry_price": 1.0,
            }
        }

        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=100.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_short_avg_price=0.0,
            mid_price=0.98,
        )

        self.assertAlmostEqual(report["actual_unrealized_pnl"], -2.0)
        self.assertAlmostEqual(report["frozen_unrealized_pnl"], -1.2)
        self.assertAlmostEqual(report["managed_unrealized_pnl"], -0.8)
        self.assertAlmostEqual(report["strategy_unrealized_pnl"], -0.8)
        self.assertAlmostEqual(report["managed_long_qty"], 40.0)
        self.assertTrue(report["isolates_risk_metrics"])

    def test_best_quote_reduce_freeze_does_not_isolate_without_frozen_inventory(self) -> None:
        report = _best_quote_reduce_freeze_report(
            state={},
            current_long_qty=100.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_short_avg_price=0.0,
            mid_price=0.98,
        )

        self.assertFalse(report["isolates_risk_metrics"])
        self.assertAlmostEqual(report["managed_unrealized_pnl"], -2.0)

    def test_best_quote_frozen_inventory_manual_reduce_places_reduce_only_ioc_order(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {"long_qty": 50.0, "long_entry_price": 1.0},
            "best_quote_frozen_inventory_manual_reduce": {
                "long": {"requested": True, "requested_at": "2026-05-22T00:00:00+00:00"}
            },
        }
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        report = apply_best_quote_frozen_inventory_manual_reduce(
            plan=plan,
            state=state,
            report={},
            bid_price=0.98,
            ask_price=0.981,
            tick_size=0.001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
        )

        self.assertTrue(report["active"])
        self.assertTrue(report["placed_long"])
        order = plan["sell_orders"][0]
        self.assertEqual(order["role"], "frozen_inventory_manual_reduce_long")
        self.assertEqual(order["side"], "SELL")
        self.assertEqual(order["position_side"], "LONG")
        self.assertTrue(order["force_reduce_only"])
        self.assertEqual(order["execution_type"], "aggressive")
        self.assertEqual(order["time_in_force"], "IOC")

    def test_best_quote_frozen_inventory_manual_reduce_honors_requested_qty(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {"long_qty": 50.0, "long_entry_price": 1.0},
            "best_quote_frozen_inventory_manual_reduce": {
                "long": {"requested": True, "requested_qty": 12.0}
            },
        }
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        apply_best_quote_frozen_inventory_manual_reduce(
            plan=plan,
            state=state,
            report={},
            bid_price=1.01,
            ask_price=1.02,
            tick_size=0.001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
        )

        self.assertEqual(plan["sell_orders"][0]["qty"], 12.0)
        self.assertNotIn("best_quote_frozen_inventory_manual_reduce", state)

    def test_best_quote_frozen_pair_release_places_paired_ioc_when_stable_and_profitable(self) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {
                "long_lots": [{"qty": 50.0, "entry_price": 1.0}],
                "short_lots": [{"qty": 30.0, "entry_price": 1.03}],
            }
        }
        report = _best_quote_reduce_freeze_report(
            state=state,
            current_long_qty=50.0,
            current_short_qty=30.0,
            current_long_avg_price=1.0,
            current_short_avg_price=1.03,
            mid_price=1.01,
        )
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        release = apply_best_quote_frozen_inventory_pair_release(
            plan=plan,
            report=report,
            bid_price=1.009,
            ask_price=1.011,
            tick_size=0.001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
            enabled=True,
            stable_allowed=True,
            max_notional=20.0,
            min_profit_ratio=0.001,
            max_slippage_ticks=2,
        )

        self.assertTrue(release["active"])
        self.assertTrue(release["stable_allowed"])
        self.assertGreater(release["estimated_pair_pnl"], release["required_profit"])
        self.assertEqual(release["release_qty"], 19.8)
        sell_order = plan["sell_orders"][0]
        buy_order = plan["buy_orders"][0]
        self.assertEqual(sell_order["role"], "frozen_inventory_pair_release_long")
        self.assertEqual(buy_order["role"], "frozen_inventory_pair_release_short")
        self.assertEqual(sell_order["side"], "SELL")
        self.assertEqual(sell_order["position_side"], "LONG")
        self.assertEqual(buy_order["side"], "BUY")
        self.assertEqual(buy_order["position_side"], "SHORT")
        self.assertEqual(sell_order["time_in_force"], "IOC")
        self.assertEqual(buy_order["time_in_force"], "IOC")
        self.assertTrue(sell_order["frozen_inventory_pair_release"])
        self.assertTrue(buy_order["frozen_inventory_pair_release"])

    def test_best_quote_frozen_pair_release_honors_requested_qty(self) -> None:
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        release = apply_best_quote_frozen_inventory_pair_release(
            plan=plan,
            report={
                "frozen_long_qty": 50.0,
                "frozen_short_qty": 30.0,
                "ledger": {"long_entry_price": 1.0, "short_entry_price": 1.03},
            },
            bid_price=1.009,
            ask_price=1.011,
            tick_size=0.001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
            enabled=True,
            stable_allowed=True,
            max_notional=100.0,
            min_profit_ratio=0.001,
            max_slippage_ticks=2,
            requested_qty=7.0,
        )

        self.assertTrue(release["active"])
        self.assertEqual(release["release_qty"], 7.0)
        self.assertEqual(plan["sell_orders"][0]["qty"], 7.0)
        self.assertEqual(plan["buy_orders"][0]["qty"], 7.0)

    def test_best_quote_frozen_pair_release_blocks_when_market_not_stable(self) -> None:
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        release = apply_best_quote_frozen_inventory_pair_release(
            plan=plan,
            report={"frozen_long_qty": 50.0, "frozen_short_qty": 50.0, "ledger": {"long_entry_price": 1.0, "short_entry_price": 1.03}},
            bid_price=1.009,
            ask_price=1.011,
            tick_size=0.001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
            enabled=True,
            stable_allowed=False,
            max_notional=20.0,
            min_profit_ratio=0.001,
            max_slippage_ticks=2,
        )

        self.assertFalse(release["active"])
        self.assertIn("market_not_stable", release["blocked_reasons"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [])

    def test_best_quote_frozen_pair_release_blocks_when_pair_pnl_below_buffer(self) -> None:
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        release = apply_best_quote_frozen_inventory_pair_release(
            plan=plan,
            report={
                "frozen_long_qty": 50.0,
                "frozen_short_qty": 50.0,
                "ledger": {"long_entry_price": 1.0, "short_entry_price": 1.001},
            },
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
            enabled=True,
            stable_allowed=True,
            max_notional=20.0,
            min_profit_ratio=0.001,
            max_slippage_ticks=2,
        )

        self.assertFalse(release["active"])
        self.assertIn("pair_pnl_below_buffer", release["blocked_reasons"])
        self.assertLess(release["estimated_pair_pnl"], release["required_profit"])

    def test_best_quote_reduce_freeze_drops_normal_reduce_long_when_no_managed_long_remains(self) -> None:
        plan: dict[str, object] = {
            "buy_orders": [],
            "sell_orders": [
                {
                    "side": "SELL",
                    "position_side": "LONG",
                    "price": 0.65,
                    "qty": 20.0,
                    "notional": 13.0,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                },
                {
                    "side": "SELL",
                    "position_side": "LONG",
                    "price": 0.65,
                    "qty": 335.0,
                    "notional": 217.75,
                    "role": "frozen_inventory_manual_reduce_long",
                    "force_reduce_only": True,
                    "manual_frozen_inventory_reduce": True,
                },
            ],
        }

        report = _cap_best_quote_reduce_orders_to_managed_inventory(
            plan=plan,
            report={
                "frozen_long_qty": 335.0,
                "frozen_short_qty": 0.0,
                "managed_long_qty": 0.0,
                "managed_short_qty": 0.0,
            },
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual([order["role"] for order in plan["sell_orders"]], ["frozen_inventory_manual_reduce_long"])
        self.assertEqual(report["dropped_long_orders"], 1)
        self.assertEqual(report["skipped_manual_reduce_orders"], 1)

    def test_best_quote_reduce_freeze_keeps_pair_release_order_when_no_managed_long_remains(self) -> None:
        plan: dict[str, object] = {
            "buy_orders": [],
            "sell_orders": [
                {
                    "side": "SELL",
                    "position_side": "LONG",
                    "price": 0.65,
                    "qty": 12.0,
                    "notional": 7.8,
                    "role": "best_quote_reduce_long",
                },
                {
                    "side": "SELL",
                    "position_side": "LONG",
                    "price": 0.65,
                    "qty": 335.0,
                    "notional": 217.75,
                    "role": "frozen_inventory_pair_release_long",
                    "force_reduce_only": True,
                    "frozen_inventory_pair_release": True,
                },
            ],
        }

        report = _cap_best_quote_reduce_orders_to_managed_inventory(
            plan=plan,
            report={
                "frozen_long_qty": 335.0,
                "frozen_short_qty": 136.0,
                "managed_long_qty": 0.0,
                "managed_short_qty": 0.0,
            },
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual([order["role"] for order in plan["sell_orders"]], ["frozen_inventory_pair_release_long"])
        self.assertEqual(report["dropped_long_orders"], 1)
        self.assertEqual(report["skipped_manual_reduce_orders"], 1)

    def test_best_quote_reduce_freeze_trims_normal_reduce_long_to_managed_qty(self) -> None:
        plan: dict[str, object] = {
            "buy_orders": [],
            "sell_orders": [
                {
                    "side": "SELL",
                    "position_side": "LONG",
                    "price": 0.65,
                    "qty": 12.0,
                    "notional": 7.8,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                }
            ],
        }

        report = _cap_best_quote_reduce_orders_to_managed_inventory(
            plan=plan,
            report={
                "frozen_long_qty": 100.0,
                "frozen_short_qty": 0.0,
                "managed_long_qty": 5.0,
                "managed_short_qty": 0.0,
            },
            step_size=0.1,
            min_qty=0.1,
            min_notional=1.0,
        )

        order = plan["sell_orders"][0]
        self.assertEqual(order["qty"], 5.0)
        self.assertEqual(order["notional"], 3.25)
        self.assertTrue(order["frozen_inventory_managed_qty_capped"])
        self.assertEqual(report["trimmed_long_orders"], 1)

    def test_best_quote_reduce_freeze_caps_normal_reduce_short_independently(self) -> None:
        plan: dict[str, object] = {
            "buy_orders": [
                {
                    "side": "BUY",
                    "position_side": "SHORT",
                    "price": 0.66,
                    "qty": 10.0,
                    "notional": 6.6,
                    "role": "best_quote_reduce_short",
                    "force_reduce_only": True,
                }
            ],
            "sell_orders": [],
        }

        report = _cap_best_quote_reduce_orders_to_managed_inventory(
            plan=plan,
            report={
                "frozen_long_qty": 0.0,
                "frozen_short_qty": 20.0,
                "managed_long_qty": 0.0,
                "managed_short_qty": 3.0,
            },
            step_size=0.1,
            min_qty=0.1,
            min_notional=1.0,
        )

        order = plan["buy_orders"][0]
        self.assertEqual(order["qty"], 3.0)
        self.assertAlmostEqual(order["notional"], 1.98)
        self.assertEqual(report["trimmed_short_orders"], 1)

    def test_best_quote_maker_volume_net_loss_reduce_credits_recent_realized_profit(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                net_loss_reduce_enabled=True,
                net_loss_reduce_min_loss=2.0,
                net_loss_reduce_ratio=0.005,
            ),
            inputs=BestQuoteMakerVolumeInputs(
                bid_price=100.0,
                ask_price=100.1,
                mid_price=100.05,
                current_net_qty=0.0,
                current_long_qty=1.0,
                current_short_qty=1.0,
                position_side_mode="hedge",
                cycle_budget_notional=40.0,
                loss_per_10k_15m=0.2,
                target_volume_remaining=10_000.0,
                unrealized_pnl=-3.5,
                recent_realized_pnl=3.0,
                tick_size=0.1,
                step_size=0.001,
                min_qty=0.001,
                min_notional=5.0,
            ),
        )

        guard = plan["metrics"]["net_loss_reduce"]
        self.assertFalse(guard["active"])
        self.assertEqual(plan["regime"], "normal")
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")

    def test_best_quote_maker_volume_net_loss_reduce_leaves_target_inventory(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                net_loss_reduce_enabled=True,
                net_loss_reduce_min_loss=2.0,
                net_loss_reduce_ratio=0.01,
                net_loss_reduce_min_inventory_notional=40.0,
            ),
            inputs=BestQuoteMakerVolumeInputs(
                bid_price=0.6416,
                ask_price=0.6417,
                mid_price=0.64165,
                current_net_qty=69.0,
                current_long_qty=69.0,
                current_short_qty=0.0,
                position_side_mode="hedge",
                cycle_budget_notional=60.0,
                loss_per_10k_15m=0.2,
                target_volume_remaining=10_000.0,
                unrealized_pnl=-3.0,
                recent_realized_pnl=0.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=1.0,
            ),
        )

        guard = plan["metrics"]["net_loss_reduce"]
        self.assertTrue(guard["active"])
        self.assertEqual(guard["min_inventory_notional"], 40.0)
        self.assertLessEqual(plan["sell_orders"][0]["notional"], 4.3)
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_reduce_long")

    def test_best_quote_maker_volume_same_side_guard_blocks_short_entry_below_existing_cost(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                same_side_entry_price_guard_enabled=True,
                same_side_entry_price_guard_min_notional=10.0,
            ),
            inputs=BestQuoteMakerVolumeInputs(
                bid_price=0.0990,
                ask_price=0.0991,
                mid_price=0.09905,
                current_net_qty=-1010.0,
                current_short_qty=1010.0,
                current_short_avg_price=0.1000,
                position_side_mode="hedge",
                cycle_budget_notional=40.0,
                loss_per_10k_15m=0.0,
                target_volume_remaining=10_000.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
            ),
        )

        guard = plan["metrics"]["same_side_entry_price_guard"]
        self.assertTrue(guard["blocked_short_entry"])
        self.assertEqual(plan["sell_orders"], [])
        self.assertIn("same_side_entry_price_guard", plan["reasons"])

    def test_best_quote_maker_volume_same_side_guard_blocks_long_entry_above_existing_cost(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                same_side_entry_price_guard_enabled=True,
                same_side_entry_price_guard_min_notional=10.0,
            ),
            inputs=BestQuoteMakerVolumeInputs(
                bid_price=0.1010,
                ask_price=0.1011,
                mid_price=0.10105,
                current_net_qty=990.0,
                current_long_qty=990.0,
                current_long_avg_price=0.1000,
                position_side_mode="hedge",
                cycle_budget_notional=40.0,
                loss_per_10k_15m=0.0,
                target_volume_remaining=10_000.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
            ),
        )

        guard = plan["metrics"]["same_side_entry_price_guard"]
        self.assertTrue(guard["blocked_long_entry"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertIn("same_side_entry_price_guard", plan["reasons"])

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_klines")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_maker_volatility_inventory_generate_plan_report_uses_futures_runner(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_klines,
        mock_market_guard,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "100.0", "ask_price": "100.2"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BARDUSDT", "positionAmt": "0", "entryPrice": "0"}],
        }
        mock_open_orders.return_value = []
        now = datetime(2026, 4, 29, 8, 0, tzinfo=timezone.utc)
        mock_klines.return_value = [
            Candle(
                open_time=now - timedelta(minutes=1),
                close_time=now,
                open=100.0,
                high=100.25,
                low=99.95,
                close=100.1,
            )
        ]
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                strategy_mode="maker_volatility_inventory_v1",
                strategy_profile="maker_volatility_inventory_v1",
                maker_base_spread_bps=4.0,
                maker_wide_spread_bps=12.0,
                maker_order_notional=30.0,
                maker_max_long_notional=300.0,
                maker_max_short_notional=300.0,
                maker_inventory_soft_ratio=0.7,
                maker_volatility_window="1m",
                maker_volatility_wide_threshold=0.006,
                maker_extreme_volatility_threshold=0.012,
                maker_directional_move_threshold=0.004,
                maker_cooldown_seconds=30.0,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["strategy_mode"], "maker_volatility_inventory_v1")
        self.assertEqual(report["maker_volatility_inventory"]["regime"], "normal")
        self.assertEqual(len(report["buy_orders"]), 1)
        self.assertEqual(len(report["sell_orders"]), 1)
        self.assertEqual(report["buy_orders"][0]["role"], "maker_entry_long")
        self.assertEqual(report["sell_orders"][0]["role"], "maker_entry_short")

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_generate_plan_report_builds_post_only_orders(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0", "entryPrice": "0"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=1_500.0,
                best_quote_maker_volume_max_short_notional=1_500.0,
                best_quote_maker_volume_loss_per_10k_15m=0.2,
                reset_state=True,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["strategy_mode"], "best_quote_maker_volume_v1")
        self.assertEqual(report["best_quote_maker_volume"]["regime"], "normal")
        self.assertEqual(len(report["buy_orders"]), 1)
        self.assertEqual(len(report["sell_orders"]), 1)
        self.assertEqual(report["buy_orders"][0]["price"], 80400.0)
        self.assertEqual(report["sell_orders"][0]["price"], 80400.1)
        self.assertTrue(report["buy_orders"][0]["post_only"])
        self.assertEqual(report["buy_orders"][0]["execution_type"], "maker")

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_hedge_best_quote_maker_volume_generate_plan_report_uses_position_sides(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": True}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "BTCUSDC", "positionSide": "LONG", "positionAmt": "0.001", "entryPrice": "80400.05"},
                {"symbol": "BTCUSDC", "positionSide": "SHORT", "positionAmt": "0.001", "entryPrice": "80400.05"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                strategy_profile="btcusdc_hedge_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=1_500.0,
                best_quote_maker_volume_max_short_notional=1_500.0,
                best_quote_maker_volume_loss_per_10k_15m=0.2,
                best_quote_maker_volume_below_soft_cost_gap_scale=0.0,
                best_quote_maker_volume_below_soft_adverse_threshold_scale=0.0,
                reset_state=True,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["strategy_mode"], "hedge_best_quote_maker_volume_v1")
        self.assertEqual(report["best_quote_maker_volume"]["regime"], "normal")
        self.assertEqual(report["buy_orders"][0]["position_side"], "LONG")
        self.assertEqual(report["sell_orders"][0]["position_side"], "SHORT")
        self.assertEqual(report["buy_orders"][0]["price"], 80400.0)
        self.assertEqual(report["sell_orders"][0]["price"], 80400.1)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_clamps_entry_short_when_long_is_losing(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0.01", "entryPrice": "80410"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=2_500.0,
                best_quote_maker_volume_max_short_notional=2_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        self.assertTrue(report["take_profit_guard"]["long_active"])
        self.assertEqual(report["take_profit_guard"]["adjusted_sell_orders"], 1)
        self.assertEqual(report["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(report["sell_orders"][0]["price"], report["take_profit_guard"]["long_floor_price"])
        self.assertEqual(report["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertLessEqual(
            report["buy_orders"][0]["price"],
            report["best_quote_maker_volume"]["inventory_cost_gate"]["long_entry_gate_price"],
        )
        self.assertEqual(report["best_quote_maker_volume"]["inventory_cost_gate"]["blocked_buy_orders"], 0)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_blocks_long_entry_above_existing_cost(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0.01", "entryPrice": "80390"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=2_500.0,
                best_quote_maker_volume_max_short_notional=2_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertEqual(report["buy_orders"], [])
        self.assertEqual(gate["blocked_buy_orders"], 1)
        self.assertEqual(gate["would_block_buy_orders"], 1)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_cost_gate_ignores_tiny_residual_inventory(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0.00005", "entryPrice": "80390"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_inventory_cost_gate_enabled=True,
                best_quote_maker_volume_inventory_cost_gate_min_notional=10.0,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=2_500.0,
                best_quote_maker_volume_max_short_notional=2_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertTrue(gate["enabled"])
        self.assertEqual(gate["min_inventory_notional"], 10.0)
        self.assertTrue(gate["long_tiny_inventory_exempt"])
        self.assertEqual(gate["blocked_buy_orders"], 0)
        self.assertEqual(report["buy_orders"][0]["role"], "best_quote_entry_long")

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_cost_gate_converts_blocked_entries_to_reduces(
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
        mock_book_tickers.return_value = [{"bid_price": "0.6625", "ask_price": "0.6627"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": True}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "PHAROSUSDT", "positionSide": "LONG", "positionAmt": "23", "entryPrice": "0.6483"},
                {"symbol": "PHAROSUSDT", "positionSide": "SHORT", "positionAmt": "-74", "entryPrice": "0.6630"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="PHAROSUSDT",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
                step_price=0.00025,
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_inventory_cost_gate_enabled=True,
                best_quote_maker_volume_inventory_cost_gate_min_notional=10.0,
                best_quote_maker_volume_cycle_budget_notional=40.0,
                best_quote_maker_volume_quote_offset_ticks=3,
                best_quote_maker_volume_max_long_notional=700.0,
                best_quote_maker_volume_max_short_notional=700.0,
                best_quote_maker_volume_inventory_soft_ratio=200.0 / 700.0,
                take_profit_min_profit_ratio=0.00008,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertEqual(gate["blocked_buy_orders"], 1)
        self.assertEqual(gate["blocked_sell_orders"], 1)
        self.assertEqual(report["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(report["buy_orders"][0]["position_side"], "SHORT")
        self.assertTrue(report["buy_orders"][0]["force_reduce_only"])
        self.assertEqual(report["sell_orders"][0]["role"], "best_quote_reduce_long")
        self.assertEqual(report["sell_orders"][0]["position_side"], "LONG")
        self.assertTrue(report["sell_orders"][0]["force_reduce_only"])

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_blocks_losing_long_entry_above_grid_gap(
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
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.14355", "ask_price": "0.14356"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "5430", "entryPrice": "0.14360"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BILLUSDT",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="billusdt_best_quote_maker_volume_reset_v1",
                step_price=0.00019,
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=260.0,
                best_quote_maker_volume_quote_offset_ticks=1,
                best_quote_maker_volume_defensive_offset_ticks=30,
                best_quote_maker_volume_max_long_notional=5500.0,
                best_quote_maker_volume_max_short_notional=5500.0,
                best_quote_maker_volume_inventory_soft_ratio=0.55,
                take_profit_min_profit_ratio=0.0003,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertAlmostEqual(gate["long_entry_gate_price"], 0.14341)
        self.assertEqual(report["buy_orders"], [])
        self.assertEqual(gate["blocked_buy_orders"], 1)
        self.assertEqual(gate["would_block_buy_orders"], 1)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_relaxes_cost_gate_below_soft_inventory(
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
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.14355", "ask_price": "0.14356"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "5430", "entryPrice": "0.14360"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BILLUSDT",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="billusdt_best_quote_maker_volume_reset_v1",
                step_price=0.00019,
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=260.0,
                best_quote_maker_volume_quote_offset_ticks=1,
                best_quote_maker_volume_defensive_offset_ticks=30,
                best_quote_maker_volume_max_long_notional=5500.0,
                best_quote_maker_volume_max_short_notional=5500.0,
                best_quote_maker_volume_inventory_soft_ratio=0.55,
                best_quote_maker_volume_below_soft_cost_gap_scale=0.0,
                take_profit_min_profit_ratio=0.0003,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertTrue(gate["long_below_soft_exempt"])
        self.assertAlmostEqual(gate["long_entry_gate_price"], 0.14360)
        self.assertAlmostEqual(gate["long_cost_gap_price"], 0.0)
        self.assertEqual(report["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(gate["blocked_buy_orders"], 0)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_allows_long_entry_below_cost_only_when_inventory_profitable(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0.01", "entryPrice": "80400.02"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=2_500.0,
                best_quote_maker_volume_max_short_notional=2_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertLessEqual(report["buy_orders"][0]["price"], 80400.02)
        self.assertEqual(report["best_quote_maker_volume"]["inventory_cost_gate"]["blocked_buy_orders"], 0)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_blocks_short_entry_below_existing_cost(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "-0.01", "entryPrice": "80410"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=2_500.0,
                best_quote_maker_volume_max_short_notional=2_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertEqual(report["sell_orders"], [])
        self.assertEqual(gate["blocked_sell_orders"], 1)
        self.assertEqual(gate["would_block_sell_orders"], 1)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_can_disable_inventory_cost_gate(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "-0.01", "entryPrice": "80410"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_inventory_cost_gate_enabled=False,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=2_500.0,
                best_quote_maker_volume_max_short_notional=2_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertFalse(gate["enabled"])
        self.assertGreater(len(report["sell_orders"]), 0)
        self.assertEqual(gate["blocked_sell_orders"], 0)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_blocks_losing_short_entry_during_uptrend(
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
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.16051", "ask_price": "0.16052"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "-26909", "entryPrice": "0.1598576369652"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "return_ratio": 0.00337,
            "amplitude_ratio": 0.00531,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BILLUSDT",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="billusdt_best_quote_maker_volume_reset_v1",
                step_price=0.00019,
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=460.0,
                best_quote_maker_volume_quote_offset_ticks=1,
                best_quote_maker_volume_defensive_offset_ticks=2,
                best_quote_maker_volume_max_long_notional=9000.0,
                best_quote_maker_volume_max_short_notional=9000.0,
                best_quote_maker_volume_inventory_soft_ratio=0.55,
                take_profit_min_profit_ratio=0.0003,
                reset_state=True,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["sell_orders"], [])
        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertEqual(gate["blocked_sell_orders"], 1)
        self.assertEqual(gate["blocked_sell_order_details"][0]["block_reason"], "losing_short_adverse_uptrend")

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_relaxes_adverse_gate_below_soft_inventory(
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
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.16051", "ask_price": "0.16052"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "-26909", "entryPrice": "0.1598576369652"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "return_ratio": 0.00337,
            "amplitude_ratio": 0.00531,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BILLUSDT",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="billusdt_best_quote_maker_volume_reset_v1",
                step_price=0.00019,
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=460.0,
                best_quote_maker_volume_quote_offset_ticks=1,
                best_quote_maker_volume_defensive_offset_ticks=2,
                best_quote_maker_volume_max_long_notional=9000.0,
                best_quote_maker_volume_max_short_notional=9000.0,
                best_quote_maker_volume_inventory_soft_ratio=0.55,
                best_quote_maker_volume_below_soft_adverse_threshold_scale=5.0,
                take_profit_min_profit_ratio=0.0003,
                reset_state=True,
            )

            report = generate_plan_report(args)

        gate = report["best_quote_maker_volume"]["inventory_cost_gate"]
        self.assertTrue(gate["short_below_soft_exempt"])
        self.assertGreater(gate["short_adverse_trend_threshold_ratio"], gate["adverse_trend_threshold_ratio"])
        self.assertEqual(report["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(gate["blocked_sell_orders"], 0)

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_maker_volume_clamps_inventory_recover_reduce_when_long_is_losing(
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
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "80400.0", "ask_price": "80400.1"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BTCUSDC", "positionAmt": "0.01", "entryPrice": "80410"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BTCUSDC",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="btcusdc_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=400.0,
                best_quote_maker_volume_max_long_notional=1_500.0,
                best_quote_maker_volume_max_short_notional=1_500.0,
                take_profit_min_profit_ratio=0.00025,
                reset_state=True,
            )

            report = generate_plan_report(args)

        self.assertEqual(report["best_quote_maker_volume"]["regime"], "inventory_recover")
        self.assertTrue(report["take_profit_guard"]["long_active"])
        self.assertEqual(report["take_profit_guard"]["adjusted_sell_orders"], 1)
        self.assertEqual(report["sell_orders"][0]["role"], "best_quote_reduce_long")
        self.assertEqual(report["sell_orders"][0]["price"], report["take_profit_guard"]["long_floor_price"])

    def test_filter_futures_strategy_orders_ignores_manual_and_flatten_orders(self) -> None:
        open_orders = [
            {"clientOrderId": "gx-btcusdc-001", "orderId": 1},
            {"clientOrderId": "mt_btcusdc_buy_001", "orderId": 2},
            {"clientOrderId": "mf_btcusdc_001", "orderId": 3},
            {"clientOrderId": "", "orderId": 4},
            "not-an-order",
        ]

        result = _filter_futures_strategy_orders(open_orders, "BTCUSDC")

        self.assertEqual(result, [{"clientOrderId": "gx-btcusdc-001", "orderId": 1}])

    def test_resolve_anti_chase_entry_guard_blocks_directional_entries(self) -> None:
        up_guard = resolve_anti_chase_entry_guard(
            market_guard={
                "window_1m": {"return_ratio": 0.003, "amplitude_ratio": 0.0036},
                "window_3m": {"return_ratio": 0.004, "amplitude_ratio": 0.0045},
            },
            enabled=True,
            window_1m_abs_return_ratio=0.0025,
            window_1m_amplitude_ratio=0.0035,
            window_3m_abs_return_ratio=0.006,
            window_3m_amplitude_ratio=0.008,
        )
        down_guard = resolve_anti_chase_entry_guard(
            market_guard={
                "window_1m": {"return_ratio": -0.003, "amplitude_ratio": 0.0036},
                "window_3m": {"return_ratio": -0.004, "amplitude_ratio": 0.0045},
            },
            enabled=True,
            window_1m_abs_return_ratio=0.0025,
            window_1m_amplitude_ratio=0.0035,
            window_3m_abs_return_ratio=0.006,
            window_3m_amplitude_ratio=0.008,
        )

        self.assertTrue(up_guard["active"])
        self.assertTrue(up_guard["block_long_entries"])
        self.assertFalse(up_guard["block_short_entries"])
        self.assertTrue(down_guard["active"])
        self.assertFalse(down_guard["block_long_entries"])
        self.assertTrue(down_guard["block_short_entries"])

    def test_resolve_anti_chase_entry_guard_requires_return_and_amplitude(self) -> None:
        return_only = resolve_anti_chase_entry_guard(
            market_guard={
                "window_1m": {"return_ratio": 0.003, "amplitude_ratio": 0.002},
            },
            enabled=True,
            window_1m_abs_return_ratio=0.0025,
            window_1m_amplitude_ratio=0.0035,
            window_3m_abs_return_ratio=0.006,
            window_3m_amplitude_ratio=0.008,
        )
        amplitude_only = resolve_anti_chase_entry_guard(
            market_guard={
                "window_1m": {"return_ratio": 0.001, "amplitude_ratio": 0.004},
            },
            enabled=True,
            window_1m_abs_return_ratio=0.0025,
            window_1m_amplitude_ratio=0.0035,
            window_3m_abs_return_ratio=0.006,
            window_3m_amplitude_ratio=0.008,
        )

        self.assertFalse(return_only["active"])
        self.assertFalse(amplitude_only["active"])

    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_periodic_reconcile_ignores_manual_open_orders(
        self,
        mock_open_orders,
        mock_account_info,
    ) -> None:
        mock_open_orders.return_value = [
            {"clientOrderId": "gx-btcusdc-001", "orderId": 1},
            {"clientOrderId": "mt_btcusdc_buy_001", "orderId": 2},
        ]
        mock_account_info.return_value = {"positions": [{"symbol": "BTCUSDC", "positionAmt": "0"}]}

        snapshot = _run_periodic_reconcile(
            state={},
            cycle=5,
            interval_cycles=5,
            symbol="BTCUSDC",
            strategy_mode="one_way_long",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            expected_open_order_count=1,
            expected_actual_net_qty=0.0,
        )

        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["actual_open_order_count"], 1)
        self.assertEqual(snapshot["total_open_order_count"], 2)

    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_periodic_reconcile_defers_rest_open_order_backfill_when_stream_is_fresh(
        self,
        mock_open_orders,
        mock_account_info,
    ) -> None:
        mock_account_info.return_value = {"positions": [{"symbol": "BTCUSDC", "positionAmt": "0"}]}
        stream = SimpleNamespace(
            snapshot_open_orders=lambda: [],
            open_order_state_age_seconds=lambda: 1.0,
            status=lambda: {"last_account_update_age_seconds": 1.0},
        )
        args = Namespace(user_data_stream=stream)
        state = {
            "last_reconcile": {
                "open_orders_rest_last_sync_at": datetime.now(timezone.utc).isoformat(),
            }
        }

        snapshot = _run_periodic_reconcile(
            state=state,
            cycle=5,
            interval_cycles=5,
            symbol="BTCUSDC",
            strategy_mode="one_way_long",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            expected_open_order_count=2,
            expected_actual_net_qty=0.0,
            args=args,
        )

        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["open_orders_source"], "stream_open_orders")
        self.assertFalse(snapshot["open_orders_rest_backfill_performed"])
        mock_open_orders.assert_not_called()
        mock_account_info.assert_called_once_with("key", "secret", recv_window=5000, use_cache=False)

    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_periodic_reconcile_uses_rest_position_even_when_stream_position_is_fresh(
        self,
        _mock_open_orders,
        mock_account_info,
    ) -> None:
        mock_account_info.return_value = {"positions": [{"symbol": "BTCUSDC", "positionAmt": "600"}]}
        now = time.monotonic()
        stream = SimpleNamespace(
            snapshot_open_orders=lambda: [],
            open_order_state_age_seconds=lambda: 1.0,
            snapshot_account_positions=lambda: [
                {"symbol": "BTCUSDC", "positionSide": "BOTH", "positionAmt": "10", "observed_at": now}
            ],
        )

        snapshot = _run_periodic_reconcile(
            state={"last_reconcile": {"open_orders_rest_last_sync_at": datetime.now(timezone.utc).isoformat()}},
            cycle=5,
            interval_cycles=5,
            symbol="BTCUSDC",
            strategy_mode="one_way_long",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            expected_open_order_count=0,
            expected_actual_net_qty=10.0,
            args=Namespace(user_data_stream=stream),
        )

        self.assertEqual(snapshot["account_position_source"], "rest")
        self.assertEqual(snapshot["actual_actual_net_qty"], 600.0)
        self.assertEqual(snapshot["actual_net_qty_diff"], 590.0)

    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_periodic_reconcile_marks_persistent_open_order_diff_for_protective_stop(
        self,
        mock_open_orders,
        mock_account_info,
    ) -> None:
        mock_open_orders.return_value = [
            {"clientOrderId": f"gx-btcusdc-{index}", "orderId": index}
            for index in range(11)
        ]
        mock_account_info.return_value = {"positions": [{"symbol": "BTCUSDC", "positionAmt": "0"}]}

        snapshot = _run_periodic_reconcile(
            state={"last_reconcile": {"open_order_diff_over_10_count": 1}},
            cycle=5,
            interval_cycles=5,
            symbol="BTCUSDC",
            strategy_mode="one_way_long",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            expected_open_order_count=0,
            expected_actual_net_qty=0.0,
        )

        self.assertEqual(snapshot["open_order_diff_over_10_count"], 2)
        self.assertTrue(snapshot["protective_stop_required"])
        self.assertIn("open_order_diff_persistent", snapshot["protective_stop_reasons"])

    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_periodic_reconcile_treats_stale_position_stream_as_warning_when_rest_is_clean(
        self,
        _mock_open_orders,
        mock_account_info,
    ) -> None:
        mock_account_info.return_value = {"positions": [{"symbol": "BTCUSDC", "positionAmt": "0"}]}
        stale_observed_at = time.monotonic() - 45.0
        stream = SimpleNamespace(
            snapshot_open_orders=lambda: [],
            open_order_state_age_seconds=lambda: 1.0,
            snapshot_account_positions=lambda: [
                {"symbol": "BTCUSDC", "positionSide": "BOTH", "positionAmt": "0", "observed_at": stale_observed_at}
            ],
        )

        snapshot = _run_periodic_reconcile(
            state={"last_reconcile": {"open_orders_rest_last_sync_at": datetime.now(timezone.utc).isoformat()}},
            cycle=5,
            interval_cycles=5,
            symbol="BTCUSDC",
            strategy_mode="one_way_long",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            expected_open_order_count=0,
            expected_actual_net_qty=0.0,
            args=Namespace(user_data_stream=stream),
        )

        self.assertTrue(snapshot["ok"])
        self.assertFalse(snapshot["protective_stop_required"])
        self.assertIn("account_position_stream_stale", snapshot["protective_warning_reasons"])

    def test_apply_execution_request_budget_defers_extra_cancel_and_place_actions(self) -> None:
        validation = {
            "ok": True,
            "errors": [],
            "actions": {
                "cancel_count": 2,
                "place_count": 3,
                "cancel_orders": [
                    {"orderId": 1, "clientOrderId": "mt_btcusdc_buy_001"},
                    {"orderId": 2, "clientOrderId": "mt_btcusdc_buy_002"},
                ],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 1.0, "price": 100.0},
                    {"role": "entry", "side": "BUY", "qty": 1.0, "price": 99.0},
                    {"role": "entry", "side": "BUY", "qty": 1.0, "price": 98.0},
                ],
            },
        }
        report: dict[str, object] = {}

        updated = apply_execution_request_budget_to_actions(
            validation=validation,
            report=report,
            max_mutations_per_cycle=3,
            max_cancels_per_cycle=1,
            max_places_per_cycle=2,
        )

        actions = updated["actions"]
        self.assertEqual(actions["cancel_count"], 1)
        self.assertEqual(actions["place_count"], 2)
        self.assertEqual([item["orderId"] for item in actions["cancel_orders"]], [1])
        self.assertEqual([item["price"] for item in actions["place_orders"]], [100.0, 99.0])
        self.assertAlmostEqual(actions["place_notional"], 0.0, places=8)
        self.assertEqual(report["execution_request_budget"]["deferred_cancel_count"], 1)
        self.assertEqual(report["execution_request_budget"]["deferred_place_count"], 1)

    def test_apply_execution_request_budget_recomputes_notional_and_preserves_cancels(self) -> None:
        validation = {
            "ok": True,
            "errors": [],
            "actions": {
                "cancel_count": 1,
                "place_count": 4,
                "place_notional": 520.0,
                "cancel_orders": [{"orderId": 410254194, "clientOrderId": "gx-billu-bestquot-2-02555661"}],
                "place_orders": [
                    {"role": "best_quote_adverse_reduce_long", "side": "SELL", "qty": 909.0, "price": 0.14464, "notional": 130.0},
                    {"role": "best_quote_entry_long", "side": "BUY", "qty": 909.0, "price": 0.14289, "notional": 130.0},
                    {"role": "take_profit_long", "side": "SELL", "qty": 909.0, "price": 0.1448, "notional": 130.0},
                    {"role": "best_quote_entry_long", "side": "BUY", "qty": 909.0, "price": 0.1427, "notional": 130.0},
                ],
            },
        }
        report: dict[str, object] = {}

        updated = apply_execution_request_budget_to_actions(
            validation=validation,
            report=report,
            max_mutations_per_cycle=0,
            max_cancels_per_cycle=0,
            max_places_per_cycle=2,
        )

        actions = updated["actions"]
        self.assertEqual(actions["cancel_count"], 1)
        self.assertEqual(actions["place_count"], 2)
        self.assertEqual([item["orderId"] for item in actions["cancel_orders"]], [410254194])
        self.assertEqual(
            [item["role"] for item in actions["place_orders"]],
            ["best_quote_adverse_reduce_long", "best_quote_entry_long"],
        )
        self.assertAlmostEqual(actions["place_notional"], 260.0, places=8)
        self.assertEqual(report["execution_request_budget"]["deferred_cancel_count"], 0)
        self.assertEqual(report["execution_request_budget"]["deferred_place_count"], 2)

    def test_inventory_reducing_place_priority_keeps_long_exits_before_new_buys(self) -> None:
        actions = {
            "place_orders": [
                {"role": "best_quote_entry_long", "side": "BUY", "price": 0.14920, "notional": 230.0},
                {"role": "adverse_reduce_long", "side": "SELL", "price": 0.14930, "notional": 230.0, "force_reduce_only": True},
                {"role": "best_quote_entry_short", "side": "SELL", "price": 0.14930, "notional": 230.0},
            ],
            "cancel_orders": [],
            "place_count": 3,
            "cancel_count": 0,
        }

        prioritized = prioritize_inventory_reducing_place_orders(
            actions=actions,
            current_actual_net_qty=20_000.0,
            current_long_avg_price=0.14900,
            step_price=0.00019,
            min_profit_ratio=0.0003,
        )
        report: dict[str, object] = {}
        capped = apply_execution_request_budget_to_actions(
            validation={"ok": True, "errors": [], "actions": prioritized},
            report=report,
            max_places_per_cycle=2,
        )

        self.assertEqual(
            [item["role"] for item in capped["actions"]["place_orders"]],
            ["adverse_reduce_long", "best_quote_entry_short"],
        )
        self.assertEqual(report["execution_request_budget"]["deferred_place_orders"][0]["role"], "best_quote_entry_long")

    def test_inventory_reducing_place_priority_keeps_low_cost_buy_ahead_of_far_exit(self) -> None:
        actions = {
            "place_orders": [
                {"role": "best_quote_entry_short", "side": "SELL", "price": 0.15100, "notional": 230.0},
                {"role": "best_quote_entry_long", "side": "BUY", "price": 0.14870, "notional": 230.0},
                {"role": "best_quote_entry_long", "side": "BUY", "price": 0.14920, "notional": 230.0},
            ],
            "cancel_orders": [],
            "place_count": 3,
            "cancel_count": 0,
        }

        prioritized = prioritize_inventory_reducing_place_orders(
            actions=actions,
            current_actual_net_qty=10_000.0,
            current_long_avg_price=0.14900,
            step_price=0.00019,
            min_profit_ratio=0.0003,
        )

        self.assertEqual(
            [item["price"] for item in prioritized["place_orders"]],
            [0.15100, 0.14870, 0.14920],
        )

    def test_inventory_reducing_place_priority_does_not_put_non_profit_exit_before_low_cost_buy(self) -> None:
        actions = {
            "place_orders": [
                {"role": "best_quote_entry_short", "side": "SELL", "price": 0.14902, "notional": 230.0},
                {"role": "best_quote_entry_long", "side": "BUY", "price": 0.14870, "notional": 230.0},
                {"role": "best_quote_entry_long", "side": "BUY", "price": 0.14920, "notional": 230.0},
            ],
            "cancel_orders": [],
            "place_count": 3,
            "cancel_count": 0,
        }

        prioritized = prioritize_inventory_reducing_place_orders(
            actions=actions,
            current_actual_net_qty=10_000.0,
            current_long_avg_price=0.14900,
            step_price=0.00019,
            min_profit_ratio=0.0003,
        )

        self.assertEqual(
            [item["price"] for item in prioritized["place_orders"]],
            [0.14870, 0.14902, 0.14920],
        )

    def test_soonusdt_volume_profiles_use_entry_price_cost_basis(self) -> None:
        self.assertTrue(_uses_entry_price_cost_basis("chip_low_wear_guarded_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("chipusdt_competition_neutral_ping_pong_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("soon_high_vol_short_grid_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("soon_volume_neutral_ping_pong_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("soonusdt_competition_neutral_ping_pong_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("billusdt_best_quote_maker_volume_reset_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("pharosusdt_adaptive_regime_router_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("pharosusdt_best_quote_maker_volume_v1"))

    def test_defi_competition_maker_neutral_profiles_use_entry_price_cost_basis(self) -> None:
        self.assertTrue(_uses_entry_price_cost_basis("bzusdt_competition_maker_neutral_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("clusdt_competition_maker_neutral_conservative_v1"))

    def test_trumpusdc_volume_long_profile_uses_v4_cost_guards(self) -> None:
        self.assertTrue(_uses_entry_price_cost_basis("trumpusdc_volume_long_v4"))
        self.assertTrue(_uses_volume_long_v4_staged_delever("trumpusdc_volume_long_v4"))

    def test_ethusdc_volume_long_profile_uses_v4_delever_guards(self) -> None:
        self.assertTrue(_uses_entry_price_cost_basis("ethusdc_um_volume_long_v1"))
        self.assertTrue(_uses_volume_long_v4_staged_delever("ethusdc_um_volume_long_v1"))

    def test_resolve_exposure_escalation_waits_for_hold_time(self) -> None:
        state: dict[str, object] = {}
        now = datetime(2026, 4, 24, 8, 0, tzinfo=timezone.utc)

        first = resolve_exposure_escalation(
            state=state,
            enabled=True,
            now=now,
            current_long_notional=1100.0,
            current_long_qty=5500.0,
            current_long_cost_basis_price=0.20,
            mid_price=0.198,
            trigger_notional=1000.0,
            hold_seconds=600.0,
            target_notional=650.0,
            max_loss_ratio=0.012,
            hard_unrealized_loss_limit=None,
        )
        later = resolve_exposure_escalation(
            state=state,
            enabled=True,
            now=now + timedelta(seconds=599),
            current_long_notional=1100.0,
            current_long_qty=5500.0,
            current_long_cost_basis_price=0.20,
            mid_price=0.198,
            trigger_notional=1000.0,
            hold_seconds=600.0,
            target_notional=650.0,
            max_loss_ratio=0.012,
            hard_unrealized_loss_limit=None,
        )

        self.assertTrue(first["enabled"])
        self.assertFalse(first["active"])
        self.assertEqual(first["blocked_reason"], "waiting_hold_time")
        self.assertEqual(later["held_seconds"], 599.0)
        self.assertFalse(later["active"])

    def test_resolve_exposure_escalation_activates_after_hold_time(self) -> None:
        now = datetime(2026, 4, 24, 8, 0, tzinfo=timezone.utc)
        state = {"exposure_escalation": {"observed_since": now.isoformat()}}

        report = resolve_exposure_escalation(
            state=state,
            enabled=True,
            now=now + timedelta(seconds=600),
            current_long_notional=1120.0,
            current_long_qty=5600.0,
            current_long_cost_basis_price=0.20,
            mid_price=0.198,
            trigger_notional=1000.0,
            hold_seconds=600.0,
            target_notional=650.0,
            max_loss_ratio=0.012,
            hard_unrealized_loss_limit=None,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["reason"], "hold_time_exceeded")
        self.assertEqual(report["target_notional"], 650.0)
        self.assertEqual(report["max_loss_ratio"], 0.012)

    def test_resolve_exposure_escalation_hard_loss_activates_immediately(self) -> None:
        report = resolve_exposure_escalation(
            state={},
            enabled=True,
            now=datetime(2026, 4, 24, 8, 0, tzinfo=timezone.utc),
            current_long_notional=900.0,
            current_long_qty=5000.0,
            current_long_cost_basis_price=0.20,
            mid_price=0.188,
            trigger_notional=1000.0,
            hold_seconds=600.0,
            target_notional=650.0,
            max_loss_ratio=0.012,
            hard_unrealized_loss_limit=60.0,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["reason"], "hard_unrealized_loss_limit")
        self.assertAlmostEqual(report["unrealized_pnl"], -60.0)

    def test_resolve_exposure_escalation_exits_below_target_even_with_hard_loss(self) -> None:
        state = {"exposure_escalation": {"observed_since": "2026-04-24T08:00:00+00:00"}}

        report = resolve_exposure_escalation(
            state=state,
            enabled=True,
            now=datetime(2026, 4, 24, 8, 2, tzinfo=timezone.utc),
            current_long_notional=489.0,
            current_long_qty=2676.0,
            current_long_cost_basis_price=0.2055,
            mid_price=0.1830,
            trigger_notional=1000.0,
            hold_seconds=600.0,
            target_notional=650.0,
            max_loss_ratio=0.012,
            hard_unrealized_loss_limit=60.0,
        )

        self.assertFalse(report["active"])
        self.assertEqual(report["blocked_reason"], "below_target")
        self.assertIsNone(state["exposure_escalation"]["observed_since"])  # type: ignore[index]

    def test_resolve_exposure_escalation_clears_timer_below_threshold(self) -> None:
        state = {"exposure_escalation": {"observed_since": "2026-04-24T08:00:00+00:00"}}

        report = resolve_exposure_escalation(
            state=state,
            enabled=True,
            now=datetime(2026, 4, 24, 8, 10, tzinfo=timezone.utc),
            current_long_notional=900.0,
            current_long_qty=4500.0,
            current_long_cost_basis_price=0.20,
            mid_price=0.199,
            trigger_notional=1000.0,
            hold_seconds=600.0,
            target_notional=650.0,
            max_loss_ratio=0.012,
            hard_unrealized_loss_limit=None,
        )

        self.assertFalse(report["active"])
        self.assertEqual(report["blocked_reason"], "below_trigger")
        self.assertIsNone(state["exposure_escalation"]["observed_since"])  # type: ignore[index]

    def test_prime_exposure_escalation_on_market_guard_marks_hold_elapsed(self) -> None:
        state: dict[str, object] = {}
        now = datetime(2026, 4, 24, 8, 10, tzinfo=timezone.utc)

        primed = prime_exposure_escalation_on_market_guard(
            state=state,
            market_guard={"buy_pause_active": True},
            enabled=True,
            now=now,
            current_long_notional=1300.0,
            trigger_notional=1250.0,
            hold_seconds=120.0,
        )

        self.assertTrue(primed)
        report = resolve_exposure_escalation(
            state=state,
            enabled=True,
            now=now,
            current_long_notional=1300.0,
            current_long_qty=6500.0,
            current_long_cost_basis_price=0.20,
            mid_price=0.197,
            trigger_notional=1250.0,
            hold_seconds=120.0,
            target_notional=780.0,
            max_loss_ratio=0.05,
            hard_unrealized_loss_limit=None,
        )
        self.assertTrue(report["active"])
        self.assertEqual(report["reason"], "hold_time_exceeded")
        self.assertAlmostEqual(report["held_seconds"], 120.0)

    def test_prime_exposure_escalation_on_market_guard_ignores_calm_market(self) -> None:
        state: dict[str, object] = {}
        now = datetime(2026, 4, 24, 8, 10, tzinfo=timezone.utc)

        primed = prime_exposure_escalation_on_market_guard(
            state=state,
            market_guard={"buy_pause_active": False},
            enabled=True,
            now=now,
            current_long_notional=1300.0,
            trigger_notional=1250.0,
            hold_seconds=120.0,
        )

        self.assertFalse(primed)
        self.assertEqual(state, {})

    def test_resolve_exposure_escalation_buy_pause_holds_during_cooldown(self) -> None:
        now = datetime(2026, 4, 24, 8, 10, tzinfo=timezone.utc)
        state = {"exposure_escalation": {"last_active_at": (now - timedelta(seconds=60)).isoformat()}}

        report = resolve_exposure_escalation_buy_pause(
            state=state,
            enabled=True,
            now=now,
            current_long_notional=900.0,
            trigger_notional=1250.0,
            cooldown_seconds=180.0,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["reason"], "cooldown")
        self.assertAlmostEqual(report["remaining_seconds"], 120.0)

    def test_resolve_exposure_escalation_buy_pause_holds_above_soft_threshold_after_cooldown(self) -> None:
        now = datetime(2026, 4, 24, 8, 10, tzinfo=timezone.utc)
        state = {"exposure_escalation": {"last_active_at": (now - timedelta(seconds=300)).isoformat()}}

        report = resolve_exposure_escalation_buy_pause(
            state=state,
            enabled=True,
            now=now,
            current_long_notional=1300.0,
            trigger_notional=1250.0,
            cooldown_seconds=180.0,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["reason"], "above_soft_threshold")

    def test_apply_hard_loss_forced_reduce_adds_ioc_reduce_only_order(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "forced_reduce_orders": []}

        report = apply_hard_loss_forced_reduce(
            plan=plan,
            enabled=True,
            active=True,
            side="SELL",
            current_qty=10000.0,
            current_notional=1800.0,
            target_notional=780.0,
            max_order_notional=400.0,
            bid_price=0.1778,
            ask_price=0.1779,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            reason="hard_unrealized_loss_limit",
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["placed_order_count"], 1)
        order = plan["sell_orders"][0]
        self.assertEqual(order["role"], "hard_loss_forced_reduce_long")
        self.assertEqual(order["execution_type"], "aggressive")
        self.assertEqual(order["time_in_force"], "IOC")
        self.assertTrue(order["force_reduce_only"])
        self.assertAlmostEqual(order["price"], 0.1778)
        self.assertLessEqual(order["notional"], 400.0)
        self.assertEqual(plan["forced_reduce_orders"], [order])

    def test_apply_hard_loss_forced_reduce_uses_hedge_position_side(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "forced_reduce_orders": []}

        report = apply_hard_loss_forced_reduce(
            plan=plan,
            enabled=True,
            active=True,
            side="SELL",
            current_qty=888.0,
            current_notional=531.42,
            target_notional=30.0,
            max_order_notional=10.0,
            bid_price=0.5984,
            ask_price=0.5985,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            reason="hard_unrealized_loss_limit",
            strategy_mode="hedge_neutral",
        )

        self.assertTrue(report["active"])
        self.assertEqual(plan["sell_orders"][0]["position_side"], "LONG")

    def test_apply_hard_loss_forced_reduce_uses_hedge_bq_position_side(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "forced_reduce_orders": []}

        report = apply_hard_loss_forced_reduce(
            plan=plan,
            enabled=True,
            active=True,
            side="SELL",
            current_qty=440.0,
            current_notional=265.0,
            target_notional=160.0,
            max_order_notional=12.0,
            bid_price=0.6028,
            ask_price=0.6030,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            reason="hard_unrealized_loss_limit",
            strategy_mode="hedge_best_quote_maker_volume_v1",
        )

        self.assertTrue(report["active"])
        order = plan["sell_orders"][0]
        self.assertEqual(order["position_side"], "LONG")
        self.assertEqual(order["time_in_force"], "IOC")

    def test_resolve_hard_loss_reduce_target_notional_prefers_explicit_target(self) -> None:
        self.assertAlmostEqual(
            _resolve_hard_loss_reduce_target_notional(
                configured_target_notional=30.0,
                pause_position_notional=520.0,
            ),
            30.0,
        )
        self.assertAlmostEqual(
            _resolve_hard_loss_reduce_target_notional(
                configured_target_notional=None,
                pause_position_notional=520.0,
            ),
            520.0,
        )

    def test_position_unrealized_or_estimate_prefers_exchange_unrealized(self) -> None:
        unrealized = _position_unrealized_or_estimate(
            position={"unRealizedProfit": "-0.47"},
            qty=51.0,
            cost_basis_price=0.7164,
            mid_price=0.5967,
            side="SELL",
        )

        self.assertAlmostEqual(unrealized, -0.47)

    def test_apply_hard_loss_forced_reduce_stops_when_target_above_current(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "forced_reduce_orders": []}

        report = apply_hard_loss_forced_reduce(
            plan=plan,
            enabled=True,
            active=True,
            side="SELL",
            current_qty=5000.0,
            current_notional=650.0,
            target_notional=2700.0,
            max_order_notional=180.0,
            bid_price=0.13,
            ask_price=0.131,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            reason="hard_unrealized_loss_limit",
        )

        self.assertFalse(report["active"])
        self.assertEqual(report["blocked_reason"], "at_or_below_target")
        self.assertEqual(report["placed_order_count"], 0)
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(plan["forced_reduce_orders"], [])

    def test_hard_loss_episode_disarms_after_target_until_loss_recovers(self) -> None:
        now = datetime(2026, 5, 16, 5, 0, tzinfo=timezone.utc)
        state: dict[str, Any] = {}

        reducing = resolve_hard_loss_forced_reduce_episode(
            state=state,
            enabled=True,
            triggered=True,
            side="BUY",
            current_notional=1400.0,
            target_notional=900.0,
            unrealized_pnl=-30.0,
            hard_unrealized_loss_limit=25.0,
            now=now,
            loss_recover_ratio=0.75,
        )
        self.assertTrue(reducing["active"])
        self.assertFalse(reducing["disarmed"])
        self.assertEqual(reducing["reason"], "reducing_to_target")

        disarmed = resolve_hard_loss_forced_reduce_episode(
            state=state,
            enabled=True,
            triggered=True,
            side="BUY",
            current_notional=880.0,
            target_notional=900.0,
            unrealized_pnl=-24.0,
            hard_unrealized_loss_limit=25.0,
            now=now + timedelta(seconds=10),
            loss_recover_ratio=0.75,
        )
        self.assertTrue(disarmed["active"])
        self.assertTrue(disarmed["disarmed"])
        self.assertEqual(disarmed["reason"], "target_reached_waiting_loss_recovery")

        still_waiting = resolve_hard_loss_forced_reduce_episode(
            state=state,
            enabled=True,
            triggered=True,
            side="BUY",
            current_notional=1040.0,
            target_notional=900.0,
            unrealized_pnl=-22.0,
            hard_unrealized_loss_limit=25.0,
            now=now + timedelta(seconds=20),
            loss_recover_ratio=0.75,
        )
        self.assertTrue(still_waiting["disarmed"])
        self.assertEqual(still_waiting["reason"], "waiting_loss_recovery")

        recovered = resolve_hard_loss_forced_reduce_episode(
            state=state,
            enabled=True,
            triggered=True,
            side="BUY",
            current_notional=1040.0,
            target_notional=900.0,
            unrealized_pnl=-18.0,
            hard_unrealized_loss_limit=25.0,
            now=now + timedelta(seconds=30),
            loss_recover_ratio=0.75,
        )
        self.assertFalse(recovered["active"])
        self.assertFalse(recovered["disarmed"])
        self.assertEqual(recovered["reason"], "recovered")
        self.assertNotIn("hard_loss_forced_reduce_episode", state)

    def test_loss_recovery_brush_keeps_tiny_entries_until_hard_loss(self) -> None:
        report = resolve_loss_recovery_brush(
            enabled=True,
            strategy_mode="synthetic_neutral",
            current_long_notional=540.0,
            current_short_notional=0.0,
            unrealized_pnl=-12.0,
            hard_loss_forced_reduce={"active": False},
            hard_unrealized_loss_limit=80.0,
            per_order_notional=20.0,
            entry_notional=6.0,
            min_unrealized_loss=2.0,
            max_entry_orders_per_side=1,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["side"], "long")
        self.assertAlmostEqual(report["same_side_probe_scale"], 0.3)
        self.assertEqual(report["max_entry_long_orders"], 1)
        self.assertEqual(report["max_entry_short_orders"], 1)
        self.assertTrue(report["allow_opposite_entry_with_single_side_inventory"])

    def test_loss_recovery_brush_yields_to_hard_loss_reduce(self) -> None:
        report = resolve_loss_recovery_brush(
            enabled=True,
            strategy_mode="synthetic_neutral",
            current_long_notional=540.0,
            current_short_notional=0.0,
            unrealized_pnl=-90.0,
            hard_loss_forced_reduce={"active": True},
            hard_unrealized_loss_limit=80.0,
            per_order_notional=20.0,
            entry_notional=6.0,
            min_unrealized_loss=2.0,
            max_entry_orders_per_side=1,
        )

        self.assertFalse(report["active"])
        self.assertEqual(report["reason"], "hard_loss_active")

    def test_apply_entry_permission_gate_prunes_short_entries_only(self) -> None:
        plan = {
            "bootstrap_orders": [
                {"side": "SELL", "role": "bootstrap_short"},
                {"side": "BUY", "role": "bootstrap_long"},
            ],
            "buy_orders": [
                {"side": "BUY", "role": "take_profit_short"},
                {"side": "BUY", "role": "entry_long"},
            ],
            "sell_orders": [
                {"side": "SELL", "role": "entry_short"},
                {"side": "SELL", "role": "take_profit_long"},
            ],
        }

        report = apply_entry_permission_gate(plan, allow_entry_short=False)

        self.assertTrue(report["applied"])
        self.assertEqual(report["pruned_bootstrap_orders"], 1)
        self.assertEqual(report["pruned_sell_orders"], 1)
        self.assertEqual(plan["bootstrap_orders"], [{"side": "BUY", "role": "bootstrap_long"}])
        self.assertEqual(
            plan["buy_orders"],
            [{"side": "BUY", "role": "take_profit_short"}, {"side": "BUY", "role": "entry_long"}],
        )
        self.assertEqual(plan["sell_orders"], [{"side": "SELL", "role": "take_profit_long"}])

    def test_apply_entry_permission_gate_prunes_long_entries_only(self) -> None:
        plan = {
            "bootstrap_orders": [
                {"side": "SELL", "role": "bootstrap_short"},
                {"side": "BUY", "role": "bootstrap_long"},
            ],
            "buy_orders": [
                {"side": "BUY", "role": "entry_long"},
                {"side": "BUY", "role": "take_profit_short"},
            ],
            "sell_orders": [
                {"side": "SELL", "role": "entry_short"},
                {"side": "SELL", "role": "take_profit_long"},
            ],
        }

        report = apply_entry_permission_gate(plan, allow_entry_long=False)

        self.assertTrue(report["applied"])
        self.assertEqual(report["pruned_bootstrap_orders"], 1)
        self.assertEqual(report["pruned_buy_orders"], 1)
        self.assertEqual(plan["bootstrap_orders"], [{"side": "SELL", "role": "bootstrap_short"}])
        self.assertEqual(plan["buy_orders"], [{"side": "BUY", "role": "take_profit_short"}])
        self.assertEqual(
            plan["sell_orders"],
            [{"side": "SELL", "role": "entry_short"}, {"side": "SELL", "role": "take_profit_long"}],
        )

    def test_unrealized_loss_entry_guard_prunes_entries_but_keeps_reducers(self) -> None:
        guard = assess_unrealized_loss_entry_guard(
            enabled=True,
            unrealized_pnl=-6.0,
            current_long_notional=300.0,
            current_short_notional=0.0,
            min_loss=3.0,
            loss_ratio=0.015,
        )
        plan = {
            "bootstrap_orders": [{"side": "BUY", "role": "bootstrap_long"}],
            "buy_orders": [{"side": "BUY", "role": "entry_long"}],
            "sell_orders": [
                {"side": "SELL", "role": "entry_short"},
                {"side": "SELL", "role": "take_profit_long"},
            ],
        }

        self.assertTrue(guard["active"])
        report = apply_entry_permission_gate(plan, allow_entry_long=False, allow_entry_short=False)

        self.assertTrue(report["applied"])
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [{"side": "SELL", "role": "take_profit_long"}])

    def test_loss_reduce_reentry_guard_blocks_long_rebuy_after_adverse_reduce(self) -> None:
        now = datetime(2026, 5, 20, 4, 10, tzinfo=timezone.utc)
        state: dict[str, object] = {}

        guard = resolve_loss_reduce_reentry_guard(
            state=state,
            enabled=True,
            adverse_inventory_reduce={
                "enabled": True,
                "long_active": True,
                "placed_reduce_orders": 1,
                "forced_reduce_orders": [{"side": "SELL", "price": 0.5660}],
            },
            hard_loss_forced_reduce={},
            current_long_notional=190.0,
            current_short_notional=0.0,
            mid_price=0.5658,
            effective_step_price=0.0001,
            now=now,
            cooldown_seconds=300.0,
            recover_buffer_steps=1.0,
        )

        self.assertTrue(guard["active"])
        self.assertTrue(guard["block_long_entries"])
        self.assertFalse(guard["block_short_entries"])
        self.assertEqual(guard["side"], "SELL")
        self.assertEqual(guard["source"], "adverse_reduce")

        still_blocked = resolve_loss_reduce_reentry_guard(
            state=state,
            enabled=True,
            adverse_inventory_reduce={"enabled": True, "placed_reduce_orders": 0},
            hard_loss_forced_reduce={},
            current_long_notional=180.0,
            current_short_notional=0.0,
            mid_price=0.5659,
            effective_step_price=0.0001,
            now=now + timedelta(seconds=120),
            cooldown_seconds=300.0,
            recover_buffer_steps=1.0,
        )

        self.assertTrue(still_blocked["active"])
        self.assertTrue(still_blocked["block_long_entries"])
        self.assertGreater(still_blocked["min_remaining_seconds"], 0)

        recovered = resolve_loss_reduce_reentry_guard(
            state=state,
            enabled=True,
            adverse_inventory_reduce={"enabled": True, "placed_reduce_orders": 0},
            hard_loss_forced_reduce={},
            current_long_notional=180.0,
            current_short_notional=0.0,
            mid_price=0.5662,
            effective_step_price=0.0001,
            now=now + timedelta(seconds=301),
            cooldown_seconds=300.0,
            recover_buffer_steps=1.0,
        )

        self.assertFalse(recovered["active"])
        self.assertEqual(recovered["reason"], "recovered")
        self.assertNotIn("loss_reduce_reentry_guard", state)

    def test_loss_reduce_reentry_guard_action_filter_keeps_reducers(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "role": "best_quote_entry_long", "qty": 10, "price": 0.565, "notional": 5.65},
                {"side": "SELL", "role": "take_profit_long", "qty": 10, "price": 0.567, "notional": 5.67},
                {"side": "SELL", "role": "best_quote_entry_short", "qty": 10, "price": 0.568, "notional": 5.68},
            ],
            "place_count": 3,
        }

        filtered = apply_loss_reduce_reentry_guard_to_actions(
            actions=actions,
            plan_report={
                "loss_reduce_reentry_guard": {
                    "active": True,
                    "block_long_entries": True,
                    "block_short_entries": False,
                    "reason": "loss_reduce_reentry_cooldown",
                }
            },
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual([order["role"] for order in filtered["place_orders"]], ["take_profit_long", "best_quote_entry_short"])
        self.assertEqual(filtered["loss_reduce_reentry_guard"]["dropped_order_count"], 1)

    def test_loss_recovery_brush_allows_small_ordinary_cross_reducer(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "role": "entry_long",
                    "qty": 39.0,
                    "price": 0.58,
                    "notional": 22.62,
                }
            ],
            "place_count": 1,
        }

        filtered = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report={
                "actual_net_qty": -58.0,
                "unrealized_pnl": -0.29,
                "current_short_avg_price": 0.57505,
                "take_profit_min_profit_ratio": 0.0007,
                "step_price": 0.00128,
                "mid_price": 0.58005,
                "loss_inventory_no_cross_small_entry_notional": 10.0,
                "loss_recovery_brush": {
                    "active": True,
                    "entry_notional": 10.0,
                },
                "symbol_info": {
                    "min_notional": 5.0,
                    "step_size": 1.0,
                    "min_qty": 1.0,
                },
            },
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(filtered["place_count"], 1)
        order = filtered["place_orders"][0]
        self.assertEqual(order["role"], "entry_long")
        self.assertEqual(order["force_reduce_only"], True)
        self.assertLessEqual(order["notional"], 10.0)
        self.assertEqual(order["loss_inventory_no_cross_guard"], "short_small_loss_reduce_resized")

    def test_apply_entry_permission_gate_limits_entry_order_count(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {"side": "BUY", "role": "entry_long", "price": 1.0},
                {"side": "BUY", "role": "take_profit_short", "price": 1.1},
                {"side": "BUY", "role": "entry_long", "price": 0.9},
                {"side": "BUY", "role": "entry_long", "price": 0.8},
            ],
            "sell_orders": [
                {"side": "SELL", "role": "entry_short", "price": 1.2},
                {"side": "SELL", "role": "entry_short", "price": 1.3},
                {"side": "SELL", "role": "take_profit_long", "price": 1.1},
            ],
        }

        report = apply_entry_permission_gate(
            plan,
            max_entry_long_orders=1,
            max_entry_short_orders=1,
        )

        self.assertTrue(report["applied"])
        self.assertEqual(report["pruned_buy_orders"], 2)
        self.assertEqual(report["pruned_sell_orders"], 1)
        self.assertEqual(
            plan["buy_orders"],
            [
                {"side": "BUY", "role": "entry_long", "price": 1.0},
                {"side": "BUY", "role": "take_profit_short", "price": 1.1},
            ],
        )
        self.assertEqual(
            plan["sell_orders"],
            [
                {"side": "SELL", "role": "entry_short", "price": 1.2},
                {"side": "SELL", "role": "take_profit_long", "price": 1.1},
            ],
        )

    def test_regime_budget_entry_reuse_tolerance_allows_small_drift_for_v2(self) -> None:
        tolerance, reason = _regime_budget_entry_reuse_tolerance(
            strategy_profile="billusdt_neutral_regime_budget_ping_pong_v2",
            regime_entry_budget={
                "enabled": True,
                "report_only": False,
                "state": "ping-pong-fast",
                "cancel_entry_required": False,
                "shock_guard_active": False,
            },
            step_price=0.0005,
        )

        self.assertAlmostEqual(tolerance, 0.001125)
        self.assertEqual(reason, "regime_budget_small_drift_reuse")

    def test_regime_budget_entry_reuse_tolerance_uses_enabled_budget_with_legacy_profile(self) -> None:
        tolerance, reason = _regime_budget_entry_reuse_tolerance(
            strategy_profile="billusdt_competition_neutral_ping_pong_v1",
            regime_entry_budget={
                "enabled": True,
                "report_only": False,
                "state": "ping-pong-safe",
                "cancel_entry_required": False,
                "shock_guard_active": False,
            },
            step_price=0.0005,
        )

        self.assertAlmostEqual(tolerance, 0.001125)
        self.assertEqual(reason, "regime_budget_small_drift_reuse")

    def test_regime_budget_entry_reuse_tolerance_blocks_during_switch(self) -> None:
        tolerance, reason = _regime_budget_entry_reuse_tolerance(
            strategy_profile="billusdt_neutral_regime_budget_ping_pong_v2",
            regime_entry_budget={
                "enabled": True,
                "report_only": False,
                "state": "ping-pong-fast",
                "cancel_entry_required": True,
                "shock_guard_active": False,
            },
            step_price=0.0005,
        )

        self.assertEqual(tolerance, 0.0)
        self.assertEqual(reason, "cancel_confirm_required")

    def test_take_profit_guard_blocks_untracked_reducers_when_cost_basis_missing(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {"side": "BUY", "price": 0.99, "qty": 10.0, "notional": 9.9, "role": "entry_long"},
                {"side": "BUY", "price": 1.01, "qty": 10.0, "notional": 10.1, "role": "take_profit_short"},
                {"side": "BUY", "price": 1.02, "qty": 10.0, "notional": 10.2, "role": "active_delever_short"},
            ],
            "sell_orders": [
                {"side": "SELL", "price": 1.01, "qty": 10.0, "notional": 10.1, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.02, "qty": 10.0, "notional": 10.2, "role": "active_delever_long"},
                {"side": "SELL", "price": 1.03, "qty": 10.0, "notional": 10.3, "role": "entry_short"},
            ],
        }

        report = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=100.0,
            current_short_qty=50.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.0,
            current_long_notional=100.0,
            current_short_notional=50.0,
            pause_long_position_notional=None,
            pause_short_position_notional=None,
            min_profit_ratio=None,
            tick_size=0.01,
            bid_price=1.00,
            ask_price=1.01,
        )

        self.assertTrue(report["enabled"])
        self.assertTrue(report["long_cost_basis_missing"])
        self.assertTrue(report["short_cost_basis_missing"])
        self.assertEqual(report["dropped_sell_orders"], 2)
        self.assertEqual(report["dropped_buy_orders"], 2)
        self.assertEqual([item["role"] for item in plan["sell_orders"]], ["entry_short"])
        self.assertEqual([item["role"] for item in plan["buy_orders"]], ["entry_long"])

    def test_apply_adverse_inventory_reduce_places_short_reduce_order(self) -> None:
        report = assess_adverse_inventory_reduce(
            enabled=True,
            mid_price=0.196,
            current_long_qty=0.0,
            current_long_notional=0.0,
            current_short_qty=2200.0,
            current_short_notional=431.2,
            current_long_cost_price=0.0,
            current_short_cost_price=0.190,
            current_long_cost_basis_source=None,
            current_short_cost_basis_source="synthetic_ledger",
            pause_long_position_notional=700.0,
            pause_short_position_notional=420.0,
            long_trigger_ratio=0.01,
            short_trigger_ratio=0.008,
        )

        self.assertTrue(report["short_active"])
        plan = {"bootstrap_orders": [], "buy_orders": [], "sell_orders": []}
        updated = apply_adverse_inventory_reduce(
            plan=plan,
            state={},
            report=report,
            now=datetime(2026, 4, 24, 3, 0, tzinfo=timezone.utc),
            current_long_qty=0.0,
            current_long_notional=0.0,
            current_short_qty=2200.0,
            current_short_notional=431.2,
            pause_long_position_notional=700.0,
            pause_short_position_notional=420.0,
            target_ratio=0.65,
            max_order_notional=120.0,
            bid_price=0.1959,
            ask_price=0.1960,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            maker_timeout_seconds=30.0,
        )

        self.assertEqual(updated["direction"], "short")
        self.assertEqual(updated["placed_reduce_orders"], 1)
        self.assertEqual(plan["buy_orders"][0]["side"], "BUY")
        self.assertTrue(plan["buy_orders"][0]["force_reduce_only"])
        self.assertEqual(plan["buy_orders"][0]["role"], "adverse_reduce_short")
        self.assertLessEqual(plan["buy_orders"][0]["notional"], 120.0)

    def test_apply_adverse_inventory_reduce_uses_hedge_position_side(self) -> None:
        report = assess_adverse_inventory_reduce(
            enabled=True,
            mid_price=0.59845,
            current_long_qty=888.0,
            current_long_notional=531.42,
            current_short_qty=0.0,
            current_short_notional=0.0,
            current_long_cost_price=0.60478,
            current_short_cost_price=0.0,
            current_long_cost_basis_source="actual_position_long",
            current_short_cost_basis_source=None,
            pause_long_position_notional=520.0,
            pause_short_position_notional=520.0,
            long_trigger_ratio=0.004,
            short_trigger_ratio=0.004,
        )

        self.assertTrue(report["long_active"])
        plan = {"bootstrap_orders": [], "buy_orders": [], "sell_orders": []}
        updated = apply_adverse_inventory_reduce(
            plan=plan,
            state={},
            report=report,
            now=datetime(2026, 5, 20, 8, 30, tzinfo=timezone.utc),
            current_long_qty=888.0,
            current_long_notional=531.42,
            current_short_qty=0.0,
            current_short_notional=0.0,
            pause_long_position_notional=520.0,
            pause_short_position_notional=520.0,
            target_ratio=0.35,
            max_order_notional=10.0,
            bid_price=0.5984,
            ask_price=0.5985,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            maker_timeout_seconds=30.0,
            strategy_mode="hedge_neutral",
        )

        self.assertEqual(updated["placed_reduce_orders"], 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "adverse_reduce_long")
        self.assertEqual(plan["sell_orders"][0]["position_side"], "LONG")

    def test_apply_adverse_inventory_reduce_probes_short_below_pause_when_adverse(self) -> None:
        report = assess_adverse_inventory_reduce(
            enabled=True,
            mid_price=0.0936,
            current_long_qty=0.0,
            current_long_notional=0.0,
            current_short_qty=4025.0,
            current_short_notional=376.74,
            current_long_cost_price=0.0,
            current_short_cost_price=0.0844,
            current_long_cost_basis_source=None,
            current_short_cost_basis_source="synthetic_ledger",
            pause_long_position_notional=380.0,
            pause_short_position_notional=420.0,
            long_trigger_ratio=0.008,
            short_trigger_ratio=0.006,
            target_ratio=0.58,
        )

        self.assertTrue(report["short_active"])
        self.assertEqual(report["short_activation_mode"], "below_pause_probe")
        plan = {"bootstrap_orders": [], "buy_orders": [], "sell_orders": []}
        updated = apply_adverse_inventory_reduce(
            plan=plan,
            state={},
            report=report,
            now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
            current_long_qty=0.0,
            current_long_notional=0.0,
            current_short_qty=4025.0,
            current_short_notional=376.74,
            pause_long_position_notional=380.0,
            pause_short_position_notional=420.0,
            target_ratio=0.58,
            max_order_notional=110.0,
            bid_price=0.0935,
            ask_price=0.0936,
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            maker_timeout_seconds=24.0,
        )

        self.assertEqual(updated["placed_reduce_orders"], 1)
        self.assertAlmostEqual(updated["short_target_notional"], 243.6, places=8)
        self.assertGreater(updated["short_reduce_notional"], 0.0)
        self.assertLessEqual(plan["buy_orders"][0]["notional"], 110.0)
        self.assertEqual(plan["buy_orders"][0]["price"], 0.0935)

    def test_best_quote_adverse_reduce_does_not_probe_short_below_soft_pause(self) -> None:
        report = assess_adverse_inventory_reduce(
            enabled=True,
            mid_price=0.156725,
            current_long_qty=0.0,
            current_long_notional=0.0,
            current_short_qty=20040.0,
            current_short_notional=3140.7690,
            current_long_cost_price=0.0,
            current_short_cost_price=0.1560144164811,
            current_long_cost_basis_source=None,
            current_short_cost_basis_source="actual_position",
            pause_long_position_notional=4950.0,
            pause_short_position_notional=4950.0,
            long_trigger_ratio=0.002,
            short_trigger_ratio=0.002,
            target_ratio=0.45,
            allow_short_below_pause_probe=False,
        )

        self.assertFalse(report["short_active"])
        self.assertFalse(report["active"])
        self.assertIsNone(report["short_activation_mode"])

    def test_adverse_inventory_reduce_does_not_probe_long_below_pause(self) -> None:
        report = assess_adverse_inventory_reduce(
            enabled=True,
            mid_price=0.1830,
            current_long_qty=3219.0,
            current_long_notional=589.0,
            current_short_qty=0.0,
            current_short_notional=0.0,
            current_long_cost_price=0.2020,
            current_short_cost_price=0.0,
            current_long_cost_basis_source="synthetic_ledger",
            current_short_cost_basis_source=None,
            pause_long_position_notional=1200.0,
            pause_short_position_notional=420.0,
            long_trigger_ratio=0.006,
            short_trigger_ratio=0.006,
            target_ratio=0.30,
        )

        self.assertFalse(report["active"])
        self.assertFalse(report["long_active"])
        self.assertIsNone(report["long_activation_mode"])

    def test_best_quote_adverse_reduce_can_use_soft_pause_threshold(self) -> None:
        args = SimpleNamespace(
            best_quote_maker_volume_inventory_soft_ratio=0.55,
            best_quote_maker_volume_max_long_notional=5500.0,
            best_quote_maker_volume_max_short_notional=5500.0,
        )
        pause_notional = _resolve_inventory_unlock_pause_notional(
            args=args,
            strategy_mode="best_quote_maker_volume_v1",
            side="long",
            fallback_pause_notional=4015.0,
            pending_entry_buffer_notional=300.0,
        )

        report = assess_adverse_inventory_reduce(
            enabled=_is_best_quote_maker_volume_mode("best_quote_maker_volume_v1"),
            mid_price=0.994,
            current_long_qty=2850.0,
            current_long_notional=2832.9,
            current_short_qty=0.0,
            current_short_notional=0.0,
            current_long_cost_price=1.0,
            current_short_cost_price=0.0,
            current_long_cost_basis_source="current_avg_price",
            current_short_cost_basis_source=None,
            pause_long_position_notional=pause_notional,
            pause_short_position_notional=4015.0,
            long_trigger_ratio=0.004,
            short_trigger_ratio=0.004,
            target_ratio=0.45,
        )

        self.assertAlmostEqual(pause_notional or 0.0, 2725.0)
        self.assertTrue(report["long_active"])
        self.assertEqual(report["long_activation_mode"], "pause")

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_best_quote_adverse_reduce_waits_until_soft_inventory(
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
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.14310", "ask_price": "0.14311"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "900", "entryPrice": "0.14400"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(
                tmpdir,
                symbol="BILLUSDT",
                strategy_mode="best_quote_maker_volume_v1",
                strategy_profile="billusdt_best_quote_maker_volume_reset_v1",
                step_price=0.00019,
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=260.0,
                best_quote_maker_volume_quote_offset_ticks=20,
                best_quote_maker_volume_defensive_offset_ticks=30,
                best_quote_maker_volume_max_long_notional=5500.0,
                best_quote_maker_volume_max_short_notional=5500.0,
                best_quote_maker_volume_inventory_soft_ratio=0.55,
                adverse_reduce_enabled=True,
                adverse_reduce_long_trigger_ratio=0.006,
                adverse_reduce_short_trigger_ratio=0.006,
                adverse_reduce_target_ratio=0.45,
                adverse_reduce_max_order_notional=130.0,
                adverse_reduce_maker_timeout_seconds=35.0,
                take_profit_min_profit_ratio=0.0003,
                reset_state=True,
            )

            report = generate_plan_report(args)

        adverse = report["adverse_inventory_reduce"]
        self.assertTrue(adverse["enabled"])
        self.assertFalse(adverse["long_active"])
        self.assertEqual(adverse["placed_reduce_orders"], 0)
        self.assertEqual(report["forced_reduce_orders"], [])

    def test_take_profit_guard_defaults_to_break_even_when_ratio_is_unset(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.04, "qty": 10.0, "notional": 10.4, "role": "take_profit_long"},
            ],
        }

        report = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=100.0,
            current_short_qty=0.0,
            current_long_avg_price=1.05,
            current_short_avg_price=0.0,
            current_long_notional=100.0,
            current_short_notional=0.0,
            pause_long_position_notional=None,
            pause_short_position_notional=None,
            min_profit_ratio=None,
            tick_size=0.01,
            bid_price=1.00,
            ask_price=1.01,
        )

        self.assertTrue(report["long_active"])
        self.assertEqual(report["adjusted_sell_orders"], 1)
        self.assertAlmostEqual(plan["sell_orders"][0]["price"], 1.05)

    def test_inventory_unlock_release_waits_for_stalled_cycles_then_adds_reduce_only_sell(self) -> None:
        plan = {"buy_orders": [], "sell_orders": []}
        state = {"inventory_unlock_release": {"side": "long", "stall_count": 2}}

        report = apply_inventory_unlock_release(
            plan=plan,
            state=state,
            side="long",
            entry_paused=True,
            take_profit_guard={"enabled": True, "long_active": True, "long_floor_price": 1.05},
            current_qty=2500.0,
            current_notional=2500.0,
            pause_notional=2000.0,
            release_cap_notional=300.0,
            per_order_notional=120.0,
            step_price=0.001,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            bid_price=0.998,
            ask_price=1.0,
        )

        release_orders = [order for order in plan["sell_orders"] if order["role"] == "inventory_unlock_reduce_long"]
        self.assertTrue(report["active"])
        self.assertEqual(report["side"], "long")
        self.assertEqual(report["stall_count"], 3)
        self.assertEqual(len(release_orders), 1)
        self.assertEqual(release_orders[0]["side"], "SELL")
        self.assertTrue(release_orders[0]["force_reduce_only"])
        self.assertEqual(release_orders[0]["time_in_force"], "GTX")
        self.assertLessEqual(release_orders[0]["notional"], 300.0)
        self.assertLess(release_orders[0]["price"], 1.05)

    def test_inventory_unlock_release_does_not_fire_before_stall_confirmation(self) -> None:
        plan = {"buy_orders": [], "sell_orders": []}
        state: dict[str, object] = {}

        report = apply_inventory_unlock_release(
            plan=plan,
            state=state,
            side="long",
            entry_paused=True,
            take_profit_guard={"enabled": True, "long_active": True, "long_floor_price": 1.05},
            current_qty=2500.0,
            current_notional=2500.0,
            pause_notional=2000.0,
            release_cap_notional=300.0,
            per_order_notional=120.0,
            step_price=0.001,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            bid_price=0.998,
            ask_price=1.0,
        )

        self.assertFalse(report["active"])
        self.assertEqual(report["stall_count"], 1)
        self.assertEqual(plan["sell_orders"], [])

    def test_best_quote_inventory_unlock_uses_soft_pause_threshold(self) -> None:
        args = SimpleNamespace(
            best_quote_maker_volume_inventory_soft_ratio=0.55,
            best_quote_maker_volume_max_long_notional=5500.0,
            best_quote_maker_volume_max_short_notional=5500.0,
        )

        short_pause = _resolve_inventory_unlock_pause_notional(
            args=args,
            strategy_mode="best_quote_maker_volume_v1",
            side="short",
            fallback_pause_notional=4015.0,
            pending_entry_buffer_notional=300.0,
        )
        long_pause = _resolve_inventory_unlock_pause_notional(
            args=args,
            strategy_mode="best_quote_maker_volume_v1",
            side="long",
            fallback_pause_notional=4015.0,
            pending_entry_buffer_notional=300.0,
        )

        self.assertAlmostEqual(short_pause or 0.0, 2725.0)
        self.assertAlmostEqual(long_pause or 0.0, 2725.0)

    def test_inventory_unlock_release_fires_for_best_quote_short_soft_stall(self) -> None:
        plan = {"buy_orders": [], "sell_orders": []}
        state = {"inventory_unlock_release": {"side": "short", "stall_count": 2}}
        args = SimpleNamespace(
            best_quote_maker_volume_inventory_soft_ratio=0.55,
            best_quote_maker_volume_max_long_notional=5500.0,
            best_quote_maker_volume_max_short_notional=5500.0,
        )
        pause_notional = _resolve_inventory_unlock_pause_notional(
            args=args,
            strategy_mode="best_quote_maker_volume_v1",
            side="short",
            fallback_pause_notional=4015.0,
            pending_entry_buffer_notional=300.0,
        )

        report = apply_inventory_unlock_release(
            plan=plan,
            state=state,
            side="short",
            entry_paused=True,
            take_profit_guard={"enabled": True, "short_active": True, "short_ceiling_price": 0.14095},
            current_qty=21500.0,
            current_notional=3054.0,
            pause_notional=pause_notional,
            release_cap_notional=733.0,
            per_order_notional=600.0,
            step_price=0.00019,
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            bid_price=0.14205,
            ask_price=0.14206,
        )

        release_orders = [order for order in plan["buy_orders"] if order["role"] == "inventory_unlock_reduce_short"]
        self.assertTrue(report["active"])
        self.assertEqual(report["side"], "short")
        self.assertEqual(report["stall_count"], 3)
        self.assertEqual(len(release_orders), 1)
        self.assertEqual(release_orders[0]["side"], "BUY")
        self.assertTrue(release_orders[0]["force_reduce_only"])
        self.assertEqual(release_orders[0]["time_in_force"], "GTX")
        self.assertEqual(release_orders[0]["execution_type"], "inventory_unlock_release")
        self.assertGreater(release_orders[0]["price"], 0.14095)
        self.assertLessEqual(release_orders[0]["notional"], 733.0)

    def test_inventory_unlock_release_blocks_same_side_reentry_after_release(self) -> None:
        plan = {
            "buy_orders": [
                {"side": "BUY", "price": 0.9995, "qty": 700.0, "notional": 699.65, "role": "best_quote_entry_long"},
            ],
            "sell_orders": [
                {"side": "SELL", "price": 1.01, "qty": 800.0, "notional": 808.0, "role": "best_quote_entry_short"},
            ],
        }
        state = {
            "inventory_unlock_release": {
                "side": "long",
                "stall_count": 3,
                "reentry_cooldown_cycles": 4,
                "target_notional": 3200.0,
                "release_price": 1.0000,
            }
        }

        report = apply_inventory_unlock_release(
            plan=plan,
            state=state,
            side="long",
            entry_paused=False,
            take_profit_guard={"enabled": True, "long_active": True, "long_floor_price": 1.05},
            current_qty=3600.0,
            current_notional=3100.0,
            pause_notional=4000.0,
            release_cap_notional=700.0,
            per_order_notional=1100.0,
            step_price=0.001,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            bid_price=0.998,
            ask_price=1.0,
        )

        self.assertFalse(report["active"])
        self.assertTrue(report["reentry_block_active"])
        self.assertEqual(report["blocked_entry_order_count"], 1)
        self.assertEqual(report["reason"], "reentry_price_gate")
        self.assertAlmostEqual(report["reentry_gate_price"], 0.999)
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)

    def test_inventory_unlock_release_allows_reentry_after_grid_profit_gap(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.0005, "qty": 800.0, "notional": 800.4, "role": "best_quote_entry_short"},
                {"side": "SELL", "price": 1.0010, "qty": 800.0, "notional": 800.8, "role": "best_quote_entry_short"},
            ],
        }
        state = {
            "inventory_unlock_release": {
                "side": "short",
                "stall_count": 3,
                "reentry_cooldown_cycles": 4,
                "target_notional": 3200.0,
                "release_price": 1.0000,
            }
        }

        report = apply_inventory_unlock_release(
            plan=plan,
            state=state,
            side="short",
            entry_paused=False,
            take_profit_guard={"enabled": True, "short_active": True, "short_ceiling_price": 0.99},
            current_qty=3600.0,
            current_notional=3100.0,
            pause_notional=4000.0,
            release_cap_notional=700.0,
            per_order_notional=1100.0,
            step_price=0.001,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            bid_price=1.0004,
            ask_price=1.0005,
        )

        self.assertFalse(report["active"])
        self.assertTrue(report["reentry_block_active"])
        self.assertEqual(report["blocked_entry_order_count"], 1)
        self.assertAlmostEqual(report["reentry_gate_price"], 1.001)
        self.assertEqual([item["price"] for item in plan["sell_orders"]], [1.0010])

    def test_synthetic_resync_does_not_use_mid_price_as_nonzero_cost_basis(self) -> None:
        resolved = _resolve_synthetic_resync_price(
            actual_position_qty=300.0,
            entry_price=0.0,
            fallback_price=1.20,
            snapshot={"virtual_long_avg_price": 0.0, "virtual_short_avg_price": 0.0},
        )

        self.assertEqual(resolved, 0.0)

    def test_bard_profile_has_human_readable_label(self) -> None:
        self.assertEqual(AUTO_REGIME_PROFILE_LABELS["bard_12h_push_neutral_v2"], "通用刷量V1")

    def test_build_parser_accepts_competition_inventory_grid_mode(self) -> None:
        parser = _build_parser()

        args = parser.parse_args(
            [
                "--strategy-mode",
                "competition_inventory_grid",
                "--first-order-multiplier",
                "4.0",
                "--threshold-position-notional",
                "50.0",
                "--max-order-position-notional",
                "80.0",
            ]
        )

        self.assertEqual(args.strategy_mode, "competition_inventory_grid")
        self.assertEqual(args.first_order_multiplier, 4.0)
        self.assertEqual(args.threshold_position_notional, 50.0)
        self.assertEqual(args.max_order_position_notional, 80.0)

    def test_build_parser_defaults_disable_threshold_delever(self) -> None:
        parser = _build_parser()

        args = parser.parse_args([])

        self.assertEqual(args.threshold_position_notional, 0.0)
        self.assertIsNone(args.take_profit_min_profit_ratio)

    def test_build_parser_accepts_near_market_and_fast_catchup_args(self) -> None:
        parser = _build_parser()

        args = parser.parse_args(
            [
                "--near-market-entry-max-center-distance-steps",
                "4.5",
                "--grid-inventory-rebalance-min-center-distance-steps",
                "6.0",
                "--near-market-reentry-confirm-cycles",
                "3",
                "--neutral-center-interval-minutes",
                "30",
                "--synthetic-center-fast-catchup-trigger-steps",
                "6",
                "--synthetic-center-fast-catchup-confirm-cycles",
                "3",
                "--synthetic-center-fast-catchup-shift-steps",
                "2",
            ]
        )

        self.assertEqual(args.near_market_entry_max_center_distance_steps, 4.5)
        self.assertEqual(args.grid_inventory_rebalance_min_center_distance_steps, 6.0)
        self.assertEqual(args.near_market_reentry_confirm_cycles, 3)
        self.assertEqual(args.neutral_center_interval_minutes, 30)
        self.assertEqual(args.synthetic_center_fast_catchup_trigger_steps, 6.0)
        self.assertEqual(args.synthetic_center_fast_catchup_confirm_cycles, 3)
        self.assertEqual(args.synthetic_center_fast_catchup_shift_steps, 2)

    def test_build_parser_accepts_synthetic_flow_sleeve_args(self) -> None:
        parser = _build_parser()

        args = parser.parse_args(
            [
                "--synthetic-flow-sleeve-enabled",
                "--synthetic-flow-sleeve-trigger-notional",
                "180",
                "--synthetic-flow-sleeve-notional",
                "120",
                "--synthetic-flow-sleeve-levels",
                "3",
                "--synthetic-flow-sleeve-order-notional",
                "45",
                "--synthetic-flow-sleeve-max-loss-ratio",
                "0.003",
            ]
        )

        self.assertTrue(args.synthetic_flow_sleeve_enabled)
        self.assertEqual(args.synthetic_flow_sleeve_trigger_notional, 180)
        self.assertEqual(args.synthetic_flow_sleeve_notional, 120)
        self.assertEqual(args.synthetic_flow_sleeve_levels, 3)
        self.assertEqual(args.synthetic_flow_sleeve_order_notional, 45)
        self.assertEqual(args.synthetic_flow_sleeve_max_loss_ratio, 0.003)

    def test_build_parser_accepts_volume_long_v4_flow_sleeve_args(self) -> None:
        parser = _build_parser()

        args = parser.parse_args(
            [
                "--volume-long-v4-flow-sleeve-enabled",
                "--volume-long-v4-flow-sleeve-trigger-notional",
                "820",
                "--volume-long-v4-flow-sleeve-reduce-to-notional",
                "720",
                "--volume-long-v4-flow-sleeve-notional",
                "180",
                "--volume-long-v4-flow-sleeve-levels",
                "3",
                "--volume-long-v4-flow-sleeve-order-notional",
                "60",
                "--volume-long-v4-flow-sleeve-max-loss-ratio",
                "0.012",
                "--volume-long-v4-soft-loss-steps",
                "10",
                "--volume-long-v4-hard-loss-steps",
                "18",
            ]
        )

        self.assertTrue(args.volume_long_v4_flow_sleeve_enabled)
        self.assertEqual(args.volume_long_v4_flow_sleeve_trigger_notional, 820)
        self.assertEqual(args.volume_long_v4_flow_sleeve_reduce_to_notional, 720)
        self.assertEqual(args.volume_long_v4_flow_sleeve_notional, 180)
        self.assertEqual(args.volume_long_v4_flow_sleeve_levels, 3)
        self.assertEqual(args.volume_long_v4_flow_sleeve_order_notional, 60)
        self.assertEqual(args.volume_long_v4_flow_sleeve_max_loss_ratio, 0.012)
        self.assertEqual(args.volume_long_v4_soft_loss_steps, 10)
        self.assertEqual(args.volume_long_v4_hard_loss_steps, 18)

    def test_synthetic_flow_sleeve_adds_reduce_only_long_flow_exits(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "bootstrap_orders": []}

        report = apply_synthetic_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_notional=220,
            current_short_notional=0,
            current_long_avg_price=0.165,
            current_short_avg_price=0,
            trigger_notional=180,
            sleeve_notional=120,
            levels=3,
            per_order_notional=45,
            order_notional=None,
            max_loss_ratio=0.003,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1,
            min_qty=1,
            min_notional=5,
            bid_price=0.1653,
            ask_price=0.1654,
            buy_offset_steps=0.25,
            sell_offset_steps=0.25,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["direction"], "long_inventory_sell_flow")
        self.assertEqual(report["placed_order_count"], 3)
        self.assertLessEqual(report["placed_notional"], 120)
        self.assertEqual(len(plan["sell_orders"]), 3)
        self.assertTrue(all(item["role"] == "flow_sleeve_long" for item in plan["sell_orders"]))
        self.assertTrue(all(item["force_reduce_only"] for item in plan["sell_orders"]))
        self.assertTrue(all(item["flow_sleeve"] for item in plan["sell_orders"]))

    def test_synthetic_flow_sleeve_clamps_long_flow_exits_to_loss_floor(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "bootstrap_orders": []}

        report = apply_synthetic_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_notional=220,
            current_short_notional=0,
            current_long_avg_price=0.17,
            current_short_avg_price=0,
            trigger_notional=180,
            sleeve_notional=120,
            levels=3,
            per_order_notional=45,
            order_notional=None,
            max_loss_ratio=0.003,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1,
            min_qty=1,
            min_notional=5,
            bid_price=0.1653,
            ask_price=0.1654,
            buy_offset_steps=0.25,
            sell_offset_steps=0.25,
        )

        self.assertTrue(report["active"])
        self.assertIsNone(report["blocked_reason"])
        self.assertEqual(report["loss_floor_price"], 0.1695)
        self.assertEqual(report["loss_guard_clamped_order_count"], 3)
        self.assertEqual([item["price"] for item in plan["sell_orders"]], [0.1695])

    def test_synthetic_flow_sleeve_uses_recent_lots_for_near_market_long_flow(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "bootstrap_orders": []}

        report = apply_synthetic_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_notional=636.0,
            current_short_notional=0,
            current_long_avg_price=0.2004,
            current_short_avg_price=0,
            trigger_notional=100,
            sleeve_notional=360,
            levels=4,
            per_order_notional=130,
            order_notional=120,
            max_loss_ratio=0.003,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1,
            min_qty=1,
            min_notional=5,
            bid_price=0.1839,
            ask_price=0.1840,
            current_long_lots=[
                {"qty": 3385.0, "price": 0.2008},
                {"qty": 8.0, "price": 0.1811},
                {"qty": 63.0, "price": 0.1813},
            ],
        )

        self.assertTrue(report["active"])
        self.assertTrue(report["lot_cost_guard_active"])
        self.assertEqual(report["placed_order_count"], 1)
        self.assertEqual(report["lot_cost_guard_consumed_qty"], 71.0)
        self.assertGreater(report["lot_cost_guard_blocked_order_count"], 0)
        self.assertEqual(plan["sell_orders"][0]["price"], 0.184)
        self.assertEqual(plan["sell_orders"][0]["qty"], 71.0)
        self.assertAlmostEqual(plan["sell_orders"][0]["notional"], 13.064, places=8)

    def test_synthetic_flow_sleeve_adds_reduce_only_short_flow_exits_at_loss_ceiling(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "bootstrap_orders": []}

        report = apply_synthetic_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_notional=0,
            current_short_notional=220,
            current_long_avg_price=0,
            current_short_avg_price=0.168575,
            trigger_notional=90,
            sleeve_notional=180,
            levels=4,
            per_order_notional=60,
            order_notional=60,
            max_loss_ratio=0.003,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1,
            min_qty=1,
            min_notional=5,
            bid_price=0.1692,
            ask_price=0.1693,
            buy_offset_steps=0.0,
            sell_offset_steps=0.0,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["direction"], "short_inventory_buy_flow")
        self.assertEqual(report["loss_ceiling_price"], 0.169)
        self.assertEqual(report["loss_guard_clamped_order_count"], 1)
        self.assertEqual(report["placed_order_count"], 3)
        self.assertEqual([item["price"] for item in plan["buy_orders"]], [0.169, 0.1688, 0.1686])
        self.assertTrue(all(item["role"] == "flow_sleeve_short" for item in plan["buy_orders"]))
        self.assertTrue(all(item["force_reduce_only"] for item in plan["buy_orders"]))

    @patch("grid_optimizer.loop_runner.determine_interval_center_price")
    def test_resolve_interval_locked_center_price_rolls_halfway_to_target_center(self, mock_determine_interval_center_price) -> None:
        state = {"last_center_update_ts": "2026-04-21T04:00:00+00:00"}
        now = datetime(2026, 4, 21, 4, 15, 0, tzinfo=timezone.utc)
        mock_determine_interval_center_price.return_value = {
            "available": True,
            "center_price": 104.0,
        }

        center, report = resolve_interval_locked_center_price(
            state=state,
            symbol="TESTUSDT",
            current_center_price=100.0,
            mid_price=104.0,
            step_price=1.0,
            interval_minutes=15,
            tick_size=1.0,
            now=now,
        )

        self.assertEqual(center, 102.0)
        self.assertEqual(report["reason"], "center_interval_roll")
        self.assertEqual(report["center_shift_steps"], 2)
        self.assertEqual(report["center_shift_ratio"], 0.5)

    def test_resolve_interval_locked_center_price_fast_catchup_shifts_after_confirm_cycles(self) -> None:
        state = {"last_center_update_ts": "2026-04-21T04:00:00+00:00"}
        now = datetime(2026, 4, 21, 4, 5, 0, tzinfo=timezone.utc)

        center, report = resolve_interval_locked_center_price(
            state=state,
            symbol="TESTUSDT",
            current_center_price=100.0,
            mid_price=103.5,
            step_price=0.5,
            interval_minutes=30,
            tick_size=0.1,
            fast_catchup_trigger_steps=6.0,
            fast_catchup_confirm_cycles=3,
            fast_catchup_shift_steps=2,
            now=now,
        )
        self.assertEqual(center, 100.0)
        self.assertEqual(report["reason"], "center_interval_locked")
        self.assertEqual(report["fast_catchup_pending_count"], 1)

        center, report = resolve_interval_locked_center_price(
            state=state,
            symbol="TESTUSDT",
            current_center_price=center,
            mid_price=103.5,
            step_price=0.5,
            interval_minutes=30,
            tick_size=0.1,
            fast_catchup_trigger_steps=6.0,
            fast_catchup_confirm_cycles=3,
            fast_catchup_shift_steps=2,
            now=now + timedelta(seconds=10),
        )
        self.assertEqual(center, 100.0)
        self.assertEqual(report["fast_catchup_pending_count"], 2)

        center, report = resolve_interval_locked_center_price(
            state=state,
            symbol="TESTUSDT",
            current_center_price=center,
            mid_price=103.5,
            step_price=0.5,
            interval_minutes=30,
            tick_size=0.1,
            fast_catchup_trigger_steps=6.0,
            fast_catchup_confirm_cycles=3,
            fast_catchup_shift_steps=2,
            now=now + timedelta(seconds=20),
        )

        self.assertEqual(center, 101.0)
        self.assertEqual(report["reason"], "center_interval_fast_catchup")
        self.assertEqual(report["center_shift_steps"], 2)

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

    @patch("grid_optimizer.loop_runner._fetch_trade_rows_since")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_competition_inventory_grid_handles_none_max_position_and_counts_bootstrap_notional(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_fetch_trade_rows_since,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 0.1,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.0999", "ask_price": "0.1001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "KATUSDT", "positionAmt": "0", "entryPrice": "0"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "shift_frozen": False,
        }
        mock_fetch_trade_rows_since.return_value = []

        with TemporaryDirectory() as tmpdir:
            args = Namespace(
                symbol="KATUSDT",
                strategy_mode="competition_inventory_grid",
                strategy_profile="volume_long_v4",
                step_price=0.01,
                buy_levels=8,
                sell_levels=8,
                per_order_notional=10.0,
                base_position_notional=30.0,
                first_order_multiplier=4.0,
                threshold_position_notional=50.0,
                max_order_position_notional=80.0,
                center_price=0.10,
                fixed_center_enabled=False,
                fixed_center_roll_enabled=False,
                fixed_center_roll_trigger_steps=1.0,
                fixed_center_roll_confirm_cycles=3,
                fixed_center_roll_shift_steps=1,
                custom_grid_enabled=False,
                custom_grid_direction=None,
                custom_grid_level_mode="arithmetic",
                custom_grid_min_price=None,
                custom_grid_max_price=None,
                custom_grid_n=None,
                custom_grid_total_notional=None,
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
                max_position_notional=None,
                max_short_position_notional=None,
                pause_buy_position_notional=None,
                pause_short_position_notional=None,
                min_mid_price_for_buys=None,
                excess_inventory_reduce_only_enabled=False,
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

        self.assertEqual(report["strategy_mode"], "competition_inventory_grid")
        self.assertIsNone(report["max_position_notional"])
        self.assertEqual(len(report["bootstrap_orders"]), 2)
        bootstrap_buy_notional = sum(item["notional"] for item in report["bootstrap_orders"] if item["side"] == "BUY")
        bootstrap_sell_notional = sum(item["notional"] for item in report["bootstrap_orders"] if item["side"] == "SELL")
        self.assertAlmostEqual(report["planned_buy_notional"], bootstrap_buy_notional)
        self.assertAlmostEqual(report["planned_short_notional"], bootstrap_sell_notional)

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_defaults_to_break_even_guard_when_ratio_omitted(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.999", "ask_price": "1.001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "300", "entryPrice": "1.035"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 300.0,
            "virtual_long_avg_price": 1.035,
            "virtual_long_lots": [{"qty": 300.0, "price": 1.035}],
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_short_lots": [],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 700.0
            args.pause_short_position_notional = 700.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        sell_prices = [item["price"] for item in report["sell_orders"] if item["role"] == "take_profit_long"]

        self.assertEqual(sell_prices, [])
        self.assertTrue(report["take_profit_guard"]["enabled"])
        self.assertEqual(report["take_profit_guard"]["min_profit_ratio"], 0.0)
        self.assertAlmostEqual(report["take_profit_guard"]["long_floor_price"], 1.035, places=8)

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_volume_long_v4_uses_entry_price_for_take_profit_guard(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 0.1630,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1630", "ask_price": "0.1631"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {
                    "symbol": "SOONUSDT",
                    "positionAmt": "4340",
                    "entryPrice": "0.1618043182344",
                    "breakEvenPrice": "0.1656403611152",
                },
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "SOONUSDT"
            args.strategy_mode = "one_way_long"
            args.strategy_profile = "volume_long_v4"
            args.step_price = 0.0002
            args.buy_levels = 8
            args.sell_levels = 8
            args.per_order_notional = 70.0
            args.base_position_notional = 420.0
            args.take_profit_min_profit_ratio = None
            args.pause_buy_position_notional = 750.0
            args.max_position_notional = 900.0
            args.exposure_escalation_enabled = True
            args.exposure_escalation_target_notional = 600.0
            args.exposure_escalation_max_loss_ratio = 0.05
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "soonusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "soonusdt_events.jsonl")

            report = generate_plan_report(args)

        self.assertTrue(report["take_profit_guard"]["long_active"])
        self.assertFalse(report["take_profit_guard"]["long_cost_basis_missing"])
        self.assertEqual(report["position_cost_basis_source"], "entryPrice")
        self.assertAlmostEqual(report["current_long_avg_price"], 0.1618043182344, places=12)
        self.assertAlmostEqual(report["take_profit_guard"]["long_floor_price"], 0.1619, places=8)
        self.assertAlmostEqual(
            report["exposure_escalation"]["unrealized_pnl"],
            (0.16305 - 0.1618043182344) * 4340,
            places=8,
        )

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_volume_long_v4_reprices_normal_take_profit_to_current_grid(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 0.1630,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1630", "ask_price": "0.1631"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {
                    "symbol": "SOONUSDT",
                    "positionAmt": "4340",
                    "entryPrice": "0.1620887365948",
                    "breakEvenPrice": "0.1655863054659",
                },
            ],
        }
        mock_open_orders.return_value = [
            {
                "symbol": "SOONUSDT",
                "side": "SELL",
                "price": "0.1665",
                "origQty": "420",
                "executedQty": "0",
                "reduceOnly": True,
                "positionSide": "BOTH",
                "clientOrderId": "gx-soonu-takeprof-1-12345678",
            },
            {
                "symbol": "SOONUSDT",
                "side": "SELL",
                "price": "0.1667",
                "origQty": "419",
                "executedQty": "0",
                "reduceOnly": True,
                "positionSide": "BOTH",
                "clientOrderId": "gx-soonu-takeprof-2-12345679",
            },
        ]
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "SOONUSDT"
            args.strategy_mode = "one_way_long"
            args.strategy_profile = "volume_long_v4"
            args.step_price = 0.0002
            args.buy_levels = 8
            args.sell_levels = 8
            args.per_order_notional = 70.0
            args.base_position_notional = 420.0
            args.take_profit_min_profit_ratio = None
            args.pause_buy_position_notional = 750.0
            args.max_position_notional = 900.0
            args.reset_state = False
            args.state_path = str(Path(tmpdir) / "soonusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "soonusdt_events.jsonl")

            report = generate_plan_report(args)

        sell_prices = [item["price"] for item in report["sell_orders"] if item["role"] == "take_profit"]
        self.assertGreaterEqual(len(sell_prices), 2)
        self.assertEqual(sell_prices[:2], [0.1632, 0.1635])

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_volume_long_v4_keeps_existing_soft_delever_prices(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 0.1652,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1653", "ask_price": "0.1654"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {
                    "symbol": "SOONUSDT",
                    "positionAmt": "5000",
                    "entryPrice": "0.1656407549484",
                    "breakEvenPrice": "0.1661000000000",
                },
            ],
        }
        mock_open_orders.return_value = [
            {
                "symbol": "SOONUSDT",
                "side": "SELL",
                "price": "0.1658",
                "origQty": "422",
                "executedQty": "0",
                "reduceOnly": True,
                "positionSide": "BOTH",
                "clientOrderId": "gx-soonu-softdele-1-12345678",
            },
            {
                "symbol": "SOONUSDT",
                "side": "SELL",
                "price": "0.1660",
                "origQty": "422",
                "executedQty": "0",
                "reduceOnly": True,
                "positionSide": "BOTH",
                "clientOrderId": "gx-soonu-softdele-2-12345679",
            },
        ]
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "SOONUSDT"
            args.strategy_mode = "one_way_long"
            args.strategy_profile = "volume_long_v4"
            args.step_price = 0.0002
            args.buy_levels = 8
            args.sell_levels = 8
            args.per_order_notional = 70.0
            args.base_position_notional = 420.0
            args.take_profit_min_profit_ratio = None
            args.pause_buy_position_notional = 750.0
            args.max_position_notional = 900.0
            args.reset_state = False
            args.state_path = str(Path(tmpdir) / "soonusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "soonusdt_events.jsonl")

            report = generate_plan_report(args)

        soft_prices = [item["price"] for item in report["sell_orders"] if item["role"] == "soft_delever_long"]
        self.assertEqual(soft_prices[:2], [0.1658, 0.1660])
        self.assertTrue(report["volume_long_v4_delever"]["active"])
        self.assertEqual(report["volume_long_v4_delever"]["stage"], "soft")

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_keeps_light_short_front_exit_at_nearest_profitable_tick(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "1.001", "ask_price": "1.002"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "-20", "entryPrice": "1.0025"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_long_lots": [],
            "virtual_short_qty": 20.0,
            "virtual_short_avg_price": 1.0025,
            "virtual_short_lots": [
                {"qty": 10.0, "price": 1.02},
                {"qty": 10.0, "price": 0.985},
            ],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 2
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 250.0
            args.pause_short_position_notional = 250.0
            args.synthetic_residual_short_flat_notional = 0.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        take_profit_prices = [item["price"] for item in report["buy_orders"] if item["role"] == "take_profit_short"]

        self.assertEqual(take_profit_prices, [0.981])

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_keeps_short_entries_while_near_market_below_pause(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "1.001", "ask_price": "1.002"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "-20", "entryPrice": "1.0025"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_long_lots": [],
            "virtual_short_qty": 20.0,
            "virtual_short_avg_price": 1.0025,
            "virtual_short_lots": [
                {"qty": 10.0, "price": 1.02},
                {"qty": 10.0, "price": 0.985},
            ],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 2
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 250.0
            args.pause_short_position_notional = 250.0
            args.synthetic_residual_short_flat_notional = 0.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        take_profit_prices = [item["price"] for item in report["buy_orders"] if item["role"] == "take_profit_short"]
        entry_short_prices = [item["price"] for item in report["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(take_profit_prices, [0.981])
        self.assertEqual(entry_short_prices, [1.03, 1.04])
        self.assertFalse(report["synthetic_inventory_exit_priority"]["active"])
        self.assertEqual(report["synthetic_inventory_exit_priority"]["direction"], "short")

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_best_quote_neutral_keeps_opposite_entry_below_pause(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "1.001", "ask_price": "1.002"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "20", "entryPrice": "0.99"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 20.0,
            "virtual_long_avg_price": 0.99,
            "virtual_long_lots": [{"qty": 20.0, "price": 0.99}],
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_short_lots": [],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_profile = "btcusdc_competition_neutral_ping_pong_v1"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 250.0
            args.pause_short_position_notional = 250.0
            args.max_position_notional = 500.0
            args.max_short_position_notional = 500.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        sell_roles = {item["role"] for item in report["sell_orders"]}
        self.assertIn("take_profit_long", sell_roles)
        self.assertIn("entry_short", sell_roles)
        self.assertFalse(report["active_delever"]["active"])

    def test_apply_synthetic_inventory_exit_priority_prunes_pure_short_inventory_after_pause_threshold(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {
                    "side": "BUY",
                    "price": 0.981,
                    "qty": 260.0,
                    "notional": 255.06,
                    "role": "take_profit_short",
                    "position_side": "SHORT",
                }
            ],
            "sell_orders": [
                {
                    "side": "SELL",
                    "price": 1.01,
                    "qty": 25.0,
                    "notional": 25.25,
                    "role": "entry_short",
                    "position_side": "SHORT",
                }
            ],
        }

        report = apply_synthetic_inventory_exit_priority(
            plan=plan,
            strategy_mode="synthetic_neutral",
            current_long_qty=0.0,
            current_short_qty=260.0,
            current_long_notional=0.0,
            current_short_notional=260.0,
            pause_long_position_notional=250.0,
            pause_short_position_notional=250.0,
            near_market_entries_allowed=True,
            step_size=1.0,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["direction"], "short")
        self.assertEqual(plan["sell_orders"], [])

    def test_apply_synthetic_inventory_exit_priority_strict_prunes_below_pause(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {
                    "side": "BUY",
                    "price": 0.99,
                    "qty": 25.0,
                    "notional": 24.75,
                    "role": "entry_long",
                    "position_side": "LONG",
                }
            ],
            "sell_orders": [
                {
                    "side": "SELL",
                    "price": 1.01,
                    "qty": 25.0,
                    "notional": 25.25,
                    "role": "take_profit_long",
                    "position_side": "LONG",
                }
            ],
        }

        report = apply_synthetic_inventory_exit_priority(
            plan=plan,
            strategy_mode="synthetic_neutral",
            current_long_qty=25.0,
            current_short_qty=0.0,
            current_long_notional=25.0,
            current_short_notional=0.0,
            pause_long_position_notional=250.0,
            pause_short_position_notional=250.0,
            near_market_entries_allowed=True,
            step_size=1.0,
            strict_exit_only=True,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["direction"], "long")
        self.assertEqual(plan["buy_orders"], [])

    def test_apply_synthetic_inventory_exit_priority_strict_prunes_without_exit(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {
                    "side": "BUY",
                    "price": 0.99,
                    "qty": 25.0,
                    "notional": 24.75,
                    "role": "entry_long",
                    "position_side": "LONG",
                }
            ],
            "sell_orders": [],
        }

        report = apply_synthetic_inventory_exit_priority(
            plan=plan,
            strategy_mode="synthetic_neutral",
            current_long_qty=25.0,
            current_short_qty=0.0,
            current_long_notional=25.0,
            current_short_notional=0.0,
            pause_long_position_notional=250.0,
            pause_short_position_notional=250.0,
            near_market_entries_allowed=True,
            step_size=1.0,
            strict_exit_only=True,
        )

        self.assertTrue(report["active"])
        self.assertFalse(report["exit_ready"])
        self.assertEqual(plan["buy_orders"], [])


    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_bard_profile_splits_light_short_exits_by_lot_cost_guard(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "1.001", "ask_price": "1.002"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "-20", "entryPrice": "1.0025"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_long_lots": [],
            "virtual_short_qty": 20.0,
            "virtual_short_avg_price": 1.0025,
            "virtual_short_lots": [
                {"qty": 10.0, "price": 1.02},
                {"qty": 10.0, "price": 0.985},
            ],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_profile = "bard_12h_push_neutral_v2"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 2
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 250.0
            args.pause_short_position_notional = 250.0
            args.synthetic_residual_short_flat_notional = 0.0
            args.take_profit_min_profit_ratio = 0.0001
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        take_profit_prices = [item["price"] for item in report["buy_orders"] if item["role"] == "take_profit_short"]

        self.assertEqual(take_profit_prices, [1.001, 0.975])

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_suppresses_long_loss_release_on_pause(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.999", "ask_price": "1.001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "300", "entryPrice": "1.035"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 300.0,
            "virtual_long_avg_price": 1.035,
            "virtual_long_lots": [{"qty": 300.0, "price": 1.035}],
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_short_lots": [],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
            "grid_buffer_realized_notional": 20.0,
            "grid_buffer_spent_notional": 0.0,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 250.0
            args.pause_short_position_notional = 700.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        active_sells = [item for item in report["sell_orders"] if item["role"] == "active_delever_long"]
        self.assertFalse(report["active_delever"]["active"])
        self.assertIsNone(report["active_delever"]["trigger_mode"])
        self.assertEqual(report["active_delever"]["active_sell_order_count"], 0)
        self.assertEqual(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_sells, [])
        self.assertAlmostEqual(report["take_profit_guard"]["long_floor_price"], 1.035, places=8)

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_suppresses_short_loss_release_on_pause(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.999", "ask_price": "1.001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "-300", "entryPrice": "0.965"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_long_lots": [],
            "virtual_short_qty": 300.0,
            "virtual_short_avg_price": 0.965,
            "virtual_short_lots": [{"qty": 300.0, "price": 0.965}],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
            "grid_buffer_realized_notional": 20.0,
            "grid_buffer_spent_notional": 0.0,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 250.0
            args.pause_short_position_notional = 250.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        active_buys = [item for item in report["buy_orders"] if item["role"] == "active_delever_short"]
        self.assertFalse(report["active_delever"]["active"])
        self.assertIsNone(report["active_delever"]["trigger_mode"])
        self.assertEqual(report["active_delever"]["active_buy_order_count"], 0)
        self.assertEqual(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_buys, [])
        self.assertAlmostEqual(report["take_profit_guard"]["short_ceiling_price"], 0.965, places=8)

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_suppresses_long_loss_release_on_threshold(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.999", "ask_price": "1.001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "300", "entryPrice": "1.035"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 300.0,
            "virtual_long_avg_price": 1.035,
            "virtual_long_lots": [{"qty": 300.0, "price": 1.035}],
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_short_lots": [],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 200.0
            args.pause_short_position_notional = 700.0
            args.threshold_position_notional = 250.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        active_sells = [item for item in report["sell_orders"] if item["role"] == "active_delever_long"]
        self.assertFalse(report["active_delever"]["active"])
        self.assertIsNone(report["active_delever"]["trigger_mode"])
        self.assertEqual(report["active_delever"]["active_sell_order_count"], 0)
        self.assertEqual(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_sells, [])
        self.assertAlmostEqual(report["take_profit_guard"]["long_floor_price"], 1.035, places=8)

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_suppresses_short_loss_release_on_threshold(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.999", "ask_price": "1.001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "-300", "entryPrice": "0.965"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_long_lots": [],
            "virtual_short_qty": 300.0,
            "virtual_short_avg_price": 0.965,
            "virtual_short_lots": [{"qty": 300.0, "price": 0.965}],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 700.0
            args.pause_short_position_notional = 200.0
            args.threshold_position_notional = 250.0
            args.take_profit_min_profit_ratio = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        active_buys = [item for item in report["buy_orders"] if item["role"] == "active_delever_short"]
        self.assertFalse(report["active_delever"]["active"])
        self.assertIsNone(report["active_delever"]["trigger_mode"])
        self.assertEqual(report["active_delever"]["active_buy_order_count"], 0)
        self.assertEqual(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_buys, [])
        self.assertAlmostEqual(report["take_profit_guard"]["short_ceiling_price"], 0.965, places=8)

    def test_apply_active_delever_short_switches_to_maker_orders_after_threshold_timeout(self) -> None:
        plan = {
            "buy_orders": [
                {"side": "BUY", "price": 0.999, "qty": 25.0, "notional": 24.975, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.989, "qty": 25.0, "notional": 24.725, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.979, "qty": 25.0, "notional": 24.475, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.969, "qty": 25.0, "notional": 24.225, "role": "take_profit_short"},
            ],
            "sell_orders": [],
        }

        result = apply_active_delever_short(
            plan=plan,
            current_short_qty=300.0,
            current_short_notional=300.0,
            current_short_avg_price=0.965,
            current_short_lots=[{"qty": 300.0, "price": 0.965}],
            pause_short_position_notional=200.0,
            threshold_position_notional=250.0,
            per_order_notional=25.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            threshold_timeout_active=True,
            timeout_target_notional=200.0,
        )

        active_buys = [item for item in plan["buy_orders"] if item["role"] == "active_delever_short"]

        self.assertTrue(result["active"])
        self.assertEqual(result["trigger_mode"], "threshold_timeout")
        self.assertEqual(result["active_buy_order_count"], 3)
        self.assertEqual([item["price"] for item in active_buys], [0.999, 0.989, 0.979])
        self.assertTrue(all(item["execution_type"] == "maker_timeout_release" for item in active_buys))
        self.assertTrue(all(item["time_in_force"] == "GTX" for item in active_buys))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_buys))

    def test_apply_active_delever_long_switches_to_maker_orders_after_inventory_pause_timeout(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.001, "qty": 30.0, "notional": 30.03, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.011, "qty": 30.0, "notional": 30.33, "role": "take_profit_long"},
            ],
        }

        result = apply_active_delever_long(
            plan=plan,
            current_long_qty=240.0,
            current_long_notional=240.0,
            current_long_avg_price=1.0,
            current_long_lots=[{"qty": 240.0, "price": 1.0}],
            pause_long_position_notional=200.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            market_guard_buy_pause_active=False,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            max_active_levels=2,
            inventory_pause_timeout_active=True,
            timeout_target_notional=200.0,
        )

        active_sells = [item for item in plan["sell_orders"] if item["role"] == "active_delever_long"]

        self.assertTrue(result["active"])
        self.assertEqual(result["trigger_mode"], "pause_timeout")
        self.assertEqual(result["active_sell_order_count"], 2)
        self.assertEqual([item["price"] for item in active_sells], [1.001, 1.011])
        self.assertTrue(all(item["execution_type"] == "maker_timeout_release" for item in active_sells))
        self.assertTrue(all(item["time_in_force"] == "GTX" for item in active_sells))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_sells))

    def test_apply_active_delever_short_pause_notional_keeps_profit_only_orders(self) -> None:
        plan = {
            "buy_orders": [
                {"side": "BUY", "price": 0.999, "qty": 30.0, "notional": 29.97, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.989, "qty": 30.0, "notional": 29.67, "role": "take_profit_short"},
            ],
            "sell_orders": [],
        }

        result = apply_active_delever_short(
            plan=plan,
            current_short_qty=240.0,
            current_short_notional=240.0,
            current_short_avg_price=0.965,
            current_short_lots=[{"qty": 240.0, "price": 0.965}],
            pause_short_position_notional=200.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            max_active_levels=2,
        )

        active_buys = [item for item in plan["buy_orders"] if item["role"] == "active_delever_short"]
        self.assertTrue(result["active"])
        self.assertEqual(result["trigger_mode"], "pause")
        self.assertEqual(result["release_buy_order_count"], 0)
        self.assertEqual([item["price"] for item in active_buys], [0.999, 0.989])
        self.assertFalse(any(item.get("execution_type") == "passive_release" for item in active_buys))
        self.assertFalse(any(bool(item.get("force_reduce_only")) for item in active_buys))

    def test_apply_active_delever_short_pause_notional_does_not_synthesize_loss_release_orders(self) -> None:
        plan = {
            "buy_orders": [
                {"side": "BUY", "price": 0.95, "qty": 30.0, "notional": 28.5, "role": "flow_sleeve_short"},
            ],
            "sell_orders": [],
        }

        result = apply_active_delever_short(
            plan=plan,
            current_short_qty=240.0,
            current_short_notional=240.0,
            current_short_avg_price=0.965,
            current_short_lots=[{"qty": 240.0, "price": 0.965}],
            pause_short_position_notional=200.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            max_active_levels=2,
        )

        active_buys = [item for item in plan["buy_orders"] if item["role"] == "active_delever_short"]
        self.assertFalse(result["active"])
        self.assertIsNone(result["trigger_mode"])
        self.assertEqual(result["synthesized_release_source_order_count"], 0)
        self.assertEqual(result["release_buy_order_count"], 0)
        self.assertEqual(result["pruned_flow_sleeve_order_count"], 0)
        self.assertEqual(active_buys, [])
        self.assertTrue(any(item["role"] == "flow_sleeve_short" for item in plan["buy_orders"]))

    def test_take_profit_guard_preserves_release_ceiling_for_active_short_delever_orders(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {
                    "side": "BUY",
                    "price": 1.003,
                    "qty": 25.0,
                    "notional": 25.075,
                    "role": "active_delever_short",
                    "execution_type": "aggressive",
                    "force_reduce_only": True,
                    "take_profit_guard_release_ceiling": 1.003,
                },
                {"side": "BUY", "price": 0.999, "qty": 25.0, "notional": 24.975, "role": "take_profit_short"},
            ],
            "sell_orders": [],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=50.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.965,
            current_long_notional=0.0,
            current_short_notional=50.0,
            pause_long_position_notional=None,
            pause_short_position_notional=200.0,
            min_profit_ratio=0.0,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
        )

        active_buys = [item for item in plan["buy_orders"] if item["role"] == "active_delever_short"]
        tp_buys = [item for item in plan["buy_orders"] if item["role"] == "take_profit_short"]
        self.assertEqual(len(active_buys), 1)
        self.assertAlmostEqual(active_buys[0]["price"], 1.003, places=8)
        self.assertEqual(len(tp_buys), 1)
        self.assertAlmostEqual(tp_buys[0]["price"], 0.965, places=8)
        self.assertEqual(result["relaxed_buy_orders"], 1)
        self.assertAlmostEqual(result["release_ceiling_price"], 1.003, places=8)

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
            args.cancel_stale = False
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

    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.resolve_futures_market_snapshot")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_uses_market_snapshot_resolver_when_stream_available(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_market_snapshot,
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
        mock_market_snapshot.return_value = {
            "symbol": "TESTUSDT",
            "bid_price": 0.3120,
            "ask_price": 0.3122,
            "mark_price": 0.3121,
            "funding_rate": 0.0001,
            "next_funding_time": 1234567890,
            "source": "websocket",
        }
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "TESTUSDT", "positionAmt": "0", "entryPrice": "0"}],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "shift_frozen": False,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "one_way_long"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")
            args.market_stream = object()

            report = generate_plan_report(args)

        self.assertAlmostEqual(report["mid_price"], 0.3121, places=8)
        self.assertEqual(mock_market_snapshot.call_count, 1)
        mock_book_tickers.assert_not_called()
        mock_premium_index.assert_not_called()

    def test_wait_for_runner_market_stream_snapshot_returns_warm_snapshot(self) -> None:
        class DummyMarketStream:
            def __init__(self) -> None:
                self.calls = 0

            def snapshot(self, *, max_age_seconds: float | None = None) -> dict[str, object] | None:
                self.calls += 1
                if self.calls < 2:
                    return None
                return {
                    "symbol": "TESTUSDT",
                    "bid_price": 1.0,
                    "ask_price": 1.1,
                    "mark_price": 1.05,
                    "source": "websocket",
                    "max_age_seconds": max_age_seconds,
                }

        stream = DummyMarketStream()

        with patch("grid_optimizer.loop_runner.time.sleep") as mock_sleep:
            snapshot = _wait_for_runner_market_stream_snapshot(stream, max_wait_seconds=1.0)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["source"], "websocket")
        self.assertEqual(snapshot["max_age_seconds"], 15.0)
        self.assertEqual(stream.calls, 2)
        mock_sleep.assert_called_once_with(0.1)

    def test_binance_rate_limit_error_detection_matches_exchange_message(self) -> None:
        exc = RuntimeError(
            "Binance API error -1003: Too many requests; current limit of IP(43.155.163.114) "
            "is 2400 requests per minute. Please use the websocket for live updates to avoid polling the API."
        )

        self.assertTrue(_is_binance_rate_limit_error(exc))
        self.assertFalse(_is_binance_rate_limit_error(RuntimeError("Binance API error -2011: Unknown order sent.")))

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

    def test_apply_synthetic_trade_fill_flow_sleeve_short_reduces_virtual_short_book(self) -> None:
        ledger = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_short_qty": 100.0,
            "virtual_short_avg_price": 0.168,
            "virtual_long_lots": [],
            "virtual_short_lots": [{"qty": 100.0, "price": 0.168}],
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "30", "price": "0.169"},
                order_ref={"role": "flow_sleeve_short"},
            )
        )

        self.assertAlmostEqual(ledger["virtual_short_qty"], 70.0)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], 30.0 * (0.169 - 0.168))

    def test_apply_synthetic_trade_fill_tracks_lifo_lots(self) -> None:
        ledger = {
            "virtual_long_qty": 15.0,
            "virtual_long_avg_price": (10.0 * 1.0 + 5.0 * 1.1) / 15.0,
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_long_lots": [
                {"qty": 10.0, "price": 1.0},
                {"qty": 5.0, "price": 1.1},
            ],
            "virtual_short_lots": [],
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "3", "price": "1.12"},
                order_ref={"role": "take_profit_long"},
            )
        )
        self.assertAlmostEqual(ledger["virtual_long_qty"], 12.0)
        self.assertAlmostEqual(ledger["virtual_long_avg_price"], (10.0 * 1.0 + 2.0 * 1.1) / 12.0)
        self.assertEqual(
            ledger["virtual_long_lots"],
            [
                {"qty": 10.0, "price": 1.0},
                {"qty": 2.0, "price": 1.1},
            ],
        )

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "4", "price": "1.20"},
                order_ref={"role": "entry_long"},
            )
        )
        self.assertAlmostEqual(ledger["virtual_long_qty"], 16.0)
        self.assertAlmostEqual(ledger["virtual_long_avg_price"], (10.0 * 1.0 + 2.0 * 1.1 + 4.0 * 1.2) / 16.0)
        self.assertEqual(
            ledger["virtual_long_lots"],
            [
                {"qty": 10.0, "price": 1.0},
                {"qty": 2.0, "price": 1.1},
                {"qty": 4.0, "price": 1.2},
            ],
        )

    def test_apply_synthetic_trade_fill_tracks_grid_buffer_realized_and_spent(self) -> None:
        ledger = {
            "virtual_long_qty": 15.0,
            "virtual_long_avg_price": (10.0 * 1.0 + 5.0 * 1.1) / 15.0,
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_long_lots": [
                {"qty": 10.0, "price": 1.0},
                {"qty": 5.0, "price": 1.1},
            ],
            "virtual_short_lots": [],
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "3", "price": "1.12"},
                order_ref={"role": "take_profit_long"},
            )
        )
        self.assertAlmostEqual(ledger["grid_buffer_realized_notional"], 0.06, places=8)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], 0.0, places=8)

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "4", "price": "0.95"},
                order_ref={"role": "active_delever_long"},
            )
        )
        self.assertAlmostEqual(ledger["grid_buffer_realized_notional"], 0.06, places=8)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], 0.40, places=8)
        self.assertAlmostEqual(ledger["virtual_long_qty"], 8.0, places=8)

    def test_apply_synthetic_trade_fill_threshold_fifo_consumes_oldest_long_lot(self) -> None:
        ledger = {
            "virtual_long_qty": 15.0,
            "virtual_long_avg_price": (5.0 * 1.3 + 5.0 * 1.1 + 5.0 * 1.0) / 15.0,
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_long_lots": [
                {"qty": 5.0, "price": 1.3},
                {"qty": 5.0, "price": 1.1},
                {"qty": 5.0, "price": 1.0},
            ],
            "virtual_short_lots": [],
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "4", "price": "0.95"},
                order_ref={"role": "active_delever_long", "lot_consume_mode": "fifo"},
            )
        )
        self.assertEqual(
            ledger["virtual_long_lots"],
            [
                {"qty": 1.0, "price": 1.3},
                {"qty": 5.0, "price": 1.1},
                {"qty": 5.0, "price": 1.0},
            ],
        )
        self.assertAlmostEqual(ledger["virtual_long_avg_price"], (1.0 * 1.3 + 5.0 * 1.1 + 5.0 * 1.0) / 11.0, places=8)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], (4.0 * 1.3) - (4.0 * 0.95), places=8)

    def test_apply_synthetic_trade_fill_tracks_grid_buffer_for_short_realized_and_spent(self) -> None:
        ledger = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_short_qty": 15.0,
            "virtual_short_avg_price": (10.0 * 1.0 + 5.0 * 0.9) / 15.0,
            "virtual_long_lots": [],
            "virtual_short_lots": [
                {"qty": 10.0, "price": 1.0},
                {"qty": 5.0, "price": 0.9},
            ],
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "3", "price": "0.88"},
                order_ref={"role": "take_profit_short"},
            )
        )
        self.assertAlmostEqual(ledger["grid_buffer_realized_notional"], 0.06, places=8)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], 0.0, places=8)

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "4", "price": "1.05"},
                order_ref={"role": "active_delever_short"},
            )
        )
        self.assertAlmostEqual(ledger["grid_buffer_realized_notional"], 0.06, places=8)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], 0.40, places=8)
        self.assertAlmostEqual(ledger["virtual_short_qty"], 8.0, places=8)

    def test_apply_synthetic_trade_fill_threshold_fifo_consumes_oldest_short_lot(self) -> None:
        ledger = {
            "virtual_long_qty": 0.0,
            "virtual_long_avg_price": 0.0,
            "virtual_short_qty": 15.0,
            "virtual_short_avg_price": (5.0 * 0.9 + 5.0 * 1.1 + 5.0 * 1.3) / 15.0,
            "virtual_long_lots": [],
            "virtual_short_lots": [
                {"qty": 5.0, "price": 0.9},
                {"qty": 5.0, "price": 1.1},
                {"qty": 5.0, "price": 1.3},
            ],
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        self.assertTrue(
            _apply_synthetic_trade_fill(
                ledger=ledger,
                trade={"qty": "4", "price": "1.35"},
                order_ref={"role": "active_delever_short", "lot_consume_mode": "fifo"},
            )
        )
        self.assertEqual(
            ledger["virtual_short_lots"],
            [
                {"qty": 1.0, "price": 0.9},
                {"qty": 5.0, "price": 1.1},
                {"qty": 5.0, "price": 1.3},
            ],
        )
        self.assertAlmostEqual(ledger["virtual_short_avg_price"], (1.0 * 0.9 + 5.0 * 1.1 + 5.0 * 1.3) / 11.0, places=8)
        self.assertAlmostEqual(ledger["grid_buffer_spent_notional"], (4.0 * 1.35) - (4.0 * 0.9), places=8)

    def test_load_synthetic_ledger_backfills_missing_lots_from_aggregate_state(self) -> None:
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 12.0,
                "virtual_long_avg_price": 1.15,
                "virtual_short_qty": 7.0,
                "virtual_short_avg_price": 0.95,
            }
        }

        ledger = _load_synthetic_ledger(
            state=state,
            actual_position_qty=0.0,
            entry_price=0.0,
        )

        self.assertEqual(ledger["virtual_long_lots"], [{"qty": 12.0, "price": 1.15}])
        self.assertEqual(ledger["virtual_short_lots"], [{"qty": 7.0, "price": 0.95}])
        self.assertAlmostEqual(ledger["virtual_long_qty"], 12.0)
        self.assertAlmostEqual(ledger["virtual_long_avg_price"], 1.15)
        self.assertAlmostEqual(ledger["virtual_short_qty"], 7.0)
        self.assertAlmostEqual(ledger["virtual_short_avg_price"], 0.95)

    def test_sync_synthetic_ledger_resyncs_flat_book_to_actual_inventory_when_trade_feed_lags(self) -> None:
        now = datetime(2026, 4, 8, 5, 0, tzinfo=timezone.utc)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 0.0,
                "virtual_long_avg_price": 0.0,
                "virtual_long_lots": [],
                "virtual_short_qty": 0.0,
                "virtual_short_avg_price": 0.0,
                "virtual_short_lots": [],
                "last_trade_time_ms": int((now - timedelta(seconds=2)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            },
            "synthetic_order_refs": {
                "123": {
                    "role": "entry_short",
                    "client_order_id": "gx-old",
                }
            },
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="BARDUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=-140.0,
                entry_price=0.3198,
                qty_tolerance=1.0,
                fallback_price=0.3198,
            )

        self.assertAlmostEqual(snapshot["virtual_long_qty"], 0.0)
        self.assertAlmostEqual(snapshot["virtual_short_qty"], 140.0)
        self.assertEqual(snapshot["virtual_short_lots"], [{"qty": 140.0, "price": 0.3198}])
        self.assertTrue(snapshot["resynced_to_actual"])
        self.assertEqual(state["synthetic_order_refs"], {})
        self.assertEqual(state["synthetic_ledger"]["resync_reason"], "actual_position_drift")

    def test_sync_synthetic_ledger_keeps_existing_book_when_drift_is_within_tolerance(self) -> None:
        now = datetime(2026, 4, 8, 5, 0, tzinfo=timezone.utc)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 0.0,
                "virtual_long_avg_price": 0.0,
                "virtual_long_lots": [],
                "virtual_short_qty": 140.0,
                "virtual_short_avg_price": 0.3198,
                "virtual_short_lots": [{"qty": 140.0, "price": 0.3198}],
                "last_trade_time_ms": int((now - timedelta(seconds=2)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            }
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="BARDUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=-140.5,
                entry_price=0.3197,
                qty_tolerance=1.0,
                fallback_price=0.3197,
            )

        self.assertAlmostEqual(snapshot["virtual_short_qty"], 140.0)
        self.assertEqual(snapshot["virtual_short_lots"], [{"qty": 140.0, "price": 0.3198}])
        self.assertFalse(snapshot.get("resynced_to_actual"))

    def test_sync_synthetic_ledger_compacts_matched_virtual_books_to_flat_actual_position(self) -> None:
        now = datetime(2026, 4, 8, 5, 0, tzinfo=timezone.utc)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 299.0,
                "virtual_long_avg_price": 0.168,
                "virtual_long_lots": [{"qty": 299.0, "price": 0.168}],
                "virtual_short_qty": 299.0,
                "virtual_short_avg_price": 0.166,
                "virtual_short_lots": [{"qty": 299.0, "price": 0.166}],
                "grid_buffer_realized_notional": 1.5,
                "grid_buffer_spent_notional": 0.25,
                "last_trade_time_ms": int((now - timedelta(seconds=2)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            }
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="SOONUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=0.0,
                entry_price=0.0,
                qty_tolerance=1.0,
                fallback_price=0.1668,
            )

        self.assertAlmostEqual(snapshot["virtual_long_qty"], 0.0)
        self.assertAlmostEqual(snapshot["virtual_short_qty"], 0.0)
        self.assertEqual(snapshot["virtual_long_lots"], [])
        self.assertEqual(snapshot["virtual_short_lots"], [])
        self.assertFalse(snapshot["resynced_to_actual"])
        self.assertTrue(snapshot["synthetic_net_compacted"])
        self.assertAlmostEqual(snapshot["synthetic_net_compacted_qty"], 299.0)
        self.assertAlmostEqual(snapshot["grid_buffer_realized_notional"], 1.5)
        self.assertAlmostEqual(snapshot["grid_buffer_spent_notional"], 0.25)
        self.assertEqual(state["synthetic_ledger"]["virtual_long_lots"], [])
        self.assertEqual(state["synthetic_ledger"]["virtual_short_lots"], [])

    def test_sync_synthetic_ledger_compacts_matched_virtual_books_to_actual_net_cost_basis(self) -> None:
        now = datetime(2026, 4, 8, 5, 0, tzinfo=timezone.utc)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 300.0,
                "virtual_long_avg_price": 0.17,
                "virtual_long_lots": [
                    {"qty": 100.0, "price": 0.16},
                    {"qty": 200.0, "price": 0.175},
                ],
                "virtual_short_qty": 120.0,
                "virtual_short_avg_price": 0.165,
                "virtual_short_lots": [{"qty": 120.0, "price": 0.165}],
                "last_trade_time_ms": int((now - timedelta(seconds=2)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            }
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="SOONUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=180.0,
                entry_price=0.168,
                qty_tolerance=1.0,
                fallback_price=0.1668,
            )

        self.assertAlmostEqual(snapshot["virtual_long_qty"], 180.0)
        self.assertAlmostEqual(snapshot["virtual_long_avg_price"], 0.168)
        self.assertEqual(snapshot["virtual_long_lots"], [{"qty": 180.0, "price": 0.168}])
        self.assertAlmostEqual(snapshot["virtual_short_qty"], 0.0)
        self.assertTrue(snapshot["synthetic_net_compacted"])
        self.assertTrue(snapshot["synthetic_net_compacted_used_actual_net"])

    def test_sync_synthetic_ledger_applies_observed_fill_before_rest_backfill(self) -> None:
        now = datetime(2026, 5, 12, 5, 0, tzinfo=timezone.utc)
        trade_time_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 0.0,
                "virtual_long_avg_price": 0.0,
                "virtual_long_lots": [],
                "virtual_short_qty": 0.0,
                "virtual_short_avg_price": 0.0,
                "virtual_short_lots": [],
                "last_trade_time_ms": int((now - timedelta(seconds=5)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            },
            "synthetic_order_refs": {
                "123": {
                    "role": "entry_short",
                    "client_order_id": "gx-chipu-entry-short-1",
                }
            },
        }
        observed_trade = {
            "id": f"123:gx-chipu-entry-short-1:{trade_time_ms}:10.0:0.0671",
            "orderId": 123,
            "clientOrderId": "gx-chipu-entry-short-1",
            "symbol": "CHIPUSDT",
            "side": "SELL",
            "positionSide": "BOTH",
            "time": trade_time_ms,
            "price": 0.0671,
            "qty": 10.0,
            "quoteQty": 0.671,
            "executionType": "TRADE",
            "orderStatus": "FILLED",
            "source": "user_data_stream",
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="CHIPUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=-10.0,
                entry_price=0.0671,
                qty_tolerance=1e-9,
                fallback_price=0.0671,
                observed_trade_rows=[observed_trade],
            )

        self.assertAlmostEqual(snapshot["virtual_short_qty"], 10.0)
        self.assertEqual(snapshot["virtual_short_lots"], [{"qty": 10.0, "price": 0.0671}])
        self.assertEqual(snapshot["applied_trade_count"], 1)
        self.assertEqual(snapshot["unmatched_trade_count"], 0)
        self.assertFalse(snapshot["resynced_to_actual"])
        self.assertEqual(state["synthetic_ledger"]["last_trade_time_ms"], trade_time_ms)

    def test_sync_synthetic_ledger_dedupes_rest_trade_after_observed_fill(self) -> None:
        now = datetime(2026, 5, 12, 5, 0, tzinfo=timezone.utc)
        trade_time_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 0.0,
                "virtual_long_avg_price": 0.0,
                "virtual_long_lots": [],
                "virtual_short_qty": 0.0,
                "virtual_short_avg_price": 0.0,
                "virtual_short_lots": [],
                "last_trade_time_ms": int((now - timedelta(seconds=5)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            },
            "synthetic_order_refs": {
                "123": {
                    "role": "entry_short",
                    "client_order_id": "gx-chipu-entry-short-1",
                }
            },
        }
        trade_row = {
            "id": f"123:gx-chipu-entry-short-1:{trade_time_ms}:10.0:0.0671",
            "orderId": 123,
            "clientOrderId": "gx-chipu-entry-short-1",
            "symbol": "CHIPUSDT",
            "side": "SELL",
            "positionSide": "BOTH",
            "time": trade_time_ms,
            "price": 0.0671,
            "qty": 10.0,
            "quoteQty": 0.671,
            "executionType": "TRADE",
            "orderStatus": "FILLED",
            "source": "user_data_stream",
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[trade_row]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="CHIPUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=-10.0,
                entry_price=0.0671,
                qty_tolerance=1e-9,
                fallback_price=0.0671,
                observed_trade_rows=[trade_row],
            )

        self.assertAlmostEqual(snapshot["virtual_short_qty"], 10.0)
        self.assertEqual(snapshot["virtual_short_lots"], [{"qty": 10.0, "price": 0.0671}])
        self.assertEqual(snapshot["applied_trade_count"], 1)
        self.assertEqual(snapshot["unmatched_trade_count"], 0)

    def test_sync_synthetic_ledger_backfills_rest_trade_older_than_latest_observed_fill(self) -> None:
        now = datetime(2026, 5, 12, 5, 0, tzinfo=timezone.utc)
        first_trade_time_ms = int((now - timedelta(seconds=3)).timestamp() * 1000)
        second_trade_time_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 0.0,
                "virtual_long_avg_price": 0.0,
                "virtual_long_lots": [],
                "virtual_short_qty": 0.0,
                "virtual_short_avg_price": 0.0,
                "virtual_short_lots": [],
                "last_trade_time_ms": int((now - timedelta(seconds=5)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            },
            "synthetic_order_refs": {
                "101": {"role": "entry_long", "client_order_id": "gx-billu-entry-long-1"},
                "102": {"role": "entry_long", "client_order_id": "gx-billu-entry-long-2"},
            },
        }
        older_rest_trade = {
            "id": f"101:gx-billu-entry-long-1:{first_trade_time_ms}:100.0:0.14",
            "orderId": 101,
            "clientOrderId": "gx-billu-entry-long-1",
            "symbol": "BILLUSDT",
            "side": "BUY",
            "time": first_trade_time_ms,
            "price": 0.14,
            "qty": 100.0,
        }
        latest_observed_trade = {
            "id": f"102:gx-billu-entry-long-2:{second_trade_time_ms}:200.0:0.141",
            "orderId": 102,
            "clientOrderId": "gx-billu-entry-long-2",
            "symbol": "BILLUSDT",
            "side": "BUY",
            "time": second_trade_time_ms,
            "price": 0.141,
            "qty": 200.0,
            "source": "user_data_stream",
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[older_rest_trade]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="BILLUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=300.0,
                entry_price=0.1407,
                qty_tolerance=1e-9,
                fallback_price=0.1407,
                observed_trade_rows=[latest_observed_trade],
            )

        self.assertAlmostEqual(snapshot["virtual_long_qty"], 300.0)
        self.assertEqual(snapshot["applied_trade_count"], 2)
        self.assertEqual(snapshot["unmatched_trade_count"], 0)
        self.assertFalse(snapshot["resynced_to_actual"])

    def test_infer_synthetic_order_ref_from_compact_client_order_id(self) -> None:
        self.assertEqual(
            _infer_synthetic_order_ref_from_trade(
                {"clientOrderId": "gx-billu-entrylon-1-12345678", "side": "BUY", "positionSide": "BOTH"}
            ),
            {
                "role": "entry_long",
                "side": "BUY",
                "position_side": "BOTH",
                "client_order_id": "gx-billu-entrylon-1-12345678",
                "inferred_from_client_order_id": True,
            },
        )
        self.assertEqual(
            _infer_synthetic_order_ref_from_trade(
                {"clientOrderId": "gx-billu-entrysho-2-12345678", "side": "SELL"}
            )["role"],
            "entry_short",
        )
        self.assertEqual(
            _infer_synthetic_order_ref_from_trade(
                {"clientOrderId": "gx-billu-takeprof-3-12345678", "side": "SELL"}
            )["role"],
            "take_profit_long",
        )
        self.assertEqual(
            _infer_synthetic_order_ref_from_trade(
                {"clientOrderId": "gx-billu-takeprof-4-12345678", "side": "BUY"}
            )["role"],
            "take_profit_short",
        )
        self.assertEqual(
            _infer_synthetic_order_ref_from_trade(
                {"clientOrderId": "gx-billu-adverser-1-12345678", "side": "SELL"}
            )["role"],
            "adverse_reduce_long",
        )

    def test_decorate_synthetic_open_orders_infers_role_from_client_order_id(self) -> None:
        decorated = _decorate_synthetic_open_orders(
            state={},
            open_orders=[
                {
                    "orderId": 25074142,
                    "clientOrderId": "gx-pharosu-takeprof-1-39087054",
                    "side": "SELL",
                    "positionSide": "BOTH",
                },
                {
                    "orderId": 25056844,
                    "clientOrderId": "gx-pharosu-adverser-1-38668581",
                    "side": "SELL",
                    "positionSide": "BOTH",
                },
            ],
        )

        self.assertEqual(decorated[0]["role"], "take_profit_long")
        self.assertEqual(decorated[1]["role"], "adverse_reduce_long")

    def test_sync_synthetic_ledger_infers_missing_refs_from_rest_client_order_id(self) -> None:
        now = datetime(2026, 5, 12, 5, 0, tzinfo=timezone.utc)
        trade_time_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 0.0,
                "virtual_long_avg_price": 0.0,
                "virtual_long_lots": [],
                "virtual_short_qty": 0.0,
                "virtual_short_avg_price": 0.0,
                "virtual_short_lots": [],
                "last_trade_time_ms": int((now - timedelta(seconds=5)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            },
            "synthetic_order_refs": {},
        }
        trade_row = {
            "id": f"777:gx-billu-entrylon-1-12345678:{trade_time_ms}:100.0:0.14",
            "orderId": 777,
            "clientOrderId": "gx-billu-entrylon-1-12345678",
            "symbol": "BILLUSDT",
            "side": "BUY",
            "positionSide": "BOTH",
            "time": trade_time_ms,
            "price": 0.14,
            "qty": 100.0,
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[trade_row]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="BILLUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=100.0,
                entry_price=0.14,
                qty_tolerance=1e-9,
                fallback_price=0.14,
            )

        self.assertAlmostEqual(snapshot["virtual_long_qty"], 100.0)
        self.assertEqual(snapshot["virtual_long_lots"], [{"qty": 100.0, "price": 0.14}])
        self.assertEqual(snapshot["applied_trade_count"], 1)
        self.assertEqual(snapshot["unmatched_trade_count"], 0)
        self.assertFalse(snapshot["resynced_to_actual"])

    def test_sync_synthetic_ledger_infers_take_profit_without_order_ref(self) -> None:
        now = datetime(2026, 5, 12, 5, 0, tzinfo=timezone.utc)
        trade_time_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
        state = {
            "synthetic_ledger": {
                "initialized": True,
                "virtual_long_qty": 100.0,
                "virtual_long_avg_price": 0.14,
                "virtual_long_lots": [{"qty": 100.0, "price": 0.14}],
                "virtual_short_qty": 0.0,
                "virtual_short_avg_price": 0.0,
                "virtual_short_lots": [],
                "grid_buffer_realized_notional": 0.0,
                "grid_buffer_spent_notional": 0.0,
                "last_trade_time_ms": int((now - timedelta(seconds=5)).timestamp() * 1000),
                "last_trade_keys_at_time": [],
                "unmatched_trade_count": 0,
            },
            "synthetic_order_refs": {},
        }
        trade_row = {
            "id": f"778:gx-billu-takeprof-1-12345678:{trade_time_ms}:40.0:0.141",
            "orderId": 778,
            "clientOrderId": "gx-billu-takeprof-1-12345678",
            "symbol": "BILLUSDT",
            "side": "SELL",
            "positionSide": "BOTH",
            "time": trade_time_ms,
            "price": 0.141,
            "qty": 40.0,
        }

        with patch("grid_optimizer.loop_runner._fetch_trade_rows_since", return_value=[trade_row]), patch(
            "grid_optimizer.loop_runner._utc_now",
            return_value=now,
        ):
            snapshot = sync_synthetic_ledger(
                state=state,
                symbol="BILLUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
                actual_position_qty=60.0,
                entry_price=0.14,
                qty_tolerance=1e-9,
                fallback_price=0.14,
            )

        self.assertAlmostEqual(snapshot["virtual_long_qty"], 60.0)
        self.assertEqual(snapshot["virtual_long_lots"], [{"qty": 60.0, "price": 0.14}])
        self.assertEqual(snapshot["applied_trade_count"], 1)
        self.assertEqual(snapshot["unmatched_trade_count"], 0)
        self.assertAlmostEqual(snapshot["grid_buffer_realized_notional"], 0.04, places=8)

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
                                "lot_consume_mode": "fifo",
                            },
                            "response": {
                                "orderId": 12345,
                                "clientOrderId": "gx-test",
                            },
                        }
                    ]
                },
            )
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertIn("synthetic_order_refs", payload)
            self.assertIn("12345", payload["synthetic_order_refs"])
            self.assertEqual(payload["synthetic_order_refs"]["12345"]["lot_consume_mode"], "fifo")

    def test_apply_active_delever_long_threshold_escalates_only_when_fifo_reduces_remaining_break_even_distance(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.001, "qty": 50.0, "notional": 50.05, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.011, "qty": 50.0, "notional": 50.55, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.021, "qty": 50.0, "notional": 51.05, "role": "take_profit_long"},
            ],
        }

        report = apply_active_delever_long(
            plan=plan,
            current_long_qty=150.0,
            current_long_notional=260.0,
            current_long_avg_price=(50.0 * 1.3 + 50.0 * 1.2 + 50.0 * 1.0) / 150.0,
            current_long_lots=[
                {"qty": 50.0, "price": 1.3},
                {"qty": 50.0, "price": 1.2},
                {"qty": 50.0, "price": 1.0},
            ],
            pause_long_position_notional=200.0,
            threshold_position_notional=250.0,
            per_order_notional=50.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.01,
            market_guard_buy_pause_active=False,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            max_active_levels=3,
        )

        active_sells = [item for item in plan["sell_orders"] if item["role"] == "active_delever_long"]
        self.assertEqual(report["trigger_mode"], "threshold")
        self.assertEqual(report["active_sell_order_count"], 2)
        self.assertEqual(len(active_sells), 2)
        self.assertTrue(all(item.get("lot_consume_mode") == "fifo" for item in active_sells))

    def test_apply_active_delever_long_disabled_threshold_waits_for_pause_notional(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.01, "qty": 30.0, "notional": 30.3, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.02, "qty": 30.0, "notional": 30.6, "role": "take_profit_long"},
            ],
        }

        report = apply_active_delever_long(
            plan=plan,
            current_long_qty=50.0,
            current_long_notional=50.0,
            current_long_avg_price=1.0,
            current_long_lots=[{"qty": 50.0, "price": 1.0}],
            pause_long_position_notional=220.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            market_guard_buy_pause_active=False,
            grid_buffer_realized_notional=100.0,
            grid_buffer_spent_notional=0.0,
            max_active_levels=2,
        )

        self.assertFalse(report["active"])
        self.assertIsNone(report["trigger_mode"])
        self.assertFalse(any(item["role"] == "active_delever_long" for item in plan["sell_orders"]))

    def test_apply_active_delever_long_pause_notional_keeps_profit_only_orders(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.001, "qty": 30.0, "notional": 30.03, "role": "take_profit_long"},
                {"side": "SELL", "price": 1.011, "qty": 30.0, "notional": 30.33, "role": "take_profit_long"},
            ],
        }

        report = apply_active_delever_long(
            plan=plan,
            current_long_qty=240.0,
            current_long_notional=240.0,
            current_long_avg_price=1.0,
            current_long_lots=[{"qty": 240.0, "price": 1.0}],
            pause_long_position_notional=200.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            market_guard_buy_pause_active=False,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            max_active_levels=2,
        )

        active_sells = [item for item in plan["sell_orders"] if item["role"] == "active_delever_long"]
        self.assertTrue(report["active"])
        self.assertEqual(report["trigger_mode"], "pause")
        self.assertEqual(report["release_sell_order_count"], 0)
        self.assertEqual([item["price"] for item in active_sells], [1.001, 1.011])
        self.assertFalse(any(item.get("execution_type") == "passive_release" for item in active_sells))
        self.assertFalse(any(bool(item.get("force_reduce_only")) for item in active_sells))

    def test_apply_active_delever_long_pause_notional_does_not_synthesize_loss_release_orders(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.07, "qty": 30.0, "notional": 32.1, "role": "flow_sleeve_long"},
            ],
        }

        report = apply_active_delever_long(
            plan=plan,
            current_long_qty=240.0,
            current_long_notional=240.0,
            current_long_avg_price=1.03,
            current_long_lots=[{"qty": 240.0, "price": 1.03}],
            pause_long_position_notional=200.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            market_guard_buy_pause_active=True,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            max_active_levels=2,
        )

        active_sells = [item for item in plan["sell_orders"] if item["role"] == "active_delever_long"]
        self.assertFalse(report["active"])
        self.assertIsNone(report["trigger_mode"])
        self.assertEqual(report["synthesized_release_source_order_count"], 0)
        self.assertEqual(report["release_sell_order_count"], 0)
        self.assertEqual(report["pruned_flow_sleeve_order_count"], 0)
        self.assertEqual(active_sells, [])
        self.assertTrue(any(item["role"] == "flow_sleeve_long" for item in plan["sell_orders"]))

    def test_apply_active_delever_long_force_release_below_pause_uses_near_market_orders(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.07, "qty": 30.0, "notional": 32.1, "role": "flow_sleeve_long"},
            ],
        }

        report = apply_active_delever_long(
            plan=plan,
            current_long_qty=1044.0,
            current_long_notional=1044.0,
            current_long_avg_price=1.03,
            current_long_lots=[{"qty": 1044.0, "price": 1.03}],
            pause_long_position_notional=1200.0,
            threshold_position_notional=0.0,
            per_order_notional=130.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            market_guard_buy_pause_active=False,
            grid_buffer_realized_notional=0.0,
            grid_buffer_spent_notional=0.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            max_active_levels=3,
            force_release_active=True,
            force_release_target_notional=650.0,
            force_release_trigger_mode="exposure_escalation",
        )

        active_sells = [item for item in plan["sell_orders"] if item["role"] == "active_delever_long"]
        self.assertTrue(report["active"])
        self.assertEqual(report["trigger_mode"], "exposure_escalation")
        self.assertEqual(report["release_sell_order_count"], 3)
        self.assertEqual(report["synthesized_release_source_order_count"], 3)
        self.assertEqual(report["pruned_flow_sleeve_order_count"], 1)
        self.assertEqual([item["price"] for item in active_sells], [1.001, 1.011, 1.021])
        self.assertTrue(all(item["execution_type"] == "passive_release" for item in active_sells))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_sells))
        self.assertFalse(any(item["role"] == "flow_sleeve_long" for item in plan["sell_orders"]))

    def test_apply_volume_long_v4_staged_delever_soft_stage_keeps_front_sell_volume_near_market(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.001, "qty": 50.0, "notional": 50.05, "role": "take_profit"},
                {"side": "SELL", "price": 1.011, "qty": 50.0, "notional": 50.55, "role": "take_profit"},
                {"side": "SELL", "price": 1.021, "qty": 50.0, "notional": 51.05, "role": "take_profit"},
            ],
        }

        report = apply_volume_long_v4_staged_delever(
            plan=plan,
            current_long_qty=150.0,
            current_long_notional=260.0,
            current_long_avg_price=1.0,
            pause_long_position_notional=200.0,
            max_position_notional=300.0,
            per_order_notional=30.0,
            step_price=0.001,
            tick_size=0.001,
            bid_price=0.997,
            ask_price=0.998,
        )

        staged_sells = [item for item in plan["sell_orders"] if item["role"] == "soft_delever_long"]
        self.assertTrue(report["active"])
        self.assertEqual(report["stage"], "soft")
        self.assertEqual(report["active_sell_order_count"], 2)
        self.assertEqual([item["price"] for item in staged_sells], [1.0, 1.0])
        self.assertEqual([item["role"] for item in plan["sell_orders"][:2]], ["soft_delever_long", "soft_delever_long"])

    def test_apply_volume_long_v4_staged_delever_hard_stage_respects_loss_floor(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 1.001, "qty": 50.0, "notional": 50.05, "role": "take_profit"},
                {"side": "SELL", "price": 1.011, "qty": 50.0, "notional": 50.55, "role": "take_profit"},
                {"side": "SELL", "price": 1.021, "qty": 50.0, "notional": 51.05, "role": "take_profit"},
                {"side": "SELL", "price": 1.031, "qty": 50.0, "notional": 51.55, "role": "take_profit"},
            ],
        }

        report = apply_volume_long_v4_staged_delever(
            plan=plan,
            current_long_qty=200.0,
            current_long_notional=330.0,
            current_long_avg_price=1.0,
            pause_long_position_notional=200.0,
            max_position_notional=300.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.979,
            ask_price=0.98,
        )

        staged_sells = [item for item in plan["sell_orders"] if item["role"] == "hard_delever_long"]
        self.assertTrue(report["active"])
        self.assertEqual(report["stage"], "hard")
        self.assertEqual(report["active_sell_order_count"], 3)
        self.assertEqual([item["price"] for item in staged_sells], [0.985, 0.99, 1.0])
        self.assertAlmostEqual(report["loss_floor_price"], 0.985, places=8)

    def test_apply_volume_long_v4_flow_sleeve_adds_reduce_only_near_market_sells(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 0.1700, "qty": 411.0, "notional": 69.87, "role": "take_profit"},
                {"side": "SELL", "price": 0.1704, "qty": 411.0, "notional": 70.03, "role": "take_profit"},
                {"side": "SELL", "price": 0.1710, "qty": 411.0, "notional": 70.28, "role": "take_profit"},
            ],
            "bootstrap_orders": [],
        }

        report = apply_volume_long_v4_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_qty=5400.0,
            current_long_notional=885.0,
            current_long_cost_basis_price=0.1676,
            trigger_notional=820.0,
            reduce_to_notional=720.0,
            sleeve_notional=180.0,
            levels=3,
            per_order_notional=70.0,
            order_notional=60.0,
            max_loss_ratio=0.012,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            bid_price=0.1658,
            ask_price=0.1659,
        )

        self.assertTrue(report["active"])
        self.assertEqual(report["placed_order_count"], 3)
        self.assertEqual(report["released_take_profit_order_count"], 3)
        self.assertLessEqual(report["placed_notional"], 165.0)
        self.assertEqual(len(plan["sell_orders"]), 3)
        self.assertTrue(all(item["role"] == "flow_sleeve_long" for item in plan["sell_orders"]))
        self.assertTrue(all(item["force_reduce_only"] for item in plan["sell_orders"]))
        self.assertEqual([item["price"] for item in plan["sell_orders"]], [0.1659, 0.1661, 0.1663])

    def test_apply_volume_long_v4_flow_sleeve_clamps_near_market_sells_to_loss_floor(self) -> None:
        plan = {"buy_orders": [], "sell_orders": [], "bootstrap_orders": []}

        report = apply_volume_long_v4_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_qty=5400.0,
            current_long_notional=885.0,
            current_long_cost_basis_price=0.1695,
            trigger_notional=820.0,
            reduce_to_notional=720.0,
            sleeve_notional=180.0,
            levels=3,
            per_order_notional=70.0,
            order_notional=60.0,
            max_loss_ratio=0.005,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            bid_price=0.1658,
            ask_price=0.1659,
        )

        self.assertTrue(report["active"])
        self.assertIsNone(report["blocked_reason"])
        self.assertAlmostEqual(report["loss_floor_price"], 0.1687)
        self.assertEqual(report["loss_guard_clamped_order_count"], 3)
        self.assertEqual(report["placed_order_count"], 1)
        self.assertEqual([item["price"] for item in plan["sell_orders"]], [0.1687])

    def test_apply_volume_long_v4_flow_sleeve_does_not_release_take_profit_when_no_flow_order_places(self) -> None:
        plan = {
            "buy_orders": [],
            "sell_orders": [
                {"side": "SELL", "price": 0.1700, "qty": 411.0, "notional": 69.87, "role": "take_profit"},
                {"side": "SELL", "price": 0.1704, "qty": 411.0, "notional": 70.03, "role": "take_profit"},
            ],
            "bootstrap_orders": [],
        }

        report = apply_volume_long_v4_flow_sleeve(
            plan=plan,
            enabled=True,
            current_long_qty=5400.0,
            current_long_notional=885.0,
            current_long_cost_basis_price=0.1695,
            trigger_notional=820.0,
            reduce_to_notional=720.0,
            sleeve_notional=180.0,
            levels=3,
            per_order_notional=70.0,
            order_notional=60.0,
            max_loss_ratio=0.005,
            step_price=0.0002,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=500.0,
            bid_price=0.1658,
            ask_price=0.1659,
        )

        self.assertFalse(report["active"])
        self.assertEqual(report["blocked_reason"], "no_valid_order")
        self.assertEqual(report["released_take_profit_order_count"], 0)
        self.assertEqual([item["role"] for item in plan["sell_orders"]], ["take_profit", "take_profit"])

    def test_apply_active_delever_short_disabled_threshold_waits_for_pause_notional(self) -> None:
        plan = {
            "buy_orders": [
                {"side": "BUY", "price": 0.99, "qty": 30.0, "notional": 29.7, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.98, "qty": 30.0, "notional": 29.4, "role": "take_profit_short"},
            ],
            "sell_orders": [],
        }

        report = apply_active_delever_short(
            plan=plan,
            current_short_qty=50.0,
            current_short_notional=50.0,
            current_short_avg_price=1.0,
            current_short_lots=[{"qty": 50.0, "price": 1.0}],
            pause_short_position_notional=220.0,
            threshold_position_notional=0.0,
            per_order_notional=30.0,
            step_price=0.01,
            tick_size=0.001,
            bid_price=0.999,
            ask_price=1.001,
            min_profit_ratio=0.0,
            grid_buffer_realized_notional=100.0,
            grid_buffer_spent_notional=0.0,
            max_active_levels=2,
        )

        self.assertFalse(report["active"])
        self.assertIsNone(report["trigger_mode"])
        self.assertFalse(any(item["role"] == "active_delever_short" for item in plan["buy_orders"]))


    def test_decorate_synthetic_open_orders_applies_role_metadata(self) -> None:
        state = {
            "synthetic_order_refs": {
                "12345": {
                    "role": "entry_long",
                    "position_side": "BOTH",
                    "client_order_id": "gx-test",
                }
            }
        }
        open_orders = [
            {
                "orderId": 12345,
                "clientOrderId": "gx-test",
                "side": "BUY",
                "price": "0.3173",
                "origQty": "141",
            }
        ]

        decorated = _decorate_synthetic_open_orders(state=state, open_orders=open_orders)

        self.assertEqual(decorated[0]["role"], "entry_long")
        self.assertEqual(decorated[0]["positionSide"], "BOTH")

    def test_generate_competition_inventory_grid_plan_bootstraps_two_orders_when_futures_flat(self) -> None:
        plan = _generate_competition_inventory_grid_plan(
            state={},
            trades=[],
            current_position_qty=0.0,
            bid_price=0.0999,
            ask_price=0.1001,
            step_price=0.01,
            first_order_multiplier=4.0,
            per_order_notional=10.0,
            threshold_position_notional=50.0,
            max_order_position_notional=80.0,
            max_position_notional=120.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=0.1,
        )

        self.assertEqual(plan["strategy_mode"], "competition_inventory_grid")
        self.assertEqual({item["side"] for item in plan["bootstrap_orders"]}, {"BUY", "SELL"})
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(plan["direction_state"], "flat")
        self.assertEqual(plan["pair_credit_steps"], 0)
        self.assertEqual(plan["recovery_mode"], "live")
        self.assertGreater(plan["grid_anchor_price"], 0.0)

    def test_generate_competition_inventory_grid_plan_uses_cached_runtime_beyond_trade_horizon(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side="BUY",
            price=100.0,
            qty=2.0,
            fill_time_ms=1000,
            step_price=5.0,
        )

        plan = _generate_competition_inventory_grid_plan(
            state={
                "competition_inventory_grid_runtime_cache": {
                    "strategy_mode": "competition_inventory_grid",
                    "market_type": "futures",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                }
            },
            trades=[],
            current_position_qty=2.0,
            bid_price=109.0,
            ask_price=111.0,
            step_price=5.0,
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

        self.assertEqual(plan["recovery_mode"], "live")
        self.assertEqual(plan["direction_state"], "long_active")
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertAlmostEqual(plan["sell_orders"][0]["qty"], 1.0, places=8)

    def test_update_inventory_grid_order_refs_records_placed_orders(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")

            _update_inventory_grid_order_refs(
                state_path=state_path,
                strategy_mode="competition_inventory_grid",
                submit_report={
                    "placed_orders": [
                        {
                            "request": {
                                "role": "bootstrap_entry",
                                "side": "SELL",
                            },
                            "response": {
                                "orderId": 67890,
                                "clientOrderId": "grid-test",
                            },
                        }
                    ]
                },
            )

            payload = state_path.read_text(encoding="utf-8")
            self.assertIn("inventory_grid_order_refs", payload)
            self.assertIn("67890", payload)

    def test_update_inventory_grid_order_refs_keeps_refs_beyond_trim_boundary_for_competition_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            refs = {
                str(idx): {
                    "role": "grid_entry",
                    "side": "BUY",
                    "updated_at": "2026-04-01T00:00:00+00:00",
                }
                for idx in range(1, 5001)
            }
            state_path.write_text(
                json.dumps({"inventory_grid_order_refs": refs}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            _update_inventory_grid_order_refs(
                state_path=state_path,
                strategy_mode="competition_inventory_grid",
                submit_report={
                    "placed_orders": [
                        {
                            "request": {
                                "role": "grid_exit",
                                "side": "SELL",
                            },
                            "response": {
                                "orderId": 5001,
                                "clientOrderId": "grid-boundary",
                            },
                        }
                    ]
                },
            )

            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["inventory_grid_order_refs"]), 5001)
            self.assertIn("1", payload["inventory_grid_order_refs"])
            self.assertIn("5001", payload["inventory_grid_order_refs"])

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

    def test_apply_hedge_position_controls_keeps_probe_short_on_external_short_pause_when_allowed(self) -> None:
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
            current_long_qty=120.0,
            current_short_qty=0.0,
            mid_price=1.0,
            pause_long_position_notional=None,
            pause_short_position_notional=None,
            min_mid_price_for_buys=None,
            external_short_pause=True,
            external_short_pause_reasons=["market_bias_strong_short_pause regime=strong score=+0.24 >= +0.15"],
            preserve_short_entry_on_external_pause=True,
        )

        self.assertTrue(result["short_paused"])
        self.assertEqual(result["short_pause_reasons"], ["market_bias_strong_short_pause regime=strong score=+0.24 >= +0.15"])
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 2)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit_long")
        self.assertEqual(plan["sell_orders"][1]["role"], "entry_short")

    def test_apply_hedge_position_controls_hard_short_pause_still_removes_probe_short(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"side": "BUY", "price": 1.18, "qty": 10, "notional": 11.8, "role": "take_profit_short", "position_side": "SHORT"}],
            "sell_orders": [
                {"side": "SELL", "price": 1.22, "qty": 10, "notional": 12.2, "role": "take_profit_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.23, "qty": 10, "notional": 12.3, "role": "entry_short", "position_side": "SHORT"},
            ],
        }

        result = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=150.0,
            mid_price=1.0,
            pause_long_position_notional=None,
            pause_short_position_notional=100.0,
            min_mid_price_for_buys=None,
            external_short_pause=True,
            external_short_pause_reasons=["market_bias_strong_short_pause regime=strong score=+0.24 >= +0.15"],
            preserve_short_entry_on_external_pause=True,
        )

        self.assertTrue(result["short_paused"])
        self.assertTrue(any("pause_short_position_notional" in reason for reason in result["short_pause_reasons"]))
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit_long")

    def test_apply_hedge_position_controls_can_keep_probe_short_on_inventory_pause_when_allowed(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"side": "BUY", "price": 1.18, "qty": 10, "notional": 11.8, "role": "take_profit_short", "position_side": "SHORT"}],
            "sell_orders": [
                {"side": "SELL", "price": 1.22, "qty": 10, "notional": 12.2, "role": "take_profit_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.23, "qty": 3, "notional": 3.69, "role": "entry_short", "position_side": "SHORT"},
            ],
        }

        result = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=150.0,
            mid_price=1.0,
            pause_long_position_notional=None,
            pause_short_position_notional=100.0,
            min_mid_price_for_buys=None,
            preserve_short_entry_on_inventory_pause=True,
        )

        self.assertTrue(result["short_paused"])
        self.assertEqual(len(plan["sell_orders"]), 2)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit_long")
        self.assertEqual(plan["sell_orders"][1]["role"], "entry_short")

    def test_apply_hedge_position_controls_counts_pending_short_entries_toward_pause(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"side": "BUY", "price": 1.18, "qty": 10, "notional": 11.8, "role": "take_profit_short", "position_side": "SHORT"}],
            "sell_orders": [
                {"side": "SELL", "price": 1.22, "qty": 10, "notional": 12.2, "role": "take_profit_long", "position_side": "LONG"},
                {"side": "SELL", "price": 1.23, "qty": 80, "notional": 98.4, "role": "entry_short", "position_side": "SHORT"},
            ],
        }

        result = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=150.0,
            mid_price=1.0,
            pause_long_position_notional=None,
            pause_short_position_notional=200.0,
            min_mid_price_for_buys=None,
            open_entry_short_notional=80.0,
            preserve_short_entry_on_inventory_pause=True,
        )

        self.assertTrue(result["short_paused"])
        self.assertAlmostEqual(result["projected_short_notional"], 230.0)
        self.assertTrue(any("projected_short_notional" in reason for reason in result["short_pause_reasons"]))
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit_long")

    def test_apply_hedge_position_controls_can_keep_probe_long_on_inventory_pause_when_allowed(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {"side": "BUY", "price": 1.18, "qty": 10, "notional": 11.8, "role": "take_profit_short", "position_side": "SHORT"},
                {"side": "BUY", "price": 1.17, "qty": 3, "notional": 3.51, "role": "entry_long", "position_side": "LONG"},
            ],
            "sell_orders": [{"side": "SELL", "price": 1.22, "qty": 10, "notional": 12.2, "role": "take_profit_long", "position_side": "LONG"}],
        }

        result = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=150.0,
            current_short_qty=0.0,
            mid_price=1.0,
            pause_long_position_notional=100.0,
            pause_short_position_notional=None,
            min_mid_price_for_buys=None,
            preserve_long_entry_on_inventory_pause=True,
        )

        self.assertTrue(result["long_paused"])
        self.assertEqual(len(plan["buy_orders"]), 2)
        self.assertEqual(plan["buy_orders"][0]["role"], "take_profit_short")
        self.assertEqual(plan["buy_orders"][1]["role"], "entry_long")

    def test_resolve_short_threshold_timeout_state_stays_active_until_pause(self) -> None:
        state: dict[str, object] = {}
        start = datetime(2026, 4, 14, 2, 0, 0, tzinfo=timezone.utc)

        initial = resolve_short_threshold_timeout_state(
            state=state,
            current_short_notional=260.0,
            pause_short_position_notional=200.0,
            threshold_position_notional=240.0,
            hold_seconds=60.0,
            now=start,
        )
        self.assertFalse(initial["timeout_active"])
        self.assertTrue(initial["armed"])

        active = resolve_short_threshold_timeout_state(
            state=state,
            current_short_notional=255.0,
            pause_short_position_notional=200.0,
            threshold_position_notional=240.0,
            hold_seconds=60.0,
            now=start + timedelta(seconds=61),
        )
        self.assertTrue(active["timeout_active"])

        still_active = resolve_short_threshold_timeout_state(
            state=state,
            current_short_notional=220.0,
            pause_short_position_notional=200.0,
            threshold_position_notional=240.0,
            hold_seconds=60.0,
            now=start + timedelta(seconds=90),
        )
        self.assertTrue(still_active["timeout_active"])

        reset = resolve_short_threshold_timeout_state(
            state=state,
            current_short_notional=199.0,
            pause_short_position_notional=200.0,
            threshold_position_notional=240.0,
            hold_seconds=60.0,
            now=start + timedelta(seconds=95),
        )
        self.assertFalse(reset["timeout_active"])
        self.assertNotIn("short_threshold_timeout_state", state)

    def test_resolve_inventory_pause_timeout_state_stays_active_until_pause(self) -> None:
        state: dict[str, object] = {}
        start = datetime(2026, 4, 14, 2, 0, 0, tzinfo=timezone.utc)

        initial = resolve_inventory_pause_timeout_state(
            state=state,
            side="long",
            current_notional=240.0,
            pause_position_notional=200.0,
            hold_seconds=60.0,
            now=start,
        )
        self.assertFalse(initial["timeout_active"])
        self.assertTrue(initial["armed"])

        active = resolve_inventory_pause_timeout_state(
            state=state,
            side="long",
            current_notional=235.0,
            pause_position_notional=200.0,
            hold_seconds=60.0,
            now=start + timedelta(seconds=61),
        )
        self.assertTrue(active["timeout_active"])

        reset = resolve_inventory_pause_timeout_state(
            state=state,
            side="long",
            current_notional=199.0,
            pause_position_notional=200.0,
            hold_seconds=60.0,
            now=start + timedelta(seconds=95),
        )
        self.assertFalse(reset["timeout_active"])
        self.assertNotIn("long_inventory_pause_timeout_state", state)

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

    def test_apply_hedge_position_notional_caps_subtracts_pending_short_entries(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"side": "BUY", "price": 1.0, "qty": 10, "notional": 10.0, "role": "take_profit_short", "position_side": "SHORT"}],
            "sell_orders": [{"side": "SELL", "price": 1.0, "qty": 100, "notional": 100.0, "role": "entry_short", "position_side": "SHORT"}],
        }

        result = apply_hedge_position_notional_caps(
            plan=plan,
            current_long_notional=0.0,
            current_short_notional=300.0,
            max_long_position_notional=None,
            max_short_position_notional=400.0,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            open_entry_short_notional=100.0,
        )

        self.assertTrue(result["short_cap_applied"])
        self.assertAlmostEqual(result["short_budget_notional"], 0.0)
        self.assertAlmostEqual(result["planned_short_notional"], 0.0)
        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(len(plan["buy_orders"]), 1)

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

    def test_assess_flat_start_guard_allows_lingering_open_orders_when_cancel_stale_will_cleanup(self) -> None:
        result = assess_flat_start_guard(
            strategy_mode="one_way_short",
            actual_net_qty=0.0,
            open_orders=[{"orderId": 1}, {"orderId": 2}],
            enabled=True,
            block_open_orders=False,
        )

        self.assertFalse(result["blocked"])
        self.assertIn("自动撤单", str(result["reason"]))
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

    def test_apply_take_profit_profit_guard_clamps_short_cover_to_cost_for_light_inventory(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {"side": "BUY", "price": 0.0596, "qty": 755, "notional": 44.998, "role": "take_profit_short"},
                {"side": "BUY", "price": 0.0588, "qty": 760, "notional": 44.688, "role": "entry_long"},
            ],
            "sell_orders": [{"side": "SELL", "price": 0.0597, "qty": 753, "notional": 44.9541, "role": "entry_short"}],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=2270.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.0590990308370044,
            current_long_notional=0.0,
            current_short_notional=135.1785,
            pause_long_position_notional=None,
            pause_short_position_notional=320.0,
            min_profit_ratio=0.001,
            tick_size=0.0001,
            bid_price=0.0592,
            ask_price=0.0593,
        )

        self.assertTrue(result["short_active"])
        self.assertEqual(result["adjusted_buy_orders"], 1)
        self.assertAlmostEqual(plan["buy_orders"][0]["price"], 0.0590, places=8)
        self.assertAlmostEqual(plan["buy_orders"][0]["notional"], 0.0590 * 755, places=8)
        self.assertEqual(plan["buy_orders"][1]["price"], 0.0588)

    def test_apply_take_profit_profit_guard_keeps_short_non_loss_floor_above_pause_threshold(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"side": "BUY", "price": 0.0596, "qty": 755, "notional": 44.998, "role": "take_profit_short"}],
            "sell_orders": [],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=6000.0,
            current_long_avg_price=0.0,
            current_short_avg_price=0.0590990308370044,
            current_long_notional=0.0,
            current_short_notional=357.0,
            pause_long_position_notional=None,
            pause_short_position_notional=320.0,
            min_profit_ratio=0.001,
            tick_size=0.0001,
            bid_price=0.0592,
            ask_price=0.0593,
        )

        self.assertTrue(result["short_active"])
        self.assertEqual(result["adjusted_buy_orders"], 1)
        self.assertAlmostEqual(plan["buy_orders"][0]["price"], 0.0590, places=8)

    def test_apply_take_profit_profit_guard_keeps_non_loss_floor_until_threshold(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [],
            "sell_orders": [{"side": "SELL", "price": 4630.0, "qty": 0.01, "notional": 46.3, "role": "take_profit_long"}],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=0.06,
            current_short_qty=0.0,
            current_long_avg_price=4635.0,
            current_short_avg_price=0.0,
            current_long_notional=278.1,
            current_short_notional=0.0,
            pause_long_position_notional=220.0,
            pause_short_position_notional=None,
            threshold_long_position_notional=320.0,
            min_profit_ratio=0.0001,
            tick_size=0.01,
            bid_price=4634.0,
            ask_price=4634.5,
        )

        self.assertTrue(result["long_active"])
        self.assertAlmostEqual(result["long_relax_threshold_notional"], 320.0, places=8)
        self.assertEqual(result["adjusted_sell_orders"], 1)
        self.assertAlmostEqual(plan["sell_orders"][0]["price"], 4635.47, places=8)

    def test_apply_take_profit_profit_guard_clamps_lot_tracked_and_active_long_orders_to_stricter_cost_floor(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [],
            "sell_orders": [
                {
                    "side": "SELL",
                    "price": 0.99,
                    "qty": 200.0,
                    "notional": 198.0,
                    "role": "take_profit_long",
                    "lot_tracked": True,
                },
                {
                    "side": "SELL",
                    "price": 1.0,
                    "qty": 100.0,
                    "notional": 100.0,
                    "role": "active_delever_long",
                },
            ],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=300.0,
            current_short_qty=0.0,
            current_long_avg_price=1.02,
            current_short_avg_price=0.0,
            synthetic_long_avg_price=1.03,
            current_long_notional=306.0,
            current_short_notional=0.0,
            pause_long_position_notional=220.0,
            pause_short_position_notional=None,
            threshold_long_position_notional=320.0,
            min_profit_ratio=0.001,
            tick_size=0.001,
            bid_price=1.005,
            ask_price=1.006,
        )

        self.assertTrue(result["long_active"])
        self.assertEqual(result["adjusted_sell_orders"], 2)
        self.assertAlmostEqual(plan["sell_orders"][0]["price"], 1.032, places=8)
        self.assertAlmostEqual(plan["sell_orders"][1]["price"], 1.032, places=8)

    def test_apply_take_profit_profit_guard_clamps_best_quote_single_way_exit_roles(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {"side": "BUY", "price": 0.99, "qty": 100.0, "notional": 99.0, "role": "best_quote_entry_long"},
            ],
            "sell_orders": [
                {"side": "SELL", "price": 1.00, "qty": 100.0, "notional": 100.0, "role": "best_quote_entry_short"},
                {"side": "SELL", "price": 1.01, "qty": 50.0, "notional": 50.5, "role": "best_quote_reduce_long"},
            ],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=150.0,
            current_short_qty=100.0,
            current_long_avg_price=1.02,
            current_short_avg_price=0.98,
            current_long_notional=153.0,
            current_short_notional=98.0,
            pause_long_position_notional=500.0,
            pause_short_position_notional=500.0,
            min_profit_ratio=0.001,
            tick_size=0.001,
            bid_price=1.000,
            ask_price=1.001,
            extra_long_guard_roles={"best_quote_entry_short", "best_quote_reduce_long"},
            extra_short_guard_roles={"best_quote_entry_long", "best_quote_reduce_short"},
        )

        self.assertTrue(result["long_active"])
        self.assertTrue(result["short_active"])
        self.assertEqual(result["adjusted_sell_orders"], 2)
        self.assertEqual(result["adjusted_buy_orders"], 1)
        self.assertAlmostEqual(plan["sell_orders"][0]["price"], 1.022, places=8)
        self.assertAlmostEqual(plan["sell_orders"][1]["price"], 1.022, places=8)
        self.assertAlmostEqual(plan["buy_orders"][0]["price"], 0.979, places=8)

    @patch("grid_optimizer.loop_runner.load_or_initialize_state")
    @patch("grid_optimizer.loop_runner.sync_synthetic_ledger")
    @patch("grid_optimizer.loop_runner.assess_market_guard")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.fetch_futures_symbol_config")
    def test_generate_plan_report_synthetic_neutral_suppresses_pause_release_after_take_profit_guard(
        self,
        mock_symbol_config,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
        mock_sync_synthetic_ledger,
        mock_load_state,
    ) -> None:
        mock_load_state.return_value = {
            "center_price": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synthetic_ledger": {},
        }
        mock_symbol_config.return_value = {
            "tick_size": 0.001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.999", "ask_price": "1.001"}]
        mock_premium_index.return_value = [{"funding_rate": "0.0001"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "TESTUSDT", "positionAmt": "300", "entryPrice": "1.035"},
            ],
        }
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {
            "buy_pause_active": False,
            "buy_pause_reasons": [],
            "short_cover_pause_active": False,
            "short_cover_pause_reasons": [],
            "shift_frozen": False,
            "shift_freeze_reasons": [],
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }
        mock_sync_synthetic_ledger.return_value = {
            "virtual_long_qty": 300.0,
            "virtual_long_avg_price": 1.035,
            "virtual_long_lots": [{"qty": 300.0, "price": 1.035}],
            "virtual_short_qty": 0.0,
            "virtual_short_avg_price": 0.0,
            "virtual_short_lots": [],
            "applied_trade_count": 0,
            "unmatched_trade_count": 0,
            "resynced_to_actual": False,
            "grid_buffer_realized_notional": 0.0,
            "grid_buffer_spent_notional": 0.0,
        }

        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.symbol = "TESTUSDT"
            args.strategy_mode = "synthetic_neutral"
            args.step_price = 0.01
            args.buy_levels = 4
            args.sell_levels = 4
            args.per_order_notional = 25.0
            args.base_position_notional = 0.0
            args.pause_buy_position_notional = 200.0
            args.pause_short_position_notional = 700.0
            args.threshold_position_notional = 250.0
            args.take_profit_min_profit_ratio = 0.001
            args.reset_state = True
            args.state_path = str(Path(tmpdir) / "testusdt_state.json")
            args.summary_jsonl = str(Path(tmpdir) / "testusdt_events.jsonl")

            report = generate_plan_report(args)

        active_sells = [item for item in report["sell_orders"] if item["role"] == "active_delever_long"]
        take_profit_sells = [item for item in report["sell_orders"] if item["role"] == "take_profit_long"]

        self.assertEqual(active_sells, [])
        self.assertFalse(report["active_delever"]["active"])
        self.assertIsNone(report["active_delever"]["trigger_mode"])
        self.assertEqual(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertTrue(all(item["price"] >= report["take_profit_guard"]["long_floor_price"] for item in take_profit_sells))
        self.assertAlmostEqual(report["take_profit_guard"]["long_floor_price"], 1.037, places=8)

    def test_apply_take_profit_profit_guard_keeps_break_even_floor_when_ratio_is_zero(self) -> None:
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [],
            "sell_orders": [{"side": "SELL", "price": 0.0597, "qty": 753, "notional": 44.9541, "role": "take_profit_long"}],
        }

        result = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=2270.0,
            current_short_qty=0.0,
            current_long_avg_price=0.060803016737019634,
            current_short_avg_price=0.0,
            current_long_notional=135.1785,
            current_short_notional=0.0,
            pause_long_position_notional=1200.0,
            pause_short_position_notional=None,
            min_profit_ratio=0.0,
            tick_size=0.0001,
            bid_price=0.0592,
            ask_price=0.0593,
        )

        self.assertTrue(result["enabled"])
        self.assertTrue(result["long_active"])
        self.assertEqual(result["adjusted_sell_orders"], 1)
        self.assertAlmostEqual(plan["sell_orders"][0]["price"], 0.0609, places=8)

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

    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index", return_value=[{"markPrice": "1.0001", "lastFundingRate": "0.0"}])
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_handles_optional_synthetic_fields(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        _mock_premium_index,
    ) -> None:
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
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_raises_entry_order_below_min_notional_after_post_only_adjustment(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_inventory_grid_refs,
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
        mock_premium_index.return_value = [{"markPrice": "0.50", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {"multiAssetsMargin": False, "positions": [{"symbol": "KATUSDT", "positionAmt": "0", "entryPrice": "0"}]}
        mock_open_orders.return_value = [{"clientOrderId": "mt_katusdt_buy_001", "orderId": 42}]
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
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertTrue(report["executed"])
        self.assertEqual(report["skipped_orders"], [])
        self.assertEqual(len(report["placed_orders"]), 1)
        self.assertAlmostEqual(report["placed_orders"][0]["request"]["qty"], 10.0, places=8)
        self.assertAlmostEqual(report["placed_orders"][0]["request"]["submitted_notional"], 5.0, places=8)
        mock_post_order.assert_called_once()
        self.assertAlmostEqual(mock_post_order.call_args.kwargs["quantity"], 10.0, places=8)
        mock_update_refs.assert_called_once()

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_competition_inventory_grid_sets_reduce_only_by_role(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_inventory_grid_refs,
        mock_update_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 5,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "bootstrap_entry", "side": "BUY", "qty": 10.0, "price": 0.10},
                    {"role": "grid_entry", "side": "BUY", "qty": 10.0, "price": 0.09},
                    {"role": "grid_exit", "side": "SELL", "qty": 10.0, "price": 0.11},
                    {"role": "forced_reduce", "side": "SELL", "qty": 10.0, "price": 0.08},
                    {"role": "tail_cleanup", "side": "SELL", "qty": 10.0, "price": 0.10},
                ],
            },
        }
        mock_book_tickers.return_value = [{"bid_price": "0.07", "ask_price": "0.12"}]
        mock_premium_index.return_value = [{"markPrice": "0.095", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "KATUSDT", "positionAmt": "30", "entryPrice": "0.10"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = [
            {"orderId": 1, "clientOrderId": "a"},
            {"orderId": 2, "clientOrderId": "b"},
            {"orderId": 3, "clientOrderId": "c"},
            {"orderId": 4, "clientOrderId": "d"},
            {"orderId": 5, "clientOrderId": "e"},
        ]

        args = Namespace(
            symbol="KATUSDT",
            strategy_mode="competition_inventory_grid",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/katusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=2,
            maker_retries=0,
            recv_window=5000,
            state_path="output/katusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "KATUSDT",
            "strategy_mode": "competition_inventory_grid",
            "mid_price": 0.095,
            "step_price": 0.01,
            "open_order_count": 0,
            "current_long_qty": 30.0,
            "current_short_qty": 0.0,
            "actual_net_qty": 30.0,
            "symbol_info": {
                "tick_size": 0.01,
                "min_qty": 0.1,
                "min_notional": 0.1,
            },
        }

        execute_plan_report(args, plan_report)

        reduce_only_values = [call.kwargs["reduce_only"] for call in mock_post_order.call_args_list]
        self.assertEqual(reduce_only_values, [None, None, True, True, True])
        mock_update_inventory_grid_refs.assert_called_once()
        mock_update_refs.assert_called_once()

    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_synthetic_neutral_exit_orders_use_reduce_only(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_synthetic_refs,
        mock_update_inventory_grid_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 4,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "take_profit_short", "side": "BUY", "qty": 100.0, "price": 0.10},
                    {"role": "active_delever_short", "side": "BUY", "qty": 100.0, "price": 0.09},
                    {"role": "flow_sleeve_short", "side": "BUY", "qty": 50.0, "price": 0.095},
                ],
            },
        }
        mock_book_tickers.return_value = [{"bid_price": "0.08", "ask_price": "0.11"}]
        mock_premium_index.return_value = [{"markPrice": "0.095", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "SOONUSDT", "positionAmt": "-250", "entryPrice": "0.18"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = [
            {"orderId": 1, "clientOrderId": "a"},
            {"orderId": 2, "clientOrderId": "b"},
            {"orderId": 3, "clientOrderId": "c"},
        ]

        args = Namespace(
            symbol="SOONUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/soonusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=2,
            maker_retries=0,
            recv_window=5000,
            state_path="output/soonusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "SOONUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.095,
            "step_price": 0.01,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 250.0,
            "actual_net_qty": -250.0,
            "symbol_info": {
                "tick_size": 0.01,
                "min_qty": 0.1,
                "min_notional": 0.1,
            },
        }

        execute_plan_report(args, plan_report)

        reduce_only_values = [call.kwargs["reduce_only"] for call in mock_post_order.call_args_list]
        self.assertEqual(reduce_only_values, [True, True, True])
        mock_update_synthetic_refs.assert_called_once()
        mock_update_inventory_grid_refs.assert_called_once()

    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_hedge_reduce_orders_omit_reduce_only(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_synthetic_refs,
        mock_update_inventory_grid_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {
                        "role": "adverse_reduce_long",
                        "side": "SELL",
                        "position_side": "LONG",
                        "qty": 16.0,
                        "price": 0.5985,
                        "force_reduce_only": True,
                    },
                ],
            },
        }
        mock_book_tickers.return_value = [{"bid_price": "0.5984", "ask_price": "0.5985"}]
        mock_premium_index.return_value = [{"markPrice": "0.59845", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": True}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [
                {"symbol": "PHAROSUSDT", "positionSide": "LONG", "positionAmt": "888", "entryPrice": "0.6047"},
                {"symbol": "PHAROSUSDT", "positionSide": "SHORT", "positionAmt": "0", "entryPrice": "0"},
            ],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 10}
        mock_post_order.return_value = {"orderId": 1, "clientOrderId": "reduce"}

        args = Namespace(
            symbol="PHAROSUSDT",
            strategy_mode="hedge_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/pharosusdt_hedge_neutral_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=10,
            maker_retries=0,
            recv_window=5000,
            state_path="output/pharosusdt_hedge_neutral_state.json",
        )
        plan_report = {
            "symbol": "PHAROSUSDT",
            "strategy_mode": "hedge_neutral",
            "mid_price": 0.59845,
            "step_price": 0.0008,
            "open_order_count": 0,
            "current_long_qty": 888.0,
            "current_short_qty": 0.0,
            "actual_net_qty": 888.0,
            "symbol_info": {
                "tick_size": 0.0001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        execute_plan_report(args, plan_report)

        self.assertIsNone(mock_post_order.call_args.kwargs["reduce_only"])
        self.assertEqual(mock_post_order.call_args.kwargs["position_side"], "LONG")
        mock_post_order.reset_mock(return_value=True, side_effect=True)
        mock_post_order.return_value = {"orderId": 2, "clientOrderId": "reduce-hedge-bq"}
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {
                        "role": "best_quote_reduce_long",
                        "side": "SELL",
                        "position_side": "LONG",
                        "qty": 16.0,
                        "price": 0.5985,
                        "force_reduce_only": True,
                    },
                ],
            },
        }
        args.strategy_mode = "hedge_best_quote_maker_volume_v1"
        args.plan_json = "output/pharosusdt_hedge_best_quote_maker_volume_latest_plan.json"
        plan_report["strategy_mode"] = "hedge_best_quote_maker_volume_v1"

        execute_plan_report(args, plan_report)

        self.assertIsNone(mock_post_order.call_args.kwargs["reduce_only"])
        self.assertEqual(mock_post_order.call_args.kwargs["position_side"], "LONG")
        self.assertEqual(mock_update_synthetic_refs.call_count, 2)
        self.assertEqual(mock_update_inventory_grid_refs.call_count, 2)

    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_caps_one_way_reduce_only_orders_to_net_position(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_synthetic_refs,
        mock_update_inventory_grid_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 4,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "take_profit_short", "side": "BUY", "qty": 80.0, "price": 0.1724},
                    {"role": "take_profit_short", "side": "BUY", "qty": 173.0, "price": 0.1725},
                    {"role": "active_delever_short", "side": "BUY", "qty": 173.0, "price": 0.1730},
                    {"role": "active_delever_short", "side": "BUY", "qty": 80.0, "price": 0.1728},
                ],
            },
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1731", "ask_price": "0.1732"}]
        mock_premium_index.return_value = [{"markPrice": "0.17315", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "SOONUSDT", "positionAmt": "-253", "entryPrice": "0.1735"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = [
            {"orderId": 1, "clientOrderId": "a"},
            {"orderId": 2, "clientOrderId": "b"},
        ]

        args = Namespace(
            symbol="SOONUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/soonusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=2,
            maker_retries=0,
            recv_window=5000,
            state_path="output/soonusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "SOONUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.1730,
            "step_price": 0.0002,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 253.0,
            "actual_net_qty": -253.0,
            "symbol_info": {
                "tick_size": 0.0001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        execute_plan_report(args, plan_report)

        posted_quantities = [call.kwargs["quantity"] for call in mock_post_order.call_args_list]
        posted_roles = [
            call.kwargs["new_client_order_id"].split("-")[2]
            for call in mock_post_order.call_args_list
        ]
        self.assertEqual(sorted(posted_quantities), [80.0, 173.0])
        self.assertTrue(set(posted_roles).issubset({"takeprof", "activede"}))
        mock_update_synthetic_refs.assert_called_once()
        mock_update_inventory_grid_refs.assert_called_once()

    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_skips_reduce_only_rejects_after_position_race(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        mock_update_synthetic_refs,
        mock_update_inventory_grid_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "take_profit_short", "side": "BUY", "qty": 100.0, "price": 0.1674}
                ],
            },
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1673", "ask_price": "0.1674"}]
        mock_premium_index.return_value = [{"markPrice": "0.16735", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "SOONUSDT", "positionAmt": "-100", "entryPrice": "0.1680"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = RuntimeError("Binance API error -2022: ReduceOnly Order is rejected.")

        args = Namespace(
            symbol="SOONUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/soonusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=2,
            maker_retries=0,
            recv_window=5000,
            state_path="output/soonusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "SOONUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.1674,
            "step_price": 0.0002,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 100.0,
            "actual_net_qty": -100.0,
            "symbol_info": {
                "tick_size": 0.0001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertTrue(report["executed"])
        self.assertEqual(report["placed_orders"], [])
        self.assertEqual(len(report["skipped_orders"]), 1)
        self.assertEqual(report["skipped_orders"][0]["reason"]["reason"], "reduce_only_rejected")
        mock_update_synthetic_refs.assert_called_once()
        mock_update_inventory_grid_refs.assert_called_once()

    @patch("grid_optimizer.loop_runner.enforce_execution_action_limits")
    @patch("grid_optimizer.loop_runner.apply_anti_chase_entry_guard_to_actions")
    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_returns_blocked_report_after_runtime_guard_filters_actions(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_update_inventory_grid_refs,
        mock_update_synthetic_refs,
        mock_anti_chase,
        mock_enforce_limits,
    ) -> None:
        initial_actions = {
            "place_count": 1,
            "cancel_count": 0,
            "cancel_orders": [],
            "place_orders": [{"role": "entry_short", "side": "SELL", "qty": 100.0, "price": 0.067}],
        }
        filtered_actions = {
            "place_count": 1,
            "cancel_count": 0,
            "cancel_orders": [],
            "place_orders": [{"role": "entry_short", "side": "SELL", "qty": 100.0, "price": 0.067}],
            "anti_chase_entry_guard": {
                "enabled": True,
                "block_short_entries": True,
                "dropped_order_count": 0,
            },
        }
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": initial_actions,
        }
        mock_anti_chase.return_value = filtered_actions
        mock_enforce_limits.return_value = {
            "ok": False,
            "errors": ["max total notional exceeded"],
            "actions": filtered_actions,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.0669", "ask_price": "0.0670"}]
        mock_premium_index.return_value = [{"markPrice": "0.06695", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "CHIPUSDT", "positionAmt": "-73", "entryPrice": "0.0680"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 3}

        args = Namespace(
            symbol="CHIPUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/chipusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=3,
            maker_retries=0,
            recv_window=5000,
            state_path="output/chipusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "CHIPUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.0670,
            "step_price": 0.00013,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 73.0,
            "actual_net_qty": -73.0,
            "symbol_info": {
                "tick_size": 0.00001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertFalse(report["executed"])
        self.assertTrue(report["blocked"])
        self.assertFalse(report["idle"])
        self.assertEqual(report["error"]["reason"], "validation_failed")
        self.assertEqual(report["validation"]["errors"], ["max total notional exceeded"])
        mock_update_synthetic_refs.assert_not_called()
        mock_update_inventory_grid_refs.assert_not_called()

    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders", return_value=[])
    @patch(
        "grid_optimizer.loop_runner.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "positions": [{"symbol": "CHIPUSDT", "positionAmt": "0", "entryPrice": "0"}],
        },
    )
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index", return_value=[{"markPrice": "0.0670", "lastFundingRate": "0.0"}])
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers", return_value=[{"bid_price": "0.0669", "ask_price": "0.0670"}])
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_exposes_top_level_submit_summary_fields(
        self,
        mock_validate_plan_report,
        _mock_book_tickers,
        _mock_premium_index,
        _mock_credentials,
        _mock_position_mode,
        _mock_account_info,
        _mock_open_orders,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 0,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [],
            },
        }
        args = Namespace(
            symbol="CHIPUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/chipusdt_loop_latest_plan.json",
            apply=False,
            margin_type="KEEP",
            leverage=3,
            maker_retries=0,
            recv_window=5000,
            state_path="output/chipusdt_loop_state.json",
        )
        plan_report = {
            "symbol": "CHIPUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.0670,
            "step_price": 0.00013,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_long_notional": 0.0,
            "current_short_qty": 73.0,
            "current_short_notional": 4.891,
            "actual_net_qty": -73.0,
            "actual_net_notional": -4.891,
            "synthetic_net_qty": -73.0,
            "synthetic_drift_qty": 0.0,
            "buy_paused": False,
            "pause_reasons": [],
            "buy_cap_applied": False,
            "buy_budget_notional": 100.0,
            "planned_buy_notional": 20.0,
            "max_position_notional": 500.0,
            "short_paused": False,
            "short_pause_reasons": [],
            "short_cap_applied": False,
            "short_budget_notional": 120.0,
            "planned_short_notional": 25.0,
            "max_short_position_notional": 500.0,
            "inventory_tier": {"enabled": False},
            "synthetic_ledger": {"virtual_short_qty": 73.0, "unmatched_trade_count": 0},
            "effective_strategy_profile": "chipusdt_competition_neutral_ping_pong_v1",
            "symbol_info": {
                "tick_size": 0.00001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertIsInstance(report["submit_generated_at"], str)
        self.assertEqual(report["plan_summary"]["symbol"], "CHIPUSDT")
        self.assertEqual(report["plan_summary"]["strategy_mode"], "synthetic_neutral")
        self.assertEqual(report["synthetic_ledger"], {"virtual_short_qty": 73.0, "unmatched_trade_count": 0})
        self.assertEqual(report["plan_snapshot"]["synthetic_ledger"], {"virtual_short_qty": 73.0, "unmatched_trade_count": 0})

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index", return_value=[{"markPrice": "0.144", "lastFundingRate": "0.0"}])
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers", return_value=[{"bid_price": "0.1439", "ask_price": "0.1441"}])
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_uses_rest_position_and_fresh_stream_open_orders(
        self,
        mock_validate_plan_report,
        _mock_book_tickers,
        _mock_premium_index,
        _mock_credentials,
        _mock_position_mode,
        mock_account_info,
        mock_open_orders,
        _mock_change_leverage,
        mock_post_order,
        _mock_update_inventory_refs,
        _mock_update_synthetic_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 35.0, "price": 0.1439},
                ],
            },
        }
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "-100", "entryPrice": "0.144"}],
        }
        mock_open_orders.return_value = []
        mock_post_order.return_value = {"orderId": 123, "clientOrderId": "gx-billu-test"}
        now = time.monotonic()
        stream = SimpleNamespace(
            snapshot_events=lambda: [],
            snapshot_account_positions=lambda: [
                {
                    "symbol": "BILLUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "-100",
                    "entryPrice": "0.144",
                    "breakEvenPrice": "0.144",
                    "observed_at": now,
                }
            ],
            snapshot_open_orders=lambda: [],
            open_order_state_age_seconds=lambda: 1.0,
            status=lambda: {"last_account_update_age_seconds": 1.0},
        )
        args = Namespace(
            symbol="BILLUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/billusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=20,
            maker_retries=0,
            recv_window=5000,
            state_path="output/billusdt_loop_state.json",
            user_data_stream=stream,
        )
        plan_report = {
            "symbol": "BILLUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.144,
            "step_price": 0.00028,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 100.0,
            "actual_net_qty": -100.0,
            "symbol_info": {
                "tick_size": 0.00001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertTrue(report["executed"])
        self.assertEqual(report["account_snapshot_sources"]["account_info"], "rest")
        self.assertEqual(report["account_snapshot_sources"]["open_orders"], "user_data_stream")
        mock_account_info.assert_called_once_with("key", "secret", recv_window=5000, use_cache=False)
        mock_open_orders.assert_called_once_with("BILLUSDT", "key", "secret", recv_window=5000, use_cache=False)

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index", return_value=[{"markPrice": "0.144", "lastFundingRate": "0.0"}])
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers", return_value=[{"bid_price": "0.1439", "ask_price": "0.1441"}])
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_uses_rest_open_orders_when_account_stream_is_stale(
        self,
        mock_validate_plan_report,
        _mock_book_tickers,
        _mock_premium_index,
        _mock_credentials,
        _mock_position_mode,
        mock_account_info,
        mock_open_orders,
        _mock_change_leverage,
        mock_post_order,
        _mock_update_inventory_refs,
        _mock_update_synthetic_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 35.0, "price": 0.1439},
                ],
            },
        }
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "-100", "entryPrice": "0.144"}],
        }
        mock_open_orders.return_value = []
        mock_post_order.return_value = {"orderId": 123, "clientOrderId": "gx-billu-test"}
        stale_observed_at = time.monotonic() - 90.0
        stream = SimpleNamespace(
            snapshot_events=lambda: [],
            snapshot_account_positions=lambda: [
                {
                    "symbol": "BILLUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "-100",
                    "entryPrice": "0.144",
                    "breakEvenPrice": "0.144",
                    "observed_at": stale_observed_at,
                }
            ],
            snapshot_open_orders=lambda: [{"symbol": "BILLUSDT", "clientOrderId": "gx-billu-stale"}],
            open_order_state_age_seconds=lambda: 1.0,
            status=lambda: {"last_account_update_age_seconds": 90.0},
            replace_open_orders_from_rest=lambda orders: None,
        )
        args = Namespace(
            symbol="BILLUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/billusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=20,
            maker_retries=0,
            recv_window=5000,
            state_path="output/billusdt_loop_state.json",
            user_data_stream=stream,
        )
        plan_report = {
            "symbol": "BILLUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.144,
            "step_price": 0.00028,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 100.0,
            "actual_net_qty": -100.0,
            "symbol_info": {
                "tick_size": 0.00001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        report = execute_plan_report(args, plan_report)

        self.assertTrue(report["executed"])
        self.assertEqual(report["account_snapshot_sources"]["account_info"], "rest")
        self.assertEqual(report["account_snapshot_sources"]["open_orders"], "rest")
        self.assertEqual(report["account_snapshot_sources"]["account_position_stream_age_seconds"], "90.000000")
        mock_account_info.assert_called_once_with("key", "secret", recv_window=5000, use_cache=False)
        mock_open_orders.assert_any_call("BILLUSDT", "key", "secret", recv_window=5000)

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_entry_orders", return_value=3)
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index", return_value=[{"markPrice": "0.144", "lastFundingRate": "0.0"}])
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers", return_value=[{"bid_price": "0.1439", "ask_price": "0.1441"}])
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_protective_stops_when_synthetic_drift_exceeds_rest_position(
        self,
        mock_validate_plan_report,
        _mock_book_tickers,
        _mock_premium_index,
        _mock_credentials,
        _mock_position_mode,
        mock_account_info,
        _mock_open_orders,
        _mock_change_leverage,
        mock_cancel_entries,
        _mock_update_inventory_refs,
        _mock_update_synthetic_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [{"role": "entry_long", "side": "BUY", "qty": 35.0, "price": 0.1439}],
            },
        }
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "6000", "entryPrice": "0.144"}],
        }
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps({"synthetic_ledger": {}, "synthetic_order_refs": {}}), encoding="utf-8")
            args = Namespace(
                symbol="BILLUSDT",
                strategy_mode="synthetic_neutral",
                max_new_orders=20,
                max_total_notional=1000.0,
                cancel_stale=False,
                max_plan_age_seconds=30,
                max_mid_drift_steps=4.0,
                plan_json="output/billusdt_loop_latest_plan.json",
                apply=True,
                margin_type="KEEP",
                leverage=20,
                maker_retries=0,
                recv_window=5000,
                state_path=str(state_path),
                max_synthetic_drift_notional=100.0,
                per_order_notional=80.0,
            )
            plan_report = {
                "symbol": "BILLUSDT",
                "strategy_mode": "synthetic_neutral",
                "mid_price": 0.144,
                "step_price": 0.00028,
                "open_order_count": 0,
                "current_long_qty": 0.0,
                "current_short_qty": 0.0,
                "actual_net_qty": 0.0,
                "state_path": str(state_path),
                "symbol_info": {"tick_size": 0.00001, "min_qty": 1.0, "min_notional": 5.0},
            }

            with self.assertRaisesRegex(Exception, "protective entry stop"):
                execute_plan_report(args, plan_report)

            mock_cancel_entries.assert_called_once()
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["protective_entry_stop"]["reason"], "submit_synthetic_rest_position_drift")

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index", return_value=[{"markPrice": "0.144", "lastFundingRate": "0.0"}])
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers", return_value=[{"bid_price": "0.1439", "ask_price": "0.1441"}])
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_skips_repeated_leverage_change_within_cache_ttl(
        self,
        mock_validate_plan_report,
        _mock_book_tickers,
        _mock_premium_index,
        _mock_credentials,
        _mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        _mock_update_inventory_refs,
        _mock_update_synthetic_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 35.0, "price": 0.1439},
                ],
            },
        }
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "BILLUSDT", "positionAmt": "-100", "entryPrice": "0.144"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 20}
        mock_post_order.return_value = {"orderId": 123, "clientOrderId": "gx-billu-test"}
        args = Namespace(
            symbol="BILLUSDT",
            strategy_mode="synthetic_neutral",
            max_new_orders=20,
            max_total_notional=1000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=4.0,
            plan_json="output/billusdt_loop_latest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=20,
            maker_retries=0,
            recv_window=5000,
            state_path="output/billusdt_loop_state.json",
            leverage_change_cache_ttl_seconds=600.0,
        )
        plan_report = {
            "symbol": "BILLUSDT",
            "strategy_mode": "synthetic_neutral",
            "mid_price": 0.144,
            "step_price": 0.00028,
            "open_order_count": 0,
            "current_long_qty": 0.0,
            "current_short_qty": 100.0,
            "actual_net_qty": -100.0,
            "symbol_info": {
                "tick_size": 0.00001,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        }

        first_report = execute_plan_report(args, plan_report)
        second_report = execute_plan_report(args, plan_report)

        self.assertTrue(first_report["executed"])
        self.assertTrue(second_report["executed"])
        self.assertEqual(mock_change_leverage.call_count, 1)
        self.assertEqual(second_report["leverage_response"]["source"], "cache")

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.resolve_futures_market_snapshot")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_uses_market_snapshot_resolver_for_initial_quote_and_retry(
        self,
        mock_validate_plan_report,
        mock_market_snapshot,
        mock_book_tickers,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        _mock_update_inventory_refs,
        _mock_update_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 11.0, "price": 0.50},
                ],
            },
        }
        mock_market_snapshot.side_effect = [
            {
                "symbol": "KATUSDT",
                "bid_price": 0.49,
                "ask_price": 0.51,
                "mark_price": 0.50,
                "funding_rate": 0.0,
                "next_funding_time": None,
                "source": "websocket",
            },
            {
                "symbol": "KATUSDT",
                "bid_price": 0.48,
                "ask_price": 0.50,
                "mark_price": 0.49,
                "funding_rate": 0.0,
                "next_funding_time": None,
                "source": "websocket",
            },
        ]
        mock_book_tickers.return_value = [{"bid_price": "0.49", "ask_price": "0.51"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "KATUSDT", "positionAmt": "0", "entryPrice": "0"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = [
            RuntimeError("Binance API error -5022: Post only order will be rejected."),
            {"orderId": 124, "clientOrderId": "cid-124"},
        ]

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
            maker_retries=1,
            recv_window=5000,
            state_path="output/katusdt_loop_state.json",
            market_stream=object(),
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
        self.assertEqual(len(report["placed_orders"]), 1)
        self.assertEqual(report["live_book"]["source"], "websocket")
        self.assertEqual(mock_market_snapshot.call_count, 2)
        mock_book_tickers.assert_not_called()

    @patch("grid_optimizer.loop_runner.update_synthetic_order_refs")
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_refreshes_book_only_when_retrying_post_only_reject(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
        mock_premium_index,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        _mock_update_inventory_refs,
        _mock_update_refs,
    ) -> None:
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 1,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": [
                    {"role": "entry", "side": "BUY", "qty": 11.0, "price": 0.50},
                ],
            },
        }
        mock_book_tickers.side_effect = [
            [{"bid_price": "0.49", "ask_price": "0.51"}],
            [{"bid_price": "0.48", "ask_price": "0.50"}],
            [{"bid_price": "0.48", "ask_price": "0.50"}],
        ]
        mock_premium_index.return_value = [{"markPrice": "0.50", "lastFundingRate": "0.0"}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {
            "multiAssetsMargin": False,
            "positions": [{"symbol": "KATUSDT", "positionAmt": "0", "entryPrice": "0"}],
        }
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = [
            RuntimeError("Binance API error -5022: Post only order will be rejected."),
            {"orderId": 124, "clientOrderId": "cid-124"},
        ]

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
            maker_retries=1,
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
        self.assertEqual(len(report["placed_orders"]), 1)
        self.assertEqual(mock_book_tickers.call_count, 2)

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

    def test_resolve_synthetic_trend_follow_uses_single_side_entry_when_flat(self) -> None:
        state = {
            "synthetic_ledger": {
                "last_trade_time_ms": int(datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc).timestamp() * 1000),
            }
        }
        report = resolve_synthetic_trend_follow(
            state=state,
            market_guard={
                "window_1m": {"return_ratio": 0.00062, "amplitude_ratio": 0.00086},
                "window_3m": {"return_ratio": 0.00118, "amplitude_ratio": 0.00152},
            },
            now=datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
            current_long_qty=0.0,
            current_short_qty=0.0,
            enabled=True,
            window_1m_abs_return_ratio=0.00045,
            window_1m_amplitude_ratio=0.00070,
            window_3m_abs_return_ratio=0.00090,
            window_3m_amplitude_ratio=0.00120,
            min_efficiency_ratio=0.58,
            reverse_delay_seconds=18.0,
        )

        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"role": "entry_long", "side": "BUY"}],
            "sell_orders": [{"role": "entry_short", "side": "SELL"}],
        }
        applied = apply_synthetic_trend_follow_guard(plan=plan, trend_follow=report)

        self.assertTrue(report["active"])
        self.assertEqual(report["mode"], "flat_follow_short")
        self.assertEqual(report["flow_entry_side"], "SELL")
        self.assertTrue(applied["applied"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "entry_short")

    def test_resolve_synthetic_trend_follow_delays_reverse_order_for_fresh_short_inventory(self) -> None:
        now = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
        state = {
            "synthetic_ledger": {
                "last_trade_time_ms": int(now.timestamp() * 1000),
            },
            "synthetic_trend_follow_state": {
                "inventory_direction": None,
                "inventory_qty": 0.0,
                "cooldown_until_ms": 0,
                "last_trade_time_ms": 0,
            },
        }
        report = resolve_synthetic_trend_follow(
            state=state,
            market_guard={
                "window_1m": {"return_ratio": 0.00058, "amplitude_ratio": 0.00082},
                "window_3m": {"return_ratio": 0.00102, "amplitude_ratio": 0.00135},
            },
            now=now,
            current_long_qty=0.0,
            current_short_qty=0.021,
            enabled=True,
            window_1m_abs_return_ratio=0.00045,
            window_1m_amplitude_ratio=0.00070,
            window_3m_abs_return_ratio=0.00090,
            window_3m_amplitude_ratio=0.00120,
            min_efficiency_ratio=0.58,
            reverse_delay_seconds=18.0,
        )

        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"role": "take_profit_short", "side": "BUY"}],
            "sell_orders": [{"role": "entry_short", "side": "SELL"}],
        }
        apply_synthetic_trend_follow_guard(plan=plan, trend_follow=report)

        self.assertTrue(report["active"])
        self.assertEqual(report["mode"], "hold_short_wait")
        self.assertTrue(report["reverse_delay_active"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [])

    def test_resolve_synthetic_trend_follow_releases_exit_only_after_delay(self) -> None:
        now = datetime(2026, 3, 18, 10, 0, 25, tzinfo=timezone.utc)
        state = {
            "synthetic_ledger": {
                "last_trade_time_ms": int((now - timedelta(seconds=25)).timestamp() * 1000),
            },
            "synthetic_trend_follow_state": {
                "inventory_direction": "short",
                "inventory_qty": 0.021,
                "cooldown_until_ms": int((now - timedelta(seconds=5)).timestamp() * 1000),
                "last_trade_time_ms": int((now - timedelta(seconds=25)).timestamp() * 1000),
            },
        }
        report = resolve_synthetic_trend_follow(
            state=state,
            market_guard={
                "window_1m": {"return_ratio": 0.00058, "amplitude_ratio": 0.00082},
                "window_3m": {"return_ratio": 0.00102, "amplitude_ratio": 0.00135},
            },
            now=now,
            current_long_qty=0.0,
            current_short_qty=0.021,
            enabled=True,
            window_1m_abs_return_ratio=0.00045,
            window_1m_amplitude_ratio=0.00070,
            window_3m_abs_return_ratio=0.00090,
            window_3m_amplitude_ratio=0.00120,
            min_efficiency_ratio=0.58,
            reverse_delay_seconds=18.0,
        )

        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {"role": "take_profit_short", "side": "BUY"},
                {"role": "entry_long", "side": "BUY"},
            ],
            "sell_orders": [{"role": "entry_short", "side": "SELL"}],
        }
        apply_synthetic_trend_follow_guard(plan=plan, trend_follow=report)

        self.assertEqual(report["mode"], "hold_short_exit")
        self.assertFalse(report["reverse_delay_active"])
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(plan["buy_orders"][0]["role"], "take_profit_short")
        self.assertEqual(plan["sell_orders"], [])

    def test_build_parser_accepts_runtime_guard_notional_thresholds(self) -> None:
        args = _build_parser().parse_args(
            [
                "--max-actual-net-notional",
                "120",
                "--max-synthetic-drift-notional",
                "60",
                "--max-unrealized-loss",
                "25",
                "--unrealized-loss-entry-guard-enabled",
                "--unrealized-loss-entry-guard-min-loss",
                "3",
                "--unrealized-loss-entry-guard-ratio",
                "0.015",
            ]
        )

        self.assertEqual(args.max_actual_net_notional, 120.0)
        self.assertEqual(args.max_synthetic_drift_notional, 60.0)
        self.assertEqual(args.max_unrealized_loss, 25.0)
        self.assertTrue(args.unrealized_loss_entry_guard_enabled)
        self.assertEqual(args.unrealized_loss_entry_guard_min_loss, 3.0)
        self.assertEqual(args.unrealized_loss_entry_guard_ratio, 0.015)

    def test_build_parser_accepts_static_quote_offset_thresholds(self) -> None:
        args = _build_parser().parse_args(
            [
                "--static-buy-offset-steps",
                "0.5",
                "--static-sell-offset-steps",
                "1.0",
            ]
        )

        self.assertEqual(args.static_buy_offset_steps, 0.5)
        self.assertEqual(args.static_sell_offset_steps, 1.0)

    def test_build_parser_accepts_sticky_entry_levels(self) -> None:
        args = _build_parser().parse_args(["--sticky-entry-levels", "2"])

        self.assertEqual(args.sticky_entry_levels, 2)

    def test_build_parser_accepts_sticky_entry_churn_controls(self) -> None:
        args = _build_parser().parse_args(
            [
                "--sticky-entry-price-tolerance-steps",
                "2.5",
                "--sticky-entry-preserve-less-aggressive",
            ]
        )

        self.assertEqual(args.sticky_entry_price_tolerance_steps, 2.5)
        self.assertTrue(args.sticky_entry_preserve_less_aggressive)

    def test_build_parser_defaults_to_sticky_entry_churn_controls(self) -> None:
        args = _build_parser().parse_args([])

        self.assertEqual(args.sticky_entry_levels, 4)
        self.assertEqual(args.sticky_entry_price_tolerance_steps, 2.0)
        self.assertTrue(args.sticky_entry_preserve_less_aggressive)

    def test_build_parser_accepts_synthetic_residual_short_flat_notional(self) -> None:
        args = _build_parser().parse_args(["--synthetic-residual-short-flat-notional", "30"])

        self.assertEqual(args.synthetic_residual_short_flat_notional, 30.0)

    def test_build_parser_accepts_synthetic_tiny_residual_notional_thresholds(self) -> None:
        args = _build_parser().parse_args(
            [
                "--synthetic-tiny-long-residual-notional",
                "45",
                "--synthetic-tiny-short-residual-notional",
                "55",
            ]
        )

        self.assertEqual(args.synthetic_tiny_long_residual_notional, 45.0)
        self.assertEqual(args.synthetic_tiny_short_residual_notional, 55.0)

    def test_build_parser_accepts_volatility_entry_pause_tiny_inventory_ignore_notional(self) -> None:
        args = _build_parser().parse_args(["--volatility-entry-pause-tiny-inventory-ignore-notional", "10"])

        self.assertEqual(args.volatility_entry_pause_tiny_inventory_ignore_notional, 10.0)

    def test_build_parser_accepts_take_profit_min_profit_ratio(self) -> None:
        args = _build_parser().parse_args(["--take-profit-min-profit-ratio", "0.0005"])

        self.assertEqual(args.take_profit_min_profit_ratio, 0.0005)

    def test_build_parser_accepts_loss_reentry_trend_return_ratio(self) -> None:
        args = _build_parser().parse_args(["--loss-reentry-trend-return-ratio", "0.0015"])

        self.assertEqual(args.loss_reentry_trend_return_ratio, 0.0015)

    def test_build_parser_defaults_loss_reentry_trend_return_ratio_to_disabled(self) -> None:
        args = _build_parser().parse_args([])

        self.assertEqual(args.loss_reentry_trend_return_ratio, 0.0)

    def test_build_parser_accepts_adaptive_step_long_window_amplitude_thresholds(self) -> None:
        args = _build_parser().parse_args(
            [
                "--adaptive-step-3m-amplitude-ratio",
                "0.006",
                "--adaptive-step-5m-amplitude-ratio",
                "0.01",
            ]
        )

        self.assertEqual(args.adaptive_step_3m_amplitude_ratio, 0.006)
        self.assertEqual(args.adaptive_step_5m_amplitude_ratio, 0.01)

    def test_build_parser_accepts_multi_timeframe_bias_controls(self) -> None:
        args = _build_parser().parse_args(
            [
                "--multi-timeframe-bias-enabled",
                "--multi-timeframe-bias-mode-adapter",
                "one_way_long",
                "--multi-timeframe-bias-max-level-delta",
                "5",
                "--multi-timeframe-bias-max-offset-steps",
                "1.25",
                "--multi-timeframe-bias-shock-abs-return-ratio",
                "0.012",
                "--multi-timeframe-bias-shock-amplitude-ratio",
                "0.02",
                "--multi-timeframe-bias-shock-notional-scale",
                "0.6",
            ]
        )

        self.assertTrue(args.multi_timeframe_bias_enabled)
        self.assertEqual(args.multi_timeframe_bias_mode_adapter, "one_way_long")
        self.assertEqual(args.multi_timeframe_bias_max_level_delta, 5)
        self.assertEqual(args.multi_timeframe_bias_max_offset_steps, 1.25)
        self.assertEqual(args.multi_timeframe_bias_shock_abs_return_ratio, 0.012)
        self.assertEqual(args.multi_timeframe_bias_shock_amplitude_ratio, 0.02)
        self.assertEqual(args.multi_timeframe_bias_shock_notional_scale, 0.6)

    def test_multi_timeframe_bias_validation_allows_one_way_modes_with_auto_adapter(self) -> None:
        for strategy_mode in ("synthetic_neutral", "one_way_long", "one_way_short", "competition_inventory_grid"):
            with self.subTest(strategy_mode=strategy_mode):
                args = _build_parser().parse_args(
                    [
                        "--strategy-mode",
                        strategy_mode,
                        "--multi-timeframe-bias-enabled",
                    ]
                )

                _validate_multi_timeframe_bias_args(args)

    def test_multi_timeframe_bias_validation_rejects_incompatible_explicit_adapter(self) -> None:
        args = _build_parser().parse_args(
            [
                "--strategy-mode",
                "one_way_long",
                "--multi-timeframe-bias-enabled",
                "--multi-timeframe-bias-mode-adapter",
                "one_way_short",
            ]
        )

        with self.assertRaises(SystemExit):
            _validate_multi_timeframe_bias_args(args)

    def test_resolve_adaptive_step_uses_5m_amplitude_for_choppy_expansion(self) -> None:
        now = datetime(2026, 4, 30, 6, 0, tzinfo=timezone.utc)
        state = {
            "adaptive_step_history": [
                {"ts": (now - timedelta(seconds=300)).isoformat(), "mid_price": 4.3000},
                {"ts": (now - timedelta(seconds=240)).isoformat(), "mid_price": 4.2800},
                {"ts": (now - timedelta(seconds=180)).isoformat(), "mid_price": 4.3184},
                {"ts": (now - timedelta(seconds=120)).isoformat(), "mid_price": 4.3000},
                {"ts": (now - timedelta(seconds=60)).isoformat(), "mid_price": 4.2950},
            ]
        }

        report = resolve_adaptive_step_price(
            state=state,
            now=now,
            mid_price=4.3000,
            base_step_price=0.001,
            tick_size=0.001,
            enabled=True,
            window_30s_abs_return_ratio=0.0,
            window_30s_amplitude_ratio=0.0,
            window_1m_abs_return_ratio=0.0,
            window_1m_amplitude_ratio=0.0,
            window_3m_abs_return_ratio=0.0,
            window_3m_amplitude_ratio=0.0,
            window_5m_abs_return_ratio=0.0,
            window_5m_amplitude_ratio=0.005,
            max_scale=2.5,
        )

        self.assertTrue(report["controls_active"])
        self.assertEqual(report["dominant_window"], "window_5m")
        self.assertEqual(report["dominant_metric"], "amplitude_ratio")
        self.assertAlmostEqual(report["effective_step_price"], 0.002)
        self.assertAlmostEqual(report["scale"], 2.0)

    def test_assess_synthetic_tp_only_watchdog_activates_after_persistent_tp_only_gap(self) -> None:
        state: dict[str, object] = {}
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [{"role": "take_profit_short", "side": "BUY", "price": 90.0}],
            "sell_orders": [{"role": "take_profit_long", "side": "SELL", "price": 110.0}],
        }
        started_at = datetime(2026, 4, 10, 0, 6, 33, tzinfo=timezone.utc)

        first = assess_synthetic_tp_only_watchdog(
            state=state,
            strategy_mode="synthetic_neutral",
            plan=plan,
            current_long_qty=0.008,
            current_short_qty=0.219,
            mid_price=100.0,
            step_price=1.0,
            now=started_at,
        )
        second = assess_synthetic_tp_only_watchdog(
            state=state,
            strategy_mode="synthetic_neutral",
            plan=plan,
            current_long_qty=0.008,
            current_short_qty=0.219,
            mid_price=100.0,
            step_price=1.0,
            now=started_at + timedelta(seconds=181),
        )

        self.assertTrue(first["tp_only_mode"])
        self.assertTrue(first["candidate"])
        self.assertFalse(first["active"])
        self.assertAlmostEqual(first["buy_gap_steps"], 10.0)
        self.assertAlmostEqual(first["sell_gap_steps"], 10.0)
        self.assertTrue(second["candidate"])
        self.assertTrue(second["active"])
        self.assertAlmostEqual(second["duration_seconds"], 181.0)
        self.assertIn("buy_gap=10.0 step", second["reason"])

    def test_build_hedge_micro_grid_plan_respects_near_price_offsets(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=100.0,
            step_price=0.02,
            buy_levels=1,
            sell_levels=1,
            per_order_notional=10.0,
            base_position_notional=0.0,
            bid_price=100.0,
            ask_price=100.01,
            tick_size=0.01,
            step_size=0.001,
            min_qty=None,
            min_notional=None,
            current_long_qty=0.0,
            current_short_qty=0.0,
            buy_offset_steps=0.5,
            sell_offset_steps=0.5,
        )

        self.assertEqual(plan["buy_orders"][0]["price"], 99.99)
        self.assertEqual(plan["sell_orders"][0]["price"], 100.02)

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
        self.assertAlmostEqual(result["effective_pause_buy_position_notional"], 2000.0, places=6)
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
        self.assertAlmostEqual(result["effective_pause_buy_position_notional"], 2000.0, places=8)

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

    def test_elastic_volume_config_reads_four_state_fields(self) -> None:
        args = Namespace(
            elastic_volume_enabled=True,
            elastic_loss_per_10k_sprint=2.0,
            elastic_loss_per_10k_cruise=3.5,
            elastic_loss_per_10k_defensive=5.0,
            elastic_loss_per_10k_cooldown=6.5,
            elastic_inventory_soft_ratio=0.6,
            elastic_inventory_hard_ratio=0.9,
            elastic_inventory_soft_ratio_ping_pong_fast=0.30,
            elastic_inventory_hard_ratio_ping_pong_fast=0.45,
            elastic_inventory_soft_ratio_ping_pong_safe=0.45,
            elastic_inventory_hard_ratio_ping_pong_safe=0.60,
            elastic_inventory_soft_ratio_wide_step_attack=0.45,
            elastic_inventory_hard_ratio_wide_step_attack=0.65,
            elastic_inventory_soft_ratio_wide_step=0.60,
            elastic_inventory_hard_ratio_wide_step=0.80,
            elastic_step_scale_sprint=0.8,
            elastic_step_scale_defensive=1.8,
            elastic_step_scale_cooldown=3.0,
            elastic_base_step_multiplier_ping_pong_fast=0.8,
            elastic_base_step_multiplier_ping_pong_safe=1.2,
            elastic_base_step_multiplier_wide_step_attack=2.2,
            elastic_base_step_multiplier_wide_step=2.5,
            elastic_base_step_multiplier_defensive=3.5,
            elastic_per_order_scale_sprint=1.25,
            elastic_per_order_scale_defensive=0.65,
            elastic_per_order_scale_ping_pong_fast=1.0,
            elastic_per_order_scale_ping_pong_safe=0.9,
            elastic_per_order_scale_wide_step_attack=1.2,
            elastic_per_order_scale_wide_step=0.6,
            elastic_per_order_scale_defensive_state=0.4,
            elastic_levels_scale_sprint=1.25,
            elastic_levels_scale_defensive=0.65,
            elastic_levels_scale_ping_pong_fast=1.0,
            elastic_levels_scale_ping_pong_safe=0.85,
            elastic_levels_scale_wide_step_attack=1.1,
            elastic_levels_scale_wide_step=0.65,
            elastic_levels_scale_defensive_state=0.35,
            elastic_threshold_scale_ping_pong_fast=1.0,
            elastic_threshold_scale_ping_pong_safe=1.1,
            elastic_threshold_scale_wide_step_attack=1.5,
            elastic_threshold_scale_wide_step=1.5,
            elastic_threshold_scale_defensive=1.8,
            elastic_pause_scale_ping_pong_fast=1.0,
            elastic_pause_scale_ping_pong_safe=1.1,
            elastic_pause_scale_wide_step_attack=1.2,
            elastic_pause_scale_wide_step=1.5,
            elastic_pause_scale_defensive=1.8,
            elastic_max_total_scale_ping_pong_fast=1.0,
            elastic_max_total_scale_ping_pong_safe=1.1,
            elastic_max_total_scale_wide_step_attack=1.2,
            elastic_max_total_scale_wide_step=1.35,
            elastic_max_total_scale_defensive=1.6,
            elastic_max_entry_orders_ping_pong_fast=6,
            elastic_max_entry_orders_ping_pong_safe=4,
            elastic_max_entry_orders_wide_step_attack=4,
            elastic_max_entry_orders_wide_step=3,
            elastic_max_entry_orders_defensive=2,
            elastic_early_micro_abs_return_ratio=0.00025,
            elastic_early_micro_amplitude_ratio=0.00035,
            elastic_early_safe_inventory_ratio=0.45,
            elastic_early_wide_inventory_ratio=0.65,
            elastic_early_wide_loss_per_10k_5m=0.5,
            elastic_cooldown_seconds=120.0,
            elastic_state_confirm_cycles=3,
            elastic_cancel_stale_entries_on_cooldown=True,
        )

        config = _elastic_volume_config(args)

        self.assertEqual(config.base_step_multiplier_ping_pong_fast, 0.8)
        self.assertEqual(config.base_step_multiplier_ping_pong_safe, 1.2)
        self.assertEqual(config.base_step_multiplier_wide_step_attack, 2.2)
        self.assertEqual(config.base_step_multiplier_wide_step, 2.5)
        self.assertEqual(config.base_step_multiplier_defensive, 3.5)
        self.assertEqual(config.inventory_hard_ratio_wide_step, 0.80)
        self.assertEqual(config.threshold_scale_wide_step, 1.5)
        self.assertEqual(config.pause_scale_defensive, 1.8)
        self.assertEqual(config.max_total_scale_defensive, 1.6)
        self.assertEqual(config.max_entry_orders_wide_step_attack, 4)
        self.assertEqual(config.early_micro_abs_return_ratio, 0.00025)
        self.assertEqual(config.early_micro_amplitude_ratio, 0.00035)
        self.assertEqual(config.early_safe_inventory_ratio, 0.45)
        self.assertEqual(config.early_wide_inventory_ratio, 0.65)
        self.assertEqual(config.early_wide_loss_per_10k_5m, 0.5)

    def test_elastic_volume_state_snapshot_preserves_recovery_confirmation(self) -> None:
        snapshot = _elastic_volume_state_snapshot(
            {
                "regime": "wide-step",
                "cooldown_until": None,
                "pending_regime": "ping-pong-safe",
                "pending_count": 2,
            },
            updated_at="2026-05-11T14:30:00+00:00",
        )

        self.assertEqual(snapshot["regime"], "wide-step")
        self.assertIsNone(snapshot["cooldown_until"])
        self.assertEqual(snapshot["pending_regime"], "ping-pong-safe")
        self.assertEqual(snapshot["pending_count"], 2)
        self.assertEqual(snapshot["updated_at"], "2026-05-11T14:30:00+00:00")


if __name__ == "__main__":
    unittest.main()
