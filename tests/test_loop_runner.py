from __future__ import annotations

from argparse import Namespace
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.audit import build_audit_paths
from grid_optimizer.inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime
from grid_optimizer.loop_runner import (
    AUTO_REGIME_PROFILE_LABELS,
    AUDIT_SYNC_MIN_INTERVAL_SECONDS,
    _build_parser,
    _should_sync_account_audit,
    _uses_entry_price_cost_basis,
    _uses_volume_long_v4_staged_delever,
    _apply_synthetic_trade_fill,
    _decorate_synthetic_open_orders,
    _load_synthetic_ledger,
    _current_check_bucket,
    _custom_grid_levels_above_current,
    _generate_competition_inventory_grid_plan,
    _filter_futures_strategy_orders,
    _read_custom_grid_trade_count,
    _resolve_custom_grid_roll,
    _resolve_synthetic_resync_price,
    _shift_custom_grid_bounds,
    _run_periodic_reconcile,
    StartupProtectionError,
    _update_inventory_grid_order_refs,
    apply_excess_inventory_reduce_only,
    apply_active_delever_short,
    apply_active_delever_long,
    apply_volume_long_v4_staged_delever,
    apply_volume_long_v4_flow_sleeve,
    apply_hard_loss_forced_reduce,
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
    resolve_interval_locked_center_price,
    resolve_inventory_pause_timeout_state,
    resolve_short_threshold_timeout_state,
    resolve_synthetic_trend_follow,
    resolve_auto_regime_profile,
    resolve_xaut_adaptive_state,
    apply_synthetic_trend_follow_guard,
    sync_synthetic_ledger,
    update_synthetic_order_refs,
)
from grid_optimizer.semi_auto_plan import build_hedge_micro_grid_plan, build_static_binance_grid_plan
from grid_optimizer.types import Candle


class LoopRunnerTests(unittest.TestCase):
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

    def test_soonusdt_volume_profiles_use_entry_price_cost_basis(self) -> None:
        self.assertTrue(_uses_entry_price_cost_basis("chip_low_wear_guarded_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("chipusdt_competition_neutral_ping_pong_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("soon_high_vol_short_grid_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("soon_volume_neutral_ping_pong_v1"))
        self.assertTrue(_uses_entry_price_cost_basis("soonusdt_competition_neutral_ping_pong_v1"))

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
    def test_generate_plan_report_synthetic_neutral_releases_long_when_pause_blocks_break_even_exit(
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
        self.assertTrue(report["active_delever"]["active"])
        self.assertEqual(report["active_delever"]["trigger_mode"], "pause")
        self.assertGreater(report["active_delever"]["active_sell_order_count"], 0)
        self.assertGreater(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_sells[0]["execution_type"], "passive_release")
        self.assertTrue(active_sells[0]["force_reduce_only"])
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
    def test_generate_plan_report_synthetic_neutral_releases_short_when_pause_blocks_break_even_exit(
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
        self.assertTrue(report["active_delever"]["active"])
        self.assertEqual(report["active_delever"]["trigger_mode"], "pause")
        self.assertGreater(report["active_delever"]["active_buy_order_count"], 0)
        self.assertGreater(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_buys[0]["execution_type"], "passive_release")
        self.assertTrue(active_buys[0]["force_reduce_only"])
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
    def test_generate_plan_report_synthetic_neutral_threshold_releases_long_below_break_even(
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
        self.assertEqual(report["active_delever"]["trigger_mode"], "threshold")
        self.assertGreater(report["active_delever"]["active_sell_order_count"], 0)
        self.assertGreater(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_sells[0]["execution_type"], "passive_release")
        self.assertTrue(active_sells[0]["force_reduce_only"])
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
    def test_generate_plan_report_synthetic_neutral_threshold_releases_short_below_break_even(
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
        self.assertEqual(report["active_delever"]["trigger_mode"], "threshold")
        self.assertGreater(report["active_delever"]["active_buy_order_count"], 0)
        self.assertGreater(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertEqual(active_buys[0]["execution_type"], "passive_release")
        self.assertTrue(active_buys[0]["force_reduce_only"])
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

    def test_apply_active_delever_short_pause_notional_creates_passive_release_orders(self) -> None:
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
        self.assertEqual(result["release_buy_order_count"], 2)
        self.assertEqual([item["price"] for item in active_buys], [0.999, 0.989])
        self.assertTrue(all(item["execution_type"] == "passive_release" for item in active_buys))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_buys))

    def test_apply_active_delever_short_synthesizes_release_orders_without_take_profit_orders(self) -> None:
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
        self.assertTrue(result["active"])
        self.assertEqual(result["trigger_mode"], "pause")
        self.assertEqual(result["synthesized_release_source_order_count"], 2)
        self.assertEqual(result["release_buy_order_count"], 2)
        self.assertEqual(result["pruned_flow_sleeve_order_count"], 1)
        self.assertEqual([item["price"] for item in active_buys], [0.999, 0.989])
        self.assertFalse(any(item["role"] == "flow_sleeve_short" for item in plan["buy_orders"]))
        self.assertTrue(all(item["execution_type"] == "passive_release" for item in active_buys))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_buys))

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

    def test_apply_active_delever_long_pause_notional_creates_passive_release_orders(self) -> None:
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
        self.assertEqual(report["release_sell_order_count"], 2)
        self.assertEqual([item["price"] for item in active_sells], [1.001, 1.011])
        self.assertTrue(all(item["execution_type"] == "passive_release" for item in active_sells))
        self.assertTrue(all(item["time_in_force"] == "GTX" for item in active_sells))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_sells))

    def test_apply_active_delever_long_synthesizes_release_orders_without_take_profit_orders(self) -> None:
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
        self.assertTrue(report["active"])
        self.assertEqual(report["trigger_mode"], "pause")
        self.assertEqual(report["synthesized_release_source_order_count"], 2)
        self.assertEqual(report["release_sell_order_count"], 2)
        self.assertEqual(report["pruned_flow_sleeve_order_count"], 1)
        self.assertEqual([item["price"] for item in active_sells], [1.001, 1.011])
        self.assertFalse(any(item["role"] == "flow_sleeve_long" for item in plan["sell_orders"]))
        self.assertTrue(all(item["execution_type"] == "passive_release" for item in active_sells))
        self.assertTrue(all(bool(item["force_reduce_only"]) for item in active_sells))

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
    def test_generate_plan_report_synthetic_neutral_preserves_pause_release_after_take_profit_guard(
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

        self.assertGreater(len(active_sells), 0)
        self.assertEqual(report["active_delever"]["trigger_mode"], "threshold")
        self.assertGreater(report["active_delever"]["synthesized_release_source_order_count"], 0)
        self.assertLess(active_sells[0]["price"], report["take_profit_guard"]["long_floor_price"])
        self.assertEqual(active_sells[0]["execution_type"], "passive_release")
        self.assertEqual(take_profit_sells, [])
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
    @patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs")
    @patch("grid_optimizer.loop_runner.post_futures_order")
    @patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_position_mode")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_raises_entry_order_below_min_notional_after_post_only_adjustment(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
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
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_competition_inventory_grid_sets_reduce_only_by_role(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
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
        mock_book_tickers.return_value = [{"bid_price": "0.09", "ask_price": "0.10"}]
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
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_synthetic_neutral_exit_orders_use_reduce_only(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
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
        mock_book_tickers.return_value = [{"bid_price": "0.09", "ask_price": "0.10"}]
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
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_caps_one_way_reduce_only_orders_to_net_position(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
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
        self.assertEqual(posted_quantities, [80.0, 173.0])
        self.assertEqual(posted_roles, ["takeprof", "takeprof"])
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
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_skips_reduce_only_rejects_after_position_race(
        self,
        mock_validate_plan_report,
        mock_book_tickers,
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
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    @patch("grid_optimizer.loop_runner.validate_plan_report")
    def test_execute_plan_report_refreshes_book_only_when_retrying_post_only_reject(
        self,
        mock_validate_plan_report,
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
        mock_book_tickers.side_effect = [
            [{"bid_price": "0.49", "ask_price": "0.51"}],
            [{"bid_price": "0.48", "ask_price": "0.50"}],
            [{"bid_price": "0.48", "ask_price": "0.50"}],
        ]
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
            ]
        )

        self.assertEqual(args.max_actual_net_notional, 120.0)
        self.assertEqual(args.max_synthetic_drift_notional, 60.0)

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

    def test_build_parser_accepts_take_profit_min_profit_ratio(self) -> None:
        args = _build_parser().parse_args(["--take-profit-min-profit-ratio", "0.0005"])

        self.assertEqual(args.take_profit_min_profit_ratio, 0.0005)

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


if __name__ == "__main__":
    unittest.main()
