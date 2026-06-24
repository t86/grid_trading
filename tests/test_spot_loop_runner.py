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
    _check_spot_freeze_hedge_gate,
    _clamp_limit_maker_price_to_book,
    _load_state,
    _make_spot_freeze_contract_callback,
    _make_spot_freeze_spot_market_callback,
    _maybe_run_spot_freeze,
    _normalize_commission_quote,
    _resolve_spot_competition_runtime,
    _spot_order_meets_exchange_mins,
    _refresh_spot_trades_after_account_snapshot,
    _sync_synthetic_neutral_trades,
    _sync_volume_shift_trades,
)
from grid_optimizer.spot_freeze_manager import new_ledger


class SpotLoopRunnerTests(unittest.TestCase):
    def _synthetic_args(self, extra: list[str] | None = None):
        argv = [
            "--symbol",
            "WLDUSDT",
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
            "--step-price",
            "0.01",
            "--per-order-notional",
            "10",
            "--neutral-base-qty",
            "100",
            "--max-short-position-notional",
            "500",
            "--max-position-notional",
            "500",
        ]
        if extra:
            argv.extend(extra)
        return _build_parser().parse_args(argv)

    def test_parser_spot_freeze_defaults_disabled(self) -> None:
        args = self._synthetic_args()

        self.assertFalse(args.spot_freeze_enabled)
        self.assertEqual(args.spot_freeze_base_hedge_qty, 0.0)
        self.assertEqual(args.spot_freeze_release_profit_ratio, 0.05)

    def test_spot_freeze_gate_rejects_non_hedge_mode(self) -> None:
        gate = _check_spot_freeze_hedge_gate(
            symbol="WLDUSDT",
            position_mode={"dualSidePosition": False},
            position_risk=[],
            base_hedge_qty=100.0,
            tolerance_qty=0.01,
        )

        self.assertFalse(gate["ok"])
        self.assertEqual(gate["reason"], "not_hedge_mode")

    def test_spot_freeze_gate_rejects_existing_long_position(self) -> None:
        gate = _check_spot_freeze_hedge_gate(
            symbol="WLDUSDT",
            position_mode={"dualSidePosition": True},
            position_risk=[
                {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0.5"},
                {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
            ],
            base_hedge_qty=100.0,
            tolerance_qty=0.01,
        )

        self.assertFalse(gate["ok"])
        self.assertEqual(gate["reason"], "long_position_not_zero")

    def test_spot_freeze_gate_rejects_short_position_not_matching_base_hedge(self) -> None:
        gate = _check_spot_freeze_hedge_gate(
            symbol="WLDUSDT",
            position_mode={"dualSidePosition": True},
            position_risk=[
                {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-95"},
            ],
            base_hedge_qty=100.0,
            tolerance_qty=0.01,
        )

        self.assertFalse(gate["ok"])
        self.assertEqual(gate["reason"], "short_position_base_mismatch")

    def test_spot_freeze_contract_wrapper_uses_position_side_short_without_reduce_kwargs(self) -> None:
        with patch("grid_optimizer.spot_loop_runner.post_futures_market_order", return_value={"executedQty": "2"}) as mock_post:
            callback = _make_spot_freeze_contract_callback(symbol="WLDUSDT", api_key="key", api_secret="secret")
            result = callback(side="BUY", qty=2.0, position_side="SHORT")

        self.assertEqual(result["executedQty"], "2")
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["symbol"], "WLDUSDT")
        self.assertEqual(kwargs["side"], "BUY")
        self.assertEqual(kwargs["quantity"], 2.0)
        self.assertEqual(kwargs["position_side"], "SHORT")
        self.assertNotIn("reduce_only", kwargs)
        self.assertNotIn("reduceOnly", kwargs)

    def test_spot_freeze_spot_market_wrapper_uses_market_order_with_price_zero(self) -> None:
        with patch("grid_optimizer.spot_loop_runner.post_spot_order", return_value={"executedQty": "2"}) as mock_post:
            callback = _make_spot_freeze_spot_market_callback(symbol="WLDUSDT", api_key="key", api_secret="secret")
            result = callback(side="SELL", qty=2.0)

        self.assertEqual(result["executedQty"], "2")
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["symbol"], "WLDUSDT")
        self.assertEqual(kwargs["side"], "SELL")
        self.assertEqual(kwargs["quantity"], 2.0)
        self.assertEqual(kwargs["price"], 0.0)
        self.assertEqual(kwargs["order_type"], "MARKET")

    def test_spot_freeze_defaults_disabled_and_does_not_call_cycle(self) -> None:
        args = self._synthetic_args()
        state = {"spot_frozen_ledger": new_ledger()}
        controls = {"actual_base_qty": 120.0, "neutral_base_qty": 100.0, "_runtime": {}}

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", side_effect=AssertionError("should not fetch")):
            with patch("grid_optimizer.spot_loop_runner.freeze_cycle", side_effect=AssertionError("should not call")):
                _maybe_run_spot_freeze(
                    args=args,
                    state=state,
                    controls=controls,
                    symbol="WLDUSDT",
                    mid_price=10.0,
                    symbol_info={"step_size": 0.001},
                    api_key="key",
                    api_secret="secret",
                    now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                )

        self.assertFalse(controls["spot_freeze_enabled"])

    def test_spot_freeze_enabled_calls_cycle_with_short_position_qty(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-base-hedge-qty",
                "100",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-min-loss-ratio",
                "0.01",
                "--spot-freeze-max-per-cycle-notional",
                "100",
                "--spot-freeze-total-cap-notional",
                "500",
            ]
        )
        state = {"spot_frozen_ledger": new_ledger()}
        controls = {
            "actual_base_qty": 120.0,
            "neutral_base_qty": 100.0,
            "_runtime": {
                "recovery_mode": "live",
                "synthetic_cost_unknown": False,
                "position_lots": [{"lot_id": "L1", "side": "long", "qty": 20.0, "entry_price": 12.0}],
            },
        }

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch(
                    "grid_optimizer.spot_loop_runner.freeze_cycle",
                    return_value={"ledger": new_ledger(), "actions": [{"type": "freeze"}], "reconcile_ok": True, "alerts": []},
                ) as mock_cycle:
                    _maybe_run_spot_freeze(
                        args=args,
                        state=state,
                        controls=controls,
                        symbol="WLDUSDT",
                        mid_price=10.0,
                        symbol_info={"step_size": 0.001},
                        api_key="key",
                        api_secret="secret",
                        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                    )

        kwargs = mock_cycle.call_args.kwargs
        self.assertEqual(kwargs["contract_short_qty"], 100.0)
        self.assertEqual(kwargs["base_hedge_qty"], 100.0)
        self.assertTrue(kwargs["loss_ratio_usable"])
        self.assertEqual(kwargs["config"].deviation_notional, 50.0)
        self.assertEqual(controls["spot_freeze_actions"], [{"type": "freeze"}])

    def test_spot_freeze_enabled_accepts_ledger_adjusted_short_position_qty(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-base-hedge-qty",
                "100",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-min-loss-ratio",
                "0.01",
                "--spot-freeze-max-per-cycle-notional",
                "100",
                "--spot-freeze-total-cap-notional",
                "500",
            ]
        )
        ledger = new_ledger()
        ledger["long_lots"] = [
            {"lot_id": "L1", "qty": 10.0, "cost_price": 12.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}
        ]
        state = {"spot_frozen_ledger": ledger}
        controls = {
            "actual_base_qty": 120.0,
            "neutral_base_qty": 100.0,
            "_runtime": {
                "recovery_mode": "live",
                "synthetic_cost_unknown": False,
                "position_lots": [{"lot_id": "L2", "side": "long", "qty": 20.0, "entry_price": 12.0}],
            },
        }

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-90"},
                ],
            ):
                with patch(
                    "grid_optimizer.spot_loop_runner.freeze_cycle",
                    return_value={"ledger": ledger, "actions": [], "reconcile_ok": True, "alerts": []},
                ) as mock_cycle:
                    _maybe_run_spot_freeze(
                        args=args,
                        state=state,
                        controls=controls,
                        symbol="WLDUSDT",
                        mid_price=10.0,
                        symbol_info={"step_size": 0.001},
                        api_key="key",
                        api_secret="secret",
                        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                    )

        kwargs = mock_cycle.call_args.kwargs
        self.assertEqual(kwargs["contract_short_qty"], 90.0)
        self.assertEqual(kwargs["base_hedge_qty"], 100.0)
        self.assertEqual(controls["spot_freeze_gate"]["reason"], "ok")

    def test_clamp_limit_maker_price_to_book_keeps_buy_post_only(self) -> None:
        price = _clamp_limit_maker_price_to_book(
            side="BUY",
            price=0.6516,
            bid_price=0.6507,
            ask_price=0.6508,
            tick_size=0.0001,
        )

        self.assertEqual(price, 0.6507)

    def test_clamp_limit_maker_price_to_book_keeps_sell_post_only(self) -> None:
        price = _clamp_limit_maker_price_to_book(
            side="SELL",
            price=0.6506,
            bid_price=0.6507,
            ask_price=0.6508,
            tick_size=0.0001,
        )

        self.assertEqual(price, 0.6508)

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
        self.assertLess(desired_orders[0]["price"], 0.099)
        self.assertLess(desired_orders[1]["price"], desired_orders[0]["price"])
        self.assertGreater(desired_orders[2]["price"], 0.101)
        self.assertGreater(desired_orders[3]["price"], desired_orders[2]["price"])
        self.assertEqual(controls["mode"], "competition_synthetic_neutral_grid")
        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["neutral_base_qty"], 1000.0)
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertAlmostEqual(controls["current_long_notional"], 0.0)
        self.assertAlmostEqual(controls["current_short_notional"], 0.0)
        self.assertAlmostEqual(controls["max_short_position_notional"], 80.0)

    def test_build_spot_competition_synthetic_neutral_grid_ignores_frozen_short_for_spot(self) -> None:
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

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY"])
        self.assertEqual([order["role"] for order in desired_orders], ["grid_exit", "forced_reduce"])
        self.assertEqual(controls["direction_state"], "short_active")
        self.assertEqual(controls["recovery_mode"], "live")
        self.assertAlmostEqual(controls["synthetic_net_qty"], -3000.0)
        self.assertGreater(controls["current_short_notional"], 0.0)
        self.assertFalse(controls["synthetic_freeze_enabled"])
        self.assertAlmostEqual(controls["synthetic_frozen_short_qty"], 0.0)
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertAlmostEqual(cached_runtime["position_lots"][0]["qty"], 3000.0)
        self.assertEqual(cached_runtime["frozen_position_lots"], [])

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

    def test_synthetic_neutral_stale_recovery_below_soft_threshold_resumes_flat_ladder(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "short_active"
        runtime["grid_anchor_price"] = 0.0902
        runtime["risk_state"] = "hard_reduce_only"
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["recovery_errors"] = ["synthetic_cost_unknown"]
        runtime["position_lots"] = []
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
            bid_price=0.0901,
            ask_price=0.0902,
            step_price=0.0001,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=20.0,
            threshold_position_notional=120.0,
            threshold_reduce_target_notional=40.0,
            max_order_position_notional=160.0,
            max_position_notional=240.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=8307.6,
            max_short_position_notional=160.0,
            actual_base_qty=7871.5,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "SELL", "SELL"])
        self.assertLess(desired_orders[0]["price"], 0.0901)
        self.assertLess(desired_orders[1]["price"], desired_orders[0]["price"])
        self.assertGreater(desired_orders[2]["price"], 0.0902)
        self.assertGreater(desired_orders[3]["price"], desired_orders[2]["price"])
        self.assertEqual(controls["direction_state"], "flat")
        self.assertEqual(controls["risk_state"], "normal")
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["direction_state"], "flat")
        self.assertEqual(cached_runtime["position_lots"], [])

    def test_synthetic_neutral_stale_recovery_above_soft_threshold_stays_defensive(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "short_active"
        runtime["grid_anchor_price"] = 0.6736
        runtime["risk_state"] = "hard_reduce_only"
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["recovery_errors"] = ["position_qty_mismatch"]
        runtime["position_lots"] = []
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
            bid_price=0.6507,
            ask_price=0.6508,
            step_price=0.001,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=40.0,
            threshold_position_notional=120.0,
            threshold_reduce_target_notional=40.0,
            max_order_position_notional=300.0,
            max_position_notional=360.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1650.1,
            max_short_position_notional=180.0,
            actual_base_qty=1450.0,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY"])
        self.assertIn("grid_exit", {str(order.get("role")) for order in desired_orders})
        self.assertIn("forced_reduce", {str(order.get("role")) for order in desired_orders})
        self.assertEqual(controls["direction_state"], "short_active")
        self.assertEqual(controls["risk_state"], "threshold_reduce_only")
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["position_lots"][0]["source_role"], "synthetic_recovery")
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertTrue(controls["synthetic_cost_unknown"])

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
        self.assertEqual(buy_prices, [682.58, 682.46, 682.34])
        self.assertEqual(sell_prices, [682.92, 683.04, 683.16])
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

    def test_build_spot_competition_synthetic_neutral_grid_reduces_instead_of_freezing_lots(self) -> None:
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

        self.assertTrue(any(order["role"] == "forced_reduce" for order in desired_orders))
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["frozen_position_lots"], [])
        self.assertAlmostEqual(controls["synthetic_frozen_position"]["long_qty"], 0.0)
        self.assertAlmostEqual(controls["synthetic_frozen_position"]["short_qty"], 0.0)
        self.assertEqual(controls["synthetic_freeze_last"], {})

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
        self.assertEqual(cached_runtime["frozen_position_lots"], [])

    def test_synthetic_neutral_active_lot_tolerates_residual_dust_after_fill(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {
                "1": {"role": "bootstrap_entry", "side": "BUY"},
            },
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }

        _desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[
                {
                    "time": 1000,
                    "id": 10,
                    "orderId": 1,
                    "price": "0.1806",
                    "qty": "1495.0",
                }
            ],
            bid_price=0.1805,
            ask_price=0.1807,
            step_price=0.0002,
            buy_levels=3,
            sell_levels=3,
            first_order_multiplier=1.5,
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
            actual_base_qty=11495.72908,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.01,
            synthetic_freeze_min_notional=20.0,
        )

        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertFalse(cached_runtime.get("synthetic_cost_unknown"))
        self.assertEqual(controls["recovery_mode"], "live")
        self.assertAlmostEqual(cached_runtime["position_lots"][0]["qty"], 1495.0)
        self.assertNotEqual(cached_runtime["position_lots"][0].get("lot_id"), "synthetic_recovered")

    def test_synthetic_neutral_runtime_replays_opposite_bootstrap_fill(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "flat"
        runtime["grid_anchor_price"] = 0.1815
        runtime["frozen_position_lots"] = [
            {
                "lot_id": "frozen",
                "source_lot_id": "held",
                "side": "long",
                "qty": 3841.22908,
                "entry_price": 0.1874,
                "opened_at_ms": 1000,
                "source_role": "grid_entry",
                "frozen_at_ms": 2000,
                "freeze_reason": "threshold_reduce_loss",
            }
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {
                "1": {"role": "bootstrap_entry", "side": "SELL"},
                "2": {"role": "bootstrap_entry", "side": "BUY"},
            },
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": runtime,
                "applied_trade_keys": [],
            },
        }
        trades = [
            {"time": 1000, "id": 10, "orderId": 1, "price": "0.1816", "qty": "991.1"},
            {"time": 1100, "id": 11, "orderId": 2, "price": "0.1815", "qty": "731.5"},
        ]

        resolved = _resolve_spot_competition_runtime(
            state=state,
            trades=trades,
            order_refs=state["known_orders"],
            step_price=0.0002,
            current_position_qty=3581.72908,
            synthetic_neutral=True,
            position_qty_tolerance=27.5,
        )

        self.assertEqual(resolved["recovery_mode"], "live")
        self.assertEqual(resolved["direction_state"], "short_active")
        self.assertAlmostEqual(
            sum(item["qty"] for item in resolved["position_lots"]),
            259.6,
        )
        self.assertAlmostEqual(
            sum(item["qty"] for item in resolved["frozen_position_lots"]),
            3841.22908,
        )

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
        self.assertEqual(cached_runtime["frozen_position_lots"], [])
        self.assertEqual(controls["synthetic_freeze_last"], {})
        self.assertAlmostEqual(controls["synthetic_frozen_long_qty"], 0.0)

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

        self.assertEqual([order["role"] for order in desired_orders], ["grid_entry", "grid_entry", "grid_entry", "grid_exit"])
        self.assertNotIn("synthetic_frozen_release", [order["role"] for order in desired_orders])
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["frozen_position_lots"], [])
        self.assertEqual(controls["synthetic_pair_release_last"], {})
        self.assertAlmostEqual(controls["synthetic_frozen_position"]["long_qty"], 0.0)
        self.assertAlmostEqual(controls["synthetic_frozen_position"]["short_qty"], 0.0)

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

    def test_refresh_spot_trades_after_account_snapshot_applies_late_synthetic_fill(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "seen_trade_ids": [1],
            "known_orders": {
                "123": {"side": "BUY", "role": "bootstrap_entry", "created_at_ms": 1},
                "124": {"side": "BUY", "role": "grid_entry", "created_at_ms": 2},
            },
            "metrics": {},
            "last_trade_time_ms": 1000,
        }
        first_trades = [
            {
                "id": 1,
                "orderId": 123,
                "price": "0.10",
                "qty": "100",
                "commission": "0",
                "commissionAsset": "USDT",
                "time": 1000,
            }
        ]
        refreshed_trades = [
            *first_trades,
            {
                "id": 2,
                "orderId": 124,
                "price": "0.10",
                "qty": "50",
                "commission": "0",
                "commissionAsset": "USDT",
                "time": 1100,
            },
        ]

        with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=refreshed_trades):
            trades, applied = _refresh_spot_trades_after_account_snapshot(
                state=state,
                trades=first_trades,
                symbol="SPKUSDT",
                api_key="key",
                api_secret="secret",
                trade_start_ms=900,
                base_asset="SPK",
                quote_asset="USDT",
            )

        self.assertEqual(trades, refreshed_trades)
        self.assertEqual(applied, 1)
        self.assertEqual(state["seen_trade_ids"], [1, 2])
        self.assertEqual(state["last_trade_time_ms"], 1100)
        self.assertAlmostEqual(state["metrics"]["gross_notional"], 5.0)

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

    def test_synthetic_neutral_resumes_with_unknown_cost_surplus_below_threshold(self) -> None:
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
        roles = {str(order.get("role")) for order in desired_orders}
        self.assertIn("bootstrap_entry", roles)
        self.assertIn("grid_entry", roles)
        self.assertEqual(cached_runtime["direction_state"], "flat")
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertEqual(cached_runtime["position_lots"], [])
        self.assertEqual(cached_runtime["recovery_errors"], [])
        self.assertFalse(controls["synthetic_cost_unknown"])
        self.assertEqual(controls["recovery_mode"], "live")
        self.assertAlmostEqual(controls["synthetic_frozen_long_qty"], 0.0, places=6)

    def test_synthetic_neutral_keeps_unknown_cost_surplus_on_normal_freeze_path(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "inventory_lots": [],
        }

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.1820,
            ask_price=0.1821,
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
            actual_base_qty=17355.7,
            max_short_position_notional=1200.0,
            synthetic_freeze_enabled=True,
            synthetic_freeze_loss_ratio=0.006,
            synthetic_freeze_min_notional=20.0,
            synthetic_freeze_max_side_notional=2000.0,
        )

        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        roles = {str(order.get("role")) for order in desired_orders}
        self.assertNotIn("synthetic_frozen_release", roles)
        self.assertNotIn("bootstrap_entry", roles)
        self.assertIn("grid_exit", roles)
        self.assertEqual(cached_runtime["position_lots"][0]["source_role"], "synthetic_recovery")
        self.assertEqual(cached_runtime["direction_state"], "long_active")
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertGreater(controls["current_long_notional"], 600.0)
        self.assertAlmostEqual(controls["synthetic_frozen_long_qty"], 0.0, places=6)
        self.assertTrue(controls["synthetic_cost_unknown"])

    def test_synthetic_neutral_resumes_cached_synthetic_recovered_lot_below_threshold(self) -> None:
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
        roles = {str(order.get("role")) for order in desired_orders}
        self.assertIn("grid_exit", roles)
        self.assertIn("grid_entry", roles)
        self.assertEqual(cached_runtime["position_lots"][0]["source_role"], "synthetic_recovery")
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertEqual(cached_runtime["recovery_errors"], [])
        self.assertTrue(controls["synthetic_cost_unknown"])
        self.assertEqual(controls["recovery_mode"], "live")

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
