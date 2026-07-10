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
    _build_client_order_id,
    _build_parser,
    _build_spot_app_loss_guard,
    _build_spot_competition_inventory_grid_orders,
    _build_spot_equity_pnl_summary,
    _check_spot_freeze_hedge_gate,
    _clamp_limit_maker_price_to_book,
    _cancel_orders,
    _load_state,
    _make_spot_freeze_contract_callback,
    _make_spot_freeze_spot_market_callback,
    _maybe_run_spot_freeze,
    _normalize_commission_quote,
    _apply_spot_app_loss_guard_to_orders,
    _apply_spot_freeze_maker_orders_to_desired_orders,
    _apply_spot_freeze_runtime_hedge_block,
    _resolve_spot_competition_runtime,
    _run_cycle,
    _spot_order_meets_exchange_mins,
    _spot_app_loss_mark_price,
    _spot_app_loss_metrics_with_trade_fallback,
    _spot_freeze_open_maker_orders,
    _refresh_spot_trades_after_account_snapshot,
    _run_same_price_take_exits,
    _sync_synthetic_neutral_trades,
    _sync_volume_shift_trades,
    _spot_app_loss_guard_should_stop_runner,
    _runtime_guard_events_from_metrics,
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
        self.assertFalse(args.spot_freeze_dry_run)
        self.assertFalse(args.spot_freeze_market_execution_enabled)
        self.assertFalse(args.spot_freeze_maker_execution_enabled)
        self.assertEqual(args.spot_freeze_base_hedge_qty, 0.0)
        self.assertEqual(args.spot_freeze_long_total_cap_notional, 0.0)
        self.assertEqual(args.spot_freeze_short_total_cap_notional, 0.0)
        self.assertEqual(args.spot_freeze_release_profit_ratio, 0.05)
        self.assertFalse(args.spot_base_restore_only)

    def test_parser_accepts_spot_base_rebalance_controls(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-base-rebalance-soft-tolerance-qty",
                "100",
                "--spot-base-rebalance-hard-tolerance-qty",
                "1000",
                "--spot-base-rebalance-max-buy-levels",
                "2",
            ]
        )

        self.assertEqual(args.spot_base_rebalance_soft_tolerance_qty, 100.0)
        self.assertEqual(args.spot_base_rebalance_hard_tolerance_qty, 1000.0)
        self.assertEqual(args.spot_base_rebalance_max_buy_levels, 2)

    def test_same_price_take_exit_sells_new_known_maker_buy_fill(self) -> None:
        state = {
            "known_orders": {"88": {"side": "BUY", "tag": "buy"}},
            "same_price_take_exit_trade_ids": [],
        }
        submitted: list[float] = []

        result = _run_same_price_take_exits(
            state=state,
            trades=[{"id": 701, "orderId": 88, "qty": "6", "price": "1.62", "isBuyer": True}],
            bid_price=1.62,
            bid_qty=10.0,
            min_qty=1.0,
            min_notional=5.0,
            step_size=0.01,
            submit_ioc_sell=lambda qty, price: submitted.append(qty) or {"orderId": 99},
        )

        self.assertEqual(result["executed_qty"], 6.0)
        self.assertEqual(submitted, [6.0])
        self.assertEqual(state["same_price_take_exit_trade_ids"], [701])

        repeat = _run_same_price_take_exits(
            state=state,
            trades=[{"id": 701, "orderId": 88, "qty": "6", "price": "1.62", "isBuyer": True}],
            bid_price=1.62,
            bid_qty=10.0,
            min_qty=1.0,
            min_notional=5.0,
            step_size=0.01,
            submit_ioc_sell=lambda qty, price: submitted.append(qty) or {"orderId": 100},
        )

        self.assertEqual(repeat["executed_qty"], 0.0)
        self.assertEqual(submitted, [6.0])

    def test_same_price_take_exit_skips_lower_or_thin_best_bid(self) -> None:
        for bid_price, bid_qty in ((1.619, 10.0), (1.62, 5.99)):
            with self.subTest(bid_price=bid_price, bid_qty=bid_qty):
                state = {"known_orders": {"88": {"side": "BUY"}}}
                result = _run_same_price_take_exits(
                    state=state,
                    trades=[{"id": 701, "orderId": 88, "qty": "6", "price": "1.62", "isBuyer": True}],
                    bid_price=bid_price,
                    bid_qty=bid_qty,
                    min_qty=1.0,
                    min_notional=5.0,
                    step_size=0.01,
                    submit_ioc_sell=lambda qty, price: self.fail(f"unexpected sell {qty} at {price}"),
                )

                self.assertEqual(result["executed_qty"], 0.0)
                self.assertEqual(state.get("same_price_take_exit_trade_ids"), [])

    def test_same_price_take_exit_skips_fill_before_feature_activation(self) -> None:
        state = {"known_orders": {"88": {"side": "BUY"}}}

        result = _run_same_price_take_exits(
            state=state,
            trades=[{"id": 701, "orderId": 88, "time": 1000, "qty": "6", "price": "1.62", "isBuyer": True}],
            bid_price=1.62,
            bid_qty=10.0,
            min_qty=1.0,
            min_notional=5.0,
            step_size=0.01,
            start_time_ms=1001,
            submit_ioc_sell=lambda qty, price: self.fail(f"unexpected sell {qty} at {price}"),
        )

        self.assertEqual(result["executed_qty"], 0.0)
        self.assertEqual(result["skipped"], [{"trade_id": 701, "reason": "before_activation"}])

    def test_same_price_take_exit_skips_while_base_restore_is_active(self) -> None:
        state = {"known_orders": {"88": {"side": "BUY"}}}

        result = _run_same_price_take_exits(
            state=state,
            trades=[{"id": 701, "orderId": 88, "qty": "6", "price": "1.62", "isBuyer": True}],
            bid_price=1.62,
            bid_qty=10.0,
            min_qty=1.0,
            min_notional=5.0,
            step_size=0.01,
            base_restore_active=True,
            submit_ioc_sell=lambda qty, price: self.fail(f"unexpected sell {qty} at {price}"),
        )

        self.assertEqual(result["executed_qty"], 0.0)
        self.assertEqual(result["reason"], "base_restore_active")

    def test_client_order_id_stays_within_binance_limit_for_spot_freeze_tag(self) -> None:
        with patch("grid_optimizer.spot_loop_runner.time.time", return_value=1782349765.432):
            client_order_id = _build_client_order_id("sgxpl114", "spot_freeze_short", "BUY")

        self.assertLessEqual(len(client_order_id), 36)
        self.assertRegex(client_order_id, r"^[a-zA-Z0-9-_]{1,36}$")
        self.assertTrue(client_order_id.startswith("sgxpl114_"))
        self.assertIn("_b_", client_order_id)
        self.assertTrue(client_order_id.endswith("_349765432"))

    def test_client_order_id_preserves_side_and_nonce_for_long_and_reduce_tags(self) -> None:
        with patch("grid_optimizer.spot_loop_runner.time.time", return_value=1782349765.432):
            freeze_short = _build_client_order_id("sgxpl114", "spot_freeze_short", "BUY")
            freeze_long = _build_client_order_id("sgxpl114", "spot_freeze_long", "SELL")
            app_loss_buy = _build_client_order_id("sgxpl114", "spot_app_loss_reduce", "BUY")
            app_loss_sell = _build_client_order_id("sgxpl114", "spot_app_loss_reduce", "SELL")

        client_order_ids = [freeze_short, freeze_long, app_loss_buy, app_loss_sell]
        self.assertEqual(len(set(client_order_ids)), len(client_order_ids))
        for client_order_id in client_order_ids:
            self.assertLessEqual(len(client_order_id), 36)
            self.assertRegex(client_order_id, r"^[a-zA-Z0-9-_]{1,36}$")
            self.assertTrue(client_order_id.endswith("_349765432"))

        self.assertIn("_b_", freeze_short)
        self.assertIn("_s_", freeze_long)
        self.assertIn("_b_", app_loss_buy)
        self.assertIn("_s_", app_loss_sell)

    def test_runtime_guard_events_exclude_spot_commission_from_net_pnl(self) -> None:
        events = _runtime_guard_events_from_metrics(
            {
                "recent_trades": [
                    {
                        "time": 1779570000000,
                        "realized_pnl": -0.12,
                        "commission_quote": 0.12,
                        "notional": 200.0,
                    }
                ]
            }
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["net_pnl"], 0.0)
        self.assertEqual(events[0]["gross_notional"], 200.0)

    def test_spot_app_loss_guard_uses_binance_app_formula_without_offsets(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "buy_notional": 1000.0,
                "sell_notional": 400.0,
                "buy_qty": 120.0,
                "sell_qty": 20.0,
                "commission_quote": 50.0,
                "realized_pnl": 999.0,
            },
            position_qty=100.0,
            latest_price=5.0,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )

        self.assertEqual(guard["formula"], "buy_notional-sell_notional-position_qty*latest_price")
        self.assertAlmostEqual(guard["buy_notional"], 1000.0)
        self.assertAlmostEqual(guard["sell_notional"], 400.0)
        self.assertEqual(guard["window_source"], "metrics")
        self.assertTrue(guard["window_aligned"])
        self.assertAlmostEqual(guard["position_value"], 500.0)
        self.assertAlmostEqual(guard["app_loss"], 100.0)
        self.assertAlmostEqual(guard["app_loss_per_10k"], 100.0 / 1400.0 * 10000.0)
        self.assertEqual(guard["state"], "blocked")

    def test_spot_app_loss_mark_price_uses_bid_for_window_long(self) -> None:
        price = _spot_app_loss_mark_price(
            metrics={"buy_qty": 100.0, "sell_qty": 90.0},
            bid_price=0.0868,
            ask_price=0.0869,
        )

        self.assertEqual(price, 0.0868)

    def test_spot_app_loss_mark_price_falls_back_to_mid_without_window_long(self) -> None:
        price = _spot_app_loss_mark_price(
            metrics={"buy_qty": 100.0, "sell_qty": 100.0},
            bid_price=0.0868,
            ask_price=0.0869,
        )

        self.assertAlmostEqual(price, 0.08685)

    def test_spot_app_loss_mark_price_uses_recent_trade_window_long(self) -> None:
        price = _spot_app_loss_mark_price(
            metrics={
                "buy_qty": 0.0,
                "sell_qty": 0.0,
                "app_loss_window_aligned": False,
                "recent_trades": [
                    {"side": "BUY", "qty": 100.0, "notional": 10.0},
                    {"side": "SELL", "qty": 90.0, "notional": 9.0},
                ],
            },
            bid_price=0.0868,
            ask_price=0.0869,
        )

        self.assertEqual(price, 0.0868)

    def test_spot_app_loss_guard_uses_window_net_qty_not_account_base_balance(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "buy_notional": 486987.18507,
                "sell_notional": 486236.63072,
                "buy_qty": 8643120.7,
                "sell_qty": 8632348.7,
            },
            position_qty=1_000_000.0,
            latest_price=0.05607,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )

        self.assertAlmostEqual(guard["position_qty"], 10772.0)
        self.assertAlmostEqual(guard["position_value"], 603.98604, places=5)
        self.assertAlmostEqual(guard["app_loss"], 146.56831, places=5)
        self.assertEqual(guard["window_source"], "metrics")

    def test_spot_app_loss_guard_includes_starting_base_inventory_value(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "app_loss_baseline_qty": 5475.3,
                "app_loss_baseline_notional": 511.0371255,
                "buy_notional": 5950.937708,
                "sell_notional": 6034.176797,
                "buy_qty": 63685.0,
                "sell_qty": 64595.9,
            },
            position_qty=4564.4,
            latest_price=0.093535,
            min_notional=0.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
        )

        self.assertAlmostEqual(guard["baseline_qty"], 5475.3)
        self.assertAlmostEqual(guard["baseline_notional"], 511.0371255)
        self.assertAlmostEqual(guard["buy_notional"], 6461.9748335)
        self.assertAlmostEqual(guard["position_qty"], 4564.4)
        self.assertAlmostEqual(guard["position_value"], 426.931154)
        self.assertAlmostEqual(guard["app_loss"], 0.8668825)
        self.assertAlmostEqual(guard["app_loss_per_10k"], 0.8668825 / 11985.114505 * 10000.0)
        self.assertEqual(guard["state"], "defensive")

    def test_spot_app_loss_guard_treats_tiny_window_net_qty_as_flat(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "buy_notional": 3662.9036000000006,
                "sell_notional": 3656.8843600000014,
                "buy_qty": 41413.6,
                "sell_qty": 41413.59999999999,
            },
            position_qty=0.0,
            latest_price=0.0914,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            tick_size=0.0001,
            min_bid_break_even_buffer_ticks=3.0,
        )

        self.assertEqual(guard["position_qty"], 0.0)
        self.assertEqual(guard["position_value"], 0.0)
        self.assertEqual(guard["break_even_price"], 0.0)
        self.assertEqual(guard["bid_break_even_buffer_ticks"], 0.0)
        self.assertFalse(guard["bid_break_even_buffer_below_min"])
        self.assertEqual(guard["state"], "blocked")

    def test_spot_app_loss_guard_uses_recent_trades_when_legacy_window_misaligned(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "buy_notional": 100000.0,
                "sell_notional": 99900.0,
                "buy_qty": 1.0,
                "sell_qty": 0.0,
                "app_loss_window_aligned": False,
                "recent_trades": [
                    {"side": "BUY", "qty": 100.0, "notional": 1000.0},
                    {"side": "SELL", "qty": 90.0, "notional": 950.0},
                ],
            },
            position_qty=1000.0,
            latest_price=4.0,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )

        self.assertEqual(guard["window_source"], "recent_trades")
        self.assertTrue(guard["window_aligned"])
        self.assertAlmostEqual(guard["buy_notional"], 1000.0)
        self.assertAlmostEqual(guard["sell_notional"], 950.0)
        self.assertAlmostEqual(guard["position_qty"], 10.0)
        self.assertAlmostEqual(guard["app_loss"], 10.0)

    def test_spot_equity_pnl_uses_initial_base_trade_cashflow_and_tracked_contract_pnl(self) -> None:
        state = {
            "spot_contract_hedge_lots": [
                {"qty": 0.12, "entry_price": 1575.68, "position_side": "SHORT"},
            ],
            "spot_frozen_ledger": {
                "short_lots": [{"qty": 0.01, "contract_entry_price": 1600.0}],
                "long_lots": [{"qty": 0.02, "contract_realized_pnl": 0.5}],
            },
        }
        metrics = {
            "app_loss_baseline_qty": 0.1,
            "app_loss_baseline_notional": 100.0,
            "buy_notional": 20.0,
            "sell_notional": 10.0,
        }

        summary = _build_spot_equity_pnl_summary(
            state=state,
            metrics=metrics,
            actual_base_qty=0.11,
            latest_price=1000.0,
            mark_price=1570.0,
        )

        self.assertAlmostEqual(summary["spot_cashflow_pnl"], 0.0)
        self.assertAlmostEqual(summary["base_hedge_contract_pnl"], (1575.68 - 1570.0) * 0.12)
        self.assertAlmostEqual(summary["freeze_contract_pnl"], (1600.0 - 1570.0) * 0.01 + 0.5)
        self.assertAlmostEqual(summary["equity_pnl"], summary["contract_pnl"])
        self.assertAlmostEqual(summary["equity_loss"], -summary["equity_pnl"])

    def test_spot_equity_pnl_offsets_initial_base_buy_when_it_is_already_in_trade_metrics(self) -> None:
        summary = _build_spot_equity_pnl_summary(
            state={},
            metrics={
                "equity_initial_base_qty": 0.12,
                "equity_initial_base_notional": 189.24,
                "equity_buy_notional_offset": 189.24,
                "buy_notional": 189.24,
                "sell_notional": 0.0,
            },
            actual_base_qty=0.12,
            latest_price=1577.0,
            mark_price=1577.0,
        )

        self.assertAlmostEqual(summary["initial_base_cost"], 189.24)
        self.assertAlmostEqual(summary["buy_notional"], 0.0)
        self.assertAlmostEqual(summary["spot_cashflow_pnl"], 0.0)

    def test_spot_app_loss_metrics_falls_back_to_raw_mytrades_when_state_window_empty(self) -> None:
        metrics = _spot_app_loss_metrics_with_trade_fallback(
            metrics={"buy_notional": 0.0, "sell_notional": 0.0, "buy_qty": 0.0, "sell_qty": 0.0},
            trades=[
                {
                    "id": 1,
                    "orderId": 10,
                    "isBuyer": True,
                    "isMaker": True,
                    "price": "0.0885",
                    "qty": "4800",
                    "quoteQty": "424.8",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 1000,
                },
                {
                    "id": 2,
                    "orderId": 11,
                    "isBuyer": False,
                    "isMaker": True,
                    "price": "0.0870",
                    "qty": "680",
                    "quoteQty": "59.16",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 1100,
                },
            ],
        )

        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics=metrics,
            position_qty=4120.0,
            latest_price=0.0868,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )

        self.assertEqual(guard["window_source"], "recent_trades")
        self.assertAlmostEqual(guard["buy_notional"], 424.8)
        self.assertAlmostEqual(guard["sell_notional"], 59.16)
        self.assertAlmostEqual(guard["position_qty"], 4120.0)
        self.assertEqual(guard["state"], "blocked")

    def test_spot_app_loss_guard_blocks_before_min_notional_when_hard_loss_budget_is_spent(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "buy_notional": 2664.6854,
                "sell_notional": 2240.60684,
                "buy_qty": 30113.9,
                "sell_qty": 25311.1,
            },
            position_qty=4802.8,
            latest_price=0.08815,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
        )

        self.assertLess(guard["gross_notional"], 5000.0)
        self.assertGreater(guard["app_loss"], 0.5)
        self.assertEqual(guard["state"], "blocked")
        self.assertEqual(guard["preactive_loss_cap"], 0.5)

    def test_spot_app_loss_guard_keeps_small_preactive_mark_loss_warming_up(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={
                "buy_notional": 422.88,
                "sell_notional": 0.0,
                "buy_qty": 4800.0,
                "sell_qty": 0.0,
            },
            position_qty=4800.0,
            latest_price=0.08805,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
        )

        self.assertLess(guard["app_loss"], 0.5)
        self.assertEqual(guard["state"], "warming_up")

    def test_spot_app_loss_guard_blocks_when_bid_buffer_is_too_small(self) -> None:
        guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={"buy_notional": 1000.0, "sell_notional": 900.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=100.0,
            latest_price=1.01,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            tick_size=0.01,
            min_bid_break_even_buffer_ticks=3.0,
        )

        self.assertEqual(guard["state"], "blocked")
        self.assertEqual(guard["app_loss_per_10k"], 0.0)
        self.assertAlmostEqual(guard["break_even_price"], 1.0)
        self.assertAlmostEqual(guard["bid_break_even_buffer_ticks"], 1.0)
        self.assertTrue(guard["bid_break_even_buffer_below_min"])

    def test_spot_app_loss_guard_hard_block_requests_runner_stop(self) -> None:
        hard_loss_guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={"buy_notional": 1000.0, "sell_notional": 800.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=100.0,
            latest_price=1.0,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )
        preactive_guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={"buy_notional": 2000.0, "sell_notional": 1999.4, "buy_qty": 2000.0, "sell_qty": 1999.4},
            position_qty=0.6,
            latest_price=0.0,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
        )
        bid_buffer_guard = _build_spot_app_loss_guard(
            enabled=True,
            metrics={"buy_notional": 1000.0, "sell_notional": 900.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=100.0,
            latest_price=1.01,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            tick_size=0.01,
            min_bid_break_even_buffer_ticks=3.0,
        )
        defensive_guard = dict(hard_loss_guard, state="defensive", app_loss_per_10k=1.2)
        recovery_guard = dict(hard_loss_guard, state="recovery_reduce_only", app_loss_per_10k=0.0)

        self.assertTrue(_spot_app_loss_guard_should_stop_runner(hard_loss_guard))
        self.assertTrue(_spot_app_loss_guard_should_stop_runner(preactive_guard))
        self.assertTrue(_spot_app_loss_guard_should_stop_runner(bid_buffer_guard))
        self.assertFalse(_spot_app_loss_guard_should_stop_runner(defensive_guard))
        self.assertFalse(_spot_app_loss_guard_should_stop_runner(recovery_guard))

    def test_spot_app_loss_guard_does_not_reduce_when_window_unaligned(self) -> None:
        controls = {"actual_base_qty": 120.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.0, "qty": 10.0},
            {"side": "SELL", "role": "forced_reduce", "price": 1.6, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 1000.0,
                "sell_notional": 850.0,
                "app_loss_window_aligned": False,
            },
            position_qty=120.0,
            latest_price=1.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )

        self.assertEqual(filtered, desired_orders)
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "insufficient_window")
        self.assertFalse(controls["spot_app_loss_guard"]["window_aligned"])

    def test_load_state_marks_legacy_app_loss_window_unaligned(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            path.write_text(
                json.dumps(
                    {
                        "symbol": "MEGAUSDT",
                        "strategy_mode": "spot_synthetic_neutral",
                        "metrics": {"buy_notional": 1000.0, "sell_notional": 900.0},
                    }
                ),
                encoding="utf-8",
            )

            state = _load_state(path, "MEGAUSDT", "spot_synthetic_neutral", 3)

        self.assertFalse(state["metrics"]["app_loss_window_aligned"])
        self.assertEqual(state["metrics"]["buy_qty"], 0.0)
        self.assertEqual(state["metrics"]["sell_qty"], 0.0)

    def test_spot_app_loss_guard_keeps_only_reduce_orders_when_long_deviation_blocks(self) -> None:
        controls = {"actual_base_qty": 120.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.0, "qty": 10.0},
            {"side": "SELL", "role": "forced_reduce", "price": 1.6, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1000.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=120.0,
            latest_price=1.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["SELL"])
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "blocked")
        self.assertEqual(controls["spot_app_loss_guard"]["action"], "reduce_only")
        self.assertEqual(controls["spot_app_loss_guard"]["dropped_order_count"], 1)
        self.assertTrue(controls["buy_paused"])
        self.assertIn("spot_app_loss_guard_blocked", controls["pause_reasons"])

    def test_spot_app_loss_guard_does_not_sell_neutral_dust_below_exchange_min(self) -> None:
        controls = {"actual_base_qty": 4802.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 0.0880, "qty": 681.0},
            {"side": "SELL", "role": "grid_exit", "price": 0.0881, "qty": 681.0},
            {"side": "SELL", "role": "grid_exit", "price": 0.0882, "qty": 680.2},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2300.53484,
                "buy_qty": 30794.9,
                "sell_qty": 25992.1,
            },
            position_qty=4802.8,
            latest_price=0.08805,
            enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=4802.8,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "blocked")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "SELL")
        self.assertAlmostEqual(controls["spot_app_loss_guard"]["reduce_deviation_qty"], 2.8)
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 0)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_app_loss_guard_keeps_non_loss_reduce_orders_for_app_window_position(self) -> None:
        controls = {"actual_base_qty": 5483.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "SELL", "role": "grid_exit", "price": 0.0884, "qty": 681.0},
            {"side": "SELL", "role": "grid_exit", "price": 0.0885, "qty": 680.2},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2240.60684,
                "buy_qty": 30794.9,
                "sell_qty": 25311.1,
            },
            position_qty=5483.8,
            latest_price=0.08805,
            enabled=True,
            min_notional=4000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=5483.8,
        )

        self.assertEqual([order["side"] for order in filtered], ["SELL"])
        self.assertAlmostEqual(filtered[0]["qty"], 681.0)
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 1)
        self.assertAlmostEqual(controls["spot_app_loss_guard"]["reduce_deviation_qty"], 683.8)

    def test_spot_app_loss_guard_replaces_lossy_reduce_order_with_break_even_sell(self) -> None:
        controls = {"actual_base_qty": 5483.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "SELL", "role": "grid_exit", "price": 0.0881, "qty": 681.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2240.60684,
                "buy_qty": 30794.9,
                "sell_qty": 25311.1,
            },
            position_qty=5483.8,
            latest_price=0.08805,
            enabled=True,
            min_notional=4000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=5483.8,
        )

        self.assertEqual([order["role"] for order in filtered], ["spot_app_loss_reduce"])
        self.assertAlmostEqual(filtered[0]["price"], 0.0883)
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 0)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 1)

    def test_spot_app_loss_guard_drops_lossy_sell_when_non_loss_exit_is_required(self) -> None:
        controls = {"actual_base_qty": 4800.0, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "SELL", "role": "grid_entry", "price": 0.09296, "qty": 118.3},
            {"side": "SELL", "role": "grid_entry", "price": 0.09298, "qty": 118.3},
            {"side": "BUY", "role": "grid_entry", "price": 0.09285, "qty": 118.4},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 10.999534,
                "sell_notional": 0.0,
                "buy_qty": 118.3,
                "sell_qty": 0.0,
            },
            position_qty=4800.0,
            latest_price=0.09296,
            enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=11.0,
            tick_size=0.00001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=4800.0,
            require_non_loss_exit=True,
        )

        self.assertEqual([(order["side"], order["price"]) for order in filtered], [("SELL", 0.09298), ("BUY", 0.09285)])
        self.assertEqual(controls["spot_app_loss_guard"]["non_loss_exit_dropped_order_count"], 1)

    def test_require_non_loss_exit_filters_even_when_app_loss_guard_disabled(self) -> None:
        controls = {"actual_base_qty": 0.18, "neutral_base_qty": 0.12}
        desired_orders = [
            {"side": "SELL", "role": "grid_exit", "price": 99.8, "qty": 0.5},
            {"side": "SELL", "role": "grid_exit", "price": 100.0, "qty": 0.5},
            {"side": "BUY", "role": "grid_entry", "price": 99.4, "qty": 0.5},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 100.0,
                "sell_notional": 0.0,
                "buy_qty": 1.0,
                "sell_qty": 0.0,
            },
            position_qty=1.0,
            latest_price=100.0,
            enabled=False,
            min_notional=500.0,
            soft_per_10k=0.0,
            hard_per_10k=0.0,
            tick_size=0.1,
            require_non_loss_exit=True,
        )

        self.assertEqual([(order["side"], order["price"]) for order in filtered], [("SELL", 100.0), ("BUY", 99.4)])
        self.assertFalse(controls["spot_app_loss_guard"]["enabled"])
        self.assertEqual(controls["spot_app_loss_guard"]["non_loss_exit_dropped_order_count"], 1)
        self.assertEqual(controls["spot_app_loss_guard"]["non_loss_exit_price"], 100.0)

    def test_spot_app_loss_guard_does_not_sell_when_neutral_deviation_is_zero(self) -> None:
        controls = {"actual_base_qty": 4800.0, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 0.0868, "qty": 690.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2300.53484,
                "buy_qty": 30794.9,
                "sell_qty": 25992.1,
            },
            position_qty=4800.0,
            latest_price=0.08805,
            enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=4800.0,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_deviation_qty"], 0.0)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_app_loss_guard_injects_maker_sell_when_long_deviation_has_no_reduce_order(self) -> None:
        controls = {"actual_base_qty": 140.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.0, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 860.0},
            position_qty=140.0,
            latest_price=2.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=25.0,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["SELL"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertEqual(filtered[0]["tag"], "app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 2.5)
        self.assertAlmostEqual(filtered[0]["qty"], 10.0)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 1)

    def test_spot_app_loss_guard_does_not_sell_when_actual_base_is_below_neutral(self) -> None:
        controls = {"actual_base_qty": 4121.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_exit", "price": 0.0867, "qty": 691.2},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2360.53094,
                "buy_qty": 30794.9,
                "sell_qty": 26673.1,
            },
            position_qty=4121.8,
            latest_price=0.0868,
            enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=4121.8,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 0.0867)
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "BUY")
        self.assertFalse(controls.get("buy_paused", False))

    def test_spot_app_loss_recovery_reduce_only_unwinds_positive_app_window_long(self) -> None:
        controls = {"actual_base_qty": 4121.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "SELL", "role": "grid_exit", "price": 0.0892, "qty": 672.0},
            {"side": "BUY", "role": "grid_exit", "price": 0.0891, "qty": 672.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2360.53094,
                "buy_qty": 30794.9,
                "sell_qty": 26673.1,
            },
            position_qty=4121.8,
            latest_price=0.0892,
            maker_reference_price=0.0893,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=4121.8,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 0.0891)
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "recovery_reduce_only")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "BUY")
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 1)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)
        self.assertAlmostEqual(controls["spot_app_loss_guard"]["app_loss_per_10k"], 0.0)
        self.assertFalse(controls.get("buy_paused", False))
        self.assertIn("spot_app_loss_guard_recovery_reduce_only", controls["pause_reasons"])

    def test_spot_app_loss_recovery_returns_to_cruise_when_neutral_deviation_is_clear(self) -> None:
        controls = {"actual_base_qty": 4800.0, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 0.0899, "qty": 222.0},
            {"side": "SELL", "role": "grid_exit", "price": 0.0902, "qty": 222.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2299.6215,
                "buy_qty": 30794.9,
                "sell_qty": 25994.9,
            },
            position_qty=4800.0,
            latest_price=0.0901,
            maker_buy_reference_price=0.09,
            maker_sell_reference_price=0.0902,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=20.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=4800.0,
        )

        self.assertEqual(filtered, desired_orders)
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "cruise")
        self.assertEqual(controls["spot_app_loss_guard"]["action"], "observe")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_deviation_qty"], 0.0)
        self.assertFalse(controls.get("buy_paused", False))
        self.assertNotIn("spot_app_loss_guard_recovery_reduce_only", controls.get("pause_reasons") or [])

    def test_spot_app_loss_recovery_treats_neutral_dust_as_cruise(self) -> None:
        controls = {
            "actual_base_qty": 4799.6,
            "neutral_base_qty": 4800.0,
            "synthetic_net_qty": 0.0,
            "synthetic_dust_qty": -0.4,
            "spot_freeze_tolerance_qty": 0.2,
        }
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 0.0931, "qty": 118.0},
            {"side": "SELL", "role": "grid_exit", "price": 0.0933, "qty": 118.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2787.96262,
                "sell_notional": 2360.53094,
                "buy_qty": 31473.0,
                "sell_qty": 26673.1,
            },
            position_qty=0.0,
            latest_price=0.0932,
            maker_buy_reference_price=0.0931,
            maker_sell_reference_price=0.0933,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=11.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=4799.9,
        )

        self.assertEqual(filtered, desired_orders)
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "cruise")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_deviation_qty"], 0.0)
        self.assertNotIn("spot_app_loss_guard_recovery_reduce_only", controls.get("pause_reasons") or [])

    def test_spot_app_loss_guard_drops_buy_reduce_above_bid_reference(self) -> None:
        controls = {"actual_base_qty": 4121.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_exit", "price": 0.0882, "qty": 672.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2360.53094,
                "buy_qty": 30794.9,
                "sell_qty": 26673.1,
            },
            position_qty=4121.8,
            latest_price=0.0878,
            maker_reference_price=0.0878,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=20.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=4121.8,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 0.0878)
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "BUY")
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 0)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 1)

    def test_spot_app_loss_guard_keeps_buy_reduce_at_bid_reference(self) -> None:
        controls = {"actual_base_qty": 4121.8, "neutral_base_qty": 4800.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_exit", "price": 0.0878, "qty": 672.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={
                "buy_notional": 2724.6815,
                "sell_notional": 2360.53094,
                "buy_qty": 30794.9,
                "sell_qty": 26673.1,
            },
            position_qty=4121.8,
            latest_price=0.0878,
            maker_reference_price=0.0878,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=5000.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=20.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=4121.8,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 0.0878)
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 1)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_app_loss_guard_allows_short_deviation_buy_recovery_when_bid_buffer_is_too_small(self) -> None:
        controls = {"actual_base_qty": 80.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_exit", "price": 1.01, "qty": 10.0},
            {"side": "SELL", "role": "grid_exit", "price": 1.02, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1000.0, "sell_notional": 900.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=80.0,
            latest_price=1.01,
            maker_buy_reference_price=1.01,
            maker_sell_reference_price=1.02,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            min_bid_break_even_buffer_ticks=3.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 1.01)
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "blocked")
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "BUY")
        self.assertEqual(controls["spot_app_loss_guard"]["dropped_order_count"], 1)
        self.assertEqual(controls["spot_app_loss_guard"]["capped_order_count"], 1)
        self.assertIn("spot_app_loss_guard_bid_buffer_below_min", controls["pause_reasons"])

    def test_spot_app_loss_guard_drops_buy_that_would_project_bid_buffer_too_low(self) -> None:
        controls = {"actual_base_qty": 100.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.04, "qty": 100.0},
            {"side": "SELL", "role": "grid_exit", "price": 1.05, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1000.0, "sell_notional": 900.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=100.0,
            latest_price=1.04,
            maker_buy_reference_price=1.04,
            maker_sell_reference_price=1.05,
            enabled=True,
            recovery_reduce_only_enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=100.0,
            min_bid_break_even_buffer_ticks=3.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["SELL"])
        self.assertEqual(controls["spot_app_loss_guard"]["state"], "cruise")
        self.assertEqual(controls["spot_app_loss_guard"]["projected_bid_buffer_dropped_buy_count"], 1)
        self.assertIn("spot_app_loss_guard_projected_bid_buffer_below_min", controls["pause_reasons"])

    def test_spot_app_loss_guard_drops_oversized_short_recovery_buy_in_cruise(self) -> None:
        controls = {"actual_base_qty": 80.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.04, "qty": 100.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1000.0, "sell_notional": 900.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=80.0,
            latest_price=1.04,
            maker_buy_reference_price=1.04,
            maker_sell_reference_price=1.05,
            enabled=True,
            recovery_reduce_only_enabled=False,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=80.0,
            min_bid_break_even_buffer_ticks=3.0,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["projected_bid_buffer_dropped_buy_count"], 1)
        self.assertIn("spot_app_loss_guard_projected_bid_buffer_below_min", controls["pause_reasons"])

    def test_spot_app_loss_guard_keeps_short_recovery_buy_within_neutral_in_cruise(self) -> None:
        controls = {"actual_base_qty": 80.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.04, "qty": 20.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1000.0, "sell_notional": 900.0, "buy_qty": 1000.0, "sell_qty": 900.0},
            position_qty=80.0,
            latest_price=1.04,
            maker_buy_reference_price=1.04,
            maker_sell_reference_price=1.05,
            enabled=True,
            recovery_reduce_only_enabled=False,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
            available_base_free=80.0,
            min_bid_break_even_buffer_ticks=3.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "grid_entry")
        self.assertEqual(controls["spot_app_loss_guard"]["projected_bid_buffer_dropped_buy_count"], 0)

    def test_spot_app_loss_guard_injects_buy_at_buy_reference(self) -> None:
        controls = {"actual_base_qty": 80.0, "neutral_base_qty": 100.0}

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=[],
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 1000.0},
            position_qty=80.0,
            latest_price=1.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            maker_buy_reference_price=0.99,
            maker_sell_reference_price=1.01,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["price"], 0.99)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 1)

    def test_spot_app_loss_guard_does_not_inject_buy_below_exchange_min(self) -> None:
        controls = {"actual_base_qty": 99.0, "neutral_base_qty": 100.0}

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=[],
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 1000.0},
            position_qty=99.0,
            latest_price=1.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            maker_buy_reference_price=0.99,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=1000.0,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "BUY")
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_app_loss_guard_injects_maker_buy_when_short_deviation_has_no_app_long_exposure(self) -> None:
        controls = {"actual_base_qty": 80.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "SELL", "role": "grid_entry", "price": 1.0, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 1000.0},
            position_qty=80.0,
            latest_price=2.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
        )

        self.assertEqual([order["side"] for order in filtered], ["BUY"])
        self.assertEqual(filtered[0]["role"], "spot_app_loss_reduce")
        self.assertAlmostEqual(filtered[0]["qty"], 5.0)
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 1)

    def test_spot_app_loss_guard_does_not_inject_without_deviation_context(self) -> None:
        controls = {}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.0, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 860.0},
            position_qty=140.0,
            latest_price=2.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=25.0,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["reduce_side"], "SELL")
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_app_loss_guard_does_not_inject_sell_without_available_base(self) -> None:
        controls = {"actual_base_qty": 140.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "BUY", "role": "grid_entry", "price": 1.0, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 860.0},
            position_qty=140.0,
            latest_price=2.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=25.0,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=0.0,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_app_loss_guard_does_not_inject_buy_without_available_quote(self) -> None:
        controls = {"actual_base_qty": 80.0, "neutral_base_qty": 100.0}
        desired_orders = [
            {"side": "SELL", "role": "grid_entry", "price": 1.0, "qty": 10.0},
        ]

        filtered = _apply_spot_app_loss_guard_to_orders(
            desired_orders=desired_orders,
            controls=controls,
            metrics={"buy_notional": 1200.0, "sell_notional": 850.0, "buy_qty": 1000.0, "sell_qty": 1000.0},
            position_qty=80.0,
            latest_price=2.0,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=1.0,
            hard_per_10k=2.0,
            maker_reduce_notional=10.0,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_quote_free=0.0,
        )

        self.assertEqual(filtered, [])
        self.assertEqual(controls["spot_app_loss_guard"]["injected_order_count"], 0)

    def test_spot_freeze_gate_observes_non_hedge_mode_without_blocking(self) -> None:
        gate = _check_spot_freeze_hedge_gate(
            symbol="WLDUSDT",
            position_mode={"dualSidePosition": False},
            position_risk=[],
            base_hedge_qty=100.0,
            tolerance_qty=0.01,
        )

        self.assertTrue(gate["ok"])
        self.assertEqual(gate["reason"], "observed")
        self.assertEqual(gate["observations"], ["not_hedge_mode", "short_position_base_mismatch"])

    def test_spot_freeze_gate_observes_existing_long_position_with_cost(self) -> None:
        gate = _check_spot_freeze_hedge_gate(
            symbol="WLDUSDT",
            position_mode={"dualSidePosition": True},
            position_risk=[
                {
                    "symbol": "WLDUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.5",
                    "entryPrice": "8.5",
                    "markPrice": "8.0",
                    "unRealizedProfit": "-0.25",
                },
                {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100", "entryPrice": "9.5"},
            ],
            base_hedge_qty=100.0,
            tolerance_qty=0.01,
        )

        self.assertTrue(gate["ok"])
        self.assertEqual(gate["reason"], "observed")
        self.assertEqual(gate["observations"], ["long_position_not_zero"])
        self.assertEqual(gate["contract_long_qty"], 0.5)
        self.assertEqual(gate["contract_long_entry_price"], 8.5)
        self.assertEqual(gate["contract_short_entry_price"], 9.5)

    def test_spot_freeze_gate_observes_short_position_not_matching_base_hedge(self) -> None:
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

        self.assertTrue(gate["ok"])
        self.assertEqual(gate["reason"], "observed")
        self.assertEqual(gate["observations"], ["short_position_base_mismatch"])
        self.assertEqual(gate["short_drift_qty"], -5.0)

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
        self.assertEqual(controls["spot_freeze_skip_reason"], "disabled")
        self.assertAlmostEqual(controls["spot_freeze_deviation_qty"], 20.0)
        self.assertAlmostEqual(controls["spot_freeze_deviation_notional"], 200.0)

    def test_spot_freeze_enabled_calls_cycle_with_short_position_qty(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-market-execution-enabled",
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
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

    def test_spot_freeze_position_fetch_error_fails_closed_as_hedge_gate_failed(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-market-execution-enabled",
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

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", side_effect=RuntimeError("timeout")):
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

        self.assertEqual(controls["spot_freeze_skip_reason"], "hedge_gate_failed")
        self.assertEqual(controls["spot_freeze_alerts"], ["futures_position_fetch_failed"])
        self.assertFalse(controls["spot_freeze_gate"]["ok"])
        self.assertEqual(controls["spot_freeze_gate"]["reason"], "futures_position_fetch_failed")

    def test_spot_freeze_dry_run_overrides_apply_for_safe_diagnostics(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-dry-run",
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={
                            "ledger": new_ledger(),
                            "actions": [{"type": "freeze_dry_run", "side": "long", "qty": 10.0}],
                            "reconcile_ok": True,
                            "alerts": [],
                        },
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

        self.assertTrue(mock_cycle.call_args.kwargs["dry_run"])
        self.assertTrue(controls["spot_freeze_dry_run"])
        self.assertEqual(controls["spot_freeze_actions"][0]["type"], "freeze_dry_run")

    def test_spot_freeze_short_apply_without_market_execution_stays_diagnostic_only(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
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
            "actual_base_qty": 80.0,
            "neutral_base_qty": 100.0,
            "_runtime": {
                "recovery_mode": "live",
                "synthetic_cost_unknown": False,
                "position_lots": [{"lot_id": "S1", "side": "short", "qty": 20.0, "entry_price": 8.0}],
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch("grid_optimizer.spot_loop_runner.freeze_cycle", side_effect=AssertionError("market freeze should be gated")):
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

        self.assertFalse(controls["spot_freeze_dry_run"])
        self.assertFalse(controls["spot_freeze_market_execution_enabled"])
        self.assertEqual(controls["spot_freeze_skip_reason"], "market_execution_disabled")
        self.assertEqual(controls["spot_freeze_alerts"], ["market_execution_disabled"])
        self.assertEqual(controls["spot_freeze_actions"], [])

    def test_spot_freeze_market_execution_requires_explicit_flag(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-market-execution-enabled",
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
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

        self.assertFalse(mock_cycle.call_args.kwargs["dry_run"])
        self.assertTrue(controls["spot_freeze_market_execution_enabled"])
        self.assertEqual(controls["spot_freeze_actions"], [{"type": "freeze"}])

    def test_spot_freeze_long_ledger_only_runs_without_market_execution_flag(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
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
        result_ledger = new_ledger()
        result_ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0}]
        result_ledger["frozen_long_qty"] = 10.0
        state = {"spot_frozen_ledger": ledger}
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={
                            "ledger": result_ledger,
                            "actions": [{"type": "freeze", "side": "long", "qty": 10.0, "mode": "spot_only"}],
                            "reconcile_ok": True,
                            "alerts": [],
                        },
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

        self.assertFalse(controls["spot_freeze_market_execution_enabled"])
        self.assertFalse(mock_cycle.call_args.kwargs["dry_run"])
        self.assertEqual(controls["spot_freeze_actions"], [{"type": "freeze", "side": "long", "qty": 10.0, "mode": "spot_only"}])
        self.assertEqual(controls["spot_freeze_skip_reason"], "frozen")
        self.assertEqual(state["spot_frozen_ledger"]["frozen_long_qty"], 10.0)

    def test_spot_freeze_long_maker_execution_uses_ledger_only_without_market_cycle(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
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

        result_ledger = new_ledger()
        result_ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0}]
        result_ledger["frozen_long_qty"] = 10.0

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={
                            "ledger": result_ledger,
                            "actions": [{"type": "freeze", "side": "long", "qty": 10.0, "mode": "spot_only"}],
                            "reconcile_ok": True,
                            "alerts": [],
                        },
                    ) as mock_cycle:
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="WLDUSDT",
                            bid_price=9.99,
                            ask_price=10.01,
                            mid_price=10.0,
                            symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                        )

        self.assertTrue(controls["spot_freeze_maker_execution_enabled"])
        self.assertFalse(mock_cycle.call_args.kwargs["dry_run"])
        self.assertEqual(controls["spot_freeze_skip_reason"], "frozen")
        self.assertEqual(controls["spot_freeze_maker_orders"], [])
        self.assertEqual(controls["spot_freeze_actions"], [{"type": "freeze", "side": "long", "qty": 10.0, "mode": "spot_only"}])

    def test_spot_freeze_long_ledger_only_ignores_existing_maker_order(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
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
                "position_lots": [{"lot_id": "L1", "side": "long", "qty": 20.0, "entry_price": 12.0}],
            },
        }

        result_ledger = new_ledger()
        result_ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0}]
        result_ledger["frozen_long_qty"] = 10.0

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={
                            "ledger": result_ledger,
                            "actions": [{"type": "freeze", "side": "long", "qty": 10.0, "mode": "spot_only"}],
                            "reconcile_ok": True,
                            "alerts": [],
                        },
                    ):
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="WLDUSDT",
                            bid_price=9.99,
                            ask_price=10.01,
                            mid_price=10.0,
                            symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                            existing_maker_orders=[{"orderId": 7001, "side": "SELL", "price": "10.01", "origQty": "10"}],
                        )

        self.assertEqual(controls["spot_freeze_skip_reason"], "frozen")
        self.assertEqual(controls["spot_freeze_maker_orders"], [])

    def test_spot_freeze_long_maker_respects_side_total_cap(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "100",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-min-loss-ratio",
                "0.01",
                "--spot-freeze-max-per-cycle-notional",
                "100",
                "--spot-freeze-long-total-cap-notional",
                "30",
                "--spot-freeze-short-total-cap-notional",
                "500",
            ]
        )
        ledger = new_ledger()
        ledger["long_lots"] = [{"lot_id": "L1", "qty": 4.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0}]
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
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    _maybe_run_spot_freeze(
                        args=args,
                        state=state,
                        controls=controls,
                        symbol="WLDUSDT",
                        bid_price=9.99,
                        ask_price=10.01,
                        mid_price=10.0,
                        symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                        api_key="key",
                        api_secret="secret",
                        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                    )

        self.assertEqual(controls["spot_freeze_skip_reason"], "long_total_cap_reached")
        self.assertEqual(controls["spot_freeze_maker_orders"], [])

    def test_spot_freeze_long_ledger_only_does_not_require_available_short_hedge(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "0",
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
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "0"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    _maybe_run_spot_freeze(
                        args=args,
                        state=state,
                        controls=controls,
                        symbol="WLDUSDT",
                        bid_price=9.99,
                        ask_price=10.01,
                        mid_price=10.0,
                        symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                        api_key="key",
                        api_secret="secret",
                        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                    )

        filtered = _apply_spot_freeze_runtime_hedge_block(
            strategy_mode="spot_competition_synthetic_neutral_grid",
            desired_orders=[
                {"side": "BUY", "price": 9.99, "qty": 1.0},
                {"side": "SELL", "price": 10.01, "qty": 1.0},
            ],
            controls=controls,
        )

        self.assertEqual(controls["spot_freeze_skip_reason"], "frozen")
        self.assertEqual(controls["spot_freeze_maker_orders"], [])
        self.assertEqual(
            filtered,
            [
                {"side": "BUY", "price": 9.99, "qty": 1.0},
                {"side": "SELL", "price": 10.01, "qty": 1.0},
            ],
        )
        self.assertFalse(controls["spot_freeze_runtime_blocked"])

    def test_spot_freeze_maker_execution_builds_short_side_buy_order(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
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
                "--max-short-position-notional",
                "2000",
            ]
        )
        state = {"spot_frozen_ledger": new_ledger()}
        controls = {
            "actual_base_qty": 80.0,
            "neutral_base_qty": 100.0,
            "_runtime": {
                "recovery_mode": "live",
                "position_lots": [{"lot_id": "S1", "side": "short", "qty": 20.0, "entry_price": 8.0}],
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch("grid_optimizer.spot_loop_runner.freeze_cycle", side_effect=AssertionError("maker freeze must not use market cycle")):
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="WLDUSDT",
                            bid_price=9.99,
                            ask_price=10.01,
                            mid_price=10.0,
                            symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                        )

        self.assertEqual(
            controls["spot_freeze_maker_orders"],
            [{"side": "BUY", "price": 9.99, "qty": 10.0, "role": "spot_freeze_maker", "tag": "spot_freeze_short"}],
        )

    def test_spot_freeze_short_maker_cap_counts_frozen_short_not_base_hedge(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "4800",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-min-loss-ratio",
                "0.0",
                "--spot-freeze-max-per-cycle-notional",
                "60",
                "--spot-freeze-total-cap-notional",
                "240",
                "--max-short-position-notional",
                "240",
            ]
        )
        state = {"spot_frozen_ledger": new_ledger()}
        controls = {
            "actual_base_qty": 4121.8,
            "neutral_base_qty": 4800.0,
            "_runtime": {
                "recovery_mode": "live",
                "position_lots": [{"lot_id": "S1", "side": "short", "qty": 678.2, "entry_price": 0.0881}],
            },
        }

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "XPLUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "XPLUSDT", "positionSide": "SHORT", "positionAmt": "-4800"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.1}):
                    with patch("grid_optimizer.spot_loop_runner.freeze_cycle", side_effect=AssertionError("maker freeze must not use market cycle")):
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="XPLUSDT",
                            bid_price=0.0887,
                            ask_price=0.0888,
                            mid_price=0.08875,
                            symbol_info={"tick_size": 0.0001, "step_size": 0.1, "min_qty": 0.1, "min_notional": 5.0},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 24, tzinfo=timezone.utc),
                        )

        self.assertEqual(controls["spot_freeze_skip_reason"], "maker_order_pending")
        self.assertEqual(
            controls["spot_freeze_maker_orders"],
            [{"side": "BUY", "price": 0.0887, "qty": 676.0, "role": "spot_freeze_maker", "tag": "spot_freeze_short"}],
        )

    def test_spot_freeze_short_maker_cap_blocks_when_frozen_short_is_full(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "4800",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-min-loss-ratio",
                "0.0",
                "--spot-freeze-max-per-cycle-notional",
                "60",
                "--spot-freeze-total-cap-notional",
                "1000",
                "--max-short-position-notional",
                "240",
            ]
        )
        ledger = new_ledger()
        ledger["short_lots"] = [
            {"lot_id": "S-full", "qty": 2704.3, "cost_price": 0.08875, "frozen_at": "t", "frozen_mid": 0.08875}
        ]
        state = {"spot_frozen_ledger": ledger}
        controls = {
            "actual_base_qty": 4121.8,
            "neutral_base_qty": 4800.0,
            "_runtime": {
                "recovery_mode": "live",
                "position_lots": [{"lot_id": "S1", "side": "short", "qty": 678.2, "entry_price": 0.0881}],
            },
        }

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "XPLUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "XPLUSDT", "positionSide": "SHORT", "positionAmt": "-7504.3"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.1}):
                    with patch("grid_optimizer.spot_loop_runner.freeze_cycle", side_effect=AssertionError("maker freeze must not use market cycle")):
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="XPLUSDT",
                            bid_price=0.0887,
                            ask_price=0.0888,
                            mid_price=0.08875,
                            symbol_info={"tick_size": 0.0001, "step_size": 0.1, "min_qty": 0.1, "min_notional": 5.0},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 24, tzinfo=timezone.utc),
                        )

        self.assertEqual(controls["spot_freeze_skip_reason"], "short_hedge_capacity_exhausted")
        self.assertEqual(controls["spot_freeze_maker_orders"], [])

    def test_spot_freeze_short_maker_respects_side_total_cap(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "4800",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-min-loss-ratio",
                "0.0",
                "--spot-freeze-max-per-cycle-notional",
                "60",
                "--spot-freeze-long-total-cap-notional",
                "30",
                "--spot-freeze-short-total-cap-notional",
                "500",
                "--max-short-position-notional",
                "1000",
            ]
        )
        ledger = new_ledger()
        ledger["long_lots"] = [
            {"lot_id": "L-over", "qty": 1350.0, "cost_price": 0.08875, "frozen_at": "t", "frozen_mid": 0.08875}
        ]
        ledger["short_lots"] = [
            {"lot_id": "S-full", "qty": 5633.9, "cost_price": 0.08875, "frozen_at": "t", "frozen_mid": 0.08875}
        ]
        state = {"spot_frozen_ledger": ledger}
        controls = {
            "actual_base_qty": 4121.8,
            "neutral_base_qty": 4800.0,
            "_runtime": {
                "recovery_mode": "live",
                "position_lots": [{"lot_id": "S1", "side": "short", "qty": 678.2, "entry_price": 0.0881}],
            },
        }

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "XPLUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "XPLUSDT", "positionSide": "SHORT", "positionAmt": "-9083.9"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.1}):
                    _maybe_run_spot_freeze(
                        args=args,
                        state=state,
                        controls=controls,
                        symbol="XPLUSDT",
                        bid_price=0.0887,
                        ask_price=0.0888,
                        mid_price=0.08875,
                        symbol_info={"tick_size": 0.0001, "step_size": 0.1, "min_qty": 0.1, "min_notional": 5.0},
                        api_key="key",
                        api_secret="secret",
                        now=datetime(2026, 6, 24, tzinfo=timezone.utc),
                    )

        self.assertEqual(controls["spot_freeze_skip_reason"], "short_total_cap_reached")
        self.assertEqual(controls["spot_freeze_maker_orders"], [])

    def test_spot_freeze_repairs_pending_contract_when_market_execution_disabled(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "100",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-max-per-cycle-notional",
                "100",
                "--spot-freeze-total-cap-notional",
                "500",
            ]
        )
        ledger = new_ledger()
        ledger["long_lots"] = [
            {"lot_id": "spot_freeze_maker_1", "qty": 10.0, "cost_price": 10.0, "hedge_pending": True}
        ]
        ledger["pending_contract_actions"] = [
            {"side": "BUY", "position_side": "SHORT", "qty": 10.0, "reason": "freeze_long_hedge", "lot_id": "spot_freeze_maker_1"}
        ]
        state = {"spot_frozen_ledger": ledger}
        repaired = new_ledger()
        repaired["long_lots"] = [
            {"lot_id": "spot_freeze_maker_1", "qty": 10.0, "cost_price": 10.0, "hedge_pending": False}
        ]
        controls = {"actual_base_qty": 100.0, "neutral_base_qty": 100.0, "_runtime": {"recovery_mode": "live"}}

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={
                            "ledger": repaired,
                            "actions": [{"type": "pending_repaired", "side": "BUY", "qty": 10.0}],
                            "reconcile_ok": True,
                            "alerts": [],
                        },
                    ) as mock_cycle:
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="WLDUSDT",
                            bid_price=9.99,
                            ask_price=10.01,
                            mid_price=10.0,
                            symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                        )

        self.assertFalse(mock_cycle.call_args.kwargs["config"].enabled)
        self.assertFalse(mock_cycle.call_args.kwargs["dry_run"])
        self.assertEqual(controls["spot_freeze_actions"][0]["type"], "pending_repaired")
        self.assertEqual(controls["spot_freeze_pending_contract_actions"], [])

    def test_spot_freeze_pending_contract_dry_run_fails_closed(self) -> None:
        args = self._synthetic_args(
            [
                "--apply",
                "--spot-freeze-enabled",
                "--spot-freeze-dry-run",
                "--spot-freeze-maker-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "100",
                "--spot-freeze-deviation-notional",
                "50",
                "--spot-freeze-max-per-cycle-notional",
                "100",
                "--spot-freeze-total-cap-notional",
                "500",
            ]
        )
        ledger = new_ledger()
        ledger["long_lots"] = [
            {"lot_id": "spot_freeze_maker_1", "qty": 10.0, "cost_price": 10.0, "hedge_pending": True}
        ]
        ledger["pending_contract_actions"] = [
            {"side": "BUY", "position_side": "SHORT", "qty": 10.0, "reason": "freeze_long_hedge", "lot_id": "spot_freeze_maker_1"}
        ]
        state = {"spot_frozen_ledger": ledger}
        controls = {"actual_base_qty": 100.0, "neutral_base_qty": 100.0, "_runtime": {"recovery_mode": "live"}}

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "WLDUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    _maybe_run_spot_freeze(
                        args=args,
                        state=state,
                        controls=controls,
                        symbol="WLDUSDT",
                        bid_price=9.99,
                        ask_price=10.01,
                        mid_price=10.0,
                        symbol_info={"tick_size": 0.01, "step_size": 0.001, "min_qty": 0.001, "min_notional": 5.0},
                        api_key="key",
                        api_secret="secret",
                        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                    )

        desired_orders = [
            {"side": "BUY", "price": 9.99, "qty": 1.0},
            {"side": "SELL", "price": 10.01, "qty": 1.0},
        ]
        filtered = _apply_spot_freeze_runtime_hedge_block(
            strategy_mode="spot_competition_synthetic_neutral_grid",
            desired_orders=desired_orders,
            controls=controls,
        )

        self.assertEqual(controls["spot_freeze_skip_reason"], "pending_contract_unrepaired")
        self.assertEqual(controls["spot_freeze_actions"][0]["type"], "pending_repair_dry_run")
        self.assertEqual(len(controls["spot_freeze_pending_contract_actions"]), 1)
        self.assertEqual(filtered, [])
        self.assertTrue(controls["spot_freeze_runtime_blocked"])
        self.assertEqual(controls["risk_state"], "spot_freeze_pending_contract_unrepaired")

    def test_spot_freeze_uses_contract_qty_step_when_coarser_than_spot(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-market-execution-enabled",
                "--spot-freeze-base-hedge-qty",
                "0",
                "--spot-freeze-deviation-notional",
                "5",
                "--spot-freeze-min-loss-ratio",
                "0.0001",
                "--spot-freeze-max-per-cycle-notional",
                "25",
                "--spot-freeze-total-cap-notional",
                "110",
            ]
        )
        state = {"spot_frozen_ledger": new_ledger()}
        controls = {
            "actual_base_qty": 560.8,
            "neutral_base_qty": 1000.0,
            "_runtime": {
                "recovery_mode": "live",
                "position_lots": [{"lot_id": "S1", "side": "short", "qty": 439.2, "entry_price": 10.0}],
            },
        }

        with patch("grid_optimizer.spot_loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}):
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_futures_position_risk_v3",
                return_value=[
                    {"symbol": "MEGAUSDT", "positionSide": "LONG", "positionAmt": "0"},
                    {"symbol": "MEGAUSDT", "positionSide": "SHORT", "positionAmt": "0"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 1.0}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={"ledger": new_ledger(), "actions": [], "reconcile_ok": True, "alerts": []},
                    ) as mock_cycle:
                        _maybe_run_spot_freeze(
                            args=args,
                            state=state,
                            controls=controls,
                            symbol="MEGAUSDT",
                            mid_price=10.0,
                            symbol_info={"step_size": 0.1},
                            api_key="key",
                            api_secret="secret",
                            now=datetime(2026, 6, 23, tzinfo=timezone.utc),
                        )

        self.assertEqual(mock_cycle.call_args.kwargs["qty_step"], 1.0)

    def test_spot_freeze_enabled_accepts_ledger_adjusted_short_position_qty(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-market-execution-enabled",
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
                    {"symbol": "WLDUSDT", "positionSide": "SHORT", "positionAmt": "-100"},
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
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
        self.assertEqual(kwargs["contract_short_qty"], 100.0)
        self.assertEqual(kwargs["base_hedge_qty"], 100.0)
        self.assertEqual(controls["spot_freeze_gate"]["reason"], "ok")

    def test_spot_freeze_uses_aligned_app_loss_cost_when_runtime_lots_unknown(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-dry-run",
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
            "spot_app_loss_guard": {
                "window_source": "metrics",
                "window_aligned": True,
                "buy_notional": 1200.0,
                "sell_notional": 960.0,
                "buy_qty": 120.0,
                "sell_qty": 100.0,
                "position_qty": 20.0,
            },
            "_runtime": {
                "recovery_mode": "live",
                "synthetic_cost_unknown": True,
                "position_lots": [{"lot_id": "L1", "side": "long", "qty": 20.0, "entry_price": 10.0}],
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={
                            "ledger": new_ledger(),
                            "actions": [{"type": "freeze_dry_run", "side": "long", "qty": 10.0}],
                            "reconcile_ok": True,
                            "alerts": [],
                        },
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
        self.assertTrue(kwargs["loss_ratio_usable"])
        self.assertAlmostEqual(kwargs["deviation_loss_ratio"], (12.0 - 10.0) / 12.0)
        self.assertAlmostEqual(controls["spot_freeze_deviation_cost_avg"], 12.0)
        self.assertEqual(controls["spot_freeze_deviation_loss_reason"], "app_loss_guard")
        self.assertEqual(controls["spot_freeze_deviation_loss_source"], "app_loss_guard")

    def test_spot_freeze_rejects_recent_trade_app_loss_cost_for_freeze(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-dry-run",
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
            "spot_app_loss_guard": {
                "window_source": "recent_trades",
                "window_aligned": True,
                "buy_notional": 1200.0,
                "sell_notional": 960.0,
                "buy_qty": 120.0,
                "sell_qty": 100.0,
                "position_qty": 20.0,
            },
            "_runtime": {
                "recovery_mode": "live",
                "synthetic_cost_unknown": True,
                "position_lots": [],
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={"ledger": new_ledger(), "actions": [], "reconcile_ok": True, "alerts": ["cost_not_usable"]},
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
        self.assertFalse(kwargs["loss_ratio_usable"])
        self.assertEqual(controls["spot_freeze_deviation_loss_reason"], "synthetic_cost_unknown")
        self.assertEqual(controls["spot_freeze_deviation_loss_source"], "runtime")

    def test_sync_synthetic_neutral_spot_freeze_maker_fill_records_pending_contract(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {
                "7001": {
                    "side": "SELL",
                    "role": "spot_freeze_maker",
                    "tag": "spot_freeze_long",
                    "created_at_ms": 1,
                }
            },
            "spot_frozen_ledger": new_ledger(),
        }

        applied = _sync_synthetic_neutral_trades(
            state=state,
            trades=[
                {
                    "id": 9001,
                    "orderId": 7001,
                    "time": 1782306000000,
                    "price": "10.0",
                    "qty": "3.5",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "isMaker": True,
                }
            ],
            base_asset="WLD",
            quote_asset="USDT",
        )

        ledger = state["spot_frozen_ledger"]
        self.assertEqual(applied, 1)
        self.assertEqual(ledger["long_lots"][0]["qty"], 3.5)
        self.assertEqual(ledger["long_lots"][0]["cost_price"], 10.0)
        self.assertFalse(ledger["long_lots"][0]["hedge_pending"])
        self.assertEqual(ledger["pending_contract_actions"], [])
        self.assertEqual(ledger["frozen_long_qty"], 3.5)

    def test_sync_synthetic_neutral_spot_freeze_maker_buy_fill_records_short_pending_contract(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {
                "7002": {
                    "side": "BUY",
                    "role": "spot_freeze_maker",
                    "tag": "spot_freeze_short",
                    "created_at_ms": 1,
                }
            },
            "spot_frozen_ledger": new_ledger(),
        }

        applied = _sync_synthetic_neutral_trades(
            state=state,
            trades=[
                {
                    "id": 9002,
                    "orderId": 7002,
                    "time": 1782306000000,
                    "price": "10.0",
                    "qty": "3.5",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "isMaker": True,
                }
            ],
            base_asset="WLD",
            quote_asset="USDT",
        )

        ledger = state["spot_frozen_ledger"]
        self.assertEqual(applied, 1)
        self.assertEqual(ledger["short_lots"][0]["qty"], 3.5)
        self.assertEqual(
            ledger["pending_contract_actions"],
            [
                {
                    "side": "SELL",
                    "position_side": "SHORT",
                    "qty": 3.5,
                    "reason": "freeze_short_hedge",
                    "lot_id": "spot_freeze_maker_9002",
                }
            ],
        )
        self.assertEqual(ledger["frozen_short_qty"], 3.5)

    def test_spot_freeze_open_maker_orders_recovers_known_order_from_client_id(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "spot_frozen_ledger": new_ledger(),
        }

        matched = _spot_freeze_open_maker_orders(
            [
                {
                    "orderId": 7002,
                    "clientOrderId": "sgxpl114_spot_freeze_short_b_123456",
                    "side": "BUY",
                    "price": "0.0887",
                    "origQty": "676.0",
                }
            ],
            state,
        )

        self.assertEqual(len(matched), 1)
        self.assertEqual(
            state["known_orders"]["7002"],
            {
                "cell_idx": 0,
                "side": "BUY",
                "client_order_id": "sgxpl114_spot_freeze_short_b_123456",
                "created_at_ms": 0,
                "role": "spot_freeze_maker",
                "tag": "spot_freeze_short",
            },
        )

    def test_spot_freeze_open_maker_orders_recovers_truncated_client_ids(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "spot_frozen_ledger": new_ledger(),
        }

        with patch("grid_optimizer.spot_loop_runner.time.time", return_value=1782349765.432):
            short_client_order_id = _build_client_order_id("sgxpl114", "spot_freeze_short", "BUY")
            long_client_order_id = _build_client_order_id("sgxpl114", "spot_freeze_long", "SELL")

        matched = _spot_freeze_open_maker_orders(
            [
                {
                    "orderId": 7004,
                    "clientOrderId": short_client_order_id,
                    "side": "BUY",
                    "price": "0.0887",
                    "origQty": "676.0",
                },
                {
                    "orderId": 7005,
                    "clientOrderId": long_client_order_id,
                    "side": "SELL",
                    "price": "0.0889",
                    "origQty": "676.0",
                },
            ],
            state,
        )

        self.assertEqual(len(matched), 2)
        self.assertEqual(state["known_orders"]["7004"]["tag"], "spot_freeze_short")
        self.assertEqual(state["known_orders"]["7005"]["tag"], "spot_freeze_long")

    def test_spot_freeze_open_maker_orders_does_not_default_unknown_direction_to_long(self) -> None:
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {},
            "spot_frozen_ledger": new_ledger(),
        }

        matched = _spot_freeze_open_maker_orders(
            [
                {
                    "orderId": 7003,
                    "clientOrderId": "sgxpl114_spot_freeze_maker_x_123456",
                    "side": "",
                    "price": "0.0887",
                    "origQty": "676.0",
                }
            ],
            state,
        )

        self.assertEqual(len(matched), 1)
        self.assertEqual(state["known_orders"], {})

    def test_sync_synthetic_neutral_spot_freeze_maker_fill_is_ledger_idempotent(self) -> None:
        ledger = new_ledger()
        ledger["long_lots"] = [{"lot_id": "spot_freeze_maker_9001", "qty": 3.5, "cost_price": 10.0}]
        ledger["pending_contract_actions"] = [
            {"side": "BUY", "position_side": "SHORT", "qty": 3.5, "reason": "freeze_long_hedge", "lot_id": "spot_freeze_maker_9001"}
        ]
        state = {
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "known_orders": {"7001": {"side": "SELL", "role": "spot_freeze_maker", "tag": "spot_freeze_long"}},
            "spot_frozen_ledger": ledger,
        }

        _sync_synthetic_neutral_trades(
            state=state,
            trades=[
                {
                    "id": 9001,
                    "orderId": 7001,
                    "time": 1782306000000,
                    "price": "10.0",
                    "qty": "3.5",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "isMaker": True,
                }
            ],
            base_asset="WLD",
            quote_asset="USDT",
        )

        self.assertEqual(len(state["spot_frozen_ledger"]["long_lots"]), 1)
        self.assertEqual(len(state["spot_frozen_ledger"]["pending_contract_actions"]), 1)

    def test_spot_freeze_maker_order_suppresses_same_side_app_loss_reduce_order(self) -> None:
        controls: dict[str, object] = {}

        filtered = _apply_spot_freeze_maker_orders_to_desired_orders(
            desired_orders=[
                {"side": "SELL", "price": 10.05, "qty": 3.0, "role": "spot_app_loss_reduce", "tag": "app_loss_reduce"},
                {"side": "BUY", "price": 9.95, "qty": 2.0, "role": "grid_entry", "tag": "grid"},
            ],
            maker_orders=[
                {"side": "SELL", "price": 10.01, "qty": 5.0, "role": "spot_freeze_maker", "tag": "spot_freeze_long"}
            ],
            controls=controls,
        )

        self.assertEqual([order["role"] for order in filtered], ["grid_entry", "spot_freeze_maker"])
        self.assertEqual(controls["spot_freeze_suppressed_app_loss_reduce_orders"], 1)

    def test_spot_app_loss_guard_retags_kept_reduce_order_before_freeze_priority(self) -> None:
        controls = {"actual_base_qty": 110.0, "neutral_base_qty": 100.0}
        guarded = _apply_spot_app_loss_guard_to_orders(
            desired_orders=[
                {"side": "SELL", "price": 10.10, "qty": 5.0, "role": "bootstrap_entry", "tag": "sell1"},
                {"side": "BUY", "price": 9.90, "qty": 5.0, "role": "grid_entry", "tag": "buy1"},
            ],
            controls=controls,
            metrics={"buy_notional": 2000.0, "sell_notional": 900.0, "buy_qty": 200.0, "sell_qty": 90.0},
            position_qty=110.0,
            latest_price=9.98,
            enabled=True,
            min_notional=10.0,
            soft_per_10k=0.6,
            hard_per_10k=1.0,
            maker_reduce_notional=60.0,
            maker_reference_price=10.0,
            tick_size=0.01,
            step_size=0.1,
            min_qty=0.1,
            exchange_min_notional=5.0,
            available_base_free=110.0,
        )

        self.assertEqual([order["role"] for order in guarded], ["spot_app_loss_reduce"])

        filtered = _apply_spot_freeze_maker_orders_to_desired_orders(
            desired_orders=guarded,
            maker_orders=[
                {"side": "SELL", "price": 10.00, "qty": 5.0, "role": "spot_freeze_maker", "tag": "spot_freeze_long"}
            ],
            controls=controls,
        )

        self.assertEqual([order["role"] for order in filtered], ["spot_freeze_maker"])
        self.assertEqual(controls["spot_freeze_suppressed_app_loss_reduce_orders"], 1)

    def test_spot_freeze_runtime_hedge_block_keeps_orders_when_gate_failed_observation(self) -> None:
        controls = {
            "spot_freeze_enabled": True,
            "spot_freeze_skip_reason": "hedge_gate_failed",
            "pause_reasons": ["existing_reason"],
        }
        desired_orders = [
            {"side": "BUY", "price": 10.0, "qty": 1.0},
            {"side": "SELL", "price": 10.1, "qty": 1.0},
        ]

        filtered = _apply_spot_freeze_runtime_hedge_block(
            strategy_mode="spot_competition_synthetic_neutral_grid",
            desired_orders=desired_orders,
            controls=controls,
        )

        self.assertEqual(filtered, desired_orders)
        self.assertFalse(controls["spot_freeze_runtime_blocked"])
        self.assertNotIn("buy_paused", controls)
        self.assertEqual(controls["pause_reasons"], ["existing_reason"])

    def test_spot_freeze_runtime_block_keeps_orders_when_capacity_is_exhausted(self) -> None:
        for skip_reason in (
            "total_cap_reached",
            "long_total_cap_reached",
            "short_total_cap_reached",
            "short_hedge_capacity_exhausted",
            "insufficient_short_hedge_to_freeze_long",
        ):
            with self.subTest(skip_reason=skip_reason):
                controls = {
                    "spot_freeze_enabled": True,
                    "spot_freeze_skip_reason": skip_reason,
                    "pause_reasons": ["existing_reason"],
                }
                desired_orders = [
                    {"side": "BUY", "price": 10.0, "qty": 1.0},
                    {"side": "SELL", "price": 10.1, "qty": 1.0},
                ]

                filtered = _apply_spot_freeze_runtime_hedge_block(
                    strategy_mode="spot_competition_synthetic_neutral_grid",
                    desired_orders=desired_orders,
                    controls=controls,
                )

                self.assertEqual(filtered, desired_orders)
                self.assertFalse(controls["spot_freeze_runtime_blocked"])
                self.assertNotIn("buy_paused", controls)
                self.assertEqual(controls["pause_reasons"], ["existing_reason"])

    def test_spot_freeze_runtime_hedge_block_keeps_orders_when_gate_ok(self) -> None:
        controls = {
            "spot_freeze_enabled": True,
            "spot_freeze_skip_reason": "eligible",
        }
        desired_orders = [{"side": "SELL", "price": 10.1, "qty": 1.0}]

        filtered = _apply_spot_freeze_runtime_hedge_block(
            strategy_mode="spot_competition_synthetic_neutral_grid",
            desired_orders=desired_orders,
            controls=controls,
        )

        self.assertEqual(filtered, desired_orders)
        self.assertFalse(controls["spot_freeze_runtime_blocked"])

    def test_spot_freeze_marks_huge_deviation_threshold_as_effectively_disabled(self) -> None:
        args = self._synthetic_args(
            [
                "--spot-freeze-enabled",
                "--spot-freeze-dry-run",
                "--spot-freeze-base-hedge-qty",
                "100",
                "--spot-freeze-deviation-notional",
                "1000000000",
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
                with patch("grid_optimizer.spot_loop_runner.fetch_futures_symbol_config", return_value={"step_size": 0.001}):
                    with patch(
                        "grid_optimizer.spot_loop_runner.freeze_cycle",
                        return_value={"ledger": new_ledger(), "actions": [], "reconcile_ok": True, "alerts": []},
                    ):
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

        self.assertFalse(controls["spot_freeze_effective_enabled"])
        self.assertEqual(controls["spot_freeze_config_reason"], "deviation_threshold_disables")
        self.assertEqual(controls["spot_freeze_skip_reason"], "below_deviation_threshold")

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

    def test_build_spot_competition_synthetic_neutral_grid_allows_small_profitable_sell_below_base(self) -> None:
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
            threshold_reduce_target_notional=10.0,
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
            spot_freeze_tolerance_qty=0.2,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY", "SELL"])
        self.assertLess(desired_orders[0]["price"], 0.099)
        self.assertLess(desired_orders[1]["price"], desired_orders[0]["price"])
        self.assertGreater(desired_orders[2]["price"], 0.101)
        self.assertEqual(controls["mode"], "competition_synthetic_neutral_grid")
        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["neutral_base_qty"], 1000.0)
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertAlmostEqual(controls["current_long_notional"], 0.0)
        self.assertAlmostEqual(controls["current_short_notional"], 0.0)
        self.assertAlmostEqual(controls["max_short_position_notional"], 80.0)
        self.assertAlmostEqual(controls["spot_freeze_tolerance_qty"], 0.2)
        self.assertTrue(controls["synthetic_base_sell_floor"]["active"])
        self.assertEqual(controls["synthetic_base_sell_floor"]["dropped_sell_orders"], 1)
        self.assertAlmostEqual(controls["synthetic_base_sell_floor"]["allowed_short_notional"], 10.0)
        self.assertAlmostEqual(controls["synthetic_base_sell_floor"]["floor_qty"], 900.0)

    def test_build_spot_competition_synthetic_neutral_grid_caps_sell_to_allowed_short_floor(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=2.0,
            buy_levels=1,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=100.0,
            threshold_position_notional=200.0,
            threshold_reduce_target_notional=50.0,
            max_order_position_notional=300.0,
            max_position_notional=500.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1.0,
            max_short_position_notional=200.0,
            actual_base_qty=2.0,
        )

        sell_orders = [order for order in desired_orders if order["side"] == "SELL"]
        self.assertTrue(sell_orders)
        self.assertLessEqual(sum(order["qty"] for order in sell_orders), 1.5 + 1e-12)
        self.assertAlmostEqual(controls["synthetic_base_sell_floor"]["floor_qty"], 0.5)
        self.assertAlmostEqual(controls["synthetic_base_sell_floor"]["available_sell_qty"], 1.5)

    def test_build_spot_competition_synthetic_neutral_rebalances_large_base_shortfall(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=0.0610,
            ask_price=0.0611,
            step_price=0.0001,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=35.0,
            threshold_reduce_target_notional=10.0,
            max_order_position_notional=80.0,
            max_position_notional=200.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=2000.0,
            max_short_position_notional=200.0,
            actual_base_qty=900.0,
            spot_freeze_tolerance_qty=1.0,
            spot_base_rebalance_soft_tolerance_qty=100.0,
            spot_base_rebalance_hard_tolerance_qty=1000.0,
            spot_base_rebalance_max_buy_levels=2,
        )

        self.assertFalse(any(order["side"] == "SELL" for order in desired_orders))
        rebalance_buys = [order for order in desired_orders if order.get("role") == "base_rebalance_buy"]
        self.assertTrue(rebalance_buys)
        self.assertTrue(controls["synthetic_base_sell_floor"]["base_rebalance_active"])
        self.assertAlmostEqual(controls["synthetic_base_sell_floor"]["base_shortfall_qty"], 1100.0)
        self.assertEqual(controls["synthetic_base_sell_floor"]["soft_tolerance_qty"], 100.0)
        self.assertEqual(controls["synthetic_base_sell_floor"]["hard_tolerance_qty"], 1000.0)

    def test_build_spot_competition_synthetic_neutral_refreshes_dust_runtime_lot(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "short_active"
        runtime["grid_anchor_price"] = 1.588
        runtime["position_lots"] = [
            {
                "lot_id": "stale-dust-short",
                "side": "short",
                "qty": 0.97,
                "entry_price": 1.588,
                "opened_at_ms": 1,
                "source_role": "bootstrap_entry",
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
            bid_price=1.596,
            ask_price=1.597,
            step_price=0.001,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=20.0,
            threshold_reduce_target_notional=0.0,
            warmup_position_notional=6.0,
            max_order_position_notional=25.0,
            max_position_notional=40.0,
            tick_size=0.001,
            step_size=0.01,
            min_qty=0.01,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=100.0,
            max_short_position_notional=40.0,
            actual_base_qty=96.23,
        )

        buy_orders = [order for order in desired_orders if order["side"] == "BUY"]
        self.assertTrue(buy_orders)
        self.assertEqual(buy_orders[0]["role"], "tail_cleanup")
        self.assertGreaterEqual(buy_orders[0]["qty"] * buy_orders[0]["price"], 5.0)
        self.assertAlmostEqual(controls["current_short_notional"], 3.77 * 1.5965)
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertAlmostEqual(cached_runtime["position_lots"][0]["qty"], 3.77)
        self.assertEqual(cached_runtime["risk_state"], "normal")
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertNotIn("synthetic_cost_unknown", cached_runtime)

    def test_build_spot_competition_synthetic_neutral_recovers_small_unknown_dust_realign(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "short_active"
        runtime["risk_state"] = "hard_reduce_only"
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["synthetic_cost_unknown"] = True
        runtime["position_lots"] = [
            {
                "lot_id": "synthetic_recovered",
                "side": "short",
                "qty": 3.77,
                "entry_price": 1.588,
                "opened_at_ms": 0,
                "source_role": "synthetic_recovery",
                "recovery_reason": "synthetic_recovered_cost_unknown",
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
            bid_price=1.598,
            ask_price=1.599,
            step_price=0.001,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=20.0,
            threshold_reduce_target_notional=0.0,
            warmup_position_notional=6.0,
            max_order_position_notional=25.0,
            max_position_notional=40.0,
            tick_size=0.001,
            step_size=0.01,
            min_qty=0.01,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=100.0,
            max_short_position_notional=40.0,
            actual_base_qty=96.23,
        )

        self.assertTrue([order for order in desired_orders if order["side"] == "BUY"])
        cached_runtime = state["spot_competition_synthetic_neutral_grid_runtime_cache"]["runtime"]
        self.assertEqual(cached_runtime["risk_state"], "normal")
        self.assertEqual(cached_runtime["recovery_mode"], "live")
        self.assertNotIn("synthetic_cost_unknown", cached_runtime)
        self.assertAlmostEqual(controls["current_short_notional"], 3.77 * 1.5985)

    def test_build_spot_competition_synthetic_neutral_reduces_buys_when_base_surplus(self) -> None:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
            },
            trades=[],
            bid_price=0.0610,
            ask_price=0.0611,
            step_price=0.0001,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=35.0,
            threshold_reduce_target_notional=10.0,
            max_order_position_notional=80.0,
            max_position_notional=200.0,
            tick_size=0.0001,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=2000.0,
            max_short_position_notional=200.0,
            actual_base_qty=2489.2,
            spot_freeze_tolerance_qty=1.0,
            spot_base_rebalance_soft_tolerance_qty=100.0,
            spot_base_rebalance_hard_tolerance_qty=1000.0,
            spot_base_rebalance_max_buy_levels=2,
        )

        self.assertEqual(sum(1 for order in desired_orders if order["side"] == "BUY"), 1)
        self.assertEqual(sum(1 for order in desired_orders if order["side"] == "SELL"), 2)
        self.assertTrue(controls["synthetic_base_sell_floor"]["base_rebalance_active"])
        self.assertEqual(controls["synthetic_base_sell_floor"]["dropped_buy_orders"], 1)

    def test_build_spot_competition_synthetic_neutral_grid_excludes_spot_frozen_long_from_active_net(self) -> None:
        ledger = new_ledger()
        ledger["long_lots"] = [{"lot_id": "freeze-long-1", "qty": 1.0}]
        ledger["frozen_long_qty"] = 1.0

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
                "spot_frozen_ledger": ledger,
            },
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=2.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=100.0,
            threshold_position_notional=200.0,
            threshold_reduce_target_notional=50.0,
            max_order_position_notional=300.0,
            max_position_notional=500.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1.0,
            max_short_position_notional=200.0,
            actual_base_qty=2.0,
        )

        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertAlmostEqual(controls["current_long_notional"], 0.0)
        self.assertAlmostEqual(controls["current_short_notional"], 0.0)
        self.assertIn("BUY", [order["side"] for order in desired_orders])
        self.assertIn("SELL", [order["side"] for order in desired_orders])

    def test_build_spot_competition_synthetic_neutral_grid_caps_stale_overfrozen_long(self) -> None:
        ledger = new_ledger()
        ledger["long_lots"] = [{"lot_id": "stale-freeze-long", "qty": 3.0}]
        ledger["frozen_long_qty"] = 3.0

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
                "spot_frozen_ledger": ledger,
            },
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=2.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=100.0,
            threshold_position_notional=200.0,
            threshold_reduce_target_notional=50.0,
            max_order_position_notional=300.0,
            max_position_notional=500.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1.0,
            max_short_position_notional=200.0,
            actual_base_qty=2.0,
        )

        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertIn("BUY", [order["side"] for order in desired_orders])
        self.assertIn("SELL", [order["side"] for order in desired_orders])

    def test_build_spot_competition_synthetic_neutral_grid_treats_small_active_net_within_reduce_target_as_flat(self) -> None:
        ledger = new_ledger()
        ledger["long_lots"] = [{"lot_id": "freeze-long-1", "qty": 0.09}]
        ledger["frozen_long_qty"] = 0.09

        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
                "spot_frozen_ledger": ledger,
            },
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=2.0,
            buy_levels=1,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=100.0,
            threshold_position_notional=200.0,
            threshold_reduce_target_notional=30.0,
            max_order_position_notional=300.0,
            max_position_notional=500.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1.0,
            max_short_position_notional=200.0,
            actual_base_qty=1.2,
        )

        self.assertEqual(controls["direction_state"], "flat")
        self.assertAlmostEqual(controls["synthetic_net_qty"], 0.0)
        self.assertIn("BUY", [order["side"] for order in desired_orders])
        self.assertIn("SELL", [order["side"] for order in desired_orders])

    def test_build_spot_competition_synthetic_neutral_grid_keeps_buys_while_allowing_small_sell_below_base(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "long_active"
        runtime["grid_anchor_price"] = 100.0
        runtime["position_lots"] = [
            {
                "lot_id": "retained",
                "side": "long",
                "qty": 0.5,
                "entry_price": 100.0,
                "opened_at_ms": 1,
                "source_role": "grid_entry",
            }
        ]
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "spot_competition_synthetic_neutral_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_synthetic_neutral_grid",
                    "market_type": "futures",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
            },
            trades=[],
            bid_price=99.0,
            ask_price=101.0,
            step_price=2.0,
            buy_levels=1,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=30.0,
            threshold_position_notional=80.0,
            threshold_reduce_target_notional=50.0,
            max_order_position_notional=300.0,
            max_position_notional=500.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1.0,
            max_short_position_notional=200.0,
            actual_base_qty=1.5,
        )

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "SELL", "SELL"])
        self.assertEqual(desired_orders[0]["role"], "grid_entry")
        self.assertLess(desired_orders[0]["price"], 99.0)
        self.assertGreaterEqual(desired_orders[1]["price"], 101.0)
        self.assertEqual(controls["synthetic_base_sell_floor"]["dropped_sell_orders"], 0)
        self.assertAlmostEqual(controls["synthetic_base_sell_floor"]["floor_qty"], 0.5)

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
        self.assertEqual(controls["synthetic_freeze_disabled_reason"], "spot_synthetic_neutral_uses_spot_freeze")
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

        self.assertEqual(desired_orders, [])

    def test_spot_base_restore_only_buys_back_base_without_selling(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        runtime["synthetic_neutral"] = True
        runtime["direction_state"] = "short_active"
        runtime["grid_anchor_price"] = 0.1
        runtime["position_lots"] = [
            {
                "lot_id": "active_short",
                "side": "short",
                "qty": 150.0,
                "entry_price": 0.1,
                "opened_at_ms": 1,
                "source_role": "grid_entry",
            }
        ]
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "spot_competition_synthetic_neutral_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_synthetic_neutral_grid",
                    "market_type": "futures",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
            },
            trades=[],
            bid_price=0.099,
            ask_price=0.101,
            step_price=0.002,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=10.0,
            threshold_position_notional=10.0,
            max_order_position_notional=80.0,
            max_position_notional=100.0,
            tick_size=0.000001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1000.0,
            max_short_position_notional=80.0,
            actual_base_qty=850.0,
            spot_freeze_tolerance_qty=0.2,
            spot_base_restore_only=True,
        )

        self.assertTrue(desired_orders)
        self.assertEqual({order["side"] for order in desired_orders}, {"BUY"})
        self.assertTrue(all(order.get("base_restore_only") for order in desired_orders))
        self.assertTrue(controls["spot_base_restore_only"])
        self.assertEqual(controls["spot_base_restore_only_status"]["allowed_side"], "BUY")
        self.assertEqual(controls["effective_sell_levels"], 0)

    def test_spot_base_restore_only_stops_orders_when_base_restored(self) -> None:
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
            actual_base_qty=1000.1,
            spot_freeze_tolerance_qty=0.2,
            spot_base_restore_only=True,
        )

        self.assertEqual(desired_orders, [])
        self.assertEqual(controls["spot_base_restore_only_status"]["reason"], "base_restored")
        self.assertEqual(controls["effective_buy_levels"], 0)
        self.assertEqual(controls["effective_sell_levels"], 0)

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

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY"])
        self.assertLess(desired_orders[0]["price"], 0.0901)
        self.assertLess(desired_orders[1]["price"], desired_orders[0]["price"])
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
        self.assertEqual(sell_prices, [])
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

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY"])
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

        self.assertEqual([order["side"] for order in desired_orders], ["BUY", "BUY"])
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
                        "same_price_take_exit_trade_ids": [701],
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
        self.assertEqual(state["same_price_take_exit_trade_ids"], [])
        self.assertNotIn("spot_competition_inventory_grid_runtime_cache", state)

    def test_run_cycle_persists_spot_freeze_ledger_before_order_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            args = self._synthetic_args(
                [
                    "--state-path",
                    str(state_path),
                    "--summary-jsonl",
                    str(summary_path),
                    "--apply",
                    "--cancel-stale",
                    "--spot-freeze-enabled",
                ]
            )
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.01,
                "step_size": 0.001,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }
            frozen_ledger = new_ledger()
            frozen_ledger["short_lots"] = [{"lot_id": "spot_freeze_1", "qty": 5.0}]
            frozen_ledger["frozen_short_qty"] = 5.0

            def mutate_freeze_state(**kwargs):
                kwargs["state"]["spot_frozen_ledger"] = frozen_ledger
                kwargs["controls"]["spot_freeze_actions"] = [{"type": "freeze", "side": "short", "qty": 5.0}]

            with patch("grid_optimizer.spot_loop_runner.fetch_spot_book_tickers", return_value=[{"bid_price": "10", "ask_price": "10.02"}]):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=[]):
                    with patch("grid_optimizer.spot_loop_runner.fetch_spot_account_info", return_value={"balances": []}):
                        with patch(
                            "grid_optimizer.spot_loop_runner.fetch_spot_open_orders",
                            return_value=[
                                {
                                    "orderId": 123,
                                    "clientOrderId": "spotgrid_old",
                                    "side": "BUY",
                                    "price": "9.9",
                                    "origQty": "1",
                                    "executedQty": "0",
                                    "status": "NEW",
                                }
                            ],
                        ):
                            with patch("grid_optimizer.spot_loop_runner._refresh_spot_trades_after_account_snapshot", return_value=([], 0)):
                                with patch(
                                    "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                    return_value=([], {"actual_base_qty": 95.0, "neutral_base_qty": 100.0, "_runtime": {}}),
                                ):
                                    with patch("grid_optimizer.spot_loop_runner._maybe_run_spot_freeze", side_effect=mutate_freeze_state):
                                        with patch("grid_optimizer.spot_loop_runner._cancel_orders", side_effect=RuntimeError("cancel failed")):
                                            with self.assertRaisesRegex(RuntimeError, "cancel failed"):
                                                _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["spot_frozen_ledger"]["frozen_short_qty"], 5.0)
            self.assertEqual(saved["spot_frozen_ledger"]["short_lots"], [{"lot_id": "spot_freeze_1", "qty": 5.0}])

    def test_run_cycle_recovers_spot_freeze_open_order_before_trade_sync(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            args = self._synthetic_args(
                [
                    "--state-path",
                    str(state_path),
                    "--summary-jsonl",
                    str(summary_path),
                    "--no-cancel-stale",
                ]
            )
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.01,
                "step_size": 0.001,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }
            trades = [
                {
                    "id": 9002,
                    "orderId": 7002,
                    "time": 1782306000000,
                    "price": "10.0",
                    "qty": "3.5",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "isMaker": True,
                }
            ]
            open_orders = [
                {
                    "orderId": 7002,
                    "clientOrderId": "spotgrid_spot_freeze_short_b_123456",
                    "side": "BUY",
                    "price": "10.0",
                    "origQty": "5.0",
                    "executedQty": "3.5",
                    "status": "PARTIALLY_FILLED",
                }
            ]

            with patch("grid_optimizer.spot_loop_runner.fetch_spot_book_tickers", return_value=[{"bid_price": "9.99", "ask_price": "10.01"}]):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=trades):
                    with patch(
                        "grid_optimizer.spot_loop_runner.fetch_spot_account_info",
                        return_value={
                            "balances": [
                                {"asset": "WLD", "free": "96.5", "locked": "3.5"},
                                {"asset": "USDT", "free": "1000", "locked": "0"},
                            ]
                        },
                    ):
                        with patch("grid_optimizer.spot_loop_runner.fetch_spot_open_orders", return_value=open_orders):
                            with patch(
                                "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                return_value=([], {"actual_base_qty": 100.0, "neutral_base_qty": 100.0, "_runtime": {}}),
                            ):
                                summary = _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            ledger = saved["spot_frozen_ledger"]
            self.assertEqual(summary["applied_trades"], 1)
            self.assertEqual(ledger["short_lots"][0]["lot_id"], "spot_freeze_maker_9002")
            self.assertEqual(
                ledger["pending_contract_actions"],
                [
                    {
                        "side": "SELL",
                        "position_side": "SHORT",
                        "qty": 3.5,
                        "reason": "freeze_short_hedge",
                        "lot_id": "spot_freeze_maker_9002",
                    }
                ],
            )

    def test_synthetic_neutral_cycle_same_price_take_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            state_path.write_text(
                json.dumps(
                    {
                        "symbol": "WLDUSDT",
                        "strategy_mode": "spot_competition_synthetic_neutral_grid",
                        "known_orders": {"88": {"side": "BUY", "tag": "buy"}},
                        "same_price_take_exit_start_time_ms": 1782349764000,
                    }
                ),
                encoding="utf-8",
            )
            args = self._synthetic_args(["--state-path", str(state_path), "--summary-jsonl", str(summary_path), "--apply"])
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.01,
                "step_size": 0.001,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }
            trades = [
                {
                    "id": 701,
                    "orderId": 88,
                    "time": 1782349765000,
                    "price": "1.62",
                    "qty": "6",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "isBuyer": True,
                    "isMaker": True,
                }
            ]
            with patch(
                "grid_optimizer.spot_loop_runner.fetch_spot_book_tickers",
                side_effect=[
                    [{"bid_price": "1.61", "bid_qty": "10", "ask_price": "1.62"}],
                    [{"bid_price": "1.62", "bid_qty": "10", "ask_price": "1.63"}],
                ],
            ):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=trades):
                    with patch(
                        "grid_optimizer.spot_loop_runner.fetch_spot_account_info",
                        return_value={
                            "balances": [
                                {"asset": "WLD", "free": "106", "locked": "0"},
                                {"asset": "USDT", "free": "1000", "locked": "0"},
                            ]
                        },
                    ):
                        with patch("grid_optimizer.spot_loop_runner.fetch_spot_open_orders", return_value=[]):
                            with patch(
                                "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                return_value=([], {"actual_base_qty": 106.0, "neutral_base_qty": 100.0, "_runtime": {}}),
                            ):
                                with patch("grid_optimizer.spot_loop_runner.post_spot_order", return_value={"orderId": 99}) as post_order:
                                    summary = _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

        self.assertEqual(summary["same_price_take_exit"]["executed_qty"], 6.0)
        post_order.assert_called_once()
        self.assertEqual(post_order.call_args.kwargs["side"], "SELL")
        self.assertEqual(post_order.call_args.kwargs["order_type"], "LIMIT")
        self.assertEqual(post_order.call_args.kwargs["time_in_force"], "IOC")
        self.assertEqual(post_order.call_args.kwargs["price"], 1.62)
        self.assertEqual(post_order.call_args.kwargs["quantity"], 6.0)

    def test_run_cycle_app_loss_guard_uses_raw_trades_when_known_orders_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            args = self._synthetic_args(
                [
                    "--state-path",
                    str(state_path),
                    "--summary-jsonl",
                    str(summary_path),
                    "--per-order-notional",
                    "60",
                    "--neutral-base-qty",
                    "4800",
                    "--spot-app-loss-guard-enabled",
                    "--spot-app-loss-min-notional",
                    "10",
                    "--spot-app-loss-per-10k-soft",
                    "0.6",
                    "--spot-app-loss-per-10k-hard",
                    "1.0",
                ]
            )
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            }
            raw_trades = [
                {
                    "id": 1,
                    "orderId": 10,
                    "isBuyer": True,
                    "isMaker": True,
                    "price": "0.0885",
                    "qty": "4800",
                    "quoteQty": "424.8",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 1000,
                },
                {
                    "id": 2,
                    "orderId": 11,
                    "isBuyer": False,
                    "isMaker": True,
                    "price": "0.0870",
                    "qty": "680",
                    "quoteQty": "59.16",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 1100,
                },
            ]

            with patch("grid_optimizer.spot_loop_runner.fetch_spot_book_tickers", return_value=[{"bid_price": "0.0868", "ask_price": "0.0869"}]):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=raw_trades):
                    with patch(
                        "grid_optimizer.spot_loop_runner.fetch_spot_account_info",
                        return_value={
                            "balances": [
                                {"asset": "WLD", "free": "4120", "locked": "0"},
                                {"asset": "USDT", "free": "3000", "locked": "0"},
                            ]
                        },
                    ):
                        with patch("grid_optimizer.spot_loop_runner.fetch_spot_open_orders", return_value=[]):
                            with patch(
                                "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                return_value=(
                                    [{"side": "BUY", "role": "grid_exit", "price": 0.0867, "qty": 690.0}],
                                    {"actual_base_qty": 4120.0, "neutral_base_qty": 4800.0, "_runtime": {}},
                                ),
                            ):
                                summary = _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

        guard = summary["spot_app_loss_guard"]
        self.assertEqual(guard["window_source"], "recent_trades")
        self.assertEqual(guard["state"], "blocked")
        self.assertEqual(guard["reduce_side"], "BUY")
        self.assertEqual(summary["active_buy_orders"], 0)
        self.assertEqual(summary["active_sell_orders"], 0)
        self.assertTrue(summary["stop_triggered"])
        self.assertEqual(summary["runtime_status"], "stopped")
        self.assertEqual(summary["stop_reason"], "spot_app_loss_hard_limit_hit")
        self.assertEqual(summary["spot_freeze_skip_reason"], "spot_app_loss_runner_stop")
        self.assertAlmostEqual(guard["latest_price"], 0.0868)
        self.assertGreaterEqual(summary["spot_app_loss_per_10k"], 1.0)

    def test_run_cycle_app_loss_guard_resets_stale_metrics_to_runtime_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            state_path.write_text(
                json.dumps(
                    {
                        "symbol": "WLDUSDT",
                        "strategy_mode": "spot_competition_synthetic_neutral_grid",
                        "last_trade_time_ms": 1_000,
                        "seen_trade_ids": [100],
                        "metrics": {
                            "gross_notional": 6897.78006,
                            "buy_notional": 3448.89003,
                            "sell_notional": 3448.89003,
                            "buy_qty": 1000.0,
                            "sell_qty": 1000.0,
                            "app_loss_window_aligned": True,
                            "trade_count": 204,
                            "maker_count": 204,
                            "recent_trades": [],
                            "first_trade_time_ms": 1_000,
                            "last_trade_time_ms": 1_000,
                        },
                    }
                )
            )
            args = self._synthetic_args(
                [
                    "--state-path",
                    str(state_path),
                    "--summary-jsonl",
                    str(summary_path),
                    "--per-order-notional",
                    "60",
                    "--neutral-base-qty",
                    "4800",
                    "--runtime-guard-stats-start-time",
                    "1970-01-01T00:00:02+00:00",
                    "--spot-app-loss-guard-enabled",
                    "--spot-app-loss-min-notional",
                    "10",
                    "--spot-app-loss-per-10k-soft",
                    "0.6",
                    "--spot-app-loss-per-10k-hard",
                    "1.0",
                ]
            )
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            }
            raw_trades = [
                {
                    "id": 200,
                    "orderId": 20,
                    "isBuyer": True,
                    "isMaker": True,
                    "price": "0.0935",
                    "qty": "4800",
                    "quoteQty": "448.8",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 2_500,
                },
            ]

            with patch("grid_optimizer.spot_loop_runner.fetch_spot_book_tickers", return_value=[{"bid_price": "0.0931", "ask_price": "0.0932"}]):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=raw_trades) as fetch_trades:
                    with patch(
                        "grid_optimizer.spot_loop_runner.fetch_spot_account_info",
                        return_value={
                            "balances": [
                                {"asset": "WLD", "free": "4800", "locked": "0"},
                                {"asset": "USDT", "free": "3000", "locked": "0"},
                            ]
                        },
                    ):
                        with patch("grid_optimizer.spot_loop_runner.fetch_spot_open_orders", return_value=[]):
                            with patch(
                                "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                return_value=(
                                    [{"side": "BUY", "role": "grid_exit", "price": 0.0930, "qty": 640.0}],
                                    {"actual_base_qty": 4800.0, "neutral_base_qty": 4800.0, "_runtime": {}},
                                ),
                            ):
                                summary = _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

        start_times = [call.kwargs.get("start_time_ms") for call in fetch_trades.call_args_list]
        self.assertTrue(start_times)
        self.assertTrue(all(start_time >= 2_000 for start_time in start_times))
        guard = summary["spot_app_loss_guard"]
        self.assertEqual(guard["window_source"], "recent_trades")
        self.assertAlmostEqual(guard["buy_notional"], 448.8)
        self.assertAlmostEqual(guard["sell_notional"], 0.0)
        self.assertEqual(guard["state"], "blocked")
        self.assertTrue(summary["stop_triggered"])
        self.assertEqual(summary["stop_reason"], "spot_app_loss_hard_limit_hit")

    def test_run_cycle_seeds_app_loss_baseline_from_starting_base_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            args = self._synthetic_args(
                [
                    "--state-path",
                    str(state_path),
                    "--summary-jsonl",
                    str(summary_path),
                    "--neutral-base-qty",
                    "4800",
                    "--spot-app-loss-guard-enabled",
                    "--spot-app-loss-min-notional",
                    "0",
                    "--spot-app-loss-per-10k-soft",
                    "0.6",
                    "--spot-app-loss-per-10k-hard",
                    "1.0",
                ]
            )
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            }

            with patch("grid_optimizer.spot_loop_runner.fetch_spot_book_tickers", return_value=[{"bid_price": "0.0935", "ask_price": "0.0935"}]):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=[]):
                    with patch(
                        "grid_optimizer.spot_loop_runner.fetch_spot_account_info",
                        return_value={
                            "balances": [
                                {"asset": "WLD", "free": "4800", "locked": "0"},
                                {"asset": "USDT", "free": "3000", "locked": "0"},
                            ]
                        },
                    ):
                        with patch("grid_optimizer.spot_loop_runner.fetch_spot_open_orders", return_value=[]):
                            with patch(
                                "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                return_value=(
                                    [],
                                    {"actual_base_qty": 4800.0, "neutral_base_qty": 4800.0, "_runtime": {}},
                                ),
                            ):
                                summary = _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

            state = json.loads(state_path.read_text(encoding="utf-8"))

        metrics = state["metrics"]
        guard = summary["spot_app_loss_guard"]
        self.assertAlmostEqual(metrics["app_loss_baseline_qty"], 4800.0)
        self.assertAlmostEqual(metrics["app_loss_baseline_price"], 0.0935)
        self.assertAlmostEqual(metrics["app_loss_baseline_notional"], 448.8)
        self.assertAlmostEqual(guard["baseline_qty"], 4800.0)
        self.assertAlmostEqual(guard["buy_notional"], 448.8)
        self.assertAlmostEqual(guard["position_value"], 448.8)
        self.assertAlmostEqual(guard["app_loss"], 0.0)

    def test_run_cycle_app_loss_hard_stop_cancels_orders_even_when_cancel_stale_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            summary_path = Path(tmpdir) / "events.jsonl"
            args = self._synthetic_args(
                [
                    "--state-path",
                    str(state_path),
                    "--summary-jsonl",
                    str(summary_path),
                    "--per-order-notional",
                    "60",
                    "--neutral-base-qty",
                    "4800",
                    "--spot-app-loss-guard-enabled",
                    "--spot-app-loss-min-notional",
                    "10",
                    "--spot-app-loss-per-10k-soft",
                    "0.6",
                    "--spot-app-loss-per-10k-hard",
                    "1.0",
                    "--no-cancel-stale",
                ]
            )
            symbol_info = {
                "base_asset": "WLD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            }
            raw_trades = [
                {
                    "id": 1,
                    "orderId": 10,
                    "isBuyer": True,
                    "isMaker": True,
                    "price": "0.0885",
                    "qty": "4800",
                    "quoteQty": "424.8",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 1000,
                },
                {
                    "id": 2,
                    "orderId": 11,
                    "isBuyer": False,
                    "isMaker": True,
                    "price": "0.0870",
                    "qty": "680",
                    "quoteQty": "59.16",
                    "commission": "0",
                    "commissionAsset": "BNB",
                    "time": 1100,
                },
            ]
            open_orders = [
                {
                    "orderId": 123,
                    "clientOrderId": "spotgrid-old",
                    "side": "BUY",
                    "type": "LIMIT_MAKER",
                    "price": "0.0867",
                    "origQty": "690.0",
                    "positionSide": "BOTH",
                }
            ]

            with patch("grid_optimizer.spot_loop_runner.fetch_spot_book_tickers", return_value=[{"bid_price": "0.0868", "ask_price": "0.0869"}]):
                with patch("grid_optimizer.spot_loop_runner.fetch_spot_user_trades", return_value=raw_trades):
                    with patch(
                        "grid_optimizer.spot_loop_runner.fetch_spot_account_info",
                        return_value={
                            "balances": [
                                {"asset": "WLD", "free": "4120", "locked": "0"},
                                {"asset": "USDT", "free": "3000", "locked": "0"},
                            ]
                        },
                    ):
                        with patch("grid_optimizer.spot_loop_runner.fetch_spot_open_orders", return_value=open_orders):
                            with patch(
                                "grid_optimizer.spot_loop_runner._build_spot_competition_inventory_grid_orders",
                                return_value=(
                                    [{"side": "BUY", "role": "grid_exit", "price": 0.0867, "qty": 690.0}],
                                    {"actual_base_qty": 4120.0, "neutral_base_qty": 4800.0, "_runtime": {}},
                                ),
                            ):
                                with patch("grid_optimizer.spot_loop_runner._cancel_orders", return_value=1) as cancel_orders:
                                    summary = _run_cycle(args, symbol_info, api_key="key", api_secret="secret")

        cancel_orders.assert_called_once()
        self.assertEqual(cancel_orders.call_args.kwargs["orders"], open_orders)
        self.assertEqual(summary["canceled_count"], 1)
        self.assertTrue(summary["stop_triggered"])
        self.assertEqual(summary["stop_reason"], "spot_app_loss_hard_limit_hit")

    def test_cancel_orders_ignores_already_gone_spot_order(self) -> None:
        with patch(
            "grid_optimizer.spot_loop_runner.delete_spot_order",
            side_effect=RuntimeError("Binance API error -2011: Unknown order sent."),
        ) as delete_order:
            canceled = _cancel_orders(
                symbol="WLDUSDT",
                api_key="key",
                api_secret="secret",
                orders=[{"orderId": "123"}],
            )

        self.assertEqual(canceled, 1)
        delete_order.assert_called_once()

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
        self.assertIn("grid_entry", roles)
        self.assertIn("SELL", {str(order.get("side")) for order in desired_orders})
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

    def test_synthetic_neutral_grid_keeps_first_buy_at_best_bid(self) -> None:
        desired_orders, _ = _build_spot_competition_inventory_grid_orders(
            state={"known_orders": {}, "inventory_lots": []},
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
            synthetic_neutral=True,
            neutral_base_qty=300.0,
            actual_base_qty=300.0,
        )

        buy_prices = [order["price"] for order in desired_orders if order["side"] == "BUY"]
        self.assertEqual(buy_prices, [100.0, 99.3, 98.6])

    def test_synthetic_neutral_long_grid_keeps_first_buy_at_best_bid(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side="BUY",
            price=100.0,
            qty=0.24,
            fill_time_ms=1000,
            step_price=0.7,
        )
        desired_orders, _ = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
                "spot_competition_synthetic_neutral_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_synthetic_neutral_grid",
                    "market_type": "futures",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
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
            synthetic_neutral=True,
            neutral_base_qty=300.0,
            actual_base_qty=300.24,
        )

        buy_prices = [order["price"] for order in desired_orders if order["side"] == "BUY"]
        self.assertEqual(buy_prices, [100.0, 99.3, 98.6])

    def test_synthetic_neutral_fallback_buy_keeps_best_bid(self) -> None:
        runtime = new_inventory_grid_runtime(market_type="futures")
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side="BUY",
            price=100.0,
            qty=0.24,
            fill_time_ms=1000,
            step_price=0.7,
        )
        desired_orders, _ = _build_spot_competition_inventory_grid_orders(
            state={
                "known_orders": {},
                "inventory_lots": [],
                "spot_competition_synthetic_neutral_grid_runtime_cache": {
                    "strategy_mode": "spot_competition_synthetic_neutral_grid",
                    "market_type": "futures",
                    "runtime": runtime,
                    "applied_trade_keys": [],
                },
            },
            trades=[],
            bid_price=100.0,
            ask_price=100.2,
            step_price=0.7,
            buy_levels=3,
            sell_levels=1,
            first_order_multiplier=1.0,
            per_order_notional=40.0,
            threshold_position_notional=80.0,
            max_order_position_notional=50.0,
            max_position_notional=120.0,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=300.0,
            actual_base_qty=300.24,
        )

        buy_prices = [order["price"] for order in desired_orders if order["side"] == "BUY"]
        self.assertEqual(buy_prices, [100.0, 99.3])

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
