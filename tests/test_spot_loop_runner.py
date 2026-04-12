from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime
from grid_optimizer.spot_loop_runner import (
    _build_parser,
    _build_spot_competition_inventory_grid_orders,
    _load_state,
    _normalize_commission_quote,
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
        self.assertEqual(args.runtime_guard_stats_start_time, "2026-04-05T00:00:00+00:00")
        self.assertEqual(args.max_order_position_notional, 300.0)
        self.assertEqual(args.max_position_notional, 400.0)

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
