from __future__ import annotations

import time
import unittest
import re
from unittest.mock import patch

from grid_optimizer.spot_loop_runner import (
    _build_attack_market_entry_order,
    _build_best_quote_inventory_desired_orders,
    _build_reduce_only_market_exit_order,
    _build_volume_shift_desired_orders,
    _build_client_order_id,
    _maybe_bootstrap_inventory_from_balance,
    _normalize_commission_quote,
    _resolve_best_quote_inventory_mode,
    _resolve_volume_shift_mode,
    _sort_orders_for_placement,
)


class SpotLoopRunnerTests(unittest.TestCase):
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

    def test_bootstrap_inventory_from_balance_creates_initial_lot_once(self) -> None:
        state = {
            "inventory_lots": [],
            "metrics": {"trade_count": 0},
        }

        created = _maybe_bootstrap_inventory_from_balance(
            state=state,
            base_free=0.1769,
            mid_price=4757.725,
            now_ms=123456789,
            step_size=0.0001,
            min_qty=0.0001,
        )

        self.assertEqual(created, 1)
        self.assertTrue(state["inventory_bootstrap_completed"])
        self.assertEqual(len(state["inventory_lots"]), 1)
        self.assertEqual(state["inventory_lots"][0]["lot_id"], "bootstrap_123456789")
        self.assertAlmostEqual(state["inventory_lots"][0]["qty"], 0.1769)
        self.assertAlmostEqual(state["inventory_lots"][0]["cost_quote"], 0.1769 * 4757.725)

        created_again = _maybe_bootstrap_inventory_from_balance(
            state=state,
            base_free=0.1769,
            mid_price=4757.725,
            now_ms=123456790,
            step_size=0.0001,
            min_qty=0.0001,
        )
        self.assertEqual(created_again, 0)
        self.assertEqual(len(state["inventory_lots"]), 1)

    def test_build_client_order_id_fits_exchange_limits(self) -> None:
        client_order_id = _build_client_order_id(
            "sgxautusdt",
            "sell_bootstrap_1775622600000",
            "SELL",
        )

        self.assertLessEqual(len(client_order_id), 36)
        self.assertIsNotNone(re.fullmatch(r"[A-Za-z0-9_-]{1,36}", client_order_id))
        self.assertTrue(client_order_id.startswith("sgxautusdt_"))

    def test_sort_orders_for_placement_prefers_nearest_prices(self) -> None:
        orders = [
            {"side": "BUY", "price": 4751.30, "qty": 0.0084, "tag": "atkbuy5"},
            {"side": "BUY", "price": 4755.15, "qty": 0.0084, "tag": "atkbuy2"},
            {"side": "BUY", "price": 4756.44, "qty": 0.0084, "tag": "atkbuy1"},
            {"side": "SELL", "price": 4759.00, "qty": 0.0084, "tag": "sell_a"},
            {"side": "SELL", "price": 4758.20, "qty": 0.0084, "tag": "sell_b"},
        ]

        sorted_orders = _sort_orders_for_placement(orders, bid_price=4757.72, ask_price=4757.73)

        self.assertEqual(
            [order["tag"] for order in sorted_orders],
            ["atkbuy1", "atkbuy2", "atkbuy5", "sell_b", "sell_a"],
        )

    def test_bootstrap_inventory_does_not_consume_near_buy_levels(self) -> None:
        symbol_info = {
            "tick_size": 0.01,
            "step_size": 0.0001,
            "min_qty": 0.0001,
            "min_notional": 5.0,
        }
        state = {
            "inventory_lots": [
                {
                    "lot_id": "bootstrap_1",
                    "qty": 0.0999,
                    "cost_quote": 475.0989255,
                    "buy_time_ms": int(time.time() * 1000),
                    "tag": "bootstrap_inventory",
                }
            ],
            "center_price": 0.0,
            "center_shift_down_cycles": 0,
            "center_shift_up_cycles": 0,
            "center_shift_count": 0,
        }
        config = {
            "attack_buy_levels": 5,
            "attack_sell_levels": 5,
            "attack_per_order_notional": 40.0,
            "defense_buy_levels": 5,
            "defense_sell_levels": 5,
            "defense_per_order_notional": 40.0,
            "inventory_soft_limit_notional": 750.0,
            "inventory_hard_limit_notional": 950.0,
            "inventory_recycle_age_minutes": 999999.0,
            "center_shift_trigger_ratio": 0.00135,
            "center_shift_confirm_cycles": 3,
            "center_shift_step_ratio": 0.00135,
            "grid_band_ratio": 0.00135,
            "inventory_recycle_loss_tolerance_ratio": 0.006,
            "inventory_recycle_min_profit_ratio": 0.001,
        }

        desired, controls = _build_volume_shift_desired_orders(
            state=state,
            config=config,
            symbol_info=symbol_info,
            bid_price=4755.74,
            ask_price=4755.75,
            mid_price=4755.745,
            market_guard={"buy_pause_active": False, "buy_pause_reasons": [], "shift_frozen": False},
        )

        self.assertEqual(controls["mode"], "attack")
        self.assertEqual(
            [order["tag"] for order in desired if order["side"] == "BUY"],
            ["atkbuy1", "atkbuy2", "atkbuy3", "atkbuy4", "atkbuy5"],
        )

    def test_build_attack_market_entry_order_replenishes_to_four_orders(self) -> None:
        order = _build_attack_market_entry_order(
            state={"attack_market_entry_last_time_ms": 0},
            config={
                "attack_market_entry_enabled": True,
                "attack_market_entry_trigger_multiplier": 1.0,
                "attack_market_entry_target_multiplier": 4.0,
                "attack_market_entry_cooldown_seconds": 60.0,
            },
            controls={
                "mode": "attack",
                "buy_paused": False,
                "inventory_notional": 6.690747,
                "effective_per_order_notional": 15.0,
            },
            ask_price=4779.11,
            quote_free=200.0,
            step_size=0.0001,
            min_qty=0.0001,
            min_notional=5.0,
            now_ms=1_000_000,
        )

        self.assertIsNotNone(order)
        self.assertEqual(order["side"], "BUY")
        self.assertEqual(order["order_type"], "MARKET")
        self.assertEqual(order["tag"], "warmentry")
        self.assertEqual(order["role"], "warm_start_entry")
        self.assertAlmostEqual(order["qty"], 0.0111)

    def test_build_attack_market_entry_order_respects_cooldown(self) -> None:
        order = _build_attack_market_entry_order(
            state={"attack_market_entry_last_time_ms": 950_000},
            config={
                "attack_market_entry_enabled": True,
                "attack_market_entry_trigger_multiplier": 1.0,
                "attack_market_entry_target_multiplier": 4.0,
                "attack_market_entry_cooldown_seconds": 60.0,
            },
            controls={
                "mode": "attack",
                "buy_paused": False,
                "inventory_notional": 0.0,
                "effective_per_order_notional": 15.0,
            },
            ask_price=4779.11,
            quote_free=200.0,
            step_size=0.0001,
            min_qty=0.0001,
            min_notional=5.0,
            now_ms=1_000_000,
        )

        self.assertIsNone(order)

    def test_resolve_volume_shift_mode_does_not_pause_buys_for_aged_light_inventory(self) -> None:
        mode, buy_paused, reasons = _resolve_volume_shift_mode(
            inventory_notional=232.0,
            soft_limit=750.0,
            hard_limit=950.0,
            oldest_age_minutes=52.0,
            recycle_age_minutes=40.0,
            market_guard={"buy_pause_active": False, "buy_pause_reasons": []},
        )

        self.assertEqual(mode, "attack")
        self.assertFalse(buy_paused)
        self.assertNotIn("inventory_age_limit", reasons)

    def test_resolve_best_quote_inventory_mode_switches_to_reduce_only_at_double_base(self) -> None:
        mode = _resolve_best_quote_inventory_mode(
            inventory_notional=240.0,
            base_position_notional=120.0,
            max_inventory_multiplier=2.0,
        )

        self.assertEqual(mode, "reduce_only")

    def test_build_best_quote_inventory_desired_orders_posts_maker_sell_when_inventory_is_light(self) -> None:
        desired, controls = _build_best_quote_inventory_desired_orders(
            state={},
            config={
                "base_position_notional": 120.0,
                "max_inventory_multiplier": 2.0,
                "quote_buy_order_notional": 25.0,
                "quote_sell_order_notional": 25.0,
                "min_profit_offset": 0.0001,
            },
            symbol_info={
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            bid_price=1.0000,
            ask_price=1.0001,
            mid_price=1.00005,
            inventory_lots=[
                {
                    "lot_id": "lot_a",
                    "qty": 150.0,
                    "cost_quote": 149.97,
                    "buy_time_ms": 1,
                    "tag": "quote",
                }
            ],
            base_free=150.0,
            now_ms=1_000,
        )

        self.assertEqual(controls["mode"], "quote")
        self.assertEqual([order["side"] for order in desired], ["BUY", "SELL"])
        self.assertEqual(desired[0]["price"], 1.0)
        self.assertEqual(desired[1]["price"], 1.0001)
        self.assertEqual(desired[1]["order_type"], "LIMIT_MAKER")
        self.assertEqual(desired[1]["time_in_force"], "GTC")

    def test_build_best_quote_inventory_desired_orders_stops_buying_in_reduce_only(self) -> None:
        desired, controls = _build_best_quote_inventory_desired_orders(
            state={},
            config={
                "base_position_notional": 120.0,
                "max_inventory_multiplier": 2.0,
                "quote_buy_order_notional": 25.0,
                "quote_sell_order_notional": 25.0,
                "min_profit_offset": 0.0001,
            },
            symbol_info={
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            bid_price=1.0000,
            ask_price=1.0001,
            mid_price=1.00005,
            inventory_lots=[
                {
                    "lot_id": "lot_a",
                    "qty": 260.0,
                    "cost_quote": 259.74,
                    "buy_time_ms": 1,
                    "tag": "quote",
                }
            ],
            base_free=260.0,
            now_ms=1_000,
        )

        self.assertEqual(controls["mode"], "reduce_only")
        self.assertEqual([order["side"] for order in desired], ["SELL"])

    def test_build_best_quote_inventory_desired_orders_keeps_sell_maker_when_profit_offset_is_not_met(self) -> None:
        desired, controls = _build_best_quote_inventory_desired_orders(
            state={},
            config={
                "base_position_notional": 120.0,
                "max_inventory_multiplier": 2.0,
                "quote_buy_order_notional": 25.0,
                "quote_sell_order_notional": 25.0,
                "min_profit_offset": 0.0003,
            },
            symbol_info={
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            bid_price=1.0000,
            ask_price=1.0001,
            mid_price=1.00005,
            inventory_lots=[
                {
                    "lot_id": "lot_a",
                    "qty": 150.0,
                    "cost_quote": 150.0,
                    "buy_time_ms": 1,
                    "tag": "quote",
                }
            ],
            base_free=150.0,
            now_ms=1_000,
        )

        self.assertEqual(controls["mode"], "quote")
        self.assertEqual([order["side"] for order in desired], ["BUY", "SELL"])
        self.assertEqual(desired[1]["price"], 1.0003)
        self.assertEqual(desired[1]["order_type"], "LIMIT_MAKER")
        self.assertEqual(desired[1]["time_in_force"], "GTC")

    def test_build_best_quote_inventory_desired_orders_reduce_only_posts_maker_sell_before_timeout(self) -> None:
        desired, controls = _build_best_quote_inventory_desired_orders(
            state={},
            config={
                "base_position_notional": 120.0,
                "max_inventory_multiplier": 1.1,
                "quote_buy_order_notional": 25.0,
                "quote_sell_order_notional": 25.0,
                "min_profit_offset": 0.0003,
                "reduce_only_taker_target_multiplier": 1.0,
            },
            symbol_info={
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            bid_price=1.0000,
            ask_price=1.0001,
            mid_price=1.00005,
            inventory_lots=[
                {
                    "lot_id": "lot_a",
                    "qty": 150.0,
                    "cost_quote": 150.0,
                    "buy_time_ms": 1,
                    "tag": "quote",
                }
            ],
            base_free=150.0,
            now_ms=1_000,
        )

        self.assertEqual(controls["mode"], "reduce_only")
        self.assertEqual([order["side"] for order in desired], ["SELL"])
        self.assertEqual(desired[0]["price"], 1.0003)
        self.assertEqual(desired[0]["order_type"], "LIMIT_MAKER")
        self.assertEqual(desired[0]["time_in_force"], "GTC")

    def test_build_reduce_only_market_exit_order_after_timeout(self) -> None:
        order = _build_reduce_only_market_exit_order(
            state={"reduce_only_entered_at_ms": 1_000},
            config={
                "base_position_notional": 120.0,
                "reduce_only_timeout_seconds": 60.0,
                "reduce_only_taker_target_multiplier": 1.4,
            },
            symbol_info={
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            bid_price=1.0,
            mid_price=1.0,
            inventory_qty=260.0,
            inventory_notional=260.0,
            now_ms=62_000,
        )

        self.assertIsNotNone(order)
        self.assertEqual(order["side"], "SELL")
        self.assertEqual(order["order_type"], "MARKET")
        self.assertAlmostEqual(order["qty"], 92.0)


if __name__ == "__main__":
    unittest.main()
