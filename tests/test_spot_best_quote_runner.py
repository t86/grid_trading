from __future__ import annotations

import unittest

from grid_optimizer.spot_best_quote_runner import (
    apply_quote_sell_timeout_override,
    build_spot_best_quote_orders_from_snapshot,
    filter_normalized_missing_orders,
    preserve_same_price_orders,
)


class SpotBestQuoteRunnerTests(unittest.TestCase):
    def test_bootstrap_mode_places_only_bid_buy(self) -> None:
        result = build_spot_best_quote_orders_from_snapshot(
            symbol="BARDUSDT",
            bid_price=0.3060,
            ask_price=0.3061,
            account_info={
                "balances": [
                    {"asset": "BARD", "free": "0", "locked": "0"},
                    {"asset": "USDT", "free": "200", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            base_position_notional=50.0,
            quote_buy_order_notional=10.0,
            quote_sell_order_notional=12.0,
            max_inventory_multiplier=1.6,
            min_profit_offset=0.0001,
        )
        self.assertEqual(result["mode"], "bootstrap")
        self.assertEqual(len(result["orders"]), 1)
        self.assertEqual(result["orders"][0]["side"], "BUY")
        self.assertEqual(result["orders"][0]["price"], 0.3060)

    def test_quote_mode_places_bid_buy_and_ask_sell(self) -> None:
        result = build_spot_best_quote_orders_from_snapshot(
            symbol="BARDUSDT",
            bid_price=0.3060,
            ask_price=0.3061,
            account_info={
                "balances": [
                    {"asset": "BARD", "free": "220", "locked": "0"},
                    {"asset": "USDT", "free": "200", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            base_position_notional=50.0,
            quote_buy_order_notional=10.0,
            quote_sell_order_notional=12.0,
            max_inventory_multiplier=1.6,
            min_profit_offset=0.0001,
        )
        self.assertEqual(result["mode"], "quote")
        self.assertEqual([item["side"] for item in result["orders"]], ["BUY", "SELL"])
        self.assertEqual(result["orders"][0]["price"], 0.3060)
        self.assertEqual(result["orders"][1]["price"], 0.3061)
        self.assertEqual(result["orders"][1]["time_in_force"], "GTC")

    def test_quote_mode_starts_selling_once_inventory_exceeds_base(self) -> None:
        result = build_spot_best_quote_orders_from_snapshot(
            symbol="BARDUSDT",
            bid_price=0.3060,
            ask_price=0.3061,
            account_info={
                "balances": [
                    {"asset": "BARD", "free": "195", "locked": "0"},
                    {"asset": "USDT", "free": "200", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            base_position_notional=50.0,
            quote_buy_order_notional=10.0,
            quote_sell_order_notional=12.0,
            max_inventory_multiplier=1.6,
            min_profit_offset=0.0001,
        )
        self.assertEqual(result["mode"], "quote")
        self.assertEqual([item["side"] for item in result["orders"]], ["BUY", "SELL"])
        self.assertEqual(result["orders"][1]["time_in_force"], "GTC")

    def test_quote_mode_sell_keeps_base_inventory(self) -> None:
        result = build_spot_best_quote_orders_from_snapshot(
            symbol="BARDUSDT",
            bid_price=0.3060,
            ask_price=0.3061,
            account_info={
                "balances": [
                    {"asset": "BARD", "free": "240", "locked": "0"},
                    {"asset": "USDT", "free": "200", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            base_position_notional=50.0,
            quote_buy_order_notional=10.0,
            quote_sell_order_notional=12.0,
            max_inventory_multiplier=1.6,
            min_profit_offset=0.0001,
        )
        self.assertEqual(result["mode"], "quote")
        self.assertEqual(result["orders"][1]["side"], "SELL")
        self.assertEqual(result["orders"][1]["price"], 0.3061)
        self.assertEqual(result["orders"][1]["time_in_force"], "GTC")
        self.assertLessEqual(result["orders"][1]["qty"] * result["orders"][1]["price"], 12.1)
        remaining_notional = (240.0 - result["orders"][1]["qty"]) * 0.3060
        self.assertGreaterEqual(remaining_notional, 50.0)

    def test_quote_mode_near_upper_limit_stops_posting_new_buy(self) -> None:
        result = build_spot_best_quote_orders_from_snapshot(
            symbol="BARDUSDT",
            bid_price=0.3060,
            ask_price=0.3061,
            account_info={
                "balances": [
                    {"asset": "BARD", "free": "255", "locked": "0"},
                    {"asset": "USDT", "free": "200", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            base_position_notional=50.0,
            quote_buy_order_notional=30.0,
            quote_sell_order_notional=12.0,
            max_inventory_multiplier=1.6,
            min_profit_offset=0.0001,
        )
        self.assertEqual(result["mode"], "quote")
        self.assertEqual([item["side"] for item in result["orders"]], ["SELL"])

    def test_filter_normalized_missing_orders_skips_micro_residuals(self) -> None:
        filtered = filter_normalized_missing_orders(
            [
                {"side": "SELL", "price": 0.3181, "qty": 1.9, "time_in_force": "GTC"},
                {"side": "BUY", "price": 0.3180, "qty": 314.4, "time_in_force": "GTC"},
            ],
            {
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["side"], "BUY")

    def test_preserve_same_price_sell_order_when_qty_change_is_small(self) -> None:
        desired = [
            {"side": "SELL", "price": 0.3184, "qty": 312.7, "time_in_force": "GTC"},
        ]
        existing = [
            {
                "side": "SELL",
                "price": "0.3184",
                "origQty": "314.0",
                "type": "LIMIT",
                "clientOrderId": "sgbardusdt_sell_1",
            }
        ]
        updated = preserve_same_price_orders(
            desired_orders=desired,
            existing_orders=existing,
            qty_tolerance_ratio=0.01,
            qty_tolerance_abs=5.0,
        )
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["qty"], 314.0)

    def test_preserve_same_price_buy_order_when_qty_change_is_small(self) -> None:
        desired = [
            {"side": "BUY", "price": 0.3183, "qty": 314.1, "time_in_force": "GTC"},
        ]
        existing = [
            {
                "side": "BUY",
                "price": "0.3183",
                "origQty": "315.4",
                "type": "LIMIT",
                "clientOrderId": "sgbardusdt_buy_1",
            }
        ]
        updated = preserve_same_price_orders(
            desired_orders=desired,
            existing_orders=existing,
            qty_tolerance_ratio=0.01,
            qty_tolerance_abs=5.0,
        )
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["qty"], 315.4)

    def test_reduce_only_mode_places_only_sell(self) -> None:
        result = build_spot_best_quote_orders_from_snapshot(
            symbol="BARDUSDT",
            bid_price=0.3060,
            ask_price=0.3061,
            account_info={
                "balances": [
                    {"asset": "BARD", "free": "400", "locked": "0"},
                    {"asset": "USDT", "free": "20", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            base_position_notional=50.0,
            quote_buy_order_notional=10.0,
            quote_sell_order_notional=12.0,
            max_inventory_multiplier=1.6,
            min_profit_offset=0.0001,
        )
        self.assertEqual(result["mode"], "reduce_only")
        self.assertEqual(len(result["orders"]), 1)
        self.assertEqual(result["orders"][0]["side"], "SELL")
        self.assertEqual(result["orders"][0]["price"], 0.3060)
        self.assertEqual(result["orders"][0]["time_in_force"], "IOC")

    def test_quote_sell_timeout_switches_to_bid_ioc(self) -> None:
        desired = [
            {"side": "BUY", "price": 0.3060, "qty": 32.6, "time_in_force": "GTC"},
            {"side": "SELL", "price": 0.3061, "qty": 39.2, "time_in_force": "GTC"},
        ]
        existing = [
            {
                "side": "SELL",
                "price": "0.3061",
                "origQty": "39.2",
                "type": "LIMIT",
                "time": 1_000,
                "clientOrderId": "sbqbard_sell_1",
            }
        ]
        updated = apply_quote_sell_timeout_override(
            desired_orders=desired,
            existing_orders=existing,
            prefix="sbqbard",
            bid_price=0.3060,
            symbol_info={
                "base_asset": "BARD",
                "quote_asset": "USDT",
                "tick_size": 0.0001,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
            timeout_seconds=20.0,
            now_ms=25_000,
        )
        self.assertEqual(updated[1]["price"], 0.3060)
        self.assertEqual(updated[1]["time_in_force"], "IOC")


if __name__ == "__main__":
    unittest.main()
