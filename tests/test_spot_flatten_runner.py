from __future__ import annotations

import unittest

from grid_optimizer.spot_flatten_runner import build_spot_flatten_orders_from_snapshot


class SpotFlattenRunnerTests(unittest.TestCase):
    def test_build_spot_flatten_orders_from_snapshot_places_best_ask_sell(self) -> None:
        result = build_spot_flatten_orders_from_snapshot(
            symbol="SAHARAUSDT",
            ask_price=0.0269,
            account_info={
                "balances": [
                    {"asset": "SAHARA", "free": "1200", "locked": "300"},
                    {"asset": "USDT", "free": "100", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "SAHARA",
                "tick_size": 0.0001,
                "step_size": 1.0,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        )
        self.assertEqual(len(result["orders"]), 1)
        self.assertEqual(result["inventory_qty"], 1500.0)
        order = result["orders"][0]
        self.assertEqual(order["side"], "SELL")
        self.assertEqual(order["price"], 0.0269)
        self.assertEqual(order["quantity"], 1500.0)
        self.assertEqual(order["time_in_force"], "GTX")

    def test_build_spot_flatten_orders_from_snapshot_skips_when_below_min_notional(self) -> None:
        result = build_spot_flatten_orders_from_snapshot(
            symbol="SAHARAUSDT",
            ask_price=0.0269,
            account_info={
                "balances": [
                    {"asset": "SAHARA", "free": "10", "locked": "0"},
                ]
            },
            symbol_info={
                "base_asset": "SAHARA",
                "tick_size": 0.0001,
                "step_size": 1.0,
                "min_qty": 1.0,
                "min_notional": 5.0,
            },
        )
        self.assertEqual(result["orders"], [])
        self.assertTrue(result["warnings"])


if __name__ == "__main__":
    unittest.main()
