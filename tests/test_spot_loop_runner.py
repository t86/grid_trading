from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.spot_loop_runner import (
    _build_volume_shift_desired_orders,
    _load_state,
    _new_metrics,
    _normalize_commission_quote,
    _oldest_inventory_age_minutes,
    _record_trade_metrics,
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

    def test_record_trade_metrics_updates_hourly_buckets(self) -> None:
        metrics = _new_metrics()
        trade_time_ms = 1_774_305_723_000

        _record_trade_metrics(
            metrics=metrics,
            trade={"id": 12, "time": trade_time_ms, "isMaker": True},
            side="SELL",
            price=0.0263,
            qty=500.0,
            commission_quote=0.05,
            commission_asset="USDT",
            commission_raw=0.05,
            realized_pnl=0.21,
            role="take_profit",
        )

        hour_start_ms = (trade_time_ms // 3_600_000) * 3_600_000
        bucket = metrics["hourly_buckets"][str(hour_start_ms)]
        self.assertAlmostEqual(bucket["gross_notional"], 13.15, places=8)
        self.assertAlmostEqual(bucket["sell_notional"], 13.15, places=8)
        self.assertEqual(bucket["trade_count"], 1)
        self.assertEqual(bucket["sell_count"], 1)
        self.assertAlmostEqual(bucket["commission"], 0.05, places=8)
        self.assertAlmostEqual(bucket["realized_pnl"], 0.21, places=8)

    def test_load_state_backfills_hourly_buckets_from_recent_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "spot_state.json"
            trade_time_ms = 1_774_305_723_000
            path.write_text(
                json.dumps(
                    {
                        "metrics": {
                            "recent_trades": [
                                {
                                    "time": trade_time_ms,
                                    "side": "BUY",
                                    "price": 0.026,
                                    "qty": 400.0,
                                    "notional": 10.4,
                                    "commission_quote": 0.01,
                                    "realized_pnl": -0.02,
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            state = _load_state(path, "SAHARAUSDT", "spot_volume_shift_long", 12)

        hour_start_ms = (trade_time_ms // 3_600_000) * 3_600_000
        bucket = state["metrics"]["hourly_buckets"][str(hour_start_ms)]
        self.assertAlmostEqual(bucket["gross_notional"], 10.4, places=8)
        self.assertAlmostEqual(bucket["buy_notional"], 10.4, places=8)
        self.assertEqual(bucket["trade_count"], 1)
        self.assertEqual(bucket["buy_count"], 1)
        self.assertAlmostEqual(bucket["commission"], 0.01, places=8)
        self.assertAlmostEqual(bucket["realized_pnl"], -0.02, places=8)

    def test_build_volume_shift_desired_orders_tightens_attack_front_sells(self) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        state = {
            "inventory_lots": [{"qty": 100.0, "cost_quote": 10005.0, "buy_time_ms": now_ms, "tag": "buy"}],
            "center_price": 100.0,
            "center_shift_count": 0,
            "center_shift_down_cycles": 0,
            "center_shift_up_cycles": 0,
            "last_mode": "attack",
        }
        config = {
            "grid_band_ratio": 0.004,
            "attack_buy_levels": 4,
            "attack_sell_levels": 4,
            "attack_per_order_notional": 100.0,
            "attack_sell_loss_tolerance_ratio": 0.001,
            "attack_sell_tight_levels": 2,
            "defense_buy_levels": 2,
            "defense_sell_levels": 4,
            "defense_per_order_notional": 50.0,
            "inventory_soft_limit_notional": 10000.0,
            "inventory_hard_limit_notional": 20000.0,
            "center_shift_trigger_ratio": 0.02,
            "center_shift_confirm_cycles": 1,
            "center_shift_step_ratio": 0.01,
            "inventory_recycle_age_minutes": 60.0,
            "inventory_recycle_loss_tolerance_ratio": 0.006,
            "inventory_recycle_min_profit_ratio": 0.001,
        }
        symbol_info = {
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }

        desired, controls = _build_volume_shift_desired_orders(
            state=state,
            config=config,
            symbol_info=symbol_info,
            bid_price=99.99,
            ask_price=100.0,
            mid_price=99.995,
            market_guard={"return_ratio": 0.0, "amplitude_ratio": 0.0, "shift_frozen": False},
        )

        sell_prices = [float(item["price"]) for item in desired if item.get("side") == "SELL"]
        self.assertEqual(controls["mode"], "attack")
        self.assertGreaterEqual(len(sell_prices), 2)
        self.assertAlmostEqual(sell_prices[0], 100.0, places=8)
        self.assertAlmostEqual(sell_prices[1], 100.01, places=8)

    def test_build_volume_shift_desired_orders_exits_recycle_when_below_soft_limit(self) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        state = {
            "inventory_lots": [{"qty": 100.0, "cost_quote": 10005.0, "buy_time_ms": now_ms - 20 * 60_000, "tag": "buy"}],
            "center_price": 100.0,
            "center_shift_count": 0,
            "center_shift_down_cycles": 0,
            "center_shift_up_cycles": 0,
            "last_mode": "recycle",
        }
        config = {
            "grid_band_ratio": 0.004,
            "attack_buy_levels": 4,
            "attack_sell_levels": 4,
            "attack_per_order_notional": 100.0,
            "attack_sell_loss_tolerance_ratio": 0.001,
            "attack_sell_tight_levels": 2,
            "defense_buy_levels": 2,
            "defense_sell_levels": 4,
            "defense_per_order_notional": 50.0,
            "inventory_soft_limit_notional": 9950.0,
            "inventory_hard_limit_notional": 20000.0,
            "center_shift_trigger_ratio": 0.02,
            "center_shift_confirm_cycles": 1,
            "center_shift_step_ratio": 0.01,
            "inventory_recycle_age_minutes": 15.0,
            "inventory_recycle_loss_tolerance_ratio": 0.02,
            "inventory_recycle_min_profit_ratio": 0.0,
        }
        symbol_info = {
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }

        desired, controls = _build_volume_shift_desired_orders(
            state=state,
            config=config,
            symbol_info=symbol_info,
            bid_price=99.49,
            ask_price=99.5,
            mid_price=99.5,
            market_guard={"return_ratio": 0.0, "amplitude_ratio": 0.0, "shift_frozen": False},
        )

        self.assertEqual(controls["mode"], "defense")
        self.assertIn("recycle_exit_below_soft_limit", controls["pause_reasons"])
        self.assertLess(_oldest_inventory_age_minutes(state["inventory_lots"]), 1.0)
        self.assertTrue(any(item.get("side") == "SELL" for item in desired))

    @patch("grid_optimizer.spot_loop_runner.time.time", return_value=1_700_000_000.0)
    def test_build_volume_shift_desired_orders_places_idle_front_buy_at_bid1(self, _mock_time) -> None:
        now_ms = int(1_700_000_000.0 * 1000)
        state = {
            "inventory_lots": [{"qty": 5.0, "cost_quote": 500.0, "buy_time_ms": now_ms - 60_000, "tag": "buy"}],
            "metrics": {"last_trade_time_ms": now_ms - 60_000},
            "last_trade_time_ms": now_ms - 60_000,
            "center_price": 100.0,
            "center_shift_count": 0,
            "center_shift_down_cycles": 0,
            "center_shift_up_cycles": 0,
            "last_mode": "attack",
        }
        config = {
            "grid_band_ratio": 0.004,
            "attack_buy_levels": 4,
            "attack_sell_levels": 4,
            "attack_per_order_notional": 100.0,
            "attack_sell_loss_tolerance_ratio": 0.001,
            "attack_sell_tight_levels": 2,
            "defense_buy_levels": 2,
            "defense_sell_levels": 4,
            "defense_per_order_notional": 50.0,
            "inventory_soft_limit_notional": 1000.0,
            "inventory_hard_limit_notional": 2000.0,
            "center_shift_trigger_ratio": 0.02,
            "center_shift_confirm_cycles": 1,
            "center_shift_step_ratio": 0.01,
            "inventory_recycle_age_minutes": 60.0,
            "inventory_recycle_loss_tolerance_ratio": 0.006,
            "inventory_recycle_min_profit_ratio": 0.001,
            "idle_buy1_after_seconds": 15.0,
            "idle_buy1_levels": 1,
        }
        symbol_info = {
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }

        desired, controls = _build_volume_shift_desired_orders(
            state=state,
            config=config,
            symbol_info=symbol_info,
            bid_price=99.99,
            ask_price=100.0,
            mid_price=100.0,
            market_guard={"return_ratio": 0.0, "amplitude_ratio": 0.0, "shift_frozen": False},
        )

        buy_orders = [item for item in desired if item.get("side") == "BUY"]
        self.assertTrue(controls["idle_buy1_active"])
        self.assertGreaterEqual(len(buy_orders), 1)
        self.assertAlmostEqual(float(buy_orders[0]["price"]), 99.99, places=8)


if __name__ == "__main__":
    unittest.main()
