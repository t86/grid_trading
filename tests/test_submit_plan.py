from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.submit_plan import (
    adjust_post_only_price,
    apply_anti_chase_entry_guard_to_actions,
    apply_hard_loss_rescue_entry_guard_to_actions,
    apply_loss_inventory_no_cross_entry_guard_to_actions,
    build_execution_actions,
    cap_reduce_only_place_orders_to_position,
    enforce_execution_action_limits,
    estimate_mid_drift_steps,
    filter_strategy_open_orders,
    preserve_queue_priority_in_execution_actions,
    prepare_post_only_order_request,
    sort_cancel_orders_farthest_from_market_first,
    suppress_same_side_nearby_place_orders,
    suppress_place_orders_with_existing_submitted_buckets,
    validate_plan_report,
)


class SubmitPlanTests(unittest.TestCase):
    def test_filter_strategy_open_orders_excludes_manual_orders(self) -> None:
        result = filter_strategy_open_orders(
            [
                {"clientOrderId": "gx-btcusdc-entry-1", "orderId": 1},
                {"clientOrderId": "mt_btcusdc_buy_1", "orderId": 2},
                {"clientOrderId": "", "orderId": 3},
            ],
            "BTCUSDC",
        )

        self.assertEqual(result, [{"clientOrderId": "gx-btcusdc-entry-1", "orderId": 1}])

    def test_build_execution_actions_combines_bootstrap_and_missing_orders(self) -> None:
        report = {
            "symbol": "BARDUSDT",
            "bootstrap_orders": [
                {"side": "BUY", "price": 0.05043, "qty": 3011.0, "notional": 151.84473, "role": "bootstrap"}
            ],
            "missing_orders": [
                {"side": "BUY", "price": 0.05029, "qty": 502.0, "notional": 25.24558, "role": "entry"},
                {"side": "SELL", "price": 0.05047, "qty": 500.0, "notional": 25.23500, "role": "take_profit"},
            ],
            "stale_orders": [
                {"orderId": 1, "clientOrderId": "gx-bardu-entry-1", "side": "BUY", "price": "0.05001", "origQty": "500"}
            ],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["place_count"], 3)
        self.assertEqual(actions["cancel_count"], 1)
        self.assertAlmostEqual(actions["place_notional"], 202.32531, places=8)

    def test_build_execution_actions_prioritizes_forced_reduce_orders(self) -> None:
        report = {
            "symbol": "BILLUSDT",
            "forced_reduce_orders": [
                {
                    "side": "SELL",
                    "price": 0.15186,
                    "qty": 526.0,
                    "notional": 79.87836,
                    "role": "hard_loss_forced_reduce_long",
                    "force_reduce_only": True,
                    "execution_type": "aggressive",
                    "time_in_force": "IOC",
                }
            ],
            "bootstrap_orders": [
                {"side": "BUY", "price": 0.1512, "qty": 100.0, "notional": 15.12, "role": "entry"}
            ],
            "missing_orders": [
                {"side": "SELL", "price": 0.1520, "qty": 100.0, "notional": 15.2, "role": "take_profit_long"}
            ],
            "stale_orders": [],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["place_count"], 3)
        self.assertEqual(actions["place_orders"][0]["role"], "hard_loss_forced_reduce_long")
        self.assertEqual(actions["place_orders"][0]["time_in_force"], "IOC")
        self.assertTrue(actions["place_orders"][0]["force_reduce_only"])
        self.assertAlmostEqual(actions["place_notional"], 110.19836, places=8)

    def test_build_execution_actions_deduplicates_forced_reduce_already_missing(self) -> None:
        forced_reduce = {
            "side": "SELL",
            "price": 0.14464,
            "qty": 900.0,
            "notional": 130.176,
            "role": "best_quote_adverse_reduce_long",
            "force_reduce_only": True,
            "execution_type": "post_only",
            "time_in_force": "GTX",
        }
        report = {
            "symbol": "BILLUSDT",
            "forced_reduce_orders": [forced_reduce],
            "bootstrap_orders": [],
            "missing_orders": [
                dict(forced_reduce),
                {"side": "BUY", "price": 0.14289, "qty": 909.0, "notional": 129.88701, "role": "best_quote_entry_long"},
            ],
            "stale_orders": [],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["place_count"], 2)
        self.assertEqual([item["role"] for item in actions["place_orders"]], [
            "best_quote_adverse_reduce_long",
            "best_quote_entry_long",
        ])
        self.assertAlmostEqual(actions["place_notional"], 260.06301, places=8)

    def test_build_execution_actions_excludes_manual_stale_orders(self) -> None:
        report = {
            "symbol": "BTCUSDC",
            "bootstrap_orders": [],
            "missing_orders": [],
            "stale_orders": [
                {"orderId": 1, "clientOrderId": "gx-btcusdc-entry-1", "side": "BUY", "price": "1", "origQty": "1"},
                {"orderId": 2, "clientOrderId": "mt_btcusdc_openlong_b_1", "side": "BUY", "price": "1", "origQty": "1"},
            ],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["cancel_orders"], [report["stale_orders"][0]])

    def test_sort_cancel_orders_farthest_from_market_first_for_buy_and_sell(self) -> None:
        actions = {
            "place_orders": [],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.15390"},
                {"orderId": 2, "side": "BUY", "price": "0.15370"},
                {"orderId": 3, "side": "SELL", "price": "0.15420"},
                {"orderId": 4, "side": "SELL", "price": "0.15440"},
            ],
            "place_count": 0,
            "cancel_count": 4,
        }

        adjusted = sort_cancel_orders_farthest_from_market_first(
            actions=actions,
            live_bid_price=0.15400,
            live_ask_price=0.15410,
        )

        self.assertEqual([item["orderId"] for item in adjusted["cancel_orders"]], [4, 2, 3, 1])

    def test_sort_cancel_orders_keeps_urgent_cancel_reason_first(self) -> None:
        actions = {
            "place_orders": [],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.15370"},
                {"orderId": 2, "side": "SELL", "price": "0.15420", "cancel_reason": "urgent_reduce_only_displaces_take_profit"},
                {"orderId": 3, "side": "SELL", "price": "0.15440"},
            ],
            "place_count": 0,
            "cancel_count": 3,
        }

        adjusted = sort_cancel_orders_farthest_from_market_first(
            actions=actions,
            live_bid_price=0.15400,
            live_ask_price=0.15410,
        )

        self.assertEqual([item["orderId"] for item in adjusted["cancel_orders"]], [2, 3, 1])
        self.assertEqual(adjusted["cancel_count"], 3)

    def test_suppress_same_side_nearby_place_orders_keeps_one_per_spacing_band(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.15900, "qty": 720.0, "notional": 114.48, "role": "best_quote_entry_long"},
                {"side": "BUY", "price": 0.15900, "qty": 720.0, "notional": 114.48, "role": "best_quote_entry_long"},
                {"side": "BUY", "price": 0.15900, "qty": 100.0, "notional": 15.9, "role": "best_quote_reduce_short", "force_reduce_only": True},
                {"side": "SELL", "price": 0.16087, "qty": 714.0, "notional": 114.86118, "role": "best_quote_entry_short"},
                {"side": "SELL", "price": 0.16106, "qty": 714.0, "notional": 114.99684, "role": "best_quote_entry_short"},
            ],
            "cancel_orders": [],
            "place_count": 4,
            "cancel_count": 0,
        }

        guarded = suppress_same_side_nearby_place_orders(
            actions=actions,
            min_price_spacing=0.00019,
            live_bid_price=0.16090,
            live_ask_price=0.16091,
            tick_size=0.00001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertEqual(guarded["place_count"], 3)
        self.assertEqual([(item["side"], item["price"]) for item in guarded["place_orders"]], [
            ("BUY", 0.15900),
            ("BUY", 0.15900),
            ("SELL", 0.16091),
        ])
        self.assertEqual(guarded["same_side_spacing_guard"]["suppressed_place_count"], 2)

    def test_suppress_same_side_nearby_place_orders_preserves_existing_queue(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 0.16006, "qty": 718.0, "notional": 114.92308, "role": "best_quote_entry_short"}
            ],
            "cancel_orders": [
                {
                    "orderId": 419310803,
                    "clientOrderId": "gx-billu-bestquot-2-12628113",
                    "side": "SELL",
                    "price": "0.16014",
                    "origQty": "717.0",
                    "positionSide": "BOTH",
                }
            ],
            "place_count": 1,
            "cancel_count": 1,
        }
        current_open_orders = [
            {
                "orderId": 419310803,
                "clientOrderId": "gx-billu-bestquot-2-12628113",
                "side": "SELL",
                "price": "0.16014",
                "origQty": "717.0",
                "positionSide": "BOTH",
            }
        ]

        guarded = suppress_same_side_nearby_place_orders(
            actions=actions,
            current_open_orders=current_open_orders,
            min_price_spacing=0.00019,
            live_bid_price=0.15993,
            live_ask_price=0.15994,
            tick_size=0.00001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertEqual(guarded["place_count"], 0)
        self.assertEqual(guarded["cancel_count"], 0)
        guard = guarded["same_side_spacing_guard"]
        self.assertEqual(guard["suppressed_place_orders"][0]["defer_reason"], "existing_same_side_nearby_open_order")
        self.assertEqual(guard["protected_cancel_count"], 1)

    def test_reduce_only_cap_counts_orders_pending_cancel_until_exchange_releases_qty(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.1674, "qty": 358.0, "notional": 59.9292, "role": "take_profit_short"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.1676", "origQty": "358", "reduceOnly": True}
            ],
            "place_count": 1,
            "cancel_count": 1,
        }
        current_open_orders = [
            {
                "orderId": 1,
                "side": "BUY",
                "price": "0.1676",
                "origQty": "358",
                "executedQty": "0",
                "reduceOnly": True,
            }
        ]

        capped = cap_reduce_only_place_orders_to_position(
            actions=actions,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-358.0,
            current_open_orders=current_open_orders,
        )

        self.assertEqual(capped["place_orders"], [])
        self.assertEqual(capped["cancel_orders"], actions["cancel_orders"])
        self.assertEqual(capped["place_count"], 0)
        self.assertEqual(capped["cancel_count"], 1)
        self.assertEqual(capped["reduce_only_position_cap"]["remaining_buy_reduce_qty"], 0.0)
        self.assertEqual(capped["reduce_only_position_cap"]["dropped_order_count"], 1)

    def test_reduce_only_cap_treats_flow_sleeve_short_as_reduce_only_buy(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.1690, "qty": 150.0, "notional": 25.35, "role": "flow_sleeve_short"},
                {"side": "BUY", "price": 0.1688, "qty": 150.0, "notional": 25.32, "role": "flow_sleeve_short"},
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
        }

        capped = cap_reduce_only_place_orders_to_position(
            actions=actions,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-200.0,
            current_open_orders=[],
        )

        self.assertEqual(capped["place_count"], 2)
        self.assertEqual(capped["place_orders"][0]["qty"], 150.0)
        self.assertEqual(capped["place_orders"][1]["qty"], 50.0)
        self.assertEqual(capped["reduce_only_position_cap"]["resized_order_count"], 1)

    def test_loss_inventory_guard_drops_losing_short_buy_above_recovery_ceiling(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.1450, "qty": 1000.0, "notional": 145.0, "role": "entry_long"}
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -900.0,
            "unrealized_pnl": -10.0,
            "current_short_avg_price": 0.1430,
            "take_profit_min_profit_ratio": 0.0002,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_orders"], [])
        self.assertEqual(guarded["loss_inventory_no_cross_entry_guard"]["dropped_order_count"], 1)

    def test_loss_inventory_guard_drops_loss_entry_cross_and_allows_reduce_only_brush(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 0.1400, "qty": 100.0, "notional": 14.0, "role": "entry_short"},
                {"side": "SELL", "price": 0.1400, "qty": 200.0, "notional": 28.0, "role": "entry_short"},
                {
                    "side": "SELL",
                    "price": 0.1400,
                    "qty": 100.0,
                    "notional": 14.0,
                    "role": "active_delever_long",
                    "force_reduce_only": True,
                    "execution_type": "maker_timeout_release",
                },
            ],
            "cancel_orders": [],
            "place_count": 3,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1000.0,
            "unrealized_pnl": -5.0,
            "current_long_avg_price": 0.1420,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "active_delever_long")
        self.assertEqual(
            guarded["place_orders"][0]["loss_inventory_no_cross_guard"],
            "long_small_loss_reduce_allowed",
        )
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["resized_small_loss_reduce_count"], 0)
        self.assertEqual(guard_report["dropped_order_count"], 2)

    def test_loss_inventory_guard_converts_profitable_short_cover_and_cap_prevents_cross(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.1429, "qty": 1000.0, "notional": 142.9, "role": "entry_long"}
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -700.0,
            "unrealized_pnl": -1.0,
            "current_short_avg_price": 0.1430,
            "take_profit_min_profit_ratio": 0.0002,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )
        capped = cap_reduce_only_place_orders_to_position(
            actions=guarded,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-700.0,
            current_open_orders=[],
        )

        self.assertEqual(capped["place_count"], 1)
        self.assertEqual(capped["place_orders"][0]["qty"], 700.0)
        self.assertIs(capped["place_orders"][0]["force_reduce_only"], True)
        self.assertEqual(capped["reduce_only_position_cap"]["resized_order_count"], 1)

    def test_loss_inventory_guard_drops_non_urgent_reduce_only_long_below_recovery_floor(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.1405,
                    "qty": 200.0,
                    "notional": 28.1,
                    "role": "adverse_reduce_long",
                    "force_reduce_only": True,
                    "execution_type": "maker_timeout_release",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1000.0,
            "unrealized_pnl": -5.0,
            "current_long_avg_price": 0.1420,
            "take_profit_min_profit_ratio": 0.001,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_orders"], [])
        self.assertEqual(guarded["loss_inventory_no_cross_entry_guard"]["dropped_order_count"], 1)
        self.assertEqual(
            guarded["loss_inventory_no_cross_entry_guard"]["dropped_orders"][0][
                "loss_inventory_no_cross_drop_reason"
            ],
            "losing_long_sell_below_recovery_floor",
        )

    def test_loss_inventory_guard_allows_small_reduce_only_long_brush_below_recovery_floor(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.1405,
                    "qty": 100.0,
                    "notional": 14.05,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1000.0,
            "unrealized_pnl": -5.0,
            "current_long_avg_price": 0.1420,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "best_quote_reduce_long")
        self.assertEqual(
            guarded["place_orders"][0]["loss_inventory_no_cross_guard"],
            "long_small_loss_reduce_allowed",
        )
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 0)

    def test_loss_inventory_guard_shrinks_large_reduce_only_long_brush_below_recovery_floor(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.1405,
                    "qty": 200.0,
                    "quantity": 200.0,
                    "notional": 28.1,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1000.0,
            "unrealized_pnl": -5.0,
            "current_long_avg_price": 0.1420,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        order = guarded["place_orders"][0]
        self.assertEqual(order["loss_inventory_no_cross_guard"], "long_small_loss_reduce_resized")
        self.assertEqual(order["qty"], 106.0)
        self.assertEqual(order["quantity"], 106.0)
        self.assertLessEqual(order["notional"], 15.0)
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["resized_small_loss_reduce_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 0)

    def test_loss_inventory_guard_drops_entry_short_that_would_loss_reduce_losing_long(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.1405,
                    "qty": 200.0,
                    "quantity": 200.0,
                    "notional": 28.1,
                    "role": "best_quote_entry_short",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1000.0,
            "unrealized_pnl": -5.0,
            "current_long_avg_price": 0.1420,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 0)
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["resized_small_loss_reduce_count"], 0)
        self.assertEqual(guard_report["dropped_order_count"], 1)
        self.assertEqual(
            guard_report["dropped_orders"][0]["loss_inventory_no_cross_drop_reason"],
            "losing_long_sell_below_recovery_floor",
        )

    def test_loss_inventory_guard_allows_best_quote_short_entry_when_losing_long_is_dust(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.5783,
                    "qty": 48.0,
                    "quantity": 48.0,
                    "notional": 27.7584,
                    "role": "best_quote_entry_short",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1.0,
            "unrealized_pnl": -0.0081,
            "current_long_avg_price": 0.5861,
            "take_profit_min_profit_ratio": 0.00008,
            "loss_inventory_no_cross_small_entry_notional": 30.0,
            "symbol_info": {"min_notional": 5.0},
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        order = guarded["place_orders"][0]
        self.assertEqual(order["role"], "best_quote_entry_short")
        self.assertNotIn("force_reduce_only", order)
        self.assertEqual(order["loss_inventory_no_cross_guard"], "long_dust_cross_allowed")
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 0)

    def test_loss_inventory_guard_allows_small_reduce_only_short_brush_above_recovery_ceiling(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 0.1450,
                    "qty": 100.0,
                    "notional": 14.5,
                    "role": "best_quote_reduce_short",
                    "force_reduce_only": True,
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -1000.0,
            "unrealized_pnl": -5.0,
            "current_short_avg_price": 0.1430,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(
            guarded["place_orders"][0]["loss_inventory_no_cross_guard"],
            "short_small_loss_reduce_allowed",
        )
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 0)

    def test_loss_inventory_guard_shrinks_large_reduce_only_short_brush_above_recovery_ceiling(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 0.1450,
                    "qty": 200.0,
                    "quantity": 200.0,
                    "notional": 29.0,
                    "role": "best_quote_reduce_short",
                    "force_reduce_only": True,
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -1000.0,
            "unrealized_pnl": -5.0,
            "current_short_avg_price": 0.1430,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        order = guarded["place_orders"][0]
        self.assertEqual(order["loss_inventory_no_cross_guard"], "short_small_loss_reduce_resized")
        self.assertEqual(order["qty"], 103.0)
        self.assertEqual(order["quantity"], 103.0)
        self.assertLessEqual(order["notional"], 15.0)
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["resized_small_loss_reduce_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 0)

    def test_loss_inventory_guard_drops_entry_long_that_would_loss_reduce_losing_short(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 0.1450,
                    "qty": 200.0,
                    "quantity": 200.0,
                    "notional": 29.0,
                    "role": "best_quote_entry_long",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -1000.0,
            "unrealized_pnl": -5.0,
            "current_short_avg_price": 0.1430,
            "take_profit_min_profit_ratio": 0.001,
            "loss_inventory_no_cross_small_entry_notional": 15.0,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 0)
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["resized_small_loss_reduce_count"], 0)
        self.assertEqual(guard_report["dropped_order_count"], 1)
        self.assertEqual(
            guard_report["dropped_orders"][0]["loss_inventory_no_cross_drop_reason"],
            "losing_short_buy_above_recovery_ceiling",
        )

    def test_loss_inventory_guard_allows_best_quote_long_entry_when_losing_short_is_dust(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 0.5780,
                    "qty": 48.0,
                    "quantity": 48.0,
                    "notional": 27.744,
                    "role": "best_quote_entry_long",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -1.0,
            "unrealized_pnl": -0.0081,
            "current_short_avg_price": 0.5700,
            "take_profit_min_profit_ratio": 0.00008,
            "loss_inventory_no_cross_small_entry_notional": 30.0,
            "symbol_info": {"min_notional": 5.0},
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        order = guarded["place_orders"][0]
        self.assertEqual(order["role"], "best_quote_entry_long")
        self.assertNotIn("force_reduce_only", order)
        self.assertEqual(order["loss_inventory_no_cross_guard"], "short_dust_cross_allowed")
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_small_entry_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 0)

    def test_loss_inventory_guard_drops_best_quote_short_entry_during_losing_short_uptrend(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.16053,
                    "qty": 716.0,
                    "quantity": 716.0,
                    "notional": 114.9,
                    "role": "best_quote_entry_short",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -26909.0,
            "unrealized_pnl": -17.0,
            "current_short_avg_price": 0.1598576369652,
            "step_price": 0.00019,
            "mid_price": 0.160515,
            "market_guard": {"return_ratio": 0.00337},
            "take_profit_min_profit_ratio": 0.0003,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="best_quote_maker_volume_v1",
        )

        self.assertEqual(guarded["place_count"], 0)
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["dropped_order_count"], 1)
        self.assertEqual(
            guard_report["dropped_orders"][0]["loss_inventory_no_cross_drop_reason"],
            "losing_short_adverse_uptrend",
        )

    def test_loss_inventory_guard_allows_best_quote_short_entry_only_after_cost_gap(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.16004,
                    "qty": 718.0,
                    "quantity": 718.0,
                    "notional": 114.9,
                    "role": "best_quote_entry_short",
                },
                {
                    "side": "SELL",
                    "price": 0.16020,
                    "qty": 717.0,
                    "quantity": 717.0,
                    "notional": 114.9,
                    "role": "best_quote_entry_short",
                },
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": -26909.0,
            "unrealized_pnl": -17.0,
            "current_short_avg_price": 0.1598576369652,
            "step_price": 0.00019,
            "mid_price": 0.16010,
            "market_guard": {"return_ratio": 0.0},
            "take_profit_min_profit_ratio": 0.0003,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="best_quote_maker_volume_v1",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertAlmostEqual(guarded["place_orders"][0]["price"], 0.16020)
        self.assertEqual(
            guarded["place_orders"][0]["loss_inventory_no_cross_guard"],
            "short_same_side_entry_allowed",
        )
        guard_report = guarded["loss_inventory_no_cross_entry_guard"]
        self.assertEqual(guard_report["allowed_same_side_entry_count"], 1)
        self.assertEqual(guard_report["dropped_order_count"], 1)
        self.assertEqual(
            guard_report["dropped_orders"][0]["loss_inventory_no_cross_drop_reason"],
            "losing_short_sell_below_entry_floor",
        )

    def test_loss_inventory_guard_keeps_hard_loss_forced_reduce_below_recovery_floor(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.1405,
                    "qty": 200.0,
                    "notional": 28.1,
                    "role": "hard_loss_forced_reduce_long",
                    "force_reduce_only": True,
                    "execution_type": "aggressive",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        report = {
            "actual_net_qty": 1000.0,
            "unrealized_pnl": -80.0,
            "current_long_avg_price": 0.1420,
            "take_profit_min_profit_ratio": 0.001,
        }

        guarded = apply_loss_inventory_no_cross_entry_guard_to_actions(
            actions=actions,
            plan_report=report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "hard_loss_forced_reduce_long")
        self.assertEqual(guarded["loss_inventory_no_cross_entry_guard"]["dropped_order_count"], 0)

    def test_urgent_reduce_only_displaces_existing_take_profit_capacity(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 2.382,
                    "qty": 283.6,
                    "notional": 675.5352,
                    "role": "active_delever_short",
                    "force_reduce_only": True,
                    "execution_type": "maker_timeout_release",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        current_open_orders = [
            {
                "orderId": 1019001946,
                "side": "BUY",
                "price": "2.360000",
                "origQty": "283.60",
                "executedQty": "0",
                "reduceOnly": True,
                "clientOrderId": "gx-trumpusdc-takeprof-1-89452771",
            }
        ]

        capped = cap_reduce_only_place_orders_to_position(
            actions=actions,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-283.6,
            current_open_orders=current_open_orders,
        )

        self.assertEqual(capped["place_count"], 1)
        self.assertEqual(capped["cancel_count"], 1)
        self.assertEqual(capped["place_orders"][0]["role"], "active_delever_short")
        self.assertEqual(capped["cancel_orders"][0]["orderId"], 1019001946)
        self.assertEqual(capped["cancel_orders"][0]["cancel_reason"], "urgent_reduce_only_displaces_take_profit")
        self.assertEqual(capped["reduce_only_position_cap"]["dropped_order_count"], 0)
        self.assertEqual(capped["reduce_only_position_cap"]["displaced_order_count"], 1)

    def test_queue_priority_preserves_urgent_forced_reduce_order(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "SELL",
                    "price": 0.15186,
                    "qty": 526.0,
                    "notional": 79.87836,
                    "role": "hard_loss_forced_reduce_long",
                    "force_reduce_only": True,
                    "execution_type": "aggressive",
                    "time_in_force": "IOC",
                },
                {
                    "side": "SELL",
                    "price": 0.15186,
                    "qty": 526.0,
                    "notional": 79.87836,
                    "role": "take_profit_long",
                },
            ],
            "cancel_orders": [
                {
                    "orderId": 1,
                    "side": "SELL",
                    "price": "0.15186",
                    "origQty": "526",
                    "positionSide": "BOTH",
                }
            ],
            "place_count": 2,
            "cancel_count": 1,
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.15186,
            live_ask_price=0.15187,
            tick_size=0.00001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertEqual(adjusted["place_orders"][0]["role"], "hard_loss_forced_reduce_long")
        self.assertEqual(adjusted["place_orders"][0]["execution_type"], "aggressive")
        self.assertEqual(adjusted["place_orders"][0]["time_in_force"], "IOC")
        self.assertTrue(adjusted["place_orders"][0]["force_reduce_only"])

    def test_anti_chase_guard_drops_long_entries_but_keeps_reduce_only_sells(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 2.42, "qty": 10.0, "notional": 24.2, "role": "entry"},
                {
                    "side": "SELL",
                    "price": 2.43,
                    "qty": 10.0,
                    "notional": 24.3,
                    "role": "active_delever_long",
                    "force_reduce_only": True,
                },
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
            "place_notional": 48.5,
        }
        plan_report = {
            "anti_chase_entry_guard": {
                "enabled": True,
                "block_long_entries": True,
                "block_short_entries": False,
                "long_reason": "window_1m return=0.30% >= 0.25%",
            }
        }

        guarded = apply_anti_chase_entry_guard_to_actions(
            actions=actions,
            plan_report=plan_report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "active_delever_long")
        self.assertEqual(guarded["anti_chase_entry_guard"]["dropped_order_count"], 1)

    def test_anti_chase_guard_drops_short_entries_but_keeps_short_exits(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 2.30, "qty": 10.0, "notional": 23.0, "role": "entry_short"},
                {"side": "BUY", "price": 2.29, "qty": 10.0, "notional": 22.9, "role": "take_profit_short"},
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
            "place_notional": 45.9,
        }
        plan_report = {
            "anti_chase_entry_guard": {
                "enabled": True,
                "block_long_entries": False,
                "block_short_entries": True,
                "short_reason": "window_1m return=-0.30% <= -0.25%",
            }
        }

        guarded = apply_anti_chase_entry_guard_to_actions(
            actions=actions,
            plan_report=plan_report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "take_profit_short")
        self.assertEqual(guarded["anti_chase_entry_guard"]["dropped_order_count"], 1)

    def test_hard_loss_rescue_guard_blocks_same_direction_entries_only(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 0.1565,
                    "qty": 766.0,
                    "notional": 119.879,
                    "role": "hard_loss_forced_reduce_short",
                    "force_reduce_only": True,
                    "execution_type": "aggressive",
                    "time_in_force": "IOC",
                },
                {"side": "BUY", "price": 0.1540, "qty": 843.0, "notional": 129.822, "role": "take_profit_short"},
                {"side": "SELL", "price": 0.1600, "qty": 812.0, "notional": 129.92, "role": "entry_short"},
                {"side": "BUY", "price": 0.1545, "qty": 841.0, "notional": 129.9345, "role": "entry_long"},
            ],
            "cancel_orders": [],
            "place_count": 4,
            "cancel_count": 0,
            "place_notional": 509.5555,
        }
        plan_report = {
            "hard_loss_rescue_entry_guard": {
                "active": True,
                "block_short_entries": True,
                "block_long_entries": False,
                "reason": "protect_window_remaining=120s",
            }
        }

        guarded = apply_hard_loss_rescue_entry_guard_to_actions(
            actions=actions,
            plan_report=plan_report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(
            [item["role"] for item in guarded["place_orders"]],
            ["hard_loss_forced_reduce_short", "take_profit_short", "entry_long"],
        )
        self.assertEqual(guarded["place_count"], 3)
        guard = guarded["hard_loss_rescue_entry_guard"]
        self.assertEqual(guard["dropped_order_count"], 1)
        self.assertEqual(guard["dropped_orders"][0]["role"], "entry_short")

    def test_deferred_action_limits_allow_capped_reduce_only_orders(self) -> None:
        now = datetime(2026, 5, 5, 0, 20, tzinfo=timezone.utc)
        report = {
            "symbol": "CHIPUSDT",
            "generated_at": now.isoformat(),
            "dual_side_position": False,
            "bootstrap_orders": [],
            "missing_orders": [
                {
                    "side": "BUY",
                    "price": 0.06064,
                    "qty": 5904.0,
                    "notional": 358.01856,
                    "role": "active_delever_short",
                    "force_reduce_only": True,
                },
                {
                    "side": "BUY",
                    "price": 0.06044,
                    "qty": 5904.0,
                    "notional": 356.83776,
                    "role": "take_profit_short",
                },
            ],
            "stale_orders": [],
        }

        validation = validate_plan_report(
            plan_report=report,
            allow_symbol="CHIPUSDT",
            max_new_orders=10,
            max_total_notional=360.0,
            cancel_stale=True,
            max_plan_age_seconds=60,
            now=now,
            enforce_place_limits=False,
        )
        validation["actions"] = cap_reduce_only_place_orders_to_position(
            actions=validation["actions"],
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-5904.0,
            current_open_orders=[],
        )
        validation = enforce_execution_action_limits(
            validation=validation,
            max_new_orders=10,
            max_total_notional=360.0,
        )

        self.assertTrue(validation["ok"])
        self.assertEqual(validation["actions"]["place_count"], 1)
        self.assertAlmostEqual(validation["actions"]["place_notional"], 358.01856, places=8)
        self.assertEqual(validation["actions"]["reduce_only_position_cap"]["dropped_order_count"], 1)

    def test_preserve_queue_priority_drops_replace_when_post_only_projects_back_to_same_bucket(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 500.0, "notional": 25.32, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"}
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["place_count"], 0)
        self.assertEqual(adjusted["cancel_count"], 0)

    def test_preserve_queue_priority_only_places_delta_when_post_only_projects_back_to_same_bucket(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 650.0, "notional": 32.916, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"}
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["cancel_count"], 0)
        self.assertEqual(adjusted["place_count"], 0)

    def test_preserve_queue_priority_keeps_replace_when_projected_bucket_needs_smaller_qty(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 350.0, "notional": 17.724, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"}
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["place_count"], 0)
        self.assertEqual(adjusted["same_bucket_cancel_place_guard"]["deferred_place_count"], 1)

    def test_preserve_queue_priority_keeps_exact_same_bucket_subset_when_qty_shrinks(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.0564, "qty": 886.0, "notional": 49.9704, "role": "take_profit_short"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.0564", "origQty": "884", "positionSide": "BOTH"},
                {"orderId": 2, "side": "BUY", "price": "0.0564", "origQty": "886", "positionSide": "BOTH"},
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.0564,
            live_ask_price=0.0565,
            tick_size=0.0001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["place_count"], 0)
        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["cancel_orders"][0]["orderId"], 1)

    def test_preserve_queue_priority_keeps_subset_without_same_bucket_delta(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 650.0, "notional": 32.916, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"},
                {"orderId": 2, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"},
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["place_count"], 0)

    def test_preserve_queue_priority_defers_same_bucket_place_while_cancel_pending(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 0.14379, "qty": 20.0, "notional": 2.8758, "role": "entry_short"},
                {"side": "BUY", "price": 0.14200, "qty": 35.0, "notional": 4.97, "role": "entry_long"},
            ],
            "cancel_orders": [
                {
                    "orderId": 1,
                    "side": "SELL",
                    "price": "0.14379",
                    "origQty": "35",
                    "positionSide": "BOTH",
                    "clientOrderId": "gx-billu-entrysho-1-11111111",
                },
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.14250,
            live_ask_price=0.14378,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=0.0,
            step_size=1.0,
        )

        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["place_count"], 1)
        self.assertEqual(adjusted["place_orders"][0]["side"], "BUY")
        guard = adjusted["same_bucket_cancel_place_guard"]
        self.assertEqual(guard["deferred_place_count"], 1)
        self.assertEqual(guard["deferred_place_orders"][0]["role"], "entry_short")

    def test_preserve_queue_priority_merges_duplicate_take_profit_place_buckets(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 0.14362, "qty": 74.0, "notional": 10.62788, "role": "take_profit_long"},
                {"side": "SELL", "price": 0.14362, "qty": 974.0, "notional": 139.912, "role": "take_profit_long"},
            ],
            "cancel_orders": [],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.14200,
            live_ask_price=0.14210,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=0.0,
            step_size=1.0,
        )

        self.assertEqual(adjusted["place_count"], 1)
        self.assertEqual(adjusted["place_orders"][0]["side"], "SELL")
        self.assertAlmostEqual(adjusted["place_orders"][0]["price"], 0.14362, places=8)
        self.assertAlmostEqual(adjusted["place_orders"][0]["qty"], 1048.0, places=8)
        guard = adjusted["duplicate_place_bucket_guard"]
        self.assertEqual(guard["merged_order_count"], 1)
        self.assertEqual(guard["merged_orders"][0]["role"], "take_profit_long")

    def test_suppress_place_orders_when_submitted_bucket_already_open(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 0.14567, "qty": 1029.0, "notional": 149.9, "role": "entry_short"},
                {"side": "SELL", "price": 0.14629, "qty": 1025.0, "notional": 149.9, "role": "entry_short"},
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
            "place_notional": 299.8,
        }
        current_open_orders = [
            {
                "orderId": 1,
                "clientOrderId": "gx-billu-entrysho-1-11111111",
                "side": "SELL",
                "price": "0.1456700",
                "origQty": "1029",
                "positionSide": "BOTH",
            }
        ]

        adjusted = suppress_place_orders_with_existing_submitted_buckets(
            actions=actions,
            current_open_orders=current_open_orders,
            live_bid_price=0.14200,
            live_ask_price=0.14210,
            tick_size=0.00001,
            min_qty=1.0,
            min_notional=0.0,
            step_size=1.0,
        )

        self.assertEqual(adjusted["place_count"], 1)
        self.assertEqual(adjusted["place_orders"][0]["price"], 0.14629)
        guard = adjusted["existing_submitted_bucket_guard"]
        self.assertEqual(guard["suppressed_place_count"], 1)
        self.assertEqual(guard["suppressed_place_orders"][0]["role"], "entry_short")

    def test_validate_plan_report_rejects_old_plan_and_stale_orders_without_flag(self) -> None:
        now = datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc)
        report = {
            "symbol": "NIGHTUSDT",
            "generated_at": (now - timedelta(seconds=120)).isoformat(),
            "dual_side_position": False,
            "bootstrap_orders": [],
            "missing_orders": [{"side": "BUY", "price": 0.05029, "qty": 502.0, "notional": 25.24558}],
            "stale_orders": [
                {"orderId": 1, "clientOrderId": "gx-nightu-entry-1", "side": "BUY", "price": "0.05001", "origQty": "500"}
            ],
        }

        validation = validate_plan_report(
            plan_report=report,
            allow_symbol="NIGHTUSDT",
            max_new_orders=24,
            max_total_notional=500.0,
            cancel_stale=False,
            max_plan_age_seconds=60,
            now=now,
        )

        self.assertFalse(validation["ok"])
        self.assertEqual(len(validation["errors"]), 2)

    def test_validate_plan_report_rejects_symbol_and_notional_limit(self) -> None:
        now = datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc)
        report = {
            "symbol": "ENSOUSDT",
            "generated_at": now.isoformat(),
            "dual_side_position": False,
            "bootstrap_orders": [{"side": "BUY", "price": 1.2, "qty": 100.0, "notional": 120.0}],
            "missing_orders": [{"side": "BUY", "price": 1.1, "qty": 100.0, "notional": 110.0}],
            "stale_orders": [],
        }

        validation = validate_plan_report(
            plan_report=report,
            allow_symbol="NIGHTUSDT",
            max_new_orders=1,
            max_total_notional=200.0,
            cancel_stale=True,
            max_plan_age_seconds=60,
            now=now,
        )

        self.assertFalse(validation["ok"])
        self.assertEqual(len(validation["errors"]), 3)

    def test_estimate_mid_drift_steps(self) -> None:
        drift = estimate_mid_drift_steps(
            report_mid_price=0.05043,
            live_bid_price=0.05047,
            live_ask_price=0.05048,
            step_price=0.00002,
        )

        self.assertAlmostEqual(drift, 2.25, places=8)

    def test_adjust_post_only_price_moves_inside_resting_side(self) -> None:
        buy_price = adjust_post_only_price(
            desired_price=0.05064,
            side="BUY",
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
        )
        sell_price = adjust_post_only_price(
            desired_price=0.05060,
            side="SELL",
            live_bid_price=0.05058,
            live_ask_price=0.05059,
            tick_size=0.00001,
        )

        self.assertAlmostEqual(buy_price, 0.05062, places=8)
        self.assertAlmostEqual(sell_price, 0.05060, places=8)

    def test_prepare_post_only_order_request_skips_order_below_min_notional_after_price_adjustment(self) -> None:
        prepared, skipped = prepare_post_only_order_request(
            order={"qty": 9.9, "price": 0.51},
            side="BUY",
            live_bid_price=0.49,
            live_ask_price=0.51,
            tick_size=0.01,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertIsNone(prepared)
        self.assertEqual(skipped["reason"], "submitted_notional_below_min_notional")
        self.assertAlmostEqual(skipped["submitted_price"], 0.50, places=8)
        self.assertAlmostEqual(skipped["submitted_notional"], 4.95, places=8)

    def test_prepare_post_only_order_request_raises_entry_qty_to_min_notional(self) -> None:
        prepared, skipped = prepare_post_only_order_request(
            order={"role": "entry_long", "qty": 24.0, "price": 0.172},
            side="BUY",
            live_bid_price=0.1719,
            live_ask_price=0.1721,
            tick_size=0.0001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertIsNone(skipped)
        self.assertEqual(prepared["qty"], 30.0)
        self.assertAlmostEqual(prepared["submitted_price"], 0.172, places=8)
        self.assertAlmostEqual(prepared["submitted_notional"], 5.16, places=8)
        self.assertTrue(prepared["qty_bumped_to_min_order"])

    def test_prepare_post_only_order_request_does_not_raise_take_profit_qty_to_min_notional(self) -> None:
        prepared, skipped = prepare_post_only_order_request(
            order={"role": "take_profit_long", "qty": 24.0, "price": 0.172},
            side="SELL",
            live_bid_price=0.1719,
            live_ask_price=0.1721,
            tick_size=0.0001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertIsNone(prepared)
        self.assertEqual(skipped["reason"], "submitted_notional_below_min_notional")


if __name__ == "__main__":
    unittest.main()
