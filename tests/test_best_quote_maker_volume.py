from __future__ import annotations

import unittest

from grid_optimizer.best_quote_maker_volume import (
    BestQuoteMakerVolumeConfig,
    BestQuoteMakerVolumeInputs,
    build_best_quote_maker_volume_plan,
    _prune_clustered_same_side_entry_orders,
)


def _inputs(**overrides):
    data = {
        "bid_price": 80400.0,
        "ask_price": 80400.1,
        "mid_price": 80400.05,
        "current_net_qty": 0.0,
        "cycle_budget_notional": 400.0,
        "loss_per_10k_15m": 0.2,
        "target_volume_remaining": 10_000.0,
        "tick_size": 0.1,
        "step_size": 0.001,
        "min_qty": 0.001,
        "min_notional": 5.0,
    }
    data.update(overrides)
    return BestQuoteMakerVolumeInputs(**data)


class BestQuoteMakerVolumeTests(unittest.TestCase):
    def test_flat_inventory_quotes_both_sides_near_best_quote(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True),
            inputs=_inputs(),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["buy_orders"][0]["price"], 80400.0)
        self.assertEqual(plan["sell_orders"][0]["price"], 80400.1)
        self.assertEqual(plan["buy_orders"][0]["execution_type"], "maker")
        self.assertTrue(plan["buy_orders"][0]["post_only"])

    def test_long_inventory_biases_toward_reduce_long(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True, max_long_notional=1_000.0),
            inputs=_inputs(current_net_qty=0.009),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_reduce_long")
        self.assertEqual(plan["sell_orders"][0]["price"], 80400.4)
        self.assertTrue(plan["sell_orders"][0]["force_reduce_only"])

    def test_open_entry_exposure_blocks_extra_long_entry_before_hard_limit(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=1_500.0,
                inventory_soft_ratio=0.95,
            ),
            inputs=_inputs(
                current_net_qty=0.0,
                open_entry_long_notional=1_250.0,
                pending_entry_buffer_notional=400.0,
            ),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        self.assertIn("open_entry_exposure", plan["reasons"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(plan["metrics"]["projected_long_entry_notional"], 1_650.0)

    def test_open_entry_exposure_caps_new_long_entry_to_remaining_capacity(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=1_500.0,
                inventory_soft_ratio=1.0,
            ),
            inputs=_inputs(
                cycle_budget_notional=600.0,
                open_entry_long_notional=1_150.0,
                pending_entry_buffer_notional=100.0,
            ),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertLessEqual(plan["buy_orders"][0]["notional"], 250.0)

    def test_high_loss_switches_to_defensive_and_keeps_only_reduce_side(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True, loss_per_10k_hard=0.8),
            inputs=_inputs(current_net_qty=-0.006, loss_per_10k_15m=0.95),
        )

        self.assertEqual(plan["regime"], "loss_defensive")
        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(plan["sell_orders"], [])

    def test_soft_loss_widens_quotes_and_reduces_budget(self) -> None:
        normal = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True),
            inputs=_inputs(loss_per_10k_15m=0.2),
        )
        soft = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True, loss_per_10k_soft=0.5),
            inputs=_inputs(loss_per_10k_15m=0.6),
        )

        self.assertEqual(soft["regime"], "loss_soft")
        self.assertLess(soft["planned_notional"], normal["planned_notional"])
        self.assertLess(soft["buy_orders"][0]["price"], normal["buy_orders"][0]["price"])
        self.assertGreater(soft["sell_orders"][0]["price"], normal["sell_orders"][0]["price"])

    def test_dynamic_tick_tightens_when_loss_and_inventory_are_low(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                dynamic_tick_enabled=True,
                dynamic_tick_tight_offset_ticks=2,
                dynamic_tick_low_loss_per_10k=3.0,
            ),
            inputs=_inputs(loss_per_10k_15m=0.2),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertEqual(plan["buy_orders"][0]["price"], 80399.8)
        self.assertEqual(plan["sell_orders"][0]["price"], 80400.3)
        self.assertEqual(plan["metrics"]["dynamic_tick"]["offset_ticks"], 2)
        self.assertEqual(plan["metrics"]["dynamic_tick"]["reason"], "low_loss_low_inventory_tighten")

    def test_inventory_bias_keeps_small_same_side_entry_while_reducing_short(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_short_notional=1_500.0,
                inventory_soft_ratio=0.6,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.25,
                inventory_bias_reduce_share=0.7,
                inventory_bias_same_side_extra_ticks=2,
                inventory_bias_reduce_extra_ticks=-1,
            ),
            inputs=_inputs(current_net_qty=-0.004),
        )

        self.assertEqual(plan["regime"], "inventory_bias")
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertTrue(plan["buy_orders"][0]["force_reduce_only"])
        self.assertEqual(plan["buy_orders"][0]["price"], 80399.8)
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(plan["sell_orders"][0]["price"], 80400.6)
        self.assertGreater(plan["buy_orders"][0]["notional"], plan["sell_orders"][0]["notional"])
        self.assertTrue(plan["metrics"]["inventory_bias"]["applied"])

    def test_multiple_entry_orders_are_spaced_by_strategy_step(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True, max_entry_orders_per_side=2),
            inputs=_inputs(
                bid_price=0.15968,
                ask_price=0.15969,
                mid_price=0.159685,
                cycle_budget_notional=460.0,
                tick_size=0.00001,
                step_size=1.0,
                entry_ladder_spacing=0.00019,
            ),
        )

        self.assertEqual(len(plan["buy_orders"]), 2)
        self.assertEqual(len(plan["sell_orders"]), 2)
        self.assertEqual([order["price"] for order in plan["buy_orders"]], [0.15968, 0.15949])
        self.assertEqual([order["price"] for order in plan["sell_orders"]], [0.15969, 0.15988])

    def test_same_side_entry_cluster_pruning_thins_adjacent_entry_orders(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_entry_orders_per_side=3,
                same_side_entry_price_guard_enabled=True,
                same_side_entry_price_guard_gap_ticks=2,
            ),
            inputs=_inputs(
                bid_price=0.15968,
                ask_price=0.15969,
                mid_price=0.159685,
                cycle_budget_notional=460.0,
                tick_size=0.00001,
                step_size=1.0,
                entry_ladder_spacing=0.00001,
            ),
        )

        self.assertEqual([order["price"] for order in plan["buy_orders"]], [0.15968, 0.15966])
        self.assertEqual([order["price"] for order in plan["sell_orders"]], [0.15969, 0.15971])
        guard = plan["metrics"]["same_side_entry_price_guard"]
        self.assertTrue(guard["cluster_pruning_applied"])
        self.assertEqual(guard["cluster_pruned_buy_orders"], 1)
        self.assertEqual(guard["cluster_pruned_sell_orders"], 1)

    def test_same_side_entry_cluster_pruning_keeps_reduce_only_over_entry(self) -> None:
        orders = [
            {
                "side": "BUY",
                "price": 0.15968,
                "role": "best_quote_reduce_short",
                "force_reduce_only": True,
            },
            {
                "side": "BUY",
                "price": 0.15967,
                "role": "best_quote_entry_long",
            },
            {
                "side": "BUY",
                "price": 0.15964,
                "role": "best_quote_entry_long",
            },
        ]

        pruned, dropped = _prune_clustered_same_side_entry_orders(
            orders,
            side="BUY",
            min_gap=0.00002,
        )

        self.assertEqual(dropped, 1)
        self.assertEqual([order["price"] for order in pruned], [0.15968, 0.15964])
        self.assertTrue(pruned[0]["force_reduce_only"])

    def test_ladder_falls_back_to_fewer_slots_when_split_orders_are_too_small(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_entry_orders_per_side=2,
                min_cycle_budget_notional=6.0,
            ),
            inputs=_inputs(
                bid_price=0.6006,
                ask_price=0.6007,
                mid_price=0.60065,
                cycle_budget_notional=14.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                entry_ladder_spacing=0.00025,
            ),
        )

        self.assertEqual(len(plan["buy_orders"]), 1)
        self.assertEqual(len(plan["sell_orders"]), 1)
        self.assertGreaterEqual(plan["buy_orders"][0]["notional"], 5.0)
        self.assertGreaterEqual(plan["sell_orders"][0]["notional"], 5.0)

    def test_hedge_mode_entries_use_long_and_short_position_sides(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True),
            inputs=_inputs(position_side_mode="hedge"),
        )

        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["buy_orders"][0]["position_side"], "LONG")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(plan["sell_orders"][0]["position_side"], "SHORT")

    def test_same_side_guard_reports_short_entry_risk_without_rewriting_volume_orders(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_entry_orders_per_side=2,
                max_long_notional=700.0,
                max_short_notional=700.0,
                inventory_soft_ratio=0.35,
                same_side_entry_price_guard_enabled=True,
                same_side_entry_price_guard_min_notional=10.0,
            ),
            inputs=_inputs(
                bid_price=0.6369,
                ask_price=0.6370,
                mid_price=0.63695,
                cycle_budget_notional=60.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                entry_ladder_spacing=0.000125,
                current_long_qty=194.0,
                current_short_qty=105.0,
                current_long_avg_price=0.6418402140366,
                current_short_avg_price=0.6414314189717,
                position_side_mode="hedge",
            ),
        )

        guard = plan["metrics"]["same_side_entry_price_guard"]
        self.assertTrue(guard["report_only"])
        self.assertFalse(guard["blocked_short_entry"])
        self.assertTrue(guard["would_block_short_entry"])
        self.assertEqual(guard["would_block_sell_orders"], 2)
        self.assertTrue(plan["sell_orders"])
        self.assertTrue(all(order["role"] == "best_quote_entry_short" for order in plan["sell_orders"]))
        self.assertTrue(all(not order.get("force_reduce_only") for order in plan["sell_orders"]))
        self.assertTrue(all(order["position_side"] == "SHORT" for order in plan["sell_orders"]))

    def test_hedge_mode_reduces_long_side_when_long_is_above_soft_band(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=1_000.0,
                inventory_soft_ratio=0.6,
            ),
            inputs=_inputs(position_side_mode="hedge", current_long_qty=0.01),
        )

        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_reduce_long")
        self.assertEqual(plan["sell_orders"][0]["position_side"], "LONG")
        self.assertTrue(plan["sell_orders"][0]["force_reduce_only"])

    def test_hedge_long_recover_adds_short_entry_when_reduce_long_would_lose(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=1.0,
                max_short_notional=1_200.0,
                max_entry_orders_per_side=2,
                inventory_soft_ratio=0.6,
            ),
            inputs=_inputs(
                bid_price=0.6437,
                ask_price=0.6438,
                mid_price=0.64375,
                current_long_qty=1_305.0,
                current_short_qty=0.0,
                current_long_avg_price=0.6673,
                current_net_qty=0.0,
                cycle_budget_notional=80.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                position_side_mode="hedge",
            ),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        sell_roles = [order["role"] for order in plan["sell_orders"]]
        self.assertIn("best_quote_reduce_long", sell_roles)
        self.assertIn("best_quote_entry_short", sell_roles)
        short_entries = [order for order in plan["sell_orders"] if order["role"] == "best_quote_entry_short"]
        self.assertTrue(short_entries)
        self.assertTrue(all(order["position_side"] == "SHORT" for order in short_entries))
        self.assertTrue(all(not order.get("force_reduce_only") for order in short_entries))
        self.assertTrue(plan["metrics"]["loss_blocked_reduce_fallback"]["short_entry"])

    def test_hedge_short_recover_adds_long_entry_when_reduce_short_would_lose(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=1_200.0,
                max_short_notional=1.0,
                max_entry_orders_per_side=2,
                inventory_soft_ratio=0.6,
            ),
            inputs=_inputs(
                bid_price=0.1659,
                ask_price=0.1660,
                mid_price=0.16595,
                current_long_qty=0.0,
                current_short_qty=4_674.0,
                current_short_avg_price=0.16336,
                current_net_qty=0.0,
                cycle_budget_notional=144.0,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                position_side_mode="hedge",
            ),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        buy_roles = [order["role"] for order in plan["buy_orders"]]
        self.assertIn("best_quote_reduce_short", buy_roles)
        self.assertIn("best_quote_entry_long", buy_roles)
        long_entries = [order for order in plan["buy_orders"] if order["role"] == "best_quote_entry_long"]
        self.assertTrue(long_entries)
        self.assertTrue(all(order["position_side"] == "LONG" for order in long_entries))
        self.assertTrue(all(not order.get("force_reduce_only") for order in long_entries))
        self.assertTrue(plan["metrics"]["loss_blocked_reduce_fallback"]["long_entry"])

    def test_hedge_inventory_recover_biases_reduce_budget_to_heavy_side(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=700.0,
                max_short_notional=700.0,
                inventory_soft_ratio=0.8,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.9,
                inventory_bias_min_ratio_gap=0.35,
                inventory_bias_min_notional_gap_soft_ratio=0.5,
                inventory_bias_reduce_share=0.75,
            ),
            inputs=_inputs(
                bid_price=0.6031,
                ask_price=0.6038,
                mid_price=0.60345,
                current_long_qty=940.0,
                current_short_qty=300.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
            ),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        self.assertTrue(plan["metrics"]["inventory_bias"]["recover_applied"])
        self.assertEqual(plan["metrics"]["inventory_bias"]["side"], "long")
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_reduce_long")
        self.assertLess(plan["buy_orders"][0]["notional"], plan["sell_orders"][0]["notional"])
        self.assertAlmostEqual(plan["sell_orders"][0]["notional"], 30.0, delta=1.0)

    def test_hedge_inventory_recover_biases_heavier_long_when_short_trips_soft(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=140.0,
                max_short_notional=100.0,
                inventory_soft_ratio=0.7,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.5,
                inventory_bias_min_notional_gap=10.0,
                inventory_bias_reduce_share=0.75,
            ),
            inputs=_inputs(
                bid_price=1.0,
                ask_price=1.01,
                mid_price=1.005,
                current_long_qty=95.0,
                current_short_qty=71.0,
                current_long_avg_price=0.99,
                current_short_avg_price=0.99,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.01,
                step_size=1.0,
                min_notional=1.0,
                position_side_mode="hedge",
            ),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        self.assertTrue(plan["metrics"]["inventory_bias"]["recover_applied"])
        self.assertEqual(plan["metrics"]["inventory_bias"]["side"], "long")
        self.assertEqual([o["role"] for o in plan["buy_orders"]], ["best_quote_reduce_short"])
        self.assertEqual([o["role"] for o in plan["sell_orders"]], ["best_quote_reduce_long"])
        self.assertLess(plan["buy_orders"][0]["notional"], plan["sell_orders"][0]["notional"])

    def test_hedge_inventory_recover_keeps_small_same_side_entry_when_other_side_is_dust(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=700.0,
                max_short_notional=700.0,
                inventory_soft_ratio=0.5,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.9,
                inventory_bias_min_ratio_gap=0.35,
                inventory_bias_min_notional_gap_soft_ratio=0.35,
                inventory_bias_reduce_share=0.8,
            ),
            inputs=_inputs(
                bid_price=0.6048,
                ask_price=0.6049,
                mid_price=0.60485,
                current_long_qty=5.0,
                current_short_qty=552.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                pending_entry_buffer_notional=20.0,
                tick_size=0.0001,
                step_size=1.0,
                min_notional=5.0,
                position_side_mode="hedge",
            ),
        )

        self.assertEqual(plan["regime"], "inventory_recover")
        self.assertTrue(plan["metrics"]["inventory_bias"]["recover_applied"])
        self.assertEqual(plan["metrics"]["inventory_bias"]["side"], "short")
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertLess(plan["sell_orders"][0]["notional"], plan["buy_orders"][0]["notional"])
        self.assertEqual(plan["buy_orders"][1]["role"], "best_quote_reduce_short")
        self.assertTrue(plan["buy_orders"][1]["paired_entry_reduce"])
        self.assertTrue(plan["buy_orders"][1]["force_reduce_only"])
        self.assertLess(plan["buy_orders"][1]["price"], plan["sell_orders"][0]["price"])
        self.assertAlmostEqual(plan["buy_orders"][1]["notional"], plan["sell_orders"][0]["notional"], delta=1.0)

    def test_hedge_mode_reduces_short_side_when_short_is_above_soft_band(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=1_000.0,
                inventory_soft_ratio=0.6,
            ),
            inputs=_inputs(position_side_mode="hedge", current_short_qty=0.01),
        )

        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(plan["buy_orders"][0]["position_side"], "SHORT")
        self.assertTrue(plan["buy_orders"][0]["force_reduce_only"])
        self.assertEqual(plan["sell_orders"], [])

    def test_dynamic_control_scales_budget_and_widens_spacing_when_volatile(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_entry_orders_per_side=2,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=0.003,
                dynamic_control_high_volatility_budget_scale=0.5,
                dynamic_control_high_volatility_extra_offset_ticks=4,
                dynamic_control_high_volatility_step_scale=2.0,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                entry_ladder_spacing=0.00025,
                market_amplitude_5m=0.004,
            ),
        )

        control = plan["metrics"]["dynamic_control"]
        self.assertTrue(control["applied"])
        self.assertEqual(control["reason"], "high_volatility_defensive")
        self.assertAlmostEqual(plan["metrics"]["cycle_budget_notional"], 20.0)
        self.assertAlmostEqual(plan["metrics"]["effective_ladder_spacing"], 0.0005)
        self.assertEqual(plan["metrics"]["dynamic_tick"]["offset_ticks"], 7)

    def test_dynamic_control_biases_budget_toward_trend_side(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                dynamic_control_enabled=True,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_bias_max=0.30,
            ),
            inputs=_inputs(cycle_budget_notional=400.0, market_return_1m=0.002),
        )

        self.assertGreater(plan["metrics"]["buy_side_notional"], plan["metrics"]["sell_side_notional"])
        self.assertGreater(plan["buy_orders"][0]["notional"], plan["sell_orders"][0]["notional"])
        self.assertGreater(plan["metrics"]["dynamic_control"]["trend_score"], 0.0)

    def test_trend_entry_guard_blocks_short_entry_during_strong_uptrend(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                dynamic_control_enabled=True,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_entry_guard_enabled=True,
                dynamic_control_trend_entry_guard_min_score=0.75,
                dynamic_control_trend_entry_guard_min_volatility_ratio=0.003,
            ),
            inputs=_inputs(
                cycle_budget_notional=400.0,
                market_return_1m=0.001,
                market_return_5m=0.006,
                market_amplitude_5m=0.007,
            ),
        )

        self.assertEqual([order["role"] for order in plan["buy_orders"]], ["best_quote_entry_long"])
        self.assertEqual(plan["sell_orders"], [])
        guard = plan["metrics"]["trend_entry_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["blocked_short_entry"])
        self.assertEqual(guard["reason"], "strong_uptrend_blocks_short_entry")
        self.assertIn("trend_entry_guard", plan["reasons"])

    def test_trend_entry_guard_blocks_short_entry_on_rebound_after_pullback(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                dynamic_control_enabled=True,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_bias_max=0.35,
                dynamic_control_trend_entry_guard_enabled=True,
                dynamic_control_trend_entry_guard_min_score=0.75,
                dynamic_control_trend_entry_guard_min_volatility_ratio=0.003,
            ),
            inputs=_inputs(
                cycle_budget_notional=400.0,
                market_return_1m=0.001,
                market_return_5m=-0.006,
                market_amplitude_5m=0.007,
            ),
        )

        self.assertEqual([order["role"] for order in plan["buy_orders"]], ["best_quote_entry_long"])
        self.assertEqual(plan["sell_orders"], [])
        guard = plan["metrics"]["trend_entry_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["blocked_short_entry"])
        self.assertEqual(guard["reason"], "conflicting_rebound_blocks_short_entry")

    def test_trend_entry_guard_does_not_block_reduce_orders(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=300.0,
                inventory_soft_ratio=0.5,
                dynamic_control_enabled=True,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_entry_guard_enabled=True,
                dynamic_control_trend_entry_guard_min_score=0.75,
            ),
            inputs=_inputs(
                position_side_mode="hedge",
                current_short_qty=0.005,
                cycle_budget_notional=400.0,
                market_return_1m=0.001,
                market_return_5m=0.006,
                market_amplitude_5m=0.007,
            ),
        )

        self.assertEqual([order["role"] for order in plan["buy_orders"]], ["best_quote_reduce_short"])
        self.assertEqual(plan["buy_orders"][0]["position_side"], "SHORT")
        self.assertTrue(plan["buy_orders"][0]["force_reduce_only"])

    def test_trend_inventory_guard_slows_short_recover_during_uptrend(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=700.0,
                inventory_soft_ratio=0.5,
                defensive_offset_ticks=4,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_inventory_guard_enabled=True,
                dynamic_control_trend_inventory_guard_start_ratio=0.70,
                dynamic_control_trend_inventory_guard_min_score=0.55,
                dynamic_control_trend_inventory_guard_min_volatility_ratio=0.0035,
                dynamic_control_trend_inventory_guard_reduce_budget_scale=0.50,
                dynamic_control_trend_inventory_guard_reduce_extra_ticks=4,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_short_qty=650.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=0.003,
                market_return_5m=0.006,
                market_amplitude_5m=0.006,
            ),
        )

        order = plan["buy_orders"][0]
        self.assertEqual(order["role"], "best_quote_reduce_short")
        self.assertEqual(order["price"], 0.6042)
        self.assertAlmostEqual(order["notional"], 20.0, delta=0.5)
        guard = plan["metrics"]["trend_inventory_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["guard_short_inventory"])
        self.assertEqual(guard["reduce_short_extra_ticks"], 4)

    def test_trend_inventory_guard_slows_long_recover_during_downtrend(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=700.0,
                inventory_soft_ratio=0.5,
                defensive_offset_ticks=4,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_inventory_guard_enabled=True,
                dynamic_control_trend_inventory_guard_start_ratio=0.70,
                dynamic_control_trend_inventory_guard_min_score=0.55,
                dynamic_control_trend_inventory_guard_min_volatility_ratio=0.0035,
                dynamic_control_trend_inventory_guard_reduce_budget_scale=0.50,
                dynamic_control_trend_inventory_guard_reduce_extra_ticks=4,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_long_qty=650.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=-0.003,
                market_return_5m=-0.006,
                market_amplitude_5m=0.006,
            ),
        )

        order = plan["sell_orders"][0]
        self.assertEqual(order["role"], "best_quote_reduce_long")
        self.assertEqual(order["price"], 0.6059)
        self.assertAlmostEqual(order["notional"], 20.0, delta=0.5)
        guard = plan["metrics"]["trend_inventory_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["guard_long_inventory"])
        self.assertEqual(guard["reduce_long_extra_ticks"], 4)

    def test_trend_loss_reduce_guard_slows_short_reduce_even_below_inventory_start(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=700.0,
                inventory_soft_ratio=0.5,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_entry_guard_enabled=True,
                dynamic_control_trend_entry_guard_min_score=0.75,
                dynamic_control_trend_loss_reduce_guard_enabled=True,
                dynamic_control_trend_loss_reduce_guard_min_score=0.75,
                dynamic_control_trend_loss_reduce_guard_min_volatility_ratio=0.0035,
                dynamic_control_trend_loss_reduce_guard_reduce_budget_scale=0.35,
                dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks=6,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_short_qty=120.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=0.003,
                market_return_5m=0.006,
                market_amplitude_5m=0.006,
            ),
        )

        self.assertEqual([order["role"] for order in plan["buy_orders"]], ["best_quote_reduce_short"])
        order = plan["buy_orders"][0]
        self.assertEqual(order["price"], 0.6044)
        self.assertAlmostEqual(order["notional"], 14.0, delta=0.5)
        guard = plan["metrics"]["trend_loss_reduce_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["guard_short_reduce"])
        self.assertEqual(guard["reduce_short_extra_ticks"], 6)
        self.assertIn("trend_loss_reduce_guard", plan["reasons"])

    def test_trend_loss_reduce_guard_slows_long_reduce_even_below_inventory_start(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_long_notional=700.0,
                inventory_soft_ratio=0.5,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_entry_guard_enabled=True,
                dynamic_control_trend_entry_guard_min_score=0.75,
                dynamic_control_trend_loss_reduce_guard_enabled=True,
                dynamic_control_trend_loss_reduce_guard_min_score=0.75,
                dynamic_control_trend_loss_reduce_guard_min_volatility_ratio=0.0035,
                dynamic_control_trend_loss_reduce_guard_reduce_budget_scale=0.35,
                dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks=6,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_long_qty=120.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=-0.003,
                market_return_5m=-0.006,
                market_amplitude_5m=0.006,
            ),
        )

        self.assertEqual([order["role"] for order in plan["sell_orders"]], ["best_quote_reduce_long"])
        order = plan["sell_orders"][0]
        self.assertEqual(order["price"], 0.6057)
        self.assertAlmostEqual(order["notional"], 14.0, delta=0.5)
        guard = plan["metrics"]["trend_loss_reduce_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["guard_long_reduce"])
        self.assertEqual(guard["reduce_long_extra_ticks"], 6)

    def test_trend_loss_reduce_guard_tightens_after_recent_loss(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=700.0,
                inventory_soft_ratio=0.5,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_entry_guard_enabled=True,
                dynamic_control_trend_entry_guard_min_score=0.75,
                dynamic_control_trend_loss_reduce_guard_enabled=True,
                dynamic_control_trend_loss_reduce_guard_recent_loss_min=0.5,
                dynamic_control_trend_loss_reduce_guard_recent_loss_budget_scale=0.20,
                dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks=10,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_short_qty=120.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=0.003,
                market_return_5m=0.006,
                market_amplitude_5m=0.006,
                recent_realized_pnl=-0.6,
            ),
        )

        order = plan["buy_orders"][0]
        self.assertEqual(order["role"], "best_quote_reduce_short")
        self.assertEqual(order["price"], 0.604)
        self.assertAlmostEqual(order["notional"], 8.0, delta=0.5)
        guard = plan["metrics"]["trend_loss_reduce_guard"]
        self.assertEqual(guard["reason"], "recent_loss_trend_reduce_cooldown")
        self.assertTrue(guard["recent_loss_active"])
        self.assertEqual(guard["reduce_short_extra_ticks"], 10)

    def test_trend_loss_reduce_guard_relaxes_on_pullback(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=700.0,
                inventory_soft_ratio=0.5,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_loss_reduce_guard_enabled=True,
                dynamic_control_trend_loss_reduce_guard_reduce_budget_scale=0.35,
                dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks=6,
                dynamic_control_trend_loss_reduce_guard_recent_loss_min=0.5,
                dynamic_control_trend_loss_reduce_guard_recent_loss_budget_scale=0.20,
                dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks=10,
                dynamic_control_trend_loss_reduce_guard_relief_return_ratio=0.0015,
                dynamic_control_trend_loss_reduce_guard_relief_budget_scale=0.50,
                dynamic_control_trend_loss_reduce_guard_relief_extra_ticks=4,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_short_qty=600.0,
                current_net_qty=0.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=-0.0016,
                market_return_5m=0.012,
                market_amplitude_5m=0.012,
                recent_realized_pnl=-0.6,
            ),
        )

        order = plan["buy_orders"][0]
        self.assertEqual(order["role"], "best_quote_reduce_short")
        self.assertEqual(order["price"], 0.6043)
        self.assertAlmostEqual(order["notional"], 20.0, delta=0.5)
        guard = plan["metrics"]["trend_loss_reduce_guard"]
        self.assertEqual(guard["reason"], "pullback_relief_allows_short_reduce")
        self.assertTrue(guard["recent_loss_active"])
        self.assertTrue(guard["relief_active"])
        self.assertEqual(guard["reduce_short_extra_ticks"], 4)

    def test_trend_inventory_guard_cuts_same_side_entry_before_soft(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_short_notional=700.0,
                inventory_soft_ratio=0.5,
                dynamic_control_enabled=True,
                dynamic_control_high_volatility_ratio=1.0,
                dynamic_control_extreme_volatility_ratio=2.0,
                dynamic_control_trend_return_ratio=0.002,
                dynamic_control_trend_bias_max=0.0,
                dynamic_control_trend_inventory_guard_enabled=True,
                dynamic_control_trend_inventory_guard_start_ratio=0.70,
                dynamic_control_trend_inventory_guard_min_score=0.55,
                dynamic_control_trend_inventory_guard_min_volatility_ratio=0.0035,
                dynamic_control_trend_inventory_guard_entry_budget_scale=0.25,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_short_qty=500.0,
                current_net_qty=0.0,
                cycle_budget_notional=50.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                market_return_1m=0.003,
                market_return_5m=0.006,
                market_amplitude_5m=0.006,
            ),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertLess(plan["sell_orders"][0]["notional"], plan["buy_orders"][0]["notional"])
        self.assertAlmostEqual(plan["sell_orders"][0]["notional"], 6.0, delta=0.5)
        guard = plan["metrics"]["trend_inventory_guard"]
        self.assertTrue(guard["applied"])
        self.assertTrue(guard["guard_short_inventory"])

    def test_dynamic_control_shortens_base_spacing_when_volume_conditions_are_quiet(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_entry_orders_per_side=2,
                dynamic_control_enabled=True,
                dynamic_control_low_volatility_ratio=0.002,
                dynamic_control_low_volatility_budget_scale=1.2,
                dynamic_control_low_volatility_step_scale=0.5,
                dynamic_control_low_volatility_extra_offset_ticks=-2,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                entry_ladder_spacing=0.0003,
                market_amplitude_1m=0.001,
            ),
        )

        control = plan["metrics"]["dynamic_control"]
        self.assertEqual(control["reason"], "low_volatility_expand")
        self.assertAlmostEqual(plan["metrics"]["cycle_budget_notional"], 48.0)
        self.assertAlmostEqual(plan["metrics"]["effective_ladder_spacing"], 0.00015)
        self.assertEqual(plan["metrics"]["dynamic_tick"]["offset_ticks"], 1)

    def test_dynamic_control_tightens_spacing_without_budget_boost_when_inventory_is_high(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_long_notional=300.0,
                inventory_soft_ratio=0.8,
                dynamic_control_enabled=True,
                dynamic_control_low_volatility_ratio=0.002,
                dynamic_control_low_volatility_budget_scale=1.2,
                dynamic_control_low_volatility_step_scale=0.5,
                dynamic_control_low_volatility_extra_offset_ticks=-2,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_net_qty=330.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                entry_ladder_spacing=0.0003,
                market_amplitude_1m=0.001,
            ),
        )

        control = plan["metrics"]["dynamic_control"]
        self.assertEqual(control["reason"], "low_volatility_tighten")
        self.assertAlmostEqual(plan["metrics"]["cycle_budget_notional"], 40.0)
        self.assertAlmostEqual(plan["metrics"]["effective_ladder_spacing"], 0.00015)
        self.assertEqual(plan["metrics"]["dynamic_tick"]["offset_ticks"], 1)

    def test_dynamic_control_can_expand_budget_with_configured_inventory_room(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_long_notional=450.0,
                inventory_soft_ratio=0.8,
                dynamic_control_enabled=True,
                dynamic_control_low_volatility_ratio=0.002,
                dynamic_control_low_volatility_budget_scale=1.35,
                dynamic_control_low_volatility_budget_max_inventory_ratio=0.85,
                dynamic_control_low_volatility_step_scale=0.5,
                dynamic_control_low_volatility_extra_offset_ticks=-2,
            ),
            inputs=_inputs(
                bid_price=0.6050,
                ask_price=0.6051,
                mid_price=0.60505,
                current_net_qty=470.0,
                cycle_budget_notional=40.0,
                tick_size=0.0001,
                step_size=1.0,
                entry_ladder_spacing=0.0003,
                market_amplitude_1m=0.001,
            ),
        )

        control = plan["metrics"]["dynamic_control"]
        self.assertEqual(control["reason"], "low_volatility_expand")
        self.assertAlmostEqual(control["budget_max_inventory_ratio"], 0.85)
        self.assertAlmostEqual(plan["metrics"]["cycle_budget_notional"], 54.0)
        self.assertAlmostEqual(plan["metrics"]["effective_ladder_spacing"], 0.00015)

    def test_hedge_inventory_bias_reduces_short_but_keeps_small_short_entry(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_short_notional=1_500.0,
                inventory_soft_ratio=0.6,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.25,
                inventory_bias_reduce_share=0.7,
                inventory_bias_same_side_extra_ticks=2,
                inventory_bias_reduce_extra_ticks=-1,
            ),
            inputs=_inputs(position_side_mode="hedge", current_short_qty=0.004),
        )

        self.assertEqual(plan["regime"], "inventory_bias")
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_reduce_short")
        self.assertEqual(plan["buy_orders"][0]["position_side"], "SHORT")
        self.assertTrue(plan["buy_orders"][0]["force_reduce_only"])
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(plan["sell_orders"][0]["position_side"], "SHORT")
        self.assertGreater(plan["buy_orders"][0]["notional"], plan["sell_orders"][0]["notional"])

    def test_hedge_inventory_bias_can_use_opposite_entry_to_balance_long(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=0,
                max_long_notional=1_000.0,
                max_short_notional=700.0,
                inventory_soft_ratio=0.85,
                min_cycle_budget_notional=6.0,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.03,
                inventory_bias_min_notional_gap=25.0,
                inventory_bias_reduce_share=0.5,
                inventory_bias_opposite_entry_enabled=True,
                inventory_bias_same_side_extra_ticks=0,
                inventory_bias_reduce_extra_ticks=-1,
            ),
            inputs=_inputs(
                bid_price=0.5140,
                ask_price=0.5141,
                mid_price=0.51405,
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                position_side_mode="hedge",
                target_volume_remaining=300_000.0,
                current_long_qty=1050.0,
                current_short_qty=960.0,
                current_long_avg_price=0.5120,
                current_short_avg_price=0.5160,
                cycle_budget_notional=12.0,
            ),
        )

        self.assertEqual(plan["regime"], "inventory_bias")
        self.assertEqual(plan["metrics"]["inventory_bias"]["side"], "long")
        self.assertTrue(plan["metrics"]["inventory_bias"]["opposite_entry_enabled"])
        self.assertEqual([order["role"] for order in plan["buy_orders"]], [])
        self.assertEqual([order["role"] for order in plan["sell_orders"]], [
            "best_quote_reduce_long",
            "best_quote_entry_short",
        ])
        self.assertEqual(plan["sell_orders"][0]["position_side"], "LONG")
        self.assertEqual(plan["sell_orders"][1]["position_side"], "SHORT")

    def test_hedge_inventory_bias_keeps_both_sides_when_gross_inventory_is_balanced(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_long_notional=300.0,
                max_short_notional=300.0,
                inventory_soft_ratio=0.8,
                min_cycle_budget_notional=6.0,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.7,
                inventory_bias_min_ratio_gap=0.05,
            ),
            inputs=_inputs(
                bid_price=0.6006,
                ask_price=0.6007,
                mid_price=0.60065,
                cycle_budget_notional=14.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                current_long_qty=312.0,
                current_short_qty=321.0,
            ),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertFalse(plan["metrics"]["inventory_bias"]["applied"])
        self.assertLess(plan["metrics"]["inventory_bias"]["ratio_gap"], 0.05)
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")

    def test_hedge_inventory_bias_requires_absolute_notional_gap(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_long_notional=125.0,
                max_short_notional=125.0,
                inventory_soft_ratio=0.8,
                min_cycle_budget_notional=6.0,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=0.7,
                inventory_bias_min_ratio_gap=0.05,
                inventory_bias_min_notional_gap=10.0,
            ),
            inputs=_inputs(
                bid_price=1.0,
                ask_price=1.01,
                mid_price=1.005,
                cycle_budget_notional=14.0,
                tick_size=0.01,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                position_side_mode="hedge",
                current_long_qty=70.0,
                current_short_qty=78.0,
            ),
        )

        self.assertEqual(plan["regime"], "normal")
        self.assertFalse(plan["metrics"]["inventory_bias"]["applied"])
        self.assertGreaterEqual(plan["metrics"]["inventory_bias"]["ratio_gap"], 0.05)
        self.assertLess(plan["metrics"]["inventory_bias"]["notional_gap"], 10.0)
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")

    def test_hedge_inventory_bias_uses_soft_threshold_ratio_for_notional_gap(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                quote_offset_ticks=3,
                max_long_notional=300.0,
                max_short_notional=300.0,
                inventory_soft_ratio=0.8,
                min_cycle_budget_notional=6.0,
                inventory_bias_enabled=True,
                inventory_bias_start_ratio=1.0,
                inventory_bias_min_ratio_gap=0.35,
                inventory_bias_min_notional_gap=10.0,
                inventory_bias_min_notional_gap_soft_ratio=0.5,
            ),
            inputs=_inputs(
                bid_price=0.6006,
                ask_price=0.6007,
                mid_price=0.60065,
                cycle_budget_notional=14.0,
                tick_size=0.0001,
                step_size=1.0,
                position_side_mode="hedge",
                current_long_qty=360.0,
                current_short_qty=190.0,
            ),
        )

        bias = plan["metrics"]["inventory_bias"]
        self.assertEqual(plan["regime"], "normal")
        self.assertFalse(bias["applied"])
        self.assertAlmostEqual(bias["min_notional_gap"], 120.0)
        self.assertAlmostEqual(bias["min_notional_gap_soft_ratio"], 0.5)
        self.assertLess(bias["notional_gap"], 120.0)

    def test_frozen_v2_default_off_reports_without_changing_orders(self) -> None:
        base_inputs = _inputs(
            bid_price=1.0,
            ask_price=1.01,
            mid_price=1.005,
            cycle_budget_notional=100.0,
            tick_size=0.01,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            position_side_mode="hedge",
            frozen_short_notional=750.0,
            frozen_short_cap_notional=1_000.0,
        )
        without_frozen_v2 = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True, max_entry_orders_per_side=1),
            inputs=base_inputs,
        )
        with_frozen_v2_disabled = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_entry_orders_per_side=1,
                frozen_v2_enabled=False,
            ),
            inputs=base_inputs,
        )

        self.assertEqual(with_frozen_v2_disabled["buy_orders"], without_frozen_v2["buy_orders"])
        self.assertEqual(with_frozen_v2_disabled["sell_orders"], without_frozen_v2["sell_orders"])
        self.assertFalse(with_frozen_v2_disabled["metrics"]["frozen_v2"]["enabled"])
        self.assertFalse(with_frozen_v2_disabled["metrics"]["frozen_v2"]["applied"])
        self.assertEqual(with_frozen_v2_disabled["metrics"]["frozen_v2"]["short_state"], "pressure")

    def test_frozen_v2_short_pressure_biases_toward_long_recovery(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_entry_orders_per_side=1,
                frozen_v2_enabled=True,
                frozen_v2_pressure_ratio=0.60,
                frozen_v2_danger_ratio=0.85,
                frozen_v2_pressure_same_side_entry_scale=0.50,
                frozen_v2_pressure_recovery_budget_share=0.30,
            ),
            inputs=_inputs(
                bid_price=1.0,
                ask_price=1.01,
                mid_price=1.005,
                cycle_budget_notional=100.0,
                tick_size=0.01,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                position_side_mode="hedge",
                frozen_short_notional=700.0,
                frozen_short_cap_notional=1_000.0,
            ),
        )

        self.assertEqual(plan["regime"], "frozen_v2_recovery")
        self.assertIn("frozen_v2_pressure", plan["reasons"])
        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertGreater(plan["buy_orders"][0]["notional"], 75.0)
        self.assertLess(plan["sell_orders"][0]["notional"], 30.0)
        report = plan["metrics"]["frozen_v2"]
        self.assertTrue(report["applied"])
        self.assertEqual(report["short_state"], "pressure")
        self.assertAlmostEqual(report["short_entry_budget_scale"], 0.5)
        self.assertAlmostEqual(report["long_recovery_budget_share"], 0.3)

    def test_frozen_v2_short_capped_blocks_short_entry(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(
                enabled=True,
                max_entry_orders_per_side=1,
                frozen_v2_enabled=True,
                frozen_v2_capped_same_side_entry_scale=0.0,
                frozen_v2_capped_recovery_budget_share=0.70,
            ),
            inputs=_inputs(
                bid_price=1.0,
                ask_price=1.01,
                mid_price=1.005,
                cycle_budget_notional=100.0,
                tick_size=0.01,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                position_side_mode="hedge",
                frozen_short_notional=1_050.0,
                frozen_short_cap_notional=1_000.0,
            ),
        )

        self.assertEqual([order["role"] for order in plan["buy_orders"]], ["best_quote_entry_long"])
        self.assertEqual(plan["sell_orders"], [])
        self.assertGreaterEqual(plan["buy_orders"][0]["notional"], 120.0)
        self.assertEqual(plan["metrics"]["frozen_v2"]["short_state"], "capped")


if __name__ == "__main__":
    unittest.main()
