from __future__ import annotations

import unittest

from grid_optimizer.best_quote_maker_volume import (
    BestQuoteMakerVolumeConfig,
    BestQuoteMakerVolumeInputs,
    build_best_quote_maker_volume_plan,
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

    def test_hedge_mode_entries_use_long_and_short_position_sides(self) -> None:
        plan = build_best_quote_maker_volume_plan(
            config=BestQuoteMakerVolumeConfig(enabled=True),
            inputs=_inputs(position_side_mode="hedge"),
        )

        self.assertEqual(plan["buy_orders"][0]["role"], "best_quote_entry_long")
        self.assertEqual(plan["buy_orders"][0]["position_side"], "LONG")
        self.assertEqual(plan["sell_orders"][0]["role"], "best_quote_entry_short")
        self.assertEqual(plan["sell_orders"][0]["position_side"], "SHORT")

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


if __name__ == "__main__":
    unittest.main()
