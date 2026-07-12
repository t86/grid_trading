from __future__ import annotations

import unittest

from grid_optimizer.bq_budget_controller import (
    choose_tier,
    resolve_budget_tiers,
    resolve_min_cycle,
)


class BqBudgetControllerTests(unittest.TestCase):
    def test_defaults_all_tiers_to_current_runner_cycle(self) -> None:
        budgets, source = resolve_budget_tiers(
            {"best_quote_maker_volume_cycle_budget_notional": 120.0}
        )

        self.assertEqual(budgets, (120.0, 120.0, 120.0, 120.0, 120.0))
        self.assertEqual(source, "runner_cycle_budget")

    def test_next_runner_cycle_change_becomes_new_default(self) -> None:
        budgets, _ = resolve_budget_tiers(
            {"best_quote_maker_volume_cycle_budget_notional": 200.0}
        )

        self.assertEqual(budgets, (200.0, 200.0, 200.0, 200.0, 200.0))

    def test_control_tiers_override_current_cycle(self) -> None:
        budgets, source = resolve_budget_tiers(
            {
                "best_quote_maker_volume_cycle_budget_notional": 120.0,
                "budget_controller_tier_budgets": [40, 60, 80, 100, 120],
            }
        )

        self.assertEqual(budgets, (40.0, 60.0, 80.0, 100.0, 120.0))
        self.assertEqual(source, "control_tiers")

    def test_cli_override_is_explicit_and_highest_priority(self) -> None:
        budgets, source = resolve_budget_tiers(
            {
                "best_quote_maker_volume_cycle_budget_notional": 120.0,
                "budget_controller_tier_budgets": [40, 60, 80, 100, 120],
            },
            "50,75,100,150,200",
        )

        self.assertEqual(budgets, (50.0, 75.0, 100.0, 150.0, 200.0))
        self.assertEqual(source, "cli_override")

    def test_min_cycle_defaults_to_runner_control(self) -> None:
        self.assertEqual(
            resolve_min_cycle(
                {"best_quote_maker_volume_min_cycle_budget_notional": 72.0}
            ),
            72.0,
        )

    def test_choose_tier_uses_resolved_budget(self) -> None:
        tier, budget = choose_tier(
            remaining=80_000,
            required_rate=12_000,
            volume_1h=3_000,
            pnl_1h=1.0,
            wear_1h=-1.0,
            volume_30m=300,
            volatility_defensive=False,
            budgets=(40.0, 60.0, 80.0, 100.0, 120.0),
        )

        self.assertEqual((tier, budget), ("boost2", 120.0))


if __name__ == "__main__":
    unittest.main()
