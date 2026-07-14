from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import grid_optimizer.bq_budget_controller as budget_controller
from grid_optimizer.bq_budget_controller import (
    Budgets,
    FlowSplit,
    resolve_budget_tiers,
    resolve_min_cycle,
    select_tier,
)


class BqBudgetControllerTests(unittest.TestCase):
    def test_control_writer_uses_a_unique_atomic_temp_path_per_write(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "arxusdt_loop_runner_control.json"
            original_replace = budget_controller.os.replace
            replaced_sources: list[str] = []

            def capture_replace(source: str | Path, destination: str | Path) -> None:
                replaced_sources.append(Path(source).name)
                return original_replace(source, destination)

            with patch.object(budget_controller.os, "replace", new=capture_replace):
                budget_controller._write_control_json(str(path), {"cycle": 1})
                budget_controller._write_control_json(str(path), {"cycle": 2})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"cycle": 2})
            self.assertEqual(len(set(replaced_sources)), 2)

    def test_defaults_all_tiers_to_current_runner_cycle(self) -> None:
        budgets, source = resolve_budget_tiers(
            {"best_quote_maker_volume_cycle_budget_notional": 120.0}
        )

        self.assertEqual(budgets, Budgets(120.0, 120.0, 120.0, 120.0, 120.0))
        self.assertEqual(source, "runner_cycle_budget")

    def test_next_runner_cycle_change_becomes_new_default(self) -> None:
        budgets, _ = resolve_budget_tiers(
            {"best_quote_maker_volume_cycle_budget_notional": 200.0}
        )

        self.assertEqual(budgets, Budgets(200.0, 200.0, 200.0, 200.0, 200.0))

    def test_control_tiers_override_current_cycle(self) -> None:
        budgets, source = resolve_budget_tiers(
            {
                "best_quote_maker_volume_cycle_budget_notional": 120.0,
                "budget_controller_tier_budgets": [40, 60, 80, 100, 120],
            }
        )

        self.assertEqual(budgets, Budgets(40.0, 60.0, 80.0, 100.0, 120.0))
        self.assertEqual(source, "control_tiers")

    def test_cli_override_is_explicit_and_highest_priority(self) -> None:
        budgets, source = resolve_budget_tiers(
            {
                "best_quote_maker_volume_cycle_budget_notional": 120.0,
                "budget_controller_tier_budgets": [40, 60, 80, 100, 120],
            },
            "50,75,100,150,200",
        )

        self.assertEqual(budgets, Budgets(50.0, 75.0, 100.0, 150.0, 200.0))
        self.assertEqual(source, "cli_override")

    def test_min_cycle_defaults_to_runner_control(self) -> None:
        self.assertEqual(
            resolve_min_cycle(
                {"best_quote_maker_volume_min_cycle_budget_notional": 72.0}
            ),
            72.0,
        )

    def test_choose_tier_uses_resolved_budget(self) -> None:
        tier, budget, reasons = select_tier(
            remaining=80_000,
            required_rate=12_000,
            vol_1h=3_000,
            vol_30m=300,
            flow=FlowSplit(flow_pnl=1.0, flow_vol=3_000),
            budgets=Budgets(40.0, 60.0, 80.0, 100.0, 120.0),
            vol_defensive=False,
            plan_stale=False,
            income_stale=False,
            fee_stale=False,
            reservoir_day=0.0,
            reservoir_day_budget=0.0,
            boost_banned=False,
        )

        self.assertEqual((tier, budget), ("boost2", 120.0))
        self.assertEqual(reasons, [])


if __name__ == "__main__":
    unittest.main()
