from __future__ import annotations

import unittest
from datetime import datetime, timezone

from grid_optimizer.regime_entry_budget import (
    RegimeEntryBudgetConfig,
    RegimeEntryBudgetInputs,
    regime_entry_budget_state_snapshot,
    resolve_regime_entry_budget_control,
)


NOW = datetime(2026, 5, 12, tzinfo=timezone.utc)


def _inputs(**overrides):
    values = {
        "now": NOW,
        "last_state": {"state": "ping-pong-safe", "reconcile_ok_count": 3},
        "candidate_regime": "ping-pong-safe",
        "mid_price": 0.1,
        "tick_size": 0.00001,
        "current_long_notional": 0.0,
        "current_short_notional": 0.0,
        "open_entry_long_notional": 0.0,
        "open_entry_short_notional": 0.0,
        "open_non_reduce_entry_orders": 0,
        "gross_notional_15m": 10_000.0,
        "loss_per_10k_15m": 0.0,
    }
    values.update(overrides)
    return RegimeEntryBudgetInputs(**values)


class RegimeEntryBudgetTests(unittest.TestCase):
    def test_disabled_returns_passthrough_report(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=False),
            inputs=_inputs(candidate_regime="wide-step"),
        )

        self.assertFalse(report["enabled"])
        self.assertEqual(report["state"], "disabled")
        self.assertIsNone(report["long_entry_capacity"])
        self.assertIn("disabled", report["reasons"])

    def test_fast_low_vol_shrinks_step_and_per_order_without_shrinking_budget(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True, defensive_inventory_usage_ratio=2.0),
            inputs=_inputs(
                last_state={"state": "ping-pong-fast", "reconcile_ok_count": 3},
                candidate_regime="ping-pong-fast",
                amplitude_30s_ratio=0.001,
                amplitude_1m_ratio=0.002,
            ),
        )

        self.assertEqual(report["state"], "ping-pong-fast")
        self.assertAlmostEqual(report["effective_per_order_notional"], 33.6)
        self.assertAlmostEqual(report["long_side_budget"], 480.0)
        self.assertEqual(report["long_entry_capacity"], 14)
        self.assertAlmostEqual(report["effective_step_ratio"], 0.0014)

    def test_capacity_subtracts_inventory_and_open_same_side_entry(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(current_long_notional=216.0, open_entry_long_notional=144.0),
        )

        self.assertAlmostEqual(report["effective_per_order_notional"], 50.4)
        self.assertAlmostEqual(report["long_side_budget"], 1080.0)
        self.assertEqual(report["long_entry_capacity"], 14)

    def test_wide_step_allows_same_side_entry_when_budget_remains(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True, defensive_inventory_usage_ratio=2.0),
            inputs=_inputs(
                last_state={"state": "wide-step", "reconcile_ok_count": 3},
                candidate_regime="wide-step",
                current_long_notional=1_000.0,
                open_entry_long_notional=200.0,
                amplitude_30s_ratio=0.020,
                amplitude_1m_ratio=0.024,
            ),
        )

        self.assertEqual(report["state"], "wide-step")
        self.assertGreater(report["long_entry_capacity"], 0)
        self.assertTrue(report["allow_entry_long"])
        self.assertTrue(report["allow_entry_short"])

    def test_budget_exhausted_blocks_that_side_only(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True, defensive_inventory_usage_ratio=2.0),
            inputs=_inputs(
                last_state={"state": "wide-step", "reconcile_ok_count": 3},
                candidate_regime="wide-step",
                current_long_notional=2_300.0,
                open_entry_long_notional=100.0,
            ),
        )

        self.assertEqual(report["long_entry_capacity"], 0)
        self.assertFalse(report["allow_entry_long"])
        self.assertGreater(report["short_entry_capacity"], 0)

    def test_regime_change_requires_cancel_confirm_before_entry(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(
                last_state={"state": "ping-pong-fast", "reconcile_ok_count": 3},
                candidate_regime="wide-step",
                open_non_reduce_entry_orders=4,
            ),
        )

        self.assertTrue(report["switching"])
        self.assertEqual(report["target_regime"], "wide-step")
        self.assertTrue(report["cancel_entry_required"])
        self.assertEqual(report["long_entry_capacity"], 0)
        self.assertEqual(report["blocked_reason"], "cancel_confirm_required")

    def test_switch_commits_after_entry_orders_clear_and_reconcile_confirms(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(
                last_state={
                    "state": "ping-pong-fast",
                    "target_regime": "wide-step",
                    "switching": True,
                    "reconcile_ok_count": 1,
                },
                candidate_regime="wide-step",
                open_non_reduce_entry_orders=0,
                reconcile_ok=True,
            ),
        )

        self.assertFalse(report["switching"])
        self.assertEqual(report["state"], "wide-step")
        self.assertFalse(report["cancel_entry_required"])
        self.assertGreater(report["long_entry_capacity"], 0)

    def test_shock_guard_blocks_entry_and_requires_cancel(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(abs_return_30s_ratio=0.020, open_non_reduce_entry_orders=2),
        )

        self.assertEqual(report["state"], "shock-guard")
        self.assertTrue(report["shock_guard_active"])
        self.assertTrue(report["cancel_entry_required"])
        self.assertEqual(report["long_entry_capacity"], 0)
        self.assertEqual(report["short_entry_capacity"], 0)

    def test_defensive_sets_entry_budget_to_zero(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(candidate_regime="defensive"),
        )

        self.assertEqual(report["state"], "defensive")
        self.assertEqual(report["long_side_budget"], 0.0)
        self.assertEqual(report["effective_per_order_notional"], 0.0)
        self.assertFalse(report["allow_entry_long"])

    def test_coarse_tick_uses_tick_step_and_scales_risk(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(
                last_state={"state": "ping-pong-fast", "reconcile_ok_count": 3},
                candidate_regime="ping-pong-fast",
                mid_price=0.1,
                tick_size=0.001,
            ),
        )

        self.assertTrue(report["tick_dominated"])
        self.assertTrue(report["coarse_tick"])
        self.assertAlmostEqual(report["effective_step_price"], 0.001)
        self.assertLess(report["effective_per_order_notional"], 48.0)
        self.assertLess(report["long_side_budget"], 480.0)

    def test_state_snapshot_preserves_switch_fields(self) -> None:
        report = resolve_regime_entry_budget_control(
            config=RegimeEntryBudgetConfig(enabled=True),
            inputs=_inputs(
                last_state={"state": "ping-pong-fast", "reconcile_ok_count": 3},
                candidate_regime="wide-step",
                open_non_reduce_entry_orders=1,
            ),
        )

        snapshot = regime_entry_budget_state_snapshot(report, updated_at="2026-05-12T00:00:00+00:00")

        self.assertEqual(snapshot["state"], "ping-pong-fast")
        self.assertEqual(snapshot["target_regime"], "wide-step")
        self.assertTrue(snapshot["switching"])
        self.assertEqual(snapshot["reconcile_ok_count"], 4)


if __name__ == "__main__":
    unittest.main()
