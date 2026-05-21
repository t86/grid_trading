from __future__ import annotations

import unittest

from grid_optimizer.central_strategy_workbench import (
    build_workbench_payload,
    get_central_strategy_target,
    list_central_strategy_targets,
)


class CentralStrategyWorkbenchTests(unittest.TestCase):
    def test_pharos_catalog_scopes_common_params_by_target(self) -> None:
        targets = list_central_strategy_targets()
        target = get_central_strategy_target(
            "114",
            "PHAROSUSDT",
            "pharosusdt_hedge_best_quote_maker_volume_v1",
        )

        self.assertEqual({item["server_id"] for item in targets}, {"114", "150"})
        self.assertEqual(target.scope, "114/PHAROSUSDT/pharosusdt_hedge_best_quote_maker_volume_v1")
        self.assertEqual(
            target["scope"],
            {
                "server_id": "114",
                "symbol": "PHAROSUSDT",
                "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
            },
        )
        self.assertIn("max_new_orders", target.common_params)
        self.assertIn(
            "best_quote_maker_volume_cycle_budget_notional",
            target.profile_params,
        )

    def test_build_workbench_payload_groups_config_by_owner(self) -> None:
        payload = build_workbench_payload(
            "114",
            "PHAROSUSDT",
            "pharosusdt_hedge_best_quote_maker_volume_v1",
            {
                "symbol": "PHAROSUSDT",
                "profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                "mode": "hedge_best_quote_maker_volume_v1",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "required_position_mode": "hedge",
                "max_new_orders": 6,
                "max_total_notional": 1450.0,
                "best_quote_maker_volume_enabled": True,
                "best_quote_maker_volume_cycle_budget_notional": 40.0,
                "best_quote_maker_volume_dynamic_control_enabled": True,
                "adaptive_regime_router_enabled": True,
                "loss_reentry_guard_enabled": True,
                "adaptive_step_max_scale": 4.5,
                "adverse_reduce_long_trigger_ratio": 0.004,
                "threshold_position_notional": 630.0,
                "volatility_trigger_enabled": False,
                "startup_jitter_seconds": 0.0,
                "synthetic_flow_sleeve_enabled": True,
                "old_unknown_knob": 123,
            },
        )

        self.assertEqual(payload["scope"]["server_id"], "114")
        self.assertEqual(payload["profile_family"], "hedge_best_quote_maker_volume")
        self.assertIn("startup_preflight", payload)
        self.assertIn("profile_boundary", payload)
        self.assertTrue(payload["profile_boundary"]["overlay_known"])
        self.assertIn("max_new_orders", payload["sections"]["common"])
        self.assertIn(
            "best_quote_maker_volume_cycle_budget_notional",
            payload["sections"]["profile"],
        )
        self.assertIn("best_quote_maker_volume_dynamic_control_enabled", payload["sections"]["profile"])
        self.assertIn("adaptive_regime_router_enabled", payload["sections"]["profile"])
        self.assertIn("loss_reentry_guard_enabled", payload["sections"]["profile"])
        self.assertIn("synthetic_flow_sleeve_enabled", payload["sections"]["forbidden"])
        self.assertIn("adaptive_step_max_scale", payload["sections"]["forbidden"])
        self.assertIn("adverse_reduce_long_trigger_ratio", payload["sections"]["forbidden"])
        self.assertIn("threshold_position_notional", payload["sections"]["global_safety"])
        self.assertIn("volatility_trigger_enabled", payload["sections"]["global_safety"])
        self.assertIn("startup_jitter_seconds", payload["sections"]["common"])
        self.assertIn("old_unknown_knob", payload["sections"]["unknown"])


if __name__ == "__main__":
    unittest.main()
