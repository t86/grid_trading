from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from grid_optimizer.central_strategy_workbench import (
    apply_remote_workbench_config,
    build_workbench_payload,
    get_central_strategy_target,
    list_central_strategy_targets,
    save_remote_workbench_config,
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

    @patch("grid_optimizer.central_strategy_workbench.subprocess.run")
    def test_save_remote_workbench_config_writes_scoped_control_file(self, mock_run) -> None:
        mock_run.return_value = Mock(returncode=0, stdout="saved\n", stderr="")

        result = save_remote_workbench_config(
            "114",
            "PHAROSUSDT",
            "pharosusdt_hedge_best_quote_maker_volume_v1",
            {
                "symbol": "PHAROSUSDT",
                "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "required_position_mode": "hedge",
                "max_new_orders": 5,
                "max_total_notional": 1450.0,
                "cancel_stale": True,
                "buy_levels": 1,
                "sell_levels": 1,
                "per_order_notional": 10.0,
                "best_quote_maker_volume_enabled": True,
            },
        )

        self.assertTrue(result["saved"])
        self.assertEqual(result["scope"]["server_id"], "114")
        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "ssh")
        self.assertEqual(command[1], "srv-43-155-163-114")
        self.assertEqual(len(command), 3)
        self.assertIn("python3 -c ", command[2])
        self.assertIn("/home/ubuntu/wangge/output/pharosusdt_loop_runner_control.json", command[2])
        self.assertNotIn("\nimport json", command[2])
        written = json.loads(mock_run.call_args.kwargs["input"])
        self.assertEqual(written["symbol"], "PHAROSUSDT")
        self.assertEqual(written["strategy_profile"], "pharosusdt_hedge_best_quote_maker_volume_v1")
        self.assertEqual(written["_central_workbench_action"], "save")

    @patch("grid_optimizer.central_strategy_workbench.subprocess.run")
    def test_save_remote_workbench_config_strips_non_writable_profile_params(self, mock_run) -> None:
        mock_run.return_value = Mock(returncode=0, stdout="saved\n", stderr="")

        result = save_remote_workbench_config(
            "114",
            "PHAROSUSDT",
            "pharosusdt_hedge_best_quote_maker_volume_v1",
            {
                "symbol": "PHAROSUSDT",
                "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "required_position_mode": "hedge",
                "max_new_orders": 5,
                "max_total_notional": 1450.0,
                "cancel_stale": True,
                "buy_levels": 1,
                "sell_levels": 1,
                "per_order_notional": 10.0,
                "best_quote_maker_volume_enabled": True,
                "synthetic_flow_sleeve_enabled": False,
                "adaptive_step_max_scale": 4.5,
                "old_unknown_knob": "",
            },
        )

        written = json.loads(mock_run.call_args.kwargs["input"])
        self.assertNotIn("synthetic_flow_sleeve_enabled", written)
        self.assertNotIn("adaptive_step_max_scale", written)
        self.assertNotIn("old_unknown_knob", written)
        self.assertEqual(
            result["stripped_params"],
            ["adaptive_step_max_scale", "old_unknown_knob", "synthetic_flow_sleeve_enabled"],
        )

    def test_save_remote_workbench_config_rejects_scope_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "strategy_profile"):
            save_remote_workbench_config(
                "114",
                "PHAROSUSDT",
                "pharosusdt_hedge_best_quote_maker_volume_v1",
                {
                    "symbol": "PHAROSUSDT",
                    "strategy_profile": "wrong_profile",
                    "strategy_mode": "hedge_best_quote_maker_volume_v1",
                },
            )

    @patch("grid_optimizer.central_strategy_workbench.subprocess.run")
    def test_apply_remote_workbench_config_saves_then_restarts_target_runner(self, mock_run) -> None:
        mock_run.return_value = Mock(returncode=0, stdout="ok\n", stderr="")

        result = apply_remote_workbench_config(
            "150",
            "PHAROSUSDT",
            "pharosusdt_hedge_best_quote_maker_volume_v1",
            {
                "symbol": "PHAROSUSDT",
                "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "required_position_mode": "hedge",
                "max_new_orders": 5,
                "max_total_notional": 1450.0,
                "cancel_stale": True,
                "buy_levels": 1,
                "sell_levels": 1,
                "per_order_notional": 10.0,
                "best_quote_maker_volume_enabled": True,
            },
        )

        self.assertTrue(result["saved"])
        self.assertTrue(result["started"])
        self.assertEqual(mock_run.call_count, 2)
        restart_command = mock_run.call_args_list[1].args[0]
        self.assertEqual(
            restart_command,
            ["ssh", "srv-43-131-232-150", "/usr/local/bin/grid-saved-runner-api2", "restart", "PHAROSUSDT"],
        )


if __name__ == "__main__":
    unittest.main()
