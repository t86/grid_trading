from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from grid_optimizer.central_strategy_workbench import (
    REMOTE_ROLLBACK_CONTROL_SCRIPT,
    REMOTE_WRITE_CONTROL_SCRIPT,
    apply_remote_workbench_config,
    build_workbench_payload,
    get_central_strategy_target,
    list_central_strategy_targets,
    rollback_remote_workbench_config,
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
        mock_run.side_effect = [
            Mock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "changed_params": ["max_new_orders"],
                        "history_entry": {
                            "version_id": "v2",
                            "action": "save",
                            "scope": "114/PHAROSUSDT/pharosusdt_hedge_best_quote_maker_volume_v1",
                            "changed_params": ["max_new_orders"],
                        },
                    },
                ),
                stderr="",
            ),
            Mock(
                returncode=0,
                stdout=json.dumps(
                    [
                        {
                            "version_id": "v1",
                            "action": "save",
                            "scope": "114/PHAROSUSDT/pharosusdt_hedge_best_quote_maker_volume_v1",
                            "changed_params": ["max_total_notional"],
                        },
                        {
                            "version_id": "v2",
                            "action": "save",
                            "scope": "114/PHAROSUSDT/pharosusdt_hedge_best_quote_maker_volume_v1",
                            "changed_params": ["max_new_orders"],
                        },
                    ],
                ),
                stderr="",
            ),
        ]

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
        self.assertTrue(result["can_rollback"])
        self.assertEqual([item["version_id"] for item in result["history"]], ["v1", "v2"])
        self.assertEqual(result["changed_params"], ["max_new_orders"])
        command = mock_run.call_args_list[0].args[0]
        self.assertEqual(command[0], "ssh")
        self.assertEqual(command[1], "srv-43-155-163-114")
        self.assertEqual(len(command), 3)
        self.assertIn("python3 -c ", command[2])
        self.assertIn("/home/ubuntu/wangge/output/pharosusdt_loop_runner_control.json", command[2])
        self.assertNotIn("\nimport json", command[2])
        written = json.loads(mock_run.call_args_list[0].kwargs["input"])
        self.assertEqual(written["symbol"], "PHAROSUSDT")
        self.assertEqual(written["strategy_profile"], "pharosusdt_hedge_best_quote_maker_volume_v1")
        self.assertEqual(written["_central_workbench_action"], "save")
        self.assertIn("pharosusdt_loop_runner_control.json.central_workbench_history.jsonl", command[2])

    def test_remote_write_script_records_history_entry_and_changed_params(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            control_path = Path(tmpdir) / "pharosusdt_loop_runner_control.json"
            history_path = Path(tmpdir) / "pharosusdt_loop_runner_control.json.central_workbench_history.jsonl"
            control_path.write_text(
                json.dumps(
                    {
                        "symbol": "PHAROSUSDT",
                        "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                        "max_new_orders": 4,
                    },
                ),
                encoding="utf-8",
            )

            completed = subprocess.run(
                [sys.executable, "-c", REMOTE_WRITE_CONTROL_SCRIPT, str(control_path), str(history_path)],
                capture_output=True,
                check=False,
                input=json.dumps(
                    {
                        "symbol": "PHAROSUSDT",
                        "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                        "max_new_orders": 5,
                        "_central_workbench_action": "save",
                        "_central_workbench_scope": "114/PHAROSUSDT/pharosusdt_hedge_best_quote_maker_volume_v1",
                        "_central_workbench_stripped_params": ["old_unknown_knob"],
                    },
                ),
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            summary = json.loads(completed.stdout)
            self.assertTrue(summary["version_id"])
            self.assertEqual(summary["changed_params"], ["max_new_orders"])
            self.assertEqual(summary["stripped_params"], ["old_unknown_knob"])
            history = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["after_config"]["max_new_orders"], 5)
            self.assertEqual(json.loads(control_path.read_text(encoding="utf-8"))["max_new_orders"], 5)

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

        written = json.loads(mock_run.call_args_list[0].kwargs["input"])
        self.assertNotIn("synthetic_flow_sleeve_enabled", written)
        self.assertNotIn("adaptive_step_max_scale", written)
        self.assertNotIn("old_unknown_knob", written)
        self.assertEqual(
            result["stripped_params"],
            ["adaptive_step_max_scale", "old_unknown_knob", "synthetic_flow_sleeve_enabled"],
        )

    @patch("grid_optimizer.central_strategy_workbench.subprocess.run")
    def test_rollback_remote_workbench_config_restores_previous_saved_version(self, mock_run) -> None:
        mock_run.return_value = Mock(
            returncode=0,
            stdout=json.dumps(
                {
                    "rolled_back": True,
                    "rollback_of": "v2",
                    "restored_version_id": "v1",
                    "changed_params": ["max_new_orders"],
                    "history_entry": {"version_id": "rb1", "action": "rollback"},
                },
            ),
            stderr="",
        )

        result = rollback_remote_workbench_config(
            "114",
            "PHAROSUSDT",
            "pharosusdt_hedge_best_quote_maker_volume_v1",
        )

        self.assertTrue(result["rolled_back"])
        self.assertEqual(result["restored_version_id"], "v1")
        command = mock_run.call_args.args[0]
        self.assertEqual(command[0], "ssh")
        self.assertEqual(command[1], "srv-43-155-163-114")
        self.assertIn("pharosusdt_loop_runner_control.json.central_workbench_history.jsonl", command[2])

    def test_remote_rollback_script_uses_previous_clean_after_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            control_path = Path(tmpdir) / "pharosusdt_loop_runner_control.json"
            history_path = Path(tmpdir) / "pharosusdt_loop_runner_control.json.central_workbench_history.jsonl"
            scope = "114/PHAROSUSDT/pharosusdt_hedge_best_quote_maker_volume_v1"
            history_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "version_id": "v1",
                                "action": "save",
                                "scope": scope,
                                "after_config": {"symbol": "PHAROSUSDT", "max_new_orders": 4},
                            },
                        ),
                        json.dumps(
                            {
                                "version_id": "v2",
                                "action": "save",
                                "scope": scope,
                                "after_config": {"symbol": "PHAROSUSDT", "max_new_orders": 5},
                            },
                        ),
                    ],
                )
                + "\n",
                encoding="utf-8",
            )
            control_path.write_text(json.dumps({"symbol": "PHAROSUSDT", "max_new_orders": 5}), encoding="utf-8")

            completed = subprocess.run(
                [sys.executable, "-c", REMOTE_ROLLBACK_CONTROL_SCRIPT, str(control_path), str(history_path), scope],
                capture_output=True,
                check=False,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            summary = json.loads(completed.stdout)
            self.assertTrue(summary["rolled_back"])
            self.assertEqual(summary["rollback_of"], "v2")
            self.assertEqual(summary["restored_version_id"], "v1")
            self.assertEqual(json.loads(control_path.read_text(encoding="utf-8"))["max_new_orders"], 4)
            history = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(history[-1]["action"], "rollback")
            self.assertEqual(history[-1]["after_config"]["max_new_orders"], 4)

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
        self.assertEqual(mock_run.call_count, 3)
        restart_command = mock_run.call_args_list[2].args[0]
        self.assertEqual(
            restart_command,
            ["ssh", "srv-43-131-232-150", "/usr/local/bin/grid-saved-runner-api2", "restart", "PHAROSUSDT"],
        )


if __name__ == "__main__":
    unittest.main()
