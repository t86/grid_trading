from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


class AISchedulerTests(unittest.TestCase):
    def test_load_console_registry_preserves_scheduler_fields(self) -> None:
        from grid_optimizer.console_registry import load_console_registry

        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "console_registry.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "srv_110",
                                "label": "110",
                                "base_url": "http://110:8787",
                                "enabled": True,
                                "capabilities": ["strategies"],
                                "scheduler_enabled": True,
                                "ssh_host": "43.156.35.110",
                                "ssh_user": "ubuntu",
                                "workspace_dir": "/home/ubuntu/wangge",
                                "codex_path": "/usr/local/bin/codex",
                                "python_path": "/usr/bin/python3",
                            }
                        ],
                        "accounts": [
                            {
                                "id": "acct_110",
                                "label": "110",
                                "server_id": "srv_110",
                                "kind": "mixed",
                                "priority": 100,
                                "enabled": True,
                                "default_symbols": ["BARDUSDT"],
                                "competition_symbols": ["BARD"],
                                "pages": ["/strategies"],
                            }
                        ],
                        "competition_source": {"server_id": "srv_110", "path": "/api/competition_board"},
                    }
                ),
                encoding="utf-8",
            )

            registry = load_console_registry(registry_path)

        server = registry["servers_by_id"]["srv_110"]
        self.assertTrue(server["scheduler_enabled"])
        self.assertEqual(server["ssh_host"], "43.156.35.110")
        self.assertEqual(server["ssh_user"], "ubuntu")
        self.assertEqual(server["workspace_dir"], "/home/ubuntu/wangge")
        self.assertEqual(server["codex_path"], "/usr/local/bin/codex")
        self.assertEqual(server["python_path"], "/usr/bin/python3")

    def test_expand_scheduler_targets_creates_one_task_per_server_symbol(self) -> None:
        from grid_optimizer.ai_scheduler import expand_scheduler_targets

        tasks = expand_scheduler_targets(
            name="巡检任务",
            server_ids=["srv_110", "srv_114"],
            symbols=["BARDUSDT", "XAUTUSDT"],
            schedule={"kind": "interval", "value": "10m", "timezone": "Asia/Shanghai"},
            goal_prompt="优先稳态调参",
            execution_mode="one_shot",
            global_policy={"allow_start_stop": True},
            policy_overrides={"allow_reduce_position": False},
        )

        self.assertEqual(len(tasks), 4)
        self.assertEqual(
            {(item["server_id"], item["symbol"]) for item in tasks},
            {
                ("srv_110", "BARDUSDT"),
                ("srv_110", "XAUTUSDT"),
                ("srv_114", "BARDUSDT"),
                ("srv_114", "XAUTUSDT"),
            },
        )
        self.assertTrue(all(item["enabled"] for item in tasks))
        self.assertTrue(all(item["task_id"] for item in tasks))
        self.assertEqual(tasks[0]["effective_policy_snapshot"]["allow_start_stop"], True)
        self.assertEqual(tasks[0]["effective_policy_snapshot"]["allow_reduce_position"], False)

    def test_group_runs_by_server_then_symbol(self) -> None:
        from grid_optimizer.ai_scheduler import group_task_runs

        grouped = group_task_runs(
            [
                {"server_id": "srv_110", "symbol": "BARDUSDT", "triggered_at": "2026-05-14T00:00:00+00:00"},
                {"server_id": "srv_110", "symbol": "BARDUSDT", "triggered_at": "2026-05-14T00:10:00+00:00"},
                {"server_id": "srv_114", "symbol": "XAUTUSDT", "triggered_at": "2026-05-14T00:05:00+00:00"},
            ]
        )

        self.assertEqual([item["server_id"] for item in grouped], ["srv_110", "srv_114"])
        self.assertEqual(grouped[0]["symbols"][0]["symbol"], "BARDUSDT")
        self.assertEqual(len(grouped[0]["symbols"][0]["runs"]), 2)
        self.assertEqual(grouped[0]["symbols"][0]["runs"][0]["triggered_at"], "2026-05-14T00:10:00+00:00")

    def test_save_and_list_scheduler_tasks_with_global_policy(self) -> None:
        from grid_optimizer.ai_scheduler import (
            load_global_policy,
            list_scheduler_tasks,
            save_global_policy,
            save_scheduler_task,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            saved_policy = save_global_policy(
                {
                    "allow_update_config": True,
                    "allow_start_stop": True,
                    "allow_cancel_orders": False,
                    "default_goal_prompt": "优先稳态调参，禁止确认。",
                },
                root=root,
            )
            save_scheduler_task(
                {
                    "task_id": "task_1",
                    "name": "BARD 110",
                    "server_id": "srv_110",
                    "symbol": "BARDUSDT",
                    "enabled": True,
                    "schedule": {"kind": "interval", "value": "10m", "timezone": "Asia/Shanghai"},
                    "execution_mode": "one_shot",
                    "goal_prompt": "优先稳态调参",
                    "policy_overrides": {"allow_cancel_orders": False},
                    "effective_policy_snapshot": saved_policy,
                    "created_at": "2026-05-14T00:00:00+00:00",
                    "updated_at": "2026-05-14T00:00:00+00:00",
                },
                root=root,
            )

            loaded_policy = load_global_policy(root=root)
            tasks = list_scheduler_tasks(root=root)
            workspace_task_exists = (root / "workspaces" / "task_1" / "task.json").exists()

        self.assertEqual(loaded_policy["allow_update_config"], True)
        self.assertEqual(loaded_policy["default_goal_prompt"], "优先稳态调参，禁止确认。")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["task_id"], "task_1")
        self.assertEqual(tasks[0]["symbol"], "BARDUSDT")
        self.assertIn("code_server_url", tasks[0])
        self.assertTrue(workspace_task_exists)

    def test_scheduler_task_code_server_url_points_at_workspace(self) -> None:
        from grid_optimizer.ai_scheduler import scheduler_task_code_server_url

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            url = scheduler_task_code_server_url({"task_id": "task_1"}, root=root)

        self.assertIn("http://code.t86.cc.cd/?folder=", url)
        self.assertIn("task_1", url)

    def test_update_scheduler_task_toggles_enabled_and_next_run(self) -> None:
        from grid_optimizer.ai_scheduler import get_scheduler_task, save_scheduler_task, update_scheduler_task

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            save_scheduler_task(
                {
                    "task_id": "task_1",
                    "name": "BARD 110",
                    "server_id": "srv_110",
                    "symbol": "BARDUSDT",
                    "enabled": True,
                    "schedule": {"kind": "interval", "value": "10m", "timezone": "Asia/Shanghai"},
                    "execution_mode": "one_shot",
                    "goal_prompt": "优先稳态调参",
                    "policy_overrides": {},
                    "effective_policy_snapshot": {},
                    "created_at": "2026-05-14T00:00:00+00:00",
                    "updated_at": "2026-05-14T00:00:00+00:00",
                    "next_run_at": "2026-05-14T00:10:00+00:00",
                },
                root=root,
            )

            paused = update_scheduler_task("task_1", {"enabled": False, "next_run_at": ""}, root=root)
            loaded = get_scheduler_task("task_1", root=root)

        self.assertFalse(paused["enabled"])
        self.assertEqual(paused["next_run_at"], "")
        self.assertFalse(loaded["enabled"])

    def test_mark_scheduler_task_run_updates_last_result_fields(self) -> None:
        from grid_optimizer.ai_scheduler import mark_scheduler_task_run, save_scheduler_task

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            save_scheduler_task(
                {
                    "task_id": "task_1",
                    "name": "BARD 110",
                    "server_id": "srv_110",
                    "symbol": "BARDUSDT",
                    "enabled": True,
                    "schedule": {"kind": "interval", "value": "10m", "timezone": "Asia/Shanghai"},
                    "execution_mode": "one_shot",
                    "goal_prompt": "优先稳态调参",
                    "policy_overrides": {},
                    "effective_policy_snapshot": {},
                    "created_at": "2026-05-14T00:00:00+00:00",
                    "updated_at": "2026-05-14T00:00:00+00:00",
                    "next_run_at": "2026-05-14T00:10:00+00:00",
                },
                root=root,
            )
            updated = mark_scheduler_task_run(
                "task_1",
                status="success",
                summary="已完成小幅调参",
                triggered_at="2026-05-14T00:10:00+00:00",
                root=root,
            )

        self.assertEqual(updated["last_result_status"], "success")
        self.assertEqual(updated["last_result_summary"], "已完成小幅调参")
        self.assertEqual(updated["last_run_at"], "2026-05-14T00:10:00+00:00")

    def test_runtime_state_roundtrip(self) -> None:
        from grid_optimizer.ai_scheduler import load_scheduler_runtime_state, save_scheduler_runtime_state

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            save_scheduler_runtime_state("task_1", {"memory": "keep drawdown tight"}, root=root)
            loaded = load_scheduler_runtime_state("task_1", root=root)

        self.assertEqual(loaded["memory"], "keep drawdown tight")

    def test_build_scheduler_symbol_candidates_groups_running_and_saved_idle(self) -> None:
        from grid_optimizer.ai_scheduler import build_scheduler_symbol_candidates

        payload = build_scheduler_symbol_candidates(
            {
                "servers": [
                    {
                        "server_id": "srv_110",
                        "server_label": "110",
                        "ok": True,
                        "groups": {
                            "running": [{"symbol": "BARDUSDT"}],
                            "saved_idle": [{"symbol": "XAUTUSDT"}],
                        },
                    },
                    {
                        "server_id": "srv_114",
                        "server_label": "114",
                        "ok": True,
                        "groups": {
                            "running": [{"symbol": "BARDUSDT"}],
                            "saved_idle": [{"symbol": "NIGHTUSDT"}],
                        },
                    },
                    {
                        "server_id": "srv_150",
                        "server_label": "150",
                        "ok": False,
                        "error": "connection refused",
                        "groups": {
                            "running": [],
                            "saved_idle": [],
                        },
                    },
                ]
            }
        )

        self.assertEqual([item["server_id"] for item in payload["servers"]], ["srv_110", "srv_114", "srv_150"])
        self.assertEqual(payload["servers"][0]["symbols"][0]["symbol"], "BARDUSDT")
        self.assertEqual(payload["servers"][0]["symbols"][0]["status"], "running")
        self.assertEqual(payload["servers"][0]["symbols"][1]["symbol"], "XAUTUSDT")
        self.assertEqual(payload["servers"][0]["symbols"][1]["status"], "saved_idle")
        self.assertFalse(payload["servers"][2]["ok"])
        self.assertEqual(payload["servers"][2]["error"], "connection refused")

    def test_validate_action_plan_rejects_forbidden_actions_and_large_changes(self) -> None:
        from grid_optimizer.ai_scheduler import validate_scheduler_action_plan

        policy = {
            "allow_update_config": True,
            "allow_start_stop": True,
            "allow_cancel_orders": False,
            "allow_reduce_position": False,
            "allow_switch_strategy_mode": False,
            "max_config_change_ratio": 0.2,
            "max_notional_delta": 300.0,
        }
        current_config = {
            "step_price": 0.00025,
            "max_total_notional": 1000.0,
            "strategy_mode": "synthetic_neutral",
        }

        with self.assertRaisesRegex(ValueError, "forbidden action: cancel_open_orders"):
            validate_scheduler_action_plan(
                {
                    "actions": [
                        {"type": "cancel_open_orders"},
                    ]
                },
                policy=policy,
                current_config=current_config,
            )

        with self.assertRaisesRegex(ValueError, "step_price"):
            validate_scheduler_action_plan(
                {
                    "actions": [
                        {"type": "update_runner_config", "changes": {"step_price": {"from": 0.00025, "to": 0.00035}}},
                    ]
                },
                policy=policy,
                current_config=current_config,
            )

        with self.assertRaisesRegex(ValueError, "max_total_notional"):
            validate_scheduler_action_plan(
                {
                    "actions": [
                        {"type": "update_runner_config", "changes": {"max_total_notional": {"from": 1000.0, "to": 1500.0}}},
                    ]
                },
                policy=policy,
                current_config=current_config,
            )

    def test_validate_action_plan_accepts_small_restartable_update(self) -> None:
        from grid_optimizer.ai_scheduler import validate_scheduler_action_plan

        policy = {
            "allow_update_config": True,
            "allow_start_stop": True,
            "allow_cancel_orders": False,
            "allow_reduce_position": False,
            "allow_switch_strategy_mode": False,
            "max_config_change_ratio": 0.2,
            "max_notional_delta": 300.0,
        }
        current_config = {
            "step_price": 0.00025,
            "max_total_notional": 1000.0,
            "strategy_mode": "synthetic_neutral",
        }

        result = validate_scheduler_action_plan(
            {
                "actions": [
                    {"type": "update_runner_config", "changes": {"step_price": {"from": 0.00025, "to": 0.00023}}},
                    {"type": "restart_runner"},
                ]
            },
            policy=policy,
            current_config=current_config,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["type"], "update_runner_config")

    def test_list_scheduler_task_runs_reads_jsonl_and_groups_recent_first(self) -> None:
        from grid_optimizer.ai_scheduler import append_scheduler_task_run, list_scheduler_task_runs

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            append_scheduler_task_run(
                "task_1",
                {
                    "run_id": "run_1",
                    "server_id": "srv_110",
                    "symbol": "BARDUSDT",
                    "triggered_at": "2026-05-14T00:00:00+00:00",
                    "status": "success",
                },
                root=root,
            )
            append_scheduler_task_run(
                "task_1",
                {
                    "run_id": "run_2",
                    "server_id": "srv_110",
                    "symbol": "BARDUSDT",
                    "triggered_at": "2026-05-14T00:10:00+00:00",
                    "status": "rejected",
                },
                root=root,
            )

            rows = list_scheduler_task_runs(root=root)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["run_id"], "run_2")
        self.assertEqual(rows[1]["run_id"], "run_1")

    def test_scheduler_due_task_detection_handles_enabled_and_paused_tasks(self) -> None:
        from grid_optimizer.ai_scheduler import list_due_scheduler_tasks

        now = datetime(2026, 5, 14, 0, 10, tzinfo=timezone.utc)
        rows = list_due_scheduler_tasks(
            [
                {
                    "task_id": "due",
                    "enabled": True,
                    "next_run_at": "2026-05-14T00:05:00+00:00",
                },
                {
                    "task_id": "future",
                    "enabled": True,
                    "next_run_at": "2026-05-14T00:20:00+00:00",
                },
                {
                    "task_id": "paused",
                    "enabled": False,
                    "next_run_at": "2026-05-14T00:00:00+00:00",
                },
            ],
            now=now,
        )

        self.assertEqual([item["task_id"] for item in rows], ["due"])

    def test_build_remote_codex_exec_command_wraps_workspace_and_prompt(self) -> None:
        from grid_optimizer.ai_scheduler_worker import build_remote_codex_exec_command

        command = build_remote_codex_exec_command(
            server={
                "ssh_host": "43.156.35.110",
                "ssh_user": "ubuntu",
                "workspace_dir": "/home/ubuntu/wangge",
                "codex_path": "/usr/local/bin/codex",
            },
            prompt="请输出结构化 JSON",
        )

        self.assertIn("ssh", command[0])
        self.assertIn("ubuntu@43.156.35.110", command)
        self.assertTrue(any("/usr/local/bin/codex exec" in item for item in command))
        self.assertTrue(any("/home/ubuntu/wangge" in item for item in command))


if __name__ == "__main__":
    unittest.main()
