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
    REMOTE_RUNTIME_EVENTS_SCRIPT,
    REMOTE_WRITE_CONTROL_SCRIPT,
    apply_remote_workbench_config,
    build_pharos_recommendation_presets,
    build_pharos_runtime_summary,
    build_workbench_payload,
    get_central_strategy_target,
    list_central_strategy_targets,
    read_remote_runtime_events,
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
        self.assertIn("recommendation_presets", payload)
        self.assertEqual(
            [item["preset_id"] for item in payload["recommendation_presets"]],
            ["conservative_profit", "balanced_volume", "final_day_volume"],
        )
        self.assertIn("runtime_summary", payload)

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

    def test_remote_runtime_events_script_tails_and_filters_summary_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            control_path = output_dir / "pharosusdt_loop_runner_control.json"
            summary_path = output_dir / "pharosusdt_hedge_bq_events.jsonl"
            control_path.write_text(
                json.dumps({"summary_jsonl": "output/pharosusdt_hedge_bq_events.jsonl"}),
                encoding="utf-8",
            )
            summary_path.write_text(
                "\n".join(
                    json.dumps(
                        {
                            "ts": f"2026-05-22T04:0{i}:00+00:00",
                            "cycle": i,
                            "cumulative_gross_notional": i * 100,
                            "actual_net_notional": i,
                            "large_unused_blob": "x" * 100,
                        },
                    )
                    for i in range(5)
                )
                + "\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                [sys.executable, "-c", REMOTE_RUNTIME_EVENTS_SCRIPT, str(control_path), "2"],
                capture_output=True,
                check=False,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["error"], "")
            self.assertEqual([event["cycle"] for event in payload["events"]], [3, 4])
            self.assertNotIn("large_unused_blob", payload["events"][0])

    def test_build_pharos_runtime_summary_reports_brush_state_and_loss(self) -> None:
        summary = build_pharos_runtime_summary(
            [
                {
                    "ts": "2026-05-22T04:00:00+00:00",
                    "cycle": 10,
                    "cumulative_gross_notional": 1000.0,
                    "rolling_hourly_gross_notional": 800.0,
                    "rolling_hourly_loss": 0.4,
                    "rolling_hourly_loss_per_10k": 5.0,
                    "actual_net_notional": 30.0,
                    "current_long_notional": 40.0,
                    "current_short_notional": 10.0,
                    "unrealized_pnl": -0.2,
                    "open_order_count": 2,
                    "stale_order_count": 0,
                    "missing_order_count": 0,
                    "volatility_entry_pause_active": False,
                },
                {
                    "ts": "2026-05-22T04:05:00+00:00",
                    "cycle": 12,
                    "cumulative_gross_notional": 1250.0,
                    "rolling_hourly_gross_notional": 1050.0,
                    "rolling_hourly_loss": 0.5,
                    "rolling_hourly_loss_per_10k": 4.76,
                    "actual_net_notional": 35.0,
                    "current_long_notional": 45.0,
                    "current_short_notional": 10.0,
                    "unrealized_pnl": -0.3,
                    "open_order_count": 2,
                    "stale_order_count": 0,
                    "missing_order_count": 0,
                    "volatility_entry_pause_active": False,
                },
            ],
            {"threshold_position_notional": 630.0, "max_cumulative_notional": 200000.0},
        )

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["state"], "brush")
        self.assertEqual(summary["mode"], "normal/fast")
        self.assertEqual(summary["event_count"], 2)
        self.assertEqual(summary["window_gross_notional"], 250.0)
        self.assertEqual(summary["latest"]["rolling_hourly_loss_per_10k"], 4.76)
        self.assertIn("刷量状态", summary["state_label"])

    def test_build_pharos_runtime_summary_reports_1h_24h_and_maker_quality(self) -> None:
        summary = build_pharos_runtime_summary(
            [
                {
                    "ts": "2026-05-21T03:59:00+00:00",
                    "cumulative_gross_notional": 100.0,
                    "gross_notional": 100.0,
                    "realized_pnl": -0.1,
                    "commission": 0.02,
                    "maker_notional": 100.0,
                    "taker_notional": 0.0,
                },
                {
                    "ts": "2026-05-21T04:30:00+00:00",
                    "cumulative_gross_notional": 300.0,
                    "gross_notional": 200.0,
                    "realized_pnl": -0.2,
                    "commission": 0.04,
                    "maker_notional": 150.0,
                    "taker_notional": 50.0,
                },
                {
                    "ts": "2026-05-22T03:40:00+00:00",
                    "cumulative_gross_notional": 600.0,
                    "gross_notional": 300.0,
                    "rolling_hourly_gross_notional": 300.0,
                    "rolling_hourly_loss": 0.25,
                    "rolling_hourly_loss_per_10k": 8.33,
                    "realized_pnl": -0.3,
                    "commission": 0.06,
                    "maker_notional": 300.0,
                    "taker_notional": 0.0,
                    "actual_net_notional": 40.0,
                    "unrealized_pnl": -0.4,
                },
                {
                    "ts": "2026-05-22T04:10:00+00:00",
                    "cumulative_gross_notional": 1000.0,
                    "gross_notional": 400.0,
                    "rolling_hourly_gross_notional": 700.0,
                    "rolling_hourly_loss": 0.8,
                    "rolling_hourly_loss_per_10k": 11.43,
                    "realized_pnl": -0.4,
                    "commission": 0.08,
                    "maker_notional": 250.0,
                    "taker_notional": 150.0,
                    "actual_net_notional": 45.0,
                    "unrealized_pnl": -0.5,
                    "open_order_count": 2,
                },
            ],
            {"threshold_position_notional": 630.0, "max_cumulative_notional": 200000.0},
        )

        self.assertEqual(summary["windows"]["1h"]["gross_notional"], 700.0)
        self.assertEqual(summary["windows"]["24h"]["gross_notional"], 900.0)
        self.assertEqual(summary["windows"]["1h"]["wear"], 0.84)
        self.assertAlmostEqual(summary["windows"]["1h"]["wear_per_10k"], 12.0, places=5)
        self.assertAlmostEqual(summary["maker_quality"]["maker_ratio"], 700.0 / 900.0)
        self.assertEqual(summary["maker_quality"]["source"], "events")

    def test_build_pharos_runtime_summary_marks_missing_maker_source(self) -> None:
        summary = build_pharos_runtime_summary(
            [
                {
                    "ts": "2026-05-22T04:10:00+00:00",
                    "cumulative_gross_notional": 1000.0,
                    "gross_notional": 400.0,
                    "actual_net_notional": 45.0,
                    "unrealized_pnl": -0.5,
                },
            ],
            {"threshold_position_notional": 630.0},
        )

        self.assertIsNone(summary["maker_quality"]["maker_ratio"])
        self.assertEqual(summary["maker_quality"]["source"], "missing")
        self.assertIn("maker_notional", summary["maker_quality"]["required_fields"])

    def test_build_pharos_runtime_summary_reports_repair_state_for_large_inventory(self) -> None:
        summary = build_pharos_runtime_summary(
            [
                {
                    "ts": "2026-05-22T04:05:00+00:00",
                    "cumulative_gross_notional": 1250.0,
                    "rolling_hourly_gross_notional": 1050.0,
                    "actual_net_notional": 700.0,
                    "threshold_position_notional": 630.0,
                    "unrealized_pnl": -3.0,
                    "open_order_count": 1,
                    "volatility_entry_pause_active": True,
                },
            ],
            {"threshold_position_notional": 630.0},
        )

        self.assertEqual(summary["state"], "repair")
        self.assertEqual(summary["mode"], "recover/safe")
        self.assertIn("修仓状态", summary["state_label"])
        self.assertIn("net_inventory_over_soft_threshold", summary["reason_codes"])

    def test_build_pharos_runtime_summary_reports_degraded_state_for_stale_orders(self) -> None:
        summary = build_pharos_runtime_summary(
            [
                {
                    "ts": "2026-05-22T04:05:00+00:00",
                    "cumulative_gross_notional": 1250.0,
                    "rolling_hourly_gross_notional": 1050.0,
                    "actual_net_notional": 40.0,
                    "unrealized_pnl": -0.2,
                    "open_order_count": 2,
                    "stale_order_count": 1,
                    "missing_order_count": 1,
                    "volatility_entry_pause_active": False,
                },
            ],
            {"threshold_position_notional": 630.0},
        )

        self.assertEqual(summary["state"], "degraded")
        self.assertEqual(summary["mode"], "safe/watch")
        self.assertIn("stale_orders_present", summary["reason_codes"])

    @patch("grid_optimizer.central_strategy_workbench.subprocess.run")
    def test_read_remote_runtime_events_uses_short_cache(self, mock_run) -> None:
        mock_run.return_value = Mock(
            returncode=0,
            stdout=json.dumps({"events": [{"cycle": 1}], "error": ""}),
            stderr="",
        )
        target = get_central_strategy_target("114", "PHAROSUSDT", "pharosusdt_hedge_best_quote_maker_volume_v1")

        first_events, first_error = read_remote_runtime_events(target, limit=120)
        second_events, second_error = read_remote_runtime_events(target, limit=120)

        self.assertEqual(first_events, [{"cycle": 1}])
        self.assertEqual(second_events, [{"cycle": 1}])
        self.assertEqual(first_error, "")
        self.assertEqual(second_error, "")
        self.assertEqual(mock_run.call_count, 1)

    def test_build_pharos_recommendation_presets_returns_patch_values(self) -> None:
        presets = build_pharos_recommendation_presets(
            {
                "per_order_notional": 10.0,
                "max_new_orders": 5,
                "max_total_notional": 1450.0,
                "rolling_hourly_loss_limit": 8.0,
                "max_cumulative_notional": 200000.0,
            },
            {"state": "brush", "latest": {"rolling_hourly_loss_per_10k": 3.0}},
        )

        self.assertEqual([item["preset_id"] for item in presets], ["conservative_profit", "balanced_volume", "final_day_volume"])
        final_day = presets[-1]
        self.assertGreater(final_day["patch"]["per_order_notional"], 10.0)
        self.assertGreaterEqual(final_day["patch"]["max_cumulative_notional"], 500000.0)
        self.assertIn("适合", final_day["scenario"])

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
