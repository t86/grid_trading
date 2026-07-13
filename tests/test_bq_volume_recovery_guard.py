from __future__ import annotations

import json
import os
import subprocess
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer import bq_volume_recovery_guard
from grid_optimizer.bq_volume_recovery_guard import (
    _apply_control_update,
    _arx_independent_freeze_policy_updates,
    apply_daily_target_pace_floor,
    apply_target_pace_cycle_budget_floor,
    check_symbol,
    fetch_recent_user_trades,
    recover_corrupt_loop_state,
    recover_inactive_runner,
    summarize_recent_volume,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _append_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows),
        encoding="utf-8",
    )


class BqVolumeRecoveryGuardTests(unittest.TestCase):
    def test_high_recovery_wear_requires_15m_confirmation_or_extreme_5m(self) -> None:
        self.assertFalse(
            bq_volume_recovery_guard._is_high_recovery_wear(
                {
                    "trailing_15m_gross_notional": 217.0,
                    "trailing_5m_realized_wear_per_10k": 26.7,
                    "trailing_15m_realized_wear_per_10k": -19.1,
                }
            )
        )
        self.assertTrue(
            bq_volume_recovery_guard._is_high_recovery_wear(
                {
                    "trailing_15m_gross_notional": 217.0,
                    "trailing_5m_realized_wear_per_10k": 4.0,
                    "trailing_15m_realized_wear_per_10k": 3.1,
                }
            )
        )
        self.assertTrue(
            bq_volume_recovery_guard._is_high_recovery_wear(
                {
                    "trailing_15m_gross_notional": 217.0,
                    "trailing_5m_realized_wear_per_10k": 80.1,
                    "trailing_15m_realized_wear_per_10k": -10.0,
                }
            )
        )

    def test_arx_independent_freeze_policy_only_disables_pair_gate_for_arx(self) -> None:
        control = {
            "best_quote_maker_volume_reduce_freeze_enabled": True,
            "best_quote_maker_volume_reduce_freeze_profitable_pair_gate_enabled": True,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
        }

        self.assertEqual(
            _arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control),
            {
                "best_quote_maker_volume_reduce_freeze_loss_ratio": 0.01,
                "best_quote_maker_volume_reduce_freeze_band_budget_enabled": True,
                "best_quote_maker_volume_reduce_freeze_band_budget_price_ratio": 0.01,
                "best_quote_maker_volume_reduce_freeze_band_budget_base_notional": 100.0,
                "best_quote_maker_volume_frozen_total_cap_notional": 800.0,
                "best_quote_maker_volume_frozen_long_cap_notional": 800.0,
                "best_quote_maker_volume_frozen_short_cap_notional": 800.0,
                "best_quote_maker_volume_reduce_freeze_quality_gate_enabled": True,
                "best_quote_maker_volume_reduce_freeze_quality_max_loss_ratio": 0.03,
                "best_quote_maker_volume_reduce_freeze_quality_release_profit_ratio": 0.002,
                "best_quote_maker_volume_reduce_freeze_quality_max_atr_multiple": 3.0,
                "best_quote_maker_volume_reduce_freeze_quality_atr_floor_ratio": 0.004,
                "best_quote_maker_volume_reduce_freeze_quality_easy_bucket_notional": 100.0,
                "best_quote_maker_volume_reduce_freeze_quality_medium_bucket_notional": 50.0,
                "best_quote_maker_volume_reduce_freeze_quality_hard_bucket_notional": 25.0,
                "best_quote_maker_volume_reduce_freeze_profitable_pair_gate_enabled": False,
                "best_quote_maker_volume_frozen_pair_release_enabled": False,
                "best_quote_maker_volume_frozen_single_leg_take_profit_enabled": True,
                "best_quote_maker_volume_frozen_pair_release_min_profit_ratio": 0.01,
                "best_quote_maker_volume_frozen_pair_release_allow_loss": False,
                "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
                "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
                "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 200.0,
                "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 1,
            },
        )
        self.assertEqual(
            _arx_independent_freeze_policy_updates(symbol="OUSDT", control=control),
            {},
        )

    def test_arx_independent_freeze_policy_is_stable_at_expected_values(self) -> None:
        control = {
            "best_quote_maker_volume_reduce_freeze_enabled": True,
            "best_quote_maker_volume_reduce_freeze_loss_ratio": 0.01,
            "best_quote_maker_volume_reduce_freeze_band_budget_enabled": True,
            "best_quote_maker_volume_reduce_freeze_band_budget_price_ratio": 0.01,
            "best_quote_maker_volume_reduce_freeze_band_budget_base_notional": 100.0,
            "best_quote_maker_volume_frozen_total_cap_notional": 800.0,
            "best_quote_maker_volume_frozen_long_cap_notional": 800.0,
            "best_quote_maker_volume_frozen_short_cap_notional": 800.0,
            "best_quote_maker_volume_reduce_freeze_quality_gate_enabled": True,
            "best_quote_maker_volume_reduce_freeze_quality_max_loss_ratio": 0.03,
            "best_quote_maker_volume_reduce_freeze_quality_release_profit_ratio": 0.002,
            "best_quote_maker_volume_reduce_freeze_quality_max_atr_multiple": 3.0,
            "best_quote_maker_volume_reduce_freeze_quality_atr_floor_ratio": 0.004,
            "best_quote_maker_volume_reduce_freeze_quality_easy_bucket_notional": 100.0,
            "best_quote_maker_volume_reduce_freeze_quality_medium_bucket_notional": 50.0,
            "best_quote_maker_volume_reduce_freeze_quality_hard_bucket_notional": 25.0,
            "best_quote_maker_volume_reduce_freeze_profitable_pair_gate_enabled": False,
            "best_quote_maker_volume_frozen_pair_release_enabled": False,
            "best_quote_maker_volume_frozen_single_leg_take_profit_enabled": True,
            "best_quote_maker_volume_frozen_pair_release_min_profit_ratio": 0.01,
            "best_quote_maker_volume_frozen_pair_release_allow_loss": False,
            "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
            "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
            "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 200.0,
            "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 1,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
        }

        self.assertEqual(
            _arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control),
            {},
        )

        control["best_quote_maker_volume_same_side_entry_price_guard_min_notional"] = 450.0
        self.assertEqual(
            _arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control),
            {},
        )
        self.assertEqual(
            _arx_independent_freeze_policy_updates(
                symbol="ARXUSDT",
                control=control,
                temporary_anti_chase_relief=True,
            ),
            {},
        )

    def test_main_never_restarts_symbol_after_target_gate_done(self) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            marker = output_dir / f"arxusdt_target_gate_done_{now.strftime('%Y%m%d')}.flag"
            marker.write_text(now.isoformat(), encoding="utf-8")
            stdout = StringIO()

            with (
                patch.object(bq_volume_recovery_guard, "_runner_is_active", return_value=False) as runner_active,
                patch.object(bq_volume_recovery_guard, "_fetch_exchange_user_trades") as fetch_trades,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--symbols",
                        "ARXUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            runner_active.assert_called_once_with("ARXUSDT")
            fetch_trades.assert_not_called()
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["results"][0]["action"], "skip_target_gate_done_terminal")
            self.assertEqual(payload["results"][0]["target_gate_done_marker"], str(marker.resolve()))

    def test_main_stops_active_runner_after_target_gate_done(self) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            marker = output_dir / f"ousdt_target_gate_done_{now.strftime('%Y%m%d')}.flag"
            marker.write_text(now.isoformat(), encoding="utf-8")
            stdout = StringIO()

            with (
                patch.object(bq_volume_recovery_guard, "_runner_is_active", return_value=True),
                patch.object(bq_volume_recovery_guard, "_default_stop_runner") as stop_runner,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--runner-wrapper",
                        "/usr/local/bin/test-wrapper",
                        "--symbols",
                        "OUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            stop_runner.assert_called_once_with("OUSDT", runner_wrapper="/usr/local/bin/test-wrapper")
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["results"][0]["action"], "stop_runner_target_gate_done")

    def test_restores_persistent_corrupt_state_from_recent_valid_backup(self) -> None:
        now = datetime(2026, 7, 11, 11, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now - timedelta(minutes=10))
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text('{"broken":', encoding="utf-8")
            backup_path = output_dir / "reusdt_loop_state.json.bak_autorealign_123"
            _write_json(backup_path, {"best_quote_volume_ledger": {"long_lots": []}})

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now,
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=0,
                max_snapshot_age_seconds=300,
                dry_run=False,
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["action"], "restore_corrupt_state_from_autorealign_backup")
            self.assertEqual(
                json.loads(state_path.read_text(encoding="utf-8")),
                {"best_quote_volume_ledger": {"long_lots": []}},
            )
            self.assertTrue(Path(result["corrupt_archive_path"]).exists())
            self.assertTrue(Path(result["archived_plan_path"]).exists())

    def test_main_stops_active_runner_with_persistent_unrecoverable_corrupt_state(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            corrupt_result = {
                "symbol": "OUSDT",
                "action": "skip_corrupt_state_recovery_safety_gate",
                "changed_keys": [],
                "backup_path": None,
                "dry_run": False,
                "restart_failed": None,
                "state_corruption": {
                    "blocking_reasons": ["no_recent_valid_autorealign_backup"],
                },
            }
            with (
                patch.object(
                    bq_volume_recovery_guard,
                    "recover_corrupt_loop_state",
                    return_value=corrupt_result,
                ),
                patch.object(bq_volume_recovery_guard, "_runner_is_active", return_value=True),
                patch.object(bq_volume_recovery_guard, "_default_stop_runner") as stop_runner,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "guard_state.json"),
                        "--runner-wrapper",
                        "/usr/local/bin/test-wrapper",
                        "--symbols",
                        "OUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            stop_runner.assert_called_once_with(
                "OUSDT", runner_wrapper="/usr/local/bin/test-wrapper"
            )
            payload = json.loads(stdout.getvalue())
            self.assertEqual(
                payload["results"][0]["action"],
                "stop_runner_persistent_corrupt_state",
            )

    def test_main_does_not_stop_runner_for_transient_corrupt_state(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            corrupt_result = {
                "symbol": "OUSDT",
                "action": "skip_corrupt_state_recovery_safety_gate",
                "changed_keys": [],
                "backup_path": None,
                "dry_run": False,
                "restart_failed": None,
                "state_corruption": {
                    "blocking_reasons": ["corrupt_state_not_persistent"],
                },
            }
            with (
                patch.object(
                    bq_volume_recovery_guard,
                    "recover_corrupt_loop_state",
                    return_value=corrupt_result,
                ),
                patch.object(bq_volume_recovery_guard, "_runner_is_active") as runner_active,
                patch.object(bq_volume_recovery_guard, "_default_stop_runner") as stop_runner,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "guard_state.json"),
                        "--symbols",
                        "OUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            runner_active.assert_not_called()
            stop_runner.assert_not_called()
            payload = json.loads(stdout.getvalue())
            self.assertEqual(
                payload["results"][0]["action"],
                "skip_corrupt_state_recovery_safety_gate",
            )

    def test_main_restarts_runner_after_live_validated_state_salvage(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            corrupt_result = {
                "symbol": "OUSDT",
                "action": "salvage_corrupt_state_applied_trade_fill_keys",
                "changed_keys": [],
                "backup_path": None,
                "dry_run": False,
                "restart_failed": None,
                "safe_restart_after_salvage": True,
                "state_corruption": {"blocking_reasons": []},
            }
            with (
                patch.object(
                    bq_volume_recovery_guard,
                    "recover_corrupt_loop_state",
                    return_value=corrupt_result,
                ),
                patch.object(bq_volume_recovery_guard, "_runner_is_active", return_value=False),
                patch.object(bq_volume_recovery_guard, "_default_restart_runner") as restart_runner,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "guard_state.json"),
                        "--runner-wrapper",
                        "/usr/local/bin/test-wrapper",
                        "--symbols",
                        "OUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            restart_runner.assert_called_once_with(
                "OUSDT", runner_wrapper="/usr/local/bin/test-wrapper"
            )
            payload = json.loads(stdout.getvalue())
            self.assertEqual(
                payload["results"][0]["action"],
                "salvage_corrupt_state_applied_trade_fill_keys_and_restart_runner",
            )

    def test_control_update_snapshots_valid_loop_state_before_restart(self) -> None:
        now = datetime(2026, 7, 12, 16, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            control_path = output_dir / "arxusdt_loop_runner_control.json"
            state_path = output_dir / "arxusdt_loop_state.json"
            control = {"best_quote_maker_volume_cycle_budget_notional": 120.0}
            _write_json(control_path, control)
            _write_json(state_path, {"best_quote_volume_ledger": {"long_lots": []}})
            restarted: list[str] = []

            changed, _ = _apply_control_update(
                symbol="ARXUSDT",
                control_path=control_path,
                control=control,
                updates={"best_quote_maker_volume_cycle_budget_notional": 160.0},
                now=now,
                dry_run=False,
                restart_runner=restarted.append,
            )

            self.assertEqual(changed, ["best_quote_maker_volume_cycle_budget_notional"])
            self.assertEqual(restarted, ["ARXUSDT"])
            backup = output_dir / "arxusdt_loop_state.json.bak_bq_recovery_restart"
            self.assertEqual(
                json.loads(backup.read_text(encoding="utf-8")),
                {"best_quote_volume_ledger": {"long_lots": []}},
            )
            self.assertEqual(backup.stat().st_mtime, now.timestamp())

    def test_control_update_reconciles_against_latest_external_control(self) -> None:
        """A guard decision must not erase a newer non-owned control field.

        This is the regression seam for the 114 multi-controller race: the
        guard reads a snapshot, another monitor writes a newer offset, then
        the guard applies only its own budget change.
        """
        now = datetime(2026, 7, 13, 6, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            control_path = Path(tmpdir) / "arxusdt_loop_runner_control.json"
            stale_control = {
                "best_quote_maker_volume_cycle_budget_notional": 120.0,
                "best_quote_maker_volume_quote_offset_ticks": 2,
            }
            _write_json(control_path, stale_control)
            _write_json(
                control_path,
                {
                    "best_quote_maker_volume_cycle_budget_notional": 120.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "external_monitor_generation": 9,
                },
            )

            changed, _ = _apply_control_update(
                symbol="ARXUSDT",
                control_path=control_path,
                control=stale_control,
                updates={"best_quote_maker_volume_cycle_budget_notional": 180.0},
                now=now,
                dry_run=False,
                restart_runner=lambda _symbol: None,
            )

            self.assertEqual(changed, ["best_quote_maker_volume_cycle_budget_notional"])
            actual = json.loads(control_path.read_text(encoding="utf-8"))
            self.assertEqual(actual["best_quote_maker_volume_cycle_budget_notional"], 180.0)
            self.assertEqual(actual["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(actual["external_monitor_generation"], 9)

    def test_effective_near_market_flow_blocks_loss_reduce_for_twelve_cycles(self) -> None:
        """Existing maker flow is never replaced by loss-reduce on a noisy window."""
        decide = bq_volume_recovery_guard.should_enter_loss_reduce
        for _cycle in range(12):
            self.assertFalse(
                decide(
                    low_volume=True,
                    effective_inventory_soft_pressure=True,
                    sla_recovery_due=True,
                    target_pace_behind=True,
                    inventory_soft_pressure=True,
                    active_order_count=4,
                    planned_order_count=4,
                    planned_reduce_only_only=False,
                    no_fill_seconds=0.0,
                    trigger_seconds=120.0,
                    recovery_hold_satisfied=True,
                    high_recovery_wear=False,
                    confirmed_loss_reduce_wear=False,
                    recovery_reapply_debounced=False,
                    post_restore_budget_cooldown_active=False,
                    recovery_expected_allow_loss=False,
                    require_soft_pressure_for_allow_loss=True,
                    effective_near_market_flow=True,
                )
            )

    def test_action_verification_escalates_after_two_unconfirmed_cycles(self) -> None:
        verify = bq_volume_recovery_guard.evaluate_action_verification
        pending = verify(
            action_age_seconds=60.0,
            plan_is_fresh=False,
            open_order_drift=3,
            prior_failures=0,
        )
        self.assertEqual(pending, ("pending", 1))
        failed = verify(
            action_age_seconds=120.0,
            plan_is_fresh=False,
            open_order_drift=3,
            prior_failures=1,
        )
        self.assertEqual(failed, ("failed", 2))
        self.assertEqual(
            verify(
                action_age_seconds=60.0,
                plan_is_fresh=True,
                open_order_drift=0,
                prior_failures=1,
            ),
            ("confirmed", 0),
        )

    def test_restores_corrupt_state_from_recovery_restart_snapshot(self) -> None:
        now = datetime(2026, 7, 12, 16, 31, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now)
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text('{"broken":', encoding="utf-8")
            backup_path = state_path.with_name(
                state_path.name + ".bak_bq_recovery_restart"
            )
            _write_json(backup_path, {"best_quote_volume_ledger": {"short_lots": []}})
            os.utime(backup_path, (now.timestamp(), now.timestamp()))

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now + timedelta(minutes=1),
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=0,
                max_snapshot_age_seconds=300,
                dry_run=False,
            )

            assert result is not None
            self.assertEqual(result["action"], "restore_corrupt_state_from_autorealign_backup")
            self.assertEqual(result["backup_path"], str(backup_path))
            self.assertEqual(
                json.loads(state_path.read_text(encoding="utf-8")),
                {"best_quote_volume_ledger": {"short_lots": []}},
            )

    def test_corrupt_state_recovery_respects_explicit_stop_reason(self) -> None:
        now = datetime(2026, 7, 11, 11, 21, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now - timedelta(minutes=10))
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text('{"broken":', encoding="utf-8")
            _write_json(
                output_dir / "reusdt_loop_state.json.bak_autorealign_123",
                {"best_quote_volume_ledger": {"long_lots": []}},
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["stop_reason"] = "manual_stop"
            _write_json(plan_path, plan)

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now,
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=0,
                max_snapshot_age_seconds=300,
                dry_run=False,
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["action"], "skip_corrupt_state_recovery_safety_gate")
            self.assertIn("explicit_stop_reason", result["state_corruption"]["blocking_reasons"])
            with self.assertRaises(json.JSONDecodeError):
                json.loads(state_path.read_text(encoding="utf-8"))

    def test_salvages_terminal_order_refs_with_zero_exchange_orders(self) -> None:
        now = datetime(2026, 7, 12, 12, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now)
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["generated_at"] = now.isoformat()
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 100.0,
                    "actual_short_notional": 200.0,
                    "ledger": {"long_lots": [{"qty": 2.0}], "short_lots": []},
                }
            }
            _write_json(plan_path, plan)
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text(
                '{\n  "version": 1,\n  "best_quote_volume_ledger": {"long_qty": 5},'
                '\n  "best_quote_volume_order_refs": {\n    "123": {"role": "bro',
                encoding="utf-8",
            )
            os.utime(state_path, (now.timestamp(), now.timestamp()))

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now + timedelta(minutes=1),
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=30,
                max_snapshot_age_seconds=300,
                dry_run=False,
                exchange_snapshot_fetcher=lambda _symbol: {
                    "open_order_count": 0,
                    "long_notional": 105.0,
                    "short_notional": 205.0,
                },
            )

            assert result is not None
            self.assertEqual(result["action"], "salvage_corrupt_state_order_refs")
            restored = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(restored["best_quote_volume_ledger"], {"long_qty": 5})
            self.assertEqual(restored["best_quote_volume_order_refs"], {})
            self.assertEqual(
                restored["best_quote_frozen_inventory"],
                {"long_lots": [{"qty": 2.0}], "short_lots": []},
            )

    def test_salvages_terminal_applied_trade_fill_key_with_live_position_gate(self) -> None:
        now = datetime(2026, 7, 12, 22, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now)
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["generated_at"] = now.isoformat()
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 125.0,
                    "actual_short_notional": 500.0,
                }
            }
            _write_json(plan_path, plan)
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text(
                '{\n  "best_quote_frozen_inventory": {"long_lots": [], "short_lots": []},'
                '\n  "best_quote_volume_ledger": {'
                '\n    "applied_trade_count_total": 2,'
                '\n    "applied_trade_fill_keys": ['
                '\n      "119803013:SELL:SHORT:1783486084005:4:0.5588",'
                '\n      "119804106:SELL:SHORT:1783486088243:17:0.5587',
                encoding="utf-8",
            )
            os.utime(state_path, (now.timestamp(), now.timestamp()))

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now + timedelta(minutes=1),
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=30,
                max_snapshot_age_seconds=300,
                dry_run=False,
                exchange_snapshot_fetcher=lambda _symbol: {
                    "open_order_count": 0,
                    "long_notional": 124.7,
                    "short_notional": 500.6,
                },
            )

            assert result is not None
            self.assertEqual(
                result["action"], "salvage_corrupt_state_applied_trade_fill_keys"
            )
            self.assertTrue(result["safe_restart_after_salvage"])
            restored = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                restored["best_quote_volume_ledger"]["applied_trade_fill_keys"][-1],
                "119804106:SELL:SHORT:1783486088243:17:0.5587",
            )
            self.assertEqual(
                restored["updated_by"],
                "bq_volume_recovery_guard_applied_trade_keys_salvage",
            )

    def test_terminal_order_refs_salvage_blocks_with_exchange_orders(self) -> None:
        now = datetime(2026, 7, 12, 12, 31, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now)
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["generated_at"] = now.isoformat()
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 0.0,
                    "actual_short_notional": 0.0,
                    "ledger": {"long_lots": [], "short_lots": []},
                }
            }
            _write_json(plan_path, plan)
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text(
                '{\n  "best_quote_volume_ledger": {},'
                '\n  "best_quote_volume_order_refs": {"123": "bro',
                encoding="utf-8",
            )
            os.utime(state_path, (now.timestamp(), now.timestamp()))

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now + timedelta(minutes=1),
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=30,
                max_snapshot_age_seconds=300,
                dry_run=False,
                exchange_snapshot_fetcher=lambda _symbol: {
                    "open_order_count": 1,
                    "long_notional": 0.0,
                    "short_notional": 0.0,
                },
            )

            assert result is not None
            self.assertEqual(result["action"], "skip_corrupt_state_recovery_safety_gate")
            self.assertIn(
                "exchange_open_orders_present",
                result["state_corruption"]["blocking_reasons"],
            )

    def test_main_checks_active_runner_normally(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            service = "grid-loop@ARXUSDT.service"
            active = subprocess.CompletedProcess(
                ["systemctl", "is-active", service],
                returncode=0,
                stdout="active\n",
                stderr="",
            )
            trades = [{"id": 1, "time": 1, "quoteQty": "10"}]
            checked_result = {
                "symbol": "ARXUSDT",
                "action": "healthy",
                "restart_failed": None,
            }

            with (
                patch.object(
                    bq_volume_recovery_guard.subprocess,
                    "run",
                    return_value=active,
                ) as run,
                patch.object(
                    bq_volume_recovery_guard,
                    "_fetch_exchange_user_trades",
                    return_value=trades,
                ) as fetch_trades,
                patch.object(
                    bq_volume_recovery_guard,
                    "check_symbol",
                    return_value=checked_result,
                ) as check_symbol_mock,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--symbols",
                        "ARXUSDT",
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run.assert_called_once_with(
                ["systemctl", "is-active", service],
                capture_output=True,
                text=True,
            )
            fetch_trades.assert_called_once()
            check_symbol_mock.assert_called_once()
            self.assertEqual(check_symbol_mock.call_args.kwargs["trade_rows"], trades)
            self.assertEqual(
                check_symbol_mock.call_args.kwargs["volume_source"],
                "exchange_user_trades",
            )
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["results"], [checked_result])

    def test_main_skips_inactive_runner_before_exchange_fetch(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            service = "grid-loop@ARXUSDT.service"
            inactive = subprocess.CompletedProcess(
                ["systemctl", "is-active", service],
                returncode=3,
                stdout="inactive\n",
                stderr="",
            )
            unexpected_result = {
                "symbol": "ARXUSDT",
                "action": "unexpected_check_symbol",
                "restart_failed": None,
            }

            with (
                patch.object(
                    bq_volume_recovery_guard.subprocess,
                    "run",
                    return_value=inactive,
                ) as run,
                patch.object(
                    bq_volume_recovery_guard,
                    "_fetch_exchange_user_trades",
                    return_value=[],
                ) as fetch_trades,
                patch.object(
                    bq_volume_recovery_guard,
                    "check_symbol",
                    return_value=unexpected_result,
                ) as check_symbol_mock,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--symbols",
                        "ARXUSDT",
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run.assert_called_once_with(
                ["systemctl", "is-active", service],
                capture_output=True,
                text=True,
            )
            fetch_trades.assert_not_called()
            check_symbol_mock.assert_not_called()
            payload = json.loads(stdout.getvalue())
            event = json.loads(
                (output_dir / "bq_volume_recovery_guard_events.jsonl").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["results"][0]["action"], "skip_runner_inactive_safety_gate")
            self.assertEqual(event["action"], "skip_runner_inactive_safety_gate")
            self.assertIn("missing_control", event["inactive_restart_gate"]["reasons"])

    def test_recent_volume_deduplicates_exchange_trade_ids(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        rows = [
            {"id": 101, "time": int((now - timedelta(seconds=10)).timestamp() * 1000), "quoteQty": "40"},
            {"id": 101, "time": int((now - timedelta(seconds=10)).timestamp() * 1000), "quoteQty": "40"},
            {"id": 102, "time": int((now - timedelta(seconds=20)).timestamp() * 1000), "quoteQty": "30"},
        ]

        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=60)

        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["gross_notional"], 70.0)

    def test_daily_target_pace_floor_raises_fixed_low_volume_threshold(self) -> None:
        now = datetime(2026, 7, 11, 4, 0, tzinfo=timezone.utc)
        rows = [
            {
                "id": 1,
                "time": int((now - timedelta(hours=3)).timestamp() * 1000),
                "quoteQty": "19840",
            },
            {
                "id": 2,
                "time": int((now - timedelta(seconds=30)).timestamp() * 1000),
                "quoteQty": "160",
            },
        ]
        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=180)

        floor = apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=rows,
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=2.0,
        )

        self.assertEqual(summary["daily_gross_notional"], 20000.0)
        self.assertEqual(summary["required_hourly_notional"], 4000.0)
        self.assertEqual(summary["target_pace_floor_notional"], 180.0)
        self.assertEqual(floor, 180.0)

    def test_daily_target_pace_floor_is_bounded_by_static_multiplier(self) -> None:
        now = datetime(2026, 7, 11, 23, 0, tzinfo=timezone.utc)
        summary: dict[str, object] = {}

        floor = apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=[],
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=2.0,
        )

        self.assertEqual(summary["target_pace_floor_notional"], 250.0)
        self.assertEqual(floor, 250.0)

    def test_daily_target_pace_floor_supports_early_completion_deadline(self) -> None:
        now = datetime(2026, 7, 11, 4, 0, tzinfo=timezone.utc)
        rows = [
            {
                "id": 1,
                "time": int((now - timedelta(hours=3)).timestamp() * 1000),
                "quoteQty": "20000",
            }
        ]
        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=180)

        floor = apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=rows,
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=5.0,
            target_completion_buffer_seconds=10800,
        )

        self.assertAlmostEqual(summary["required_hourly_notional"], 80000 / 17)
        self.assertAlmostEqual(summary["target_pace_floor_notional"], (80000 / 17) * 0.05 * 0.9)
        self.assertEqual(summary["target_completion_buffer_seconds"], 10800.0)
        self.assertEqual(summary["target_deadline"], "2026-07-11T21:00:00+00:00")
        self.assertAlmostEqual(floor, (80000 / 17) * 0.05 * 0.9)

    def test_daily_target_pace_floor_uses_day_end_after_buffer_expires(self) -> None:
        now = datetime(2026, 7, 11, 21, 5, tzinfo=timezone.utc)
        summary: dict[str, object] = {}

        apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=[],
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=5.0,
            target_completion_buffer_seconds=10800,
        )

        self.assertEqual(summary["target_deadline"], "2026-07-11T21:00:00+00:00")
        self.assertEqual(
            summary["effective_target_deadline"], "2026-07-12T00:00:00+00:00"
        )
        self.assertTrue(summary["completion_buffer_expired"])
        self.assertEqual(summary["remaining_target_seconds"], 10500.0)
        self.assertAlmostEqual(summary["required_hourly_notional"], 100000 * 3600 / 10500)

    def test_parse_symbol_notionals_accepts_comma_separated_targets(self) -> None:
        targets = bq_volume_recovery_guard._parse_symbol_notionals(
            ["ARXUSDT=100000,OUSDT=60000", "INVALID", "BAD=0"]
        )

        self.assertEqual(targets, {"ARXUSDT": 100000.0, "OUSDT": 60000.0})

    def test_inactive_runner_restarts_after_safe_confirmation(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "run_start_time": (now - timedelta(hours=1)).isoformat(),
                    "run_end_time": (now + timedelta(hours=1)).isoformat(),
                },
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "first_inactive_at": (now - timedelta(seconds=130)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = recover_inactive_runner(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                trigger_seconds=120,
                restart_cooldown_seconds=600,
                max_snapshot_age_seconds=300,
                runner_wrapper="/usr/local/bin/grid-saved-runner",
                dry_run=False,
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "restart_runner_inactive")
            self.assertTrue(result["inactive_restart_gate"]["ok"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_loss_reduce_pause_threshold_never_becomes_zero(self) -> None:
        updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_min_cycle_budget_notional": 72.0,
                "best_quote_maker_volume_cycle_budget_notional": 120.0,
                "per_order_notional": 50.0,
                "pause_buy_position_notional": 380.0,
                "pause_short_position_notional": 380.0,
            },
            assessment={
                "current_long_notional": 0.0,
                "current_short_notional": 32.0,
                "max_long_notional": 1350.0,
                "max_short_notional": 1350.0,
            },
            pause_baseline_long_notional=380.0,
            pause_baseline_short_notional=380.0,
        )

        self.assertEqual(updates["pause_short_position_notional"], 1.0)

    def test_recovery_parameters_use_one_authoritative_floor_for_all_config_values(self) -> None:
        for configured_min in (32.0, 72.0, 100.0, 120.0, 200.0):
            with self.subTest(configured_min=configured_min):
                control = {
                    "best_quote_maker_volume_min_cycle_budget_notional": configured_min,
                    "best_quote_maker_volume_cycle_budget_notional": 44.0,
                    "per_order_notional": 8.0,
                }
                parameters = bq_volume_recovery_guard._resolve_recovery_parameters(
                    control=control,
                    static_cycle_budget_floor_notional=32.0,
                    effective_cycle_budget_floor_notional=32.0,
                    cycle_budget_increment_notional=12.0,
                )
                updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
                    control=control,
                    assessment={},
                    parameters=parameters,
                )

                self.assertEqual(parameters.effective_cycle_budget_floor_notional, configured_min)
                self.assertEqual(parameters.loss_reduce_cycle_budget_cap_notional, configured_min)
                self.assertEqual(
                    updates["best_quote_maker_volume_cycle_budget_notional"],
                    configured_min,
                )

    def test_recovery_parameters_only_use_per_order_multiple_as_last_resort(self) -> None:
        fallback = bq_volume_recovery_guard._resolve_recovery_parameters(
            control={"per_order_notional": 8.0},
            static_cycle_budget_floor_notional=0.0,
        )
        strategy_floor = bq_volume_recovery_guard._resolve_recovery_parameters(
            control={"per_order_notional": 8.0},
            static_cycle_budget_floor_notional=100.0,
        )

        self.assertEqual(fallback.effective_cycle_budget_floor_notional, 32.0)
        self.assertEqual(strategy_floor.effective_cycle_budget_floor_notional, 100.0)

    def test_inactive_runner_repairs_recovery_owned_zero_pause_with_live_gate(self) -> None:
        now = datetime(2026, 7, 12, 18, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now - timedelta(minutes=10),
                control={
                    "run_start_time": (now - timedelta(hours=1)).isoformat(),
                    "run_end_time": (now + timedelta(hours=1)).isoformat(),
                    "pause_buy_position_notional": 380.0,
                    "pause_short_position_notional": 0.0,
                },
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 100.0,
                    "actual_short_notional": 200.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "first_inactive_at": (now - timedelta(minutes=5)).isoformat(),
                        "recovery_owned": True,
                        "guard_original_controls": {
                            "pause_buy_position_notional": 380.0,
                            "pause_short_position_notional": 380.0,
                        },
                        "last_assessment": {
                            "ledger_position_drift_threshold_notional": 270.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = recover_inactive_runner(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                trigger_seconds=120,
                restart_cooldown_seconds=600,
                max_snapshot_age_seconds=300,
                runner_wrapper="/usr/local/bin/grid-saved-runner",
                dry_run=False,
                restart_runner=restarts.append,
                exchange_snapshot_fetcher=lambda _symbol: {
                    "open_order_count": 0,
                    "long_notional": 105.0,
                    "short_notional": 205.0,
                },
            )

            self.assertEqual(result["action"], "repair_invalid_recovery_pause_and_restart")
            self.assertTrue(result["invalid_control_live_gate"]["ok"])
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(control["pause_short_position_notional"], 380.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_inactive_runner_does_not_restart_outside_run_window(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "run_start_time": (now - timedelta(hours=2)).isoformat(),
                    "run_end_time": (now - timedelta(seconds=1)).isoformat(),
                },
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "first_inactive_at": (now - timedelta(seconds=130)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = recover_inactive_runner(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                trigger_seconds=120,
                restart_cooldown_seconds=600,
                max_snapshot_age_seconds=300,
                runner_wrapper="/usr/local/bin/grid-saved-runner",
                dry_run=False,
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "skip_runner_inactive_safety_gate")
            self.assertIn("after_run_window", result["inactive_restart_gate"]["reasons"])
            self.assertEqual(restarts, [], result)

    def test_fetch_recent_user_trades_pages_and_deduplicates_ids(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        start_ms = int((now - timedelta(minutes=3)).timestamp() * 1000)
        first_time = start_ms + 1_000
        second_time = start_ms + 2_000
        calls: list[int] = []

        def fetch_page(*, start_time_ms: int, limit: int) -> list[dict[str, object]]:
            calls.append(start_time_ms)
            if len(calls) == 1:
                return ([{"id": index, "time": first_time, "quoteQty": "1"} for index in range(1_000)])
            return [
                {"id": 999, "time": first_time, "quoteQty": "1"},
                {"id": 1_000, "time": second_time, "quoteQty": "2"},
            ]

        rows = fetch_recent_user_trades(
            fetch_page=fetch_page,
            now=now,
            window_seconds=180,
            max_pages=3,
        )

        self.assertEqual(len(rows), 1_001)
        self.assertEqual(calls, [start_ms, first_time])

    def _write_common_files(
        self,
        output_dir: Path,
        *,
        now: datetime,
        control: dict[str, object] | None = None,
        long_notional: float = 990.0,
        short_notional: float = 980.0,
        open_order_count: int = 0,
        active_order_count: int = 0,
        orders_near_market: bool = False,
        recent_trade_notional: float = 0.0,
    ) -> None:
        control_payload = {
            "symbol": "REUSDT",
            "best_quote_maker_volume_allow_loss_reduce_only": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "best_quote_maker_volume_max_long_notional": 1000.0,
            "best_quote_maker_volume_max_short_notional": 1000.0,
        }
        if control:
            control_payload.update(control)
        _write_json(output_dir / "reusdt_loop_runner_control.json", control_payload)

        buy_price = 0.5968 if orders_near_market else 0.5800
        sell_price = 0.5972 if orders_near_market else 0.6200
        _write_json(
            output_dir / "reusdt_loop_latest_plan.json",
            {
                "generated_at": now.isoformat(),
                "symbol": "REUSDT",
                "bid_price": 0.5968,
                "ask_price": 0.5972,
                "symbol_info": {"tick_size": 0.0001},
                "current_long_notional": long_notional,
                "current_short_notional": short_notional,
                "effective_max_position_notional": 1000.0,
                "effective_max_short_position_notional": 1000.0,
                "open_order_count": open_order_count,
                "total_open_order_count": open_order_count,
                "buy_orders": ([{"side": "BUY", "price": buy_price, "qty": 16.0}] if open_order_count else []),
                "sell_orders": ([{"side": "SELL", "price": sell_price, "qty": 16.0}] if open_order_count else []),
            },
        )
        _write_json(
            output_dir / "reusdt_loop_latest_submit.json",
            {
                "generated_at": now.isoformat(),
                "observed_strategy_open_order_state": {"active_order_count": active_order_count},
                "plan_summary": {"open_order_count": open_order_count},
                "validation": {"actions": {"place_orders": []}},
                "live_book": {"bid_price": 0.5968, "ask_price": 0.5972},
            },
        )

        rows = []
        if recent_trade_notional > 0:
            rows.append(
                {
                    "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                    "quoteQty": recent_trade_notional,
                }
            )
        else:
            rows.append({"time": int((now - timedelta(minutes=10)).timestamp() * 1000), "quoteQty": 500})
        _append_jsonl(output_dir / "reusdt_loop_trade_audit.jsonl", rows)

    def test_enables_temporary_allow_loss_after_persistent_near_cap_stall(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_net_loss_reduce_enabled": True},
            )
            state: dict[str, object] = {}
            restarts: list[str] = []

            first = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )
            second = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now + timedelta(seconds=130),
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            backups = list(output_dir.glob("reusdt_loop_runner_control.json.bak_bq_volume_recovery_*"))

            self.assertEqual(first["action"], "wait_low_volume_confirmation")
            self.assertEqual(second["action"], "enable_allow_loss_reduce_only")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])
            self.assertEqual(len(backups), 1)

    def test_enables_temporary_allow_loss_when_recent_volume_masks_zero_active_orders(self) -> None:
        now = datetime(2026, 6, 26, 7, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                    "pause_buy_position_notional": 900.0,
                    "pause_short_position_notional": 900.0,
                },
                long_notional=500.0,
                short_notional=550.0,
                open_order_count=0,
                active_order_count=0,
                recent_trade_notional=100.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertIn("no_active_orders", result["assessment"]["reasons"])
            self.assertTrue(result["assessment"]["low_volume"])
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["pause_buy_position_notional"], 900.0)
            self.assertEqual(control["pause_short_position_notional"], 526.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_confirmed_fresh_wear_does_not_reenable_allow_loss_reduce(self) -> None:
        now = datetime(2026, 7, 12, 13, 25, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 24.0,
                    "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "pause_buy_position_notional": 380.0,
                    "pause_short_position_notional": 380.0,
                    "max_position_notional": 450.0,
                    "max_short_position_notional": 450.0,
                    "per_order_notional": 12.0,
                },
                long_notional=440.0,
                short_notional=210.0,
                open_order_count=0,
                active_order_count=0,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=10)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []
            trade_rows = [
                {
                    "time": int((now - timedelta(minutes=4)).timestamp() * 1000),
                    "quoteQty": 120.0,
                    "realizedPnl": -0.6,
                }
            ]

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=500.0,
                trigger_seconds=120,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertNotIn("enable_allow_loss", result["action"])
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_soft_pressure_does_not_reenable_loss_reduce_during_post_restore_cooldown(self) -> None:
        now = datetime(2026, 7, 12, 22, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 160.0,
                    "best_quote_maker_volume_quote_offset_ticks": 2,
                    "pause_buy_position_notional": 380.0,
                    "pause_short_position_notional": 380.0,
                },
                long_notional=440.0,
                short_notional=210.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=10)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "last_recovery_action": "disable_loss_reduce_for_high_wear",
                        "post_restore_budget_cooldown_until": (
                            now + timedelta(minutes=5)
                        ).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=500.0,
                trigger_seconds=120,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "120",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertFalse(
                control["best_quote_maker_volume_allow_loss_reduce_only"], result
            )

    def test_effective_flow_disables_allow_loss_before_inventory_buffer(self) -> None:
        now = datetime(2026, 6, 26, 7, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=982.0,
                short_notional=978.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=50.0,
            )
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "disable_allow_loss_after_recovery")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_cap_pressure_reduce_only_flow_gets_one_bounded_budget_step(self) -> None:
        now = datetime(2026, 7, 12, 12, 45, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 18.0,
                    "buy_levels": 4,
                    "sell_levels": 4,
                    "best_quote_maker_volume_max_long_notional": 780.0,
                    "best_quote_maker_volume_max_short_notional": 780.0,
                },
                long_notional=688.0,
                short_notional=396.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 89.0,
                    "role": "inventory_unlock_reduce_long",
                    "position_side": "LONG",
                    "force_reduce_only": True,
                }
            ]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=36.0,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "20",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertEqual(
                result["action"], "raise_cap_pressure_reduce_budget_for_pace", result
            )
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 144.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_cap_pressure_enables_loss_reduce_when_pace_is_severely_behind(self) -> None:
        now = datetime(2026, 7, 12, 20, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "best_quote_maker_volume_cycle_budget_notional": 240.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 50.0,
                    "best_quote_maker_volume_max_long_notional": 1350.0,
                    "best_quote_maker_volume_max_short_notional": 1350.0,
                },
                long_notional=1275.0,
                short_notional=5.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 89.0,
                    "role": "inventory_unlock_reduce_long",
                    "position_side": "LONG",
                    "force_reduce_only": True,
                }
            ]
            plan["pause_reasons"] = ["inventory_soft"]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "200",
                        "realizedPnl": "1",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertEqual(
                result["action"], "enable_cap_pressure_loss_reduce_for_pace", result
            )
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 240.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_critical_arx_pace_can_open_bounded_loss_reduce_despite_wear(self) -> None:
        now = datetime(2026, 7, 12, 23, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "best_quote_maker_volume_cycle_budget_notional": 240.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 50.0,
                    "best_quote_maker_volume_max_long_notional": 1350.0,
                    "best_quote_maker_volume_max_short_notional": 1350.0,
                },
                long_notional=1275.0,
                short_notional=425.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            for stem in ("runner_control", "latest_plan", "latest_submit", "trade_audit"):
                source = output_dir / f"reusdt_loop_{stem}.json"
                if stem == "trade_audit":
                    source = output_dir / "reusdt_loop_trade_audit.jsonl"
                    target = output_dir / "arxusdt_loop_trade_audit.jsonl"
                else:
                    target = output_dir / f"arxusdt_loop_{stem}.json"
                target.write_text(
                    source.read_text(encoding="utf-8").replace("REUSDT", "ARXUSDT"),
                    encoding="utf-8",
                )
            plan_path = output_dir / "arxusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [{"side": "BUY", "price": 0.5968, "qty": 16.0}]
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 89.0,
                    "role": "inventory_unlock_reduce_long",
                    "position_side": "LONG",
                    "force_reduce_only": True,
                }
            ]
            plan["pause_reasons"] = ["inventory_soft"]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "ARXUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=5)).isoformat(),
                        "low_pace_since": (now - timedelta(minutes=5)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="ARXUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "600",
                        "realizedPnl": "-0.6",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertTrue(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertTrue(result["assessment"]["critical_arx_inventory_pace_override"])
            self.assertEqual(
                result["action"],
                "enable_critical_arx_inventory_loss_reduce_for_pace",
                result,
            )
            control = json.loads(
                (output_dir / "arxusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 240.0)
            self.assertEqual(restarts, ["ARXUSDT"])

    def test_ousdt_budget_below_minimum_deadlock_opens_bounded_loss_reduce(self) -> None:
        now = datetime(2026, 7, 12, 23, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "best_quote_maker_volume_cycle_budget_notional": 60.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "pause_buy_position_notional": 380.0,
                    "pause_short_position_notional": 380.0,
                    "per_order_notional": 60.0,
                    "best_quote_maker_volume_max_long_notional": 380.0,
                    "best_quote_maker_volume_max_short_notional": 380.0,
                },
                long_notional=324.0,
                short_notional=189.0,
                open_order_count=0,
                active_order_count=0,
            )
            for suffix in ("runner_control.json", "latest_plan.json", "latest_submit.json"):
                source = output_dir / f"reusdt_loop_{suffix}"
                target = output_dir / f"ousdt_loop_{suffix}"
                target.write_text(
                    source.read_text(encoding="utf-8").replace("REUSDT", "OUSDT"),
                    encoding="utf-8",
                )
            plan_path = output_dir / "ousdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["pause_reasons"] = ["budget_below_minimum"]
            _write_json(plan_path, plan)
            state: dict[str, object] = {"symbols": {"OUSDT": {"status": "normal"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="OUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                daily_target_notional=100_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[],
                restart_runner=restarts.append,
            )

            self.assertEqual(
                result["action"], "enable_ousdt_budget_deadlock_loss_reduce", result
            )
            control = json.loads(
                (output_dir / "ousdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["OUSDT"])

    def test_cap_pressure_balancing_flow_gets_one_bounded_budget_step(self) -> None:
        now = datetime(2026, 7, 12, 13, 55, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 120.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 30.0,
                    "best_quote_maker_volume_max_long_notional": 860.0,
                    "best_quote_maker_volume_max_short_notional": 860.0,
                },
                long_notional=930.0,
                short_notional=120.0,
                open_order_count=3,
                active_order_count=3,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {
                    "side": "BUY",
                    "price": 0.5971,
                    "qty": 201.0,
                    "role": "best_quote_reduce_short",
                    "position_side": "SHORT",
                    "force_reduce_only": True,
                }
            ]
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 201.0,
                    "role": "best_quote_entry_short",
                    "position_side": "SHORT",
                },
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 27.0,
                    "role": "inventory_unlock_reduce_long",
                    "position_side": "LONG",
                    "force_reduce_only": True,
                },
            ]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=30)).isoformat(),
                        "soft_recovery_extension_count": 1,
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                cycle_budget_floor_notional=132.0,
                volume_recovery_cycle_budget_increment=12.0,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "20",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertEqual(
                result["action"],
                "raise_exhausted_soft_recovery_budget_for_pace",
                result,
            )
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 132.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_bridges_soft_hysteresis_gap_before_restoring_entry(self) -> None:
        now = datetime(2026, 7, 12, 14, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.92,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 30.0,
                    "best_quote_maker_volume_max_long_notional": 860.0,
                    "best_quote_maker_volume_max_short_notional": 860.0,
                },
                long_notional=608.0,
                short_notional=0.2,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 60.0,
                    "role": "best_quote_entry_short",
                    "position_side": "SHORT",
                }
            ]
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 710.0,
                    "actual_short_notional": 1.0,
                    "frozen_long_notional": 102.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=4)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "pause_buy_position_notional": 620.0,
                            "pause_short_position_notional": 620.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                recover_cap_ratio=0.96,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[],
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "bridge_soft_hysteresis_gap_for_recovery")
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertAlmostEqual(control["pause_buy_position_notional"], 595.2)
            self.assertEqual(control["pause_short_position_notional"], 620.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_forces_net_loss_reduce_off_while_holding_recovery(self) -> None:
        now = datetime(2026, 6, 26, 7, 11, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_net_loss_reduce_enabled": True,
                },
                long_notional=982.0,
                short_notional=978.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=50.0,
            )
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "disable_net_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_restores_allow_loss_after_volume_orders_and_cap_buffer_recover(self) -> None:
        now = datetime(2026, 6, 26, 7, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=940.0,
                short_notional=930.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "disable_allow_loss_after_recovery")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_recovery_owned_loss_reduce_closes_once_both_sides_are_below_original_soft(self) -> None:
        now = datetime(2026, 6, 26, 7, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 600.0,
                    "pause_short_position_notional": 600.0,
                },
                long_notional=850.0,
                short_notional=820.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 700.0,
                    "actual_short_notional": 680.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_cycle_budget_notional": 72.0,
                            "pause_buy_position_notional": 800.0,
                            "pause_short_position_notional": 800.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                cycle_budget_floor_notional=108,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_after_inventory_below_soft")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(control["pause_buy_position_notional"], 800.0)
            self.assertEqual(control["pause_short_position_notional"], 800.0)
            self.assertEqual(state["symbols"]["REUSDT"]["status"], "normal")
            self.assertIn("cooldown_until", state["symbols"]["REUSDT"])
            self.assertEqual(
                state["symbols"]["REUSDT"]["post_restore_budget_cooldown_until"],
                state["symbols"]["REUSDT"]["cooldown_until"],
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_frozen_inventory_prevents_actual_position_recovery_fallback(self) -> None:
        recovered = bq_volume_recovery_guard._recovery_inventory_buffer_ok(
            {
                "current_long_notional": 850.0,
                "current_short_notional": 820.0,
                "actual_long_notional": 750.0,
                "actual_short_notional": 700.0,
                "frozen_total_notional": 100.0,
                "max_long_notional": 1000.0,
                "max_short_notional": 1000.0,
                "long_soft_limit_notional": 600.0,
                "short_soft_limit_notional": 600.0,
                "inventory_soft_ratio": 0.8,
            },
            recover_cap_ratio=0.96,
            original_controls={
                "pause_buy_position_notional": 800.0,
                "pause_short_position_notional": 800.0,
            },
        )

        self.assertFalse(recovered)

    def test_recovery_buffer_applies_below_soft_boundary(self) -> None:
        assessment = {
            "current_long_notional": 790.0,
            "current_short_notional": 700.0,
            "actual_long_notional": 790.0,
            "actual_short_notional": 700.0,
            "frozen_total_notional": 0.0,
            "max_long_notional": 1000.0,
            "max_short_notional": 1000.0,
            "long_soft_limit_notional": 800.0,
            "short_soft_limit_notional": 800.0,
            "inventory_soft_ratio": 0.8,
        }

        self.assertFalse(
            bq_volume_recovery_guard._recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=0.96,
                original_controls={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
            )
        )
        assessment["actual_long_notional"] = 760.0
        self.assertTrue(
            bq_volume_recovery_guard._recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=0.96,
                original_controls={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
            )
        )
        self.assertFalse(
            bq_volume_recovery_guard._recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=0.96,
                original_controls={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                entry_reserve_notional=36.0,
            )
        )
        assessment["actual_long_notional"] = 730.0
        self.assertTrue(
            bq_volume_recovery_guard._recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=0.96,
                original_controls={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                entry_reserve_notional=36.0,
            )
        )

    def test_loss_reduce_temporarily_adds_one_quote_offset_tick_without_ratchet(self) -> None:
        updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_quote_offset_ticks": 0,
            },
            assessment={},
            quote_offset_extra_ticks=1,
        )
        self.assertEqual(updates["best_quote_maker_volume_quote_offset_ticks"], 1)

        already_active = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": True,
                "best_quote_maker_volume_quote_offset_ticks": 1,
            },
            assessment={},
            quote_offset_extra_ticks=1,
        )
        self.assertNotIn("best_quote_maker_volume_quote_offset_ticks", already_active)

    def test_active_loss_reduce_repairs_missing_configured_offset_tick(self) -> None:
        now = datetime(2026, 7, 12, 4, 16, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=810.0,
                short_notional=700.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                loss_reduce_quote_offset_extra_ticks=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "add_loss_reduce_offset_for_wear")
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_recent_sla_action_prevents_immediate_loss_reduce_offset_reversal(self) -> None:
        now = datetime(2026, 7, 12, 7, 53, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=648.0,
                short_notional=526.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={
                    "symbols": {
                        "REUSDT": {
                            "last_sla_action_at": (now - timedelta(seconds=60)).isoformat(),
                        }
                    }
                },
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                loss_reduce_quote_offset_extra_ticks=1,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertNotEqual(result["action"], "add_loss_reduce_offset_for_wear")
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 0)
            self.assertEqual(restarts, [])

    def test_no_fill_sla_pulls_bounded_loss_reduce_to_best_quote(self) -> None:
        now = datetime(2026, 7, 12, 7, 52, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "sticky_entry_price_tolerance_steps": 1.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 18.0,
                    "maker_order_notional": 18.0,
                    "buy_levels": 4,
                    "sell_levels": 4,
                },
                long_notional=648.0,
                short_notional=526.0,
                open_order_count=3,
                active_order_count=3,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {
                    "side": "BUY",
                    "price": 0.5968,
                    "qty": 16.0,
                    "role": "best_quote_entry_long",
                    "position_side": "LONG",
                }
            ]
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 16.0,
                    "role": "best_quote_reduce_long",
                    "position_side": "LONG",
                    "force_reduce_only": True,
                },
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 16.0,
                    "role": "best_quote_entry_short",
                    "position_side": "SHORT",
                },
            ]
            _write_json(plan_path, plan)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=108.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": "100",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "accelerate_loss_reduce_for_no_fill_sla")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 0)
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_wrong_way_loss_recovery_enables_dominant_leg_reduce_share(self) -> None:
        now = datetime(2026, 7, 12, 4, 21, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_reduce_share": 0.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.95,
                    "pause_buy_position_notional": 380.0,
                    "pause_short_position_notional": 350.0,
                },
                long_notional=145.0,
                short_notional=368.0,
                open_order_count=3,
                active_order_count=3,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {"side": "BUY", "price": 0.5968, "qty": 16.0, "role": "best_quote_entry_long", "position_side": "LONG"}
            ]
            plan["sell_orders"] = [
                {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_reduce_long", "position_side": "LONG", "force_reduce_only": True}
            ]
            _write_json(plan_path, plan)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={
                    "symbols": {
                        "REUSDT": {
                            "status": "recovery_active",
                            "guard_recovery_controls": {
                                "best_quote_maker_volume_cycle_budget_notional": 108.0,
                            },
                        }
                    }
                },
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                loss_reduce_quote_offset_extra_ticks=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(
                result["action"],
                "enable_dominant_leg_reduce_share_for_wrong_way_recovery",
            )
            self.assertEqual(result["assessment"]["planned_reduce_long_order_count"], 1)
            self.assertEqual(result["assessment"]["planned_reduce_short_order_count"], 0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_reduce_share"], 0.25)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_loss_reduce_only_lowers_dominant_leg_pause(self) -> None:
        updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                "pause_buy_position_notional": 740.0,
                "pause_short_position_notional": 144.0,
            },
            assessment={
                "current_long_notional": 725.0,
                "current_short_notional": 168.0,
            },
            pause_baseline_long_notional=620.0,
            pause_baseline_short_notional=620.0,
        )

        self.assertEqual(updates["pause_buy_position_notional"], 701.0)
        self.assertEqual(updates["pause_short_position_notional"], 620.0)

    def test_loss_reduce_uses_pair_reducer_and_aligns_soft_to_pause(self) -> None:
        updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_cycle_budget_notional": 400.0,
                "best_quote_maker_volume_min_cycle_budget_notional": 120.0,
                "best_quote_maker_volume_inventory_soft_ratio": 0.95,
                "best_quote_maker_volume_active_pair_reduce_enabled": False,
                "best_quote_maker_volume_active_pair_reduce_order_notional": 24.0,
                "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 48.0,
                "per_order_notional": 50.0,
                "pause_buy_position_notional": 1000.0,
                "pause_short_position_notional": 1000.0,
            },
            assessment={
                "current_long_notional": 1150.0,
                "current_short_notional": 1120.0,
                "max_long_notional": 1350.0,
                "max_short_notional": 1350.0,
            },
            pause_baseline_long_notional=1000.0,
            pause_baseline_short_notional=1000.0,
        )

        self.assertTrue(updates["best_quote_maker_volume_active_pair_reduce_enabled"])
        self.assertEqual(
            updates["best_quote_maker_volume_active_pair_reduce_order_notional"],
            50.0,
        )
        self.assertEqual(
            updates["best_quote_maker_volume_active_pair_reduce_max_notional_per_side"],
            60.0,
        )
        self.assertAlmostEqual(
            updates["best_quote_maker_volume_inventory_soft_ratio"],
            1000.0 / 1350.0,
        )

    def test_active_loss_reduce_converges_only_flow_controls(self) -> None:
        desired = {
            "best_quote_maker_volume_allow_loss_reduce_only": True,
            "best_quote_maker_volume_cycle_budget_notional": 200.0,
            "best_quote_maker_volume_active_pair_reduce_enabled": True,
            "best_quote_maker_volume_active_pair_reduce_order_notional": 50.0,
            "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 100.0,
            "best_quote_maker_volume_inventory_soft_ratio": 0.74,
        }

        self.assertEqual(
            bq_volume_recovery_guard._loss_reduce_flow_control_updates(
                {
                    "best_quote_maker_volume_active_pair_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_order_notional": 24.0,
                    "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 48.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.92,
                },
                desired,
            ),
            {
                "best_quote_maker_volume_active_pair_reduce_enabled": True,
                "best_quote_maker_volume_active_pair_reduce_order_notional": 50.0,
                "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 100.0,
                "best_quote_maker_volume_inventory_soft_ratio": 0.74,
            },
        )

    def test_loss_reduce_does_not_stack_offset_on_dynamic_widening(self) -> None:
        updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_quote_offset_ticks": 1,
            },
            assessment={
                "dynamic_quote_offset_applied": True,
                "planned_entry_order_count": 2,
            },
            quote_offset_extra_ticks=1,
        )

        self.assertNotIn("best_quote_maker_volume_quote_offset_ticks", updates)

    def test_dry_run_reports_recovery_without_mutating_control(self) -> None:
        now = datetime(2026, 6, 26, 7, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now)
            before = (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=5)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                dry_run=True,
                restart_runner=restarts.append,
            )
            after = (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")

            self.assertEqual(result["action"], "dry_run_enable_allow_loss_reduce_only")
            self.assertEqual(before, after)
            self.assertEqual(restarts, [])

    def test_restores_only_guard_changed_cost_gate_after_volume_recovers(self) -> None:
        now = datetime(2026, 6, 26, 7, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_cost_gate_enabled": False,
                },
                long_notional=940.0,
                short_notional=930.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "guard_original_controls": {
                            "best_quote_maker_volume_inventory_cost_gate_enabled": True,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertTrue(control["best_quote_maker_volume_inventory_cost_gate_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_times_out_temporary_loss_reduce_and_enters_cooldown(self) -> None:
        now = datetime(2026, 6, 26, 7, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=982.0,
                short_notional=978.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                cooldown_seconds=600,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(item["status"], "cooldown")
            self.assertEqual(restarts, ["REUSDT"])

    def test_relaxes_ordinary_inventory_bias_when_effective_orders_still_have_no_volume(self) -> None:
        now = datetime(2026, 6, 26, 7, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "relax_inventory_bias_for_volume")
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 164.0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_restores_inventory_bias_after_volume_recovers(self) -> None:
        now = datetime(2026, 6, 26, 8, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 164.0},
                long_notional=940.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "guard_original_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 80.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_enables_bounded_loss_reduce_when_bias_is_already_relaxed_but_volume_stays_zero(self) -> None:
        now = datetime(2026, 6, 26, 8, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_enables_loss_reduce_when_near_market_fills_stay_below_volume_floor(self) -> None:
        now = datetime(2026, 6, 26, 8, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=70.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_holds_near_market_flow_at_recovery_floor(self) -> None:
        now = datetime(2026, 6, 26, 8, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=70.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                recover_min_volume_notional=70,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "hold_effective_near_market_flow")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_manual_loss_reduce_close_wins_when_near_market_flow_is_effective(self) -> None:
        now = datetime(2026, 6, 26, 8, 13, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 164.0,
                },
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=120.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": True,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 164.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recover_min_volume_notional=100,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "abandon_loss_reduce_for_effective_flow")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 80.0)
            self.assertEqual(item["status"], "normal")
            self.assertNotIn("guard_recovery_controls", item)
            self.assertEqual(restarts, ["REUSDT"])

    def test_does_not_relax_inventory_bias_when_current_gap_is_below_threshold(self) -> None:
        now = datetime(2026, 6, 26, 8, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 150.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts.clear()
            gated = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                require_soft_pressure_for_allow_loss=True,
                restart_runner=restarts.append,
            )
            gated_control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(gated["action"], "hold_low_volume_without_soft_pressure")
            self.assertFalse(gated_control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_raises_cycle_budget_when_orders_are_effective_and_inventory_is_not_near_cap(self) -> None:
        now = datetime(2026, 6, 26, 8, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                        "recovery_started_at": (now - timedelta(minutes=20)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 60.0)
            self.assertEqual(state["symbols"]["REUSDT"]["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_does_not_raise_cycle_budget_with_large_active_inventory_imbalance(self) -> None:
        now = datetime(2026, 7, 12, 2, 39, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 96.0,
                    "pause_buy_position_notional": 780.0,
                    "pause_short_position_notional": 780.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.92,
                },
                long_notional=673.0,
                short_notional=169.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_budget_raise_for_inventory_imbalance")
            self.assertTrue(result["assessment"]["budget_raise_inventory_buffer_blocked"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 96.0)
            self.assertEqual(restarts, [])

    def test_backs_off_cycle_budget_when_recent_realized_wear_is_high(self) -> None:
        now = datetime(2026, 7, 12, 2, 47, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 300.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "per_order_notional": 32.0,
                    "maker_order_notional": 32.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=425.0,
                short_notional=500.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": "1000",
                    "realizedPnl": "-0.7",
                }
            ]
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "normal"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=12.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "backoff_cycle_budget_for_wear")
            self.assertAlmostEqual(result["volume_summary"]["trailing_60m_realized_wear_per_10k"], 7.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 288.0)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_pace_behind_ignores_stale_15m_loss_reduce_wear(self) -> None:
        summary = {
            "trailing_5m_gross_notional": 220.0,
            "trailing_5m_realized_wear_per_10k": 0.0,
            "trailing_15m_gross_notional": 1700.0,
            "trailing_15m_realized_wear_per_10k": 5.2,
        }

        self.assertFalse(
            bq_volume_recovery_guard._normal_entry_wear_backoff_confirmed(
                summary,
                target_pace_behind=True,
            )
        )
        self.assertTrue(
            bq_volume_recovery_guard._normal_entry_wear_backoff_confirmed(
                summary,
                target_pace_behind=False,
            )
        )
        summary["trailing_5m_realized_wear_per_10k"] = 3.5
        self.assertTrue(
            bq_volume_recovery_guard._normal_entry_wear_backoff_confirmed(
                summary,
                target_pace_behind=True,
            )
        )

    def test_inventory_imbalance_allows_budget_raise_for_balancing_entry_only(self) -> None:
        now = datetime(2026, 7, 12, 6, 55, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=230.0,
                short_notional=595.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {"side": "BUY", "position_side": "LONG", "price": 0.5968, "qty": 120.0}
            ]
            plan["sell_orders"] = []
            plan["pause_reasons"] = ["inventory_bias"]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["balancing_entry_only"])
            self.assertEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_balancing_entry_budget_raise_ignores_wear_from_prior_reductions(self) -> None:
        now = datetime(2026, 7, 12, 11, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 144.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=599.0,
                short_notional=144.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {"side": "SELL", "position_side": "SHORT", "price": 0.1785, "qty": 800.0}
            ]
            plan["pause_reasons"] = ["inventory_bias"]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": "300",
                    "realizedPnl": "-1.2",
                }
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                cycle_budget_floor_notional=216.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["balancing_budget_raise_safe"])
            self.assertGreater(result["volume_summary"]["trailing_15m_realized_wear_per_10k"], 3.0)
            self.assertEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 216.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_balancing_entry_pulls_one_tick_closer_despite_prior_reduction_wear(self) -> None:
        now = datetime(2026, 7, 12, 11, 24, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 132.0,
                    "best_quote_maker_volume_quote_offset_ticks": 2,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=599.0,
                short_notional=144.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {"side": "SELL", "position_side": "SHORT", "price": 0.1785, "qty": 800.0}
            ]
            plan["pause_reasons"] = ["inventory_bias"]
            _write_json(plan_path, plan)
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": "300",
                    "realizedPnl": "-1.2",
                }
            ]
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "normal"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=144.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["balancing_entry_requote_safe"])
            self.assertEqual(result["action"], "pull_imbalanced_entry_one_tick_closer_for_pace")
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 132.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_raises_directional_recovery_budget_to_floor_for_severe_pace_deficit(self) -> None:
        now = datetime(2026, 7, 12, 3, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "per_order_notional": 18.0,
                    "maker_order_notional": 18.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=725.0,
                short_notional=290.0,
                open_order_count=3,
                active_order_count=3,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {"side": "BUY", "price": 0.5969, "qty": 30.0, "role": "best_quote_reduce_short", "force_reduce_only": True}
            ]
            plan["sell_orders"] = [
                {"side": "SELL", "price": 0.5971, "qty": 30.0, "role": "best_quote_entry_short"},
                {"side": "SELL", "price": 0.5971, "qty": 10.0, "role": "inventory_unlock_reduce_long", "force_reduce_only": True},
            ]
            _write_json(plan_path, plan)
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                    "quoteQty": "200",
                    "realizedPnl": "-0.02",
                }
            ]
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=12.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "raise_directional_recovery_budget_to_floor_for_pace")
            self.assertTrue(result["volume_summary"]["severe_target_pace_deficit"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_pulls_far_directional_entry_closer_when_reduce_order_is_near(self) -> None:
        now = datetime(2026, 7, 12, 3, 8, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 84.0,
                    "best_quote_maker_volume_quote_offset_ticks": 2,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=630.0,
                short_notional=288.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["bid_price"] = 0.5969
            plan["ask_price"] = 0.5970
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {"side": "SELL", "price": 0.5979, "qty": 30.0, "role": "best_quote_entry_short"},
                {"side": "SELL", "price": 0.5970, "qty": 10.0, "role": "inventory_unlock_reduce_long", "force_reduce_only": True},
            ]
            _write_json(plan_path, plan)
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=4)).timestamp() * 1000),
                    "quoteQty": "200",
                    "realizedPnl": "-0.02",
                }
            ]
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=108.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "pull_directional_entry_one_tick_closer")
            self.assertTrue(result["assessment"]["all_entry_orders_far"])
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(restarts, ["REUSDT"])

    def test_pulls_imbalanced_entry_to_zero_offset_when_pace_is_still_low(self) -> None:
        now = datetime(2026, 7, 12, 3, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 126.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=615.0,
                short_notional=288.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {"side": "SELL", "price": 0.5972, "qty": 40.0, "role": "best_quote_entry_short"},
                {"side": "SELL", "price": 0.5970, "qty": 10.0, "role": "inventory_unlock_reduce_long", "force_reduce_only": True},
            ]
            _write_json(plan_path, plan)
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=4)).timestamp() * 1000),
                    "quoteQty": "200",
                    "realizedPnl": "-0.02",
                }
            ]
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "normal"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=108.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "pull_imbalanced_entry_one_tick_closer_for_pace")
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 126.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_blocks_recovery_changes_when_active_ledger_drift_is_large(self) -> None:
        now = datetime(2026, 6, 26, 8, 21, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=500.0,
                short_notional=700.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 300.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_ledger_position_drift_safety_gate")
            self.assertTrue(result["assessment"]["ledger_position_drift_blocked"])
            self.assertEqual(result["assessment"]["ledger_position_drift_notional"], 400.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(restarts, [])

    def test_cycle_budget_floor_recovers_external_budget_override_in_one_step(self) -> None:
        now = datetime(2026, 6, 26, 8, 22, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 72.0},
                long_notional=400.0,
                short_notional=350.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                cycle_budget_floor_notional=108,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_pace_cycle_floor_raises_one_bounded_step(self) -> None:
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        rows = [
            {
                "id": 1,
                "time": int((now - timedelta(hours=2)).timestamp() * 1000),
                "quoteQty": "80000",
            },
            {
                "id": 2,
                "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                "quoteQty": "4000",
            },
        ]
        volume_summary = {
            "required_hourly_notional": 11000.0,
        }
        floor = apply_target_pace_cycle_budget_floor(
            volume_summary=volume_summary,
            rows=rows,
            now=now,
            control={
                "best_quote_maker_volume_cycle_budget_notional": 128.0,
                "per_order_notional": 32.0,
                "maker_order_notional": 32.0,
                "buy_levels": 4,
                "sell_levels": 4,
            },
            assessment={
                "actual_long_notional": 400.0,
                "actual_short_notional": 350.0,
                "frozen_total_notional": 0.0,
                "max_long_notional": 700.0,
                "max_short_notional": 700.0,
            },
            static_floor_notional=108.0,
            target_pace_fraction=0.9,
            cycle_budget_increment=12.0,
        )

        self.assertEqual(floor, 192.0)
        self.assertEqual(volume_summary["trailing_60m_gross_notional"], 4000.0)
        self.assertEqual(volume_summary["target_cycle_budget_ladder_capacity"], 256.0)

    def test_target_pace_budget_is_capped_during_bounded_loss_reduce(self) -> None:
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 144.0,
                    "per_order_notional": 18.0,
                    "maker_order_notional": 18.0,
                    "buy_levels": 4,
                    "sell_levels": 4,
                },
                long_notional=400.0,
                short_notional=700.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {}
            restarts: list[str] = []
            rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(hours=2)).timestamp() * 1000),
                    "quoteQty": "80000",
                },
                {
                    "id": 2,
                    "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                    "quoteQty": "3000",
                },
            ]

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=180000,
                target_completion_buffer_seconds=10800,
                cycle_budget_floor_notional=108,
                volume_recovery_cycle_budget_increment=12,
                trade_rows=rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "cap_loss_reduce_budget_for_wear")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_pace_raise_respects_validation_safety_gate(self) -> None:
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "per_order_notional": 18.0,
                    "maker_order_notional": 18.0,
                    "buy_levels": 4,
                    "sell_levels": 4,
                },
                long_notional=400.0,
                short_notional=350.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            submit_path = output_dir / "reusdt_loop_latest_submit.json"
            submit = json.loads(submit_path.read_text(encoding="utf-8"))
            submit["validation"] = {"ok": False, "errors": ["invalid order"]}
            _write_json(submit_path, submit)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=180000,
                target_completion_buffer_seconds=10800,
                cycle_budget_floor_notional=108,
                trade_rows=[],
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "skip_recovery_safety_gate")
            self.assertIn("latest_validation_failed", result["recovery_gate"]["reasons"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, [])

    def test_restores_explicit_pause_baseline_after_recovery_drift(self) -> None:
        now = datetime(2026, 7, 11, 12, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 529.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=300.0,
                short_notional=500.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                pause_baseline_long_notional=620.0,
                pause_baseline_short_notional=620.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_pause_baseline_after_recovery_drift")
            self.assertEqual(control["pause_buy_position_notional"], 620.0)
            self.assertEqual(control["pause_short_position_notional"], 620.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_disables_active_pair_reduce_when_it_suppresses_the_only_entry(self) -> None:
        now = datetime(2026, 7, 11, 12, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_active_pair_reduce_enabled": True},
                long_notional=280.0,
                short_notional=315.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "no_valid_reduce_order",
                "order_count": 0,
                "suppressed_entry_order_count": 1,
            }
            _write_json(plan_path, plan)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "disable_stalled_active_pair_reduce_suppression")
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_disables_stale_soft_pair_reduce_after_actual_inventory_recovers(self) -> None:
        now = datetime(2026, 7, 11, 12, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "pause_buy_position_notional": 740.0,
                    "pause_short_position_notional": 740.0,
                },
                long_notional=470.0,
                short_notional=450.0,
                open_order_count=4,
                active_order_count=2,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 470.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "soft_pair_reduce",
                "order_count": 4,
                "normal_entry_suppressed": True,
                "suppressed_entry_order_count": 2,
                "completed": False,
            }
            _write_json(plan_path, plan)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "disable_stalled_active_pair_reduce_suppression")
            self.assertTrue(result["assessment"]["active_pair_reduce_below_soft_deadlock"])
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_keeps_soft_pair_reduce_when_actual_inventory_is_not_below_soft(self) -> None:
        now = datetime(2026, 7, 11, 12, 13, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "pause_buy_position_notional": 740.0,
                    "pause_short_position_notional": 740.0,
                },
                long_notional=760.0,
                short_notional=510.0,
                open_order_count=1,
                active_order_count=1,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 760.0,
                    "actual_short_notional": 510.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "soft_pair_reduce",
                "order_count": 1,
                "normal_entry_suppressed": True,
                "suppressed_entry_order_count": 2,
                "completed": False,
            }
            _write_json(plan_path, plan)

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=lambda _: None,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertFalse(result["assessment"]["active_pair_reduce_below_soft_deadlock"])
            self.assertTrue(control["best_quote_maker_volume_active_pair_reduce_enabled"])

    def test_does_not_enable_loss_reduce_for_stale_ledger_soft_pressure(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=810.0,
                short_notional=805.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                pause_baseline_long_notional=800.0,
                pause_baseline_short_notional=800.0,
                restart_runner=lambda _: None,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertTrue(result["assessment"]["actual_inventory_below_soft"])
            self.assertFalse(result["assessment"]["effective_inventory_soft_pressure"])
            self.assertEqual(result["action"], "hold_loss_reduce_when_actual_inventory_below_soft")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])

    def test_soft_pressure_blocks_budget_raise_when_target_pace_is_behind(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=120.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(minutes=5)).isoformat(),
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=10_000.0,
                target_pace_fraction=1.05,
                target_pace_max_multiplier=1.0,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=12.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_ledger_position_drift_safety_gate")
            self.assertFalse(result["assessment"]["low_volume"])
            self.assertTrue(result["assessment"]["target_pace_behind"])
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_post_restore_cooldown_blocks_target_pace_budget_raise(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=500.0,
                short_notional=450.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            cooldown_until = (now + timedelta(minutes=5)).isoformat()
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "normal",
                        "cooldown_until": cooldown_until,
                        "post_restore_budget_cooldown_until": cooldown_until,
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=10_000.0,
                target_pace_fraction=1.05,
                target_pace_max_multiplier=1.0,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=12.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertNotEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, [])

    def test_recent_non_safety_action_debounces_budget_raise(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=500.0,
                short_notional=450.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                        "last_recovery_action": "relax_inventory_bias_for_volume",
                        "last_recovery_action_at": (now - timedelta(seconds=60)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=10_000.0,
                cycle_budget_floor_notional=120.0,
                volume_recovery_cycle_budget_increment=12.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertNotEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, [], result)

    def test_keeps_loss_reduce_eligible_when_frozen_inventory_exists(self) -> None:
        now = datetime(2026, 7, 11, 12, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=810.0,
                short_notional=805.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 100.0,
                }
            }
            _write_json(plan_path, plan)

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                pause_baseline_long_notional=800.0,
                pause_baseline_short_notional=800.0,
                restart_runner=lambda _: None,
            )

            self.assertFalse(result["assessment"]["actual_inventory_below_soft"])
            self.assertTrue(result["assessment"]["effective_inventory_soft_pressure"])

    def test_frozen_inventory_uses_managed_inventory_for_soft_recovery(self) -> None:
        assessment = {
            "frozen_total_notional": 100.0,
            "current_long_notional": 480.0,
            "current_short_notional": 510.0,
            "actual_long_notional": 580.0,
            "actual_short_notional": 510.0,
            "long_soft_limit_notional": 620.0,
            "short_soft_limit_notional": 620.0,
        }

        self.assertTrue(
            bq_volume_recovery_guard._actual_inventory_below_soft_limits(
                assessment,
                pause_baseline_long_notional=620.0,
                pause_baseline_short_notional=620.0,
            )
        )

    def test_two_sided_stale_no_fill_bypasses_cooldown_and_refreshes_sticky(self) -> None:
        now = datetime(2026, 7, 12, 7, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 144.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=480.0,
                short_notional=510.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["stale_orders"] = [{"side": "BUY", "price": 0.5965}]
            plan["missing_orders"] = [{"side": "BUY", "price": 0.5968}]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "normal",
                        "cooldown_until": (now + timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                daily_target_notional=120_000.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": "100",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["two_sided_stale_no_fill"])
            self.assertEqual(result["action"], "refresh_stale_two_sided_entries")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_no_fill_sla_escalates_normal_entry_through_cooldown(self) -> None:
        now = datetime(2026, 7, 12, 7, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "sticky_entry_price_tolerance_steps": 8.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 18.0,
                    "buy_levels": 2,
                    "sell_levels": 2,
                },
                long_notional=300.0,
                short_notional=320.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "normal",
                        "cooldown_until": (now + timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=72.0,
                volume_recovery_cycle_budget_increment=36.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": "100",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["sla_recovery_due"])
            self.assertGreaterEqual(result["assessment"]["no_fill_seconds"], 600.0)
            self.assertEqual(result["action"], "escalate_normal_entry_for_sla")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_sla_action_is_debounced_for_two_minutes(self) -> None:
        now = datetime(2026, 7, 12, 7, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 18.0,
                    "buy_levels": 2,
                    "sell_levels": 2,
                },
                long_notional=300.0,
                short_notional=320.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "normal",
                        "cooldown_until": (now + timedelta(minutes=4)).isoformat(),
                        "last_sla_action_at": (now - timedelta(seconds=60)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                daily_target_notional=120_000.0,
                cycle_budget_floor_notional=72.0,
                volume_recovery_cycle_budget_increment=36.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": "100",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertTrue(result["assessment"]["sla_action_debounced"])
            self.assertFalse(result["assessment"]["sla_recovery_due"])
            self.assertEqual(result["action"], "cooldown")
            self.assertEqual(restarts, [])

    def test_stale_hourly_pace_does_not_mask_recent_zero_volume(self) -> None:
        now = datetime(2026, 7, 11, 12, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                    "quoteQty": "5000",
                }
            ]

            restarts: list[str] = []
            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=6000,
                target_pace_fraction=1.05,
                trade_rows=rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "enable_soft_inventory_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_buffer_does_not_enable_loss_reduce_with_effective_raw_target_flow(self) -> None:
        now = datetime(2026, 7, 13, 4, 48, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            rows = [
                {
                    "id": index,
                    "time": int((now - age).timestamp() * 1000),
                    "quoteQty": str(notional),
                    "realizedPnl": "0",
                }
                for index, (age, notional) in enumerate(
                    (
                        (timedelta(seconds=20), 600.0),
                        (timedelta(minutes=4), 400.0),
                        (timedelta(minutes=10), 2000.0),
                        (timedelta(minutes=30), 6200.0),
                        (timedelta(hours=2), 17800.0),
                    ),
                    start=1,
                )
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=200_000.0,
                target_pace_fraction=1.05,
                trade_rows=rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertFalse(result["assessment"]["low_volume"])
            self.assertTrue(result["assessment"]["target_pace_behind"])
            required_hourly = result["volume_summary"]["required_hourly_notional"]
            self.assertGreaterEqual(
                result["volume_summary"]["trailing_60m_hourly_notional"], required_hourly
            )
            self.assertGreaterEqual(
                result["volume_summary"]["trailing_15m_gross_notional"],
                required_hourly * 0.25,
            )
            self.assertGreaterEqual(
                result["volume_summary"]["trailing_5m_gross_notional"],
                required_hourly / 12.0 * 0.5,
            )
            self.assertEqual(result["action"], "hold_effective_near_market_flow")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_persistent_unprotected_one_sided_entries_trigger_fast_requote(self) -> None:
        now = datetime(2026, 7, 12, 12, 18, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"sticky_entry_price_tolerance_steps": 8.0},
                long_notional=100.0,
                short_notional=100.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {"side": "BUY", "price": 0.5969, "qty": 30.0, "role": "best_quote_entry_long"}
            ]
            plan["sell_orders"] = []
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=3)).isoformat(),
                        "one_sided_entry_since": (now - timedelta(minutes=2)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=1)).timestamp() * 1000),
                        "quoteQty": "200",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertTrue(result["assessment"]["planned_entry_one_sided"])
            self.assertEqual(result["assessment"]["one_sided_entry_seconds"], 120.0)
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertEqual(restarts, ["REUSDT"])

    def test_closes_owned_loss_reduce_when_hourly_target_pace_is_ahead(self) -> None:
        now = datetime(2026, 7, 11, 12, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "pause_buy_position_notional": 760.0,
                    "pause_short_position_notional": 760.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "pause_buy_position_notional": 800.0,
                            "pause_short_position_notional": 800.0,
                        },
                    }
                }
            }
            rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                    "quoteQty": "4500",
                },
                {
                    "id": 2,
                    "time": int((now - timedelta(minutes=1)).timestamp() * 1000),
                    "quoteQty": "500",
                }
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=6000,
                target_pace_fraction=1.05,
                trade_rows=rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_loss_reduce_when_target_pace_ahead")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["pause_buy_position_notional"], 800.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_reapplies_non_loss_recovery_control_after_external_override(self) -> None:
        now = datetime(2026, 6, 26, 8, 25, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=2)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "reapply_recovery_controls_after_drift")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 60.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_times_out_non_loss_recovery_and_restores_original_control(self) -> None:
        now = datetime(2026, 6, 26, 8, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 60.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                cooldown_seconds=600,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(item["status"], "cooldown")
            self.assertNotIn("guard_recovery_controls", item)
            self.assertEqual(restarts, ["REUSDT"])

    def test_recovered_volume_clears_recovery_instead_of_reapplying_drifted_control(self) -> None:
        now = datetime(2026, 6, 26, 8, 35, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=2)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                max_recovery_seconds=300,
                restart_runner=restarts.append,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertEqual(item["status"], "normal")
            self.assertNotIn("guard_recovery_controls", item)
            self.assertEqual(restarts, [])

    def test_stale_inputs_do_not_bypass_non_loss_recovery_timeout(self) -> None:
        now = datetime(2026, 6, 26, 8, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now - timedelta(minutes=10),
                control={"best_quote_maker_volume_cycle_budget_notional": 60.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                cooldown_seconds=600,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(state["symbols"]["REUSDT"]["status"], "cooldown")
            self.assertEqual(restarts, ["REUSDT"])

    def test_restart_failure_still_records_non_loss_recovery_start_time(self) -> None:
        now = datetime(2026, 6, 26, 8, 45, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }

            def fail_restart(symbol: str) -> None:
                raise subprocess.CalledProcessError(1, ["restart", symbol])

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=fail_restart,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restart_failed")
            self.assertEqual(item["status"], "recovery_active")
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertEqual(
                item["guard_recovery_controls"]["best_quote_maker_volume_cycle_budget_notional"],
                60.0,
            )

    def test_cost_gate_recovery_records_expected_disabled_target(self) -> None:
        now = datetime(2026, 6, 26, 8, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_cost_gate_enabled": True},
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "inventory_cost_gate": {"blocked_buy_orders": 1, "blocked_sell_orders": 0}
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "disable_inventory_cost_gate")
            self.assertEqual(
                item["guard_recovery_controls"]["best_quote_maker_volume_inventory_cost_gate_enabled"],
                False,
            )
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_bias_restart_failure_still_records_recovery_start_and_target(self) -> None:
        now = datetime(2026, 6, 26, 8, 55, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }

            def fail_restart(symbol: str) -> None:
                raise subprocess.CalledProcessError(1, ["restart", symbol])

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=fail_restart,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restart_failed")
            self.assertEqual(item["status"], "recovery_active")
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertEqual(
                item["guard_recovery_controls"]["best_quote_maker_volume_inventory_bias_min_notional_gap"],
                164.0,
            )

    def test_loss_reduce_restart_failure_still_records_recovery_start(self) -> None:
        now = datetime(2026, 6, 26, 9, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }

            def fail_restart(symbol: str) -> None:
                raise subprocess.CalledProcessError(1, ["restart", symbol])

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=fail_restart,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restart_failed")
            self.assertEqual(item["status"], "recovery_active")
            self.assertEqual(item["recovery_started_at"], now.isoformat())

    def test_recovery_holds_before_restoring_controls(self) -> None:
        now = datetime(2026, 6, 26, 9, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(seconds=60)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_recovery_min_duration")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_recovery_control_drift_is_debounced(self) -> None:
        now = datetime(2026, 6, 26, 9, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(seconds=60)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(seconds=60)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recovery_reapply_min_seconds=180,
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "hold_recovery_control_drift_debounce")
            self.assertEqual(restarts, [])

    def test_soft_inventory_pressure_uses_loss_reduce_before_budget_increase(self) -> None:
        now = datetime(2026, 6, 26, 9, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 48.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "enable_soft_inventory_loss_reduce")
            self.assertTrue(result["assessment"]["long_near_soft"])
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_aware_soft_inventory_recovery_does_not_backoff_budget(self) -> None:
        now = datetime(2026, 7, 12, 14, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 180.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.92,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                    "per_order_notional": 30.0,
                },
                long_notional=935.0,
                short_notional=480.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                cycle_budget_floor_notional=72.0,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "20",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "enable_soft_inventory_loss_reduce")
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 180.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_high_wear_immediately_disables_active_loss_reduce(self) -> None:
        now = datetime(2026, 7, 12, 6, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "per_order_notional": 18.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=740.0,
                short_notional=560.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_active_pair_reduce_enabled": True,
                            "best_quote_maker_volume_cycle_budget_notional": 108.0,
                        },
                    }
                }
            }
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                    "quoteQty": "1000",
                    "realizedPnl": "-4",
                }
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                cycle_budget_floor_notional=72.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "disable_loss_reduce_for_high_wear")
            self.assertTrue(result["assessment"]["high_recovery_wear"])
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 96.0)
            self.assertNotIn("guard_original_controls", state["symbols"]["REUSDT"])
            self.assertNotIn("guard_recovery_controls", state["symbols"]["REUSDT"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_high_wear_suppresses_restored_active_pair_reduce(self) -> None:
        now = datetime(2026, 7, 12, 19, 35, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "best_quote_maker_volume_cycle_budget_notional": 200.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "per_order_notional": 24.0,
                },
                long_notional=1250.0,
                short_notional=798.0,
                open_order_count=4,
                active_order_count=4,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {
                    "side": "BUY",
                    "price": 0.5507,
                    "qty": 48.0,
                    "role": "best_quote_active_pair_reduce_short",
                    "force_reduce_only": True,
                }
            ]
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5509,
                    "qty": 48.0,
                    "role": "best_quote_active_pair_reduce_long",
                    "force_reduce_only": True,
                }
            ]
            _write_json(plan_path, plan)
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "normal"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=300,
                min_volume_notional=400,
                trigger_seconds=120,
                cycle_budget_floor_notional=72.0,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                        "quoteQty": "600",
                        "realizedPnl": "-3",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "suppress_active_pair_reduce_for_high_wear")
            self.assertTrue(result["assessment"]["target_pace_behind"])
            self.assertTrue(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 188.0)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(restarts, ["REUSDT"])

    def test_fresh_five_minute_wear_disables_loss_reduce_before_fifteen_minute_turns(self) -> None:
        now = datetime(2026, 7, 12, 21, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 160.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "per_order_notional": 25.0,
                },
                long_notional=650.0,
                short_notional=530.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                        "quoteQty": "100",
                        "realizedPnl": "-0.1",
                    },
                    {
                        "id": 2,
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": "1000",
                        "realizedPnl": "5",
                    },
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertFalse(result["assessment"]["high_recovery_wear"])
            self.assertTrue(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertEqual(result["action"], "disable_loss_reduce_for_high_wear")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_high_wear_waits_for_loss_reduce_minimum_observation_period(self) -> None:
        now = datetime(2026, 7, 12, 12, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 24.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "per_order_notional": 8.0,
                    "pause_buy_position_notional": 361.0,
                    "pause_short_position_notional": 361.0,
                },
                long_notional=370.0,
                short_notional=250.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(seconds=60)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_cycle_budget_notional": 24.0,
                        },
                    }
                }
            }
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                    "quoteQty": "1000",
                    "realizedPnl": "-4",
                }
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                cycle_budget_floor_notional=24.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
                recovery_min_hold_seconds=120.0,
            )

            self.assertEqual(
                result["action"],
                "hold_loss_reduce_min_duration_before_wear_judgement",
            )
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 24.0)
            self.assertEqual(restarts, [])

    def test_old_fifteen_minute_wear_cannot_close_clean_loss_reduce_sample(self) -> None:
        now = datetime(2026, 7, 12, 12, 43, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 32.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "per_order_notional": 8.0,
                    "pause_buy_position_notional": 361.0,
                    "pause_short_position_notional": 361.0,
                },
                long_notional=358.0,
                short_notional=327.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_cycle_budget_notional": 32.0,
                        },
                    }
                }
            }
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": "120",
                    "realizedPnl": "-0.3",
                },
                {
                    "id": 2,
                    "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                    "quoteQty": "120",
                    "realizedPnl": "0",
                },
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
                recovery_min_hold_seconds=120.0,
            )

            self.assertTrue(result["assessment"]["high_recovery_wear"])
            self.assertFalse(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertEqual(
                result["action"], "hold_loss_reduce_until_fresh_wear_confirmation"
            )
            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_zero_order_pair_suppression_deadlock_preempts_stale_wear_hold(self) -> None:
        now = datetime(2026, 7, 12, 12, 43, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "best_quote_maker_volume_cycle_budget_notional": 32.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "per_order_notional": 8.0,
                    "pause_buy_position_notional": 361.0,
                    "pause_short_position_notional": 361.0,
                },
                long_notional=358.0,
                short_notional=327.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "no_valid_reduce_order",
                "order_count": 0,
                "suppressed_entry_order_count": 1,
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_active_pair_reduce_enabled": False,
                            "best_quote_maker_volume_cycle_budget_notional": 32.0,
                        },
                    }
                }
            }
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": "120",
                    "realizedPnl": "-0.3",
                },
                {
                    "id": 2,
                    "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                    "quoteQty": "120",
                    "realizedPnl": "0",
                },
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=400,
                trigger_seconds=120,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
                recovery_min_hold_seconds=120.0,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                result["action"], "disable_stalled_active_pair_reduce_suppression"
            )
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_high_wear_backoff_keeps_cycle_budget_runnable(self) -> None:
        now = datetime(2026, 7, 12, 10, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 24.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                    "per_order_notional": 8.0,
                },
                long_notional=340.0,
                short_notional=345.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_cycle_budget_notional": 24.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": True,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=300,
                min_volume_notional=1,
                trigger_seconds=120,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                        "quoteQty": "100",
                        "realizedPnl": "-0.3",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "disable_loss_reduce_for_high_wear")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 32.0)
            self.assertNotIn("guard_original_controls", state["symbols"]["REUSDT"])
            self.assertNotIn("guard_recovery_controls", state["symbols"]["REUSDT"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_zero_order_budget_pause_repairs_legacy_wear_backoff_state(self) -> None:
        now = datetime(2026, 7, 12, 10, 35, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 12.0,
                    "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                    "per_order_notional": 8.0,
                },
                long_notional=340.0,
                short_notional=345.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["pause_reasons"] = ["budget_below_minimum"]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=5)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 24.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 12.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=100_000.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": "100",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "restore_runnable_budget_after_wear_backoff")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 32.0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertNotIn("guard_original_controls", state["symbols"]["REUSDT"])
            self.assertNotIn("guard_recovery_controls", state["symbols"]["REUSDT"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_high_wear_cooldown_restores_safe_normal_entry_pace(self) -> None:
        now = datetime(2026, 7, 12, 6, 16, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_quote_offset_ticks": 2,
                    "per_order_notional": 18.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=140.0,
                short_notional=150.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "normal",
                        "cooldown_until": (now + timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=2)).timestamp() * 1000),
                    "quoteQty": "1000",
                    "realizedPnl": "-4",
                }
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                target_completion_buffer_seconds=10_800.0,
                cycle_budget_floor_notional=108.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "restore_normal_entry_pace_after_high_wear")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertEqual(
                state["symbols"]["REUSDT"]["post_restore_budget_cooldown_until"],
                (now + timedelta(minutes=5)).isoformat(),
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_budget_recovery_switches_to_soft_inventory_loss_reduce(self) -> None:
        now = datetime(2026, 6, 26, 9, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 60.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=5)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=5)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "switch_to_soft_inventory_loss_reduce")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(state["symbols"]["REUSDT"]["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_zero_order_sla_uses_managed_soft_pressure_with_frozen_inventory(self) -> None:
        now = datetime(2026, 7, 12, 8, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 12.0,
                    "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                    "per_order_notional": 8.0,
                    "maker_order_notional": 8.0,
                    "pause_buy_position_notional": 361.0,
                    "pause_short_position_notional": 361.0,
                    "best_quote_maker_volume_max_long_notional": 380.0,
                    "best_quote_maker_volume_max_short_notional": 380.0,
                },
                long_notional=366.0,
                short_notional=353.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 373.0,
                    "actual_short_notional": 956.0,
                    "frozen_long_notional": 7.0,
                    "frozen_short_notional": 603.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 12.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 40.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            with patch.object(
                bq_volume_recovery_guard,
                "_actual_inventory_below_soft_limits",
                return_value=True,
            ):
                result = check_symbol(
                    symbol="REUSDT",
                    output_dir=output_dir,
                    state=state,
                    now=now,
                    window_seconds=180,
                    min_volume_notional=100,
                    trigger_seconds=0,
                    daily_target_notional=60_000.0,
                    trade_rows=[
                        {
                            "id": 1,
                            "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                            "quoteQty": "100",
                        }
                    ],
                    restart_runner=restarts.append,
                )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["sla_recovery_due"])
            self.assertFalse(result["assessment"]["effective_inventory_soft_pressure"])
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertEqual(result["action"], "switch_to_soft_inventory_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 24.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_reduce_only_no_fill_sla_uses_managed_soft_pressure_with_frozen_inventory(self) -> None:
        now = datetime(2026, 7, 12, 10, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 24.0,
                    "pause_buy_position_notional": 361.0,
                    "pause_short_position_notional": 361.0,
                    "best_quote_maker_volume_max_long_notional": 380.0,
                    "best_quote_maker_volume_max_short_notional": 380.0,
                },
                long_notional=364.0,
                short_notional=368.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"][0]["reduce_only"] = True
            plan["sell_orders"][0]["reduce_only"] = True
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 370.0,
                    "actual_short_notional": 961.0,
                    "frozen_long_notional": 6.0,
                    "frozen_short_notional": 593.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=5)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=5)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 40.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 48.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            with patch.object(
                bq_volume_recovery_guard,
                "_actual_inventory_below_soft_limits",
                return_value=True,
            ):
                result = check_symbol(
                    symbol="REUSDT",
                    output_dir=output_dir,
                    state=state,
                    now=now,
                    window_seconds=180,
                    min_volume_notional=100,
                    trigger_seconds=120,
                    daily_target_notional=100_000.0,
                    trade_rows=[
                        {
                            "id": 1,
                            "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                            "quoteQty": "100",
                        }
                    ],
                    restart_runner=restarts.append,
                )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["sla_recovery_due"])
            self.assertFalse(result["assessment"]["effective_inventory_soft_pressure"])
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertTrue(result["assessment"]["planned_reduce_only_only"])
            self.assertEqual(result["action"], "switch_to_soft_inventory_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_lag_uses_managed_soft_pressure_for_optional_loss_reduce(self) -> None:
        now = datetime(2026, 7, 12, 10, 22, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 24.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 48.0,
                    "pause_buy_position_notional": 361.0,
                    "pause_short_position_notional": 361.0,
                    "best_quote_maker_volume_max_long_notional": 380.0,
                    "best_quote_maker_volume_max_short_notional": 380.0,
                },
                long_notional=364.0,
                short_notional=332.0,
                open_order_count=3,
                active_order_count=3,
                orders_near_market=True,
                recent_trade_notional=120.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 370.0,
                    "actual_short_notional": 923.0,
                    "frozen_long_notional": 6.0,
                    "frozen_short_notional": 591.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "first_low_volume_at": (now - timedelta(minutes=5)).isoformat(),
                        "low_pace_since": (now - timedelta(minutes=5)).isoformat(),
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 40.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 48.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            with patch.object(
                bq_volume_recovery_guard,
                "_actual_inventory_below_soft_limits",
                return_value=True,
            ):
                result = check_symbol(
                    symbol="REUSDT",
                    output_dir=output_dir,
                    state=state,
                    now=now,
                    window_seconds=180,
                    min_volume_notional=400,
                    trigger_seconds=120,
                    daily_target_notional=100_000.0,
                    trade_rows=[
                        {
                            "id": 1,
                            "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                            "quoteQty": "120",
                        }
                    ],
                    restart_runner=restarts.append,
                )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertTrue(result["assessment"]["target_pace_behind"])
            self.assertFalse(result["assessment"]["effective_inventory_soft_pressure"])
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertEqual(result["action"], "switch_to_soft_inventory_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_recovered_volume_closes_loss_reduce_above_soft_limit(self) -> None:
        now = datetime(2026, 6, 26, 9, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_soft_inventory_recovery_gets_one_bounded_extension(self) -> None:
        now = datetime(2026, 6, 26, 10, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "soft_recovery_extension_count": 0,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            item = state["symbols"]["REUSDT"]
            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "extend_soft_inventory_loss_recovery")
            self.assertEqual(item["soft_recovery_extension_count"], 1)
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_exhausted_soft_recovery_holds_until_both_sides_are_below_soft(self) -> None:
        now = datetime(2026, 6, 26, 10, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(
                result["action"],
                "hold_soft_inventory_loss_recovery_until_both_below",
            )
            self.assertEqual(state["symbols"]["REUSDT"]["status"], "recovery_active")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_stalled_loss_recovery_enables_bounded_pair_reduce_when_no_reduce_orders_exist(self) -> None:
        now = datetime(2026, 6, 26, 10, 7, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_min_side_notional": 100.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]
            self.assertEqual(
                result["action"],
                "enable_bounded_pair_reduce_for_stalled_loss_recovery",
            )
            self.assertTrue(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertFalse(
                item["guard_original_controls"]["best_quote_maker_volume_active_pair_reduce_enabled"]
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_stalled_imbalanced_loss_recovery_enables_bounded_dominant_leg_reduce(self) -> None:
        now = datetime(2026, 6, 26, 10, 7, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_bias_reduce_share": 0.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=200.0,
                short_notional=805.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]
            self.assertEqual(
                result["action"],
                "enable_dominant_leg_reduce_share_for_wrong_way_recovery",
            )
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_reduce_share"], 0.25)
            self.assertEqual(
                item["guard_original_controls"]["best_quote_maker_volume_inventory_bias_reduce_share"],
                0.0,
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_stalled_loss_recovery_does_not_enable_pair_reduce_for_imbalanced_inventory(self) -> None:
        now = datetime(2026, 6, 26, 10, 8, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_min_side_notional": 100.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=300.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_soft_inventory_loss_recovery_until_both_below")
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, [])

    def test_exhausted_soft_recovery_tightens_reduce_offset_one_tick(self) -> None:
        now = datetime(2026, 7, 12, 11, 2, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "best_quote_maker_volume_quote_offset_ticks": 2,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "best_quote_maker_volume_max_long_notional": 700.0,
                    "best_quote_maker_volume_max_short_notional": 700.0,
                    "pause_buy_position_notional": 620.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=690.0,
                short_notional=10.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"][0].update(
                {"role": "best_quote_reduce_short", "force_reduce_only": True}
            )
            plan["sell_orders"][0].update(
                {"role": "best_quote_reduce_long", "force_reduce_only": True}
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "low_pace_since": (now - timedelta(minutes=10)).isoformat(),
                        "no_fill_since": (now - timedelta(minutes=10)).isoformat(),
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "last_sla_action_at": (now - timedelta(seconds=60)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_quote_offset_ticks": 1,
                            "pause_buy_position_notional": 620.0,
                            "pause_short_position_notional": 620.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": True,
                            "best_quote_maker_volume_quote_offset_ticks": 2,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(minutes=15)).timestamp() * 1000),
                        "quoteQty": "100",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                result["action"],
                "tighten_exhausted_soft_recovery_one_tick",
                msg=json.dumps(result, sort_keys=True),
            )
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 1)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(state["symbols"]["REUSDT"]["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_timed_out_loss_recovery_restores_after_both_sides_clear_original_soft_limits(self) -> None:
        now = datetime(2026, 6, 26, 10, 9, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 600.0,
                    "pause_short_position_notional": 600.0,
                },
                long_notional=750.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_active_pair_reduce_enabled": False,
                            "pause_buy_position_notional": 800.0,
                            "pause_short_position_notional": 800.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(control["pause_buy_position_notional"], 800.0)
            self.assertEqual(control["pause_short_position_notional"], 800.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_one_sided_inventory_bias_stall_tightens_entry_sticky(self) -> None:
        now = datetime(2026, 6, 26, 10, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertTrue(result["assessment"]["one_sided_inventory_bias"])
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_volatility_inventory_deadlock_enables_reduce_only_below_soft(self) -> None:
        now = datetime(2026, 7, 12, 2, 25, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                },
                long_notional=650.0,
                short_notional=70.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["volatility_entry_pause"] = {
                "active": True,
                "inventory_gate_active": True,
            }
            plan["pause_reasons"] = ["volatility_entry_pause"]
            plan["short_pause_reasons"] = ["volatility_entry_pause"]
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 650.0,
                    "actual_short_notional": 70.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                require_soft_pressure_for_allow_loss=True,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "enable_volatility_inventory_reduce_only")
            self.assertTrue(result["assessment"]["volatility_inventory_reduce_deadlock"])
            self.assertFalse(result["assessment"]["effective_inventory_soft_pressure"])
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_severe_one_sided_stall_bypasses_general_cooldown(self) -> None:
        now = datetime(2026, 6, 26, 10, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(minutes=5)).isoformat(),
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_soft_inventory_low_volume_bypasses_general_cooldown_after_confirmation(self) -> None:
        now = datetime(2026, 6, 26, 10, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "pause_buy_position_notional": 700.0,
                    "pause_short_position_notional": 700.0,
                },
                long_notional=750.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(minutes=5)).isoformat(),
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "enable_soft_inventory_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_multiple_same_direction_entries_are_still_one_sided_inventory_bias(self) -> None:
        now = datetime(2026, 6, 26, 10, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=340.0,
                short_notional=180.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=0.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_orders": [],
                    "sell_orders": [
                        {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_entry_short"},
                        {"side": "SELL", "price": 0.5973, "qty": 16.0, "role": "best_quote_entry_short"},
                    ],
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertTrue(result["assessment"]["one_sided_inventory_bias"])
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_inventory_bias_stall_with_tight_sticky_relaxes_bias_instead_of_budget(self) -> None:
        now = datetime(2026, 6, 26, 10, 16, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 100.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=340.0,
                short_notional=180.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_orders": [],
                    "sell_orders": [
                        {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_entry_short"},
                        {"side": "SELL", "price": 0.5973, "qty": 16.0, "role": "best_quote_entry_short"},
                    ],
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "relax_inventory_bias_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 184.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_active_budget_recovery_switches_to_one_sided_sticky_requote(self) -> None:
        now = datetime(2026, 6, 26, 10, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 84.0,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 72.0,
                            "best_quote_maker_volume_quote_offset_ticks": 2,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 84.0,
                            "best_quote_maker_volume_quote_offset_ticks": 0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "switch_to_one_sided_sticky_requote")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 84.0)
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 0)
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_active_sticky_recovery_escalates_to_inventory_bias_relief(self) -> None:
        now = datetime(2026, 6, 26, 10, 25, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=340.0,
                short_notional=179.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_orders": [],
                    "sell_orders": [
                        {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_entry_short"},
                        {"side": "SELL", "price": 0.5973, "qty": 16.0, "role": "best_quote_entry_short"},
                    ],
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "sticky_entry_price_tolerance_steps": 8.0,
                        },
                        "guard_recovery_controls": {
                            "sticky_entry_price_tolerance_steps": 1.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]
            self.assertEqual(result["action"], "relax_inventory_bias_after_sticky_stall")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 185.0)
            self.assertEqual(
                item["guard_original_controls"]["best_quote_maker_volume_inventory_bias_min_notional_gap"],
                150.0,
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_resolved_inventory_relief_switches_to_cycle_budget_recovery(self) -> None:
        now = datetime(2026, 6, 26, 10, 28, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 185.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=400.0,
                short_notional=380.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "sticky_entry_price_tolerance_steps": 8.0,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0,
                        },
                        "guard_recovery_controls": {
                            "sticky_entry_price_tolerance_steps": 1.0,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 185.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "switch_to_cycle_budget_after_inventory_relief")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 8.0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 150.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 84.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_near_market_reduce_only_flow_does_not_raise_cycle_budget(self) -> None:
        now = datetime(2026, 6, 26, 10, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 72.0},
                long_notional=650.0,
                short_notional=620.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 16.0,
                    "role": "best_quote_active_pair_reduce_long",
                    "force_reduce_only": True,
                }
            ]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_near_market_reduce_only_flow")
            self.assertTrue(result["assessment"]["planned_reduce_only_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(restarts, [])

            floor_result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                cycle_budget_floor_notional=108,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(floor_result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_low_pace_reduce_only_flow_tightens_sticky_exit_to_top_of_book(self) -> None:
        now = datetime(2026, 7, 13, 5, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_quote_offset_ticks": 1,
                    "sticky_exit_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=620.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=10.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {
                    "side": "BUY",
                    "price": 0.5958,
                    "qty": 16.0,
                    "role": "best_quote_reduce_short",
                    "force_reduce_only": True,
                }
            ]
            plan["sell_orders"] = []
            _write_json(plan_path, plan)
            for suffix in ("runner_control.json", "latest_plan.json", "latest_submit.json"):
                source = output_dir / f"reusdt_loop_{suffix}"
                target = output_dir / f"ousdt_loop_{suffix}"
                target.write_text(
                    source.read_text(encoding="utf-8").replace("REUSDT", "OUSDT"),
                    encoding="utf-8",
                )
            state: dict[str, object] = {
                "symbols": {
                    "OUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                        "low_pace_since": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="OUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=120_000.0,
                trade_rows=[
                    {
                        "id": 1,
                        "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                        "quoteQty": "10",
                        "realizedPnl": "0",
                    }
                ],
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "ousdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["assessment"]["near_market_order_count"], 0)
            self.assertAlmostEqual(
                result["assessment"]["nearest_reduce_only_distance_ticks"], 10.0
            )
            self.assertEqual(result["action"], "tighten_sticky_exit_for_low_pace_reduce_only_flow")
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 0)
            self.assertEqual(control["sticky_exit_price_tolerance_steps"], 1.0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["OUSDT"])

    def test_stalled_reduce_only_flow_enables_bounded_active_loss_recovery(self) -> None:
        now = datetime(2026, 7, 12, 11, 56, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_cycle_budget_notional": 24.0,
                    "best_quote_maker_volume_quote_offset_ticks": 3,
                    "pause_buy_position_notional": 380.0,
                    "pause_short_position_notional": 380.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.95,
                },
                long_notional=385.0,
                short_notional=354.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=0.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {
                    "side": "BUY",
                    "price": 0.5507,
                    "qty": 24.0,
                    "role": "best_quote_reduce_short",
                    "force_reduce_only": True,
                }
            ]
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5509,
                    "qty": 24.0,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                }
            ]
            plan["pause_reasons"] = ["inventory_soft"]
            plan.setdefault("best_quote_maker_volume", {}).setdefault("reduce_freeze", {}).update(
                {"actual_long_notional": 300.0, "actual_short_notional": 300.0}
            )
            _write_json(plan_path, plan)
            trade_rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": "120",
                    "realizedPnl": "-0.3",
                },
                {
                    "id": 2,
                    "time": int((now - timedelta(minutes=4)).timestamp() * 1000),
                    "quoteQty": "120",
                    "realizedPnl": "0",
                },
            ]
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=30)).isoformat(),
                        "no_fill_since": (now - timedelta(minutes=30)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=100_000.0,
                trade_rows=trade_rows,
                restart_runner=restarts.append,
            )

            control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["action"], "enable_stalled_reduce_only_loss_recovery", result)
            self.assertTrue(result["assessment"]["high_recovery_wear"])
            self.assertFalse(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_quote_offset_ticks"], 2)
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_arx_v3_relaxes_anti_chase_for_target_behind_no_fill_reduce_only_sla(self) -> None:
        now = datetime(2026, 7, 12, 9, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_reduce_freeze_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 200.0,
                    "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 1,
                    "per_order_notional": 25.0,
                },
                long_notional=400.0,
                short_notional=620.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            for path in list(output_dir.glob("reusdt_*")):
                path.rename(output_dir / path.name.replace("reusdt", "arxusdt", 1))

            control_path = output_dir / "arxusdt_loop_runner_control.json"
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control.update(_arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control))
            _write_json(control_path, control)

            plan_path = output_dir / "arxusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 16.0,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                }
            ]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "ARXUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []
            trade_rows = [
                {
                    "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                    "quoteQty": 500.0,
                }
            ]

            result = check_symbol(
                symbol="ARXUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100.0,
                daily_target_notional=120_000.0,
                trigger_seconds=120,
                trade_rows=trade_rows,
                volume_source="exchange_user_trades",
                restart_runner=restarts.append,
            )

            control = json.loads(control_path.read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "relax_arx_v3_anti_chase_for_no_fill_sla")
            self.assertEqual(
                control["best_quote_maker_volume_same_side_entry_price_guard_min_notional"],
                425.0,
            )
            self.assertEqual(
                state["symbols"]["ARXUSDT"]["guard_original_controls"][
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional"
                ],
                200.0,
            )
            self.assertEqual(restarts, ["ARXUSDT"])

    def test_arx_v3_relaxes_anti_chase_when_it_blocks_all_entry_orders(self) -> None:
        now = datetime(2026, 7, 12, 9, 45, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_reduce_freeze_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 200.0,
                    "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 1,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                    "per_order_notional": 25.0,
                },
                long_notional=500.0,
                short_notional=328.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            for path in list(output_dir.glob("reusdt_*")):
                path.rename(output_dir / path.name.replace("reusdt", "arxusdt", 1))

            control_path = output_dir / "arxusdt_loop_runner_control.json"
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control.update(_arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control))
            _write_json(control_path, control)

            plan_path = output_dir / "arxusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = []
            plan["best_quote_maker_volume"] = {
                "metrics": {
                    "same_side_entry_price_guard": {
                        "enabled": True,
                        "report_only": False,
                        "blocked_long_entry": True,
                        "blocked_short_entry": False,
                    }
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "ARXUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                        "low_pace_since": (now - timedelta(minutes=4)).isoformat(),
                        "no_fill_since": (now - timedelta(minutes=10)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(seconds=30)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="ARXUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100.0,
                daily_target_notional=120_000.0,
                trigger_seconds=120,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=36.0,
                trade_rows=[
                    {
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": 500.0,
                        "realizedPnl": -0.5,
                    },
                ],
                volume_source="exchange_user_trades",
                restart_runner=restarts.append,
            )

            control = json.loads(control_path.read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "relax_arx_v3_anti_chase_for_missing_entry_leg", result)
            self.assertTrue(result["assessment"]["high_recovery_wear"])
            self.assertFalse(result["assessment"]["confirmed_loss_reduce_wear"])
            self.assertEqual(
                control["best_quote_maker_volume_same_side_entry_price_guard_min_notional"],
                525.0,
            )
            self.assertEqual(
                control["best_quote_maker_volume_same_side_entry_price_guard_gap_ticks"],
                0,
            )
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(restarts, ["ARXUSDT"])

    def test_arx_v3_recovery_relaxes_anti_chase_again_after_balancing_fill_reblocks(self) -> None:
        now = datetime(2026, 7, 12, 9, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_reduce_freeze_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 370.0,
                    "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 0,
                    "best_quote_maker_volume_cycle_budget_notional": 172.0,
                    "best_quote_maker_volume_min_cycle_budget_notional": 128.0,
                    "best_quote_maker_volume_max_long_notional": 800.0,
                    "best_quote_maker_volume_max_short_notional": 800.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.75,
                    "sticky_entry_price_tolerance_steps": 1.0,
                    "per_order_notional": 40.0,
                },
                long_notional=590.0,
                short_notional=370.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=False,
            )
            for path in list(output_dir.glob("reusdt_*")):
                path.rename(output_dir / path.name.replace("reusdt", "arxusdt", 1))

            control_path = output_dir / "arxusdt_loop_runner_control.json"
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control.update(_arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control))
            _write_json(control_path, control)

            plan_path = output_dir / "arxusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_paused"] = True
            plan["pause_reasons"] = ["inventory_bias"]
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5981,
                    "qty": 16.0,
                    "role": "best_quote_reduce_long",
                    "force_reduce_only": True,
                }
            ]
            plan["best_quote_maker_volume"] = {
                "metrics": {
                    "same_side_entry_price_guard": {
                        "enabled": True,
                        "report_only": False,
                        "blocked_long_entry": False,
                        "blocked_short_entry": True,
                    }
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "ARXUSDT": {
                        "status": "recovery_active",
                        "first_low_volume_at": (now - timedelta(minutes=10)).isoformat(),
                        "low_pace_since": (now - timedelta(minutes=10)).isoformat(),
                        "no_fill_since": (now - timedelta(minutes=3)).isoformat(),
                        "recovery_started_at": (now - timedelta(minutes=4)).isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 160.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 200.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 1,
                            "sticky_entry_price_tolerance_steps": 8.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 172.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 370.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 0,
                            "sticky_entry_price_tolerance_steps": 1.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="ARXUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100.0,
                daily_target_notional=120_000.0,
                trigger_seconds=120,
                cycle_budget_floor_notional=204.0,
                volume_recovery_cycle_budget_increment=12.0,
                trade_rows=[
                    {
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": 500.0,
                    },
                ],
                volume_source="exchange_user_trades",
                restart_runner=restarts.append,
            )

            control = json.loads(control_path.read_text(encoding="utf-8"))
            self.assertEqual(
                result["action"],
                "relax_arx_v3_anti_chase_for_missing_entry_leg",
                result,
            )
            self.assertTrue(result["assessment"]["budget_raise_inventory_buffer_blocked"])
            self.assertTrue(result["assessment"]["ineffective_orders"])
            self.assertEqual(
                control["best_quote_maker_volume_same_side_entry_price_guard_min_notional"],
                589.0,
            )
            self.assertEqual(
                state["symbols"]["ARXUSDT"]["guard_original_controls"][
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional"
                ],
                200.0,
            )
            self.assertEqual(restarts, ["ARXUSDT"])

    def test_arx_v3_budget_hold_is_not_skipped_before_anti_chase_sla_is_due(self) -> None:
        now = datetime(2026, 7, 12, 9, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_reduce_freeze_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
                    "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 370.0,
                    "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 0,
                    "best_quote_maker_volume_cycle_budget_notional": 172.0,
                    "best_quote_maker_volume_min_cycle_budget_notional": 128.0,
                    "best_quote_maker_volume_max_long_notional": 800.0,
                    "best_quote_maker_volume_max_short_notional": 800.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.75,
                    "best_quote_maker_volume_quote_offset_ticks": 0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                    "per_order_notional": 40.0,
                },
                long_notional=590.0,
                short_notional=370.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            for path in list(output_dir.glob("reusdt_*")):
                path.rename(output_dir / path.name.replace("reusdt", "arxusdt", 1))

            control_path = output_dir / "arxusdt_loop_runner_control.json"
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control.update(_arx_independent_freeze_policy_updates(symbol="ARXUSDT", control=control))
            _write_json(control_path, control)

            plan_path = output_dir / "arxusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = [
                {
                    "side": "BUY",
                    "price": 0.5968,
                    "qty": 16.0,
                    "role": "best_quote_entry_long",
                    "position_side": "LONG",
                }
            ]
            plan["sell_orders"] = []
            plan["best_quote_maker_volume"] = {
                "metrics": {
                    "same_side_entry_price_guard": {
                        "enabled": True,
                        "report_only": False,
                        "blocked_long_entry": False,
                        "blocked_short_entry": True,
                    }
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "ARXUSDT": {
                        "status": "recovery_active",
                        "first_low_volume_at": (now - timedelta(minutes=10)).isoformat(),
                        "low_pace_since": (now - timedelta(minutes=10)).isoformat(),
                        "no_fill_since": (now - timedelta(seconds=60)).isoformat(),
                        "recovery_started_at": (now - timedelta(minutes=4)).isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 160.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 200.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 1,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 172.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_min_notional": 370.0,
                            "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks": 0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="ARXUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100.0,
                daily_target_notional=120_000.0,
                trigger_seconds=120,
                cycle_budget_floor_notional=204.0,
                volume_recovery_cycle_budget_increment=12.0,
                trade_rows=[
                    {
                        "time": int((now - timedelta(minutes=10)).timestamp() * 1000),
                        "quoteQty": 5_200.0,
                    },
                ],
                volume_source="exchange_user_trades",
                restart_runner=restarts.append,
            )

            control = json.loads(control_path.read_text(encoding="utf-8"))
            self.assertGreater(result["assessment"]["pace_ratio"], 0.5)
            self.assertLess(result["assessment"]["no_fill_seconds"], 180.0)
            self.assertTrue(result["assessment"]["budget_raise_inventory_buffer_blocked"])
            self.assertEqual(result["action"], "hold_budget_raise_for_inventory_imbalance", result)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 172.0)
            self.assertEqual(restarts, [])

if __name__ == "__main__":
    unittest.main()
