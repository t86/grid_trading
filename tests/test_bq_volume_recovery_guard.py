from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.bq_volume_recovery_guard import (
    check_symbol,
    fetch_recent_user_trades,
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

    def test_keeps_allow_loss_enabled_until_inventory_has_recovered_buffer(self) -> None:
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

            self.assertEqual(result["action"], "hold_recovery_until_cap_buffer")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

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
            self.assertEqual(restarts, ["REUSDT"])


if __name__ == "__main__":
    unittest.main()
