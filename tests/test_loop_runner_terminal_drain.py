from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from grid_optimizer.loop_runner import (
    _handle_terminal_drain_round,
    _terminal_drain_decision_id,
    _terminal_drain_fetch_user_trades_exact,
    _terminal_drain_runtime_owner_is_integral,
    _terminal_drain_runtime_integrity_digest,
    _terminal_drain_settle_loss_receipt,
)
from grid_optimizer.futures_run_lifecycle import run_contract_identity_from_config
from grid_optimizer.futures_terminal_drain import (
    DrainActionId,
    DrainLossLease,
    TerminalDrainState,
)


NOW = datetime(2026, 7, 15, 10, 9, 15, tzinfo=timezone.utc)


class LoopRunnerTerminalDrainTests(unittest.TestCase):
    def _loss_receipt_state(self) -> TerminalDrainState:
        return replace(
            TerminalDrainState.initial("BCHUSDT", decision_id="bch:receipt"),
            loss_reserved=5.0,
            loss_lease=DrainLossLease(
                action_id=DrainActionId.DRAIN_NET_LONG,
                maximum_loss=5.0,
                expires_at=NOW + timedelta(minutes=5),
            ),
        )

    def _loss_receipt_record(self) -> dict[str, object]:
        return {
            "client_order_id": "gtd-bch-receipt",
            "order_id": 901,
            "executed_qty": 0.1,
            "order": {"quantity": 0.1},
        }

    def _args(self, state_path: Path) -> argparse.Namespace:
        return argparse.Namespace(
            symbol="BCHUSDT",
            recv_window=5000,
            strategy_profile="test",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            run_start_time=(NOW - timedelta(hours=1)).isoformat(),
            runtime_guard_stats_start_time=(NOW - timedelta(hours=1)).isoformat(),
            run_end_time=NOW.isoformat(),
            max_cumulative_notional=20_000.0,
            state_path=str(state_path),
            per_order_notional=22.0,
            terminal_drain_max_order_notional=22.0,
            terminal_drain_absolute_loss_budget=5.0,
            terminal_drain_loss_lease_seconds=300.0,
            terminal_drain_order_reprice_seconds=120.0,
            terminal_drain_flat_confirm_cycles=2,
            terminal_drain_max_wait_seconds=900.0,
            terminal_drain_exit_policy="drain_then_preserve",
            terminal_drain_stop_preserve_reason=None,
            auto_regime_enabled=False,
        )

    def _guard_config(self) -> SimpleNamespace:
        return SimpleNamespace(
            run_start_time=NOW - timedelta(hours=1),
            run_end_time=NOW,
            rolling_hourly_loss_limit=None,
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10_000.0,
            max_cumulative_notional=20_000.0,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
            max_unrealized_loss=None,
        )

    def _guard_result(self, *, reasons: list[str] | None = None) -> SimpleNamespace:
        matched = reasons or ["after_end_window"]
        return SimpleNamespace(
            runtime_status="stopped",
            stop_triggered=True,
            primary_reason=matched[0],
            matched_reasons=matched,
            triggered_at=NOW.isoformat(),
            rolling_hourly_loss=0.0,
            rolling_hourly_gross_notional=1_817.0,
            rolling_hourly_loss_per_10k=0.0,
            rolling_hourly_loss_per_10k_active=False,
            cumulative_gross_notional=1_817.0,
            unrealized_loss=4.7,
        )

    def _positions(self) -> list[dict[str, str]]:
        return [
            {
                "symbol": "BCHUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.748",
                "entryPrice": "236.102158290782",
            },
            {
                "symbol": "BCHUSDT",
                "positionSide": "SHORT",
                "positionAmt": "-0.465",
                "entryPrice": "235.572267",
            },
        ]

    def _bq_ledger_state(
        self,
        *,
        normal_long_qty: float = 0.0,
        normal_short_qty: float = 0.0,
        frozen_long_qty: float = 0.0,
        frozen_short_qty: float = 0.0,
    ) -> dict[str, object]:
        def lots(qty: float, price: float) -> list[dict[str, object]]:
            return (
                [{"qty": qty, "price": price, "source": "test_owned_lot"}]
                if qty > 0
                else []
            )

        state: dict[str, object] = {
            "best_quote_volume_ledger": {
                "schema": "best_quote_volume_ledger_v1",
                "initialized": True,
                "sync_ok": True,
                "long_lots": lots(normal_long_qty, 236.0),
                "short_lots": lots(normal_short_qty, 235.0),
            }
        }
        if frozen_long_qty > 0 or frozen_short_qty > 0:
            state["best_quote_frozen_inventory"] = {
                "long_qty": frozen_long_qty,
                "short_qty": frozen_short_qty,
                "long_lots": lots(frozen_long_qty, 234.0),
                "short_lots": lots(frozen_short_qty, 237.0),
            }
        return state

    def _default_bq_state(self) -> dict[str, object]:
        return self._bq_ledger_state(
            normal_long_qty=0.748,
            normal_short_qty=0.465,
        )

    def test_exit_terms_are_part_of_immutable_run_decision_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            args = self._args(Path(tmpdir) / "state.json")
            clean_id = _terminal_drain_decision_id(args, self._guard_config())
            args.terminal_drain_max_wait_seconds = 1_200.0
            bounded_id = _terminal_drain_decision_id(args, self._guard_config())

        self.assertNotEqual(clean_id, bounded_id)

    def test_trade_watermark_full_single_millisecond_page_fails_closed(self) -> None:
        time_ms = int(NOW.timestamp() * 1000)
        full_page = [
            {
                "id": index,
                "orderId": 900 + index,
                "time": time_ms,
                "realizedPnl": "0",
            }
            for index in range(1000)
        ]
        with patch(
            "grid_optimizer.loop_runner.fetch_futures_user_trades",
            return_value=full_page,
        ):
            with self.assertRaisesRegex(ValueError, "density exceeds"):
                _terminal_drain_fetch_user_trades_exact(
                    symbol="BCHUSDT",
                    api_key="key",
                    api_secret="secret",
                    start_time_ms=time_ms,
                    end_time_ms=time_ms,
                    recv_window=5000,
                )

    def test_trade_watermark_missing_realized_pnl_fails_closed(self) -> None:
        time_ms = int(NOW.timestamp() * 1000)
        with patch(
            "grid_optimizer.loop_runner.fetch_futures_user_trades",
            return_value=[{"id": 1, "orderId": 901, "time": time_ms}],
        ):
            with self.assertRaisesRegex(ValueError, "realizedPnl is required"):
                _terminal_drain_fetch_user_trades_exact(
                    symbol="BCHUSDT",
                    api_key="key",
                    api_secret="secret",
                    start_time_ms=time_ms,
                    end_time_ms=time_ms,
                    recv_window=5000,
                )

    def test_filled_order_waits_for_user_trade_coverage_before_settling_loss(self) -> None:
        state = self._loss_receipt_state()
        unchanged, error = _terminal_drain_settle_loss_receipt(
            drain_state=state,
            active_records=[self._loss_receipt_record()],
            trades=[],
        )

        self.assertIs(unchanged, state)
        self.assertEqual(error["reason"], "loss_receipt_trade_coverage_incomplete")
        self.assertIsNotNone(unchanged.loss_lease)

    def test_partial_user_trade_coverage_keeps_loss_lease_active(self) -> None:
        state = self._loss_receipt_state()
        unchanged, error = _terminal_drain_settle_loss_receipt(
            drain_state=state,
            active_records=[self._loss_receipt_record()],
            trades=[
                {
                    "id": 1,
                    "orderId": 901,
                    "qty": "0.04",
                    "realizedPnl": "-1.0",
                }
            ],
        )

        self.assertIs(unchanged, state)
        self.assertEqual(error["reason"], "loss_receipt_trade_coverage_incomplete")
        self.assertEqual(error["covered_qty"], 0.04)

    def test_full_user_trade_coverage_settles_from_realized_pnl_only(self) -> None:
        state = self._loss_receipt_state()
        settled, error = _terminal_drain_settle_loss_receipt(
            drain_state=state,
            active_records=[self._loss_receipt_record()],
            trades=[
                {
                    "id": 1,
                    "orderId": 901,
                    "qty": "0.1",
                    "realizedPnl": "0",
                }
            ],
        )

        self.assertIsNone(error)
        self.assertIsNone(settled.loss_lease)
        self.assertEqual(settled.loss_used, 0.0)

    def test_late_complete_receipt_records_full_budget_breach(self) -> None:
        state = self._loss_receipt_state()
        pending, pending_error = _terminal_drain_settle_loss_receipt(
            drain_state=state,
            active_records=[self._loss_receipt_record()],
            trades=[],
        )
        breached, breach_error = _terminal_drain_settle_loss_receipt(
            drain_state=pending,
            active_records=[self._loss_receipt_record()],
            trades=[
                {
                    "id": 1,
                    "orderId": 901,
                    "qty": "0.1",
                    "realizedPnl": "-10.0",
                }
            ],
        )

        self.assertEqual(
            pending_error["reason"],
            "loss_receipt_trade_coverage_incomplete",
        )
        self.assertEqual(
            breach_error["reason"],
            "realized_loss_exceeds_authorized_lease",
        )
        self.assertTrue(breached.loss_budget_breached)
        self.assertEqual(breached.loss_used, 10.0)

    def test_missing_credentials_persists_exit_blocked_owner_instead_of_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            with patch(
                "grid_optimizer.loop_runner.load_binance_api_credentials",
                return_value=None,
            ):
                summary = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))[
                "futures_terminal_drain"
            ]

        self.assertEqual(summary["terminal_drain_action"], "hold_blocked")
        self.assertEqual(summary["exit_status"], "exit_blocked")
        self.assertFalse(summary["runner_exit_allowed"])
        self.assertEqual(persisted["exit_status"], "exit_blocked")
        self.assertEqual(
            persisted["last_errors"][0]["reason"],
            "credentials_unavailable",
        )

    def test_persisted_loss_budget_cannot_be_widened_mid_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_absolute_loss_budget = 1.0
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch(
                "grid_optimizer.loop_runner.post_futures_order",
                return_value={"orderId": 901, "status": "NEW"},
            ) as mock_post:
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                args.terminal_drain_absolute_loss_budget = 999.0
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertIn("loss_budget", blocked["terminal_drain_reasons"])
        mock_post.assert_not_called()

    def test_tampered_exit_contract_blocks_before_order_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    return_value={"orderId": 901, "status": "NEW", "executedQty": "0"},
                ) as mock_post,
                patch("grid_optimizer.loop_runner.delete_futures_order") as mock_cancel,
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                tampered = json.loads(state_path.read_text(encoding="utf-8"))
                tampered["futures_terminal_drain"]["exit_contract"][
                    "absolute_loss_budget"
                ] = 999.0
                state_path.write_text(json.dumps(tampered), encoding="utf-8")
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=tampered,
                    state_path=state_path,
                )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))[
                "futures_terminal_drain"
            ]

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertEqual(blocked["exit_status"], "exit_blocked")
        self.assertIn("run_contract_integrity", blocked["terminal_drain_reasons"][0])
        self.assertEqual(
            persisted["last_errors"][0]["reason"],
            "run_contract_integrity_exit_contract_mismatch",
        )
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()

    def test_terminal_owner_integrity_fields_fail_closed_before_order(self) -> None:
        def change_schema(state: dict[str, object]) -> None:
            state["futures_terminal_drain"]["schema"] = "wrong"  # type: ignore[index]

        def change_symbol_with_matching_digest(state: dict[str, object]) -> None:
            owner = state["futures_terminal_drain"]  # type: ignore[assignment]
            snapshot = owner["run_contract_snapshot"]
            snapshot["symbol"] = "ETHUSDT"
            contract_id = run_contract_identity_from_config(snapshot)
            owner["run_contract_id"] = contract_id
            owner["decision_id"] = f"ETHUSDT|{contract_id}"
            owner["drain_state"]["symbol"] = "ETHUSDT"
            owner["drain_state"]["decision_id"] = owner["decision_id"]

        def change_decision(state: dict[str, object]) -> None:
            state["futures_terminal_drain"]["decision_id"] = "BCHUSDT|wrong"  # type: ignore[index]

        def change_drain_state_owner(state: dict[str, object]) -> None:
            state["futures_terminal_drain"]["drain_state"]["symbol"] = "ETHUSDT"  # type: ignore[index]

        def move_started_at_and_exit_copy(state: dict[str, object]) -> None:
            owner = state["futures_terminal_drain"]  # type: ignore[assignment]
            changed = (NOW - timedelta(hours=5)).isoformat()
            owner["started_at"] = changed
            owner["exit_contract"]["captured_at"] = changed

        def change_durable_frozen_ledger(state: dict[str, object]) -> None:
            state["best_quote_frozen_inventory"] = {
                "long_qty": 0.1,
                "short_qty": 0.0,
            }

        cases = (
            ("schema", change_schema),
            ("symbol", change_symbol_with_matching_digest),
            ("decision", change_decision),
            ("drain_state", change_drain_state_owner),
            ("started_at", move_started_at_and_exit_copy),
            ("frozen_ledger", change_durable_frozen_ledger),
        )
        for case_name, mutate in cases:
            with self.subTest(case=case_name), tempfile.TemporaryDirectory() as tmpdir:
                state_path = Path(tmpdir) / "state.json"
                state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
                args = self._args(state_path)
                patches = self._base_patches(
                    positions=self._positions(),
                    open_orders=[],
                )
                with (
                    patches[0],
                    patches[1],
                    patches[2],
                    patches[3],
                    patches[4],
                    patches[5],
                    patches[6],
                    patch(
                        "grid_optimizer.loop_runner.post_futures_order",
                        return_value={"orderId": 901, "status": "NEW"},
                    ) as mock_post,
                ):
                    _handle_terminal_drain_round(
                        args=args,
                        cycle=1,
                        cycle_started_at=NOW,
                        stats_start_time=None,
                        runtime_guard_config=self._guard_config(),
                        runtime_guard_result=self._guard_result(),
                        cumulative_gross_notional=1_817.0,
                        state=self._default_bq_state(),
                        state_path=state_path,
                    )
                    changed = json.loads(state_path.read_text(encoding="utf-8"))
                    mutate(changed)
                    state_path.write_text(json.dumps(changed), encoding="utf-8")
                    blocked = _handle_terminal_drain_round(
                        args=args,
                        cycle=2,
                        cycle_started_at=NOW + timedelta(seconds=2),
                        stats_start_time=None,
                        runtime_guard_config=self._guard_config(),
                        runtime_guard_result=self._guard_result(),
                        cumulative_gross_notional=1_817.0,
                        state=changed,
                        state_path=state_path,
                    )

                self.assertEqual(blocked["exit_status"], "exit_blocked")
                self.assertIn(
                    "run_contract_integrity",
                    blocked["terminal_drain_reasons"][0],
                )
                mock_post.assert_not_called()

    def _base_patches(self, *, positions: list[dict[str, str]], open_orders: list[dict[str, object]]):
        return (
            patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret")),
            patch("grid_optimizer.loop_runner.fetch_futures_symbol_config", return_value={
                "min_notional": 20.0,
                "step_size": 0.001,
                "tick_size": 0.01,
            }),
            patch("grid_optimizer.loop_runner.fetch_futures_book_tickers", return_value=[{
                "bid_price": 220.37,
                "ask_price": 220.38,
            }]),
            patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}),
            patch("grid_optimizer.loop_runner.fetch_futures_position_risk_v3", return_value=positions),
            patch("grid_optimizer.loop_runner.fetch_futures_open_orders", return_value=open_orders),
            patch("grid_optimizer.loop_runner.fetch_futures_user_trades", return_value=[]),
        )

    def test_terminal_owner_freezes_first_observed_run_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(
                positions=self._positions(),
                open_orders=[],
            )
            target_result = self._guard_result(
                reasons=["max_cumulative_notional_hit"]
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    return_value={"orderId": 901, "status": "NEW"},
                ),
            ):
                first = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=NOW - timedelta(hours=1),
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                later = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=NOW - timedelta(hours=1),
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=target_result,
                    cumulative_gross_notional=25_000.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(first["run_outcome"], "target_unmet_deadline")
        self.assertEqual(first["achieved_value"], 1_817.0)
        self.assertEqual(later["run_outcome"], "target_unmet_deadline")
        self.assertEqual(later["achieved_value"], 1_817.0)
        self.assertEqual(later["target_shortfall"], 18_183.0)

    def test_deadline_blocks_entry_then_submits_one_hedge_gtx_reduce_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch(
                "grid_optimizer.loop_runner.post_futures_order",
                return_value={"orderId": 901, "status": "NEW"},
            ) as mock_post:
                block = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=NOW - timedelta(hours=1),
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                drain = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=NOW - timedelta(hours=1),
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))["futures_terminal_drain"]

        self.assertEqual(block["terminal_drain_action"], "block_entry")
        self.assertFalse(block["stop_triggered"])
        self.assertFalse(block["runner_exit_allowed"])
        self.assertEqual(drain["terminal_drain_action"], "drain_net_long")
        self.assertEqual(drain["run_outcome"], "target_unmet_deadline")
        self.assertEqual(drain["target_shortfall"], 18_183.0)
        self.assertFalse(drain["stop_triggered"])
        self.assertEqual(mock_post.call_count, 1)
        request = mock_post.call_args.kwargs
        self.assertEqual(request["time_in_force"], "GTX")
        self.assertEqual(request["side"], "SELL")
        self.assertEqual(request["position_side"], "LONG")
        self.assertIsNone(request["reduce_only"])
        self.assertGreaterEqual(request["quantity"] * request["price"], 20.0)
        self.assertLessEqual(request["quantity"] * request["price"], 22.0)
        self.assertFalse(persisted["drain_state"]["global_allow_loss"])

    def test_bq_terminal_missing_normal_ledger_holds_without_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(
                positions=self._positions(),
                open_orders=[],
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
            ):
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state={},
                    state_path=state_path,
                )
                recovered = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertIn("ledger", blocked["terminal_drain_reasons"][0])
        self.assertEqual(recovered["terminal_drain_action"], "block_entry")
        mock_post.assert_not_called()

    def test_bq_terminal_frozen_lot_summary_mismatch_holds_without_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state = self._bq_ledger_state(normal_long_qty=0.19)
            state["best_quote_frozen_inventory"] = {
                "long_qty": 0.05,
                "short_qty": 0.0,
                "long_lots": [{"qty": 0.04, "price": 234.0}],
                "short_lots": [],
            }
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            positions = [
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.24",
                    "entryPrice": "236",
                },
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                },
            ]
            patches = self._base_patches(positions=positions, open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
            ):
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=state,
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertIn("frozen", blocked["terminal_drain_reasons"][0])
        mock_post.assert_not_called()

    def test_bq_terminal_owner_freezes_normal_ledger_snapshot_and_drains_gtx(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state = self._bq_ledger_state(normal_long_qty=0.19)
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            positions = [
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.19",
                    "entryPrice": "236",
                },
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                },
            ]
            patches = self._base_patches(positions=positions, open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    return_value={"orderId": 901, "status": "NEW", "executedQty": "0"},
                ) as mock_post,
            ):
                first = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=state,
                    state_path=state_path,
                )
                persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
                second = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=persisted_state,
                    state_path=state_path,
                )
                owner = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]

        self.assertEqual(first["terminal_drain_action"], "block_entry")
        self.assertEqual(second["terminal_drain_action"], "drain_net_long")
        self.assertEqual(owner["initial_normal_long_qty"], 0.19)
        self.assertEqual(owner["initial_normal_short_qty"], 0.0)
        self.assertIn("initial_owned_ledger_digest", owner)
        self.assertEqual(mock_post.call_count, 1)
        self.assertEqual(mock_post.call_args.kwargs["time_in_force"], "GTX")

    def test_bq_terminal_fill_does_not_reclassify_manual_extra_as_owned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state = self._bq_ledger_state(normal_long_qty=0.095)
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            initial_positions = [
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.395",
                    "entryPrice": "236",
                },
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                },
            ]
            manual_only_positions = [
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.3",
                    "entryPrice": "236",
                },
                {
                    "symbol": "BCHUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                },
            ]
            position_calls = 0
            trade_calls = 0
            posted_qty: list[float] = []

            def positions_fetch(*_args, **_kwargs):
                nonlocal position_calls
                position_calls += 1
                return initial_positions if position_calls <= 2 else manual_only_positions

            def post_order(**kwargs):
                qty = float(kwargs["quantity"])
                posted_qty.append(qty)
                return {
                    "orderId": 900 + len(posted_qty),
                    "status": "FILLED",
                    "executedQty": str(qty),
                }

            def trades_fetch(*_args, **_kwargs):
                nonlocal trade_calls
                trade_calls += 1
                if trade_calls <= 2 or not posted_qty:
                    return []
                return [
                    {
                        "id": 1,
                        "orderId": 901,
                        "time": int((NOW + timedelta(seconds=1)).timestamp() * 1000),
                        "side": "SELL",
                        "positionSide": "LONG",
                        "qty": str(posted_qty[0]),
                        "price": "220.38",
                        "quoteQty": str(posted_qty[0] * 220.38),
                        "realizedPnl": "0",
                    }
                ]

            patches = self._base_patches(positions=initial_positions, open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_position_risk_v3",
                    side_effect=positions_fetch,
                ),
                patches[5],
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_user_trades",
                    side_effect=trades_fetch,
                ),
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    side_effect=post_order,
                ) as mock_post,
            ):
                summaries = []
                for cycle in range(1, 5):
                    current_state = json.loads(
                        state_path.read_text(encoding="utf-8")
                    )
                    summaries.append(
                        _handle_terminal_drain_round(
                            args=args,
                            cycle=cycle,
                            cycle_started_at=NOW + timedelta(seconds=cycle - 1),
                            stats_start_time=None,
                            runtime_guard_config=self._guard_config(),
                            runtime_guard_result=self._guard_result(),
                            cumulative_gross_notional=1_817.0,
                            state=current_state,
                            state_path=state_path,
                        )
                    )
                owner = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]

        self.assertEqual(summaries[0]["terminal_drain_action"], "block_entry")
        self.assertEqual(summaries[1]["terminal_drain_action"], "drain_net_long")
        self.assertAlmostEqual(posted_qty[0], 0.095, places=12)
        self.assertEqual(mock_post.call_count, 1)
        self.assertEqual(summaries[-1]["terminal_drain_action"], "hold_blocked")
        self.assertAlmostEqual(owner["terminal_filled_long_qty"], 0.095, places=12)
        self.assertAlmostEqual(owner["initial_normal_long_qty"], 0.095, places=12)

    def test_flat_requires_two_distinct_proofs_before_runner_exit(self) -> None:
        flat_positions = [
            {"symbol": "BCHUSDT", "positionSide": "LONG", "positionAmt": "0", "entryPrice": "0"},
            {"symbol": "BCHUSDT", "positionSide": "SHORT", "positionAmt": "0", "entryPrice": "0"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            intent = {
                "schema": "futures_lifecycle_intent_v2",
                "intent_id": "BCHUSDT-test-flat-intent",
                "symbol": "BCHUSDT",
                "action": "lifecycle_drain",
                "status": "accepted",
            }
            flat_state = self._bq_ledger_state()
            flat_state["futures_terminal_intent"] = intent
            state_path.write_text(
                json.dumps(flat_state),
                encoding="utf-8",
            )
            intent_path.write_text(json.dumps(intent), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=flat_positions, open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                block = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                verify = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                stop = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=4),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertEqual(block["terminal_drain_action"], "block_entry")
        self.assertEqual(verify["terminal_drain_action"], "verify_flat")
        self.assertFalse(verify["runner_exit_allowed"])
        self.assertEqual(stop["terminal_drain_action"], "stop_clean")
        self.assertTrue(stop["stop_triggered"])
        self.assertTrue(stop["runner_exit_allowed"])
        self.assertEqual(stop["exit_status"], "stopped_clean")
        self.assertEqual(persisted_intent["status"], "stopped_clean")

    def test_frozen_only_inventory_stops_preserved_without_reset_or_order(self) -> None:
        frozen_positions = [
            {
                "symbol": "BCHUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.283",
                "entryPrice": "236.102158290782",
            },
            {
                "symbol": "BCHUSDT",
                "positionSide": "SHORT",
                "positionAmt": "0",
                "entryPrice": "0",
            },
        ]
        frozen_state = self._bq_ledger_state(frozen_long_qty=0.283)
        frozen_ledger = frozen_state["best_quote_frozen_inventory"]
        intent = {
            "schema": "futures_lifecycle_intent_v2",
            "intent_id": "BCHUSDT-test-frozen-intent",
            "symbol": "BCHUSDT",
            "action": "lifecycle_drain",
            "status": "accepted",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            state_path.write_text(
                json.dumps(
                    {**frozen_state, "futures_terminal_intent": intent}
                ),
                encoding="utf-8",
            )
            intent_path.write_text(json.dumps(intent), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=frozen_positions, open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
                patch("grid_optimizer.loop_runner.delete_futures_order") as mock_cancel,
                patch(
                    "grid_optimizer.loop_runner.reset_best_quote_hedge_ledgers_after_exchange_flat"
                ) as mock_reset,
            ):
                block = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                verify = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=4),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
            persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
            persisted = persisted_state["futures_terminal_drain"]
            persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertEqual(block["terminal_drain_action"], "block_entry")
        self.assertEqual(verify["terminal_drain_action"], "verify_flat")
        self.assertEqual(stopped["terminal_drain_action"], "stop_preserve_frozen")
        self.assertTrue(stopped["runner_exit_allowed"])
        self.assertEqual(stopped["exit_status"], "stopped_preserved")
        self.assertEqual(persisted_state["best_quote_frozen_inventory"], frozen_ledger)
        self.assertNotIn("ledger_reset", persisted)
        self.assertNotIn("stopped_clean_at", persisted)
        self.assertEqual(
            persisted["preserve_reason"],
            "frozen_inventory_preserved_by_ledger_boundary",
        )
        self.assertEqual(persisted["residual_snapshot"]["long_qty"], 0.283)
        self.assertEqual(persisted["residual_snapshot"]["frozen_long_qty"], 0.283)
        self.assertEqual(persisted_intent["status"], "stopped_preserved")
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()
        mock_reset.assert_not_called()

    def test_durable_frozen_open_order_does_not_enter_terminal_entry_cancel(self) -> None:
        frozen_client_id = "gx-bchu-frozenin-1-deadbeef"
        frozen_order = {
            "symbol": "BCHUSDT",
            "orderId": 771,
            "clientOrderId": frozen_client_id,
            "side": "SELL",
            "positionSide": "LONG",
            "type": "LIMIT",
            "status": "NEW",
            "price": "220.50",
            "origQty": "0.100",
            "executedQty": "0",
        }
        frozen_positions = [
            {
                "symbol": "BCHUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.283",
                "entryPrice": "236.102158290782",
            },
            {
                "symbol": "BCHUSDT",
                "positionSide": "SHORT",
                "positionAmt": "0",
                "entryPrice": "0",
            },
        ]
        state = self._bq_ledger_state(frozen_long_qty=0.283)
        state["best_quote_frozen_inventory_manual_limit"] = {
            "long": {
                "requested": True,
                "submitted": True,
                "request_id": "frozen-limit-long-1",
                "requested_qty": 0.1,
            }
        }
        state["best_quote_volume_order_refs"] = {
            "771": {
                "book": "frozen_bq",
                "role": "frozen_inventory_manual_limit_long",
                "side": "SELL",
                "position_side": "LONG",
                "client_order_id": frozen_client_id,
                "frozen_inventory_request_id": "frozen-limit-long-1",
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(
                positions=frozen_positions,
                open_orders=[frozen_order],
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
                patch("grid_optimizer.loop_runner.delete_futures_order") as mock_cancel,
            ):
                first = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=state,
                    state_path=state_path,
                )
                second = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(first["terminal_drain_action"], "block_entry")
        self.assertEqual(second["terminal_drain_action"], "verify_flat")
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()

    def test_frozen_fill_during_terminal_owner_uses_current_frozen_subtraction(self) -> None:
        positions = [
            {
                "symbol": "BCHUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.240",
                "entryPrice": "236",
            },
            {
                "symbol": "BCHUSDT",
                "positionSide": "SHORT",
                "positionAmt": "0",
                "entryPrice": "0",
            },
        ]
        state = self._bq_ledger_state(
            normal_long_qty=0.19,
            frozen_long_qty=0.05,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=positions, open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    return_value={"orderId": 901, "status": "NEW"},
                ) as mock_post,
            ):
                first = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=state,
                    state_path=state_path,
                )
                next_state = json.loads(state_path.read_text(encoding="utf-8"))
                next_state["best_quote_frozen_inventory"] = self._bq_ledger_state(
                    frozen_long_qty=0.03,
                )["best_quote_frozen_inventory"]
                self.assertTrue(
                    _terminal_drain_runtime_owner_is_integral(
                        next_state["futures_terminal_drain"],
                        symbol="BCHUSDT",
                        loop_state=next_state,
                    )
                )
                positions[0]["positionAmt"] = "0.220"
                second = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=next_state,
                    state_path=state_path,
                )

        self.assertEqual(first["terminal_drain_action"], "block_entry")
        self.assertEqual(second["terminal_drain_action"], "drain_net_long")
        self.assertNotIn(
            "run_contract_integrity_frozen_ledger_mismatch",
            second["terminal_drain_reasons"],
        )
        self.assertNotIn(
            "owned_inventory_exceeds_exchange",
            second["terminal_drain_reasons"],
        )
        mock_post.assert_called_once()

    def test_frozen_increase_during_terminal_owner_keeps_ordinary_flat(self) -> None:
        positions = [
            {
                "symbol": "BCHUSDT",
                "positionSide": "LONG",
                "positionAmt": "0.050",
                "entryPrice": "236",
            },
            {
                "symbol": "BCHUSDT",
                "positionSide": "SHORT",
                "positionAmt": "0",
                "entryPrice": "0",
            },
        ]
        state = self._bq_ledger_state(frozen_long_qty=0.05)
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=positions, open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
            ):
                first = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=state,
                    state_path=state_path,
                )
                next_state = json.loads(state_path.read_text(encoding="utf-8"))
                next_state["best_quote_frozen_inventory"] = self._bq_ledger_state(
                    frozen_long_qty=0.07,
                )["best_quote_frozen_inventory"]
                second = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=next_state,
                    state_path=state_path,
                )

        self.assertEqual(first["terminal_drain_action"], "block_entry")
        self.assertEqual(second["terminal_drain_action"], "verify_flat")
        self.assertNotIn(
            "run_contract_integrity_frozen_ledger_mismatch",
            second["terminal_drain_reasons"],
        )
        mock_post.assert_not_called()

    def test_unbound_frozen_prefix_open_order_remains_terminal_entry_cancel(self) -> None:
        spoofed_client_id = "gx-bchu-frozenin-99-spoofed"
        spoofed_order = {
            "symbol": "BCHUSDT",
            "orderId": 772,
            "clientOrderId": spoofed_client_id,
            "side": "BUY",
            "positionSide": "LONG",
            "type": "LIMIT",
            "status": "NEW",
            "price": "220.00",
            "origQty": "0.100",
            "executedQty": "0",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state = self._default_bq_state()
            state_path.write_text(json.dumps(state), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(
                positions=self._positions(),
                open_orders=[spoofed_order],
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
                patch(
                    "grid_optimizer.loop_runner.delete_futures_order",
                    return_value={"status": "CANCELED", "executedQty": "0"},
                ) as mock_cancel,
            ):
                first = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=state,
                    state_path=state_path,
                )
                second = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(first["terminal_drain_action"], "block_entry")
        self.assertEqual(second["terminal_drain_action"], "cancel_entry")
        mock_post.assert_not_called()
        self.assertEqual(mock_cancel.call_count, 1)
        self.assertEqual(
            mock_cancel.call_args.kwargs["orig_client_order_id"],
            spoofed_client_id,
        )

    def test_only_explicit_stop_preserve_can_exit_with_residual_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "stop_preserve"
            args.terminal_drain_stop_preserve_reason = "operator_keep_for_validation"
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch(
                "grid_optimizer.loop_runner.post_futures_order"
            ) as mock_post, patch("grid_optimizer.loop_runner.delete_futures_order") as mock_cancel:
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                args.terminal_drain_exit_policy = "drain_then_preserve"
                args.terminal_drain_max_wait_seconds = 1_200.0
                args.terminal_drain_absolute_loss_budget = 999.0
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))["futures_terminal_drain"]

        self.assertEqual(blocked["terminal_drain_action"], "block_entry")
        self.assertFalse(blocked["runner_exit_allowed"])
        self.assertTrue(stopped["runner_exit_allowed"])
        self.assertEqual(stopped["run_outcome"], "target_unmet_deadline")
        self.assertEqual(stopped["current_long_qty"], 0.748)
        self.assertEqual(stopped["current_short_qty"], 0.465)
        self.assertTrue(persisted["preserved_by_user"])
        self.assertEqual(persisted["preserve_reason"], "operator_keep_for_validation")
        self.assertEqual(persisted["residual_snapshot"]["long_qty"], 0.748)
        self.assertEqual(persisted["residual_snapshot"]["short_qty"], 0.465)
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()

    def test_stop_preserve_blocks_on_unmanaged_open_order_without_canceling_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "stop_preserve"
            args.terminal_drain_stop_preserve_reason = "operator_keep_for_validation"
            unmanaged_order = {
                "orderId": 777,
                "clientOrderId": "manual-validation-order",
                "status": "NEW",
                "executedQty": "0",
            }
            patches = self._base_patches(
                positions=self._positions(),
                open_orders=[unmanaged_order],
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
                patch("grid_optimizer.loop_runner.delete_futures_order") as mock_cancel,
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertEqual(blocked["exit_status"], "exit_blocked")
        self.assertIn("unmanaged_orders_open", blocked["terminal_drain_reasons"])
        self.assertFalse(blocked["runner_exit_allowed"])
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()

    def test_stop_preserve_rejects_incomplete_position_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "stop_preserve"
            args.terminal_drain_stop_preserve_reason = "operator_keep_for_validation"
            patches = self._base_patches(positions=[], open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertEqual(blocked["exit_status"], "exit_blocked")
        self.assertFalse(blocked["runner_exit_allowed"])
        self.assertEqual(
            blocked["terminal_drain_errors"][0]["reason"],
            "position_observation_incomplete",
        )

    def test_stop_preserve_rejects_non_finite_position_quantity(self) -> None:
        invalid_positions = [
            {
                "symbol": "BCHUSDT",
                "positionSide": "LONG",
                "positionAmt": "nan",
                "entryPrice": "236.1",
            },
            {
                "symbol": "BCHUSDT",
                "positionSide": "SHORT",
                "positionAmt": "-0.465",
                "entryPrice": "235.57",
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "stop_preserve"
            args.terminal_drain_stop_preserve_reason = "operator_keep_for_validation"
            patches = self._base_patches(positions=invalid_positions, open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertEqual(blocked["exit_status"], "exit_blocked")
        self.assertIn(
            "non_finite_position_amount:LONG",
            blocked["terminal_drain_errors"][0]["details"],
        )

    def test_drain_then_preserve_has_bounded_residual_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "drain_then_preserve"
            args.terminal_drain_max_wait_seconds = 60.0
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch(
                "grid_optimizer.loop_runner.post_futures_order"
            ) as mock_post, patch("grid_optimizer.loop_runner.delete_futures_order") as mock_cancel:
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=61),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))["futures_terminal_drain"]

        self.assertEqual(blocked["terminal_drain_action"], "block_entry")
        self.assertEqual(stopped["terminal_drain_action"], "stop_preserve")
        self.assertTrue(stopped["runner_exit_allowed"])
        self.assertEqual(stopped["exit_status"], "stopped_preserved")
        self.assertEqual(persisted["preserve_reason"], "terminal_drain_max_wait_exceeded")
        self.assertTrue(persisted["preserved_by_contract"])
        self.assertEqual(persisted["residual_snapshot"]["long_qty"], 0.748)
        self.assertEqual(persisted["residual_snapshot"]["short_qty"], 0.465)
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()

    def test_bounded_preserve_stops_with_fresh_unmanaged_order_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(self._default_bq_state()),
                encoding="utf-8",
            )
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "drain_then_preserve"
            args.terminal_drain_max_wait_seconds = 60.0
            unmanaged_order = {
                "orderId": 777,
                "clientOrderId": "manual-validation-order",
                "status": "NEW",
                "executedQty": "0",
            }
            patches = self._base_patches(
                positions=self._positions(),
                open_orders=[unmanaged_order],
            )
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch("grid_optimizer.loop_runner.post_futures_order") as mock_post,
                patch(
                    "grid_optimizer.loop_runner.delete_futures_order"
                ) as mock_cancel,
            ):
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=61),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]

        self.assertEqual(blocked["terminal_drain_action"], "block_entry")
        self.assertEqual(stopped["terminal_drain_action"], "stop_preserve")
        self.assertEqual(stopped["exit_status"], "stopped_preserved")
        self.assertTrue(stopped["runner_exit_allowed"])
        self.assertEqual(
            persisted["drain_state"]["stage"],
            "stopped_preserved",
        )
        self.assertEqual(
            persisted["residual_snapshot"]["unmanaged_open_orders"],
            [
                {
                    "orderId": "777",
                    "clientOrderId": "manual-validation-order",
                }
            ],
        )
        self.assertEqual(persisted["residual_snapshot"]["long_qty"], 0.748)
        self.assertEqual(persisted["residual_snapshot"]["short_qty"], 0.465)
        mock_post.assert_not_called()
        mock_cancel.assert_not_called()

    def test_timeout_cancels_owned_order_before_preserving_residual(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "drain_then_preserve"
            args.terminal_drain_max_wait_seconds = 60.0
            owned_order = {
                "orderId": 901,
                "clientOrderId": "gtd-bchusdt-owned",
                "status": "NEW",
                "executedQty": "0",
            }
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patch(
                "grid_optimizer.loop_runner.fetch_futures_open_orders",
                side_effect=[[owned_order], [owned_order], []],
            ), patches[6], patch("grid_optimizer.loop_runner.post_futures_order") as mock_post, patch(
                "grid_optimizer.loop_runner.delete_futures_order",
                return_value={"status": "CANCELED"},
            ) as mock_cancel:
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                canceled = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=61),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=62),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "block_entry")
        self.assertEqual(canceled["terminal_drain_action"], "cancel_drain_order")
        self.assertFalse(canceled["runner_exit_allowed"])
        self.assertEqual(stopped["terminal_drain_action"], "stop_preserve")
        self.assertTrue(stopped["runner_exit_allowed"])
        mock_cancel.assert_called_once()
        mock_post.assert_not_called()

    def test_loss_lease_is_reconciled_before_any_next_drain_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch(
                "grid_optimizer.loop_runner.fetch_futures_user_trades",
                return_value=[
                    {
                        "id": 1,
                        "orderId": 901,
                        "time": int(NOW.timestamp() * 1000),
                        "side": "SELL",
                        "positionSide": "LONG",
                        "qty": "0.094",
                        "realizedPnl": "-1.0",
                    }
                ],
            ), patch(
                "grid_optimizer.loop_runner.post_futures_order",
                return_value={"orderId": 901, "status": "NEW"},
            ) as mock_post, patch(
                "grid_optimizer.loop_runner.fetch_futures_order",
                return_value={"orderId": 901, "status": "FILLED", "executedQty": "0.094"},
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                receipt = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=4),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))["futures_terminal_drain"]

        self.assertEqual(receipt["terminal_drain_action"], "reconcile_loss_receipt")
        self.assertEqual(mock_post.call_count, 1)
        self.assertIsNone(persisted["drain_state"]["loss_lease"])
        self.assertEqual(persisted["drain_state"]["loss_reserved"], 0.0)
        self.assertGreater(persisted["drain_state"]["loss_used"], 0.0)

    def test_preserve_reconciles_terminal_active_loss_lease_before_stopping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "drain_then_preserve"
            args.terminal_drain_max_wait_seconds = 60.0
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch(
                "grid_optimizer.loop_runner.fetch_futures_user_trades",
                return_value=[
                    {
                        "id": 1,
                        "orderId": 901,
                        "time": int(NOW.timestamp() * 1000),
                        "side": "SELL",
                        "positionSide": "LONG",
                        "qty": "0.094",
                        "realizedPnl": "-1.0",
                    }
                ],
            ), patch(
                "grid_optimizer.loop_runner.post_futures_order",
                return_value={"orderId": 901, "status": "FILLED", "executedQty": "0.094"},
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                receipt = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=61),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=4,
                    cycle_started_at=NOW + timedelta(seconds=62),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]

        self.assertEqual(receipt["terminal_drain_action"], "reconcile_loss_receipt")
        self.assertFalse(receipt["runner_exit_allowed"])
        self.assertEqual(stopped["terminal_drain_action"], "stop_preserve")
        self.assertTrue(stopped["runner_exit_allowed"])
        self.assertIsNone(persisted["drain_state"]["loss_lease"])
        self.assertEqual(persisted["drain_state"]["loss_reserved"], 0.0)

    def test_actual_loss_over_authorized_lease_blocks_exit_instead_of_clipping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "drain_then_preserve"
            args.terminal_drain_max_wait_seconds = 60.0
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_user_trades",
                    return_value=[
                        {
                            "id": 1,
                            "orderId": 901,
                            "time": int(NOW.timestamp() * 1000),
                            "side": "SELL",
                            "positionSide": "LONG",
                            "qty": "0.094",
                            "realizedPnl": "-10.0",
                        }
                    ],
                ),
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    return_value={"orderId": 901, "status": "FILLED", "executedQty": "0.094"},
                ),
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                blocked = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=61),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                breach_state = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=4,
                    cycle_started_at=NOW + timedelta(seconds=62),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(blocked["terminal_drain_action"], "hold_blocked")
        self.assertEqual(blocked["exit_status"], "exit_blocked")
        self.assertFalse(blocked["runner_exit_allowed"])
        self.assertEqual(
            blocked["terminal_drain_errors"][0]["reason"],
            "realized_loss_exceeds_authorized_lease",
        )
        self.assertIsNone(breach_state["drain_state"]["loss_lease"])
        self.assertEqual(breach_state["drain_state"]["loss_reserved"], 0.0)
        self.assertEqual(breach_state["drain_state"]["loss_used"], 10.0)
        self.assertTrue(breach_state["drain_state"]["loss_budget_breached"])
        self.assertEqual(breach_state["budget_breaches"][0]["realized_loss"], 10.0)
        self.assertEqual(stopped["terminal_drain_action"], "stop_preserve")
        self.assertEqual(stopped["exit_status"], "stopped_preserved")
        self.assertTrue(stopped["runner_exit_allowed"])

    def test_ambiguous_submit_reuses_deterministic_client_id_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch(
                "grid_optimizer.loop_runner.post_futures_order",
                side_effect=[RuntimeError("submit timeout"), {"orderId": 902, "status": "NEW"}],
            ) as mock_post, patch(
                "grid_optimizer.loop_runner.fetch_futures_order",
                side_effect=RuntimeError("Binance API error -2013: Order does not exist"),
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                failed = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                resumed = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=4),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(failed["exit_status"], "exit_blocked")
        self.assertEqual(resumed["terminal_drain_action"], "drain_net_long")
        self.assertEqual(mock_post.call_count, 2)
        first_id = mock_post.call_args_list[0].kwargs["new_client_order_id"]
        second_id = mock_post.call_args_list[1].kwargs["new_client_order_id"]
        self.assertEqual(first_id, second_id)

    def test_gtx_reject_reclaims_loss_lease_and_replans_from_fresh_book(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with (
                patches[0],
                patches[1],
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_book_tickers",
                    side_effect=[
                        [{"bid_price": 220.37, "ask_price": 220.38}],
                        [{"bid_price": 220.37, "ask_price": 220.38}],
                        [{"bid_price": 221.37, "ask_price": 221.38}],
                    ],
                ),
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    side_effect=[
                        RuntimeError(
                            "Binance API error -5022: Post Only Order will be rejected."
                        ),
                        {"orderId": 902, "status": "NEW"},
                    ],
                ) as mock_post,
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_order",
                    side_effect=RuntimeError(
                        "Binance API error -2013: Order does not exist"
                    ),
                ),
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                rejected = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                rejected_state = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]
                replanned = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=4),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(rejected["exit_status"], "exiting")
        self.assertEqual(rejected_state["active_intent_ids"], [])
        self.assertEqual(rejected_state["intents"][-1]["status"], "REJECTED")
        self.assertEqual(
            rejected_state["intents"][-1]["terminal_resolution"],
            "gtx_post_only_rejected",
        )
        self.assertIsNone(rejected_state["drain_state"]["loss_lease"])
        self.assertEqual(rejected_state["drain_state"]["loss_reserved"], 0.0)
        self.assertEqual(replanned["terminal_drain_action"], "drain_net_long")
        self.assertEqual(mock_post.call_count, 2)
        first_call, second_call = mock_post.call_args_list
        self.assertNotEqual(
            first_call.kwargs["new_client_order_id"],
            second_call.kwargs["new_client_order_id"],
        )
        self.assertNotEqual(first_call.kwargs["price"], second_call.kwargs["price"])

    def test_expired_prepared_loss_lease_is_reclaimed_without_reposting(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    side_effect=[
                        RuntimeError("submit timeout"),
                        {"orderId": 902, "status": "NEW"},
                    ],
                ) as mock_post,
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_order",
                    side_effect=RuntimeError(
                        "Binance API error -2013: Order does not exist"
                    ),
                ),
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                crashed_state = json.loads(state_path.read_text(encoding="utf-8"))
                envelope = crashed_state["futures_terminal_drain"]
                record = envelope["intents"][-1]
                record["status"] = "PREPARED"
                record.pop("ambiguous_at", None)
                record.pop("last_error", None)
                envelope["drain_state"]["loss_lease"]["expires_at"] = (
                    NOW + timedelta(seconds=3)
                ).isoformat()
                envelope["runtime_integrity_digest"] = (
                    _terminal_drain_runtime_integrity_digest(envelope)
                )
                state_path.write_text(json.dumps(crashed_state), encoding="utf-8")
                mock_post.reset_mock()
                first_proof = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=400),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                first_proof_state = json.loads(
                    state_path.read_text(encoding="utf-8")
                )["futures_terminal_drain"]
                expired = _handle_terminal_drain_round(
                    args=args,
                    cycle=4,
                    cycle_started_at=NOW + timedelta(seconds=410),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]
                replanned = _handle_terminal_drain_round(
                    args=args,
                    cycle=5,
                    cycle_started_at=NOW + timedelta(seconds=412),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(mock_post.call_count, 1)
        self.assertEqual(first_proof["exit_status"], "exit_blocked")
        self.assertEqual(
            first_proof_state["active_intent_ids"],
            [first_proof_state["intents"][-1]["client_order_id"]],
        )
        self.assertEqual(first_proof_state["intents"][-1]["status"], "AMBIGUOUS")
        self.assertIsNotNone(first_proof_state["drain_state"]["loss_lease"])
        self.assertEqual(expired["exit_status"], "exiting")
        self.assertEqual(persisted["active_intent_ids"], [])
        self.assertEqual(persisted["intents"][-1]["status"], "EXPIRED")
        self.assertEqual(
            persisted["intents"][-1]["terminal_resolution"],
            "loss_lease_expired_before_submit",
        )
        self.assertIsNone(persisted["drain_state"]["loss_lease"])
        self.assertEqual(persisted["drain_state"]["loss_reserved"], 0.0)
        self.assertEqual(replanned["terminal_drain_action"], "drain_net_long")
        self.assertNotEqual(
            persisted["intents"][-1]["client_order_id"],
            mock_post.call_args.kwargs["new_client_order_id"],
        )

    def test_prepared_order_holds_when_exchange_position_falls_below_owned_snapshot(self) -> None:
        reduced_positions = self._positions()
        reduced_positions[0] = dict(reduced_positions[0], positionAmt="0.648")
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_position_risk_v3",
                    side_effect=[
                        self._positions(),
                        self._positions(),
                        reduced_positions,
                        reduced_positions,
                        reduced_positions,
                    ],
                ),
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    side_effect=[
                        RuntimeError("submit timeout"),
                        {"orderId": 902, "status": "NEW"},
                    ],
                ) as mock_post,
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_order",
                    side_effect=RuntimeError(
                        "Binance API error -2013: Order does not exist"
                    ),
                ),
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                crashed_state = json.loads(state_path.read_text(encoding="utf-8"))
                record = crashed_state["futures_terminal_drain"]["intents"][-1]
                record["status"] = "PREPARED"
                record.pop("ambiguous_at", None)
                record.pop("last_error", None)
                crashed_state["futures_terminal_drain"][
                    "runtime_integrity_digest"
                ] = _terminal_drain_runtime_integrity_digest(
                    crashed_state["futures_terminal_drain"]
                )
                state_path.write_text(json.dumps(crashed_state), encoding="utf-8")
                mock_post.reset_mock()
                first_proof = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=12),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                first_proof_state = json.loads(
                    state_path.read_text(encoding="utf-8")
                )["futures_terminal_drain"]
                retired = _handle_terminal_drain_round(
                    args=args,
                    cycle=4,
                    cycle_started_at=NOW + timedelta(seconds=22),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                persisted = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]
                replanned = _handle_terminal_drain_round(
                    args=args,
                    cycle=5,
                    cycle_started_at=NOW + timedelta(seconds=24),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(mock_post.call_count, 0)
        self.assertEqual(
            first_proof["terminal_drain_action"],
            "hold_blocked",
        )
        self.assertIn(
            "owned_inventory_exceeds_exchange",
            first_proof["terminal_drain_reasons"],
        )
        self.assertEqual(first_proof["exit_status"], "exit_blocked")
        self.assertEqual(
            first_proof_state["active_intent_ids"],
            [first_proof_state["intents"][-1]["client_order_id"]],
        )
        self.assertIsNotNone(first_proof_state["drain_state"]["loss_lease"])
        self.assertEqual(retired["terminal_drain_action"], "hold_blocked")
        self.assertEqual(
            persisted["active_intent_ids"],
            [persisted["intents"][-1]["client_order_id"]],
        )
        self.assertEqual(persisted["intents"][-1]["status"], "PREPARED")
        self.assertIsNotNone(persisted["drain_state"]["loss_lease"])
        self.assertEqual(replanned["terminal_drain_action"], "hold_blocked")

    def test_timeout_requires_two_negative_order_proofs_before_preserving_ambiguous_submit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._default_bq_state()), encoding="utf-8")
            args = self._args(state_path)
            args.terminal_drain_exit_policy = "drain_then_preserve"
            args.terminal_drain_max_wait_seconds = 60.0
            patches = self._base_patches(positions=self._positions(), open_orders=[])
            with (
                patches[0],
                patches[1],
                patches[2],
                patches[3],
                patches[4],
                patches[5],
                patches[6],
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    side_effect=RuntimeError("submit timeout"),
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_order",
                    side_effect=RuntimeError(
                        "Binance API error -2013: Order does not exist"
                    ),
                ) as mock_fetch_order,
            ):
                _handle_terminal_drain_round(
                    args=args,
                    cycle=1,
                    cycle_started_at=NOW,
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=self._default_bq_state(),
                    state_path=state_path,
                )
                ambiguous = _handle_terminal_drain_round(
                    args=args,
                    cycle=2,
                    cycle_started_at=NOW + timedelta(seconds=2),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                first_proof = _handle_terminal_drain_round(
                    args=args,
                    cycle=3,
                    cycle_started_at=NOW + timedelta(seconds=61),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                too_soon = _handle_terminal_drain_round(
                    args=args,
                    cycle=4,
                    cycle_started_at=NOW + timedelta(seconds=62),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                second_proof = _handle_terminal_drain_round(
                    args=args,
                    cycle=5,
                    cycle_started_at=NOW + timedelta(seconds=72),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )
                proof_state = json.loads(state_path.read_text(encoding="utf-8"))[
                    "futures_terminal_drain"
                ]
                stopped = _handle_terminal_drain_round(
                    args=args,
                    cycle=6,
                    cycle_started_at=NOW + timedelta(seconds=73),
                    stats_start_time=None,
                    runtime_guard_config=self._guard_config(),
                    runtime_guard_result=self._guard_result(),
                    cumulative_gross_notional=1_817.0,
                    state=json.loads(state_path.read_text(encoding="utf-8")),
                    state_path=state_path,
                )

        self.assertEqual(ambiguous["exit_status"], "exit_blocked")
        self.assertFalse(first_proof["runner_exit_allowed"])
        self.assertEqual(first_proof["terminal_drain_action"], "hold_blocked")
        self.assertFalse(too_soon["runner_exit_allowed"])
        self.assertEqual(too_soon["terminal_drain_action"], "hold_blocked")
        self.assertIn("quiet_window", too_soon["terminal_drain_reasons"][0])
        self.assertFalse(second_proof["runner_exit_allowed"])
        self.assertEqual(second_proof["terminal_drain_action"], "reconcile_order_receipt")
        self.assertTrue(stopped["runner_exit_allowed"])
        self.assertEqual(stopped["exit_status"], "stopped_preserved")
        self.assertEqual(mock_fetch_order.call_count, 3)
        proof_record = proof_state["intents"][0]
        self.assertEqual(proof_record["not_found_confirmations"], 2)
        self.assertEqual(len(proof_record["not_found_proofs"]), 2)
        for proof in proof_record["not_found_proofs"]:
            self.assertEqual(proof["client_order_id"], proof_record["client_order_id"])
            self.assertTrue(proof["client_id_absent_from_open_orders"])
            self.assertIn("position_observed_at", proof)
            self.assertIn("trade_watermark_end_at", proof)


if __name__ == "__main__":
    unittest.main()
