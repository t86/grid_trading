from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

from grid_optimizer.futures_run_lifecycle import (
    CleanFlatProof,
    LifecycleActionId,
    RUN_CONTRACT_OWNER_KEY,
    RecoveryIntent,
    RunLifecyclePolicy,
    RunLifecycleSnapshot,
    RunContract,
    RunOutcome,
    RunPhase,
    RunState,
    TerminalDrainFeedback,
    bind_run_contract_owner,
    decide_run_lifecycle,
    run_contract_snapshot_digest,
    run_contract_snapshot_from_config,
    resolve_authoritative_run_contract,
    validate_run_contract_owner,
    validate_run_contract,
)


NOW = datetime(2026, 7, 15, 9, 30, tzinfo=timezone.utc)
END = NOW + timedelta(minutes=30)


class FuturesRunLifecycleTests(unittest.TestCase):
    def _policy(self) -> RunLifecyclePolicy:
        return RunLifecyclePolicy(
            flat_tolerance=1e-9,
            flat_proof_max_age=timedelta(seconds=10),
            flat_confirmations_required=2,
        )

    def _state(self) -> RunState:
        return RunState.initial("GENERICUSDT", run_id="run-1")

    def _snapshot(self, **overrides: object) -> RunLifecycleSnapshot:
        values: dict[str, object] = {
            "symbol": "GENERICUSDT",
            "captured_at": NOW,
            "target_value": 20_000.0,
            "achieved_value": 2_000.0,
            "deadline": END,
            "observed_rate": 5.0,
            "recovery_intent": None,
            "terminal_condition_reason": None,
            "terminal_drain": None,
            "stop_preserve_requested": False,
            "stop_preserve_reason": None,
        }
        values.update(overrides)
        return RunLifecycleSnapshot(**values)

    def _clean_proof(self, **overrides: object) -> CleanFlatProof:
        values: dict[str, object] = {
            "captured_at": END + timedelta(seconds=2),
            "exchange_long_qty": 0.0,
            "exchange_short_qty": 0.0,
            "owned_long_qty": 0.0,
            "owned_short_qty": 0.0,
            "open_managed_order_count": 0,
            "ledger_reconciled": True,
            "trade_watermark_reconciled": True,
            "confirmation_count": 2,
        }
        values.update(overrides)
        return CleanFlatProof(**values)

    def _terminal_state(self, *, outcome: RunOutcome = RunOutcome.TARGET_UNMET_DEADLINE) -> RunState:
        decision = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(captured_at=END),
            policy=self._policy(),
        )
        self.assertEqual(decision.action_id, LifecycleActionId.START_TERMINAL_DRAIN)
        self.assertEqual(decision.next_state.outcome, outcome)
        return decision.next_state

    def test_low_pace_before_deadline_gets_one_recovery_intent_not_early_exit(self) -> None:
        decision = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(observed_rate=5.0),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.APPLY_RECOVERY)
        self.assertEqual(decision.next_state.phase, RunPhase.RECOVERING)
        self.assertIsNone(decision.next_state.outcome)
        self.assertEqual(decision.intent.action_id, "restore_required_pace")
        self.assertEqual(decision.intent.reason, "pace_below_required")
        self.assertFalse(decision.stop_allowed)

    def test_recoverable_blocker_has_priority_over_generic_pace_recovery(self) -> None:
        recovery = RecoveryIntent(
            action_id="restore_missing_quotes",
            reason="no_managed_orders",
        )

        decision = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(recovery_intent=recovery, observed_rate=0.0),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.APPLY_RECOVERY)
        self.assertEqual(decision.intent, recovery)
        self.assertEqual(decision.next_state.recovery_attempts, 1)
        self.assertIsNone(decision.next_state.outcome)

    def test_recovered_pace_keeps_running_toward_target(self) -> None:
        recovering = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(observed_rate=5.0),
            policy=self._policy(),
        ).next_state

        decision = decide_run_lifecycle(
            state=recovering,
            snapshot=self._snapshot(observed_rate=20.0),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.CONTINUE_TARGET)
        self.assertEqual(decision.next_state.phase, RunPhase.RUNNING)
        self.assertIsNone(decision.next_state.outcome)

    def test_target_reached_enters_terminal_drain_with_explicit_outcome(self) -> None:
        decision = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(achieved_value=20_010.0),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.START_TERMINAL_DRAIN)
        self.assertEqual(decision.next_state.phase, RunPhase.TERMINAL_DRAIN)
        self.assertEqual(decision.next_state.outcome, RunOutcome.TARGET_REACHED)
        self.assertEqual(decision.next_state.target_value, 20_000.0)
        self.assertEqual(decision.next_state.terminal_achieved_value, 20_010.0)
        self.assertEqual(decision.next_state.terminal_shortfall, 0.0)
        self.assertFalse(decision.stop_allowed)

    def test_deadline_unmet_enters_terminal_drain_and_records_shortfall(self) -> None:
        decision = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(minutes=30),
                deadline=NOW + timedelta(minutes=30),
                achieved_value=1_817.0,
                recovery_intent=RecoveryIntent("restore_missing_quotes", "no_managed_orders"),
            ),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.START_TERMINAL_DRAIN)
        self.assertEqual(decision.next_state.outcome, RunOutcome.TARGET_UNMET_DEADLINE)
        self.assertEqual(decision.next_state.terminal_shortfall, 18_183.0)
        self.assertEqual(decision.next_state.terminal_reason, "deadline_reached")
        self.assertFalse(decision.stop_allowed)

    def test_explicit_terminal_condition_has_a_defined_drain_exit(self) -> None:
        decision = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(terminal_condition_reason="market_not_tradable"),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.START_TERMINAL_DRAIN)
        self.assertEqual(decision.next_state.outcome, RunOutcome.CONDITION_UNMET)
        self.assertEqual(decision.next_state.terminal_reason, "market_not_tradable")

    def test_terminal_drain_runs_only_one_returned_action_per_round(self) -> None:
        terminal = self._terminal_state()

        decision = decide_run_lifecycle(
            state=terminal,
            snapshot=self._snapshot(
                captured_at=END + timedelta(seconds=1),
                terminal_drain=TerminalDrainFeedback(action_id="drain_net_long"),
            ),
            policy=self._policy(),
        )

        self.assertEqual(decision.action_id, LifecycleActionId.RUN_TERMINAL_DRAIN)
        self.assertEqual(decision.intent.action_id, "drain_net_long")
        self.assertEqual(decision.next_state.phase, RunPhase.TERMINAL_DRAIN)
        self.assertFalse(decision.stop_allowed)

    def test_nonclean_stop_claim_enters_exit_blocked_and_persists_retry(self) -> None:
        terminal = self._terminal_state()
        invalid_proof = self._clean_proof(owned_long_qty=0.1)

        blocked = decide_run_lifecycle(
            state=terminal,
            snapshot=self._snapshot(
                captured_at=END + timedelta(seconds=3),
                terminal_drain=TerminalDrainFeedback(
                    stop_allowed=True,
                    flat_proof=invalid_proof,
                ),
            ),
            policy=self._policy(),
        )

        self.assertEqual(blocked.action_id, LifecycleActionId.MARK_EXIT_BLOCKED)
        self.assertEqual(blocked.next_state.phase, RunPhase.EXIT_BLOCKED)
        self.assertEqual(blocked.next_state.exit_blocked_attempts, 1)
        self.assertFalse(blocked.stop_allowed)

        retry = decide_run_lifecycle(
            state=blocked.next_state,
            snapshot=self._snapshot(
                captured_at=END + timedelta(seconds=4),
                terminal_drain=TerminalDrainFeedback(blocked_reason="min_notional_dust"),
            ),
            policy=self._policy(),
        )
        self.assertEqual(retry.action_id, LifecycleActionId.RETRY_TERMINAL_DRAIN)
        self.assertEqual(retry.next_state.phase, RunPhase.EXIT_BLOCKED)
        self.assertEqual(retry.next_state.exit_blocked_attempts, 2)
        self.assertEqual(retry.next_state.outcome, RunOutcome.TARGET_UNMET_DEADLINE)
        self.assertFalse(retry.stop_allowed)

    def test_only_clean_fresh_reconciled_proof_can_stop_normally(self) -> None:
        terminal = self._terminal_state()

        stopped = decide_run_lifecycle(
            state=terminal,
            snapshot=self._snapshot(
                captured_at=END + timedelta(seconds=4),
                terminal_drain=TerminalDrainFeedback(
                    stop_allowed=True,
                    flat_proof=self._clean_proof(captured_at=END + timedelta(seconds=3)),
                ),
            ),
            policy=self._policy(),
        )

        self.assertEqual(stopped.action_id, LifecycleActionId.STOP_CLEAN)
        self.assertEqual(stopped.next_state.phase, RunPhase.STOPPED_CLEAN)
        self.assertTrue(stopped.stop_allowed)
        self.assertFalse(stopped.next_state.inventory_preserved)

    def test_stale_or_unreconciled_flat_proof_never_stops(self) -> None:
        terminal = self._terminal_state()
        bad_proofs = (
            self._clean_proof(captured_at=END - timedelta(seconds=30)),
            self._clean_proof(ledger_reconciled=False),
            self._clean_proof(trade_watermark_reconciled=False),
            self._clean_proof(open_managed_order_count=1),
            self._clean_proof(confirmation_count=1),
        )

        for proof in bad_proofs:
            with self.subTest(proof=proof):
                decision = decide_run_lifecycle(
                    state=terminal,
                    snapshot=self._snapshot(
                        captured_at=END + timedelta(seconds=4),
                        terminal_drain=TerminalDrainFeedback(stop_allowed=True, flat_proof=proof),
                    ),
                    policy=self._policy(),
                )
                self.assertEqual(decision.next_state.phase, RunPhase.EXIT_BLOCKED)
                self.assertFalse(decision.stop_allowed)

    def test_carrying_inventory_can_stop_only_via_explicit_stop_preserve(self) -> None:
        preserve = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(
                stop_preserve_requested=True,
                stop_preserve_reason="operator_requested_preserve",
            ),
            policy=self._policy(),
        )

        self.assertEqual(preserve.action_id, LifecycleActionId.STOP_PRESERVE)
        self.assertEqual(preserve.next_state.phase, RunPhase.STOPPED_PRESERVED)
        self.assertEqual(preserve.next_state.outcome, RunOutcome.STOP_PRESERVE)
        self.assertTrue(preserve.next_state.inventory_preserved)
        self.assertTrue(preserve.stop_allowed)

        implicit = decide_run_lifecycle(
            state=self._state(),
            snapshot=self._snapshot(stop_preserve_requested=False),
            policy=self._policy(),
        )
        self.assertNotEqual(implicit.action_id, LifecycleActionId.STOP_PRESERVE)
        self.assertNotIn(implicit.next_state.phase, {RunPhase.STOPPED_CLEAN, RunPhase.STOPPED_PRESERVED})


class FuturesRunContractTests(unittest.TestCase):
    def _validate(self, **overrides: object) -> RunContract:
        values: dict[str, object] = {
            "run_start_time": NOW,
            "runtime_guard_stats_start_time": NOW,
            "run_end_time": END,
            "target_value": 20_000.0,
            "exit_policy": "drain_then_preserve",
            "loss_budget": 5.0,
            "max_wait_seconds": 900.0,
            "preserve_reason": None,
        }
        values.update(overrides)
        return validate_run_contract(**values)

    def test_bounded_drain_then_preserve_returns_immutable_normalized_contract(self) -> None:
        contract = self._validate(run_end_time=END.isoformat(), target_value="20000")

        self.assertTrue(contract.bounded)
        self.assertEqual(contract.run_start_time, NOW)
        self.assertEqual(contract.runtime_guard_stats_start_time, NOW)
        self.assertEqual(contract.run_end_time, END)
        self.assertEqual(contract.target_value, 20_000.0)
        self.assertEqual(contract.exit_policy, "drain_then_preserve")
        self.assertEqual(contract.loss_budget, 5.0)
        self.assertEqual(contract.max_wait_seconds, 900.0)
        self.assertIsNone(contract.wear_stop_per_10k)
        self.assertIsNone(contract.wear_stop_min_gross_notional)
        with self.assertRaises(FrozenInstanceError):
            contract.loss_budget = 6.0  # type: ignore[misc]

    def test_wear_exit_terms_are_explicit_and_immutable(self) -> None:
        contract = self._validate(
            wear_stop_per_10k=2.0,
            wear_stop_min_gross_notional=75_000.0,
        )

        self.assertEqual(contract.wear_stop_per_10k, 2.0)
        self.assertEqual(contract.wear_stop_min_gross_notional, 75_000.0)

    def test_wear_exit_requires_a_complete_bounded_threshold_pair(self) -> None:
        invalid_cases = (
            ({"wear_stop_per_10k": 2.0}, "wear_stop_min_gross_notional is required"),
            (
                {"wear_stop_min_gross_notional": 75_000.0},
                "wear_stop_min_gross_notional requires wear_stop_per_10k",
            ),
            (
                {
                    "wear_stop_per_10k": 2.0,
                    "wear_stop_min_gross_notional": 0.0,
                },
                "wear_stop_min_gross_notional must be > 0",
            ),
        )
        for overrides, message in invalid_cases:
            with self.subTest(overrides=overrides), self.assertRaisesRegex(
                ValueError,
                message,
            ):
                self._validate(**overrides)

    def test_target_requires_an_explicit_deadline(self) -> None:
        with self.assertRaisesRegex(ValueError, "target_value requires run_end_time"):
            self._validate(run_end_time=None)

    def test_every_bounded_run_requires_an_explicit_exit_policy(self) -> None:
        for policy in (None, "", "   "):
            with self.subTest(policy=policy), self.assertRaisesRegex(
                ValueError,
                "bounded run requires exit_policy",
            ):
                self._validate(exit_policy=policy)

    def test_bounded_drain_clean_is_rejected_because_it_has_no_bounded_escape(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "bounded run exit_policy must be drain_then_preserve or stop_preserve",
        ):
            self._validate(exit_policy="drain_clean")

    def test_drain_then_preserve_requires_positive_explicit_budget_and_wait(self) -> None:
        invalid_cases = (
            (None, 900.0, "loss_budget is required"),
            (0.0, 900.0, "loss_budget must be > 0"),
            (5.0, None, "max_wait_seconds is required"),
            (5.0, 0.0, "max_wait_seconds must be > 0"),
        )
        for loss_budget, max_wait_seconds, message in invalid_cases:
            with self.subTest(
                loss_budget=loss_budget,
                max_wait_seconds=max_wait_seconds,
            ), self.assertRaisesRegex(ValueError, message):
                self._validate(
                    loss_budget=loss_budget,
                    max_wait_seconds=max_wait_seconds,
                )

    def test_stop_preserve_requires_explicit_nonnegative_budget_and_reason(self) -> None:
        contract = self._validate(
            exit_policy="stop_preserve",
            loss_budget=0.0,
            max_wait_seconds=None,
            preserve_reason="operator_keeps_inventory_for_validation",
        )
        self.assertEqual(contract.loss_budget, 0.0)
        self.assertIsNone(contract.max_wait_seconds)
        self.assertEqual(
            contract.preserve_reason,
            "operator_keeps_inventory_for_validation",
        )

        invalid_cases = (
            (None, "operator_keeps_inventory", "loss_budget is required"),
            (0.0, None, "preserve_reason is required"),
            (0.0, "   ", "preserve_reason is required"),
        )
        for loss_budget, reason, message in invalid_cases:
            with self.subTest(
                loss_budget=loss_budget,
                reason=reason,
            ), self.assertRaisesRegex(ValueError, message):
                self._validate(
                    exit_policy="stop_preserve",
                    loss_budget=loss_budget,
                    max_wait_seconds=None,
                    preserve_reason=reason,
                )

    def test_unbounded_run_can_leave_policy_unset(self) -> None:
        contract = validate_run_contract(
            run_start_time=None,
            runtime_guard_stats_start_time=None,
            run_end_time=None,
            target_value=None,
            exit_policy=None,
            loss_budget=None,
            max_wait_seconds=None,
            preserve_reason=None,
        )

        self.assertFalse(contract.bounded)
        self.assertIsNone(contract.exit_policy)

    def test_negative_nan_and_infinite_numeric_inputs_are_rejected(self) -> None:
        cases = (
            ("target_value", -1.0),
            ("target_value", float("nan")),
            ("target_value", float("inf")),
            ("loss_budget", -1.0),
            ("loss_budget", float("nan")),
            ("loss_budget", float("inf")),
            ("max_wait_seconds", -1.0),
            ("max_wait_seconds", float("nan")),
            ("max_wait_seconds", float("inf")),
            ("wear_stop_per_10k", -1.0),
            ("wear_stop_per_10k", float("nan")),
            ("wear_stop_per_10k", float("inf")),
            ("wear_stop_min_gross_notional", -1.0),
            ("wear_stop_min_gross_notional", float("nan")),
            ("wear_stop_min_gross_notional", float("inf")),
        )
        for field, value in cases:
            with self.subTest(field=field, value=value), self.assertRaisesRegex(
                ValueError,
                field,
            ):
                self._validate(**{field: value})

    def test_deadline_must_be_valid_and_timezone_aware(self) -> None:
        for deadline in ("not-a-time", datetime(2026, 7, 15, 9, 30)):
            with self.subTest(deadline=deadline), self.assertRaisesRegex(
                ValueError,
                "run_end_time",
            ):
                self._validate(run_end_time=deadline)


class FuturesRunContractOwnerTests(unittest.TestCase):
    def _config(self, **overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "symbol": "BCHUSDT",
            "strategy_profile": "bch-volume-v1",
            "strategy_mode": "best_quote_maker_volume",
            "run_start_time": NOW.isoformat(),
            "runtime_guard_stats_start_time": NOW.isoformat(),
            "run_end_time": END.isoformat(),
            "max_cumulative_notional": 20_000.0,
            "per_order_notional": 25.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 5.0,
            "terminal_drain_max_wait_seconds": 900.0,
            "terminal_drain_max_order_notional": 40.0,
        }
        values.update(overrides)
        return values

    def test_first_bounded_start_binds_one_canonical_snapshot_and_digest(self) -> None:
        prepared, changed = bind_run_contract_owner(
            self._config(),
            activated_at=NOW,
        )

        self.assertTrue(changed)
        owner = prepared[RUN_CONTRACT_OWNER_KEY]
        self.assertIsInstance(owner, dict)
        snapshot = owner["run_contract_snapshot"]  # type: ignore[index]
        self.assertEqual(snapshot, run_contract_snapshot_from_config(prepared))
        self.assertNotIn("per_order_notional", snapshot)
        self.assertEqual(owner["initial_per_order_notional"], 25.0)  # type: ignore[index]
        self.assertEqual(
            owner["run_contract_digest"],  # type: ignore[index]
            run_contract_snapshot_digest(snapshot),
        )
        self.assertEqual(owner["generation"], 1)  # type: ignore[index]
        validate_run_contract_owner(owner, expected_symbol="BCHUSDT")

    def test_active_owner_rejects_mutable_control_contract_changes_without_handoff(self) -> None:
        prepared, _ = bind_run_contract_owner(self._config(), activated_at=NOW)
        mutations = (
            {"run_start_time": (NOW + timedelta(minutes=1)).isoformat()},
            {"runtime_guard_stats_start_time": (NOW + timedelta(minutes=1)).isoformat()},
            {"run_end_time": (END + timedelta(minutes=1)).isoformat()},
            {"max_cumulative_notional": 25_000.0},
            {"terminal_drain_absolute_loss_budget": 7.0},
            {"terminal_drain_max_order_notional": 41.0},
            {
                "lifecycle_wear_stop_per_10k": 2.0,
                "lifecycle_wear_stop_min_gross_notional": 75_000.0,
            },
        )

        for mutation in mutations:
            candidate = dict(prepared)
            candidate.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                ValueError,
                "explicit run contract handoff is required",
            ):
                bind_run_contract_owner(candidate, activated_at=NOW)

    def test_per_order_can_change_after_fallback_terminal_cap_is_materialized(self) -> None:
        initial = self._config()
        initial.pop("terminal_drain_max_order_notional")
        prepared, _ = bind_run_contract_owner(initial, activated_at=NOW)
        owner = prepared[RUN_CONTRACT_OWNER_KEY]
        self.assertEqual(prepared["terminal_drain_max_order_notional"], 25.0)
        self.assertEqual(  # type: ignore[index]
            owner["run_contract_snapshot"]["terminal_drain_max_order_notional"],
            25.0,
        )

        resized = dict(prepared)
        resized["per_order_notional"] = 30.0
        rebound, changed = bind_run_contract_owner(resized, activated_at=NOW)

        self.assertFalse(changed)
        self.assertEqual(rebound[RUN_CONTRACT_OWNER_KEY], owner)
        snapshot, _ = resolve_authoritative_run_contract(rebound)
        self.assertEqual(snapshot["terminal_drain_max_order_notional"], 25.0)

    def test_explicit_handoff_replaces_owner_once_and_records_predecessor(self) -> None:
        prepared, _ = bind_run_contract_owner(self._config(), activated_at=NOW)
        previous = prepared[RUN_CONTRACT_OWNER_KEY]
        candidate = dict(prepared)
        candidate["run_end_time"] = (END + timedelta(hours=1)).isoformat()

        handed_off, changed = bind_run_contract_owner(
            candidate,
            activated_at=NOW + timedelta(minutes=5),
            handoff_reason="operator_started_next_bounded_run",
        )

        self.assertTrue(changed)
        current = handed_off[RUN_CONTRACT_OWNER_KEY]
        self.assertEqual(current["generation"], 2)  # type: ignore[index]
        self.assertEqual(  # type: ignore[index]
            current["handoff_from_contract_id"],
            previous["run_contract_id"],  # type: ignore[index]
        )
        self.assertEqual(  # type: ignore[index]
            current["handoff_reason"],
            "operator_started_next_bounded_run",
        )
        validate_run_contract_owner(current, expected_symbol="BCHUSDT")

        same, changed_again = bind_run_contract_owner(
            handed_off,
            activated_at=NOW + timedelta(minutes=6),
        )
        self.assertFalse(changed_again)
        self.assertEqual(same[RUN_CONTRACT_OWNER_KEY], current)

    def test_owner_snapshot_tamper_is_rejected_before_reuse(self) -> None:
        prepared, _ = bind_run_contract_owner(self._config(), activated_at=NOW)
        candidate = dict(prepared)
        owner = dict(candidate[RUN_CONTRACT_OWNER_KEY])  # type: ignore[arg-type]
        snapshot = dict(owner["run_contract_snapshot"])
        snapshot["terminal_drain_max_order_notional"] = 999.0
        owner["run_contract_snapshot"] = snapshot
        candidate[RUN_CONTRACT_OWNER_KEY] = owner

        with self.assertRaisesRegex(ValueError, "run contract owner digest mismatch"):
            bind_run_contract_owner(candidate, activated_at=NOW)

    def test_bounded_authoritative_resolver_requires_owner(self) -> None:
        with self.assertRaisesRegex(ValueError, "bounded run contract owner is missing"):
            resolve_authoritative_run_contract(
                self._config(),
                expected_symbol="BCHUSDT",
            )

    def test_authoritative_resolver_rejects_raw_contract_drift(self) -> None:
        prepared, _ = bind_run_contract_owner(self._config(), activated_at=NOW)
        prepared["max_cumulative_notional"] = 25_000.0

        with self.assertRaisesRegex(
            ValueError,
            "raw run contract does not match authoritative owner",
        ):
            resolve_authoritative_run_contract(
                prepared,
                expected_symbol="BCHUSDT",
            )

    def test_authoritative_resolver_returns_owner_snapshot_and_id(self) -> None:
        prepared, _ = bind_run_contract_owner(self._config(), activated_at=NOW)

        snapshot, contract_id = resolve_authoritative_run_contract(
            prepared,
            expected_symbol="BCHUSDT",
        )

        owner = prepared[RUN_CONTRACT_OWNER_KEY]
        self.assertEqual(snapshot, owner["run_contract_snapshot"])  # type: ignore[index]
        self.assertEqual(contract_id, owner["run_contract_id"])  # type: ignore[index]


if __name__ == "__main__":
    unittest.main()
