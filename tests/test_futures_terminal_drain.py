from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.futures_terminal_drain import (
    DrainActionId,
    TerminalDrainPolicy,
    TerminalDrainSnapshot,
    TerminalDrainState,
    decide_terminal_drain,
    settle_breached_loss_lease,
    settle_loss_lease,
    terminal_drain_state_from_dict,
    terminal_drain_state_to_dict,
)


NOW = datetime(2026, 7, 15, 10, 9, 15, tzinfo=timezone.utc)


class FuturesTerminalDrainTests(unittest.TestCase):
    def _policy(self, *, loss_budget: float = 5.0) -> TerminalDrainPolicy:
        return TerminalDrainPolicy(
            max_order_notional=22.0,
            min_order_notional=20.0,
            step_size=0.001,
            tick_size=0.01,
            absolute_loss_budget=loss_budget,
            loss_lease_ttl=timedelta(minutes=5),
            flat_confirm_cycles=2,
        )

    def _snapshot(self, **overrides: object) -> TerminalDrainSnapshot:
        values: dict[str, object] = {
            "symbol": "BCHUSDT",
            "captured_at": NOW,
            "bid_price": 220.37,
            "ask_price": 220.38,
            "actual_long_qty": 0.748,
            "actual_short_qty": 0.465,
            "owned_long_qty": 0.748,
            "owned_short_qty": 0.465,
            "frozen_long_qty": 0.283,
            "frozen_short_qty": 0.0,
            "long_cost_basis": 236.102158290782,
            "short_cost_basis": 235.572267,
            "entries_blocked": True,
            "open_entry_order_ids": (),
            "open_drain_order_ids": (),
            "exchange_observation_available": True,
            "ledger_reconciled": True,
            "trade_watermark_reconciled": True,
            "hedge_mode": True,
            "open_drain_tail_below_min_notional": False,
            "open_drain_oldest_created_at": None,
            "unmanaged_open_order_ids": (),
        }
        values.update(overrides)
        return TerminalDrainSnapshot(**values)

    def test_bch_net_long_uses_one_legal_gtx_reduce_action(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")

        plan = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(frozen_long_qty=0.0),
            policy=self._policy(),
        )

        self.assertEqual(plan.action_id, DrainActionId.DRAIN_NET_LONG)
        self.assertFalse(plan.stop_allowed)
        self.assertEqual(len(plan.orders), 1)
        order = plan.orders[0]
        self.assertEqual(order.side, "SELL")
        self.assertEqual(order.position_side, "LONG")
        self.assertTrue(order.closes_position)
        self.assertIsNone(order.reduce_only)
        self.assertTrue(order.post_only)
        self.assertEqual(order.order_type, "LIMIT")
        self.assertEqual(order.time_in_force, "GTX")
        self.assertGreaterEqual(order.quantity * order.price, 20.0)
        self.assertLessEqual(order.quantity * order.price, 22.0)
        remaining = 0.283 - order.quantity
        self.assertTrue(plan.remaining_qty_is_legally_drainable(remaining, order.price))
        self.assertIsNotNone(plan.next_state.loss_lease)
        self.assertEqual(plan.next_state.loss_lease.action_id, DrainActionId.DRAIN_NET_LONG)
        self.assertFalse(plan.next_state.global_allow_loss)

    def test_frozen_long_is_excluded_from_drainable_net_inventory(self) -> None:
        plan = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(),
            policy=self._policy(),
        )

        self.assertEqual(plan.action_id, DrainActionId.DRAIN_MATCHED_PAIR)
        self.assertEqual({order.position_side for order in plan.orders}, {"LONG", "SHORT"})
        self.assertTrue(all(order.quantity < 0.283 for order in plan.orders))

    def test_frozen_only_inventory_stops_preserved_after_two_fresh_proofs(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        frozen_only = self._snapshot(
            actual_long_qty=0.283,
            actual_short_qty=0.0,
            owned_long_qty=0.283,
            owned_short_qty=0.0,
            frozen_long_qty=0.283,
            frozen_short_qty=0.0,
        )

        verify = decide_terminal_drain(
            state=state,
            snapshot=frozen_only,
            policy=self._policy(),
        )
        preserved = decide_terminal_drain(
            state=verify.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=2),
                actual_long_qty=0.283,
                actual_short_qty=0.0,
                owned_long_qty=0.283,
                owned_short_qty=0.0,
                frozen_long_qty=0.283,
                frozen_short_qty=0.0,
            ),
            policy=self._policy(),
        )

        self.assertEqual(verify.action_id, DrainActionId.VERIFY_FLAT)
        self.assertEqual(preserved.action_id, DrainActionId.STOP_PRESERVE_FROZEN)
        self.assertTrue(preserved.stop_allowed)
        self.assertTrue(preserved.preserve_inventory)

    def test_normal_pair_drains_to_frozen_floor_without_consuming_frozen_ledger(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        long_qty = 0.748
        short_qty = 0.465
        total_sold = 0.0
        total_bought = 0.0

        for cycle in range(5):
            plan = decide_terminal_drain(
                state=state,
                snapshot=self._snapshot(
                    captured_at=NOW + timedelta(seconds=cycle),
                    actual_long_qty=long_qty,
                    actual_short_qty=short_qty,
                    owned_long_qty=long_qty,
                    owned_short_qty=short_qty,
                    frozen_long_qty=0.283,
                ),
                policy=self._policy(),
            )
            self.assertEqual(plan.action_id, DrainActionId.DRAIN_MATCHED_PAIR)
            sell = next(order for order in plan.orders if order.side == "SELL")
            buy = next(order for order in plan.orders if order.side == "BUY")
            self.assertAlmostEqual(sell.quantity, buy.quantity)
            long_qty -= sell.quantity
            short_qty -= buy.quantity
            total_sold += sell.quantity
            total_bought += buy.quantity
            state = settle_loss_lease(
                plan.next_state,
                action_id=plan.action_id,
                realized_loss=0.0,
            )

        self.assertAlmostEqual(total_sold, 0.465)
        self.assertAlmostEqual(total_bought, 0.465)
        self.assertAlmostEqual(long_qty, 0.283)
        self.assertAlmostEqual(short_qty, 0.0)
        verify = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=6),
                actual_long_qty=long_qty,
                actual_short_qty=short_qty,
                owned_long_qty=long_qty,
                owned_short_qty=short_qty,
                frozen_long_qty=0.283,
            ),
            policy=self._policy(),
        )
        stopped = decide_terminal_drain(
            state=verify.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=7),
                actual_long_qty=long_qty,
                actual_short_qty=short_qty,
                owned_long_qty=long_qty,
                owned_short_qty=short_qty,
                frozen_long_qty=0.283,
            ),
            policy=self._policy(),
        )
        self.assertEqual(verify.action_id, DrainActionId.VERIFY_FLAT)
        self.assertEqual(stopped.action_id, DrainActionId.STOP_PRESERVE_FROZEN)

    def test_loss_budget_block_keeps_nonzero_inventory_draining(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")

        plan = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(),
            policy=self._policy(loss_budget=1.0),
        )

        self.assertEqual(plan.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("loss_budget", plan.reasons)
        self.assertFalse(plan.stop_allowed)
        self.assertEqual(plan.orders, ())
        self.assertFalse(plan.next_state.global_allow_loss)

    def test_net_flat_but_gross_inventory_drains_one_matched_pair_action(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        snapshot = self._snapshot(
            actual_long_qty=0.465,
            actual_short_qty=0.465,
            owned_long_qty=0.465,
            owned_short_qty=0.465,
            frozen_long_qty=0.0,
        )

        plan = decide_terminal_drain(
            state=state,
            snapshot=snapshot,
            policy=self._policy(),
        )

        self.assertEqual(plan.action_id, DrainActionId.DRAIN_MATCHED_PAIR)
        self.assertEqual(len(plan.orders), 2)
        self.assertEqual({order.side for order in plan.orders}, {"BUY", "SELL"})
        self.assertEqual({order.position_side for order in plan.orders}, {"LONG", "SHORT"})
        for order in plan.orders:
            self.assertEqual(order.order_type, "LIMIT")
            self.assertEqual(order.time_in_force, "GTX")
            self.assertTrue(order.post_only)
            self.assertTrue(order.closes_position)
            self.assertIsNone(order.reduce_only)
            self.assertGreaterEqual(order.quantity * order.price, 20.0)
            self.assertLessEqual(order.quantity * order.price, 22.0)
        self.assertFalse(plan.stop_allowed)

    def test_entry_block_and_cancel_precede_any_drain_order(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")

        block = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(entries_blocked=False),
            policy=self._policy(),
        )
        self.assertEqual(block.action_id, DrainActionId.BLOCK_ENTRY)
        self.assertEqual(block.orders, ())

        cancel = decide_terminal_drain(
            state=block.next_state,
            snapshot=self._snapshot(open_entry_order_ids=("entry-1",)),
            policy=self._policy(),
        )
        self.assertEqual(cancel.action_id, DrainActionId.CANCEL_ENTRY)
        self.assertEqual(cancel.cancel_order_ids, ("entry-1",))
        self.assertEqual(cancel.orders, ())

    def test_stop_requires_two_fresh_flat_proofs_and_revokes_loss_lease(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        draining = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(frozen_long_qty=0.0),
            policy=self._policy(),
        )
        settled = settle_loss_lease(
            draining.next_state,
            action_id=DrainActionId.DRAIN_NET_LONG,
            realized_loss=1.4,
        )
        flat = self._snapshot(
            captured_at=NOW + timedelta(seconds=2),
            actual_long_qty=0.0,
            actual_short_qty=0.0,
            owned_long_qty=0.0,
            owned_short_qty=0.0,
            frozen_long_qty=0.0,
            open_drain_order_ids=(),
        )

        verify = decide_terminal_drain(
            state=settled,
            snapshot=flat,
            policy=self._policy(),
        )
        self.assertEqual(verify.action_id, DrainActionId.VERIFY_FLAT)
        self.assertFalse(verify.stop_allowed)
        self.assertIsNone(verify.next_state.loss_lease)
        self.assertFalse(verify.next_state.global_allow_loss)

        stop = decide_terminal_drain(
            state=verify.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=4),
                actual_long_qty=0.0,
                actual_short_qty=0.0,
                owned_long_qty=0.0,
                owned_short_qty=0.0,
                frozen_long_qty=0.0,
                open_drain_order_ids=(),
            ),
            policy=self._policy(),
        )
        self.assertEqual(stop.action_id, DrainActionId.STOP_CLEAN)
        self.assertTrue(stop.stop_allowed)

    def test_missing_exchange_or_ledger_proof_never_means_flat(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        for snapshot in (
            self._snapshot(exchange_observation_available=False),
            self._snapshot(ledger_reconciled=False),
            self._snapshot(trade_watermark_reconciled=False),
        ):
            with self.subTest(snapshot=snapshot):
                plan = decide_terminal_drain(
                    state=state,
                    snapshot=snapshot,
                    policy=self._policy(),
                )
                self.assertEqual(plan.action_id, DrainActionId.HOLD_BLOCKED)
                self.assertFalse(plan.stop_allowed)
                self.assertEqual(plan.orders, ())

    def test_unmanaged_open_order_blocks_clean_flat_proof(self) -> None:
        plan = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(
                actual_long_qty=0.0,
                actual_short_qty=0.0,
                owned_long_qty=0.0,
                owned_short_qty=0.0,
                frozen_long_qty=0.0,
                unmanaged_open_order_ids=("manual-late-order",),
            ),
            policy=self._policy(),
        )

        self.assertEqual(plan.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("unmanaged_orders_open", plan.reasons)
        self.assertFalse(plan.stop_allowed)

    def test_one_way_mode_uses_api_reduce_only_without_position_side(self) -> None:
        plan = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(
                hedge_mode=False,
                actual_long_qty=0.283,
                actual_short_qty=0.0,
                owned_long_qty=0.283,
                owned_short_qty=0.0,
                frozen_long_qty=0.0,
            ),
            policy=self._policy(),
        )

        self.assertEqual(len(plan.orders), 1)
        self.assertTrue(plan.orders[0].closes_position)
        self.assertTrue(plan.orders[0].reduce_only)
        self.assertIsNone(plan.orders[0].position_side)

    def test_unsettled_loss_lease_blocks_another_loss_authorization(self) -> None:
        policy = self._policy(loss_budget=2.0)
        first = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(frozen_long_qty=0.0),
            policy=policy,
        )

        pending_receipt = decide_terminal_drain(
            state=first.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=2),
                actual_long_qty=0.654,
                owned_long_qty=0.654,
                frozen_long_qty=0.189,
            ),
            policy=policy,
        )
        self.assertEqual(pending_receipt.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("loss_receipt_pending", pending_receipt.reasons)

        settled = settle_loss_lease(
            first.next_state,
            action_id=DrainActionId.DRAIN_NET_LONG,
            realized_loss=1.47,
        )
        second = decide_terminal_drain(
            state=settled,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=3),
                actual_long_qty=0.654,
                owned_long_qty=0.654,
                frozen_long_qty=0.189,
            ),
            policy=policy,
        )
        self.assertEqual(second.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("loss_budget", second.reasons)

    def test_budget_breach_closes_lease_and_forbids_another_loss_authorization(self) -> None:
        first = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(frozen_long_qty=0.0),
            policy=self._policy(loss_budget=5.0),
        )
        breached = settle_breached_loss_lease(
            first.next_state,
            action_id=DrainActionId.DRAIN_NET_LONG,
            realized_loss=10.0,
        )

        second = decide_terminal_drain(
            state=breached,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=2),
                actual_long_qty=0.654,
                owned_long_qty=0.654,
                frozen_long_qty=0.189,
            ),
            policy=self._policy(loss_budget=50.0),
        )

        self.assertIsNone(breached.loss_lease)
        self.assertEqual(breached.loss_reserved, 0.0)
        self.assertEqual(breached.loss_used, 10.0)
        self.assertTrue(breached.loss_budget_breached)
        self.assertFalse(breached.global_allow_loss)
        self.assertEqual(second.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("loss_budget_breached", second.reasons)

    def test_missing_cost_basis_cannot_bypass_zero_loss_budget(self) -> None:
        plan = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(long_cost_basis=0.0),
            policy=self._policy(loss_budget=0.0),
        )

        self.assertEqual(plan.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("long_cost_basis_unavailable", plan.reasons)
        self.assertEqual(plan.orders, ())

    def test_duplicate_flat_observation_does_not_count_twice(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        flat = self._snapshot(
            actual_long_qty=0.0,
            actual_short_qty=0.0,
            owned_long_qty=0.0,
            owned_short_qty=0.0,
            frozen_long_qty=0.0,
        )
        first = decide_terminal_drain(state=state, snapshot=flat, policy=self._policy())
        duplicate = decide_terminal_drain(
            state=first.next_state,
            snapshot=flat,
            policy=self._policy(),
        )

        self.assertEqual(first.action_id, DrainActionId.VERIFY_FLAT)
        self.assertEqual(duplicate.action_id, DrainActionId.VERIFY_FLAT)
        self.assertFalse(duplicate.stop_allowed)
        self.assertEqual(duplicate.next_state.flat_confirmations, 1)

    def test_failed_observation_resets_flat_confirmation_sequence(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        flat = self._snapshot(
            actual_long_qty=0.0,
            actual_short_qty=0.0,
            owned_long_qty=0.0,
            owned_short_qty=0.0,
            frozen_long_qty=0.0,
        )
        first = decide_terminal_drain(state=state, snapshot=flat, policy=self._policy())
        unavailable = decide_terminal_drain(
            state=first.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=1),
                exchange_observation_available=False,
            ),
            policy=self._policy(),
        )
        restarted = decide_terminal_drain(
            state=unavailable.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(seconds=2),
                actual_long_qty=0.0,
                actual_short_qty=0.0,
                owned_long_qty=0.0,
                owned_short_qty=0.0,
                frozen_long_qty=0.0,
            ),
            policy=self._policy(),
        )

        self.assertEqual(restarted.action_id, DrainActionId.VERIFY_FLAT)
        self.assertEqual(restarted.next_state.flat_confirmations, 1)

    def test_owned_inventory_mismatch_is_not_clamped_into_flat(self) -> None:
        plan = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(
                actual_long_qty=0.0,
                actual_short_qty=0.0,
                owned_long_qty=0.283,
                owned_short_qty=0.0,
                frozen_long_qty=0.283,
            ),
            policy=self._policy(),
        )

        self.assertEqual(plan.action_id, DrainActionId.HOLD_BLOCKED)
        self.assertIn("owned_inventory_exceeds_exchange", plan.reasons)
        self.assertFalse(plan.stop_allowed)

    def test_legal_partial_fill_tail_is_held_but_lost_tail_is_explicitly_blocked(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        active = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(
                open_drain_order_ids=("drain-1",),
                open_drain_tail_below_min_notional=True,
            ),
            policy=self._policy(),
        )
        self.assertEqual(active.action_id, DrainActionId.HOLD_ACTIVE_ORDER)
        self.assertIn("bounded_tail_wait", active.reasons)

        lost = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(
                actual_long_qty=0.55,
                actual_short_qty=0.465,
                owned_long_qty=0.55,
                owned_short_qty=0.465,
                frozen_long_qty=0.0,
            ),
            policy=self._policy(),
        )
        self.assertEqual(lost.action_id, DrainActionId.DRAIN_DUST_BLOCKED)
        self.assertIn("sub_minimum_tail_without_live_order", lost.reasons)

    def test_stale_legal_drain_order_is_one_cancel_reprice_action(self) -> None:
        plan = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(minutes=3),
                open_drain_order_ids=("drain-1",),
                open_drain_oldest_created_at=NOW,
            ),
            policy=self._policy(),
        )

        self.assertEqual(plan.action_id, DrainActionId.CANCEL_DRAIN_ORDER)
        self.assertEqual(plan.cancel_order_ids, ("drain-1",))
        self.assertEqual(plan.orders, ())
        self.assertIn("reprice_stale_drain_order", plan.reasons)

    def test_expired_loss_lease_cancels_even_a_partial_fill_tail(self) -> None:
        draining = decide_terminal_drain(
            state=TerminalDrainState.initial("BCHUSDT", decision_id="bch:end"),
            snapshot=self._snapshot(),
            policy=self._policy(),
        )
        self.assertIsNotNone(draining.next_state.loss_lease)

        expired = decide_terminal_drain(
            state=draining.next_state,
            snapshot=self._snapshot(
                captured_at=NOW + timedelta(minutes=5),
                open_drain_order_ids=("drain-1",),
                open_drain_tail_below_min_notional=True,
                open_drain_oldest_created_at=NOW,
            ),
            policy=self._policy(),
        )

        self.assertEqual(expired.action_id, DrainActionId.CANCEL_DRAIN_ORDER)
        self.assertEqual(expired.cancel_order_ids, ("drain-1",))
        self.assertIn("loss_lease_expired", expired.reasons)

    def test_state_round_trip_preserves_budget_and_flat_proof_watermark(self) -> None:
        state = TerminalDrainState.initial("BCHUSDT", decision_id="bch:end")
        first = decide_terminal_drain(
            state=state,
            snapshot=self._snapshot(
                actual_long_qty=0.0,
                actual_short_qty=0.0,
                owned_long_qty=0.0,
                owned_short_qty=0.0,
                frozen_long_qty=0.0,
            ),
            policy=self._policy(),
        ).next_state

        restored = terminal_drain_state_from_dict(terminal_drain_state_to_dict(first))

        self.assertEqual(restored, first)


if __name__ == "__main__":
    unittest.main()
