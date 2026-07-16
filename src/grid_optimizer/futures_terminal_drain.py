"""Predictable maker-only terminal drain decisions for futures strategies.

The module is intentionally pure.  One symbol snapshot produces exactly one
top-level action.  Adapters may execute the returned cancel/order intents and
persist ``next_state``; they must not append another recovery action in the
same round.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum


class DrainActionId(str, Enum):
    BLOCK_ENTRY = "block_entry"
    CANCEL_ENTRY = "cancel_entry"
    CANCEL_DRAIN_ORDER = "cancel_drain_order"
    DRAIN_NET_LONG = "drain_net_long"
    DRAIN_NET_SHORT = "drain_net_short"
    DRAIN_MATCHED_PAIR = "drain_matched_pair"
    HOLD_ACTIVE_ORDER = "hold_active_order"
    HOLD_BLOCKED = "hold_blocked"
    DRAIN_DUST_BLOCKED = "drain_dust_blocked"
    VERIFY_FLAT = "verify_flat"
    STOP_CLEAN = "stop_clean"
    STOP_PRESERVE_FROZEN = "stop_preserve_frozen"


class DrainStage(str, Enum):
    BLOCK_ENTRY = "block_entry"
    DRAIN_NET = "drain_net"
    DRAIN_PAIR = "drain_pair"
    VERIFY_FLAT = "verify_flat"
    STOPPED_CLEAN = "stopped_clean"
    STOPPED_PRESERVED = "stopped_preserved"


@dataclass(frozen=True)
class DrainLossLease:
    action_id: DrainActionId
    maximum_loss: float
    expires_at: datetime


@dataclass(frozen=True)
class DrainOrder:
    symbol: str
    side: str
    position_side: str
    quantity: float
    price: float
    closes_position: bool = True
    reduce_only: bool | None = None
    order_type: str = "LIMIT"
    time_in_force: str = "GTX"
    post_only: bool = True


@dataclass(frozen=True)
class TerminalDrainPolicy:
    max_order_notional: float
    min_order_notional: float
    step_size: float
    tick_size: float
    absolute_loss_budget: float
    loss_lease_ttl: timedelta
    flat_confirm_cycles: int = 2
    active_order_timeout: timedelta = timedelta(minutes=2)

    def __post_init__(self) -> None:
        if self.max_order_notional <= 0:
            raise ValueError("max_order_notional must be positive")
        if self.min_order_notional <= 0:
            raise ValueError("min_order_notional must be positive")
        if self.max_order_notional < self.min_order_notional:
            raise ValueError("max_order_notional must cover min_order_notional")
        if self.step_size <= 0 or self.tick_size <= 0:
            raise ValueError("step_size and tick_size must be positive")
        if self.absolute_loss_budget < 0:
            raise ValueError("absolute_loss_budget must be non-negative")
        if self.loss_lease_ttl.total_seconds() <= 0:
            raise ValueError("loss_lease_ttl must be positive")
        if self.flat_confirm_cycles < 2:
            raise ValueError("flat_confirm_cycles must be at least two")
        if self.active_order_timeout.total_seconds() <= 0:
            raise ValueError("active_order_timeout must be positive")


@dataclass(frozen=True)
class TerminalDrainSnapshot:
    symbol: str
    captured_at: datetime
    bid_price: float
    ask_price: float
    actual_long_qty: float
    actual_short_qty: float
    owned_long_qty: float
    owned_short_qty: float
    frozen_long_qty: float
    frozen_short_qty: float
    long_cost_basis: float
    short_cost_basis: float
    entries_blocked: bool
    open_entry_order_ids: tuple[str, ...]
    open_drain_order_ids: tuple[str, ...]
    exchange_observation_available: bool
    ledger_reconciled: bool
    trade_watermark_reconciled: bool
    hedge_mode: bool = True
    open_drain_tail_below_min_notional: bool = False
    open_drain_oldest_created_at: datetime | None = None
    unmanaged_open_order_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class TerminalDrainState:
    symbol: str
    decision_id: str
    generation: int
    stage: DrainStage
    flat_confirmations: int
    last_flat_proof_at: datetime | None
    loss_used: float
    loss_reserved: float
    loss_lease: DrainLossLease | None
    loss_budget_breached: bool = False
    global_allow_loss: bool = False

    @classmethod
    def initial(cls, symbol: str, *, decision_id: str) -> "TerminalDrainState":
        normalized = str(symbol).upper().strip()
        if not normalized:
            raise ValueError("symbol is required")
        if not str(decision_id).strip():
            raise ValueError("decision_id is required")
        return cls(
            symbol=normalized,
            decision_id=str(decision_id),
            generation=0,
            stage=DrainStage.BLOCK_ENTRY,
            flat_confirmations=0,
            last_flat_proof_at=None,
            loss_used=0.0,
            loss_reserved=0.0,
            loss_lease=None,
            loss_budget_breached=False,
            global_allow_loss=False,
        )


def terminal_drain_state_to_dict(state: TerminalDrainState) -> dict[str, object]:
    lease = state.loss_lease
    return {
        "symbol": state.symbol,
        "decision_id": state.decision_id,
        "generation": state.generation,
        "stage": state.stage.value,
        "flat_confirmations": state.flat_confirmations,
        "last_flat_proof_at": state.last_flat_proof_at.isoformat() if state.last_flat_proof_at else None,
        "loss_used": state.loss_used,
        "loss_reserved": state.loss_reserved,
        "loss_budget_breached": state.loss_budget_breached,
        "loss_lease": (
            {
                "action_id": lease.action_id.value,
                "maximum_loss": lease.maximum_loss,
                "expires_at": lease.expires_at.isoformat(),
            }
            if lease is not None
            else None
        ),
        "global_allow_loss": False,
    }


def _parse_datetime(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO datetime") from exc


def terminal_drain_state_from_dict(payload: dict[str, object]) -> TerminalDrainState:
    raw_lease = payload.get("loss_lease")
    lease: DrainLossLease | None = None
    if raw_lease is not None:
        if not isinstance(raw_lease, dict):
            raise ValueError("loss_lease must be an object")
        lease = DrainLossLease(
            action_id=DrainActionId(str(raw_lease.get("action_id"))),
            maximum_loss=float(raw_lease.get("maximum_loss", 0.0)),
            expires_at=_parse_datetime(raw_lease.get("expires_at"), field="loss_lease.expires_at"),
        )
    last_flat_raw = payload.get("last_flat_proof_at")
    state = TerminalDrainState(
        symbol=str(payload.get("symbol") or "").upper().strip(),
        decision_id=str(payload.get("decision_id") or "").strip(),
        generation=int(payload.get("generation", 0)),
        stage=DrainStage(str(payload.get("stage"))),
        flat_confirmations=int(payload.get("flat_confirmations", 0)),
        last_flat_proof_at=(
            _parse_datetime(last_flat_raw, field="last_flat_proof_at")
            if last_flat_raw is not None
            else None
        ),
        loss_used=float(payload.get("loss_used", 0.0)),
        loss_reserved=float(payload.get("loss_reserved", 0.0)),
        loss_lease=lease,
        loss_budget_breached=bool(payload.get("loss_budget_breached", False)),
        global_allow_loss=bool(payload.get("global_allow_loss", False)),
    )
    if not state.symbol or not state.decision_id:
        raise ValueError("persisted terminal drain owner is incomplete")
    if state.generation < 0 or state.flat_confirmations < 0:
        raise ValueError("persisted terminal drain counters must be non-negative")
    if state.loss_used < 0 or state.loss_reserved < 0:
        raise ValueError("persisted terminal drain loss values must be non-negative")
    if state.global_allow_loss:
        raise ValueError("persisted terminal drain may not enable global allow_loss")
    if (state.loss_lease is None) != (state.loss_reserved <= 1e-12):
        raise ValueError("loss reservation must exactly match an active lease")
    return state


def settle_loss_lease(
    state: TerminalDrainState,
    *,
    action_id: DrainActionId,
    realized_loss: float,
) -> TerminalDrainState:
    """Commit one action receipt and atomically reclaim its scoped loss lease."""

    lease = state.loss_lease
    if lease is None:
        raise ValueError("no loss lease is active")
    if lease.action_id is not action_id:
        raise ValueError("loss receipt does not match the active action")
    safe_loss = float(realized_loss)
    if not math.isfinite(safe_loss) or safe_loss < 0:
        raise ValueError("realized_loss must be non-negative and finite")
    if safe_loss > lease.maximum_loss + 1e-9:
        raise ValueError("realized_loss exceeds the authorized lease")
    return replace(
        state,
        generation=state.generation + 1,
        loss_used=state.loss_used + safe_loss,
        loss_reserved=0.0,
        loss_lease=None,
        global_allow_loss=False,
    )


def settle_breached_loss_lease(
    state: TerminalDrainState,
    *,
    action_id: DrainActionId,
    realized_loss: float,
) -> TerminalDrainState:
    """Record the full overrun and revoke the lease without hiding the breach."""

    lease = state.loss_lease
    if lease is None:
        raise ValueError("no loss lease is active")
    if lease.action_id is not action_id:
        raise ValueError("loss receipt does not match the active action")
    safe_loss = float(realized_loss)
    if not math.isfinite(safe_loss) or safe_loss < 0:
        raise ValueError("realized_loss must be non-negative and finite")
    if safe_loss <= lease.maximum_loss + 1e-9:
        raise ValueError("realized_loss does not breach the authorized lease")
    return replace(
        state,
        generation=state.generation + 1,
        loss_used=state.loss_used + safe_loss,
        loss_reserved=0.0,
        loss_lease=None,
        loss_budget_breached=True,
        global_allow_loss=False,
    )


@dataclass(frozen=True)
class TerminalDrainPlan:
    action_id: DrainActionId
    next_state: TerminalDrainState
    orders: tuple[DrainOrder, ...] = ()
    cancel_order_ids: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    stop_allowed: bool = False
    preserve_inventory: bool = False
    min_order_notional: float = 0.0
    max_order_notional: float = 0.0
    step_size: float = 0.0

    def remaining_qty_is_legally_drainable(self, quantity: float, price: float) -> bool:
        safe_qty = max(float(quantity), 0.0)
        if safe_qty <= self.step_size / 2.0:
            return True
        if price <= 0 or self.step_size <= 0:
            return False
        q_min, q_max = _quantity_bounds(
            price=price,
            min_notional=self.min_order_notional,
            max_notional=self.max_order_notional,
            step_size=self.step_size,
        )
        return _legal_partition_count(safe_qty, q_min=q_min, q_max=q_max, step_size=self.step_size) is not None


def _ceil_step(value: float, step: float) -> float:
    return math.ceil((value / step) - 1e-12) * step


def _floor_step(value: float, step: float) -> float:
    return math.floor((value / step) + 1e-12) * step


def _quantity_bounds(*, price: float, min_notional: float, max_notional: float, step_size: float) -> tuple[float, float]:
    if price <= 0:
        return 0.0, 0.0
    return (
        _ceil_step(min_notional / price, step_size),
        _floor_step(max_notional / price, step_size),
    )


def _legal_partition_count(quantity: float, *, q_min: float, q_max: float, step_size: float) -> int | None:
    if quantity <= step_size / 2.0:
        return 0
    if q_min <= 0 or q_max < q_min:
        return None
    minimum_parts = max(int(math.ceil((quantity - step_size / 2.0) / q_max)), 1)
    maximum_parts = int(math.floor((quantity + step_size / 2.0) / q_min))
    for count in range(minimum_parts, maximum_parts + 1):
        if count * q_min <= quantity + step_size / 2.0 and count * q_max >= quantity - step_size / 2.0:
            return count
    return None


def _next_legal_chunk(
    quantity: float,
    *,
    price: float,
    policy: TerminalDrainPolicy,
) -> float | None:
    q_min, q_max = _quantity_bounds(
        price=price,
        min_notional=policy.min_order_notional,
        max_notional=policy.max_order_notional,
        step_size=policy.step_size,
    )
    count = _legal_partition_count(quantity, q_min=q_min, q_max=q_max, step_size=policy.step_size)
    if count in {None, 0}:
        return None
    lower = max(q_min, quantity - (count - 1) * q_max)
    upper = min(q_max, quantity - (count - 1) * q_min)
    if lower > upper + policy.step_size / 2.0:
        return None
    even = quantity / count
    candidate = _floor_step(min(max(even, lower), upper), policy.step_size)
    if candidate < lower - policy.step_size / 2.0:
        candidate = _ceil_step(lower, policy.step_size)
    if candidate < q_min - policy.step_size / 2.0 or candidate > q_max + policy.step_size / 2.0:
        return None
    remaining = max(quantity - candidate, 0.0)
    remaining_count = _legal_partition_count(
        remaining,
        q_min=q_min,
        q_max=q_max,
        step_size=policy.step_size,
    )
    if remaining_count is None:
        return None
    return round(candidate, 12)


def _next_pair_chunk(quantity: float, *, bid_price: float, ask_price: float, policy: TerminalDrainPolicy) -> float | None:
    long_min, long_max = _quantity_bounds(
        price=ask_price,
        min_notional=policy.min_order_notional,
        max_notional=policy.max_order_notional,
        step_size=policy.step_size,
    )
    short_min, short_max = _quantity_bounds(
        price=bid_price,
        min_notional=policy.min_order_notional,
        max_notional=policy.max_order_notional,
        step_size=policy.step_size,
    )
    q_min = max(long_min, short_min)
    q_max = min(long_max, short_max)
    count = _legal_partition_count(quantity, q_min=q_min, q_max=q_max, step_size=policy.step_size)
    if count in {None, 0}:
        return None
    lower = max(q_min, quantity - (count - 1) * q_max)
    upper = min(q_max, quantity - (count - 1) * q_min)
    candidate = _floor_step(min(max(quantity / count, lower), upper), policy.step_size)
    if candidate < lower - policy.step_size / 2.0:
        candidate = _ceil_step(lower, policy.step_size)
    if candidate < q_min - policy.step_size / 2.0 or candidate > q_max + policy.step_size / 2.0:
        return None
    return round(candidate, 12)


def _loss_for_order(*, side: str, quantity: float, price: float, cost_basis: float) -> float:
    if quantity <= 0 or price <= 0 or cost_basis <= 0:
        return 0.0
    if side == "SELL":
        return max(cost_basis - price, 0.0) * quantity
    return max(price - cost_basis, 0.0) * quantity


def _next_state(
    state: TerminalDrainState,
    *,
    stage: DrainStage | None = None,
    flat_confirmations: int | None = None,
    last_flat_proof_at: datetime | None | object = ...,
    loss_reserved: float | None = None,
    loss_lease: DrainLossLease | None | object = ...,
) -> TerminalDrainState:
    updates: dict[str, object] = {
        "generation": state.generation + 1,
        "global_allow_loss": False,
    }
    if stage is not None:
        updates["stage"] = stage
    if flat_confirmations is not None:
        updates["flat_confirmations"] = flat_confirmations
    if last_flat_proof_at is not ...:
        updates["last_flat_proof_at"] = last_flat_proof_at
    if loss_reserved is not None:
        updates["loss_reserved"] = loss_reserved
    if loss_lease is not ...:
        updates["loss_lease"] = loss_lease
    return replace(state, **updates)


def _reset_flat_proof(state: TerminalDrainState, **changes: object) -> TerminalDrainState:
    return _next_state(
        state,
        flat_confirmations=0,
        last_flat_proof_at=None,
        **changes,
    )


def _plan(
    *,
    state: TerminalDrainState,
    policy: TerminalDrainPolicy,
    action_id: DrainActionId,
    next_state: TerminalDrainState,
    orders: tuple[DrainOrder, ...] = (),
    cancel_order_ids: tuple[str, ...] = (),
    reasons: tuple[str, ...] = (),
    stop_allowed: bool = False,
    preserve_inventory: bool = False,
) -> TerminalDrainPlan:
    return TerminalDrainPlan(
        action_id=action_id,
        next_state=next_state,
        orders=orders,
        cancel_order_ids=cancel_order_ids,
        reasons=reasons,
        stop_allowed=stop_allowed,
        preserve_inventory=preserve_inventory,
        min_order_notional=policy.min_order_notional,
        max_order_notional=policy.max_order_notional,
        step_size=policy.step_size,
    )


def decide_terminal_drain(
    *,
    state: TerminalDrainState,
    snapshot: TerminalDrainSnapshot,
    policy: TerminalDrainPolicy,
) -> TerminalDrainPlan:
    """Return the only allowed terminal-drain action for this symbol round."""

    symbol = str(snapshot.symbol).upper().strip()
    if symbol != state.symbol:
        raise ValueError("snapshot symbol does not match drain state")
    if state.global_allow_loss:
        raise ValueError("terminal drain may not persist global allow_loss")

    if not snapshot.entries_blocked:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.BLOCK_ENTRY,
            next_state=_reset_flat_proof(state, stage=DrainStage.BLOCK_ENTRY),
            reasons=("entry_block_required",),
        )
    if not snapshot.exchange_observation_available:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("exchange_observation_unavailable",),
        )
    if snapshot.open_entry_order_ids:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.CANCEL_ENTRY,
            next_state=_reset_flat_proof(state, stage=DrainStage.BLOCK_ENTRY),
            cancel_order_ids=tuple(dict.fromkeys(snapshot.open_entry_order_ids)),
            reasons=("entry_orders_must_clear_before_drain",),
        )
    if snapshot.unmanaged_open_order_ids:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("unmanaged_orders_open",),
        )
    if snapshot.open_drain_order_ids:
        if (
            state.loss_lease is not None
            and snapshot.captured_at >= state.loss_lease.expires_at
        ):
            return _plan(
                state=state,
                policy=policy,
                action_id=DrainActionId.CANCEL_DRAIN_ORDER,
                next_state=_reset_flat_proof(state),
                cancel_order_ids=tuple(dict.fromkeys(snapshot.open_drain_order_ids)),
                reasons=("loss_lease_expired",),
            )
        oldest = snapshot.open_drain_oldest_created_at
        stale = (
            oldest is not None
            and snapshot.captured_at >= oldest
            and snapshot.captured_at - oldest >= policy.active_order_timeout
        )
        if stale and not snapshot.open_drain_tail_below_min_notional:
            return _plan(
                state=state,
                policy=policy,
                action_id=DrainActionId.CANCEL_DRAIN_ORDER,
                next_state=_reset_flat_proof(state),
                cancel_order_ids=tuple(dict.fromkeys(snapshot.open_drain_order_ids)),
                reasons=("reprice_stale_drain_order",),
            )
        reason = (
            "bounded_tail_wait"
            if snapshot.open_drain_tail_below_min_notional
            else "drain_order_in_flight"
        )
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_ACTIVE_ORDER,
            next_state=_reset_flat_proof(state),
            reasons=(reason,),
        )
    if state.loss_lease is not None or state.loss_reserved > 1e-12:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("loss_receipt_pending",),
        )
    if not snapshot.ledger_reconciled:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("ledger_not_reconciled",),
        )
    if not snapshot.trade_watermark_reconciled:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("trade_watermark_not_reconciled",),
        )

    dust = policy.step_size / 2.0
    raw_quantities = (
        snapshot.actual_long_qty,
        snapshot.actual_short_qty,
        snapshot.owned_long_qty,
        snapshot.owned_short_qty,
        snapshot.frozen_long_qty,
        snapshot.frozen_short_qty,
    )
    if any(not math.isfinite(float(value)) or float(value) < -dust for value in raw_quantities):
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("invalid_inventory_observation",),
        )
    actual_long = max(snapshot.actual_long_qty, 0.0)
    actual_short = max(snapshot.actual_short_qty, 0.0)
    owned_long = max(snapshot.owned_long_qty, 0.0)
    owned_short = max(snapshot.owned_short_qty, 0.0)
    frozen_long = max(snapshot.frozen_long_qty, 0.0)
    frozen_short = max(snapshot.frozen_short_qty, 0.0)
    if owned_long > actual_long + dust or owned_short > actual_short + dust:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("owned_inventory_exceeds_exchange",),
        )
    if frozen_long > owned_long + dust or frozen_short > owned_short + dust:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("frozen_inventory_exceeds_owned",),
        )
    clean_flat = (
        actual_long <= dust
        and actual_short <= dust
        and owned_long <= dust
        and owned_short <= dust
        and frozen_long <= dust
        and frozen_short <= dust
    )
    drainable_long = max(owned_long - frozen_long, 0.0)
    drainable_short = max(owned_short - frozen_short, 0.0)
    protected_only = (
        drainable_long <= dust
        and drainable_short <= dust
        and (frozen_long > dust or frozen_short > dust)
        and abs(actual_long - frozen_long) <= dust
        and abs(actual_short - frozen_short) <= dust
        and abs(owned_long - frozen_long) <= dust
        and abs(owned_short - frozen_short) <= dust
    )
    if clean_flat or protected_only:
        is_fresh = state.last_flat_proof_at is None or snapshot.captured_at > state.last_flat_proof_at
        confirmations = state.flat_confirmations + (1 if is_fresh else 0)
        if confirmations >= policy.flat_confirm_cycles:
            preserve_inventory = protected_only
            return _plan(
                state=state,
                policy=policy,
                action_id=(
                    DrainActionId.STOP_PRESERVE_FROZEN
                    if preserve_inventory
                    else DrainActionId.STOP_CLEAN
                ),
                next_state=_next_state(
                    state,
                    stage=(
                        DrainStage.STOPPED_PRESERVED
                        if preserve_inventory
                        else DrainStage.STOPPED_CLEAN
                    ),
                    flat_confirmations=confirmations,
                    last_flat_proof_at=snapshot.captured_at,
                    loss_reserved=0.0,
                    loss_lease=None,
                ),
                reasons=(
                    "frozen_inventory_preserved_by_ledger_boundary"
                    if preserve_inventory
                    else "reconciled_flat_proof",
                ),
                stop_allowed=True,
                preserve_inventory=preserve_inventory,
            )
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.VERIFY_FLAT,
            next_state=_next_state(
                state,
                stage=DrainStage.VERIFY_FLAT,
                flat_confirmations=confirmations,
                last_flat_proof_at=(snapshot.captured_at if is_fresh else state.last_flat_proof_at),
                loss_reserved=0.0,
                loss_lease=None,
            ),
            reasons=(
                (
                    "frozen_inventory_preserve_confirmation_pending"
                    if protected_only
                    else "flat_confirmation_pending"
                ,)
                if is_fresh
                else ("duplicate_flat_observation",)
            ),
        )

    if snapshot.bid_price <= 0 or snapshot.ask_price <= 0 or snapshot.bid_price >= snapshot.ask_price:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state),
            reasons=("invalid_or_crossed_book",),
        )

    sell_price = _ceil_step(snapshot.ask_price, policy.tick_size)
    buy_price = _floor_step(snapshot.bid_price, policy.tick_size)
    position_long = "LONG" if snapshot.hedge_mode else None
    position_short = "SHORT" if snapshot.hedge_mode else None
    api_reduce_only = None if snapshot.hedge_mode else True

    net_qty = drainable_long - drainable_short
    action_id: DrainActionId
    orders: tuple[DrainOrder, ...]
    estimated_loss: float
    if net_qty > dust:
        quantity = _next_legal_chunk(net_qty, price=sell_price, policy=policy)
        if quantity is None:
            action = (
                DrainActionId.DRAIN_DUST_BLOCKED
                if net_qty * sell_price < policy.min_order_notional - 1e-12
                else DrainActionId.HOLD_BLOCKED
            )
            return _plan(
                state=state,
                policy=policy,
                action_id=action,
                next_state=_reset_flat_proof(state, stage=DrainStage.DRAIN_NET),
                reasons=(
                    "sub_minimum_tail_without_live_order"
                    if action is DrainActionId.DRAIN_DUST_BLOCKED
                    else "illegal_net_long_remainder"
                ,),
            )
        if snapshot.long_cost_basis <= 0:
            return _plan(
                state=state,
                policy=policy,
                action_id=DrainActionId.HOLD_BLOCKED,
                next_state=_reset_flat_proof(state, stage=DrainStage.DRAIN_NET),
                reasons=("long_cost_basis_unavailable",),
            )
        action_id = DrainActionId.DRAIN_NET_LONG
        orders = (
            DrainOrder(
                symbol=symbol,
                side="SELL",
                position_side=position_long,
                quantity=quantity,
                price=sell_price,
                reduce_only=api_reduce_only,
            ),
        )
        estimated_loss = _loss_for_order(
            side="SELL",
            quantity=quantity,
            price=sell_price,
            cost_basis=snapshot.long_cost_basis,
        )
        stage = DrainStage.DRAIN_NET
    elif net_qty < -dust:
        quantity = _next_legal_chunk(-net_qty, price=buy_price, policy=policy)
        if quantity is None:
            action = (
                DrainActionId.DRAIN_DUST_BLOCKED
                if -net_qty * buy_price < policy.min_order_notional - 1e-12
                else DrainActionId.HOLD_BLOCKED
            )
            return _plan(
                state=state,
                policy=policy,
                action_id=action,
                next_state=_reset_flat_proof(state, stage=DrainStage.DRAIN_NET),
                reasons=(
                    "sub_minimum_tail_without_live_order"
                    if action is DrainActionId.DRAIN_DUST_BLOCKED
                    else "illegal_net_short_remainder"
                ,),
            )
        if snapshot.short_cost_basis <= 0:
            return _plan(
                state=state,
                policy=policy,
                action_id=DrainActionId.HOLD_BLOCKED,
                next_state=_reset_flat_proof(state, stage=DrainStage.DRAIN_NET),
                reasons=("short_cost_basis_unavailable",),
            )
        action_id = DrainActionId.DRAIN_NET_SHORT
        orders = (
            DrainOrder(
                symbol=symbol,
                side="BUY",
                position_side=position_short,
                quantity=quantity,
                price=buy_price,
                reduce_only=api_reduce_only,
            ),
        )
        estimated_loss = _loss_for_order(
            side="BUY",
            quantity=quantity,
            price=buy_price,
            cost_basis=snapshot.short_cost_basis,
        )
        stage = DrainStage.DRAIN_NET
    else:
        pair_qty = min(drainable_long, drainable_short)
        quantity = _next_pair_chunk(
            pair_qty,
            bid_price=buy_price,
            ask_price=sell_price,
            policy=policy,
        )
        if quantity is None:
            return _plan(
                state=state,
                policy=policy,
                action_id=DrainActionId.HOLD_BLOCKED,
                next_state=_reset_flat_proof(state, stage=DrainStage.DRAIN_PAIR),
                reasons=("illegal_matched_pair_remainder",),
            )
        missing_costs: list[str] = []
        if snapshot.long_cost_basis <= 0:
            missing_costs.append("long_cost_basis_unavailable")
        if snapshot.short_cost_basis <= 0:
            missing_costs.append("short_cost_basis_unavailable")
        if missing_costs:
            return _plan(
                state=state,
                policy=policy,
                action_id=DrainActionId.HOLD_BLOCKED,
                next_state=_reset_flat_proof(state, stage=DrainStage.DRAIN_PAIR),
                reasons=tuple(missing_costs),
            )
        action_id = DrainActionId.DRAIN_MATCHED_PAIR
        orders = (
            DrainOrder(
                symbol=symbol,
                side="SELL",
                position_side=position_long,
                quantity=quantity,
                price=sell_price,
                reduce_only=api_reduce_only,
            ),
            DrainOrder(
                symbol=symbol,
                side="BUY",
                position_side=position_short,
                quantity=quantity,
                price=buy_price,
                reduce_only=api_reduce_only,
            ),
        )
        estimated_loss = _loss_for_order(
            side="SELL",
            quantity=quantity,
            price=sell_price,
            cost_basis=snapshot.long_cost_basis,
        ) + _loss_for_order(
            side="BUY",
            quantity=quantity,
            price=buy_price,
            cost_basis=snapshot.short_cost_basis,
        )
        stage = DrainStage.DRAIN_PAIR

    if estimated_loss > 1e-12 and state.loss_budget_breached:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state, stage=stage),
            reasons=("loss_budget_breached",),
        )
    remaining_budget = max(policy.absolute_loss_budget - state.loss_used - state.loss_reserved, 0.0)
    if estimated_loss > remaining_budget + 1e-12:
        return _plan(
            state=state,
            policy=policy,
            action_id=DrainActionId.HOLD_BLOCKED,
            next_state=_reset_flat_proof(state, stage=stage),
            reasons=("loss_budget",),
        )
    lease = (
        DrainLossLease(
            action_id=action_id,
            maximum_loss=estimated_loss,
            expires_at=snapshot.captured_at + policy.loss_lease_ttl,
        )
        if estimated_loss > 0
        else None
    )
    return _plan(
        state=state,
        policy=policy,
        action_id=action_id,
        next_state=_next_state(
            state,
            stage=stage,
            flat_confirmations=0,
            last_flat_proof_at=None,
            loss_reserved=estimated_loss,
            loss_lease=lease,
        ),
        orders=orders,
        reasons=("terminal_inventory_drain",),
    )
