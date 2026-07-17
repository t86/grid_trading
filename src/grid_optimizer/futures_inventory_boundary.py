from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


def _non_negative_number(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, bool):
        raise ValueError("inventory boundary value must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("inventory boundary value must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError("inventory boundary value must be finite and non-negative")
    return parsed


def _strict_non_negative_number(value: Any, *, label: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError(f"{label} must be finite and non-negative")
    return parsed


def strict_frozen_side_qtys(
    ledger: Mapping[str, Any] | None,
) -> tuple[float, float]:
    """Load frozen LONG/SHORT quantities without repairing inconsistent data.

    A missing ledger or a side absent from an otherwise valid ledger represents
    zero frozen quantity.  When a side contains both an aggregate ``*_qty`` and
    ``*_lots``, their quantities must agree; malformed values fail closed.
    """

    if ledger is None:
        return 0.0, 0.0
    if not isinstance(ledger, Mapping):
        raise ValueError("frozen inventory ledger must be a mapping")

    def _side_qty(side: str) -> float:
        summary_key = f"{side}_qty"
        lots_key = f"{side}_lots"
        has_summary = summary_key in ledger
        has_lots = lots_key in ledger

        summary_qty = (
            _strict_non_negative_number(
                ledger[summary_key],
                label=f"{side} frozen summary",
            )
            if has_summary
            else None
        )
        lots_qty: float | None = None
        if has_lots:
            raw_lots = ledger[lots_key]
            if not isinstance(raw_lots, list):
                raise ValueError(f"{side} frozen lots must be a list")
            lots_qty = 0.0
            for index, row in enumerate(raw_lots):
                if not isinstance(row, Mapping) or "qty" not in row:
                    raise ValueError(
                        f"{side} frozen lot {index} must be a mapping with qty"
                    )
                lots_qty += _strict_non_negative_number(
                    row["qty"],
                    label=f"{side} frozen lot {index} qty",
                )
                if not math.isfinite(lots_qty):
                    raise ValueError(
                        f"{side} frozen lot total must be finite and non-negative"
                    )

        if (
            summary_qty is not None
            and lots_qty is not None
            and not math.isclose(
                summary_qty,
                lots_qty,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        ):
            raise ValueError(f"{side} frozen summary does not match lots")
        if summary_qty is not None:
            return summary_qty
        if lots_qty is not None:
            return lots_qty
        return 0.0

    return _side_qty("long"), _side_qty("short")


def ordinary_position_qtys(
    *,
    exchange_long_qty: Any,
    exchange_short_qty: Any,
    frozen_long_qty: Any = 0.0,
    frozen_short_qty: Any = 0.0,
) -> tuple[float, float]:
    """Return ordinary side quantities after removing the frozen ledger."""

    exchange_long = _non_negative_number(exchange_long_qty)
    exchange_short = _non_negative_number(exchange_short_qty)
    frozen_long = _non_negative_number(frozen_long_qty)
    frozen_short = _non_negative_number(frozen_short_qty)
    if (
        frozen_long > exchange_long + 1e-12
        or frozen_short > exchange_short + 1e-12
    ):
        raise ValueError("frozen inventory exceeds exchange position")
    return exchange_long - frozen_long, exchange_short - frozen_short


def ordinary_side_notionals(
    *,
    exchange_long_notional: Any,
    exchange_short_notional: Any,
    frozen_long_notional: Any = 0.0,
    frozen_short_notional: Any = 0.0,
) -> tuple[float, float]:
    """Return ordinary side notionals after removing the frozen ledger."""

    return ordinary_position_qtys(
        exchange_long_qty=exchange_long_notional,
        exchange_short_qty=exchange_short_notional,
        frozen_long_qty=frozen_long_notional,
        frozen_short_qty=frozen_short_notional,
    )


def is_durable_frozen_order_record(raw: Any) -> bool:
    """Return whether a durable state record belongs to the frozen lane.

    Exchange client-order prefixes are deliberately not ownership evidence.
    Only persisted ledger metadata can keep an order outside ordinary recovery.
    """

    record = raw if isinstance(raw, Mapping) else {}
    role = str(record.get("role") or "").lower().strip()
    book = str(record.get("book") or record.get("ledger_class") or "").lower().strip()
    return bool(
        book == "frozen_bq"
        or role.startswith("frozen_inventory_")
        or record.get("manual_frozen_inventory_reduce")
        or record.get("manual_frozen_inventory_limit")
        or record.get("frozen_inventory_pair_release")
        or record.get("frozen_inventory_per_lot_release")
    )


def durable_frozen_order_identities(
    state: Mapping[str, Any] | None,
) -> tuple[set[str], set[str]]:
    """Return exact order/client IDs durably owned by frozen inventory.

    The pair submission manifest is included because it is written before the
    first exchange call.  A crash between submit and order-ref persistence must
    not let an older repair/cancel path reinterpret that leg as ordinary.
    """

    if not isinstance(state, Mapping):
        return set(), set()

    order_ids: set[str] = set()
    client_order_ids: set[str] = set()
    refs = state.get("best_quote_volume_order_refs")
    if isinstance(refs, Mapping):
        for ref_key, raw_ref in refs.items():
            if not is_durable_frozen_order_record(raw_ref):
                continue
            ref = raw_ref if isinstance(raw_ref, Mapping) else {}
            order_id = str(ref.get("order_id") or ref.get("orderId") or "").strip()
            if not order_id and not str(ref_key).startswith("pending:"):
                order_id = str(ref_key).strip()
            client_order_id = str(
                ref.get("client_order_id") or ref.get("clientOrderId") or ""
            ).strip()
            if order_id:
                order_ids.add(order_id)
            if client_order_id:
                client_order_ids.add(client_order_id)

    pair_directive = state.get("best_quote_frozen_inventory_pair_release")
    manifest = (
        pair_directive.get("submission_manifest")
        if isinstance(pair_directive, Mapping)
        else None
    )
    legs = manifest.get("legs") if isinstance(manifest, Mapping) else None
    if isinstance(legs, Mapping):
        for raw_leg in legs.values():
            if not isinstance(raw_leg, Mapping):
                continue
            order_id = str(raw_leg.get("order_id") or raw_leg.get("orderId") or "").strip()
            client_order_id = str(
                raw_leg.get("client_order_id") or raw_leg.get("clientOrderId") or ""
            ).strip()
            if order_id:
                order_ids.add(order_id)
            if client_order_id:
                client_order_ids.add(client_order_id)
    return order_ids, client_order_ids
