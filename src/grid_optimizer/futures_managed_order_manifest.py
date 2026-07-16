"""Stable client-order identities for coordinator-owned futures orders."""

from __future__ import annotations

import hashlib
import json
import re
import time

from .futures_recovery_coordinator import (
    ManagedOrderIdentity,
    ManagedOrderManifest,
)


FUTURES_CLIENT_ORDER_ID_MAX_LENGTH = 36
MANAGED_ORDER_NONCE_MIN_LENGTH = 6
_ORDINARY_ACTION_IDS = frozenset(
    {"inventory_recover", "maker_flow_recover", "baseline_tune"}
)
_ORDER_ROLES = frozenset({"entry", "reduce_only"})
_SIDES = frozenset({"BUY", "SELL"})
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+$")
_PROFILE_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_ORDINARY_PREFIX_RE = re.compile(r"^gx-[a-z0-9]+-rc-[0-9a-f]{12}-$")


def ordinary_recovery_client_order_prefix(
    *,
    symbol: str,
    generation: int,
    decision_id: str,
    profile_digest: str,
    action_id: str,
    side: str,
    order_role: str,
) -> str:
    """Return the exact prefix for one ordinary recovery decision.

    The digest binds every control field needed to distinguish a surviving
    order from orders issued by another decision or generation.  Callers must
    never weaken this to the symbol-wide ``gx-<symbol>-`` prefix.
    """

    normalized_symbol = str(symbol).upper().strip()
    normalized_decision = str(decision_id).strip()
    normalized_profile = str(profile_digest).strip().lower()
    normalized_action = str(action_id).strip().lower()
    normalized_side = str(side).strip().upper()
    normalized_role = str(order_role).strip().lower()
    if not (
        _SYMBOL_RE.fullmatch(normalized_symbol)
        and type(generation) is int
        and generation >= 0
        and normalized_decision
        and normalized_decision == decision_id
        and _PROFILE_DIGEST_RE.fullmatch(normalized_profile)
        and normalized_action in _ORDINARY_ACTION_IDS
        and normalized_side in _SIDES
        and normalized_role in _ORDER_ROLES
        and not (
            normalized_action == "inventory_recover"
            and normalized_role != "reduce_only"
        )
        and not (
            normalized_action == "maker_flow_recover"
            and normalized_role != "entry"
        )
    ):
        raise ValueError("ordinary recovery client id binding is invalid")
    binding = {
        "symbol": normalized_symbol,
        "generation": generation,
        "decision_id": normalized_decision,
        "profile_digest": normalized_profile,
        "action_id": normalized_action,
        "side": normalized_side,
        "order_role": normalized_role,
    }
    digest = hashlib.sha256(
        json.dumps(
            binding,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:12]
    symbol_token = normalized_symbol.lower().replace("usdt", "u")
    prefix = f"gx-{symbol_token}-rc-{digest}-"
    if len(prefix) > (
        FUTURES_CLIENT_ORDER_ID_MAX_LENGTH - MANAGED_ORDER_NONCE_MIN_LENGTH
    ):
        raise ValueError(
            "symbol is too long for a reconstructable recovery client id"
        )
    return prefix


def build_managed_order_client_order_id(
    *,
    prefix: str,
    order_index: int,
    nonce_ns: int | None = None,
) -> str:
    """Append a per-submit nonce without losing the reconstructable prefix."""

    normalized_prefix = str(prefix).strip()
    if not (
        normalized_prefix == prefix
        and _ORDINARY_PREFIX_RE.fullmatch(normalized_prefix)
        and len(normalized_prefix)
        <= FUTURES_CLIENT_ORDER_ID_MAX_LENGTH - MANAGED_ORDER_NONCE_MIN_LENGTH
        and type(order_index) is int
        and order_index >= 1
    ):
        raise ValueError("managed futures client order id prefix is invalid")
    suffix_length = FUTURES_CLIENT_ORDER_ID_MAX_LENGTH - len(normalized_prefix)
    nonce = time.time_ns() if nonce_ns is None else int(nonce_ns)
    suffix = hashlib.sha256(
        f"{order_index}:{nonce}".encode("utf-8")
    ).hexdigest()[:suffix_length]
    return normalized_prefix + suffix
