from __future__ import annotations

import math

import pytest

from grid_optimizer.futures_inventory_boundary import (
    durable_frozen_order_identities,
    is_durable_frozen_order_record,
    ordinary_position_qtys,
    strict_frozen_side_qtys,
)


def test_strict_frozen_side_qtys_returns_zero_for_missing_or_empty_ledger() -> None:
    assert strict_frozen_side_qtys(None) == (0.0, 0.0)
    assert strict_frozen_side_qtys({}) == (0.0, 0.0)


def test_durable_frozen_order_identities_require_state_evidence_not_prefix() -> None:
    state = {
        "best_quote_volume_order_refs": {
            "41": {
                "book": "frozen_bq",
                "client_order_id": "gx-bchu-frozen-owned",
            },
            "42": {
                "book": "normal_bq",
                "client_order_id": "gx-bchu-frozen-prefix-only",
            },
            "pending:gx-bchu-frozen-pending": {
                "role": "frozen_inventory_pair_release_long",
                "client_order_id": "gx-bchu-frozen-pending",
            },
        },
        "best_quote_frozen_inventory_pair_release": {
            "submission_manifest": {
                "legs": {
                    "short": {
                        "order_id": "43",
                        "client_order_id": "gx-bchu-frozen-manifest",
                    }
                }
            }
        },
    }

    order_ids, client_order_ids = durable_frozen_order_identities(state)

    assert order_ids == {"41", "43"}
    assert client_order_ids == {
        "gx-bchu-frozen-owned",
        "gx-bchu-frozen-pending",
        "gx-bchu-frozen-manifest",
    }
    assert "42" not in order_ids
    assert "gx-bchu-frozen-prefix-only" not in client_order_ids


def test_durable_frozen_order_record_uses_metadata_only() -> None:
    assert is_durable_frozen_order_record({"book": "frozen_bq"}) is True
    assert is_durable_frozen_order_record(
        {"role": "frozen_inventory_manual_reduce_long"}
    ) is True
    assert is_durable_frozen_order_record(
        {"client_order_id": "gx-bchu-frozen-looking"}
    ) is False


def test_strict_frozen_side_qtys_loads_summary_only() -> None:
    assert strict_frozen_side_qtys(
        {"long_qty": "1.25", "short_qty": 0.5}
    ) == (1.25, 0.5)


def test_strict_frozen_side_qtys_loads_and_sums_lots_only() -> None:
    assert strict_frozen_side_qtys(
        {
            "long_lots": [{"qty": "0.1"}, {"qty": 0.2}],
            "short_lots": [{"qty": 0.4}],
        }
    ) == pytest.approx((0.3, 0.4))


def test_strict_frozen_side_qtys_accepts_consistent_summary_and_lots() -> None:
    assert strict_frozen_side_qtys(
        {
            "long_qty": 0.3,
            "long_lots": [{"qty": 0.1}, {"qty": 0.2}],
            "short_qty": 0.4,
            "short_lots": [{"qty": 0.4}],
        }
    ) == pytest.approx((0.3, 0.4))


def test_strict_frozen_side_qtys_rejects_summary_lot_mismatch() -> None:
    with pytest.raises(ValueError, match="long frozen summary does not match lots"):
        strict_frozen_side_qtys(
            {
                "long_qty": 0.4,
                "long_lots": [{"qty": 0.1}, {"qty": 0.2}],
            }
        )


@pytest.mark.parametrize(
    "value",
    [None, "", "not-a-number", -0.1, math.inf, -math.inf, math.nan, True],
)
def test_strict_frozen_side_qtys_rejects_invalid_summary(value: object) -> None:
    with pytest.raises(ValueError, match="long frozen summary"):
        strict_frozen_side_qtys({"long_qty": value})


@pytest.mark.parametrize("lots", [None, {}, "bad", 1.0])
def test_strict_frozen_side_qtys_rejects_invalid_lots_container(lots: object) -> None:
    with pytest.raises(ValueError, match="long frozen lots must be a list"):
        strict_frozen_side_qtys({"long_lots": lots})


@pytest.mark.parametrize(
    "row",
    [None, "bad", {}, {"qty": None}, {"qty": -0.1}, {"qty": math.inf}, {"qty": True}],
)
def test_strict_frozen_side_qtys_rejects_bad_lot_rows(row: object) -> None:
    with pytest.raises(ValueError, match="long frozen lot"):
        strict_frozen_side_qtys({"long_lots": [row]})


def test_strict_frozen_side_qtys_rejects_non_finite_lot_sum() -> None:
    with pytest.raises(ValueError, match="long frozen lot total"):
        strict_frozen_side_qtys(
            {"long_lots": [{"qty": 1e308}, {"qty": 1e308}]}
        )


def test_strict_frozen_side_qtys_rejects_non_mapping_ledger() -> None:
    with pytest.raises(ValueError, match="frozen inventory ledger must be a mapping"):
        strict_frozen_side_qtys([])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field",
    (
        "exchange_long_qty",
        "exchange_short_qty",
        "frozen_long_qty",
        "frozen_short_qty",
    ),
)
def test_ordinary_position_qtys_rejects_boolean_numeric_fields(field: str) -> None:
    values: dict[str, object] = {
        "exchange_long_qty": 1.0,
        "exchange_short_qty": 1.0,
        "frozen_long_qty": 0.0,
        "frozen_short_qty": 0.0,
    }
    values[field] = True
    with pytest.raises(ValueError, match="inventory boundary value must be numeric"):
        ordinary_position_qtys(**values)
