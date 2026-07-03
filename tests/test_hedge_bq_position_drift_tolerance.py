from __future__ import annotations

import argparse
import unittest

from grid_optimizer.loop_runner import hedge_bq_position_drift_tolerance_qty

HEDGE_BQ = "hedge_best_quote_maker_volume_v1"


def _args(tolerance: float | None = None) -> argparse.Namespace:
    ns = argparse.Namespace()
    if tolerance is not None:
        ns.hedge_bq_position_drift_tolerance_notional = tolerance
    return ns


class HedgeBqPositionDriftToleranceTest(unittest.TestCase):
    def test_default_zero_keeps_strict_behavior(self) -> None:
        self.assertEqual(
            hedge_bq_position_drift_tolerance_qty(
                args=_args(), strategy_mode=HEDGE_BQ, mid_price=0.55
            ),
            0.0,
        )
        self.assertEqual(
            hedge_bq_position_drift_tolerance_qty(
                args=_args(0.0), strategy_mode=HEDGE_BQ, mid_price=0.55
            ),
            0.0,
        )

    def test_notional_converts_to_qty_at_mid(self) -> None:
        qty = hedge_bq_position_drift_tolerance_qty(
            args=_args(40.0), strategy_mode=HEDGE_BQ, mid_price=0.5
        )
        self.assertAlmostEqual(qty, 80.0)

    def test_other_modes_stay_strict(self) -> None:
        for mode in ("one_way_long", "synthetic_neutral", "competition_inventory_grid_v1"):
            self.assertEqual(
                hedge_bq_position_drift_tolerance_qty(
                    args=_args(40.0), strategy_mode=mode, mid_price=0.5
                ),
                0.0,
            )

    def test_invalid_mid_or_negative_tolerance_zero(self) -> None:
        self.assertEqual(
            hedge_bq_position_drift_tolerance_qty(
                args=_args(40.0), strategy_mode=HEDGE_BQ, mid_price=0.0
            ),
            0.0,
        )
        self.assertEqual(
            hedge_bq_position_drift_tolerance_qty(
                args=_args(-5.0), strategy_mode=HEDGE_BQ, mid_price=0.5
            ),
            0.0,
        )


if __name__ == "__main__":
    unittest.main()
