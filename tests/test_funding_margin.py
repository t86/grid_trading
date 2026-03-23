from __future__ import annotations

import unittest

from grid_optimizer.web import _funding_margin_snapshot, _infer_settle_asset


class FundingMarginTests(unittest.TestCase):
    def test_infer_settle_asset_for_coinm_symbol(self) -> None:
        self.assertEqual(_infer_settle_asset("ETHUSD_PERP", "coinm"), "ETH")
        self.assertEqual(_infer_settle_asset("BTCUSD_PERP", "coinm"), "BTC")
        self.assertEqual(_infer_settle_asset("ETHUSDT", "usdm"), "U")

    def test_long_margin_snapshot_uses_half_margin_and_lower_liq(self) -> None:
        snap = _funding_margin_snapshot(
            position_notional=10_000.0,
            reference_price=2_000.0,
            account_equity=10_000.0,
        )
        self.assertAlmostEqual(float(snap["minimum_margin"] or 0.0), 5_000.0, places=8)
        self.assertAlmostEqual(float(snap["liquidation_price"] or 0.0), 1_000.0, places=8)
        self.assertAlmostEqual(float(snap["withdrawable_amount"] or 0.0), 5_000.0, places=8)

    def test_short_margin_snapshot_uses_half_margin_and_upper_liq(self) -> None:
        snap = _funding_margin_snapshot(
            position_notional=-10_000.0,
            reference_price=2_000.0,
            account_equity=10_000.0,
        )
        self.assertAlmostEqual(float(snap["minimum_margin"] or 0.0), 5_000.0, places=8)
        self.assertAlmostEqual(float(snap["liquidation_price"] or 0.0), 3_000.0, places=8)
        self.assertAlmostEqual(float(snap["withdrawable_amount"] or 0.0), 5_000.0, places=8)

    def test_flat_position_has_no_liquidation_price(self) -> None:
        snap = _funding_margin_snapshot(
            position_notional=0.0,
            reference_price=2_000.0,
            account_equity=10_000.0,
        )
        self.assertAlmostEqual(float(snap["minimum_margin"] or 0.0), 0.0, places=8)
        self.assertIsNone(snap["liquidation_price"])
        self.assertAlmostEqual(float(snap["withdrawable_amount"] or 0.0), 10_000.0, places=8)

    def test_withdrawable_amount_tracks_equity_changes(self) -> None:
        # Same position notional, but higher equity should increase withdrawable amount.
        low_equity = _funding_margin_snapshot(
            position_notional=10_000.0,
            reference_price=2_000.0,
            account_equity=10_000.0,
        )
        high_equity = _funding_margin_snapshot(
            position_notional=10_000.0,
            reference_price=2_000.0,
            account_equity=13_000.0,
        )
        self.assertAlmostEqual(float(low_equity["withdrawable_amount"] or 0.0), 5_000.0, places=8)
        self.assertAlmostEqual(float(high_equity["withdrawable_amount"] or 0.0), 8_000.0, places=8)


if __name__ == "__main__":
    unittest.main()
