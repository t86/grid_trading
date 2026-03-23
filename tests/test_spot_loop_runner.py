from __future__ import annotations

import unittest
from unittest.mock import patch

from grid_optimizer.spot_loop_runner import _normalize_commission_quote


class SpotLoopRunnerTests(unittest.TestCase):
    def test_normalize_commission_quote_handles_quote_asset(self) -> None:
        fee = _normalize_commission_quote(
            commission=0.015,
            commission_asset="USDT",
            price=0.0269,
            base_asset="SAHARA",
            quote_asset="USDT",
        )
        self.assertAlmostEqual(fee, 0.015, places=10)

    def test_normalize_commission_quote_handles_base_asset(self) -> None:
        fee = _normalize_commission_quote(
            commission=1.5,
            commission_asset="SAHARA",
            price=0.0269,
            base_asset="SAHARA",
            quote_asset="USDT",
        )
        self.assertAlmostEqual(fee, 1.5 * 0.0269, places=10)

    @patch("grid_optimizer.spot_loop_runner.fetch_spot_latest_price")
    def test_normalize_commission_quote_converts_bnb_to_quote(self, mock_latest_price) -> None:
        mock_latest_price.return_value = 600.0
        fee = _normalize_commission_quote(
            commission=0.01,
            commission_asset="BNB",
            price=0.0269,
            base_asset="SAHARA",
            quote_asset="USDT",
        )
        self.assertAlmostEqual(fee, 6.0, places=10)


if __name__ == "__main__":
    unittest.main()
