from __future__ import annotations

import unittest

from grid_optimizer.data import (
    _filter_futures_symbols,
    _format_request_number,
    _merge_futures_position_risk_into_account_info,
)


class DataHelpersTests(unittest.TestCase):
    def test_format_request_number_avoids_float_artifacts(self) -> None:
        self.assertEqual(_format_request_number(78078.6), "78078.6")
        self.assertEqual(_format_request_number(78078.7), "78078.7")
        self.assertEqual(_format_request_number(78000.1), "78000.1")

    def test_filter_futures_symbols_perpetual_usdt_trading(self) -> None:
        exchange_info = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "ETHUSDT",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "BTCUSDT_260327",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "CURRENT_QUARTER",
                },
                {
                    "symbol": "BNBBUSD",
                    "status": "TRADING",
                    "quoteAsset": "BUSD",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "XRPUSDT",
                    "status": "PENDING_TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                },
            ]
        }
        symbols = _filter_futures_symbols(
            exchange_info=exchange_info,
            quote_asset="USDT",
            contract_type="PERPETUAL",
            only_trading=True,
        )
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])

    def test_filter_futures_symbols_without_contract_filter(self) -> None:
        exchange_info = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "BTCUSDT_260327",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "CURRENT_QUARTER",
                },
            ]
        }
        symbols = _filter_futures_symbols(
            exchange_info=exchange_info,
            quote_asset="USDT",
            contract_type=None,
            only_trading=True,
        )
        self.assertEqual(symbols, ["BTCUSDT", "BTCUSDT_260327"])

    def test_merge_futures_position_risk_restores_cost_basis_fields(self) -> None:
        account_info = {
            "positions": [
                {
                    "symbol": "SOONUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "1852",
                }
            ]
        }
        merged = _merge_futures_position_risk_into_account_info(
            account_info,
            [
                {
                    "symbol": "SOONUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "1852",
                    "entryPrice": "0.1620",
                    "breakEvenPrice": "0.1621",
                    "unRealizedProfit": "-0.10",
                }
            ],
        )

        position = merged["positions"][0]
        self.assertEqual(position["entryPrice"], "0.1620")
        self.assertEqual(position["breakEvenPrice"], "0.1621")
        self.assertEqual(position["unRealizedProfit"], "-0.10")


if __name__ == "__main__":
    unittest.main()
