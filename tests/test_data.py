from __future__ import annotations

import unittest

from grid_optimizer.data import _filter_futures_symbols


class DataHelpersTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
