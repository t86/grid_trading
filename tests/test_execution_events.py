from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from grid_optimizer.execution_events import (
    AccountPositionStore,
    ExecutionEventStore,
    FuturesListenKeyClient,
    FuturesUserDataStream,
    MarketTick,
    OpenOrderStateStore,
    detect_crossed_grid_levels,
    normalize_order_trade_update,
)


class ExecutionEventStoreTests(unittest.TestCase):
    def test_order_trade_update_normalizes_filled_event(self) -> None:
        event = normalize_order_trade_update(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1000,
                "T": 990,
                "o": {
                    "s": "CHIPUSDT",
                    "c": "grid_1",
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "GTX",
                    "q": "10",
                    "p": "0.0671",
                    "ap": "0.0671",
                    "x": "TRADE",
                    "X": "FILLED",
                    "i": 12345,
                    "l": "10",
                    "z": "10",
                    "L": "0.0671",
                    "n": "0.001",
                    "N": "USDT",
                    "ps": "BOTH",
                    "rp": "0.12",
                },
            }
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.kind, "ORDER_FILLED")
        self.assertEqual(event.symbol, "CHIPUSDT")
        self.assertEqual(event.side, "SELL")
        self.assertEqual(event.order_id, 12345)
        self.assertEqual(event.client_order_id, "grid_1")
        self.assertEqual(event.execution_type, "TRADE")
        self.assertEqual(event.order_status, "FILLED")
        self.assertEqual(event.last_filled_qty, 10.0)
        self.assertEqual(event.cumulative_filled_qty, 10.0)
        self.assertEqual(event.last_filled_price, 0.0671)
        self.assertEqual(event.realized_pnl, 0.12)

    def test_event_store_deduplicates_order_updates_by_stable_key(self) -> None:
        store = ExecutionEventStore(max_events=10)
        event = normalize_order_trade_update(
            {
                "E": 1000,
                "T": 990,
                "o": {
                    "s": "CHIPUSDT",
                    "c": "grid_1",
                    "S": "BUY",
                    "x": "NEW",
                    "X": "NEW",
                    "i": 99,
                    "l": "0",
                    "z": "0",
                },
            }
        )
        assert event is not None

        self.assertTrue(store.add(event))
        self.assertFalse(store.add(event))
        self.assertEqual(len(store.snapshot()), 1)


class AccountPositionStoreTests(unittest.TestCase):
    def test_account_update_tracks_latest_positions(self) -> None:
        store = AccountPositionStore()

        changed = store.update_from_account_update(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1000,
                "a": {
                    "P": [
                        {
                            "s": "CHIPUSDT",
                            "pa": "-73",
                            "ep": "0.067",
                            "bep": "0.0671",
                            "up": "0.12",
                            "mt": "cross",
                            "iw": "0",
                            "ps": "BOTH",
                        }
                    ]
                },
            }
        )

        snapshot = store.snapshot()
        self.assertEqual(changed, 1)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["symbol"], "CHIPUSDT")
        self.assertEqual(snapshot[0]["positionAmt"], "-73")
        self.assertEqual(snapshot[0]["positionSide"], "BOTH")
        self.assertEqual(snapshot[0]["source"], "user_data_stream")


class OpenOrderStateStoreTests(unittest.TestCase):
    def test_execution_events_track_active_open_orders(self) -> None:
        store = OpenOrderStateStore()
        new_event = normalize_order_trade_update(
            {
                "E": 1000,
                "T": 990,
                "o": {
                    "s": "CHIPUSDT",
                    "c": "gx-chipu-a",
                    "S": "BUY",
                    "o": "LIMIT",
                    "f": "GTX",
                    "q": "10",
                    "p": "0.067",
                    "x": "NEW",
                    "X": "NEW",
                    "i": 1,
                },
            }
        )
        assert new_event is not None

        store.update_from_execution_event(new_event)

        self.assertEqual(len(store.snapshot()), 1)
        filled_event = normalize_order_trade_update(
            {
                "E": 1010,
                "T": 1005,
                "o": {
                    "s": "CHIPUSDT",
                    "c": "gx-chipu-a",
                    "S": "BUY",
                    "x": "TRADE",
                    "X": "FILLED",
                    "i": 1,
                    "l": "10",
                    "z": "10",
                    "L": "0.067",
                },
            }
        )
        assert filled_event is not None

        store.update_from_execution_event(filled_event)

        self.assertEqual(store.snapshot(), [])

    def test_rest_backfill_replaces_open_order_baseline(self) -> None:
        store = OpenOrderStateStore()

        store.replace_from_rest_open_orders(
            [
                {
                    "symbol": "CHIPUSDT",
                    "orderId": 9,
                    "clientOrderId": "gx-chipu-live",
                    "side": "SELL",
                    "type": "LIMIT",
                    "timeInForce": "GTX",
                    "origQty": "10",
                    "price": "0.0672",
                    "executedQty": "0",
                    "status": "NEW",
                }
            ]
        )

        snapshot = store.snapshot()
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["clientOrderId"], "gx-chipu-live")
        self.assertEqual(snapshot[0]["source"], "rest_backfill")
        self.assertIsNotNone(store.last_update_age_seconds())


class FuturesListenKeyClientTests(unittest.TestCase):
    @patch("grid_optimizer.execution_events._http_api_key_request_json")
    def test_listen_key_client_create_keepalive_and_close(self, mock_request) -> None:
        mock_request.return_value = {"listenKey": "abc123"}
        client = FuturesListenKeyClient(api_key="key")

        listen_key = client.create()
        client.keepalive(listen_key)
        client.close(listen_key)

        self.assertEqual(listen_key, "abc123")
        self.assertEqual(mock_request.call_count, 3)
        self.assertEqual(mock_request.call_args_list[0].kwargs["method"], "POST")
        self.assertEqual(mock_request.call_args_list[1].kwargs["method"], "PUT")
        self.assertEqual(mock_request.call_args_list[2].kwargs["method"], "DELETE")


class FuturesUserDataStreamTests(unittest.TestCase):
    def test_user_data_stream_routes_order_events_into_store(self) -> None:
        store = ExecutionEventStore()
        stream = FuturesUserDataStream(api_key="key", event_store=store)

        stream._on_message(
            None,
            json.dumps(
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 1000,
                    "T": 990,
                    "o": {
                        "s": "CHIPUSDT",
                        "c": "grid_1",
                        "S": "SELL",
                        "x": "TRADE",
                        "X": "PARTIALLY_FILLED",
                        "i": 12345,
                        "l": "5",
                        "z": "5",
                        "L": "0.0671",
                    },
                }
            ),
        )

        events = store.snapshot()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].kind, "ORDER_PARTIALLY_FILLED")
        self.assertEqual(events[0].last_filled_qty, 5.0)
        self.assertEqual(len(stream.snapshot_open_orders()), 1)

    def test_user_data_stream_routes_account_updates_into_position_store(self) -> None:
        stream = FuturesUserDataStream(api_key="key")

        stream._on_message(
            None,
            json.dumps(
                {
                    "e": "ACCOUNT_UPDATE",
                    "E": 1000,
                    "a": {
                        "P": [
                            {
                                "s": "CHIPUSDT",
                                "pa": "-11",
                                "ep": "0.067",
                                "bep": "0.0671",
                                "up": "0",
                                "ps": "BOTH",
                            }
                        ]
                    },
                }
            ),
        )

        positions = stream.snapshot_account_positions()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["symbol"], "CHIPUSDT")
        self.assertEqual(positions[0]["positionAmt"], "-11")
        self.assertEqual(stream.status()["account_position_count"], 1)

    def test_user_data_stream_uses_private_listen_key_url(self) -> None:
        stream = FuturesUserDataStream(api_key="key")

        self.assertEqual(
            stream._user_stream_url("listen-key"),
            "wss://fstream.binance.com/private/ws/listen-key",
        )

    def test_user_data_stream_marks_connected_after_open(self) -> None:
        stream = FuturesUserDataStream(api_key="key")

        stream._on_open(MagicMock())

        self.assertEqual(stream.status()["connection_state"], "connected")


class MarketCrossingEventTests(unittest.TestCase):
    def test_detect_crossed_grid_levels_excludes_protected_nearest_level_on_up_cross(self) -> None:
        crossings = detect_crossed_grid_levels(
            last_price=0.0670,
            current_price=0.0674,
            lower_price=0.05,
            upper_price=0.08,
            step=0.0001,
        )

        self.assertEqual([item["side"] for item in crossings], ["SELL", "SELL", "SELL"])
        self.assertEqual([item["price"] for item in crossings], [0.0671, 0.0672, 0.0673])

    def test_detect_crossed_grid_levels_excludes_protected_nearest_level_on_down_cross(self) -> None:
        crossings = detect_crossed_grid_levels(
            last_price=0.0674,
            current_price=0.0670,
            lower_price=0.05,
            upper_price=0.08,
            step=0.0001,
        )

        self.assertEqual([item["side"] for item in crossings], ["BUY", "BUY", "BUY"])
        self.assertEqual([item["price"] for item in crossings], [0.0673, 0.0672, 0.0671])

    def test_market_tick_can_be_built_from_book_ticker_payload(self) -> None:
        tick = MarketTick.from_book_ticker(
            {"s": "CHIPUSDT", "b": "0.0671", "a": "0.0672", "T": 1234}
        )

        self.assertEqual(tick.symbol, "CHIPUSDT")
        self.assertEqual(tick.bid_price, 0.0671)
        self.assertEqual(tick.ask_price, 0.0672)
        self.assertAlmostEqual(tick.mid_price, 0.06715)
        self.assertEqual(tick.exchange_time, 1234)


if __name__ == "__main__":
    unittest.main()
