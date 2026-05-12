from __future__ import annotations

import argparse
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from grid_optimizer.execution_events import ExecutionEvent
from grid_optimizer.loop_runner import (
    _drain_new_runner_execution_events,
    _maybe_start_runner_user_data_stream,
    _run_periodic_reconcile,
    _should_backfill_open_orders_rest,
    _should_backfill_trade_rest,
    _snapshot_runner_account_position,
    _summarize_runner_strategy_open_order_state,
    _summarize_runner_strategy_execution_events,
    _snapshot_runner_execution_events,
    _trade_event_to_audit_row,
)


class LoopRunnerExecutionEventHelpersTests(unittest.TestCase):
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    def test_maybe_start_runner_user_data_stream_skips_without_credentials(self, mock_credentials) -> None:
        mock_credentials.return_value = None
        args = argparse.Namespace()

        stream = _maybe_start_runner_user_data_stream(args)

        self.assertIsNone(stream)
        self.assertIsNone(getattr(args, "user_data_stream", None))

    @patch("grid_optimizer.loop_runner.FuturesUserDataStream")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    def test_maybe_start_runner_user_data_stream_starts_when_credentials_exist(
        self,
        mock_credentials,
        mock_stream_cls,
    ) -> None:
        mock_credentials.return_value = ("key", "secret")
        stream = MagicMock()
        mock_stream_cls.return_value = stream
        args = argparse.Namespace()

        started = _maybe_start_runner_user_data_stream(args)

        self.assertIs(started, stream)
        self.assertIs(args.user_data_stream, stream)
        mock_stream_cls.assert_called_once_with(api_key="key")
        stream.start.assert_called_once_with()

    def test_snapshot_runner_execution_events_returns_recent_normalized_payloads(self) -> None:
        stream = SimpleNamespace(
            snapshot_events=lambda: [
                ExecutionEvent(
                    kind="ORDER_NEW",
                    symbol="CHIPUSDT",
                    event_time=1000,
                    transaction_time=990,
                    order_id=1,
                    client_order_id="grid_1",
                    side="BUY",
                    execution_type="NEW",
                    order_status="NEW",
                ),
                ExecutionEvent(
                    kind="ORDER_FILLED",
                    symbol="CHIPUSDT",
                    event_time=1010,
                    transaction_time=1005,
                    order_id=2,
                    client_order_id="grid_2",
                    side="SELL",
                    execution_type="TRADE",
                    order_status="FILLED",
                    last_filled_qty=10.0,
                    cumulative_filled_qty=10.0,
                    last_filled_price=0.0671,
                ),
            ]
        )
        args = argparse.Namespace(user_data_stream=stream)

        snapshot = _snapshot_runner_execution_events(args, max_events=1)

        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["kind"], "ORDER_FILLED")
        self.assertEqual(snapshot[0]["symbol"], "CHIPUSDT")
        self.assertEqual(snapshot[0]["last_filled_price"], 0.0671)

    def test_drain_new_runner_execution_events_returns_only_unseen_items(self) -> None:
        event = ExecutionEvent(
            kind="ORDER_FILLED",
            symbol="CHIPUSDT",
            event_time=1010,
            transaction_time=1005,
            order_id=2,
            client_order_id="grid_2",
            side="SELL",
            execution_type="TRADE",
            order_status="FILLED",
            last_filled_qty=10.0,
            cumulative_filled_qty=10.0,
            last_filled_price=0.0671,
        )
        stream = SimpleNamespace(snapshot_events=lambda: [event])
        args = argparse.Namespace(user_data_stream=stream)

        first = _drain_new_runner_execution_events(args, max_events=5)
        second = _drain_new_runner_execution_events(args, max_events=5)

        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["kind"], "ORDER_FILLED")
        self.assertEqual(second, [])

    def test_summarize_runner_strategy_execution_events_filters_to_strategy_prefix(self) -> None:
        stream = SimpleNamespace(
            snapshot_events=lambda: [
                ExecutionEvent(
                    kind="ORDER_NEW",
                    symbol="CHIPUSDT",
                    event_time=1000,
                    transaction_time=990,
                    order_id=1,
                    client_order_id="gx-chipu-abc",
                    side="BUY",
                    execution_type="NEW",
                    order_status="NEW",
                ),
                ExecutionEvent(
                    kind="ORDER_FILLED",
                    symbol="CHIPUSDT",
                    event_time=1010,
                    transaction_time=1005,
                    order_id=2,
                    client_order_id="gx-chipu-def",
                    side="SELL",
                    execution_type="TRADE",
                    order_status="FILLED",
                ),
                ExecutionEvent(
                    kind="ORDER_FILLED",
                    symbol="CHIPUSDT",
                    event_time=1015,
                    transaction_time=1010,
                    order_id=3,
                    client_order_id="manual-order",
                    side="SELL",
                    execution_type="TRADE",
                    order_status="FILLED",
                ),
            ]
        )
        args = argparse.Namespace(user_data_stream=stream)

        summary = _summarize_runner_strategy_execution_events(args, "CHIPUSDT", max_events=10)

        self.assertEqual(summary["observed_event_count"], 2)
        self.assertEqual(summary["counts"]["ORDER_NEW"], 1)
        self.assertEqual(summary["counts"]["ORDER_FILLED"], 1)
        self.assertEqual(summary["filled_client_order_ids"], ["gx-chipu-def"])

    def test_trade_event_to_audit_row_maps_filled_event_to_trade_schema(self) -> None:
        event = ExecutionEvent(
            kind="ORDER_FILLED",
            symbol="CHIPUSDT",
            event_time=1010,
            transaction_time=1005,
            order_id=2,
            client_order_id="gx-chipu-def",
            side="SELL",
            execution_type="TRADE",
            order_status="FILLED",
            order_type="LIMIT",
            time_in_force="GTX",
            position_side="BOTH",
            original_qty=10.0,
            original_price=0.0672,
            average_price=0.0671,
            last_filled_qty=10.0,
            cumulative_filled_qty=10.0,
            last_filled_price=0.0671,
            commission=0.001,
            commission_asset="USDT",
            realized_pnl=0.12,
        )

        row = _trade_event_to_audit_row(event)

        self.assertEqual(row["id"], "2:gx-chipu-def:1005:10.0:0.0671")
        self.assertEqual(row["symbol"], "CHIPUSDT")
        self.assertEqual(row["side"], "SELL")
        self.assertEqual(row["time"], 1005)
        self.assertEqual(row["price"], 0.0671)
        self.assertEqual(row["qty"], 10.0)
        self.assertEqual(row["commission"], 0.001)
        self.assertEqual(row["realizedPnl"], 0.12)

    def test_should_backfill_trade_rest_skips_recent_backfill_when_observed_trades_exist(self) -> None:
        decision = _should_backfill_trade_rest(
            {"trade_rest_last_sync_at": "2026-05-12T00:00:00+00:00"},
            now_utc="2026-05-12T00:02:00+00:00",
            observed_trade_appended=3,
        )

        self.assertFalse(decision)

    def test_should_backfill_trade_rest_allows_periodic_backfill_after_interval(self) -> None:
        decision = _should_backfill_trade_rest(
            {"trade_rest_last_sync_at": "2026-05-12T00:00:00+00:00"},
            now_utc="2026-05-12T00:10:00+00:00",
            observed_trade_appended=1,
        )

        self.assertTrue(decision)

    def test_summarize_runner_strategy_open_order_state_tracks_active_orders_from_events(self) -> None:
        stream = SimpleNamespace(
            snapshot_events=lambda: [
                ExecutionEvent(
                    kind="ORDER_NEW",
                    symbol="CHIPUSDT",
                    event_time=1000,
                    transaction_time=990,
                    order_id=1,
                    client_order_id="gx-chipu-a",
                    side="BUY",
                    execution_type="NEW",
                    order_status="NEW",
                ),
                ExecutionEvent(
                    kind="ORDER_PARTIALLY_FILLED",
                    symbol="CHIPUSDT",
                    event_time=1010,
                    transaction_time=1005,
                    order_id=1,
                    client_order_id="gx-chipu-a",
                    side="BUY",
                    execution_type="TRADE",
                    order_status="PARTIALLY_FILLED",
                    cumulative_filled_qty=4.0,
                ),
                ExecutionEvent(
                    kind="ORDER_NEW",
                    symbol="CHIPUSDT",
                    event_time=1020,
                    transaction_time=1015,
                    order_id=2,
                    client_order_id="gx-chipu-b",
                    side="SELL",
                    execution_type="NEW",
                    order_status="NEW",
                ),
                ExecutionEvent(
                    kind="ORDER_FILLED",
                    symbol="CHIPUSDT",
                    event_time=1030,
                    transaction_time=1025,
                    order_id=1,
                    client_order_id="gx-chipu-a",
                    side="BUY",
                    execution_type="TRADE",
                    order_status="FILLED",
                ),
                ExecutionEvent(
                    kind="ORDER_CANCELED",
                    symbol="CHIPUSDT",
                    event_time=1040,
                    transaction_time=1035,
                    order_id=99,
                    client_order_id="manual-order",
                    side="SELL",
                    execution_type="CANCELED",
                    order_status="CANCELED",
                ),
            ]
        )
        args = argparse.Namespace(user_data_stream=stream)

        summary = _summarize_runner_strategy_open_order_state(args, "CHIPUSDT", max_events=20)

        self.assertEqual(summary["active_order_count"], 1)
        self.assertEqual(summary["active_client_order_ids"], ["gx-chipu-b"])
        self.assertEqual(summary["active_order_ids"], [2])
        self.assertEqual(summary["source"], "observed_events")

    def test_summarize_runner_strategy_open_order_state_prefers_stream_current_state(self) -> None:
        stream = SimpleNamespace(
            snapshot_open_orders=lambda: [
                {
                    "symbol": "CHIPUSDT",
                    "clientOrderId": "gx-chipu-live",
                    "orderId": 9,
                }
            ],
            open_order_state_age_seconds=lambda: 1.0,
            snapshot_events=lambda: [],
        )
        args = argparse.Namespace(user_data_stream=stream)

        summary = _summarize_runner_strategy_open_order_state(args, "CHIPUSDT", max_events=20)

        self.assertEqual(summary["active_order_count"], 1)
        self.assertEqual(summary["active_client_order_ids"], ["gx-chipu-live"])
        self.assertEqual(summary["active_order_ids"], [9])
        self.assertEqual(summary["source"], "stream_open_orders")

    def test_summarize_runner_strategy_open_order_state_accepts_fresh_empty_stream_state(self) -> None:
        stream = SimpleNamespace(
            snapshot_open_orders=lambda: [],
            open_order_state_age_seconds=lambda: 1.0,
            snapshot_events=lambda: [
                ExecutionEvent(
                    kind="ORDER_NEW",
                    symbol="CHIPUSDT",
                    event_time=1000,
                    transaction_time=990,
                    order_id=1,
                    client_order_id="gx-chipu-stale-window",
                    side="BUY",
                    execution_type="NEW",
                    order_status="NEW",
                )
            ],
        )
        args = argparse.Namespace(user_data_stream=stream)

        summary = _summarize_runner_strategy_open_order_state(args, "CHIPUSDT", max_events=20)

        self.assertEqual(summary["active_order_count"], 0)
        self.assertEqual(summary["source"], "stream_open_orders")

    def test_should_backfill_open_orders_rest_skips_recent_rest_when_observed_state_matches_expected(self) -> None:
        decision = _should_backfill_open_orders_rest(
            {"open_orders_rest_last_sync_at": "2026-05-12T00:00:00+00:00"},
            now_utc="2026-05-12T00:02:00+00:00",
            observed_active_order_count=3,
            expected_open_order_count=3,
        )

        self.assertFalse(decision)

    def test_should_backfill_open_orders_rest_keeps_rest_when_observed_state_disagrees(self) -> None:
        decision = _should_backfill_open_orders_rest(
            {"open_orders_rest_last_sync_at": "2026-05-12T00:00:00+00:00"},
            now_utc="2026-05-12T00:02:00+00:00",
            observed_active_order_count=2,
            expected_open_order_count=3,
        )

        self.assertTrue(decision)

    def test_snapshot_runner_account_position_returns_recent_stream_position(self) -> None:
        stream = SimpleNamespace(
            snapshot_account_positions=lambda: [
                {
                    "symbol": "CHIPUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "-73",
                    "observed_at": __import__("time").monotonic(),
                }
            ]
        )
        args = argparse.Namespace(user_data_stream=stream)

        position = _snapshot_runner_account_position(args, "CHIPUSDT")

        assert position is not None
        self.assertEqual(position["positionAmt"], "-73")
        self.assertEqual(position["symbol"], "CHIPUSDT")
        self.assertIsNotNone(position["stream_age_seconds"])

    @patch("grid_optimizer.loop_runner.fetch_futures_account_info_v3")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders", return_value=[])
    def test_periodic_reconcile_uses_stream_position_when_available(
        self,
        _mock_open_orders,
        mock_account_info,
    ) -> None:
        stream = SimpleNamespace(
            snapshot_events=lambda: [],
            snapshot_account_positions=lambda: [
                {
                    "symbol": "CHIPUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "-73",
                    "observed_at": __import__("time").monotonic(),
                }
            ],
        )
        args = argparse.Namespace(user_data_stream=stream)

        snapshot = _run_periodic_reconcile(
            state={},
            cycle=1,
            interval_cycles=1,
            symbol="CHIPUSDT",
            strategy_mode="synthetic_neutral",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            expected_open_order_count=0,
            expected_actual_net_qty=-73.0,
            args=args,
        )

        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["account_position_source"], "user_data_stream")
        mock_account_info.assert_not_called()


if __name__ == "__main__":
    unittest.main()
