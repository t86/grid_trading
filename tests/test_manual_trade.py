from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.web import (
    MANUAL_TRADE_PAGE,
    _build_manual_trade_plan,
    _is_manual_trade_order,
    _manual_trade_append_history,
    _manual_trade_client_order_prefix,
    _manual_trade_chase_leg,
    _manual_trade_cancel,
    _manual_trade_current_task,
    _manual_trade_ensure_isolated,
    _manual_trade_history_for_symbol,
    _manual_trade_maker_worker,
    MANUAL_TRADE_PREPARE_TIMEOUT_SECONDS,
    MANUAL_TRADE_SLEEP_SECONDS,
    _manual_trade_set_task,
    _manual_trade_prepare_plan,
)


class ManualTradeTests(unittest.TestCase):
    def setUp(self) -> None:
        import grid_optimizer.web as web

        self._tmpdir = tempfile.TemporaryDirectory()
        self.history_path = Path(self._tmpdir.name) / "manual_trade_history.json"
        self._history_patch = patch.object(
            web,
            "MANUAL_TRADE_HISTORY_PATH",
            self.history_path,
        )
        self._history_patch.start()

    def tearDown(self) -> None:
        from grid_optimizer.web import MANUAL_TRADE_TASKS

        self._history_patch.stop()
        self._tmpdir.cleanup()
        MANUAL_TRADE_TASKS.clear()

    def _symbol_info(self) -> dict[str, float]:
        return {
            "tick_size": 0.0001,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }

    def test_manual_maker_chase_checks_every_second(self) -> None:
        self.assertEqual(MANUAL_TRADE_SLEEP_SECONDS, 1.0)
        self.assertEqual(MANUAL_TRADE_PREPARE_TIMEOUT_SECONDS, 12.0)

    def test_manual_trade_prefix_is_symbol_scoped(self) -> None:
        prefix = _manual_trade_client_order_prefix("BARDUSDT")

        self.assertTrue(prefix.startswith("mt_bardusdt"))
        self.assertTrue(_is_manual_trade_order({"clientOrderId": f"{prefix}_buy_123"}, prefix))
        self.assertFalse(_is_manual_trade_order({"clientOrderId": "grid_bardusdt_buy_123"}, prefix))

    def test_buy_plan_closes_short_before_opening_long(self) -> None:
        plan = _build_manual_trade_plan(
            symbol="BARDUSDT",
            side="BUY",
            notional=100.0,
            bid_price=1.0,
            ask_price=1.01,
            position_amt=-40.0,
            symbol_info=self._symbol_info(),
        )

        self.assertEqual([leg["role"] for leg in plan["legs"]], ["close_short", "open_long"])
        self.assertEqual(plan["legs"][0]["side"], "BUY")
        self.assertTrue(plan["legs"][0]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][0]["quantity"], 40.0)
        self.assertFalse(plan["legs"][1]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][1]["quantity"], 59.0)
        self.assertAlmostEqual(plan["planned_notional"], 99.0)
        self.assertAlmostEqual(plan["legs"][0]["estimated_notional"], 40.0)
        self.assertAlmostEqual(plan["legs"][1]["estimated_notional"], 59.0)

    def test_sell_plan_closes_long_before_opening_short(self) -> None:
        plan = _build_manual_trade_plan(
            symbol="BARDUSDT",
            side="SELL",
            notional=80.0,
            bid_price=2.0,
            ask_price=2.02,
            position_amt=12.0,
            symbol_info=self._symbol_info(),
        )

        self.assertEqual([leg["role"] for leg in plan["legs"]], ["close_long", "open_short"])
        self.assertEqual(plan["legs"][0]["side"], "SELL")
        self.assertTrue(plan["legs"][0]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][0]["quantity"], 12.0)
        self.assertFalse(plan["legs"][1]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][1]["quantity"], 28.0)
        self.assertAlmostEqual(plan["planned_notional"], 80.8)
        self.assertAlmostEqual(plan["legs"][0]["estimated_notional"], 24.24)
        self.assertAlmostEqual(plan["legs"][1]["estimated_notional"], 56.56)

    def test_plan_rejects_too_small_notional_after_rounding(self) -> None:
        with self.assertRaisesRegex(ValueError, "below minimum notional"):
            _build_manual_trade_plan(
                symbol="BARDUSDT",
                side="BUY",
                notional=1.0,
                bid_price=1.0,
                ask_price=1.01,
                position_amt=0.0,
                symbol_info=self._symbol_info(),
            )

    @patch("grid_optimizer.web.post_futures_change_margin_type")
    def test_ensure_isolated_skips_api_call_when_position_already_isolated(self, mock_change_margin) -> None:
        result = _manual_trade_ensure_isolated(
            "BARDUSDT",
            "key",
            "secret",
            account_info={
                "positions": [
                    {
                        "symbol": "BARDUSDT",
                        "positionSide": "BOTH",
                        "isolated": True,
                    }
                ]
            },
        )

        self.assertEqual(result["already_isolated"], True)
        mock_change_margin.assert_not_called()

    @patch("grid_optimizer.web.fetch_futures_account_info_v3")
    @patch("grid_optimizer.web.post_futures_change_margin_type")
    def test_ensure_isolated_allows_open_order_rejection_when_refreshed_position_is_isolated(
        self,
        mock_change_margin,
        mock_account_info,
    ) -> None:
        mock_change_margin.side_effect = RuntimeError(
            "Binance API error -4047: Margin type cannot be changed if there exists open orders."
        )
        mock_account_info.return_value = {
            "positions": [
                {
                    "symbol": "BARDUSDT",
                    "positionSide": "BOTH",
                    "isolated": True,
                }
            ]
        }

        result = _manual_trade_ensure_isolated("BARDUSDT", "key", "secret")

        self.assertEqual(result["already_isolated"], True)
        self.assertIn("open orders", result["warning"])

    def test_manual_trade_page_contains_required_controls(self) -> None:
        self.assertIn('id="manual_symbol"', MANUAL_TRADE_PAGE)
        self.assertIn('id="manual_notional"', MANUAL_TRADE_PAGE)
        self.assertIn('id="manual_margin_mode"', MANUAL_TRADE_PAGE)
        self.assertIn('<option value="KEEP" selected>保持当前保证金模式</option>', MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/status", MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/maker", MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/take", MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/cancel", MANUAL_TRADE_PAGE)
        self.assertIn("setInterval(() => refreshStatus().catch(() => {}), 8000)", MANUAL_TRADE_PAGE)
        self.assertIn("fetchJsonWithTimeout", MANUAL_TRADE_PAGE)
        self.assertIn("cancelStatusRefresh()", MANUAL_TRADE_PAGE)
        self.assertIn('setTimeout(() => refreshStatus({ force: true }).catch(() => {}), 500)', MANUAL_TRADE_PAGE)
        self.assertIn('id="history_body"', MANUAL_TRADE_PAGE)
        self.assertIn("renderHistory(snapshot.history || [])", MANUAL_TRADE_PAGE)

    def test_manual_trade_history_is_symbol_scoped(self) -> None:
        _manual_trade_append_history(
            {
                "source": "maker",
                "symbol": "BTCUSDC",
                "side": "BUY",
                "executed_qty": 0.01,
                "avg_fill_price": 100.0,
            }
        )
        _manual_trade_append_history(
            {
                "source": "maker",
                "symbol": "ETHUSDC",
                "side": "SELL",
                "executed_qty": 0.1,
                "avg_fill_price": 10.0,
            }
        )

        btc_history = _manual_trade_history_for_symbol("BTCUSDC")
        eth_history = _manual_trade_history_for_symbol("ETHUSDC")
        self.assertEqual(len(btc_history), 1)
        self.assertEqual(len(eth_history), 1)
        self.assertEqual(btc_history[0]["symbol"], "BTCUSDC")
        self.assertEqual(eth_history[0]["symbol"], "ETHUSDC")
        self.assertIn('"BTCUSDC"', self.history_path.read_text(encoding="utf-8"))
        self.assertIn('"ETHUSDC"', self.history_path.read_text(encoding="utf-8"))

    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    @patch("grid_optimizer.web.fetch_futures_account_info_v3")
    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_position_mode")
    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web.post_futures_change_margin_type")
    def test_prepare_plan_keeps_current_margin_mode_by_default(
        self,
        mock_change_margin,
        mock_credentials,
        mock_position_mode,
        mock_book,
        mock_account_info,
        mock_symbol_config,
    ) -> None:
        mock_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_book.return_value = [{"bid_price": "1.0", "ask_price": "1.01"}]
        mock_account_info.return_value = {"positions": [{"symbol": "BARDUSDT", "positionAmt": "0"}]}
        mock_symbol_config.return_value = self._symbol_info()

        _manual_trade_prepare_plan("BARDUSDT", "BUY", 100.0)

        mock_change_margin.assert_not_called()

    @patch("grid_optimizer.web._manual_trade_set_task")
    @patch("grid_optimizer.web.fetch_futures_order")
    @patch("grid_optimizer.web.post_futures_order")
    @patch("grid_optimizer.web._manual_trade_book_prices")
    @patch("grid_optimizer.web._manual_trade_cancel_requested")
    @patch("grid_optimizer.web.delete_futures_order")
    @patch("grid_optimizer.web.time.sleep")
    def test_maker_chase_keeps_resting_order_when_best_price_is_unchanged(
        self,
        mock_sleep,
        mock_delete_order,
        mock_cancel_requested,
        mock_book_prices,
        mock_post_order,
        mock_fetch_order,
        mock_set_task,
    ) -> None:
        mock_cancel_requested.return_value = False
        mock_book_prices.return_value = (1.0, 1.01)
        mock_post_order.return_value = {"orderId": 123, "clientOrderId": "mt_bardusdt_open_long_buy_1"}
        mock_fetch_order.side_effect = [
            {"status": "NEW", "executedQty": "0", "price": "1.0"},
            {"status": "FILLED", "executedQty": "10", "price": "1.0"},
        ]

        result = _manual_trade_chase_leg(
            symbol="BARDUSDT",
            api_key="key",
            api_secret="secret",
            prefix=_manual_trade_client_order_prefix("BARDUSDT"),
            leg={"role": "open_long", "side": "BUY", "quantity": 10.0, "reduce_only": False},
        )

        self.assertEqual(result["executed_qty"], 10.0)
        mock_post_order.assert_called_once()
        mock_delete_order.assert_not_called()

    @patch("grid_optimizer.web._manual_trade_set_task")
    @patch("grid_optimizer.web.fetch_futures_order")
    @patch("grid_optimizer.web.post_futures_order")
    @patch("grid_optimizer.web._manual_trade_book_prices")
    @patch("grid_optimizer.web._manual_trade_cancel_requested")
    @patch("grid_optimizer.web.delete_futures_order")
    @patch("grid_optimizer.web.time.monotonic")
    @patch("grid_optimizer.web.time.sleep")
    def test_maker_chase_does_not_reprice_before_min_interval(
        self,
        mock_sleep,
        mock_monotonic,
        mock_delete_order,
        mock_cancel_requested,
        mock_book_prices,
        mock_post_order,
        mock_fetch_order,
        mock_set_task,
    ) -> None:
        mock_cancel_requested.side_effect = [False, False, False]
        mock_book_prices.side_effect = [(1.0, 1.01), (1.0, 1.02), (1.0, 1.02)]
        mock_monotonic.side_effect = [100.0, 100.5, 107.0]
        mock_post_order.return_value = {"orderId": 123, "clientOrderId": "mt_bardusdt_open_short_sell_1"}
        mock_fetch_order.side_effect = [
            {"status": "NEW", "executedQty": "0", "price": "1.01"},
            {"status": "FILLED", "executedQty": "10", "price": "1.01", "avgPrice": "1.01"},
        ]

        result = _manual_trade_chase_leg(
            symbol="BARDUSDT",
            api_key="key",
            api_secret="secret",
            prefix=_manual_trade_client_order_prefix("BARDUSDT"),
            leg={"role": "open_short", "side": "SELL", "quantity": 10.0, "reduce_only": False},
        )

        self.assertEqual(result["executed_qty"], 10.0)
        mock_post_order.assert_called_once()
        mock_delete_order.assert_not_called()

    @patch("grid_optimizer.web._manual_trade_snapshot")
    @patch("grid_optimizer.web._manual_trade_chase_leg")
    @patch("grid_optimizer.web._manual_trade_prepare_plan")
    @patch("grid_optimizer.web._manual_trade_set_task")
    def test_maker_worker_clears_current_order_after_fill(
        self,
        mock_set_task,
        mock_prepare_plan,
        mock_chase_leg,
        mock_snapshot,
    ) -> None:
        plan = {
            "symbol": "BTCUSDC",
            "side": "BUY",
            "notional": 100.0,
            "legs": [{"role": "close_short", "side": "BUY", "quantity": 0.001, "reduce_only": True}],
        }
        mock_prepare_plan.return_value = ("BTCUSDC", "key", "secret", plan, 0.1, 0.001)
        mock_chase_leg.return_value = {
            "leg": plan["legs"][0],
            "attempts": 2,
            "executed_qty": 0.001,
            "last_order": {
                "status": "FILLED",
                "clientOrderId": "mt_btcusdc_close_b_1",
                "avgPrice": "76784.1000",
                "price": "76784.1",
            },
        }
        mock_snapshot.return_value = {"symbol": "BTCUSDC"}
        mock_set_task.side_effect = lambda symbol, patch: {
            "id": "task-1",
            "symbol": symbol,
            "updated_at": "2026-04-28T12:00:00+00:00",
            **patch,
        }

        _manual_trade_maker_worker("task-1", {"symbol": "BTCUSDC", "side": "BUY", "notional": 100.0})

        final_patch = mock_set_task.call_args_list[-1].args[1]
        self.assertEqual(final_patch["status"], "filled")
        self.assertIsNone(final_patch["current_order"])
        self.assertIsNone(final_patch["current_leg"])
        self.assertEqual(final_patch["executed_qty"], 0.001)
        self.assertEqual(final_patch["avg_fill_price"], 76784.1)
        self.assertEqual(final_patch["last_fill_price"], 76784.1)
        self.assertEqual(final_patch["remaining_qty"], 0.0)
        self.assertEqual(final_patch["attempts"], 2)
        history = _manual_trade_history_for_symbol("BTCUSDC")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["source"], "maker")
        self.assertEqual(history[0]["side"], "BUY")
        self.assertAlmostEqual(history[0]["avg_fill_price"], 76784.1)
        self.assertAlmostEqual(history[0]["fill_notional"], 76.7841)

    def test_starting_new_task_resets_previous_fill_fields(self) -> None:
        _manual_trade_set_task(
            "BTCUSDC",
            {
                "status": "filled",
                "avg_fill_price": 76784.1,
                "last_fill_price": 76784.1,
                "executed_qty": 0.001,
                "remaining_qty": 0.0,
                "attempts": 5,
                "current_order": {"orderId": 1},
                "current_leg": {"role": "open_long"},
                "legs_done": [{"last_order": {"status": "FILLED"}}],
                "error": None,
            },
        )

        from grid_optimizer.web import _manual_trade_initialize_task

        _manual_trade_initialize_task(
            "BTCUSDC",
            {
                "id": "next-task",
                "status": "pending",
                "side": "SELL",
                "notional": 100.0,
                "cancel_requested": False,
                "message": "manual maker task queued",
            },
        )

        task = _manual_trade_current_task("BTCUSDC")
        self.assertEqual(task["avg_fill_price"], 0.0)
        self.assertEqual(task["last_fill_price"], 0.0)
        self.assertEqual(task["executed_qty"], 0.0)
        self.assertEqual(task["remaining_qty"], 0.0)
        self.assertEqual(task["attempts"], 0)
        self.assertIsNone(task["current_order"])
        self.assertIsNone(task["current_leg"])
        self.assertEqual(task["legs_done"], [])

    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web._manual_trade_cancel_open_orders")
    def test_cancel_clears_current_order_after_cleanup(self, mock_cancel_orders, mock_credentials) -> None:
        mock_credentials.return_value = ("key", "secret")
        mock_cancel_orders.return_value = {"attempted": 1, "success": 1, "errors": []}
        _manual_trade_set_task(
            "BTCUSDC",
            {"status": "running", "current_order": {"orderId": 1}, "current_leg": {"role": "close_long"}},
        )

        result = _manual_trade_cancel({"symbol": "BTCUSDC"})

        task = result["task"]
        self.assertEqual(task["status"], "canceled")
        self.assertIsNone(task["current_order"])
        self.assertIsNone(task["current_leg"])
        self.assertEqual(task["remaining_qty"], 0.0)

    def test_monitor_page_links_to_manual_trade_page(self) -> None:
        from grid_optimizer.web import MONITOR_PAGE

        self.assertIn('href="/manual_trade"', MONITOR_PAGE)


if __name__ == "__main__":
    unittest.main()
