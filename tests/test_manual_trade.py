from __future__ import annotations

import unittest
from unittest.mock import patch

from grid_optimizer.web import (
    MANUAL_TRADE_PAGE,
    _build_manual_trade_plan,
    _is_manual_trade_order,
    _manual_trade_client_order_prefix,
    _manual_trade_chase_leg,
    _manual_trade_ensure_isolated,
    _manual_trade_maker_worker,
    _manual_trade_prepare_plan,
)


class ManualTradeTests(unittest.TestCase):
    def _symbol_info(self) -> dict[str, float]:
        return {
            "tick_size": 0.0001,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }

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
            "last_order": {"status": "FILLED", "clientOrderId": "mt_btcusdc_close_b_1"},
        }
        mock_snapshot.return_value = {"symbol": "BTCUSDC"}

        _manual_trade_maker_worker("task-1", {"symbol": "BTCUSDC", "side": "BUY", "notional": 100.0})

        final_patch = mock_set_task.call_args_list[-1].args[1]
        self.assertEqual(final_patch["status"], "filled")
        self.assertIsNone(final_patch["current_order"])
        self.assertIsNone(final_patch["current_leg"])
        self.assertEqual(final_patch["executed_qty"], 0.001)
        self.assertEqual(final_patch["remaining_qty"], 0.0)
        self.assertEqual(final_patch["attempts"], 2)

    def test_monitor_page_links_to_manual_trade_page(self) -> None:
        from grid_optimizer.web import MONITOR_PAGE

        self.assertIn('href="/manual_trade"', MONITOR_PAGE)


if __name__ == "__main__":
    unittest.main()
