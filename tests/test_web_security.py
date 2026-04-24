from __future__ import annotations

import base64
import io
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from grid_optimizer.web import (
    MONITOR_PAGE,
    HTML_PAGE,
    STRATEGIES_PAGE,
    _Handler,
    _basic_auth_header_matches,
    _build_custom_grid_runner_preset,
    _build_flatten_command,
    _delete_custom_grid_runner_preset,
    _build_runner_command,
    _client_ip_allowed,
    _default_runtime_paths_for_symbol,
    _execute_stop_actions,
    _get_custom_runner_preset,
    _load_runner_control_config,
    _normalize_runner_control_payload,
    _parse_allowed_networks,
    _resolve_runner_volume_trigger_action,
    _resolve_runner_volatility_trigger_action,
    _resolve_runner_start_config,
    _run_loop_monitor_query,
    _runtime_guard_input_summary,
    _runner_service_name_for_symbol,
    _runner_preset_payload,
    _runner_preset_summaries,
    _save_runner_control_config,
    _start_runner_process,
    _update_custom_grid_runner_preset,
    _uses_legacy_runner,
    _volatility_reduce_escalation_reason,
    _volatility_trigger_orphan_recovery_action,
)


class WebSecurityTests(unittest.TestCase):
    def _mock_symbol_config(self) -> dict[str, float]:
        return {
            "tick_size": 0.0001,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }

    def _mock_book(self) -> list[dict[str, str]]:
        return [{"bid_price": "1.2345", "ask_price": "1.2347"}]

    def test_basic_auth_header_matches_expected_credentials(self) -> None:
        token = base64.b64encode(b"grid:secret-pass").decode("ascii")
        self.assertTrue(_basic_auth_header_matches(f"Basic {token}", "grid", "secret-pass"))

    def test_monitor_page_contains_custom_grid_startup_inventory_text(self) -> None:
        self.assertIn("现价启动底仓", MONITOR_PAGE)
        self.assertIn("合约 USDT", MONITOR_PAGE)
        self.assertIn("合约 BNB", MONITOR_PAGE)
        self.assertIn("奖励回本量", MONITOR_PAGE)
        self.assertIn("万3", MONITOR_PAGE)
        self.assertIn("万4", MONITOR_PAGE)
        self.assertIn("万5", MONITOR_PAGE)
        self.assertIn("定位参数", MONITOR_PAGE)
        self.assertIn("生成建议参数", MONITOR_PAGE)
        self.assertIn("data-alert-action", MONITOR_PAGE)

    def test_monitor_page_contains_xaut_adaptive_status_text(self) -> None:
        self.assertIn("XAUT 三态状态", MONITOR_PAGE)
        self.assertIn("XAUT 三态原因", MONITOR_PAGE)

    def test_monitor_page_includes_runtime_guard_inputs(self) -> None:
        self.assertIn('id="monitor_run_start_time"', MONITOR_PAGE)
        self.assertIn('id="monitor_run_end_time"', MONITOR_PAGE)
        self.assertIn('id="monitor_runtime_guard_stats_start_time"', MONITOR_PAGE)
        self.assertIn('id="monitor_rolling_hourly_loss_limit"', MONITOR_PAGE)
        self.assertIn('id="monitor_max_cumulative_notional"', MONITOR_PAGE)
        self.assertIn('id="monitor_volume_trigger_enabled"', MONITOR_PAGE)
        self.assertIn('id="monitor_volume_trigger_window"', MONITOR_PAGE)
        self.assertIn('id="monitor_volume_trigger_start_threshold"', MONITOR_PAGE)
        self.assertIn('id="monitor_volume_trigger_stop_threshold"', MONITOR_PAGE)
        self.assertIn('id="monitor_volatility_trigger_enabled"', MONITOR_PAGE)
        self.assertIn('id="monitor_volatility_trigger_window"', MONITOR_PAGE)
        self.assertIn('id="monitor_volatility_trigger_amplitude_ratio"', MONITOR_PAGE)
        self.assertIn('id="monitor_volatility_trigger_abs_return_ratio"', MONITOR_PAGE)
        self.assertIn('id="monitor_volatility_trigger_stop_reduce_to_notional"', MONITOR_PAGE)
        self.assertIn('id="monitor_volatility_trigger_reduce_max_loss_ratio"', MONITOR_PAGE)
        self.assertIn('id="save_params_btn"', MONITOR_PAGE)

    def test_monitor_page_does_not_reference_undefined_get_selected_symbol(self) -> None:
        if "getSelectedSymbol(" in MONITOR_PAGE:
            self.assertIn("function getSelectedSymbol()", MONITOR_PAGE)

    def test_main_page_does_not_duplicate_symbol_element_declaration(self) -> None:
        needle = 'const symbolEl = document.getElementById("symbol");'
        self.assertEqual(HTML_PAGE.count(needle), 1)

    def test_monitor_page_includes_quick_runner_controls(self) -> None:
        self.assertIn('id="quick_start_last_btn"', MONITOR_PAGE)
        self.assertIn('id="quick_flatten_btn"', MONITOR_PAGE)
        self.assertIn("/api/runner/quick_start_last", MONITOR_PAGE)
        self.assertIn("/api/runner/quick_flatten", MONITOR_PAGE)

    @patch("grid_optimizer.web._quick_flatten_runner_symbol")
    def test_handler_routes_quick_flatten_post(self, mock_quick_flatten) -> None:
        mock_quick_flatten.return_value = {"symbol": "SOONUSDT", "post_stop_actions": {}}
        payload = b'{"symbol":"SOONUSDT"}'
        handler = object.__new__(_Handler)
        handler.path = "/api/runner/quick_flatten"
        handler.headers = {"Content-Length": str(len(payload))}
        handler.rfile = io.BytesIO(payload)
        handler._authorize_request = lambda: True
        handler._send_json = Mock()

        _Handler.do_POST(handler)

        mock_quick_flatten.assert_called_once_with("SOONUSDT")
        handler._send_json.assert_called_once()
        self.assertTrue(handler._send_json.call_args.args[0]["ok"])

    @patch("grid_optimizer.web._start_runner_from_last_config")
    def test_handler_routes_quick_start_last_post(self, mock_start_last) -> None:
        mock_start_last.return_value = {"symbol": "SOONUSDT", "runner": {"config": {}}}
        payload = b'{"symbol":"SOONUSDT"}'
        handler = object.__new__(_Handler)
        handler.path = "/api/runner/quick_start_last"
        handler.headers = {"Content-Length": str(len(payload))}
        handler.rfile = io.BytesIO(payload)
        handler._authorize_request = lambda: True
        handler._send_json = Mock()

        _Handler.do_POST(handler)

        mock_start_last.assert_called_once_with("SOONUSDT")
        handler._send_json.assert_called_once()
        self.assertTrue(handler._send_json.call_args.args[0]["ok"])

    def test_monitor_page_keeps_newline_escape_in_editor_locator(self) -> None:
        self.assertIn('split("\\n").length - 1', MONITOR_PAGE)

    def test_monitor_page_start_button_uses_selected_preset_payload(self) -> None:
        self.assertIn("function buildRunnerPayloadFromEditor(", MONITOR_PAGE)
        self.assertIn("function buildRunnerStartPayload(", MONITOR_PAGE)
        self.assertIn('? startPayload', MONITOR_PAGE)
        self.assertIn("正在按当前选中预设启动策略", MONITOR_PAGE)

    def test_basic_auth_header_rejects_invalid_credentials(self) -> None:
        token = base64.b64encode(b"grid:wrong-pass").decode("ascii")
        self.assertFalse(_basic_auth_header_matches(f"Basic {token}", "grid", "secret-pass"))
        self.assertFalse(_basic_auth_header_matches("Bearer abc", "grid", "secret-pass"))

    def test_parse_allowed_networks_supports_ip_and_cidr(self) -> None:
        networks = _parse_allowed_networks("1.2.3.4,10.0.0.0/24,invalid")
        self.assertEqual(len(networks), 2)
        self.assertTrue(_client_ip_allowed("1.2.3.4", networks))
        self.assertTrue(_client_ip_allowed("10.0.0.8", networks))
        self.assertFalse(_client_ip_allowed("8.8.8.8", networks))

    def test_client_ip_allowed_always_accepts_loopback(self) -> None:
        self.assertTrue(_client_ip_allowed("127.0.0.1", []))
        self.assertTrue(_client_ip_allowed("::1", []))

    def test_runner_preset_payload_applies_quasi_neutral_profile(self) -> None:
        payload = _runner_preset_payload("defensive_quasi_neutral_v1", {"symbol": "NIGHTUSDT"})
        self.assertEqual(payload["strategy_profile"], "defensive_quasi_neutral_v1")
        self.assertEqual(payload["buy_levels"], 6)
        self.assertEqual(payload["sell_levels"], 12)
        self.assertAlmostEqual(payload["base_position_notional"], 160.0)

    def test_runner_preset_payload_applies_synthetic_neutral_profile(self) -> None:
        payload = _runner_preset_payload("synthetic_neutral_v1", {"symbol": "NIGHTUSDT"})
        self.assertEqual(payload["strategy_profile"], "synthetic_neutral_v1")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["max_short_position_notional"], 500.0)

    def test_runner_preset_payload_applies_volume_neutral_ping_pong_profile(self) -> None:
        payload = _runner_preset_payload("volume_neutral_ping_pong_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["strategy_profile"], "volume_neutral_ping_pong_v1")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["startup_entry_multiplier"], 4.0)
        self.assertAlmostEqual(payload["base_position_notional"], 0.0)
        self.assertEqual(payload["neutral_center_interval_minutes"], 15)
        self.assertEqual(payload["threshold_position_notional"], 0.0)
        self.assertEqual(payload["take_profit_min_profit_ratio"], 0.0)
        self.assertTrue(payload["market_bias_enabled"])
        self.assertAlmostEqual(payload["market_bias_max_shift_steps"], 0.75)
        self.assertTrue(payload["market_bias_weak_buy_pause_enabled"])
        self.assertAlmostEqual(payload["market_bias_weak_buy_pause_threshold"], 0.15)
        self.assertTrue(payload["market_bias_strong_short_pause_enabled"])
        self.assertAlmostEqual(payload["market_bias_strong_short_pause_threshold"], 0.15)
        self.assertTrue(payload["market_bias_regime_switch_enabled"])
        self.assertEqual(payload["market_bias_regime_switch_confirm_cycles"], 2)
        self.assertAlmostEqual(payload["market_bias_regime_switch_weak_threshold"], 0.15)
        self.assertAlmostEqual(payload["market_bias_regime_switch_strong_threshold"], 0.15)
        self.assertAlmostEqual(payload["max_position_notional"], 600.0)
        self.assertAlmostEqual(payload["sleep_seconds"], 5.0)

    def test_runner_preset_payload_applies_soon_volume_neutral_ping_pong_profile(self) -> None:
        payload = _runner_preset_payload("soon_volume_neutral_ping_pong_v1", {"symbol": "SOONUSDT"})
        self.assertEqual(payload["strategy_profile"], "soon_volume_neutral_ping_pong_v1")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["symbol"], "SOONUSDT")
        self.assertAlmostEqual(payload["step_price"], 0.0002)
        self.assertEqual(payload["buy_levels"], 12)
        self.assertEqual(payload["sell_levels"], 12)
        self.assertAlmostEqual(payload["per_order_notional"], 30.0)
        self.assertEqual(payload["neutral_center_interval_minutes"], 15)
        self.assertEqual(payload["near_market_entry_max_center_distance_steps"], 4.5)
        self.assertEqual(payload["grid_inventory_rebalance_min_center_distance_steps"], 6.0)
        self.assertEqual(payload["near_market_reentry_confirm_cycles"], 3)
        self.assertEqual(payload["threshold_position_notional"], 0.0)
        self.assertEqual(payload["take_profit_min_profit_ratio"], 0.0003)
        self.assertTrue(payload["adaptive_step_enabled"])
        self.assertEqual(payload["adaptive_step_30s_abs_return_ratio"], 0.001)
        self.assertEqual(payload["adaptive_step_30s_amplitude_ratio"], 0.0016)
        self.assertEqual(payload["adaptive_step_1m_abs_return_ratio"], 0.002)
        self.assertEqual(payload["adaptive_step_1m_amplitude_ratio"], 0.003)
        self.assertEqual(payload["adaptive_step_3m_abs_return_ratio"], 0.003)
        self.assertEqual(payload["adaptive_step_5m_abs_return_ratio"], 0.004)
        self.assertEqual(payload["adaptive_step_max_scale"], 3.0)
        self.assertEqual(payload["adaptive_step_min_per_order_scale"], 0.6)
        self.assertEqual(payload["adaptive_step_min_position_limit_scale"], 0.75)
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertTrue(payload["autotune_min_order_notional_only"])
        self.assertEqual(payload["volatility_trigger_stop_reduce_to_notional"], 150.0)
        self.assertEqual(payload["volatility_trigger_reduce_max_loss_ratio"], 0.015)
        self.assertEqual(payload["volatility_trigger_reduce_escalate_after_seconds"], 900.0)
        self.assertEqual(payload["volatility_trigger_reduce_escalate_abs_return_ratio"], 0.02)
        self.assertTrue(payload["volatility_trigger_stop_close_all_positions"])

    def test_runner_preset_payload_rejects_soon_profile_for_other_symbols(self) -> None:
        with self.assertRaises(ValueError):
            _runner_preset_payload("soon_volume_neutral_ping_pong_v1", {"symbol": "BASEDUSDT"})

    def test_monitor_page_exposes_competition_inventory_grid_preset(self) -> None:
        self.assertIn("合约竞赛库存网格", MONITOR_PAGE)
        self.assertIn("competition_inventory_grid", MONITOR_PAGE)

    def test_runner_preset_payload_applies_competition_inventory_grid_profile(self) -> None:
        payload = _runner_preset_payload("competition_inventory_grid_v1", {"symbol": "BARDUSDT"})
        self.assertEqual(payload["strategy_profile"], "competition_inventory_grid_v1")
        self.assertEqual(payload["strategy_mode"], "competition_inventory_grid")
        self.assertEqual(payload["first_order_multiplier"], 4.0)
        self.assertEqual(payload["threshold_position_notional"], 50.0)
        self.assertEqual(payload["max_order_position_notional"], 80.0)

    def test_runner_preset_summaries_include_competition_inventory_grid_profile(self) -> None:
        keys = {item["key"] for item in _runner_preset_summaries("BARDUSDT")}
        self.assertIn("competition_inventory_grid_v1", keys)

    def test_runner_preset_payload_applies_volatility_defensive_profile(self) -> None:
        payload = _runner_preset_payload("volatility_defensive_v1", {"symbol": "OPNUSDT"})
        self.assertEqual(payload["strategy_profile"], "volatility_defensive_v1")
        self.assertEqual(payload["buy_levels"], 4)
        self.assertEqual(payload["sell_levels"], 12)
        self.assertAlmostEqual(payload["max_position_notional"], 420.0)

    def test_runner_preset_payload_applies_adaptive_profile(self) -> None:
        payload = _runner_preset_payload("adaptive_volatility_v1", {"symbol": "OPNUSDT"})
        self.assertEqual(payload["strategy_profile"], "adaptive_volatility_v1")
        self.assertTrue(payload["auto_regime_enabled"])
        self.assertEqual(payload["auto_regime_confirm_cycles"], 2)

    def test_runner_preset_payload_applies_bard_volume_long_v2_profile(self) -> None:
        payload = _runner_preset_payload("bard_volume_long_v2", {"symbol": "BARDUSDT"})
        self.assertEqual(payload["strategy_profile"], "bard_volume_long_v2")
        self.assertEqual(payload["symbol"], "BARDUSDT")
        self.assertEqual(payload["strategy_mode"], "one_way_long")
        self.assertTrue(payload["flat_start_enabled"])
        self.assertTrue(payload["warm_start_enabled"])
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertTrue(payload["excess_inventory_reduce_only_enabled"])
        self.assertEqual(payload["buy_levels"], 5)
        self.assertEqual(payload["sell_levels"], 11)
        self.assertEqual(payload["base_position_notional"], 120.0)

    def test_runner_preset_payload_rejects_bard_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=BARDUSDT"):
            _runner_preset_payload("bard_volume_long_v2", {"symbol": "NIGHTUSDT"})

    def test_runner_preset_payload_applies_xaut_long_adaptive_profile(self) -> None:
        payload = _runner_preset_payload("xaut_long_adaptive_v1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_long_adaptive_v1")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "one_way_long")
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertEqual(payload["step_price"], 7.5)

    def test_runner_preset_payload_applies_xaut_short_adaptive_profile(self) -> None:
        payload = _runner_preset_payload("xaut_short_adaptive_v1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_short_adaptive_v1")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "one_way_short")
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertEqual(payload["step_price"], 7.4)
        self.assertEqual(payload["per_order_notional"], 60.0)
        self.assertEqual(payload["base_position_notional"], 220.0)
        self.assertEqual(payload["pause_short_position_notional"], 620.0)
        self.assertEqual(payload["max_short_position_notional"], 720.0)
        self.assertEqual(payload["inventory_tier_start_notional"], 320.0)
        self.assertEqual(payload["inventory_tier_end_notional"], 520.0)
        self.assertEqual(payload["inventory_tier_buy_levels"], 14)
        self.assertEqual(payload["inventory_tier_sell_levels"], 3)
        self.assertEqual(payload["inventory_tier_per_order_notional"], 45.0)
        self.assertEqual(payload["inventory_tier_base_position_notional"], 100.0)

    def test_runner_preset_payload_applies_xaut_guarded_ping_pong_profile(self) -> None:
        payload = _runner_preset_payload("xaut_guarded_ping_pong_v1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_guarded_ping_pong_v1")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 2.2)
        self.assertEqual(payload["buy_levels"], 1)
        self.assertEqual(payload["sell_levels"], 1)
        self.assertEqual(payload["per_order_notional"], 100.0)
        self.assertEqual(payload["base_position_notional"], 0.0)
        self.assertTrue(payload["market_bias_enabled"])
        self.assertTrue(payload["adaptive_step_enabled"])
        self.assertTrue(payload["synthetic_trend_follow_enabled"])
        self.assertEqual(payload["synthetic_trend_follow_1m_abs_return_ratio"], 0.00045)
        self.assertEqual(payload["synthetic_trend_follow_1m_amplitude_ratio"], 0.00070)
        self.assertEqual(payload["synthetic_trend_follow_3m_abs_return_ratio"], 0.00090)
        self.assertEqual(payload["synthetic_trend_follow_3m_amplitude_ratio"], 0.00120)
        self.assertEqual(payload["synthetic_trend_follow_min_efficiency_ratio"], 0.58)
        self.assertEqual(payload["synthetic_trend_follow_reverse_delay_seconds"], 18.0)
        self.assertEqual(payload["max_position_notional"], 220.0)
        self.assertEqual(payload["max_short_position_notional"], 220.0)
        self.assertEqual(payload["max_actual_net_notional"], 120.0)
        self.assertFalse(payload["autotune_symbol_enabled"])

    def test_runner_preset_payload_applies_xaut_near_price_guarded_profile(self) -> None:
        payload = _runner_preset_payload("xaut_near_price_guarded_v1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_near_price_guarded_v1")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.02)
        self.assertEqual(payload["static_buy_offset_steps"], 0.5)
        self.assertEqual(payload["static_sell_offset_steps"], 0.5)
        self.assertFalse(payload["market_bias_enabled"])
        self.assertTrue(payload["adaptive_step_enabled"])
        self.assertTrue(payload["synthetic_trend_follow_enabled"])
        self.assertEqual(payload["max_actual_net_notional"], 90.0)
        self.assertFalse(payload["autotune_symbol_enabled"])

    def test_runner_preset_payload_applies_xaut_volume_guarded_bard_v2_profile(self) -> None:
        payload = _runner_preset_payload("xaut_volume_guarded_bard_v2", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_volume_guarded_bard_v2")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 1.0)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 5)
        self.assertEqual(payload["per_order_notional"], 20.0)
        self.assertEqual(payload["synthetic_tiny_long_residual_notional"], 45.0)
        self.assertEqual(payload["synthetic_tiny_short_residual_notional"], 45.0)
        self.assertEqual(payload["pause_buy_position_notional"], 200.0)
        self.assertEqual(payload["pause_short_position_notional"], 200.0)
        self.assertEqual(payload["max_position_notional"], 320.0)
        self.assertEqual(payload["max_short_position_notional"], 320.0)
        self.assertEqual(payload["max_total_notional"], 2200.0)
        self.assertEqual(payload["max_new_orders"], 24)
        self.assertEqual(payload["take_profit_min_profit_ratio"], 0.0001)
        self.assertFalse(payload["adaptive_step_enabled"])
        self.assertFalse(payload["synthetic_trend_follow_enabled"])
        self.assertEqual(payload["leverage"], 10)
        self.assertFalse(payload["autotune_symbol_enabled"])

    def test_normalize_runner_control_payload_supports_volume_trigger_fields(self) -> None:
        payload = _normalize_runner_control_payload(
            {
                "symbol": "BARDUSDT",
                "strategy_profile": "bard_volume_long_v2",
                "volume_trigger_enabled": True,
                "volume_trigger_window": "15m",
                "volume_trigger_start_threshold": 250000,
                "volume_trigger_stop_threshold": 180000,
                "volume_trigger_stop_cancel_open_orders": False,
                "volume_trigger_stop_close_all_positions": True,
            }
        )

        self.assertTrue(payload["volume_trigger_enabled"])
        self.assertEqual(payload["volume_trigger_window"], "15m")
        self.assertEqual(payload["volume_trigger_start_threshold"], 250000)
        self.assertEqual(payload["volume_trigger_stop_threshold"], 180000)
        self.assertTrue(payload["volume_trigger_stop_cancel_open_orders"])
        self.assertTrue(payload["volume_trigger_stop_close_all_positions"])

    def test_normalize_runner_control_payload_supports_volatility_trigger_fields(self) -> None:
        payload = _normalize_runner_control_payload(
            {
                "symbol": "BARDUSDT",
                "strategy_profile": "bard_volume_long_v2",
                "volatility_trigger_enabled": True,
                "volatility_trigger_window": "1h",
                "volatility_trigger_amplitude_ratio": 0.04,
                "volatility_trigger_abs_return_ratio": 0.02,
                "volatility_trigger_stop_reduce_to_notional": 150,
                "volatility_trigger_reduce_max_loss_ratio": 0.015,
                "volatility_trigger_reduce_escalate_after_seconds": 900,
                "volatility_trigger_reduce_escalate_abs_return_ratio": 0.03,
                "volatility_trigger_stop_cancel_open_orders": False,
                "volatility_trigger_stop_close_all_positions": True,
            }
        )

        self.assertTrue(payload["volatility_trigger_enabled"])
        self.assertEqual(payload["volatility_trigger_window"], "1h")
        self.assertEqual(payload["volatility_trigger_amplitude_ratio"], 0.04)
        self.assertEqual(payload["volatility_trigger_abs_return_ratio"], 0.02)
        self.assertEqual(payload["volatility_trigger_stop_reduce_to_notional"], 150)
        self.assertEqual(payload["volatility_trigger_reduce_max_loss_ratio"], 0.015)
        self.assertEqual(payload["volatility_trigger_reduce_escalate_after_seconds"], 900)
        self.assertEqual(payload["volatility_trigger_reduce_escalate_abs_return_ratio"], 0.03)
        self.assertTrue(payload["volatility_trigger_stop_cancel_open_orders"])
        self.assertTrue(payload["volatility_trigger_stop_close_all_positions"])
        self.assertTrue(payload["volatility_trigger_stop_cancel_open_orders"])
        self.assertTrue(payload["volatility_trigger_stop_close_all_positions"])

    def test_build_flatten_command_includes_target_position_notional(self) -> None:
        command = _build_flatten_command(
            {
                "symbol": "SOONUSDT",
                "client_order_prefix": "mfsoon",
                "sleep_seconds": 2.0,
                "recv_window": 5000,
                "max_consecutive_errors": 20,
                "events_jsonl": "output/soonusdt_maker_flatten_events.jsonl",
                "target_position_notional": 150.0,
            }
        )

        self.assertIn("--target-position-notional", command)
        self.assertIn("150.0", command)

    def test_build_flatten_command_requires_explicit_allow_loss_flag(self) -> None:
        base_config = {
            "symbol": "SOONUSDT",
            "client_order_prefix": "mfsoon",
            "sleep_seconds": 2.0,
            "recv_window": 5000,
            "max_consecutive_errors": 20,
            "events_jsonl": "output/soonusdt_maker_flatten_events.jsonl",
        }

        default_command = _build_flatten_command(base_config)
        allow_loss_command = _build_flatten_command(
            {
                **base_config,
                "allow_loss": True,
                "min_profit_ratio": 0.001,
                "max_loss_ratio": 0.015,
            }
        )

        self.assertNotIn("--allow-loss", default_command)
        self.assertNotIn("--min-profit-ratio", default_command)
        self.assertNotIn("--max-loss-ratio", default_command)
        self.assertIn("--allow-loss", allow_loss_command)
        self.assertIn("--min-profit-ratio", allow_loss_command)
        self.assertIn("--max-loss-ratio", allow_loss_command)

    @patch("grid_optimizer.web.load_live_flatten_snapshot")
    @patch("grid_optimizer.web._cancel_symbol_open_orders")
    @patch("grid_optimizer.web.load_binance_api_credentials")
    def test_execute_stop_actions_does_not_mark_blocked_reduce_as_target_reached(
        self,
        mock_creds,
        mock_cancel_orders,
        mock_flatten_snapshot,
    ) -> None:
        mock_creds.return_value = ("key", "secret")
        mock_cancel_orders.return_value = {"attempted": 0, "success": 0, "errors": []}
        mock_flatten_snapshot.return_value = {
            "orders": [],
            "warnings": ["SOONUSDT BOTH maker_flatten 价格 0.173 高于最大亏损保护价 0.1711，已跳过"],
            "short_target_reached": False,
        }

        result = _execute_stop_actions(
            symbol="SOONUSDT",
            cancel_open_orders=True,
            close_all_positions=True,
            close_position_target_notional=150.0,
            close_position_allow_loss=True,
            close_position_max_loss_ratio=0.015,
        )

        self.assertFalse(result["close_target_reached"])
        self.assertTrue(any("仍高于 150.0000U" in warning for warning in result["warnings"]))

    def test_normalize_runner_control_payload_supports_adaptive_step_fields(self) -> None:
        payload = _normalize_runner_control_payload(
            {
                "symbol": "BASEDUSDT",
                "strategy_profile": "based_volume_push_bard_v1",
                "adaptive_step_enabled": True,
                "adaptive_step_30s_abs_return_ratio": 0.0028,
                "adaptive_step_30s_amplitude_ratio": 0.0035,
                "adaptive_step_1m_abs_return_ratio": 0.0045,
                "adaptive_step_1m_amplitude_ratio": 0.0065,
                "adaptive_step_3m_abs_return_ratio": 0.0100,
                "adaptive_step_5m_abs_return_ratio": 0.0140,
                "adaptive_step_max_scale": 3.0,
                "adaptive_step_min_per_order_scale": 0.35,
                "adaptive_step_min_position_limit_scale": 0.45,
            }
        )

        self.assertTrue(payload["adaptive_step_enabled"])
        self.assertEqual(payload["adaptive_step_30s_abs_return_ratio"], 0.0028)
        self.assertEqual(payload["adaptive_step_30s_amplitude_ratio"], 0.0035)
        self.assertEqual(payload["adaptive_step_1m_abs_return_ratio"], 0.0045)
        self.assertEqual(payload["adaptive_step_1m_amplitude_ratio"], 0.0065)
        self.assertEqual(payload["adaptive_step_3m_abs_return_ratio"], 0.0100)
        self.assertEqual(payload["adaptive_step_5m_abs_return_ratio"], 0.0140)
        self.assertEqual(payload["adaptive_step_max_scale"], 3.0)
        self.assertEqual(payload["adaptive_step_min_per_order_scale"], 0.35)
        self.assertEqual(payload["adaptive_step_min_position_limit_scale"], 0.45)

    def test_normalize_runner_control_payload_supports_synthetic_trend_follow_fields(self) -> None:
        payload = _normalize_runner_control_payload(
            {
                "symbol": "XAUTUSDT",
                "strategy_profile": "xaut_guarded_ping_pong_v1",
                "synthetic_trend_follow_enabled": True,
                "synthetic_trend_follow_1m_abs_return_ratio": 0.00045,
                "synthetic_trend_follow_1m_amplitude_ratio": 0.00070,
                "synthetic_trend_follow_3m_abs_return_ratio": 0.00090,
                "synthetic_trend_follow_3m_amplitude_ratio": 0.00120,
                "synthetic_trend_follow_min_efficiency_ratio": 0.58,
                "synthetic_trend_follow_reverse_delay_seconds": 18.0,
            }
        )

        self.assertTrue(payload["synthetic_trend_follow_enabled"])
        self.assertEqual(payload["synthetic_trend_follow_1m_abs_return_ratio"], 0.00045)
        self.assertEqual(payload["synthetic_trend_follow_1m_amplitude_ratio"], 0.00070)
        self.assertEqual(payload["synthetic_trend_follow_3m_abs_return_ratio"], 0.00090)
        self.assertEqual(payload["synthetic_trend_follow_3m_amplitude_ratio"], 0.00120)
        self.assertEqual(payload["synthetic_trend_follow_min_efficiency_ratio"], 0.58)
        self.assertEqual(payload["synthetic_trend_follow_reverse_delay_seconds"], 18.0)

    def test_normalize_runner_control_payload_supports_take_profit_min_profit_ratio(self) -> None:
        payload = _normalize_runner_control_payload(
            {
                "symbol": "BASEDUSDT",
                "strategy_profile": "based_volume_guarded_bard_v2",
                "take_profit_min_profit_ratio": 0.0005,
            }
        )

        self.assertEqual(payload["take_profit_min_profit_ratio"], 0.0005)

    def test_resolve_runner_volume_trigger_action_starts_when_volume_above_threshold(self) -> None:
        decision = _resolve_runner_volume_trigger_action(
            {
                "volume_trigger_enabled": True,
                "volume_trigger_window": "1h",
                "volume_trigger_start_threshold": 100000,
                "volume_trigger_stop_threshold": 50000,
            },
            current_quote_volume=120000,
            runner_running=False,
            flatten_running=False,
        )

        self.assertEqual(decision["action"], "start")
        self.assertEqual(decision["reason"], "volume_above_start_threshold")

    def test_resolve_runner_volume_trigger_action_stops_when_volume_below_threshold(self) -> None:
        decision = _resolve_runner_volume_trigger_action(
            {
                "volume_trigger_enabled": True,
                "volume_trigger_window": "1h",
                "volume_trigger_start_threshold": 100000,
                "volume_trigger_stop_threshold": 50000,
            },
            current_quote_volume=42000,
            runner_running=True,
            flatten_running=False,
        )

        self.assertEqual(decision["action"], "stop")
        self.assertEqual(decision["reason"], "volume_below_stop_threshold")

    def test_resolve_runner_volatility_trigger_action_stops_when_threshold_hit(self) -> None:
        decision = _resolve_runner_volatility_trigger_action(
            {
                "volatility_trigger_enabled": True,
                "volatility_trigger_window": "1h",
                "volatility_trigger_amplitude_ratio": 0.04,
                "volatility_trigger_abs_return_ratio": 0.02,
            },
            current_amplitude_ratio=0.051,
            current_return_ratio=0.018,
            runner_running=True,
            flatten_running=False,
            paused_by_trigger=False,
        )

        self.assertEqual(decision["action"], "stop")
        self.assertEqual(decision["reason"], "volatility_above_threshold")
        self.assertIn("amplitude_above_threshold", decision["matched_reasons"])

    def test_resolve_runner_volatility_trigger_action_resumes_after_cooldown(self) -> None:
        decision = _resolve_runner_volatility_trigger_action(
            {
                "volatility_trigger_enabled": True,
                "volatility_trigger_window": "1h",
                "volatility_trigger_amplitude_ratio": 0.04,
                "volatility_trigger_abs_return_ratio": 0.02,
            },
            current_amplitude_ratio=0.018,
            current_return_ratio=0.007,
            runner_running=False,
            flatten_running=False,
            paused_by_trigger=True,
        )

        self.assertEqual(decision["action"], "start")
        self.assertEqual(decision["reason"], "volatility_back_within_threshold")

    def test_volatility_reduce_does_not_escalate_when_target_already_reached(self) -> None:
        reason = _volatility_reduce_escalation_reason(
            {
                "volatility_trigger_reduce_escalate_after_seconds": 900,
                "volatility_trigger_reduce_escalate_abs_return_ratio": 0.02,
            },
            {
                "phase": "reduce_to_notional",
                "reduce_started_at": "2026-04-21T13:53:03+00:00",
                "reduce_effective": False,
                "reduce_target_reached": True,
                "result": {
                    "post_stop_actions": {
                        "close_attempted_count": 0,
                        "flatten_started": False,
                    }
                },
            },
            checked_at=datetime(2026, 4, 21, 14, 10, 0, tzinfo=timezone.utc),
            current_return_ratio=-0.025,
        )

        self.assertIsNone(reason)

    def test_volatility_orphan_recovery_retries_reduce_when_runner_stopped(self) -> None:
        action = _volatility_trigger_orphan_recovery_action(
            {"volatility_trigger_stop_close_all_positions": True},
            {
                "paused_by_trigger": True,
                "phase": "reduce_to_notional",
                "reduce_target_reached": False,
            },
            signal_hit=True,
            runner_running=False,
            flatten_running=False,
            reduce_target_notional=150.0,
        )

        self.assertEqual(action, {"action": "reduce_to_notional", "reason": "retry_reduce_to_notional"})

    def test_volatility_orphan_recovery_does_not_retry_reached_reduce_target(self) -> None:
        action = _volatility_trigger_orphan_recovery_action(
            {"volatility_trigger_stop_close_all_positions": True},
            {
                "paused_by_trigger": True,
                "phase": "reduce_to_notional",
                "reduce_target_reached": True,
            },
            signal_hit=True,
            runner_running=False,
            flatten_running=False,
            reduce_target_notional=150.0,
        )

        self.assertIsNone(action)

    def test_volatility_orphan_recovery_retries_full_flatten_when_runner_stopped(self) -> None:
        action = _volatility_trigger_orphan_recovery_action(
            {"volatility_trigger_stop_close_all_positions": True},
            {
                "paused_by_trigger": True,
                "phase": "full_flatten",
                "full_flatten_target_reached": False,
            },
            signal_hit=True,
            runner_running=False,
            flatten_running=False,
            reduce_target_notional=150.0,
        )

        self.assertEqual(action, {"action": "full_flatten", "reason": "retry_full_flatten"})

    def test_volatility_reduce_escalates_when_reduce_was_blocked(self) -> None:
        reason = _volatility_reduce_escalation_reason(
            {
                "volatility_trigger_reduce_escalate_after_seconds": 900,
                "volatility_trigger_reduce_escalate_abs_return_ratio": 0.02,
            },
            {
                "phase": "reduce_to_notional",
                "reduce_started_at": "2026-04-21T13:53:03+00:00",
                "reduce_effective": False,
                "reduce_target_reached": False,
            },
            checked_at=datetime(2026, 4, 21, 13, 54, 0, tzinfo=timezone.utc),
            current_return_ratio=-0.025,
        )

        self.assertEqual(reason, "reduce_escalate_abs_return_ratio")

    def test_volatility_reduce_escalates_after_effective_reduce(self) -> None:
        reason = _volatility_reduce_escalation_reason(
            {
                "volatility_trigger_reduce_escalate_after_seconds": 900,
                "volatility_trigger_reduce_escalate_abs_return_ratio": 0.02,
            },
            {
                "phase": "reduce_to_notional",
                "reduce_started_at": "2026-04-21T13:53:03+00:00",
                "reduce_effective": True,
            },
            checked_at=datetime(2026, 4, 21, 13, 54, 0, tzinfo=timezone.utc),
            current_return_ratio=-0.025,
        )

        self.assertEqual(reason, "reduce_escalate_abs_return_ratio")

    def test_runner_preset_payload_rejects_xaut_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _runner_preset_payload("xaut_long_adaptive_v1", {"symbol": "BTCUSDT"})

    def test_runner_preset_payload_keeps_user_overrides(self) -> None:
        payload = _runner_preset_payload(
            "defensive_quasi_neutral_aggressive_v1",
            {
                "symbol": "BARDUSDT",
                "step_price": 0.0001,
                "buy_levels": 4,
                "sell_levels": 8,
                "per_order_notional": 70.0,
                "base_position_notional": 140.0,
                "pause_buy_position_notional": 400.0,
                "max_position_notional": 600.0,
            },
        )
        self.assertEqual(payload["symbol"], "BARDUSDT")
        self.assertEqual(payload["strategy_profile"], "defensive_quasi_neutral_aggressive_v1")
        self.assertEqual(payload["step_price"], 0.0001)
        self.assertEqual(payload["buy_levels"], 4)
        self.assertEqual(payload["sell_levels"], 8)
        self.assertEqual(payload["per_order_notional"], 70.0)
        self.assertEqual(payload["base_position_notional"], 140.0)
        self.assertEqual(payload["pause_buy_position_notional"], 400.0)
        self.assertEqual(payload["max_position_notional"], 600.0)

    def test_load_runner_control_config_normalizes_mismatched_runtime_paths(self) -> None:
        control_path = Path("output/test_katusdt_loop_runner_control.json")
        control_path.parent.mkdir(parents=True, exist_ok=True)
        control_path.write_text(
            json.dumps(
                {
                    "symbol": "KATUSDT",
                    "state_path": "output/nightusdt_loop_state.json",
                    "plan_json": "output/nightusdt_loop_latest_plan.json",
                    "submit_report_json": "output/nightusdt_loop_latest_submit.json",
                    "summary_jsonl": "output/nightusdt_loop_events.jsonl",
                }
            ),
            encoding="utf-8",
        )
        try:
            with patch("grid_optimizer.web._runner_control_path", return_value=control_path), patch(
                "grid_optimizer.web._read_runner_process_for_symbol",
                return_value={},
            ):
                config = _load_runner_control_config("KATUSDT")
            self.assertEqual(config["state_path"], "output/katusdt_loop_state.json")
            self.assertEqual(config["plan_json"], "output/katusdt_loop_latest_plan.json")
            self.assertEqual(config["submit_report_json"], "output/katusdt_loop_latest_submit.json")
            self.assertEqual(config["summary_jsonl"], "output/katusdt_loop_events.jsonl")
        finally:
            control_path.unlink(missing_ok=True)

    def test_symbol_runner_template_disables_legacy_mode(self) -> None:
        with patch.dict("os.environ", {"GRID_RUNNER_SERVICE_TEMPLATE": "grid-loop@{symbol}.service"}):
            self.assertFalse(_uses_legacy_runner("NIGHTUSDT"))
            self.assertEqual(_runner_service_name_for_symbol("NIGHTUSDT"), "grid-loop@NIGHTUSDT.service")

    def test_runner_preset_payload_applies_volume_neutral_target_profile(self) -> None:
        payload = _runner_preset_payload("volume_neutral_target_v1", {"symbol": "OPNUSDT"})
        self.assertEqual(payload["strategy_profile"], "volume_neutral_target_v1")
        self.assertEqual(payload["strategy_mode"], "inventory_target_neutral")
        self.assertEqual(payload["neutral_center_interval_minutes"], 3)
        self.assertEqual(payload["pause_buy_position_notional"], 900.0)
        self.assertEqual(payload["pause_short_position_notional"], 900.0)
        self.assertEqual(payload["max_position_notional"], 900.0)
        self.assertEqual(payload["max_short_position_notional"], 900.0)
        self.assertEqual(payload["max_total_notional"], 1800.0)
        self.assertTrue(payload["neutral_hourly_scale_enabled"])
        self.assertAlmostEqual(payload["neutral_band1_target_ratio"], 0.20)
        self.assertAlmostEqual(payload["neutral_band2_target_ratio"], 0.50)
        self.assertAlmostEqual(payload["neutral_band3_target_ratio"], 1.00)

    def test_runner_preset_payload_applies_xaut_short_profile(self) -> None:
        payload = _runner_preset_payload("xaut_volume_short_v1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_volume_short_v1")
        self.assertEqual(payload["strategy_mode"], "one_way_short")
        self.assertEqual(payload["buy_levels"], 10)
        self.assertEqual(payload["sell_levels"], 10)
        self.assertEqual(payload["per_order_notional"], 100.0)
        self.assertEqual(payload["base_position_notional"], 240.0)
        self.assertEqual(payload["pause_short_position_notional"], 850.0)
        self.assertEqual(payload["max_short_position_notional"], 1000.0)
        self.assertTrue(payload["autotune_symbol_enabled"])

    def test_runner_preset_payload_rejects_xaut_short_profile_for_other_symbol(self) -> None:
        with self.assertRaises(ValueError):
            _runner_preset_payload("xaut_volume_short_v1", {"symbol": "OPNUSDT"})

    def test_runner_preset_payload_rejects_xaut_volume_guarded_bard_v2_for_other_symbol(self) -> None:
        with self.assertRaises(ValueError):
            _runner_preset_payload("xaut_volume_guarded_bard_v2", {"symbol": "OPNUSDT"})

    def test_runner_preset_payload_applies_xaut_competition_profile(self) -> None:
        payload = _runner_preset_payload("xaut_competition_push_neutral_v1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_competition_push_neutral_v1")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.1)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 4)
        self.assertEqual(payload["per_order_notional"], 45.0)
        self.assertEqual(payload["base_position_notional"], 0.0)
        self.assertEqual(payload["sticky_entry_levels"], 2)
        self.assertEqual(payload["synthetic_residual_short_flat_notional"], 30.0)
        self.assertEqual(payload["max_cumulative_notional"], 660000.0)
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertFalse(payload["volume_trigger_enabled"])

    def test_runner_preset_payload_rejects_xaut_competition_profile_for_other_symbol(self) -> None:
        with self.assertRaises(ValueError):
            _runner_preset_payload("xaut_competition_push_neutral_v1", {"symbol": "OPNUSDT"})

    def test_runner_preset_payload_applies_btcusdc_competition_neutral_profile(self) -> None:
        payload = _runner_preset_payload("btcusdc_competition_maker_neutral_v1", {"symbol": "BTCUSDC"})
        self.assertEqual(payload["strategy_profile"], "btcusdc_competition_maker_neutral_v1")
        self.assertEqual(payload["symbol"], "BTCUSDC")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["step_price"], 1.0)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 8)
        self.assertAlmostEqual(payload["per_order_notional"], 120.0)
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 360.0)
        self.assertAlmostEqual(payload["pause_short_position_notional"], 360.0)
        self.assertAlmostEqual(payload["max_position_notional"], 450.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 450.0)
        self.assertAlmostEqual(payload["max_total_notional"], 1800.0)
        self.assertAlmostEqual(payload["max_actual_net_notional"], 300.0)
        self.assertAlmostEqual(payload["max_synthetic_drift_notional"], 160.0)
        self.assertFalse(payload["autotune_symbol_enabled"])

    def test_runner_preset_payload_applies_btcusdc_competition_aggressive_profile(self) -> None:
        payload = _runner_preset_payload("btcusdc_competition_maker_neutral_aggressive_v1", {"symbol": "BTCUSDC"})
        self.assertEqual(payload["strategy_profile"], "btcusdc_competition_maker_neutral_aggressive_v1")
        self.assertEqual(payload["symbol"], "BTCUSDC")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["step_price"], 1.0)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 8)
        self.assertAlmostEqual(payload["per_order_notional"], 180.0)
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 540.0)
        self.assertAlmostEqual(payload["pause_short_position_notional"], 540.0)
        self.assertAlmostEqual(payload["max_position_notional"], 675.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 675.0)
        self.assertAlmostEqual(payload["max_total_notional"], 2700.0)
        self.assertAlmostEqual(payload["max_actual_net_notional"], 420.0)
        self.assertAlmostEqual(payload["max_synthetic_drift_notional"], 220.0)
        self.assertEqual(payload["max_new_orders"], 28)
        self.assertFalse(payload["autotune_symbol_enabled"])

    def test_runner_preset_payload_rejects_btcusdc_competition_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=BTCUSDC"):
            _runner_preset_payload("btcusdc_competition_maker_neutral_v1", {"symbol": "ETHUSDC"})

    def test_runner_preset_payload_applies_ethusdc_um_volume_long_profile(self) -> None:
        payload = _runner_preset_payload("ethusdc_um_volume_long_v1", {"symbol": "ETHUSDC"})
        self.assertEqual(payload["strategy_profile"], "ethusdc_um_volume_long_v1")
        self.assertEqual(payload["symbol"], "ETHUSDC")
        self.assertEqual(payload["strategy_mode"], "one_way_long")
        self.assertAlmostEqual(payload["step_price"], 0.6)
        self.assertEqual(payload["buy_levels"], 6)
        self.assertEqual(payload["sell_levels"], 10)
        self.assertAlmostEqual(payload["per_order_notional"], 100.0)
        self.assertAlmostEqual(payload["base_position_notional"], 250.0)
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 700.0)
        self.assertAlmostEqual(payload["max_position_notional"], 900.0)
        self.assertAlmostEqual(payload["max_total_notional"], 1800.0)
        self.assertEqual(payload["leverage"], 5)
        self.assertAlmostEqual(payload["take_profit_min_profit_ratio"], 0.0)
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["volatility_trigger_stop_close_all_positions"])
        self.assertFalse(payload["volume_trigger_enabled"])
        self.assertFalse(payload["volume_trigger_stop_close_all_positions"])
        self.assertFalse(payload["adverse_reduce_enabled"])
        self.assertFalse(payload["excess_inventory_reduce_only_enabled"])

    def test_runner_preset_payload_rejects_ethusdc_um_volume_long_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=ETHUSDC"):
            _runner_preset_payload("ethusdc_um_volume_long_v1", {"symbol": "BTCUSDC"})

    def test_runner_preset_payload_applies_ethusdc_best_quote_long_profile(self) -> None:
        payload = _runner_preset_payload("ethusdc_best_quote_long_ping_pong_v1", {"symbol": "ETHUSDC"})
        self.assertEqual(payload["strategy_profile"], "ethusdc_best_quote_long_ping_pong_v1")
        self.assertEqual(payload["symbol"], "ETHUSDC")
        self.assertEqual(payload["strategy_mode"], "one_way_long")
        self.assertAlmostEqual(payload["step_price"], 0.01)
        self.assertEqual(payload["buy_levels"], 6)
        self.assertEqual(payload["sell_levels"], 6)
        self.assertAlmostEqual(payload["per_order_notional"], 25.0)
        self.assertAlmostEqual(payload["base_position_notional"], 0.0)
        self.assertFalse(payload["flat_start_enabled"])
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 900.0)
        self.assertAlmostEqual(payload["max_position_notional"], 900.0)
        self.assertAlmostEqual(payload["max_total_notional"], 1800.0)
        self.assertEqual(payload["leverage"], 5)
        self.assertAlmostEqual(payload["take_profit_min_profit_ratio"], 0.0)
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["volatility_trigger_stop_close_all_positions"])
        self.assertFalse(payload["volume_trigger_enabled"])
        self.assertFalse(payload["volume_trigger_stop_close_all_positions"])
        self.assertFalse(payload["adverse_reduce_enabled"])
        self.assertFalse(payload["excess_inventory_reduce_only_enabled"])

    def test_runner_preset_payload_rejects_ethusdc_best_quote_long_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=ETHUSDC"):
            _runner_preset_payload("ethusdc_best_quote_long_ping_pong_v1", {"symbol": "BTCUSDC"})

    def test_runner_preset_payload_applies_btcusdc_best_quote_long_profile(self) -> None:
        payload = _runner_preset_payload("btcusdc_best_quote_long_ping_pong_v1", {"symbol": "BTCUSDC"})
        self.assertEqual(payload["strategy_profile"], "btcusdc_best_quote_long_ping_pong_v1")
        self.assertEqual(payload["symbol"], "BTCUSDC")
        self.assertEqual(payload["strategy_mode"], "one_way_long")
        self.assertAlmostEqual(payload["step_price"], 0.1)
        self.assertEqual(payload["buy_levels"], 6)
        self.assertEqual(payload["sell_levels"], 6)
        self.assertAlmostEqual(payload["per_order_notional"], 120.0)
        self.assertAlmostEqual(payload["base_position_notional"], 0.0)
        self.assertFalse(payload["flat_start_enabled"])
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 900.0)
        self.assertAlmostEqual(payload["max_position_notional"], 1200.0)
        self.assertAlmostEqual(payload["max_total_notional"], 2400.0)
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["volume_trigger_enabled"])
        self.assertFalse(payload["adverse_reduce_enabled"])
        self.assertFalse(payload["excess_inventory_reduce_only_enabled"])

    def test_runner_preset_payload_rejects_btcusdc_best_quote_long_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=BTCUSDC"):
            _runner_preset_payload("btcusdc_best_quote_long_ping_pong_v1", {"symbol": "ETHUSDC"})

    def test_runner_preset_payload_applies_competition_neutral_ping_pong_profiles(self) -> None:
        cases = {
            "soonusdt_competition_neutral_ping_pong_v1": ("SOONUSDT", 30.0, 8),
            "btcusdc_competition_neutral_ping_pong_v1": ("BTCUSDC", 120.0, 6),
            "ethusdc_competition_neutral_ping_pong_v1": ("ETHUSDC", 25.0, 6),
            "xauusdt_competition_neutral_ping_pong_v1": ("XAUUSDT", 40.0, 6),
            "xagusdt_competition_neutral_ping_pong_v1": ("XAGUSDT", 30.0, 6),
            "clusdt_competition_neutral_ping_pong_v1": ("CLUSDT", 40.0, 6),
            "bzusdt_competition_neutral_ping_pong_v1": ("BZUSDT", 40.0, 6),
            "ordiusdc_competition_neutral_ping_pong_v1": ("ORDIUSDC", 25.0, 6),
        }
        for profile, (symbol, per_order, levels) in cases.items():
            with self.subTest(profile=profile):
                payload = _runner_preset_payload(profile, {"symbol": symbol})
                self.assertEqual(payload["strategy_profile"], profile)
                self.assertEqual(payload["symbol"], symbol)
                self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
                self.assertEqual(payload["buy_levels"], levels)
                self.assertEqual(payload["sell_levels"], levels)
                self.assertAlmostEqual(payload["per_order_notional"], per_order)
                self.assertFalse(payload["flat_start_enabled"])
                self.assertFalse(payload["market_bias_enabled"])
                self.assertFalse(payload["adaptive_step_enabled"])
                self.assertAlmostEqual(payload["near_market_entry_max_center_distance_steps"], 999.0)
                self.assertAlmostEqual(payload["take_profit_min_profit_ratio"], 0.0)

    def test_runner_preset_payload_rejects_competition_neutral_ping_pong_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUUSDT"):
            _runner_preset_payload("xauusdt_competition_neutral_ping_pong_v1", {"symbol": "BTCUSDC"})

    @patch("grid_optimizer.web.CUSTOM_RUNNER_PRESETS_PATH", new=Path("output/test_custom_runner_presets.json"))
    def test_runner_preset_payload_normalizes_custom_grid_runtime(self) -> None:
        custom_path = Path("output/test_custom_runner_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            json.dumps(
                {
                    "custom_grid_opn_long": {
                        "key": "custom_grid_opn_long",
                        "label": "OPN 做多网格",
                        "symbol": "OPNUSDT",
                        "kind": "custom_grid",
                        "custom": True,
                        "config": {
                            "symbol": "OPNUSDT",
                            "strategy_mode": "one_way_long",
                            "fixed_center_enabled": True,
                            "fixed_center_roll_enabled": True,
                            "autotune_symbol_enabled": True,
                            "buy_levels": 145,
                            "sell_levels": 5,
                            "per_order_notional": 20.0,
                            "base_position_notional": 54.5,
                            "pause_buy_position_notional": 1396.0,
                            "max_position_notional": 1643.0,
                            "buy_pause_amp_trigger_ratio": 0.0075,
                            "inventory_tier_start_notional": 600.0,
                            "inventory_tier_end_notional": 750.0,
                            "inventory_tier_buy_levels": 4,
                            "inventory_tier_sell_levels": 12,
                            "inventory_tier_per_order_notional": 70.0,
                            "inventory_tier_base_position_notional": 280.0,
                        },
                        "grid_preview_params": {
                            "strategy_direction": "long",
                            "grid_level_mode": "arithmetic",
                            "min_price": 0.15,
                            "max_price": 0.30,
                            "n": 120,
                            "margin_amount": 100.0,
                            "leverage": 10.0,
                        },
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            payload = _runner_preset_payload("custom_grid_opn_long", {"symbol": "OPNUSDT"})
            self.assertEqual(payload["strategy_profile"], "custom_grid_opn_long")
            self.assertEqual(payload["buy_levels"], 145)
            self.assertEqual(payload["sell_levels"], 5)
            self.assertEqual(payload["pause_buy_position_notional"], 1643.0)
            self.assertFalse(payload["fixed_center_enabled"])
            self.assertFalse(payload["fixed_center_roll_enabled"])
            self.assertTrue(payload["custom_grid_enabled"])
            self.assertEqual(payload["custom_grid_direction"], "long")
            self.assertEqual(payload["custom_grid_n"], 120)
            self.assertAlmostEqual(payload["custom_grid_total_notional"], 1000.0)
            self.assertFalse(payload["excess_inventory_reduce_only_enabled"])
            self.assertFalse(payload["auto_regime_enabled"])
            self.assertFalse(payload["neutral_hourly_scale_enabled"])
            self.assertIsNone(payload["inventory_tier_start_notional"])
            self.assertIsNone(payload["inventory_tier_buy_levels"])
            self.assertIsNone(payload["buy_pause_amp_trigger_ratio"])
            self.assertFalse(payload["custom_grid_roll_enabled"])
            self.assertEqual(payload["custom_grid_roll_interval_minutes"], 5)
            self.assertEqual(payload["custom_grid_roll_trade_threshold"], 100)
            self.assertAlmostEqual(payload["custom_grid_roll_upper_distance_ratio"], 0.30)
            self.assertEqual(payload["custom_grid_roll_shift_levels"], 1)
        finally:
            custom_path.unlink(missing_ok=True)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_neutral_hedge_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "ENSOUSDT", "strategy_profile": "neutral_hedge_v1"})
        self.assertEqual(config["strategy_profile"], "neutral_hedge_v1")
        self.assertEqual(config["strategy_mode"], "hedge_neutral")
        self.assertEqual(config["max_short_position_notional"], 500.0)
        self.assertEqual(config["state_path"], "output/ensousdt_loop_state.json")
        self.assertGreater(config["step_price"], 0)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_synthetic_neutral_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "ENSOUSDT", "strategy_profile": "synthetic_neutral_v1"})
        self.assertEqual(config["strategy_profile"], "synthetic_neutral_v1")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_volume_neutral_ping_pong_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "BASEDUSDT", "strategy_profile": "volume_neutral_ping_pong_v1"})
        self.assertEqual(config["strategy_profile"], "volume_neutral_ping_pong_v1")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(config["startup_entry_multiplier"], 4.0)
        self.assertAlmostEqual(config["base_position_notional"], 0.0)
        self.assertEqual(config["neutral_center_interval_minutes"], 15)
        self.assertEqual(config["threshold_position_notional"], 0.0)
        self.assertEqual(config["take_profit_min_profit_ratio"], 0.0)
        self.assertTrue(config["market_bias_enabled"])
        self.assertAlmostEqual(config["market_bias_max_shift_steps"], 0.75)
        self.assertEqual(config["state_path"], "output/basedusdt_loop_state.json")
        self.assertGreater(config["step_price"], 0)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_soon_profile_without_step_autotune(
        self,
        mock_symbol_config,
        mock_book_tickers,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1725", "ask_price": "0.1726"}]
        config = _resolve_runner_start_config(
            {"symbol": "SOONUSDT", "strategy_profile": "soon_volume_neutral_ping_pong_v1"}
        )
        self.assertEqual(config["strategy_profile"], "soon_volume_neutral_ping_pong_v1")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(config["step_price"], 0.0002)
        self.assertEqual(config["buy_levels"], 12)
        self.assertEqual(config["sell_levels"], 12)
        self.assertAlmostEqual(config["per_order_notional"], 30.0)
        self.assertAlmostEqual(config["pause_buy_position_notional"], 220.0)
        self.assertAlmostEqual(config["max_position_notional"], 260.0)
        self.assertEqual(config["volatility_trigger_stop_reduce_to_notional"], 150.0)
        self.assertEqual(config["volatility_trigger_reduce_max_loss_ratio"], 0.015)
        self.assertEqual(config["volatility_trigger_reduce_escalate_after_seconds"], 900.0)
        self.assertEqual(config["volatility_trigger_reduce_escalate_abs_return_ratio"], 0.02)
        self.assertTrue(config["volatility_trigger_stop_close_all_positions"])
        self.assertEqual(config["state_path"], "output/soonusdt_loop_state.json")

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_soon_profile_only_raises_min_order_notional(
        self,
        mock_symbol_config,
        mock_book_tickers,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 40.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "0.1725", "ask_price": "0.1726"}]
        config = _resolve_runner_start_config(
            {"symbol": "SOONUSDT", "strategy_profile": "soon_volume_neutral_ping_pong_v1"}
        )
        self.assertAlmostEqual(config["step_price"], 0.0002)
        self.assertEqual(config["buy_levels"], 12)
        self.assertEqual(config["sell_levels"], 12)
        self.assertAlmostEqual(config["per_order_notional"], 40.0)
        self.assertAlmostEqual(config["pause_buy_position_notional"], 220.0)
        self.assertAlmostEqual(config["max_position_notional"], 260.0)
        self.assertAlmostEqual(config["max_total_notional"], 520.0)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_volatility_defensive_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "OPNUSDT", "strategy_profile": "volatility_defensive_v1"})
        self.assertEqual(config["strategy_profile"], "volatility_defensive_v1")
        self.assertEqual(config["strategy_mode"], "one_way_long")
        self.assertEqual(config["state_path"], "output/opnusdt_loop_state.json")
        self.assertGreaterEqual(config["step_price"], 0.0004)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_adaptive_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "OPNUSDT", "strategy_profile": "adaptive_volatility_v1"})
        self.assertEqual(config["strategy_profile"], "adaptive_volatility_v1")
        self.assertTrue(config["auto_regime_enabled"])
        self.assertEqual(config["state_path"], "output/opnusdt_loop_state.json")

    def test_resolve_runner_start_config_rejects_xaut_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _resolve_runner_start_config({"symbol": "BTCUSDT", "strategy_profile": "xaut_long_adaptive_v1"})

    def test_resolve_runner_start_config_rejects_xaut_guarded_ping_pong_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _resolve_runner_start_config({"symbol": "BTCUSDT", "strategy_profile": "xaut_guarded_ping_pong_v1"})

    def test_resolve_runner_start_config_rejects_xaut_near_price_guarded_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _resolve_runner_start_config({"symbol": "BTCUSDT", "strategy_profile": "xaut_near_price_guarded_v1"})

    def test_resolve_runner_start_config_rejects_xaut_volume_guarded_bard_v2_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _resolve_runner_start_config({"symbol": "BTCUSDT", "strategy_profile": "xaut_volume_guarded_bard_v2"})

    def test_resolve_runner_start_config_rejects_xaut_competition_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _resolve_runner_start_config({"symbol": "BTCUSDT", "strategy_profile": "xaut_competition_push_neutral_v1"})

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_xaut_guarded_ping_pong_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "XAUTUSDT", "strategy_profile": "xaut_guarded_ping_pong_v1"})
        self.assertEqual(config["strategy_profile"], "xaut_guarded_ping_pong_v1")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertEqual(config["state_path"], "output/xautusdt_loop_state.json")
        self.assertAlmostEqual(config["step_price"], 2.2)
        self.assertEqual(config["buy_levels"], 1)
        self.assertEqual(config["sell_levels"], 1)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_xaut_near_price_guarded_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "XAUTUSDT", "strategy_profile": "xaut_near_price_guarded_v1"})
        self.assertEqual(config["strategy_profile"], "xaut_near_price_guarded_v1")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(config["step_price"], 0.02)
        self.assertEqual(config["static_buy_offset_steps"], 0.5)
        self.assertEqual(config["static_sell_offset_steps"], 0.5)
        self.assertEqual(config["state_path"], "output/xautusdt_loop_state.json")
        self.assertTrue(config["adaptive_step_enabled"])
        self.assertFalse(config["market_bias_enabled"])
        self.assertTrue(config["synthetic_trend_follow_enabled"])
        self.assertEqual(config["synthetic_trend_follow_reverse_delay_seconds"], 12.0)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_xaut_volume_guarded_bard_v2_profile(
        self,
        mock_symbol_config,
        mock_book_tickers,
    ) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.1,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "4696.0", "ask_price": "4696.2"}]
        config = _resolve_runner_start_config({"symbol": "XAUTUSDT", "strategy_profile": "xaut_volume_guarded_bard_v2"})
        self.assertEqual(config["strategy_profile"], "xaut_volume_guarded_bard_v2")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertEqual(config["state_path"], "output/xautusdt_loop_state.json")
        self.assertEqual(config["step_price"], 1.0)
        self.assertEqual(config["buy_levels"], 8)
        self.assertEqual(config["sell_levels"], 5)
        self.assertEqual(config["per_order_notional"], 20.0)
        self.assertEqual(config["synthetic_tiny_long_residual_notional"], 45.0)
        self.assertEqual(config["synthetic_tiny_short_residual_notional"], 45.0)
        self.assertEqual(config["pause_buy_position_notional"], 200.0)
        self.assertEqual(config["pause_short_position_notional"], 200.0)
        self.assertEqual(config["max_position_notional"], 320.0)
        self.assertEqual(config["max_short_position_notional"], 320.0)
        self.assertEqual(config["max_total_notional"], 2200.0)
        self.assertEqual(config["leverage"], 10)
        self.assertFalse(config["adaptive_step_enabled"])
        self.assertFalse(config["synthetic_trend_follow_enabled"])
        self.assertFalse(config["autotune_symbol_enabled"])

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_xaut_competition_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "4669.00", "ask_price": "4669.02"}]
        config = _resolve_runner_start_config({"symbol": "XAUTUSDT", "strategy_profile": "xaut_competition_push_neutral_v1"})
        self.assertEqual(config["strategy_profile"], "xaut_competition_push_neutral_v1")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertEqual(config["state_path"], "output/xautusdt_loop_state.json")
        self.assertAlmostEqual(config["step_price"], 0.1)
        self.assertEqual(config["buy_levels"], 8)
        self.assertEqual(config["sell_levels"], 4)
        self.assertEqual(config["sticky_entry_levels"], 2)
        self.assertEqual(config["synthetic_residual_short_flat_notional"], 30.0)
        self.assertFalse(config["autotune_symbol_enabled"])
        self.assertFalse(config["volume_trigger_enabled"])

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_volume_neutral_target_profile(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "OPNUSDT", "strategy_profile": "volume_neutral_target_v1"})
        self.assertEqual(config["strategy_profile"], "volume_neutral_target_v1")
        self.assertEqual(config["strategy_mode"], "inventory_target_neutral")
        self.assertEqual(config["neutral_center_interval_minutes"], 3)
        self.assertEqual(config["pause_buy_position_notional"], 900.0)
        self.assertEqual(config["pause_short_position_notional"], 900.0)
        self.assertEqual(config["max_position_notional"], 900.0)
        self.assertEqual(config["max_short_position_notional"], 900.0)
        self.assertEqual(config["max_total_notional"], 1800.0)
        self.assertTrue(config["neutral_hourly_scale_enabled"])

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_xaut_profile_with_tight_step(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.01,
            "step_size": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": "4669.00", "ask_price": "4669.02"}]
        config = _resolve_runner_start_config({"symbol": "XAUTUSDT", "strategy_profile": "xaut_volume_short_v1"})
        self.assertEqual(config["strategy_profile"], "xaut_volume_short_v1")
        self.assertEqual(config["strategy_mode"], "one_way_short")
        self.assertEqual(config["state_path"], "output/xautusdt_loop_state.json")
        self.assertAlmostEqual(config["step_price"], 0.80)
        self.assertEqual(config["max_short_position_notional"], 1000.0)

    def test_default_runtime_paths_are_symbol_specific(self) -> None:
        paths = _default_runtime_paths_for_symbol("ENSOUSDT")
        self.assertEqual(paths["state_path"], "output/ensousdt_loop_state.json")
        self.assertEqual(paths["plan_json"], "output/ensousdt_loop_latest_plan.json")

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_derives_paths_from_symbol(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config(
            {
                "symbol": "ENSOUSDT",
                "strategy_profile": "defensive_quasi_neutral_v1",
            }
        )
        self.assertEqual(config["symbol"], "ENSOUSDT")
        self.assertEqual(config["strategy_profile"], "defensive_quasi_neutral_v1")
        self.assertEqual(config["state_path"], "output/ensousdt_loop_state.json")
        self.assertEqual(config["summary_jsonl"], "output/ensousdt_loop_events.jsonl")
        self.assertTrue(config["flat_start_enabled"])
        self.assertTrue(config["warm_start_enabled"])

    def test_build_runner_command_resets_state_on_start(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "NIGHTUSDT",
                "strategy_profile": "adaptive_volatility_v1",
                "strategy_mode": "one_way_long",
                "step_price": 0.00002,
                "buy_levels": 8,
                "sell_levels": 8,
                "per_order_notional": 70.0,
                "base_position_notional": 420.0,
                "down_trigger_steps": 4,
                "up_trigger_steps": 6,
                "shift_steps": 4,
                "flat_start_enabled": True,
                "warm_start_enabled": False,
                "auto_regime_enabled": True,
                "auto_regime_confirm_cycles": 2,
                "auto_regime_stable_15m_max_amplitude_ratio": 0.02,
                "auto_regime_stable_60m_max_amplitude_ratio": 0.05,
                "auto_regime_stable_60m_return_floor_ratio": -0.01,
                "auto_regime_defensive_15m_amplitude_ratio": 0.035,
                "auto_regime_defensive_60m_amplitude_ratio": 0.08,
                "auto_regime_defensive_15m_return_ratio": -0.015,
                "auto_regime_defensive_60m_return_ratio": -0.03,
                "neutral_center_interval_minutes": 3,
                "neutral_band1_offset_ratio": 0.005,
                "neutral_band2_offset_ratio": 0.01,
                "neutral_band3_offset_ratio": 0.02,
                "neutral_band1_target_ratio": 0.20,
                "neutral_band2_target_ratio": 0.50,
                "neutral_band3_target_ratio": 1.00,
                "neutral_hourly_scale_enabled": True,
                "neutral_hourly_scale_stable": 1.0,
                "neutral_hourly_scale_transition": 0.85,
                "neutral_hourly_scale_defensive": 0.65,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 20,
                "max_total_notional": 1000.0,
                "sleep_seconds": 15,
                "state_path": "output/night_loop_state.json",
                "plan_json": "output/night_loop_latest_plan.json",
                "submit_report_json": "output/night_loop_latest_submit.json",
                "summary_jsonl": "output/night_loop_events.jsonl",
                "center_price": 1.2345,
                "fixed_center_enabled": True,
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--strategy-profile", command)
        self.assertIn("adaptive_volatility_v1", command)
        self.assertIn("--auto-regime-enabled", command)
        self.assertIn("--neutral-center-interval-minutes", command)
        self.assertIn("--neutral-hourly-scale-enabled", command)
        self.assertIn("--center-price", command)
        self.assertIn("--flat-start-enabled", command)
        self.assertIn("--no-warm-start-enabled", command)
        self.assertIn("--fixed-center-enabled", command)
        self.assertIn("--reset-state", command)

    def test_build_runner_command_includes_custom_grid_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "KATUSDT",
                "strategy_profile": "custom_grid_katusdt_demo",
                "strategy_mode": "one_way_long",
                "step_price": 0.00001,
                "buy_levels": 10,
                "sell_levels": 10,
                "per_order_notional": 10.0,
                "base_position_notional": 100.0,
                "custom_grid_enabled": True,
                "custom_grid_direction": "long",
                "custom_grid_level_mode": "arithmetic",
                "custom_grid_min_price": 0.00995,
                "custom_grid_max_price": 0.01115,
                "custom_grid_n": 120,
                "custom_grid_total_notional": 1000.0,
                "margin_type": "KEEP",
                "leverage": 10,
                "max_new_orders": 200,
                "max_total_notional": 1200.0,
                "state_path": "output/katusdt_loop_state.json",
                "plan_json": "output/katusdt_loop_plan.json",
                "submit_report_json": "output/katusdt_loop_submit.json",
                "summary_jsonl": "output/katusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--custom-grid-enabled", command)
        self.assertIn("--custom-grid-direction", command)
        self.assertIn("--custom-grid-min-price", command)
        self.assertIn("--custom-grid-total-notional", command)

    def test_build_runner_command_includes_custom_grid_roll_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "KATUSDT",
                "strategy_profile": "custom_grid_katusdt_demo",
                "strategy_mode": "one_way_long",
                "step_price": 0.00001,
                "buy_levels": 10,
                "sell_levels": 10,
                "per_order_notional": 10.0,
                "base_position_notional": 100.0,
                "custom_grid_enabled": True,
                "custom_grid_direction": "long",
                "custom_grid_level_mode": "arithmetic",
                "custom_grid_min_price": 0.00995,
                "custom_grid_max_price": 0.01115,
                "custom_grid_n": 120,
                "custom_grid_total_notional": 1000.0,
                "custom_grid_roll_enabled": True,
                "custom_grid_roll_interval_minutes": 5,
                "custom_grid_roll_trade_threshold": 100,
                "custom_grid_roll_upper_distance_ratio": 0.30,
                "custom_grid_roll_shift_levels": 1,
                "margin_type": "KEEP",
                "leverage": 10,
                "max_new_orders": 200,
                "max_total_notional": 1200.0,
                "state_path": "output/katusdt_loop_state.json",
                "plan_json": "output/katusdt_loop_plan.json",
                "submit_report_json": "output/katusdt_loop_submit.json",
                "summary_jsonl": "output/katusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--custom-grid-roll-enabled", command)
        self.assertIn("--custom-grid-roll-interval-minutes", command)
        self.assertIn("--custom-grid-roll-trade-threshold", command)
        self.assertIn("--custom-grid-roll-upper-distance-ratio", command)
        self.assertIn("--custom-grid-roll-shift-levels", command)

    def test_build_runner_command_supports_one_way_short(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "OPNUSDT",
                "strategy_profile": "custom_grid_opn_short",
                "strategy_mode": "one_way_short",
                "step_price": 0.001,
                "buy_levels": 3,
                "sell_levels": 5,
                "per_order_notional": 50.0,
                "base_position_notional": 120.0,
                "center_price": 0.25,
                "fixed_center_enabled": True,
                "down_trigger_steps": 4,
                "up_trigger_steps": 4,
                "shift_steps": 2,
                "pause_short_position_notional": 260.0,
                "max_short_position_notional": 300.0,
                "short_cover_pause_amp_trigger_ratio": 0.004,
                "short_cover_pause_down_return_trigger_ratio": -0.0018,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 20,
                "max_total_notional": 300.0,
                "sleep_seconds": 15,
                "state_path": "output/opnusdt_loop_state.json",
                "plan_json": "output/opnusdt_loop_latest_plan.json",
                "submit_report_json": "output/opnusdt_loop_latest_submit.json",
                "summary_jsonl": "output/opnusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("one_way_short", command)
        self.assertIn("--pause-short-position-notional", command)
        self.assertIn("--max-short-position-notional", command)
        self.assertIn("--short-cover-pause-amp-trigger-ratio", command)
        self.assertIn("--short-cover-pause-down-return-trigger-ratio", command)

    def test_build_runner_command_includes_startup_entry_multiplier(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "BASEDUSDT",
                "strategy_profile": "volume_neutral_ping_pong_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.0001,
                "buy_levels": 4,
                "sell_levels": 4,
                "per_order_notional": 45.0,
                "startup_entry_multiplier": 4.0,
                "base_position_notional": 0.0,
                "pause_buy_position_notional": 500.0,
                "pause_short_position_notional": 500.0,
                "max_position_notional": 600.0,
                "max_short_position_notional": 600.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 20,
                "max_total_notional": 1200.0,
                "sleep_seconds": 5,
                "state_path": "output/basedusdt_loop_state.json",
                "plan_json": "output/basedusdt_loop_latest_plan.json",
                "submit_report_json": "output/basedusdt_loop_latest_submit.json",
                "summary_jsonl": "output/basedusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--startup-entry-multiplier", command)
        self.assertIn("4.0", command)

    def test_build_runner_command_includes_near_market_rebalance_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "SOONUSDT",
                "strategy_profile": "soon_volume_neutral_ping_pong_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.0002,
                "buy_levels": 12,
                "sell_levels": 12,
                "per_order_notional": 30.0,
                "startup_entry_multiplier": 1.0,
                "base_position_notional": 0.0,
                "near_market_entry_max_center_distance_steps": 4.5,
                "grid_inventory_rebalance_min_center_distance_steps": 6.0,
                "near_market_reentry_confirm_cycles": 3,
                "pause_buy_position_notional": 220.0,
                "pause_short_position_notional": 220.0,
                "max_position_notional": 260.0,
                "max_short_position_notional": 260.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 30,
                "max_total_notional": 520.0,
                "sleep_seconds": 5,
                "state_path": "output/soonusdt_loop_state.json",
                "plan_json": "output/soonusdt_loop_latest_plan.json",
                "submit_report_json": "output/soonusdt_loop_latest_submit.json",
                "summary_jsonl": "output/soonusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--near-market-entry-max-center-distance-steps", command)
        self.assertIn("4.5", command)
        self.assertIn("--grid-inventory-rebalance-min-center-distance-steps", command)
        self.assertIn("6.0", command)
        self.assertIn("--near-market-reentry-confirm-cycles", command)
        self.assertIn("3", command)

    def test_build_runner_command_includes_market_bias_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "BASEDUSDT",
                "strategy_profile": "volume_neutral_ping_pong_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.0002,
                "buy_levels": 4,
                "sell_levels": 4,
                "per_order_notional": 45.0,
                "startup_entry_multiplier": 4.0,
                "base_position_notional": 0.0,
                "market_bias_enabled": True,
                "market_bias_max_shift_steps": 0.75,
                "market_bias_signal_steps": 2.0,
                "market_bias_drift_weight": 0.65,
                "market_bias_return_weight": 0.35,
                "market_bias_weak_buy_pause_enabled": True,
                "market_bias_weak_buy_pause_threshold": 0.15,
                "market_bias_strong_short_pause_enabled": True,
                "market_bias_strong_short_pause_threshold": 0.15,
                "market_bias_regime_switch_enabled": True,
                "market_bias_regime_switch_confirm_cycles": 2,
                "market_bias_regime_switch_weak_threshold": 0.15,
                "market_bias_regime_switch_strong_threshold": 0.15,
                "pause_buy_position_notional": 500.0,
                "pause_short_position_notional": 500.0,
                "max_position_notional": 600.0,
                "max_short_position_notional": 600.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 20,
                "max_total_notional": 1200.0,
                "sleep_seconds": 5,
                "state_path": "output/basedusdt_loop_state.json",
                "plan_json": "output/basedusdt_loop_latest_plan.json",
                "submit_report_json": "output/basedusdt_loop_latest_submit.json",
                "summary_jsonl": "output/basedusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--market-bias-enabled", command)
        self.assertIn("--market-bias-max-shift-steps", command)
        self.assertIn("--market-bias-signal-steps", command)
        self.assertIn("--market-bias-drift-weight", command)
        self.assertIn("--market-bias-return-weight", command)
        self.assertIn("--market-bias-weak-buy-pause-enabled", command)
        self.assertIn("--market-bias-weak-buy-pause-threshold", command)
        self.assertIn("--market-bias-strong-short-pause-enabled", command)
        self.assertIn("--market-bias-strong-short-pause-threshold", command)
        self.assertIn("--market-bias-regime-switch-enabled", command)
        self.assertIn("--market-bias-regime-switch-confirm-cycles", command)
        self.assertIn("--market-bias-regime-switch-weak-threshold", command)
        self.assertIn("--market-bias-regime-switch-strong-threshold", command)

    def test_build_runner_command_includes_adaptive_step_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "BASEDUSDT",
                "strategy_profile": "based_volume_push_bard_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.0001,
                "buy_levels": 8,
                "sell_levels": 8,
                "per_order_notional": 100.0,
                "base_position_notional": 0.0,
                "adaptive_step_enabled": True,
                "adaptive_step_30s_abs_return_ratio": 0.0028,
                "adaptive_step_30s_amplitude_ratio": 0.0035,
                "adaptive_step_1m_abs_return_ratio": 0.0045,
                "adaptive_step_1m_amplitude_ratio": 0.0065,
                "adaptive_step_3m_abs_return_ratio": 0.0100,
                "adaptive_step_5m_abs_return_ratio": 0.0140,
                "adaptive_step_max_scale": 3.0,
                "adaptive_step_min_per_order_scale": 0.35,
                "adaptive_step_min_position_limit_scale": 0.45,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 40,
                "max_total_notional": 3600.0,
                "sleep_seconds": 3,
                "state_path": "output/basedusdt_loop_state.json",
                "plan_json": "output/basedusdt_loop_latest_plan.json",
                "submit_report_json": "output/basedusdt_loop_latest_submit.json",
                "summary_jsonl": "output/basedusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )

        self.assertIn("--adaptive-step-enabled", command)
        self.assertIn("--adaptive-step-30s-abs-return-ratio", command)
        self.assertIn("--adaptive-step-30s-amplitude-ratio", command)
        self.assertIn("--adaptive-step-1m-abs-return-ratio", command)
        self.assertIn("--adaptive-step-1m-amplitude-ratio", command)
        self.assertIn("--adaptive-step-3m-abs-return-ratio", command)
        self.assertIn("--adaptive-step-5m-abs-return-ratio", command)
        self.assertIn("--adaptive-step-max-scale", command)
        self.assertIn("--adaptive-step-min-per-order-scale", command)
        self.assertIn("--adaptive-step-min-position-limit-scale", command)

    def test_build_runner_command_includes_synthetic_trend_follow_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "XAUTUSDT",
                "strategy_profile": "xaut_guarded_ping_pong_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 2.2,
                "buy_levels": 1,
                "sell_levels": 1,
                "per_order_notional": 100.0,
                "base_position_notional": 0.0,
                "synthetic_trend_follow_enabled": True,
                "synthetic_trend_follow_1m_abs_return_ratio": 0.00045,
                "synthetic_trend_follow_1m_amplitude_ratio": 0.00070,
                "synthetic_trend_follow_3m_abs_return_ratio": 0.00090,
                "synthetic_trend_follow_3m_amplitude_ratio": 0.00120,
                "synthetic_trend_follow_min_efficiency_ratio": 0.58,
                "synthetic_trend_follow_reverse_delay_seconds": 18.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 6,
                "max_total_notional": 260.0,
                "sleep_seconds": 3.0,
                "state_path": "output/xautusdt_loop_state.json",
                "plan_json": "output/xautusdt_loop_latest_plan.json",
                "submit_report_json": "output/xautusdt_loop_latest_submit.json",
                "summary_jsonl": "output/xautusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
            }
        )
        self.assertIn("--synthetic-trend-follow-enabled", command)
        self.assertIn("--synthetic-trend-follow-1m-abs-return-ratio", command)
        self.assertIn("--synthetic-trend-follow-1m-amplitude-ratio", command)
        self.assertIn("--synthetic-trend-follow-3m-abs-return-ratio", command)
        self.assertIn("--synthetic-trend-follow-3m-amplitude-ratio", command)
        self.assertIn("--synthetic-trend-follow-min-efficiency-ratio", command)
        self.assertIn("--synthetic-trend-follow-reverse-delay-seconds", command)

    def test_build_runner_command_includes_static_quote_offset_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "XAUTUSDT",
                "strategy_profile": "xaut_near_price_guarded_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.02,
                "buy_levels": 1,
                "sell_levels": 1,
                "per_order_notional": 80.0,
                "startup_entry_multiplier": 1.0,
                "base_position_notional": 0.0,
                "static_buy_offset_steps": 0.5,
                "static_sell_offset_steps": 0.5,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 4,
                "max_total_notional": 220.0,
                "sleep_seconds": 2.0,
                "state_path": "output/xautusdt_loop_state.json",
                "plan_json": "output/xautusdt_loop_latest_plan.json",
                "submit_report_json": "output/xautusdt_loop_latest_submit.json",
                "summary_jsonl": "output/xautusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": False,
                "reset_state": True,
            }
        )

        self.assertIn("--static-buy-offset-steps", command)
        self.assertIn("0.5", command)
        self.assertIn("--static-sell-offset-steps", command)

    def test_build_runner_command_includes_sticky_entry_levels(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "XAUTUSDT",
                "strategy_profile": "xaut_competition_push_neutral_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.1,
                "buy_levels": 8,
                "sell_levels": 4,
                "per_order_notional": 45.0,
                "startup_entry_multiplier": 1.0,
                "base_position_notional": 0.0,
                "sticky_entry_levels": 2,
                "synthetic_residual_short_flat_notional": 30.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 40,
                "max_total_notional": 3600.0,
                "sleep_seconds": 3.0,
                "state_path": "output/xautusdt_loop_state.json",
                "plan_json": "output/xautusdt_loop_latest_plan.json",
                "submit_report_json": "output/xautusdt_loop_latest_submit.json",
                "summary_jsonl": "output/xautusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": False,
                "reset_state": True,
            }
        )

        self.assertIn("--sticky-entry-levels", command)
        sticky_index = command.index("--sticky-entry-levels")
        self.assertEqual(command[sticky_index + 1], "2")
        self.assertIn("--synthetic-residual-short-flat-notional", command)

    def test_build_runner_command_includes_synthetic_tiny_residual_thresholds(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "XAUTUSDT",
                "strategy_profile": "xaut_volume_guarded_bard_v2",
                "strategy_mode": "synthetic_neutral",
                "step_price": 1.0,
                "buy_levels": 8,
                "sell_levels": 5,
                "per_order_notional": 20.0,
                "synthetic_tiny_long_residual_notional": 45.0,
                "synthetic_tiny_short_residual_notional": 45.0,
                "startup_entry_multiplier": 1.0,
                "base_position_notional": 0.0,
                "margin_type": "KEEP",
                "leverage": 10,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 32,
                "max_total_notional": 2200.0,
                "sleep_seconds": 4.0,
                "state_path": "output/xautusdt_loop_state.json",
                "plan_json": "output/xautusdt_loop_latest_plan.json",
                "submit_report_json": "output/xautusdt_loop_latest_submit.json",
                "summary_jsonl": "output/xautusdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": False,
                "reset_state": False,
            }
        )

        self.assertIn("--synthetic-tiny-long-residual-notional", command)
        long_index = command.index("--synthetic-tiny-long-residual-notional")
        self.assertEqual(command[long_index + 1], "45.0")
        self.assertIn("--synthetic-tiny-short-residual-notional", command)
        short_index = command.index("--synthetic-tiny-short-residual-notional")
        self.assertEqual(command[short_index + 1], "45.0")

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_keeps_runtime_guard_fields(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config(
            {
                "symbol": "ENSOUSDT",
                "strategy_profile": "defensive_quasi_neutral_v1",
                "run_start_time": "2026-03-31T01:00:00+00:00",
                "run_end_time": "2026-03-31T03:00:00+00:00",
                "runtime_guard_stats_start_time": "2026-03-31T02:00:00+00:00",
                "rolling_hourly_loss_limit": 150.0,
                "max_cumulative_notional": 100000.0,
            }
        )
        self.assertEqual(config["run_start_time"], "2026-03-31T01:00:00+00:00")
        self.assertEqual(config["run_end_time"], "2026-03-31T03:00:00+00:00")
        self.assertEqual(config["runtime_guard_stats_start_time"], "2026-03-31T02:00:00+00:00")
        self.assertEqual(config["rolling_hourly_loss_limit"], 150.0)
        self.assertEqual(config["max_cumulative_notional"], 100000.0)

    def test_runtime_guard_input_summary_reads_trade_and_income_audits(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "bardusdt_loop_events.jsonl"
            (root / "bardusdt_loop_trade_audit.jsonl").write_text(
                json.dumps(
                    {
                        "price": "2.5",
                        "qty": "10",
                        "time": int(datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                        "realizedPnl": "1.2",
                        "commission": "0.2",
                        "commissionAsset": "USDT",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "bardusdt_loop_income_audit.jsonl").write_text(
                json.dumps(
                    {
                        "time": int(datetime(2026, 4, 5, 0, 5, tzinfo=timezone.utc).timestamp() * 1000),
                        "income": "0.8",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            events_path.write_text("", encoding="utf-8")

            gross, pnl_events, stats_start_time = _runtime_guard_input_summary(events_path)

            self.assertAlmostEqual(gross, 25.0, places=8)
            self.assertEqual(len(pnl_events), 2)
            self.assertIsNone(stats_start_time)

    @patch("grid_optimizer.runtime_guards.resolve_active_competition_board")
    def test_runtime_guard_input_summary_uses_active_competition_phase_start(
        self,
        mock_board,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "bardusdt_loop_events.jsonl"
            phase_start = datetime(2026, 4, 5, 1, 0, tzinfo=timezone.utc)
            before_phase_ms = int(datetime(2026, 4, 5, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)
            after_phase_ms = int(datetime(2026, 4, 5, 1, 30, tzinfo=timezone.utc).timestamp() * 1000)
            (root / "bardusdt_loop_trade_audit.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "price": "10",
                                "qty": "10",
                                "time": before_phase_ms,
                                "realizedPnl": "1",
                                "commission": "0.1",
                                "commissionAsset": "USDT",
                            },
                            ensure_ascii=False,
                        ),
                        json.dumps(
                            {
                                "price": "20",
                                "qty": "5",
                                "time": after_phase_ms,
                                "realizedPnl": "2",
                                "commission": "0.2",
                                "commissionAsset": "USDT",
                            },
                            ensure_ascii=False,
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "bardusdt_loop_income_audit.jsonl").write_text(
                json.dumps(
                    {
                        "time": after_phase_ms,
                        "income": "0.8",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            events_path.write_text("", encoding="utf-8")
            mock_board.return_value = {
                "symbol": "BARD",
                "market": "futures",
                "activity_start_at": phase_start.isoformat(),
                "activity_end_at": datetime(2026, 4, 15, 1, 0, tzinfo=timezone.utc).isoformat(),
            }

            gross, pnl_events, stats_start_time = _runtime_guard_input_summary(
                events_path,
                symbol="BARDUSDT",
                now=datetime(2026, 4, 5, 2, 0, tzinfo=timezone.utc),
            )

            self.assertAlmostEqual(gross, 100.0, places=8)
            self.assertEqual(len(pnl_events), 2)
            self.assertEqual(stats_start_time, phase_start)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_start_runner_process_blocks_when_cumulative_notional_already_hit(
        self,
        mock_symbol_config,
        mock_book_tickers,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "bardusdt_loop_events.jsonl"
            (root / "bardusdt_loop_trade_audit.jsonl").write_text(
                json.dumps(
                    {
                        "price": "10",
                        "qty": "20",
                        "time": int(datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                        "realizedPnl": "0",
                        "commission": "0",
                        "commissionAsset": "USDT",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "bardusdt_loop_income_audit.jsonl").write_text("", encoding="utf-8")
            events_path.write_text("", encoding="utf-8")
            mock_symbol_config.return_value = self._mock_symbol_config()
            mock_book_tickers.return_value = self._mock_book()

            config = _resolve_runner_start_config(
                {
                    "strategy_profile": "bard_12h_push_neutral_v2",
                    "symbol": "BARDUSDT",
                    "summary_jsonl": str(events_path),
                    "max_cumulative_notional": 150.0,
                }
            )

            with self.assertRaisesRegex(ValueError, "启动前风控预检已拦截"):
                _start_runner_process(config)

    def test_build_runner_command_includes_runtime_guard_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "ENSOUSDT",
                "strategy_profile": "defensive_quasi_neutral_v1",
                "strategy_mode": "one_way_long",
                "step_price": 0.0001,
                "buy_levels": 4,
                "sell_levels": 8,
                "per_order_notional": 50.0,
                "base_position_notional": 100.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 20,
                "max_total_notional": 500.0,
                "sleep_seconds": 15,
                "state_path": "output/ensousdt_loop_state.json",
                "plan_json": "output/ensousdt_loop_latest_plan.json",
                "submit_report_json": "output/ensousdt_loop_latest_submit.json",
                "summary_jsonl": "output/ensousdt_loop_events.jsonl",
                "cancel_stale": True,
                "apply": True,
                "reset_state": True,
                "run_start_time": "2026-03-31T01:00:00+00:00",
                "run_end_time": "2026-03-31T03:00:00+00:00",
                "runtime_guard_stats_start_time": "2026-03-31T02:00:00+00:00",
                "rolling_hourly_loss_limit": 150.0,
                "max_cumulative_notional": 100000.0,
            }
        )
        self.assertIn("--run-start-time", command)
        self.assertIn("2026-03-31T01:00:00+00:00", command)
        self.assertIn("--run-end-time", command)
        self.assertIn("--runtime-guard-stats-start-time", command)
        self.assertIn("2026-03-31T02:00:00+00:00", command)
        self.assertIn("--rolling-hourly-loss-limit", command)
        self.assertIn("--max-cumulative-notional", command)

    def test_build_runner_command_includes_adverse_reduce_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "SOONUSDT",
                "strategy_profile": "soon_volume_neutral_ping_pong_v1",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.0003,
                "buy_levels": 12,
                "sell_levels": 12,
                "per_order_notional": 90.0,
                "base_position_notional": 0.0,
                "adverse_reduce_enabled": True,
                "adverse_reduce_short_trigger_ratio": 0.008,
                "adverse_reduce_long_trigger_ratio": 0.01,
                "adverse_reduce_target_ratio": 0.65,
                "adverse_reduce_maker_timeout_seconds": 20.0,
                "adverse_reduce_max_order_notional": 240.0,
                "adverse_reduce_keep_probe_scale": 0.08,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 40,
                "max_total_notional": 1800.0,
                "sleep_seconds": 4,
                "state_path": "output/soonusdt_loop_state.json",
                "plan_json": "output/soonusdt_loop_latest_plan.json",
                "submit_report_json": "output/soonusdt_loop_latest_submit.json",
                "summary_jsonl": "output/soonusdt_loop_events.jsonl",
            }
        )

        self.assertIn("--adverse-reduce-enabled", command)
        self.assertIn("--adverse-reduce-short-trigger-ratio", command)
        self.assertIn("0.008", command)
        self.assertIn("--adverse-reduce-long-trigger-ratio", command)
        self.assertIn("--adverse-reduce-target-ratio", command)
        self.assertIn("--adverse-reduce-maker-timeout-seconds", command)
        self.assertIn("--adverse-reduce-max-order-notional", command)
        self.assertIn("--adverse-reduce-keep-probe-scale", command)

    def test_build_runner_command_includes_exposure_escalation_arguments(self) -> None:
        command = _build_runner_command(
            {
                "symbol": "SOONUSDT",
                "strategy_profile": "volume_long_v4",
                "strategy_mode": "one_way_long",
                "step_price": 0.0003,
                "buy_levels": 12,
                "sell_levels": 12,
                "per_order_notional": 100.0,
                "base_position_notional": 0.0,
                "exposure_escalation_enabled": True,
                "exposure_escalation_notional": 1000.0,
                "exposure_escalation_hold_seconds": 600.0,
                "exposure_escalation_target_notional": 650.0,
                "exposure_escalation_max_loss_ratio": 0.012,
                "exposure_escalation_hard_unrealized_loss_limit": 60.0,
                "margin_type": "KEEP",
                "leverage": 2,
                "max_plan_age_seconds": 30,
                "max_mid_drift_steps": 4.0,
                "maker_retries": 2,
                "max_new_orders": 40,
                "max_total_notional": 1800.0,
                "sleep_seconds": 4,
                "state_path": "output/soonusdt_loop_state.json",
                "plan_json": "output/soonusdt_loop_latest_plan.json",
                "submit_report_json": "output/soonusdt_loop_latest_submit.json",
                "summary_jsonl": "output/soonusdt_loop_events.jsonl",
            }
        )

        self.assertIn("--exposure-escalation-enabled", command)
        self.assertIn("--exposure-escalation-notional", command)
        self.assertIn("1000.0", command)
        self.assertIn("--exposure-escalation-hold-seconds", command)
        self.assertIn("--exposure-escalation-target-notional", command)
        self.assertIn("--exposure-escalation-max-loss-ratio", command)
        self.assertIn("--exposure-escalation-hard-unrealized-loss-limit", command)

    @patch("grid_optimizer.web._run_grid_preview")
    def test_build_custom_grid_runner_preset_creates_symbol_bound_preset(self, mock_preview) -> None:
        mock_preview.return_value = {
            "ok": True,
            "summary": {
                "symbol": "OPNUSDT",
                "strategy_direction": "neutral",
                "grid_count": 12,
                "current_price": 0.25,
                "position_budget_notional": 1000.0,
                "neutral_anchor_price": 0.252,
                "long_grid_count": 6,
                "short_grid_count": 6,
                "startup_long_notional": 120.0,
                "startup_short_notional": 80.0,
                "full_long_entry_notional": 500.0,
                "full_short_entry_notional": 500.0,
                "symbol_info": {
                    "min_notional": 5.0,
                    "min_qty": 1.0,
                },
            },
            "rows": [
                {"entry_notional": 11.25},
                {"entry_notional": 11.25},
                {"entry_notional": 11.25},
            ],
        }
        built = _build_custom_grid_runner_preset(
            {
                "contract_type": "usdm",
                "symbol": "OPNUSDT",
                "name": "OPN 中性固定网格",
                "strategy_direction": "neutral",
                "grid_level_mode": "arithmetic",
                "min_price": 0.20,
                "max_price": 0.32,
                "n": 12,
                "margin_amount": 500.0,
                "leverage": 2.0,
            }
        )
        self.assertEqual(built["preset"]["symbol"], "OPNUSDT")
        self.assertTrue(built["preset"]["custom"])
        self.assertEqual(built["preset"]["config"]["strategy_mode"], "synthetic_neutral")
        self.assertTrue(built["preset"]["config"]["custom_grid_enabled"])
        self.assertFalse(built["preset"]["config"]["fixed_center_enabled"])
        self.assertFalse(built["preset"]["config"]["fixed_center_roll_enabled"])
        self.assertFalse(built["preset"]["config"]["excess_inventory_reduce_only_enabled"])
        self.assertFalse(built["preset"]["config"]["autotune_symbol_enabled"])
        self.assertEqual(built["preset"]["config"]["max_total_notional"], 1000.0)
        self.assertAlmostEqual(built["preset"]["config"]["per_order_notional"], 11.25)
        self.assertFalse(built["preset"]["config"]["auto_regime_enabled"])
        self.assertFalse(built["preset"]["config"]["neutral_hourly_scale_enabled"])
        self.assertIsNone(built["preset"]["config"]["inventory_tier_start_notional"])
        self.assertEqual(built["preset"]["config"]["pause_buy_position_notional"], 500.0)
        self.assertEqual(built["preset"]["config"]["pause_short_position_notional"], 500.0)

    @patch("grid_optimizer.web.CUSTOM_RUNNER_PRESETS_PATH", new=Path("output/test_custom_runner_presets.json"))
    def test_runner_preset_summaries_filter_custom_presets_by_symbol(self) -> None:
        custom_path = Path("output/test_custom_runner_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            '{"custom_grid_night":{"label":"Night 自定义","symbol":"NIGHTUSDT","custom":true,"config":{"strategy_mode":"one_way_long"}},"custom_grid_opn":{"label":"OPN 自定义","symbol":"OPNUSDT","custom":true,"config":{"strategy_mode":"one_way_long"}}}',
            encoding="utf-8",
        )
        try:
            night_keys = {item["key"] for item in _runner_preset_summaries("NIGHTUSDT")}
            opn_keys = {item["key"] for item in _runner_preset_summaries("OPNUSDT")}
            self.assertIn("custom_grid_night", night_keys)
            self.assertNotIn("custom_grid_opn", night_keys)
            self.assertIn("custom_grid_opn", opn_keys)
        finally:
            custom_path.unlink(missing_ok=True)

    def test_runner_preset_summaries_filter_builtin_symbol_bound_presets(self) -> None:
        bard_keys = {item["key"] for item in _runner_preset_summaries("BARDUSDT")}
        based_keys = {item["key"] for item in _runner_preset_summaries("BASEDUSDT")}
        xaut_keys = {item["key"] for item in _runner_preset_summaries("XAUTUSDT")}
        opn_keys = {item["key"] for item in _runner_preset_summaries("OPNUSDT")}
        self.assertIn("bard_12h_push_neutral_v2", bard_keys)
        self.assertNotIn("bard_12h_push_neutral_v2", opn_keys)
        self.assertIn("synthetic_neutral_bard_style_v1", bard_keys)
        self.assertIn("synthetic_neutral_bard_style_v1", based_keys)
        self.assertIn("synthetic_neutral_bard_style_v1", opn_keys)
        self.assertIn("based_volume_long_trigger_v1", based_keys)
        self.assertIn("based_volume_guarded_bard_v2", based_keys)
        self.assertIn("based_overnight_volume_v1", based_keys)
        self.assertIn("based_volume_push_bard_v1", based_keys)
        self.assertNotIn("based_volume_long_trigger_v1", opn_keys)
        self.assertNotIn("based_volume_guarded_bard_v2", opn_keys)
        self.assertNotIn("based_overnight_volume_v1", opn_keys)
        self.assertIn("based_volume_push_bard_v1", opn_keys)
        self.assertIn("xaut_competition_push_neutral_v1", xaut_keys)
        self.assertIn("xaut_volume_guarded_bard_v2", xaut_keys)
        self.assertIn("xaut_volume_short_v1", xaut_keys)
        self.assertIn("xaut_guarded_ping_pong_v1", xaut_keys)
        self.assertIn("xaut_near_price_guarded_v1", xaut_keys)
        self.assertNotIn("xaut_competition_push_neutral_v1", opn_keys)
        self.assertNotIn("xaut_volume_guarded_bard_v2", opn_keys)
        self.assertNotIn("xaut_volume_short_v1", opn_keys)
        self.assertNotIn("xaut_guarded_ping_pong_v1", opn_keys)
        self.assertNotIn("xaut_near_price_guarded_v1", opn_keys)

    def test_runner_preset_summaries_include_sprint_symbol_presets_with_config(self) -> None:
        btc_summaries = {item["key"]: item for item in _runner_preset_summaries("BTCUSDC")}
        xau_summaries = {item["key"]: item for item in _runner_preset_summaries("XAUUSDT")}
        eth_summaries = {item["key"]: item for item in _runner_preset_summaries("ETHUSDC")}
        self.assertIn("btcusdc_competition_maker_neutral_v1", btc_summaries)
        self.assertIn("btcusdc_competition_maker_neutral_conservative_v1", btc_summaries)
        self.assertIn("btcusdc_competition_maker_neutral_aggressive_v1", btc_summaries)
        self.assertIn("btcusdc_competition_neutral_ping_pong_v1", btc_summaries)
        self.assertIn("btcusdc_best_quote_long_ping_pong_v1", btc_summaries)
        self.assertNotIn("btcusdc_competition_maker_neutral_v1", xau_summaries)
        self.assertNotIn("btcusdc_competition_maker_neutral_v1", eth_summaries)
        self.assertIn("ethusdc_um_volume_long_v1", eth_summaries)
        self.assertNotIn("ethusdc_um_volume_long_v1", btc_summaries)
        self.assertIn("ethusdc_best_quote_long_ping_pong_v1", eth_summaries)
        self.assertIn("ethusdc_competition_neutral_ping_pong_v1", eth_summaries)
        self.assertNotIn("ethusdc_best_quote_long_ping_pong_v1", btc_summaries)
        self.assertNotIn("btcusdc_best_quote_long_ping_pong_v1", eth_summaries)
        preset = btc_summaries["btcusdc_competition_maker_neutral_v1"]
        self.assertEqual(preset["label"], "UM 冲刺赛 BTCUSDC")
        self.assertEqual(preset["config"]["symbol"], "BTCUSDC")
        self.assertEqual(preset["config"]["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(preset["config"]["per_order_notional"], 120.0)
        eth_long = eth_summaries["ethusdc_um_volume_long_v1"]
        self.assertEqual(eth_long["label"], "UM 冲刺赛 ETHUSDC 做多冲量")
        self.assertEqual(eth_long["config"]["symbol"], "ETHUSDC")
        self.assertEqual(eth_long["config"]["strategy_mode"], "one_way_long")
        self.assertAlmostEqual(eth_long["config"]["base_position_notional"], 250.0)
        eth_best_quote = eth_summaries["ethusdc_best_quote_long_ping_pong_v1"]
        self.assertEqual(eth_best_quote["label"], "ETHUSDC 做多 Best Quote Ping-Pong")
        self.assertEqual(eth_best_quote["config"]["symbol"], "ETHUSDC")
        self.assertEqual(eth_best_quote["config"]["strategy_mode"], "one_way_long")
        self.assertAlmostEqual(eth_best_quote["config"]["per_order_notional"], 25.0)
        btc_best_quote = btc_summaries["btcusdc_best_quote_long_ping_pong_v1"]
        self.assertEqual(btc_best_quote["config"]["symbol"], "BTCUSDC")
        self.assertEqual(btc_best_quote["config"]["strategy_mode"], "one_way_long")
        self.assertAlmostEqual(btc_best_quote["config"]["per_order_notional"], 120.0)
        btc_ping_pong = btc_summaries["btcusdc_competition_neutral_ping_pong_v1"]
        self.assertEqual(btc_ping_pong["config"]["strategy_mode"], "synthetic_neutral")
        self.assertFalse(btc_ping_pong["config"]["flat_start_enabled"])
        conservative = btc_summaries["btcusdc_competition_maker_neutral_conservative_v1"]
        aggressive = btc_summaries["btcusdc_competition_maker_neutral_aggressive_v1"]
        self.assertEqual(conservative["label"], "UM 冲刺赛 BTCUSDC（保守）")
        self.assertEqual(aggressive["label"], "UM 冲刺赛 BTCUSDC（激进）")
        self.assertLess(conservative["config"]["per_order_notional"], preset["config"]["per_order_notional"])
        self.assertGreater(aggressive["config"]["per_order_notional"], preset["config"]["per_order_notional"])

    def test_runner_preset_payload_for_bard_12h_push_neutral_v2(self) -> None:
        payload = _runner_preset_payload("bard_12h_push_neutral_v2", {"symbol": "BARDUSDT"})
        self.assertEqual(payload["symbol"], "BARDUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.0005)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 4)
        self.assertEqual(payload["per_order_notional"], 45.0)
        self.assertEqual(payload["base_position_notional"], 0.0)
        self.assertEqual(payload["pause_buy_position_notional"], 420.0)
        self.assertEqual(payload["pause_short_position_notional"], 220.0)
        self.assertEqual(payload["max_position_notional"], 650.0)
        self.assertEqual(payload["max_short_position_notional"], 320.0)
        self.assertEqual(payload["max_total_notional"], 3600.0)
        self.assertEqual(payload["max_cumulative_notional"], 660000.0)
        self.assertEqual(payload["max_new_orders"], 40)
        self.assertEqual(payload["inventory_tier_start_notional"], 420.0)
        self.assertEqual(payload["inventory_tier_end_notional"], 520.0)
        self.assertEqual(payload["inventory_tier_buy_levels"], 4)
        self.assertEqual(payload["inventory_tier_sell_levels"], 12)
        self.assertEqual(payload["inventory_tier_per_order_notional"], 70.0)
        self.assertEqual(payload["inventory_tier_base_position_notional"], 280.0)
        self.assertEqual(payload["short_cover_pause_amp_trigger_ratio"], 0.004)
        self.assertEqual(payload["short_cover_pause_down_return_trigger_ratio"], -0.0018)
        self.assertEqual(payload["take_profit_min_profit_ratio"], 0.0001)
        self.assertFalse(payload["excess_inventory_reduce_only_enabled"])
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertIsNone(payload["rolling_hourly_loss_limit"])
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["volume_trigger_enabled"])
        self.assertEqual(payload["volume_trigger_window"], "15m")
        self.assertIsNone(payload["volume_trigger_start_threshold"])
        self.assertIsNone(payload["volume_trigger_stop_threshold"])
        self.assertTrue(payload["volume_trigger_stop_cancel_open_orders"])
        self.assertFalse(payload["volume_trigger_stop_close_all_positions"])

    def test_runner_preset_payload_for_synthetic_neutral_bard_style_v1(self) -> None:
        payload = _runner_preset_payload("synthetic_neutral_bard_style_v1", {"symbol": "OPNUSDT"})
        self.assertEqual(payload["symbol"], "OPNUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.0007)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 4)
        self.assertEqual(payload["per_order_notional"], 45.0)
        self.assertEqual(payload["base_position_notional"], 0.0)
        self.assertEqual(payload["pause_buy_position_notional"], 2000.0)
        self.assertEqual(payload["pause_short_position_notional"], 220.0)
        self.assertEqual(payload["max_position_notional"], 2400.0)
        self.assertEqual(payload["max_short_position_notional"], 320.0)
        self.assertEqual(payload["max_total_notional"], 3600.0)
        self.assertEqual(payload["max_new_orders"], 40)
        self.assertEqual(payload["short_cover_pause_amp_trigger_ratio"], 0.004)
        self.assertEqual(payload["short_cover_pause_down_return_trigger_ratio"], -0.0018)
        self.assertIsNone(payload["take_profit_min_profit_ratio"])
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["excess_inventory_reduce_only_enabled"])
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertIsNone(payload["rolling_hourly_loss_limit"])

    def test_runner_preset_payload_for_based_volume_long_trigger_includes_volume_guard(self) -> None:
        payload = _runner_preset_payload("based_volume_long_trigger_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["symbol"], "BASEDUSDT")
        self.assertEqual(payload["strategy_mode"], "one_way_long")
        self.assertEqual(payload["runtime_guard_stats_start_time"], "2026-03-31T18:00:00+08:00")
        self.assertTrue(payload["volume_trigger_enabled"])
        self.assertEqual(payload["volume_trigger_window"], "15m")
        self.assertEqual(payload["volume_trigger_start_threshold"], 260000.0)
        self.assertEqual(payload["volume_trigger_stop_threshold"], 180000.0)
        self.assertTrue(payload["volume_trigger_stop_cancel_open_orders"])
        self.assertTrue(payload["volume_trigger_stop_close_all_positions"])
        self.assertTrue(payload["volatility_trigger_enabled"])
        self.assertEqual(payload["volatility_trigger_window"], "1h")
        self.assertEqual(payload["volatility_trigger_amplitude_ratio"], 0.04)
        self.assertEqual(payload["volatility_trigger_abs_return_ratio"], 0.02)

    def test_runner_preset_payload_for_based_volume_guarded_bard_v2(self) -> None:
        payload = _runner_preset_payload("based_volume_guarded_bard_v2", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["symbol"], "BASEDUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.0005)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 5)
        self.assertEqual(payload["per_order_notional"], 45.0)
        self.assertEqual(payload["pause_buy_position_notional"], 420.0)
        self.assertEqual(payload["pause_short_position_notional"], 260.0)
        self.assertEqual(payload["threshold_position_notional"], 520.0)
        self.assertEqual(payload["max_position_notional"], 650.0)
        self.assertEqual(payload["max_short_position_notional"], 420.0)
        self.assertEqual(payload["max_total_notional"], 2200.0)
        self.assertEqual(payload["short_cover_pause_amp_trigger_ratio"], 0.005)
        self.assertEqual(payload["short_cover_pause_down_return_trigger_ratio"], -0.0022)
        self.assertEqual(payload["take_profit_min_profit_ratio"], 0.0)
        self.assertFalse(payload["adaptive_step_enabled"])
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertIsNone(payload["rolling_hourly_loss_limit"])
        self.assertFalse(payload["volume_trigger_enabled"])
        self.assertIsNone(payload["volume_trigger_start_threshold"])
        self.assertIsNone(payload["volume_trigger_stop_threshold"])
        self.assertTrue(payload["volume_trigger_stop_cancel_open_orders"])
        self.assertFalse(payload["volume_trigger_stop_close_all_positions"])

    def test_runner_preset_payload_for_based_overnight_volume_v1(self) -> None:
        payload = _runner_preset_payload("based_overnight_volume_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["symbol"], "BASEDUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.0001)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 5)
        self.assertEqual(payload["per_order_notional"], 50.0)
        self.assertEqual(payload["sticky_entry_levels"], 1)
        self.assertEqual(payload["synthetic_residual_long_flat_notional"], 400.0)
        self.assertEqual(payload["synthetic_residual_short_flat_notional"], 300.0)
        self.assertEqual(payload["pause_buy_position_notional"], 800.0)
        self.assertEqual(payload["pause_short_position_notional"], 260.0)
        self.assertEqual(payload["max_position_notional"], 1000.0)
        self.assertEqual(payload["max_short_position_notional"], 320.0)
        self.assertEqual(payload["max_total_notional"], 1500.0)
        self.assertEqual(payload["max_new_orders"], 24)
        self.assertEqual(payload["leverage"], 10)
        self.assertEqual(payload["max_cumulative_notional"], 250000.0)
        self.assertEqual(payload["rolling_hourly_loss_limit"], 8.0)
        self.assertFalse(payload["adaptive_step_enabled"])
        self.assertFalse(payload["volatility_trigger_enabled"])
        self.assertFalse(payload["volume_trigger_enabled"])
        self.assertIsNone(payload["volume_trigger_start_threshold"])
        self.assertIsNone(payload["volume_trigger_stop_threshold"])
        self.assertTrue(payload["volume_trigger_stop_cancel_open_orders"])
        self.assertFalse(payload["volume_trigger_stop_close_all_positions"])

    def test_runner_preset_payload_for_based_volume_push_bard_is_generic(self) -> None:
        payload = _runner_preset_payload("based_volume_push_bard_v1", {"symbol": "OPNUSDT"})
        self.assertEqual(payload["symbol"], "OPNUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertEqual(payload["step_price"], 0.0001)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 8)
        self.assertEqual(payload["per_order_notional"], 100.0)
        self.assertEqual(payload["base_position_notional"], 0.0)
        self.assertTrue(payload["autotune_symbol_enabled"])
        self.assertFalse(payload["excess_inventory_reduce_only_enabled"])
        self.assertIsNone(payload["runtime_guard_stats_start_time"])
        self.assertIsNone(payload["rolling_hourly_loss_limit"])
        self.assertTrue(payload["adaptive_step_enabled"])
        self.assertEqual(payload["adaptive_step_30s_abs_return_ratio"], 0.0028)
        self.assertEqual(payload["adaptive_step_30s_amplitude_ratio"], 0.0035)
        self.assertEqual(payload["adaptive_step_1m_abs_return_ratio"], 0.0045)
        self.assertEqual(payload["adaptive_step_1m_amplitude_ratio"], 0.0065)
        self.assertEqual(payload["adaptive_step_3m_abs_return_ratio"], 0.0100)
        self.assertEqual(payload["adaptive_step_5m_abs_return_ratio"], 0.0140)
        self.assertEqual(payload["adaptive_step_max_scale"], 3.0)
        self.assertEqual(payload["adaptive_step_min_per_order_scale"], 1.0)
        self.assertEqual(payload["adaptive_step_min_position_limit_scale"], 0.65)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_resolve_runner_start_config_starts_generic_based_push_profile_for_other_symbol(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = self._mock_symbol_config()
        mock_book_tickers.return_value = self._mock_book()
        config = _resolve_runner_start_config({"symbol": "OPNUSDT", "strategy_profile": "based_volume_push_bard_v1"})
        self.assertEqual(config["strategy_profile"], "based_volume_push_bard_v1")
        self.assertEqual(config["symbol"], "OPNUSDT")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertTrue(config["autotune_symbol_enabled"])
        self.assertEqual(config["state_path"], "output/opnusdt_loop_state.json")
        self.assertGreater(config["step_price"], 0)

    def test_resolve_runner_start_config_starts_bard_style_template_for_other_symbol(self) -> None:
        config = _resolve_runner_start_config({"symbol": "OPNUSDT", "strategy_profile": "synthetic_neutral_bard_style_v1"})
        self.assertEqual(config["strategy_profile"], "synthetic_neutral_bard_style_v1")
        self.assertEqual(config["symbol"], "OPNUSDT")
        self.assertEqual(config["strategy_mode"], "synthetic_neutral")
        self.assertFalse(config["autotune_symbol_enabled"])
        self.assertFalse(config["volatility_trigger_enabled"])
        self.assertEqual(config["step_price"], 0.0007)
        self.assertEqual(config["state_path"], "output/opnusdt_loop_state.json")

    @patch("grid_optimizer.web.CUSTOM_RUNNER_PRESETS_PATH", new=Path("output/test_custom_runner_presets.json"))
    def test_runner_preset_summaries_include_custom_grid_edit_metadata(self) -> None:
        custom_path = Path("output/test_custom_runner_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            json.dumps(
                {
                    "custom_grid_opn": {
                        "key": "custom_grid_opn",
                        "label": "OPN 自定义",
                        "symbol": "OPNUSDT",
                        "custom": True,
                        "kind": "custom_grid",
                        "grid_preview_params": {
                            "strategy_direction": "long",
                            "grid_level_mode": "arithmetic",
                            "min_price": 0.15,
                            "max_price": 0.30,
                            "n": 200,
                            "margin_amount": 200.0,
                            "leverage": 10.0,
                        },
                        "preview_summary": {"startup_long_notional": 212.18},
                        "config": {"strategy_mode": "one_way_long"},
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            summaries = _runner_preset_summaries("OPNUSDT")
            preset = next(item for item in summaries if item["key"] == "custom_grid_opn")
            self.assertTrue(preset["custom"])
            self.assertEqual(preset["grid_preview_params"]["n"], 200)
            self.assertAlmostEqual(preset["preview_summary"]["startup_long_notional"], 212.18)
        finally:
            custom_path.unlink(missing_ok=True)

    @patch("grid_optimizer.web._run_grid_preview")
    @patch("grid_optimizer.web.CUSTOM_RUNNER_PRESETS_PATH", new=Path("output/test_custom_runner_presets.json"))
    def test_update_custom_grid_runner_preset_preserves_key(self, mock_preview) -> None:
        custom_path = Path("output/test_custom_runner_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            json.dumps(
                {
                    "custom_grid_opn_old": {
                        "key": "custom_grid_opn_old",
                        "label": "旧 OPN 网格",
                        "symbol": "OPNUSDT",
                        "custom": True,
                        "kind": "custom_grid",
                        "created_at": "2026-03-20T00:00:00+00:00",
                        "grid_preview_params": {
                            "contract_type": "usdm",
                            "strategy_direction": "long",
                            "grid_level_mode": "arithmetic",
                            "min_price": 0.15,
                            "max_price": 0.30,
                            "n": 200,
                            "margin_amount": 200.0,
                            "leverage": 10.0,
                        },
                        "config": {"strategy_mode": "one_way_long"},
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        mock_preview.return_value = {
            "ok": True,
            "summary": {
                "symbol": "OPNUSDT",
                "strategy_direction": "long",
                "grid_count": 100,
                "current_price": 0.25,
                "position_budget_notional": 1000.0,
                "active_buy_orders": 90,
                "active_sell_orders": 10,
                "startup_long_notional": 123.0,
                "full_long_entry_notional": 1000.0,
                "symbol_info": {"min_notional": 5.0, "min_qty": 1.0},
            },
            "rows": [],
        }
        try:
            result = _update_custom_grid_runner_preset(
                "custom_grid_opn_old",
                {
                    "contract_type": "usdm",
                    "symbol": "OPNUSDT",
                    "name": "新 OPN 网格",
                    "strategy_direction": "long",
                    "grid_level_mode": "arithmetic",
                    "min_price": 0.16,
                    "max_price": 0.31,
                    "n": 100,
                    "margin_amount": 150.0,
                    "leverage": 5.0,
                },
            )
            self.assertEqual(result["preset_key"], "custom_grid_opn_old")
            loaded = _get_custom_runner_preset("custom_grid_opn_old", "OPNUSDT")
            self.assertEqual(loaded["label"], "新 OPN 网格")
            self.assertEqual(loaded["created_at"], "2026-03-20T00:00:00+00:00")
            self.assertIn("updated_at", loaded)
        finally:
            custom_path.unlink(missing_ok=True)

    @patch("grid_optimizer.web._read_runner_process_for_symbol")
    @patch("grid_optimizer.web.CUSTOM_RUNNER_PRESETS_PATH", new=Path("output/test_custom_runner_presets.json"))
    def test_delete_custom_grid_runner_preset_removes_saved_preset(self, mock_read_runner) -> None:
        custom_path = Path("output/test_custom_runner_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            json.dumps(
                {
                    "custom_grid_opn_delete": {
                        "key": "custom_grid_opn_delete",
                        "label": "删除测试",
                        "symbol": "OPNUSDT",
                        "custom": True,
                        "kind": "custom_grid",
                        "config": {"strategy_mode": "one_way_long"},
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        mock_read_runner.return_value = {"is_running": False, "config": {}}
        try:
            result = _delete_custom_grid_runner_preset("custom_grid_opn_delete", "OPNUSDT")
            self.assertEqual(result["preset_key"], "custom_grid_opn_delete")
            self.assertNotIn("custom_grid_opn_delete", {item["key"] for item in _runner_preset_summaries("OPNUSDT")})
            self.assertNotIn("custom_grid_opn_delete", json.loads(custom_path.read_text(encoding="utf-8")))
        finally:
            custom_path.unlink(missing_ok=True)

    @patch("grid_optimizer.web.read_symbol_runner_process")
    @patch("grid_optimizer.web.build_monitor_snapshot")
    def test_run_loop_monitor_query_uses_symbol_runner_and_paths(self, mock_snapshot, mock_read_runner) -> None:
        mock_read_runner.return_value = {
            "configured": True,
            "config": {
                "symbol": "OPNUSDT",
                "summary_jsonl": "output/opnusdt_loop_events.jsonl",
                "plan_json": "output/opnusdt_loop_latest_plan.json",
                "submit_report_json": "output/opnusdt_loop_latest_submit.json",
            },
        }
        mock_snapshot.return_value = {"ok": True}

        result = _run_loop_monitor_query({"symbol": ["OPNUSDT"]})

        self.assertTrue(result["ok"])
        mock_read_runner.assert_called_once_with("OPNUSDT")
        mock_snapshot.assert_called_once()
        kwargs = mock_snapshot.call_args.kwargs
        self.assertEqual(kwargs["symbol"], "OPNUSDT")
        self.assertEqual(kwargs["events_path"], "output/opnusdt_loop_events.jsonl")
        self.assertEqual(kwargs["plan_path"], "output/opnusdt_loop_latest_plan.json")
        self.assertEqual(kwargs["submit_report_path"], "output/opnusdt_loop_latest_submit.json")
        self.assertEqual(kwargs["runner_process"]["config"]["symbol"], "OPNUSDT")
        self.assertEqual(kwargs["runner_process"]["config"]["summary_jsonl"], "output/opnusdt_loop_events.jsonl")
        self.assertEqual(kwargs["runner_process"]["config"]["plan_json"], "output/opnusdt_loop_latest_plan.json")
        self.assertEqual(kwargs["runner_process"]["config"]["submit_report_json"], "output/opnusdt_loop_latest_submit.json")

    @patch("grid_optimizer.web.read_symbol_runner_process")
    @patch("grid_optimizer.web.build_monitor_snapshot")
    def test_run_loop_monitor_query_normalizes_runner_runtime_paths(self, mock_snapshot, mock_read_runner) -> None:
        mock_read_runner.return_value = {
            "configured": True,
            "config": {
                "symbol": "BARDUSDT",
                "state_path": "output/nightusdt_loop_state.json",
                "summary_jsonl": "output/nightusdt_loop_events.jsonl",
                "plan_json": "output/nightusdt_loop_latest_plan.json",
                "submit_report_json": "output/nightusdt_loop_latest_submit.json",
            },
        }
        mock_snapshot.return_value = {"ok": True}

        _run_loop_monitor_query({"symbol": ["BARDUSDT"]})

        kwargs = mock_snapshot.call_args.kwargs
        runner_process = kwargs["runner_process"]
        self.assertEqual(runner_process["config"]["symbol"], "BARDUSDT")
        self.assertEqual(runner_process["config"]["state_path"], "output/bardusdt_loop_state.json")
        self.assertEqual(runner_process["config"]["summary_jsonl"], "output/bardusdt_loop_events.jsonl")
        self.assertEqual(runner_process["config"]["plan_json"], "output/bardusdt_loop_latest_plan.json")
        self.assertEqual(runner_process["config"]["submit_report_json"], "output/bardusdt_loop_latest_submit.json")
        self.assertEqual(kwargs["events_path"], "output/bardusdt_loop_events.jsonl")
        self.assertEqual(kwargs["plan_path"], "output/bardusdt_loop_latest_plan.json")
        self.assertEqual(kwargs["submit_report_path"], "output/bardusdt_loop_latest_submit.json")

    def test_symbol_specific_runner_control_round_trip(self) -> None:
        control_path = Path("output/opnusdt_loop_runner_control.json")
        if control_path.exists():
            control_path.unlink()
        try:
            _save_runner_control_config({"symbol": "OPNUSDT", "strategy_profile": "volume_long_v4"}, symbol="OPNUSDT")
            config = _load_runner_control_config("OPNUSDT")
            self.assertEqual(config["symbol"], "OPNUSDT")
            self.assertEqual(config["strategy_profile"], "volume_long_v4")
        finally:
            if control_path.exists():
                control_path.unlink()

    def test_monitor_page_uses_symbol_dropdown_for_supported_symbols(self) -> None:
        self.assertIn('<select id="symbol">', MONITOR_PAGE)
        self.assertIn("loadMonitorSymbols", MONITOR_PAGE)
        self.assertIn("fetchMonitorSnapshot", MONITOR_PAGE)
        self.assertIn("async function loadRunningConfigToEditor", MONITOR_PAGE)
        self.assertIn("小时损益拆解", MONITOR_PAGE)
        self.assertIn('id="hourly_body"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_name"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_preview_btn"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_save_btn"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_load_btn"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_update_btn"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_delete_btn"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_roll_enabled"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_roll_interval_minutes"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_roll_trade_threshold"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_roll_upper_distance_ratio"', MONITOR_PAGE)
        self.assertIn('id="custom_grid_roll_shift_levels"', MONITOR_PAGE)

    def test_monitor_page_exposes_readable_runner_param_form(self) -> None:
        self.assertIn('id="runner_params_form"', MONITOR_PAGE)
        self.assertIn("单笔金额", MONITOR_PAGE)
        self.assertIn("软停买", MONITOR_PAGE)
        self.assertIn("硬上限", MONITOR_PAGE)

    def test_monitor_page_default_monitor_symbols_include_current_sprint_symbols(self) -> None:
        self.assertIn('const DEFAULT_MONITOR_SYMBOLS = ["SOONUSDT", "BTCUSDC", "ETHUSDC", "XAUUSDT", "XAGUSDT", "CLUSDT", "BZUSDT", "ORDIUSDC"]', MONITOR_PAGE)

    def test_monitor_page_keeps_raw_json_in_advanced_panel(self) -> None:
        self.assertIn('id="runner_params_advanced_panel"', MONITOR_PAGE)
        self.assertIn("高级模式 / 原始 JSON", MONITOR_PAGE)
        self.assertIn('id="runner_params_editor"', MONITOR_PAGE)

    def test_monitor_page_exposes_advanced_runner_fields_and_mode_visibility_logic(self) -> None:
        self.assertIn('id="runner_field_inventory_tier_start_notional"', MONITOR_PAGE)
        self.assertIn('id="runner_field_buy_pause_amp_trigger_ratio"', MONITOR_PAGE)
        self.assertIn('id="runner_field_market_bias_enabled"', MONITOR_PAGE)
        self.assertIn('id="runner_field_auto_regime_enabled"', MONITOR_PAGE)
        self.assertIn('id="runner_field_synthetic_trend_follow_enabled"', MONITOR_PAGE)
        self.assertIn('id="runner_field_neutral_band1_offset_ratio"', MONITOR_PAGE)
        self.assertIn("库存分层", MONITOR_PAGE)
        self.assertIn("合成中性跟随", MONITOR_PAGE)
        self.assertIn("目标中性", MONITOR_PAGE)
        self.assertIn("function applyRunnerModeVisibility(mode)", MONITOR_PAGE)
        self.assertIn("GRID_BASED_RUNNER_MODES", MONITOR_PAGE)

    def test_monitor_page_contains_sprint_preset_labels_and_auto_selects_first_available(self) -> None:
        self.assertIn("UM 冲刺赛 BTCUSDC", MONITOR_PAGE)
        self.assertIn("UM 冲刺赛 BTCUSDC（保守）", MONITOR_PAGE)
        self.assertIn("UM 冲刺赛 BTCUSDC（激进）", MONITOR_PAGE)
        self.assertIn("黄金冲刺赛 XAUUSDT", MONITOR_PAGE)
        self.assertIn("黄金冲刺赛 XAUUSDT（激进）", MONITOR_PAGE)
        self.assertIn("TradFi 冲刺赛 CLUSDT", MONITOR_PAGE)
        self.assertIn("Alt 冲刺赛 ORDIUSDC", MONITOR_PAGE)
        self.assertIn("presets[0]?.key", MONITOR_PAGE)

    def test_monitor_page_contains_dedicated_sprint_preset_zone(self) -> None:
        self.assertIn("冲刺赛专区", MONITOR_PAGE)
        self.assertIn('id="sprint_preset_zone"', MONITOR_PAGE)
        self.assertIn('id="sprint_preset_zone_meta"', MONITOR_PAGE)
        self.assertIn("UM 冲刺赛专区", MONITOR_PAGE)
        self.assertIn("黄金 / 白银", MONITOR_PAGE)
        self.assertIn("function renderSprintPresetZone()", MONITOR_PAGE)
        self.assertIn("function ensureMonitorSymbolOption(symbol)", MONITOR_PAGE)
        self.assertIn('data-sprint-preset-key="${escapeHtml(preset.key)}"', MONITOR_PAGE)

    def test_strategies_page_contains_manual_symbol_list_controls(self) -> None:
        self.assertIn('id="monitor_symbol_input"', STRATEGIES_PAGE)
        self.assertIn('id="competition_symbol_input"', STRATEGIES_PAGE)
        self.assertIn('id="monitor_symbol_chips"', STRATEGIES_PAGE)
        self.assertIn('id="competition_symbol_chips"', STRATEGIES_PAGE)
        self.assertIn("/api/symbol_lists", STRATEGIES_PAGE)

    def test_strategies_page_default_competition_symbols_include_current_sprint_symbols(self) -> None:
        self.assertIn('const DEFAULT_COMPETITION_SYMBOLS = ["SOONUSDT", "BTCUSDC", "ETHUSDC", "XAUUSDT", "XAGUSDT", "CLUSDT", "BZUSDT", "ORDIUSDC"]', STRATEGIES_PAGE)

    @patch("grid_optimizer.web._read_runner_process_for_symbol")
    def test_start_runner_process_returns_already_running_when_config_matches(self, mock_read_runner) -> None:
        config = {
            "strategy_profile": "volume_long_v4",
            "strategy_mode": "one_way_long",
            "symbol": "OPNUSDT",
            "step_price": 0.0003,
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 70.0,
            "base_position_notional": 420.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 750.0,
            "max_position_notional": 900.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 600.0,
            "inventory_tier_end_notional": 750.0,
            "inventory_tier_buy_levels": 4,
            "inventory_tier_sell_levels": 12,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 280.0,
            "margin_type": "KEEP",
            "leverage": 2,
            "max_plan_age_seconds": 30,
            "max_mid_drift_steps": 4.0,
            "maker_retries": 2,
            "max_new_orders": 20,
            "max_total_notional": 1000.0,
            "sleep_seconds": 15.0,
            "cancel_stale": True,
            "apply": True,
            "reset_state": True,
            "state_path": "output/opnusdt_loop_state.json",
            "plan_json": "output/opnusdt_loop_latest_plan.json",
            "submit_report_json": "output/opnusdt_loop_latest_submit.json",
            "summary_jsonl": "output/opnusdt_loop_events.jsonl",
        }
        mock_read_runner.return_value = {"is_running": True, "config": dict(config)}
        result = _start_runner_process(config)
        self.assertFalse(result["started"])
        self.assertTrue(result["already_running"])
        self.assertFalse(result["restarted"])

    @patch("grid_optimizer.web.subprocess.Popen")
    @patch("grid_optimizer.web._save_runner_control_config")
    @patch("grid_optimizer.web._stop_runner_process")
    @patch("grid_optimizer.web._read_runner_process_for_symbol")
    def test_start_runner_process_restarts_when_config_changes(
        self,
        mock_read_runner,
        mock_stop_runner,
        mock_save_runner,
        mock_popen,
    ) -> None:
        current = {
            "strategy_profile": "volume_long_v4",
            "strategy_mode": "one_way_long",
            "symbol": "OPNUSDT",
            "step_price": 0.0003,
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 70.0,
            "base_position_notional": 420.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 750.0,
            "max_position_notional": 900.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 600.0,
            "inventory_tier_end_notional": 750.0,
            "inventory_tier_buy_levels": 4,
            "inventory_tier_sell_levels": 12,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 280.0,
            "margin_type": "KEEP",
            "leverage": 2,
            "max_plan_age_seconds": 30,
            "max_mid_drift_steps": 4.0,
            "maker_retries": 2,
            "max_new_orders": 20,
            "max_total_notional": 1000.0,
            "sleep_seconds": 15.0,
            "cancel_stale": True,
            "apply": True,
            "reset_state": True,
            "state_path": "output/opnusdt_loop_state.json",
            "plan_json": "output/opnusdt_loop_latest_plan.json",
            "submit_report_json": "output/opnusdt_loop_latest_submit.json",
            "summary_jsonl": "output/opnusdt_loop_events.jsonl",
        }
        desired = dict(current)
        desired["strategy_profile"] = "volatility_defensive_v1"
        desired["step_price"] = 0.0004
        desired["buy_levels"] = 4
        desired["sell_levels"] = 12
        desired["pause_buy_position_notional"] = 300.0
        desired["max_position_notional"] = 420.0
        mock_read_runner.side_effect = [
            {"is_running": True, "config": dict(current)},
            {"is_running": True, "config": dict(desired)},
        ]
        mock_popen.return_value.pid = 12345
        result = _start_runner_process(desired)
        mock_stop_runner.assert_called_once_with("OPNUSDT")
        self.assertTrue(result["started"])
        self.assertTrue(result["restarted"])

    @patch.dict("os.environ", {"GRID_RUNNER_SERVICE_TEMPLATE": "grid-loop@{symbol}.service"})
    @patch("grid_optimizer.web._run_systemctl")
    @patch("grid_optimizer.web._runner_service_available")
    @patch("grid_optimizer.web._save_runner_control_config")
    @patch("grid_optimizer.web._read_runner_process_for_symbol")
    def test_start_runner_process_prefers_symbol_systemd_service(
        self,
        mock_read_runner,
        mock_save_runner,
        mock_service_available,
        mock_run_systemctl,
    ) -> None:
        config = {
            "strategy_profile": "defensive_quasi_neutral_v1",
            "strategy_mode": "one_way_long",
            "symbol": "NIGHTUSDT",
            "step_price": 0.00002,
            "buy_levels": 6,
            "sell_levels": 12,
            "per_order_notional": 80.0,
            "base_position_notional": 160.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 700.0,
            "max_position_notional": 850.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 500.0,
            "inventory_tier_end_notional": 650.0,
            "inventory_tier_buy_levels": 4,
            "inventory_tier_sell_levels": 14,
            "inventory_tier_per_order_notional": 80.0,
            "inventory_tier_base_position_notional": 80.0,
            "margin_type": "KEEP",
            "leverage": 2,
            "max_plan_age_seconds": 30,
            "max_mid_drift_steps": 4.0,
            "maker_retries": 2,
            "max_new_orders": 20,
            "max_total_notional": 1000.0,
            "sleep_seconds": 15.0,
            "cancel_stale": True,
            "apply": True,
            "reset_state": True,
            "state_path": "output/nightusdt_loop_state.json",
            "plan_json": "output/nightusdt_loop_latest_plan.json",
            "submit_report_json": "output/nightusdt_loop_latest_submit.json",
            "summary_jsonl": "output/nightusdt_loop_events.jsonl",
        }
        mock_read_runner.side_effect = [
            {"is_running": False, "config": {}},
            {"is_running": True, "config": dict(config), "pid": 99999},
        ]
        mock_service_available.return_value = True

        result = _start_runner_process(config)

        mock_run_systemctl.assert_called_once_with(["start", "grid-loop@NIGHTUSDT.service"], check=True)
        self.assertTrue(result["started"])
        self.assertEqual(result["service"], "grid-loop@NIGHTUSDT.service")


if __name__ == "__main__":
    unittest.main()
