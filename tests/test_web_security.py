from __future__ import annotations

import base64
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.web import (
    HTML_PAGE,
    MONITOR_PAGE,
    RANKING_PAGE,
    SPOT_RUNNER_PAGE,
    STRATEGIES_PAGE,
    _basic_auth_header_matches,
    _build_custom_grid_runner_preset,
    _build_flatten_command,
    _delete_custom_grid_runner_preset,
    _create_custom_grid_runner_preset,
    _build_runner_command,
    _client_ip_allowed,
    _default_runtime_paths_for_symbol,
    _get_custom_runner_preset,
    _load_runner_control_config,
    _normalize_spot_runner_payload,
    _parse_allowed_networks,
    _resolve_runner_start_config,
    _render_monitor_page,
    _render_spot_runner_page,
    _render_spot_strategies_page,
    _render_strategies_page,
    _run_loop_monitor_query,
    _runner_service_name_for_symbol,
    _resolve_volatility_directional_flatten_filter,
    _runner_preset_payload,
    _runner_preset_summaries,
    _save_running_runner_preset,
    _spot_runner_preset_payload,
    _spot_runner_preset_summaries,
    _save_runner_control_config,
    _start_runner_process,
    _update_custom_grid_runner_preset,
    _uses_legacy_runner,
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

    def test_monitor_page_contains_save_running_preset_controls(self) -> None:
        self.assertIn('id="save_running_preset_name"', MONITOR_PAGE)
        self.assertIn('id="save_running_preset_description"', MONITOR_PAGE)
        self.assertIn('id="save_running_preset_btn"', MONITOR_PAGE)
        self.assertIn("/api/runner/presets/save_running", MONITOR_PAGE)

    def test_monitor_page_contains_xaut_adaptive_status_text(self) -> None:
        self.assertIn("XAUT 三态状态", MONITOR_PAGE)
        self.assertIn("XAUT 三态原因", MONITOR_PAGE)

    def test_monitor_page_uses_safe_json_reader_for_runner_actions(self) -> None:
        self.assertIn("async function readJsonResponse(resp)", MONITOR_PAGE)
        self.assertIn("const raw = await resp.text();", MONITOR_PAGE)
        self.assertIn("return raw ? JSON.parse(raw) : {};", MONITOR_PAGE)

    def test_monitor_page_uses_safe_json_reader_for_stop_action(self) -> None:
        self.assertIn("const data = await readJsonResponse(resp);", MONITOR_PAGE)

    def test_volatility_directional_flatten_closes_long_on_fast_drop(self) -> None:
        result = _resolve_volatility_directional_flatten_filter(
            current_return_ratio=-0.08,
            live_snapshot={"long_qty": 10.0, "short_qty": 0.0},
        )
        self.assertEqual(result["position_side_filter"], "LONG")
        self.assertEqual(result["reason"], "close_long_on_down_move")

    def test_volatility_directional_flatten_keeps_short_on_fast_drop(self) -> None:
        result = _resolve_volatility_directional_flatten_filter(
            current_return_ratio=-0.08,
            live_snapshot={"long_qty": 0.0, "short_qty": 10.0},
        )
        self.assertIsNone(result["position_side_filter"])
        self.assertEqual(result["reason"], "no_adverse_position")

    def test_volatility_directional_flatten_closes_short_on_fast_rally(self) -> None:
        result = _resolve_volatility_directional_flatten_filter(
            current_return_ratio=0.08,
            live_snapshot={"long_qty": 0.0, "short_qty": 10.0},
        )
        self.assertEqual(result["position_side_filter"], "SHORT")
        self.assertEqual(result["reason"], "close_short_on_up_move")

    def test_spot_runner_page_contains_strategy_preset_controls(self) -> None:
        self.assertIn('id="spot_strategy_preset"', SPOT_RUNNER_PAGE)
        self.assertIn("1.25XAUT现货交易赛", SPOT_RUNNER_PAGE)
        self.assertIn('id="load_spot_preset_btn"', SPOT_RUNNER_PAGE)

    def test_ranking_page_uses_conservative_default_refresh_seconds(self) -> None:
        self.assertIn('id="rank_refresh_seconds" type="number" step="1" value="120"', HTML_PAGE)
        self.assertIn("Math.max(60, Number(rankRefreshSecondsEl.value) || 120)", HTML_PAGE)
        self.assertIn('id="refresh_seconds" type="number" step="1" value="120"', RANKING_PAGE)
        self.assertIn("Math.max(60, Number(refreshSecondsEl.value) || 120)", RANKING_PAGE)

    @patch.dict("os.environ", {}, clear=False)
    def test_competition_board_auto_refresh_interval_defaults_to_twenty_minutes(self) -> None:
        from grid_optimizer.web import _competition_board_auto_refresh_interval_seconds

        self.assertEqual(_competition_board_auto_refresh_interval_seconds(), 1200.0)

    @patch.dict("os.environ", {"GRID_COMPETITION_BOARD_REFRESH_SECONDS": "1500"}, clear=False)
    def test_competition_board_auto_refresh_interval_respects_environment_override(self) -> None:
        from grid_optimizer.web import _competition_board_auto_refresh_interval_seconds

        self.assertEqual(_competition_board_auto_refresh_interval_seconds(), 1500.0)

    @patch.dict("os.environ", {}, clear=False)
    def test_render_monitor_page_uses_conservative_default_refresh_seconds(self) -> None:
        page = _render_monitor_page()

        self.assertIn('id="refresh_sec" type="number" min="2" step="1" value="15"', page)
        self.assertIn("Math.max(2, Number(refreshSecEl.value || 15))", page)

    @patch.dict("os.environ", {"GRID_WEB_MONITOR_REFRESH_DEFAULT_SECONDS": "21"}, clear=False)
    def test_render_monitor_page_respects_environment_refresh_override(self) -> None:
        page = _render_monitor_page()

        self.assertIn('id="refresh_sec" type="number" min="2" step="1" value="21"', page)
        self.assertIn("Math.max(2, Number(refreshSecEl.value || 21))", page)

    @patch.dict("os.environ", {}, clear=False)
    def test_render_spot_runner_page_uses_conservative_default_refresh_seconds(self) -> None:
        page = _render_spot_runner_page()

        self.assertIn('id="refresh_seconds" type="number" min="2" step="1" value="15"', page)
        self.assertIn("Math.max(2, Number(refreshSecondsEl.value || 15))", page)

    @patch.dict("os.environ", {"GRID_WEB_SPOT_RUNNER_REFRESH_DEFAULT_SECONDS": "19"}, clear=False)
    def test_render_spot_runner_page_respects_environment_refresh_override(self) -> None:
        page = _render_spot_runner_page()

        self.assertIn('id="refresh_seconds" type="number" min="2" step="1" value="19"', page)
        self.assertIn("Math.max(2, Number(refreshSecondsEl.value || 19))", page)

    @patch.dict("os.environ", {}, clear=False)
    def test_render_strategy_pages_use_slower_default_refresh_seconds(self) -> None:
        spot_page = _render_spot_strategies_page()
        futures_page = _render_strategies_page()

        self.assertIn('id="refresh_sec" type="number" min="3" step="1" value="20"', spot_page)
        self.assertIn("Math.max(3, Number(refreshSecEl.value || 20))", spot_page)
        self.assertIn('id="refresh_sec" type="number" min="3" step="1" value="20"', futures_page)
        self.assertIn("Math.max(3, Number(refreshSecEl.value || 20))", futures_page)

    @patch.dict(
        "os.environ",
        {
            "GRID_WEB_SPOT_STRATEGIES_REFRESH_DEFAULT_SECONDS": "26",
            "GRID_WEB_STRATEGIES_REFRESH_DEFAULT_SECONDS": "24",
        },
        clear=False,
    )
    def test_render_strategy_pages_respect_environment_refresh_overrides(self) -> None:
        spot_page = _render_spot_strategies_page()
        futures_page = _render_strategies_page()

        self.assertIn('id="refresh_sec" type="number" min="3" step="1" value="26"', spot_page)
        self.assertIn("Math.max(3, Number(refreshSecEl.value || 26))", spot_page)
        self.assertIn('id="refresh_sec" type="number" min="3" step="1" value="24"', futures_page)
        self.assertIn("Math.max(3, Number(refreshSecEl.value || 24))", futures_page)

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

    def test_runner_preset_payload_applies_volume_neutral_push_v1_for_based(self) -> None:
        payload = _runner_preset_payload("volume_neutral_push_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["strategy_profile"], "volume_neutral_push_v1")
        self.assertEqual(payload["symbol"], "BASEDUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertAlmostEqual(payload["step_price"], 0.0001)
        self.assertEqual(payload["buy_levels"], 8)
        self.assertEqual(payload["sell_levels"], 8)
        self.assertAlmostEqual(payload["per_order_notional"], 45.0)
        self.assertAlmostEqual(payload["base_position_notional"], 0.0)
        self.assertEqual(payload["up_trigger_steps"], 3)
        self.assertEqual(payload["down_trigger_steps"], 3)
        self.assertEqual(payload["shift_steps"], 1)
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 1000.0)
        self.assertAlmostEqual(payload["pause_short_position_notional"], 1000.0)
        self.assertAlmostEqual(payload["max_position_notional"], 1000.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 1000.0)
        self.assertAlmostEqual(payload["max_total_notional"], 2200.0)

    def test_runner_preset_payload_rejects_volume_neutral_push_v1_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=BASEDUSDT"):
            _runner_preset_payload("volume_neutral_push_v1", {"symbol": "BTCUSDT"})

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

    def test_runner_preset_payload_applies_xaut_volume_guarded_bard_v3(self) -> None:
        payload = _runner_preset_payload("xaut_volume_guarded_bard_v3", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_volume_guarded_bard_v3")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["step_price"], 1.0)
        self.assertEqual(payload["buy_levels"], 10)
        self.assertEqual(payload["sell_levels"], 6)
        self.assertAlmostEqual(payload["per_order_notional"], 35.0)
        self.assertAlmostEqual(payload["startup_entry_multiplier"], 3.0)
        self.assertAlmostEqual(payload["base_position_notional"], 120.0)
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 2600.0)
        self.assertAlmostEqual(payload["pause_short_position_notional"], 2600.0)
        self.assertAlmostEqual(payload["max_position_notional"], 3200.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 3200.0)
        self.assertAlmostEqual(payload["max_total_notional"], 3000.0)
        self.assertAlmostEqual(payload["sleep_seconds"], 3.0)

    def test_runner_preset_payload_applies_xaut_volume_guarded_bard_v3_1(self) -> None:
        payload = _runner_preset_payload("xaut_volume_guarded_bard_v3_1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_volume_guarded_bard_v3_1")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["step_price"], 1.25)
        self.assertEqual(payload["buy_levels"], 10)
        self.assertEqual(payload["sell_levels"], 6)
        self.assertAlmostEqual(payload["per_order_notional"], 35.0)
        self.assertAlmostEqual(payload["startup_entry_multiplier"], 2.0)
        self.assertAlmostEqual(payload["base_position_notional"], 80.0)
        self.assertAlmostEqual(payload["pause_buy_position_notional"], 2600.0)
        self.assertAlmostEqual(payload["pause_short_position_notional"], 2600.0)
        self.assertAlmostEqual(payload["max_position_notional"], 3200.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 3200.0)
        self.assertAlmostEqual(payload["max_total_notional"], 3000.0)
        self.assertAlmostEqual(payload["sleep_seconds"], 3.0)

    def test_runner_preset_payload_applies_bard_push_neutral_step_and_stop_cap(self) -> None:
        payload = _runner_preset_payload("bard_12h_push_neutral_v2", {"symbol": "BARDUSDT"})
        self.assertEqual(payload["strategy_profile"], "bard_12h_push_neutral_v2")
        self.assertEqual(payload["symbol"], "BARDUSDT")
        self.assertEqual(payload["strategy_mode"], "synthetic_neutral")
        self.assertAlmostEqual(payload["step_price"], 0.0001)
        self.assertAlmostEqual(payload["max_cumulative_notional"], 600000.0)

    def test_runner_preset_payload_rejects_xaut_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=XAUTUSDT"):
            _runner_preset_payload("xaut_long_adaptive_v1", {"symbol": "BTCUSDT"})

    def test_runner_preset_summaries_hide_xaut_volume_guarded_bard_v3_from_dropdown(self) -> None:
        xaut_keys = {item["key"] for item in _runner_preset_summaries("XAUTUSDT")}
        self.assertNotIn("xaut_volume_guarded_bard_v3", xaut_keys)
        payload = _runner_preset_payload("xaut_volume_guarded_bard_v3", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_volume_guarded_bard_v3")

    def test_runner_preset_summaries_hide_xaut_volume_guarded_bard_v3_1_from_dropdown(self) -> None:
        xaut_keys = {item["key"] for item in _runner_preset_summaries("XAUTUSDT")}
        self.assertNotIn("xaut_volume_guarded_bard_v3_1", xaut_keys)
        payload = _runner_preset_payload("xaut_volume_guarded_bard_v3_1", {"symbol": "XAUTUSDT"})
        self.assertEqual(payload["strategy_profile"], "xaut_volume_guarded_bard_v3_1")

    def test_spot_runner_preset_payload_applies_xaut_competition_125_profile(self) -> None:
        payload = _spot_runner_preset_payload("xaut_spot_competition_1_25_v1", {"symbol": "XAUTUSDT"})

        self.assertEqual(payload["strategy_profile"], "xaut_spot_competition_1_25_v1")
        self.assertEqual(payload["symbol"], "XAUTUSDT")
        self.assertEqual(payload["strategy_mode"], "spot_volume_shift_long")
        self.assertAlmostEqual(payload["grid_band_ratio"], 0.00084)
        self.assertEqual(payload["attack_buy_levels"], 5)
        self.assertEqual(payload["attack_sell_levels"], 5)
        self.assertAlmostEqual(payload["attack_per_order_notional"], 40.0)
        self.assertAlmostEqual(payload["inventory_soft_limit_notional"], 750.0)
        self.assertAlmostEqual(payload["inventory_hard_limit_notional"], 950.0)
        self.assertAlmostEqual(payload["rolling_hourly_loss_limit"], 120.0)
        self.assertAlmostEqual(payload["max_cumulative_notional"], 1_000_000.0)
        self.assertAlmostEqual(payload["max_cumulative_loss_limit"], 800.0)

    def test_spot_runner_preset_summaries_include_xaut_competition_125_for_xaut_only(self) -> None:
        xaut_keys = {item["key"] for item in _spot_runner_preset_summaries("XAUTUSDT")}
        btc_keys = {item["key"] for item in _spot_runner_preset_summaries("BTCUSDT")}

        self.assertIn("xaut_spot_competition_1_25_v1", xaut_keys)
        self.assertNotIn("xaut_spot_competition_1_25_v1", btc_keys)

    def test_spot_runner_preset_payload_applies_bard_best_quote_inventory_profile(self) -> None:
        payload = _spot_runner_preset_payload("bard_spot_best_quote_inventory_v1", {"symbol": "BARDUSDT"})

        self.assertEqual(payload["strategy_profile"], "bard_spot_best_quote_inventory_v1")
        self.assertEqual(payload["symbol"], "BARDUSDT")
        self.assertEqual(payload["strategy_mode"], "spot_best_quote_inventory")
        self.assertAlmostEqual(payload["base_position_notional"], 120.0)
        self.assertAlmostEqual(payload["max_inventory_multiplier"], 2.0)
        self.assertAlmostEqual(payload["quote_buy_order_notional"], 20.0)
        self.assertAlmostEqual(payload["quote_sell_order_notional"], 20.0)
        self.assertAlmostEqual(payload["min_profit_offset"], 0.0001)
        self.assertAlmostEqual(payload["reduce_only_timeout_seconds"], 60.0)
        self.assertAlmostEqual(payload["reduce_only_taker_target_multiplier"], 1.4)

    def test_spot_runner_preset_summaries_include_bard_best_quote_for_bard_only(self) -> None:
        bard_keys = {item["key"] for item in _spot_runner_preset_summaries("BARDUSDT")}
        btc_keys = {item["key"] for item in _spot_runner_preset_summaries("BTCUSDT")}

        self.assertIn("bard_spot_best_quote_inventory_v1", bard_keys)
        self.assertNotIn("bard_spot_best_quote_inventory_v1", btc_keys)

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_applies_xaut_competition_125_profile(self, mock_validate) -> None:
        config = _normalize_spot_runner_payload(
            {
                "strategy_profile": "xaut_spot_competition_1_25_v1",
                "symbol": "XAUTUSDT",
            }
        )

        mock_validate.assert_called_once_with(symbol="XAUTUSDT", market_type="spot", contract_type=None)
        self.assertEqual(config["strategy_profile"], "xaut_spot_competition_1_25_v1")
        self.assertEqual(config["symbol"], "XAUTUSDT")
        self.assertEqual(config["strategy_mode"], "spot_volume_shift_long")
        self.assertAlmostEqual(config["grid_band_ratio"], 0.00084)
        self.assertAlmostEqual(config["attack_per_order_notional"], 40.0)
        self.assertAlmostEqual(config["inventory_hard_limit_notional"], 950.0)
        self.assertAlmostEqual(config["max_cumulative_notional"], 1_000_000.0)
        self.assertAlmostEqual(config["max_cumulative_loss_limit"], 800.0)
        self.assertEqual(config["state_path"], "output/xautusdt_spot_state.json")

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

    def test_load_runner_control_config_prefers_saved_config_over_stale_runner_config(self) -> None:
        control_path = Path("output/test_basedusdt_loop_runner_control.json")
        control_path.parent.mkdir(parents=True, exist_ok=True)
        control_path.write_text(
            json.dumps(
                {
                    "symbol": "BASEDUSDT",
                    "strategy_profile": "volume_long_v4",
                    "step_price": 0.003,
                    "autotune_symbol_enabled": False,
                    "volatility_trigger_enabled": False,
                }
            ),
            encoding="utf-8",
        )
        try:
            with patch("grid_optimizer.web._runner_control_path", return_value=control_path), patch(
                "grid_optimizer.web._read_runner_process_for_symbol",
                return_value={
                    "config": {
                        "symbol": "BASEDUSDT",
                        "strategy_profile": "volume_long_v4",
                        "step_price": 0.00012,
                        "autotune_symbol_enabled": True,
                        "volatility_trigger_enabled": True,
                    }
                },
            ):
                config = _load_runner_control_config("BASEDUSDT")
            self.assertEqual(config["step_price"], 0.003)
            self.assertFalse(config["autotune_symbol_enabled"])
            self.assertFalse(config["volatility_trigger_enabled"])
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

    def test_runner_preset_payload_applies_based_competition_neutral_profile(self) -> None:
        payload = _runner_preset_payload("based_competition_neutral_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["strategy_profile"], "based_competition_neutral_v1")
        self.assertEqual(payload["symbol"], "BASEDUSDT")
        self.assertEqual(payload["strategy_mode"], "inventory_target_neutral")
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertEqual(payload["neutral_center_interval_minutes"], 3)
        self.assertAlmostEqual(payload["neutral_band1_offset_ratio"], 0.009)
        self.assertAlmostEqual(payload["neutral_band2_offset_ratio"], 0.018)
        self.assertAlmostEqual(payload["neutral_band3_offset_ratio"], 0.036)
        self.assertAlmostEqual(payload["max_position_notional"], 360.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 540.0)
        self.assertAlmostEqual(payload["max_total_notional"], 900.0)

    def test_runner_preset_payload_applies_based_competition_neutral_aggressive_profile(self) -> None:
        payload = _runner_preset_payload("based_competition_neutral_aggressive_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["strategy_profile"], "based_competition_neutral_aggressive_v1")
        self.assertEqual(payload["symbol"], "BASEDUSDT")
        self.assertEqual(payload["strategy_mode"], "inventory_target_neutral")
        self.assertFalse(payload["autotune_symbol_enabled"])
        self.assertEqual(payload["neutral_center_interval_minutes"], 3)
        self.assertAlmostEqual(payload["neutral_band1_offset_ratio"], 0.005)
        self.assertAlmostEqual(payload["neutral_band2_offset_ratio"], 0.010)
        self.assertAlmostEqual(payload["neutral_band3_offset_ratio"], 0.020)
        self.assertAlmostEqual(payload["neutral_band1_target_ratio"], 0.30)
        self.assertAlmostEqual(payload["neutral_band2_target_ratio"], 0.70)
        self.assertAlmostEqual(payload["neutral_band3_target_ratio"], 1.00)
        self.assertAlmostEqual(payload["max_position_notional"], 540.0)
        self.assertAlmostEqual(payload["max_short_position_notional"], 900.0)
        self.assertAlmostEqual(payload["max_total_notional"], 1440.0)

    def test_runner_preset_payload_rejects_based_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=BASEDUSDT"):
            _runner_preset_payload("based_competition_neutral_v1", {"symbol": "BTCUSDT"})

    def test_runner_preset_payload_rejects_based_competition_neutral_aggressive_profile_for_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires symbol=BASEDUSDT"):
            _runner_preset_payload("based_competition_neutral_aggressive_v1", {"symbol": "BTCUSDT"})

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_runner_preset_payload_normalizes_custom_grid_runtime(self) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
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
        self.assertEqual(config["state_path"], "output/ensousdt_loop_state.json")
        self.assertGreater(config["step_price"], 0)

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

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_resolve_runner_start_config_uses_selected_captured_preset(self) -> None:
        path = Path("output/test_runner_user_presets.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "captured_bard": {
                        "key": "captured_bard",
                        "label": "BARD captured",
                        "description": "captured",
                        "symbol": "BARDUSDT",
                        "custom": True,
                        "startable": True,
                        "kind": "synthetic",
                        "source": "captured_running_config",
                        "config": {
                            "symbol": "BARDUSDT",
                            "strategy_profile": "captured_bard",
                            "strategy_mode": "synthetic_neutral",
                            "step_price": 0.0001,
                            "per_order_notional": 88.0,
                            "autotune_symbol_enabled": False,
                        },
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            config = _resolve_runner_start_config({"symbol": "BARDUSDT", "strategy_profile": "captured_bard"})
            self.assertEqual(config["strategy_profile"], "captured_bard")
            self.assertEqual(config["strategy_mode"], "synthetic_neutral")
            self.assertEqual(config["per_order_notional"], 88.0)
            self.assertFalse(config["autotune_symbol_enabled"])
        finally:
            path.unlink(missing_ok=True)

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

    def test_resolve_runner_start_config_preserves_saved_manual_runner_config(self) -> None:
        control_path = Path("output/test_basedusdt_loop_runner_control.json")
        control_path.parent.mkdir(parents=True, exist_ok=True)
        control_path.write_text(
            json.dumps(
                {
                    "symbol": "BASEDUSDT",
                    "strategy_profile": "volume_long_v4",
                    "step_price": 0.003,
                    "autotune_symbol_enabled": False,
                    "volatility_trigger_enabled": False,
                }
            ),
            encoding="utf-8",
        )
        try:
            with patch("grid_optimizer.web._runner_control_path", return_value=control_path), patch(
                "grid_optimizer.web._read_runner_process_for_symbol",
                return_value={
                    "config": {
                        "symbol": "BASEDUSDT",
                        "strategy_profile": "volume_long_v4",
                        "step_price": 0.00012,
                        "autotune_symbol_enabled": True,
                        "volatility_trigger_enabled": True,
                    }
                },
            ):
                config = _resolve_runner_start_config({"symbol": "BASEDUSDT", "strategy_profile": "volume_long_v4"})
            self.assertEqual(config["step_price"], 0.003)
            self.assertFalse(config["autotune_symbol_enabled"])
            self.assertFalse(config["volatility_trigger_enabled"])
        finally:
            control_path.unlink(missing_ok=True)

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
        self.assertIn("--fixed-center-enabled", command)
        self.assertIn("--reset-state", command)

    def test_build_flatten_command_requires_explicit_allow_loss_flag(self) -> None:
        base_config = {
            "symbol": "TESTUSDT",
            "client_order_prefix": "mftest",
            "sleep_seconds": 2.0,
            "recv_window": 5000,
            "max_consecutive_errors": 20,
            "events_jsonl": "output/test_maker_flatten_events.jsonl",
        }

        default_command = _build_flatten_command(base_config)
        allow_loss_command = _build_flatten_command(
            {
                **base_config,
                "allow_loss": True,
                "min_profit_ratio": 0.001,
            }
        )

        self.assertNotIn("--allow-loss", default_command)
        self.assertNotIn("--min-profit-ratio", default_command)
        self.assertIn("--allow-loss", allow_loss_command)
        self.assertIn("--min-profit-ratio", allow_loss_command)

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
                "rolling_hourly_loss_limit": 150.0,
                "max_cumulative_notional": 100000.0,
                "max_cumulative_loss_limit": 800.0,
            }
        )
        self.assertEqual(config["run_start_time"], "2026-03-31T01:00:00+00:00")
        self.assertEqual(config["run_end_time"], "2026-03-31T03:00:00+00:00")
        self.assertEqual(config["rolling_hourly_loss_limit"], 150.0)
        self.assertEqual(config["max_cumulative_notional"], 100000.0)
        self.assertEqual(config["max_cumulative_loss_limit"], 800.0)

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
                "rolling_hourly_loss_limit": 150.0,
                "max_cumulative_notional": 100000.0,
                "max_cumulative_loss_limit": 800.0,
            }
        )
        self.assertIn("--run-start-time", command)
        self.assertIn("2026-03-31T01:00:00+00:00", command)
        self.assertIn("--run-end-time", command)
        self.assertIn("--rolling-hourly-loss-limit", command)
        self.assertIn("--max-cumulative-notional", command)
        self.assertIn("--max-cumulative-loss-limit", command)
        self.assertIn("800.0", command)

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

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_runner_preset_summaries_filter_custom_presets_by_symbol(self) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
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

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_runner_preset_summaries_hide_non_whitelist_builtins_but_keep_custom_presets(self) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            json.dumps(
                {
                    "custom_xaut_competition_grid": {
                        "key": "custom_xaut_competition_grid",
                        "label": "合约竞赛库存网格",
                        "symbol": "XAUTUSDT",
                        "custom": True,
                        "kind": "synthetic",
                        "config": {"symbol": "XAUTUSDT", "strategy_mode": "synthetic_neutral"},
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            keys = {item["key"] for item in _runner_preset_summaries("XAUTUSDT")}
            self.assertEqual(
                keys,
                {
                    "volume_long_v4",
                    "based_volume_push_bard_v1",
                    "volume_short_v1",
                    "custom_xaut_competition_grid",
                },
            )
        finally:
            custom_path.unlink(missing_ok=True)

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_runner_preset_summaries_keep_built_in_presets_read_only(self) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
        custom_path.parent.mkdir(parents=True, exist_ok=True)
        custom_path.write_text(
            json.dumps(
                {
                    "volume_long_v4": {
                        "key": "volume_long_v4",
                        "label": "用户覆盖版本",
                        "description": "should be ignored",
                        "symbol": "OPNUSDT",
                        "custom": True,
                        "kind": "one_way",
                        "source": "custom_grid",
                        "config": {"strategy_mode": "one_way_short"},
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            summaries = {item["key"]: item for item in _runner_preset_summaries("OPNUSDT")}
            self.assertEqual(summaries["volume_long_v4"]["label"], "量优先做多 v4")
            self.assertFalse(summaries["volume_long_v4"]["custom"])
        finally:
            custom_path.unlink(missing_ok=True)

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"), create=True)
    def test_runner_preset_summaries_read_repo_managed_user_presets(self) -> None:
        path = Path("output/test_runner_user_presets.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "captured_bard": {
                        "key": "captured_bard",
                        "label": "BARD 线上运行参数",
                        "description": "captured",
                        "symbol": "BARDUSDT",
                        "custom": True,
                        "startable": True,
                        "kind": "synthetic",
                        "source": "captured_running_config",
                        "config": {"symbol": "BARDUSDT", "strategy_mode": "synthetic_neutral"},
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            summaries = _runner_preset_summaries("BARDUSDT")
            preset = next(item for item in summaries if item["key"] == "captured_bard")
            self.assertTrue(preset["custom"])
            self.assertEqual(preset["symbol"], "BARDUSDT")
        finally:
            path.unlink(missing_ok=True)

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"), create=True)
    def test_custom_grid_runner_preset_is_saved_into_repo_managed_store(self) -> None:
        path = Path("output/test_runner_user_presets.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        try:
            with patch("grid_optimizer.web._run_grid_preview") as mock_preview:
                mock_preview.return_value = {
                    "ok": True,
                    "summary": {
                        "symbol": "OPNUSDT",
                        "strategy_direction": "long",
                        "grid_count": 8,
                        "current_price": 0.25,
                        "position_budget_notional": 200.0,
                        "active_buy_orders": 4,
                        "active_sell_orders": 4,
                        "startup_long_notional": 30.0,
                        "full_long_entry_notional": 200.0,
                        "symbol_info": {"min_notional": 5.0, "min_qty": 1.0},
                    },
                    "rows": [],
                }
                result = _create_custom_grid_runner_preset(
                    {
                        "contract_type": "usdm",
                        "symbol": "OPNUSDT",
                        "name": "OPN 自定义",
                        "strategy_direction": "long",
                        "grid_level_mode": "arithmetic",
                        "min_price": 0.20,
                        "max_price": 0.30,
                        "n": 8,
                        "margin_amount": 100.0,
                        "leverage": 2.0,
                    }
                )
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertIn(result["preset_key"], saved)
            self.assertEqual(saved[result["preset_key"]]["source"], "custom_grid")
        finally:
            path.unlink(missing_ok=True)

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    @patch("grid_optimizer.web._read_runner_process_for_symbol")
    def test_save_running_runner_preset_captures_current_config(self, mock_read_runner) -> None:
        path = Path("output/test_runner_user_presets.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        mock_read_runner.return_value = {
            "is_running": True,
            "config": {
                "symbol": "BARDUSDT",
                "strategy_profile": "bard_12h_push_neutral_v2",
                "strategy_mode": "synthetic_neutral",
                "step_price": 0.0001,
                "per_order_notional": 100.0,
                "state_path": "output/nightusdt_loop_state.json",
                "plan_json": "output/nightusdt_loop_latest_plan.json",
                "submit_report_json": "output/nightusdt_loop_latest_submit.json",
                "summary_jsonl": "output/nightusdt_loop_events.jsonl",
            },
        }
        try:
            result = _save_running_runner_preset(
                {"symbol": "BARDUSDT", "name": "BARD 8788 当前运行", "description": "captured from monitor"}
            )
            self.assertEqual(result["preset"]["source"], "captured_running_config")
            self.assertEqual(result["preset"]["config"]["symbol"], "BARDUSDT")
            self.assertEqual(result["preset"]["config"]["strategy_mode"], "synthetic_neutral")
            self.assertEqual(result["preset"]["config"]["state_path"], "output/bardusdt_loop_state.json")
            self.assertEqual(result["preset"]["config"]["plan_json"], "output/bardusdt_loop_latest_plan.json")
            self.assertEqual(result["preset"]["config"]["submit_report_json"], "output/bardusdt_loop_latest_submit.json")
            self.assertEqual(result["preset"]["config"]["summary_jsonl"], "output/bardusdt_loop_events.jsonl")
        finally:
            path.unlink(missing_ok=True)

    @patch("grid_optimizer.web._read_runner_process_for_symbol")
    def test_save_running_runner_preset_rejects_missing_running_config(self, mock_read_runner) -> None:
        mock_read_runner.return_value = {"is_running": False, "config": {}}
        with self.assertRaisesRegex(ValueError, "没有运行中策略"):
            _save_running_runner_preset({"symbol": "BARDUSDT", "name": "bad"})

    def test_runner_preset_summaries_include_based_competition_builtins(self) -> None:
        based_keys = {item["key"] for item in _runner_preset_summaries("BASEDUSDT")}
        opn_keys = {item["key"] for item in _runner_preset_summaries("OPNUSDT")}
        self.assertIn("based_competition_neutral_v1", based_keys)
        self.assertIn("based_competition_neutral_aggressive_v1", based_keys)
        self.assertNotIn("based_competition_neutral_v1", opn_keys)
        self.assertNotIn("based_competition_neutral_aggressive_v1", opn_keys)

    def test_runner_preset_summaries_hide_volume_neutral_push_v1_from_dropdown(self) -> None:
        based = {item["key"]: item for item in _runner_preset_summaries("BASEDUSDT")}
        self.assertNotIn("volume_neutral_push_v1", based)
        payload = _runner_preset_payload("volume_neutral_push_v1", {"symbol": "BASEDUSDT"})
        self.assertEqual(payload["strategy_profile"], "volume_neutral_push_v1")

    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_runner_preset_summaries_include_custom_grid_edit_metadata(self) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
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
    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_update_custom_grid_runner_preset_preserves_key(self, mock_preview) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
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
    @patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
    def test_delete_custom_grid_runner_preset_removes_saved_preset(self, mock_read_runner) -> None:
        custom_path = Path("output/test_runner_user_presets.json")
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
