from __future__ import annotations

import base64
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.web import (
    MONITOR_PAGE,
    STRATEGIES_PAGE,
    _basic_auth_header_matches,
    _build_custom_grid_runner_preset,
    _delete_custom_grid_runner_preset,
    _build_runner_command,
    _client_ip_allowed,
    _default_runtime_paths_for_symbol,
    _get_custom_runner_preset,
    _load_runner_control_config,
    _parse_allowed_networks,
    _resolve_runner_start_config,
    _run_loop_monitor_query,
    _runner_service_name_for_symbol,
    _runner_preset_payload,
    _runner_preset_summaries,
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

    def test_strategies_page_contains_manual_symbol_list_controls(self) -> None:
        self.assertIn('id="monitor_symbol_input"', STRATEGIES_PAGE)
        self.assertIn('id="competition_symbol_input"', STRATEGIES_PAGE)
        self.assertIn('id="monitor_symbol_chips"', STRATEGIES_PAGE)
        self.assertIn('id="competition_symbol_chips"', STRATEGIES_PAGE)
        self.assertIn("/api/symbol_lists", STRATEGIES_PAGE)

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
