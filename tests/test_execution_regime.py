from __future__ import annotations

import unittest
from argparse import Namespace

from grid_optimizer.execution_regime import (
    REGIME_CAUTION,
    REGIME_EXIT,
    REGIME_NORMAL,
    REGIME_SAFE,
    ExecutionRegimeConfig,
    ExecutionRegimeFeatures,
    ExecutionRegimeMemory,
    assess_execution_regime,
    calculate_risk_score,
)
from grid_optimizer.loop_runner import build_execution_regime_report


class ExecutionRegimeTests(unittest.TestCase):
    def _runner_args(self, **overrides):
        args = {
            "execution_regime_enabled": True,
            "execution_regime_vol_p50_ratio": 0.003,
            "execution_regime_vol_p95_ratio": 0.012,
            "execution_regime_spread_p50_bps": 4.0,
            "execution_regime_spread_p95_bps": 20.0,
            "execution_regime_trend_p50_ratio": 0.002,
            "execution_regime_trend_p95_ratio": 0.010,
            "execution_regime_depth_value": None,
            "execution_regime_depth_p10_notional": 0.0,
            "execution_regime_depth_p50_notional": 0.0,
            "execution_regime_impact_q": None,
            "execution_regime_anomaly_q": None,
            "execution_regime_safe_score_upper": 0.35,
            "execution_regime_normal_score_upper": 0.60,
            "execution_regime_caution_score_upper": 0.80,
            "execution_regime_recover_exit_to_caution_score": 0.75,
            "execution_regime_recover_caution_to_normal_score": 0.55,
            "execution_regime_recover_normal_to_safe_score": 0.30,
            "execution_regime_confirm_exit_to_caution": 5,
            "execution_regime_confirm_caution_to_normal": 3,
            "execution_regime_confirm_normal_to_safe": 5,
            "execution_regime_confirm_normal_to_caution": 2,
            "execution_regime_vol_exit_q": 0.95,
            "execution_regime_spread_exit_q": 0.95,
            "execution_regime_depth_exit_q": 0.10,
            "execution_regime_latency_ms": None,
            "execution_regime_latency_ms_exit": None,
            "execution_regime_order_failure_rate": None,
            "execution_regime_order_failure_rate_exit": None,
            "execution_regime_inventory_notional_limit": None,
            "execution_regime_rolling_loss_abs": None,
            "execution_regime_rolling_loss_limit": None,
        }
        args.update(overrides)
        return Namespace(**args)

    def test_calculate_risk_score_uses_depth_as_protective_feature(self) -> None:
        low_depth_score, _, _ = calculate_risk_score(
            ExecutionRegimeFeatures(
                vol_q=0.4,
                spread_q=0.4,
                impact_q=0.4,
                trend_q=0.4,
                anomaly_q=0.4,
                depth_q=0.1,
            )
        )
        high_depth_score, _, _ = calculate_risk_score(
            ExecutionRegimeFeatures(
                vol_q=0.4,
                spread_q=0.4,
                impact_q=0.4,
                trend_q=0.4,
                anomaly_q=0.4,
                depth_q=0.9,
            )
        )

        self.assertGreater(low_depth_score, high_depth_score)

    def test_safe_state_requires_recovery_confirmation_from_normal(self) -> None:
        features = ExecutionRegimeFeatures(
            vol_q=0.05,
            spread_q=0.05,
            impact_q=0.05,
            trend_q=0.05,
            anomaly_q=0.05,
            depth_q=0.95,
        )
        memory = ExecutionRegimeMemory(state=REGIME_NORMAL)

        for _ in range(4):
            decision = assess_execution_regime(features, previous=memory)
            self.assertEqual(decision.state, REGIME_NORMAL)
            self.assertEqual(decision.memory.pending_state, REGIME_SAFE)
            memory = decision.memory

        decision = assess_execution_regime(features, previous=memory)

        self.assertEqual(decision.state, REGIME_SAFE)
        self.assertIn("pending_transition_confirmed", decision.reason_codes)

    def test_normal_to_caution_requires_two_windows(self) -> None:
        features = ExecutionRegimeFeatures(
            vol_q=0.8,
            spread_q=0.7,
            impact_q=0.6,
            trend_q=0.65,
            anomaly_q=0.4,
            depth_q=0.55,
        )
        memory = ExecutionRegimeMemory(state=REGIME_NORMAL)

        first = assess_execution_regime(features, previous=memory)
        second = assess_execution_regime(features, previous=first.memory)

        self.assertEqual(first.raw_state, REGIME_CAUTION)
        self.assertEqual(first.state, REGIME_NORMAL)
        self.assertEqual(first.memory.pending_state, REGIME_CAUTION)
        self.assertEqual(second.state, REGIME_CAUTION)

    def test_hard_risk_overrides_confirmation_and_enters_exit(self) -> None:
        decision = assess_execution_regime(
            ExecutionRegimeFeatures(
                vol_q=0.96,
                spread_q=0.2,
                impact_q=0.2,
                trend_q=0.2,
                anomaly_q=0.2,
                depth_q=0.8,
            ),
            previous=ExecutionRegimeMemory(state=REGIME_SAFE),
        )

        self.assertEqual(decision.state, REGIME_EXIT)
        self.assertTrue(decision.hard_risk)
        self.assertIn("vol_q_exit", decision.reason_codes)
        self.assertFalse(decision.params.should_place_orders)
        self.assertTrue(decision.params.cancel_non_protective_orders)

    def test_invalid_market_data_enters_exit_even_with_low_score(self) -> None:
        decision = assess_execution_regime(
            ExecutionRegimeFeatures(
                vol_q=0.05,
                spread_q=0.05,
                impact_q=0.05,
                trend_q=0.05,
                anomaly_q=0.05,
                depth_q=0.95,
                valid_market_data=False,
            ),
            previous=ExecutionRegimeMemory(state=REGIME_NORMAL),
        )

        self.assertEqual(decision.state, REGIME_EXIT)
        self.assertIn("invalid_market_data", decision.reason_codes)

    def test_exit_recovers_to_caution_only_after_confirmation(self) -> None:
        features = ExecutionRegimeFeatures(
            vol_q=0.2,
            spread_q=0.2,
            impact_q=0.2,
            trend_q=0.2,
            anomaly_q=0.2,
            depth_q=0.8,
        )
        memory = ExecutionRegimeMemory(state=REGIME_EXIT)

        for _ in range(4):
            decision = assess_execution_regime(features, previous=memory)
            self.assertEqual(decision.state, REGIME_EXIT)
            self.assertEqual(decision.memory.pending_state, REGIME_CAUTION)
            memory = decision.memory

        decision = assess_execution_regime(features, previous=memory)

        self.assertEqual(decision.state, REGIME_CAUTION)
        self.assertEqual(decision.memory.pending_state, None)

    def test_missing_quantiles_are_neutral_and_auditable(self) -> None:
        decision = assess_execution_regime(
            ExecutionRegimeFeatures(
                vol_q=0.2,
                spread_q=None,
                impact_q=None,
                trend_q=0.2,
                anomaly_q=0.2,
                depth_q=0.8,
            )
        )

        self.assertIn("spread_q", decision.missing_features)
        self.assertIn("impact_q", decision.missing_features)
        self.assertIn("missing_spread_q", decision.reason_codes)
        self.assertEqual(decision.normalized_features["spread_q"], 0.5)

    def test_execution_params_become_more_conservative_in_caution(self) -> None:
        normal = assess_execution_regime(
            ExecutionRegimeFeatures(
                vol_q=0.35,
                spread_q=0.35,
                impact_q=0.35,
                trend_q=0.35,
                anomaly_q=0.35,
                depth_q=0.7,
            ),
            previous=ExecutionRegimeMemory(state=REGIME_NORMAL),
        )
        caution = assess_execution_regime(
            ExecutionRegimeFeatures(
                vol_q=0.8,
                spread_q=0.7,
                impact_q=0.6,
                trend_q=0.65,
                anomaly_q=0.4,
                depth_q=0.55,
            ),
            previous=ExecutionRegimeMemory(state=REGIME_NORMAL, pending_state=REGIME_CAUTION, pending_count=1),
        )

        self.assertEqual(normal.state, REGIME_NORMAL)
        self.assertEqual(caution.state, REGIME_CAUTION)
        self.assertGreater(caution.params.quote_offset_bps, normal.params.quote_offset_bps)
        self.assertGreater(caution.params.refresh_interval_ms, normal.params.refresh_interval_ms)
        self.assertLess(caution.params.order_size_pct, normal.params.order_size_pct)
        self.assertLess(caution.params.max_active_orders, normal.params.max_active_orders)

    def test_optional_operational_limits_trigger_hard_risk(self) -> None:
        config = ExecutionRegimeConfig(latency_ms_exit=1000.0, order_failure_rate_exit=0.25)

        decision = assess_execution_regime(
            ExecutionRegimeFeatures(
                vol_q=0.2,
                spread_q=0.2,
                impact_q=0.2,
                trend_q=0.2,
                anomaly_q=0.2,
                depth_q=0.8,
                latency_ms=1200.0,
                order_failure_rate=0.3,
            ),
            config=config,
        )

        self.assertEqual(decision.state, REGIME_EXIT)
        self.assertIn("latency_ms_exit", decision.reason_codes)
        self.assertIn("order_failure_rate_exit", decision.reason_codes)

    def test_runner_shadow_report_updates_state_without_applying_orders(self) -> None:
        state: dict[str, object] = {}
        report = build_execution_regime_report(
            args=self._runner_args(),
            state=state,
            market_guard={
                "window_1m": {"amplitude_ratio": 0.003, "return_ratio": 0.001},
                "window_3m": {"amplitude_ratio": 0.004, "return_ratio": 0.002},
            },
            bid_price=100.0,
            ask_price=100.1,
            mid_price=100.05,
            actual_net_notional=50.0,
        )

        self.assertTrue(report["enabled"])
        self.assertEqual(report["mode"], "shadow")
        self.assertFalse(report["applied"])
        self.assertIn(report["state"], {REGIME_SAFE, REGIME_NORMAL, REGIME_CAUTION, REGIME_EXIT})
        self.assertIn("execution_regime", state)

    def test_runner_shadow_report_marks_missing_window_features_as_neutral(self) -> None:
        state: dict[str, object] = {}
        report = build_execution_regime_report(
            args=self._runner_args(),
            state=state,
            market_guard={"warning": "kline_fetch_failed"},
            bid_price=100.0,
            ask_price=100.1,
            mid_price=100.05,
            actual_net_notional=50.0,
        )

        self.assertIn("vol_q", report["missing_features"])
        self.assertIn("trend_q", report["missing_features"])
        self.assertEqual(report["features"]["vol_q"], 0.5)
        self.assertEqual(report["features"]["trend_q"], 0.5)

    def test_runner_shadow_report_disabled_clears_memory(self) -> None:
        state = {"execution_regime": {"state": REGIME_CAUTION, "pending_count": 1}}

        report = build_execution_regime_report(
            args=self._runner_args(execution_regime_enabled=False),
            state=state,
            market_guard={},
            bid_price=100.0,
            ask_price=100.1,
            mid_price=100.05,
            actual_net_notional=50.0,
        )

        self.assertFalse(report["enabled"])
        self.assertNotIn("execution_regime", state)


if __name__ == "__main__":
    unittest.main()
