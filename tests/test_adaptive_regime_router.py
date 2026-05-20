from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.adaptive_regime_router import (
    REGIME_DOWN,
    REGIME_NO_TRADE,
    REGIME_RANGE,
    REGIME_UP,
    AdaptiveRegimeRouterConfig,
    AdaptiveRegimeRouterInputs,
    adaptive_regime_router_state_snapshot,
    resolve_adaptive_regime_router_control,
)


NOW = datetime(2026, 5, 20, 8, 0, tzinfo=timezone.utc)


def _inputs(**overrides):
    data = {
        "now": NOW,
        "last_state": {},
        "return_1m_ratio": 0.0005,
        "amplitude_1m_ratio": 0.0020,
        "return_5m_ratio": 0.0010,
        "amplitude_5m_ratio": 0.0080,
        "spread_bps": 6.0,
        "depth_notional": None,
        "long_notional": 20.0,
        "short_notional": 15.0,
        "actual_net_notional": 5.0,
    }
    data.update(overrides)
    return AdaptiveRegimeRouterInputs(**data)


class AdaptiveRegimeRouterTests(unittest.TestCase):
    def test_disabled_returns_passthrough(self) -> None:
        control = resolve_adaptive_regime_router_control(
            config=AdaptiveRegimeRouterConfig(enabled=False),
            inputs=_inputs(),
        )

        self.assertFalse(control["enabled"])
        self.assertEqual(control["regime"], "disabled")
        self.assertTrue(control["entry_allowed"])
        self.assertEqual(control["step_scale"], 1.0)

    def test_range_ping_pong_keeps_both_sides_open(self) -> None:
        control = resolve_adaptive_regime_router_control(
            config=AdaptiveRegimeRouterConfig(enabled=True, range_max_entry_orders=3),
            inputs=_inputs(),
        )

        self.assertEqual(control["regime"], REGIME_RANGE)
        self.assertTrue(control["allow_entry_long"])
        self.assertTrue(control["allow_entry_short"])
        self.assertEqual(control["max_entry_long_orders"], 3)
        self.assertEqual(control["max_entry_short_orders"], 3)

    def test_downtrend_confirms_then_allows_short_entries_only(self) -> None:
        config = AdaptiveRegimeRouterConfig(enabled=True, confirm_cycles=2, min_dwell_seconds=0.0)

        first = resolve_adaptive_regime_router_control(
            config=config,
            inputs=_inputs(return_1m_ratio=-0.006, return_5m_ratio=-0.018),
        )
        self.assertEqual(first["regime"], REGIME_RANGE)
        self.assertEqual(first["candidate_regime"], REGIME_DOWN)
        self.assertEqual(first["pending_regime"], REGIME_DOWN)

        state = adaptive_regime_router_state_snapshot(first, updated_at=NOW.isoformat())
        second = resolve_adaptive_regime_router_control(
            config=config,
            inputs=_inputs(
                now=NOW + timedelta(seconds=10),
                last_state=state,
                return_1m_ratio=-0.005,
                return_5m_ratio=-0.016,
            ),
        )

        self.assertEqual(second["regime"], REGIME_DOWN)
        self.assertFalse(second["allow_entry_long"])
        self.assertTrue(second["allow_entry_short"])
        self.assertEqual(second["max_entry_long_orders"], 0)
        self.assertEqual(second["max_entry_short_orders"], 2)
        self.assertTrue(second["switched"])

    def test_uptrend_confirms_then_allows_long_entries_only(self) -> None:
        config = AdaptiveRegimeRouterConfig(enabled=True, confirm_cycles=2, min_dwell_seconds=0.0)

        first = resolve_adaptive_regime_router_control(
            config=config,
            inputs=_inputs(return_1m_ratio=0.007, return_5m_ratio=0.012),
        )
        state = adaptive_regime_router_state_snapshot(first, updated_at=NOW.isoformat())
        second = resolve_adaptive_regime_router_control(
            config=config,
            inputs=_inputs(
                now=NOW + timedelta(seconds=10),
                last_state=state,
                return_1m_ratio=0.0065,
                return_5m_ratio=0.011,
            ),
        )

        self.assertEqual(second["regime"], REGIME_UP)
        self.assertTrue(second["allow_entry_long"])
        self.assertFalse(second["allow_entry_short"])
        self.assertEqual(second["max_entry_long_orders"], 2)
        self.assertEqual(second["max_entry_short_orders"], 0)

    def test_shock_enters_no_trade_immediately(self) -> None:
        control = resolve_adaptive_regime_router_control(
            config=AdaptiveRegimeRouterConfig(enabled=True, confirm_cycles=3),
            inputs=_inputs(return_1m_ratio=-0.025, amplitude_1m_ratio=0.030),
        )

        self.assertEqual(control["regime"], REGIME_NO_TRADE)
        self.assertFalse(control["entry_allowed"])
        self.assertFalse(control["allow_entry_long"])
        self.assertFalse(control["allow_entry_short"])
        self.assertEqual(control["max_entry_long_orders"], 0)
        self.assertEqual(control["max_entry_short_orders"], 0)
        self.assertTrue(control["cancel_stale_entries"])

    def test_wide_spread_blocks_entries_before_trend_classification(self) -> None:
        control = resolve_adaptive_regime_router_control(
            config=AdaptiveRegimeRouterConfig(enabled=True, max_spread_bps=20.0),
            inputs=_inputs(spread_bps=28.0, return_5m_ratio=0.030),
        )

        self.assertEqual(control["regime"], REGIME_NO_TRADE)
        self.assertIn("spread_bps", control["reasons"][0])


if __name__ == "__main__":
    unittest.main()
