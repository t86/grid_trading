from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


def _load_module():
    module_path = Path("/Volumes/WORK/binance/wangge/scripts/bard_profit_protect_replay.py")
    spec = importlib.util.spec_from_file_location("bard_profit_protect_replay", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load bard_profit_protect_replay")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class BardProfitProtectReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def test_market_bias_entry_pause_blocks_counter_trend_entries(self) -> None:
        pause = self.module.resolve_market_bias_entry_pause(
            buy_pause_enabled=True,
            short_pause_enabled=True,
            market_bias={"bias_score": -0.4, "regime": "weak"},
            weak_buy_pause_threshold=0.15,
            strong_short_pause_threshold=0.15,
        )
        self.assertTrue(pause["buy_pause_active"])
        self.assertFalse(pause["short_pause_active"])

        pause = self.module.resolve_market_bias_entry_pause(
            buy_pause_enabled=True,
            short_pause_enabled=True,
            market_bias={"bias_score": 0.4, "regime": "strong"},
            weak_buy_pause_threshold=0.15,
            strong_short_pause_threshold=0.15,
        )
        self.assertFalse(pause["buy_pause_active"])
        self.assertTrue(pause["short_pause_active"])

    def test_resolve_volatility_trigger_action_stops_then_waits_then_resumes(self) -> None:
        config = {
            "volatility_trigger_enabled": True,
            "volatility_trigger_amplitude_ratio": 0.04,
            "volatility_trigger_abs_return_ratio": 0.02,
        }
        stop = self.module.resolve_volatility_trigger_action(
            config,
            current_amplitude_ratio=0.05,
            current_return_ratio=0.01,
            runner_running=True,
            paused_by_trigger=False,
        )
        self.assertEqual(stop["action"], "stop")

        wait = self.module.resolve_volatility_trigger_action(
            config,
            current_amplitude_ratio=0.05,
            current_return_ratio=0.01,
            runner_running=False,
            paused_by_trigger=True,
        )
        self.assertIsNone(wait["action"])
        self.assertEqual(wait["reason"], "waiting_for_volatility_cooldown")

        resume = self.module.resolve_volatility_trigger_action(
            config,
            current_amplitude_ratio=0.01,
            current_return_ratio=0.005,
            runner_running=False,
            paused_by_trigger=True,
        )
        self.assertEqual(resume["action"], "start")


if __name__ == "__main__":
    unittest.main()
