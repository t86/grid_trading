from __future__ import annotations

import unittest

from grid_optimizer.competition import (
    COMPETITION_SYMBOLS,
    build_competition_strategy,
    competition_profile_keys,
    competition_symbols,
)


class CompetitionTests(unittest.TestCase):
    def test_supported_competition_symbols_include_kat_opn_and_robo(self) -> None:
        self.assertEqual(tuple(competition_symbols()), COMPETITION_SYMBOLS)
        self.assertIn("KATUSDT", COMPETITION_SYMBOLS)
        self.assertIn("OPNUSDT", COMPETITION_SYMBOLS)
        self.assertIn("ROBOUSDT", COMPETITION_SYMBOLS)

    def test_build_competition_strategy_uses_reference_price_band(self) -> None:
        strategy = build_competition_strategy(reference_price=100.0, profile_key="conservative")
        self.assertAlmostEqual(strategy["min_price"], 97.0, places=8)
        self.assertAlmostEqual(strategy["max_price"], 103.0, places=8)
        self.assertEqual(strategy["n"], 20)
        self.assertEqual(strategy["allocation_mode"], "linear_reverse")
        self.assertEqual(strategy["strategy_direction"], "long")

    def test_build_competition_strategy_rejects_unknown_profile(self) -> None:
        with self.assertRaises(ValueError):
            build_competition_strategy(reference_price=100.0, profile_key="unknown")

    def test_profile_keys_are_stable(self) -> None:
        self.assertEqual(competition_profile_keys(), ["conservative", "aggressive"])
