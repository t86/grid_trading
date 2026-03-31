from __future__ import annotations

import unittest
from pathlib import Path

from grid_optimizer.symbol_lists import (
    DEFAULT_SYMBOL_LISTS,
    get_symbol_list,
    load_symbol_lists,
    save_symbol_lists,
    set_symbol_list,
    update_symbol_list,
)


class SymbolListsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.path = Path("output/test_symbol_lists.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.unlink(missing_ok=True)

    def tearDown(self) -> None:
        self.path.unlink(missing_ok=True)

    def test_load_symbol_lists_defaults_when_file_missing(self) -> None:
        self.assertEqual(load_symbol_lists(self.path), DEFAULT_SYMBOL_LISTS)
        self.assertIn("BASEDUSDT", DEFAULT_SYMBOL_LISTS["competition"])

    def test_set_symbol_list_normalizes_and_deduplicates(self) -> None:
        symbols = set_symbol_list("monitor", [" nightusdt ", "xautusdt", "NIGHTUSDT"], self.path)
        self.assertEqual(symbols, ["NIGHTUSDT", "XAUTUSDT"])
        self.assertEqual(get_symbol_list("monitor", self.path), ["NIGHTUSDT", "XAUTUSDT"])

    def test_update_symbol_list_add_and_remove(self) -> None:
        save_symbol_lists({"monitor": ["NIGHTUSDT"], "competition": ["KATUSDT"]}, self.path)
        self.assertEqual(update_symbol_list("monitor", action="add", symbol="xautusdt", path=self.path), ["NIGHTUSDT", "XAUTUSDT"])
        self.assertEqual(update_symbol_list("monitor", action="remove", symbol="nightusdt", path=self.path), ["XAUTUSDT"])

    def test_save_symbol_lists_preserves_empty_list(self) -> None:
        saved = save_symbol_lists({"monitor": [], "competition": ["XAUTUSDT"]}, self.path)
        self.assertEqual(saved["monitor"], [])
        self.assertEqual(load_symbol_lists(self.path)["monitor"], [])


if __name__ == "__main__":
    unittest.main()
