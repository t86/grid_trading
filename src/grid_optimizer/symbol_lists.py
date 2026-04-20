from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

SYMBOL_LISTS_PATH = Path("output/symbol_lists.json")
DEFAULT_SYMBOL_LISTS: dict[str, list[str]] = {
    "monitor": ["SOONUSDT"],
    "competition": ["SOONUSDT"],
}
SUPPORTED_SYMBOL_LIST_TYPES = tuple(DEFAULT_SYMBOL_LISTS.keys())
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9_]{3,40}$")


def normalize_symbol_list_type(list_type: str | None) -> str:
    normalized = str(list_type or "").strip().lower()
    if normalized not in DEFAULT_SYMBOL_LISTS:
        raise ValueError(
            f"Unsupported list_type: {list_type}. "
            f"Supported: {', '.join(SUPPORTED_SYMBOL_LIST_TYPES)}"
        )
    return normalized


def normalize_symbol(symbol: Any) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol cannot be empty")
    if not _SYMBOL_PATTERN.fullmatch(normalized):
        raise ValueError(f"invalid symbol: {symbol}")
    return normalized


def _normalize_symbols(symbols: list[Any] | tuple[Any, ...] | None, *, fallback: list[str]) -> list[str]:
    deduped: list[str] = []
    raw_items = list(symbols) if symbols is not None else list(fallback)
    for item in raw_items:
        normalized = normalize_symbol(item)
        if normalized not in deduped:
            deduped.append(normalized)
    return deduped


def load_symbol_lists(path: Path = SYMBOL_LISTS_PATH) -> dict[str, list[str]]:
    data: dict[str, Any] = {}
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            data = loaded
    result: dict[str, list[str]] = {}
    for list_type, default_symbols in DEFAULT_SYMBOL_LISTS.items():
        symbols = data.get(list_type, default_symbols)
        if not isinstance(symbols, list):
            symbols = default_symbols
        result[list_type] = _normalize_symbols(symbols, fallback=default_symbols)
    return result


def save_symbol_lists(symbol_lists: dict[str, list[Any]], path: Path = SYMBOL_LISTS_PATH) -> dict[str, list[str]]:
    normalized = {
        list_type: _normalize_symbols(symbol_lists.get(list_type), fallback=default_symbols)
        for list_type, default_symbols in DEFAULT_SYMBOL_LISTS.items()
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return normalized


def get_symbol_list(list_type: str, path: Path = SYMBOL_LISTS_PATH) -> list[str]:
    normalized_type = normalize_symbol_list_type(list_type)
    return list(load_symbol_lists(path).get(normalized_type, DEFAULT_SYMBOL_LISTS[normalized_type]))


def set_symbol_list(list_type: str, symbols: list[Any], path: Path = SYMBOL_LISTS_PATH) -> list[str]:
    normalized_type = normalize_symbol_list_type(list_type)
    symbol_lists = load_symbol_lists(path)
    symbol_lists[normalized_type] = _normalize_symbols(symbols, fallback=DEFAULT_SYMBOL_LISTS[normalized_type])
    return save_symbol_lists(symbol_lists, path)[normalized_type]


def update_symbol_list(
    list_type: str,
    *,
    action: str,
    symbol: Any,
    path: Path = SYMBOL_LISTS_PATH,
) -> list[str]:
    normalized_type = normalize_symbol_list_type(list_type)
    normalized_action = str(action or "").strip().lower()
    normalized_symbol = normalize_symbol(symbol)
    symbol_lists = load_symbol_lists(path)
    items = list(symbol_lists[normalized_type])
    if normalized_action == "add":
        if normalized_symbol not in items:
            items.append(normalized_symbol)
    elif normalized_action == "remove":
        items = [item for item in items if item != normalized_symbol]
    else:
        raise ValueError("action must be add or remove")
    symbol_lists[normalized_type] = items
    return save_symbol_lists(symbol_lists, path)[normalized_type]
