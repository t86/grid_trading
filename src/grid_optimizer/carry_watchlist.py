from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .data import fetch_futures_premium_index

DEFAULT_WATCHLIST_PATH = Path("output/carry_watchlist.json")
DEFAULT_MONITOR_STATE_PATH = Path("output/carry_watchlist_monitor_state.json")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{3,32}$")
_SHANGHAI = timezone(timedelta(hours=8))


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_watchlist(path: Path = DEFAULT_WATCHLIST_PATH) -> list[str]:
    raw = _load_json(path).get("symbols", [])
    if not isinstance(raw, list):
        return []
    return sorted({str(symbol).upper().strip() for symbol in raw if _SYMBOL_RE.fullmatch(str(symbol).upper().strip())})


def update_watchlist(symbol: str, watched: bool, path: Path = DEFAULT_WATCHLIST_PATH) -> list[str]:
    normalized = str(symbol).upper().strip()
    if not _SYMBOL_RE.fullmatch(normalized):
        raise ValueError("invalid futures symbol")
    symbols = set(load_watchlist(path))
    if watched:
        symbols.add(normalized)
    else:
        symbols.discard(normalized)
    result = sorted(symbols)
    _save_json(path, {"symbols": result, "updated_at": datetime.now(timezone.utc).isoformat()})
    return result


def bark_configured() -> bool:
    return bool(str(os.environ.get("GRID_CARRY_BARK_ENDPOINT") or "").strip())


def _send_bark(symbol: str, previous_rate: float, current_rate: float) -> dict[str, Any]:
    endpoint = str(os.environ.get("GRID_CARRY_BARK_ENDPOINT") or "").strip().rstrip("/")
    if not endpoint:
        return {"sent": False, "error": "bark_disabled"}
    try:
        response = requests.post(
            endpoint,
            json={
                "title": f"资金费率转负：{symbol}",
                "body": f"持有观察池 {symbol} 的资金费率已从 {previous_rate:.4%} 转为 {current_rate:.4%}。请复核对冲与借币成本。",
                "group": "grid-carry-watchlist",
                "level": "timeSensitive",
                "isArchive": "1",
            },
            timeout=12,
        )
        response.raise_for_status()
        return {"sent": True, "error": None}
    except Exception as exc:
        return {"sent": False, "error": f"{type(exc).__name__}: {exc}"}


def check_watchlist(
    *,
    watchlist_path: Path = DEFAULT_WATCHLIST_PATH,
    state_path: Path = DEFAULT_MONITOR_STATE_PATH,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_now = now or datetime.now(timezone.utc)
    symbols = load_watchlist(watchlist_path)
    state = _load_json(state_path)
    entries = state.get("symbols", {})
    entries = entries if isinstance(entries, dict) else {}
    premium = {row["symbol"]: row for row in fetch_futures_premium_index("usdm")}
    # "一天一次" follows the operator's local trading day, rather than UTC.
    day = current_now.astimezone(_SHANGHAI).date().isoformat()
    transitions: list[dict[str, Any]] = []
    sent = 0
    for symbol in symbols:
        row = premium.get(symbol)
        if row is None:
            continue
        current_rate = float(row.get("funding_rate") or 0.0)
        previous = entries.get(symbol, {})
        previous = previous if isinstance(previous, dict) else {}
        previous_rate = previous.get("last_rate")
        transition = isinstance(previous_rate, (int, float)) and previous_rate > 0 and current_rate < 0
        result = {"symbol": symbol, "previous_rate": previous_rate, "current_rate": current_rate, "transition": transition, "sent": False}
        # Limit attempts too: an unavailable Bark endpoint must not result in a
        # request every five minutes for the same sign change.
        if transition and bark_configured() and previous.get("last_alert_attempt_day") != day:
            bark = _send_bark(symbol, float(previous_rate), current_rate)
            result.update(bark)
            previous["last_alert_attempt_day"] = day
            if bark.get("sent"):
                sent += 1
                previous["last_alert_day"] = day
        entries[symbol] = {**previous, "last_rate": current_rate, "last_checked_at": current_now.isoformat()}
        transitions.append(result)
    _save_json(state_path, {"symbols": entries, "updated_at": current_now.isoformat()})
    return {"ok": True, "watch_count": len(symbols), "bark_configured": bark_configured(), "bark_sent": sent, "rows": transitions}


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor carry watchlist funding-rate sign changes.")
    parser.add_argument("--watchlist-path", default=str(DEFAULT_WATCHLIST_PATH))
    parser.add_argument("--state-path", default=str(DEFAULT_MONITOR_STATE_PATH))
    args = parser.parse_args()
    print(json.dumps(check_watchlist(watchlist_path=Path(args.watchlist_path), state_path=Path(args.state_path)), ensure_ascii=False))


if __name__ == "__main__":
    main()
