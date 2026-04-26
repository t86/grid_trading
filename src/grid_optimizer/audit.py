from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

DEFAULT_AUDIT_LOOKBACK_DAYS = 7
_JSONL_LINE_COUNT_CACHE: dict[str, tuple[int, int, int]] = {}
_ISO_BOUNDS_CACHE: dict[str, tuple[int, int, datetime | None, datetime | None]] = {}


def build_audit_paths(events_path: str | Path) -> dict[str, Path]:
    path = Path(events_path)
    stem = path.stem
    base = stem[:-7] if stem.endswith("_events") else stem
    return {
        "plan_audit": path.with_name(f"{base}_plan_audit.jsonl"),
        "submit_audit": path.with_name(f"{base}_submit_audit.jsonl"),
        "order_audit": path.with_name(f"{base}_order_audit.jsonl"),
        "trade_audit": path.with_name(f"{base}_trade_audit.jsonl"),
        "income_audit": path.with_name(f"{base}_income_audit.jsonl"),
        "audit_state": path.with_name(f"{base}_audit_state.json"),
    }


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    yield item
    except OSError:
        return


def read_jsonl(path: Path, limit: int = 500) -> list[dict[str, Any]]:
    if limit > 0:
        rows: deque[dict[str, Any]] = deque(maxlen=limit)
        for item in iter_jsonl(path):
            rows.append(item)
        return list(rows)
    return list(iter_jsonl(path))


def read_jsonl_filtered(
    path: Path,
    *,
    limit: int = 500,
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=limit if limit > 0 else None)
    for item in iter_jsonl(path):
        if predicate is not None and not predicate(item):
            continue
        rows.append(item)
    return list(rows)


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        stat = path.stat()
    except OSError:
        return 0
    cache_key = str(path)
    cached = _JSONL_LINE_COUNT_CACHE.get(cache_key)
    signature = (int(stat.st_mtime_ns), int(stat.st_size))
    if cached is not None and cached[:2] == signature:
        return int(cached[2])

    count = 0
    try:
        with path.open("rb") as f:
            for raw in f:
                if raw.strip():
                    count += 1
    except OSError:
        return 0

    _JSONL_LINE_COUNT_CACHE[cache_key] = (signature[0], signature[1], count)
    return count


def parse_iso_ts(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def scan_iso_bounds(path: Path, field: str = "ts") -> tuple[datetime | None, datetime | None]:
    if not path.exists():
        return None, None
    try:
        stat = path.stat()
    except OSError:
        return None, None
    cache_key = f"{path}:{field}"
    signature = (int(stat.st_mtime_ns), int(stat.st_size))
    cached = _ISO_BOUNDS_CACHE.get(cache_key)
    if cached is not None and cached[:2] == signature:
        return cached[2], cached[3]

    first_ts: datetime | None = None
    last_ts: datetime | None = None
    for item in iter_jsonl(path):
        ts = parse_iso_ts(item.get(field))
        if ts is None:
            continue
        if first_ts is None or ts < first_ts:
            first_ts = ts
        if last_ts is None or ts > last_ts:
            last_ts = ts
    _ISO_BOUNDS_CACHE[cache_key] = (signature[0], signature[1], first_ts, last_ts)
    return first_ts, last_ts


def epoch_ms(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def trade_row_time_ms(row: dict[str, Any]) -> int:
    return epoch_ms(row.get("time"))


def trade_row_key(row: dict[str, Any]) -> str:
    raw_id = str(row.get("id", "")).strip()
    if raw_id:
        return raw_id
    return ":".join(
        [
            str(row.get("time", "")).strip(),
            str(row.get("side", "")).upper().strip(),
            str(row.get("price", "")).strip(),
            str(row.get("qty", "")).strip(),
        ]
    )


def income_row_time_ms(row: dict[str, Any]) -> int:
    return epoch_ms(row.get("time"))


def income_row_key(row: dict[str, Any]) -> str:
    raw_tran_id = str(row.get("tranId", "")).strip()
    if raw_tran_id:
        return raw_tran_id
    return ":".join(
        [
            str(row.get("time", "")).strip(),
            str(row.get("incomeType", "")).upper().strip(),
            str(row.get("symbol", "")).upper().strip(),
            str(row.get("asset", "")).upper().strip(),
            str(row.get("income", "")).strip(),
            str(row.get("info", "")).strip(),
        ]
    )


def fetch_time_paged(
    *,
    fetch_page: Callable[..., list[dict[str, Any]]],
    start_time_ms: int | None,
    end_time_ms: int | None,
    limit: int,
    row_time_ms: Callable[[dict[str, Any]], int],
    row_key: Callable[[dict[str, Any]], str],
    max_window_ms: int | None = DEFAULT_AUDIT_LOOKBACK_DAYS * 24 * 60 * 60 * 1000 - 1,
    max_pages: int = 200,
) -> list[dict[str, Any]]:
    cursor = start_time_ms
    rows: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()

    for _ in range(max_pages):
        page_end_time_ms = end_time_ms
        if cursor is not None and max_window_ms is not None and max_window_ms > 0:
            window_end_time_ms = cursor + max_window_ms
            page_end_time_ms = min(end_time_ms, window_end_time_ms) if end_time_ms is not None else window_end_time_ms

        page = fetch_page(start_time_ms=cursor, end_time_ms=page_end_time_ms, limit=limit)
        can_advance_window = (
            cursor is not None
            and page_end_time_ms is not None
            and end_time_ms is not None
            and page_end_time_ms < end_time_ms
        )
        if not page:
            if can_advance_window:
                cursor = page_end_time_ms + 1
                continue
            break
        normalized = [item for item in page if isinstance(item, dict)]
        normalized.sort(key=lambda item: (row_time_ms(item), row_key(item)))
        max_page_time: int | None = None
        for item in normalized:
            ts_ms = row_time_ms(item)
            key = row_key(item)
            if ts_ms <= 0 or not key:
                continue
            identity = (ts_ms, key)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(item)
            if max_page_time is None or ts_ms > max_page_time:
                max_page_time = ts_ms
        if len(normalized) < limit or max_page_time is None:
            if can_advance_window:
                cursor = page_end_time_ms + 1
                continue
            break
        next_cursor = max_page_time + 1
        if cursor is not None and next_cursor <= cursor:
            next_cursor = cursor + 1
        if end_time_ms is not None and next_cursor > end_time_ms:
            break
        cursor = next_cursor
    return rows


def collect_new_rows(
    *,
    rows: list[dict[str, Any]],
    last_time_ms: int | None,
    last_keys_at_time: list[str] | None,
    row_time_ms: Callable[[dict[str, Any]], int],
    row_key: Callable[[dict[str, Any]], str],
) -> tuple[list[dict[str, Any]], int | None, list[str]]:
    current_last_time = last_time_ms if last_time_ms and last_time_ms > 0 else None
    current_keys = set(str(item) for item in (last_keys_at_time or []) if str(item).strip())
    fresh_rows: list[dict[str, Any]] = []

    normalized = [item for item in rows if isinstance(item, dict)]
    normalized.sort(key=lambda item: (row_time_ms(item), row_key(item)))

    for item in normalized:
        ts_ms = row_time_ms(item)
        key = row_key(item)
        if ts_ms <= 0 or not key:
            continue
        if current_last_time is not None:
            if ts_ms < current_last_time:
                continue
            if ts_ms == current_last_time and key in current_keys:
                continue
        fresh_rows.append(item)
        if current_last_time is None or ts_ms > current_last_time:
            current_last_time = ts_ms
            current_keys = {key}
        elif ts_ms == current_last_time:
            current_keys.add(key)

    return fresh_rows, current_last_time, sorted(current_keys)
