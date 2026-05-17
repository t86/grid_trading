from __future__ import annotations

import json
import os
import sqlite3
import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

DEFAULT_AUDIT_LOOKBACK_DAYS = 7
_JSONL_LINE_COUNT_CACHE: dict[str, tuple[int, int, int]] = {}
_ISO_BOUNDS_CACHE: dict[str, tuple[int, int, datetime | None, datetime | None]] = {}
_ARCHIVED_TRADE_AUDIT_ROWS_CACHE: dict[str, tuple[int, int, list[dict[str, Any]]]] = {}


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


def strategy_adjustments_db_path(output_dir: Path | str = "output") -> Path:
    return Path(output_dir) / "strategy_adjustments.sqlite3"


def _top_level_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, dict[str, Any]]:
    diff: dict[str, dict[str, Any]] = {}
    for key in sorted(set(before) | set(after)):
        old = before.get(key)
        new = after.get(key)
        if old != new:
            diff[key] = {"before": old, "after": new}
    return diff


def _strategy_adjustment_event(
    *,
    symbol: str,
    before: dict[str, Any] | None,
    after: dict[str, Any],
    control_path: Path,
    source: str,
    actor: str | None,
    reason: str | None,
    created_at: str | None = None,
) -> dict[str, Any]:
    safe_before = dict(before or {})
    safe_after = dict(after or {})
    safe_symbol = str(symbol or safe_after.get("symbol") or safe_before.get("symbol") or "").upper().strip()
    return {
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        "server_id": os.environ.get("GRID_SERVER_ID") or socket.gethostname(),
        "symbol": safe_symbol,
        "source": str(source or "runner_control_save"),
        "actor": actor,
        "reason": reason,
        "control_path": str(control_path),
        "before": safe_before,
        "after": safe_after,
        "diff": _top_level_diff(safe_before, safe_after),
        "git_commit": os.environ.get("GRID_GIT_COMMIT") or None,
    }


def _ensure_strategy_adjustment_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            server_id TEXT,
            symbol TEXT NOT NULL,
            source TEXT NOT NULL,
            actor TEXT,
            reason TEXT,
            control_path TEXT NOT NULL,
            before_json TEXT NOT NULL,
            after_json TEXT NOT NULL,
            diff_json TEXT NOT NULL,
            event_json TEXT NOT NULL
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(strategy_adjustments)")}
    for name, ddl in {
        "server_id": "ALTER TABLE strategy_adjustments ADD COLUMN server_id TEXT",
        "event_json": "ALTER TABLE strategy_adjustments ADD COLUMN event_json TEXT NOT NULL DEFAULT '{}'",
    }.items():
        if name not in columns:
            conn.execute(ddl)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategy_adjustments_symbol_time "
        "ON strategy_adjustments(symbol, created_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_adjustment_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            target_url TEXT NOT NULL,
            event_json TEXT NOT NULL,
            error TEXT,
            delivered_at TEXT
        )
        """
    )


def _post_strategy_adjustment_to_controller(event: dict[str, Any], *, timeout: float = 2.0) -> tuple[bool, str | None]:
    url = str(os.environ.get("GRID_STRATEGY_AUDIT_CONTROLLER_URL") or "").strip()
    if not url:
        return True, None
    token = str(os.environ.get("GRID_STRATEGY_AUDIT_TOKEN") or "").strip()
    data = json.dumps(event, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if token:
        headers["X-Grid-Audit-Token"] = token
    try:
        with urlopen(Request(url, data=data, headers=headers, method="POST"), timeout=timeout) as response:
            status = getattr(response, "status", 200)
            if 200 <= int(status) < 300:
                return True, None
            return False, f"HTTP {status}"
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        return False, f"{type(exc).__name__}: {exc}"


def append_strategy_adjustment_audit(
    *,
    symbol: str,
    before: dict[str, Any] | None,
    after: dict[str, Any],
    control_path: Path,
    source: str = "runner_control_save",
    actor: str | None = None,
    reason: str | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Persist runner strategy/config changes locally and optionally mirror to a controller."""

    target_db = db_path or strategy_adjustments_db_path(Path(control_path).parent)
    target_db.parent.mkdir(parents=True, exist_ok=True)
    event = _strategy_adjustment_event(
        symbol=symbol,
        before=before,
        after=after,
        control_path=control_path,
        source=source,
        actor=actor,
        reason=reason,
    )
    with sqlite3.connect(target_db) as conn:
        _ensure_strategy_adjustment_schema(conn)
        conn.execute(
            """
            INSERT INTO strategy_adjustments (
                created_at, server_id, symbol, source, actor, reason, control_path,
                before_json, after_json, diff_json, event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["created_at"],
                event["server_id"],
                event["symbol"],
                event["source"],
                actor,
                reason,
                str(control_path),
                json.dumps(event["before"], ensure_ascii=False, sort_keys=True),
                json.dumps(event["after"], ensure_ascii=False, sort_keys=True),
                json.dumps(event["diff"], ensure_ascii=False, sort_keys=True),
                json.dumps(event, ensure_ascii=False, sort_keys=True),
            ),
        )
        delivered, error = _post_strategy_adjustment_to_controller(event)
        if not delivered:
            conn.execute(
                """
                INSERT INTO strategy_adjustment_outbox (
                    created_at, target_url, event_json, error, delivered_at
                ) VALUES (?, ?, ?, ?, NULL)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    str(os.environ.get("GRID_STRATEGY_AUDIT_CONTROLLER_URL") or ""),
                    json.dumps(event, ensure_ascii=False, sort_keys=True),
                    error,
                ),
            )
    return event


def ingest_strategy_adjustment_event(
    event: dict[str, Any],
    *,
    db_path: Path | None = None,
) -> dict[str, Any]:
    target_db = db_path or strategy_adjustments_db_path()
    target_db.parent.mkdir(parents=True, exist_ok=True)
    safe_event = dict(event or {})
    before = safe_event.get("before") if isinstance(safe_event.get("before"), dict) else {}
    after = safe_event.get("after") if isinstance(safe_event.get("after"), dict) else {}
    diff = safe_event.get("diff") if isinstance(safe_event.get("diff"), dict) else _top_level_diff(before, after)
    created_at = str(safe_event.get("created_at") or datetime.now(timezone.utc).isoformat())
    symbol = str(safe_event.get("symbol") or after.get("symbol") or before.get("symbol") or "").upper().strip()
    with sqlite3.connect(target_db) as conn:
        _ensure_strategy_adjustment_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO strategy_adjustments (
                created_at, server_id, symbol, source, actor, reason, control_path,
                before_json, after_json, diff_json, event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                str(safe_event.get("server_id") or ""),
                symbol,
                str(safe_event.get("source") or "controller_ingest"),
                safe_event.get("actor"),
                safe_event.get("reason"),
                str(safe_event.get("control_path") or ""),
                json.dumps(before, ensure_ascii=False, sort_keys=True),
                json.dumps(after, ensure_ascii=False, sort_keys=True),
                json.dumps(diff, ensure_ascii=False, sort_keys=True),
                json.dumps(safe_event, ensure_ascii=False, sort_keys=True),
            ),
        )
        row_id = int(cursor.lastrowid or 0)
    return {"id": row_id, "symbol": symbol, "created_at": created_at}


def read_strategy_adjustments(
    *,
    db_path: Path | None = None,
    symbol: str | None = None,
    server_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    target_db = db_path or strategy_adjustments_db_path()
    if not target_db.exists():
        return []
    safe_limit = min(max(int(limit or 100), 1), 1000)
    where: list[str] = []
    params: list[Any] = []
    normalized_symbol = str(symbol or "").upper().strip()
    normalized_server = str(server_id or "").strip()
    if normalized_symbol:
        where.append("symbol = ?")
        params.append(normalized_symbol)
    if normalized_server:
        where.append("server_id = ?")
        params.append(normalized_server)
    sql = (
        "SELECT id, created_at, server_id, symbol, source, actor, reason, control_path, "
        "before_json, after_json, diff_json, event_json "
        "FROM strategy_adjustments"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(safe_limit)
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(target_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(sql, params):
            item = dict(row)
            for key in ("before_json", "after_json", "diff_json", "event_json"):
                try:
                    item[key[:-5] if key.endswith("_json") else key] = json.loads(item.pop(key) or "{}")
                except json.JSONDecodeError:
                    item[key[:-5] if key.endswith("_json") else key] = {}
            rows.append(item)
    return rows


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


def related_trade_audit_paths(path: Path) -> list[Path]:
    """Return current and archived trade audit files for a symbol audit path."""

    audit_path = Path(path)
    paths: list[Path] = []
    seen: set[Path] = set()

    def add(candidate: Path) -> None:
        normalized = candidate.resolve() if candidate.exists() else candidate
        if normalized in seen:
            return
        seen.add(normalized)
        paths.append(candidate)

    parent = audit_path.parent
    if parent.exists():
        name_pattern = f"{audit_path.name}*"
        archive_paths: list[Path] = []
        for archive_dir in sorted(parent.glob("archive*")):
            if not archive_dir.is_dir():
                continue
            archive_paths.extend(
                candidate
                for candidate in sorted(archive_dir.rglob(name_pattern))
                if candidate.is_file()
            )
        archive_paths.sort(key=lambda candidate: (candidate.stat().st_mtime_ns, str(candidate)))
        for candidate in archive_paths:
            add(candidate)

    add(audit_path)
    return paths


def _read_archived_trade_audit_rows(path: Path) -> list[dict[str, Any]]:
    try:
        stat = path.stat()
    except OSError:
        return []
    signature = (int(stat.st_mtime_ns), int(stat.st_size))
    cache_key = str(path)
    cached = _ARCHIVED_TRADE_AUDIT_ROWS_CACHE.get(cache_key)
    if cached is not None and cached[:2] == signature:
        return cached[2]
    rows = list(iter_jsonl(path))
    _ARCHIVED_TRADE_AUDIT_ROWS_CACHE[cache_key] = (signature[0], signature[1], rows)
    return rows


def read_trade_audit_rows(
    path: Path,
    *,
    include_archives: bool = True,
    limit: int = 0,
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
    """Read trade audit rows across current and archived files, de-duplicated."""

    rows: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()
    paths = related_trade_audit_paths(path) if include_archives else [Path(path)]
    current_path = Path(path)
    try:
        current_resolved = current_path.resolve()
    except OSError:
        current_resolved = current_path
    for audit_path in paths:
        try:
            audit_resolved = audit_path.resolve()
        except OSError:
            audit_resolved = audit_path
        source_rows = (
            iter_jsonl(audit_path)
            if audit_resolved == current_resolved
            else _read_archived_trade_audit_rows(audit_path)
        )
        for item in source_rows:
            if predicate is not None and not predicate(item):
                continue
            ts_ms = trade_row_time_ms(item)
            key = trade_row_key(item)
            identity = (ts_ms, key)
            if ts_ms > 0 and key:
                if identity in seen:
                    continue
                seen.add(identity)
            rows.append(item)
    sorted_rows = sorted(rows, key=lambda item: (trade_row_time_ms(item), trade_row_key(item)))
    return sorted_rows[-limit:] if limit > 0 else sorted_rows


def count_jsonl_lines(path: Path, *, max_scan_bytes: int | None = None) -> int | None:
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
    if max_scan_bytes is not None and int(stat.st_size) > max_scan_bytes:
        return None

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
