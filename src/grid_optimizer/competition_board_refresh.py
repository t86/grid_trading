from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any

from .competition_board import (
    COMPETITION_SOURCES,
    CACHE_PATH,
    UTC_PLUS_8,
    _attach_snapshot_analytics,
    _compose_board,
    _fetch_leaderboard_rows,
    _hinted_boards_for_source,
    _load_cached_snapshot,
    _now_iso,
    _persist_boards_to_history,
    _remember_snapshot,
    _write_json_file,
    competition_board_daily_refresh_due,
)


def _refresh_targeted_sources(
    *,
    slugs: set[str],
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = (now or datetime.now(UTC_PLUS_8)).astimezone(UTC_PLUS_8)
    base_snapshot = _load_cached_snapshot(max_age_seconds=None) or {
        "generated_at_utc": "",
        "boards": [],
        "entries": [],
        "errors": [],
    }
    boards = list(base_snapshot.get("boards", [])) if isinstance(base_snapshot.get("boards"), list) else []
    errors = list(base_snapshot.get("errors", [])) if isinstance(base_snapshot.get("errors"), list) else []
    refreshed: list[str] = []

    board_index = {
        str(item.get("board_key")): idx
        for idx, item in enumerate(boards)
        if isinstance(item, dict) and item.get("board_key")
    }

    for source in COMPETITION_SOURCES:
        if source.slug not in slugs:
            continue
        hinted = _hinted_boards_for_source(source)
        if hinted is None:
            errors.append(f"{source.slug}: missing hinted board config")
            continue
        meta, board_metas = hinted
        for board_meta in board_metas:
            preview = _compose_board(
                source,
                board_meta,
                {
                    "resource_id": int(board_meta.get("resourceId", 0) or 0),
                    "eligible_user_count": 0,
                    "eligible_metric_total": 0.0,
                    "updated_time_ms": 0,
                    "rows_truncated": False,
                    "last_rank_fetched": 0,
                    "rows": [],
                },
                meta if isinstance(meta, dict) else {},
            )
            if not competition_board_daily_refresh_due(preview, now=current_time):
                continue
            resource_id = int(board_meta.get("resourceId", 0) or 0)
            leaderboard = _fetch_leaderboard_rows(
                resource_id,
                str(board_meta.get("url", source.url)),
                max_rows=int(board_meta.get("maxRows", 200) or 200),
            )
            board = _compose_board(source, board_meta, leaderboard, meta if isinstance(meta, dict) else {})
            key = str(board.get("board_key"))
            if key in board_index:
                boards[board_index[key]] = board
            else:
                board_index[key] = len(boards)
                boards.append(board)
            refreshed.append(key)

    boards.sort(key=lambda item: (str(item.get("market", "")), str(item.get("symbol", "")), str(item.get("tab_label", ""))))
    generated_at = _now_iso()
    snapshot = {
        **base_snapshot,
        "generated_at_utc": generated_at,
        "boards": boards,
        "entries": base_snapshot.get("entries", []),
        "errors": errors,
    }
    _persist_boards_to_history(boards, snapshot_generated_at_utc=generated_at)
    snapshot = _attach_snapshot_analytics(snapshot)
    _write_json_file(CACHE_PATH, snapshot)
    _remember_snapshot(snapshot)
    return {
        "generated_at_utc": generated_at,
        "refreshed_board_keys": refreshed,
        "boards": boards,
        "errors": errors,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh targeted competition board caches.")
    parser.add_argument("--slug", action="append", dest="slugs", default=None)
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    slugs = {str(item).strip() for item in (args.slugs or []) if str(item).strip()}
    result = _refresh_targeted_sources(slugs=slugs or {"futures_chip", "futures_bill"})
    print(f"refreshed {len(result['refreshed_board_keys'])} boards at {result['generated_at_utc']}")
    for key in result["refreshed_board_keys"]:
        print(key)
    if result["errors"]:
        print("errors:")
        for item in result["errors"]:
            print(item)


if __name__ == "__main__":
    main()
