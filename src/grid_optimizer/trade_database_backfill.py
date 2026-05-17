from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .audit import build_audit_paths, iter_jsonl, read_json
from .trade_database import ensure_trade_database_schema, persist_cycle_snapshot, persist_trade_audit_files


def _read_json_dict(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    return payload if isinstance(payload, dict) else {}


def _symbol_from_events_path(path: Path) -> str:
    stem = path.stem
    if stem.endswith("_spot_loop_events"):
        return stem[: -len("_spot_loop_events")].upper()
    if stem.endswith("_loop_events"):
        return stem[: -len("_loop_events")].upper()
    if stem.endswith("_events"):
        return stem[: -len("_events")].upper()
    return stem.upper()


def _control_path_for_events(path: Path) -> Path:
    stem = path.stem
    if stem.endswith("_spot_loop_events"):
        return path.with_name(f"{stem[: -len('_spot_loop_events')]}_spot_loop_runner_control.json")
    if stem.endswith("_loop_events"):
        return path.with_name(f"{stem[: -len('_loop_events')]}_loop_runner_control.json")
    return path.with_name(f"{stem}_runner_control.json")


def _config_for_events(path: Path) -> dict[str, Any]:
    control = _read_json_dict(_control_path_for_events(path))
    config = control.get("config") if isinstance(control.get("config"), dict) else {}
    return dict(config or control)


def _market_type(path: Path, config: dict[str, Any]) -> str:
    raw = str(config.get("market_type") or "").strip().lower()
    if raw in {"spot", "futures"}:
        return raw
    return "spot" if "_spot_" in path.name else "futures"


def _strategy_mode(config: dict[str, Any]) -> str:
    return str(config.get("strategy_profile") or config.get("strategy_mode") or "unknown").strip() or "unknown"


def backfill_output_dir(output_dir: Path, *, symbol_filter: set[str] | None = None) -> dict[str, Any]:
    ensure_trade_database_schema()
    results: list[dict[str, Any]] = []
    paths = sorted({*output_dir.glob("*_loop_events.jsonl"), *output_dir.glob("*_spot_loop_events.jsonl")})
    for events_path in paths:
        symbol = _symbol_from_events_path(events_path)
        if symbol_filter and symbol not in symbol_filter:
            continue
        audit_paths = build_audit_paths(events_path)
        if not audit_paths["trade_audit"].exists():
            continue
        config = _config_for_events(events_path)
        result = persist_trade_audit_files(
            symbol=symbol,
            market_type=_market_type(events_path, config),
            strategy_mode=_strategy_mode(config),
            config=config,
            trade_audit_path=audit_paths["trade_audit"],
            income_audit_path=audit_paths["income_audit"],
        )
        cycle_inserted = 0
        for summary in iter_jsonl(events_path):
            if not isinstance(summary, dict) or "cycle" not in summary:
                continue
            cycle_result = persist_cycle_snapshot(
                symbol=symbol,
                market_type=_market_type(events_path, config),
                strategy_mode=_strategy_mode(config),
                config=config,
                summary=summary,
            )
            cycle_inserted += int(cycle_result.get("cycle_inserted") or 0)
        result["cycle_inserted"] = cycle_inserted
        result.update({"symbol": symbol, "events_path": str(events_path)})
        results.append(result)
    return {
        "ok": True,
        "file_count": len(results),
        "trade_inserted": sum(int(item.get("trade_inserted") or 0) for item in results),
        "income_inserted": sum(int(item.get("income_inserted") or 0) for item in results),
        "cycle_inserted": sum(int(item.get("cycle_inserted") or 0) for item in results),
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill strategy trade audit JSONL files into PostgreSQL.")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols; empty means all.")
    args = parser.parse_args()
    symbols = {item.strip().upper() for item in args.symbols.split(",") if item.strip()} or None
    result = backfill_output_dir(args.output_dir, symbol_filter=symbols)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
