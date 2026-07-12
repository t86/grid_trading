from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

_CYCLE_BUDGET_KEY = "best_quote_maker_volume_cycle_budget_notional"
_MIN_CYCLE_BUDGET_KEY = "best_quote_maker_volume_min_cycle_budget_notional"
_TIER_BUDGETS_KEY = "budget_controller_tier_budgets"


def _parse_budgets(value: object) -> tuple[float, float, float, float, float]:
    if isinstance(value, str):
        raw_values: Sequence[object] = [part.strip() for part in value.split(",")]
    elif isinstance(value, Sequence):
        raw_values = value
    else:
        raise ValueError("budget tiers must be a comma-separated string or a five-item list")
    if len(raw_values) != 5:
        raise ValueError("budget tiers must contain defensive,cautious,cruise,boost1,boost2")
    budgets = tuple(float(item) for item in raw_values)
    if any(item <= 0 for item in budgets):
        raise ValueError("budget tiers must all be positive")
    return budgets  # type: ignore[return-value]


def resolve_budget_tiers(
    control: dict[str, Any],
    override: object | None = None,
) -> tuple[tuple[float, float, float, float, float], str]:
    """Resolve controller tiers without letting cron constants override runner config."""
    if override is not None:
        return _parse_budgets(override), "cli_override"
    configured_tiers = control.get(_TIER_BUDGETS_KEY)
    if configured_tiers is not None:
        return _parse_budgets(configured_tiers), "control_tiers"
    configured_cycle = float(control.get(_CYCLE_BUDGET_KEY) or 0.0)
    if configured_cycle <= 0:
        raise ValueError(f"missing positive {_CYCLE_BUDGET_KEY} in runner control")
    return (configured_cycle,) * 5, "runner_cycle_budget"


def resolve_min_cycle(control: dict[str, Any], override: float | None = None) -> float:
    if override is not None:
        if override <= 0:
            raise ValueError("min cycle budget must be positive")
        return float(override)
    configured = float(control.get(_MIN_CYCLE_BUDGET_KEY) or 0.0)
    if configured <= 0:
        raise ValueError(f"missing positive {_MIN_CYCLE_BUDGET_KEY} in runner control")
    return configured


def choose_tier(
    *,
    remaining: float,
    required_rate: float,
    volume_1h: float,
    pnl_1h: float,
    wear_1h: float,
    volume_30m: float,
    volatility_defensive: bool,
    budgets: tuple[float, float, float, float, float],
) -> tuple[str, float]:
    defensive, cautious, cruise, boost1, boost2 = budgets
    wear_ok = wear_1h <= 3.0 and pnl_1h > -2.0
    if remaining <= 0:
        return "done", cruise
    if (volume_1h >= 1500 and wear_1h > 3.0) or pnl_1h <= -4.0:
        return "defensive", defensive
    if (volume_1h >= 1500 and wear_1h > 1.5) or pnl_1h <= -2.0:
        return "cautious", cautious
    if not volatility_defensive and wear_ok and (
        volume_1h < 0.70 * required_rate or volume_30m < 500
    ):
        return "boost2", boost2
    if not volatility_defensive and wear_ok and volume_1h < 0.95 * required_rate:
        return "boost1", boost1
    return "cruise", cruise


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    temporary.replace(path)


def _fetch_day_trades(*, symbol: str, api_key: str, api_secret: str, start_ms: int) -> list[dict]:
    from grid_optimizer.data import fetch_futures_user_trades

    cursor = start_ms
    trades: list[dict] = []
    seen: set[object] = set()
    for _ in range(90):
        batch = fetch_futures_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=cursor,
            limit=1000,
        )
        fresh = [trade for trade in batch if trade.get("id") not in seen]
        for trade in fresh:
            seen.add(trade.get("id"))
        trades.extend(fresh)
        if len(batch) < 1000 or not fresh:
            break
        cursor = max(int(trade.get("time", 0)) for trade in batch)
    return trades


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Target-aware budget controller that reads budgets from runner control by default."
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--runner-wrapper", required=True)
    parser.add_argument(
        "--budgets",
        default=None,
        help="Optional five-tier override. Prefer budget_controller_tier_budgets in runner control.",
    )
    parser.add_argument("--min-cycle", type=float, default=None)
    parser.add_argument("--enforce", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    from grid_optimizer.monitor import summarize_user_trades

    args = build_parser().parse_args(argv)
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    day_start_ms = int(day_start.timestamp() * 1000)
    prefix = args.symbol.lower()
    workdir = Path(args.workdir)
    control_path = workdir / "output" / f"{prefix}_loop_runner_control.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    budgets, budget_source = resolve_budget_tiers(control, args.budgets)
    min_cycle = resolve_min_cycle(control, args.min_cycle)

    trades = _fetch_day_trades(
        symbol=args.symbol,
        api_key=os.environ["BINANCE_API_KEY"],
        api_secret=os.environ["BINANCE_API_SECRET"],
        start_ms=day_start_ms,
    )
    daily_volume = float(summarize_user_trades(trades)["gross_notional"])
    hour_trades = [trade for trade in trades if int(trade.get("time", 0)) >= now_ms - 3_600_000]
    hour_summary = summarize_user_trades(hour_trades)
    volume_1h = float(hour_summary["gross_notional"])
    pnl_1h = float(hour_summary["realized_pnl"])
    wear_1h = (-pnl_1h / volume_1h * 1e4) if volume_1h > 0 else 0.0
    volume_30m = sum(
        abs(float(trade.get("quoteQty") or 0.0))
        for trade in trades
        if int(trade.get("time", 0)) >= now_ms - 1_800_000
    )

    target = float(control.get("max_cumulative_notional") or 0.0)
    hours_left = max(((day_start.timestamp() + 86400) - now.timestamp()) / 3600.0, 0.25)
    remaining = max(target - daily_volume, 0.0)
    required_rate = remaining / hours_left
    volatility_defensive = False
    try:
        plan = json.loads(
            (workdir / "output" / f"{prefix}_loop_latest_plan.json").read_text(encoding="utf-8")
        )
        dynamic_control = (
            ((plan.get("best_quote_maker_volume") or {}).get("metrics") or {}).get("dynamic_control")
            or {}
        )
        volatility_defensive = str(dynamic_control.get("reason") or "") in {
            "high_volatility_defensive",
            "extreme_volatility_defensive",
        }
    except (OSError, ValueError, TypeError):
        pass

    tier, budget = choose_tier(
        remaining=remaining,
        required_rate=required_rate,
        volume_1h=volume_1h,
        pnl_1h=pnl_1h,
        wear_1h=wear_1h,
        volume_30m=volume_30m,
        volatility_defensive=volatility_defensive,
        budgets=budgets,
    )
    changed = float(control.get(_CYCLE_BUDGET_KEY, -1.0)) != budget
    status = {
        "ts": now.isoformat(),
        "symbol": args.symbol,
        "daily_volume": round(daily_volume),
        "target": target,
        "required_rate": round(required_rate),
        "volume_1h": round(volume_1h),
        "wear_1h": round(wear_1h, 2),
        "volume_30m": round(volume_30m),
        "volatility_defensive": volatility_defensive,
        "tier": tier,
        "budget": budget,
        "budget_source": budget_source,
        "resolved_budgets": budgets,
        "min_cycle": min_cycle,
        "changed": changed,
    }
    if args.enforce and changed:
        control[_CYCLE_BUDGET_KEY] = budget
        control[_MIN_CYCLE_BUDGET_KEY] = min_cycle
        control["budget_controller_tier"] = tier
        control["budget_controller_updated_at"] = now.isoformat()
        _write_json_atomic(control_path, control)
        completed = subprocess.run(
            [args.runner_wrapper, "restart", args.symbol],
            capture_output=True,
            text=True,
            check=False,
        )
        status["applied"] = True
        status["restart_returncode"] = completed.returncode
        if completed.returncode != 0:
            status["restart_stderr"] = completed.stderr[-1000:]
    print(json.dumps(status, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
