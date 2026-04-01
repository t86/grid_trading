from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from collections.abc import Callable

from .backtest import run_backtest
from .data import (
    fetch_futures_premium_index,
    load_or_fetch_candles,
    load_or_fetch_funding_rates,
)


ProgressCallback = Callable[[dict[str, Any]], None] | None


@dataclass(frozen=True)
class CandidateRow:
    min_price: float
    max_price: float
    min_offset: float
    max_offset: float
    grid_level_mode: str
    allocation_mode: str
    n: int
    turnover_multiple: float
    net_profit: float
    total_return: float
    max_drawdown: float
    trade_count: int
    funding_pnl: float
    total_fees: float
    avg_capital_usage: float

    @property
    def key(self) -> tuple[Any, ...]:
        return (
            round(self.min_price, 8),
            round(self.max_price, 8),
            self.grid_level_mode,
            self.allocation_mode,
            self.n,
        )


@dataclass(frozen=True)
class LabeledCandidate:
    label: str
    title: str
    description: str
    recommended_objective: str
    row: CandidateRow


def _find_current_funding(symbol: str, contract_type: str) -> dict[str, Any] | None:
    try:
        rows = fetch_futures_premium_index(contract_type=contract_type, symbol=symbol)
    except Exception:
        return None
    if not rows:
        return None
    return rows[0]


def _emit_progress(
    progress_callback: ProgressCallback,
    *,
    processed: int,
    total: int,
    status: str,
    min_offset: float | None = None,
    max_offset: float | None = None,
    n: int | None = None,
    mode: str | None = None,
) -> None:
    if progress_callback is None:
        return
    message = f"候选搜索 {processed}/{total}"
    if min_offset is not None and max_offset is not None:
        message += f" · 区间 {min_offset * 100:.2f}% / +{max_offset * 100:.2f}%"
    if n is not None:
        message += f" · N={n}"
    if mode:
        message += f" · {mode}"
    progress_callback(
        {
            "processed": processed,
            "total": total,
            "status": status,
            "n": n,
            "mode": mode,
            "message": message,
        }
    )


def _pick_unique(
    rows: list[CandidateRow],
    *,
    label: str,
    title: str,
    description: str,
    recommended_objective: str,
    used_keys: set[tuple[Any, ...]],
    predicate,
    sort_key,
) -> LabeledCandidate:
    for row in sorted((item for item in rows if predicate(item)), key=sort_key, reverse=True):
        if row.key in used_keys:
            continue
        used_keys.add(row.key)
        return LabeledCandidate(
            label=label,
            title=title,
            description=description,
            recommended_objective=recommended_objective,
            row=row,
        )
    raise RuntimeError(f"Unable to select candidate for {label}")


def _select_candidates(rows: list[CandidateRow]) -> list[LabeledCandidate]:
    used_keys: set[tuple[Any, ...]] = set()
    candidates: list[LabeledCandidate] = []
    candidates.append(
        _pick_unique(
            rows,
            label="balanced_short",
            title="均衡空头",
            description="优先保证纯空为正收益，再把换手尽量做高。",
            recommended_objective="competition_volume",
            used_keys=used_keys,
            predicate=lambda item: item.net_profit >= 0.0,
            sort_key=lambda item: (item.turnover_multiple, item.net_profit),
        )
    )
    candidates.append(
        _pick_unique(
            rows,
            label="profit_guarded_short",
            title="稳健空头",
            description="优先守住净利润，接受换手略低，适合作为空头刷量底仓。",
            recommended_objective="net_profit",
            used_keys=used_keys,
            predicate=lambda item: item.net_profit >= 0.0,
            sort_key=lambda item: (item.net_profit, item.turnover_multiple),
        )
    )
    candidates.append(
        _pick_unique(
            rows,
            label="aggressive_volume_short",
            title="激进刷量",
            description="以成交额优先，允许极小回撤换更高换手。",
            recommended_objective="competition_volume",
            used_keys=used_keys,
            predicate=lambda item: item.total_return >= -0.001,
            sort_key=lambda item: (item.turnover_multiple, item.net_profit),
        )
    )
    return candidates


def _candidate_payload(candidate: LabeledCandidate) -> dict[str, Any]:
    row = candidate.row
    return {
        "label": candidate.label,
        "title": candidate.title,
        "description": candidate.description,
        "recommended_objective": candidate.recommended_objective,
        "form_patch": {
            "calc_mode": "optimize",
            "strategy_direction": "short",
            "min_price": row.min_price,
            "max_price": row.max_price,
            "grid_level_mode": row.grid_level_mode,
            "allocation_modes": [row.allocation_mode],
            "n_min": row.n,
            "n_max": row.n,
            "objective": candidate.recommended_objective,
        },
        "params": {
            "strategy_direction": "short",
            "min_price": row.min_price,
            "max_price": row.max_price,
            "min_offset_pct": row.min_offset * 100.0,
            "max_offset_pct": row.max_offset * 100.0,
            "grid_level_mode": row.grid_level_mode,
            "allocation_mode": row.allocation_mode,
            "n": row.n,
        },
        "metrics": {
            "turnover_multiple": row.turnover_multiple,
            "net_profit": row.net_profit,
            "total_return": row.total_return,
            "max_drawdown": row.max_drawdown,
            "trade_count": row.trade_count,
            "funding_pnl": row.funding_pnl,
            "total_fees": row.total_fees,
            "avg_capital_usage": row.avg_capital_usage,
        },
    }


def build_short_volume_candidate_report(
    *,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: str,
    total_buy_notional: float,
    contract_type: str = "usdm",
    cache_dir: str = "data",
    include_funding: bool = True,
    fee_rate: float = 0.0002,
    slippage: float = 0.0,
    refresh: bool = False,
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    candles = load_or_fetch_candles(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        cache_dir=cache_dir,
        contract_type=contract_type,
        market_type="futures",
        refresh=refresh,
    )
    funding_rates = (
        load_or_fetch_funding_rates(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            cache_dir=cache_dir,
            contract_type=contract_type,
            market_type="futures",
            refresh=refresh,
        )
        if include_funding
        else []
    )
    reference_price = candles[-1].close
    lower_offsets = [-0.0100, -0.0075, -0.0050, -0.0025, 0.0]
    upper_offsets = [0.0100, 0.0125, 0.0150, 0.0175, 0.0200]
    grid_level_modes = ["arithmetic", "geometric"]
    allocation_modes = ["equal", "equal_qty", "center_heavy", "linear_reverse"]
    n_values = list(range(18, 41))
    total = (
        len(lower_offsets)
        * len(upper_offsets)
        * len(grid_level_modes)
        * len(allocation_modes)
        * len(n_values)
    )
    processed = 0
    _emit_progress(progress_callback, processed=0, total=total, status="started")

    rows: list[CandidateRow] = []
    for min_offset in lower_offsets:
        for max_offset in upper_offsets:
            min_price = reference_price * (1.0 + min_offset)
            max_price = reference_price * (1.0 + max_offset)
            if min_price >= max_price:
                continue
            for grid_level_mode in grid_level_modes:
                for allocation_mode in allocation_modes:
                    for n in n_values:
                        result = run_backtest(
                            candles=candles,
                            min_price=min_price,
                            max_price=max_price,
                            n=n,
                            total_buy_notional=total_buy_notional,
                            grid_level_mode=grid_level_mode,
                            allocation_mode=allocation_mode,
                            strategy_direction="short",
                            fee_rate=fee_rate,
                            slippage=slippage,
                            funding_rates=funding_rates,
                        )
                        rows.append(
                            CandidateRow(
                                min_price=min_price,
                                max_price=max_price,
                                min_offset=min_offset,
                                max_offset=max_offset,
                                grid_level_mode=grid_level_mode,
                                allocation_mode=allocation_mode,
                                n=n,
                                turnover_multiple=result.turnover_multiple,
                                net_profit=result.net_profit,
                                total_return=result.total_return,
                                max_drawdown=result.max_drawdown,
                                trade_count=result.trade_count,
                                funding_pnl=result.funding_pnl,
                                total_fees=result.total_fees,
                                avg_capital_usage=result.avg_capital_usage,
                            )
                        )
                        processed += 1
                        if processed == 1 or processed % 50 == 0 or processed == total:
                            _emit_progress(
                                progress_callback,
                                processed=processed,
                                total=total,
                                status="tested",
                                min_offset=min_offset,
                                max_offset=max_offset,
                                n=n,
                                mode=f"{grid_level_mode}/{allocation_mode}",
                            )

    candidates = _select_candidates(rows)
    current_funding_snapshot = _find_current_funding(symbol=symbol, contract_type=contract_type)
    context = {
        "start_time_utc": candles[0].open_time.isoformat(),
        "end_time_utc": candles[-1].close_time.isoformat(),
        "candle_count": len(candles),
        "reference_price": reference_price,
        "window_low": min(item.low for item in candles),
        "window_high": max(item.high for item in candles),
        "window_return": (candles[-1].close / candles[0].open - 1.0) if candles[0].open else 0.0,
        "funding_events": len(funding_rates),
        "funding_sum": sum(item.rate for item in funding_rates),
        "budget": total_buy_notional,
        "interval": interval,
        "include_funding": include_funding,
        "fee_rate": fee_rate,
        "slippage": slippage,
    }
    return {
        "candidates": [_candidate_payload(item) for item in candidates],
        "context": context,
        "current_funding_snapshot": current_funding_snapshot,
        "search": {
            "tested": processed,
            "total": total,
        },
    }
