from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .audit import income_row_time_ms, read_jsonl, trade_row_key, trade_row_time_ms
from .strategy_analytics import classify_trade, infer_trade_role

TRADE_DB_ENABLED_ENV_VAR = "GRID_TRADE_DB_ENABLED"
TRADE_DB_WORKSPACE_ENV_VAR = "GRID_TRADE_DB_WORKSPACE"
TRADE_DB_ACCOUNT_ENV_VAR = "GRID_TRADE_DB_ACCOUNT"
DATABASE_URL_ENV_VAR = "GRID_PLATFORM_DATABASE_URL"
DEFAULT_DATABASE_URL = "postgresql://grid:grid@127.0.0.1:5432/grid_platform"


def trade_database_enabled() -> bool:
    return str(os.environ.get(TRADE_DB_ENABLED_ENV_VAR, "")).strip().lower() in {"1", "true", "yes", "on"}


def resolve_trade_database_url() -> str:
    return str(os.environ.get(DATABASE_URL_ENV_VAR, DEFAULT_DATABASE_URL)).strip() or DEFAULT_DATABASE_URL


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _utc_from_ms(value: int) -> datetime:
    if value <= 0:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)


def _json_dumps(payload: Any) -> str:
    return json.dumps(_json_safe(payload if payload is not None else {}), ensure_ascii=False, sort_keys=True)


def _json_safe(payload: Any) -> Any:
    if payload is None or isinstance(payload, (str, int, float, bool)):
        return payload
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, dict):
        return {str(key): _json_safe(value) for key, value in payload.items()}
    if isinstance(payload, (list, tuple)):
        return [_json_safe(value) for value in payload]
    if isinstance(payload, set):
        return [_json_safe(value) for value in sorted(payload, key=lambda item: repr(item))]
    return str(payload)


def _config_fingerprint(config: dict[str, Any]) -> str:
    encoded = _json_dumps(config)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()[:16]


def _summary_fingerprint(summary: dict[str, Any]) -> str:
    seed = {
        "ts": summary.get("ts"),
        "cycle": summary.get("cycle"),
        "symbol": summary.get("symbol"),
        "strategy_mode": summary.get("strategy_mode"),
        "gross_notional": summary.get("gross_notional"),
        "trade_count": summary.get("trade_count"),
    }
    return hashlib.sha1(_json_dumps(seed).encode("utf-8")).hexdigest()


def _parse_iso_datetime(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _summary_market_features(summary: dict[str, Any]) -> dict[str, Any]:
    mid = _safe_float(summary.get("mid_price"))
    effective_step = _safe_float(summary.get("effective_step_price"))
    base_step = _safe_float(summary.get("base_step_price"))
    return {
        "mid_price": mid,
        "bid_price": _safe_float(summary.get("bid_price")),
        "ask_price": _safe_float(summary.get("ask_price")),
        "mark_price": _safe_float(summary.get("mark_price")),
        "return_1m": _safe_float(summary.get("market_guard_return_ratio")),
        "amplitude_1m": _safe_float(summary.get("market_guard_amplitude_ratio")),
        "effective_step_price": effective_step,
        "base_step_price": base_step,
        "effective_step_ratio": effective_step / mid if mid > 0 and effective_step > 0 else 0.0,
        "step_scale": effective_step / base_step if base_step > 0 and effective_step > 0 else 0.0,
        "execution_regime": summary.get("execution_regime_state") or summary.get("mode"),
        "risk_state": summary.get("risk_state") or summary.get("runtime_status"),
        "direction_state": summary.get("direction_state"),
    }


def _summary_outcome(summary: dict[str, Any]) -> dict[str, Any]:
    gross = _safe_float(summary.get("gross_notional") or summary.get("cumulative_gross_notional"))
    realized = _safe_float(summary.get("realized_pnl"))
    commission = _safe_float(summary.get("commission_quote") or summary.get("commission"))
    net = _safe_float(summary.get("net_pnl_estimate"))
    if net == 0.0:
        net = realized - commission + _safe_float(summary.get("funding_fee"))
    return {
        "gross_notional": gross,
        "trade_count": _safe_int(summary.get("trade_count")),
        "realized_pnl": realized,
        "commission": commission,
        "net_pnl": net,
        "loss_per_10k": abs(min(net, 0.0)) / gross * 10_000 if gross > 0 else 0.0,
        "maker_count": _safe_int(summary.get("maker_count")),
        "buy_notional": _safe_float(summary.get("buy_notional")),
        "sell_notional": _safe_float(summary.get("sell_notional")),
        "unrealized_pnl": _safe_float(summary.get("unrealized_pnl")),
        "inventory_notional": _safe_float(summary.get("inventory_notional") or summary.get("actual_net_notional")),
    }


def _strategy_run_key(*, symbol: str, market_type: str, strategy_mode: str, config: dict[str, Any]) -> str:
    workspace = str(os.environ.get(TRADE_DB_WORKSPACE_ENV_VAR) or "default").strip() or "default"
    account = str(os.environ.get(TRADE_DB_ACCOUNT_ENV_VAR) or "binance-main").strip() or "binance-main"
    run_start = str(config.get("run_start_time") or config.get("runtime_guard_stats_start_time") or "").strip()
    seed = {
        "workspace": workspace,
        "account": account,
        "symbol": symbol.upper().strip(),
        "market_type": market_type,
        "strategy_mode": strategy_mode,
        "run_start": run_start,
        "summary_jsonl": str(config.get("summary_jsonl") or ""),
        "state_path": str(config.get("state_path") or ""),
        "config_fp": _config_fingerprint(config),
    }
    return hashlib.sha1(_json_dumps(seed).encode("utf-8")).hexdigest()


def ensure_trade_database_schema(database_url: str | None = None) -> None:
    import psycopg

    conninfo = database_url or resolve_trade_database_url()
    ddl = """
    CREATE TABLE IF NOT EXISTS strategy_runs_audit (
      run_key text PRIMARY KEY,
      workspace text NOT NULL,
      account_alias text NOT NULL,
      venue text NOT NULL DEFAULT 'binance',
      symbol text NOT NULL,
      market_type text NOT NULL,
      strategy_mode text NOT NULL,
      config_fingerprint text NOT NULL,
      config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      first_seen_at timestamptz NOT NULL DEFAULT now(),
      last_seen_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS strategy_trade_fills_audit (
      id bigserial PRIMARY KEY,
      run_key text NOT NULL REFERENCES strategy_runs_audit(run_key) ON DELETE CASCADE,
      workspace text NOT NULL,
      account_alias text NOT NULL,
      venue text NOT NULL DEFAULT 'binance',
      symbol text NOT NULL,
      market_type text NOT NULL,
      strategy_mode text NOT NULL,
      venue_trade_id text NOT NULL,
      venue_order_id text,
      venue_client_order_id text,
      side text NOT NULL,
      position_side text,
      role text,
      category text NOT NULL,
      price numeric(30,12) NOT NULL DEFAULT 0,
      qty numeric(30,12) NOT NULL DEFAULT 0,
      quote_qty numeric(30,12) NOT NULL DEFAULT 0,
      realized_pnl numeric(30,12) NOT NULL DEFAULT 0,
      fee numeric(30,12) NOT NULL DEFAULT 0,
      fee_asset text,
      is_maker boolean,
      filled_at timestamptz NOT NULL,
      raw_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      inserted_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (workspace, account_alias, venue, market_type, symbol, venue_trade_id)
    );

    CREATE INDEX IF NOT EXISTS ix_strategy_trade_fills_audit_symbol_time
      ON strategy_trade_fills_audit(symbol, filled_at);
    CREATE INDEX IF NOT EXISTS ix_strategy_trade_fills_audit_mode_time
      ON strategy_trade_fills_audit(strategy_mode, filled_at);
    CREATE INDEX IF NOT EXISTS ix_strategy_trade_fills_audit_category_time
      ON strategy_trade_fills_audit(category, filled_at);

    CREATE TABLE IF NOT EXISTS strategy_income_audit (
      id bigserial PRIMARY KEY,
      run_key text NOT NULL REFERENCES strategy_runs_audit(run_key) ON DELETE CASCADE,
      workspace text NOT NULL,
      account_alias text NOT NULL,
      venue text NOT NULL DEFAULT 'binance',
      symbol text NOT NULL,
      market_type text NOT NULL,
      income_id text NOT NULL,
      income_type text NOT NULL,
      asset text,
      income numeric(30,12) NOT NULL DEFAULT 0,
      occurred_at timestamptz NOT NULL,
      raw_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      inserted_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (workspace, account_alias, venue, market_type, symbol, income_id)
    );
    CREATE INDEX IF NOT EXISTS ix_strategy_income_audit_symbol_time
      ON strategy_income_audit(symbol, occurred_at);

    CREATE TABLE IF NOT EXISTS strategy_cycle_snapshots_audit (
      id bigserial PRIMARY KEY,
      run_key text NOT NULL REFERENCES strategy_runs_audit(run_key) ON DELETE CASCADE,
      workspace text NOT NULL,
      account_alias text NOT NULL,
      venue text NOT NULL DEFAULT 'binance',
      symbol text NOT NULL,
      market_type text NOT NULL,
      strategy_mode text NOT NULL,
      cycle integer NOT NULL DEFAULT 0,
      observed_at timestamptz NOT NULL,
      config_fingerprint text NOT NULL,
      market_features_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      params_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      outcome_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      raw_summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      inserted_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (run_key, cycle, observed_at)
    );
    CREATE INDEX IF NOT EXISTS ix_strategy_cycle_snapshots_symbol_time
      ON strategy_cycle_snapshots_audit(symbol, observed_at);
    CREATE INDEX IF NOT EXISTS ix_strategy_cycle_snapshots_mode_time
      ON strategy_cycle_snapshots_audit(strategy_mode, observed_at);
    """
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def _upsert_run(cur: Any, *, run_key: str, symbol: str, market_type: str, strategy_mode: str, config: dict[str, Any]) -> None:
    workspace = str(os.environ.get(TRADE_DB_WORKSPACE_ENV_VAR) or "default").strip() or "default"
    account = str(os.environ.get(TRADE_DB_ACCOUNT_ENV_VAR) or "binance-main").strip() or "binance-main"
    cur.execute(
        """
        INSERT INTO strategy_runs_audit (
          run_key, workspace, account_alias, symbol, market_type, strategy_mode, config_fingerprint, config_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (run_key) DO UPDATE SET
          last_seen_at = now(),
          config_json = EXCLUDED.config_json,
          config_fingerprint = EXCLUDED.config_fingerprint
        """,
        (run_key, workspace, account, symbol, market_type, strategy_mode, _config_fingerprint(config), _json_dumps(config)),
    )


def _trade_identity(row: dict[str, Any]) -> str:
    raw_id = str(row.get("id") or "").strip()
    if raw_id:
        return raw_id
    key = trade_row_key(row)
    return hashlib.sha1(_json_dumps({"key": key, "row": row}).encode("utf-8")).hexdigest()


def persist_trade_rows(
    *,
    symbol: str,
    market_type: str,
    strategy_mode: str,
    config: dict[str, Any],
    trade_rows: list[dict[str, Any]],
    income_rows: list[dict[str, Any]] | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    if not trade_database_enabled():
        return {"enabled": False, "trade_inserted": 0, "income_inserted": 0}
    import psycopg

    normalized_symbol = symbol.upper().strip()
    normalized_market = market_type.lower().strip()
    normalized_mode = strategy_mode.strip() or "unknown"
    run_key = _strategy_run_key(
        symbol=normalized_symbol,
        market_type=normalized_market,
        strategy_mode=normalized_mode,
        config=config,
    )
    workspace = str(os.environ.get(TRADE_DB_WORKSPACE_ENV_VAR) or "default").strip() or "default"
    account = str(os.environ.get(TRADE_DB_ACCOUNT_ENV_VAR) or "binance-main").strip() or "binance-main"
    trade_inserted = 0
    income_inserted = 0
    with psycopg.connect(database_url or resolve_trade_database_url()) as conn:
        with conn.cursor() as cur:
            _upsert_run(
                cur,
                run_key=run_key,
                symbol=normalized_symbol,
                market_type=normalized_market,
                strategy_mode=normalized_mode,
                config=config,
            )
            for row in trade_rows:
                if not isinstance(row, dict):
                    continue
                price = _safe_float(row.get("price"))
                qty = _safe_float(row.get("qty"))
                role = infer_trade_role(row)
                cur.execute(
                    """
                    INSERT INTO strategy_trade_fills_audit (
                      run_key, workspace, account_alias, symbol, market_type, strategy_mode,
                      venue_trade_id, venue_order_id, venue_client_order_id, side, position_side,
                      role, category, price, qty, quote_qty, realized_pnl, fee, fee_asset,
                      is_maker, filled_at, raw_payload_json
                    ) VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (workspace, account_alias, venue, market_type, symbol, venue_trade_id) DO NOTHING
                    """,
                    (
                        run_key,
                        workspace,
                        account,
                        normalized_symbol,
                        normalized_market,
                        normalized_mode,
                        _trade_identity(row),
                        str(row.get("orderId") or row.get("order_id") or "").strip() or None,
                        str(row.get("clientOrderId") or row.get("client_order_id") or "").strip() or None,
                        str(row.get("side") or "").upper().strip(),
                        str(row.get("positionSide") or row.get("position_side") or "").upper().strip() or None,
                        role or None,
                        classify_trade(row),
                        price,
                        qty,
                        _safe_float(row.get("quoteQty")) or price * qty,
                        _safe_float(row.get("realizedPnl") if row.get("realizedPnl") is not None else row.get("realized_pnl")),
                        abs(_safe_float(row.get("commission") if row.get("commission") is not None else row.get("fee"))),
                        str(row.get("commissionAsset") or row.get("fee_asset") or "").upper().strip() or None,
                        bool(row.get("maker")) if row.get("maker") is not None else (bool(row.get("isMaker")) if row.get("isMaker") is not None else None),
                        _utc_from_ms(trade_row_time_ms(row)),
                        _json_dumps(row),
                    ),
                )
                trade_inserted += int(cur.rowcount == 1)
            for row in income_rows or []:
                if not isinstance(row, dict):
                    continue
                income_id = str(row.get("tranId") or "").strip()
                if not income_id:
                    seed = {
                        "time": row.get("time"),
                        "type": row.get("incomeType"),
                        "income": row.get("income"),
                        "asset": row.get("asset"),
                        "info": row.get("info"),
                    }
                    income_id = hashlib.sha1(_json_dumps(seed).encode("utf-8")).hexdigest()
                cur.execute(
                    """
                    INSERT INTO strategy_income_audit (
                      run_key, workspace, account_alias, symbol, market_type,
                      income_id, income_type, asset, income, occurred_at, raw_payload_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (workspace, account_alias, venue, market_type, symbol, income_id) DO NOTHING
                    """,
                    (
                        run_key,
                        workspace,
                        account,
                        normalized_symbol,
                        normalized_market,
                        income_id,
                        str(row.get("incomeType") or "").upper().strip() or "UNKNOWN",
                        str(row.get("asset") or "").upper().strip() or None,
                        _safe_float(row.get("income")),
                        _utc_from_ms(income_row_time_ms(row)),
                        _json_dumps(row),
                    ),
                )
                income_inserted += int(cur.rowcount == 1)
        conn.commit()
    return {"enabled": True, "run_key": run_key, "trade_inserted": trade_inserted, "income_inserted": income_inserted}


def persist_cycle_snapshot(
    *,
    symbol: str,
    market_type: str,
    strategy_mode: str,
    config: dict[str, Any],
    summary: dict[str, Any],
    database_url: str | None = None,
) -> dict[str, Any]:
    if not trade_database_enabled():
        return {"enabled": False, "cycle_inserted": 0}
    import psycopg

    normalized_symbol = symbol.upper().strip()
    normalized_market = market_type.lower().strip()
    normalized_mode = strategy_mode.strip() or "unknown"
    run_key = _strategy_run_key(
        symbol=normalized_symbol,
        market_type=normalized_market,
        strategy_mode=normalized_mode,
        config=config,
    )
    workspace = str(os.environ.get(TRADE_DB_WORKSPACE_ENV_VAR) or "default").strip() or "default"
    account = str(os.environ.get(TRADE_DB_ACCOUNT_ENV_VAR) or "binance-main").strip() or "binance-main"
    market_features = _summary_market_features(summary)
    outcome = _summary_outcome(summary)
    state = {
        "position_qty": _safe_float(summary.get("actual_net_qty") or summary.get("managed_base_qty")),
        "position_notional": _safe_float(summary.get("actual_net_notional") or summary.get("inventory_notional")),
        "long_notional": _safe_float(summary.get("current_long_notional")),
        "short_notional": _safe_float(summary.get("current_short_notional")),
        "open_strategy_orders": _safe_int(summary.get("open_strategy_orders")),
        "active_buy_orders": _safe_int(summary.get("active_buy_orders")),
        "active_sell_orders": _safe_int(summary.get("active_sell_orders")),
        "buy_paused": bool(summary.get("buy_paused")),
        "shift_frozen": bool(summary.get("shift_frozen")),
        "stop_triggered": bool(summary.get("stop_triggered")),
        "stop_reason": summary.get("stop_reason"),
    }
    with psycopg.connect(database_url or resolve_trade_database_url()) as conn:
        with conn.cursor() as cur:
            _upsert_run(
                cur,
                run_key=run_key,
                symbol=normalized_symbol,
                market_type=normalized_market,
                strategy_mode=normalized_mode,
                config=config,
            )
            cur.execute(
                """
                INSERT INTO strategy_cycle_snapshots_audit (
                  run_key, workspace, account_alias, symbol, market_type, strategy_mode,
                  cycle, observed_at, config_fingerprint, market_features_json,
                  params_json, state_json, outcome_json, raw_summary_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                ON CONFLICT (run_key, cycle, observed_at) DO NOTHING
                """,
                (
                    run_key,
                    workspace,
                    account,
                    normalized_symbol,
                    normalized_market,
                    normalized_mode,
                    _safe_int(summary.get("cycle")),
                    _parse_iso_datetime(summary.get("ts")),
                    _config_fingerprint(config),
                    _json_dumps(market_features),
                    _json_dumps(config),
                    _json_dumps(state),
                    _json_dumps(outcome),
                    _json_dumps(summary),
                ),
            )
            inserted = int(cur.rowcount == 1)
        conn.commit()
    return {"enabled": True, "run_key": run_key, "cycle_inserted": inserted}


def persist_trade_audit_files(
    *,
    symbol: str,
    market_type: str,
    strategy_mode: str,
    config: dict[str, Any],
    trade_audit_path: str | Path,
    income_audit_path: str | Path | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    trade_rows = read_jsonl(Path(trade_audit_path), limit=0)
    income_rows = read_jsonl(Path(income_audit_path), limit=0) if income_audit_path else []
    return persist_trade_rows(
        symbol=symbol,
        market_type=market_type,
        strategy_mode=strategy_mode,
        config=config,
        trade_rows=trade_rows,
        income_rows=income_rows,
        database_url=database_url,
    )
