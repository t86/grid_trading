#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_HOSTS = ("ecs-114", "ecs-150")
DEFAULT_REPORT_HOST = "ecs-114"
DEFAULT_REMOTE_DIR = "/home/ubuntu/wangge/output/kat_guard"

REMOTE_STATUS_SCRIPT = textwrap.dedent(
    r"""
    import json
    import os
    import sys
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    SYMBOL = "KATUSDT"
    BASE = Path("/home/ubuntu/wangge/output")
    NOW = datetime.now(timezone.utc)
    CUTOFF = NOW - timedelta(hours=24)

    def load_json(path: Path) -> dict:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def parse_dt(raw: object) -> datetime | None:
        if raw in (None, ""):
            return None
        try:
            dt = datetime.fromisoformat(str(raw))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def parse_env_file(path: Path) -> None:
        if not path.exists():
            return
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

    def read_runner_pid() -> dict[str, object]:
        pid_path = BASE / "katusdt_loop_runner.pid"
        if not pid_path.exists():
            return {"pid": None, "is_running": False}
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except Exception:
            return {"pid": None, "is_running": False}
        running = False
        try:
            os.kill(pid, 0)
            running = True
        except OSError:
            running = False
        return {"pid": pid, "is_running": running}

    control = load_json(BASE / "katusdt_loop_runner_control.json")
    state = load_json(BASE / "katusdt_loop_state.json")
    plan = load_json(BASE / "katusdt_loop_latest_plan.json")
    submit = load_json(BASE / "katusdt_loop_latest_submit.json")
    audit_state = load_json(BASE / "katusdt_loop_audit_state.json")

    last_event = {}
    last_nonnull_event = {}
    event_rows_24h = 0
    shift_cycles_24h = 0
    shift_moves_24h = 0
    buy_paused_cycles_24h = 0
    center_values_24h: list[float] = []
    current_long_values_24h: list[float] = []

    events_path = BASE / "katusdt_loop_events.jsonl"
    if events_path.exists():
        for raw in events_path.open(encoding="utf-8"):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict) or str(obj.get("symbol")) != SYMBOL:
                continue
            last_event = obj
            if obj.get("cumulative_gross_notional") is not None:
                last_nonnull_event = obj
            dt = parse_dt(obj.get("ts"))
            if dt is None or dt < CUTOFF:
                continue
            event_rows_24h += 1
            if obj.get("buy_paused"):
                buy_paused_cycles_24h += 1
            moves = obj.get("shift_moves") or []
            if moves:
                shift_cycles_24h += 1
                shift_moves_24h += len(moves)
            center = obj.get("center_price")
            if center is not None:
                try:
                    center_values_24h.append(float(center))
                except Exception:
                    pass
            long_notional = obj.get("current_long_notional")
            if long_notional is not None:
                try:
                    current_long_values_24h.append(float(long_notional))
                except Exception:
                    pass

    trades_24h = {
        "count": 0,
        "gross_notional": 0.0,
        "commission": 0.0,
        "realized_pnl": 0.0,
        "by_side": {
            "BUY": {"count": 0, "gross_notional": 0.0, "commission": 0.0, "realized_pnl": 0.0},
            "SELL": {"count": 0, "gross_notional": 0.0, "commission": 0.0, "realized_pnl": 0.0},
        },
    }
    trade_path = BASE / "katusdt_loop_trade_audit.jsonl"
    if trade_path.exists():
        for raw in trade_path.open(encoding="utf-8"):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            ts_ms = obj.get("time") or obj.get("T") or obj.get("transactTime")
            if ts_ms is None:
                continue
            try:
                dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)
            except Exception:
                continue
            if dt < CUTOFF:
                continue
            side = str(obj.get("side") or "").upper()
            gross = abs(float(obj.get("quoteQty") or obj.get("cumQuote") or obj.get("notional") or 0.0))
            commission = float(obj.get("commission") or 0.0)
            realized = float(obj.get("realizedPnl") or 0.0)
            trades_24h["count"] += 1
            trades_24h["gross_notional"] += gross
            trades_24h["commission"] += commission
            trades_24h["realized_pnl"] += realized
            if side in trades_24h["by_side"]:
                bucket = trades_24h["by_side"][side]
                bucket["count"] += 1
                bucket["gross_notional"] += gross
                bucket["commission"] += commission
                bucket["realized_pnl"] += realized

    exchange = {}
    try:
        parse_env_file(Path("/home/ubuntu/.config/wangge/binance_api_env.env"))
        sys.path.insert(0, "/home/ubuntu/wangge/src")
        from grid_optimizer.data import fetch_futures_account_info_v3, fetch_futures_open_orders

        api_key = os.environ["BINANCE_API_KEY"]
        api_secret = os.environ["BINANCE_API_SECRET"]
        account = fetch_futures_account_info_v3(api_key, api_secret)
        position = next((item for item in account.get("positions", []) if item.get("symbol") == SYMBOL), {})
        orders = fetch_futures_open_orders(SYMBOL, api_key, api_secret)
        exchange = {
            "position_amt": float(position.get("positionAmt") or 0.0),
            "notional": float(position.get("notional") or 0.0),
            "entry_price": position.get("entryPrice"),
            "break_even_price": position.get("breakEvenPrice"),
            "unrealized_profit": position.get("unRealizedProfit"),
            "open_order_count": len(orders),
            "buy_orders": sum(1 for item in orders if item.get("side") == "BUY"),
            "sell_orders": sum(1 for item in orders if item.get("side") == "SELL"),
            "buy_notional": sum(float(item.get("price") or 0.0) * float(item.get("origQty") or 0.0) for item in orders if item.get("side") == "BUY"),
            "sell_notional": sum(float(item.get("price") or 0.0) * float(item.get("origQty") or 0.0) for item in orders if item.get("side") == "SELL"),
        }
    except Exception as exc:
        exchange = {"error": f"{type(exc).__name__}: {exc}"}

    cumulative = last_nonnull_event.get("cumulative_gross_notional")
    remaining_to_target = None
    if cumulative is not None:
        try:
            remaining_to_target = 110000.0 - float(cumulative)
        except Exception:
            remaining_to_target = None

    result = {
        "generated_at": NOW.isoformat(),
        "symbol": SYMBOL,
        "runner": read_runner_pid(),
        "control": {
            "strategy_profile": control.get("strategy_profile"),
            "strategy_mode": control.get("strategy_mode"),
            "center_price": control.get("center_price"),
            "buy_levels": control.get("buy_levels"),
            "sell_levels": control.get("sell_levels"),
            "per_order_notional": control.get("per_order_notional"),
            "base_position_notional": control.get("base_position_notional"),
            "pause_buy_position_notional": control.get("pause_buy_position_notional"),
            "max_position_notional": control.get("max_position_notional"),
            "max_total_notional": control.get("max_total_notional"),
            "max_cumulative_notional": control.get("max_cumulative_notional"),
            "runtime_guard_stats_start_time": control.get("runtime_guard_stats_start_time"),
            "reset_state": control.get("reset_state"),
            "inventory_tier_start_notional": control.get("inventory_tier_start_notional"),
            "inventory_tier_end_notional": control.get("inventory_tier_end_notional"),
            "inventory_tier_buy_levels": control.get("inventory_tier_buy_levels"),
            "inventory_tier_sell_levels": control.get("inventory_tier_sell_levels"),
            "inventory_tier_per_order_notional": control.get("inventory_tier_per_order_notional"),
            "inventory_tier_base_position_notional": control.get("inventory_tier_base_position_notional"),
        },
        "state": {
            "center_price": state.get("center_price"),
            "updated_at": state.get("updated_at"),
        },
        "plan": {
            "center_price": plan.get("center_price"),
            "current_long_notional": plan.get("current_long_notional"),
            "actual_net_notional": plan.get("actual_net_notional"),
            "buy_paused": plan.get("buy_paused"),
            "buy_cap_applied": plan.get("buy_cap_applied"),
            "effective_buy_levels": plan.get("effective_buy_levels"),
            "effective_sell_levels": plan.get("effective_sell_levels"),
            "effective_per_order_notional": plan.get("effective_per_order_notional"),
            "effective_base_position_notional": plan.get("effective_base_position_notional"),
            "inventory_tier": plan.get("inventory_tier"),
            "open_order_count": plan.get("open_order_count"),
            "missing_orders_count": len(plan.get("missing_orders") or []),
            "stale_orders_count": len(plan.get("stale_orders") or []),
        },
        "submit": {
            "executed": submit.get("executed"),
            "error": submit.get("error"),
            "validation_ok": ((submit.get("validation") or {}).get("ok")),
            "placed_orders_count": len(submit.get("placed_orders") or []),
            "canceled_orders_count": len(submit.get("canceled_orders") or []),
        },
        "last_event": {
            "ts": last_nonnull_event.get("ts") or last_event.get("ts"),
            "cumulative_gross_notional": cumulative,
            "current_long_notional": last_nonnull_event.get("current_long_notional"),
            "open_order_count": last_nonnull_event.get("open_order_count"),
            "buy_paused": last_nonnull_event.get("buy_paused"),
            "remaining_to_110k": remaining_to_target,
        },
        "window_24h": {
            "event_rows": event_rows_24h,
            "shift_cycles": shift_cycles_24h,
            "shift_moves": shift_moves_24h,
            "buy_paused_cycles": buy_paused_cycles_24h,
            "center_min": min(center_values_24h) if center_values_24h else None,
            "center_max": max(center_values_24h) if center_values_24h else None,
            "current_long_notional_min": min(current_long_values_24h) if current_long_values_24h else None,
            "current_long_notional_max": max(current_long_values_24h) if current_long_values_24h else None,
            "trades": trades_24h,
        },
        "exchange": exchange,
        "audit_state": {
            "updated_at": audit_state.get("updated_at"),
            "session_start_time_ms": audit_state.get("session_start_time_ms"),
            "trade_last_time_ms": audit_state.get("trade_last_time_ms"),
            "income_last_time_ms": audit_state.get("income_last_time_ms"),
        },
    }
    print(json.dumps(result, ensure_ascii=False))
    """
)


def run(cmd: list[str], *, input_text: str | None = None) -> str:
    proc = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"command failed: {' '.join(cmd)}")
    return proc.stdout


def fetch_server_snapshot(host: str) -> dict[str, Any]:
    raw = run(["ssh", host, "python3", "-"], input_text=REMOTE_STATUS_SCRIPT)
    snapshot = json.loads(raw)
    snapshot["host"] = host
    return snapshot


def format_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def bool_text(value: Any) -> str:
    return "yes" if bool(value) else "no"


def render_host_markdown(snapshot: dict[str, Any]) -> str:
    host = snapshot.get("host", "-")
    runner = snapshot.get("runner") or {}
    control = snapshot.get("control") or {}
    plan = snapshot.get("plan") or {}
    last_event = snapshot.get("last_event") or {}
    window = snapshot.get("window_24h") or {}
    exchange = snapshot.get("exchange") or {}
    trades = window.get("trades") or {}
    inv = plan.get("inventory_tier") or {}
    lines = [
        f"## {host}",
        f"- runner: `{'running' if runner.get('is_running') else 'stopped'}` pid=`{runner.get('pid')}` profile=`{control.get('strategy_profile')}` reset_state=`{control.get('reset_state')}`",
        f"- position: `exchange_notional={format_num(exchange.get('notional'))}U` `plan_notional={format_num(plan.get('current_long_notional'))}U` `buy_paused={bool_text(plan.get('buy_paused'))}` `buy_cap={bool_text(plan.get('buy_cap_applied'))}`",
        f"- orders: `open={exchange.get('open_order_count', '-')}` `buy={exchange.get('buy_orders', '-')}` `sell={exchange.get('sell_orders', '-')}`",
        f"- control: `buy/sell={control.get('buy_levels')}/{control.get('sell_levels')}` `per_order={format_num(control.get('per_order_notional'))}U` `base={format_num(control.get('base_position_notional'))}U` `max_pos={format_num(control.get('max_position_notional'))}U`",
        f"- effective: `buy/sell={plan.get('effective_buy_levels')}/{plan.get('effective_sell_levels')}` `per_order={format_num(plan.get('effective_per_order_notional'))}U` `tier_active={bool_text(inv.get('active'))}` `tier_ratio={format_num((inv.get('ratio') or 0) * 100, 0)}%`",
        f"- 110k: `cum={format_num(last_event.get('cumulative_gross_notional'))}U` `remaining={format_num(last_event.get('remaining_to_110k'))}U`",
        f"- 24h trades: `count={trades.get('count', 0)}` `gross={format_num(trades.get('gross_notional'))}U` `realized={format_num(trades.get('realized_pnl'))}U` `commission={format_num(trades.get('commission'), 6)}`",
        f"- 24h movement: `center_range={format_num(window.get('center_min'), 5)} -> {format_num(window.get('center_max'), 5)}` `shift_cycles={window.get('shift_cycles', 0)}` `shift_moves={window.get('shift_moves', 0)}` `notional_range={format_num(window.get('current_long_notional_min'))}U -> {format_num(window.get('current_long_notional_max'))}U`",
    ]
    return "\n".join(lines)


def render_markdown(snapshot: dict[str, Any]) -> str:
    generated_at = snapshot["generated_at"]
    header = [
        "# KAT Guard Snapshot",
        "",
        f"- generated_at: `{generated_at}`",
        f"- report_host: `{snapshot['report_host']}`",
        f"- remote_dir: `{snapshot['remote_dir']}`",
        "",
    ]
    body = [render_host_markdown(item) for item in snapshot["servers"]]
    return "\n".join(header + body) + "\n"


def write_remote_text(host: str, remote_path: str, content: str, *, append: bool = False) -> None:
    mode = ">>" if append else ">"
    remote_dir = shlex.quote(str(Path(remote_path).parent))
    target = shlex.quote(remote_path)
    cmd = ["ssh", host, f"mkdir -p {remote_dir} && cat {mode} {target}"]
    run(cmd, input_text=content)


def build_snapshot(hosts: tuple[str, ...], report_host: str, remote_dir: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    servers = [fetch_server_snapshot(host) for host in hosts]
    return {
        "generated_at": now,
        "report_host": report_host,
        "remote_dir": remote_dir,
        "servers": servers,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect KAT runner snapshots and write them to ecs-114.")
    parser.add_argument("--hosts", nargs="+", default=list(DEFAULT_HOSTS), help="SSH hosts to inspect")
    parser.add_argument("--report-host", default=DEFAULT_REPORT_HOST, help="SSH host used to store reports")
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR, help="Remote directory used to store reports")
    parser.add_argument("--skip-remote-write", action="store_true", help="Collect only and do not write to remote files")
    parser.add_argument("--print-markdown", action="store_true", help="Print markdown summary to stdout")
    parser.add_argument("--print-json", action="store_true", help="Print JSON snapshot to stdout")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = build_snapshot(tuple(args.hosts), args.report_host, args.remote_dir)
    markdown = render_markdown(snapshot)
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"
    history_line = json.dumps(snapshot, ensure_ascii=False) + "\n"

    if not args.skip_remote_write:
        write_remote_text(args.report_host, f"{args.remote_dir}/latest.json", payload)
        write_remote_text(args.report_host, f"{args.remote_dir}/latest.md", markdown)
        write_remote_text(args.report_host, f"{args.remote_dir}/history.jsonl", history_line, append=True)

    if args.print_json:
        sys.stdout.write(payload)
    if args.print_markdown:
        if args.print_json:
            sys.stdout.write("\n")
        sys.stdout.write(markdown)

    if not args.print_json and not args.print_markdown:
        sys.stdout.write(
            f"wrote KAT guard snapshot to {args.report_host}:{args.remote_dir}/latest.json and latest.md\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
