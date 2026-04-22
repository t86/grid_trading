# Competition Runner Auto-Stop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为合约和现货交易赛 runner 增加运行开始/结束时间、最近 60 分钟滚动亏损阈值、累计成交额阈值，并在触发后执行撤单加顶档 `maker` 持续平仓。

**Architecture:** 新增一个共享的运行时 guard helper，统一计算时间窗、滚动亏损、累计成交额和停止原因；合约 runner 复用现有 flatten runner，现货新增等价的 spot flatten runner。`web.py` 负责配置归一化、命令传参和状态页展示，runner 负责实际判断与执行。

**Tech Stack:** Python 3、`unittest`、现有 Binance data helpers、现有 `web.py` / `loop_runner.py` / `spot_loop_runner.py` / `maker_flatten_runner.py`

---

### Task 1: Shared Runtime Guard Helper

**Files:**
- Create: `src/grid_optimizer/runtime_guards.py`
- Test: `tests/test_runtime_guards.py`

- [ ] **Step 1: Write the failing helper tests**

```python
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.runtime_guards import (
    RuntimeGuardConfig,
    evaluate_runtime_guards,
    normalize_runtime_guard_config,
)


class RuntimeGuardsTests(unittest.TestCase):
    def test_normalize_runtime_guard_config_accepts_empty_values(self) -> None:
        cfg = normalize_runtime_guard_config(
            {
                "run_start_time": "",
                "run_end_time": None,
                "rolling_hourly_loss_limit": "",
                "max_cumulative_notional": None,
            }
        )
        self.assertIsNone(cfg.run_start_time)
        self.assertIsNone(cfg.run_end_time)
        self.assertIsNone(cfg.rolling_hourly_loss_limit)
        self.assertIsNone(cfg.max_cumulative_notional)

    def test_normalize_runtime_guard_config_rejects_inverted_window(self) -> None:
        with self.assertRaisesRegex(ValueError, "run_start_time must be earlier than run_end_time"):
            normalize_runtime_guard_config(
                {
                    "run_start_time": "2026-03-30T10:00:00+00:00",
                    "run_end_time": "2026-03-30T09:00:00+00:00",
                }
            )

    def test_evaluate_runtime_guards_returns_waiting_before_start(self) -> None:
        now = datetime(2026, 3, 30, 8, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc),
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[],
        )
        self.assertFalse(result.tradable)
        self.assertFalse(result.stop_triggered)
        self.assertEqual(result.runtime_status, "waiting")
        self.assertEqual(result.primary_reason, "before_start_window")

    def test_evaluate_runtime_guards_stops_after_end_time(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[],
        )
        self.assertFalse(result.tradable)
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "after_end_window")

    def test_evaluate_runtime_guards_stops_on_rolling_loss(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=50.0,
            max_cumulative_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[
                {"ts": (now - timedelta(minutes=20)).isoformat(), "net_pnl": -30.0},
                {"ts": (now - timedelta(minutes=5)).isoformat(), "net_pnl": -25.0},
            ],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "rolling_hourly_loss_limit_hit")
        self.assertAlmostEqual(result.rolling_hourly_loss, 55.0)

    def test_evaluate_runtime_guards_stops_on_cumulative_notional(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=1000.0,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=1000.0,
            pnl_events=[],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "max_cumulative_notional_hit")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/test_runtime_guards.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.runtime_guards'`

- [ ] **Step 3: Write minimal runtime guard implementation**

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def _parse_datetime(value: Any, field_name: str) -> datetime | None:
    if value in {"", None}:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).strip())
    if dt.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information")
    return dt.astimezone(timezone.utc)


def _parse_positive_float(value: Any, field_name: str) -> float | None:
    if value in {"", None}:
        return None
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


def _event_net_pnl(item: dict[str, Any]) -> float:
    if "net_pnl" in item:
        return float(item.get("net_pnl") or 0.0)
    realized = float(item.get("realized_pnl") or 0.0)
    commission = float(item.get("commission_quote") or 0.0)
    recycle_loss = float(item.get("recycle_loss_abs") or 0.0)
    return realized - commission - recycle_loss


@dataclass(frozen=True)
class RuntimeGuardConfig:
    run_start_time: datetime | None
    run_end_time: datetime | None
    rolling_hourly_loss_limit: float | None
    max_cumulative_notional: float | None


@dataclass(frozen=True)
class RuntimeGuardResult:
    tradable: bool
    stop_triggered: bool
    runtime_status: str
    primary_reason: str | None
    matched_reasons: list[str]
    triggered_at: str | None
    rolling_hourly_loss: float
    cumulative_gross_notional: float


def normalize_runtime_guard_config(raw: dict[str, Any]) -> RuntimeGuardConfig:
    config = RuntimeGuardConfig(
        run_start_time=_parse_datetime(raw.get("run_start_time"), "run_start_time"),
        run_end_time=_parse_datetime(raw.get("run_end_time"), "run_end_time"),
        rolling_hourly_loss_limit=_parse_positive_float(raw.get("rolling_hourly_loss_limit"), "rolling_hourly_loss_limit"),
        max_cumulative_notional=_parse_positive_float(raw.get("max_cumulative_notional"), "max_cumulative_notional"),
    )
    if config.run_start_time and config.run_end_time and config.run_start_time >= config.run_end_time:
        raise ValueError("run_start_time must be earlier than run_end_time")
    return config


def evaluate_runtime_guards(
    *,
    config: RuntimeGuardConfig,
    now: datetime,
    cumulative_gross_notional: float,
    pnl_events: list[dict[str, Any]],
) -> RuntimeGuardResult:
    current = now.astimezone(timezone.utc)
    reasons: list[str] = []
    rolling_loss = 0.0

    window_start = current - timedelta(minutes=60)
    window_net_pnl = 0.0
    for event in pnl_events:
        event_ts = _parse_datetime(event.get("ts"), "ts")
        if event_ts is None or event_ts < window_start or event_ts > current:
            continue
        window_net_pnl += _event_net_pnl(event)
    rolling_loss = max(0.0, -window_net_pnl)

    if config.run_start_time and current < config.run_start_time:
        return RuntimeGuardResult(
            tradable=False,
            stop_triggered=False,
            runtime_status="waiting",
            primary_reason="before_start_window",
            matched_reasons=["before_start_window"],
            triggered_at=None,
            rolling_hourly_loss=rolling_loss,
            cumulative_gross_notional=float(cumulative_gross_notional),
        )

    if config.run_end_time and current >= config.run_end_time:
        reasons.append("after_end_window")
    if config.rolling_hourly_loss_limit is not None and rolling_loss >= config.rolling_hourly_loss_limit:
        reasons.append("rolling_hourly_loss_limit_hit")
    if config.max_cumulative_notional is not None and float(cumulative_gross_notional) >= config.max_cumulative_notional:
        reasons.append("max_cumulative_notional_hit")

    return RuntimeGuardResult(
        tradable=not reasons,
        stop_triggered=bool(reasons),
        runtime_status="stopped" if reasons else "running",
        primary_reason=reasons[0] if reasons else None,
        matched_reasons=reasons,
        triggered_at=current.isoformat() if reasons else None,
        rolling_hourly_loss=rolling_loss,
        cumulative_gross_notional=float(cumulative_gross_notional),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/test_runtime_guards.py -v`
Expected: PASS with 6 passed

- [ ] **Step 5: Commit**

```bash
git add tests/test_runtime_guards.py src/grid_optimizer/runtime_guards.py
git commit -m "feat: add runtime guard helper for competition runners"
```

### Task 2: Web Config Plumbing For Futures And Spot

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`
- Test: `tests/test_spot_runner.py`

- [ ] **Step 1: Write the failing web tests for new config fields**

```python
    def test_normalize_runner_control_payload_accepts_runtime_guard_fields(self) -> None:
        payload = _normalize_runner_control_payload(
            {
                "symbol": "NIGHTUSDT",
                "step_price": 0.01,
                "buy_levels": 4,
                "sell_levels": 4,
                "per_order_notional": 20,
                "base_position_notional": 40,
                "run_start_time": "2026-03-30T09:00:00+08:00",
                "run_end_time": "2026-03-30T18:00:00+08:00",
                "rolling_hourly_loss_limit": 50,
                "max_cumulative_notional": 1000,
            }
        )
        self.assertEqual(payload["run_start_time"], "2026-03-30T01:00:00+00:00")
        self.assertEqual(payload["run_end_time"], "2026-03-30T10:00:00+00:00")
        self.assertEqual(payload["rolling_hourly_loss_limit"], 50.0)
        self.assertEqual(payload["max_cumulative_notional"], 1000.0)

    def test_build_runner_command_includes_runtime_guard_args(self) -> None:
        command = _build_runner_command(
            {
                **_load_runner_control_config("NIGHTUSDT"),
                "symbol": "NIGHTUSDT",
                "step_price": 0.01,
                "buy_levels": 4,
                "sell_levels": 4,
                "per_order_notional": 20.0,
                "base_position_notional": 40.0,
                "run_start_time": "2026-03-30T01:00:00+00:00",
                "run_end_time": "2026-03-30T10:00:00+00:00",
                "rolling_hourly_loss_limit": 50.0,
                "max_cumulative_notional": 1000.0,
            }
        )
        self.assertIn("--run-start-time", command)
        self.assertIn("--run-end-time", command)
        self.assertIn("--rolling-hourly-loss-limit", command)
        self.assertIn("--max-cumulative-notional", command)
```

```python
    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_accepts_runtime_guard_fields(self, _mock_validate_symbol) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "btcusdt",
                "min_price": 50000,
                "max_price": 100000,
                "n": 10,
                "total_quote_budget": 1000,
                "run_start_time": "2026-03-30T09:00:00+08:00",
                "run_end_time": "2026-03-30T18:00:00+08:00",
                "rolling_hourly_loss_limit": 25,
                "max_cumulative_notional": 5000,
            }
        )
        self.assertEqual(payload["run_start_time"], "2026-03-30T01:00:00+00:00")
        self.assertEqual(payload["run_end_time"], "2026-03-30T10:00:00+00:00")
        self.assertEqual(payload["rolling_hourly_loss_limit"], 25.0)
        self.assertEqual(payload["max_cumulative_notional"], 5000.0)

    def test_build_spot_runner_command_includes_runtime_guard_args(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "BTCUSDT",
                "strategy_mode": "spot_one_way_long",
                "min_price": 50000.0,
                "max_price": 100000.0,
                "n": 12,
                "total_quote_budget": 1000.0,
                "run_start_time": "2026-03-30T01:00:00+00:00",
                "run_end_time": "2026-03-30T10:00:00+00:00",
                "rolling_hourly_loss_limit": 25.0,
                "max_cumulative_notional": 5000.0,
            }
        )
        command = _build_spot_runner_command(config)
        self.assertIn("--run-start-time", command)
        self.assertIn("--run-end-time", command)
        self.assertIn("--rolling-hourly-loss-limit", command)
        self.assertIn("--max-cumulative-notional", command)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/test_web_security.py tests/test_spot_runner.py -k "runtime_guard or build_runner_command_includes_runtime_guard_args or build_spot_runner_command_includes_runtime_guard_args" -v`
Expected: FAIL because the new fields are ignored and the new command flags are missing

- [ ] **Step 3: Implement web normalization and command plumbing**

```python
from .runtime_guards import normalize_runtime_guard_config


def _guard_fields_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw = {
        "run_start_time": payload.get("run_start_time"),
        "run_end_time": payload.get("run_end_time"),
        "rolling_hourly_loss_limit": payload.get("rolling_hourly_loss_limit"),
        "max_cumulative_notional": payload.get("max_cumulative_notional"),
    }
    guard = normalize_runtime_guard_config(raw)
    return {
        "run_start_time": guard.run_start_time.isoformat() if guard.run_start_time else None,
        "run_end_time": guard.run_end_time.isoformat() if guard.run_end_time else None,
        "rolling_hourly_loss_limit": guard.rolling_hourly_loss_limit,
        "max_cumulative_notional": guard.max_cumulative_notional,
    }
```

```python
    float_fields = {
        # existing fields...
        "rolling_hourly_loss_limit",
        "max_cumulative_notional",
    }
    str_fields = {
        # existing fields...
        "run_start_time",
        "run_end_time",
    }
```

```python
    config.update(_guard_fields_from_payload(payload))
```

```python
    for flag, value in [
        ("--run-start-time", config.get("run_start_time")),
        ("--run-end-time", config.get("run_end_time")),
        ("--rolling-hourly-loss-limit", config.get("rolling_hourly_loss_limit")),
        ("--max-cumulative-notional", config.get("max_cumulative_notional")),
    ]:
        if value is not None:
            command.extend([flag, str(value)])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/test_web_security.py tests/test_spot_runner.py -k "runtime_guard or build_runner_command_includes_runtime_guard_args or build_spot_runner_command_includes_runtime_guard_args" -v`
Expected: PASS for the new normalization and command-builder tests

- [ ] **Step 5: Commit**

```bash
git add tests/test_web_security.py tests/test_spot_runner.py src/grid_optimizer/web.py
git commit -m "feat: wire runtime guard fields through web runner config"
```

### Task 3: Futures Runner Auto-Stop Integration

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_loop_runner.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing futures runner tests**

```python
    @patch("grid_optimizer.loop_runner.execute_plan_report")
    @patch("grid_optimizer.loop_runner.generate_plan_report")
    @patch("grid_optimizer.loop_runner._sync_account_audit")
    @patch("grid_optimizer.loop_runner._execute_stop_actions")
    def test_main_stops_after_runtime_guard_hits_end_time(
        self,
        mock_stop_actions,
        mock_sync_audit,
        mock_generate_plan,
        mock_execute_plan,
    ) -> None:
        mock_generate_plan.return_value = {"state_path": "output/night_state.json", "mid_price": 1.0}
        mock_execute_plan.return_value = {"executed": False, "placed_orders": [], "canceled_orders": []}
        mock_sync_audit.return_value = {"trade_appended": 0, "income_appended": 0, "error": None}
        mock_stop_actions.return_value = {"flatten_started": True}

        args = _build_test_args(
            run_start_time=None,
            run_end_time="2026-03-30T00:00:00+00:00",
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
            iterations=1,
        )

        with self.assertRaises(SystemExit):
            run_main_for_test(args)

        mock_stop_actions.assert_called_once()
        mock_execute_plan.assert_not_called()

    def test_build_runtime_guard_events_from_summary_rows(self) -> None:
        rows = [
            {"ts": "2026-03-30T09:40:00+00:00", "realized_pnl": -20.0},
            {"ts": "2026-03-30T09:50:00+00:00", "realized_pnl": -15.0},
        ]
        events = _build_futures_runtime_guard_events(rows)
        self.assertEqual(len(events), 2)
        self.assertAlmostEqual(events[0]["net_pnl"], -20.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/test_loop_runner.py -k "runtime_guard or end_time" -v`
Expected: FAIL because the futures runner has no runtime guard integration and no helper to build guard events

- [ ] **Step 3: Implement futures guard evaluation and stop orchestration**

```python
from .runtime_guards import evaluate_runtime_guards, normalize_runtime_guard_config


def _build_futures_runtime_guard_events(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in summary_rows:
        if not isinstance(row, dict):
            continue
        if not row.get("ts"):
            continue
        realized = _safe_float(row.get("realized_pnl"))
        events.append({"ts": row["ts"], "net_pnl": realized})
    return events
```

```python
    parser.add_argument("--run-start-time", type=str, default=None)
    parser.add_argument("--run-end-time", type=str, default=None)
    parser.add_argument("--rolling-hourly-loss-limit", type=float, default=None)
    parser.add_argument("--max-cumulative-notional", type=float, default=None)
```

```python
    guard_config = normalize_runtime_guard_config(
        {
            "run_start_time": args.run_start_time,
            "run_end_time": args.run_end_time,
            "rolling_hourly_loss_limit": args.rolling_hourly_loss_limit,
            "max_cumulative_notional": args.max_cumulative_notional,
        }
    )
```

```python
            recent_summary_rows = _tail_jsonl_dicts(summary_path, limit=240)
            guard_result = evaluate_runtime_guards(
                config=guard_config,
                now=cycle_started_at,
                cumulative_gross_notional=sum(
                    _safe_float(item.get("placed_notional"))
                    for item in recent_summary_rows
                    if isinstance(item, dict)
                ) or 0.0,
                pnl_events=_build_futures_runtime_guard_events(recent_summary_rows),
            )
            if guard_result.runtime_status == "waiting":
                waiting_summary = {
                    "ts": cycle_started_at.isoformat(),
                    "cycle": cycle,
                    "symbol": args.symbol.upper().strip(),
                    "runtime_status": "waiting",
                    "stop_triggered": False,
                    "stop_reason": "before_start_window",
                    "stop_reasons": ["before_start_window"],
                    "rolling_hourly_loss": guard_result.rolling_hourly_loss,
                    "cumulative_gross_notional": guard_result.cumulative_gross_notional,
                }
                _append_jsonl(summary_path, waiting_summary)
                _print_cycle_summary({**waiting_summary, "mid_price": 0.0, "center_price": 0.0, "current_long_qty": 0.0, "current_long_notional": 0.0, "open_order_count": 0, "kept_order_count": 0, "missing_order_count": 0, "stale_order_count": 0, "placed_count": 0, "canceled_count": 0, "buy_paused": False, "buy_cap_applied": False})
                time.sleep(args.sleep_seconds)
                continue
            if guard_result.stop_triggered:
                stop_actions = _execute_stop_actions(
                    symbol=args.symbol.upper().strip(),
                    cancel_open_orders=True,
                    close_all_positions=True,
                )
                _append_jsonl(
                    summary_path,
                    {
                        "ts": cycle_started_at.isoformat(),
                        "cycle": cycle,
                        "symbol": args.symbol.upper().strip(),
                        "runtime_status": "stopped",
                        "stop_triggered": True,
                        "stop_reason": guard_result.primary_reason,
                        "stop_reasons": guard_result.matched_reasons,
                        "stop_triggered_at": guard_result.triggered_at,
                        "rolling_hourly_loss": guard_result.rolling_hourly_loss,
                        "cumulative_gross_notional": guard_result.cumulative_gross_notional,
                        "post_stop_actions": stop_actions,
                    },
                )
                raise SystemExit(f"runner stopped by runtime guard: {guard_result.primary_reason}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/test_loop_runner.py -k "runtime_guard or end_time" -v`
Expected: PASS for the new futures runtime guard coverage

- [ ] **Step 5: Commit**

```bash
git add tests/test_loop_runner.py src/grid_optimizer/loop_runner.py src/grid_optimizer/web.py
git commit -m "feat: add runtime guard stops to futures runner"
```

### Task 4: Spot Maker Flatten Runner

**Files:**
- Create: `src/grid_optimizer/spot_flatten_runner.py`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_spot_flatten_runner.py`
- Test: `tests/test_spot_runner.py`

- [ ] **Step 1: Write the failing spot flatten tests**

```python
from __future__ import annotations

import unittest

from grid_optimizer.spot_flatten_runner import build_spot_flatten_orders_from_snapshot


class SpotFlattenRunnerTests(unittest.TestCase):
    def test_build_spot_flatten_orders_creates_sell_order_for_inventory(self) -> None:
        snapshot = build_spot_flatten_orders_from_snapshot(
            symbol="BTCUSDT",
            bid_price=68000.0,
            ask_price=68001.0,
            base_free=0.25,
            symbol_info={
                "tick_size": 0.01,
                "step_size": 0.00001,
                "min_qty": 0.00001,
                "min_notional": 5.0,
            },
        )
        self.assertEqual(len(snapshot["orders"]), 1)
        self.assertEqual(snapshot["orders"][0]["side"], "SELL")
        self.assertEqual(snapshot["orders"][0]["time_in_force"], "GTX")

    def test_build_spot_flatten_orders_returns_empty_when_flat(self) -> None:
        snapshot = build_spot_flatten_orders_from_snapshot(
            symbol="BTCUSDT",
            bid_price=68000.0,
            ask_price=68001.0,
            base_free=0.0,
            symbol_info={
                "tick_size": 0.01,
                "step_size": 0.00001,
                "min_qty": 0.00001,
                "min_notional": 5.0,
            },
        )
        self.assertEqual(snapshot["orders"], [])
```

```python
    @patch("grid_optimizer.web._start_spot_flatten_process")
    @patch("grid_optimizer.web._cancel_spot_strategy_orders")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_stop_spot_runner_process_can_launch_spot_flatten(
        self,
        mock_read_runner,
        mock_cancel_orders,
        mock_start_flatten,
    ) -> None:
        mock_read_runner.return_value = {"pid": None, "is_running": False, "config": {}}
        mock_cancel_orders.return_value = {"canceled": 1}
        mock_start_flatten.return_value = {"started": True}
        result = _stop_spot_runner_process("BTCUSDT", cancel_orders=True, close_all_positions=True)
        self.assertTrue(result["post_stop_actions"]["close_all_positions_requested"])
        self.assertTrue(result["post_stop_actions"]["flatten_started"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/test_spot_flatten_runner.py tests/test_spot_runner.py -k "flatten" -v`
Expected: FAIL with `ModuleNotFoundError` for `grid_optimizer.spot_flatten_runner` and missing spot close-all flow in `_stop_spot_runner_process`

- [ ] **Step 3: Implement the spot flatten module and stop integration**

```python
from __future__ import annotations

import argparse
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data import (
    delete_spot_order,
    fetch_spot_account_info,
    fetch_spot_book_tickers,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
    post_spot_order,
)
from .dry_run import _round_order_price, _round_order_qty
from .semi_auto_plan import diff_open_orders


def build_spot_flatten_orders_from_snapshot(*, symbol: str, bid_price: float, ask_price: float, base_free: float, symbol_info: dict[str, Any]) -> dict[str, Any]:
    if base_free <= 0:
        return {"orders": [], "warnings": [], "base_free": 0.0}
    price = _round_order_price(ask_price, symbol_info.get("tick_size"), "SELL")
    qty = _round_order_qty(base_free, symbol_info.get("step_size"))
    if qty <= 0:
        return {"orders": [], "warnings": [f"{symbol} 可卖数量过小"], "base_free": base_free}
    return {
        "orders": [
            {
                "symbol": symbol,
                "side": "SELL",
                "quantity": qty,
                "price": price,
                "time_in_force": "GTX",
                "tag": "closespot",
            }
        ],
        "warnings": [],
        "base_free": base_free,
    }
```

```python
def _build_start_spot_flatten_command(config: dict[str, Any]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "grid_optimizer.spot_flatten_runner",
        "--symbol",
        str(config["symbol"]),
        "--client-order-prefix",
        str(config["client_order_prefix"]),
        "--sleep-seconds",
        str(config.get("sleep_seconds", 2.0)),
        "--events-jsonl",
        str(config["events_jsonl"]),
    ]
```

```python
def _execute_spot_stop_actions(*, symbol: str, cancel_open_orders: bool, close_all_positions: bool) -> dict[str, Any]:
    summary = {
        "symbol": symbol,
        "cancel_open_orders_requested": bool(cancel_open_orders),
        "close_all_positions_requested": bool(close_all_positions),
        "cancel_open_orders_executed": False,
        "close_all_positions_executed": False,
        "flatten_started": False,
        "flatten_already_running": False,
        "warnings": [],
    }
    config = _load_spot_runner_control_config(symbol)
    if cancel_open_orders:
        cleanup = _cancel_spot_strategy_orders(config)
        summary["cancel_open_orders_executed"] = True
        summary["cancel_success_count"] = int(cleanup.get("canceled", 0) or 0)
    if close_all_positions:
        flatten_result = _start_spot_flatten_process(
            {
                "symbol": symbol,
                "client_order_prefix": _spot_flatten_client_order_prefix(symbol),
                "sleep_seconds": 2.0,
                "events_jsonl": str(_spot_flatten_events_path(symbol)),
            }
        )
        summary["close_all_positions_executed"] = True
        summary["flatten_started"] = bool(flatten_result.get("started"))
        summary["flatten_already_running"] = bool(flatten_result.get("already_running"))
    return summary
```

```python
def _stop_spot_runner_process(
    symbol: str | None = None,
    *,
    timeout_seconds: float = 10.0,
    cancel_orders: bool = True,
    close_all_positions: bool = False,
) -> dict[str, Any]:
    # keep existing stop logic
    return {
        "stopped": True,
        "killed": False,
        "runner": probe,
        "symbol": normalized_symbol,
        "cleanup": cleanup,
        "post_stop_actions": _execute_spot_stop_actions(
            symbol=normalized_symbol,
            cancel_open_orders=cancel_orders,
            close_all_positions=close_all_positions,
        ),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/test_spot_flatten_runner.py tests/test_spot_runner.py -k "flatten" -v`
Expected: PASS for the new spot flatten builder and stop integration tests

- [ ] **Step 5: Commit**

```bash
git add tests/test_spot_flatten_runner.py tests/test_spot_runner.py src/grid_optimizer/spot_flatten_runner.py src/grid_optimizer/web.py
git commit -m "feat: add maker flatten flow for spot runner stops"
```

### Task 5: Spot Runner Runtime Guard And Snapshot Integration

**Files:**
- Modify: `src/grid_optimizer/spot_loop_runner.py`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_spot_runner.py`

- [ ] **Step 1: Write the failing spot runtime guard tests**

```python
    @patch("grid_optimizer.spot_loop_runner.fetch_spot_symbol_config")
    @patch("grid_optimizer.spot_loop_runner.load_binance_api_credentials")
    def test_run_once_rejects_inverted_runtime_window(self, mock_load_creds, mock_symbol_config) -> None:
        mock_load_creds.return_value = ("key", "secret")
        mock_symbol_config.return_value = {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "tick_size": 0.01,
            "step_size": 0.00001,
            "min_qty": 0.00001,
            "min_notional": 5.0,
        }
        args = Namespace(
            symbol="BTCUSDT",
            strategy_mode="spot_one_way_long",
            min_price=50000.0,
            max_price=100000.0,
            n=10,
            total_quote_budget=1000.0,
            run_start_time="2026-03-30T10:00:00+00:00",
            run_end_time="2026-03-30T09:00:00+00:00",
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
        )
        with self.assertRaisesRegex(RuntimeError, "run_start_time must be earlier than run_end_time"):
            run_once(args)

    @patch("grid_optimizer.web._load_spot_runner_control_config")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._read_json_dict")
    @patch("grid_optimizer.web._tail_jsonl_dicts")
    def test_build_spot_runner_snapshot_exposes_runtime_guard_fields(
        self,
        mock_tail,
        mock_read_json,
        mock_read_runner,
        mock_load_config,
    ) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "BTCUSDT",
                "run_start_time": "2026-03-30T01:00:00+00:00",
                "run_end_time": "2026-03-30T10:00:00+00:00",
                "rolling_hourly_loss_limit": 25.0,
                "max_cumulative_notional": 5000.0,
            }
        )
        mock_load_config.return_value = config
        mock_read_runner.return_value = {"configured": True, "pid": 123, "is_running": True, "args": None, "config": config}
        mock_read_json.return_value = {"metrics": {"gross_notional": 3200.0, "realized_pnl": -10.0}}
        mock_tail.return_value = [{"runtime_status": "running", "rolling_hourly_loss": 12.5, "stop_triggered": False}]
        snapshot = _build_spot_runner_snapshot("BTCUSDT")
        self.assertEqual(snapshot["risk_controls"]["runtime_status"], "running")
        self.assertEqual(snapshot["risk_controls"]["run_start_time"], "2026-03-30T01:00:00+00:00")
        self.assertEqual(snapshot["risk_controls"]["rolling_hourly_loss_limit"], 25.0)
        self.assertEqual(snapshot["risk_controls"]["max_cumulative_notional"], 5000.0)
        self.assertEqual(snapshot["risk_controls"]["rolling_hourly_loss"], 12.5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/test_spot_runner.py -k "runtime_guard or runtime_window or snapshot_exposes_runtime_guard_fields" -v`
Expected: FAIL because the spot runner does not parse these fields and the snapshot does not expose them

- [ ] **Step 3: Implement spot runtime guard evaluation and snapshot fields**

```python
from .runtime_guards import evaluate_runtime_guards, normalize_runtime_guard_config


def _build_spot_runtime_guard_events(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        if not trade.get("time"):
            continue
        ts = datetime.fromtimestamp(int(trade["time"]) / 1000, tz=timezone.utc).isoformat()
        net_pnl = float(trade.get("realized_pnl") or 0.0) - float(trade.get("commission_quote") or 0.0) - float(trade.get("recycle_loss_abs") or 0.0)
        events.append({"ts": ts, "net_pnl": net_pnl})
    return events
```

```python
    parser.add_argument("--run-start-time", type=str, default=None)
    parser.add_argument("--run-end-time", type=str, default=None)
    parser.add_argument("--rolling-hourly-loss-limit", type=float, default=None)
    parser.add_argument("--max-cumulative-notional", type=float, default=None)
```

```python
    guard_config = normalize_runtime_guard_config(
        {
            "run_start_time": getattr(args, "run_start_time", None),
            "run_end_time": getattr(args, "run_end_time", None),
            "rolling_hourly_loss_limit": getattr(args, "rolling_hourly_loss_limit", None),
            "max_cumulative_notional": getattr(args, "max_cumulative_notional", None),
        }
    )
    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else _new_metrics()
    guard_result = evaluate_runtime_guards(
        config=guard_config,
        now=_utc_now(),
        cumulative_gross_notional=_safe_float(metrics.get("gross_notional")),
        pnl_events=_build_spot_runtime_guard_events(trades),
    )
    if guard_result.runtime_status == "waiting":
        summary = {
            "ts": _utc_now().isoformat(),
            "cycle": int(state.get("cycle", 0) or 0),
            "symbol": symbol,
            "strategy_mode": strategy_mode,
            "runtime_status": "waiting",
            "stop_triggered": False,
            "stop_reason": "before_start_window",
            "stop_reasons": ["before_start_window"],
            "rolling_hourly_loss": guard_result.rolling_hourly_loss,
            "cumulative_gross_notional": guard_result.cumulative_gross_notional,
        }
        _save_state(state_path, state)
        _append_jsonl(Path(args.summary_jsonl), summary)
        return summary
    if guard_result.stop_triggered:
        stop_result = _execute_spot_stop_actions(symbol=symbol, cancel_open_orders=True, close_all_positions=True)
        summary = {
            "ts": _utc_now().isoformat(),
            "cycle": int(state.get("cycle", 0) or 0),
            "symbol": symbol,
            "strategy_mode": strategy_mode,
            "runtime_status": "stopped",
            "stop_triggered": True,
            "stop_reason": guard_result.primary_reason,
            "stop_reasons": guard_result.matched_reasons,
            "stop_triggered_at": guard_result.triggered_at,
            "rolling_hourly_loss": guard_result.rolling_hourly_loss,
            "cumulative_gross_notional": guard_result.cumulative_gross_notional,
            "post_stop_actions": stop_result,
        }
        _save_state(state_path, state)
        _append_jsonl(Path(args.summary_jsonl), summary)
        raise SystemExit(f"spot runner stopped by runtime guard: {guard_result.primary_reason}")
```

```python
        "risk_controls": {
            # existing fields...
            "runtime_status": str(latest_event.get("runtime_status", "running") or "running"),
            "run_start_time": config.get("run_start_time"),
            "run_end_time": config.get("run_end_time"),
            "rolling_hourly_loss_limit": _float_or_default(config.get("rolling_hourly_loss_limit"), "rolling_hourly_loss_limit"),
            "max_cumulative_notional": _float_or_default(config.get("max_cumulative_notional"), "max_cumulative_notional"),
            "rolling_hourly_loss": _float_or_default(latest_event.get("rolling_hourly_loss"), "rolling_hourly_loss"),
            "cumulative_gross_notional": _float_or_default(latest_event.get("cumulative_gross_notional", metrics.get("gross_notional")), "cumulative_gross_notional"),
            "stop_triggered": bool(latest_event.get("stop_triggered")),
            "stop_reason": str(latest_event.get("stop_reason", "") or ""),
            "stop_triggered_at": latest_event.get("stop_triggered_at"),
        },
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/test_spot_runner.py -k "runtime_guard or runtime_window or snapshot_exposes_runtime_guard_fields" -v`
Expected: PASS for spot runtime guard validation, waiting/stopped summaries, and snapshot coverage

- [ ] **Step 5: Commit**

```bash
git add tests/test_spot_runner.py src/grid_optimizer/spot_loop_runner.py src/grid_optimizer/web.py
git commit -m "feat: add runtime guard stops to spot runner"
```

### Task 6: End-To-End Verification And Deployment Preparation

**Files:**
- Modify: `README.md`
- Modify: `deploy/oracle/README.md`

- [ ] **Step 1: Write the failing documentation assertions as grep checks**

```bash
rg -n "run_start_time|rolling_hourly_loss_limit|max_cumulative_notional|43.155.136.111" README.md deploy/oracle/README.md
```

Expected: no matches for the new runtime guard fields before docs are updated

- [ ] **Step 2: Update operator docs with the new runner flags and stop semantics**

```markdown
### Competition runner runtime guard flags

- `--run-start-time`: UTC ISO 8601 start time. Before this time the runner stays alive but does not place strategy orders.
- `--run-end-time`: UTC ISO 8601 end time. At or after this time the runner stops, cancels strategy orders, and starts flattening.
- `--rolling-hourly-loss-limit`: stop once rolling 60-minute realized loss reaches the configured threshold.
- `--max-cumulative-notional`: stop once cumulative `gross_notional` reaches the configured threshold.

When a runtime guard stop is triggered, the runner records `stop_reason`, `stop_triggered_at`, and starts cancel + top-of-book maker flattening until the position is flat.
```

```markdown
## Post-merge deployment target for this feature

After verification, deploy this change to `43.155.136.111` using the existing Oracle update flow and confirm the updated runner UI exposes the new runtime guard fields.
```

- [ ] **Step 3: Run the full targeted verification suite**

Run: `PYTHONPATH=src pytest tests/test_runtime_guards.py tests/test_web_security.py tests/test_loop_runner.py tests/test_spot_runner.py tests/test_spot_flatten_runner.py -v`
Expected: PASS for all targeted tests

Run: `PYTHONPATH=src python -m grid_optimizer.loop_runner --help | rg "run-start-time|rolling-hourly-loss-limit|max-cumulative-notional"`
Expected: all three new futures CLI flags are listed

Run: `PYTHONPATH=src python -m grid_optimizer.spot_loop_runner --help | rg "run-start-time|rolling-hourly-loss-limit|max-cumulative-notional"`
Expected: all three new spot CLI flags are listed

- [ ] **Step 4: Commit**

```bash
git add README.md deploy/oracle/README.md tests/test_runtime_guards.py tests/test_web_security.py tests/test_loop_runner.py tests/test_spot_runner.py tests/test_spot_flatten_runner.py src/grid_optimizer/runtime_guards.py src/grid_optimizer/loop_runner.py src/grid_optimizer/spot_loop_runner.py src/grid_optimizer/spot_flatten_runner.py src/grid_optimizer/web.py
git commit -m "docs: document competition runner runtime guards"
```

- [ ] **Step 5: Prepare deployment command for `43.155.136.111`**

```bash
git push origin main
ssh 43.155.136.111 'cd /home/opc/grid_trading && bash deploy/oracle/install_or_update.sh'
ssh 43.155.136.111 'sudo systemctl --no-pager --full status grid-web.service | sed -n "1,25p"'
```

Expected:

- push succeeds
- remote install/update script exits 0
- remote service status shows the updated service running without immediate restart failures
