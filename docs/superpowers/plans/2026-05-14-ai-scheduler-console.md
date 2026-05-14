# AI Scheduler Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a first-pass AI scheduler console with task storage, server/symbol candidate APIs, task list/result views, and a scheduler worker foundation.

**Architecture:** Keep the current Python stdlib web app. Add a focused `ai_scheduler.py` module for registry filtering, task/global-policy storage, symbol expansion, run grouping, and worker helpers; then wire new HTML/JS pages and JSON APIs into `grid_optimizer.web`.

**Tech Stack:** Python stdlib (`json`, `pathlib`, `subprocess`, `datetime`), existing `grid_optimizer.web`, existing console/running-status payloads, `unittest`.

---

### Task 1: Lock scheduler data model and registry extensions with tests

**Files:**
- Create: `tests/test_ai_scheduler.py`
- Modify: `src/grid_optimizer/console_registry.py`
- Create: `src/grid_optimizer/ai_scheduler.py`

- [ ] **Step 1: Write failing tests for registry filtering, task expansion, policy inheritance, and run grouping**

```python
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


class AISchedulerTests(unittest.TestCase):
    def test_load_console_registry_preserves_scheduler_fields(self) -> None:
        from grid_optimizer.console_registry import load_console_registry

        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "console_registry.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "srv_110",
                                "label": "110",
                                "base_url": "http://110:8787",
                                "enabled": True,
                                "capabilities": ["strategies"],
                                "scheduler_enabled": True,
                                "ssh_host": "43.156.35.110",
                                "ssh_user": "ubuntu",
                                "workspace_dir": "/home/ubuntu/wangge",
                                "codex_path": "/usr/local/bin/codex",
                                "python_path": "/usr/bin/python3",
                            }
                        ],
                        "accounts": [],
                        "competition_source": {"server_id": "srv_110", "path": "/api/competition_board"},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "No enabled accounts"):
                load_console_registry(registry_path)

    def test_expand_scheduler_targets_creates_one_task_per_server_symbol(self) -> None:
        from grid_optimizer.ai_scheduler import expand_scheduler_targets

        tasks = expand_scheduler_targets(
            name="巡检任务",
            server_ids=["srv_110", "srv_114"],
            symbols=["BARDUSDT", "XAUTUSDT"],
            schedule={"kind": "interval", "value": "10m", "timezone": "Asia/Shanghai"},
            goal_prompt="优先稳态调参",
            execution_mode="one_shot",
            global_policy={"allow_start_stop": True},
            policy_overrides={"allow_reduce_position": False},
        )

        self.assertEqual(len(tasks), 4)
        self.assertEqual({(item["server_id"], item["symbol"]) for item in tasks}, {
            ("srv_110", "BARDUSDT"),
            ("srv_110", "XAUTUSDT"),
            ("srv_114", "BARDUSDT"),
            ("srv_114", "XAUTUSDT"),
        })

    def test_group_runs_by_server_then_symbol(self) -> None:
        from grid_optimizer.ai_scheduler import group_task_runs

        grouped = group_task_runs(
            [
                {"server_id": "srv_110", "symbol": "BARDUSDT", "triggered_at": "2026-05-14T00:00:00+00:00"},
                {"server_id": "srv_110", "symbol": "BARDUSDT", "triggered_at": "2026-05-14T00:10:00+00:00"},
                {"server_id": "srv_114", "symbol": "XAUTUSDT", "triggered_at": "2026-05-14T00:05:00+00:00"},
            ]
        )

        self.assertEqual([item["server_id"] for item in grouped], ["srv_110", "srv_114"])
        self.assertEqual(grouped[0]["symbols"][0]["symbol"], "BARDUSDT")
        self.assertEqual(len(grouped[0]["symbols"][0]["runs"]), 2)
```

- [ ] **Step 2: Run the targeted test file and verify it fails**

Run: `PYTHONPATH=src python3 -m unittest tests.test_ai_scheduler -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.ai_scheduler'` or missing scheduler fields/assertions.

- [ ] **Step 3: Implement minimal registry preservation and scheduler helpers**

```python
# src/grid_optimizer/console_registry.py
def _normalize_server(item: Any) -> dict[str, Any]:
    server = _require_mapping(item, "server")
    server_id = _require_str(server, "id", "server")
    normalized = dict(server)
    normalized["id"] = server_id
    normalized["label"] = _require_str(server, "label", f"server {server_id}")
    normalized["base_url"] = _require_str(server, "base_url", f"server {server_id}")
    normalized["enabled"] = bool(server.get("enabled", True))
    normalized["capabilities"] = _require_str_list(server, "capabilities", f"server {server_id}")
    normalized["scheduler_enabled"] = bool(server.get("scheduler_enabled", False))
    for key in ("ssh_host", "ssh_user", "workspace_dir", "codex_path", "python_path"):
        value = server.get(key)
        if value is not None:
            normalized[key] = str(value).strip()
    return normalized
```

```python
# src/grid_optimizer/ai_scheduler.py
from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


def expand_scheduler_targets(
    *,
    name: str,
    server_ids: list[str],
    symbols: list[str],
    schedule: dict[str, Any],
    goal_prompt: str,
    execution_mode: str,
    global_policy: dict[str, Any],
    policy_overrides: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for server_id in server_ids:
        for symbol in symbols:
            normalized_symbol = str(symbol).upper().strip()
            task_id = hashlib.sha1(f"{server_id}:{normalized_symbol}:{name}".encode("utf-8")).hexdigest()[:16]
            tasks.append(
                {
                    "task_id": task_id,
                    "name": name,
                    "server_id": str(server_id).strip(),
                    "symbol": normalized_symbol,
                    "enabled": True,
                    "schedule": dict(schedule),
                    "execution_mode": execution_mode,
                    "goal_prompt": goal_prompt,
                    "policy_overrides": dict(policy_overrides or {}),
                    "effective_policy_snapshot": {**global_policy, **dict(policy_overrides or {})},
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return tasks


def group_task_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[str(row.get("server_id") or "")][str(row.get("symbol") or "")].append(row)
    result: list[dict[str, Any]] = []
    for server_id in sorted(grouped):
        symbols = []
        for symbol in sorted(grouped[server_id]):
            runs = sorted(grouped[server_id][symbol], key=lambda item: str(item.get("triggered_at") or ""), reverse=True)
            symbols.append({"symbol": symbol, "runs": runs})
        result.append({"server_id": server_id, "symbols": symbols})
    return result
```

- [ ] **Step 4: Run the targeted tests and verify they pass**

Run: `PYTHONPATH=src python3 -m unittest tests.test_ai_scheduler -v`
Expected: PASS

### Task 2: Add file-backed policy/task storage and candidate symbol payloads

**Files:**
- Modify: `src/grid_optimizer/ai_scheduler.py`
- Modify: `tests/test_ai_scheduler.py`

- [ ] **Step 1: Write failing tests for saving global policy, saving tasks, listing tasks, and deriving candidate symbols from cross-running-status payloads**
- [ ] **Step 2: Run `PYTHONPATH=src python3 -m unittest tests.test_ai_scheduler -v` and verify the new tests fail**
- [ ] **Step 3: Implement `load/save_global_policy()`, `save_task()`, `list_tasks()`, `list_runs()`, and `build_scheduler_symbol_candidates()` in `src/grid_optimizer/ai_scheduler.py`**
- [ ] **Step 4: Re-run `PYTHONPATH=src python3 -m unittest tests.test_ai_scheduler -v` and verify PASS**

### Task 3: Add web pages and read APIs for scheduler console and result view

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_web_security.py`

- [ ] **Step 1: Write failing tests that assert new pages/API hooks exist**
- [ ] **Step 2: Run `PYTHONPATH=src python3 -m unittest tests.test_web_security.WebSecurityTests -v` and verify the new tests fail**
- [ ] **Step 3: Add `AI_SCHEDULER_PAGE`, `AI_SCHEDULER_RESULTS_PAGE`, render helpers, and GET APIs**
- [ ] **Step 4: Re-run `PYTHONPATH=src python3 -m unittest tests.test_web_security.WebSecurityTests -v` and verify PASS**

### Task 4: Add write APIs for task CRUD, enable/disable, and immediate-run recording

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `src/grid_optimizer/ai_scheduler.py`
- Modify: `tests/test_web_security.py`

- [ ] **Step 1: Write failing tests for POST handlers that create tasks, update policy, toggle tasks, and request immediate execution**
- [ ] **Step 2: Run the targeted `tests.test_web_security` cases and verify they fail**
- [ ] **Step 3: Implement POST routes and the corresponding storage mutations**
- [ ] **Step 4: Re-run the targeted tests and verify PASS**

### Task 5: Add scheduler worker foundations and remote Codex execution wrapper

**Files:**
- Modify: `src/grid_optimizer/ai_scheduler.py`
- Create: `src/grid_optimizer/ai_scheduler_worker.py`
- Modify: `tests/test_ai_scheduler.py`

- [ ] **Step 1: Write failing tests for task due-time evaluation, forbidden-action validation, and SSH/Codex command construction**
- [ ] **Step 2: Run `PYTHONPATH=src python3 -m unittest tests.test_ai_scheduler -v` and verify the new tests fail**
- [ ] **Step 3: Implement minimal worker helpers and a first-pass remote execution wrapper**
- [ ] **Step 4: Re-run `PYTHONPATH=src python3 -m unittest tests.test_ai_scheduler -v` and verify PASS**

### Task 6: Verify integrated coverage

**Files:**
- Test: `tests/test_ai_scheduler.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Run the focused suite**

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_ai_scheduler \
  tests.test_web_security \
  -v
```

Expected: PASS

- [ ] **Step 2: Inspect the diff**

Run: `git -C /Volumes/WORK/binance/wangge diff -- src/grid_optimizer/ai_scheduler.py src/grid_optimizer/ai_scheduler_worker.py src/grid_optimizer/console_registry.py src/grid_optimizer/web.py tests/test_ai_scheduler.py tests/test_web_security.py`
Expected: only scheduler console, worker foundation, registry, and related test changes appear.
