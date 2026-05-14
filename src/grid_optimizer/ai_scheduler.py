from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote


AI_SCHEDULER_ROOT = Path("output/ai_scheduler")
GLOBAL_POLICY_PATH = AI_SCHEDULER_ROOT / "global_policy.json"
TASKS_DIR = AI_SCHEDULER_ROOT / "tasks"
RUNTIME_DIR = AI_SCHEDULER_ROOT / "runtime"
TASK_RUNS_DIR = AI_SCHEDULER_ROOT / "task_runs"
WORKSPACES_DIR = AI_SCHEDULER_ROOT / "workspaces"

DEFAULT_GLOBAL_POLICY: dict[str, Any] = {
    "allow_update_config": True,
    "allow_start_stop": True,
    "allow_cancel_orders": False,
    "allow_reduce_position": False,
    "allow_switch_strategy_mode": False,
    "max_config_change_ratio": 0.2,
    "max_notional_delta": 300.0,
    "default_goal_prompt": "优先稳态调参，允许直接启停策略，禁止平仓/减仓和策略模式切换。",
}


def _root_path(root: Path | str | None = None) -> Path:
    return Path(root) if root is not None else AI_SCHEDULER_ROOT


def _ensure_dirs(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    TASKS_DIR.relative_to(AI_SCHEDULER_ROOT)
    (root / "tasks").mkdir(parents=True, exist_ok=True)
    (root / "runtime").mkdir(parents=True, exist_ok=True)
    (root / "task_runs").mkdir(parents=True, exist_ok=True)
    (root / "workspaces").mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, *, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    return payload


def load_global_policy(*, root: Path | str | None = None) -> dict[str, Any]:
    base = _root_path(root)
    _ensure_dirs(base)
    payload = _read_json(base / "global_policy.json", fallback={})
    if not isinstance(payload, dict):
        payload = {}
    return {**DEFAULT_GLOBAL_POLICY, **payload}


def save_global_policy(policy: dict[str, Any], *, root: Path | str | None = None) -> dict[str, Any]:
    base = _root_path(root)
    _ensure_dirs(base)
    merged = {**DEFAULT_GLOBAL_POLICY, **dict(policy or {})}
    (base / "global_policy.json").write_text(
        json.dumps(merged, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return merged


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
    overrides = dict(policy_overrides or {})
    tasks: list[dict[str, Any]] = []
    created_at = datetime.now(timezone.utc).isoformat()
    for server_id in server_ids:
        normalized_server_id = str(server_id or "").strip()
        if not normalized_server_id:
            continue
        for symbol in symbols:
            normalized_symbol = str(symbol or "").upper().strip()
            if not normalized_symbol:
                continue
            task_id = hashlib.sha1(f"{normalized_server_id}:{normalized_symbol}:{name}".encode("utf-8")).hexdigest()[:16]
            tasks.append(
                {
                    "task_id": task_id,
                    "name": str(name or "").strip() or normalized_symbol,
                    "server_id": normalized_server_id,
                    "symbol": normalized_symbol,
                    "enabled": True,
                    "schedule": dict(schedule or {}),
                    "execution_mode": str(execution_mode or "").strip() or "one_shot",
                    "goal_prompt": str(goal_prompt or "").strip(),
                    "policy_overrides": overrides,
                    "effective_policy_snapshot": {**dict(global_policy or {}), **overrides},
                    "created_at": created_at,
                    "updated_at": created_at,
                    "last_run_at": None,
                    "last_result_status": None,
                    "last_result_summary": None,
                    "next_run_at": created_at,
                }
            )
    return tasks


def _task_path(task_id: str, *, root: Path | str | None = None) -> Path:
    base = _root_path(root)
    _ensure_dirs(base)
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise ValueError("task_id is required")
    return base / "tasks" / f"{normalized_task_id}.json"


def save_scheduler_task(task: dict[str, Any], *, root: Path | str | None = None) -> dict[str, Any]:
    payload = dict(task or {})
    task_id = str(payload.get("task_id") or "").strip()
    if not task_id:
        raise ValueError("task_id is required")
    payload["workspace_path"] = str(scheduler_task_workspace_dir(payload, root=root))
    payload["code_server_url"] = scheduler_task_code_server_url(payload, root=root)
    _task_path(task_id, root=root).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_scheduler_workspace_snapshot(payload, root=root)
    return payload


def scheduler_task_workspace_dir(task: dict[str, Any] | str, *, root: Path | str | None = None) -> Path:
    base = _root_path(root)
    _ensure_dirs(base)
    task_id = task if isinstance(task, str) else str((task or {}).get("task_id") or "").strip()
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise ValueError("task_id is required")
    return base / "workspaces" / normalized_task_id


def scheduler_task_code_server_url(task: dict[str, Any] | str, *, root: Path | str | None = None) -> str:
    base_url = str(os.environ.get("AI_SCHEDULER_CODE_SERVER_URL") or "http://code.t86.cc.cd").strip().rstrip("/")
    workspace = scheduler_task_workspace_dir(task, root=root)
    absolute_workspace = workspace.resolve() if workspace.is_absolute() else (Path.cwd() / workspace).resolve()
    return f"{base_url}/?folder={quote(str(absolute_workspace), safe='')}"


def write_scheduler_workspace_snapshot(
    task: dict[str, Any],
    *,
    root: Path | str | None = None,
    prompt: str | None = None,
    result: dict[str, Any] | None = None,
    runtime_state: dict[str, Any] | None = None,
    actions: list[dict[str, Any]] | None = None,
    codex_stdout: str | None = None,
    codex_stderr: str | None = None,
) -> Path:
    workspace = scheduler_task_workspace_dir(task, root=root)
    (workspace / "runs").mkdir(parents=True, exist_ok=True)
    (workspace / "actions").mkdir(parents=True, exist_ok=True)
    (workspace / "task.json").write_text(
        json.dumps(dict(task or {}), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    readme_lines = [
        f"# AI Scheduler Task {task.get('task_id') or ''}",
        "",
        f"- name: {task.get('name') or ''}",
        f"- server_id: {task.get('server_id') or ''}",
        f"- symbol: {task.get('symbol') or ''}",
        f"- enabled: {task.get('enabled')}",
        f"- schedule: {json.dumps(task.get('schedule') or {}, ensure_ascii=False, sort_keys=True)}",
        "",
        "## Goal",
        "",
        str(task.get("goal_prompt") or ""),
        "",
    ]
    (workspace / "README.md").write_text("\n".join(readme_lines), encoding="utf-8")
    if prompt is not None:
        (workspace / "prompt.md").write_text(str(prompt), encoding="utf-8")
    if result is not None:
        (workspace / "last_result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    if runtime_state is not None:
        (workspace / "runtime_memory.json").write_text(
            json.dumps(runtime_state, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    if actions is not None:
        (workspace / "last_actions.json").write_text(
            json.dumps(actions, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    if codex_stdout is not None:
        (workspace / "last_stdout.txt").write_text(str(codex_stdout), encoding="utf-8")
    if codex_stderr is not None:
        (workspace / "last_stderr.txt").write_text(str(codex_stderr), encoding="utf-8")
    return workspace


def get_scheduler_task(task_id: str, *, root: Path | str | None = None) -> dict[str, Any] | None:
    payload = _read_json(_task_path(task_id, root=root), fallback=None)
    return payload if isinstance(payload, dict) else None


def delete_scheduler_task(task_id: str, *, root: Path | str | None = None) -> bool:
    path = _task_path(task_id, root=root)
    if not path.exists():
        return False
    path.unlink()
    return True


def effective_scheduler_policy(
    task: dict[str, Any],
    *,
    global_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **DEFAULT_GLOBAL_POLICY,
        **dict(global_policy or {}),
        **dict(task.get("policy_overrides") or {}),
    }


def load_scheduler_runtime_state(task_id: str, *, root: Path | str | None = None) -> dict[str, Any]:
    base = _root_path(root)
    _ensure_dirs(base)
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise ValueError("task_id is required")
    payload = _read_json(base / "runtime" / f"{normalized_task_id}.json", fallback={})
    return payload if isinstance(payload, dict) else {}


def save_scheduler_runtime_state(
    task_id: str,
    payload: dict[str, Any],
    *,
    root: Path | str | None = None,
) -> dict[str, Any]:
    base = _root_path(root)
    _ensure_dirs(base)
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise ValueError("task_id is required")
    data = dict(payload or {})
    (base / "runtime" / f"{normalized_task_id}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return data


def _parse_schedule_interval(schedule: dict[str, Any]) -> timedelta:
    value = str((schedule or {}).get("value") or "").strip().lower()
    if not value:
        return timedelta(minutes=10)
    if value.endswith("m"):
        return timedelta(minutes=max(int(value[:-1] or "0"), 1))
    if value.endswith("h"):
        return timedelta(hours=max(int(value[:-1] or "0"), 1))
    if value.endswith("d"):
        return timedelta(days=max(int(value[:-1] or "0"), 1))
    raise ValueError(f"unsupported schedule value: {value}")


def next_run_at_for_task(task: dict[str, Any], *, now: datetime | None = None) -> str:
    current_time = now or datetime.now(timezone.utc)
    return (current_time + _parse_schedule_interval(dict(task.get("schedule") or {}))).isoformat()


def update_scheduler_task(
    task_id: str,
    updates: dict[str, Any],
    *,
    root: Path | str | None = None,
) -> dict[str, Any]:
    task = get_scheduler_task(task_id, root=root)
    if task is None:
        raise ValueError(f"unknown task_id: {task_id}")
    merged = {**task, **dict(updates or {})}
    merged["task_id"] = task["task_id"]
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()
    if "policy_overrides" in merged and not isinstance(merged.get("policy_overrides"), dict):
        raise ValueError("policy_overrides must be object")
    if "enabled" in updates and bool(merged.get("enabled")) and not str(merged.get("next_run_at") or "").strip():
        merged["next_run_at"] = merged["updated_at"]
    save_scheduler_task(merged, root=root)
    return merged


def mark_scheduler_task_run(
    task_id: str,
    *,
    status: str,
    summary: str,
    triggered_at: str | None = None,
    root: Path | str | None = None,
) -> dict[str, Any]:
    task = get_scheduler_task(task_id, root=root)
    if task is None:
        raise ValueError(f"unknown task_id: {task_id}")
    current_triggered_at = str(triggered_at or datetime.now(timezone.utc).isoformat())
    next_run_at = next_run_at_for_task(task)
    if not bool(task.get("enabled", False)):
        next_run_at = ""
    return update_scheduler_task(
        task_id,
        {
            "last_run_at": current_triggered_at,
            "last_result_status": str(status or "").strip() or "unknown",
            "last_result_summary": str(summary or "").strip() or "--",
            "next_run_at": next_run_at,
        },
        root=root,
    )


def list_scheduler_tasks(*, root: Path | str | None = None) -> list[dict[str, Any]]:
    base = _root_path(root)
    _ensure_dirs(base)
    rows: list[dict[str, Any]] = []
    for path in sorted((base / "tasks").glob("*.json")):
        payload = _read_json(path, fallback=None)
        if isinstance(payload, dict):
            rows.append(payload)
    rows.sort(key=lambda item: (str(item.get("server_id") or ""), str(item.get("symbol") or ""), str(item.get("task_id") or "")))
    return rows


def append_scheduler_task_run(task_id: str, payload: dict[str, Any], *, root: Path | str | None = None) -> dict[str, Any]:
    base = _root_path(root)
    _ensure_dirs(base)
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise ValueError("task_id is required")
    path = base / "task_runs" / f"{normalized_task_id}.jsonl"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(dict(payload or {}), ensure_ascii=False, sort_keys=True))
        fh.write("\n")
    return dict(payload or {})


def list_scheduler_task_runs(*, root: Path | str | None = None) -> list[dict[str, Any]]:
    base = _root_path(root)
    _ensure_dirs(base)
    rows: list[dict[str, Any]] = []
    for path in sorted((base / "task_runs").glob("*.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for raw_line in lines:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    rows.sort(key=lambda item: str(item.get("triggered_at") or ""), reverse=True)
    return rows


def group_task_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        server_id = str(row.get("server_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        if not server_id or not symbol:
            continue
        grouped[server_id][symbol].append(row)
    result: list[dict[str, Any]] = []
    for server_id in sorted(grouped):
        symbols: list[dict[str, Any]] = []
        for symbol in sorted(grouped[server_id]):
            runs = sorted(
                grouped[server_id][symbol],
                key=lambda item: str(item.get("triggered_at") or ""),
                reverse=True,
            )
            symbols.append({"symbol": symbol, "runs": runs})
        result.append({"server_id": server_id, "symbols": symbols})
    return result


def list_due_scheduler_tasks(tasks: list[dict[str, Any]], *, now: datetime | None = None) -> list[dict[str, Any]]:
    current_time = now or datetime.now(timezone.utc)
    due: list[dict[str, Any]] = []
    for task in tasks:
        if not isinstance(task, dict):
            continue
        if not bool(task.get("enabled", False)):
            continue
        raw_next_run_at = str(task.get("next_run_at") or "").strip()
        if not raw_next_run_at:
            continue
        try:
            next_run_at = datetime.fromisoformat(raw_next_run_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        if next_run_at <= current_time:
            due.append(task)
    due.sort(key=lambda item: str(item.get("next_run_at") or ""))
    return due


def build_scheduler_symbol_candidates(running_status_payload: dict[str, Any]) -> dict[str, Any]:
    servers_out: list[dict[str, Any]] = []
    for server in running_status_payload.get("servers") or []:
        if not isinstance(server, dict):
            continue
        symbols: list[dict[str, Any]] = []
        groups = server.get("groups") if isinstance(server.get("groups"), dict) else {}
        for item in groups.get("running") or []:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper().strip()
                if symbol:
                    symbols.append({"symbol": symbol, "status": "running"})
        for item in groups.get("saved_idle") or []:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper().strip()
                if symbol:
                    symbols.append({"symbol": symbol, "status": "saved_idle"})
        symbols.sort(key=lambda item: (0 if item["status"] == "running" else 1, item["symbol"]))
        servers_out.append(
            {
                "server_id": str(server.get("server_id") or server.get("id") or "").strip(),
                "server_label": str(server.get("server_label") or server.get("label") or "").strip(),
                "server_base_url": str(server.get("server_base_url") or server.get("base_url") or "").strip(),
                "ok": bool(server.get("ok", True)),
                "error": str(server.get("error") or "").strip(),
                "symbols": symbols,
            }
        )
    return {"servers": servers_out}


def _as_float(value: Any) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def validate_scheduler_action_plan(
    payload: dict[str, Any],
    *,
    policy: dict[str, Any],
    current_config: dict[str, Any],
) -> list[dict[str, Any]]:
    actions = payload.get("actions")
    if not isinstance(actions, list):
        raise ValueError("actions must be list")

    validated: list[dict[str, Any]] = []
    max_ratio = float(policy.get("max_config_change_ratio", DEFAULT_GLOBAL_POLICY["max_config_change_ratio"]) or 0.0)
    max_notional_delta = float(policy.get("max_notional_delta", DEFAULT_GLOBAL_POLICY["max_notional_delta"]) or 0.0)
    allow_update_config = bool(policy.get("allow_update_config", DEFAULT_GLOBAL_POLICY["allow_update_config"]))
    allow_start_stop = bool(policy.get("allow_start_stop", DEFAULT_GLOBAL_POLICY["allow_start_stop"]))
    allow_cancel_orders = bool(policy.get("allow_cancel_orders", DEFAULT_GLOBAL_POLICY["allow_cancel_orders"]))
    allow_reduce_position = bool(policy.get("allow_reduce_position", DEFAULT_GLOBAL_POLICY["allow_reduce_position"]))
    allow_switch_strategy_mode = bool(policy.get("allow_switch_strategy_mode", DEFAULT_GLOBAL_POLICY["allow_switch_strategy_mode"]))

    for item in actions:
        if not isinstance(item, dict):
            raise ValueError("action must be object")
        action_type = str(item.get("type") or "").strip()
        if not action_type:
            raise ValueError("action type is required")
        if action_type == "update_runner_config":
            if not allow_update_config:
                raise ValueError("forbidden action: update_runner_config")
            changes = item.get("changes")
            if not isinstance(changes, dict) or not changes:
                raise ValueError("update_runner_config requires changes")
            for field, change in changes.items():
                if not isinstance(change, dict):
                    raise ValueError(f"{field} change must be object")
                before = _as_float(change.get("from"))
                after = _as_float(change.get("to"))
                if field == "strategy_mode":
                    if not allow_switch_strategy_mode and str(change.get("from") or "") != str(change.get("to") or ""):
                        raise ValueError("forbidden action: switch strategy_mode")
                    continue
                if field == "max_total_notional":
                    if before is not None and after is not None and abs(after - before) > max_notional_delta:
                        raise ValueError(f"{field} exceeds max notional delta")
                if before not in {None, 0.0} and after is not None:
                    change_ratio = abs(after - before) / abs(before)
                    if change_ratio > max_ratio:
                        raise ValueError(f"{field} exceeds max config change ratio")
            validated.append(item)
            continue
        if action_type in {"start_runner", "stop_runner", "restart_runner", "save_runner_config"}:
            if not allow_start_stop:
                raise ValueError(f"forbidden action: {action_type}")
            validated.append(item)
            continue
        if action_type == "cancel_open_orders":
            if not allow_cancel_orders:
                raise ValueError("forbidden action: cancel_open_orders")
            validated.append(item)
            continue
        if action_type == "flatten_or_reduce_position":
            if not allow_reduce_position:
                raise ValueError("forbidden action: flatten_or_reduce_position")
            validated.append(item)
            continue
        if action_type == "noop":
            validated.append(item)
            continue
        raise ValueError(f"unsupported action type: {action_type}")
    return validated
