from __future__ import annotations

import json
import shlex
import subprocess
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from .ai_scheduler import (
    append_scheduler_task_run,
    effective_scheduler_policy,
    list_due_scheduler_tasks,
    list_scheduler_tasks,
    load_global_policy,
    load_scheduler_runtime_state,
    mark_scheduler_task_run,
    save_scheduler_runtime_state,
    validate_scheduler_action_plan,
    write_scheduler_workspace_snapshot,
)
from .console_overview import _fetch_remote_json
from .console_registry import load_console_registry


def build_remote_codex_exec_command(*, server: dict[str, Any], prompt: str) -> list[str]:
    ssh_host = str(server.get("ssh_host") or "").strip()
    ssh_user = str(server.get("ssh_user") or "").strip()
    workspace_dir = str(server.get("workspace_dir") or "").strip()
    codex_path = str(server.get("codex_path") or "codex").strip()
    if not ssh_host or not ssh_user:
        raise ValueError("server ssh target is incomplete")
    if not workspace_dir:
        raise ValueError("workspace_dir is required")
    remote_target = f"{ssh_user}@{ssh_host}"
    remote_cmd = (
        f"cd {shlex.quote(workspace_dir)} && "
        f"{shlex.quote(codex_path)} exec --dangerously-bypass-approvals-and-sandbox "
        f"{shlex.quote(prompt)}"
    )
    return ["ssh", remote_target, remote_cmd]


def _registry_server_map() -> dict[str, dict[str, Any]]:
    registry = load_console_registry()
    return {
        str(server.get("id") or "").strip(): server
        for server in registry.get("servers") or []
        if isinstance(server, dict)
    }


def _fetch_remote_runner_snapshot(server: dict[str, Any], symbol: str) -> dict[str, Any]:
    payload = _fetch_remote_json(server, "/api/running_status", params={"scope": "local", "symbol": symbol})
    groups = payload.get("groups") if isinstance(payload.get("groups"), dict) else {}
    for row in list(groups.get("running") or []) + list(groups.get("saved_idle") or []):
        if isinstance(row, dict) and str(row.get("symbol") or "").upper().strip() == symbol:
            return row
    raise RuntimeError(f"runner snapshot not found for {symbol}")


def _build_codex_prompt(task: dict[str, Any], current_snapshot: dict[str, Any], runtime_state: dict[str, Any]) -> str:
    policy = task.get("effective_policy_snapshot") if isinstance(task.get("effective_policy_snapshot"), dict) else {}
    config = current_snapshot.get("config") if isinstance(current_snapshot.get("config"), dict) else {}
    payload = {
        "task": {
            "task_id": task.get("task_id"),
            "name": task.get("name"),
            "server_id": task.get("server_id"),
            "symbol": task.get("symbol"),
            "goal_prompt": task.get("goal_prompt"),
            "execution_mode": task.get("execution_mode"),
        },
        "policy": policy,
        "runtime_state": runtime_state,
        "runner_snapshot": {
            "symbol": current_snapshot.get("symbol"),
            "is_running": current_snapshot.get("is_running"),
            "status": current_snapshot.get("status"),
            "position_summary": current_snapshot.get("position_summary"),
            "open_order_count": current_snapshot.get("open_order_count"),
            "total_pnl": current_snapshot.get("total_pnl"),
            "recent_hour_pnl": current_snapshot.get("recent_hour_pnl"),
            "config": config,
        },
        "output_contract": {
            "instruction": "Only return JSON with keys summary, runtime_memory, actions.",
            "actions": [
                "noop",
                "update_runner_config",
                "save_runner_config",
                "start_runner",
                "stop_runner",
                "restart_runner",
                "cancel_open_orders",
                "flatten_or_reduce_position",
            ],
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def _parse_codex_output(stdout: str) -> dict[str, Any]:
    text = str(stdout or "").strip()
    if not text:
        raise ValueError("empty codex output")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("codex output is not valid json") from None
        payload = json.loads(text[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("codex output must be json object")
    return payload


def _remote_post_json(server: dict[str, Any], path: str, payload: dict[str, Any]) -> dict[str, Any]:
    import base64
    import os

    server_id = str(server.get("id", "")).strip().upper()
    username = os.getenv(f"GRID_NODE_{server_id}_USERNAME")
    password = os.getenv(f"GRID_NODE_{server_id}_PASSWORD")
    if not username or not password:
        raise RuntimeError(f"Missing Basic Auth credentials for server_id {server_id}")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    base_url = str(server.get("base_url", "")).strip().rstrip("/")
    response = requests.post(
        f"{base_url}{path}",
        headers={"Authorization": f"Basic {token}", "Accept": "application/json"},
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("unexpected json payload")
    return data


def apply_scheduler_action_plan(
    *,
    server: dict[str, Any],
    task: dict[str, Any],
    current_snapshot: dict[str, Any],
    validated_actions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    current_config = dict(current_snapshot.get("config") or {})
    symbol = str(task.get("symbol") or "").upper().strip()
    for action in validated_actions:
        action_type = str(action.get("type") or "").strip()
        if action_type == "noop":
            applied.append({"type": "noop", "ok": True})
            continue
        if action_type == "update_runner_config":
            changes = dict(action.get("changes") or {})
            for field, values in changes.items():
                if isinstance(values, dict) and "to" in values:
                    current_config[field] = values.get("to")
            current_config["symbol"] = symbol
            applied.append({"type": "update_runner_config", "ok": True, "changed_fields": sorted(changes)})
            continue
        if action_type == "save_runner_config":
            payload = {**current_config, "symbol": symbol}
            result = _remote_post_json(server, "/api/runner/save", payload)
            applied.append({"type": action_type, "ok": bool(result.get("ok", True))})
            continue
        if action_type == "start_runner":
            payload = {**current_config, "symbol": symbol}
            result = _remote_post_json(server, "/api/runner/start", payload)
            applied.append({"type": action_type, "ok": bool(result.get("ok", True))})
            continue
        if action_type == "restart_runner":
            payload = {**current_config, "symbol": symbol}
            result = _remote_post_json(server, "/api/runner/start", payload)
            applied.append({"type": action_type, "ok": bool(result.get("ok", True))})
            continue
        if action_type == "stop_runner":
            result = _remote_post_json(server, "/api/runner/stop", {"symbol": symbol})
            applied.append({"type": action_type, "ok": bool(result.get("ok", True))})
            continue
        if action_type == "cancel_open_orders":
            result = _remote_post_json(server, "/api/runner/stop", {"symbol": symbol, "cancel_open_orders": True})
            applied.append({"type": action_type, "ok": bool(result.get("ok", True))})
            continue
        if action_type == "flatten_or_reduce_position":
            result = _remote_post_json(
                server,
                "/api/runner/stop",
                {"symbol": symbol, "cancel_open_orders": True, "close_all_positions": True},
            )
            applied.append({"type": action_type, "ok": bool(result.get("ok", True))})
            continue
        raise ValueError(f"unsupported action type: {action_type}")
    return applied


def run_scheduler_task(task: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    server = _registry_server_map().get(str(task.get("server_id") or "").strip())
    if server is None:
        raise ValueError(f"unknown server_id: {task.get('server_id')}")
    runtime_state = load_scheduler_runtime_state(str(task.get("task_id") or ""))
    current_snapshot = _fetch_remote_runner_snapshot(server, str(task.get("symbol") or "").upper().strip())
    prompt = _build_codex_prompt(task, current_snapshot, runtime_state)
    write_scheduler_workspace_snapshot(task, prompt=prompt, runtime_state=runtime_state)
    command = build_remote_codex_exec_command(server=server, prompt=prompt)
    completed = subprocess.run(command, capture_output=True, text=True, check=False, timeout=180)
    run_id = uuid.uuid4().hex[:12]
    triggered_at = current_time.isoformat()
    if completed.returncode != 0:
        summary = (completed.stderr or completed.stdout or "codex exec failed").strip()
        append_scheduler_task_run(
            str(task.get("task_id") or ""),
            {
                "run_id": run_id,
                "task_id": task.get("task_id"),
                "server_id": task.get("server_id"),
                "symbol": task.get("symbol"),
                "triggered_at": triggered_at,
                "status": "error",
                "summary": summary,
            },
        )
        write_scheduler_workspace_snapshot(
            task,
            prompt=prompt,
            runtime_state=runtime_state,
            result={
                "run_id": run_id,
                "triggered_at": triggered_at,
                "status": "error",
                "summary": summary,
            },
            codex_stdout=completed.stdout,
            codex_stderr=completed.stderr,
        )
        mark_scheduler_task_run(str(task.get("task_id") or ""), status="error", summary=summary, triggered_at=triggered_at)
        return {"ok": False, "status": "error", "summary": summary}
    codex_payload = _parse_codex_output(completed.stdout)
    policy = effective_scheduler_policy(task, global_policy=load_global_policy())
    validated_actions = validate_scheduler_action_plan(
        codex_payload,
        policy=policy,
        current_config=dict(current_snapshot.get("config") or {}),
    )
    applied_actions = apply_scheduler_action_plan(
        server=server,
        task=task,
        current_snapshot=current_snapshot,
        validated_actions=validated_actions,
    )
    runtime_memory = codex_payload.get("runtime_memory")
    if isinstance(runtime_memory, dict):
        save_scheduler_runtime_state(str(task.get("task_id") or ""), runtime_memory)
        runtime_state = runtime_memory
    summary = str(codex_payload.get("summary") or "").strip() or "AI execution completed"
    status = "success"
    run_payload = {
        "run_id": run_id,
        "task_id": task.get("task_id"),
        "server_id": task.get("server_id"),
        "symbol": task.get("symbol"),
        "triggered_at": triggered_at,
        "status": status,
        "summary": summary,
        "actions": validated_actions,
        "applied_actions": applied_actions,
    }
    append_scheduler_task_run(
        str(task.get("task_id") or ""),
        run_payload,
    )
    write_scheduler_workspace_snapshot(
        task,
        prompt=prompt,
        runtime_state=runtime_state,
        result={**run_payload, "codex_payload": codex_payload},
        actions=validated_actions,
        codex_stdout=completed.stdout,
        codex_stderr=completed.stderr,
    )
    mark_scheduler_task_run(str(task.get("task_id") or ""), status=status, summary=summary, triggered_at=triggered_at)
    return {"ok": True, "status": status, "summary": summary, "actions": applied_actions}


def run_due_scheduler_tasks(*, now: datetime | None = None) -> list[dict[str, Any]]:
    current_time = now or datetime.now(timezone.utc)
    tasks = list_due_scheduler_tasks(list_scheduler_tasks(), now=current_time)
    return [run_scheduler_task(task, now=current_time) for task in tasks]
