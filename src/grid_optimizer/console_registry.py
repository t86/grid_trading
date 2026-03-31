from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_CONSOLE_REGISTRY_PATH = Path("config/console_registry.json")
ALLOWED_ACCOUNT_KINDS = {"futures", "spot", "mixed"}


def load_console_registry(path: Path | str | None = None) -> dict[str, Any]:
    registry_path = Path(path) if path is not None else DEFAULT_CONSOLE_REGISTRY_PATH
    raw = json.loads(registry_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Console registry must be a JSON object")

    servers = [_normalize_server(item) for item in _require_sequence(raw, "servers")]
    servers_by_id = {server["id"]: server for server in servers}
    if len(servers_by_id) != len(servers):
        raise ValueError("Duplicate server_id in console registry")

    accounts = [_normalize_account(item, servers_by_id) for item in _require_sequence(raw, "accounts")]
    accounts_by_id = {account["id"]: account for account in accounts}
    if len(accounts_by_id) != len(accounts):
        raise ValueError("Duplicate account id in console registry")

    competition_source = _normalize_competition_source(raw.get("competition_source"), servers_by_id)
    default_account = _select_default_account(accounts)

    return {
        "servers": servers,
        "servers_by_id": servers_by_id,
        "accounts": accounts,
        "accounts_by_id": accounts_by_id,
        "default_account": default_account,
        "competition_source": competition_source,
    }


def serialize_console_registry(registry: dict[str, Any]) -> dict[str, Any]:
    return {
        "servers": registry["servers"],
        "accounts": registry["accounts"],
        "default_account_id": registry["default_account"]["id"],
        "competition_source": registry["competition_source"],
    }


def _require_sequence(raw: dict[str, Any], key: str) -> list[Any]:
    value = raw.get(key, [])
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a list")
    return value


def _require_mapping(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def _require_str(item: dict[str, Any], key: str, context: str) -> str:
    value = item.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context} must include a non-empty {key}")
    return value.strip()


def _require_int(item: dict[str, Any], key: str, context: str) -> int:
    value = item.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{context} must include an integer {key}")
    return value


def _require_str_list(item: dict[str, Any], key: str, context: str) -> list[str]:
    value = item.get(key, [])
    if not isinstance(value, list) or any(not isinstance(entry, str) for entry in value):
        raise ValueError(f"{context} must include a list of strings for {key}")
    return [entry.strip() for entry in value]


def _normalize_server(item: Any) -> dict[str, Any]:
    server = _require_mapping(item, "server")
    server_id = _require_str(server, "id", "server")
    normalized = dict(server)
    normalized["id"] = server_id
    normalized["label"] = _require_str(server, "label", f"server {server_id}")
    normalized["base_url"] = _require_str(server, "base_url", f"server {server_id}")
    normalized["enabled"] = bool(server.get("enabled", True))
    normalized["capabilities"] = _require_str_list(server, "capabilities", f"server {server_id}")
    return normalized


def _normalize_account(item: Any, servers_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    account = _require_mapping(item, "account")
    account_id = _require_str(account, "id", "account")
    server_id = _require_str(account, "server_id", f"account {account_id}")
    if server_id not in servers_by_id:
        raise ValueError(f"Unknown server_id for account {account_id}: {server_id}")
    kind = _require_str(account, "kind", f"account {account_id}")
    if kind not in ALLOWED_ACCOUNT_KINDS:
        raise ValueError(f"Unsupported account kind for account {account_id}: {kind}")
    normalized = dict(account)
    normalized["id"] = account_id
    normalized["server_id"] = server_id
    normalized["label"] = _require_str(account, "label", f"account {account_id}")
    normalized["kind"] = kind
    normalized["priority"] = _require_int(account, "priority", f"account {account_id}")
    normalized["enabled"] = bool(account.get("enabled", True))
    normalized["default_symbols"] = _require_str_list(account, "default_symbols", f"account {account_id}")
    normalized["competition_symbols"] = _require_str_list(account, "competition_symbols", f"account {account_id}")
    normalized["pages"] = _require_str_list(account, "pages", f"account {account_id}")
    return normalized


def _normalize_competition_source(
    value: Any,
    servers_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    competition_source = _require_mapping(value, "competition_source")
    server_id = _require_str(competition_source, "server_id", "competition_source")
    if server_id not in servers_by_id:
        raise ValueError(f"Unknown server_id for competition_source: {server_id}")
    path = _require_str(competition_source, "path", "competition_source")
    normalized = dict(competition_source)
    normalized["server_id"] = server_id
    normalized["path"] = path
    return normalized


def _select_default_account(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    best_account: dict[str, Any] | None = None
    best_priority: int | None = None
    for account in accounts:
        if not account["enabled"]:
            continue
        priority = account["priority"]
        if best_account is None or priority > best_priority:
            best_account = account
            best_priority = priority
    if best_account is None:
        raise ValueError("No enabled accounts available in console registry")
    return best_account
