from __future__ import annotations

import argparse
import atexit
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from .futures_run_lifecycle import (
    RUN_CONTRACT_OWNER_KEY,
    bind_run_contract_owner,
    validate_run_contract,
)
from .futures_recovery_store import (
    decode_recovery_control_state,
    recovery_coordinator_registered,
)
from .monitor import RUNNER_PID_PATH, runner_pid_path_for_symbol
from .recovery_control_ownership import (
    exclusive_control_lock,
    write_control_json_atomically,
)
from .spot_app_loss_audit import main as spot_app_loss_audit_main
from .web import (
    _build_runner_command,
    _build_spot_runner_command,
    _load_runner_control_config,
    _load_spot_runner_control_config,
    _runner_start_safety_preflight,
    _runner_control_path,
    _spot_runner_control_path,
)


DEFAULT_RUNNER_SYMBOL = "SOONUSDT"
RUNTIME_PATH_FLAGS = {
    "--plan-json",
    "--recovery-control-path",
    "--state-path",
    "--submit-report-json",
    "--summary-jsonl",
}
DEFAULT_SPOT_APP_LOSS_PRESTART_MAX_LOSS_PER_10K = 1.0
DEFAULT_SPOT_APP_LOSS_PRESTART_MAX_SAFE_SELL_GAP_TICKS = 2.0
DEFAULT_SPOT_APP_LOSS_PRESTART_MIN_BID_BREAK_EVEN_BUFFER_TICKS = 0.0
DEFAULT_SPOT_APP_LOSS_PRESTART_MIN_MAKER_RATIO = 0.99


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _float_config(config: dict[str, object], key: str, default: float) -> float:
    try:
        value = float(config.get(key, default))
    except (TypeError, ValueError):
        return default
    return value if value >= 0.0 else default


def _write_pid(path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(os.getpid()), encoding="utf-8")


def _cleanup_pid(path) -> None:
    try:
        if path.exists():
            current = path.read_text(encoding="utf-8").strip()
            if current == str(os.getpid()):
                path.unlink()
    except OSError:
        pass


def _anchor_relative_runtime_paths(command: list[str], runner_work_dir: str) -> list[str]:
    if not runner_work_dir:
        return command
    anchored = list(command)
    base = Path(runner_work_dir)
    for index, token in enumerate(anchored[:-1]):
        if token not in RUNTIME_PATH_FLAGS:
            continue
        path = Path(anchored[index + 1])
        if path.is_absolute():
            continue
        anchored[index + 1] = str(base / path)
    return anchored


def _should_use_spot_runner(symbol: str) -> bool:
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return False
    return _spot_runner_control_path(normalized).exists() and not _runner_control_path(normalized).exists()


def _validate_futures_run_contract(config: dict[str, object]) -> None:
    validate_run_contract(
        run_start_time=config.get("run_start_time"),
        runtime_guard_stats_start_time=config.get("runtime_guard_stats_start_time"),
        run_end_time=config.get("run_end_time"),
        target_value=config.get("max_cumulative_notional"),
        exit_policy=config.get("terminal_drain_exit_policy"),
        loss_budget=config.get("terminal_drain_absolute_loss_budget"),
        max_wait_seconds=config.get("terminal_drain_max_wait_seconds"),
        preserve_reason=config.get("terminal_drain_stop_preserve_reason"),
        wear_stop_per_10k=config.get("lifecycle_wear_stop_per_10k"),
        wear_stop_min_gross_notional=config.get(
            "lifecycle_wear_stop_min_gross_notional"
        ),
    )


def _persist_run_contract_owner(
    control_path: Path,
    *,
    effective_config: dict[str, object],
    prepared_config: dict[str, object],
) -> None:
    """Persist only the new owner alongside the operator's raw control."""

    raw_control: dict[str, object]
    try:
        payload = json.loads(control_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    raw_control = dict(payload) if isinstance(payload, dict) else dict(effective_config)
    if recovery_coordinator_registered(raw_control):
        decode_recovery_control_state(
            raw_control,
            expected_symbol=str(
                raw_control.get("symbol") or effective_config.get("symbol") or ""
            ),
        )
    raw_control[RUN_CONTRACT_OWNER_KEY] = prepared_config[RUN_CONTRACT_OWNER_KEY]
    raw_control["terminal_drain_max_order_notional"] = prepared_config[
        "terminal_drain_max_order_notional"
    ]
    write_control_json_atomically(control_path, raw_control)


def _load_and_bind_futures_run_config(symbol: str) -> dict[str, object]:
    """Load the effective control and freeze a bounded contract before exec."""

    config = _load_runner_control_config(symbol, include_running_process=False)
    config["symbol"] = symbol
    _validate_futures_run_contract(config)
    prepared, owner_changed = bind_run_contract_owner(
        config,
        activated_at=datetime.now(timezone.utc),
    )
    if not owner_changed:
        return prepared

    # Re-read under the same cross-process actuator lock used by the Web and
    # recovery writers.  This prevents first-start owner creation from
    # overwriting a control change that raced the initial read.
    control_path = _runner_control_path(symbol)
    with exclusive_control_lock(control_path):
        current = _load_runner_control_config(symbol, include_running_process=False)
        current["symbol"] = symbol
        _validate_futures_run_contract(current)
        prepared, owner_changed = bind_run_contract_owner(
            current,
            activated_at=datetime.now(timezone.utc),
        )
        if owner_changed:
            _persist_run_contract_owner(
                control_path,
                effective_config=current,
                prepared_config=prepared,
            )
    return prepared


def _run_spot_app_loss_prestart_gate(config: dict[str, object]) -> int:
    if not _truthy(config.get("spot_app_loss_prestart_gate_enabled", False)):
        return 0
    symbol = str(config.get("symbol") or "").upper().strip()
    if not symbol:
        raise SystemExit("spot_app_loss_prestart_gate requires symbol")
    max_loss = _float_config(
        config,
        "spot_app_loss_prestart_gate_max_loss_per_10k",
        _float_config(config, "spot_app_loss_per_10k_hard", DEFAULT_SPOT_APP_LOSS_PRESTART_MAX_LOSS_PER_10K)
        or DEFAULT_SPOT_APP_LOSS_PRESTART_MAX_LOSS_PER_10K,
    )
    max_gap = _float_config(
        config,
        "spot_app_loss_prestart_gate_max_safe_sell_gap_ticks",
        DEFAULT_SPOT_APP_LOSS_PRESTART_MAX_SAFE_SELL_GAP_TICKS,
    )
    min_bid_buffer = _float_config(
        config,
        "spot_app_loss_prestart_gate_min_bid_break_even_buffer_ticks",
        DEFAULT_SPOT_APP_LOSS_PRESTART_MIN_BID_BREAK_EVEN_BUFFER_TICKS,
    )
    min_maker_ratio = _float_config(
        config,
        "spot_app_loss_prestart_gate_min_maker_ratio",
        DEFAULT_SPOT_APP_LOSS_PRESTART_MIN_MAKER_RATIO,
    )
    min_gross_notional = _float_config(
        config,
        "spot_app_loss_prestart_gate_min_gross_notional",
        _float_config(config, "spot_app_loss_min_notional", 0.0),
    )
    argv = ["--symbol", symbol]
    start_time = str(
        config.get("spot_app_loss_prestart_gate_start_time") or config.get("runtime_guard_stats_start_time") or ""
    ).strip()
    if start_time:
        argv.extend(["--start-time", start_time])
    argv.extend(
        [
            "--max-app-loss-per-10k",
            str(max_loss),
            "--max-safe-maker-sell-gap-ticks",
            str(max_gap),
        ]
    )
    if min_bid_buffer > 0:
        argv.extend(["--min-bid-break-even-buffer-ticks", str(min_bid_buffer)])
    argv.extend(
        [
            "--min-maker-ratio",
            str(min_maker_ratio),
            "--min-gross-notional",
            str(min_gross_notional),
            "--require-gate",
        ]
    )
    return spot_app_loss_audit_main(argv)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="")
    args = parser.parse_args()

    original_cwd = os.getcwd()
    runner_work_dir = str(os.environ.get("GRID_RUNNER_WORK_DIR") or "").strip()
    if runner_work_dir:
        os.chdir(runner_work_dir)

    symbol = str(args.symbol or os.environ.get("GRID_RUNNER_SYMBOL") or DEFAULT_RUNNER_SYMBOL).upper().strip()
    if symbol and not os.environ.get("GRID_RUNNER_SERVICE_TEMPLATE"):
        os.environ["GRID_RUNNER_SERVICE_TEMPLATE"] = "grid-loop@{symbol}.service"
    pid_path = runner_pid_path_for_symbol(symbol) if symbol else RUNNER_PID_PATH
    pid_path = Path(pid_path).resolve()
    if _should_use_spot_runner(symbol):
        config = _load_spot_runner_control_config(symbol)
        try:
            _runner_start_safety_preflight(config, spot=True)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2) from exc
        gate_code = _run_spot_app_loss_prestart_gate(config)
        if gate_code != 0:
            raise SystemExit(gate_code)
        command_builder = _build_spot_runner_command
    else:
        # A restart must honor the persisted control, not the arguments from
        # the process being replaced.  Otherwise a stale one-way command can
        # overwrite a corrected hedge-mode control during bootstrap.
        try:
            # The systemd instance is the authority for its symbol.  A stale
            # cross-symbol document is rebound before its immutable run owner
            # is verified and persisted.
            config = _load_and_bind_futures_run_config(symbol)
        except ValueError as exc:
            print(f"{symbol} 运行终止契约无效：{exc}", file=sys.stderr)
            raise SystemExit(2) from exc
        command_builder = _build_runner_command
    command = command_builder(config)
    command = _anchor_relative_runtime_paths(command, runner_work_dir)
    _write_pid(pid_path)
    atexit.register(_cleanup_pid, pid_path)
    if runner_work_dir:
        os.chdir(original_cwd)
    exec_env = os.environ.copy()
    exec_env.setdefault("GRID_AUTO_RESET_ON_CONFIG_CHANGE", "1")
    os.execvpe(command[0], command, exec_env)


if __name__ == "__main__":
    main()
