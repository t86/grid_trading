from __future__ import annotations

import io
import json
from unittest.mock import Mock, patch

import pytest

from grid_optimizer.web import _Handler


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/api/runner/stop",
            {
                "symbol": "BCHUSDT",
                "cancel_open_orders": True,
                "close_all_positions": True,
            },
        ),
        ("/api/runner/quick_flatten", {"symbol": "BCHUSDT"}),
        (
            "/api/runner/frozen_inventory",
            {"symbol": "BCHUSDT", "action": "reduce_long"},
        ),
    ],
)
@patch("grid_optimizer.web._update_runner_frozen_inventory")
@patch("grid_optimizer.web._quick_flatten_runner_symbol")
@patch("grid_optimizer.web._stop_runner_process")
@patch("grid_optimizer.web._read_json_dict")
def test_registered_recovery_routes_fail_before_any_mutation(
    read_json,
    stop_runner,
    quick_flatten,
    update_frozen,
    path: str,
    payload: dict[str, object],
) -> None:
    read_json.return_value = {
        "symbol": "BCHUSDT",
        "_futures_recovery_state": {"schema_version": 1},
    }
    stop_runner.return_value = {"symbol": "BCHUSDT"}
    quick_flatten.return_value = {"symbol": "BCHUSDT"}
    update_frozen.return_value = {"symbol": "BCHUSDT"}
    raw = json.dumps(payload).encode("utf-8")
    handler = object.__new__(_Handler)
    handler.path = path
    handler.headers = {"Content-Length": str(len(raw))}
    handler.rfile = io.BytesIO(raw)
    handler._authorize_request = lambda: True
    handler._send_json = Mock()

    _Handler.do_POST(handler)

    stop_runner.assert_not_called()
    quick_flatten.assert_not_called()
    update_frozen.assert_not_called()
    body = handler._send_json.call_args.args[0]
    assert handler._send_json.call_args.kwargs["status"] == 409
    assert body == {
        "ok": False,
        "error": (
            "recovery coordinator owns BCHUSDT; "
            f"{path.rsplit('/', 1)[-1]} must be requested through the coordinator"
        ),
        "reason": "recovery_coordinator_owns_symbol",
        "symbol": "BCHUSDT",
        "operation": path.rsplit("/", 1)[-1],
    }


@patch("grid_optimizer.web._stop_runner_process")
@patch("grid_optimizer.web._read_json_dict", return_value={"symbol": "ARXUSDT"})
def test_legacy_arx_without_recovery_envelope_keeps_stop_route(
    _read_json,
    stop_runner,
) -> None:
    stop_runner.return_value = {"symbol": "ARXUSDT", "post_stop_actions": {}}
    raw = b'{"symbol":"ARXUSDT"}'
    handler = object.__new__(_Handler)
    handler.path = "/api/runner/stop"
    handler.headers = {"Content-Length": str(len(raw))}
    handler.rfile = io.BytesIO(raw)
    handler._authorize_request = lambda: True
    handler._send_json = Mock()

    _Handler.do_POST(handler)

    stop_runner.assert_called_once_with(
        "ARXUSDT",
        cancel_open_orders=False,
        close_all_positions=False,
        clear_volatility_resume_state=True,
    )
    assert handler._send_json.call_args.kwargs["status"] == 200


@patch("grid_optimizer.web._stop_runner_process")
@patch("grid_optimizer.web._read_json_dict", return_value=None)
@patch("grid_optimizer.web._runner_control_path")
def test_unreadable_existing_control_blocks_stop_before_mutation(
    runner_control_path,
    _read_json,
    stop_runner,
) -> None:
    runner_control_path.return_value.exists.return_value = True
    raw = b'{"symbol":"BCHUSDT"}'
    handler = object.__new__(_Handler)
    handler.path = "/api/runner/stop"
    handler.headers = {"Content-Length": str(len(raw))}
    handler.rfile = io.BytesIO(raw)
    handler._authorize_request = lambda: True
    handler._send_json = Mock()

    _Handler.do_POST(handler)

    stop_runner.assert_not_called()
    assert handler._send_json.call_args.kwargs["status"] == 409
    assert (
        handler._send_json.call_args.args[0]["reason"]
        == "runner_control_unreadable"
    )
