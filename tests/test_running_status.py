from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


class RunningStatusTests(unittest.TestCase):
    def _build_local_payload(
        self,
        control_configs: list[dict[str, str]],
        *,
        focused_symbol: str | None = None,
    ) -> dict[str, object]:
        from grid_optimizer.web import _build_running_status_local_payload

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()

            profiles_by_symbol: dict[str, str] = {}
            for config in control_configs:
                symbol = str(config["symbol"]).upper().strip()
                profile = str(config["strategy_profile"]).strip()
                profiles_by_symbol[symbol] = profile
                (output_dir / f"{symbol.lower()}_loop_runner_control.json").write_text(
                    (
                        "{"
                        f'"symbol":"{symbol}",'
                        f'"strategy_profile":"{profile}",'
                        '"strategy_mode":"synthetic_neutral"'
                        "}"
                    ),
                    encoding="utf-8",
                )

            def fake_runner(symbol: str) -> dict[str, object]:
                normalized = str(symbol).upper().strip()
                return {
                    "configured": True,
                    "is_running": normalized == "SOONUSDT",
                    "pid": 11 if normalized == "SOONUSDT" else None,
                    "config": {
                        "symbol": normalized,
                        "strategy_profile": profiles_by_symbol[normalized],
                        "strategy_mode": "synthetic_neutral",
                    },
                }

            def fake_snapshot(*, symbol: str, **_: object) -> dict[str, object]:
                normalized = str(symbol).upper().strip()
                return {
                    "ok": True,
                    "symbol": normalized,
                    "runner": fake_runner(normalized),
                    "risk_controls": {"strategy_mode": "synthetic_neutral"},
                    "market": {"mid_price": 1.0},
                    "position": {
                        "position_amt": 0.0,
                        "long_qty": 0.0,
                        "short_qty": 0.0,
                        "unrealized_pnl": 0.0,
                    },
                    "open_orders": [],
                    "trade_summary": {
                        "gross_notional": 0.0,
                        "realized_pnl": 0.0,
                        "net_pnl_estimate": 0.0,
                        "commission": 0.0,
                    },
                    "income_summary": {"funding_fee": 0.0},
                    "warnings": [],
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.web._read_runner_process_for_symbol", side_effect=fake_runner):
                    with patch("grid_optimizer.web.build_monitor_snapshot", side_effect=fake_snapshot):
                        return _build_running_status_local_payload(symbol=focused_symbol)
            finally:
                os.chdir(previous_cwd)

    def test_build_running_status_local_payload_groups_running_and_saved_idle(self) -> None:
        payload = self._build_local_payload(
            [
                {"symbol": "SOONUSDT", "strategy_profile": "soon_profile"},
                {"symbol": "CHIPUSDT", "strategy_profile": "chip_profile"},
            ]
        )

        self.assertEqual([item["symbol"] for item in payload["groups"]["running"]], ["SOONUSDT"])
        self.assertEqual([item["symbol"] for item in payload["groups"]["saved_idle"]], ["CHIPUSDT"])

    def test_build_running_status_local_payload_marks_focused_symbol(self) -> None:
        payload = self._build_local_payload(
            [{"symbol": "CHIPUSDT", "strategy_profile": "chip_profile"}],
            focused_symbol="CHIPUSDT",
        )

        self.assertEqual(payload["view_mode"], "local")
        self.assertEqual(payload["focused_symbol"], "CHIPUSDT")
        self.assertEqual(payload["groups"]["running"], [])
        self.assertEqual(payload["groups"]["saved_idle"][0]["symbol"], "CHIPUSDT")
        self.assertTrue(payload["groups"]["saved_idle"][0]["focused"])
        self.assertEqual(payload["groups"]["saved_idle"][0]["target_url"], "/running_status?symbol=CHIPUSDT")

    def test_build_running_status_local_payload_avoids_full_monitor_snapshot(self) -> None:
        from grid_optimizer.web import _build_running_status_local_payload

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "soonusdt_loop_runner_control.json").write_text(
                '{"symbol":"SOONUSDT","strategy_profile":"soon_profile","strategy_mode":"synthetic_neutral"}',
                encoding="utf-8",
            )
            (output_dir / "chipusdt_loop_runner_control.json").write_text(
                '{"symbol":"CHIPUSDT","strategy_profile":"chip_profile","strategy_mode":"synthetic_neutral"}',
                encoding="utf-8",
            )

            def fake_runner(symbol: str) -> dict[str, object]:
                normalized = str(symbol).upper().strip()
                return {
                    "configured": True,
                    "is_running": normalized == "SOONUSDT",
                    "pid": 11 if normalized == "SOONUSDT" else None,
                    "config": {
                        "symbol": normalized,
                        "strategy_profile": "soon_profile" if normalized == "SOONUSDT" else "chip_profile",
                        "strategy_mode": "synthetic_neutral",
                    },
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.web._read_runner_process_for_symbol", side_effect=fake_runner):
                    with patch(
                        "grid_optimizer.web.build_monitor_snapshot",
                        side_effect=AssertionError("should not build full monitor snapshot"),
                    ):
                        with patch(
                            "grid_optimizer.web._build_fast_running_status_item",
                            return_value={"recent_hour_volume": 12.0, "open_order_count": 2, "total_pnl": 3.5},
                        ):
                            payload = _build_running_status_local_payload()
            finally:
                os.chdir(previous_cwd)

        self.assertEqual([item["symbol"] for item in payload["groups"]["running"]], ["SOONUSDT"])
        self.assertEqual([item["symbol"] for item in payload["groups"]["saved_idle"]], ["CHIPUSDT"])
        self.assertEqual(payload["groups"]["running"][0]["recent_hour_volume"], 12.0)
        self.assertEqual(payload["groups"]["running"][0]["open_order_count"], 2)
        self.assertEqual(payload["groups"]["running"][0]["total_pnl"], 3.5)

    def test_build_running_status_cross_payload_aggregates_servers(self) -> None:
        from grid_optimizer.web import _build_running_status_cross_payload

        def fake_local(symbol: str | None = None, **_: object) -> dict[str, object]:
            self.assertIsNone(symbol)
            return {
                "ok": True,
                "view_mode": "local",
                "server_id": "srv_114",
                "server_label": "114",
                "server_base_url": "",
                "groups": {
                    "running": [{"symbol": "SOONUSDT", "target_url": "/running_status?symbol=SOONUSDT"}],
                    "saved_idle": [{"symbol": "BTCUSDC", "target_url": "/running_status?symbol=BTCUSDC"}],
                },
                "summary": {"running_symbol_count": 1, "saved_idle_symbol_count": 1},
            }

        def fake_remote(server: dict[str, object]) -> dict[str, object]:
            self.assertEqual(server["id"], "srv_111")
            return {
                "ok": True,
                "view_mode": "local",
                "server_id": "srv_111",
                "server_label": "111",
                "server_base_url": "http://111",
                "groups": {
                    "running": [{"symbol": "CHIPUSDT", "target_url": "http://111/running_status?symbol=CHIPUSDT"}],
                    "saved_idle": [{"symbol": "ETHUSDC", "target_url": "http://111/running_status?symbol=ETHUSDC"}],
                },
                "summary": {"running_symbol_count": 1, "saved_idle_symbol_count": 1},
            }

        registry = {
            "servers": [
                {"id": "srv_114", "label": "114", "base_url": "http://114", "enabled": True, "capabilities": []},
                {"id": "srv_111", "label": "111", "base_url": "http://111", "enabled": True, "capabilities": []},
            ],
            "accounts": [],
            "competition_source": {"server_id": "srv_114", "path": "/api/competition_board"},
        }

        with patch("grid_optimizer.web.load_console_registry", return_value=registry):
            with patch("grid_optimizer.web._local_running_status_server_id", return_value="srv_114"):
                with patch("grid_optimizer.web._build_running_status_local_payload", side_effect=fake_local):
                    with patch("grid_optimizer.web._fetch_remote_running_status_payload", side_effect=fake_remote):
                        payload = _build_running_status_cross_payload()

        self.assertEqual(payload["scope"], "cross")
        self.assertEqual(payload["view_mode"], "cross")
        self.assertEqual(payload["summary"]["server_count"], 2)
        self.assertEqual(payload["summary"]["running_symbol_count"], 2)
        self.assertEqual(payload["summary"]["saved_idle_symbol_count"], 2)
        self.assertEqual([item["server_id"] for item in payload["servers"]], ["srv_114", "srv_111"])
        self.assertEqual(payload["servers"][0]["server_base_url"], "")

    def test_run_running_status_query_uses_cross_scope(self) -> None:
        from grid_optimizer.web import _run_running_status_query

        with patch(
            "grid_optimizer.web._build_running_status_cross_payload",
            return_value={"ok": True, "scope": "cross", "view_mode": "cross"},
        ) as cross_mock:
            with patch("grid_optimizer.web._build_running_status_local_payload") as local_mock:
                payload = _run_running_status_query({"scope": ["cross"]})

        self.assertEqual(payload["scope"], "cross")
        cross_mock.assert_called_once_with()
        local_mock.assert_not_called()

    def test_run_running_status_query_uses_local_scope_for_symbol(self) -> None:
        from grid_optimizer.web import _run_running_status_query

        with patch("grid_optimizer.web._build_running_status_cross_payload") as cross_mock:
            with patch(
                "grid_optimizer.web._build_running_status_local_payload",
                return_value={"ok": True, "scope": "local", "view_mode": "local", "focused_symbol": "CHIPUSDT"},
            ) as local_mock:
                payload = _run_running_status_query({"symbol": ["chipusdt"]})

        self.assertEqual(payload["scope"], "local")
        self.assertEqual(payload["focused_symbol"], "CHIPUSDT")
        local_mock.assert_called_once_with(symbol="CHIPUSDT")
        cross_mock.assert_not_called()
