from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


class RunningStatusTests(unittest.TestCase):
    @patch("grid_optimizer.running_status.fetch_spot_latest_price")
    def test_commission_usdt_converts_bnb_fee_assets(self, mock_price) -> None:
        from grid_optimizer.running_status import _commission_usdt

        mock_price.return_value = 600.0

        value = _commission_usdt(
            {
                "commission": 9.0,
                "commission_raw_by_asset": {
                    "USDT": 1.0,
                    "BNB": 0.25,
                },
            }
        )

        self.assertAlmostEqual(value, 151.0, places=8)
        mock_price.assert_called_once_with("BNBUSDT")

    def _build_local_payload(
        self,
        control_configs: list[dict[str, str]],
        *,
        focused_symbol: str | None = None,
    ) -> dict[str, object]:
        from grid_optimizer.running_status import build_running_status_local_payload

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

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.running_status.read_symbol_runner_process", side_effect=fake_runner):
                    return build_running_status_local_payload(symbol=focused_symbol)
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
        from grid_optimizer.running_status import build_running_status_local_payload

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "soonusdt_loop_runner_control.json").write_text(
                '{"symbol":"SOONUSDT","strategy_profile":"soon_profile","strategy_mode":"synthetic_neutral"}',
                encoding="utf-8",
            )

            def fake_runner(symbol: str) -> dict[str, object]:
                self.assertEqual(symbol, "SOONUSDT")
                return {
                    "configured": True,
                    "is_running": False,
                    "pid": None,
                    "config": {
                        "symbol": "SOONUSDT",
                        "strategy_profile": "soon_profile",
                        "strategy_mode": "synthetic_neutral",
                    },
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.running_status.read_symbol_runner_process", side_effect=fake_runner):
                    with patch(
                        "grid_optimizer.running_status.build_monitor_snapshot",
                        side_effect=AssertionError("running_status must not call build_monitor_snapshot"),
                        create=True,
                    ):
                        payload = build_running_status_local_payload()
            finally:
                os.chdir(previous_cwd)

        card = payload["groups"]["saved_idle"][0]
        self.assertEqual(card["current_position_display"], "--")
        self.assertIsNone(card["total_volume"])
        self.assertEqual(card["last_run_state"], "saved")

    def test_build_running_status_local_payload_reads_last_run_stats_for_saved_idle(self) -> None:
        from grid_optimizer.running_status import build_running_status_local_payload

        now = datetime.now(timezone.utc)
        older_trade_one_ms = int((now - timedelta(hours=3)).timestamp() * 1000)
        older_trade_two_ms = int((now - timedelta(hours=2)).timestamp() * 1000)
        recent_trade_ms = int((now - timedelta(minutes=20)).timestamp() * 1000)
        older_income_ms = int((now - timedelta(hours=2, minutes=30)).timestamp() * 1000)
        recent_income_ms = int((now - timedelta(minutes=10)).timestamp() * 1000)

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "ethusdc_loop_runner_control.json").write_text(
                '{"symbol":"ETHUSDC","strategy_profile":"eth_profile","strategy_mode":"synthetic_neutral"}',
                encoding="utf-8",
            )
            (output_dir / "ethusdc_loop_latest_plan.json").write_text(
                json.dumps(
                    {
                        "actual_net_qty": 1.25,
                        "current_long_qty": 1.25,
                        "current_short_qty": 0.0,
                        "unrealized_pnl": 0.0,
                        "strategy_mode": "synthetic_neutral",
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "ethusdc_loop_trade_audit.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "symbol": "ETHUSDC",
                                "id": 1,
                                "time": older_trade_one_ms,
                                "side": "BUY",
                                "price": "10",
                                "qty": "20",
                                "realizedPnl": "12.0",
                                "commission": "1.2",
                                "commissionAsset": "USDT",
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "ETHUSDC",
                                "id": 2,
                                "time": older_trade_two_ms,
                                "side": "SELL",
                                "price": "12",
                                "qty": "10",
                                "realizedPnl": "7.5",
                                "commission": "0.5",
                                "commissionAsset": "USDT",
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "ETHUSDC",
                                "id": 3,
                                "time": recent_trade_ms,
                                "side": "SELL",
                                "price": "8",
                                "qty": "10",
                                "realizedPnl": "-1.0",
                                "commission": "0.2",
                                "commissionAsset": "USDT",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (output_dir / "ethusdc_loop_income_audit.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "symbol": "ETHUSDC",
                                "tranId": 10,
                                "time": older_income_ms,
                                "incomeType": "FUNDING_FEE",
                                "income": "1.2",
                                "asset": "USDT",
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "ETHUSDC",
                                "tranId": 11,
                                "time": recent_income_ms,
                                "incomeType": "FUNDING_FEE",
                                "income": "-0.4",
                                "asset": "USDT",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_runner(symbol: str) -> dict[str, object]:
                self.assertEqual(symbol, "ETHUSDC")
                return {
                    "configured": True,
                    "is_running": False,
                    "pid": None,
                    "config": {
                        "symbol": "ETHUSDC",
                        "strategy_profile": "eth_profile",
                        "strategy_mode": "synthetic_neutral",
                    },
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.running_status.read_symbol_runner_process", side_effect=fake_runner):
                    payload = build_running_status_local_payload(symbol="ETHUSDC")
            finally:
                os.chdir(previous_cwd)

        card = payload["groups"]["saved_idle"][0]
        self.assertEqual(card["last_run_state"], "last_run")
        self.assertEqual(card["current_position_display"], "净 1.25 · 多 1.25 / 空 0")
        self.assertEqual(card["position_amt"], 1.25)
        self.assertEqual(card["long_qty"], 1.25)
        self.assertEqual(card["short_qty"], 0.0)
        self.assertEqual(card["unrealized_pnl"], 0.0)
        self.assertAlmostEqual(card["total_volume"], 400.0, places=8)
        self.assertAlmostEqual(card["recent_hour_volume"], 80.0, places=8)
        self.assertAlmostEqual(card["total_pnl"], 17.4, places=8)
        self.assertAlmostEqual(card["recent_hour_pnl"], -1.6, places=8)
        self.assertAlmostEqual(card["total_fees"], 1.9, places=8)

    def test_build_running_status_local_payload_filters_stats_to_last_run_session(self) -> None:
        from grid_optimizer.running_status import build_running_status_local_payload

        now = datetime.now(timezone.utc)
        old_run_start = now - timedelta(hours=6)
        old_trade_ms = int((old_run_start + timedelta(minutes=20)).timestamp() * 1000)
        old_income_ms = int((old_run_start + timedelta(minutes=25)).timestamp() * 1000)
        new_run_start = now - timedelta(minutes=45)
        new_trade_ms = int((new_run_start + timedelta(minutes=10)).timestamp() * 1000)
        new_income_ms = int((new_run_start + timedelta(minutes=15)).timestamp() * 1000)
        new_run_end = now - timedelta(minutes=5)

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "mixusdt_loop_runner_control.json").write_text(
                '{"symbol":"MIXUSDT","strategy_profile":"mix_profile","strategy_mode":"one_way_long"}',
                encoding="utf-8",
            )
            (output_dir / "mixusdt_loop_latest_plan.json").write_text(
                json.dumps(
                    {
                        "actual_net_qty": 2.0,
                        "current_long_qty": 2.0,
                        "current_short_qty": 0.0,
                        "unrealized_pnl": 3.0,
                        "strategy_mode": "one_way_long",
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "mixusdt_loop_latest_submit.json").write_text(
                json.dumps({"generated_at": new_run_end.isoformat(), "placed_orders": []}),
                encoding="utf-8",
            )
            (output_dir / "mixusdt_loop_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts": (old_run_start + timedelta(minutes=30)).isoformat(),
                                "run_start_time": old_run_start.isoformat(),
                                "run_end_time": (old_run_start + timedelta(hours=1)).isoformat(),
                            }
                        ),
                        json.dumps(
                            {
                                "ts": new_run_end.isoformat(),
                                "run_start_time": new_run_start.isoformat(),
                                "run_end_time": new_run_end.isoformat(),
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (output_dir / "mixusdt_loop_trade_audit.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "symbol": "MIXUSDT",
                                "id": 1,
                                "time": old_trade_ms,
                                "side": "BUY",
                                "price": "100",
                                "qty": "10",
                                "realizedPnl": "50.0",
                                "commission": "5.0",
                                "commissionAsset": "USDT",
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "MIXUSDT",
                                "id": 2,
                                "time": new_trade_ms,
                                "side": "BUY",
                                "price": "20",
                                "qty": "5",
                                "realizedPnl": "7.0",
                                "commission": "1.0",
                                "commissionAsset": "USDT",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (output_dir / "mixusdt_loop_income_audit.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "symbol": "MIXUSDT",
                                "tranId": 1,
                                "time": old_income_ms,
                                "incomeType": "FUNDING_FEE",
                                "income": "2.0",
                                "asset": "USDT",
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "MIXUSDT",
                                "tranId": 2,
                                "time": new_income_ms,
                                "incomeType": "FUNDING_FEE",
                                "income": "0.5",
                                "asset": "USDT",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_runner(symbol: str) -> dict[str, object]:
                self.assertEqual(symbol, "MIXUSDT")
                return {
                    "configured": True,
                    "is_running": False,
                    "pid": None,
                    "config": {
                        "symbol": "MIXUSDT",
                        "strategy_profile": "mix_profile",
                        "strategy_mode": "one_way_long",
                    },
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.running_status.read_symbol_runner_process", side_effect=fake_runner):
                    payload = build_running_status_local_payload(symbol="MIXUSDT")
            finally:
                os.chdir(previous_cwd)

        card = payload["groups"]["saved_idle"][0]
        self.assertEqual(card["last_run_state"], "last_run")
        self.assertAlmostEqual(card["total_volume"], 100.0, places=8)
        self.assertAlmostEqual(card["total_pnl"], 9.5, places=8)
        self.assertAlmostEqual(card["total_fees"], 1.0, places=8)

    def test_build_running_status_local_payload_prefers_effective_runtime_values_for_running_cards(self) -> None:
        from grid_optimizer.running_status import build_running_status_local_payload

        now = datetime.now(timezone.utc)

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "runusdt_loop_runner_control.json").write_text(
                json.dumps(
                    {
                        "symbol": "RUNUSDT",
                        "strategy_profile": "run_profile",
                        "strategy_mode": "synthetic_neutral",
                        "step_price": 10.0,
                        "buy_levels": 8,
                        "sell_levels": 8,
                        "base_position_notional": 1000.0,
                        "pause_buy_position_notional": 1200.0,
                        "pause_short_position_notional": 1300.0,
                        "max_position_notional": 1500.0,
                        "max_short_position_notional": 1600.0,
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "runusdt_loop_latest_plan.json").write_text(
                json.dumps(
                    {
                        "actual_net_qty": 0.5,
                        "current_long_qty": 1.5,
                        "current_short_qty": 1.0,
                        "unrealized_pnl": 4.0,
                        "strategy_mode": "synthetic_neutral",
                        "effective_buy_levels": 3,
                        "effective_sell_levels": 11,
                        "effective_base_position_notional": 700.0,
                        "effective_pause_buy_position_notional": 900.0,
                        "effective_pause_short_position_notional": 950.0,
                        "effective_max_position_notional": 1100.0,
                        "effective_max_short_position_notional": 1250.0,
                        "adaptive_step": {
                            "effective_step_price": 12.5,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "runusdt_loop_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts": now.isoformat(),
                        "run_start_time": (now - timedelta(minutes=30)).isoformat(),
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_runner(symbol: str) -> dict[str, object]:
                self.assertEqual(symbol, "RUNUSDT")
                return {
                    "configured": True,
                    "is_running": True,
                    "pid": 77,
                    "config": {
                        "symbol": "RUNUSDT",
                        "strategy_profile": "run_profile",
                        "strategy_mode": "synthetic_neutral",
                        "step_price": 10.0,
                        "buy_levels": 8,
                        "sell_levels": 8,
                        "base_position_notional": 1000.0,
                        "pause_buy_position_notional": 1200.0,
                        "pause_short_position_notional": 1300.0,
                        "max_position_notional": 1500.0,
                        "max_short_position_notional": 1600.0,
                    },
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.running_status.read_symbol_runner_process", side_effect=fake_runner):
                    payload = build_running_status_local_payload(symbol="RUNUSDT")
            finally:
                os.chdir(previous_cwd)

        card = payload["groups"]["running"][0]
        self.assertEqual(card["last_run_state"], "running")
        self.assertEqual(card["step_price"], 12.5)
        self.assertEqual(card["buy_levels"], 3)
        self.assertEqual(card["sell_levels"], 11)
        self.assertEqual(card["base_position_notional"], 700.0)
        self.assertEqual(card["pause_buy_position_notional"], 900.0)
        self.assertEqual(card["pause_short_position_notional"], 950.0)
        self.assertEqual(card["max_position_notional"], 1100.0)
        self.assertEqual(card["max_short_position_notional"], 1250.0)

    def test_build_running_status_local_payload_deduplicates_overlapping_open_orders(self) -> None:
        from grid_optimizer.running_status import build_running_status_local_payload

        now = datetime.now(timezone.utc)

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "ordusdt_loop_runner_control.json").write_text(
                '{"symbol":"ORDUSDT","strategy_profile":"ord_profile","strategy_mode":"one_way_long"}',
                encoding="utf-8",
            )
            (output_dir / "ordusdt_loop_latest_plan.json").write_text(
                json.dumps(
                    {
                        "actual_net_qty": 1.0,
                        "current_long_qty": 1.0,
                        "current_short_qty": 0.0,
                        "strategy_mode": "one_way_long",
                        "kept_orders": [
                            {"orderId": 101, "clientOrderId": "keep-101", "side": "BUY", "price": "10", "origQty": "1"},
                            {"clientOrderId": "client-only", "side": "SELL", "price": "12", "origQty": "1"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "ordusdt_loop_latest_submit.json").write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "placed_orders": [
                            {"orderId": 101, "clientOrderId": "keep-101", "side": "BUY", "price": "10", "origQty": "1"},
                            {"clientOrderId": "client-only", "side": "SELL", "price": "12", "origQty": "1"},
                            {"orderId": 202, "clientOrderId": "new-202", "side": "BUY", "price": "9", "origQty": "2"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "ordusdt_loop_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts": now.isoformat(),
                        "run_start_time": (now - timedelta(minutes=15)).isoformat(),
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_runner(symbol: str) -> dict[str, object]:
                self.assertEqual(symbol, "ORDUSDT")
                return {
                    "configured": True,
                    "is_running": False,
                    "pid": None,
                    "config": {
                        "symbol": "ORDUSDT",
                        "strategy_profile": "ord_profile",
                        "strategy_mode": "one_way_long",
                    },
                }

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch("grid_optimizer.running_status.read_symbol_runner_process", side_effect=fake_runner):
                    payload = build_running_status_local_payload(symbol="ORDUSDT")
            finally:
                os.chdir(previous_cwd)

        card = payload["groups"]["saved_idle"][0]
        self.assertEqual(card["open_order_count"], 3)

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

        with patch("grid_optimizer.running_status.load_console_registry", return_value=registry):
            with patch("grid_optimizer.running_status.local_running_status_server_id", return_value="srv_114"):
                with patch("grid_optimizer.running_status.build_running_status_local_payload", side_effect=fake_local):
                    with patch("grid_optimizer.running_status.fetch_remote_running_status_payload", side_effect=fake_remote):
                        payload = _build_running_status_cross_payload()

        self.assertEqual(payload["scope"], "cross")
        self.assertEqual(payload["view_mode"], "cross")
        self.assertEqual(payload["summary"]["server_count"], 2)
        self.assertEqual(payload["summary"]["running_symbol_count"], 2)
        self.assertEqual(payload["summary"]["saved_idle_symbol_count"], 2)
        self.assertEqual([item["server_id"] for item in payload["servers"]], ["srv_114", "srv_111"])
        self.assertEqual(payload["servers"][0]["server_base_url"], "")
        self.assertEqual(payload["servers"][0]["groups"]["running"][0]["server_label"], "114")
        self.assertEqual(payload["servers"][1]["groups"]["running"][0]["server_label"], "111")

    def test_build_running_status_cross_payload_summarizes_running_cards_only(self) -> None:
        from grid_optimizer.running_status import build_running_status_cross_payload

        registry = {
            "servers": [
                {"id": "srv_114", "label": "114", "base_url": "http://114", "enabled": True, "capabilities": []},
                {"id": "srv_111", "label": "111", "base_url": "http://111", "enabled": True, "capabilities": []},
            ]
        }

        def fake_local(**_: object) -> dict[str, object]:
            return {
                "ok": True,
                "view_mode": "local",
                "server_id": "srv_114",
                "server_label": "114",
                "server_base_url": "",
                "groups": {
                    "running": [
                        {
                            "symbol": "SOONUSDT",
                            "total_volume": 120.0,
                            "recent_hour_volume": 20.0,
                            "total_pnl": 7.0,
                            "total_fees": 1.5,
                        }
                    ],
                    "saved_idle": [
                        {
                            "symbol": "BTCUSDC",
                            "total_volume": 900.0,
                            "recent_hour_volume": 0.0,
                            "total_pnl": 80.0,
                            "total_fees": 2.5,
                        }
                    ],
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
                    "running": [
                        {
                            "symbol": "CHIPUSDT",
                            "total_volume": 50.0,
                            "recent_hour_volume": 5.0,
                            "total_pnl": -2.0,
                            "total_fees": 0.4,
                        }
                    ],
                    "saved_idle": [],
                },
                "summary": {"running_symbol_count": 1, "saved_idle_symbol_count": 0},
            }

        with patch("grid_optimizer.running_status.load_console_registry", return_value=registry):
            with patch("grid_optimizer.running_status.local_running_status_server_id", return_value="srv_114"):
                with patch("grid_optimizer.running_status.build_running_status_local_payload", side_effect=fake_local):
                    with patch("grid_optimizer.running_status.fetch_remote_running_status_payload", side_effect=fake_remote):
                        payload = build_running_status_cross_payload()

        self.assertEqual(payload["summary"]["running_symbol_count"], 2)
        self.assertEqual(payload["summary"]["saved_idle_symbol_count"], 1)
        self.assertEqual(payload["summary"]["running_total_volume"], 170.0)
        self.assertEqual(payload["summary"]["recent_hour_volume"], 25.0)
        self.assertEqual(payload["summary"]["total_pnl"], 5.0)
        self.assertEqual(payload["summary"]["total_fees"], 1.9)
        self.assertEqual(payload["servers"][0]["groups"]["running"][0]["server_id"], "srv_114")
        self.assertEqual(payload["servers"][1]["groups"]["running"][0]["server_id"], "srv_111")

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
