from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import grid_optimizer.loop_runner as loop_runner


class FrozenPairSubmissionManifestTests(TestCase):
    def _state(self, *, expires_at: str = "2099-01-01T00:00:00+00:00") -> dict:
        return {
            "center_price": 100.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "startup_pending": False,
            "best_quote_frozen_inventory": {
                "long_qty": 10.0,
                "short_qty": 10.0,
                "long_lots": [{"qty": 10.0, "entry_price": 99.0}],
                "short_lots": [{"qty": 10.0, "entry_price": 101.0}],
            },
            "best_quote_frozen_inventory_pair_release": {
                "requested": True,
                "request_id": "pair-request-1",
                "requested_qty": 10.0,
                "requested_at": "2026-07-17T00:00:00+00:00",
                "expires_at": expires_at,
            },
        }

    def _pair_orders(self) -> list[dict]:
        common = {
            "qty": 10.0,
            "notional": 1000.0,
            "force_reduce_only": True,
            "execution_type": "maker",
            "time_in_force": "GTX",
            "post_only": True,
            "frozen_inventory_pair_release": True,
            "frozen_inventory_request_id": "pair-request-1",
            "frozen_inventory_authorization_validated": True,
        }
        return [
            {
                **common,
                "role": "frozen_inventory_pair_release_long",
                "side": "SELL",
                "position_side": "LONG",
                "price": 100.1,
            },
            {
                **common,
                "role": "frozen_inventory_pair_release_short",
                "side": "BUY",
                "position_side": "SHORT",
                "price": 99.9,
            },
        ]

    def _repair_manifest_state(self, *, prepared_at: datetime) -> dict:
        state = self._state()
        state["best_quote_frozen_inventory_pair_release"]["repair_side"] = "long"
        loop_runner._prepare_frozen_pair_submission_manifest(
            state=state,
            symbol="BCHUSDT",
            orders=[self._pair_orders()[0]],
            now=prepared_at,
        )
        return state

    def _execute_args(self, state_path: Path) -> Namespace:
        return Namespace(
            symbol="BCHUSDT",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            max_new_orders=20,
            max_total_notional=5000.0,
            cancel_stale=False,
            max_plan_age_seconds=30,
            max_mid_drift_steps=20.0,
            plan_json="output/bchusdt_pair_manifest_plan.json",
            apply=True,
            margin_type="KEEP",
            leverage=10,
            maker_retries=0,
            recv_window=5000,
            state_path=str(state_path),
            market_stream=SimpleNamespace(
                snapshot=lambda max_age_seconds: {
                    "bid_price": 99.9,
                    "ask_price": 100.1,
                    "mark_price": 100.0,
                    "funding_rate": 0.0001,
                }
            ),
        )

    def _plan_report(self, state_path: Path) -> dict:
        return {
            "symbol": "BCHUSDT",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "effective_strategy_profile": "bch_pair_manifest_test",
            "state_path": str(state_path),
            "mid_price": 100.0,
            "step_price": 0.1,
            "open_order_count": 0,
            "current_long_qty": 1.0,
            "current_short_qty": 1.0,
            "ordinary_long_qty": 1.0,
            "ordinary_short_qty": 1.0,
            "frozen_long_qty": 10.0,
            "frozen_short_qty": 10.0,
            "actual_net_qty": 0.0,
            "dual_side_position": True,
            "best_quote_maker_volume": {
                "reduce_freeze": {
                    "isolates_risk_metrics": True,
                    "frozen_long_qty": 10.0,
                    "frozen_short_qty": 10.0,
                }
            },
            "symbol_info": {
                "tick_size": 0.1,
                "step_size": 0.1,
                "min_qty": 0.1,
                "min_notional": 5.0,
            },
        }

    def _execute_patches(self, pair_orders: list[dict], post_side_effect):
        validation = {
            "ok": True,
            "errors": [],
            "actions": {
                "place_count": 2,
                "cancel_count": 0,
                "cancel_orders": [],
                "place_orders": pair_orders,
            },
        }
        return (
            patch("grid_optimizer.loop_runner.validate_plan_report", return_value=validation),
            patch("grid_optimizer.loop_runner.load_binance_api_credentials", return_value=("key", "secret")),
            patch("grid_optimizer.loop_runner.fetch_futures_position_mode", return_value={"dualSidePosition": True}),
            patch(
                "grid_optimizer.loop_runner.fetch_futures_account_info_v3",
                return_value={
                    "multiAssetsMargin": False,
                    "positions": [
                        {
                            "symbol": "BCHUSDT",
                            "positionSide": "LONG",
                            "positionAmt": "11",
                            "entryPrice": "99",
                        },
                        {
                            "symbol": "BCHUSDT",
                            "positionSide": "SHORT",
                            "positionAmt": "-11",
                            "entryPrice": "101",
                        },
                    ],
                },
            ),
            patch("grid_optimizer.loop_runner.fetch_futures_open_orders", return_value=[]),
            patch("grid_optimizer.loop_runner.post_futures_change_initial_leverage", return_value={"leverage": 10}),
            patch("grid_optimizer.loop_runner.post_futures_order", side_effect=post_side_effect),
            patch("grid_optimizer.loop_runner.update_synthetic_order_refs"),
            patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs"),
        )

    def test_pair_manifest_is_durable_and_request_bound_before_first_post(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._state()), encoding="utf-8")
            seen_client_ids: list[str] = []

            def post_order(**kwargs):
                persisted = json.loads(state_path.read_text(encoding="utf-8"))
                directive = persisted["best_quote_frozen_inventory_pair_release"]
                self.assertIn("submission_manifest", directive)
                manifest = directive["submission_manifest"]
                self.assertEqual(manifest["request_id"], "pair-request-1")
                self.assertEqual(set(manifest["legs"]), {"long", "short"})
                manifest_client_ids = {
                    manifest["legs"]["long"]["client_order_id"],
                    manifest["legs"]["short"]["client_order_id"],
                }
                self.assertEqual(len(manifest_client_ids), 2)
                self.assertIn(kwargs["new_client_order_id"], manifest_client_ids)
                seen_client_ids.append(kwargs["new_client_order_id"])
                return {
                    "orderId": len(seen_client_ids),
                    "clientOrderId": kwargs["new_client_order_id"],
                }

            patches = self._execute_patches(self._pair_orders(), post_order)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
                report = loop_runner.execute_plan_report(
                    self._execute_args(state_path),
                    self._plan_report(state_path),
                )

            self.assertTrue(report["executed"])
            self.assertEqual(len(seen_client_ids), 2)

    def test_frozen_open_order_drift_does_not_block_ordinary_submit(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state = self._state()
            manifest = loop_runner._prepare_frozen_pair_submission_manifest(
                state=state,
                symbol="BCHUSDT",
                orders=self._pair_orders(),
            )
            state_path.write_text(json.dumps(state), encoding="utf-8")
            frozen_leg = manifest["legs"]["long"]
            current_open_orders = [
                {
                    "symbol": "BCHUSDT",
                    "orderId": 41,
                    "clientOrderId": frozen_leg["client_order_id"],
                    "side": "SELL",
                    "positionSide": "LONG",
                    "status": "NEW",
                    "origQty": "10",
                    "executedQty": "0",
                    "price": "100.1",
                }
            ]
            ordinary_order = {
                "role": "best_quote_entry_long",
                "side": "BUY",
                "position_side": "LONG",
                "qty": 0.1,
                "notional": 9.99,
                "price": 99.9,
                "execution_type": "maker",
                "time_in_force": "GTX",
                "post_only": True,
            }
            validation = {
                "ok": True,
                "errors": [],
                "actions": {
                    "place_count": 1,
                    "cancel_count": 0,
                    "cancel_orders": [],
                    "place_orders": [ordinary_order],
                },
            }
            plan_report = self._plan_report(state_path)
            plan_report["open_order_count"] = 0
            plan_report["ordinary_open_order_count"] = 0
            plan_report["frozen_open_order_count"] = 0

            with (
                patch(
                    "grid_optimizer.loop_runner.validate_plan_report",
                    return_value=validation,
                ),
                patch(
                    "grid_optimizer.loop_runner.load_binance_api_credentials",
                    return_value=("key", "secret"),
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_position_mode",
                    return_value={"dualSidePosition": True},
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_account_info_v3",
                    return_value={
                        "multiAssetsMargin": False,
                        "positions": [
                            {
                                "symbol": "BCHUSDT",
                                "positionSide": "LONG",
                                "positionAmt": "11",
                                "entryPrice": "99",
                            },
                            {
                                "symbol": "BCHUSDT",
                                "positionSide": "SHORT",
                                "positionAmt": "-11",
                                "entryPrice": "101",
                            },
                        ],
                    },
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_open_orders",
                    return_value=current_open_orders,
                ),
                patch(
                    "grid_optimizer.loop_runner.post_futures_change_initial_leverage",
                    return_value={"leverage": 10},
                ),
                patch(
                    "grid_optimizer.loop_runner.post_futures_order",
                    return_value={
                        "orderId": 42,
                        "clientOrderId": "gx-bchu-bestquot-42",
                    },
                ) as post_order,
                patch("grid_optimizer.loop_runner.update_synthetic_order_refs"),
                patch("grid_optimizer.loop_runner._update_inventory_grid_order_refs"),
            ):
                report = loop_runner.execute_plan_report(
                    self._execute_args(state_path),
                    plan_report,
                )

            self.assertTrue(report["executed"])
            self.assertEqual(report["ordinary_open_order_count"], 0)
            self.assertEqual(report["frozen_open_order_count"], 1)
            post_order.assert_called_once()

    def test_order_ref_update_does_not_overwrite_concurrent_pair_leg_receipt(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state = self._state()
            loop_runner._prepare_frozen_pair_submission_manifest(
                state=state,
                symbol="BCHUSDT",
                orders=self._pair_orders(),
            )
            state_path.write_text(json.dumps(state), encoding="utf-8")
            original_read_text = Path.read_text
            original_write_text = Path.write_text
            injected = False

            def read_then_record_receipt(path: Path, *args, **kwargs) -> str:
                nonlocal injected
                snapshot = original_read_text(path, *args, **kwargs)
                if not injected and path == state_path:
                    injected = True
                    concurrent_state = json.loads(snapshot)
                    long_leg = concurrent_state[
                        "best_quote_frozen_inventory_pair_release"
                    ]["submission_manifest"]["legs"]["long"]
                    long_leg.update(
                        {
                            "status": "accepted",
                            "submit_state": "accepted",
                            "order_id": "11",
                            "executed_qty": 0.0,
                        }
                    )
                    original_write_text(
                        path,
                        json.dumps(concurrent_state),
                        encoding="utf-8",
                    )
                return snapshot

            with patch.object(
                Path,
                "read_text",
                autospec=True,
                side_effect=read_then_record_receipt,
            ):
                loop_runner.update_best_quote_volume_order_refs(
                    state_path=state_path,
                    strategy_mode="hedge_best_quote_maker_volume_v1",
                    submit_report={
                        "placed_orders": [
                            {
                                "request": {
                                    "role": "best_quote_entry_long",
                                    "side": "BUY",
                                    "position_side": "LONG",
                                },
                                "response": {
                                    "orderId": 21,
                                    "clientOrderId": "gx-bchu-entry-21",
                                },
                            }
                        ]
                    },
                )

            persisted = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertIn("21", persisted["best_quote_volume_order_refs"])
        long_leg = persisted["best_quote_frozen_inventory_pair_release"][
            "submission_manifest"
        ]["legs"]["long"]
        self.assertEqual(long_leg["status"], "accepted")
        self.assertEqual(long_leg["order_id"], "11")

    def test_first_leg_receipt_survives_second_leg_failure_and_restart_reconcile(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps(self._state()), encoding="utf-8")
            calls = 0

            def post_order(**kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return {
                        "orderId": 11,
                        "clientOrderId": kwargs["new_client_order_id"],
                    }
                raise RuntimeError("second pair leg transport failure")

            patches = self._execute_patches(self._pair_orders(), post_order)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "second pair leg transport failure",
                ):
                    loop_runner.execute_plan_report(
                        self._execute_args(state_path),
                        self._plan_report(state_path),
                    )

            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            directive = persisted["best_quote_frozen_inventory_pair_release"]
            manifest = directive["submission_manifest"]
            long_leg = manifest["legs"]["long"]
            self.assertEqual(long_leg["order_id"], "11")
            self.assertEqual(long_leg["status"], "accepted")
            self.assertIn(
                manifest["legs"]["short"]["status"],
                {"submitting", "unknown", "rejected"},
            )

            reconcile = getattr(
                loop_runner,
                "reconcile_best_quote_frozen_pair_release",
                None,
            )
            self.assertTrue(callable(reconcile))
            reconcile(
                state=persisted,
                symbol="BCHUSDT",
                api_key="key",
                api_secret="secret",
                current_open_orders=[
                    {
                        "orderId": 11,
                        "clientOrderId": long_leg["client_order_id"],
                        "side": "SELL",
                        "positionSide": "LONG",
                        "status": "NEW",
                        "origQty": "10",
                        "executedQty": "0",
                    }
                ],
                observed_trade_rows=[],
                recv_window=5000,
            )
            self.assertEqual(
                persisted["best_quote_volume_order_refs"]["11"][
                    "frozen_inventory_request_id"
                ],
                "pair-request-1",
            )

    def test_empty_pair_fill_sync_keeps_awaiting_for_nonterminal_manifest(self) -> None:
        state = self._state()
        directive = state["best_quote_frozen_inventory_pair_release"]
        directive["awaiting_fill_confirmation"] = True
        directive["submission_manifest"] = {
            "request_id": "pair-request-1",
            "phase": "awaiting_reconcile",
            "legs": {
                "long": {
                    "client_order_id": "gx-bchu-frp-request1-l-1",
                    "status": "accepted",
                    "order_id": "11",
                },
                "short": {
                    "client_order_id": "gx-bchu-frp-request1-s-1",
                    "status": "accepted",
                    "order_id": "12",
                },
            },
        }

        loop_runner.sync_best_quote_frozen_pair_release(
            state=state,
            observed_trade_rows=[],
        )

        self.assertTrue(
            state["best_quote_frozen_inventory_pair_release"][
                "awaiting_fill_confirmation"
            ]
        )

    def test_late_user_trade_does_not_double_count_archived_manifest_execution(self) -> None:
        state = self._state()
        manifest = loop_runner._prepare_frozen_pair_submission_manifest(
            state=state,
            symbol="BCHUSDT",
            orders=self._pair_orders(),
        )
        long_leg = manifest["legs"]["long"]
        loop_runner._record_frozen_pair_leg_transition(
            state=state,
            request_id="pair-request-1",
            role="frozen_inventory_pair_release_long",
            status="partial",
            response={
                "orderId": 11,
                "clientOrderId": long_leg["client_order_id"],
                "executedQty": "5",
            },
        )
        loop_runner._account_frozen_pair_manifest_execution(
            state=state,
            request_id="pair-request-1",
            role="frozen_inventory_pair_release_long",
            executed_qty=5.0,
        )
        directive = state["best_quote_frozen_inventory_pair_release"]
        directive["submission_manifest"] = {
            "version": 1,
            "request_id": "pair-request-1",
            "submission_seq": 2,
            "kind": "repair",
            "phase": "awaiting_reconcile",
            "legs": {
                "short": {
                    "role": "frozen_inventory_pair_release_short",
                    "client_order_id": "gx-bchu-frps-2-repair",
                    "status": "prepared",
                }
            },
        }

        loop_runner.sync_best_quote_frozen_pair_release(
            state=state,
            observed_trade_rows=[
                {
                    "id": 1001,
                    "orderId": 11,
                    "side": "SELL",
                    "positionSide": "LONG",
                    "qty": "5",
                    "price": "100",
                    "time": 1_721_177_200_000,
                }
            ],
        )

        self.assertEqual(
            state["best_quote_frozen_inventory_pair_release"]["filled_long_qty"],
            5.0,
        )

    def test_expired_directive_keeps_nonterminal_submission_manifest(self) -> None:
        state = self._state(expires_at="2000-01-01T00:00:00+00:00")
        state["best_quote_frozen_inventory_pair_release"].update(
            {
                "awaiting_fill_confirmation": True,
                "submission_manifest": {
                    "request_id": "pair-request-1",
                    "phase": "awaiting_reconcile",
                    "legs": {
                        "long": {
                            "client_order_id": "gx-bchu-frp-request1-l-1",
                            "status": "accepted",
                            "order_id": "11",
                        },
                        "short": {
                            "client_order_id": "gx-bchu-frp-request1-s-1",
                            "status": "unknown",
                        },
                    },
                },
            }
        )

        with TemporaryDirectory() as tmpdir:
            args = loop_runner._build_parser().parse_args([])
            args.symbol = "BCHUSDT"
            args.strategy_mode = "hedge_best_quote_maker_volume_v1"
            args.strategy_profile = "bch_pair_manifest_test"
            args.state_path = str(Path(tmpdir) / "state.json")
            args.plan_json = str(Path(tmpdir) / "plan.json")
            args.reset_state = False
            market_guard = {
                "buy_pause_active": False,
                "buy_pause_reasons": [],
                "short_cover_pause_active": False,
                "short_cover_pause_reasons": [],
                "shift_frozen": False,
            }
            with (
                patch("grid_optimizer.loop_runner.load_or_initialize_state", return_value=state),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_symbol_config",
                    return_value={
                        "tick_size": 0.1,
                        "step_size": 0.1,
                        "min_qty": 0.1,
                        "min_notional": 5.0,
                    },
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_book_tickers",
                    return_value=[{"bid_price": "99.9", "ask_price": "100.1"}],
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_premium_index",
                    return_value=[{"funding_rate": "0.0001"}],
                ),
                patch(
                    "grid_optimizer.loop_runner.load_binance_api_credentials",
                    return_value=("key", "secret"),
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_position_mode",
                    return_value={"dualSidePosition": True},
                ),
                patch(
                    "grid_optimizer.loop_runner.fetch_futures_account_info_v3",
                    return_value={
                        "multiAssetsMargin": False,
                        "positions": [
                            {
                                "symbol": "BCHUSDT",
                                "positionSide": "LONG",
                                "positionAmt": "10",
                                "entryPrice": "99",
                            },
                            {
                                "symbol": "BCHUSDT",
                                "positionSide": "SHORT",
                                "positionAmt": "-10",
                                "entryPrice": "101",
                            },
                        ],
                    },
                ),
                patch("grid_optimizer.loop_runner.fetch_futures_open_orders", return_value=[]),
                patch("grid_optimizer.loop_runner.assess_market_guard", return_value=market_guard),
            ):
                loop_runner.generate_plan_report(args)

        directive = state.get("best_quote_frozen_inventory_pair_release")
        self.assertIsInstance(directive, dict)
        self.assertEqual(
            directive["submission_manifest"]["phase"],
            "awaiting_reconcile",
        )

    def test_not_found_proofs_require_quiet_window_and_fresh_open_order_observation(
        self,
    ) -> None:
        prepared_at = datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)
        state = self._repair_manifest_state(prepared_at=prepared_at)
        error = RuntimeError("Binance API error -2013: Order does not exist")

        with patch(
            "grid_optimizer.loop_runner.fetch_futures_order",
            side_effect=error,
        ):
            loop_runner.reconcile_best_quote_frozen_pair_release(
                state=state,
                symbol="BCHUSDT",
                api_key="key",
                api_secret="secret",
                current_open_orders=[],
                observed_trade_rows=[],
                recv_window=5000,
                now=prepared_at + timedelta(seconds=20),
            )
            loop_runner.reconcile_best_quote_frozen_pair_release(
                state=state,
                symbol="BCHUSDT",
                api_key="key",
                api_secret="secret",
                current_open_orders=[],
                observed_trade_rows=[],
                recv_window=5000,
                now=prepared_at + timedelta(seconds=21),
            )

            leg = state["best_quote_frozen_inventory_pair_release"][
                "submission_manifest"
            ]["legs"]["long"]
            self.assertNotEqual(leg["status"], "not_found")
            self.assertEqual(len(leg["not_found_proofs"]), 1)

            loop_runner.reconcile_best_quote_frozen_pair_release(
                state=state,
                symbol="BCHUSDT",
                api_key="key",
                api_secret="secret",
                current_open_orders=[],
                observed_trade_rows=[],
                recv_window=5000,
                now=prepared_at + timedelta(seconds=30),
            )

        leg = state["best_quote_frozen_inventory_pair_release"][
            "submission_manifest"
        ]["legs"]["long"]
        self.assertEqual(leg["status"], "not_found")
        proofs = leg["not_found_proofs"]
        self.assertEqual(len(proofs), 2)
        for proof in proofs:
            self.assertIsInstance(proof, dict)
            self.assertEqual(proof["client_order_id"], leg["client_order_id"])
            self.assertTrue(proof["client_id_absent_from_open_orders"])
            self.assertIn("open_orders_observed_at", proof)
        first_observed_at = datetime.fromisoformat(
            proofs[0]["open_orders_observed_at"]
        )
        second_observed_at = datetime.fromisoformat(
            proofs[1]["open_orders_observed_at"]
        )
        self.assertGreaterEqual(
            (second_observed_at - first_observed_at).total_seconds(),
            10.0,
        )

    def test_malformed_manifest_fields_fail_closed_without_exchange_query(
        self,
    ) -> None:
        prepared_at = datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)

        def corrupt_version(manifest: dict) -> None:
            manifest["version"] = "not-an-int"

        def corrupt_submission_seq(manifest: dict) -> None:
            manifest["submission_seq"] = "not-an-int"

        def corrupt_timestamp(manifest: dict) -> None:
            manifest["legs"]["long"]["prepared_at"] = "not-a-timestamp"
            manifest["legs"]["long"]["updated_at"] = "not-a-timestamp"

        def corrupt_client_order_id(manifest: dict) -> None:
            manifest["legs"]["long"]["client_order_id"] = ""

        def empty_legs(manifest: dict) -> None:
            manifest["legs"] = {}

        def malformed_legs(manifest: dict) -> None:
            manifest["legs"] = ["not-a-mapping"]

        cases = (
            ("version", corrupt_version),
            ("submission_seq", corrupt_submission_seq),
            ("timestamp", corrupt_timestamp),
            ("client_order_id", corrupt_client_order_id),
            ("empty_legs", empty_legs),
            ("malformed_legs", malformed_legs),
        )
        for field, corrupt in cases:
            with self.subTest(field=field):
                state = self._repair_manifest_state(prepared_at=prepared_at)
                directive = state["best_quote_frozen_inventory_pair_release"]
                manifest = directive["submission_manifest"]
                corrupt(manifest)
                with patch(
                    "grid_optimizer.loop_runner.fetch_futures_order"
                ) as fetch_order:
                    try:
                        report = loop_runner.reconcile_best_quote_frozen_pair_release(
                            state=state,
                            symbol="BCHUSDT",
                            api_key="key",
                            api_secret="secret",
                            current_open_orders=[],
                            observed_trade_rows=[],
                            recv_window=5000,
                            now=prepared_at + timedelta(seconds=30),
                        )
                    except Exception as exc:  # pragma: no cover - red-test guard
                        self.fail(
                            f"malformed {field} must fail closed, not raise "
                            f"{type(exc).__name__}: {exc}"
                        )

                self.assertTrue(report.get("blocked"))
                self.assertTrue(report["errors"])
                fetch_order.assert_not_called()
                directive = state["best_quote_frozen_inventory_pair_release"]
                self.assertTrue(directive["awaiting_fill_confirmation"])
                self.assertTrue(
                    loop_runner._frozen_pair_manifest_has_unresolved_legs(
                        directive["submission_manifest"]
                    )
                )

    def test_recovered_manual_limit_only_exempts_expiry_not_other_validation(
        self,
    ) -> None:
        cases = (
            (
                "invalid_numeric",
                {
                    "best_quote_frozen_inventory": {
                        "long_qty": 5.0,
                        "long_manual_limit_isolated_qty": 5.0,
                        "long_manual_limit_price": 100.0,
                        "long_manual_limit_request_id": "req-long",
                    },
                    "best_quote_frozen_inventory_manual_limit": {
                        "long": {
                            "requested": True,
                            "requested_qty": True,
                            "price": 100.0,
                            "request_id": "req-long",
                            "expires_at": "2000-01-01T00:00:00+00:00",
                            "recovered_from_isolated_qty": True,
                        }
                    },
                },
                "invalid_requested_qty",
            ),
            (
                "ledger_isolation_exceeds_frozen_qty",
                {
                    "best_quote_frozen_inventory": {
                        "short_qty": 5.0,
                        "short_manual_limit_isolated_qty": 10.0,
                        "short_manual_limit_price": 100.0,
                        "short_manual_limit_request_id": "req-short",
                    }
                },
                "ledger",
            ),
            (
                "missing_request_id",
                {
                    "best_quote_frozen_inventory": {
                        "long_qty": 5.0,
                        "long_manual_limit_isolated_qty": 5.0,
                        "long_manual_limit_price": 100.0,
                    }
                },
                "request",
            ),
        )
        for case, state, expected_reason in cases:
            with self.subTest(case=case):
                plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}
                report = loop_runner.apply_best_quote_frozen_inventory_manual_limit(
                    plan=plan,
                    state=state,
                    bid_price=99.9,
                    ask_price=100.1,
                    tick_size=0.1,
                    step_size=0.1,
                    min_qty=0.1,
                    min_notional=5.0,
                    hedge_mode=True,
                )

                self.assertFalse(report["active"])
                self.assertEqual(plan["buy_orders"], [])
                self.assertEqual(plan["sell_orders"], [])
                self.assertTrue(
                    any(
                        expected_reason in reason
                        for reason in report["blocked_reasons"]
                    )
                )

    def test_recovered_manual_limit_does_not_reissue_existing_request_ref(
        self,
    ) -> None:
        state: dict[str, object] = {
            "best_quote_frozen_inventory": {
                "short_qty": 5.0,
                "short_manual_limit_isolated_qty": 5.0,
                "short_manual_limit_price": 100.0,
                "short_manual_limit_request_id": "req-short",
            },
            "best_quote_volume_order_refs": {
                "123": {
                    "book": "frozen_bq",
                    "role": "frozen_inventory_manual_limit_short",
                    "side": "BUY",
                    "position_side": "SHORT",
                    "client_order_id": "gx-bchu-fml-short-123",
                    "frozen_inventory_request_id": "req-short",
                }
            },
        }
        plan: dict[str, object] = {"buy_orders": [], "sell_orders": []}

        report = loop_runner.apply_best_quote_frozen_inventory_manual_limit(
            plan=plan,
            state=state,
            bid_price=99.9,
            ask_price=100.1,
            tick_size=0.1,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            hedge_mode=True,
        )

        self.assertFalse(report["active"])
        self.assertEqual(plan["buy_orders"], [])
        self.assertEqual(plan["sell_orders"], [])
        self.assertTrue(
            any("reconcile" in reason for reason in report["blocked_reasons"])
        )
