"""Unit tests for the tracked, symbol-generic competition ops stack.

These cover the pure decision logic (no exchange calls): frozen take-profit
pricing, the balanced-hedge matched-close math, and the self-contained config
patch that replaces the server ``/tmp/apply_cfg.py`` shell-out.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import grid_optimizer.competition_health_monitor as hm
import grid_optimizer.competition_state_realign as ra
import grid_optimizer.competition_target_gate as tg
from grid_optimizer.futures_run_lifecycle import bind_run_contract_owner
from grid_optimizer.recovery_control_ownership import is_recovery_managed


def _write_state(workdir: Path, slug: str, short_lots: list[dict]) -> None:
    (workdir / "output").mkdir(parents=True, exist_ok=True)
    (workdir / "output" / f"{slug}_loop_state.json").write_text(
        json.dumps({"best_quote_frozen_inventory": {"short_lots": short_lots}}),
        encoding="utf-8",
    )


def test_frozen_short_lots_filters_zero_and_missing(tmp_path: Path) -> None:
    _write_state(tmp_path, "arxusdt", [
        {"qty": 100, "entry_price": 0.22},
        {"qty": 0, "entry_price": 0.21},        # zero qty dropped
        {"qty": 50, "entry_price": 0},           # zero price dropped
        {"qty": 40, "price": 0.23},              # falls back to `price`
    ])
    lots = tg._frozen_short_lots(str(tmp_path), "arxusdt")
    assert lots == [(100.0, 0.22), (40.0, 0.23)]


def test_frozen_tp_price_never_leaves_a_lot_underwater(tmp_path: Path, monkeypatch) -> None:
    # wavg = (100*0.30 + 100*0.20)/200 = 0.25; 5% below = 0.2375, but min_entry=0.20
    # must cap the fill so the cheapest-entry lot is not underwater -> price floored to 0.20.
    _write_state(tmp_path, "arxusdt", [
        {"qty": 100, "entry_price": 0.30},
        {"qty": 100, "entry_price": 0.20},
    ])
    captured: dict = {}

    def fake_post_order(**kw):
        captured.update(kw)
        return {"orderId": 555}

    monkeypatch.setattr(tg, "post_futures_order", fake_post_order)
    out = tg.place_frozen_short_tp("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", 0.05, 0.0001)
    assert out["placed"] is True
    assert out["qty"] == 200
    assert out["price"] == 0.20          # min_entry guard beats the 5% line
    assert captured["side"] == "BUY"
    assert captured["position_side"] == "SHORT"
    assert captured["price"] <= 0.20     # a fill here realizes >= 0 on every lot


def test_frozen_tp_uses_profit_line_when_below_min_entry(tmp_path: Path, monkeypatch) -> None:
    # Uniform entries: the 5% profit line (0.19) is the binding constraint, floored to tick.
    _write_state(tmp_path, "arxusdt", [{"qty": 100, "entry_price": 0.20}])
    monkeypatch.setattr(tg, "post_futures_order", lambda **kw: {"orderId": 1})
    out = tg.place_frozen_short_tp("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", 0.05, 0.0001)
    assert out["price"] == 0.19


def test_frozen_tp_no_lots_is_a_no_op(tmp_path: Path) -> None:
    (tmp_path / "output").mkdir(parents=True)
    out = tg.place_frozen_short_tp("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", 0.05, 0.0001)
    assert out == {"placed": False, "reason": "no_frozen_short_lots"}


def _plan(workdir: Path, slug: str, fl: float, fs: float) -> None:
    (workdir / "output").mkdir(parents=True, exist_ok=True)
    (workdir / "output" / f"{slug}_loop_latest_plan.json").write_text(
        json.dumps({"best_quote_maker_volume": {"reduce_freeze": {
            "frozen_long_qty": fl, "frozen_short_qty": fs}}}),
        encoding="utf-8",
    )


def _patch_positions(monkeypatch, long_amt, short_amt, long_entry, short_entry):
    def fake_pos(**kw):
        return [
            {"positionSide": "LONG", "positionAmt": long_amt, "entryPrice": long_entry},
            {"positionSide": "SHORT", "positionAmt": short_amt, "entryPrice": short_entry},
        ]

    monkeypatch.setattr(hm, "fetch_futures_position_risk_v3", fake_pos, raising=False)
    import grid_optimizer.data as data
    monkeypatch.setattr(data, "fetch_futures_position_risk_v3", fake_pos, raising=False)


def _patch_market_orders(monkeypatch) -> list:
    orders: list = []

    def rec(**kw):
        orders.append((kw["side"], kw["quantity"], kw["position_side"]))

    monkeypatch.setattr(hm, "post_futures_market_order", rec, raising=False)
    import grid_optimizer.data as data
    monkeypatch.setattr(data, "post_futures_market_order", rec, raising=False)
    return orders


def test_deadlock_is_detect_only_by_default(tmp_path: Path, monkeypatch) -> None:
    # A closable net-neutral hedge exists, but auto_close defaults to False: no market order,
    # just a blocked_by_config detection for alerting (policy: no automatic managed pair-reduce).
    _plan(tmp_path, "arxusdt", 20, 10)
    _patch_positions(monkeypatch, long_amt=120, short_amt=-100, long_entry=0.2200, short_entry=0.2201)
    orders = _patch_market_orders(monkeypatch)
    out = hm.deadlock_unstick("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", min_matched=20)
    assert out["matched"] == 90
    assert out["acted"] is False
    assert out["reason"] == "blocked_by_config"
    assert out["would_unstick"] is True
    assert orders == []


def test_deadlock_closes_only_matched_neutral_hedge_when_opted_in(tmp_path: Path, monkeypatch) -> None:
    # Balanced hedge opened near the same price: 120 long (20 frozen) + 100 short (10 frozen)
    # -> managed 100 long / 90 short -> matched 90, net-neutral; only acts with auto_close=True.
    _plan(tmp_path, "arxusdt", 20, 10)
    _patch_positions(monkeypatch, long_amt=120, short_amt=-100, long_entry=0.2200, short_entry=0.2201)
    orders = _patch_market_orders(monkeypatch)
    out = hm.deadlock_unstick("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", min_matched=20, auto_close=True)
    assert out["matched"] == 90
    assert out["acted"] is True
    assert ("SELL", 90, "LONG") in orders and ("BUY", 90, "SHORT") in orders


def test_deadlock_skips_spread_hedge_that_would_realize_a_loss(tmp_path: Path, monkeypatch) -> None:
    # Long built high (0.30), short low (0.20): closing the matched 90 realizes ~ -9 -> skip.
    _plan(tmp_path, "arxusdt", 0, 0)
    _patch_positions(monkeypatch, long_amt=90, short_amt=-90, long_entry=0.30, short_entry=0.20)
    fired: list = []
    monkeypatch.setattr(hm, "post_futures_market_order", lambda **kw: fired.append(kw), raising=False)
    out = hm.deadlock_unstick("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", min_matched=20)
    assert out["acted"] is False
    assert out["reason"] == "spread_hedge_would_realize_loss"
    assert fired == []


def test_deadlock_leaves_one_sided_position_untouched(tmp_path: Path, monkeypatch) -> None:
    # Pure long, no short -> matched 0 -> a legit one-sided loss-reduce is never auto-closed.
    _plan(tmp_path, "arxusdt", 0, 0)
    _patch_positions(monkeypatch, long_amt=150, short_amt=0, long_entry=0.22, short_entry=0.0)
    out = hm.deadlock_unstick("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", min_matched=20)
    assert out["matched"] == 0
    assert out["acted"] is False
    assert out["reason"] == "no_matched_hedge"


def test_apply_offset_patches_atomically_with_audit(tmp_path: Path) -> None:
    cfg = tmp_path / "arxusdt_loop_runner_control.json"
    cfg.write_text(json.dumps({"best_quote_maker_volume_quote_offset_ticks": 1, "step_price": 0.0005}),
                   encoding="utf-8")
    hm.apply_offset(str(cfg), "best_quote_maker_volume_quote_offset_ticks", 2, "health_governor_brake")
    patched = json.loads(cfg.read_text(encoding="utf-8"))
    assert patched["best_quote_maker_volume_quote_offset_ticks"] == 2
    assert patched["step_price"] == 0.0005                 # untouched keys preserved
    assert patched["updated_by"] == "health_governor_brake"
    assert "updated_at" in patched
    backups = list(tmp_path.glob("*.bak_health_governor_brake_*"))
    assert len(backups) == 1                               # timestamped backup kept
    assert json.loads(backups[0].read_text())["best_quote_maker_volume_quote_offset_ticks"] == 1


def test_get_offset_prefers_bq_key_then_falls_back(tmp_path: Path) -> None:
    a = tmp_path / "a.json"
    a.write_text(json.dumps({"best_quote_maker_volume_quote_offset_ticks": 3}))
    assert hm.get_offset(str(a)) == ("best_quote_maker_volume_quote_offset_ticks", 3)
    b = tmp_path / "b.json"
    b.write_text(json.dumps({"quote_offset_ticks": 4}))
    assert hm.get_offset(str(b)) == ("quote_offset_ticks", 4)


def test_recovery_managed_symbols_make_external_controllers_read_only() -> None:
    assert is_recovery_managed("ARXUSDT", {}) is True
    assert is_recovery_managed("OUSDT", {}) is True
    assert is_recovery_managed("REUSDT", {}) is False
    assert is_recovery_managed("REUSDT", {"recovery_control_owner": "bq_volume_recovery_guard"}) is True
    assert is_recovery_managed(
        "BCHUSDT",
        {"_futures_recovery_state": {"schema_version": 1}},
    ) is True
    assert is_recovery_managed(
        "BCHUSDT",
        {"_futures_recovery_state": None},
    ) is True
    assert is_recovery_managed(
        "BCHUSDT",
        {"_futures_recovery_state_mirror": {"schema_version": 1}},
    ) is True


def test_registered_bch_health_monitor_observes_without_control_or_restart(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    original = {
        "symbol": "BCHUSDT",
        "best_quote_maker_volume_quote_offset_ticks": 0,
        "_futures_recovery_state": None,
    }
    control_path.write_text(json.dumps(original), encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "hm",
            "--symbol",
            "BCHUSDT",
            "--service",
            "grid-loop@BCHUSDT.service",
            "--workdir",
            str(tmp_path),
            "--first",
            "3000",
            "--enforce",
        ],
    )
    monkeypatch.setattr(hm, "is_active", lambda _service: True)
    monkeypatch.setattr(hm, "journal", lambda _service, _minutes: "mid=500 placed=0")
    monkeypatch.setattr(
        hm,
        "daily_recent_wear",
        lambda *_args, **_kwargs: (5000.0, 1.8, 500.0, 5.0),
    )
    monkeypatch.setattr(
        hm,
        "get_offset",
        lambda _cfg: ("best_quote_maker_volume_quote_offset_ticks", 0),
    )
    forbidden: list[str] = []
    monkeypatch.setattr(
        hm,
        "restart",
        lambda *_args, **_kwargs: forbidden.append("restart") or {"ok": True},
    )
    monkeypatch.setattr(
        hm,
        "apply_offset",
        lambda *_args, **_kwargs: forbidden.append("apply_offset"),
    )
    monkeypatch.setattr(
        hm,
        "deadlock_unstick",
        lambda *_args, **_kwargs: forbidden.append("deadlock_unstick") or {},
    )

    hm.main()

    assert forbidden == []
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["action"] == "observe_only_recovery_managed_symbol"
    assert json.loads(control_path.read_text(encoding="utf-8")) == original


def test_placed_sum_parses_journal_lines() -> None:
    assert hm.placed_sum("foo placed=2 bar\nbaz placed=0\nqux placed=5") == 7
    assert hm.placed_sum("no counters here") == 0


# --- target_gate main() production-safety guards (fixes 2/3/4) ---

class _FakeProc:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _fake_subprocess(calls: list, active: str = "active"):
    def run(cmd, **kw):
        calls.append(cmd)
        return _FakeProc(stdout=active if "is-active" in cmd else "")
    return run


def _run_gate_main(monkeypatch, argv: list[str]) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setattr("sys.argv", ["competition_target_gate", *argv])
    tg.main()


def _target_window_stats(vol: float, wear: float) -> dict[str, object]:
    return {
        "gross_notional": vol,
        "realized_pnl": -(wear * vol / 10_000.0),
        "wear_per_10k": wear,
        "trade_count": 1 if vol > 0 else 0,
        "window_start": "2026-07-16T00:00:00+00:00",
        "window_end": "2026-07-17T00:00:00+00:00",
        "query_end": "2026-07-16T01:00:00+00:00",
    }


def _owned_contract(control: dict[str, object]) -> dict[str, object]:
    owned, _ = bind_run_contract_owner(
        control,
        activated_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    return owned


def _control(
    workdir: Path,
    slug: str,
    max_cum: float,
    *,
    wear_stop: float | None = None,
    wear_first: float | None = None,
) -> dict[str, object]:
    (workdir / "output").mkdir(parents=True, exist_ok=True)
    control = _owned_contract({
        "symbol": slug.upper(),
        "strategy_profile": "test_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": max_cum,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 5.0,
        "terminal_drain_max_wait_seconds": 900.0,
        "terminal_drain_stop_preserve_reason": None,
        "lifecycle_wear_stop_per_10k": wear_stop,
        "lifecycle_wear_stop_min_gross_notional": wear_first,
    })
    (workdir / "output" / f"{slug}_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8")
    return control


def test_gate_zero_target_never_stops_even_with_huge_volume(tmp_path: Path, monkeypatch, capsys) -> None:
    # No control JSON -> target stays 0. Huge volume must not submit a target intent.
    (tmp_path / "output").mkdir(parents=True)
    calls: list = []
    monkeypatch.setattr(tg, "subprocess", type("S", (), {"run": staticmethod(_fake_subprocess(calls))}))
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: _target_window_stats(10_000_000.0, 0.0))
    monkeypatch.setattr(tg, "cancel_frozen_tp", lambda *a, **k: 0)   # pre-cleanup must not hit network
    _run_gate_main(monkeypatch, ["--symbol", "ARXUSDT", "--service", "grid-loop@ARXUSDT.service",
                                 "--workdir", str(tmp_path), "--enforce"])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["hit_target"] is False
    assert out["config_error"] == "missing_config"
    assert not any("stop" in c for c in calls)          # never stopped the runner


def test_gate_dry_run_does_not_cancel_frozen_tp(tmp_path: Path, monkeypatch, capsys) -> None:
    # Active runner + no --enforce: the FROZENTP cleanup must not touch the exchange.
    _control(tmp_path, "arxusdt", 100_000)
    calls: list = []
    monkeypatch.setattr(tg, "subprocess", type("S", (), {"run": staticmethod(_fake_subprocess(calls))}))
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: _target_window_stats(0.0, 0.0))
    canceled = {"n": 0}
    monkeypatch.setattr(tg, "cancel_frozen_tp", lambda *a, **k: canceled.__setitem__("n", 1))
    _run_gate_main(monkeypatch, ["--symbol", "ARXUSDT", "--service", "grid-loop@ARXUSDT.service",
                                 "--workdir", str(tmp_path)])
    assert canceled["n"] == 0


def test_gate_rejects_bounded_control_without_owner_before_exchange_query(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    raw = {
        key: value
        for key, value in _owned_contract(
            {
                "symbol": "ARXUSDT",
                "strategy_profile": "test_profile",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "per_order_notional": 20.0,
                "run_start_time": "2026-07-16T00:00:00+00:00",
                "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
                "run_end_time": "2026-07-17T00:00:00+00:00",
                "max_cumulative_notional": 100_000.0,
                "terminal_drain_exit_policy": "drain_then_preserve",
                "terminal_drain_absolute_loss_budget": 5.0,
                "terminal_drain_max_wait_seconds": 900.0,
            }
        ).items()
        if key != "futures_run_contract_owner"
    }
    (output_dir / "arxusdt_loop_runner_control.json").write_text(
        json.dumps(raw), encoding="utf-8"
    )
    monkeypatch.setattr(
        tg,
        "daily_vol_wear",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("owner must be checked before exchange query")
        ),
    )

    _run_gate_main(
        monkeypatch,
        [
            "--symbol",
            "ARXUSDT",
            "--service",
            "grid-loop@ARXUSDT.service",
            "--workdir",
            str(tmp_path),
            "--enforce",
        ],
    )

    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "LIFECYCLE_INTENT_REJECTED"
    assert out["config_error"] == "invalid_run_contract_owner"
    assert "owner is missing" in out["error"]


def test_gate_enforce_submits_terminal_intent_without_runtime_or_exchange_actions(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    # The external gate is only an observer/intent producer.  The loop runner is
    # the sole owner of entry blocking, cancellation and terminal drain orders.
    control = _control(tmp_path, "arxusdt", 100_000)
    calls: list = []
    monkeypatch.setattr(tg, "subprocess", type("S", (), {"run": staticmethod(_fake_subprocess(calls))}))
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: _target_window_stats(200_000.0, 0.0))
    monkeypatch.setattr(tg, "confirm_stopped", lambda *a, **k: False)
    touched: list = []
    monkeypatch.setattr(tg, "cancel_frozen_tp", lambda *a, **k: touched.append("cancel_tp"))
    monkeypatch.setattr(tg, "fetch_futures_open_orders", lambda *a, **k: touched.append("cancel") or [])
    monkeypatch.setattr(tg, "fetch_futures_position_risk_v3", lambda *a, **k: touched.append("pos") or [])
    monkeypatch.setattr(tg, "post_futures_market_order", lambda *a, **k: touched.append("flatten"))
    monkeypatch.setattr(tg, "post_futures_order", lambda *a, **k: touched.append("limit"))
    monkeypatch.setattr(tg, "delete_futures_order", lambda *a, **k: touched.append("delete"))
    monkeypatch.setattr(
        tg,
        "load_live_runner_contract",
        lambda **kwargs: tg.run_contract_identity_from_config(control),
    )
    _run_gate_main(monkeypatch, ["--symbol", "ARXUSDT", "--service", "grid-loop@ARXUSDT.service",
                                 "--workdir", str(tmp_path), "--enforce"])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "LIFECYCLE_INTENT_SUBMITTED"
    assert touched == []
    assert not any("stop" in c for c in calls)

    intent_path = tmp_path / "output" / "arxusdt_terminal_intent.json"
    intent = json.loads(intent_path.read_text(encoding="utf-8"))
    intent_id = intent.pop("intent_id")
    assert intent_id.startswith("ARXUSDT-competition_target_gate-target_reached-")
    assert intent == {
        "schema": "futures_lifecycle_intent_v2",
        "symbol": "ARXUSDT",
        "source": "competition_target_gate",
        "action": "lifecycle_drain",
        "status": "pending",
        "requested_at": out["ts"],
        "trigger_reason": "target_reached",
        "exit_policy": "use_immutable_run_contract",
        "run_contract_id": tg.run_contract_identity_from_config(control),
        "run_contract_snapshot": tg.run_contract_snapshot_from_config(control),
        "observed": {
            "gross_notional": 200000.0,
            "realized_pnl": 0.0,
            "wear_per_10k": 0.0,
            "trade_count": 1,
            "target": 100000.0,
            "first": None,
            "wear_stop": None,
            "window_start": "2026-07-16T00:00:00+00:00",
            "window_end": "2026-07-17T00:00:00+00:00",
            "query_end": "2026-07-16T01:00:00+00:00",
        },
    }


def test_gate_terminal_intent_retry_preserves_first_pending_contract(tmp_path: Path) -> None:
    observed = {
        "gross_notional": 100000.0,
        "wear_per_10k": 0.8,
        "target": 100000.0,
        "first": 75000.0,
        "wear_stop": 2.0,
    }
    first_contract = _owned_contract({
        "symbol": "ARXUSDT",
        "strategy_profile": "test_profile",
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 100000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 5.0,
        "terminal_drain_max_wait_seconds": 900.0,
    })
    first, created = tg.submit_lifecycle_intent(
        workdir=str(tmp_path),
        symbol="ARXUSDT",
        trigger_reason="target_reached",
        requested_at="2026-07-16T01:00:00+00:00",
        observed=observed,
        run_contract_config=first_contract,
    )
    assert created is True

    retry_observed = {**observed, "gross_notional": 110000.0}
    retry, created = tg.submit_lifecycle_intent(
        workdir=str(tmp_path),
        symbol="ARXUSDT",
        trigger_reason="target_reached",
        requested_at="2026-07-16T01:01:00+00:00",
        observed=retry_observed,
        run_contract_config=first_contract,
    )

    assert created is False
    assert retry == first
    assert retry["requested_at"] == "2026-07-16T01:00:00+00:00"
    assert retry["observed"] == observed
    assert list((tmp_path / "output").glob("*.tmp")) == []


def test_gate_cli_wear_thresholds_cannot_enable_wear_exit(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _control(tmp_path, "arxusdt", 100_000)
    monkeypatch.setattr(
        tg,
        "daily_vol_wear",
        lambda *a, **k: _target_window_stats(80_000.0, 3.0),
    )

    _run_gate_main(
        monkeypatch,
        [
            "--symbol",
            "ARXUSDT",
            "--service",
            "grid-loop@ARXUSDT.service",
            "--workdir",
            str(tmp_path),
            "--first",
            "1",
            "--wear-stop",
            "0.1",
            "--enforce",
        ],
    )

    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["hit_target"] is False
    assert out["hit_wear"] is False
    assert not (tmp_path / "output" / "arxusdt_terminal_intent.json").exists()


def test_gate_wear_exit_uses_only_immutable_snapshot_thresholds(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    control = _control(
        tmp_path,
        "arxusdt",
        100_000,
        wear_stop=2.0,
        wear_first=75_000.0,
    )
    monkeypatch.setattr(
        tg,
        "daily_vol_wear",
        lambda *a, **k: _target_window_stats(80_000.0, 3.0),
    )
    monkeypatch.setattr(
        tg,
        "load_live_runner_contract",
        lambda **kwargs: tg.run_contract_identity_from_config(control),
    )

    _run_gate_main(
        monkeypatch,
        [
            "--symbol",
            "ARXUSDT",
            "--service",
            "grid-loop@ARXUSDT.service",
            "--workdir",
            str(tmp_path),
            "--first",
            "999999999",
            "--wear-stop",
            "999",
            "--enforce",
        ],
    )

    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["trigger"] == "wear"
    intent = json.loads(
        (tmp_path / "output" / "arxusdt_terminal_intent.json").read_text(
            encoding="utf-8"
        )
    )
    assert intent["observed"]["first"] == 75_000.0
    assert intent["observed"]["wear_stop"] == 2.0


def test_gate_archives_orphan_completed_intent_before_new_run(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    intent_path = output_dir / "arxusdt_terminal_intent.json"
    intent_path.write_text(
        json.dumps(
            {
                "schema": "futures_lifecycle_intent_v2",
                "intent_id": "ARXUSDT-old-completed",
                "symbol": "ARXUSDT",
                "source": "competition_target_gate",
                "action": "lifecycle_drain",
                "trigger_reason": "target_reached",
                "requested_at": "2026-07-15T01:00:00+00:00",
                "status": "stopped_preserved",
                "run_contract_id": "run-contract-old",
                "observed": {},
            }
        ),
        encoding="utf-8",
    )

    replacement, created = tg.submit_lifecycle_intent(
        workdir=str(tmp_path),
        symbol="ARXUSDT",
        trigger_reason="target_reached",
        requested_at="2026-07-16T01:00:00+00:00",
        observed={
            "gross_notional": 100000.0,
            "wear_per_10k": 0.5,
            "target": 100000.0,
            "first": 75000.0,
            "wear_stop": 2.0,
        },
        run_contract_config=_owned_contract({
            "symbol": "ARXUSDT",
            "strategy_profile": "new_profile",
            "run_start_time": "2026-07-16T00:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
            "run_end_time": "2026-07-17T00:00:00+00:00",
            "max_cumulative_notional": 100000.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 5.0,
            "terminal_drain_max_wait_seconds": 900.0,
        }),
    )

    assert created is True
    assert replacement["run_contract_id"] == tg.run_contract_identity_from_config(
        replacement["run_contract_snapshot"]
    )
    archived = list((output_dir / "terminal_intent_history").glob("*.json"))
    assert len(archived) == 1
    assert json.loads(archived[0].read_text(encoding="utf-8"))["intent_id"] == "ARXUSDT-old-completed"


def test_gate_rejects_wear_trigger_without_bounded_exit_contract(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    (output_dir / "arxusdt_loop_runner_control.json").write_text(
        json.dumps(
            {
                "symbol": "ARXUSDT",
                "strategy_profile": "test_profile",
                "max_cumulative_notional": None,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        tg,
        "daily_vol_wear",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("invalid contract must be rejected before exchange query")
        ),
    )

    _run_gate_main(
        monkeypatch,
        [
            "--symbol",
            "ARXUSDT",
            "--service",
            "grid-loop@ARXUSDT.service",
            "--workdir",
            str(tmp_path),
            "--enforce",
        ],
    )

    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "LIFECYCLE_INTENT_REJECTED"
    assert out["config_error"] == "invalid_run_contract"
    assert not (output_dir / "arxusdt_terminal_intent.json").exists()


def test_gate_rejects_disk_target_that_differs_from_live_run_contract(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    control = _control(tmp_path, "arxusdt", 100_000)
    live_control = dict(control)
    live_control["max_cumulative_notional"] = 200_000.0
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: _target_window_stats(100_000.0, 0.0))
    monkeypatch.setattr(
        tg,
        "load_live_runner_contract",
        lambda **kwargs: tg.run_contract_identity_from_config(live_control),
    )

    _run_gate_main(
        monkeypatch,
        [
            "--symbol",
            "ARXUSDT",
            "--service",
            "grid-loop@ARXUSDT.service",
            "--workdir",
            str(tmp_path),
            "--enforce",
        ],
    )

    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "LIFECYCLE_INTENT_REJECTED"
    assert out["config_error"] == "live_run_contract_mismatch"
    assert not (tmp_path / "output" / "arxusdt_terminal_intent.json").exists()


def test_live_runner_command_reconstructs_same_immutable_contract(tmp_path: Path, monkeypatch) -> None:
    control = _control(tmp_path, "arxusdt", 100_000)
    pid_path = tmp_path / "output" / "arxusdt_loop_runner.pid"
    pid_path.write_text("321", encoding="utf-8")
    command = (
        "python -u -m grid_optimizer.loop_runner "
        "--symbol ARXUSDT --strategy-profile test_profile "
        "--strategy-mode hedge_best_quote_maker_volume_v1 "
        "--per-order-notional 20 "
        "--run-start-time 2026-07-16T00:00:00+00:00 "
        "--runtime-guard-stats-start-time 2026-07-16T00:00:00+00:00 "
        "--run-end-time 2026-07-17T00:00:00+00:00 "
        "--max-cumulative-notional 100000 "
        "--terminal-drain-exit-policy drain_then_preserve "
        "--terminal-drain-absolute-loss-budget 5 "
        "--terminal-drain-max-wait-seconds 900"
    )
    monkeypatch.setattr(
        tg.subprocess,
        "run",
        lambda *args, **kwargs: _FakeProc(returncode=0, stdout=command),
    )

    live_contract_id = tg.load_live_runner_contract(
        workdir=str(tmp_path),
        slug="arxusdt",
    )

    assert live_contract_id == tg.run_contract_identity_from_config(control)


def test_live_runner_command_reconstructs_immutable_wear_thresholds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    control = _control(
        tmp_path,
        "arxusdt",
        100_000,
        wear_stop=2.0,
        wear_first=75_000.0,
    )
    (tmp_path / "output" / "arxusdt_loop_runner.pid").write_text(
        "321",
        encoding="utf-8",
    )
    command = (
        "python -u -m grid_optimizer.loop_runner "
        "--symbol ARXUSDT --strategy-profile test_profile "
        "--strategy-mode hedge_best_quote_maker_volume_v1 "
        "--per-order-notional 20 "
        "--run-start-time 2026-07-16T00:00:00+00:00 "
        "--runtime-guard-stats-start-time 2026-07-16T00:00:00+00:00 "
        "--run-end-time 2026-07-17T00:00:00+00:00 "
        "--max-cumulative-notional 100000 "
        "--lifecycle-wear-stop-per-10k 2 "
        "--lifecycle-wear-stop-min-gross-notional 75000 "
        "--terminal-drain-exit-policy drain_then_preserve "
        "--terminal-drain-absolute-loss-budget 5 "
        "--terminal-drain-max-wait-seconds 900"
    )
    monkeypatch.setattr(
        tg.subprocess,
        "run",
        lambda *args, **kwargs: _FakeProc(returncode=0, stdout=command),
    )

    live_contract_id = tg.load_live_runner_contract(
        workdir=str(tmp_path),
        slug="arxusdt",
    )

    assert live_contract_id == tg.run_contract_identity_from_config(control)


def test_gate_frozen_tp_qty_clamped_to_kept_short_and_step(tmp_path: Path, monkeypatch) -> None:
    # 250 frozen-short lot qty but only 90 kept short -> BUY clamped to 90, truncated to step 5.
    _write_state(tmp_path, "arxusdt", [{"qty": 250, "entry_price": 0.30}])
    captured: dict = {}
    monkeypatch.setattr(tg, "post_futures_order", lambda **kw: captured.update(kw) or {"orderId": 1})
    out = tg.place_frozen_short_tp("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", 0.05, 0.0001,
                                   qty_step=5.0, max_qty=92.0)
    assert out["placed"] is True
    assert out["qty"] == 90                              # min(250, 92) truncated to a multiple of 5
    assert captured["quantity"] == 90
    assert str(out["coid"]).startswith("FROZENTParxusdt")


def test_evaluate_triggers_zero_target_is_never_a_hit() -> None:
    # target <= 0 -> target_ok False and hit_target forced False regardless of volume.
    assert tg.evaluate_triggers(1e9, 0.0, 0.0, None, None) == (False, False, False)
    assert tg.evaluate_triggers(1e9, 0.0, -5.0, None, None) == (False, False, False)
    # positive target: hit only once volume reaches it.
    assert tg.evaluate_triggers(90_000, 0.0, 100_000, 75000, 2.0) == (True, False, False)
    assert tg.evaluate_triggers(100_000, 0.0, 100_000, 75000, 2.0) == (True, True, False)
    # Wear stop is independent of the target, but only when explicitly bound.
    assert tg.evaluate_triggers(80_000, 3.0, 0.0, 75000, 2.0) == (False, False, True)
    assert tg.evaluate_triggers(50_000, 9.0, 0.0, 75000, 2.0) == (False, False, False)


def test_restart_reports_returncode(monkeypatch) -> None:
    # A failed restart must surface ok=False + the code so the caller retries instead of
    # assuming the runner came back.
    def fake_run(cmd, **kw):
        rc = 0 if "reset-failed" in cmd else 1
        return _FakeProc(returncode=rc, stderr="boom")

    monkeypatch.setattr(hm.subprocess, "run", fake_run)
    out = hm.restart("grid-loop@ARXUSDT.service")
    assert out["ok"] is False
    assert out["restart_rc"] == 1
    assert out["reset_failed_rc"] == 0
    assert out["error"] == "boom"

    monkeypatch.setattr(hm.subprocess, "run", lambda cmd, **kw: _FakeProc(returncode=0))
    assert hm.restart("grid-loop@ARXUSDT.service")["ok"] is True


# --- competition_state_realign: never revive a runner it did not stop itself ---


def test_realign_never_starts_an_already_inactive_runner() -> None:
    # The 2026-07-04 ARX incident: auto_realign blind-started a guard-stopped runner
    # into a crash three times. Inactive stays down unless explicitly allowed.
    assert ra.should_start_after_realign(was_active=False, allow_start_when_stopped=False) is False
    assert ra.should_start_after_realign(was_active=True, allow_start_when_stopped=False) is True
    assert ra.should_start_after_realign(was_active=False, allow_start_when_stopped=True) is True


def test_registered_recovery_realign_reports_drift_without_actuating(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    state_path = output_dir / "arxusdt_loop_state.json"
    original_state = {
        "best_quote_volume_ledger": {
            "long_lots": [{"qty": 500.0}],
            "short_lots": [],
        }
    }
    state_path.write_text(json.dumps(original_state), encoding="utf-8")
    (output_dir / "arxusdt_loop_runner_control.json").write_text(
        json.dumps({"symbol": "ARXUSDT", "_futures_recovery_state": {"schema_version": 1}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setattr(
        "sys.argv",
        [
            "realign",
            "--symbol",
            "ARXUSDT",
            "--service",
            "svc",
            "--workdir",
            str(tmp_path),
            "--threshold-qty",
            "10",
            "--enforce",
        ],
    )
    monkeypatch.setattr(
        ra,
        "fetch_exchange_sides",
        lambda *args, **kwargs: (0.0, 0.0, 0.0, 0.0),
    )
    monkeypatch.setattr(ra, "is_active", lambda _service: True)
    side_effects: list[str] = []
    monkeypatch.setattr(
        ra.subprocess,
        "run",
        lambda *args, **kwargs: side_effects.append("systemctl") or _FakeProc(),
    )
    monkeypatch.setattr(
        ra,
        "fetch_futures_open_orders",
        lambda *args, **kwargs: side_effects.append("open_orders") or [],
    )
    monkeypatch.setattr(
        ra,
        "delete_futures_order",
        lambda *args, **kwargs: side_effects.append("cancel"),
    )

    ra.main()

    status = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert status["action"] == "DEFERRED_TO_FUTURES_RECOVERY_COORDINATOR"
    assert status["requested_action"] == "REALIGN_LEDGER"
    assert status["recovery_coordinator_registered"] is True
    assert side_effects == []
    assert json.loads(state_path.read_text(encoding="utf-8")) == original_state


def test_realign_rechecks_coordinator_ownership_before_stopping_or_mutating(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    state_path = output_dir / "arxusdt_loop_state.json"
    original_state = {
        "best_quote_volume_ledger": {
            "long_lots": [{"qty": 500.0}],
            "short_lots": [],
        }
    }
    state_path.write_text(json.dumps(original_state), encoding="utf-8")
    (output_dir / "arxusdt_loop_runner_control.json").write_text(
        json.dumps({"symbol": "ARXUSDT"}), encoding="utf-8"
    )
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setattr(
        "sys.argv",
        [
            "realign",
            "--symbol",
            "ARXUSDT",
            "--service",
            "svc",
            "--workdir",
            str(tmp_path),
            "--threshold-qty",
            "10",
            "--enforce",
        ],
    )
    ownership_checks = iter((False, True))
    monkeypatch.setattr(
        ra,
        "recovery_coordinator_registered",
        lambda _control: next(ownership_checks),
    )
    monkeypatch.setattr(
        ra,
        "fetch_exchange_sides",
        lambda *args, **kwargs: (0.0, 0.0, 0.0, 0.0),
    )
    monkeypatch.setattr(ra, "is_active", lambda _service: True)
    side_effects: list[str] = []
    monkeypatch.setattr(
        ra.subprocess,
        "run",
        lambda *args, **kwargs: side_effects.append("systemctl") or _FakeProc(),
    )
    monkeypatch.setattr(
        ra,
        "fetch_futures_open_orders",
        lambda *args, **kwargs: side_effects.append("open_orders") or [],
    )

    ra.main()

    status = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert status["action"] == "DEFERRED_TO_FUTURES_RECOVERY_COORDINATOR"
    assert status["recovery_coordinator_registered"] is True
    assert side_effects == []
    assert json.loads(state_path.read_text(encoding="utf-8")) == original_state


def test_compute_drift_compares_only_ordinary_inventory() -> None:
    state = {
        "best_quote_volume_ledger": {
            "long_lots": [{"qty": 5000}, {"qty": 838}],
            "short_lots": [{"qty": 2709}],
        },
        "best_quote_frozen_inventory": {"long_lots": [], "short_lots": [{"qty": 291}]},
    }
    ldrift, sdrift = ra.compute_drift(state, long_qty=0.0, short_qty=291.0)
    assert ldrift == 5838.0                       # ledger thinks long, exchange flat
    assert sdrift == 2709.0                       # frozen 291 is outside ordinary drift
    ldrift2, sdrift2 = ra.compute_drift(state, long_qty=5838.0, short_qty=3000.0)
    assert ldrift2 == 0.0 and sdrift2 == 0.0


def test_compute_drift_fails_closed_when_frozen_exceeds_exchange() -> None:
    state = {
        "best_quote_volume_ledger": {"long_lots": [], "short_lots": []},
        "best_quote_frozen_inventory": {
            "long_lots": [],
            "short_lots": [{"qty": 291}],
        },
    }

    with pytest.raises(ValueError, match="frozen inventory exceeds exchange position"):
        ra.compute_drift(state, long_qty=0.0, short_qty=0.0)


def test_realign_ledger_preserves_frozen_and_writes_active_remainder() -> None:
    state = {
        "best_quote_volume_ledger": {"long_lots": [{"qty": 999}], "short_lots": []},
        "best_quote_frozen_inventory": {"long_lots": [{"qty": 100}], "short_lots": [{"qty": 40}]},
    }
    out = ra.realign_ledger(state, lq=250.0, lavg=0.21, sq=40.0, savg=0.22)
    led = state["best_quote_volume_ledger"]
    assert out == {"new_long": 150.0, "new_short": 0.0}   # 250 exch - 100 frozen; short fully frozen
    assert led["long_lots"] == [{"qty": 150.0, "price": 0.21, "source": "auto_realign", "side": "LONG"}]
    assert led["short_lots"] == []


def test_realign_ledger_seals_reflected_trade_cursor_against_restart_replay(monkeypatch) -> None:
    import grid_optimizer.loop_runner as lr

    trade = {
        "id": 15800462,
        "orderId": 81319375,
        "clientOrderId": "gx-arxu-bestquot-2-14962130",
        "symbol": "ARXUSDT",
        "side": "BUY",
        "positionSide": "SHORT",
        "time": 2_000,
        "price": "0.1755",
        "qty": "967",
        "quoteQty": "169.7085",
    }
    state = {
        "best_quote_volume_order_refs": {
            "81319375": {
                "book": "normal_bq",
                "role": "best_quote_active_pair_reduce_short",
                "side": "BUY",
                "position_side": "SHORT",
            }
        },
        "best_quote_volume_ledger": {
            "initialized": True,
            "sync_ok": True,
            "long_lots": [],
            "short_lots": [{"qty": 3_763.6339548294677, "price": 0.1762}],
            "last_trade_time_ms": 1_000,
            "last_trade_keys_at_time": [],
            "applied_trade_fill_keys": [],
        },
        "best_quote_frozen_inventory": {
            "long_lots": [],
            "short_lots": [{"qty": 1_747.3660451705323, "price": 0.1762}],
        },
    }

    ra.realign_ledger(
        state,
        lq=0.0,
        lavg=0.0,
        sq=4_544.0,
        savg=0.1762,
        reflected_trade_rows=[trade],
    )
    monkeypatch.setattr(lr, "_fetch_trade_rows_since", lambda **_kwargs: [trade])
    snapshot = lr.sync_best_quote_volume_ledger(
        state=state,
        symbol="ARXUSDT",
        api_key="k",
        api_secret="s",
        recv_window=5_000,
        current_long_qty=0.0,
        current_short_qty=4_544.0,
        current_long_avg_price=0.0,
        current_short_avg_price=0.1762,
        mid_price=0.1755,
        observed_trade_rows=[],
    )

    assert snapshot["short_qty"] == 2_796.6339548294677
    assert state["best_quote_volume_ledger"]["last_applied_trade_count"] == 0
    assert "81319375:trade:15800462" in state["best_quote_volume_ledger"]["applied_trade_fill_keys"]


def test_archive_stale_plan_moves_file_and_tolerates_missing(tmp_path) -> None:
    (tmp_path / "output").mkdir(parents=True)
    plan = tmp_path / "output" / "arxusdt_loop_latest_plan.json"
    plan.write_text(json.dumps({"actual_net_qty": 5838}), encoding="utf-8")
    dst = ra.archive_stale_plan(str(tmp_path), "arxusdt")
    assert dst is not None and not plan.exists()          # moved aside -> startup guard can't latch
    assert json.loads(Path(dst).read_text())["actual_net_qty"] == 5838
    assert ra.archive_stale_plan(str(tmp_path), "arxusdt") is None   # idempotent when absent


def test_realign_order_prefix_matches_loop_runner() -> None:
    # The realign cancel filter must track the runner's managed-order prefix exactly;
    # a drift here silently flips "cancel managed only" into "cancel nothing/em everything".
    import grid_optimizer.loop_runner as lr

    for sym in ("ARXUSDT", "OUSDT", "REUSDT"):
        assert ra.strategy_client_order_prefix(sym) == lr._strategy_client_order_prefix(sym)


def test_realign_cancels_managed_only_preserves_frozentp_and_external() -> None:
    managed = {"clientOrderId": "gx-arxu-bestquot-1-08624"}
    frozen_tp = {"clientOrderId": "FROZENTParxusdt20260705"}     # target gate protective TP
    manual_flatten = {"clientOrderId": "mfarxusd_closelon_s_1"}  # external maker flatten
    manual_reduce = {"clientOrderId": "usrreduceL2607050"}
    missing = {}
    assert ra.is_managed_order(managed, "ARXUSDT") is True
    assert ra.is_managed_order(frozen_tp, "ARXUSDT") is False
    assert ra.is_managed_order(manual_flatten, "ARXUSDT") is False
    assert ra.is_managed_order(manual_reduce, "ARXUSDT") is False
    assert ra.is_managed_order(missing, "ARXUSDT") is False


def test_realign_aborts_before_any_cancel_when_backup_fails(tmp_path: Path, monkeypatch, capsys) -> None:
    # Backup failure must abort the WHOLE realign before any order cancel or state
    # mutation -- otherwise a failed copy leaves no rollback for the ledger rewrite.
    (tmp_path / "output").mkdir(parents=True)
    state_file = tmp_path / "output" / "arxusdt_loop_state.json"
    original_state = {"best_quote_volume_ledger": {"long_lots": [{"qty": 5268}], "short_lots": []}}
    state_file.write_text(json.dumps(original_state), encoding="utf-8")

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setattr("sys.argv", ["realign", "--symbol", "ARXUSDT", "--service", "svc",
                                     "--workdir", str(tmp_path), "--enforce"])
    monkeypatch.setattr(ra, "fetch_exchange_sides", lambda *a, **k: (1400.0, 0.21, 1400.0, 0.21))
    monkeypatch.setattr(ra, "is_active", lambda service: True)
    monkeypatch.setattr(ra.subprocess, "run", lambda *a, **k: _FakeProc())
    monkeypatch.setattr(ra.time, "sleep", lambda *_: None)

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(ra.shutil, "copy2", boom)
    touched: list = []
    monkeypatch.setattr(ra, "fetch_futures_open_orders", lambda *a, **k: touched.append("list") or [])
    monkeypatch.setattr(ra, "delete_futures_order", lambda *a, **k: touched.append("cancel"))

    try:
        ra.main()
        raise AssertionError("expected SystemExit")
    except SystemExit as exc:
        assert exc.code == 1
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "ABORTED_BACKUP_FAILED"
    assert touched == []                                            # no cancels attempted
    assert json.loads(state_file.read_text()) == original_state     # state untouched


# --- loop_runner runtime-guard stale-plan fallback (fixes the startup latch) ---


def test_guard_plan_freshness_thresholds() -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 7, 5, 0, 0, 0, tzinfo=timezone.utc)
    fresh = {"generated_at": (now - timedelta(seconds=60)).isoformat()}
    stale = {"generated_at": (now - timedelta(seconds=400)).isoformat()}
    assert lr._runtime_guard_plan_report_is_fresh(fresh, now=now) is True
    assert lr._runtime_guard_plan_report_is_fresh(stale, now=now) is False
    assert lr._runtime_guard_plan_report_is_fresh({}, now=now) is False               # no timestamp
    assert lr._runtime_guard_plan_report_is_fresh({"generated_at": "garbage"}, now=now) is False


def test_guard_live_exposure_excludes_frozen_inventory_and_is_fail_closed() -> None:
    import grid_optimizer.loop_runner as lr

    creds = lambda: ("k", "s")  # noqa: E731

    # Balanced hedge nets to ~0 even though both legs are large.
    hedge = [
        {"positionAmt": "1400", "markPrice": "0.2", "unRealizedProfit": "-10.5"},
        {"positionAmt": "-1400", "markPrice": "0.2", "unRealizedProfit": "4.5"},
    ]
    net, upnl = lr._runtime_guard_live_exposure(
        "ARXUSDT", fetch_position_risk=lambda **kw: hedge, load_credentials=creds)
    assert net == 0.0
    assert upnl == -6.0

    # Frozen inventory is a separate ledger.  The ordinary position guard sees
    # only 2 LONG - 3 non-frozen SHORT, not the 720 frozen SHORT reservoir.
    reservoir = [
        {"positionAmt": "2", "markPrice": "1.0", "unRealizedProfit": "0"},
        {"positionAmt": "-723", "markPrice": "1.0", "unRealizedProfit": "12.0"},
    ]
    net, _ = lr._runtime_guard_live_exposure(
        "OUSDT",
        frozen_short_qty=720.0,
        fetch_position_risk=lambda **kw: reservoir,
        load_credentials=creds,
    )
    assert net == -1.0
    assert lr._runtime_guard_live_exposure(
        "OUSDT",
        frozen_short_qty=800.0,
        fetch_position_risk=lambda **kw: reservoir,
        load_credentials=creds,
    ) is None

    # Fail-closed: fetch failure or missing credentials -> None (caller keeps stale values).
    def boom(**kw):
        raise RuntimeError("api down")

    assert lr._runtime_guard_live_exposure("ARXUSDT", fetch_position_risk=boom, load_credentials=creds) is None
    assert lr._runtime_guard_live_exposure("ARXUSDT", fetch_position_risk=lambda **kw: [],
                                           load_credentials=lambda: None) is None


def test_guard_exposure_inputs_fresh_plan_never_touches_live(monkeypatch) -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 7, 5, 0, 0, 0, tzinfo=timezone.utc)
    fresh_plan = {
        "generated_at": (now - timedelta(seconds=30)).isoformat(),
        "strategy_actual_net_notional": 123.0,
        "strategy_unrealized_pnl": -4.0,
    }
    calls: list = []

    def fetcher(symbol):
        calls.append(symbol)
        return (999.0, 999.0)

    net, upnl, source = lr._runtime_guard_exposure_inputs(
        fresh_plan, now=now, symbol="ARXUSDT", live_exposure_fetcher=fetcher)
    assert (net, upnl, source) == (123.0, -4.0, "plan")
    assert calls == []                                   # fresh plan: live NOT consulted


def test_guard_exposure_inputs_fresh_hedge_plan_excludes_frozen_inventory() -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timezone

    now = datetime(2026, 7, 17, 0, 0, 0, tzinfo=timezone.utc)
    price = 190.96
    fresh_plan = {
        "generated_at": now.isoformat(),
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "mid_price": price,
        # The ordinary strategy net excludes the frozen LONG reservoir.
        "actual_net_notional": 0.748 * price,
        "strategy_actual_net_notional": (0.465 - 0.073) * price,
        "exchange_actual_net_notional": (0.748 - 0.073) * price,
        "best_quote_maker_volume": {
            "reduce_freeze": {
                "actual_long_qty": 0.748,
                "actual_short_qty": 0.073,
                "frozen_long_qty": 0.283,
                "frozen_short_qty": 0.0,
            }
        },
    }

    net, _upnl, source = lr._runtime_guard_exposure_inputs(
        fresh_plan,
        now=now,
        symbol="BCHUSDT",
        live_exposure_fetcher=lambda _symbol: (_ for _ in ()).throw(
            AssertionError("fresh plan must not fetch live exposure")
        ),
    )

    assert source == "plan"
    assert abs(net - ((0.465 - 0.073) * price)) < 1e-9
    guard = lr.evaluate_runtime_guards(
        config=lr.normalize_runtime_guard_config(
            {"max_actual_net_notional": 120.0}
        ),
        now=now,
        cumulative_gross_notional=0.0,
        pnl_events=[],
        actual_net_notional=net,
    )
    assert guard.primary_reason is None
    assert guard.tradable is True


def test_ordinary_position_qtys_subtract_frozen_ledger_from_exchange_sides() -> None:
    import grid_optimizer.loop_runner as lr

    ordinary_long, ordinary_short = lr._ordinary_position_qtys(
        exchange_long_qty=0.748,
        exchange_short_qty=0.073,
        frozen_long_qty=0.283,
        frozen_short_qty=0.0,
    )

    assert abs(ordinary_long - 0.465) < 1e-12
    assert abs(ordinary_short - 0.073) < 1e-12


def test_ordinary_position_qtys_can_have_opposite_sign_from_exchange_total() -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    ordinary_long, ordinary_short = lr._ordinary_position_qtys(
        exchange_long_qty=10.0,
        exchange_short_qty=5.0,
        frozen_long_qty=9.0,
        frozen_short_qty=0.0,
    )

    assert ordinary_long - ordinary_short == -4.0
    conditions = lr._runtime_guard_safety_conditions(
        runtime_guard_result=SimpleNamespace(
            matched_reasons=["max_actual_net_notional_hit"],
            actual_net_notional_abs=4.0,
        ),
        actual_net_notional=ordinary_long - ordinary_short,
        observed_at=datetime(2026, 7, 17, tzinfo=timezone.utc),
        ttl=timedelta(seconds=120),
    )
    assert conditions[0].side.value == "BUY"


def test_ordinary_position_qtys_reject_frozen_ledger_above_exchange_side() -> None:
    import grid_optimizer.loop_runner as lr

    with pytest.raises(ValueError, match="frozen inventory exceeds exchange position"):
        lr._ordinary_position_qtys(
            exchange_long_qty=1.0,
            exchange_short_qty=2.0,
            frozen_long_qty=1.1,
            frozen_short_qty=0.0,
        )


@pytest.mark.parametrize("invalid", ["bad", float("nan"), float("inf"), -1.0])
def test_ordinary_position_qtys_rejects_invalid_or_negative_boundary_values(
    invalid,
) -> None:
    import grid_optimizer.loop_runner as lr

    with pytest.raises(ValueError, match="inventory boundary"):
        lr._ordinary_position_qtys(
            exchange_long_qty=invalid,
            exchange_short_qty=0.0,
            frozen_long_qty=0.0,
            frozen_short_qty=0.0,
        )


def test_runtime_guard_prefers_exchange_minus_frozen_over_strategy_ledger() -> None:
    import grid_optimizer.loop_runner as lr

    actual = lr._runtime_guard_plan_actual_net_notional(
        {
            "mid_price": 222.78,
            "exchange_long_qty": 0.650,
            "exchange_short_qty": 0.073,
            "frozen_long_qty": 0.283,
            "frozen_short_qty": 0.0,
            "ordinary_actual_net_notional": 999.0,
            "strategy_actual_net_notional": 888.0,
        }
    )

    assert actual == pytest.approx(((0.650 - 0.283) - 0.073) * 222.78)


def test_actual_net_safety_cancel_executes_when_stale_cancel_is_disabled() -> None:
    import grid_optimizer.loop_runner as lr
    from argparse import Namespace

    actions = {
        "cancel_count": 1,
        "cancel_orders": [{"orderId": 7, "side": "BUY"}],
        "actual_net_exposure_decision": {
            "action": "cancel_risk_increasing",
        },
    }

    assert lr._execution_cancel_actions_enabled(
        Namespace(cancel_stale=False),
        actions,
    )
    assert not lr._execution_cancel_actions_enabled(
        Namespace(cancel_stale=False),
        {
            "cancel_count": 1,
            "cancel_orders": [{"orderId": 8, "side": "BUY"}],
            "actual_net_exposure_decision": {"action": "normal"},
        },
    )
    refresh_actions = {
        **actions,
        "actual_net_exposure_decision": {
            "action": "cancel_net_decrease_refresh",
        },
    }
    assert lr._execution_cancel_actions_enabled(
        Namespace(cancel_stale=False),
        refresh_actions,
    )


def test_actual_net_safety_cancel_only_overrides_stale_cancel_validation_error() -> None:
    import grid_optimizer.loop_runner as lr

    stale_error = (
        "plan contains stale orders; rerun with --cancel-stale or regenerate the plan"
    )
    validation = {
        "ok": False,
        "errors": [stale_error],
        "actions": {
            "cancel_count": 1,
            "cancel_orders": [{"orderId": 7, "side": "BUY"}],
            "actual_net_exposure_decision": {
                "action": "cancel_risk_increasing",
            },
        },
    }

    updated = lr._authorize_actual_net_safety_cancel_validation(validation)

    assert updated["ok"] is True
    assert updated["errors"] == []

    validation["errors"] = [stale_error, "unrelated validation failure"]
    validation["ok"] = False
    updated = lr._authorize_actual_net_safety_cancel_validation(validation)
    assert updated["ok"] is False
    assert updated["errors"] == ["unrelated validation failure"]


def test_actual_net_cap_uses_fresh_full_exchange_order_snapshot(monkeypatch) -> None:
    import grid_optimizer.loop_runner as lr
    from argparse import Namespace

    manual_order = {
        "orderId": 991,
        "clientOrderId": "manual-order",
        "symbol": "BCHUSDT",
        "side": "BUY",
    }
    calls = []

    def fetch(symbol, api_key, api_secret, *, recv_window, use_cache):
        calls.append((symbol, api_key, api_secret, recv_window, use_cache))
        return [manual_order]

    monkeypatch.setattr(lr, "fetch_futures_open_orders", fetch)

    rows = lr._actual_net_exposure_open_orders(
        args=Namespace(max_actual_net_notional=120.0, recv_window=5000),
        symbol="BCHUSDT",
        api_key="key",
        api_secret="secret",
        snapshot_open_orders=[{"orderId": 1, "clientOrderId": "gx-bchu-1"}],
    )

    assert rows == [manual_order]
    assert calls == [("BCHUSDT", "key", "secret", 5000, False)]


def test_guard_exposure_inputs_stale_or_missing_ts_uses_live(monkeypatch) -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 7, 5, 0, 0, 0, tzinfo=timezone.utc)
    # The incident shape: pre-stop snapshot says net 812 but the account was reduced
    # externally to a flat hedge -- the stale value must be replaced by live truth.
    stale_plan = {
        "generated_at": (now - timedelta(seconds=400)).isoformat(),
        "strategy_actual_net_notional": 812.0,
        "strategy_unrealized_pnl": -58.0,
    }
    net, upnl, source = lr._runtime_guard_exposure_inputs(
        stale_plan, now=now, symbol="ARXUSDT", live_exposure_fetcher=lambda sym: (0.0, -1.0))
    assert (net, upnl, source) == (0.0, -1.0, "live_position_risk")

    no_ts_plan = {"strategy_actual_net_notional": 812.0}
    net, upnl, source = lr._runtime_guard_exposure_inputs(
        no_ts_plan, now=now, symbol="ARXUSDT", live_exposure_fetcher=lambda sym: (5.0, 0.0))
    assert (net, upnl, source) == (5.0, 0.0, "live_position_risk")

    # EMPTY report (no plan file yet) keeps legacy semantics: nothing stale to latch
    # on, and a plan-less guard evaluation must not require network access.
    calls: list = []
    net, upnl, source = lr._runtime_guard_exposure_inputs(
        {}, now=now, symbol="ARXUSDT",
        live_exposure_fetcher=lambda sym: calls.append(sym) or (9.0, 9.0))
    assert (net, upnl, source) == (None, None, "no_plan")
    assert calls == []


def test_guard_exposure_inputs_stale_frozen_scope_keeps_strategy_upnl() -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 7, 5, 0, 0, 0, tzinfo=timezone.utc)
    stale_plan = {
        "generated_at": (now - timedelta(seconds=400)).isoformat(),
        "strategy_actual_net_notional": -1.0,
        "strategy_unrealized_pnl": -2.0,
        "frozen_short_qty": 720.0,
    }

    net, upnl, source = lr._runtime_guard_exposure_inputs(
        stale_plan,
        now=now,
        symbol="OUSDT",
        live_exposure_fetcher=lambda _symbol: (-1.0, -999.0),
    )

    assert (net, upnl, source) == (-1.0, -2.0, "live_position_risk")


def test_guard_exposure_inputs_fail_closed_keeps_stale_values() -> None:
    import grid_optimizer.loop_runner as lr
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 7, 5, 0, 0, 0, tzinfo=timezone.utc)
    stale_plan = {
        "generated_at": (now - timedelta(seconds=400)).isoformat(),
        "strategy_actual_net_notional": 812.0,
        "strategy_unrealized_pnl": -58.0,
    }
    # Live fetch fails -> keep the stale (conservative) values, never blind-release.
    net, upnl, source = lr._runtime_guard_exposure_inputs(
        stale_plan, now=now, symbol="ARXUSDT", live_exposure_fetcher=lambda sym: None)
    assert (net, upnl, source) == (812.0, -58.0, "stale_plan_fail_closed")


def test_realign_main_cancels_managed_only_and_archives_plan(tmp_path: Path, monkeypatch, capsys) -> None:
    # End-to-end enforce path with an ACTIVE runner: stop -> cancel MANAGED (gx-) only
    # -> realign ledger -> archive stale plan -> start. Durable frozen orders,
    # FROZENTP / manual / external orders must survive; an unbound gx- prefix
    # remains ordinary and cannot spoof frozen ownership.
    (tmp_path / "output").mkdir(parents=True)
    state_file = tmp_path / "output" / "arxusdt_loop_state.json"
    state_file.write_text(json.dumps({
        "best_quote_volume_ledger": {"long_lots": [{"qty": 5268}], "short_lots": [{"qty": 2314}]},
        "best_quote_frozen_inventory": {},
        "best_quote_volume_order_refs": {
            "2": {
                "book": "frozen_bq",
                "role": "frozen_inventory_manual_reduce_long",
                "client_order_id": "gx-arxu-frozen-owned-2",
            },
        },
        "best_quote_frozen_inventory_pair_release": {
            "submission_manifest": {
                "legs": {
                    "short": {
                        "order_id": "6",
                        "client_order_id": "gx-arxu-frozen-pair-6",
                    },
                },
            },
        },
    }), encoding="utf-8")
    plan_file = tmp_path / "output" / "arxusdt_loop_latest_plan.json"
    plan_file.write_text(json.dumps({"strategy_actual_net_notional": 812}), encoding="utf-8")

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setattr("sys.argv", ["realign", "--symbol", "ARXUSDT", "--service", "svc",
                                     "--workdir", str(tmp_path), "--enforce"])
    monkeypatch.setattr(ra, "fetch_exchange_sides", lambda *a, **k: (1400.0, 0.2175, 1400.0, 0.2142))
    monkeypatch.setattr(
        ra,
        "fetch_settled_realign_snapshot",
        lambda *a, **k: (1400.0, 0.2175, 1400.0, 0.2142, []),
    )
    monkeypatch.setattr(ra, "is_active", lambda service: True)
    sysctl: list = []
    monkeypatch.setattr(ra.subprocess, "run",
                        lambda cmd, **kw: sysctl.append(" ".join(cmd)) or _FakeProc())
    monkeypatch.setattr(ra.time, "sleep", lambda *_: None)
    open_orders = [
        {"orderId": 1, "clientOrderId": "gx-arxu-bestquot-1-08624"},
        {"orderId": 2, "clientOrderId": "gx-arxu-frozen-owned-2"},
        {"orderId": 3, "clientOrderId": "mfarxusd_closelon_s_1"},
        {"orderId": 4, "clientOrderId": "usrreduceL2607050"},
        {"orderId": 5, "clientOrderId": "gx-arxu-frozen-prefix-spoof"},
        {"orderId": 6, "clientOrderId": "gx-arxu-frozen-pair-6"},
        {"orderId": 7, "clientOrderId": "FROZENTParxusdt202607050113"},
    ]
    monkeypatch.setattr(ra, "fetch_futures_open_orders", lambda *a, **k: open_orders)
    canceled: list = []
    monkeypatch.setattr(ra, "delete_futures_order",
                        lambda **kw: canceled.append(kw["order_id"]))

    ra.main()

    assert canceled == [1, 5]                                 # ordinary gx- + prefix spoof
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "REALIGNED_AND_RESTARTED"
    assert out["canceled_managed_orders"] == 2
    assert out["kept_frozen_orders"] == 2                     # durable ref + pair manifest
    assert out["kept_unmanaged_orders"] == 3                  # FROZENTP + mf + usr preserved
    assert not plan_file.exists()                             # stale plan archived before start
    assert any("stop" in c for c in sysctl) and any("start" in c for c in sysctl)
    new_state = json.loads(state_file.read_text())
    lots = new_state["best_quote_volume_ledger"]
    assert lots["long_lots"][0]["qty"] == 1400.0              # ledger realigned to exchange truth
    assert lots["short_lots"][0]["qty"] == 1400.0


def test_health_monitor_terminal_stop_suppresses_governor_and_deadlock(tmp_path: Path, monkeypatch, capsys) -> None:
    # A runner held in a runtime-guard stop can stay process-alive (service active,
    # journal shows stop_reason). Neither the wear governor (config change + restart)
    # nor the deadlock path may touch it -- the stop encodes a risk decision.
    (tmp_path / "output").mkdir(parents=True)
    (tmp_path / "output" / "arxusdt_health_monitor_state.json").write_text(
        json.dumps({"high_streak": 1}), encoding="utf-8")     # one more hot reading would brake

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setattr("sys.argv", ["hm", "--symbol", "ARXUSDT", "--service", "svc",
                                     "--workdir", str(tmp_path), "--first", "3000", "--enforce"])
    monkeypatch.setattr(hm, "is_active", lambda service: True)
    monkeypatch.setattr(hm, "journal",
                        lambda service, minutes: "mid=0.2 placed=0\n  stop_reason: max_actual_net_notional_hit")
    # Wear numbers that WOULD brake (rwear > brake 3.0, day 1.8 in (brake_day 1.5, hard 2.0)).
    monkeypatch.setattr(hm, "daily_recent_wear", lambda *a, **k: (5000.0, 1.8, 500.0, 5.0))
    monkeypatch.setattr(hm, "get_offset", lambda cfg: ("best_quote_maker_volume_quote_offset_ticks", 0))
    forbidden: list = []
    monkeypatch.setattr(hm, "apply_offset", lambda *a, **k: forbidden.append("apply_offset"))
    monkeypatch.setattr(hm, "restart", lambda *a, **k: forbidden.append("restart") or {"ok": True})
    monkeypatch.setattr(hm, "deadlock_unstick", lambda *a, **k: forbidden.append("unstick") or {})

    hm.main()

    assert forbidden == []                                    # no restart, no config change, no unstick
    rec = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rec["terminal_stop"] is True
    assert rec["intended_stop"] is True
    assert rec["deadlock"].get("terminal") is True
    assert "action" not in rec.get("governor", {})
