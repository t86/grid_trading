"""Unit tests for the tracked, symbol-generic competition ops stack.

These cover the pure decision logic (no exchange calls): frozen take-profit
pricing, the balanced-hedge matched-close math, and the self-contained config
patch that replaces the server ``/tmp/apply_cfg.py`` shell-out.
"""
from __future__ import annotations

import json
from pathlib import Path

import grid_optimizer.competition_health_monitor as hm
import grid_optimizer.competition_state_realign as ra
import grid_optimizer.competition_target_gate as tg


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


def _control(workdir: Path, slug: str, max_cum: float) -> None:
    (workdir / "output").mkdir(parents=True, exist_ok=True)
    (workdir / "output" / f"{slug}_loop_runner_control.json").write_text(
        json.dumps({"max_cumulative_notional": max_cum}), encoding="utf-8")


def test_gate_zero_target_never_stops_even_with_huge_volume(tmp_path: Path, monkeypatch, capsys) -> None:
    # No control JSON -> target stays 0. A 0 target with huge day volume must NOT stop+flatten.
    (tmp_path / "output").mkdir(parents=True)
    calls: list = []
    monkeypatch.setattr(tg, "subprocess", type("S", (), {"run": staticmethod(_fake_subprocess(calls))}))
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: (10_000_000.0, 0.0))
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
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: (0.0, 0.0))
    canceled = {"n": 0}
    monkeypatch.setattr(tg, "cancel_frozen_tp", lambda *a, **k: canceled.__setitem__("n", 1))
    _run_gate_main(monkeypatch, ["--symbol", "ARXUSDT", "--service", "grid-loop@ARXUSDT.service",
                                 "--workdir", str(tmp_path)])
    assert canceled["n"] == 0


def test_gate_aborts_when_stop_not_confirmed_inactive(tmp_path: Path, monkeypatch, capsys) -> None:
    # Target hit + --enforce, but the service never goes inactive: abort BEFORE cancelling or
    # flattening anything.
    _control(tmp_path, "arxusdt", 100_000)
    calls: list = []
    monkeypatch.setattr(tg, "subprocess", type("S", (), {"run": staticmethod(_fake_subprocess(calls))}))
    monkeypatch.setattr(tg, "daily_vol_wear", lambda *a, **k: (200_000.0, 0.0))
    monkeypatch.setattr(tg, "confirm_stopped", lambda *a, **k: False)
    monkeypatch.setattr(tg, "cancel_frozen_tp", lambda *a, **k: 0)   # isolate the flatten path
    touched: list = []
    monkeypatch.setattr(tg, "fetch_futures_open_orders", lambda *a, **k: touched.append("cancel") or [])
    monkeypatch.setattr(tg, "fetch_futures_position_risk_v3", lambda *a, **k: touched.append("pos") or [])
    monkeypatch.setattr(tg, "post_futures_market_order", lambda *a, **k: touched.append("flatten"))
    _run_gate_main(monkeypatch, ["--symbol", "ARXUSDT", "--service", "grid-loop@ARXUSDT.service",
                                 "--workdir", str(tmp_path), "--enforce"])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "ABORTED_STOP_FAILED"
    assert touched == []                                 # no cancel / no position read / no flatten


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
    assert tg.evaluate_triggers(1e9, 0.0, 0.0, 75000, 2.0) == (False, False, False)
    assert tg.evaluate_triggers(1e9, 0.0, -5.0, 75000, 2.0) == (False, False, False)
    # positive target: hit only once volume reaches it.
    assert tg.evaluate_triggers(90_000, 0.0, 100_000, 75000, 2.0) == (True, False, False)
    assert tg.evaluate_triggers(100_000, 0.0, 100_000, 75000, 2.0) == (True, True, False)
    # wear stop is independent of the target and only arms past `first`.
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


def test_compute_drift_counts_ledger_plus_frozen_vs_exchange() -> None:
    state = {
        "best_quote_volume_ledger": {
            "long_lots": [{"qty": 5000}, {"qty": 838}],
            "short_lots": [{"qty": 2709}],
        },
        "best_quote_frozen_inventory": {"long_lots": [], "short_lots": [{"qty": 291}]},
    }
    ldrift, sdrift = ra.compute_drift(state, long_qty=0.0, short_qty=0.0)
    assert ldrift == 5838.0                       # ledger thinks long, exchange flat
    assert sdrift == 3000.0                       # 2709 ledger + 291 frozen
    ldrift2, sdrift2 = ra.compute_drift(state, long_qty=5838.0, short_qty=3000.0)
    assert ldrift2 == 0.0 and sdrift2 == 0.0


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


def test_guard_live_exposure_is_account_level_and_fail_closed() -> None:
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

    # ACCOUNT-level scope: a frozen short reservoir (OUSDT ~720) is included, so the
    # reading is conservative; the symbol's max_actual_net_notional (1500) must be
    # sized above reservoir + headroom -- the documented operating convention.
    reservoir = [
        {"positionAmt": "2", "markPrice": "1.0", "unRealizedProfit": "0"},
        {"positionAmt": "-723", "markPrice": "1.0", "unRealizedProfit": "12.0"},
    ]
    net, _ = lr._runtime_guard_live_exposure(
        "OUSDT", fetch_position_risk=lambda **kw: reservoir, load_credentials=creds)
    assert net == -721.0
    assert abs(net) < 1500.0                             # reservoir alone must not trip the O guard

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
    # -> realign ledger -> archive stale plan -> start. FROZENTP / manual / external
    # orders must survive.
    (tmp_path / "output").mkdir(parents=True)
    state_file = tmp_path / "output" / "arxusdt_loop_state.json"
    state_file.write_text(json.dumps({
        "best_quote_volume_ledger": {"long_lots": [{"qty": 5268}], "short_lots": [{"qty": 2314}]},
        "best_quote_frozen_inventory": {},
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
        {"orderId": 2, "clientOrderId": "FROZENTParxusdt202607050113"},
        {"orderId": 3, "clientOrderId": "mfarxusd_closelon_s_1"},
        {"orderId": 4, "clientOrderId": "usrreduceL2607050"},
    ]
    monkeypatch.setattr(ra, "fetch_futures_open_orders", lambda *a, **k: open_orders)
    canceled: list = []
    monkeypatch.setattr(ra, "delete_futures_order",
                        lambda **kw: canceled.append(kw["order_id"]))

    ra.main()

    assert canceled == [1]                                    # ONLY the managed gx- order
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["action"] == "REALIGNED_AND_RESTARTED"
    assert out["canceled_managed_orders"] == 1
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
