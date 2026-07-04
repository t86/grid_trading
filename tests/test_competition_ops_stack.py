"""Unit tests for the tracked, symbol-generic competition ops stack.

These cover the pure decision logic (no exchange calls): frozen take-profit
pricing, the balanced-hedge matched-close math, and the self-contained config
patch that replaces the server ``/tmp/apply_cfg.py`` shell-out.
"""
from __future__ import annotations

import json
from pathlib import Path

import grid_optimizer.competition_health_monitor as hm
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
