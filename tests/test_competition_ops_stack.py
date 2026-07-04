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


def test_deadlock_closes_only_matched_neutral_hedge(tmp_path: Path, monkeypatch) -> None:
    # Balanced hedge opened near the same price: 120 long (20 frozen) + 100 short (10 frozen)
    # -> managed 100 long / 90 short -> matched 90, net-neutral, so it acts.
    _plan(tmp_path, "arxusdt", 20, 10)
    _patch_positions(monkeypatch, long_amt=120, short_amt=-100, long_entry=0.2200, short_entry=0.2201)
    orders: list = []
    monkeypatch.setattr(hm, "post_futures_market_order",
                        lambda **kw: orders.append((kw["side"], kw["quantity"], kw["position_side"])), raising=False)
    import grid_optimizer.data as data
    monkeypatch.setattr(data, "post_futures_market_order",
                        lambda **kw: orders.append((kw["side"], kw["quantity"], kw["position_side"])), raising=False)
    out = hm.deadlock_unstick("ARXUSDT", "k", "s", str(tmp_path), "arxusdt", min_matched=20)
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
