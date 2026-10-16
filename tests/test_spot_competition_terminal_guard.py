from __future__ import annotations

from grid_optimizer.spot_competition_terminal_guard import cleanup_trigger, select_effective_target
from grid_optimizer import spot_competition_terminal_guard as guard
from argparse import Namespace


def test_cross_quote_cleanup_uses_distinct_symbols(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(guard, "fetch_spot_open_orders", lambda symbol, *a: [{"orderId": 1}])
    monkeypatch.setattr(guard, "fetch_futures_open_orders", lambda symbol, *a, **k: [{"orderId": 2}])
    monkeypatch.setattr(guard, "delete_spot_order", lambda **k: calls.append(("spot", k["symbol"])))
    monkeypatch.setattr(guard, "delete_futures_order", lambda **k: calls.append(("futures", k["symbol"])))
    assert guard._cancel_symbol_orders("ALGOUSDC", "key", "secret", "ALGOUSDT") == {"spot": 1, "futures": 1}
    assert calls == [("spot", "ALGOUSDC"), ("futures", "ALGOUSDT")]


def test_cross_quote_flatteners_route_to_correct_markets(monkeypatch, tmp_path) -> None:
    commands = []
    monkeypatch.setattr(guard.subprocess, "run", lambda cmd, **k: commands.append(cmd))
    args = Namespace(
        python_bin="python", symbol="ALGOUSDC", hedge_symbol="ALGOUSDT",
        spot_flatten_prefix="spot", futures_flatten_prefix="futures", flatten_sleep_seconds=2,
        state=str(tmp_path / "state.json"), spot_flatten_events="spot.jsonl", futures_flatten_events="futures.jsonl",
    )
    guard._run_flatteners(args, {}, tmp_path / "events.jsonl")
    assert commands[0][commands[0].index("--symbol") + 1] == "ALGOUSDC"
    assert commands[1][commands[1].index("--symbol") + 1] == "ALGOUSDT"


def test_cross_quote_flat_verification_checks_hedge(monkeypatch) -> None:
    monkeypatch.setattr(guard, "fetch_spot_symbol_config", lambda s: {"base_asset": "ALGO", "min_notional": 5})
    monkeypatch.setattr(guard, "fetch_spot_book_tickers", lambda s: [{"bid_price": 0.1}])
    monkeypatch.setattr(guard, "_spot_inventory", lambda *a: (1, 0))
    checked = []
    monkeypatch.setattr(guard, "_futures_position_qty", lambda s, *a: checked.append(s) or (0, 0))
    monkeypatch.setattr(guard, "fetch_spot_open_orders", lambda *a: [])
    monkeypatch.setattr(guard, "fetch_futures_open_orders", lambda s, *a, **k: checked.append(s) or [])
    snapshot = guard._verify_flat(args=Namespace(symbol="ALGOUSDC", hedge_symbol="ALGOUSDT"), api_key="key", api_secret="secret")
    assert snapshot["flat"] and snapshot["spot_dust"]
    assert checked == ["ALGOUSDT", "ALGOUSDT"]


def test_trade_progress_counts_usdc_commission(monkeypatch) -> None:
    monkeypatch.setattr(guard, "fetch_spot_user_trades", lambda **k: [
        {"id": 1, "time": 10, "quoteQty": "100", "commissionAsset": "USDC", "commission": "0.1"},
    ])
    state = guard._update_trade_progress(state={"quote_asset": "USDC"}, symbol="ALGOUSDC", api_key="key", api_secret="secret", start_ms=1)
    assert state["gross_notional"] == 100
    assert state["commission_quote"] == 0.1


def test_cleanup_trigger_prefers_target() -> None:
    assert cleanup_trigger(
        volume=50_000.01,
        target=50_000.0,
        armed=True,
        runner_active=True,
        inactive_since=None,
        now_monotonic=100.0,
        inactive_grace_seconds=30.0,
    ) == "target_reached"


def test_cleanup_trigger_waits_for_inactive_grace() -> None:
    assert cleanup_trigger(
        volume=100.0,
        target=50_000.0,
        armed=True,
        runner_active=False,
        inactive_since=80.0,
        now_monotonic=100.0,
        inactive_grace_seconds=30.0,
    ) is None
    assert cleanup_trigger(
        volume=100.0,
        target=50_000.0,
        armed=True,
        runner_active=False,
        inactive_since=60.0,
        now_monotonic=100.0,
        inactive_grace_seconds=30.0,
    ) == "runner_stopped"


def test_cleanup_trigger_ignores_prearm_inactive_runner() -> None:
    assert cleanup_trigger(
        volume=0.0,
        target=50_000.0,
        armed=False,
        runner_active=False,
        inactive_since=0.0,
        now_monotonic=1_000.0,
        inactive_grace_seconds=30.0,
    ) is None


def test_effective_target_uses_primary_during_observation() -> None:
    assert select_effective_target(
        primary_target=80_000.0,
        fallback_target=15_000.0,
        decision_after_seconds=3_600.0,
        elapsed_seconds=3_599.0,
        loss_per_10k=9.0,
        loss_active=True,
        loss_threshold_per_10k=3.0,
    ) == (80_000.0, "primary")


def test_effective_target_falls_back_after_observation() -> None:
    assert select_effective_target(
        primary_target=80_000.0,
        fallback_target=15_000.0,
        decision_after_seconds=3_600.0,
        elapsed_seconds=3_600.0,
        loss_per_10k=3.01,
        loss_active=True,
        loss_threshold_per_10k=3.0,
    ) == (15_000.0, "loss_fallback")


def test_effective_target_requires_active_loss_sample() -> None:
    assert select_effective_target(
        primary_target=80_000.0,
        fallback_target=15_000.0,
        decision_after_seconds=3_600.0,
        elapsed_seconds=7_200.0,
        loss_per_10k=5.0,
        loss_active=False,
        loss_threshold_per_10k=3.0,
    ) == (80_000.0, "primary")
