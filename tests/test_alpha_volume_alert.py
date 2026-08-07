from __future__ import annotations

from pathlib import Path

from grid_optimizer import alpha_volume_alert
from grid_optimizer.alpha_market import AlphaToken


def test_parse_args_from_preserves_production_defaults() -> None:
    args = alpha_volume_alert.parse_args_from(["--symbols", "QUID,GRVT"])

    assert args.symbols == "QUID,GRVT"
    assert args.interval == "1m"
    assert args.baseline_candles == 20
    assert args.absolute_min_quote_volume == 60000.0
    assert args.cooldown_minutes == 30


def _token() -> AlphaToken:
    return AlphaToken("QUID", "ALPHA_2", "QUID", "", 1.0, 0.0, 0, "ALPHA_2USDC")


def _spike(close_time_ms: int) -> alpha_volume_alert.VolumeSpike:
    return alpha_volume_alert.VolumeSpike(
        token=_token(),
        close_time_ms=close_time_ms,
        latest_quote_volume=60_000.0,
        quote_volume_1h=60_000.0,
        baseline_quote_volume=1_000.0,
        multiple=60.0,
        latest_base_volume=1.0,
        trades=1,
        last_price=1.0,
        ticker_quote_volume_24h=60_000.0,
        trigger_reason="test trigger",
    )


def test_find_spike_excludes_forming_final_kline(monkeypatch) -> None:
    monkeypatch.setattr(
        alpha_volume_alert,
        "fetch_klines",
        lambda *args, **kwargs: [
            [0, 0, 0, 0, 0, 1, 1, 1_000, 1],
            [0, 0, 0, 0, 0, 1, 2, 1_000, 1],
            [0, 0, 0, 0, 0, 1, 3, 59_999, 1],
            [0, 0, 0, 0, 0, 1, 4, 100_000, 1],
        ],
    )

    result = alpha_volume_alert.find_spike(
        _token(),
        interval="1m",
        baseline_candles=2,
        spike_multiple=3.0,
        min_quote_volume=1_000.0,
        absolute_min_quote_volume=60_000.0,
    )

    assert result is None


def test_run_deduplicates_same_close_time_and_cooldown(monkeypatch, tmp_path: Path) -> None:
    args = alpha_volume_alert.parse_args_from(["--symbols", "QUID", "--state-file", str(tmp_path / "state.json")])
    spikes = iter([_spike(1), _spike(2), _spike(1)])
    sent: list[tuple[str, str]] = []
    clock = iter([10_000.0, 10_001.0, 12_000.0])
    monkeypatch.setattr(alpha_volume_alert, "fetch_tokens", lambda: {"QUID": _token()})
    monkeypatch.setattr(alpha_volume_alert, "find_spike", lambda *args, **kwargs: next(spikes))
    monkeypatch.setattr(alpha_volume_alert, "send_email", lambda subject, body: sent.append((subject, body)))
    monkeypatch.setattr(alpha_volume_alert.time, "time", lambda: next(clock))

    assert alpha_volume_alert.run(args) == 0
    assert alpha_volume_alert.run(args) == 0
    assert alpha_volume_alert.run(args) == 0

    assert len(sent) == 1


def test_dry_run_records_and_prints_candidate_without_sending(monkeypatch, tmp_path: Path, capsys) -> None:
    state_path = tmp_path / "state.json"
    args = alpha_volume_alert.parse_args_from(["--symbols", "QUID", "--state-file", str(state_path), "--dry-run"])
    monkeypatch.setattr(alpha_volume_alert, "fetch_tokens", lambda: {"QUID": _token()})
    monkeypatch.setattr(alpha_volume_alert, "find_spike", lambda *args, **kwargs: _spike(123))
    monkeypatch.setattr(alpha_volume_alert, "send_email", lambda *args: (_ for _ in ()).throw(AssertionError("must not send")))
    monkeypatch.setattr(alpha_volume_alert.time, "time", lambda: 10_000.0)

    assert alpha_volume_alert.run(args) == 0

    assert "QUID" in capsys.readouterr().out
    assert alpha_volume_alert.load_state(state_path)["sent"]["QUID"]["close_time_ms"] == 123
