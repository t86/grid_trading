from __future__ import annotations

from grid_optimizer.spot_competition_terminal_guard import cleanup_trigger


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
