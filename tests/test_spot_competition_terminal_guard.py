from __future__ import annotations

from grid_optimizer.spot_competition_terminal_guard import cleanup_trigger, select_effective_target


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
