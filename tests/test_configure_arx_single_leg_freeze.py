import json

from deploy.oracle.configure_arx_single_leg_freeze import configure


def test_configure_accepts_arx_total_frozen_cap_800(tmp_path) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    control_path.write_text(
        json.dumps(
            {
                "symbol": "ARXUSDT",
                "best_quote_maker_volume_frozen_pair_release_allow_loss": False,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "hard_loss_forced_reduce_enabled": False,
                "best_quote_maker_volume_reduce_freeze_band_budget_base_notional": 100.0,
                "best_quote_maker_volume_frozen_total_cap_notional": 800.0,
            }
        )
    )

    result = configure(control_path)

    assert result["changed"]
    updated = json.loads(control_path.read_text())
    assert updated["best_quote_maker_volume_frozen_total_cap_notional"] == 800.0
