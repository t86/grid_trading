import json

from deploy.oracle.configure_ousdt_short_freeze_reward import configure


def test_configure_arms_ousdt_short_only_reward_take_profit(tmp_path) -> None:
    control_path = tmp_path / "ousdt_loop_runner_control.json"
    control_path.write_text(
        json.dumps(
            {
                "symbol": "OUSDT",
                "best_quote_maker_volume_frozen_total_cap_notional": 400.0,
                "best_quote_maker_volume_frozen_long_cap_notional": 0.01,
                "best_quote_maker_volume_frozen_short_cap_notional": 400.0,
                "best_quote_maker_volume_frozen_pair_release_enabled": True,
                "best_quote_maker_volume_frozen_single_leg_take_profit_enabled": False,
                "best_quote_maker_volume_frozen_pair_release_min_profit_ratio": 0.01,
                "best_quote_maker_volume_frozen_pair_release_allow_loss": True,
                "best_quote_maker_volume_net_loss_reduce_enabled": True,
                "hard_loss_forced_reduce_enabled": True,
            }
        )
    )

    result = configure(control_path)

    assert result["changed"]
    updated = json.loads(control_path.read_text())
    assert updated["best_quote_maker_volume_frozen_pair_release_enabled"] is False
    assert updated["best_quote_maker_volume_frozen_single_leg_take_profit_enabled"] is True
    assert updated["best_quote_maker_volume_frozen_pair_release_min_profit_ratio"] == 0.05
    assert updated["best_quote_maker_volume_frozen_pair_release_allow_loss"] is False
    assert updated["best_quote_maker_volume_net_loss_reduce_enabled"] is False
    assert updated["hard_loss_forced_reduce_enabled"] is False
