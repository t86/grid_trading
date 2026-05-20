import unittest

from grid_optimizer.strategy_diagnostics import build_strategy_diagnostics


class StrategyDiagnosticsTests(unittest.TestCase):
    def test_execution_caps_explain_partial_order_submission(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_profile": "aigensynusdt_best_quote_maker_volume_v1",
                "strategy_mode": "one_way_long",
                "required_position_mode": "one_way",
                "buy_levels": 2,
                "sell_levels": 2,
                "per_order_notional": 120.0,
                "max_new_orders": 3,
                "max_total_notional": 400.0,
            },
            startup_preflight={
                "can_start": True,
                "status": "warning",
                "warning_codes": ["global_safety_limiting_params"],
            },
            safety_preflight={
                "estimated_cycle_order_count": 4,
                "estimated_cycle_notional": 480.0,
                "limiting_params": ["max_new_orders", "max_total_notional"],
                "blocking_params": [],
                "warning_params": [],
                "stop_guard_params": [],
                "takeover_params": [],
            },
        )

        self.assertEqual(report["status"], "warning")
        self.assertTrue(report["can_start"])
        self.assertEqual(report["order_cycle"]["estimated_order_count"], 4)
        self.assertEqual(report["order_cycle"]["estimated_notional"], 480.0)
        self.assertIn("max_total_notional", _item_keys(report, "execution_caps"))
        self.assertIn("max_new_orders", _item_keys(report, "execution_caps"))
        item = _item(report, "execution_caps", "max_total_notional")
        self.assertEqual(item["severity"], "warning")
        self.assertEqual(item["category"], "limits_volume")
        self.assertIn("只提交部分挂单", item["impact"])

    def test_startup_blocker_is_promoted_to_blocked_status(self) -> None:
        report = build_strategy_diagnostics(
            config={"buy_levels": 1, "sell_levels": 1, "per_order_notional": 100.0},
            startup_preflight={
                "can_start": False,
                "status": "blocked",
                "blocker_codes": ["global_safety_blocking_params"],
                "warning_codes": [],
                "blocking_params": ["cancel_stale"],
            },
            safety_preflight={"blocking_params": ["cancel_stale"], "limiting_params": []},
        )

        self.assertFalse(report["can_start"])
        self.assertEqual(report["status"], "blocked")
        self.assertGreaterEqual(report["blocker_count"], 1)
        self.assertIn("cancel_stale", _item_keys(report, "startup"))

    def test_volume_targets_include_200k_and_500k_feasibility(self) -> None:
        report = build_strategy_diagnostics(
            config={"buy_levels": 4, "sell_levels": 4, "per_order_notional": 250.0},
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={
                "estimated_cycle_order_count": 8,
                "estimated_cycle_notional": 2_000.0,
                "limiting_params": [],
                "blocking_params": [],
            },
        )

        targets = {int(item["target_notional"]): item for item in report["volume_targets"]}
        self.assertEqual(sorted(targets), [200_000, 500_000])
        self.assertEqual(targets[200_000]["required_full_cycles_per_hour"], 100.0)
        self.assertEqual(targets[200_000]["required_seconds_per_full_cycle"], 36.0)
        self.assertEqual(targets[500_000]["required_full_cycles_per_hour"], 250.0)
        self.assertEqual(targets[500_000]["required_seconds_per_full_cycle"], 14.4)

    def test_one_way_inventory_near_soft_threshold_warns(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_mode": "one_way_long",
                "pause_buy_position_notional": 1_000.0,
                "max_position_notional": 1_500.0,
            },
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={},
            position={"long_notional": 950.0, "short_notional": 0.0, "net_notional": 950.0},
        )

        item = _item(report, "inventory_thresholds", "long_soft_threshold")
        self.assertEqual(item["severity"], "warning")
        self.assertEqual(item["category"], "forces_repair")
        self.assertIn("接近软阈值", item["why"])

    def test_one_way_inventory_over_hard_threshold_blocks(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_mode": "one_way_long",
                "pause_buy_position_notional": 1_000.0,
                "max_position_notional": 1_500.0,
            },
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={},
            position={"long_notional": 1_600.0, "short_notional": 0.0, "net_notional": 1_600.0},
        )

        item = _item(report, "inventory_thresholds", "long_hard_threshold")
        self.assertEqual(item["severity"], "blocker")
        self.assertEqual(item["category"], "forces_repair")
        self.assertEqual(report["status"], "blocked")

    def test_repair_state_is_classified_from_active_state_and_ladder(self) -> None:
        report = build_strategy_diagnostics(
            config={},
            startup_preflight={"can_start": True, "status": "warning"},
            latest_loop={"active_state": "inventory_recover", "repair_ladder_level": "passive_reduce"},
            runner_running=True,
        )

        self.assertEqual(report["mode"], "repair")
        self.assertIn("修仓状态", report["summary"])

    def test_profile_boundary_lists_ignored_and_unknown_params(self) -> None:
        report = build_strategy_diagnostics(
            config={"required_position_mode": "one_way"},
            startup_preflight={
                "can_start": True,
                "status": "warning",
                "ignored_params": ["hard_loss_forced_reduce_enabled"],
                "unknown_params": ["old_cross_strategy_knob"],
                "required_position_mode": "one_way",
                "required_position_mode_defaulted": True,
            },
        )

        keys = _item_keys(report, "profile_boundary")
        self.assertIn("ignored_params", keys)
        self.assertIn("unknown_params", keys)
        self.assertIn("required_position_mode", keys)

    def test_plan_report_no_submit_reason_classifies_blocked_mode(self) -> None:
        report = build_strategy_diagnostics(
            config={},
            startup_preflight={"can_start": True, "status": "ready"},
            plan_report={
                "active_state": "normal",
                "no_submit_reason": "position limit reached",
            },
            runner_running=True,
        )

        self.assertEqual(report["mode"], "blocked")
        state_item = _item(report, "state_machine", "state_classification")
        self.assertEqual(state_item["current_value"], "blocked")
        self.assertEqual(state_item["category"], "blocks_orders")
        self.assertIn("position limit reached", state_item["why"])

    def test_submit_report_no_submit_reason_classifies_blocked_mode(self) -> None:
        report = build_strategy_diagnostics(
            config={},
            startup_preflight={"can_start": True, "status": "ready"},
            submit_report={"no_submit_reason": "post only guard kept all orders"},
            runner_running=True,
        )

        self.assertEqual(report["mode"], "blocked")
        state_item = _item(report, "state_machine", "state_classification")
        self.assertEqual(state_item["current_value"], "blocked")
        self.assertIn("post only guard kept all orders", state_item["why"])

    def test_volume_targets_cite_config_caps_when_safety_preflight_is_stale(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "buy_levels": 2,
                "sell_levels": 2,
                "per_order_notional": 120.0,
                "max_new_orders": 3,
                "max_total_notional": 400.0,
            },
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={"limiting_params": [], "blocking_params": []},
        )

        targets = {int(item["target_notional"]): item for item in report["volume_targets"]}
        self.assertEqual(targets[200_000]["severity"], "warning")
        self.assertFalse(targets[200_000]["plausible"])
        self.assertEqual(targets[200_000]["limiting_params"], ["max_new_orders", "max_total_notional"])
        self.assertIn("max_new_orders", targets[200_000]["suggestion"])
        self.assertIn("max_total_notional", targets[200_000]["suggestion"])

    def test_startup_section_explains_schema_codes_and_profile_boundary_details(self) -> None:
        report = build_strategy_diagnostics(
            config={"required_position_mode": "one_way"},
            startup_preflight={
                "can_start": False,
                "status": "blocked",
                "strict_ok": False,
                "schema_known": False,
                "blocker_codes": ["strict_unknown_params"],
                "warning_codes": ["required_position_mode_defaulted", "ignored_params"],
                "ignored_params": ["hard_loss_forced_reduce_enabled"],
                "unknown_params": ["old_cross_strategy_knob"],
                "required_position_mode": "one_way",
                "required_position_mode_defaulted": True,
            },
        )

        keys = _item_keys(report, "startup")
        self.assertIn("blocker_codes", keys)
        self.assertIn("warning_codes", keys)
        self.assertIn("strict_schema", keys)
        self.assertIn("schema_known", keys)
        self.assertIn("ignored_params", keys)
        self.assertIn("unknown_params", keys)
        self.assertIn("required_position_mode", keys)
        self.assertEqual(_item(report, "startup", "strict_schema")["severity"], "blocker")
        self.assertEqual(_item(report, "startup", "schema_known")["severity"], "warning")
        self.assertEqual(_item(report, "startup", "unknown_params")["severity"], "blocker")


def _section(report: dict, key: str) -> dict:
    for section in report["sections"]:
        if section["key"] == key:
            return section
    raise AssertionError(f"section not found: {key}")


def _item_keys(report: dict, section_key: str) -> set[str]:
    return {item["key"] for item in _section(report, section_key)["items"]}


def _item(report: dict, section_key: str, item_key: str) -> dict:
    for item in _section(report, section_key)["items"]:
        if item["key"] == item_key:
            return item
    raise AssertionError(f"item not found: {section_key}.{item_key}")


if __name__ == "__main__":
    unittest.main()
