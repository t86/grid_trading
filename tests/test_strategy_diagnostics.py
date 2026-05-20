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

    def test_profile_boundary_shows_overlay_details(self) -> None:
        report = build_strategy_diagnostics(
            config={"required_position_mode": "one_way"},
            startup_preflight={
                "can_start": True,
                "status": "warning",
                "profile_boundary": {
                    "profile_key": "aigensynusdt_best_quote_maker_volume_v1",
                    "overlay_known": True,
                    "status": "blocked",
                    "active_allowed_params": ["per_order_notional", "elastic_volume_enabled"],
                    "active_global_safety_params": ["max_new_orders", "max_total_notional"],
                    "forbidden_active_params": ["hard_loss_forced_reduce_enabled"],
                    "required_missing_params": ["per_order_notional"],
                    "ignored_params": ["synthetic_flow_sleeve_enabled"],
                    "unknown_params": [],
                },
            },
        )

        keys = _item_keys(report, "profile_boundary")
        self.assertIn("profile_overlay", keys)
        self.assertIn("active_allowed_params", keys)
        self.assertIn("active_global_safety_params", keys)
        self.assertIn("forbidden_active_params", keys)
        self.assertIn("required_missing_params", keys)
        self.assertIn("ignored_params", keys)
        self.assertEqual(_item(report, "profile_boundary", "required_missing_params")["severity"], "blocker")
        self.assertEqual(_item(report, "profile_boundary", "forbidden_active_params")["severity"], "warning")

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

    def test_can_start_false_is_blocked_even_when_only_warning_items_exist(self) -> None:
        report = build_strategy_diagnostics(
            config={"required_position_mode": "one_way"},
            startup_preflight={
                "can_start": False,
                "status": "blocked",
                "warning_codes": ["ignored_params"],
                "ignored_params": ["legacy_knob"],
            },
        )

        self.assertFalse(report["can_start"])
        self.assertEqual(report["status"], "blocked")
        self.assertIn("startup_preflight", _item_keys(report, "startup"))
        self.assertIn("不可启动", report["summary"])

    def test_one_way_short_inventory_uses_short_thresholds(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_mode": "one_way_short",
                "pause_short_position_notional": 1_000.0,
                "max_short_position_notional": 1_500.0,
            },
            startup_preflight={"can_start": True, "status": "ready"},
            position={"long_notional": 0.0, "short_notional": 1_600.0, "net_notional": -1_600.0},
        )

        item = _item(report, "inventory_thresholds", "short_hard_threshold")
        self.assertEqual(item["severity"], "blocker")
        self.assertEqual(item["category"], "forces_repair")
        self.assertEqual(report["status"], "blocked")

    def test_global_safety_sections_explain_refresh_drift_stops_and_takeovers(self) -> None:
        report = build_strategy_diagnostics(
            config={"buy_levels": 1, "sell_levels": 1, "per_order_notional": 100.0},
            startup_preflight={"can_start": True, "status": "warning"},
            safety_preflight={
                "blocking_params": ["cancel_stale"],
                "warning_params": ["max_mid_drift_steps"],
                "stop_guard_params": ["rolling_hourly_loss_limit", "max_cumulative_notional"],
                "takeover_params": ["hard_loss_forced_reduce_enabled"],
                "items": [
                    {
                        "key": "cancel_stale",
                        "value": False,
                        "active": True,
                        "effect": "关闭后贴盘口策略遇到旧挂单时可能无法撤旧换新。",
                        "detail": "关闭时 stale orders 可能让贴盘口策略无法撤旧换新。",
                    },
                    {
                        "key": "max_mid_drift_steps",
                        "value": 1.0,
                        "active": True,
                        "effect": "计划生成后盘口漂移过大时拒绝下单。",
                        "detail": "当前允许漂移 1 steps。",
                    },
                    {
                        "key": "rolling_hourly_loss_limit",
                        "value": 30.0,
                        "active": True,
                        "effect": "滚动小时亏损达到阈值后会触发停机或冷却。",
                        "detail": "阈值 30。",
                    },
                    {
                        "key": "max_cumulative_notional",
                        "value": 200_000.0,
                        "active": True,
                        "effect": "累计刷量达到上限后停机。",
                        "detail": "阈值 200000。",
                    },
                    {
                        "key": "hard_loss_forced_reduce_enabled",
                        "value": True,
                        "active": True,
                        "effect": "亏损达到阈值后强制减仓模块可能接管挂单。",
                        "detail": "开启后亏损强减模块可能接管挂单。",
                    },
                ],
            },
        )

        self.assertIn("cancel_stale", _item_keys(report, "order_refresh"))
        self.assertIn("max_mid_drift_steps", _item_keys(report, "drift_guards"))
        self.assertIn("rolling_hourly_loss_limit", _item_keys(report, "loss_and_stop_guards"))
        self.assertIn("max_cumulative_notional", _item_keys(report, "loss_and_stop_guards"))
        self.assertIn("hard_loss_forced_reduce_enabled", _item_keys(report, "takeover_modules"))

    def test_volume_targets_warn_when_cumulative_notional_is_below_target(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "buy_levels": 8,
                "sell_levels": 8,
                "per_order_notional": 250.0,
                "max_cumulative_notional": 100_000.0,
            },
            startup_preflight={"can_start": True, "status": "warning"},
            safety_preflight={
                "estimated_cycle_order_count": 16,
                "estimated_cycle_notional": 4_000.0,
                "stop_guard_params": ["max_cumulative_notional"],
                "items": [
                    {
                        "key": "max_cumulative_notional",
                        "value": 100_000.0,
                        "active": True,
                        "effect": "累计刷量达到上限后停机。",
                        "detail": "阈值 100000。",
                    }
                ],
            },
        )

        targets = {int(item["target_notional"]): item for item in report["volume_targets"]}
        self.assertEqual(targets[200_000]["severity"], "warning")
        self.assertFalse(targets[200_000]["plausible"])
        self.assertIn("max_cumulative_notional", targets[200_000]["limiting_params"])
        self.assertIn("max_cumulative_notional", targets[200_000]["suggestion"])

    def test_takeover_section_reports_adverse_reduce_enabled(self) -> None:
        report = build_strategy_diagnostics(
            config={},
            startup_preflight={"can_start": True, "status": "warning"},
            safety_preflight={
                "takeover_params": ["adverse_reduce_enabled"],
                "items": [
                    {
                        "key": "adverse_reduce_enabled",
                        "value": True,
                        "active": True,
                        "effect": "逆向行情减仓模块可能接管挂单。",
                        "detail": "开启后逆向行情会触发减仓。",
                    }
                ],
            },
        )

        item = _item(report, "takeover_modules", "adverse_reduce_enabled")
        self.assertEqual(item["severity"], "warning")
        self.assertEqual(item["category"], "takes_over_orders")


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
