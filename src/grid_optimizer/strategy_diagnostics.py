from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any


DEFAULT_VOLUME_TARGETS = (200_000.0, 500_000.0)
_STATUS_RANK = {"ok": 0, "info": 1, "ready": 1, "warning": 2, "blocker": 3, "blocked": 3, "unknown": 0}


def build_strategy_diagnostics(
    *,
    config: Mapping[str, Any],
    startup_preflight: Mapping[str, Any] | None = None,
    safety_preflight: Mapping[str, Any] | None = None,
    position: Mapping[str, Any] | None = None,
    position_mode: Mapping[str, Any] | None = None,
    latest_loop: Mapping[str, Any] | None = None,
    orders: Mapping[str, Any] | None = None,
    plan_report: Mapping[str, Any] | None = None,
    submit_report: Mapping[str, Any] | None = None,
    runner_running: bool | None = None,
    volume_targets: Sequence[float] = DEFAULT_VOLUME_TARGETS,
) -> dict[str, Any]:
    """Build a lightweight report-only diagnostic summary for strategy editor."""
    del orders, plan_report
    cfg = dict(config or {})
    startup = dict(startup_preflight or {})
    safety = dict(safety_preflight or {})
    pos = dict(position or {})
    pos_mode = dict(position_mode or {})
    loop = dict(latest_loop or {})
    submit = dict(submit_report or {})

    estimated_order_count = max(
        _as_int(
            safety.get("estimated_cycle_order_count"),
            _as_int(cfg.get("buy_levels")) + _as_int(cfg.get("sell_levels")),
        ),
        0,
    )
    estimated_notional = max(
        _as_float(
            safety.get("estimated_cycle_notional"),
            estimated_order_count * max(_as_float(cfg.get("per_order_notional")), 0.0),
        ),
        0.0,
    )

    sections = [
        _startup_section(startup, safety, pos_mode),
        _execution_caps_section(cfg, estimated_order_count, estimated_notional, safety),
        _empty_section("order_refresh", "挂单刷新"),
        _empty_section("drift_guards", "盘口漂移保护"),
        _empty_section("loss_and_stop_guards", "亏损与停止保护"),
        _empty_section("takeover_modules", "接管模块"),
        _inventory_section(cfg, pos),
        _state_section(loop, submit, runner_running),
        _profile_boundary_section(startup, cfg),
    ]
    targets = _volume_target_items(volume_targets, estimated_notional, safety)
    sections.append(
        {
            "key": "volume_targets",
            "title": "刷量目标可行性",
            "status": _max_status(item["severity"] for item in targets),
            "items": targets,
        }
    )

    blocker_count = sum(1 for section in sections for item in section["items"] if item["severity"] == "blocker")
    warning_count = sum(1 for section in sections for item in section["items"] if item["severity"] == "warning")
    status = "blocked" if blocker_count else "warning" if warning_count else "ready"
    mode = _classify_state(loop, submit, runner_running)

    return {
        "status": status,
        "can_start": blocker_count == 0 and bool(startup.get("can_start", True)),
        "mode": mode,
        "summary": _summary(status, mode, blocker_count, warning_count),
        "issue_count": blocker_count + warning_count,
        "blocker_count": blocker_count,
        "warning_count": warning_count,
        "sections": sections,
        "volume_targets": targets,
        "inventory": _inventory_snapshot(cfg, pos),
        "order_cycle": {
            "estimated_order_count": estimated_order_count,
            "estimated_notional": estimated_notional,
            "per_order_notional": _as_float(cfg.get("per_order_notional")),
            "buy_levels": _as_int(cfg.get("buy_levels")),
            "sell_levels": _as_int(cfg.get("sell_levels")),
        },
        "state": {
            "mode": mode,
            "active_state": str(loop.get("active_state") or ""),
            "repair_ladder_level": str(loop.get("repair_ladder_level") or ""),
            "error_message": str(loop.get("error_message") or submit.get("error_message") or submit.get("error") or ""),
        },
    }


def _startup_section(startup: Mapping[str, Any], safety: Mapping[str, Any], position_mode: Mapping[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    blocking_params = _string_list(startup.get("blocking_params")) or _string_list(safety.get("blocking_params"))
    for param in blocking_params:
        items.append(
            _diagnostic_item(
                key=param,
                severity="blocker",
                category="blocks_start",
                active=True,
                title=f"{param} 阻止启动",
                why=f"启动预检或全局安全预检把 {param} 标记为阻塞参数。",
                impact="Runner 启动前会被拦截，策略不会进入挂单循环。",
                suggestion=f"检查 {param} 的当前值，确认它是否应该在该策略配置中启用或调高到有效范围。",
                tradeoff="放宽启动阻塞会让策略更容易开始运行，但也会减少启动前的安全保护。",
                related_params=[param],
            )
        )

    if startup.get("can_start") is False and not items:
        codes = _string_list(startup.get("blocker_codes")) or ["startup_preflight"]
        items.append(
            _diagnostic_item(
                key="startup_preflight",
                severity="blocker",
                category="blocks_start",
                active=True,
                title="启动预检阻止启动",
                why=f"startup_preflight.can_start=false，阻塞码: {', '.join(codes)}。",
                impact="Runner 启动前会被拦截，策略不会提交挂单。",
                suggestion="先处理启动预检中的阻塞项，再启动该策略。",
                tradeoff="清除阻塞通常需要放宽某些保护或修正账户/参数状态。",
                related_params=codes,
            )
        )

    if position_mode.get("compatible") is False:
        required = str(position_mode.get("required") or "")
        current = str(position_mode.get("current") or "")
        items.append(
            _diagnostic_item(
                key="position_mode",
                severity="blocker",
                category="position_mode",
                current_value=current,
                expected_value=required,
                active=True,
                title="账户持仓模式不兼容",
                why=f"策略要求 {required or '未知'}，当前账户是 {current or '未知'}。",
                impact="提交前会被持仓模式校验拦截，避免把 hedge/one-way 策略跑在错误账户模式。",
                suggestion="切换账户持仓模式，或选择匹配当前账户模式的策略配置。",
                tradeoff="切换持仓模式可能影响同账户下其他正在运行的策略。",
                related_params=["required_position_mode"],
            )
        )

    return {
        "key": "startup",
        "title": "启动预检",
        "status": _max_status(item["severity"] for item in items),
        "items": items,
    }


def _execution_caps_section(
    config: Mapping[str, Any],
    estimated_order_count: int,
    estimated_notional: float,
    safety: Mapping[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    limiting_params = set(_string_list(safety.get("limiting_params")))
    blocking_params = set(_string_list(safety.get("blocking_params")))

    max_new_orders = _as_float(config.get("max_new_orders"))
    if max_new_orders <= 0 and ("max_new_orders" in limiting_params or "max_new_orders" in blocking_params or "max_new_orders" in config):
        items.append(
            _execution_cap_item(
                key="max_new_orders",
                severity="blocker",
                current_value=max_new_orders,
                expected_value=max(estimated_order_count, 1),
                title="单轮新挂单数量上限无效",
                why="max_new_orders 小于等于 0，执行层无法提交新的挂单。",
                impact="策略可能能启动，但每轮不会产生有效新挂单。",
            )
        )
    elif max_new_orders and estimated_order_count and max_new_orders < estimated_order_count:
        items.append(
            _execution_cap_item(
                key="max_new_orders",
                severity="warning",
                current_value=max_new_orders,
                expected_value=estimated_order_count,
                title="单轮新挂单数量上限低于理论档位数",
                why=f"当前买卖档位预计生成 {estimated_order_count} 笔挂单，但 max_new_orders 只有 {_format_number(max_new_orders)}。",
                impact="冲量策略可能每轮只提交部分挂单，表现为只出现一侧或部分档位。",
            )
        )

    max_total_notional = _as_float(config.get("max_total_notional"))
    if max_total_notional <= 0 and (
        "max_total_notional" in limiting_params or "max_total_notional" in blocking_params or "max_total_notional" in config
    ):
        items.append(
            _execution_cap_item(
                key="max_total_notional",
                severity="blocker",
                current_value=max_total_notional,
                expected_value=max(estimated_notional, 1.0),
                title="单轮名义金额上限无效",
                why="max_total_notional 小于等于 0，执行层无法给单轮挂单分配名义金额。",
                impact="策略可能能启动，但提交阶段会被容量上限挡住。",
            )
        )
    elif max_total_notional and estimated_notional and max_total_notional < estimated_notional:
        items.append(
            _execution_cap_item(
                key="max_total_notional",
                severity="warning",
                current_value=max_total_notional,
                expected_value=estimated_notional,
                title="单轮名义金额上限低于理论挂单名义金额",
                why=(
                    f"当前买卖档位和单笔金额预计生成 {_format_number(estimated_notional)}U 挂单，"
                    f"但 max_total_notional 只有 {_format_number(max_total_notional)}U。"
                ),
                impact="冲量策略可能每轮只提交部分挂单，表现为刷量速度低于预期。",
            )
        )

    return {
        "key": "execution_caps",
        "title": "执行容量限制",
        "status": _max_status(item["severity"] for item in items),
        "items": items,
    }


def _execution_cap_item(
    *,
    key: str,
    severity: str,
    current_value: float,
    expected_value: float,
    title: str,
    why: str,
    impact: str,
) -> dict[str, Any]:
    return _diagnostic_item(
        key=key,
        severity=severity,
        category="limits_volume" if severity != "blocker" else "blocks_orders",
        current_value=current_value,
        expected_value=expected_value,
        active=True,
        title=title,
        why=why,
        impact=impact,
        suggestion=f"如果要跑冲量，将 {key} 调到单轮理论需求以上，并预留盘口波动空间。",
        tradeoff="调高后单轮瞬时敞口和误成交风险都会变大。",
        related_params=["buy_levels", "sell_levels", "per_order_notional", key],
    )


def _inventory_section(config: Mapping[str, Any], position: Mapping[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    long_notional = _as_float(position.get("long_notional"))
    short_notional = _as_float(position.get("short_notional"))
    net_notional = _as_float(position.get("net_notional"), long_notional - short_notional)
    strategy_mode = str(config.get("strategy_mode") or "").strip().lower()
    mode_label = strategy_mode or "unknown"

    long_soft = _as_optional_float(config.get("pause_buy_position_notional"))
    long_hard = _as_optional_float(config.get("max_position_notional"))
    short_soft = _as_optional_float(config.get("pause_short_position_notional"))
    short_hard = _as_optional_float(config.get("max_short_position_notional"))
    if short_hard is None:
        short_hard = long_hard

    long_basis = long_notional if long_notional > 0 else max(net_notional, 0.0)
    short_basis = short_notional if short_notional > 0 else max(-net_notional, 0.0)
    one_way_long = mode_label in ("one_way_long", "long", "best_quote_maker_volume", "unknown") or "hedge" not in mode_label

    if one_way_long:
        items.extend(_threshold_items("long", "多头", long_basis, long_soft, long_hard, best_effort=(mode_label == "unknown")))
    else:
        items.extend(_threshold_items("long", "多头", long_basis, long_soft, long_hard, best_effort=False))
        items.extend(_threshold_items("short", "空头", short_basis, short_soft, short_hard, best_effort=False))

    if not items:
        items.append(
            _diagnostic_item(
                key="inventory_snapshot",
                severity="info",
                category="inventory_distance",
                current_value={"long": long_notional, "short": short_notional, "net": net_notional},
                active=False,
                title="库存阈值未配置",
                why="当前配置没有可解释的软阈值或硬阈值。",
                impact="诊断只能展示当前多空和净敞口，无法判断距离修仓阈值还有多远。",
                suggestion="如需在页面上看到修仓距离，请配置 pause_buy_position_notional 和 max_position_notional。",
                tradeoff="增加阈值后，策略可能更早暂停买入或进入修仓保护。",
                related_params=["pause_buy_position_notional", "max_position_notional"],
            )
        )

    return {
        "key": "inventory_thresholds",
        "title": "库存阈值",
        "status": _max_status(item["severity"] for item in items),
        "items": items,
    }


def _threshold_items(
    side_key: str,
    side_title: str,
    current_notional: float,
    soft_threshold: float | None,
    hard_threshold: float | None,
    *,
    best_effort: bool,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    suffix = "（best-effort）" if best_effort else ""
    if soft_threshold is not None and soft_threshold > 0:
        soft_ratio = current_notional / soft_threshold
        if soft_ratio >= 1.0:
            severity = "warning"
            why = (
                f"{side_title}库存 {_format_number(current_notional)}U 已达到软阈值 "
                f"{_format_number(soft_threshold)}U{suffix}。"
            )
            impact = "策略可能暂停同方向开仓，并更容易进入修仓或减仓路径。"
        elif soft_ratio >= 0.8:
            severity = "warning"
            why = (
                f"{side_title}库存 {_format_number(current_notional)}U 已接近软阈值 "
                f"{_format_number(soft_threshold)}U{suffix}。"
            )
            impact = "继续成交同方向订单后，策略可能很快触发暂停买入或修仓保护。"
        else:
            severity = "ok"
            why = (
                f"{side_title}库存 {_format_number(current_notional)}U 距离软阈值 "
                f"{_format_number(soft_threshold)}U 仍有 {_format_number(soft_threshold - current_notional)}U 空间{suffix}。"
            )
            impact = "当前库存距离软阈值仍有余量。"
        items.append(
            _diagnostic_item(
                key=f"{side_key}_soft_threshold",
                severity=severity,
                category="forces_repair" if severity == "warning" else "inventory_distance",
                current_value=current_notional,
                expected_value=soft_threshold,
                active=True,
                title=f"{side_title}库存软阈值",
                why=why,
                impact=impact,
                suggestion="如果这是预期库存，保持当前阈值；如果还需要持续冲量，先确认风险后再提高软阈值。",
                tradeoff="提高软阈值会延后修仓保护，也会允许更大的方向库存积累。",
                related_params=["pause_buy_position_notional" if side_key == "long" else "pause_short_position_notional"],
            )
        )

    if hard_threshold is not None and hard_threshold > 0:
        hard_ratio = current_notional / hard_threshold
        if hard_ratio >= 1.0:
            severity = "blocker"
            why = (
                f"{side_title}库存 {_format_number(current_notional)}U 已达到或超过硬阈值 "
                f"{_format_number(hard_threshold)}U{suffix}。"
            )
            impact = "策略应停止继续扩大该方向库存，执行层或修仓模块可能接管订单。"
        elif hard_ratio >= 0.9:
            severity = "warning"
            why = (
                f"{side_title}库存 {_format_number(current_notional)}U 接近硬阈值 "
                f"{_format_number(hard_threshold)}U{suffix}。"
            )
            impact = "少量继续成交就可能触发硬限制或强制修仓。"
        else:
            severity = "ok"
            why = (
                f"{side_title}库存 {_format_number(current_notional)}U 距离硬阈值 "
                f"{_format_number(hard_threshold)}U 仍有 {_format_number(hard_threshold - current_notional)}U 空间{suffix}。"
            )
            impact = "当前库存距离硬阈值仍有余量。"
        items.append(
            _diagnostic_item(
                key=f"{side_key}_hard_threshold",
                severity=severity,
                category="forces_repair" if severity in ("blocker", "warning") else "inventory_distance",
                current_value=current_notional,
                expected_value=hard_threshold,
                active=True,
                title=f"{side_title}库存硬阈值",
                why=why,
                impact=impact,
                suggestion="先降低该方向库存，或在确认风险后调整硬阈值。",
                tradeoff="提高硬阈值会允许更大的最大库存和更大的极端行情回撤。",
                related_params=["max_position_notional" if side_key == "long" else "max_short_position_notional"],
            )
        )
    return items


def _state_section(
    latest_loop: Mapping[str, Any],
    submit_report: Mapping[str, Any],
    runner_running: bool | None,
) -> dict[str, Any]:
    mode = _classify_state(latest_loop, submit_report, runner_running)
    severity = "warning" if mode in ("repair", "blocked", "unknown") else "info"
    if mode == "blocked":
        category = "blocks_orders"
    elif mode == "repair":
        category = "forces_repair"
    else:
        category = "inventory_distance"
    active_state = str(latest_loop.get("active_state") or latest_loop.get("state") or latest_loop.get("mode") or "")
    ladder = str(latest_loop.get("repair_ladder_level") or "")
    items = [
        _diagnostic_item(
            key="state_classification",
            severity=severity,
            category=category,
            current_value=mode,
            active=mode not in ("idle", "unknown"),
            title="当前运行状态分类",
            why=_state_why(mode, active_state, ladder),
            impact=_state_impact(mode),
            suggestion=_state_suggestion(mode),
            tradeoff="状态分类只解释最新轻量状态，不会改变 runner 的状态机或提交行为。",
            related_params=["active_state", "repair_ladder_level"],
        )
    ]
    return {
        "key": "state_machine",
        "title": "状态机",
        "status": _max_status(item["severity"] for item in items),
        "items": items,
    }


def _profile_boundary_section(startup: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    ignored_params = _string_list(startup.get("ignored_params"))
    unknown_params = _string_list(startup.get("unknown_params"))
    required_mode = str(startup.get("required_position_mode") or config.get("required_position_mode") or "").strip()

    if ignored_params:
        items.append(
            _diagnostic_item(
                key="ignored_params",
                severity="warning",
                category="outside_profile",
                current_value=ignored_params,
                active=True,
                title="策略边界内被忽略的参数",
                why=f"当前 profile 会忽略这些参数: {', '.join(ignored_params)}。",
                impact="这些开关或数值不会影响当前策略，页面上看似配置了，实际执行可能不会生效。",
                suggestion="确认这些参数是否属于另一个 profile；如果不需要，建议从当前策略配置中移除。",
                tradeoff="移除无效参数能减少误解，但切换到其他 profile 前需要重新确认需要的功能开关。",
                related_params=ignored_params,
            )
        )

    if unknown_params:
        severity = "blocker" if startup.get("can_start") is False else "warning"
        items.append(
            _diagnostic_item(
                key="unknown_params",
                severity=severity,
                category="outside_profile",
                current_value=unknown_params,
                active=True,
                title="策略 schema 未识别的参数",
                why=f"当前 profile schema 不认识这些参数: {', '.join(unknown_params)}。",
                impact="严格模式下未知参数会阻止启动；非严格场景下也可能代表旧配置残留或拼写错误。",
                suggestion="删除未知参数，或把它们加入对应 profile schema 后再使用。",
                tradeoff="保留未知参数不会带来功能收益，还会增加启动失败和误判风险。",
                related_params=unknown_params,
            )
        )

    if required_mode:
        severity = "warning" if bool(startup.get("required_position_mode_defaulted")) else "info"
        items.append(
            _diagnostic_item(
                key="required_position_mode",
                severity=severity,
                category="position_mode",
                current_value=required_mode,
                active=True,
                title="策略要求的账户持仓模式",
                why=(
                    f"当前 profile 要求账户持仓模式为 {required_mode}。"
                    if severity == "info"
                    else f"当前 profile 未显式声明 required_position_mode，诊断按默认 {required_mode} 解释。"
                ),
                impact="账户模式不匹配时，启动或提交前会被拦截，避免策略跑在错误持仓模型上。",
                suggestion="对 one-way 策略保持 one_way；只有明确 hedge 策略才设置 hedge。",
                tradeoff="显式声明持仓模式能减少误启动，但会让不匹配账户在启动前被拦截。",
                related_params=["required_position_mode"],
            )
        )

    if not items:
        items.append(
            _diagnostic_item(
                key="profile_boundary",
                severity="info",
                category="outside_profile",
                active=False,
                title="未发现策略边界问题",
                why="startup_preflight 没有报告 ignored_params、unknown_params 或 required_position_mode。",
                impact="当前诊断没有发现 profile 边界相关风险。",
                suggestion="保持 strict profile schema 预检开启，以便启动前暴露旧参数和拼写错误。",
                tradeoff="严格 schema 会拒绝未知参数，因此新增参数需要同步更新 profile schema。",
                related_params=[],
            )
        )

    return {
        "key": "profile_boundary",
        "title": "策略边界",
        "status": _max_status(item["severity"] for item in items),
        "items": items,
    }


def _volume_target_items(
    volume_targets: Sequence[float],
    estimated_notional: float,
    safety: Mapping[str, Any],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    limiting_params = _string_list(safety.get("limiting_params"))
    blocking_params = _string_list(safety.get("blocking_params"))
    cap_params = [param for param in ("max_new_orders", "max_total_notional") if param in limiting_params or param in blocking_params]
    cycle_notional = max(estimated_notional, 1.0)
    for target in volume_targets:
        target_notional = max(_as_float(target), 0.0)
        cycles_per_hour = target_notional / cycle_notional
        seconds_per_cycle = 3600.0 / cycles_per_hour if cycles_per_hour > 0 else 0.0
        severity = "warning" if cap_params or seconds_per_cycle < 15.0 else "info"
        items.append(
            _diagnostic_item(
                key=f"target_{int(target_notional)}",
                severity=severity,
                category="target_feasibility",
                current_value=estimated_notional,
                expected_value=target_notional,
                active=True,
                title=f"{int(target_notional):,}U 小时目标可行性",
                why=(
                    f"按单轮 {_format_number(estimated_notional)}U 估算，需要每小时 "
                    f"{_format_number(cycles_per_hour)} 个完整循环。"
                ),
                impact="这是容量可行性检查，不承诺成交；真实成交取决于深度、价差、排队位置和波动。",
                suggestion=(
                    "若目标不可行，优先提高单轮容量、缩短安全刷新周期，或降低小时目标；"
                    f"当前限制参数: {', '.join(cap_params) if cap_params else '无明显执行容量限制'}。"
                ),
                tradeoff="提高冲量能力通常会增加挂单磨损、瞬时敞口和被动成交的不确定性。",
                related_params=["buy_levels", "sell_levels", "per_order_notional", "max_new_orders", "max_total_notional"],
                extra={
                    "target_notional": target_notional,
                    "required_full_cycles_per_hour": round(cycles_per_hour, 4),
                    "required_seconds_per_full_cycle": round(seconds_per_cycle, 4),
                    "required_full_cycles_per_minute": round(cycles_per_hour / 60.0, 4),
                    "limiting_params": cap_params,
                    "plausible": not cap_params and seconds_per_cycle >= 15.0,
                },
            )
        )
    return items


def _inventory_snapshot(config: Mapping[str, Any], position: Mapping[str, Any]) -> dict[str, Any]:
    long_notional = _as_float(position.get("long_notional"))
    short_notional = _as_float(position.get("short_notional"))
    net_notional = _as_float(position.get("net_notional"), long_notional - short_notional)
    return {
        "long_notional": long_notional,
        "short_notional": short_notional,
        "net_notional": net_notional,
        "pause_buy_position_notional": _as_optional_float(config.get("pause_buy_position_notional")),
        "pause_short_position_notional": _as_optional_float(config.get("pause_short_position_notional")),
        "max_position_notional": _as_optional_float(config.get("max_position_notional")),
        "max_short_position_notional": _as_optional_float(config.get("max_short_position_notional")),
    }


def _classify_state(latest_loop: Mapping[str, Any], submit_report: Mapping[str, Any], runner_running: bool | None) -> str:
    if runner_running is False:
        return "idle"
    text = " ".join(
        str(value or "").strip().lower()
        for value in (
            latest_loop.get("active_state"),
            latest_loop.get("state"),
            latest_loop.get("mode"),
            latest_loop.get("repair_ladder_level"),
            latest_loop.get("no_submit_reason"),
            latest_loop.get("error_message"),
            submit_report.get("status"),
            submit_report.get("no_submit_reason"),
            submit_report.get("error"),
            submit_report.get("error_message"),
        )
    )
    if any(token in text for token in ("error", "refusal", "refused", "blocked", "no_submit", "no submit")):
        return "blocked"
    if any(token in text for token in ("recover", "safe", "repair", "reduce")):
        return "repair"
    if any(token in text for token in ("normal", "fast", "make", "volume")):
        return "volume"
    if runner_running is True:
        return "unknown"
    return "unknown"


def _state_why(mode: str, active_state: str, ladder: str) -> str:
    details = ", ".join(part for part in (f"active_state={active_state}" if active_state else "", f"repair_ladder_level={ladder}" if ladder else "") if part)
    if mode == "repair":
        return f"最新轻量状态包含 recover、safe、repair 或 reduce 信号，当前处于修仓状态；{details or '没有更多状态细节'}。"
    if mode == "volume":
        return f"最新轻量状态看起来是 normal、fast、make 或 volume，当前处于刷量状态；{details or '没有更多状态细节'}。"
    if mode == "blocked":
        return f"最新计划或提交报告包含错误、拒绝或 no-submit 信号；{details or '没有更多状态细节'}。"
    if mode == "idle":
        return "runner_running=false，当前未运行。"
    return f"轻量状态不足，无法可靠判断当前模式；{details or '没有更多状态细节'}。"


def _state_impact(mode: str) -> str:
    if mode == "repair":
        return "策略可能优先降低库存或减少风险，而不是追求满档冲量。"
    if mode == "volume":
        return "策略当前更可能按冲量逻辑刷新和提交挂单。"
    if mode == "blocked":
        return "策略可能已经停止提交订单，或每轮计划被拒绝。"
    if mode == "idle":
        return "未运行时不会提交挂单，只能查看静态配置和最近快照。"
    return "状态未知时，页面只能展示可用快照，不能解释当前为何不挂单。"


def _state_suggestion(mode: str) -> str:
    if mode == "repair":
        return "检查库存软/硬阈值和减仓模块，确认修仓是否符合预期。"
    if mode == "volume":
        return "若冲量不足，优先检查 execution caps、刷新逻辑和成交环境。"
    if mode == "blocked":
        return "查看 startup、execution caps 和 submit error，先处理阻塞原因。"
    if mode == "idle":
        return "启动前先确认 startup 和 profile boundary 没有 blocker。"
    return "刷新状态或查看最新 runner 事件，以获得可分类的 active_state。"


def _summary(status: str, mode: str, blocker_count: int, warning_count: int) -> str:
    mode_text = {
        "volume": "刷量状态",
        "repair": "修仓状态",
        "blocked": "阻塞状态",
        "idle": "未运行",
        "unknown": "未知状态",
    }.get(mode, "未知状态")
    if status == "blocked":
        return f"当前处于{mode_text}，有 {blocker_count} 个阻塞项和 {warning_count} 个风险项。"
    if status == "warning":
        return f"当前处于{mode_text}，可启动，但有 {warning_count} 个风险会影响冲量或状态。"
    return f"当前处于{mode_text}，未发现阻塞或风险项。"


def _empty_section(key: str, title: str) -> dict[str, Any]:
    return {"key": key, "title": title, "status": "ok", "items": []}


def _diagnostic_item(
    *,
    key: str,
    severity: str,
    category: str,
    active: bool,
    title: str,
    why: str,
    impact: str,
    suggestion: str,
    tradeoff: str,
    related_params: Sequence[str],
    current_value: Any | None = None,
    expected_value: Any | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        "key": key,
        "severity": severity,
        "category": category,
        "active": bool(active),
        "title": title,
        "why": why,
        "impact": impact,
        "suggestion": suggestion,
        "tradeoff": tradeoff,
        "related_params": list(related_params),
    }
    if current_value is not None:
        item["current_value"] = current_value
    if expected_value is not None:
        item["expected_value"] = expected_value
    if extra:
        item.update(dict(extra))
    return item


def _max_status(statuses: Iterable[str]) -> str:
    winner = "ok"
    for status in statuses:
        normalized = "blocker" if status == "blocked" else str(status or "ok")
        if _STATUS_RANK.get(normalized, 0) > _STATUS_RANK.get(winner, 0):
            winner = normalized
    return "blocked" if winner == "blocker" else winner


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Mapping):
        return [str(key) for key in value]
    if isinstance(value, Iterable):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _format_number(value: Any) -> str:
    number = _as_float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.4f}".rstrip("0").rstrip(".")
