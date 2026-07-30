"""冲刺刷量引擎决策核（纯函数，无网络/无副作用）。

用途：竞赛最后一天，当大家集中上量、排名门槛快速抬高时，短时间内**确定性地**爆出大量成交，
清掉目标档门槛。这是决胜手，可靠性第一。

核心机制：taker IOC 自回转——买一口立刻卖一口。
- 为什么 taker：maker 靠市场穿越挂单成交，薄盘/特定时点不可靠；taker 主动吃单**保证成交**，
  速率只受盘口深度限制，是确定性的爆量手段。NIGHT 这种薄盘尤其只能靠它。
- 代价：每次回转付 ~2×taker费 + 价差（约 8.5/万U），但清掉档位门槛解锁的固定奖励远大于此。

安全设计（确保不掉链子）：
- 深度感知：单笔 ≤ 盘口可用深度 × 比例，绝不打穿盘口。
- 持仓中性：买卖等量瞬时回平，不留方向敞口。
- 磨损预算闸门：实测磨损超预算立即暂停。
- 目标感知：到目标量即停，不超刷。
- 价差闸门：价差过宽（成本过高）时暂停等待。

本模块只做决策。下单/查盘口/对账在 sprint_volume_runner。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True)
class SprintConfig:
    enabled: bool = False
    target_total_volume: float = 0.0      # 目标累计成交量（含已有量），达到即停——这是唯一的常规停机条件
    depth_fraction: float = 0.25          # 单笔回转名义额 ≤ 盘口最浅一侧可用深度 × 此比例
    max_order_notional: float = 300.0     # 单笔回转名义额硬上限
    min_order_notional: float = 10.0      # 低于交易所最小名义额则不值得做
    # 极端价差保护：仅在盘口明显异常（如断档/瞬时极宽）时短暂暂停重试，不是效率闸门。
    # 默认设得很宽，绝不因为"贵一点"而停——冲刺第一原则是刷到量，磨损是次要的。
    abnormal_spread_bps: float = 40.0
    # 紧价差护栏（成本可控）：只在价差 ≤ 此值时才 take，宽于此就等它收窄。
    # 0 = 不限（XAUT 这种恒 1-tick 的随便吃）；非 0 用于薄盘/会变宽价差的币（如 NIGHT），
    # 让 taker 只在价差最紧的窗口出手，避免宽价差时跨价差成本尖刺。
    # 注意：有 deadline 必须保量时应调大或设 0，否则可能一直等不到窗口。
    max_take_spread_bps: float = 0.0
    # 注意：故意没有"磨损预算"停机参数。冲刺绝不因磨损超标中途停——量不够拿不到奖励的损失
    # 远大于多花的磨损。磨损只监控上报，灾难性兜底（绝对亏损）放在执行层的 sanity kill。


def resolve_sprint_action(
    *,
    config: SprintConfig,
    current_volume: float,
    mid_price: float,
    spread_bps: float,
    top_bid_notional: float,
    top_ask_notional: float,
) -> dict[str, Any]:
    """决定下一步冲刺动作。

    返回 dict：
      action ∈ {disabled, roundtrip, stop, pause}
      roundtrip_notional：本次买/卖各自的名义额（USDT），action=roundtrip 时 >0
      reason、warnings
    一次 roundtrip = IOC 买入 roundtrip_notional + 立即 IOC 卖出等量，产生约 2×roundtrip_notional 成交量。

    第一原则：只要没到目标量就继续刷，绝不因磨损停。唯一常规停机=到目标量。
    暂停仅限"暂时无法安全成交"（无价、深度太薄、极端异常价差），是短暂等待重试不是放弃。
    """
    warnings: list[str] = []
    report: dict[str, Any] = {
        "action": "pause",
        "roundtrip_notional": 0.0,
        "reason": None,
        "remaining_volume": 0.0,
        "warnings": warnings,
    }
    if not config.enabled:
        report.update(action="disabled", reason="disabled")
        return report

    price = max(_safe_float(mid_price), 0.0)
    if price <= 0:
        report.update(action="pause", reason="no_price")
        warnings.append("price_unavailable")
        return report

    remaining = _safe_float(config.target_total_volume) - _safe_float(current_volume)
    report["remaining_volume"] = remaining
    if remaining <= 0:
        report.update(action="stop", reason="target_reached")
        return report

    # 仅防极端异常价差（断档/瞬时极宽），短暂等待重试；不是效率闸门，绝不因"贵一点"而停
    if _safe_float(spread_bps) > _safe_float(config.abnormal_spread_bps) > 0:
        report.update(action="pause", reason="abnormal_spread")
        warnings.append("abnormal_spread")
        return report

    # 紧价差护栏：价差宽于阈值就等收窄，只在最紧的窗口 take（成本可控）。0=不限。
    if _safe_float(config.max_take_spread_bps) > 0 and _safe_float(spread_bps) > _safe_float(config.max_take_spread_bps):
        report.update(action="pause", reason="spread_above_take_threshold")
        warnings.append("waiting_tighter_spread")
        return report

    avail_depth = min(max(_safe_float(top_bid_notional), 0.0), max(_safe_float(top_ask_notional), 0.0))
    depth_cap = avail_depth * max(_safe_float(config.depth_fraction), 0.0)
    # 一次 roundtrip 产生 2× 名义额的量；不超刷目标
    target_cap = remaining / 2.0
    size = min(depth_cap, _safe_float(config.max_order_notional), target_cap)

    if size < _safe_float(config.min_order_notional):
        # 因目标将达成而变小 → 停；因深度不足而变小 → 暂停等深度
        if target_cap <= _safe_float(config.max_order_notional) and target_cap < _safe_float(config.min_order_notional):
            report.update(action="stop", reason="near_target")
            return report
        report.update(action="pause", reason="depth_too_thin")
        warnings.append("depth_thin")
        return report

    report.update(action="roundtrip", roundtrip_notional=size, reason="burst")
    return report
