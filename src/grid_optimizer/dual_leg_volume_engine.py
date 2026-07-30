"""通用 make+take 双腿刷量引擎决策核（纯函数，无网络/无副作用）。

用途：以**可控成本**刷量,通吃各种价差档位的币种。核心是"maker 优先 + taker 收口"的自回转:
每轮先在 bid 挂一条 **maker 买单**,成交后立刻 **taker 市价卖出**回平——
maker 腿在 bid 买(省掉跨价差),taker 腿保证瞬时回平(持仓中性),
全程只付"一条 maker 费 + 一条 taker 费",几乎不吃价差。

为什么比纯 taker 便宜:纯 taker 回转买在 ask、卖在 bid,白吃整个价差;
本引擎买腿走 maker 贴 bid,省掉买侧的半个价差。宽价差币(BABY ~6.6bps)省得最多
(成本约纯 taker 的一半),窄价差币省得少。

为什么比纯 maker 网格快:不傻等"买卖两条 maker 都成交",买腿一成交立刻 taker 收口,
每轮确定产出 2× 量,速率只受 maker 买腿成交快慢限制。

自适应(通用性的关键):
- 价差太窄(maker 省不了多少 / 1-tick 深队列挂不动)→ 直接退化纯 taker。
- maker 挂单超时没成交 → 撤掉,按紧迫度决定重挂(省钱)还是 taker 保量(确定)。
- deadline 紧迫 → 直接 taker 保量(由执行层算出 prefer_taker 传入)。

本模块只做决策。下单/查单/撤单/对账、成本闸和 maker→taker 切换在
dual_leg_volume_runner。
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
class DualLegConfig:
    enabled: bool = False
    target_total_volume: float = 0.0      # 目标累计成交量（含已有量），达到即停——唯一常规停机条件
    depth_fraction: float = 0.25          # 单腿名义额 ≤ 盘口最浅一侧可用深度 × 此比例
    max_order_notional: float = 200.0     # 单腿名义额硬上限
    min_order_notional: float = 10.0      # 低于交易所最小名义额则不值得做
    maker_wait_seconds: float = 6.0       # maker 挂单等待成交的耐心(秒);超时撤单转重挂或 taker
    maker_improve_ticks: int = 0          # maker 买价在 bid 上加几个 tick(仍 < ask 才算 maker);0=贴 bid
    # 价差窄于此值 → maker 省不了多少、且常是 1-tick 深队列挂不动 → 直接走 taker(通用退化)
    min_maker_spread_bps: float = 2.5
    abnormal_spread_bps: float = 60.0     # 极端异常价差(断档)短暂暂停重试,不是效率闸门
    # 成本闸和 maker→taker 切换属于运行时状态，由执行层负责。


def decide_entry(
    *,
    config: DualLegConfig,
    current_volume: float,
    mid_price: float,
    spread_bps: float,
    top_bid_notional: float,
    top_ask_notional: float,
    prefer_taker: bool = False,
) -> dict[str, Any]:
    """IDLE 态:决定本轮怎么开腿。

    返回 dict：
      action ∈ {disabled, stop, pause, post_maker, taker_roundtrip}
      mode：'maker_first' | 'taker'（仅 action=post_maker/taker_roundtrip 时有意义）
      leg_notional：本腿/本回转的名义额(USDT)
      reason、warnings
    """
    warnings: list[str] = []
    report: dict[str, Any] = {
        "action": "pause", "mode": None, "leg_notional": 0.0,
        "reason": None, "remaining_volume": 0.0, "warnings": warnings,
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

    if _safe_float(spread_bps) > _safe_float(config.abnormal_spread_bps) > 0:
        report.update(action="pause", reason="abnormal_spread")
        warnings.append("abnormal_spread")
        return report

    # 单腿定量:深度感知 + 硬上限 + 不超刷(一轮回转 ≈ 2× 腿名义额)
    avail_depth = min(max(_safe_float(top_bid_notional), 0.0), max(_safe_float(top_ask_notional), 0.0))
    depth_cap = avail_depth * max(_safe_float(config.depth_fraction), 0.0)
    target_cap = remaining / 2.0
    size = min(depth_cap, _safe_float(config.max_order_notional), target_cap)

    if size < _safe_float(config.min_order_notional):
        # 因目标将达成而变小 → 停;因深度不足而变小 → 暂停等深度
        if target_cap <= _safe_float(config.max_order_notional) and target_cap < _safe_float(config.min_order_notional):
            report.update(action="stop", reason="near_target")
            return report
        report.update(action="pause", reason="depth_too_thin")
        warnings.append("depth_thin")
        return report

    # 模式选择:价差太窄 或 紧迫 → taker;否则 maker 优先
    tight = _safe_float(spread_bps) < _safe_float(config.min_maker_spread_bps)
    if tight or prefer_taker:
        report.update(action="taker_roundtrip", mode="taker", leg_notional=size,
                      reason="spread_too_tight_for_maker" if tight else "prefer_taker_deadline")
        if tight:
            warnings.append("spread_tight_taker")
        return report

    report.update(action="post_maker", mode="maker_first", leg_notional=size, reason="maker_first")
    return report


def decide_pending(
    *,
    config: DualLegConfig,
    filled_notional: float,
    order_notional: float,
    elapsed_seconds: float,
) -> dict[str, Any]:
    """MAKER_PENDING 态:maker 买单挂着,根据成交进度/耗时决定下一步。

    返回 action ∈ {wait, complete, cancel_and_complete, cancel_unfilled}
      complete：已(近)全额成交 → 去 taker 收口
      cancel_and_complete：部分成交且超时 → 撤掉剩余,把已成交部分 taker 收口
      cancel_unfilled：超时几乎没成交 → 撤单(执行层决定重挂或 taker 保量)
      wait：继续等
    """
    filled = max(_safe_float(filled_notional), 0.0)
    order = max(_safe_float(order_notional), 0.0)
    # 近似全成(留 0.1% 容差,防浮点/部分微残)
    if order > 0 and filled >= order * 0.999:
        return {"action": "complete", "reason": "maker_filled"}
    if _safe_float(elapsed_seconds) >= _safe_float(config.maker_wait_seconds):
        if filled >= _safe_float(config.min_order_notional):
            return {"action": "cancel_and_complete", "reason": "maker_partial_timeout"}
        return {"action": "cancel_unfilled", "reason": "maker_unfilled_timeout"}
    return {"action": "wait", "reason": "maker_waiting"}


def maker_buy_price(*, bid: float, ask: float, tick_size: float, improve_ticks: int) -> float:
    """maker 买单价:贴 bid,可上浮 improve_ticks 个 tick 抢队列,但必须严格 < ask(否则越价变 taker)。"""
    b = _safe_float(bid)
    a = _safe_float(ask)
    tick = _safe_float(tick_size)
    if b <= 0:
        return 0.0
    if improve_ticks > 0 and tick > 0 and a > 0:
        improved = b + improve_ticks * tick
        # 必须仍是 maker:严格低于 ask 至少一个 tick
        if improved <= a - tick:
            return improved
    return b


def should_switch_to_taker(
    *,
    total_spread_loss: float,
    done_volume: float,
    threshold_per10k: float,
    min_volume: float,
) -> bool:
    """达到最小样本后，价格磨损超过阈值即永久切换到 taker。"""
    volume = max(_safe_float(done_volume), 0.0)
    threshold = max(_safe_float(threshold_per10k), 0.0)
    if threshold <= 0 or volume <= 0 or volume < max(_safe_float(min_volume), 0.0):
        return False
    price_wear_per10k = _safe_float(total_spread_loss) / volume * 10_000.0
    return price_wear_per10k > threshold
