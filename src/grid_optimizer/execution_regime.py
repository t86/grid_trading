from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

REGIME_SAFE = "SAFE"
REGIME_NORMAL = "NORMAL"
REGIME_CAUTION = "CAUTION"
REGIME_EXIT = "EXIT"
REGIME_ORDER = (REGIME_SAFE, REGIME_NORMAL, REGIME_CAUTION, REGIME_EXIT)
REGIME_RANK = {name: index for index, name in enumerate(REGIME_ORDER)}

DEFAULT_RISK_WEIGHTS = {
    "vol_q": 0.30,
    "spread_q": 0.20,
    "impact_q": 0.15,
    "trend_q": 0.15,
    "anomaly_q": 0.10,
    "depth_inverse_q": 0.10,
}


@dataclass(frozen=True)
class ExecutionRegimeFeatures:
    vol_q: float | None = None
    spread_q: float | None = None
    impact_q: float | None = None
    trend_q: float | None = None
    anomaly_q: float | None = None
    depth_q: float | None = None
    valid_market_data: bool = True
    latency_ms: float | None = None
    order_failure_rate: float | None = None
    inventory_notional_abs: float | None = None
    rolling_loss_abs: float | None = None


@dataclass(frozen=True)
class ExecutionRegimeConfig:
    safe_score_upper: float = 0.35
    normal_score_upper: float = 0.60
    caution_score_upper: float = 0.80
    recover_exit_to_caution_score: float = 0.75
    recover_caution_to_normal_score: float = 0.55
    recover_normal_to_safe_score: float = 0.30
    confirm_exit_to_caution: int = 5
    confirm_caution_to_normal: int = 3
    confirm_normal_to_safe: int = 5
    confirm_normal_to_caution: int = 2
    vol_exit_q: float = 0.95
    spread_exit_q: float = 0.95
    depth_exit_q: float = 0.10
    latency_ms_exit: float | None = None
    order_failure_rate_exit: float | None = None
    inventory_notional_limit: float | None = None
    rolling_loss_limit: float | None = None


@dataclass(frozen=True)
class ExecutionRegimeMemory:
    state: str = REGIME_NORMAL
    pending_state: str | None = None
    pending_count: int = 0


@dataclass(frozen=True)
class ExecutionParams:
    should_place_orders: bool
    quote_offset_bps: float
    refresh_interval_ms: int
    order_size_pct: float
    max_active_orders: int
    allow_taker: bool
    cancel_non_protective_orders: bool
    aggressiveness: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionRegimeDecision:
    previous_state: str
    state: str
    raw_state: str
    risk_score: float
    hard_risk: bool
    reason_codes: tuple[str, ...]
    missing_features: tuple[str, ...]
    memory: ExecutionRegimeMemory
    params: ExecutionParams
    normalized_features: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        payload["missing_features"] = list(self.missing_features)
        return payload


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _positive_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return max(float(value), 0.0)


def _normalize_quantiles(features: ExecutionRegimeFeatures) -> tuple[dict[str, float], tuple[str, ...]]:
    normalized: dict[str, float] = {}
    missing: list[str] = []
    for field_name in ("vol_q", "spread_q", "impact_q", "trend_q", "anomaly_q", "depth_q"):
        raw_value = getattr(features, field_name)
        if raw_value is None:
            normalized[field_name] = 0.5
            missing.append(field_name)
            continue
        normalized[field_name] = _clamp(float(raw_value), 0.0, 1.0)
    return normalized, tuple(missing)


def calculate_risk_score(features: ExecutionRegimeFeatures) -> tuple[float, dict[str, float], tuple[str, ...]]:
    normalized, missing = _normalize_quantiles(features)
    score = (
        DEFAULT_RISK_WEIGHTS["vol_q"] * normalized["vol_q"]
        + DEFAULT_RISK_WEIGHTS["spread_q"] * normalized["spread_q"]
        + DEFAULT_RISK_WEIGHTS["impact_q"] * normalized["impact_q"]
        + DEFAULT_RISK_WEIGHTS["trend_q"] * normalized["trend_q"]
        + DEFAULT_RISK_WEIGHTS["anomaly_q"] * normalized["anomaly_q"]
        + DEFAULT_RISK_WEIGHTS["depth_inverse_q"] * (1.0 - normalized["depth_q"])
    )
    return _clamp(score, 0.0, 1.0), normalized, missing


def score_to_raw_state(score: float, config: ExecutionRegimeConfig | None = None) -> str:
    cfg = config or ExecutionRegimeConfig()
    if score >= cfg.caution_score_upper:
        return REGIME_EXIT
    if score >= cfg.normal_score_upper:
        return REGIME_CAUTION
    if score >= cfg.safe_score_upper:
        return REGIME_NORMAL
    return REGIME_SAFE


def evaluate_hard_risk(
    features: ExecutionRegimeFeatures,
    normalized_features: dict[str, float],
    config: ExecutionRegimeConfig | None = None,
) -> tuple[bool, tuple[str, ...]]:
    cfg = config or ExecutionRegimeConfig()
    reasons: list[str] = []

    if not features.valid_market_data:
        reasons.append("invalid_market_data")
    if normalized_features["vol_q"] >= cfg.vol_exit_q:
        reasons.append("vol_q_exit")
    if normalized_features["spread_q"] >= cfg.spread_exit_q:
        reasons.append("spread_q_exit")
    if normalized_features["depth_q"] <= cfg.depth_exit_q:
        reasons.append("depth_q_exit")

    latency_ms = _positive_or_none(features.latency_ms)
    if cfg.latency_ms_exit is not None and latency_ms is not None and latency_ms >= cfg.latency_ms_exit:
        reasons.append("latency_ms_exit")

    order_failure_rate = _positive_or_none(features.order_failure_rate)
    if (
        cfg.order_failure_rate_exit is not None
        and order_failure_rate is not None
        and order_failure_rate >= cfg.order_failure_rate_exit
    ):
        reasons.append("order_failure_rate_exit")

    inventory_notional_abs = _positive_or_none(features.inventory_notional_abs)
    if (
        cfg.inventory_notional_limit is not None
        and inventory_notional_abs is not None
        and inventory_notional_abs >= cfg.inventory_notional_limit
    ):
        reasons.append("inventory_notional_limit")

    rolling_loss_abs = _positive_or_none(features.rolling_loss_abs)
    if cfg.rolling_loss_limit is not None and rolling_loss_abs is not None and rolling_loss_abs >= cfg.rolling_loss_limit:
        reasons.append("rolling_loss_limit")

    return bool(reasons), tuple(reasons)


def _valid_state(value: str | None) -> str:
    if value in REGIME_RANK:
        return str(value)
    return REGIME_NORMAL


def _transition_confirmation_required(current: str, candidate: str, config: ExecutionRegimeConfig) -> int:
    if current == REGIME_EXIT and candidate == REGIME_CAUTION:
        return max(int(config.confirm_exit_to_caution), 1)
    if current == REGIME_CAUTION and candidate == REGIME_NORMAL:
        return max(int(config.confirm_caution_to_normal), 1)
    if current == REGIME_NORMAL and candidate == REGIME_SAFE:
        return max(int(config.confirm_normal_to_safe), 1)
    if current == REGIME_NORMAL and candidate == REGIME_CAUTION:
        return max(int(config.confirm_normal_to_caution), 1)
    return 1


def _next_pending_memory(
    *,
    current: str,
    candidate: str,
    previous_memory: ExecutionRegimeMemory,
    required_count: int,
) -> tuple[str, ExecutionRegimeMemory, tuple[str, ...]]:
    if previous_memory.pending_state == candidate:
        pending_count = previous_memory.pending_count + 1
    else:
        pending_count = 1

    if pending_count >= required_count:
        return (
            candidate,
            ExecutionRegimeMemory(state=candidate, pending_state=None, pending_count=0),
            ("pending_transition_confirmed",),
        )

    return (
        current,
        ExecutionRegimeMemory(state=current, pending_state=candidate, pending_count=pending_count),
        ("pending_transition",),
    )


def advance_regime_state(
    *,
    previous: ExecutionRegimeMemory | None,
    raw_state: str,
    risk_score: float,
    hard_risk: bool,
    config: ExecutionRegimeConfig | None = None,
) -> tuple[str, ExecutionRegimeMemory, tuple[str, ...]]:
    cfg = config or ExecutionRegimeConfig()
    current = _valid_state(previous.state if previous is not None else REGIME_NORMAL)
    target = _valid_state(raw_state)

    if hard_risk:
        return REGIME_EXIT, ExecutionRegimeMemory(state=REGIME_EXIT), ("hard_risk_override",)

    if current == target:
        return current, ExecutionRegimeMemory(state=current), ("state_unchanged",)

    current_rank = REGIME_RANK[current]
    target_rank = REGIME_RANK[target]
    previous_memory = previous or ExecutionRegimeMemory(state=current)

    if target_rank > current_rank:
        required = _transition_confirmation_required(current, target, cfg)
        return _next_pending_memory(
            current=current,
            candidate=target,
            previous_memory=previous_memory,
            required_count=required,
        )

    if current == REGIME_EXIT and risk_score < cfg.recover_exit_to_caution_score:
        required = _transition_confirmation_required(current, REGIME_CAUTION, cfg)
        return _next_pending_memory(
            current=current,
            candidate=REGIME_CAUTION,
            previous_memory=previous_memory,
            required_count=required,
        )

    if current == REGIME_CAUTION and risk_score < cfg.recover_caution_to_normal_score:
        required = _transition_confirmation_required(current, REGIME_NORMAL, cfg)
        return _next_pending_memory(
            current=current,
            candidate=REGIME_NORMAL,
            previous_memory=previous_memory,
            required_count=required,
        )

    if current == REGIME_NORMAL and risk_score < cfg.recover_normal_to_safe_score:
        required = _transition_confirmation_required(current, REGIME_SAFE, cfg)
        return _next_pending_memory(
            current=current,
            candidate=REGIME_SAFE,
            previous_memory=previous_memory,
            required_count=required,
        )

    return current, ExecutionRegimeMemory(state=current), ("recovery_not_confirmed",)


def build_execution_params(
    state: str,
    normalized_features: dict[str, float],
) -> ExecutionParams:
    profile_by_state = {
        REGIME_SAFE: {
            "quote": (0.5, 2.0),
            "refresh": (500, 1500),
            "order_size": (0.5, 2.0),
            "max_active_orders": 8,
            "aggressiveness": (0.70, 0.95, 0.90),
        },
        REGIME_NORMAL: {
            "quote": (2.0, 5.0),
            "refresh": (1500, 5000),
            "order_size": (0.25, 1.0),
            "max_active_orders": 4,
            "aggressiveness": (0.40, 0.70, 0.60),
        },
        REGIME_CAUTION: {
            "quote": (5.0, 15.0),
            "refresh": (5000, 15000),
            "order_size": (0.1, 0.5),
            "max_active_orders": 2,
            "aggressiveness": (0.10, 0.35, 0.25),
        },
    }

    if state == REGIME_EXIT:
        return ExecutionParams(
            should_place_orders=False,
            quote_offset_bps=0.0,
            refresh_interval_ms=0,
            order_size_pct=0.0,
            max_active_orders=0,
            allow_taker=False,
            cancel_non_protective_orders=True,
            aggressiveness=0.0,
        )

    profile = profile_by_state.get(state, profile_by_state[REGIME_NORMAL])
    aggr_min, aggr_max, aggr_base = profile["aggressiveness"]
    raw_aggressiveness = (
        aggr_base
        - 0.20 * normalized_features["vol_q"]
        - 0.15 * normalized_features["spread_q"]
        - 0.10 * normalized_features["impact_q"]
        + 0.10 * normalized_features["depth_q"]
    )
    aggressiveness = _clamp(raw_aggressiveness, aggr_min, aggr_max)
    aggr_span = max(aggr_max - aggr_min, 1e-12)
    state_ratio = (aggressiveness - aggr_min) / aggr_span

    quote_min, quote_max = profile["quote"]
    refresh_min, refresh_max = profile["refresh"]
    order_size_min, order_size_max = profile["order_size"]

    quote_offset_bps = quote_max - state_ratio * (quote_max - quote_min)
    refresh_interval_ms = int(round(refresh_max - state_ratio * (refresh_max - refresh_min)))
    order_size_pct = order_size_min + state_ratio * (order_size_max - order_size_min)

    return ExecutionParams(
        should_place_orders=True,
        quote_offset_bps=quote_offset_bps,
        refresh_interval_ms=refresh_interval_ms,
        order_size_pct=order_size_pct,
        max_active_orders=int(profile["max_active_orders"]),
        allow_taker=False,
        cancel_non_protective_orders=False,
        aggressiveness=aggressiveness,
    )


def assess_execution_regime(
    features: ExecutionRegimeFeatures,
    *,
    previous: ExecutionRegimeMemory | None = None,
    config: ExecutionRegimeConfig | None = None,
) -> ExecutionRegimeDecision:
    cfg = config or ExecutionRegimeConfig()
    risk_score, normalized_features, missing_features = calculate_risk_score(features)
    hard_risk, hard_risk_reasons = evaluate_hard_risk(features, normalized_features, cfg)
    raw_state = REGIME_EXIT if hard_risk else score_to_raw_state(risk_score, cfg)
    previous_state = _valid_state(previous.state if previous is not None else REGIME_NORMAL)
    state, memory, transition_reasons = advance_regime_state(
        previous=previous,
        raw_state=raw_state,
        risk_score=risk_score,
        hard_risk=hard_risk,
        config=cfg,
    )
    params = build_execution_params(state, normalized_features)

    reasons: list[str] = [f"score_state_{raw_state.lower()}"]
    reasons.extend(hard_risk_reasons)
    reasons.extend(transition_reasons)
    reasons.extend(f"missing_{field_name}" for field_name in missing_features)

    return ExecutionRegimeDecision(
        previous_state=previous_state,
        state=state,
        raw_state=raw_state,
        risk_score=risk_score,
        hard_risk=hard_risk,
        reason_codes=tuple(reasons),
        missing_features=missing_features,
        memory=memory,
        params=params,
        normalized_features=normalized_features,
    )


@dataclass(frozen=True)
class VolatilityPositionProtection:
    enabled: bool
    active: bool
    reason: str | None
    scale: float
    effective_max_position_notional: float | None
    effective_max_short_position_notional: float | None
    effective_max_total_notional: float | None
    effective_pause_buy_position_notional: float | None
    effective_pause_short_position_notional: float | None
    effective_inventory_tier_start_notional: float | None
    effective_inventory_tier_end_notional: float | None
    adverse_reduce_target_ratio: float
    should_cancel_non_protective_orders: bool


def get_volatility_position_protection(
    regime_state: str,
    risk_score: float,
    *,
    enabled: bool,
    max_position_notional: float | None,
    max_short_position_notional: float | None,
    max_total_notional: float | None,
    pause_buy_position_notional: float | None,
    pause_short_position_notional: float | None,
    inventory_tier_start_notional: float | None,
    inventory_tier_end_notional: float | None,
    adverse_reduce_target_ratio: float,
    cancel_non_protective_on_exit: bool = True,
) -> VolatilityPositionProtection:
    scale = 1.0
    reason = None
    should_cancel = False

    if not enabled:
        return VolatilityPositionProtection(
            enabled=False,
            active=False,
            reason=None,
            scale=1.0,
            effective_max_position_notional=max_position_notional,
            effective_max_short_position_notional=max_short_position_notional,
            effective_max_total_notional=max_total_notional,
            effective_pause_buy_position_notional=pause_buy_position_notional,
            effective_pause_short_position_notional=pause_short_position_notional,
            effective_inventory_tier_start_notional=inventory_tier_start_notional,
            effective_inventory_tier_end_notional=inventory_tier_end_notional,
            adverse_reduce_target_ratio=adverse_reduce_target_ratio,
            should_cancel_non_protective_orders=False,
        )

    if regime_state in (REGIME_CAUTION, REGIME_EXIT):
        # Linear scale: at score=0.6 → scale=1.0, at score=0.8+ → scale=0.2
        raw_scale = 1.0 - (risk_score - 0.6) * 4.0
        scale = _clamp(raw_scale, 0.2, 1.0)
        if regime_state == REGIME_EXIT:
            scale = _clamp(scale, 0.1, scale)
            should_cancel = cancel_non_protective_on_exit
            reason = f"regime={regime_state} score={risk_score:.3f} scale={scale:.2f}"
        else:
            reason = f"regime={regime_state} score={risk_score:.3f} scale={scale:.2f}"

    # Adjust adverse_reduce target_ratio: more aggressive at higher risk
    # Base ratio (e.g. 0.65) → scaled down to 0.05 at EXIT
    adjusted_adverse_target = adverse_reduce_target_ratio * scale
    adjusted_adverse_target = _clamp(adjusted_adverse_target, 0.05, 1.0)

    def _scale_limit(value: float | None) -> float | None:
        if value is None:
            return None
        return max(float(value) * scale, 0.0)

    return VolatilityPositionProtection(
        enabled=True,
        active=regime_state in (REGIME_CAUTION, REGIME_EXIT),
        reason=reason,
        scale=scale,
        effective_max_position_notional=_scale_limit(max_position_notional),
        effective_max_short_position_notional=_scale_limit(max_short_position_notional),
        effective_max_total_notional=_scale_limit(max_total_notional),
        effective_pause_buy_position_notional=_scale_limit(pause_buy_position_notional),
        effective_pause_short_position_notional=_scale_limit(pause_short_position_notional),
        effective_inventory_tier_start_notional=_scale_limit(inventory_tier_start_notional),
        effective_inventory_tier_end_notional=_scale_limit(inventory_tier_end_notional),
        adverse_reduce_target_ratio=adjusted_adverse_target,
        should_cancel_non_protective_orders=should_cancel,
    )
