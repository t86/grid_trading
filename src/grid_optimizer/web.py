from __future__ import annotations

import argparse
import base64
import binascii
import hmac
import ipaddress
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from collections.abc import Callable
from statistics import mean
from typing import Any, Union
from urllib.parse import parse_qs, urlparse

from .backtest import (
    build_grid_levels,
    run_backtest,
    supported_allocation_modes,
    supported_grid_level_modes,
    supported_strategy_directions,
)
from .competition_board import (
    COMPETITION_BOARD_PAGE,
    build_competition_board_snapshot,
    delete_competition_entry,
    upsert_competition_entry,
)
from .data import (
    cache_file_path,
    delete_futures_order,
    delete_spot_order,
    fetch_futures_account_info_v3,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    fetch_spot_account_info,
    fetch_spot_latest_price,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    fetch_spot_user_trades,
    load_binance_api_key,
    load_binance_borrow_lookup_mode,
    load_binance_api_credentials,
    fetch_margin_all_assets,
    fetch_margin_available_inventory,
    fetch_margin_isolated_all_pairs,
    fetch_margin_max_borrowable,
    fetch_margin_next_hourly_interest_rates,
    fetch_margin_restricted_assets,
    fetch_futures_book_tickers,
    fetch_futures_latest_price,
    fetch_futures_premium_index,
    fetch_futures_symbol_config,
    post_futures_order,
    fetch_recent_funding_records,
    fetch_spot_book_tickers,
    fetch_spot_klines,
    fetch_vip_borrow_interest_rate,
    fetch_vip_loanable_assets_data,
    funding_cache_file_path,
    load_candles_from_csv,
    load_funding_rates_from_csv,
    load_or_fetch_candles,
    load_or_fetch_funding_rates,
    load_or_fetch_futures_symbols,
    load_or_fetch_spot_symbols,
    load_or_fetch_spot_markets,
    normalize_contract_type,
    normalize_market_type,
    parse_interval_ms,
    read_latest_cached_close,
)
from .dry_run import _round_order_price, _round_order_qty
from .monitor import (
    RUNNER_CONTROL_PATH,
    RUNNER_LOG_PATH,
    RUNNER_PID_PATH,
    _read_runner_process,
    build_monitor_snapshot,
    read_symbol_runner_process,
    runner_control_path_for_symbol,
    runner_log_path_for_symbol,
    runner_pid_path_for_symbol,
)
from .maker_flatten_runner import (
    flatten_client_order_prefix,
    is_flatten_order,
    load_live_flatten_snapshot,
)
from .optimize import min_step_ratio_for_cost, objective_value, optimize_grid_count
from .runtime_guards import normalize_runtime_guard_payload
from .short_volume_candidates import build_short_volume_candidate_report
from .symbol_lists import (
    DEFAULT_SYMBOL_LISTS,
    get_symbol_list,
    load_symbol_lists,
    normalize_symbol_list_type,
    set_symbol_list,
    update_symbol_list,
)
from .types import Candle

JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
RANKING_CACHE: dict[str, dict[str, Any]] = {}
RANKING_CACHE_LOCK = threading.Lock()
BASIS_CACHE: dict[str, dict[str, Any]] = {}
BASIS_CACHE_LOCK = threading.Lock()
DETAIL_CACHE: dict[str, dict[str, Any]] = {}
DETAIL_CACHE_LOCK = threading.Lock()
LAST_FUNDING_CACHE: dict[str, dict[str, Any]] = {}
LAST_FUNDING_CACHE_LOCK = threading.Lock()
SPOT_SNAPSHOT_CACHE: dict[str, dict[str, Any]] = {}
SPOT_SNAPSHOT_CACHE_LOCK = threading.Lock()
BORROW_LOOKUP_CACHE: dict[str, dict[str, Any]] = {}
BORROW_LOOKUP_CACHE_LOCK = threading.Lock()
FUNDING_MARGIN_RATIO = 0.5
GRID_PREVIEW_MAINTENANCE_MARGIN_RATIO = 0.05
SECOND_INTERVAL_MAX_SPAN = timedelta(days=31)
STABLE_SPOT_QUOTES = ("USDT", "USDC", "FDUSD", "BUSD")
RUNNER_LOG_PATH = Path("output/night_loop_runner.log")
MONITOR_SYMBOL_OPTIONS = tuple(DEFAULT_SYMBOL_LISTS["monitor"])
CUSTOM_RUNNER_PRESETS_PATH = Path("output/custom_runner_presets.json")
RUNNER_STRATEGY_PRESETS: dict[str, dict[str, Any]] = {
    "volume_long_v4": {
        "label": "量优先做多 v4",
        "description": "当前实盘主策略。偏多滚动微网格，保留成交量，带分钟熔断和库存分层。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_long",
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 70.0,
            "base_position_notional": 420.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 750.0,
            "max_position_notional": 900.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 600.0,
            "inventory_tier_end_notional": 750.0,
            "inventory_tier_buy_levels": 4,
            "inventory_tier_sell_levels": 12,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 280.0,
        },
    },
    "volatility_defensive_v1": {
        "label": "高波动防守 v1",
        "description": "高振幅市场的保守预设。更轻底仓、更早停买、更慢追跌，并收紧分钟级熔断，优先控制损耗。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_long",
            "buy_levels": 4,
            "sell_levels": 12,
            "per_order_notional": 45.0,
            "base_position_notional": 120.0,
            "up_trigger_steps": 5,
            "down_trigger_steps": 7,
            "shift_steps": 3,
            "pause_buy_position_notional": 300.0,
            "max_position_notional": 420.0,
            "buy_pause_amp_trigger_ratio": 0.0055,
            "buy_pause_down_return_trigger_ratio": -0.0025,
            "freeze_shift_abs_return_trigger_ratio": 0.0035,
            "inventory_tier_start_notional": 220.0,
            "inventory_tier_end_notional": 320.0,
            "inventory_tier_buy_levels": 2,
            "inventory_tier_sell_levels": 14,
            "inventory_tier_per_order_notional": 45.0,
            "inventory_tier_base_position_notional": 90.0,
        },
    },
    "adaptive_volatility_v1": {
        "label": "自适应刷量/防守 v1",
        "description": "自动识别稳定与高波动状态。稳定时用量优先做多 v4，高波动或下跌扩振时切到高波动防守 v1，并带确认周期避免来回抖动。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_long",
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 70.0,
            "base_position_notional": 420.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 750.0,
            "max_position_notional": 900.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 600.0,
            "inventory_tier_end_notional": 750.0,
            "inventory_tier_buy_levels": 4,
            "inventory_tier_sell_levels": 12,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 280.0,
            "auto_regime_enabled": True,
            "auto_regime_confirm_cycles": 2,
            "auto_regime_stable_15m_max_amplitude_ratio": 0.02,
            "auto_regime_stable_60m_max_amplitude_ratio": 0.05,
            "auto_regime_stable_60m_return_floor_ratio": -0.01,
            "auto_regime_defensive_15m_amplitude_ratio": 0.035,
            "auto_regime_defensive_60m_amplitude_ratio": 0.08,
            "auto_regime_defensive_15m_return_ratio": -0.015,
            "auto_regime_defensive_60m_return_ratio": -0.03,
        },
    },
    "bard_volume_long_v2": {
        "label": "BARD 量优先做多 v2",
        "description": "BARDUSDT 专用做多预设。加入启动门禁和首轮 warm-start，避免带仓或带旧挂单时一上来补到底仓。",
        "startable": True,
        "kind": "one_way",
        "symbol": "BARDUSDT",
        "config": {
            "symbol": "BARDUSDT",
            "strategy_mode": "one_way_long",
            "step_price": 0.0002,
            "buy_levels": 5,
            "sell_levels": 11,
            "per_order_notional": 40.0,
            "base_position_notional": 120.0,
            "flat_start_enabled": True,
            "warm_start_enabled": True,
            "up_trigger_steps": 2,
            "down_trigger_steps": 2,
            "shift_steps": 2,
            "pause_buy_position_notional": 420.0,
            "max_position_notional": 560.0,
            "buy_pause_amp_trigger_ratio": 0.0048,
            "buy_pause_down_return_trigger_ratio": -0.002,
            "freeze_shift_abs_return_trigger_ratio": 0.004,
            "inventory_tier_start_notional": 260.0,
            "inventory_tier_end_notional": 380.0,
            "inventory_tier_buy_levels": 2,
            "inventory_tier_sell_levels": 14,
            "inventory_tier_per_order_notional": 35.0,
            "inventory_tier_base_position_notional": 80.0,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": True,
            "sleep_seconds": 5.0,
        },
    },
    "xaut_long_adaptive_v1": {
        "label": "XAUT 自适应做多 v1",
        "description": "仅用于 XAUTUSDT 的三态自适应做多。平稳时刷量，扩振时转防守，极端波动时立即撤买单并只保留卖单减仓。",
        "startable": True,
        "kind": "one_way",
        "symbol": "XAUTUSDT",
        "config": {
            "symbol": "XAUTUSDT",
            "strategy_mode": "one_way_long",
            "step_price": 7.5,
            "buy_levels": 6,
            "sell_levels": 10,
            "per_order_notional": 80.0,
            "base_position_notional": 320.0,
            "up_trigger_steps": 5,
            "down_trigger_steps": 4,
            "shift_steps": 3,
            "pause_buy_position_notional": 520.0,
            "max_position_notional": 680.0,
            "buy_pause_amp_trigger_ratio": 0.0060,
            "buy_pause_down_return_trigger_ratio": -0.0045,
            "freeze_shift_abs_return_trigger_ratio": 0.0048,
            "inventory_tier_start_notional": 420.0,
            "inventory_tier_end_notional": 520.0,
            "inventory_tier_buy_levels": 3,
            "inventory_tier_sell_levels": 12,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 160.0,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
    },
    "xaut_short_adaptive_v1": {
        "label": "XAUT 自适应做空 v1",
        "description": "仅用于 XAUTUSDT 的三态自适应做空。平稳时刷量，扩振时转防守，极端波动时立即撤卖单并只保留买单回补减仓。",
        "startable": True,
        "kind": "one_way",
        "symbol": "XAUTUSDT",
        "config": {
            "symbol": "XAUTUSDT",
            "strategy_mode": "one_way_short",
            "step_price": 7.6,
            "buy_levels": 10,
            "sell_levels": 6,
            "per_order_notional": 80.0,
            "base_position_notional": 260.0,
            "up_trigger_steps": 4,
            "down_trigger_steps": 5,
            "shift_steps": 3,
            "pause_short_position_notional": 600.0,
            "max_short_position_notional": 660.0,
            "inventory_tier_start_notional": 360.0,
            "inventory_tier_end_notional": 500.0,
            "inventory_tier_buy_levels": 14,
            "inventory_tier_sell_levels": 2,
            "inventory_tier_per_order_notional": 65.0,
            "inventory_tier_base_position_notional": 120.0,
            "short_cover_pause_amp_trigger_ratio": 0.0060,
            "short_cover_pause_down_return_trigger_ratio": -0.0045,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
    },
    "volume_short_v1": {
        "label": "量优先做空 v1",
        "description": "偏空滚动微网格。结构上镜像量优先做多 v4，适合 OPN 这类偏弱下跌窗口，优先在反抽中开空、在回落中回补。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_short",
            "step_price": 0.00002,
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 70.0,
            "base_position_notional": 420.0,
            "up_trigger_steps": 3,
            "down_trigger_steps": 4,
            "shift_steps": 3,
            "pause_short_position_notional": 900.0,
            "max_short_position_notional": 900.0,
            "inventory_tier_start_notional": 600.0,
            "inventory_tier_end_notional": 750.0,
            "inventory_tier_buy_levels": 12,
            "inventory_tier_sell_levels": 4,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 280.0,
            "sleep_seconds": 10.0,
            "short_cover_pause_amp_trigger_ratio": 0.0055,
            "short_cover_pause_down_return_trigger_ratio": -0.0025,
        },
    },
    "volume_short_v1_aggressive": {
        "label": "量优先做空 v1（激进）",
        "description": "激进空头滚动微网格。沿用 OPN 的高量空头参数，适合明显弱势下跌窗口，优先提升成交量。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_short",
            "step_price": 0.00002,
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 70.0,
            "base_position_notional": 420.0,
            "up_trigger_steps": 3,
            "down_trigger_steps": 4,
            "shift_steps": 3,
            "pause_short_position_notional": 900.0,
            "max_short_position_notional": 900.0,
            "inventory_tier_start_notional": 600.0,
            "inventory_tier_end_notional": 750.0,
            "inventory_tier_buy_levels": 12,
            "inventory_tier_sell_levels": 4,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 280.0,
            "sleep_seconds": 10.0,
            "short_cover_pause_amp_trigger_ratio": 0.0055,
            "short_cover_pause_down_return_trigger_ratio": -0.0025,
        },
    },
    "night_volume_short_v1": {
        "label": "NIGHT 专用做空高换手",
        "description": "针对 NIGHTUSDT 的低价高换手做空预设。仍然保留更快轮询和更近的首笔往返，但降低单笔、底仓和总空仓上限，并在急跌放量时暂停追着买回。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_short",
            "step_price": 0.00002,
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 45.0,
            "base_position_notional": 180.0,
            "up_trigger_steps": 4,
            "down_trigger_steps": 4,
            "shift_steps": 2,
            "pause_short_position_notional": 450.0,
            "max_short_position_notional": 600.0,
            "inventory_tier_start_notional": 300.0,
            "inventory_tier_end_notional": 450.0,
            "inventory_tier_buy_levels": 10,
            "inventory_tier_sell_levels": 4,
            "inventory_tier_per_order_notional": 45.0,
            "inventory_tier_base_position_notional": 90.0,
            "sleep_seconds": 10.0,
            "autotune_symbol_enabled": False,
            "short_cover_pause_amp_trigger_ratio": 0.004,
            "short_cover_pause_down_return_trigger_ratio": -0.0018,
            "freeze_shift_abs_return_trigger_ratio": 0.0045,
        },
    },
    "volume_short_v1_conservative": {
        "label": "量优先做空 v1（保守）",
        "description": "保守空头滚动微网格。更轻底仓、更低单笔和更慢追涨开空，适合 NIGHT 这类偏震荡币种试空。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_short",
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 45.0,
            "base_position_notional": 180.0,
            "up_trigger_steps": 5,
            "down_trigger_steps": 7,
            "shift_steps": 3,
            "pause_short_position_notional": 450.0,
            "max_short_position_notional": 600.0,
            "inventory_tier_start_notional": 300.0,
            "inventory_tier_end_notional": 450.0,
            "inventory_tier_buy_levels": 10,
            "inventory_tier_sell_levels": 4,
            "inventory_tier_per_order_notional": 45.0,
            "inventory_tier_base_position_notional": 90.0,
            "short_cover_pause_amp_trigger_ratio": 0.0045,
            "short_cover_pause_down_return_trigger_ratio": -0.002,
        },
    },
    "xaut_volume_short_v1": {
        "label": "XAUT 专用做空高换手",
        "description": "面向 XAUTUSDT 的高换手空头预设。维持 1000 USDT 空仓上限，收紧动态步长到现价约 0.017%，优先把小时成交额顶到 1 万附近。",
        "startable": True,
        "kind": "one_way",
        "symbol": "XAUTUSDT",
        "config": {
            "strategy_mode": "one_way_short",
            "step_price": 0.00002,
            "buy_levels": 10,
            "sell_levels": 10,
            "per_order_notional": 100.0,
            "base_position_notional": 240.0,
            "up_trigger_steps": 2,
            "down_trigger_steps": 3,
            "shift_steps": 2,
            "pause_short_position_notional": 850.0,
            "max_short_position_notional": 1000.0,
            "inventory_tier_start_notional": 650.0,
            "inventory_tier_end_notional": 850.0,
            "inventory_tier_buy_levels": 14,
            "inventory_tier_sell_levels": 6,
            "inventory_tier_per_order_notional": 100.0,
            "inventory_tier_base_position_notional": 150.0,
            "sleep_seconds": 5.0,
            "autotune_symbol_enabled": True,
        },
    },
    "defensive_quasi_neutral_aggressive_v1": {
        "label": "准中性降损激进版",
        "description": "基于 ROBO 最近实盘运行参数固化出的激进准中性版本。仍以做多为主，但提高卖侧卸仓能力、放宽总上限，适合趋势不明时保量控损。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_long",
            "step_price": 0.00001,
            "buy_levels": 8,
            "sell_levels": 16,
            "per_order_notional": 180.0,
            "base_position_notional": 300.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 1200.0,
            "max_position_notional": 1500.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 800.0,
            "inventory_tier_end_notional": 1200.0,
            "inventory_tier_buy_levels": 6,
            "inventory_tier_sell_levels": 18,
            "inventory_tier_per_order_notional": 180.0,
            "inventory_tier_base_position_notional": 180.0,
            "max_new_orders": 30,
            "max_total_notional": 5000.0,
            "sleep_seconds": 5.0,
            "autotune_symbol_enabled": False,
        },
    },
    "volume_neutral_target_v1": {
        "label": "量优先中性 v1",
        "description": "单向模式下的目标净仓中性策略。每 3 分钟重定中心，按上下 0.5% / 1% / 2% 三档目标仓位曲线挂单，并按小时缩放目标仓位，优先控下跌损耗。",
        "startable": True,
        "kind": "target_neutral",
        "config": {
            "strategy_mode": "inventory_target_neutral",
            "buy_levels": 3,
            "sell_levels": 3,
            "per_order_notional": 50.0,
            "base_position_notional": 0.0,
            "up_trigger_steps": 1,
            "down_trigger_steps": 1,
            "shift_steps": 1,
            "pause_buy_position_notional": 900.0,
            "pause_short_position_notional": 900.0,
            "max_position_notional": 900.0,
            "max_short_position_notional": 900.0,
            "max_total_notional": 1800.0,
            "buy_pause_amp_trigger_ratio": 0.009,
            "buy_pause_down_return_trigger_ratio": -0.005,
            "freeze_shift_abs_return_trigger_ratio": 0.006,
            "neutral_center_interval_minutes": 3,
            "neutral_band1_offset_ratio": 0.005,
            "neutral_band2_offset_ratio": 0.01,
            "neutral_band3_offset_ratio": 0.02,
            "neutral_band1_target_ratio": 0.20,
            "neutral_band2_target_ratio": 0.50,
            "neutral_band3_target_ratio": 1.00,
            "neutral_hourly_scale_enabled": True,
            "neutral_hourly_scale_stable": 1.0,
            "neutral_hourly_scale_transition": 0.85,
            "neutral_hourly_scale_defensive": 0.65,
        },
    },
    "defensive_quasi_neutral_v1": {
        "label": "准中性降损",
        "description": "单向兼容的降损预设。减少底仓和买侧权重，增加卖侧卸仓速度，适合量够后控损耗。",
        "startable": True,
        "kind": "one_way",
        "config": {
            "strategy_mode": "one_way_long",
            "buy_levels": 6,
            "sell_levels": 12,
            "per_order_notional": 80.0,
            "base_position_notional": 160.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 4,
            "pause_buy_position_notional": 700.0,
            "max_position_notional": 850.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
            "inventory_tier_start_notional": 500.0,
            "inventory_tier_end_notional": 650.0,
            "inventory_tier_buy_levels": 4,
            "inventory_tier_sell_levels": 14,
            "inventory_tier_per_order_notional": 80.0,
            "inventory_tier_base_position_notional": 80.0,
        },
    },
    "neutral_hedge_v1": {
        "label": "真中性 Hedge",
        "description": "双向中性微网格。Long/Short 两边独立限仓，适合量够之后压损耗。",
        "startable": True,
        "kind": "hedge",
        "config": {
            "strategy_mode": "hedge_neutral",
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 35.0,
            "base_position_notional": 140.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 6,
            "shift_steps": 4,
            "pause_buy_position_notional": 450.0,
            "pause_short_position_notional": 450.0,
            "max_position_notional": 500.0,
            "max_short_position_notional": 500.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
        },
    },
    "synthetic_neutral_v1": {
        "label": "单向合成中性",
        "description": "单向持仓下的合成中性微网格。内部维护虚拟 long/short 两本账，适合不方便切 Hedge Mode 的账户。",
        "startable": True,
        "kind": "synthetic",
        "config": {
            "strategy_mode": "synthetic_neutral",
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 35.0,
            "base_position_notional": 140.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 6,
            "shift_steps": 4,
            "pause_buy_position_notional": 450.0,
            "pause_short_position_notional": 450.0,
            "max_position_notional": 500.0,
            "max_short_position_notional": 500.0,
            "buy_pause_amp_trigger_ratio": 0.0075,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.005,
        },
    },
}
RUNNER_DEFAULT_CONFIG: dict[str, Any] = {
    "strategy_profile": "volume_long_v4",
    "strategy_mode": "one_way_long",
    "symbol": "NIGHTUSDT",
    "step_price": 0.00002,
    "buy_levels": 8,
    "sell_levels": 8,
    "per_order_notional": 70.0,
    "base_position_notional": 420.0,
    "center_price": None,
    "flat_start_enabled": True,
    "warm_start_enabled": True,
    "fixed_center_enabled": False,
    "fixed_center_roll_enabled": False,
    "excess_inventory_reduce_only_enabled": False,
    "custom_grid_roll_enabled": False,
    "custom_grid_roll_interval_minutes": 5,
    "custom_grid_roll_trade_threshold": 100,
    "custom_grid_roll_upper_distance_ratio": 0.30,
    "custom_grid_roll_shift_levels": 1,
    "autotune_symbol_enabled": True,
    "up_trigger_steps": 6,
    "down_trigger_steps": 4,
    "shift_steps": 4,
    "pause_buy_position_notional": 750.0,
    "max_position_notional": 900.0,
    "buy_pause_amp_trigger_ratio": 0.0075,
    "buy_pause_down_return_trigger_ratio": -0.0035,
    "freeze_shift_abs_return_trigger_ratio": 0.005,
    "auto_regime_enabled": False,
    "auto_regime_confirm_cycles": 2,
    "auto_regime_stable_15m_max_amplitude_ratio": 0.02,
    "auto_regime_stable_60m_max_amplitude_ratio": 0.05,
    "auto_regime_stable_60m_return_floor_ratio": -0.01,
    "auto_regime_defensive_15m_amplitude_ratio": 0.035,
    "auto_regime_defensive_60m_amplitude_ratio": 0.08,
    "auto_regime_defensive_15m_return_ratio": -0.015,
    "auto_regime_defensive_60m_return_ratio": -0.03,
    "neutral_center_interval_minutes": 3,
    "neutral_band1_offset_ratio": 0.005,
    "neutral_band2_offset_ratio": 0.01,
    "neutral_band3_offset_ratio": 0.02,
    "neutral_band1_target_ratio": 0.20,
    "neutral_band2_target_ratio": 0.50,
    "neutral_band3_target_ratio": 1.00,
    "neutral_hourly_scale_enabled": False,
    "neutral_hourly_scale_stable": 1.0,
    "neutral_hourly_scale_transition": 0.85,
    "neutral_hourly_scale_defensive": 0.65,
    "inventory_tier_start_notional": 600.0,
    "inventory_tier_end_notional": 750.0,
    "inventory_tier_buy_levels": 4,
    "inventory_tier_sell_levels": 12,
    "inventory_tier_per_order_notional": 70.0,
    "inventory_tier_base_position_notional": 280.0,
    "margin_type": "KEEP",
    "leverage": 2,
    "max_plan_age_seconds": 30,
    "max_mid_drift_steps": 4.0,
    "maker_retries": 2,
    "max_new_orders": 20,
    "max_total_notional": 1000.0,
    "run_start_time": None,
    "run_end_time": None,
    "rolling_hourly_loss_limit": None,
    "max_cumulative_notional": None,
    "sleep_seconds": 15.0,
    "cancel_stale": True,
    "apply": True,
    "reset_state": True,
    "state_path": "output/night_large_state.json",
    "plan_json": "output/night_loop_latest_plan.json",
    "submit_report_json": "output/night_loop_latest_submit.json",
    "summary_jsonl": "output/night_loop_events.jsonl",
}
RUNNER_SERVICE_NAME = "grid-loop.service"
RUNNER_LAUNCH_AGENT_LABEL = "com.tl.grid-optimizer.loop"
RUNNER_LAUNCH_AGENT_PATH = Path.home() / "Library/LaunchAgents" / f"{RUNNER_LAUNCH_AGENT_LABEL}.plist"
WEB_AUTH_REALM = "grid-web"
IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]


def _security_headers() -> dict[str, str]:
    return {
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
    }


def _load_web_auth_credentials() -> tuple[str, str] | None:
    username = os.environ.get("GRID_WEB_USERNAME", "").strip()
    password = os.environ.get("GRID_WEB_PASSWORD", "")
    if username and password:
        return username, password
    return None


def _basic_auth_header_matches(header_value: str | None, expected_username: str, expected_password: str) -> bool:
    raw = str(header_value or "").strip()
    if not raw or not raw.lower().startswith("basic "):
        return False
    token = raw.split(" ", 1)[1].strip()
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return False
    username, sep, password = decoded.partition(":")
    if not sep:
        return False
    return hmac.compare_digest(username, expected_username) and hmac.compare_digest(password, expected_password)


def _parse_allowed_networks(raw_value: str | None) -> list[IPNetwork]:
    networks: list[IPNetwork] = []
    raw = str(raw_value or "").strip()
    if not raw:
        return networks
    for token in raw.split(","):
        item = token.strip()
        if not item:
            continue
        try:
            if "/" not in item:
                address = ipaddress.ip_address(item)
                suffix = "32" if address.version == 4 else "128"
                item = f"{item}/{suffix}"
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            continue
    return networks


def _client_ip_allowed(client_ip: str | None, allowed_networks: list[IPNetwork]) -> bool:
    normalized = str(client_ip or "").strip()
    if not normalized:
        return False
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    if address.is_loopback:
        return True
    if not allowed_networks:
        return True
    return any(address in network for network in allowed_networks)


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _legacy_runner_symbol() -> str:
    stored = _read_json_dict(RUNNER_CONTROL_PATH) or {}
    symbol = str(stored.get("symbol", "")).upper().strip()
    if symbol:
        return symbol
    runner = _read_runner_process()
    symbol = str((runner.get("config") or {}).get("symbol", "")).upper().strip()
    if symbol:
        return symbol
    return str(RUNNER_DEFAULT_CONFIG.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"


def _runner_service_template() -> str:
    return str(os.environ.get("GRID_RUNNER_SERVICE_TEMPLATE", "")).strip()


def _symbol_runner_systemd_enabled() -> bool:
    return bool(_runner_service_template())


def _uses_legacy_runner(symbol: str) -> bool:
    if _symbol_runner_systemd_enabled():
        return False
    normalized_symbol = str(symbol or "").upper().strip()
    if not normalized_symbol:
        return True
    return normalized_symbol == _legacy_runner_symbol()


def _runner_pid_path(symbol: str) -> Path:
    return RUNNER_PID_PATH if _uses_legacy_runner(symbol) else runner_pid_path_for_symbol(symbol)


def _runner_control_path(symbol: str) -> Path:
    return RUNNER_CONTROL_PATH if _uses_legacy_runner(symbol) else runner_control_path_for_symbol(symbol)


def _runner_log_path(symbol: str) -> Path:
    return RUNNER_LOG_PATH if _uses_legacy_runner(symbol) else runner_log_path_for_symbol(symbol)


def _read_runner_process_for_symbol(symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").upper().strip()
    if _uses_legacy_runner(normalized_symbol):
        return _read_runner_process()
    return read_symbol_runner_process(normalized_symbol)


def _load_runner_control_config(symbol: str | None = None) -> dict[str, Any]:
    config = dict(RUNNER_DEFAULT_CONFIG)
    normalized_symbol = str(symbol or config.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"
    stored = _read_json_dict(_runner_control_path(normalized_symbol))
    if stored:
        config.update(stored)
    runner = _read_runner_process_for_symbol(normalized_symbol)
    if runner.get("config"):
        config.update(runner["config"])
    config = _normalize_runner_runtime_paths(config, normalized_symbol)
    return config


def _flatten_pid_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_maker_flatten.pid")


def _flatten_control_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_maker_flatten_control.json")


def _flatten_log_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_maker_flatten.log")


def _flatten_events_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_maker_flatten_events.jsonl")


def _read_flatten_process_for_symbol(symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").upper().strip()
    runner = _read_runner_process(
        pid_path=_flatten_pid_path(normalized_symbol),
        control_path=_flatten_control_path(normalized_symbol),
    )
    runner["scope"] = "flatten"
    runner["symbol"] = normalized_symbol
    return runner


def _save_flatten_control_config(config: dict[str, Any], *, symbol: str | None = None) -> None:
    normalized_symbol = str(symbol or config.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"
    control_path = _flatten_control_path(normalized_symbol)
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_runner_control_config(config: dict[str, Any], *, symbol: str | None = None) -> None:
    normalized_symbol = str(symbol or config.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"
    control_path = _runner_control_path(normalized_symbol)
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def _symbol_output_slug(symbol: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(symbol or "").strip()).strip("_").lower()
    return normalized or "symbol"


def _runner_service_name_for_symbol(symbol: str | None = None) -> str | None:
    normalized_symbol = str(symbol or "").upper().strip()
    template = _runner_service_template()
    if template and normalized_symbol:
        return template.format(
            symbol=normalized_symbol,
            symbol_upper=normalized_symbol,
            symbol_lower=normalized_symbol.lower(),
            slug=_symbol_output_slug(normalized_symbol),
        )
    if normalized_symbol and _uses_legacy_runner(normalized_symbol):
        return RUNNER_SERVICE_NAME
    return None


def _run_systemctl(args: list[str], *, check: bool = False, capture_output: bool = False, text: bool = False) -> subprocess.CompletedProcess[str]:
    commands: list[list[str]] = [["systemctl", *args]]
    if shutil.which("sudo"):
        commands.append(["sudo", "-n", "systemctl", *args])

    last_result: subprocess.CompletedProcess[str] | None = None
    for command in commands:
        result = subprocess.run(
            command,
            capture_output=capture_output,
            text=text,
            check=False,
        )
        last_result = result
        if result.returncode == 0:
            return result

    if check and last_result is not None:
        raise subprocess.CalledProcessError(
            last_result.returncode,
            last_result.args,
            output=last_result.stdout,
            stderr=last_result.stderr,
        )
    if last_result is None:
        raise RuntimeError("systemctl execution was not attempted")
    return last_result


def _default_runtime_paths_for_symbol(symbol: str) -> dict[str, str]:
    slug = _symbol_output_slug(symbol)
    return {
        "state_path": f"output/{slug}_loop_state.json",
        "plan_json": f"output/{slug}_loop_latest_plan.json",
        "submit_report_json": f"output/{slug}_loop_latest_submit.json",
        "summary_jsonl": f"output/{slug}_loop_events.jsonl",
    }


def _normalize_runner_runtime_paths(config: dict[str, Any], symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol or config.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"
    runtime_paths = _default_runtime_paths_for_symbol(normalized_symbol)
    normalized = dict(config)
    for key, expected in runtime_paths.items():
        raw_value = str(normalized.get(key, "")).strip()
        if not raw_value:
            normalized[key] = expected
            continue
        path = Path(raw_value)
        # Only auto-correct default-style output paths that were accidentally copied from another symbol.
        if path.is_absolute() or path.parts[:1] != ("output",):
            continue
        if "_loop_" not in path.name:
            continue
        if raw_value != expected:
            normalized[key] = expected
    return normalized


SPOT_RUNNER_DEFAULT_CONFIG: dict[str, Any] = {
    "market_type": "spot",
    "strategy_mode": "spot_one_way_long",
    "symbol": "BTCUSDT",
    "grid_level_mode": "arithmetic",
    "min_price": 50000.0,
    "max_price": 130000.0,
    "n": 20,
    "total_quote_budget": 1000.0,
    "sleep_seconds": 10.0,
    "cancel_stale": True,
    "apply": True,
    "reset_state": True,
    "state_path": "output/spot_btcusdt_state.json",
    "summary_jsonl": "output/spot_btcusdt_events.jsonl",
    "client_order_prefix": "sgbtc",
    "grid_band_ratio": 0.045,
    "attack_buy_levels": 14,
    "attack_sell_levels": 22,
    "attack_per_order_notional": 20.0,
    "defense_buy_levels": 8,
    "defense_sell_levels": 20,
    "defense_per_order_notional": 12.0,
    "inventory_soft_limit_notional": 350.0,
    "inventory_hard_limit_notional": 500.0,
    "center_shift_trigger_ratio": 0.012,
    "center_shift_confirm_cycles": 3,
    "center_shift_step_ratio": 0.006,
    "buy_pause_amp_trigger_ratio": 0.045,
    "buy_pause_down_return_trigger_ratio": -0.022,
    "freeze_shift_abs_return_trigger_ratio": 0.03,
    "inventory_recycle_age_minutes": 40.0,
    "inventory_recycle_loss_tolerance_ratio": 0.006,
    "inventory_recycle_min_profit_ratio": 0.001,
    "max_single_cycle_new_orders": 8,
    "run_start_time": None,
    "run_end_time": None,
    "rolling_hourly_loss_limit": None,
    "max_cumulative_notional": None,
}


def _spot_runner_pid_path(symbol: str) -> Path:
    slug = _symbol_output_slug(symbol)
    return Path(f"output/{slug}_spot_loop_runner.pid")


def _spot_runner_control_path(symbol: str) -> Path:
    slug = _symbol_output_slug(symbol)
    return Path(f"output/{slug}_spot_loop_runner_control.json")


def _spot_runner_log_path(symbol: str) -> Path:
    slug = _symbol_output_slug(symbol)
    return Path(f"output/{slug}_spot_loop_runner.log")


def _spot_client_order_prefix(symbol: str) -> str:
    slug = _symbol_output_slug(symbol)
    return f"sg{slug[:8]}"


def _default_spot_runtime_paths_for_symbol(symbol: str) -> dict[str, str]:
    slug = _symbol_output_slug(symbol)
    return {
        "state_path": f"output/{slug}_spot_state.json",
        "summary_jsonl": f"output/{slug}_spot_events.jsonl",
        "client_order_prefix": _spot_client_order_prefix(symbol),
    }


def _read_spot_runner_process_for_symbol(symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol or SPOT_RUNNER_DEFAULT_CONFIG["symbol"]).upper().strip() or "BTCUSDT"
    return _read_runner_process(
        pid_path=_spot_runner_pid_path(normalized_symbol),
        control_path=_spot_runner_control_path(normalized_symbol),
    )


def _load_spot_runner_control_config(symbol: str | None = None) -> dict[str, Any]:
    config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
    normalized_symbol = str(symbol or config.get("symbol", "BTCUSDT")).upper().strip() or "BTCUSDT"
    config["symbol"] = normalized_symbol
    runtime_paths = _default_spot_runtime_paths_for_symbol(normalized_symbol)
    for key, value in runtime_paths.items():
        config[key] = value
    stored = _read_json_dict(_spot_runner_control_path(normalized_symbol))
    if stored:
        config.update(stored)
    runner = _read_spot_runner_process_for_symbol(normalized_symbol)
    if runner.get("config"):
        config.update(runner["config"])
    config["symbol"] = str(config.get("symbol", normalized_symbol)).upper().strip() or normalized_symbol
    runtime_paths = _default_spot_runtime_paths_for_symbol(str(config["symbol"]))
    for key, value in runtime_paths.items():
        config.setdefault(key, value)
    return config


def _save_spot_runner_control_config(config: dict[str, Any], *, symbol: str | None = None) -> None:
    normalized_symbol = str(symbol or config.get("symbol", "BTCUSDT")).upper().strip() or "BTCUSDT"
    control_path = _spot_runner_control_path(normalized_symbol)
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def _tail_jsonl_dicts(path: Path, limit: int = 20) -> list[dict[str, Any]]:
    if limit <= 0 or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows[-limit:]


def _is_spot_strategy_order(order: dict[str, Any], prefix: str) -> bool:
    client_order_id = str(order.get("clientOrderId", "") or "")
    return client_order_id.startswith(prefix)


def _extract_spot_balance(account_info: dict[str, Any], asset: str) -> tuple[float, float]:
    wanted = str(asset or "").upper().strip()
    balances = account_info.get("balances", [])
    if not isinstance(balances, list):
        return 0.0, 0.0
    for item in balances:
        if not isinstance(item, dict):
            continue
        if str(item.get("asset", "")).upper().strip() != wanted:
            continue
        return _safe_float(item.get("free"), f"{wanted}.free"), _safe_float(item.get("locked"), f"{wanted}.locked")
    return 0.0, 0.0


def _round_up_to_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    units = math.ceil(max(float(value), 0.0) / float(step))
    return units * float(step)


def _autotune_runner_symbol_config(config: dict[str, Any]) -> dict[str, Any]:
    tuned = dict(config)
    if not _safe_bool(tuned.get("autotune_symbol_enabled", True), "autotune_symbol_enabled"):
        return tuned
    symbol = str(tuned.get("symbol", "")).upper().strip()
    if not symbol:
        return tuned

    try:
        symbol_info = fetch_futures_symbol_config(symbol)
        book_rows = fetch_futures_book_tickers(symbol=symbol)
    except Exception:
        return tuned

    book = book_rows[0] if book_rows else {}
    bid_price = _safe_float(book.get("bid_price"), "bid_price")
    ask_price = _safe_float(book.get("ask_price"), "ask_price")
    mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else 0.0
    tick_size = _safe_float(symbol_info.get("tick_size"), "tick_size")
    step_size = _safe_float(symbol_info.get("step_size"), "step_size")
    min_qty = _safe_float(symbol_info.get("min_qty"), "min_qty")
    min_notional = _safe_float(symbol_info.get("min_notional"), "min_notional")
    spread = max(ask_price - bid_price, 0.0)

    profile = str(tuned.get("strategy_profile", "volume_long_v4")).strip() or "volume_long_v4"
    step_ratio = 0.0004
    min_ticks = 2
    if profile == "volatility_defensive_v1":
        step_ratio = 0.0008
        min_ticks = 4
    elif profile == "xaut_volume_short_v1":
        step_ratio = 0.00017
        min_ticks = 2
    elif profile == "volume_neutral_target_v1":
        step_ratio = 0.0006
        min_ticks = 3
    elif profile in {"neutral_hedge_v1", "synthetic_neutral_v1"}:
        step_ratio = 0.0005
        min_ticks = 3
    desired_step = max(mid_price * step_ratio, tick_size * min_ticks, spread * 2.0)
    if desired_step > 0:
        tuned["step_price"] = _round_up_to_step(desired_step, tick_size if tick_size > 0 else None)

    minimum_working_notional = 0.0
    if min_notional > 0:
        minimum_working_notional = max(minimum_working_notional, min_notional * 4.0)
    if min_qty > 0 and mid_price > 0:
        minimum_working_notional = max(minimum_working_notional, min_qty * mid_price * 2.0)
    if minimum_working_notional > 0:
        tuned["per_order_notional"] = max(_safe_float(tuned.get("per_order_notional"), "per_order_notional"), minimum_working_notional)
        base_position_notional = _safe_float(tuned.get("base_position_notional"), "base_position_notional")
        if base_position_notional > 0:
            tuned["base_position_notional"] = max(base_position_notional, tuned["per_order_notional"] * 2.0)

    max_new_orders = max(_safe_int(tuned.get("max_new_orders", 20), "max_new_orders"), 1)
    current_max_total_notional = _safe_float(tuned.get("max_total_notional"), "max_total_notional")
    strategy_mode = str(tuned.get("strategy_mode", "one_way_long")).strip() or "one_way_long"
    max_long_raw = tuned.get("max_position_notional")
    max_short_raw = tuned.get("max_short_position_notional")
    max_long_notional = 0.0 if max_long_raw in {None, ""} else _safe_float(max_long_raw, "max_position_notional")
    max_short_notional = 0.0 if max_short_raw in {None, ""} else _safe_float(max_short_raw, "max_short_position_notional")
    target_dual_side_total = 0.0
    if strategy_mode in {"hedge_neutral", "synthetic_neutral", "inventory_target_neutral"}:
        if max_long_notional > 0:
            target_dual_side_total += max_long_notional
        if max_short_notional > 0:
            target_dual_side_total += max_short_notional
    if current_max_total_notional > 0:
        tuned["max_total_notional"] = max(
            current_max_total_notional,
            tuned["per_order_notional"] * min(max_new_orders, 12),
            target_dual_side_total,
        )
    return tuned


def _load_custom_runner_presets() -> dict[str, dict[str, Any]]:
    payload = _read_json_dict(CUSTOM_RUNNER_PRESETS_PATH)
    if not payload:
        return {}
    return {str(key): value for key, value in payload.items() if isinstance(value, dict)}


def _save_custom_runner_presets(presets: dict[str, dict[str, Any]]) -> None:
    CUSTOM_RUNNER_PRESETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CUSTOM_RUNNER_PRESETS_PATH.write_text(json.dumps(presets, ensure_ascii=False, indent=2), encoding="utf-8")


def _current_symbol_lists() -> dict[str, list[str]]:
    return load_symbol_lists()


def _current_monitor_symbols() -> list[str]:
    return _current_symbol_lists()["monitor"]


def _current_competition_symbols() -> list[str]:
    return _current_symbol_lists()["competition"]


def _runner_preset_map(symbol: str | None = None) -> dict[str, dict[str, Any]]:
    requested_symbol = str(symbol or "").upper().strip()
    merged: dict[str, dict[str, Any]] = {}
    for key, item in RUNNER_STRATEGY_PRESETS.items():
        preset_symbol = str(item.get("symbol", "")).upper().strip()
        if requested_symbol and preset_symbol and preset_symbol != requested_symbol:
            continue
        merged[key] = item
    for key, item in _load_custom_runner_presets().items():
        preset_symbol = str(item.get("symbol", "")).upper().strip()
        if requested_symbol and preset_symbol and preset_symbol != requested_symbol:
            continue
        merged[key] = item
    return merged


def _runner_preset_payload(profile: str, base_config: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = str(profile or "").strip()
    requested_symbol = str((base_config or {}).get("symbol", "")).upper().strip()
    preset = _runner_preset_map(requested_symbol).get(normalized)
    if preset is None:
        preset = _runner_preset_map().get(normalized)
    if preset is None:
        raise ValueError(f"Unknown strategy_profile: {profile}")
    preset_symbol = str(preset.get("symbol", "")).upper().strip()
    config = dict(RUNNER_DEFAULT_CONFIG)
    config.update(preset.get("config", {}))
    if base_config:
        config.update(base_config)
    resolved_symbol = str(config.get("symbol", "")).upper().strip()
    if preset_symbol and resolved_symbol and resolved_symbol != preset_symbol:
        raise ValueError(f"{preset.get('label', normalized)} requires symbol={preset_symbol}")
    if preset.get("kind") == "custom_grid":
        preview_params = dict(preset.get("grid_preview_params") or {})
        preview_summary = dict(preset.get("preview_summary") or {})
        config.setdefault("custom_grid_enabled", True)
        if preview_params:
            config.setdefault("custom_grid_direction", preview_params.get("strategy_direction"))
            config.setdefault("custom_grid_level_mode", preview_params.get("grid_level_mode"))
            config.setdefault("custom_grid_min_price", preview_params.get("min_price"))
            config.setdefault("custom_grid_max_price", preview_params.get("max_price"))
            config.setdefault("custom_grid_n", preview_params.get("n"))
            config.setdefault(
                "custom_grid_total_notional",
                _safe_float(preview_params.get("margin_amount"), "margin_amount")
                * _safe_float(preview_params.get("leverage"), "leverage"),
            )
        if config.get("custom_grid_direction") == "neutral":
            config.setdefault(
                "custom_grid_neutral_anchor_price",
                _safe_float(preview_summary.get("neutral_anchor_price"), "neutral_anchor_price"),
            )
        config = _normalize_custom_grid_runtime_config(config)
    config["strategy_profile"] = normalized
    return config


def _runner_preset_summaries(symbol: str | None = None) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for key, item in _runner_preset_map(symbol).items():
        config = item.get("config", {})
        summaries.append(
            {
                "key": key,
                "label": item.get("label", key),
                "description": item.get("description", ""),
                "startable": bool(item.get("startable", True)),
                "kind": item.get("kind", "one_way"),
                "custom": bool(item.get("custom", False)),
                "symbol": item.get("symbol"),
                "strategy_mode": config.get("strategy_mode", "one_way_long"),
                "buy_levels": config.get("buy_levels"),
                "sell_levels": config.get("sell_levels"),
                "per_order_notional": config.get("per_order_notional"),
                "base_position_notional": config.get("base_position_notional"),
                "pause_buy_position_notional": config.get("pause_buy_position_notional"),
                "max_position_notional": config.get("max_position_notional"),
                "pause_short_position_notional": config.get("pause_short_position_notional"),
                "max_short_position_notional": config.get("max_short_position_notional"),
                "auto_regime_enabled": config.get("auto_regime_enabled"),
                "grid_preview_params": item.get("grid_preview_params") if item.get("custom") else None,
                "preview_summary": item.get("preview_summary") if item.get("custom") else None,
            }
        )
    return summaries


def _slugify_runner_preset_name(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", str(name or "").strip()).strip("_").lower()
    return slug or "grid"


def _custom_runner_preset_key(symbol: str, name: str) -> str:
    base = f"custom_grid_{_symbol_output_slug(symbol)}_{_slugify_runner_preset_name(name)}"
    existing = set(_runner_preset_map().keys())
    if base not in existing:
        return base
    index = 2
    while f"{base}_{index}" in existing:
        index += 1
    return f"{base}_{index}"


def _normalize_grid_strategy_create_payload(payload: dict[str, Any]) -> dict[str, Any]:
    params = _normalize_grid_preview_payload(payload)
    name = str(payload.get("name", "")).strip()
    if not name:
        raise ValueError("strategy name is required")
    params["custom_grid_roll_enabled"] = _safe_bool(payload.get("custom_grid_roll_enabled", False), "custom_grid_roll_enabled")
    params["custom_grid_roll_interval_minutes"] = _safe_int(
        payload.get("custom_grid_roll_interval_minutes", 5),
        "custom_grid_roll_interval_minutes",
    )
    params["custom_grid_roll_trade_threshold"] = _safe_int(
        payload.get("custom_grid_roll_trade_threshold", 100),
        "custom_grid_roll_trade_threshold",
    )
    params["custom_grid_roll_upper_distance_ratio"] = _safe_float(
        payload.get("custom_grid_roll_upper_distance_ratio", 0.30),
        "custom_grid_roll_upper_distance_ratio",
    )
    params["custom_grid_roll_shift_levels"] = _safe_int(
        payload.get("custom_grid_roll_shift_levels", 1),
        "custom_grid_roll_shift_levels",
    )
    if params["custom_grid_roll_interval_minutes"] <= 0:
        raise ValueError("custom_grid_roll_interval_minutes must be > 0")
    if params["custom_grid_roll_trade_threshold"] < 0:
        raise ValueError("custom_grid_roll_trade_threshold must be >= 0")
    if not 0 <= params["custom_grid_roll_upper_distance_ratio"] <= 1:
        raise ValueError("custom_grid_roll_upper_distance_ratio must be between 0 and 1")
    if params["custom_grid_roll_shift_levels"] <= 0:
        raise ValueError("custom_grid_roll_shift_levels must be > 0")
    params["name"] = name
    return params


def _minimum_live_notional(symbol_info: dict[str, Any], current_price: float) -> float:
    min_notional = _safe_float(symbol_info.get("min_notional"), "min_notional")
    min_qty = _safe_float(symbol_info.get("min_qty"), "min_qty")
    minimum_working_notional = 0.0
    if min_notional > 0:
        minimum_working_notional = max(minimum_working_notional, min_notional * 4.0)
    if min_qty > 0 and current_price > 0:
        minimum_working_notional = max(minimum_working_notional, min_qty * current_price * 2.0)
    return minimum_working_notional


def _normalize_custom_grid_runtime_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(config)
    normalized["custom_grid_enabled"] = True
    normalized["fixed_center_enabled"] = False
    normalized["fixed_center_roll_enabled"] = False
    normalized["excess_inventory_reduce_only_enabled"] = False
    normalized["custom_grid_roll_enabled"] = _safe_bool(
        normalized.get("custom_grid_roll_enabled", False),
        "custom_grid_roll_enabled",
    )
    normalized["custom_grid_roll_interval_minutes"] = _safe_int(
        normalized.get("custom_grid_roll_interval_minutes", 5),
        "custom_grid_roll_interval_minutes",
    )
    normalized["custom_grid_roll_trade_threshold"] = _safe_int(
        normalized.get("custom_grid_roll_trade_threshold", 100),
        "custom_grid_roll_trade_threshold",
    )
    normalized["custom_grid_roll_upper_distance_ratio"] = _safe_float(
        normalized.get("custom_grid_roll_upper_distance_ratio", 0.30),
        "custom_grid_roll_upper_distance_ratio",
    )
    normalized["custom_grid_roll_shift_levels"] = _safe_int(
        normalized.get("custom_grid_roll_shift_levels", 1),
        "custom_grid_roll_shift_levels",
    )
    normalized["autotune_symbol_enabled"] = False
    normalized["auto_regime_enabled"] = False
    normalized["neutral_hourly_scale_enabled"] = False
    for field in {
        "buy_pause_amp_trigger_ratio",
        "buy_pause_down_return_trigger_ratio",
        "short_cover_pause_amp_trigger_ratio",
        "short_cover_pause_down_return_trigger_ratio",
        "freeze_shift_abs_return_trigger_ratio",
        "inventory_tier_start_notional",
        "inventory_tier_end_notional",
        "inventory_tier_buy_levels",
        "inventory_tier_sell_levels",
        "inventory_tier_per_order_notional",
        "inventory_tier_base_position_notional",
    }:
        normalized[field] = None

    strategy_mode = str(normalized.get("strategy_mode", "one_way_long")).strip().lower()
    if strategy_mode == "one_way_long":
        max_long = normalized.get("max_position_notional")
        normalized["pause_buy_position_notional"] = max_long
        normalized["pause_short_position_notional"] = None
        normalized["max_short_position_notional"] = None
    elif strategy_mode == "one_way_short":
        max_short = normalized.get("max_short_position_notional")
        normalized["pause_short_position_notional"] = max_short
        normalized["pause_buy_position_notional"] = None
        normalized["max_position_notional"] = None
    elif strategy_mode == "synthetic_neutral":
        if normalized.get("max_position_notional") is not None:
            normalized["pause_buy_position_notional"] = normalized.get("max_position_notional")
        if normalized.get("max_short_position_notional") is not None:
            normalized["pause_short_position_notional"] = normalized.get("max_short_position_notional")
    return normalized


def _build_custom_grid_runner_preset(params: dict[str, Any]) -> dict[str, Any]:
    preview = _run_grid_preview(params)
    summary = dict(preview.get("summary") or {})
    preview_rows = [dict(item) for item in preview.get("rows", []) if isinstance(item, dict)]
    symbol = str(summary.get("symbol") or params["symbol"]).upper().strip()
    current_price = _safe_float(summary.get("current_price"), "current_price")
    grid_count = max(_safe_int(summary.get("grid_count", params["n"]), "grid_count"), 1)
    levels = build_grid_levels(
        min_price=float(params["min_price"]),
        max_price=float(params["max_price"]),
        n=grid_count,
        grid_level_mode=str(params["grid_level_mode"]),
    )
    step_price = float(levels[1] - levels[0]) if len(levels) >= 2 else 0.0
    if step_price <= 0:
        raise ValueError("invalid custom grid step_price")
    symbol_info = dict(summary.get("symbol_info") or {})
    position_budget_notional = _safe_float(summary.get("position_budget_notional"), "position_budget_notional")
    preview_entry_notionals = [
        _safe_float(item.get("entry_notional"), "entry_notional")
        for item in preview_rows
        if _safe_float(item.get("entry_notional"), "entry_notional") > 0
    ]
    if preview_entry_notionals:
        per_order_notional = sum(preview_entry_notionals) / float(len(preview_entry_notionals))
    else:
        per_order_notional = max(
            position_budget_notional / float(grid_count),
            _minimum_live_notional(symbol_info, current_price),
        )
    max_new_orders = max(min(grid_count * 2 + 4, 200), 16)
    direction = str(summary.get("strategy_direction") or params["strategy_direction"]).strip().lower()
    center_price = current_price if current_price > 0 else (float(params["min_price"]) + float(params["max_price"])) / 2.0

    config: dict[str, Any] = {
        "symbol": symbol,
        "center_price": center_price,
        "custom_grid_enabled": True,
        "custom_grid_direction": direction,
        "custom_grid_level_mode": params["grid_level_mode"],
        "custom_grid_min_price": params["min_price"],
        "custom_grid_max_price": params["max_price"],
        "custom_grid_n": grid_count,
        "custom_grid_total_notional": position_budget_notional,
        "custom_grid_neutral_anchor_price": _safe_float(summary.get("neutral_anchor_price"), "neutral_anchor_price")
        if direction == "neutral"
        else None,
        "custom_grid_roll_enabled": bool(params.get("custom_grid_roll_enabled", False)),
        "custom_grid_roll_interval_minutes": _safe_int(
            params.get("custom_grid_roll_interval_minutes", 5),
            "custom_grid_roll_interval_minutes",
        ),
        "custom_grid_roll_trade_threshold": _safe_int(
            params.get("custom_grid_roll_trade_threshold", 100),
            "custom_grid_roll_trade_threshold",
        ),
        "custom_grid_roll_upper_distance_ratio": _safe_float(
            params.get("custom_grid_roll_upper_distance_ratio", 0.30),
            "custom_grid_roll_upper_distance_ratio",
        ),
        "custom_grid_roll_shift_levels": _safe_int(
            params.get("custom_grid_roll_shift_levels", 1),
            "custom_grid_roll_shift_levels",
        ),
        "step_price": step_price,
        "per_order_notional": per_order_notional,
        "margin_type": "KEEP",
        "leverage": _safe_int(params["leverage"], "leverage"),
        "max_new_orders": max_new_orders,
        "cancel_stale": True,
        "apply": True,
        "reset_state": True,
    }
    if direction == "long":
        config.update(
            {
                "strategy_mode": "one_way_long",
                "buy_levels": max(_safe_int(summary.get("active_buy_orders"), "active_buy_orders"), 0),
                "sell_levels": max(_safe_int(summary.get("active_sell_orders"), "active_sell_orders"), 0),
                "base_position_notional": _safe_float(summary.get("startup_long_notional"), "startup_long_notional"),
                "pause_buy_position_notional": _safe_float(summary.get("full_long_entry_notional"), "full_long_entry_notional") * 0.85,
                "max_position_notional": _safe_float(summary.get("full_long_entry_notional"), "full_long_entry_notional"),
                "max_total_notional": max(
                    _safe_float(summary.get("full_long_entry_notional"), "full_long_entry_notional"),
                    position_budget_notional,
                ),
            }
        )
    elif direction == "short":
        config.update(
            {
                "strategy_mode": "one_way_short",
                "buy_levels": max(_safe_int(summary.get("active_buy_orders"), "active_buy_orders"), 0),
                "sell_levels": max(_safe_int(summary.get("active_sell_orders"), "active_sell_orders"), 0),
                "base_position_notional": _safe_float(summary.get("startup_short_notional"), "startup_short_notional"),
                "pause_short_position_notional": _safe_float(summary.get("full_short_entry_notional"), "full_short_entry_notional") * 0.85,
                "max_short_position_notional": _safe_float(summary.get("full_short_entry_notional"), "full_short_entry_notional"),
                "max_total_notional": max(
                    _safe_float(summary.get("full_short_entry_notional"), "full_short_entry_notional"),
                    position_budget_notional,
                ),
            }
        )
    else:
        anchor_price = _safe_float(summary.get("neutral_anchor_price"), "neutral_anchor_price")
        config.update(
            {
                "strategy_mode": "synthetic_neutral",
                "center_price": anchor_price if anchor_price > 0 else center_price,
                "buy_levels": max(_safe_int(summary.get("long_grid_count"), "long_grid_count"), 0),
                "sell_levels": max(_safe_int(summary.get("short_grid_count"), "short_grid_count"), 0),
                "base_position_notional": (
                    _safe_float(summary.get("startup_long_notional"), "startup_long_notional")
                    + _safe_float(summary.get("startup_short_notional"), "startup_short_notional")
                ) / 2.0,
                "pause_buy_position_notional": _safe_float(summary.get("full_long_entry_notional"), "full_long_entry_notional") * 0.85,
                "pause_short_position_notional": _safe_float(summary.get("full_short_entry_notional"), "full_short_entry_notional") * 0.85,
                "max_position_notional": _safe_float(summary.get("full_long_entry_notional"), "full_long_entry_notional"),
                "max_short_position_notional": _safe_float(summary.get("full_short_entry_notional"), "full_short_entry_notional"),
                "max_total_notional": max(
                    _safe_float(summary.get("full_long_entry_notional"), "full_long_entry_notional")
                    + _safe_float(summary.get("full_short_entry_notional"), "full_short_entry_notional"),
                    position_budget_notional,
                ),
            }
        )

    config = _normalize_custom_grid_runtime_config(config)

    preset_key = _custom_runner_preset_key(symbol, params["name"])
    direction_label = {"long": "做多", "short": "做空", "neutral": "中性"}.get(direction, direction)
    description = (
        f"自定义固定梯子静态网格 · {symbol} · {direction_label} · "
        f"{params['min_price']:.7f}-{params['max_price']:.7f} · N={grid_count} · "
        f"保证金 {params['margin_amount']:.4f} · 杠杆 {params['leverage']:.2f}x"
    )
    preset = {
        "key": preset_key,
        "label": params["name"],
        "description": description,
        "startable": True,
        "kind": "custom_grid",
        "custom": True,
        "symbol": symbol,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "grid_preview_params": {
            "contract_type": params["contract_type"],
            "strategy_direction": direction,
            "grid_level_mode": params["grid_level_mode"],
            "min_price": params["min_price"],
            "max_price": params["max_price"],
            "n": grid_count,
            "margin_amount": params["margin_amount"],
            "leverage": params["leverage"],
        },
        "config": config,
        "preview_summary": summary,
    }
    return {
        "preset_key": preset_key,
        "preset": preset,
        "preview": preview,
    }


def _create_custom_grid_runner_preset(params: dict[str, Any]) -> dict[str, Any]:
    built = _build_custom_grid_runner_preset(params)
    presets = _load_custom_runner_presets()
    preset = dict(built["preset"])
    preset_key = str(built["preset_key"])
    presets[preset_key] = preset
    _save_custom_runner_presets(presets)
    return {
        "ok": True,
        "preset_key": preset_key,
        "preset": preset,
        "preview": built["preview"],
    }


def _get_custom_runner_preset(preset_key: str, symbol: str | None = None) -> dict[str, Any]:
    normalized_key = str(preset_key or "").strip()
    if not normalized_key:
        raise ValueError("preset_key is required")
    preset = _load_custom_runner_presets().get(normalized_key)
    if not isinstance(preset, dict) or not preset.get("custom"):
        raise ValueError("custom preset not found")
    requested_symbol = str(symbol or "").upper().strip()
    preset_symbol = str(preset.get("symbol", "")).upper().strip()
    if requested_symbol and preset_symbol and requested_symbol != preset_symbol:
        raise ValueError("preset does not belong to selected symbol")
    return preset


def _update_custom_grid_runner_preset(preset_key: str, params: dict[str, Any]) -> dict[str, Any]:
    existing = _get_custom_runner_preset(preset_key, params.get("symbol"))
    built = _build_custom_grid_runner_preset(params)
    presets = _load_custom_runner_presets()
    preset = dict(built["preset"])
    existing_created_at = existing.get("created_at")
    preset["key"] = str(preset_key)
    if existing_created_at:
        preset["created_at"] = existing_created_at
    preset["updated_at"] = datetime.now(timezone.utc).isoformat()
    presets[str(preset_key)] = preset
    _save_custom_runner_presets(presets)
    return {
        "ok": True,
        "preset_key": str(preset_key),
        "preset": preset,
        "preview": built["preview"],
    }


def _delete_custom_grid_runner_preset(preset_key: str, symbol: str | None = None) -> dict[str, Any]:
    existing = _get_custom_runner_preset(preset_key, symbol)
    preset_symbol = str(existing.get("symbol", "")).upper().strip()
    runner = _read_runner_process_for_symbol(preset_symbol)
    runner_cfg = dict(runner.get("config") or {})
    current_profile = str(runner_cfg.get("strategy_profile", "")).strip()
    if runner.get("is_running") and current_profile == str(preset_key):
        raise ValueError("当前策略正在运行，请先停止或切换后再删除")
    presets = _load_custom_runner_presets()
    removed = presets.pop(str(preset_key), None)
    if removed is None:
        raise ValueError("custom preset not found")
    _save_custom_runner_presets(presets)
    return {
        "ok": True,
        "preset_key": str(preset_key),
        "label": str(existing.get("label") or preset_key),
        "symbol": preset_symbol,
    }


def _normalize_runner_control_payload(payload: dict[str, Any]) -> dict[str, Any]:
    requested_symbol = str((payload or {}).get("symbol", "")).upper().strip()
    config = _load_runner_control_config(requested_symbol or None)
    if not payload:
        return config

    float_fields = {
        "center_price",
        "step_price",
        "per_order_notional",
        "base_position_notional",
        "pause_buy_position_notional",
        "pause_short_position_notional",
        "max_position_notional",
        "max_short_position_notional",
        "min_mid_price_for_buys",
        "buy_pause_amp_trigger_ratio",
        "buy_pause_down_return_trigger_ratio",
        "short_cover_pause_amp_trigger_ratio",
        "short_cover_pause_down_return_trigger_ratio",
        "freeze_shift_abs_return_trigger_ratio",
        "auto_regime_stable_15m_max_amplitude_ratio",
        "auto_regime_stable_60m_max_amplitude_ratio",
        "auto_regime_stable_60m_return_floor_ratio",
        "auto_regime_defensive_15m_amplitude_ratio",
        "auto_regime_defensive_60m_amplitude_ratio",
        "auto_regime_defensive_15m_return_ratio",
        "auto_regime_defensive_60m_return_ratio",
        "neutral_band1_offset_ratio",
        "neutral_band2_offset_ratio",
        "neutral_band3_offset_ratio",
        "neutral_band1_target_ratio",
        "neutral_band2_target_ratio",
        "neutral_band3_target_ratio",
        "neutral_hourly_scale_stable",
        "neutral_hourly_scale_transition",
        "neutral_hourly_scale_defensive",
        "inventory_tier_start_notional",
        "inventory_tier_end_notional",
        "inventory_tier_per_order_notional",
        "inventory_tier_base_position_notional",
        "max_plan_age_seconds",
        "max_mid_drift_steps",
        "max_total_notional",
        "rolling_hourly_loss_limit",
        "max_cumulative_notional",
        "sleep_seconds",
    }
    int_fields = {
        "buy_levels",
        "sell_levels",
        "up_trigger_steps",
        "down_trigger_steps",
        "shift_steps",
        "leverage",
        "maker_retries",
        "max_new_orders",
        "auto_regime_confirm_cycles",
        "neutral_center_interval_minutes",
        "inventory_tier_buy_levels",
        "inventory_tier_sell_levels",
    }
    bool_fields = {
        "cancel_stale",
        "apply",
        "reset_state",
        "flat_start_enabled",
        "warm_start_enabled",
        "auto_regime_enabled",
        "neutral_hourly_scale_enabled",
        "fixed_center_enabled",
        "fixed_center_roll_enabled",
        "excess_inventory_reduce_only_enabled",
        "autotune_symbol_enabled",
    }
    str_fields = {
        "strategy_profile",
        "strategy_mode",
        "symbol",
        "margin_type",
        "state_path",
        "plan_json",
        "submit_report_json",
        "summary_jsonl",
        "run_start_time",
        "run_end_time",
    }
    noneable_fields = {
        "center_price",
        "pause_buy_position_notional",
        "pause_short_position_notional",
        "max_position_notional",
        "max_short_position_notional",
        "min_mid_price_for_buys",
        "buy_pause_amp_trigger_ratio",
        "buy_pause_down_return_trigger_ratio",
        "short_cover_pause_amp_trigger_ratio",
        "short_cover_pause_down_return_trigger_ratio",
        "freeze_shift_abs_return_trigger_ratio",
        "inventory_tier_start_notional",
        "inventory_tier_end_notional",
        "inventory_tier_per_order_notional",
        "inventory_tier_base_position_notional",
        "inventory_tier_buy_levels",
        "inventory_tier_sell_levels",
        "rolling_hourly_loss_limit",
        "max_cumulative_notional",
        "run_start_time",
        "run_end_time",
    }

    for key, value in payload.items():
        if key not in float_fields | int_fields | bool_fields | str_fields:
            continue
        if key in noneable_fields and value in {"", None}:
            config[key] = None
            continue
        if key in float_fields:
            config[key] = _safe_float(value, key)
            continue
        if key in int_fields:
            config[key] = _safe_int(value, key)
            continue
        if key in bool_fields:
            config[key] = _safe_bool(value, key)
            continue
        if key in str_fields:
            text = str(value).strip()
            if text:
                config[key] = text

    config["symbol"] = str(config.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"
    config["strategy_mode"] = str(config.get("strategy_mode", "one_way_long")).strip() or "one_way_long"
    config["margin_type"] = str(config.get("margin_type", "KEEP")).upper().strip() or "KEEP"
    config.update(normalize_runtime_guard_payload(config))
    return config


def _resolve_runner_start_config(payload: dict[str, Any]) -> dict[str, Any]:
    raw_payload = dict(payload)
    config = _normalize_runner_control_payload(payload)
    profile = str(config.get("strategy_profile", RUNNER_DEFAULT_CONFIG["strategy_profile"])).strip() or RUNNER_DEFAULT_CONFIG["strategy_profile"]
    preset = _runner_preset_map(str(config.get("symbol", ""))).get(profile)
    if preset is None:
        preset = _runner_preset_map().get(profile)
    if preset is None:
        raise ValueError(f"Unknown strategy_profile: {profile}")
    if not preset.get("startable", True):
        raise ValueError(f"{preset.get('label', profile)} 当前是模板预设，页面已展示参数，但还不能直接启动。")
    preset_symbol = str(preset.get("symbol", "")).upper().strip()
    resolved_symbol = str(config.get("symbol", "")).upper().strip()
    if preset_symbol and resolved_symbol and resolved_symbol != preset_symbol:
        raise ValueError(f"{preset.get('label', profile)} requires symbol={preset_symbol}")
    raw_payload.setdefault("symbol", config.get("symbol"))
    raw_payload.setdefault("strategy_profile", profile)
    resolved = _normalize_runner_control_payload(_runner_preset_payload(profile, raw_payload))
    runtime_paths = _default_runtime_paths_for_symbol(str(resolved.get("symbol", "")))
    for key, value in runtime_paths.items():
        if not str(payload.get(key, "")).strip():
            resolved[key] = value
    return _autotune_runner_symbol_config(resolved)


def _build_runner_command(config: dict[str, Any]) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "grid_optimizer.loop_runner",
        "--symbol",
        str(config["symbol"]),
        "--strategy-profile",
        str(config.get("strategy_profile", RUNNER_DEFAULT_CONFIG["strategy_profile"])),
        "--strategy-mode",
        str(config.get("strategy_mode", "one_way_long")),
        "--step-price",
        str(config["step_price"]),
        "--buy-levels",
        str(config["buy_levels"]),
        "--sell-levels",
        str(config["sell_levels"]),
        "--per-order-notional",
        str(config["per_order_notional"]),
        "--base-position-notional",
        str(config["base_position_notional"]),
        "--margin-type",
        str(config.get("margin_type", "KEEP")),
        "--leverage",
        str(config.get("leverage", 2)),
        "--max-plan-age-seconds",
        str(int(config.get("max_plan_age_seconds", 30))),
        "--max-mid-drift-steps",
        str(config.get("max_mid_drift_steps", 4.0)),
        "--maker-retries",
        str(config.get("maker_retries", 2)),
        "--max-new-orders",
        str(config.get("max_new_orders", 20)),
        "--max-total-notional",
        str(config.get("max_total_notional", 1000.0)),
        "--sleep-seconds",
        str(config.get("sleep_seconds", 15.0)),
        "--state-path",
        str(config.get("state_path", RUNNER_DEFAULT_CONFIG["state_path"])),
        "--plan-json",
        str(config.get("plan_json", RUNNER_DEFAULT_CONFIG["plan_json"])),
        "--submit-report-json",
        str(config.get("submit_report_json", RUNNER_DEFAULT_CONFIG["submit_report_json"])),
        "--summary-jsonl",
        str(config.get("summary_jsonl", RUNNER_DEFAULT_CONFIG["summary_jsonl"])),
    ]
    if config.get("center_price") is not None:
        command.extend(["--center-price", str(config["center_price"])])
    command.append("--flat-start-enabled" if config.get("flat_start_enabled", True) else "--no-flat-start-enabled")
    command.append("--warm-start-enabled" if config.get("warm_start_enabled", True) else "--no-warm-start-enabled")
    command.append("--fixed-center-enabled" if config.get("fixed_center_enabled", False) else "--no-fixed-center-enabled")
    command.append("--fixed-center-roll-enabled" if config.get("fixed_center_roll_enabled", False) else "--no-fixed-center-roll-enabled")
    command.append(
        "--excess-inventory-reduce-only-enabled"
        if config.get("excess_inventory_reduce_only_enabled", False)
        else "--no-excess-inventory-reduce-only-enabled"
    )
    command.append(
        "--custom-grid-enabled"
        if config.get("custom_grid_enabled", False)
        else "--no-custom-grid-enabled"
    )
    if config.get("custom_grid_direction") is not None:
        command.extend(["--custom-grid-direction", str(config["custom_grid_direction"])])
    if config.get("custom_grid_level_mode") is not None:
        command.extend(["--custom-grid-level-mode", str(config["custom_grid_level_mode"])])
    if config.get("custom_grid_min_price") is not None:
        command.extend(["--custom-grid-min-price", str(config["custom_grid_min_price"])])
    if config.get("custom_grid_max_price") is not None:
        command.extend(["--custom-grid-max-price", str(config["custom_grid_max_price"])])
    if config.get("custom_grid_n") is not None:
        command.extend(["--custom-grid-n", str(config["custom_grid_n"])])
    if config.get("custom_grid_total_notional") is not None:
        command.extend(["--custom-grid-total-notional", str(config["custom_grid_total_notional"])])
    if config.get("custom_grid_neutral_anchor_price") is not None:
        command.extend(["--custom-grid-neutral-anchor-price", str(config["custom_grid_neutral_anchor_price"])])
    command.append(
        "--custom-grid-roll-enabled"
        if config.get("custom_grid_roll_enabled", False)
        else "--no-custom-grid-roll-enabled"
    )
    if config.get("custom_grid_roll_interval_minutes") is not None:
        command.extend(["--custom-grid-roll-interval-minutes", str(config["custom_grid_roll_interval_minutes"])])
    if config.get("custom_grid_roll_trade_threshold") is not None:
        command.extend(["--custom-grid-roll-trade-threshold", str(config["custom_grid_roll_trade_threshold"])])
    if config.get("custom_grid_roll_upper_distance_ratio") is not None:
        command.extend(["--custom-grid-roll-upper-distance-ratio", str(config["custom_grid_roll_upper_distance_ratio"])])
    if config.get("custom_grid_roll_shift_levels") is not None:
        command.extend(["--custom-grid-roll-shift-levels", str(config["custom_grid_roll_shift_levels"])])
    command.extend([
        "--down-trigger-steps",
        str(config.get("down_trigger_steps", 4)),
        "--up-trigger-steps",
        str(config.get("up_trigger_steps", 8)),
        "--shift-steps",
        str(config.get("shift_steps", 4)),
    ])
    if config.get("pause_buy_position_notional") is not None:
        command.extend(["--pause-buy-position-notional", str(config["pause_buy_position_notional"])])
    if config.get("pause_short_position_notional") is not None:
        command.extend(["--pause-short-position-notional", str(config["pause_short_position_notional"])])
    if config.get("max_position_notional") is not None:
        command.extend(["--max-position-notional", str(config["max_position_notional"])])
    if config.get("max_short_position_notional") is not None:
        command.extend(["--max-short-position-notional", str(config["max_short_position_notional"])])
    if config.get("min_mid_price_for_buys") is not None:
        command.extend(["--min-mid-price-for-buys", str(config["min_mid_price_for_buys"])])
    if config.get("buy_pause_amp_trigger_ratio") is not None:
        command.extend(["--buy-pause-amp-trigger-ratio", str(config["buy_pause_amp_trigger_ratio"])])
    if config.get("buy_pause_down_return_trigger_ratio") is not None:
        command.extend(["--buy-pause-down-return-trigger-ratio", str(config["buy_pause_down_return_trigger_ratio"])])
    if config.get("short_cover_pause_amp_trigger_ratio") is not None:
        command.extend(["--short-cover-pause-amp-trigger-ratio", str(config["short_cover_pause_amp_trigger_ratio"])])
    if config.get("short_cover_pause_down_return_trigger_ratio") is not None:
        command.extend(["--short-cover-pause-down-return-trigger-ratio", str(config["short_cover_pause_down_return_trigger_ratio"])])
    if config.get("freeze_shift_abs_return_trigger_ratio") is not None:
        command.extend(["--freeze-shift-abs-return-trigger-ratio", str(config["freeze_shift_abs_return_trigger_ratio"])])
    if config.get("run_start_time") is not None:
        command.extend(["--run-start-time", str(config["run_start_time"])])
    if config.get("run_end_time") is not None:
        command.extend(["--run-end-time", str(config["run_end_time"])])
    if config.get("rolling_hourly_loss_limit") is not None:
        command.extend(["--rolling-hourly-loss-limit", str(config["rolling_hourly_loss_limit"])])
    if config.get("max_cumulative_notional") is not None:
        command.extend(["--max-cumulative-notional", str(config["max_cumulative_notional"])])
    command.append("--auto-regime-enabled" if config.get("auto_regime_enabled", False) else "--no-auto-regime-enabled")
    if config.get("auto_regime_confirm_cycles") is not None:
        command.extend(["--auto-regime-confirm-cycles", str(config["auto_regime_confirm_cycles"])])
    if config.get("auto_regime_stable_15m_max_amplitude_ratio") is not None:
        command.extend(["--auto-regime-stable-15m-max-amplitude-ratio", str(config["auto_regime_stable_15m_max_amplitude_ratio"])])
    if config.get("auto_regime_stable_60m_max_amplitude_ratio") is not None:
        command.extend(["--auto-regime-stable-60m-max-amplitude-ratio", str(config["auto_regime_stable_60m_max_amplitude_ratio"])])
    if config.get("auto_regime_stable_60m_return_floor_ratio") is not None:
        command.extend(["--auto-regime-stable-60m-return-floor-ratio", str(config["auto_regime_stable_60m_return_floor_ratio"])])
    if config.get("auto_regime_defensive_15m_amplitude_ratio") is not None:
        command.extend(["--auto-regime-defensive-15m-amplitude-ratio", str(config["auto_regime_defensive_15m_amplitude_ratio"])])
    if config.get("auto_regime_defensive_60m_amplitude_ratio") is not None:
        command.extend(["--auto-regime-defensive-60m-amplitude-ratio", str(config["auto_regime_defensive_60m_amplitude_ratio"])])
    if config.get("auto_regime_defensive_15m_return_ratio") is not None:
        command.extend(["--auto-regime-defensive-15m-return-ratio", str(config["auto_regime_defensive_15m_return_ratio"])])
    if config.get("auto_regime_defensive_60m_return_ratio") is not None:
        command.extend(["--auto-regime-defensive-60m-return-ratio", str(config["auto_regime_defensive_60m_return_ratio"])])
    if config.get("neutral_center_interval_minutes") is not None:
        command.extend(["--neutral-center-interval-minutes", str(config["neutral_center_interval_minutes"])])
    if config.get("neutral_band1_offset_ratio") is not None:
        command.extend(["--neutral-band1-offset-ratio", str(config["neutral_band1_offset_ratio"])])
    if config.get("neutral_band2_offset_ratio") is not None:
        command.extend(["--neutral-band2-offset-ratio", str(config["neutral_band2_offset_ratio"])])
    if config.get("neutral_band3_offset_ratio") is not None:
        command.extend(["--neutral-band3-offset-ratio", str(config["neutral_band3_offset_ratio"])])
    if config.get("neutral_band1_target_ratio") is not None:
        command.extend(["--neutral-band1-target-ratio", str(config["neutral_band1_target_ratio"])])
    if config.get("neutral_band2_target_ratio") is not None:
        command.extend(["--neutral-band2-target-ratio", str(config["neutral_band2_target_ratio"])])
    if config.get("neutral_band3_target_ratio") is not None:
        command.extend(["--neutral-band3-target-ratio", str(config["neutral_band3_target_ratio"])])
    command.append("--neutral-hourly-scale-enabled" if config.get("neutral_hourly_scale_enabled", False) else "--no-neutral-hourly-scale-enabled")
    if config.get("neutral_hourly_scale_stable") is not None:
        command.extend(["--neutral-hourly-scale-stable", str(config["neutral_hourly_scale_stable"])])
    if config.get("neutral_hourly_scale_transition") is not None:
        command.extend(["--neutral-hourly-scale-transition", str(config["neutral_hourly_scale_transition"])])
    if config.get("neutral_hourly_scale_defensive") is not None:
        command.extend(["--neutral-hourly-scale-defensive", str(config["neutral_hourly_scale_defensive"])])
    if config.get("inventory_tier_start_notional") is not None:
        command.extend(["--inventory-tier-start-notional", str(config["inventory_tier_start_notional"])])
    if config.get("inventory_tier_end_notional") is not None:
        command.extend(["--inventory-tier-end-notional", str(config["inventory_tier_end_notional"])])
    if config.get("inventory_tier_buy_levels") is not None:
        command.extend(["--inventory-tier-buy-levels", str(config["inventory_tier_buy_levels"])])
    if config.get("inventory_tier_sell_levels") is not None:
        command.extend(["--inventory-tier-sell-levels", str(config["inventory_tier_sell_levels"])])
    if config.get("inventory_tier_per_order_notional") is not None:
        command.extend(["--inventory-tier-per-order-notional", str(config["inventory_tier_per_order_notional"])])
    if config.get("inventory_tier_base_position_notional") is not None:
        command.extend(["--inventory-tier-base-position-notional", str(config["inventory_tier_base_position_notional"])])
    command.append("--cancel-stale" if config.get("cancel_stale", True) else "--no-cancel-stale")
    command.append("--apply" if config.get("apply", True) else "--no-apply")
    if config.get("reset_state", True):
        command.append("--reset-state")
    return command


def _runner_service_available(symbol: str | None = None) -> bool:
    service_name = _runner_service_name_for_symbol(symbol)
    if not service_name:
        return False
    try:
        probe = _run_systemctl(["cat", service_name], capture_output=True, text=True, check=False)
    except OSError:
        return False
    return probe.returncode == 0


def _runner_launch_agent_available() -> bool:
    return sys.platform == "darwin" and RUNNER_LAUNCH_AGENT_PATH.exists()


def _launchctl_target(label: str) -> str:
    return f"gui/{os.getuid()}/{label}"


def _launchctl_loaded(label: str) -> bool:
    try:
        probe = subprocess.run(
            ["launchctl", "print", _launchctl_target(label)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return False
    return probe.returncode == 0


def _start_runner_process(config: dict[str, Any]) -> dict[str, Any]:
    symbol = str(config.get("symbol", RUNNER_DEFAULT_CONFIG.get("symbol", "NIGHTUSDT"))).upper().strip() or "NIGHTUSDT"
    _stop_flatten_process(symbol, cancel_orders=True)
    runner = _read_runner_process_for_symbol(symbol)
    restarted = False
    if runner.get("is_running"):
        current_config = dict(runner.get("config") or {})
        compare_fields = {
            "strategy_profile",
            "strategy_mode",
            "symbol",
            "center_price",
            "flat_start_enabled",
            "warm_start_enabled",
            "fixed_center_enabled",
            "autotune_symbol_enabled",
            "step_price",
            "buy_levels",
            "sell_levels",
            "per_order_notional",
            "base_position_notional",
            "up_trigger_steps",
            "down_trigger_steps",
            "shift_steps",
            "pause_buy_position_notional",
            "pause_short_position_notional",
            "max_position_notional",
            "max_short_position_notional",
            "custom_grid_roll_enabled",
            "custom_grid_roll_interval_minutes",
            "custom_grid_roll_trade_threshold",
            "custom_grid_roll_upper_distance_ratio",
            "custom_grid_roll_shift_levels",
            "min_mid_price_for_buys",
            "buy_pause_amp_trigger_ratio",
            "buy_pause_down_return_trigger_ratio",
            "short_cover_pause_amp_trigger_ratio",
            "short_cover_pause_down_return_trigger_ratio",
            "freeze_shift_abs_return_trigger_ratio",
            "auto_regime_enabled",
            "auto_regime_confirm_cycles",
            "auto_regime_stable_15m_max_amplitude_ratio",
            "auto_regime_stable_60m_max_amplitude_ratio",
            "auto_regime_stable_60m_return_floor_ratio",
            "auto_regime_defensive_15m_amplitude_ratio",
            "auto_regime_defensive_60m_amplitude_ratio",
            "auto_regime_defensive_15m_return_ratio",
            "auto_regime_defensive_60m_return_ratio",
            "neutral_center_interval_minutes",
            "neutral_band1_offset_ratio",
            "neutral_band2_offset_ratio",
            "neutral_band3_offset_ratio",
            "neutral_band1_target_ratio",
            "neutral_band2_target_ratio",
            "neutral_band3_target_ratio",
            "neutral_hourly_scale_enabled",
            "neutral_hourly_scale_stable",
            "neutral_hourly_scale_transition",
            "neutral_hourly_scale_defensive",
            "inventory_tier_start_notional",
            "inventory_tier_end_notional",
            "inventory_tier_buy_levels",
            "inventory_tier_sell_levels",
            "inventory_tier_per_order_notional",
            "inventory_tier_base_position_notional",
            "margin_type",
            "leverage",
            "max_plan_age_seconds",
            "max_mid_drift_steps",
            "maker_retries",
            "max_new_orders",
            "max_total_notional",
            "run_start_time",
            "run_end_time",
            "rolling_hourly_loss_limit",
            "max_cumulative_notional",
            "sleep_seconds",
            "cancel_stale",
            "apply",
            "reset_state",
            "state_path",
            "plan_json",
            "submit_report_json",
            "summary_jsonl",
        }
        config_changed = any(current_config.get(field) != config.get(field) for field in compare_fields)
        if not config_changed:
            return {"started": False, "already_running": True, "runner": runner, "symbol": symbol, "restarted": False}
        _stop_runner_process(symbol)
        restarted = True

    _save_runner_control_config(config, symbol=symbol)
    use_legacy_runner = _uses_legacy_runner(symbol)
    service_name = _runner_service_name_for_symbol(symbol)
    if use_legacy_runner and _runner_launch_agent_available():
        target = _launchctl_target(RUNNER_LAUNCH_AGENT_LABEL)
        if not _launchctl_loaded(RUNNER_LAUNCH_AGENT_LABEL):
            subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(RUNNER_LAUNCH_AGENT_PATH)], check=True)
        subprocess.run(["launchctl", "enable", target], check=False)
        subprocess.run(["launchctl", "kickstart", "-k", target], check=True)
        time.sleep(1.0)
        return {
            "started": True,
            "already_running": False,
            "restarted": restarted,
            "runner": _read_runner_process_for_symbol(symbol),
            "service": RUNNER_LAUNCH_AGENT_LABEL,
            "launch_agent": str(RUNNER_LAUNCH_AGENT_PATH),
            "symbol": symbol,
        }
    if service_name and _runner_service_available(symbol):
        _run_systemctl(["start", service_name], check=True)
        time.sleep(1.0)
        return {
            "started": True,
            "already_running": False,
            "restarted": restarted,
            "runner": _read_runner_process_for_symbol(symbol),
            "service": service_name,
            "symbol": symbol,
        }

    log_path = _runner_log_path(symbol)
    pid_path = _runner_pid_path(symbol)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_path = str((Path.cwd() / "src").resolve())
    current_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"

    command = _build_runner_command(config)
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(Path.cwd()),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    time.sleep(1.0)
    return {
        "started": True,
        "already_running": False,
        "restarted": restarted,
        "runner": _read_runner_process_for_symbol(symbol),
        "command": command,
        "symbol": symbol,
    }


def _cancel_flatten_orders(*, symbol: str, api_key: str, api_secret: str, prefix: str) -> dict[str, Any]:
    result = {"attempted": 0, "success": 0, "errors": []}
    open_orders = fetch_futures_open_orders(symbol, api_key, api_secret)
    ours = [order for order in open_orders if isinstance(order, dict) and is_flatten_order(order, prefix)]
    result["attempted"] = len(ours)
    for order in ours:
        order_id = order.get("orderId")
        try:
            delete_futures_order(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                order_id=int(order_id) if order_id is not None else None,
                orig_client_order_id=str(order.get("clientOrderId", "")).strip() or None,
            )
            result["success"] += 1
        except Exception as exc:
            result["errors"].append(
                {
                    "order_id": order_id,
                    "client_order_id": order.get("clientOrderId"),
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
    return result


def _build_flatten_command(config: dict[str, Any]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "grid_optimizer.maker_flatten_runner",
        "--symbol",
        str(config["symbol"]),
        "--client-order-prefix",
        str(config["client_order_prefix"]),
        "--sleep-seconds",
        str(config.get("sleep_seconds", 2.0)),
        "--recv-window",
        str(config.get("recv_window", 5000)),
        "--max-consecutive-errors",
        str(config.get("max_consecutive_errors", 20)),
        "--events-jsonl",
        str(config["events_jsonl"]),
    ]


def _start_flatten_process(config: dict[str, Any]) -> dict[str, Any]:
    symbol = str(config.get("symbol", "NIGHTUSDT")).upper().strip() or "NIGHTUSDT"
    flatten = _read_flatten_process_for_symbol(symbol)
    desired = {
        "symbol": symbol,
        "client_order_prefix": str(config.get("client_order_prefix") or flatten_client_order_prefix(symbol)),
        "sleep_seconds": float(config.get("sleep_seconds", 2.0)),
        "recv_window": int(config.get("recv_window", 5000)),
        "max_consecutive_errors": int(config.get("max_consecutive_errors", 20)),
        "events_jsonl": str(config.get("events_jsonl") or _flatten_events_path(symbol)),
    }
    if flatten.get("is_running"):
        current_config = dict(flatten.get("config") or {})
        compare_fields = {"symbol", "client_order_prefix", "sleep_seconds", "recv_window", "max_consecutive_errors", "events_jsonl"}
        if all(current_config.get(field) == desired.get(field) for field in compare_fields):
            return {"started": False, "already_running": True, "flatten_runner": flatten, "symbol": symbol}
        _stop_flatten_process(symbol, cancel_orders=True)

    _save_flatten_control_config(desired, symbol=symbol)
    log_path = _flatten_log_path(symbol)
    pid_path = _flatten_pid_path(symbol)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_path = str((Path.cwd() / "src").resolve())
    current_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"
    command = _build_flatten_command(desired)
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(Path.cwd()),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    time.sleep(0.5)
    return {
        "started": True,
        "already_running": False,
        "flatten_runner": _read_flatten_process_for_symbol(symbol),
        "command": command,
        "symbol": symbol,
    }


def _stop_flatten_process(symbol: str | None = None, *, cancel_orders: bool = False, timeout_seconds: float = 5.0) -> dict[str, Any]:
    normalized_symbol = str(symbol or _legacy_runner_symbol()).upper().strip() or _legacy_runner_symbol()
    runner = _read_flatten_process_for_symbol(normalized_symbol)
    pid_path = _flatten_pid_path(normalized_symbol)
    control = dict(runner.get("config") or {})
    prefix = str(control.get("client_order_prefix") or flatten_client_order_prefix(normalized_symbol)).strip()

    if cancel_orders:
        creds = load_binance_api_credentials()
        if creds is not None:
            api_key, api_secret = creds
            _cancel_flatten_orders(symbol=normalized_symbol, api_key=api_key, api_secret=api_secret, prefix=prefix)

    pid = runner.get("pid")
    if not pid or not runner.get("is_running"):
        if pid_path.exists():
            pid_path.unlink(missing_ok=True)
        return {"stopped": False, "already_stopped": True, "flatten_runner": _read_flatten_process_for_symbol(normalized_symbol), "symbol": normalized_symbol}

    os.kill(int(pid), signal.SIGTERM)
    deadline = time.time() + max(timeout_seconds, 0.5)
    while time.time() < deadline:
        probe = _read_flatten_process_for_symbol(normalized_symbol)
        if not probe.get("is_running"):
            pid_path.unlink(missing_ok=True)
            return {"stopped": True, "killed": False, "flatten_runner": probe, "symbol": normalized_symbol}
        time.sleep(0.25)

    os.kill(int(pid), signal.SIGKILL)
    time.sleep(0.25)
    pid_path.unlink(missing_ok=True)
    return {"stopped": True, "killed": True, "flatten_runner": _read_flatten_process_for_symbol(normalized_symbol), "symbol": normalized_symbol}


def _stop_runner_process(
    symbol: str | None = None,
    timeout_seconds: float = 10.0,
    *,
    cancel_open_orders: bool = False,
    close_all_positions: bool = False,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or _legacy_runner_symbol()).upper().strip() or _legacy_runner_symbol()
    runner = _read_runner_process_for_symbol(normalized_symbol)
    pid_path = _runner_pid_path(normalized_symbol)
    use_legacy_runner = _uses_legacy_runner(normalized_symbol)
    service_name = _runner_service_name_for_symbol(normalized_symbol)

    if use_legacy_runner and _runner_launch_agent_available():
        target = _launchctl_target(RUNNER_LAUNCH_AGENT_LABEL)
        loaded = _launchctl_loaded(RUNNER_LAUNCH_AGENT_LABEL)
        if not loaded and not runner.get("is_running"):
            if pid_path.exists():
                try:
                    pid_path.unlink()
                except OSError:
                    pass
            return {
                "stopped": False,
                "already_stopped": True,
                "runner": _read_runner_process_for_symbol(normalized_symbol),
                "service": RUNNER_LAUNCH_AGENT_LABEL,
                "symbol": normalized_symbol,
                "post_stop_actions": _execute_stop_actions(
                    symbol=normalized_symbol,
                    cancel_open_orders=cancel_open_orders,
                    close_all_positions=close_all_positions,
                ),
            }
        subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}", str(RUNNER_LAUNCH_AGENT_PATH)], check=False)
        time.sleep(1.0)
        if pid_path.exists():
            try:
                pid_path.unlink()
            except OSError:
                pass
        post_stop_actions = _execute_stop_actions(
            symbol=normalized_symbol,
            cancel_open_orders=cancel_open_orders,
            close_all_positions=close_all_positions,
        )
        return {
            "stopped": True,
            "killed": False,
            "runner": _read_runner_process_for_symbol(normalized_symbol),
            "service": RUNNER_LAUNCH_AGENT_LABEL,
            "launch_agent": str(RUNNER_LAUNCH_AGENT_PATH),
            "symbol": normalized_symbol,
            "post_stop_actions": post_stop_actions,
        }
    if service_name and _runner_service_available(normalized_symbol):
        active = _run_systemctl(["is-active", "--quiet", service_name], check=False).returncode == 0
        if not active:
            if pid_path.exists():
                try:
                    pid_path.unlink()
                except OSError:
                    pass
            return {
                "stopped": False,
                "already_stopped": True,
                "runner": _read_runner_process_for_symbol(normalized_symbol),
                "service": service_name,
                "symbol": normalized_symbol,
                "post_stop_actions": _execute_stop_actions(
                    symbol=normalized_symbol,
                    cancel_open_orders=cancel_open_orders,
                    close_all_positions=close_all_positions,
                ),
            }
        _run_systemctl(["stop", service_name], check=True)
        time.sleep(1.0)
        if pid_path.exists():
            try:
                pid_path.unlink()
            except OSError:
                pass
        post_stop_actions = _execute_stop_actions(
            symbol=normalized_symbol,
            cancel_open_orders=cancel_open_orders,
            close_all_positions=close_all_positions,
        )
        return {
            "stopped": True,
            "killed": False,
            "runner": _read_runner_process_for_symbol(normalized_symbol),
            "service": service_name,
            "symbol": normalized_symbol,
            "post_stop_actions": post_stop_actions,
        }

    pid = runner.get("pid")
    if not pid or not runner.get("is_running"):
        if pid_path.exists():
            try:
                pid_path.unlink()
            except OSError:
                pass
        return {
            "stopped": False,
            "already_stopped": True,
            "runner": _read_runner_process_for_symbol(normalized_symbol),
            "symbol": normalized_symbol,
            "post_stop_actions": _execute_stop_actions(
                symbol=normalized_symbol,
                cancel_open_orders=cancel_open_orders,
                close_all_positions=close_all_positions,
            ),
        }

    os.kill(int(pid), signal.SIGTERM)
    deadline = time.time() + max(timeout_seconds, 0.5)
    while time.time() < deadline:
        probe = _read_runner_process_for_symbol(normalized_symbol)
        if not probe.get("is_running"):
            if pid_path.exists():
                try:
                    pid_path.unlink()
                except OSError:
                    pass
            return {
                "stopped": True,
                "killed": False,
                "runner": probe,
                "symbol": normalized_symbol,
                "post_stop_actions": _execute_stop_actions(
                    symbol=normalized_symbol,
                    cancel_open_orders=cancel_open_orders,
                    close_all_positions=close_all_positions,
                ),
            }
        time.sleep(0.25)

    os.kill(int(pid), signal.SIGKILL)
    time.sleep(0.5)
    if pid_path.exists():
        try:
            pid_path.unlink()
        except OSError:
            pass
    post_stop_actions = _execute_stop_actions(
        symbol=normalized_symbol,
        cancel_open_orders=cancel_open_orders,
        close_all_positions=close_all_positions,
    )
    return {
        "stopped": True,
        "killed": True,
        "runner": _read_runner_process_for_symbol(normalized_symbol),
        "symbol": normalized_symbol,
        "post_stop_actions": post_stop_actions,
    }


def _safe_numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_futures_position(
    account_info: dict[str, Any],
    symbol: str,
    position_side: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").upper().strip()
    normalized_side = str(position_side or "").upper().strip()
    positions = account_info.get("positions", [])
    if not isinstance(positions, list):
        return {}
    for item in positions:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol", "")).upper().strip() != normalized_symbol:
            continue
        if normalized_side and str(item.get("positionSide", "")).upper().strip() != normalized_side:
            continue
        return item
    return {}


def _build_stop_execution_summary(
    *,
    symbol: str,
    cancel_open_orders: bool,
    close_all_positions: bool,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "cancel_open_orders_requested": bool(cancel_open_orders),
        "close_all_positions_requested": bool(close_all_positions),
        "cancel_open_orders_executed": False,
        "close_all_positions_executed": False,
        "cancel_attempted_count": 0,
        "cancel_success_count": 0,
        "cancel_errors": [],
        "close_attempted_count": 0,
        "close_submitted_count": 0,
        "close_errors": [],
        "close_orders": [],
        "flatten_started": False,
        "flatten_already_running": False,
        "warnings": [],
    }


def _cancel_symbol_open_orders(*, symbol: str, api_key: str, api_secret: str) -> dict[str, Any]:
    result = {"attempted": 0, "success": 0, "errors": []}
    open_orders = fetch_futures_open_orders(symbol, api_key, api_secret)
    result["attempted"] = len(open_orders)
    for order in open_orders:
        order_id = order.get("orderId")
        try:
            delete_futures_order(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                order_id=int(order_id) if order_id is not None else None,
                orig_client_order_id=str(order.get("clientOrderId", "")).strip() or None,
            )
            result["success"] += 1
        except Exception as exc:
            result["errors"].append(
                {
                    "order_id": order_id,
                    "client_order_id": order.get("clientOrderId"),
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
    return result


def _build_close_order_request(
    *,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    reduce_only: bool | None,
    position_side: str | None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "quantity": qty,
        "price": price,
        "time_in_force": "IOC",
    }
    if reduce_only is not None:
        request["reduce_only"] = reduce_only
    if position_side:
        request["position_side"] = position_side
    return request


def _close_symbol_positions_at_top_of_book(*, symbol: str, api_key: str, api_secret: str) -> dict[str, Any]:
    result = {"attempted": 0, "submitted": 0, "orders": [], "errors": [], "warnings": []}
    symbol_info = fetch_futures_symbol_config(symbol)
    book = fetch_futures_book_tickers(symbol=symbol)
    if not book:
        raise RuntimeError(f"{symbol} 缺少盘口数据，无法按买一/卖一平仓")
    bid_price = _safe_numeric(book[0].get("bid_price"))
    ask_price = _safe_numeric(book[0].get("ask_price"))
    if bid_price <= 0 or ask_price <= 0:
        raise RuntimeError(f"{symbol} 盘口价格无效，无法按买一/卖一平仓")
    account_info = fetch_futures_account_info_v3(api_key, api_secret)
    position_mode = fetch_futures_position_mode(api_key, api_secret)
    dual_side = str(position_mode.get("dualSidePosition", "")).strip().lower() in {"true", "1", "yes"}

    orders_to_submit: list[dict[str, Any]] = []
    tick_size = symbol_info.get("tick_size")
    step_size = symbol_info.get("step_size")
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")

    def _append_close_order(raw_qty: float, *, side: str, price: float, reduce_only: bool | None, position_side: str | None) -> None:
        rounded_price = _round_order_price(price, tick_size, side)
        rounded_qty = _round_order_qty(abs(raw_qty), step_size)
        if rounded_qty <= 0:
            result["warnings"].append(f"{symbol} {position_side or 'BOTH'} 平仓数量向下取整后为 0，已跳过")
            return
        if min_qty is not None and rounded_qty < float(min_qty):
            result["warnings"].append(f"{symbol} {position_side or 'BOTH'} 平仓数量低于最小数量，已跳过")
            return
        if min_notional is not None and rounded_qty * rounded_price < float(min_notional):
            result["warnings"].append(f"{symbol} {position_side or 'BOTH'} 平仓名义低于最小下单额，已跳过")
            return
        orders_to_submit.append(
            _build_close_order_request(
                symbol=symbol,
                side=side,
                qty=rounded_qty,
                price=rounded_price,
                reduce_only=reduce_only,
                position_side=position_side,
            )
        )

    if dual_side:
        long_position = _extract_futures_position(account_info, symbol, "LONG")
        short_position = _extract_futures_position(account_info, symbol, "SHORT")
        long_qty = _safe_numeric(long_position.get("positionAmt"))
        short_qty = abs(_safe_numeric(short_position.get("positionAmt")))
        if long_qty > 0:
            _append_close_order(long_qty, side="SELL", price=bid_price, reduce_only=None, position_side="LONG")
        if short_qty > 0:
            _append_close_order(short_qty, side="BUY", price=ask_price, reduce_only=None, position_side="SHORT")
    else:
        both_position = _extract_futures_position(account_info, symbol)
        position_amt = _safe_numeric(both_position.get("positionAmt"))
        if position_amt > 0:
            _append_close_order(position_amt, side="SELL", price=bid_price, reduce_only=True, position_side=None)
        elif position_amt < 0:
            _append_close_order(abs(position_amt), side="BUY", price=ask_price, reduce_only=True, position_side=None)

    result["attempted"] = len(orders_to_submit)
    for order_request in orders_to_submit:
        try:
            response = post_futures_order(
                symbol=order_request["symbol"],
                side=order_request["side"],
                quantity=order_request["quantity"],
                price=order_request["price"],
                api_key=api_key,
                api_secret=api_secret,
                time_in_force=order_request["time_in_force"],
                reduce_only=order_request.get("reduce_only"),
                position_side=order_request.get("position_side"),
            )
            result["submitted"] += 1
            result["orders"].append(
                {
                    **order_request,
                    "order_id": response.get("orderId"),
                    "client_order_id": response.get("clientOrderId"),
                }
            )
        except Exception as exc:
            result["errors"].append(
                {
                    **order_request,
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
    return result


def _execute_stop_actions(
    *,
    symbol: str,
    cancel_open_orders: bool,
    close_all_positions: bool,
) -> dict[str, Any]:
    summary = _build_stop_execution_summary(
        symbol=symbol,
        cancel_open_orders=cancel_open_orders,
        close_all_positions=close_all_positions,
    )
    if not cancel_open_orders and not close_all_positions:
        return summary
    if close_all_positions and not cancel_open_orders:
        summary["warnings"].append("未勾选撤销委托时直接平仓，旧挂单可能继续成交")
    creds = load_binance_api_credentials()
    if not creds:
        raise RuntimeError("未加载 Binance API 凭据，无法执行撤单/平仓")
    api_key, api_secret = creds
    if cancel_open_orders:
        cancel_result = _cancel_symbol_open_orders(symbol=symbol, api_key=api_key, api_secret=api_secret)
        summary["cancel_open_orders_executed"] = True
        summary["cancel_attempted_count"] = cancel_result["attempted"]
        summary["cancel_success_count"] = cancel_result["success"]
        summary["cancel_errors"] = cancel_result["errors"]
    if close_all_positions:
        summary["close_all_positions_executed"] = True
        live_snapshot = load_live_flatten_snapshot(symbol, api_key, api_secret)
        summary["close_attempted_count"] = len(live_snapshot.get("orders", []))
        summary["warnings"].extend(live_snapshot.get("warnings", []))
        if not live_snapshot.get("orders"):
            summary["warnings"].append("当前无可平持仓，未启动跟价平仓进程")
        else:
            flatten_result = _start_flatten_process(
                {
                    "symbol": symbol,
                    "client_order_prefix": flatten_client_order_prefix(symbol),
                    "sleep_seconds": 2.0,
                    "recv_window": 5000,
                    "max_consecutive_errors": 20,
                    "events_jsonl": str(_flatten_events_path(symbol)),
                }
            )
            summary["flatten_started"] = bool(flatten_result.get("started"))
            summary["flatten_already_running"] = bool(flatten_result.get("already_running"))
            if flatten_result.get("started"):
                summary["warnings"].append("已启动买一/卖一 maker 跟价平仓，直到仓位归零")
            elif flatten_result.get("already_running"):
                summary["warnings"].append("买一/卖一 maker 跟价平仓已在运行")
    return summary


def _normalize_spot_runner_payload(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol", SPOT_RUNNER_DEFAULT_CONFIG["symbol"])).upper().strip() or "BTCUSDT"
    strategy_mode = str(payload.get("strategy_mode", SPOT_RUNNER_DEFAULT_CONFIG["strategy_mode"])).strip() or "spot_one_way_long"
    grid_level_mode = str(payload.get("grid_level_mode", "arithmetic")).strip().lower()
    min_price = _safe_float(payload.get("min_price", SPOT_RUNNER_DEFAULT_CONFIG["min_price"]), "min_price")
    max_price = _safe_float(payload.get("max_price", SPOT_RUNNER_DEFAULT_CONFIG["max_price"]), "max_price")
    n = _safe_int(payload.get("n", SPOT_RUNNER_DEFAULT_CONFIG["n"]), "n")
    total_quote_budget = _safe_float(
        payload.get("total_quote_budget", SPOT_RUNNER_DEFAULT_CONFIG["total_quote_budget"]),
        "total_quote_budget",
    )
    sleep_seconds = _safe_float(payload.get("sleep_seconds", SPOT_RUNNER_DEFAULT_CONFIG["sleep_seconds"]), "sleep_seconds")
    cancel_stale = _safe_bool(payload.get("cancel_stale", True), "cancel_stale")
    apply = _safe_bool(payload.get("apply", True), "apply")
    reset_state = _safe_bool(payload.get("reset_state", True), "reset_state")
    grid_band_ratio = _safe_float(payload.get("grid_band_ratio", SPOT_RUNNER_DEFAULT_CONFIG["grid_band_ratio"]), "grid_band_ratio")
    attack_buy_levels = _safe_int(payload.get("attack_buy_levels", SPOT_RUNNER_DEFAULT_CONFIG["attack_buy_levels"]), "attack_buy_levels")
    attack_sell_levels = _safe_int(payload.get("attack_sell_levels", SPOT_RUNNER_DEFAULT_CONFIG["attack_sell_levels"]), "attack_sell_levels")
    attack_per_order_notional = _safe_float(
        payload.get("attack_per_order_notional", SPOT_RUNNER_DEFAULT_CONFIG["attack_per_order_notional"]),
        "attack_per_order_notional",
    )
    defense_buy_levels = _safe_int(payload.get("defense_buy_levels", SPOT_RUNNER_DEFAULT_CONFIG["defense_buy_levels"]), "defense_buy_levels")
    defense_sell_levels = _safe_int(payload.get("defense_sell_levels", SPOT_RUNNER_DEFAULT_CONFIG["defense_sell_levels"]), "defense_sell_levels")
    defense_per_order_notional = _safe_float(
        payload.get("defense_per_order_notional", SPOT_RUNNER_DEFAULT_CONFIG["defense_per_order_notional"]),
        "defense_per_order_notional",
    )
    inventory_soft_limit_notional = _safe_float(
        payload.get("inventory_soft_limit_notional", SPOT_RUNNER_DEFAULT_CONFIG["inventory_soft_limit_notional"]),
        "inventory_soft_limit_notional",
    )
    inventory_hard_limit_notional = _safe_float(
        payload.get("inventory_hard_limit_notional", SPOT_RUNNER_DEFAULT_CONFIG["inventory_hard_limit_notional"]),
        "inventory_hard_limit_notional",
    )
    center_shift_trigger_ratio = _safe_float(
        payload.get("center_shift_trigger_ratio", SPOT_RUNNER_DEFAULT_CONFIG["center_shift_trigger_ratio"]),
        "center_shift_trigger_ratio",
    )
    center_shift_confirm_cycles = _safe_int(
        payload.get("center_shift_confirm_cycles", SPOT_RUNNER_DEFAULT_CONFIG["center_shift_confirm_cycles"]),
        "center_shift_confirm_cycles",
    )
    center_shift_step_ratio = _safe_float(
        payload.get("center_shift_step_ratio", SPOT_RUNNER_DEFAULT_CONFIG["center_shift_step_ratio"]),
        "center_shift_step_ratio",
    )
    buy_pause_amp_trigger_ratio = _safe_float(
        payload.get("buy_pause_amp_trigger_ratio", SPOT_RUNNER_DEFAULT_CONFIG["buy_pause_amp_trigger_ratio"]),
        "buy_pause_amp_trigger_ratio",
    )
    buy_pause_down_return_trigger_ratio = _safe_float(
        payload.get(
            "buy_pause_down_return_trigger_ratio",
            SPOT_RUNNER_DEFAULT_CONFIG["buy_pause_down_return_trigger_ratio"],
        ),
        "buy_pause_down_return_trigger_ratio",
    )
    freeze_shift_abs_return_trigger_ratio = _safe_float(
        payload.get(
            "freeze_shift_abs_return_trigger_ratio",
            SPOT_RUNNER_DEFAULT_CONFIG["freeze_shift_abs_return_trigger_ratio"],
        ),
        "freeze_shift_abs_return_trigger_ratio",
    )
    inventory_recycle_age_minutes = _safe_float(
        payload.get("inventory_recycle_age_minutes", SPOT_RUNNER_DEFAULT_CONFIG["inventory_recycle_age_minutes"]),
        "inventory_recycle_age_minutes",
    )
    inventory_recycle_loss_tolerance_ratio = _safe_float(
        payload.get(
            "inventory_recycle_loss_tolerance_ratio",
            SPOT_RUNNER_DEFAULT_CONFIG["inventory_recycle_loss_tolerance_ratio"],
        ),
        "inventory_recycle_loss_tolerance_ratio",
    )
    inventory_recycle_min_profit_ratio = _safe_float(
        payload.get(
            "inventory_recycle_min_profit_ratio",
            SPOT_RUNNER_DEFAULT_CONFIG["inventory_recycle_min_profit_ratio"],
        ),
        "inventory_recycle_min_profit_ratio",
    )
    max_single_cycle_new_orders = _safe_int(
        payload.get("max_single_cycle_new_orders", SPOT_RUNNER_DEFAULT_CONFIG["max_single_cycle_new_orders"]),
        "max_single_cycle_new_orders",
    )
    runtime_guard_config = normalize_runtime_guard_payload(payload)

    if grid_level_mode not in set(supported_grid_level_modes()):
        raise ValueError(
            f"unsupported grid_level_mode: {grid_level_mode}; "
            f"supported: {','.join(supported_grid_level_modes())}"
        )
    if strategy_mode not in {"spot_one_way_long", "spot_volume_shift_long"}:
        raise ValueError("unsupported strategy_mode")
    if strategy_mode == "spot_one_way_long":
        if min_price <= 0 or max_price <= 0 or min_price >= max_price:
            raise ValueError("invalid min_price/max_price")
        if n <= 0:
            raise ValueError("n must be > 0")
    if total_quote_budget <= 0:
        raise ValueError("total_quote_budget must be > 0")
    if sleep_seconds <= 0:
        raise ValueError("sleep_seconds must be > 0")
    if grid_band_ratio <= 0:
        raise ValueError("grid_band_ratio must be > 0")
    if attack_buy_levels <= 0 or attack_sell_levels <= 0 or defense_buy_levels <= 0 or defense_sell_levels <= 0:
        raise ValueError("buy/sell levels must be > 0")
    if attack_per_order_notional <= 0 or defense_per_order_notional <= 0:
        raise ValueError("per_order_notional must be > 0")
    if inventory_soft_limit_notional <= 0 or inventory_hard_limit_notional <= 0:
        raise ValueError("inventory limits must be > 0")
    if inventory_soft_limit_notional > inventory_hard_limit_notional:
        raise ValueError("inventory_soft_limit_notional cannot exceed inventory_hard_limit_notional")
    if center_shift_trigger_ratio <= 0 or center_shift_confirm_cycles <= 0 or center_shift_step_ratio <= 0:
        raise ValueError("center shift params must be > 0")
    if max_single_cycle_new_orders <= 0:
        raise ValueError("max_single_cycle_new_orders must be > 0")
    _validate_market_symbol(symbol=symbol, market_type="spot", contract_type=None)

    config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
    config.update(
        {
            "market_type": "spot",
            "strategy_mode": strategy_mode,
            "symbol": symbol,
            "grid_level_mode": grid_level_mode,
            "min_price": min_price,
            "max_price": max_price,
            "n": n,
            "total_quote_budget": total_quote_budget,
            "sleep_seconds": sleep_seconds,
            "cancel_stale": cancel_stale,
            "apply": apply,
            "reset_state": reset_state,
            "grid_band_ratio": grid_band_ratio,
            "attack_buy_levels": attack_buy_levels,
            "attack_sell_levels": attack_sell_levels,
            "attack_per_order_notional": attack_per_order_notional,
            "defense_buy_levels": defense_buy_levels,
            "defense_sell_levels": defense_sell_levels,
            "defense_per_order_notional": defense_per_order_notional,
            "inventory_soft_limit_notional": inventory_soft_limit_notional,
            "inventory_hard_limit_notional": inventory_hard_limit_notional,
            "center_shift_trigger_ratio": center_shift_trigger_ratio,
            "center_shift_confirm_cycles": center_shift_confirm_cycles,
            "center_shift_step_ratio": center_shift_step_ratio,
            "buy_pause_amp_trigger_ratio": buy_pause_amp_trigger_ratio,
            "buy_pause_down_return_trigger_ratio": buy_pause_down_return_trigger_ratio,
            "freeze_shift_abs_return_trigger_ratio": freeze_shift_abs_return_trigger_ratio,
            "inventory_recycle_age_minutes": inventory_recycle_age_minutes,
            "inventory_recycle_loss_tolerance_ratio": inventory_recycle_loss_tolerance_ratio,
            "inventory_recycle_min_profit_ratio": inventory_recycle_min_profit_ratio,
            "max_single_cycle_new_orders": max_single_cycle_new_orders,
            "run_start_time": runtime_guard_config["run_start_time"],
            "run_end_time": runtime_guard_config["run_end_time"],
            "rolling_hourly_loss_limit": runtime_guard_config["rolling_hourly_loss_limit"],
            "max_cumulative_notional": runtime_guard_config["max_cumulative_notional"],
        }
    )
    config.update(_default_spot_runtime_paths_for_symbol(symbol))
    return config


def _build_spot_runner_command(config: dict[str, Any]) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "grid_optimizer.spot_loop_runner",
        "--symbol",
        str(config["symbol"]),
        "--strategy-mode",
        str(config.get("strategy_mode", "spot_one_way_long")),
        "--min-price",
        str(config["min_price"]),
        "--max-price",
        str(config["max_price"]),
        "--n",
        str(config["n"]),
        "--total-quote-budget",
        str(config["total_quote_budget"]),
        "--grid-level-mode",
        str(config.get("grid_level_mode", "arithmetic")),
        "--sleep-seconds",
        str(config.get("sleep_seconds", 10.0)),
        "--client-order-prefix",
        str(config.get("client_order_prefix", _spot_client_order_prefix(str(config["symbol"])))),
        "--state-path",
        str(config.get("state_path")),
        "--summary-jsonl",
        str(config.get("summary_jsonl")),
    ]
    extra_args: list[tuple[str, Any]] = [
        ("--grid-band-ratio", config.get("grid_band_ratio")),
        ("--attack-buy-levels", config.get("attack_buy_levels")),
        ("--attack-sell-levels", config.get("attack_sell_levels")),
        ("--attack-per-order-notional", config.get("attack_per_order_notional")),
        ("--defense-buy-levels", config.get("defense_buy_levels")),
        ("--defense-sell-levels", config.get("defense_sell_levels")),
        ("--defense-per-order-notional", config.get("defense_per_order_notional")),
        ("--inventory-soft-limit-notional", config.get("inventory_soft_limit_notional")),
        ("--inventory-hard-limit-notional", config.get("inventory_hard_limit_notional")),
        ("--center-shift-trigger-ratio", config.get("center_shift_trigger_ratio")),
        ("--center-shift-confirm-cycles", config.get("center_shift_confirm_cycles")),
        ("--center-shift-step-ratio", config.get("center_shift_step_ratio")),
        ("--buy-pause-amp-trigger-ratio", config.get("buy_pause_amp_trigger_ratio")),
        ("--buy-pause-down-return-trigger-ratio", config.get("buy_pause_down_return_trigger_ratio")),
        ("--freeze-shift-abs-return-trigger-ratio", config.get("freeze_shift_abs_return_trigger_ratio")),
        ("--inventory-recycle-age-minutes", config.get("inventory_recycle_age_minutes")),
        ("--inventory-recycle-loss-tolerance-ratio", config.get("inventory_recycle_loss_tolerance_ratio")),
        ("--inventory-recycle-min-profit-ratio", config.get("inventory_recycle_min_profit_ratio")),
        ("--max-single-cycle-new-orders", config.get("max_single_cycle_new_orders")),
        ("--run-start-time", config.get("run_start_time")),
        ("--run-end-time", config.get("run_end_time")),
        ("--rolling-hourly-loss-limit", config.get("rolling_hourly_loss_limit")),
        ("--max-cumulative-notional", config.get("max_cumulative_notional")),
    ]
    for flag, value in extra_args:
        if value is not None:
            command.extend([flag, str(value)])
    command.append("--cancel-stale" if config.get("cancel_stale", True) else "--no-cancel-stale")
    command.append("--apply" if config.get("apply", True) else "--no-apply")
    return command


def _cancel_spot_strategy_orders(config: dict[str, Any]) -> dict[str, Any]:
    creds = load_binance_api_credentials()
    if not creds:
        return {"canceled": 0, "warning": "Binance API credentials are missing"}
    api_key, api_secret = creds
    symbol = str(config.get("symbol", "")).upper().strip()
    prefix = str(config.get("client_order_prefix", _spot_client_order_prefix(symbol))).strip()
    if not symbol or not prefix:
        return {"canceled": 0}
    open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
    ours = [order for order in open_orders if _is_spot_strategy_order(order, prefix)]
    canceled = 0
    errors: list[str] = []
    for order in ours:
        order_id = order.get("orderId")
        if order_id is None:
            continue
        try:
            delete_spot_order(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                order_id=int(order_id),
            )
            canceled += 1
        except Exception as exc:  # pragma: no cover - depends on runtime/API
            errors.append(f"{type(exc).__name__}: {exc}")
    return {"canceled": canceled, "errors": errors}


def _start_spot_runner_process(config: dict[str, Any]) -> dict[str, Any]:
    symbol = str(config.get("symbol", SPOT_RUNNER_DEFAULT_CONFIG["symbol"])).upper().strip() or "BTCUSDT"
    runner = _read_spot_runner_process_for_symbol(symbol)
    restarted = False
    if runner.get("is_running"):
        current_config = dict(runner.get("config") or {})
        compare_fields = {
            "symbol",
            "strategy_mode",
            "grid_level_mode",
            "min_price",
            "max_price",
            "n",
            "total_quote_budget",
            "sleep_seconds",
            "cancel_stale",
            "apply",
            "state_path",
            "summary_jsonl",
            "client_order_prefix",
            "grid_band_ratio",
            "attack_buy_levels",
            "attack_sell_levels",
            "attack_per_order_notional",
            "defense_buy_levels",
            "defense_sell_levels",
            "defense_per_order_notional",
            "inventory_soft_limit_notional",
            "inventory_hard_limit_notional",
            "center_shift_trigger_ratio",
            "center_shift_confirm_cycles",
            "center_shift_step_ratio",
            "buy_pause_amp_trigger_ratio",
            "buy_pause_down_return_trigger_ratio",
            "freeze_shift_abs_return_trigger_ratio",
            "inventory_recycle_age_minutes",
            "inventory_recycle_loss_tolerance_ratio",
            "inventory_recycle_min_profit_ratio",
            "max_single_cycle_new_orders",
            "run_start_time",
            "run_end_time",
            "rolling_hourly_loss_limit",
            "max_cumulative_notional",
        }
        config_changed = any(current_config.get(field) != config.get(field) for field in compare_fields)
        if not config_changed:
            return {"started": False, "already_running": True, "runner": runner, "symbol": symbol, "restarted": False}
        _stop_spot_runner_process(symbol, cancel_orders=False)
        restarted = True

    if _truthy(config.get("reset_state", True)):
        cleanup = _cancel_spot_strategy_orders(config)
        state_path = Path(str(config.get("state_path", "")))
        if state_path.exists():
            try:
                state_path.unlink()
            except OSError:
                pass
    else:
        cleanup = {"canceled": 0}

    _save_spot_runner_control_config(config, symbol=symbol)
    log_path = _spot_runner_log_path(symbol)
    pid_path = _spot_runner_pid_path(symbol)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_path = str((Path.cwd() / "src").resolve())
    current_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"

    command = _build_spot_runner_command(config)
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(Path.cwd()),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    time.sleep(1.0)
    return {
        "started": True,
        "already_running": False,
        "restarted": restarted,
        "runner": _read_spot_runner_process_for_symbol(symbol),
        "command": command,
        "symbol": symbol,
        "cleanup": cleanup,
    }


def _stop_spot_runner_process(
    symbol: str | None = None,
    *,
    timeout_seconds: float = 10.0,
    cancel_orders: bool = True,
) -> dict[str, Any]:
    normalized_symbol = (
        str(symbol or SPOT_RUNNER_DEFAULT_CONFIG["symbol"]).upper().strip()
        or str(SPOT_RUNNER_DEFAULT_CONFIG["symbol"])
    )
    runner = _read_spot_runner_process_for_symbol(normalized_symbol)
    pid_path = _spot_runner_pid_path(normalized_symbol)
    config = _load_spot_runner_control_config(normalized_symbol)
    cleanup = _cancel_spot_strategy_orders(config) if cancel_orders else {"canceled": 0}

    pid = runner.get("pid")
    if not pid or not runner.get("is_running"):
        if pid_path.exists():
            try:
                pid_path.unlink()
            except OSError:
                pass
        return {
            "stopped": False,
            "already_stopped": True,
            "runner": _read_spot_runner_process_for_symbol(normalized_symbol),
            "symbol": normalized_symbol,
            "cleanup": cleanup,
        }

    os.kill(int(pid), signal.SIGTERM)
    deadline = time.time() + max(timeout_seconds, 0.5)
    while time.time() < deadline:
        probe = _read_spot_runner_process_for_symbol(normalized_symbol)
        if not probe.get("is_running"):
            if pid_path.exists():
                try:
                    pid_path.unlink()
                except OSError:
                    pass
            return {
                "stopped": True,
                "killed": False,
                "runner": probe,
                "symbol": normalized_symbol,
                "cleanup": cleanup,
            }
        time.sleep(0.25)

    os.kill(int(pid), signal.SIGKILL)
    time.sleep(0.5)
    if pid_path.exists():
        try:
            pid_path.unlink()
        except OSError:
            pass
    return {
        "stopped": True,
        "killed": True,
        "runner": _read_spot_runner_process_for_symbol(normalized_symbol),
        "symbol": normalized_symbol,
        "cleanup": cleanup,
    }


def _build_spot_runner_snapshot(symbol: str | None = None) -> dict[str, Any]:
    config = _load_spot_runner_control_config(symbol)
    normalized_symbol = str(config.get("symbol", SPOT_RUNNER_DEFAULT_CONFIG["symbol"])).upper().strip() or "BTCUSDT"
    runner = _read_spot_runner_process_for_symbol(normalized_symbol)
    config = dict(config)
    config["symbol"] = normalized_symbol
    runtime_paths = _default_spot_runtime_paths_for_symbol(normalized_symbol)
    for key, value in runtime_paths.items():
        config.setdefault(key, value)

    def _float_or_default(value: Any, name: str, default: float = 0.0) -> float:
        return _safe_float(default if value is None else value, name)

    def _int_or_default(value: Any, name: str, default: int = 0) -> int:
        return _safe_int(default if value is None else value, name)

    state = _read_json_dict(Path(str(config["state_path"]))) or {}
    events = _tail_jsonl_dicts(Path(str(config["summary_jsonl"])), limit=20)
    latest_event = events[-1] if events else {}
    managed_base_qty = 0.0
    cells = state.get("cells", {})
    if isinstance(cells, dict):
        managed_base_qty = sum(
            _float_or_default((item or {}).get("position_qty"), "position_qty")
            for item in cells.values()
            if isinstance(item, dict)
        )
    inventory_lots = state.get("inventory_lots", [])
    if not isinstance(inventory_lots, list):
        inventory_lots = []
    inventory_qty = sum(_float_or_default((item or {}).get("qty"), "qty") for item in inventory_lots if isinstance(item, dict))
    inventory_cost_quote = sum(
        _float_or_default((item or {}).get("cost_quote"), "cost_quote")
        for item in inventory_lots
        if isinstance(item, dict)
    )
    inventory_avg_cost = (inventory_cost_quote / inventory_qty) if inventory_qty > 1e-12 else 0.0
    metrics = state.get("metrics")
    if not isinstance(metrics, dict):
        metrics = {}
    recent_trades = metrics.get("recent_trades")
    if not isinstance(recent_trades, list):
        recent_trades = []
    market_mid_price = None
    if isinstance(latest_event, dict):
        market_mid_price = _float_or_default(latest_event.get("mid_price"), "mid_price")
    if market_mid_price <= 0:
        market_mid_price = 0.0
    unrealized_pnl = ((market_mid_price - inventory_avg_cost) * inventory_qty) if market_mid_price > 0 and inventory_qty > 0 else 0.0

    snapshot: dict[str, Any] = {
        "symbol": normalized_symbol,
        "runner": runner,
        "config": config,
        "state": {
            "cycle": int(state.get("cycle", 0) or 0),
            "last_trade_time_ms": int(state.get("last_trade_time_ms", 0) or 0),
            "managed_base_qty": max(managed_base_qty, inventory_qty),
            "cell_count": len(cells) if isinstance(cells, dict) else 0,
            "cells": cells if isinstance(cells, dict) else {},
            "inventory_qty": inventory_qty,
            "inventory_avg_cost": inventory_avg_cost,
            "inventory_cost_quote": inventory_cost_quote,
            "center_price": _float_or_default(state.get("center_price"), "center_price"),
            "last_mode": str(state.get("last_mode", "") or ""),
            "center_shift_count": _int_or_default(state.get("center_shift_count"), "center_shift_count"),
        },
        "trade_summary": {
            "gross_notional": _float_or_default(metrics.get("gross_notional"), "gross_notional"),
            "buy_notional": _float_or_default(metrics.get("buy_notional"), "buy_notional"),
            "sell_notional": _float_or_default(metrics.get("sell_notional"), "sell_notional"),
            "commission_quote": _float_or_default(metrics.get("commission_quote"), "commission_quote"),
            "commission_raw_by_asset": metrics.get("commission_raw_by_asset") if isinstance(metrics.get("commission_raw_by_asset"), dict) else {},
            "realized_pnl": _float_or_default(metrics.get("realized_pnl"), "realized_pnl"),
            "recycle_realized_pnl": _float_or_default(metrics.get("recycle_realized_pnl"), "recycle_realized_pnl"),
            "recycle_loss_abs": _float_or_default(metrics.get("recycle_loss_abs"), "recycle_loss_abs"),
            "trade_count": _int_or_default(metrics.get("trade_count"), "trade_count"),
            "maker_count": _int_or_default(metrics.get("maker_count"), "maker_count"),
            "buy_count": _int_or_default(metrics.get("buy_count"), "buy_count"),
            "sell_count": _int_or_default(metrics.get("sell_count"), "sell_count"),
            "recent_trades": recent_trades[-30:],
            "unrealized_pnl": unrealized_pnl,
            "net_pnl_estimate": _float_or_default(metrics.get("realized_pnl"), "realized_pnl") + unrealized_pnl,
        },
        "risk_controls": {
            "strategy_mode": str(config.get("strategy_mode", "spot_one_way_long")),
            "center_price": _float_or_default(state.get("center_price"), "center_price"),
            "center_shift_count": _int_or_default(state.get("center_shift_count"), "center_shift_count"),
            "mode": str(state.get("last_mode", "") or latest_event.get("mode", "") if isinstance(latest_event, dict) else ""),
            "runtime_status": str(latest_event.get("runtime_status", "") or "running") if isinstance(latest_event, dict) else "running",
            "stop_triggered": bool(latest_event.get("stop_triggered")) if isinstance(latest_event, dict) else False,
            "stop_reason": latest_event.get("stop_reason") if isinstance(latest_event, dict) else None,
            "stop_reasons": list(latest_event.get("stop_reasons") or []) if isinstance(latest_event, dict) else [],
            "stop_triggered_at": latest_event.get("stop_triggered_at") if isinstance(latest_event, dict) else None,
            "run_start_time": config.get("run_start_time"),
            "run_end_time": config.get("run_end_time"),
            "rolling_hourly_loss_limit": _float_or_default(config.get("rolling_hourly_loss_limit"), "rolling_hourly_loss_limit"),
            "max_cumulative_notional": _float_or_default(config.get("max_cumulative_notional"), "max_cumulative_notional"),
            "rolling_hourly_loss": _float_or_default(latest_event.get("rolling_hourly_loss"), "rolling_hourly_loss") if isinstance(latest_event, dict) else 0.0,
            "cumulative_gross_notional": _float_or_default(latest_event.get("cumulative_gross_notional"), "cumulative_gross_notional") if isinstance(latest_event, dict) else 0.0,
            "buy_paused": bool(latest_event.get("buy_paused")) if isinstance(latest_event, dict) else False,
            "pause_reasons": list(latest_event.get("pause_reasons") or []) if isinstance(latest_event, dict) else [],
            "inventory_soft_limit_notional": _float_or_default(config.get("inventory_soft_limit_notional"), "inventory_soft_limit_notional"),
            "inventory_hard_limit_notional": _float_or_default(config.get("inventory_hard_limit_notional"), "inventory_hard_limit_notional"),
            "market_guard_return_ratio": _float_or_default(latest_event.get("market_guard_return_ratio"), "market_guard_return_ratio") if isinstance(latest_event, dict) else 0.0,
            "market_guard_amplitude_ratio": _float_or_default(latest_event.get("market_guard_amplitude_ratio"), "market_guard_amplitude_ratio") if isinstance(latest_event, dict) else 0.0,
            "shift_frozen": bool(latest_event.get("shift_frozen")) if isinstance(latest_event, dict) else False,
            "effective_buy_levels": _int_or_default(latest_event.get("effective_buy_levels"), "effective_buy_levels") if isinstance(latest_event, dict) else 0,
            "effective_sell_levels": _int_or_default(latest_event.get("effective_sell_levels"), "effective_sell_levels") if isinstance(latest_event, dict) else 0,
            "effective_per_order_notional": _float_or_default(latest_event.get("effective_per_order_notional"), "effective_per_order_notional") if isinstance(latest_event, dict) else 0.0,
        },
        "events": events,
        "latest_event": latest_event,
        "warnings": [],
    }

    try:
        symbol_info = fetch_spot_symbol_config(normalized_symbol)
        snapshot["symbol_info"] = {
            "base_asset": symbol_info.get("base_asset"),
            "quote_asset": symbol_info.get("quote_asset"),
            "tick_size": symbol_info.get("tick_size"),
            "step_size": symbol_info.get("step_size"),
            "min_qty": symbol_info.get("min_qty"),
            "min_notional": symbol_info.get("min_notional"),
        }
        book_rows = fetch_spot_book_tickers(symbol=normalized_symbol)
        if book_rows:
            book = book_rows[0]
            market_mid_price = (
                (_float_or_default(book.get("bid_price"), "bid_price") + _float_or_default(book.get("ask_price"), "ask_price")) / 2.0
            )
            snapshot["market"] = {
                "bid_price": book.get("bid_price"),
                "ask_price": book.get("ask_price"),
                "mid_price": market_mid_price,
            }
            snapshot["trade_summary"]["unrealized_pnl"] = ((market_mid_price - inventory_avg_cost) * inventory_qty) if inventory_qty > 0 else 0.0
            snapshot["trade_summary"]["net_pnl_estimate"] = snapshot["trade_summary"]["realized_pnl"] + snapshot["trade_summary"]["unrealized_pnl"]
        creds = load_binance_api_credentials()
        if creds:
            api_key, api_secret = creds
            account_info = fetch_spot_account_info(api_key, api_secret)
            open_orders = fetch_spot_open_orders(normalized_symbol, api_key, api_secret)
            prefix = str(config.get("client_order_prefix", _spot_client_order_prefix(normalized_symbol))).strip()
            strategy_open_orders = [order for order in open_orders if _is_spot_strategy_order(order, prefix)]
            base_asset = str(symbol_info.get("base_asset") or "").upper().strip()
            quote_asset = str(symbol_info.get("quote_asset") or "").upper().strip()
            base_free, base_locked = _extract_spot_balance(account_info, base_asset)
            quote_free, quote_locked = _extract_spot_balance(account_info, quote_asset)
            snapshot["balances"] = {
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "base_free": base_free,
                "base_locked": base_locked,
                "quote_free": quote_free,
                "quote_locked": quote_locked,
            }
            snapshot["open_orders"] = strategy_open_orders
        else:
            snapshot["warnings"].append("Binance API credentials are missing")
    except Exception as exc:  # pragma: no cover - runtime specific
        snapshot["warnings"].append(f"{type(exc).__name__}: {exc}")
    return snapshot

SERVER_HUB_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>交易界面入口</title>
  <style>
    :root {
      --bg: #f4f1ea;
      --panel: rgba(255, 255, 255, 0.92);
      --text: #1c1b19;
      --muted: #6a665f;
      --line: #ded7ca;
      --brand: #0b6f68;
      --brand-deep: #084c47;
      --brand-soft: #e4f5f2;
      --warn: #946200;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(11, 111, 104, 0.12), transparent 28%),
        radial-gradient(circle at 90% 10%, rgba(148, 98, 0, 0.10), transparent 24%),
        linear-gradient(180deg, #faf8f2 0%, var(--bg) 100%);
    }
    .wrap {
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 18px 56px;
    }
    .hero {
      background: linear-gradient(135deg, rgba(255,255,255,0.96), rgba(247,244,236,0.90));
      border: 1px solid var(--line);
      border-radius: 28px;
      padding: 28px;
      box-shadow: 0 18px 40px rgba(36, 32, 24, 0.08);
    }
    .eyebrow {
      margin: 0 0 10px;
      color: var(--brand);
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(32px, 5vw, 52px);
      line-height: 1.04;
    }
    .lead {
      margin: 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 17px;
      line-height: 1.7;
    }
    .tips {
      margin-top: 18px;
      display: grid;
      gap: 8px;
      color: var(--muted);
      font-size: 14px;
    }
    .grid {
      margin-top: 22px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px;
      box-shadow: 0 14px 30px rgba(24, 20, 12, 0.06);
      display: grid;
      gap: 14px;
    }
    .card h2 {
      margin: 0;
      font-size: 22px;
    }
    .meta {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .badge {
      display: inline-flex;
      width: fit-content;
      align-items: center;
      gap: 8px;
      padding: 7px 12px;
      border-radius: 999px;
      background: var(--brand-soft);
      color: var(--brand-deep);
      font-size: 13px;
      font-weight: 700;
    }
    .badge.warn {
      background: #fff5dd;
      color: var(--warn);
    }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .btn {
      text-decoration: none;
      border-radius: 999px;
      padding: 11px 16px;
      font-weight: 700;
      font-size: 14px;
      border: 1px solid var(--line);
      color: var(--brand-deep);
      background: #fff;
    }
    .btn.primary {
      border-color: var(--brand);
      background: var(--brand);
      color: #fff;
    }
    .foot {
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }
    code {
      font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 12px;
      background: rgba(15, 23, 42, 0.06);
      border-radius: 8px;
      padding: 2px 6px;
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <p class="eyebrow">Trading Hub</p>
      <h1>三台交易界面统一入口</h1>
      <p class="lead">这个页面只负责分发入口，不承载交易逻辑。点击下面任一服务器卡片，即可进入对应机器上的交易界面或监控页。</p>
      <div class="tips">
        <div>进入目标页面后，浏览器会提示 Basic Auth。当前统一用户名是 <code>grid</code>。</div>
        <div>第三台 <code>43.155.136.111</code> 目前还没有检测到运行中的 <code>wangge-web</code>，如果点不开，需要先把该机服务拉起来。</div>
      </div>
    </section>

    <section class="grid">
      <article class="card">
        <span class="badge">主入口 A</span>
        <h2>43.131.232.150</h2>
        <div class="meta">当前已验证可访问，适合作为统一入口的主跳板。</div>
        <div class="actions">
          <a class="btn primary" href="http://43.131.232.150:8788/" target="_blank" rel="noopener noreferrer">打开交易界面</a>
          <a class="btn" href="http://43.131.232.150:8788/monitor" target="_blank" rel="noopener noreferrer">打开监控页</a>
        </div>
      </article>

      <article class="card">
        <span class="badge">主入口 B</span>
        <h2>43.155.163.114</h2>
        <div class="meta">当前已验证可访问，已部署 web 服务，可从这里进入对应账户的交易页面。</div>
        <div class="actions">
          <a class="btn primary" href="http://43.155.163.114:8788/" target="_blank" rel="noopener noreferrer">打开交易界面</a>
          <a class="btn" href="http://43.155.163.114:8788/monitor" target="_blank" rel="noopener noreferrer">打开监控页</a>
        </div>
      </article>

      <article class="card">
        <span class="badge">在线入口</span>
        <h2>43.155.136.111</h2>
        <div class="meta">当前已确认该机通过 <code>8787</code> 对外提供交易界面，点下面按钮即可直接进入。</div>
        <div class="actions">
          <a class="btn primary" href="http://43.155.136.111:8787/" target="_blank" rel="noopener noreferrer">尝试打开交易界面</a>
          <a class="btn" href="http://43.155.136.111:8787/monitor" target="_blank" rel="noopener noreferrer">尝试打开监控页</a>
        </div>
      </article>
    </section>

    <div class="foot">
      公开的是入口页，具体交易页面仍然保留各自服务器的认证。
    </div>
  </main>
</body>
</html>
"""

HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>网格策略测算器</title>
  <style>
    :root {
      --bg: #f8f7f4;
      --panel: #ffffff;
      --text: #171717;
      --muted: #6a6a6a;
      --line: #e5e1d8;
      --brand: #0f766e;
      --brand-soft: #e6f6f4;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      background: radial-gradient(circle at 10% 0%, #fffef8 0%, var(--bg) 45%, #f1efe8 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1200px;
      margin: 24px auto 48px;
      padding: 0 16px;
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 8px 30px rgba(13, 18, 30, 0.05);
    }
    .legacy-ranking {
      display: none;
    }
    .header h1 {
      margin: 0 0 6px;
      font-size: 26px;
      letter-spacing: 0.02em;
    }
    .header p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }
    .header-links {
      margin-top: 10px;
      display: flex;
      gap: 10px;
    }
    .header-links a {
      display: inline-flex;
      align-items: center;
      height: 34px;
      padding: 0 12px;
      border-radius: 10px;
      border: 1px solid var(--line);
      text-decoration: none;
      color: #0c4b46;
      background: var(--brand-soft);
      font-size: 13px;
      font-weight: 600;
    }
    form.grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    .is-hidden {
      display: none;
    }
    .field label {
      color: var(--muted);
      font-size: 13px;
    }
    .field input, .field select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      font-size: 14px;
      background: #fff;
    }
    .mode-group {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 6px 8px;
      background: #fff;
      min-height: 38px;
      align-content: center;
      min-width: 0;
    }
    .mode-item {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: #303030;
      white-space: normal;
      word-break: break-word;
      min-width: 0;
    }
    .mode-item input {
      width: 14px;
      height: 14px;
      margin: 0;
      padding: 0;
      flex: 0 0 auto;
    }
    .mode-desc {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
      margin-top: 6px;
    }
    .field input:focus, .field select:focus {
      outline: none;
      border-color: var(--brand);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 4px;
      min-width: 0;
    }
    .actions .msg {
      min-width: 0;
      flex: 1 1 260px;
    }
    button {
      height: 40px;
      border-radius: 10px;
      border: none;
      background: var(--brand);
      color: #fff;
      font-size: 14px;
      font-weight: 600;
      padding: 0 14px;
      cursor: pointer;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.7;
    }
    .hint {
      color: var(--muted);
      font-size: 13px;
    }
    .formula {
      margin: 12px 0 0;
      color: #2f463f;
      font-size: 13px;
      line-height: 1.5;
    }
    .msg {
      font-size: 13px;
      color: var(--muted);
    }
    .msg.error {
      color: var(--danger);
      font-weight: 600;
    }
    .progress-box {
      grid-column: 1 / -1;
      display: grid;
      gap: 6px;
    }
    .progress-track {
      width: 100%;
      height: 10px;
      background: #ebe8de;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid var(--line);
    }
    .progress-fill {
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, #0f766e 0%, #1d9a90 100%);
      transition: width 0.25s ease;
    }
    .progress-text {
      color: var(--muted);
      font-size: 12px;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: var(--brand-soft);
    }
    .kpi .k {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .kpi .v {
      font-size: 18px;
      font-weight: 700;
      color: #0c4b46;
    }
    .value-sub {
      display: block;
      margin-top: 2px;
      font-size: 12px;
      font-weight: 500;
      color: var(--muted);
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
    }
    .layer-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 10px;
    }
    .layer-actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 10px;
      margin-top: 2px;
    }
    .funding-actions {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 10px;
      min-width: 0;
    }
    .plan-overview {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 10px;
    }
    .plan-pill {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      background: #fcfffe;
    }
    .plan-pill .k {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .plan-pill .v {
      font-size: 16px;
      font-weight: 700;
      color: #0c4b46;
    }
    .rank-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 10px;
    }
    .rank-actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 2px;
      min-width: 0;
    }
    .rank-two-cols {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .rank-subtitle {
      margin: 0 0 8px;
      font-size: 15px;
    }
    .delta-better {
      color: #0b6b44;
      font-weight: 700;
    }
    .delta-worse {
      color: #b42318;
      font-weight: 700;
    }
    .delta-neutral {
      color: var(--muted);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 740px;
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      white-space: nowrap;
    }
    .candidate-row {
      cursor: pointer;
    }
    .candidate-row:hover {
      background: #f7f7f2;
    }
    .candidate-row.active {
      background: #e9f6f4;
    }
    th:first-child, td:first-child { text-align: left; }
    thead th {
      background: #faf8f2;
      font-weight: 700;
      color: #3f3f3f;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    @media (max-width: 980px) {
      form.grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .plan-overview { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .layer-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .rank-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .rank-two-cols { grid-template-columns: 1fr; }
    }
    @media (max-width: 600px) {
      form.grid { grid-template-columns: 1fr; }
      .summary { grid-template-columns: 1fr; }
      .plan-overview { grid-template-columns: 1fr; }
      .layer-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>币安网格策略测算器</h1>
      <p>支持现货 V1 单向做多网格 与 U本位/币本位合约网格回测 · 指定起止时间回测 · 本地缓存复用</p>
      <div class="header-links">
        <a href="/rankings">打开独立排行榜页面</a>
        <a href="/basis">打开现货/合约价差监控页</a>
        <a href="/monitor">打开实盘网格监控页</a>
        <a href="/spot_runner">打开现货执行台</a>
        <a href="/strategies">打开策略总览页</a>
      </div>
    </section>

    <section class="card">
      <form id="form" class="grid">
        <div class="field">
          <label>市场类型</label>
          <select id="market_type">
            <option value="futures" selected>合约</option>
            <option value="spot">现货 V1（单向做多）</option>
          </select>
        </div>
        <div class="field market-futures-only">
          <label>合约类型</label>
          <select id="contract_type">
            <option value="usdm" selected>U本位（USDT-M）</option>
            <option value="coinm">币本位（COIN-M）</option>
          </select>
        </div>
        <div class="field">
          <label>交易对</label>
          <select id="symbol">
            <option value="BTCUSDT">加载中...</option>
          </select>
        </div>
        <div class="field">
          <label>测算模式</label>
          <select id="calc_mode">
            <option value="optimize">优化模式（自动找最优N）</option>
            <option value="fixed">固定参数模式（你指定N和每格金额）</option>
          </select>
        </div>
        <div class="field">
          <label>最低价 min_price</label>
          <input id="min_price" type="number" step="0.0001" value="50000" />
        </div>
        <div class="field">
          <label>最高价 max_price</label>
          <input id="max_price" type="number" step="0.0001" value="130000" />
        </div>
        <div class="field optimize-only">
          <label>最大投入资金（名义 / quote）</label>
          <input id="total_buy_notional" type="number" step="0.0001" value="10000" />
        </div>
        <div class="field optimize-only">
          <label>目标成交额（交易赛）</label>
          <input id="target_trade_volume" type="number" step="0.0001" value="500000" />
        </div>
        <div class="field fixed-only">
          <label>固定格子数 N</label>
          <input id="fixed_n" type="number" step="1" value="20" />
        </div>
        <div class="field fixed-only">
          <label>每格买入方式</label>
          <select id="fixed_buy_unit">
            <option value="notional">金额（报价资产）</option>
            <option value="qty">币种份额（数量）</option>
          </select>
        </div>
        <div class="field fixed-only">
          <label id="fixed_buy_value_label">每格买入金额</label>
          <input id="fixed_buy_value" type="number" step="0.0001" value="500" />
        </div>

        <div class="field">
          <label>起始时间（本地时区）</label>
          <input id="start_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>结束时间（本地时区）</label>
          <input id="end_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>K线周期</label>
          <select id="interval">
            <option value="1h" selected>1h</option>
            <option value="1s">1s</option>
            <option value="4h">4h</option>
            <option value="1m">1m</option>
            <option value="1s">1s</option>
          </select>
        </div>
        <div class="field optimize-only">
          <label>N最小值</label>
          <input id="n_min" type="number" step="1" value="5" />
        </div>
        <div class="field optimize-only">
          <label>N最大值</label>
          <input id="n_max" type="number" step="1" value="200" />
        </div>

        <div class="field">
          <label>手续费（单边）</label>
          <input id="fee_rate" type="number" step="0.0001" value="0.0002" />
        </div>
        <div class="field">
          <label>滑点（单边）</label>
          <input id="slippage" type="number" step="0.0001" value="0" />
        </div>
        <div class="field market-futures-only">
          <label>策略方向</label>
          <select id="strategy_direction">
            <option value="long" selected>做多（long）</option>
            <option value="short">做空（short）</option>
            <option value="neutral">中性（起始价为中轴）</option>
          </select>
        </div>
        <div class="field">
          <label>价格网格</label>
          <select id="grid_level_mode">
            <option value="arithmetic" selected>等差（固定价差）</option>
            <option value="geometric">等比（固定百分比）</option>
          </select>
        </div>
        <div class="field market-futures-only">
          <label>资金费率</label>
          <select id="include_funding">
            <option value="1" selected>计入真实资金费率</option>
            <option value="0">忽略资金费率</option>
          </select>
        </div>
        <div class="field optimize-only">
          <label>分配模式（多选）</label>
          <div id="allocation_modes" class="mode-group">
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="equal" checked />equal</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="equal_qty" />equal_qty</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="linear" checked />linear</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="linear_reverse" />linear_reverse</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="quadratic" />quadratic</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="quadratic_reverse" />quadratic_reverse</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="geometric" />geometric</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="geometric_reverse" />geometric_reverse</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="center_heavy" />center_heavy</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="edge_heavy" />edge_heavy</label>
          </div>
          <div class="mode-desc">
            equal: 每格等额；equal_qty: 每格等数量（币安网格更贴近）；linear: 越低价越多；linear_reverse: 越高价越多；
            quadratic/geometric: 低价加仓更激进；对应 reverse 为高价更激进；
            center_heavy: 中间格子更多；edge_heavy: 两端格子更多。
          </div>
        </div>
        <div class="field">
          <label>目标函数</label>
          <select id="objective">
            <option value="competition_volume">competition_volume（交易赛推荐）</option>
            <option value="calmar">calmar（收益/回撤）</option>
            <option value="net_profit">net_profit（净收益）</option>
            <option value="total_return">total_return（总收益率）</option>
            <option value="annualized_return">annualized_return（年化）</option>
            <option value="competition_volume">competition_volume（交易赛推荐）</option>
          </select>
        </div>
        <div class="field optimize-only">
          <label>最小成交数</label>
          <input id="min_trade_count" type="number" step="1" value="0" />
        </div>
        <div class="field optimize-only">
          <label>最小平均资金占用(0-1)</label>
          <input id="min_avg_capital_usage" type="number" step="0.01" value="0" />
        </div>
        <div class="field optimize-only">
          <label>Top候选数量</label>
          <input id="top_k" type="number" step="1" value="5" />
        </div>

        <div class="actions">
          <button id="run_btn" type="submit">开始测算</button>
          <button id="suggest_btn" type="button" class="optimize-only">智能建议 min/max</button>
          <button id="short_candidate_btn" type="button" class="market-futures-only">空头刷量候选</button>
          <button id="csv_btn" type="button" disabled>下载当前买入计划 CSV</button>
          <span id="status" class="msg">等待输入参数。</span>
        </div>
        <div class="actions optimize-only">
          <span id="suggest_status" class="msg">可先点击“智能建议 min/max”，再进行正式测算。</span>
        </div>
        <div id="progress_box" class="progress-box">
          <div class="progress-track">
            <div id="progress_fill" class="progress-fill"></div>
          </div>
          <div id="progress_text" class="progress-text">进度 0.0% · ETA --</div>
        </div>
      </form>
      <p class="hint">提示：合约模式默认采用“启动预建仓”；现货 V1 不自动预建底仓，只会先挂当前价下方买单，买到后再补对应卖单。K线和资金费率按 交易对(+周期) 缓存到本地，选择不同时间区间会复用缓存并按需增量拉取；秒级K线(如1s)单次区间最多31天。交易赛场景优先使用 `competition_volume` 与“智能建议 min/max”。</p>
    </section>

    <section class="card optimize-only">
      <h3>智能区间建议（交易赛）</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>建议 min_price</th>
              <th>建议 max_price</th>
              <th>推荐 N</th>
              <th>推荐模式</th>
              <th>单格价差</th>
              <th>每格买入额(均/最小/最大)</th>
              <th>成交额</th>
              <th>目标达成率</th>
              <th>净收益</th>
              <th>最大回撤</th>
              <th>成交数</th>
              <th>说明</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody id="suggest_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h3>币安式网格预览器（当前盘面）</h3>
      <p class="hint">使用上方“市场类型 / 交易对”按当前盘口预览网格。合约模式支持 做多 / 做空 / 中性；现货 V1 仅支持单向做多，且不自动补历史底仓。默认按每格等额（固定名义金额）分配，因此价格越低数量越大、价格越高数量越小；合约爆仓价按 5% 维持保证金率做近似估算。</p>
      <div class="grid">
        <div class="field market-futures-only">
          <label>预览方向</label>
          <select id="preview_strategy_direction">
            <option value="long">做多（long）</option>
            <option value="short">做空（short）</option>
            <option value="neutral" selected>中性（neutral）</option>
          </select>
        </div>
        <div class="field">
          <label>价格网格</label>
          <select id="preview_grid_level_mode">
            <option value="arithmetic" selected>等差（固定价差）</option>
            <option value="geometric">等比（固定百分比）</option>
          </select>
        </div>
        <div class="field">
          <label>网格数 N</label>
          <input id="preview_n" type="number" step="1" value="20" min="1" />
        </div>
        <div class="field">
          <label>最低价</label>
          <input id="preview_min_price" type="number" step="0.0001" value="0.2000" />
        </div>
        <div class="field">
          <label>最高价</label>
          <input id="preview_max_price" type="number" step="0.0001" value="0.3000" />
        </div>
        <div class="field">
          <label>最大投入金额（保证金 / quote预算）</label>
          <input id="preview_margin_amount" type="number" step="0.0001" value="500" />
        </div>
        <div class="field market-futures-only">
          <label>杠杆</label>
          <input id="preview_leverage" type="number" step="0.1" value="2" min="0.1" />
        </div>
        <div class="actions">
          <button id="preview_btn" type="button">生成当前网格预览</button>
          <span id="preview_status" class="msg">等待生成。</span>
        </div>
      </div>
      <div id="preview_summary" class="summary"></div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>格子</th>
              <th>方向</th>
              <th>区间下限</th>
              <th>区间上限</th>
              <th>开仓方向</th>
              <th>开仓价</th>
              <th>平仓方向</th>
              <th>平仓价</th>
              <th>每格名义(结算币/≈U)</th>
              <th>数量</th>
              <th>启动状态</th>
              <th>当前活动委托</th>
            </tr>
          </thead>
          <tbody id="preview_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card legacy-ranking market-futures-only">
      <h3>全市场排行（波动率 / 总资金费率）</h3>
      <div class="rank-grid">
        <div class="field">
          <label>排行起始时间（本地时区）</label>
          <input id="rank_start_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>排行结束时间（本地时区）</label>
          <input id="rank_end_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>K线周期（波动率）</label>
          <select id="rank_interval">
            <option value="1h" selected>1h</option>
            <option value="1s">1s</option>
            <option value="1m">1m</option>
          </select>
        </div>
        <div class="field">
          <label>Top数量</label>
          <input id="rank_top_k" type="number" step="1" value="30" min="5" max="200" />
        </div>
        <div class="field">
          <label>合约数量上限(0=全部)</label>
          <input id="rank_max_symbols" type="number" step="1" value="0" min="0" max="2000" />
        </div>
        <div class="field">
          <label>数据源</label>
          <select id="rank_cache_only">
            <option value="1" selected>仅本地缓存（快，推荐）</option>
            <option value="0">允许增量拉取（慢）</option>
          </select>
        </div>
        <div class="field">
          <label>刷新间隔（秒）</label>
          <input id="rank_refresh_seconds" type="number" step="1" value="60" min="60" max="3600" />
        </div>
        <div class="rank-actions">
          <button id="rank_run_btn" type="button">刷新排行</button>
          <button id="rank_auto_btn" type="button">开启自动刷新</button>
          <span id="rank_status" class="msg">等待查询。</span>
        </div>
      </div>
      <div class="rank-two-cols">
        <div>
          <h4 class="rank-subtitle">波动率排行（年化）</h4>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>年化波动率</th>
                  <th>区间涨跌</th>
                  <th>K线数</th>
                </tr>
              </thead>
              <tbody id="vol_rank_tbody"></tbody>
            </table>
          </div>
        </div>
        <div>
          <h4 class="rank-subtitle">总资金费率排行</h4>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>总资金费率</th>
                  <th>多头资金费率</th>
                  <th title="统计区间内资金费结算记录数（通常8小时一次）">事件数</th>
                </tr>
              </thead>
              <tbody id="fund_rank_tbody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section class="card market-futures-only">
      <h3>空头刷量候选</h3>
      <p class="hint">基于当前交易对、时间窗、预算、手续费和资金费设定，自动生成 3 个纯空候选。点击“应用”会把候选参数回填到上方主表单，你可以继续修改后再点“开始测算”。</p>
      <div id="short_candidate_summary" class="summary"></div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>候选</th>
              <th>说明</th>
              <th>区间</th>
              <th>N / 模式</th>
              <th>成交额倍数</th>
              <th>净收益</th>
              <th>收益率</th>
              <th>最大回撤</th>
              <th>成交数</th>
              <th>资金费</th>
              <th>手续费</th>
              <th>平均占用</th>
              <th>推荐目标</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody id="short_candidate_tbody"></tbody>
        </table>
      </div>
      <p id="short_candidate_status" class="hint">等待生成。</p>
    </section>

    <section class="card">
      <h3>当前方案（可在Top候选点击切换）</h3>
      <div id="summary" class="summary"></div>
      <p id="pnl_formula" class="formula"></p>
    </section>

    <section class="card">
      <h3>Top 候选（点击行切换到该方案）</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>N</th>
              <th>Mode</th>
              <th>Score</th>
              <th>净收益</th>
              <th>总收益率</th>
              <th>年化</th>
              <th>最大回撤</th>
              <th>成交数</th>
              <th>成交额</th>
              <th>目标达成率</th>
              <th>手续费</th>
              <th id="top_funding_header">资金费收益</th>
            </tr>
          </thead>
          <tbody id="top_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h3>每格买入计划</h3>
      <div id="plan_overview" class="plan-overview">
        <div class="plan-pill"><div class="k">总持仓量（满格）</div><div class="v">-</div></div>
        <div class="plan-pill"><div class="k">按终止价估算满格名义</div><div class="v">-</div></div>
        <div class="plan-pill"><div class="k">期末实际持仓量</div><div class="v">-</div></div>
        <div class="plan-pill"><div class="k">期末实际持仓名义</div><div class="v">-</div></div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>格子</th>
              <th>方向</th>
              <th>买入价</th>
              <th>卖出价</th>
              <th>买入名义(结算币/≈U)</th>
              <th>买入数量</th>
            </tr>
          </thead>
          <tbody id="plan_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card market-futures-only">
      <h3>资金费明细（当前方案）</h3>
      <div class="funding-actions">
        <button id="funding_csv_btn" type="button" disabled>下载资金费明细 CSV</button>
        <span id="funding_status" class="msg">等待测算结果。</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>费率</th>
              <th>参照价格</th>
              <th>初始资产(主口径/≈对照)</th>
              <th>持仓名义(结算币/≈U)</th>
              <th>当时净收益(结算币/≈U)</th>
              <th>净收益/初始资产(同口径)</th>
              <th>账户权益(结算币/≈U)</th>
              <th>权益/初始资产(同口径)</th>
              <th>最低保证金(50%)</th>
              <th>估算爆仓价</th>
              <th>可提取金额(结算币/≈U)</th>
              <th>本次资金费(结算币/≈U)</th>
              <th>累计资金费(结算币/≈U)</th>
            </tr>
          </thead>
          <tbody id="funding_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h3>一键分层参数生成器（多机器人近似线性加仓）</h3>
      <div class="layer-grid">
        <div class="field">
          <label>分层数量</label>
          <input id="layer_count" type="number" step="1" value="5" min="2" max="20" />
        </div>
        <div class="field">
          <label>N建议模式</label>
          <select id="layer_n_mode">
            <option value="fit_selected_plan">拟合当前方案（最接近最佳策略）</option>
            <option value="fit_nested_cover_qty">递减覆盖（币安数量模式）</option>
            <option value="budget_equal_step">总N对齐（按区间宽度，推荐）</option>
            <option value="budget_by_weight">总N对齐（按资金权重）</option>
            <option value="target_step">独立步长模式（每层单独算N）</option>
          </select>
        </div>
        <div class="field layer-manual-field">
          <label>低价层加权方式</label>
          <select id="layer_weight_mode">
            <option value="linear">linear（默认）</option>
            <option value="quadratic">quadratic（更激进）</option>
            <option value="geometric">geometric（更陡峭）</option>
            <option value="equal">equal（均匀）</option>
          </select>
        </div>
        <div class="field layer-budget-field">
          <label>总格子预算（所有层合计）</label>
          <input id="layer_total_n_budget" type="number" step="1" value="20" min="2" max="500" />
        </div>
        <div class="field layer-target-step-field">
          <label>目标单格价差(%)</label>
          <input id="layer_target_step_pct" type="number" step="0.1" value="0.5" min="0.1" />
        </div>
        <div class="field layer-target-step-field">
          <label>每层建议N上限</label>
          <input id="layer_n_cap" type="number" step="1" value="80" min="5" max="300" />
        </div>
        <div class="layer-actions">
          <button id="layer_btn" type="button">一键生成分层参数</button>
          <button id="layer_compare_btn" type="button" disabled>分层组合回测对比</button>
          <button id="layer_csv_btn" type="button" disabled>下载分层参数 CSV</button>
          <span id="layer_status" class="msg">使用当前输入参数点击生成。</span>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>层级(低->高)</th>
              <th>覆盖格子</th>
              <th>区间下限</th>
              <th>区间上限</th>
              <th>资金占比</th>
              <th>层总投入</th>
              <th>建议N</th>
              <th>单格价差(%)</th>
              <th>建议每格买入额</th>
              <th>建议每格买入数量</th>
              <th>建议</th>
            </tr>
          </thead>
          <tbody id="layer_tbody"></tbody>
        </table>
      </div>
      <p class="hint">说明：若交易所按“每格固定数量”下单，优先用“递减覆盖（币安数量模式）”；第一层覆盖全部格子，后续层逐步下调最高价以增强低价区买入。</p>
    </section>

    <section class="card">
      <h3>分层组合回测 vs 当前方案</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>指标</th>
              <th>当前方案</th>
              <th>分层组合</th>
              <th>差值(分层-当前)</th>
            </tr>
          </thead>
          <tbody id="compare_tbody"></tbody>
        </table>
      </div>
      <p id="compare_status" class="hint">先生成分层参数，再点击“分层组合回测对比”。</p>
    </section>
  </div>

  <script>
    const form = document.getElementById("form");
    const statusEl = document.getElementById("status");
    const runBtn = document.getElementById("run_btn");
    const suggestBtn = document.getElementById("suggest_btn");
    const shortCandidateBtn = document.getElementById("short_candidate_btn");
    const csvBtn = document.getElementById("csv_btn");
    const suggestStatusEl = document.getElementById("suggest_status");
    const shortCandidateStatusEl = document.getElementById("short_candidate_status");
    const shortCandidateSummaryEl = document.getElementById("short_candidate_summary");
    const shortCandidateBody = document.getElementById("short_candidate_tbody");
    const rankRunBtn = document.getElementById("rank_run_btn");
    const rankAutoBtn = document.getElementById("rank_auto_btn");
    const rankStatusEl = document.getElementById("rank_status");
    const rankStartTimeEl = document.getElementById("rank_start_time");
    const rankEndTimeEl = document.getElementById("rank_end_time");
    const rankIntervalEl = document.getElementById("rank_interval");
    const rankTopKEl = document.getElementById("rank_top_k");
    const rankMaxSymbolsEl = document.getElementById("rank_max_symbols");
    const rankCacheOnlyEl = document.getElementById("rank_cache_only");
    const rankRefreshSecondsEl = document.getElementById("rank_refresh_seconds");
    const volRankBody = document.getElementById("vol_rank_tbody");
    const fundRankBody = document.getElementById("fund_rank_tbody");
    const previewBtn = document.getElementById("preview_btn");
    const previewStatusEl = document.getElementById("preview_status");
    const previewSummaryEl = document.getElementById("preview_summary");
    const previewBody = document.getElementById("preview_tbody");
    const previewStrategyDirectionEl = document.getElementById("preview_strategy_direction");
    const previewGridLevelModeEl = document.getElementById("preview_grid_level_mode");
    const previewNEl = document.getElementById("preview_n");
    const previewMinPriceEl = document.getElementById("preview_min_price");
    const previewMaxPriceEl = document.getElementById("preview_max_price");
    const previewMarginAmountEl = document.getElementById("preview_margin_amount");
    const previewLeverageEl = document.getElementById("preview_leverage");
    const summaryEl = document.getElementById("summary");
    const formulaEl = document.getElementById("pnl_formula");
    const suggestBody = document.getElementById("suggest_tbody");
    const topBody = document.getElementById("top_tbody");
    const planBody = document.getElementById("plan_tbody");
    const planOverviewEl = document.getElementById("plan_overview");
    const fundingBody = document.getElementById("funding_tbody");
    const fundingStatusEl = document.getElementById("funding_status");
    const fundingCsvBtn = document.getElementById("funding_csv_btn");
    const layerBody = document.getElementById("layer_tbody");
    const layerBtn = document.getElementById("layer_btn");
    const layerCompareBtn = document.getElementById("layer_compare_btn");
    const layerCsvBtn = document.getElementById("layer_csv_btn");
    const layerStatusEl = document.getElementById("layer_status");
    const compareBody = document.getElementById("compare_tbody");
    const compareStatusEl = document.getElementById("compare_status");
    const layerCountEl = document.getElementById("layer_count");
    const layerNModeEl = document.getElementById("layer_n_mode");
    const layerWeightModeEl = document.getElementById("layer_weight_mode");
    const layerTotalNEl = document.getElementById("layer_total_n_budget");
    const layerTargetStepPctEl = document.getElementById("layer_target_step_pct");
    const layerNCapEl = document.getElementById("layer_n_cap");
    const startTimeEl = document.getElementById("start_time");
    const endTimeEl = document.getElementById("end_time");
    const symbolEl = document.getElementById("symbol");
    const layerManualFields = Array.from(document.querySelectorAll(".layer-manual-field"));
    const layerBudgetFields = Array.from(document.querySelectorAll(".layer-budget-field"));
    const layerTargetStepFields = Array.from(document.querySelectorAll(".layer-target-step-field"));
    const calcModeEl = document.getElementById("calc_mode");
    const marketTypeEl = document.getElementById("market_type");
    const fixedBuyUnitEl = document.getElementById("fixed_buy_unit");
    const fixedBuyValueLabelEl = document.getElementById("fixed_buy_value_label");
    const fixedBuyValueEl = document.getElementById("fixed_buy_value");
    const strategyDirectionEl = document.getElementById("strategy_direction");
    const gridLevelModeEl = document.getElementById("grid_level_mode");
    const includeFundingEl = document.getElementById("include_funding");
    const intervalEl = document.getElementById("interval");
    const progressBoxEl = document.getElementById("progress_box");
    const progressFillEl = document.getElementById("progress_fill");
    const progressTextEl = document.getElementById("progress_text");
    const optimizeOnlyFields = Array.from(document.querySelectorAll(".optimize-only"));
    const fixedOnlyFields = Array.from(document.querySelectorAll(".fixed-only"));
    const marketFuturesOnlyFields = Array.from(document.querySelectorAll(".market-futures-only"));
    const contractTypeEl = document.getElementById("contract_type");
    const topFundingHeaderEl = document.getElementById("top_funding_header");

    let latestPlanRows = [];
    let latestTopCandidates = [];
    let latestRangeSuggestions = [];
    let latestShortCandidates = [];
    let latestShortCandidateContext = null;
    let latestCandleCount = 0;
    let latestExpectedCandleCount = 0;
    let latestCandleCoverage = null;
    let latestResultMarketType = "futures";
    let latestResultContractType = "usdm";
    let latestResultSymbol = "BTCUSDT";
    let latestLayerRows = [];
    let latestComparison = null;
    let latestFundingRows = [];
    let selectedTopIndex = 0;
    let selectedShortCandidateIndex = -1;
    let currentPollToken = 0;
    let currentShortCandidatePollToken = 0;
    let rankAutoTimer = null;
    let lastFuturesContractType = "usdm";
    let lastFuturesStrategyDirection = "long";
    let lastFuturesPreviewStrategyDirection = "neutral";
    let lastFuturesIncludeFunding = "1";
    let lastFuturesPreviewLeverage = "2";
    const SECOND_INTERVAL_MAX_SPAN_MS = 31 * 24 * 3600 * 1000;

    function parseIntervalMs(interval) {
      const text = String(interval || "").trim();
      const m = text.match(/^(\\d+)([smhdw])$/);
      if (!m) return null;
      const value = Number(m[1]);
      const unit = m[2];
      const factor = { s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }[unit];
      if (!Number.isFinite(value) || value <= 0 || !factor) return null;
      return value * factor;
    }

    function validateSecondIntervalRange(startIso, endIso, interval, labelPrefix = "当前") {
      if (!startIso || !endIso) return null;
      const stepMs = parseIntervalMs(interval);
      if (!Number.isFinite(stepMs)) return "K线周期格式不正确";
      if (stepMs >= 60000) return null;
      const startMs = Date.parse(startIso);
      const endMs = Date.parse(endIso);
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return "起止时间格式不正确";
      if (endMs - startMs > SECOND_INTERVAL_MAX_SPAN_MS) {
        return `${labelPrefix}秒级K线区间不能超过31天`;
      }
      return null;
    }

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return Number(v).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return `${(Number(v) * 100).toFixed(2)}%`;
    }

    function fmtDateTime(v) {
      if (!v) return "-";
      const d = new Date(v);
      if (Number.isNaN(d.getTime())) return String(v);
      return d.toLocaleString();
    }

    function inferBaseAsset(symbol) {
      const raw = String(symbol || "").trim().toUpperCase();
      if (!raw) return "BASE";
      let m = raw.match(/^([A-Z0-9]+)USD(?:_PERP)?$/);
      if (m) return m[1];
      m = raw.match(/^([A-Z0-9]+)(USDT|USDC|BUSD)$/);
      if (m) return m[1];
      return raw;
    }

    function getSelectedMarketType() {
      const marketType = String((marketTypeEl && marketTypeEl.value) || "futures").trim().toLowerCase();
      return marketType === "spot" ? "spot" : "futures";
    }

    function inferQuoteAsset(symbol, marketType = "futures", contractType = "usdm") {
      const raw = String(symbol || "").trim().toUpperCase();
      if (!raw) return marketType === "spot" ? "QUOTE" : "U";
      if (marketType === "futures" && String(contractType || "").trim().toLowerCase() === "coinm") {
        return "USD";
      }
      const quoteCandidates = [
        "USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP", "DAI",
        "BTC", "ETH", "BNB", "TRY", "EUR", "BRL", "AUD", "GBP",
      ];
      for (const quote of quoteCandidates) {
        if (raw.endsWith(quote) && raw.length > quote.length) {
          return quote;
        }
      }
      return marketType === "spot" ? "QUOTE" : "U";
    }

    function currentResultContext() {
      const marketType = String(latestResultMarketType || getSelectedMarketType()).trim().toLowerCase();
      return {
        marketType,
        contractType: marketType === "futures"
          ? String(latestResultContractType || contractTypeEl.value || "usdm").trim().toLowerCase()
          : "",
        symbol: String(latestResultSymbol || symbolEl.value || "").trim().toUpperCase(),
      };
    }

    function toSettleAmount(valueQuote, price) {
      const n = Number(valueQuote);
      const p = Number(price);
      if (!Number.isFinite(n) || !Number.isFinite(p) || p <= 0) return null;
      return n / p;
    }

    function fmtMoneyForContext(valueQuote, price, context, digitsSettle = 6, digitsQuote = 4) {
      if (valueQuote === null || valueQuote === undefined || Number.isNaN(valueQuote)) return "-";
      const quoteAsset = inferQuoteAsset(context.symbol, context.marketType, context.contractType);
      const quoteText = `${fmtNum(valueQuote, digitsQuote)} ${quoteAsset}`;
      if (context.contractType !== "coinm") return quoteText;
      const settle = toSettleAmount(valueQuote, price);
      if (settle === null) return quoteText;
      const settleAsset = inferBaseAsset(context.symbol);
      return `${fmtNum(settle, digitsSettle)} ${settleAsset}<span class="value-sub">≈${quoteText}</span>`;
    }

    function fmtMoney(valueQuote, price, digitsSettle = 6, digitsQuote = 4) {
      return fmtMoneyForContext(valueQuote, price, currentResultContext(), digitsSettle, digitsQuote);
    }

    function initialAssetValues(row) {
      const mode = String(row && row.initial_asset_mode || "").trim().toLowerCase();
      const quote = Number(row && row.initial_asset_quote);
      const startPrice = Number(row && row.initial_asset_start_price);
      if (mode === "quote") {
        const settleApprox = Number(row && row.initial_asset_qty_approx);
        return {
          mode: "quote",
          quote: Number.isFinite(quote) ? quote : null,
          settle: Number.isFinite(settleApprox)
            ? settleApprox
            : toSettleAmount(quote, startPrice),
        };
      }
      const settle = Number(row && row.initial_asset_qty);
      return {
        mode: "settle",
        quote: Number.isFinite(quote) ? quote : null,
        settle: Number.isFinite(settle) ? settle : null,
      };
    }

    function fmtInitialAsset(row) {
      const vals = initialAssetValues(row);
      const context = currentResultContext();
      const settleAsset = inferBaseAsset(context.symbol);
      if (vals.mode === "quote") {
        if (!Number.isFinite(vals.quote)) return "-";
        const quoteText = `${fmtNum(vals.quote)} ${inferQuoteAsset(context.symbol, context.marketType, context.contractType)}`;
        if (context.contractType === "coinm" && Number.isFinite(vals.settle)) {
          return `${quoteText}<span class="value-sub">≈${fmtNum(vals.settle, 6)} ${settleAsset}</span>`;
        }
        return quoteText;
      }
      if (!Number.isFinite(vals.settle)) return "-";
      if (context.contractType === "coinm") {
        const quoteSub = Number.isFinite(vals.quote)
          ? `<span class="value-sub">≈${fmtNum(vals.quote)} ${inferQuoteAsset(context.symbol, context.marketType, context.contractType)}</span>`
          : "";
        return `${fmtNum(vals.settle, 6)} ${settleAsset}${quoteSub}`;
      }
      if (Number.isFinite(vals.quote)) {
        return `${fmtNum(vals.quote)} ${inferQuoteAsset(context.symbol, context.marketType, context.contractType)}`;
      }
      return fmtNum(vals.settle, 6);
    }

    function toDateTimeLocalValue(d) {
      if (!(d instanceof Date) || Number.isNaN(d.getTime())) return "";
      const pad = (x) => String(x).padStart(2, "0");
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function initializeDefaultTimeRange() {
      const now = new Date();
      const oneYearAgo = new Date(now.getTime() - 365 * 24 * 3600 * 1000);
      if (!startTimeEl.value) startTimeEl.value = toDateTimeLocalValue(oneYearAgo);
      if (!endTimeEl.value) endTimeEl.value = toDateTimeLocalValue(now);
      if (!rankStartTimeEl.value) rankStartTimeEl.value = toDateTimeLocalValue(oneYearAgo);
      if (!rankEndTimeEl.value) rankEndTimeEl.value = toDateTimeLocalValue(now);
    }

    async function loadSymbols(refresh = false) {
      const marketType = getSelectedMarketType();
      const contractType = (contractTypeEl.value || "usdm").trim();
      try {
        symbolEl.innerHTML = '<option value="">加载交易对...</option>';
        const qs = new URLSearchParams();
        qs.set("market_type", marketType);
        if (marketType === "futures") {
          qs.set("contract_type", contractType);
        }
        if (refresh) qs.set("refresh", "1");
        const resp = await fetch(`/api/symbols?${qs.toString()}`);
        const data = await resp.json();
        if (!resp.ok || !data.ok || !Array.isArray(data.symbols)) {
          throw new Error(data.error || `获取交易对失败(${resp.status})`);
        }
        const prev = symbolEl.value;
        symbolEl.innerHTML = data.symbols
          .map((s) => `<option value="${s}">${s}</option>`)
          .join("");
        const defaultSymbol = marketType === "spot"
          ? "BTCUSDT"
          : (contractType === "coinm" ? "BTCUSD_PERP" : "BTCUSDT");
        if (prev && data.symbols.includes(prev)) {
          symbolEl.value = prev;
        } else if (data.symbols.includes(defaultSymbol)) {
          symbolEl.value = defaultSymbol;
        } else if (data.symbols.length > 0) {
          symbolEl.value = data.symbols[0];
        }
        if (data.warning) {
          setStatus(`交易对列表使用本地兜底：${data.warning}`);
        }
      } catch (err) {
        if (marketType === "futures" && contractType === "coinm") {
          symbolEl.innerHTML = '<option value="BTCUSD_PERP">BTCUSD_PERP</option><option value="ETHUSD_PERP">ETHUSD_PERP</option>';
        } else {
          symbolEl.innerHTML = '<option value="BTCUSDT">BTCUSDT</option><option value="ETHUSDT">ETHUSDT</option>';
        }
        setStatus(`交易对列表加载失败，已使用默认列表：${err.message}`, true);
      }
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "msg error" : "msg";
    }

    function setSuggestStatus(text, isError = false) {
      suggestStatusEl.textContent = text;
      suggestStatusEl.className = isError ? "msg error" : "msg";
    }

    function setShortCandidateStatus(text, isError = false) {
      shortCandidateStatusEl.textContent = text;
      shortCandidateStatusEl.className = isError ? "hint msg error" : "hint";
    }

    function setLayerStatus(text, isError = false) {
      layerStatusEl.textContent = text;
      layerStatusEl.className = isError ? "msg error" : "msg";
    }

    function setCompareStatus(text, isError = false) {
      compareStatusEl.textContent = text;
      compareStatusEl.className = isError ? "hint msg error" : "hint";
    }

    function setFundingStatus(text, isError = false) {
      fundingStatusEl.textContent = text;
      fundingStatusEl.className = isError ? "msg error" : "msg";
    }

    function setRankStatus(text, isError = false) {
      rankStatusEl.textContent = text;
      rankStatusEl.className = isError ? "msg error" : "msg";
    }

    function setPreviewStatus(text, isError = false) {
      previewStatusEl.textContent = text;
      previewStatusEl.className = isError ? "msg error" : "msg";
    }

    function clearGridPreview(message = "等待生成。") {
      previewSummaryEl.innerHTML = "";
      previewBody.innerHTML = "";
      setPreviewStatus(message, false);
    }

    function clearShortCandidates(message = "等待生成。") {
      latestShortCandidates = [];
      latestShortCandidateContext = null;
      selectedShortCandidateIndex = -1;
      shortCandidateSummaryEl.innerHTML = "";
      shortCandidateBody.innerHTML = "";
      setShortCandidateStatus(message, false);
    }

    function setAllocationModeSelection(modes) {
      const normalized = new Set((Array.isArray(modes) ? modes : []).map((item) => String(item || "").trim().toLowerCase()));
      for (const checkbox of document.querySelectorAll('input[name="allocation_mode"]')) {
        checkbox.checked = normalized.has(String(checkbox.value || "").trim().toLowerCase());
      }
    }

    function capSecondIntervalRange(startInput, endInput, intervalValue) {
      const stepMs = parseIntervalMs(intervalValue);
      if (!Number.isFinite(stepMs) || stepMs >= 60000) {
        endInput.removeAttribute("max");
        return false;
      }
      const start = new Date(startInput.value);
      if (Number.isNaN(start.getTime())) {
        endInput.removeAttribute("max");
        return false;
      }
      const maxEnd = new Date(start.getTime() + SECOND_INTERVAL_MAX_SPAN_MS);
      const maxEndValue = toDateTimeLocalValue(maxEnd);
      endInput.max = maxEndValue;
      const end = new Date(endInput.value);
      if (Number.isNaN(end.getTime())) return false;
      if (end.getTime() <= maxEnd.getTime()) return false;
      endInput.value = maxEndValue;
      return true;
    }

    function applyMainSecondIntervalLimit(notify = false) {
      if (!startTimeEl.value || !endTimeEl.value) return;
      const changed = capSecondIntervalRange(startTimeEl, endTimeEl, intervalEl.value.trim());
      if (changed && notify) {
        setStatus("已自动限制为31天：1s秒级K线区间最多31天。");
      }
    }

    function applyRankSecondIntervalLimit(notify = false) {
      if (!rankStartTimeEl.value || !rankEndTimeEl.value) return;
      const changed = capSecondIntervalRange(
        rankStartTimeEl,
        rankEndTimeEl,
        rankIntervalEl.value.trim()
      );
      if (changed && notify) {
        setRankStatus("已自动限制为31天：1s秒级K线区间最多31天。");
      }
    }

    function signedNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      const n = Number(v);
      if (Math.abs(n) < 1e-12) return "0";
      const sign = n > 0 ? "+" : "";
      return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits })}`;
    }

    function signedPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      const n = Number(v);
      if (Math.abs(n) < 1e-12) return "0.00%";
      const sign = n > 0 ? "+" : "";
      return `${sign}${(n * 100).toFixed(2)}%`;
    }

    function deltaClass(delta, preferLower = false) {
      if (delta === null || delta === undefined || Number.isNaN(delta)) return "delta-neutral";
      if (Math.abs(Number(delta)) < 1e-12) return "delta-neutral";
      const better = preferLower ? Number(delta) < 0 : Number(delta) > 0;
      return better ? "delta-better" : "delta-worse";
    }

    function clearComparison(message = "先生成分层参数，再点击“分层组合回测对比”。") {
      latestComparison = null;
      compareBody.innerHTML = "";
      setCompareStatus(message, false);
    }

    function formatEta(seconds) {
      if (seconds === null || seconds === undefined) return "--";
      const s = Number(seconds);
      if (Number.isNaN(s) || s < 0) return "--";
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = Math.floor(s % 60);
      if (h > 0) return `${h}h ${m}m ${sec}s`;
      if (m > 0) return `${m}m ${sec}s`;
      return `${sec}s`;
    }

    function setProgress(progress, etaSeconds, message = "") {
      const pct = Math.max(0, Math.min(100, Number(progress || 0) * 100));
      progressFillEl.style.width = `${pct.toFixed(2)}%`;
      const etaText = formatEta(etaSeconds);
      progressTextEl.textContent = `进度 ${pct.toFixed(1)}% · ETA ${etaText}${message ? ` · ${message}` : ""}`;
    }

    function setGroupVisible(nodes, visible) {
      for (const node of nodes) {
        node.classList.toggle("is-hidden", !visible);
        for (const input of node.querySelectorAll("input,select")) {
          input.disabled = !visible;
        }
      }
    }

    function applyCalcModeUI() {
      const mode = calcModeEl.value;
      const isOptimize = mode === "optimize";
      setGroupVisible(optimizeOnlyFields, isOptimize);
      setGroupVisible(fixedOnlyFields, !isOptimize);
    }

    function applyMarketTypeUI() {
      const marketType = getSelectedMarketType();
      const isSpot = marketType === "spot";
      if (!isSpot) {
        contractTypeEl.value = lastFuturesContractType || "usdm";
        strategyDirectionEl.value = lastFuturesStrategyDirection || "long";
        previewStrategyDirectionEl.value = lastFuturesPreviewStrategyDirection || "neutral";
        includeFundingEl.value = lastFuturesIncludeFunding || "1";
        previewLeverageEl.value = lastFuturesPreviewLeverage || "2";
      } else {
        lastFuturesContractType = String(contractTypeEl.value || "usdm");
        lastFuturesStrategyDirection = String(strategyDirectionEl.value || "long");
        lastFuturesPreviewStrategyDirection = String(previewStrategyDirectionEl.value || "neutral");
        lastFuturesIncludeFunding = String(includeFundingEl.value || "1");
        lastFuturesPreviewLeverage = String(previewLeverageEl.value || "2");
        strategyDirectionEl.value = "long";
        previewStrategyDirectionEl.value = "long";
        includeFundingEl.value = "0";
        previewLeverageEl.value = "1";
      }
      setGroupVisible(marketFuturesOnlyFields, !isSpot);
      if (topFundingHeaderEl) {
        topFundingHeaderEl.classList.toggle("is-hidden", isSpot);
      }
    }

    function applyFixedBuyUnitUI() {
      const unit = fixedBuyUnitEl.value;
      if (unit === "qty") {
        fixedBuyValueLabelEl.textContent = "每格买入币种份额";
        fixedBuyValueEl.step = "0.000001";
      } else {
        fixedBuyValueLabelEl.textContent = "每格买入金额";
        fixedBuyValueEl.step = "0.0001";
      }
    }

    function readForm() {
      const checkedModes = Array.from(
        document.querySelectorAll('input[name="allocation_mode"]:checked')
      ).map((x) => x.value);
      const startIso = startTimeEl.value ? new Date(startTimeEl.value).toISOString() : "";
      const endIso = endTimeEl.value ? new Date(endTimeEl.value).toISOString() : "";
      const fixedBuyUnit = fixedBuyUnitEl.value.trim();
      const fixedBuyValue = Number(fixedBuyValueEl.value);
      const marketType = getSelectedMarketType();
      return {
        market_type: marketType,
        calc_mode: document.getElementById("calc_mode").value.trim(),
        contract_type: marketType === "futures" ? contractTypeEl.value.trim() : "",
        symbol: document.getElementById("symbol").value.trim(),
        min_price: Number(document.getElementById("min_price").value),
        max_price: Number(document.getElementById("max_price").value),
        total_buy_notional: Number(document.getElementById("total_buy_notional").value),
        max_buy_notional: Number(document.getElementById("total_buy_notional").value),
        target_trade_volume: Number(document.getElementById("target_trade_volume").value),
        fixed_n: Number(document.getElementById("fixed_n").value),
        fixed_buy_unit: fixedBuyUnit,
        fixed_per_grid_notional: fixedBuyUnit === "notional" ? fixedBuyValue : 0,
        fixed_per_grid_qty: fixedBuyUnit === "qty" ? fixedBuyValue : 0,
        strategy_direction: marketType === "spot" ? "long" : strategyDirectionEl.value.trim(),
        grid_level_mode: gridLevelModeEl.value.trim(),
        include_funding: marketType === "futures" && includeFundingEl.value === "1",
        start_time: startIso,
        end_time: endIso,
        interval: document.getElementById("interval").value.trim(),
        n_min: Number(document.getElementById("n_min").value),
        n_max: Number(document.getElementById("n_max").value),
        fee_rate: Number(document.getElementById("fee_rate").value),
        slippage: Number(document.getElementById("slippage").value),
        allocation_modes: checkedModes,
        objective: document.getElementById("objective").value.trim(),
        min_trade_count: Number(document.getElementById("min_trade_count").value),
        min_avg_capital_usage: Number(document.getElementById("min_avg_capital_usage").value),
        top_k: Number(document.getElementById("top_k").value)
      };
    }

    function readRankForm() {
      const startIso = rankStartTimeEl.value ? new Date(rankStartTimeEl.value).toISOString() : "";
      const endIso = rankEndTimeEl.value ? new Date(rankEndTimeEl.value).toISOString() : "";
      return {
        market_type: getSelectedMarketType(),
        contract_type: contractTypeEl.value.trim(),
        start_time: startIso,
        end_time: endIso,
        interval: rankIntervalEl.value.trim(),
        top_k: Number(rankTopKEl.value),
        max_symbols: Number(rankMaxSymbolsEl.value),
        cache_only: rankCacheOnlyEl.value === "1",
      };
    }

    function readGridPreviewForm() {
      const marketType = getSelectedMarketType();
      return {
        market_type: marketType,
        contract_type: marketType === "futures" ? contractTypeEl.value.trim() : "",
        symbol: symbolEl.value.trim(),
        strategy_direction: marketType === "spot" ? "long" : previewStrategyDirectionEl.value.trim(),
        grid_level_mode: previewGridLevelModeEl.value.trim(),
        n: Number(previewNEl.value),
        min_price: Number(previewMinPriceEl.value),
        max_price: Number(previewMaxPriceEl.value),
        margin_amount: Number(previewMarginAmountEl.value),
        leverage: marketType === "spot" ? 1 : Number(previewLeverageEl.value),
      };
    }

    function formatStartupInventory(summary, quantityDigits = 4, notionalDigits = 4) {
      const direction = String(summary.strategy_direction || "");
      const longQty = Number(summary.startup_long_qty || 0);
      const shortQty = Number(summary.startup_short_qty || 0);
      const netQty = Number(summary.startup_net_qty || 0);
      const longNotional = Number(summary.startup_long_notional || 0);
      const shortNotional = Number(summary.startup_short_notional || 0);
      const netNotional = Number(summary.startup_net_notional || 0);
      if (direction === "long") {
        return `多仓 ${fmtNum(longQty, quantityDigits)} / ${fmtNum(longNotional, notionalDigits)}U`;
      }
      if (direction === "short") {
        return `空仓 ${fmtNum(shortQty, quantityDigits)} / ${fmtNum(shortNotional, notionalDigits)}U`;
      }
      return `多 ${fmtNum(longQty, quantityDigits)} / 空 ${fmtNum(shortQty, quantityDigits)} / 净 ${signedNum(netQty, quantityDigits)} · ${signedNum(netNotional, notionalDigits)}U`;
    }

    function renderGridPreview(payload) {
      const summary = payload && payload.summary ? payload.summary : {};
      const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
      const symbolInfo = summary.symbol_info || {};
      const marketType = String(summary.market_type || getSelectedMarketType()).trim().toLowerCase();
      const isSpot = marketType === "spot";
      const previewContext = {
        marketType,
        contractType: marketType === "futures" ? String(summary.contract_type || contractTypeEl.value || "usdm").trim().toLowerCase() : "",
        symbol: String(summary.symbol || symbolEl.value || "").trim().toUpperCase(),
      };
      const directionMap = { long: "做多", short: "做空", neutral: "中性" };
      const gridModeMap = { arithmetic: "等差（固定价差）", geometric: "等比（固定百分比）" };
      const allocationModeMap = { equal: "等额（固定名义）", equal_qty: "等数量（固定份额）" };
      const warnings = Array.isArray(summary.warnings) ? summary.warnings.filter(Boolean) : [];
      const ruleText = `tick ${fmtNum(symbolInfo.tick_size, 8)} / step ${fmtNum(symbolInfo.step_size, 8)} / minQty ${fmtNum(symbolInfo.min_qty, 8)} / minNotional ${fmtNum(symbolInfo.min_notional, 4)}`;
      const items = isSpot
        ? [
            ["市场", "现货 V1（单向做多）"],
            ["交易对", summary.symbol || "-"],
            ["当前盘口", `${fmtNum(summary.bid_price)} / ${fmtNum(summary.ask_price)}`],
            ["当前中间价", fmtNum(summary.current_price)],
            ["价格网格", gridModeMap[String(summary.grid_level_mode || "-")] || String(summary.grid_level_mode || "-")],
            ["分配模式", allocationModeMap[String(summary.allocation_mode || "-")] || String(summary.allocation_mode || "-")],
            ["区间", `${fmtNum(summary.min_price)} - ${fmtNum(summary.max_price)}`],
            ["网格数", summary.grid_count],
            ["最大投入预算", fmtMoneyForContext(summary.position_budget_notional, summary.current_price, previewContext)],
            ["当前活动委托", `BUY ${summary.active_buy_orders || 0} / SELL ${summary.active_sell_orders || 0}`],
            ["启动库存", "V1 不自动预建底仓"],
            ["满格买入总额", fmtMoneyForContext(summary.full_long_entry_notional, summary.current_price, previewContext)],
            ["满格持仓量", fmtNum(summary.full_long_qty, 6)],
            ["交易规则", ruleText],
            ["提示", warnings.length ? warnings.join(" ") : "仅当前价下方格子会挂 BUY，买到后再补对应 SELL。"],
          ]
        : [
            ["交易对", summary.symbol || "-"],
            ["当前盘口", `${fmtNum(summary.bid_price)} / ${fmtNum(summary.ask_price)}`],
            ["当前中间价", fmtNum(summary.current_price)],
            ["方向", directionMap[String(summary.strategy_direction || "-")] || String(summary.strategy_direction || "-")],
            ["价格网格", gridModeMap[String(summary.grid_level_mode || "-")] || String(summary.grid_level_mode || "-")],
            ["分配模式", allocationModeMap[String(summary.allocation_mode || "-")] || String(summary.allocation_mode || "-")],
            ["区间", `${fmtNum(summary.min_price)} - ${fmtNum(summary.max_price)}`],
            ["网格数", summary.grid_count],
            ["保证金 / 杠杆", `${fmtMoneyForContext(summary.margin_amount, summary.current_price, previewContext)} / ${fmtNum(summary.leverage, 2)}x`],
            ["最大工作名义", fmtMoneyForContext(summary.position_budget_notional, summary.current_price, previewContext)],
            ["Long / Short 格子", `${summary.long_grid_count || 0} / ${summary.short_grid_count || 0}`],
            ["当前活动委托", `BUY ${summary.active_buy_orders || 0} / SELL ${summary.active_sell_orders || 0}`],
            ["现价启动底仓", formatStartupInventory(summary, 6, 4)],
            ["启动净仓数量", signedNum(summary.startup_net_qty, 6)],
            ["启动净仓名义", signedNum(summary.startup_net_notional, 4)],
            ["满格 Long / Short 名义", `${fmtMoneyForContext(summary.full_long_entry_notional, summary.current_price, previewContext)} / ${fmtMoneyForContext(summary.full_short_entry_notional, summary.current_price, previewContext)}`],
            ["中性锚点", fmtNum(summary.neutral_anchor_price)],
            ["启动多仓爆仓价", fmtNum(summary.startup_long_liquidation_price)],
            ["启动空仓爆仓价", fmtNum(summary.startup_short_liquidation_price)],
            ["满格多仓爆仓价", fmtNum(summary.full_long_liquidation_price)],
            ["满格空仓爆仓价", fmtNum(summary.full_short_liquidation_price)],
            ["维持保证金率", fmtPct(summary.maintenance_margin_ratio)],
            ["交易规则", ruleText],
          ];
      previewSummaryEl.innerHTML = items
        .map(([k, v]) => `<div class="kpi"><div class="k">${k}</div><div class="v">${v ?? "-"}</div></div>`)
        .join("");

      previewBody.innerHTML = rows
        .map((row) => {
          const activeOrder = row.active_order_side
            ? `${row.active_order_side} @ ${fmtNum(row.active_order_price)}`
            : "-";
          return `
            <tr>
              <td>${row.idx}</td>
              <td>${row.grid_side}</td>
              <td>${fmtNum(row.lower_price)}</td>
              <td>${fmtNum(row.upper_price)}</td>
              <td>${row.entry_side}</td>
              <td>${fmtNum(row.entry_price)}</td>
              <td>${row.exit_side}</td>
              <td>${fmtNum(row.exit_price)}</td>
              <td>${fmtNum(row.entry_notional)}</td>
              <td>${fmtNum(row.qty, 6)}</td>
              <td>${row.startup_state || "-"}</td>
              <td>${activeOrder}</td>
            </tr>
          `;
        })
        .join("");
    }

    async function runGridPreview() {
      const payload = readGridPreviewForm();
      previewBtn.disabled = true;
      setPreviewStatus("正在按当前盘口生成网格预览...");
      try {
        const resp = await fetch("/api/grid_preview", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        renderGridPreview(data);
        setPreviewStatus(`已生成：当前盘口 ${fmtNum(data.summary.current_price)}，共 ${data.rows.length} 个格子。`);
      } catch (err) {
        previewSummaryEl.innerHTML = "";
        previewBody.innerHTML = "";
        setPreviewStatus(`预览失败：${err.message}`, true);
      } finally {
        previewBtn.disabled = false;
      }
    }

    function renderVolatilityRanking(rows) {
      volRankBody.innerHTML = (rows || []).map((row) => `
        <tr>
          <td>${row.symbol}</td>
          <td>${fmtPct(row.volatility_annualized)}</td>
          <td>${fmtPct(row.price_return)}</td>
          <td>${row.candle_count}</td>
        </tr>
      `).join("");
    }

    function renderFundingRanking(rows) {
      fundRankBody.innerHTML = (rows || []).map((row) => `
        <tr>
          <td>${row.symbol}</td>
          <td>${fmtPct(row.total_rate)}</td>
          <td>${fmtPct(row.long_side_total_rate)}</td>
          <td>${row.event_count}</td>
        </tr>
      `).join("");
    }

    async function runMarketRanking(isAuto = false) {
      const payload = readRankForm();
      if (!payload.start_time || !payload.end_time) {
        setRankStatus("请先输入排行起止时间。", true);
        return;
      }
      const rankRangeErr = validateSecondIntervalRange(
        payload.start_time,
        payload.end_time,
        payload.interval,
        "排行"
      );
      if (rankRangeErr) {
        setRankStatus(rankRangeErr, true);
        return;
      }
      rankRunBtn.disabled = true;
      if (!isAuto) {
        setRankStatus("正在计算全市场排行，请稍候...");
      }
      try {
        const resp = await fetch("/api/market_rankings", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const topK = Math.max(1, Number(payload.top_k) || 1);
        const volRows = Array.isArray(data.volatility && data.volatility.rows) ? data.volatility.rows : [];
        const fundRows = Array.isArray(data.funding && data.funding.rows) ? data.funding.rows : [];
        renderVolatilityRanking(volRows.slice(0, topK));
        renderFundingRanking(fundRows.slice(0, topK));
        const src = payload.cache_only ? "本地缓存" : "缓存+增量拉取";
        setRankStatus(
          `完成：样本=${data.meta.symbols_considered}，波动率有效=${data.volatility.count}，资金费有效=${data.funding.count}，展示Top=${topK}（排序基于全量样本），耗时=${data.meta.elapsed_seconds}s，来源=${src}。`
        );
      } catch (err) {
        setRankStatus(`排行失败：${err.message}`, true);
      } finally {
        rankRunBtn.disabled = false;
      }
    }

    function stopRankAutoRefresh() {
      if (rankAutoTimer) {
        clearInterval(rankAutoTimer);
        rankAutoTimer = null;
      }
      rankAutoBtn.textContent = "开启自动刷新";
    }

    function startRankAutoRefresh() {
      const seconds = Math.max(60, Number(rankRefreshSecondsEl.value) || 60);
      stopRankAutoRefresh();
      rankAutoTimer = setInterval(() => {
        runMarketRanking(true);
      }, seconds * 1000);
      rankAutoBtn.textContent = "停止自动刷新";
      setRankStatus(`已开启自动刷新：每 ${seconds}s 执行一次。`);
    }

    function buildLayerWeights(layerCount, mode) {
      const m = String(mode || "").trim().toLowerCase();
      if (m === "equal") {
        return Array.from({ length: layerCount }, () => 1);
      }
      if (m === "quadratic") {
        return Array.from({ length: layerCount }, (_, i) => (layerCount - i) ** 2);
      }
      if (m === "geometric") {
        const ratio = 1.25;
        return Array.from({ length: layerCount }, (_, i) => ratio ** (layerCount - i - 1));
      }
      return Array.from({ length: layerCount }, (_, i) => layerCount - i);
    }

    function applyLayerModeUI() {
      const mode = layerNModeEl.value;
      const isFitMode = mode === "fit_selected_plan" || mode === "fit_nested_cover_qty";
      const showBudget = !isFitMode && mode.startsWith("budget_");
      const showTargetStep = !isFitMode && mode === "target_step";
      setGroupVisible(layerManualFields, !isFitMode);
      setGroupVisible(layerBudgetFields, showBudget);
      setGroupVisible(layerTargetStepFields, showTargetStep);
    }

    function allocateIntegerByScore(total, scores, minEach = 1) {
      const n = scores.length;
      if (n === 0) return [];
      const minTotal = minEach * n;
      if (total < minTotal) {
        throw new Error(`总N预算至少为 ${minTotal}`);
      }
      const scoreSum = scores.reduce((a, b) => a + Math.max(0, b), 0);
      if (scoreSum <= 0) {
        const base = Array.from({ length: n }, () => minEach);
        let remain = total - minTotal;
        let idx = 0;
        while (remain > 0) {
          base[idx % n] += 1;
          idx += 1;
          remain -= 1;
        }
        return base;
      }

      const normalized = scores.map((x) => Math.max(0, x) / scoreSum);
      const base = Array.from({ length: n }, () => minEach);
      let remain = total - minTotal;
      const extras = normalized.map((r) => r * remain);
      const floors = extras.map((x) => Math.floor(x));
      for (let i = 0; i < n; i += 1) {
        base[i] += floors[i];
      }
      let used = floors.reduce((a, b) => a + b, 0);
      let left = remain - used;
      const remainders = extras.map((x, i) => ({ i, frac: x - floors[i] }));
      remainders.sort((a, b) => b.frac - a.frac);
      for (let k = 0; k < remainders.length && left > 0; k += 1) {
        base[remainders[k].i] += 1;
        left -= 1;
      }
      return base;
    }

    function buildFittedLayersFromPlan(layerCount) {
      if (!latestPlanRows.length) {
        throw new Error("请先完成一次测算并选中候选方案，再做拟合分层");
      }
      const n = latestPlanRows.length;
      if (n <= 0) {
        throw new Error("当前方案没有可用格子");
      }
      const m = Math.max(1, Math.min(layerCount, n));
      const targets = latestPlanRows.map((row) => Number(row.buy_notional));
      const totalNotional = targets.reduce((a, b) => a + b, 0);

      const prefix1 = [0];
      const prefix2 = [0];
      for (const t of targets) {
        prefix1.push(prefix1[prefix1.length - 1] + t);
        prefix2.push(prefix2[prefix2.length - 1] + t * t);
      }

      function segCost(i, j) {
        const len = j - i + 1;
        const sum = prefix1[j + 1] - prefix1[i];
        const sum2 = prefix2[j + 1] - prefix2[i];
        return sum2 - (sum * sum) / len;
      }

      const dp = Array.from({ length: m + 1 }, () => Array(n).fill(Number.POSITIVE_INFINITY));
      const prev = Array.from({ length: m + 1 }, () => Array(n).fill(-1));

      for (let j = 0; j < n; j += 1) {
        dp[1][j] = segCost(0, j);
      }

      for (let k = 2; k <= m; k += 1) {
        for (let j = k - 1; j < n; j += 1) {
          let best = Number.POSITIVE_INFINITY;
          let bestI = -1;
          for (let i = k - 2; i <= j - 1; i += 1) {
            const val = dp[k - 1][i] + segCost(i + 1, j);
            if (val < best) {
              best = val;
              bestI = i;
            }
          }
          dp[k][j] = best;
          prev[k][j] = bestI;
        }
      }

      const segments = [];
      let k = m;
      let j = n - 1;
      while (k >= 1 && j >= 0) {
        const i = prev[k][j];
        const start = i + 1;
        segments.push([start, j]);
        j = i;
        k -= 1;
      }
      segments.reverse();

      const rows = [];
      let absErrSum = 0;
      let absPctSum = 0;
      for (let sIdx = 0; sIdx < segments.length; sIdx += 1) {
        const [start, end] = segments[sIdx];
        const len = end - start + 1;
        const sum = prefix1[end + 1] - prefix1[start];
        const mean = sum / len;

        let layerAbsPct = 0;
        let layerCountPct = 0;
        for (let i = start; i <= end; i += 1) {
          const err = Math.abs(targets[i] - mean);
          absErrSum += err;
          if (targets[i] > 0) {
            const pct = err / targets[i];
            absPctSum += pct;
            layerAbsPct += pct;
            layerCountPct += 1;
          }
        }

        const low = Number(latestPlanRows[start].buy_price);
        const high = Number(latestPlanRows[end].sell_price);
        const mid = (low + high) / 2;
        const stepPct = mid > 0 ? ((high - low) / len) / mid : 0;
        const notional = mean * len;
        const weightRatio = totalNotional > 0 ? notional / totalNotional : 0;
        const layerMape = layerCountPct > 0 ? layerAbsPct / layerCountPct : 0;
        rows.push({
          layer: sIdx + 1,
          grid_span: `${start + 1}-${end + 1}`,
          min_price: low,
          max_price: high,
          weight_ratio: weightRatio,
          notional,
          suggest_n: len,
          step_pct: stepPct,
          per_grid_notional: mean,
          per_grid_qty: null,
          layer_mode: "equal",
          hint: `拟合层；层内MAPE=${(layerMape * 100).toFixed(2)}%`,
        });
      }

      const fitMae = n > 0 ? absErrSum / n : 0;
      const fitMape = n > 0 ? absPctSum / n : 0;
      return {
        rows,
        totalNotional,
        mode: "fit_selected_plan",
        nMode: "fit_selected_plan",
        layerCount: m,
        requestedLayerCount: layerCount,
        targetN: n,
        fitMae,
        fitMape,
      };
    }

    function buildNestedCoverageQtyFromPlan(layerCount) {
      if (!latestPlanRows.length) {
        throw new Error("请先完成一次测算并选中候选方案，再生成递减覆盖方案");
      }
      const n = latestPlanRows.length;
      if (n <= 0) {
        throw new Error("当前方案没有可用格子");
      }
      const m = Math.max(1, Math.min(layerCount, n));
      const prices = latestPlanRows.map((row) => Number(row.buy_price));
      const targetQty = latestPlanRows.map((row) => Number(row.qty));
      const totalNotional = latestPlanRows.reduce((acc, row) => acc + Number(row.buy_notional), 0);

      const coverNs = [];
      for (let i = 0; i < m; i += 1) {
        coverNs.push(Math.max(1, Math.ceil(n * (m - i) / m)));
      }

      const segMeans = [];
      for (let k = 0; k < m; k += 1) {
        const upper = coverNs[k];
        const lower = (k + 1 < m) ? coverNs[k + 1] : 0;
        let sum = 0;
        let cnt = 0;
        for (let idx = lower; idx < upper; idx += 1) {
          sum += targetQty[idx];
          cnt += 1;
        }
        const mean = cnt > 0 ? (sum / cnt) : targetQty[n - 1];
        segMeans.push(mean);
      }

      // Enforce non-decreasing cumulative qty from high->low, so each layer qty stays >= 0.
      for (let i = 1; i < segMeans.length; i += 1) {
        if (segMeans[i] < segMeans[i - 1]) {
          segMeans[i] = segMeans[i - 1];
        }
      }

      const layerQty = [];
      for (let i = 0; i < segMeans.length; i += 1) {
        if (i === 0) {
          layerQty.push(Math.max(0, segMeans[i]));
        } else {
          layerQty.push(Math.max(0, segMeans[i] - segMeans[i - 1]));
        }
      }

      const predicted = Array.from({ length: n }, () => 0);
      for (let i = 0; i < m; i += 1) {
        const q = layerQty[i];
        const coverN = coverNs[i];
        for (let idx = 0; idx < coverN; idx += 1) {
          predicted[idx] += q;
        }
      }

      let absErrSum = 0;
      let absPctSum = 0;
      for (let idx = 0; idx < n; idx += 1) {
        const err = Math.abs(predicted[idx] - targetQty[idx]);
        absErrSum += err;
        if (targetQty[idx] > 0) {
          absPctSum += err / targetQty[idx];
        }
      }

      const rows = [];
      let layeredTotalNotional = 0;
      for (let i = 0; i < m; i += 1) {
        const coverN = coverNs[i];
        const q = layerQty[i];
        const maxPrice = Number(latestPlanRows[coverN - 1].sell_price);
        const minPrice = Number(latestPlanRows[0].buy_price);
        const mid = (minPrice + maxPrice) / 2;
        const stepPct = mid > 0 ? ((maxPrice - minPrice) / coverN) / mid : 0;
        let layerNotional = 0;
        for (let idx = 0; idx < coverN; idx += 1) {
          layerNotional += prices[idx] * q;
        }
        layeredTotalNotional += layerNotional;
        const avgPerGridNotional = coverN > 0 ? layerNotional / coverN : 0;
        rows.push({
          layer: i + 1,
          grid_span: `1-${coverN}`,
          min_price: minPrice,
          max_price: maxPrice,
          weight_ratio: 0,
          notional: layerNotional,
          suggest_n: coverN,
          step_pct: stepPct,
          per_grid_notional: avgPerGridNotional,
          per_grid_qty: q,
          layer_mode: "equal_qty",
          hint: "递减覆盖层；每格同数量，覆盖低价区更深",
        });
      }
      for (const row of rows) {
        row.weight_ratio = layeredTotalNotional > 0 ? row.notional / layeredTotalNotional : 0;
      }

      const fitMae = n > 0 ? absErrSum / n : 0;
      const fitMape = n > 0 ? absPctSum / n : 0;
      return {
        rows,
        totalNotional: layeredTotalNotional,
        mode: "fit_nested_cover_qty",
        nMode: "fit_nested_cover_qty",
        layerCount: m,
        requestedLayerCount: layerCount,
        targetN: n,
        fitMae,
        fitMape,
      };
    }

    function suggestLayerN(low, high, targetStepPct, nCap, feeRate, slippage) {
      const mid = (low + high) / 2;
      if (mid <= 0 || high <= low) return 6;
      const widthRatio = (high - low) / mid;
      const targetStepRatio = Math.max(
        Math.max(0.0001, targetStepPct / 100),
        (2 * Math.max(0, feeRate) + 2 * Math.max(0, slippage)) * 1.2
      );
      const rawN = Math.floor(widthRatio / targetStepRatio);
      const cap = Math.max(5, Math.floor(nCap));
      return Math.max(5, Math.min(cap, rawN));
    }

    function renderLayerRows(rows) {
      latestLayerRows = rows;
      layerCsvBtn.disabled = rows.length === 0;
      layerCompareBtn.disabled = rows.length === 0;
      layerBody.innerHTML = rows.map((x) => `
        <tr>
          <td>${x.layer}</td>
          <td>${x.grid_span || "-"}</td>
          <td>${fmtNum(x.min_price)}</td>
          <td>${fmtNum(x.max_price)}</td>
          <td>${fmtPct(x.weight_ratio)}</td>
          <td>${fmtNum(x.notional)}</td>
          <td>${x.suggest_n}</td>
          <td>${fmtPct(x.step_pct)}</td>
          <td>${fmtNum(x.per_grid_notional)}</td>
          <td>${x.per_grid_qty === null || x.per_grid_qty === undefined ? "-" : fmtNum(x.per_grid_qty, 6)}</td>
          <td>${x.hint}</td>
        </tr>
      `).join("");
    }

    function buildLayeredParams() {
      const payload = readForm();
      const layerCount = Math.floor(Number(layerCountEl.value));
      const targetStepPct = Number(layerTargetStepPctEl.value);
      const nCap = Math.floor(Number(layerNCapEl.value));
      const nMode = layerNModeEl.value;
      const totalNBudget = Math.floor(Number(layerTotalNEl.value));
      const mode = layerWeightModeEl.value;

      if (nMode === "fit_selected_plan") {
        if (!(layerCount >= 1 && layerCount <= 50)) {
          throw new Error("分层数量需在 1~50 之间");
        }
        return buildFittedLayersFromPlan(layerCount);
      }
      if (nMode === "fit_nested_cover_qty") {
        if (!(layerCount >= 1 && layerCount <= 50)) {
          throw new Error("分层数量需在 1~50 之间");
        }
        return buildNestedCoverageQtyFromPlan(layerCount);
      }

      const minPrice = Number(payload.min_price);
      const maxPrice = Number(payload.max_price);
      let totalNotional;
      if (payload.calc_mode === "fixed") {
        if (payload.fixed_buy_unit === "qty") {
          if (latestPlanRows.length) {
            totalNotional = latestPlanRows.reduce((acc, row) => acc + Number(row.buy_notional || 0), 0);
          } else {
            const n = Math.max(1, Math.floor(Number(payload.fixed_n)));
            const perGridQty = Number(payload.fixed_per_grid_qty);
            const step = (maxPrice - minPrice) / n;
            totalNotional = 0;
            for (let i = 0; i < n; i += 1) {
              totalNotional += (minPrice + i * step) * perGridQty;
            }
          }
        } else {
          totalNotional = Number(payload.fixed_n) * Number(payload.fixed_per_grid_notional);
        }
      } else {
        totalNotional = Number(payload.total_buy_notional);
      }

      if (!(layerCount >= 2 && layerCount <= 50)) {
        throw new Error("分层数量需在 2~50 之间");
      }
      if (!(minPrice > 0 && maxPrice > minPrice)) {
        throw new Error("价格区间无效，请检查最低价和最高价");
      }
      if (!(totalNotional > 0)) {
        throw new Error("总投入必须大于 0");
      }
      if (!(targetStepPct > 0)) {
        if (nMode === "target_step") {
          throw new Error("目标单格价差(%) 必须大于 0");
        }
      }
      if (!(nCap >= 5)) {
        if (nMode === "target_step") {
          throw new Error("每层建议N上限至少为 5");
        }
      }
      if (nMode.startsWith("budget_")) {
        if (!(totalNBudget >= layerCount)) {
          throw new Error(`总格子预算至少为分层数量（>=${layerCount}）`);
        }
        if (totalNBudget > 5000) {
          throw new Error("总格子预算过大（>5000），请检查是否误填。");
        }
        if (latestTopCandidates.length) {
          const current = latestTopCandidates[selectedTopIndex] || latestTopCandidates[0];
          const refN = Number(current && current.n ? current.n : 0);
          if (refN > 0) {
            const softCap = Math.max(layerCount, refN * 20);
            if (totalNBudget > softCap) {
              throw new Error(
                `总格子预算=${totalNBudget} 远大于当前方案N=${refN}。` +
                `建议先用 ${refN}（对齐）或 ${Math.min(softCap, refN * 3)}（适度加密）。`
              );
            }
          }
        }
      }

      const width = (maxPrice - minPrice) / layerCount;
      const weights = buildLayerWeights(layerCount, mode);
      const totalWeight = weights.reduce((a, b) => a + b, 0);
      const widths = Array.from({ length: layerCount }, () => width);
      let nPlan = [];
      if (nMode === "budget_equal_step") {
        nPlan = allocateIntegerByScore(totalNBudget, widths, 1);
      } else if (nMode === "budget_by_weight") {
        nPlan = allocateIntegerByScore(totalNBudget, weights, 1);
      }
      const rows = [];

      for (let i = 0; i < layerCount; i += 1) {
        const low = minPrice + i * width;
        const high = (i === layerCount - 1) ? maxPrice : (minPrice + (i + 1) * width);
        const ratio = weights[i] / totalWeight;
        const layerNotional = totalNotional * ratio;
        let suggestN = nPlan[i];
        if (nMode === "target_step") {
          suggestN = suggestLayerN(
            low,
            high,
            targetStepPct,
            nCap,
            payload.fee_rate,
            payload.slippage
          );
        }
        const mid = (low + high) / 2;
        const stepPct = mid > 0 ? ((high - low) / suggestN) / mid : 0;
        const perGridNotional = layerNotional / suggestN;
        let hint = "中间过渡层";
        if (i === 0) hint = "最低价层，资金更重";
        if (i === layerCount - 1) hint = "高价层，资金更轻";
        if (nMode === "budget_equal_step") hint += "；N按区间宽度分配";
        if (nMode === "budget_by_weight") hint += "；N按资金权重分配";
        if (nMode === "target_step") hint += "；N按目标步长独立计算";
        rows.push({
          layer: i + 1,
          grid_span: "-",
          min_price: low,
          max_price: high,
          weight_ratio: ratio,
          notional: layerNotional,
          suggest_n: suggestN,
          step_pct: stepPct,
          per_grid_notional: perGridNotional,
          per_grid_qty: null,
          layer_mode: "equal",
          hint,
        });
      }
      return { rows, totalNotional, mode, layerCount, nMode };
    }

    function renderComparison(baseline, layered) {
      const marketType = currentResultContext().marketType;
      const specs = [
        { key: "total_buy_notional", label: "总投入基准", type: "num", preferLower: false, neutral: true },
        { key: "net_profit", label: "净收益", type: "num", preferLower: false },
        { key: "total_return", label: "总收益率", type: "pct", preferLower: false },
        { key: "annualized_return", label: "年化收益", type: "pct", preferLower: false },
        { key: "max_drawdown", label: "最大回撤", type: "pct", preferLower: true },
        { key: "calmar", label: "Calmar", type: "num", preferLower: false },
        { key: "trade_count", label: "成交数", type: "int", preferLower: false, neutral: true },
        { key: "trade_volume", label: "成交额", type: "num", preferLower: false },
        { key: "total_fees", label: "手续费", type: "num", preferLower: true },
        { key: "funding_pnl", label: "资金费收益", type: "num", preferLower: false },
        { key: "funding_event_count", label: "资金费次数", type: "int", preferLower: false, neutral: true },
        { key: "avg_capital_usage", label: "平均资金占用", type: "pct", preferLower: false, neutral: true },
        { key: "realized_pnl", label: "已实现收益", type: "num", preferLower: false },
        { key: "unrealized_pnl", label: "期末浮盈", type: "num", preferLower: false },
      ].filter((spec) => {
        if (marketType !== "spot") return true;
        return spec.key !== "funding_pnl" && spec.key !== "funding_event_count";
      });

      function fmtByType(value, type) {
        if (value === null || value === undefined || Number.isNaN(value)) return "-";
        if (type === "pct") return fmtPct(value);
        if (type === "int") return String(Math.round(Number(value)));
        return fmtNum(value);
      }

      function fmtDeltaByType(value, type) {
        if (value === null || value === undefined || Number.isNaN(value)) return "-";
        if (type === "pct") return signedPct(value);
        if (type === "int") {
          const n = Math.round(Number(value));
          if (n === 0) return "0";
          return `${n > 0 ? "+" : ""}${n}`;
        }
        return signedNum(value);
      }

      compareBody.innerHTML = specs.map((spec) => {
        const rawBase = baseline[spec.key];
        const rawLay = layered[spec.key];
        const base = Number(rawBase);
        const lay = Number(rawLay);
        const hasBase = rawBase !== null && rawBase !== undefined && Number.isFinite(base);
        const hasLay = rawLay !== null && rawLay !== undefined && Number.isFinite(lay);
        const delta = hasBase && hasLay ? (lay - base) : null;
        const cls = spec.neutral ? "delta-neutral" : deltaClass(delta, !!spec.preferLower);
        return `
          <tr>
            <td>${spec.label}</td>
            <td>${hasBase ? fmtByType(base, spec.type) : "-"}</td>
            <td>${hasLay ? fmtByType(lay, spec.type) : "-"}</td>
            <td class="${cls}">${fmtDeltaByType(delta, spec.type)}</td>
          </tr>
        `;
      }).join("");
    }

    function gridBoundsFromPlan(best) {
      const rows = Array.isArray(best && best.plan) ? best.plan : [];
      if (!rows.length) return { minPrice: null, maxPrice: null };
      let minPrice = Infinity;
      let maxPrice = -Infinity;
      for (const row of rows) {
        const buy = Number(row && row.buy_price);
        const sell = Number(row && row.sell_price);
        if (Number.isFinite(buy) && buy > 0) minPrice = Math.min(minPrice, buy);
        if (Number.isFinite(sell) && sell > 0) maxPrice = Math.max(maxPrice, sell);
      }
      return {
        minPrice: Number.isFinite(minPrice) ? minPrice : null,
        maxPrice: Number.isFinite(maxPrice) ? maxPrice : null,
      };
    }

    function capitalBaseReferencePrice(best) {
      const direction = String(best && best.strategy_direction || "").trim().toLowerCase();
      const bounds = gridBoundsFromPlan(best);
      const neutralAnchor = Number(best && best.neutral_anchor_price);
      const startPrice = Number(best && best.start_price);
      const endPrice = Number(best && best.end_price);
      if (direction === "long" && Number.isFinite(bounds.minPrice) && bounds.minPrice > 0) {
        return bounds.minPrice;
      }
      if (direction === "short" && Number.isFinite(bounds.maxPrice) && bounds.maxPrice > 0) {
        return bounds.maxPrice;
      }
      if (direction === "neutral" && Number.isFinite(neutralAnchor) && neutralAnchor > 0) {
        return neutralAnchor;
      }
      if (Number.isFinite(startPrice) && startPrice > 0) return startPrice;
      if (Number.isFinite(endPrice) && endPrice > 0) return endPrice;
      return null;
    }

    function renderSummary(best, candleCount) {
      const marketType = String(best.market_type || latestResultMarketType || getSelectedMarketType()).trim().toLowerCase();
      const isSpot = marketType === "spot";
      const directionMap = { long: "做多", short: "做空", neutral: "中性" };
      const gridModeMap = { arithmetic: "等差", geometric: "等比" };
      const directionText = directionMap[String(best.strategy_direction || "").toLowerCase()] || String(best.strategy_direction || "-");
      const gridModeText = gridModeMap[String(best.grid_level_mode || "").toLowerCase()] || String(best.grid_level_mode || "等差");
      const endPrice = Number(best.end_price);
      const baseRefPrice = capitalBaseReferencePrice(best);
      const grossTradeNotional = Number(best.gross_trade_notional ?? best.trade_volume ?? 0);
      const targetTradeVolume = Number(best.target_trade_volume ?? 0);
      const volumeCoverage = Number(best.volume_coverage ?? (targetTradeVolume > 0 ? grossTradeNotional / targetTradeVolume : 1));
      const items = isSpot
        ? [
            ["市场", "现货 V1（单向做多）"],
            ["当前 N", best.n],
            ["当前分配模式", best.allocation_mode],
            ["价格网格", gridModeText],
            ["策略方向", directionText],
            ["净收益", fmtMoney(best.net_profit, endPrice)],
            ["总投入基准", fmtMoney(best.total_buy_notional, baseRefPrice)],
            ["已实现收益", fmtMoney(best.realized_pnl, endPrice)],
            ["期末浮盈", fmtMoney(best.unrealized_pnl, endPrice)],
            ["期末持仓量", fmtNum(best.final_position_qty, 6)],
            ["期末持仓市值", fmtMoney(best.final_position_notional, endPrice)],
            ["手续费", fmtMoney(best.total_fees, endPrice)],
            ["总收益率", fmtPct(best.total_return)],
            ["标的区间涨跌", fmtPct(best.underlying_return)],
            ["区间最低价", fmtNum(best.period_low)],
            ["区间最高价", fmtNum(best.period_high)],
            ["区间振幅(高低/低)", fmtPct(best.period_amplitude)],
            ["策略起始价(首笔成交)", fmtNum(best.start_price)],
            ["终止价格", fmtNum(best.end_price)],
            ["最大回撤", fmtPct(best.max_drawdown)],
            ["年化收益", fmtPct(best.annualized_return)],
            ["成交数", best.trade_count],
            ["成交额", fmtMoney(grossTradeNotional, endPrice)],
            ["目标成交额", fmtMoney(targetTradeVolume, endPrice)],
            ["目标达成率", fmtPct(volumeCoverage)],
            ["平均资金占用", fmtPct(best.avg_capital_usage)],
            ["K线数量", candleCount],
            ["K线覆盖率", fmtPct(latestCandleCoverage)],
          ]
        : [
            ["当前 N", best.n],
            ["当前分配模式", best.allocation_mode],
            ["价格网格", gridModeText],
            ["策略方向", directionText],
            ["净收益", fmtMoney(best.net_profit, endPrice)],
            ["总投入基准", fmtMoney(best.total_buy_notional, baseRefPrice)],
            ["已实现收益", fmtMoney(best.realized_pnl, endPrice)],
            ["期末浮盈", fmtMoney(best.unrealized_pnl, endPrice)],
            ["期末持仓量", fmtNum(best.final_position_qty, 6)],
            ["期末持仓名义", fmtMoney(best.final_position_notional, endPrice)],
            ["手续费", fmtMoney(best.total_fees, endPrice)],
            ["资金费收益", fmtMoney(best.funding_pnl, endPrice)],
            ["资金费次数", best.funding_event_count],
            ["总收益率", fmtPct(best.total_return)],
            ["标的区间涨跌", fmtPct(best.underlying_return)],
            ["区间最低价", fmtNum(best.period_low)],
            ["区间最高价", fmtNum(best.period_high)],
            ["区间振幅(高低/低)", fmtPct(best.period_amplitude)],
            ["策略起始价(首笔成交)", fmtNum(best.start_price)],
            ["终止价格", fmtNum(best.end_price)],
            ["最大回撤", fmtPct(best.max_drawdown)],
            ["年化收益", fmtPct(best.annualized_return)],
            ["成交数", best.trade_count],
            ["成交额", fmtMoney(grossTradeNotional, endPrice)],
            ["目标成交额", fmtMoney(targetTradeVolume, endPrice)],
            ["目标达成率", fmtPct(volumeCoverage)],
            ["平均资金占用", fmtPct(best.avg_capital_usage)],
            ["K线数量", candleCount],
            ["K线覆盖率", fmtPct(latestCandleCoverage)],
          ];
      if (String(best.strategy_direction).toLowerCase() === "neutral" && best.neutral_anchor_price !== null && best.neutral_anchor_price !== undefined) {
        items.splice(3, 0, ["中性锚定价", fmtNum(best.neutral_anchor_price)]);
      }
      summaryEl.innerHTML = items.map(([k, v]) => (
        `<div class="kpi"><div class="k">${k}</div><div class="v">${v}</div></div>`
      )).join("");
      const baseRefLabel = Number.isFinite(Number(baseRefPrice))
        ? `（总投入基准币种折算参考价=${fmtNum(baseRefPrice)}）`
        : "";
      formulaEl.textContent = isSpot
        ? `净收益 = 已实现收益(${fmtNum(best.realized_pnl)}) + 期末浮盈(${fmtNum(best.unrealized_pnl)}) - 手续费(${fmtNum(best.total_fees)}) = ${fmtNum(best.net_profit)}。` +
          ` 总收益率 = 净收益(${fmtNum(best.net_profit)}) / 总投入基准(${fmtNum(best.total_buy_notional)}) = ${fmtPct(best.total_return)}。` +
          ` 现货 V1 不自动预建底仓；回测有效区间：${fmtDateTime(best.start_time)} @ ${fmtNum(best.start_price)} -> ${fmtDateTime(best.end_time)} @ ${fmtNum(best.end_price)}（无成交时起始价回退为首根K线开盘）。${baseRefLabel}`
        : `净收益 = 已实现收益(${fmtNum(best.realized_pnl)}) + 期末浮盈(${fmtNum(best.unrealized_pnl)}) - 手续费(${fmtNum(best.total_fees)}) + 资金费收益(${fmtNum(best.funding_pnl)}) = ${fmtNum(best.net_profit)} (U口径)。` +
          ` 总收益率 = 净收益(${fmtNum(best.net_profit)}) / 总投入基准(${fmtNum(best.total_buy_notional)}) = ${fmtPct(best.total_return)}。` +
          ` 回测有效区间：${fmtDateTime(best.start_time)} @ ${fmtNum(best.start_price)} -> ${fmtDateTime(best.end_time)} @ ${fmtNum(best.end_price)}（无成交时起始价回退为首根K线开盘）。${baseRefLabel}`;
    }

    function renderTop(rows, activeIndex) {
      const active = rows[activeIndex] || rows[0] || {};
      const marketType = String(active.market_type || latestResultMarketType || getSelectedMarketType()).trim().toLowerCase();
      const isSpot = marketType === "spot";
      if (topFundingHeaderEl) {
        topFundingHeaderEl.classList.toggle("is-hidden", isSpot);
      }
      topBody.innerHTML = rows.map((x, idx) => `
        <tr class="candidate-row ${idx === activeIndex ? "active" : ""}" data-idx="${idx}">
          <td>${x.n}</td>
          <td>${x.allocation_mode}</td>
          <td>${fmtNum(x.score, 4)}</td>
          <td>${fmtMoney(x.net_profit, x.end_price)}</td>
          <td>${fmtPct(x.total_return)}</td>
          <td>${fmtPct(x.annualized_return)}</td>
          <td>${fmtPct(x.max_drawdown)}</td>
          <td>${x.trade_count}</td>
          <td>${fmtMoney(x.gross_trade_notional ?? x.trade_volume, x.end_price)}</td>
          <td>${fmtPct(x.volume_coverage)}</td>
          <td>${fmtMoney(x.total_fees, x.end_price)}</td>
          ${isSpot ? "" : `<td>${fmtMoney(x.funding_pnl, x.end_price)}</td>`}
        </tr>
      `).join("");
    }

    function renderRangeSuggestions(rows) {
      latestRangeSuggestions = Array.isArray(rows) ? rows : [];
      suggestBody.innerHTML = latestRangeSuggestions.map((x, idx) => `
        <tr>
          <td>${idx + 1}</td>
          <td>${fmtNum(x.min_price)}</td>
          <td>${fmtNum(x.max_price)}</td>
          <td>${x.recommended_n}</td>
          <td>${x.recommended_mode}</td>
          <td>${fmtPct(x.step_pct)}</td>
          <td>${fmtMoney(x.avg_per_grid_notional)} / ${fmtMoney(x.min_per_grid_notional)} / ${fmtMoney(x.max_per_grid_notional)}</td>
          <td>${fmtMoney(x.gross_trade_notional ?? x.trade_volume, x.max_price)}</td>
          <td>${fmtPct(x.volume_coverage)}</td>
          <td>${fmtMoney(x.net_profit, x.max_price)}</td>
          <td>${fmtPct(x.max_drawdown)}</td>
          <td>${x.trade_count}</td>
          <td>${x.reason || "-"}</td>
          <td><button type="button" data-suggest-idx="${idx}">应用</button></td>
        </tr>
      `).join("");
    }

    function renderShortCandidateSummary(report) {
      const context = (report && report.context) || {};
      const snapshot = (report && report.current_funding_snapshot) || null;
      const items = [
        ["窗口涨跌", fmtPct(context.window_return)],
        ["当前 funding", snapshot ? fmtPct(snapshot.funding_rate) : "-"],
        ["最近 funding 累计", fmtPct(context.funding_sum)],
        ["参考收盘价", fmtNum(context.reference_price)],
        ["区间低/高", `${fmtNum(context.window_low)} / ${fmtNum(context.window_high)}`],
        ["预算", fmtMoney(context.budget, context.reference_price)],
        ["K线数", context.candle_count ?? "-"],
        ["资金费", context.include_funding ? "计入" : "忽略"],
      ];
      shortCandidateSummaryEl.innerHTML = items.map(([k, v]) => (
        `<div class="kpi"><div class="k">${k}</div><div class="v">${v}</div></div>`
      )).join("");
    }

    function renderShortCandidates(rows, activeIndex = selectedShortCandidateIndex) {
      latestShortCandidates = Array.isArray(rows) ? rows : [];
      if (!latestShortCandidates.length) {
        shortCandidateBody.innerHTML = '<tr><td colspan="14">当前没有可用候选，请调整时间窗、预算或手续费后重试。</td></tr>';
        return;
      }
      shortCandidateBody.innerHTML = latestShortCandidates.map((item, idx) => {
        const params = item.params || {};
        const metrics = item.metrics || {};
        const isActive = idx === activeIndex;
        return `
          <tr class="candidate-row ${isActive ? "active" : ""}">
            <td>${escapeHtml(item.title || item.label || `候选${idx + 1}`)}</td>
            <td>${escapeHtml(item.description || "-")}</td>
            <td>
              <div>${fmtNum(params.min_price)} - ${fmtNum(params.max_price)}</div>
              <div class="value-sub">${fmtNum(params.min_offset_pct, 2)}% / +${fmtNum(params.max_offset_pct, 2)}%</div>
            </td>
            <td>${params.n || "-"} / ${escapeHtml(`${params.grid_level_mode || "-"} / ${params.allocation_mode || "-"}`)}</td>
            <td>${fmtNum(metrics.turnover_multiple, 2)}x</td>
            <td>${fmtMoney(metrics.net_profit, params.max_price)}</td>
            <td>${fmtPct(metrics.total_return)}</td>
            <td>${fmtPct(metrics.max_drawdown)}</td>
            <td>${metrics.trade_count ?? "-"}</td>
            <td>${fmtMoney(metrics.funding_pnl, params.max_price)}</td>
            <td>${fmtMoney(metrics.total_fees, params.max_price)}</td>
            <td>${fmtPct(metrics.avg_capital_usage)}</td>
            <td>${escapeHtml(item.recommended_objective || "-")}</td>
            <td><button type="button" data-short-candidate-idx="${idx}">应用</button></td>
          </tr>
        `;
      }).join("");
    }

    function applyShortCandidate(index) {
      if (index < 0 || index >= latestShortCandidates.length) return;
      const candidate = latestShortCandidates[index];
      const patch = candidate.form_patch || {};
      const data = (latestShortCandidateContext && latestShortCandidateContext.data) || {};
      if (marketTypeEl.value !== "futures") {
        marketTypeEl.value = "futures";
        applyMarketTypeUI();
      }
      if (data.contract_type && contractTypeEl.value !== data.contract_type) {
        contractTypeEl.value = data.contract_type;
      }
      if (data.symbol && symbolEl.querySelector(`option[value="${data.symbol}"]`)) {
        symbolEl.value = data.symbol;
      }
      calcModeEl.value = String(patch.calc_mode || "optimize");
      applyCalcModeUI();
      strategyDirectionEl.value = String(patch.strategy_direction || "short");
      gridLevelModeEl.value = String(patch.grid_level_mode || "arithmetic");
      document.getElementById("min_price").value = String(Number(patch.min_price || 0));
      document.getElementById("max_price").value = String(Number(patch.max_price || 0));
      document.getElementById("n_min").value = String(Number(patch.n_min || 0));
      document.getElementById("n_max").value = String(Number(patch.n_max || 0));
      document.getElementById("objective").value = String(patch.objective || "competition_volume");
      if (Number.isFinite(Number(data.total_buy_notional)) && Number(data.total_buy_notional) > 0) {
        document.getElementById("total_buy_notional").value = String(Number(data.total_buy_notional));
      }
      if (includeFundingEl && data.include_funding !== undefined) {
        includeFundingEl.value = data.include_funding ? "1" : "0";
      }
      setAllocationModeSelection(patch.allocation_modes || []);
      selectedShortCandidateIndex = index;
      renderShortCandidates(latestShortCandidates, selectedShortCandidateIndex);
      setStatus(
        `已应用空头候选：${candidate.title || candidate.label}。现在可以直接点“开始测算”，也可以先微调区间、N、分配模式后再回测。`
      );
    }

    function renderPlanOverview(selected, rows) {
      const planRows = Array.isArray(rows) ? rows : [];
      const totalQty = planRows.reduce((acc, row) => acc + Number(row.qty || 0), 0);
      const endPrice = Number(selected && selected.end_price);
      const estimatedFullNotional = Number.isFinite(endPrice) ? totalQty * endPrice : null;
      const finalQty = Number(selected && selected.final_position_qty);
      const finalNotional = Number(selected && selected.final_position_notional);
      const items = [
        ["总持仓量（满格）", fmtNum(totalQty, 6)],
        ["按终止价估算满格名义", fmtMoney(estimatedFullNotional, endPrice)],
        ["期末实际持仓量", fmtNum(finalQty, 6)],
        ["期末实际持仓名义", fmtMoney(finalNotional, endPrice)],
      ];
      planOverviewEl.innerHTML = items.map(([k, v]) => (
        `<div class="plan-pill"><div class="k">${k}</div><div class="v">${v}</div></div>`
      )).join("");
    }

    function renderPlan(rows, selected = null) {
      latestPlanRows = rows;
      csvBtn.disabled = rows.length === 0;
      renderPlanOverview(selected, rows);
      planBody.innerHTML = rows.map((x) => `
        <tr>
          <td>${x.idx}</td>
          <td>${x.grid_side || "-"}</td>
          <td>${fmtNum(x.buy_price)}</td>
          <td>${fmtNum(x.sell_price)}</td>
          <td>${fmtMoney(x.buy_notional, x.buy_price)}</td>
          <td>${fmtNum(x.qty, 6)}</td>
        </tr>
      `).join("");
    }

    function renderFundingRows(rows) {
      latestFundingRows = rows || [];
      fundingCsvBtn.disabled = latestFundingRows.length === 0;
      fundingBody.innerHTML = latestFundingRows.map((row) => `
        <tr>
          <td>${fmtDateTime(row.ts)}</td>
          <td>${fmtPct(row.rate)}</td>
          <td>${fmtNum(row.reference_price)}</td>
          <td>${fmtInitialAsset(row)}</td>
          <td>${fmtMoney(row.position_notional, row.reference_price)}</td>
          <td>${fmtMoney(row.net_pnl, row.reference_price)}</td>
          <td>${fmtPct(row.net_to_initial)}</td>
          <td>${fmtMoney(row.account_equity, row.reference_price)}</td>
          <td>${fmtPct(row.equity_to_initial)}</td>
          <td>${fmtMoney(row.minimum_margin, row.reference_price)}</td>
          <td>${fmtNum(row.liquidation_price)}</td>
          <td>${fmtMoney(row.withdrawable_amount, row.reference_price)}</td>
          <td>${fmtMoney(row.pnl, row.reference_price)}</td>
          <td>${fmtMoney(row.cumulative_pnl, row.reference_price)}</td>
        </tr>
      `).join("");
    }

    function downloadPlanCsv() {
      if (!latestPlanRows.length) return;
      const lines = ["idx,grid_side,buy_price,sell_price,buy_amount_settle,buy_amount_u,qty,settle_asset"];
      const context = currentResultContext();
      const settleAsset = context.contractType === "coinm" ? inferBaseAsset(context.symbol) : "U";
      for (const row of latestPlanRows) {
        const buySettle = toSettleAmount(row.buy_notional, row.buy_price);
        lines.push(
          [
            row.idx,
            row.grid_side || "",
            row.buy_price,
            row.sell_price,
            buySettle === null ? "" : buySettle,
            row.buy_notional,
            row.qty,
            settleAsset,
          ].join(",")
        );
      }
      const blob = new Blob([lines.join("\\n")], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "grid_plan.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    csvBtn.addEventListener("click", downloadPlanCsv);

    function downloadFundingCsv() {
      if (!latestFundingRows.length) return;
      const context = currentResultContext();
      const lines = [
        "ts,rate,reference_price,initial_asset_settle,initial_asset_u,position_notional_settle,position_notional_u,net_pnl_settle,net_pnl_u,net_pnl_to_initial,account_equity_settle,account_equity_u,equity_to_initial,minimum_margin_settle,minimum_margin_u,liquidation_price,withdrawable_amount_settle,withdrawable_amount_u,pnl_settle,pnl_u,cumulative_pnl_settle,cumulative_pnl_u,settle_asset"
      ];
      for (const row of latestFundingRows) {
        const price = Number(row.reference_price);
        const settleAsset = context.contractType === "coinm" ? inferBaseAsset(context.symbol) : "U";
        const initVals = initialAssetValues(row);
        const toSettle = (value) => {
          const out = toSettleAmount(value, price);
          return out === null ? "" : out;
        };
        lines.push(
          [
            row.ts,
            row.rate,
            row.reference_price,
            Number.isFinite(initVals.settle) ? initVals.settle : "",
            Number.isFinite(initVals.quote) ? initVals.quote : "",
            toSettle(row.position_notional),
            row.position_notional,
            toSettle(row.net_pnl),
            row.net_pnl,
            row.net_to_initial,
            toSettle(row.account_equity),
            row.account_equity,
            row.equity_to_initial,
            toSettle(row.minimum_margin),
            row.minimum_margin,
            row.liquidation_price,
            toSettle(row.withdrawable_amount),
            row.withdrawable_amount,
            toSettle(row.pnl),
            row.pnl,
            toSettle(row.cumulative_pnl),
            row.cumulative_pnl,
            settleAsset,
          ].join(",")
        );
      }
      const blob = new Blob([lines.join("\\n")], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "funding_breakdown.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    fundingCsvBtn.addEventListener("click", downloadFundingCsv);

    function downloadLayerCsv() {
      if (!latestLayerRows.length) return;
      const lines = [
        "layer,grid_span,min_price,max_price,weight_ratio,notional,suggest_n,step_pct,per_grid_notional,per_grid_qty,layer_mode,hint"
      ];
      for (const row of latestLayerRows) {
        lines.push(
          [
            row.layer,
            row.grid_span,
            row.min_price,
            row.max_price,
            row.weight_ratio,
            row.notional,
            row.suggest_n,
            row.step_pct,
            row.per_grid_notional,
            row.per_grid_qty,
            row.layer_mode,
            row.hint,
          ].join(",")
        );
      }
      const blob = new Blob([lines.join("\\n")], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "layered_grid_params.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    layerBtn.addEventListener("click", () => {
      try {
        const out = buildLayeredParams();
        renderLayerRows(out.rows);
        clearComparison("分层参数已更新，点击“分层组合回测对比”查看与当前方案差异。");
        const nSum = out.rows.reduce((acc, row) => acc + row.suggest_n, 0);
        if (out.nMode === "fit_selected_plan" || out.nMode === "fit_nested_cover_qty") {
          const extra = out.requestedLayerCount !== out.layerCount
            ? `（请求${out.requestedLayerCount}层，实际${out.layerCount}层）`
            : "";
          const modeText = out.nMode === "fit_nested_cover_qty"
            ? "递减覆盖拟合"
            : "分段拟合";
          setLayerStatus(
            `已完成${modeText}${extra}：目标N=${out.targetN}，压缩后层数=${out.layerCount}，合计N=${nSum}，MAPE=${fmtPct(out.fitMape)}，MAE=${fmtNum(out.fitMae)}。`
          );
        } else {
          setLayerStatus(
            `已生成 ${out.layerCount} 层，资金模式=${out.mode}，N模式=${out.nMode}，合计N=${nSum}，总投入=${fmtNum(out.totalNotional)}。`
          );
        }
      } catch (err) {
        renderLayerRows([]);
        clearComparison();
        setLayerStatus(`生成失败：${err.message}`, true);
      }
    });
    layerCsvBtn.addEventListener("click", downloadLayerCsv);
    layerNModeEl.addEventListener("change", applyLayerModeUI);
    applyLayerModeUI();
    clearComparison();

    layerCompareBtn.addEventListener("click", async () => {
      if (!latestLayerRows.length) {
        setCompareStatus("请先生成分层参数。", true);
        return;
      }
      if (!latestTopCandidates.length) {
        setCompareStatus("请先完成测算并选中当前方案。", true);
        return;
      }
      const baseline = latestTopCandidates[selectedTopIndex];
      const payload = readForm();
      const req = {
        market_type: payload.market_type,
        contract_type: payload.contract_type,
        symbol: payload.symbol,
        start_time: payload.start_time,
        end_time: payload.end_time,
        interval: payload.interval,
        strategy_direction: payload.strategy_direction,
        grid_level_mode: payload.grid_level_mode,
        include_funding: payload.include_funding,
        fee_rate: payload.fee_rate,
        slippage: payload.slippage,
        refresh: false,
        layers: latestLayerRows
          .map((x) => ({
            min_price: Number(x.min_price),
            max_price: Number(x.max_price),
            n: Number(x.suggest_n),
            notional: Number(x.notional),
            grid_span: x.grid_span || "-",
            layer_mode: x.layer_mode || "equal",
          }))
          .filter((x) => x.n > 0 && x.notional > 0),
        baseline: {
          n: baseline.n,
          allocation_mode: baseline.allocation_mode,
          grid_level_mode: baseline.grid_level_mode,
          total_buy_notional: baseline.total_buy_notional,
          net_profit: baseline.net_profit,
          total_return: baseline.total_return,
          annualized_return: baseline.annualized_return,
          max_drawdown: baseline.max_drawdown,
          calmar: baseline.calmar,
          trade_count: baseline.trade_count,
          trade_volume: baseline.trade_volume,
          total_fees: baseline.total_fees,
          funding_pnl: baseline.funding_pnl,
          funding_event_count: baseline.funding_event_count,
          avg_capital_usage: baseline.avg_capital_usage,
          realized_pnl: baseline.realized_pnl,
          unrealized_pnl: baseline.unrealized_pnl,
        },
      };
      if (!req.layers.length) {
        setCompareStatus("当前分层参数没有可回测层（可能每层资金过低）。", true);
        return;
      }

      layerCompareBtn.disabled = true;
      setCompareStatus("正在回测分层组合，请稍候...");
      try {
        const resp = await fetch("/api/layer_compare", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(req)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        latestComparison = data;
        renderComparison(data.baseline, data.layered);
        const cacheLabel = data.data.cache_file ? ` 缓存：${data.data.cache_file}` : "";
        const fundingLabel = data.data.market_type === "spot"
          ? " 现货模式"
          : data.data.include_funding
          ? ` 资金费事件=${data.layered.funding_event_count}`
          : " 资金费：忽略";
        setCompareStatus(
          `对比完成：分层=${data.layered.layer_count}层，合计N=${data.layered.layer_total_n}，K线=${data.data.candles}。${cacheLabel}${fundingLabel}`
        );
      } catch (err) {
        compareBody.innerHTML = "";
        setCompareStatus(`对比失败：${err.message}`, true);
      } finally {
        layerCompareBtn.disabled = latestLayerRows.length === 0;
      }
    });

    function clearFunding(message = "等待测算结果。") {
      renderFundingRows([]);
      setFundingStatus(message);
    }

    async function refreshFundingBreakdown(selected) {
      const payload = readForm();
      if (payload.market_type === "spot") {
        clearFunding("现货模式无资金费。");
        return;
      }
      if (!payload.include_funding) {
        clearFunding("资金费率已设置为忽略。");
        return;
      }
      const rangeErr = validateSecondIntervalRange(
        payload.start_time,
        payload.end_time,
        payload.interval,
        "资金费明细"
      );
      if (rangeErr) {
        clearFunding(rangeErr);
        return;
      }
      if (!selected) {
        clearFunding("未找到当前方案。");
        return;
      }
      const req = {
        market_type: payload.market_type,
        contract_type: payload.contract_type,
        symbol: payload.symbol,
        start_time: payload.start_time,
        end_time: payload.end_time,
        interval: payload.interval,
        strategy_direction: payload.strategy_direction,
        grid_level_mode: payload.grid_level_mode,
        min_price: payload.min_price,
        max_price: payload.max_price,
        n: selected.n,
        allocation_mode: selected.allocation_mode,
        total_buy_notional: selected.total_buy_notional,
        fee_rate: payload.fee_rate,
        slippage: payload.slippage,
        include_funding: payload.include_funding,
        refresh: false,
        fixed_per_grid_qty:
          payload.calc_mode === "fixed" && payload.fixed_buy_unit === "qty"
            ? payload.fixed_per_grid_qty
            : null,
      };

      setFundingStatus("正在计算资金费明细...");
      try {
        const resp = await fetch("/api/funding_breakdown", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(req)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const rows = Array.isArray(data.rows) ? data.rows : [];
        renderFundingRows(rows);
        const cacheLabel = data.data && data.data.funding_cache_file
          ? ` 缓存：${data.data.funding_cache_file}`
          : "";
        const summary = data.summary || {};
        const settleAsset = String(summary.settle_asset || "").trim() || inferBaseAsset(payload.symbol);
        const mode = String(summary.initial_asset_mode || "").trim().toLowerCase();
        const initialQty = Number(summary.initial_asset_qty);
        const initialQtyApprox = Number(summary.initial_asset_qty_approx);
        const initialQuote = Number(summary.initial_asset_quote);
        const startPrice = Number(summary.start_price);
        let initialAssetLabel = "";
        let initialValueLabel = "";
        if (mode === "quote") {
          if (Number.isFinite(initialQuote)) {
            initialAssetLabel = ` 初始资产=${fmtNum(initialQuote)} U`;
          }
          const qtyForRef = Number.isFinite(initialQtyApprox) ? initialQtyApprox : null;
          if (qtyForRef !== null) {
            initialValueLabel = ` (≈${fmtNum(qtyForRef, 6)} ${settleAsset} @起始价${fmtNum(startPrice)})`;
          }
        } else {
          if (Number.isFinite(initialQty)) {
            initialAssetLabel = ` 初始资产=${fmtNum(initialQty, 6)} ${settleAsset}`;
          }
          if (Number.isFinite(initialQuote)) {
            initialValueLabel = ` (≈${fmtNum(initialQuote)} U @起始价${fmtNum(startPrice)})`;
          }
        }
        setFundingStatus(
          `资金费明细已更新：事件=${rows.length}，累计资金费=${fmtNum(data.summary.funding_pnl)} (U口径)，` +
          `保证金假设=${fmtPct((data.summary && data.summary.margin_ratio) || 0)}。` +
          `${initialAssetLabel}${initialValueLabel}${cacheLabel}`
        );
      } catch (err) {
        clearFunding();
        setFundingStatus(`资金费明细失败：${err.message}`, true);
      }
    }

    function selectTopCandidate(index) {
      if (!latestTopCandidates.length) return;
      if (index < 0 || index >= latestTopCandidates.length) return;
      selectedTopIndex = index;
      const selected = latestTopCandidates[index];
      if (layerTotalNEl && Number.isFinite(Number(selected.n)) && selected.n > 0) {
        layerTotalNEl.value = String(selected.n);
      }
      renderSummary(selected, latestCandleCount);
      renderPlan(selected.plan || [], selected);
      clearComparison("当前方案已切换，请重新点击“分层组合回测对比”。");
      renderTop(latestTopCandidates, selectedTopIndex);
      const gridMode = String(selected.grid_level_mode || "").trim() || "arithmetic";
      setStatus(`已切换候选：N=${selected.n}，模式=${selected.allocation_mode}，价格网格=${gridMode}。`);
      refreshFundingBreakdown(selected);
    }

    topBody.addEventListener("click", (e) => {
      const row = e.target.closest("tr.candidate-row");
      if (!row) return;
      const idx = Number(row.dataset.idx);
      if (Number.isNaN(idx)) return;
      selectTopCandidate(idx);
    });
    calcModeEl.addEventListener("change", applyCalcModeUI);
    fixedBuyUnitEl.addEventListener("change", applyFixedBuyUnitUI);
    intervalEl.addEventListener("change", () => applyMainSecondIntervalLimit(true));
    intervalEl.addEventListener("change", () => clearShortCandidates("参数已变更，请重新生成空头候选。"));
    startTimeEl.addEventListener("change", () => applyMainSecondIntervalLimit(true));
    startTimeEl.addEventListener("change", () => clearShortCandidates("参数已变更，请重新生成空头候选。"));
    endTimeEl.addEventListener("change", () => applyMainSecondIntervalLimit(true));
    endTimeEl.addEventListener("change", () => clearShortCandidates("参数已变更，请重新生成空头候选。"));
    rankIntervalEl.addEventListener("change", () => applyRankSecondIntervalLimit(true));
    rankStartTimeEl.addEventListener("change", () => applyRankSecondIntervalLimit(true));
    rankEndTimeEl.addEventListener("change", () => applyRankSecondIntervalLimit(true));
    document.getElementById("total_buy_notional").addEventListener("change", () => {
      clearShortCandidates("参数已变更，请重新生成空头候选。");
    });
    document.getElementById("fee_rate").addEventListener("change", () => {
      clearShortCandidates("参数已变更，请重新生成空头候选。");
    });
    document.getElementById("slippage").addEventListener("change", () => {
      clearShortCandidates("参数已变更，请重新生成空头候选。");
    });
    includeFundingEl.addEventListener("change", () => {
      clearShortCandidates("参数已变更，请重新生成空头候选。");
    });
    marketTypeEl.addEventListener("change", () => {
      stopRankAutoRefresh();
      applyMarketTypeUI();
      clearGridPreview("市场类型已切换，请重新生成当前网格预览。");
      summaryEl.innerHTML = "";
      formulaEl.textContent = "";
      topBody.innerHTML = "";
      renderPlan([], null);
      latestTopCandidates = [];
      latestResultMarketType = getSelectedMarketType();
      latestResultContractType = latestResultMarketType === "futures" ? String(contractTypeEl.value || "usdm") : "";
      latestResultSymbol = String(symbolEl.value || "").toUpperCase();
      selectedTopIndex = 0;
      clearShortCandidates(getSelectedMarketType() === "spot" ? "现货模式不支持空头刷量候选。" : "等待生成。");
      latestCandleCount = 0;
      latestExpectedCandleCount = 0;
      latestCandleCoverage = null;
      clearComparison("市场类型已切换，请重新点击“分层组合回测对比”。");
      if (getSelectedMarketType() === "spot") {
        clearFunding("现货模式无资金费。");
      } else {
        clearFunding();
      }
      loadSymbols(true);
    });
    contractTypeEl.addEventListener("change", () => {
      clearGridPreview("交易对已切换，请重新生成当前网格预览。");
      clearShortCandidates("参数已变更，请重新生成空头候选。");
      loadSymbols(true);
    });
    symbolEl.addEventListener("change", () => {
      clearGridPreview("交易对已切换，请重新生成当前网格预览。");
      clearShortCandidates("参数已变更，请重新生成空头候选。");
    });
    previewBtn.addEventListener("click", runGridPreview);
    rankRunBtn.addEventListener("click", () => runMarketRanking(false));
    rankAutoBtn.addEventListener("click", () => {
      if (rankAutoTimer) {
        stopRankAutoRefresh();
        setRankStatus("已停止自动刷新。");
      } else {
        startRankAutoRefresh();
      }
    });
    applyMarketTypeUI();
    applyCalcModeUI();
    applyFixedBuyUnitUI();
    initializeDefaultTimeRange();
    applyMainSecondIntervalLimit(false);
    applyRankSecondIntervalLimit(false);
    loadSymbols();
    renderRangeSuggestions([]);
    clearShortCandidates(getSelectedMarketType() === "spot" ? "现货模式不支持空头刷量候选。" : "等待生成。");
    clearFunding(getSelectedMarketType() === "spot" ? "现货模式无资金费。" : "等待测算结果。");
    clearGridPreview();

    if (suggestBtn) {
      suggestBody.addEventListener("click", (e) => {
        const btn = e.target.closest("button[data-suggest-idx]");
        if (!btn) return;
        const idx = Number(btn.dataset.suggestIdx);
        if (Number.isNaN(idx) || idx < 0 || idx >= latestRangeSuggestions.length) return;
        const picked = latestRangeSuggestions[idx];
        document.getElementById("min_price").value = String(Number(picked.min_price.toFixed(8)));
        document.getElementById("max_price").value = String(Number(picked.max_price.toFixed(8)));
        document.getElementById("n_min").value = String(picked.recommended_n);
        document.getElementById("n_max").value = String(picked.recommended_n);
        for (const checkbox of document.querySelectorAll('input[name="allocation_mode"]')) {
          checkbox.checked = checkbox.value === picked.recommended_mode;
        }
        setStatus(
          `已应用智能建议：min=${fmtNum(picked.min_price)}，max=${fmtNum(picked.max_price)}，N=${picked.recommended_n}，mode=${picked.recommended_mode}。点击“开始测算”可查看每格买入计划。`
        );
      });

      suggestBtn.addEventListener("click", async () => {
        const payload = readForm();
        if (payload.calc_mode !== "optimize") {
          setSuggestStatus("固定参数模式不需要区间建议，请切换到优化模式。", true);
          return;
        }
        const rangeErr = validateSecondIntervalRange(
          payload.start_time,
          payload.end_time,
          payload.interval,
          "区间建议"
        );
        if (rangeErr) {
          setSuggestStatus(rangeErr, true);
          return;
        }
        suggestBtn.disabled = true;
        setSuggestStatus("正在计算区间建议，请稍候...");
        try {
          const resp = await fetch("/api/suggest_range", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
          });
          const data = await resp.json();
          if (!resp.ok || !data.ok) {
            throw new Error(data.error || `请求失败(${resp.status})`);
          }
          const suggestions = Array.isArray(data.suggestions) ? data.suggestions : [];
          renderRangeSuggestions(suggestions);
          if (!suggestions.length) {
            setSuggestStatus("未生成可用建议，请放宽参数（例如降低目标成交额或放宽N范围）。", true);
            return;
          }
          const best = suggestions[0];
          setSuggestStatus(
            `已生成 ${suggestions.length} 组建议。首选达成率=${fmtPct(best.volume_coverage)}，成交额=${fmtMoney(best.gross_trade_notional ?? best.trade_volume, best.max_price)}。`
          );
        } catch (err) {
          renderRangeSuggestions([]);
          setSuggestStatus(`区间建议失败：${err.message}`, true);
        } finally {
          suggestBtn.disabled = false;
        }
      });
    }

    if (shortCandidateBtn) {
      shortCandidateBody.addEventListener("click", (e) => {
        const btn = e.target.closest("button[data-short-candidate-idx]");
        if (!btn) return;
        const idx = Number(btn.dataset.shortCandidateIdx);
        if (Number.isNaN(idx)) return;
        applyShortCandidate(idx);
      });

      shortCandidateBtn.addEventListener("click", async () => {
        const payload = readForm();
        if (payload.market_type !== "futures") {
          setShortCandidateStatus("空头刷量候选仅支持合约。", true);
          return;
        }
        const rangeErr = validateSecondIntervalRange(
          payload.start_time,
          payload.end_time,
          payload.interval,
          "空头候选"
        );
        if (rangeErr) {
          setShortCandidateStatus(rangeErr, true);
          return;
        }
        currentShortCandidatePollToken += 1;
        const token = currentShortCandidatePollToken;
        shortCandidateBtn.disabled = true;
        setShortCandidateStatus("正在提交空头候选任务...");
        try {
          const submitResp = await fetch("/api/short_volume_candidates", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
          });
          const submitData = await submitResp.json();
          if (!submitResp.ok || !submitData.ok) {
            throw new Error(submitData.error || `请求失败(${submitResp.status})`);
          }
          setShortCandidateStatus(`任务已启动：${submitData.job_id.slice(0, 8)}...`);
          const data = await pollBackgroundJob(
            submitData.job_id,
            token,
            () => token === currentShortCandidatePollToken,
            (job) => {
              const pct = Math.max(0, Math.min(100, Number(job.progress || 0) * 100));
              setShortCandidateStatus(`${job.message || "正在生成候选..."} · ${pct.toFixed(1)}%`);
            }
          );
          latestShortCandidateContext = data;
          selectedShortCandidateIndex = -1;
          renderShortCandidateSummary(data);
          renderShortCandidates(Array.isArray(data.candidates) ? data.candidates : [], selectedShortCandidateIndex);
          const candidates = Array.isArray(data.candidates) ? data.candidates : [];
          if (!candidates.length) {
            setShortCandidateStatus("未生成可用候选，请放宽时间窗或预算后重试。", true);
            return;
          }
          const cacheLabel = data.data && data.data.cache_file ? ` 缓存：${data.data.cache_file}` : "";
          const fundingLabel = data.current_funding_snapshot
            ? ` 当前 funding=${fmtPct(data.current_funding_snapshot.funding_rate)}`
            : "";
          setShortCandidateStatus(`已生成 ${candidates.length} 个空头候选。点击“应用”即可回填主表单。${fundingLabel}${cacheLabel}`);
        } catch (err) {
          clearShortCandidates("等待生成。");
          setShortCandidateStatus(`空头候选失败：${err.message}`, true);
        } finally {
          if (token === currentShortCandidatePollToken) {
            shortCandidateBtn.disabled = false;
          }
        }
      });
    }

    async function pollBackgroundJob(jobId, token, isStillCurrent, onProgress) {
      while (isStillCurrent()) {
        const resp = await fetch(`/api/job/${jobId}`);
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `任务查询失败(${resp.status})`);
        }
        if (typeof onProgress === "function") {
          onProgress(data);
        }

        if (data.status === "done") {
          return data.result;
        }
        if (data.status === "failed") {
          throw new Error(data.error || data.message || "任务失败");
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
      throw new Error("任务已被新的测算请求替换");
    }

    async function pollJob(jobId, token) {
      return pollBackgroundJob(
        jobId,
        token,
        () => token === currentPollToken,
        (data) => setProgress(data.progress, data.eta_seconds, data.message || "")
      );
    }

    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const payload = readForm();
      const rangeErr = validateSecondIntervalRange(
        payload.start_time,
        payload.end_time,
        payload.interval,
        "测算"
      );
      if (rangeErr) {
        setStatus(rangeErr, true);
        return;
      }
      currentPollToken += 1;
      const token = currentPollToken;
      runBtn.disabled = true;
      csvBtn.disabled = true;
      setStatus("正在提交测算任务...");
      setProgress(0, null, "任务初始化");
      try {
        const submitResp = await fetch("/api/optimize", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const submitData = await submitResp.json();
        if (!submitResp.ok || !submitData.ok) {
          throw new Error(submitData.error || `请求失败(${submitResp.status})`);
        }
        setStatus(`任务已启动：${submitData.job_id.slice(0, 8)}...`);
        const data = await pollJob(submitData.job_id, token);
        if (!data.best) {
          const msg = data.error ? `未找到可用方案：${data.error}` : "未找到可用方案，请调整参数。";
          setStatus(msg, true);
          summaryEl.innerHTML = "";
          formulaEl.textContent = "";
          topBody.innerHTML = "";
          renderPlan([], null);
          latestTopCandidates = [];
          clearComparison();
          clearFunding("未找到可用方案。");
          return;
        }
        latestTopCandidates = data.top || [];
        latestCandleCount = data.data.candles;
        latestExpectedCandleCount = Number(data.data.expected_candles || 0);
        latestCandleCoverage = data.data.candle_coverage;
        latestResultMarketType = String((data.data && data.data.market_type) || payload.market_type || "futures");
        latestResultContractType = latestResultMarketType === "futures"
          ? String((data.data && data.data.contract_type) || payload.contract_type || "usdm")
          : "";
        latestResultSymbol = String((data.data && data.data.symbol) || payload.symbol || "").toUpperCase();
        if (!latestTopCandidates.length) {
          latestTopCandidates = [data.best];
        }
        selectTopCandidate(0);
        const cacheLabel = data.data.cache_file ? ` 缓存：${data.data.cache_file}` : "";
        const fundingLabel = latestResultMarketType === "spot"
          ? " 现货模式"
          : data.data.include_funding
          ? ` 资金费事件=${data.data.funding_events || 0}`
          : " 资金费：忽略";
        const coverageLabel = latestExpectedCandleCount > 0
          ? ` K线覆盖=${fmtPct(latestCandleCoverage)} (${latestCandleCount}/${latestExpectedCandleCount})`
          : "";
        if (payload.calc_mode === "fixed") {
          setStatus(`完成：固定参数回测已完成。${coverageLabel}${cacheLabel}${fundingLabel}`);
        } else if (payload.objective === "competition_volume") {
          const best = latestTopCandidates[0];
          setStatus(
            `完成：已按交易赛目标排序，Top1达成率=${fmtPct(best.volume_coverage)}，成交额=${fmtMoney(best.gross_trade_notional ?? best.trade_volume, best.end_price)}。${coverageLabel}${cacheLabel}${fundingLabel}`
          );
        } else {
          setStatus(`完成：默认显示最优候选。${coverageLabel}${cacheLabel}${fundingLabel}`);
        }
        setProgress(1, 0, "完成");
      } catch (err) {
        if (token === currentPollToken) {
          setStatus(`测算失败：${err.message}`, true);
          clearComparison();
          clearFunding("测算失败。");
        }
      } finally {
        if (token === currentPollToken) {
          runBtn.disabled = false;
        }
      }
    });
  </script>
</body>
</html>
"""


RANKING_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>合约市场排行</title>
  <style>
    :root {
      --bg: #f8f7f4;
      --panel: #ffffff;
      --text: #171717;
      --muted: #6a6a6a;
      --line: #e5e1d8;
      --brand: #0f766e;
      --brand-soft: #e6f6f4;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      background: radial-gradient(circle at 10% 0%, #fffef8 0%, var(--bg) 45%, #f1efe8 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1280px;
      margin: 24px auto 48px;
      padding: 0 16px;
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 8px 30px rgba(13, 18, 30, 0.05);
    }
    .header h1 {
      margin: 0 0 6px;
      font-size: 26px;
      letter-spacing: 0.02em;
    }
    .header p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }
    .header a {
      margin-top: 10px;
      display: inline-flex;
      height: 34px;
      align-items: center;
      padding: 0 12px;
      border-radius: 10px;
      border: 1px solid var(--line);
      text-decoration: none;
      color: #0c4b46;
      background: var(--brand-soft);
      font-size: 13px;
      font-weight: 600;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    .field label {
      color: var(--muted);
      font-size: 13px;
    }
    .field input, .field select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      font-size: 14px;
      background: #fff;
    }
    .field input:focus, .field select:focus {
      outline: none;
      border-color: var(--brand);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 4px;
      min-width: 0;
    }
    .actions .msg {
      min-width: 0;
      flex: 1 1 260px;
    }
    button {
      height: 40px;
      border-radius: 10px;
      border: none;
      background: var(--brand);
      color: #fff;
      font-size: 14px;
      font-weight: 600;
      padding: 0 14px;
      cursor: pointer;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.7;
    }
    .msg {
      font-size: 13px;
      color: var(--muted);
    }
    .msg.error {
      color: var(--danger);
      font-weight: 600;
    }
    .two-cols {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
      position: relative;
      background: #fff;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 620px;
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      white-space: nowrap;
    }
    th:first-child, td:first-child {
      text-align: left;
    }
    th {
      background: #faf8f2;
      font-weight: 700;
      color: #3f3f3f;
      position: sticky;
      top: 0;
      z-index: 3;
      box-shadow: inset 0 -1px 0 var(--line), 0 6px 12px rgba(15, 23, 42, 0.04);
      background-clip: padding-box;
      user-select: none;
    }
    th.sortable {
      cursor: pointer;
    }
    .subtitle {
      margin: 0 0 8px;
      font-size: 16px;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .two-cols { grid-template-columns: 1fr; }
    }
    @media (max-width: 600px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>全市场波动率 / 资金费率排行</h1>
      <p>可按指定时间段筛选，支持手动刷新、自动刷新、表头点击切换排序方向、币种模糊过滤。</p>
      <a href="/">返回策略测算页</a>
      <a href="/basis" style="margin-left:8px;">打开现货/合约价差监控</a>
      <a href="/monitor" style="margin-left:8px;">打开实盘网格监控</a>
    </section>

    <section class="card">
      <div class="grid">
        <div class="field">
          <label>起始时间（本地时区）</label>
          <input id="start_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>结束时间（本地时区）</label>
          <input id="end_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>波动率K线周期</label>
          <select id="interval">
            <option value="1s">1s</option>
            <option value="1h">1h</option>
            <option value="1m">1m</option>
          </select>
        </div>
        <div class="field">
          <label>Top数量</label>
          <input id="top_k" type="number" step="1" value="100" min="5" max="500" />
        </div>
        <div class="field">
          <label>合约数量上限(0=全部)</label>
          <input id="max_symbols" type="number" step="1" value="0" min="0" max="5000" />
        </div>
        <div class="field">
          <label>数据源</label>
          <select id="cache_only">
            <option value="1" selected>仅本地缓存（快）</option>
            <option value="0">缓存+增量拉取（慢）</option>
          </select>
        </div>
        <div class="field">
          <label>刷新间隔（秒，最小60）</label>
          <input id="refresh_seconds" type="number" step="1" value="60" min="60" max="3600" />
        </div>
        <div class="field">
          <label>并发数</label>
          <input id="workers" type="number" step="1" value="16" min="1" max="128" />
        </div>
        <div class="field">
          <label>币种模糊过滤</label>
          <input id="symbol_filter" type="text" placeholder="如: BTC, 1000, DOGE" />
        </div>
        <div class="actions">
          <button id="run_btn" type="button">刷新排行</button>
          <button id="auto_btn" type="button">开启自动刷新</button>
          <span id="status" class="msg">等待查询。</span>
        </div>
      </div>
    </section>

    <section class="card">
      <div class="two-cols">
        <div>
          <h3 class="subtitle">波动率排行（年化）</h3>
          <div class="table-wrap">
            <table id="vol_table">
              <thead>
                <tr>
                  <th class="sortable" data-table="volatility" data-key="symbol" data-type="str" data-label="Symbol">Symbol</th>
                  <th class="sortable" data-table="volatility" data-key="volatility_annualized" data-type="num" data-label="年化波动率">年化波动率</th>
                  <th class="sortable" data-table="volatility" data-key="price_return" data-type="num" data-label="区间涨跌">区间涨跌</th>
                  <th class="sortable" data-table="volatility" data-key="candle_count" data-type="num" data-label="K线数">K线数</th>
                </tr>
              </thead>
              <tbody id="vol_tbody"></tbody>
            </table>
          </div>
        </div>
        <div>
          <h3 class="subtitle">总资金费率排行</h3>
          <div class="table-wrap">
            <table id="fund_table">
              <thead>
                <tr>
                  <th class="sortable" data-table="funding" data-key="symbol" data-type="str" data-label="Symbol">Symbol</th>
                  <th class="sortable" data-table="funding" data-key="total_rate" data-type="num" data-label="总资金费率">总资金费率</th>
                  <th class="sortable" data-table="funding" data-key="long_side_total_rate" data-type="num" data-label="多头资金费率">多头资金费率</th>
                  <th class="sortable" data-table="funding" data-key="event_count" data-type="num" data-label="事件数" title="统计区间内资金费结算记录数（通常8小时一次）">事件数</th>
                </tr>
              </thead>
              <tbody id="fund_tbody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
  </div>

  <script>
    const startTimeEl = document.getElementById("start_time");
    const endTimeEl = document.getElementById("end_time");
    const intervalEl = document.getElementById("interval");
    const topKEl = document.getElementById("top_k");
    const maxSymbolsEl = document.getElementById("max_symbols");
    const cacheOnlyEl = document.getElementById("cache_only");
    const refreshSecondsEl = document.getElementById("refresh_seconds");
    const workersEl = document.getElementById("workers");
    const symbolFilterEl = document.getElementById("symbol_filter");
    const runBtn = document.getElementById("run_btn");
    const autoBtn = document.getElementById("auto_btn");
    const statusEl = document.getElementById("status");
    const volBody = document.getElementById("vol_tbody");
    const fundBody = document.getElementById("fund_tbody");

    let rawVolRows = [];
    let rawFundRows = [];
    let autoTimer = null;
    const sortState = {
      volatility: { key: "volatility_annualized", dir: "desc", type: "num" },
      funding: { key: "total_rate", dir: "desc", type: "num" },
    };
    const SECOND_INTERVAL_MAX_SPAN_MS = 31 * 24 * 3600 * 1000;

    function parseIntervalMs(interval) {
      const text = String(interval || "").trim();
      const m = text.match(/^(\\d+)([smhdw])$/);
      if (!m) return null;
      const value = Number(m[1]);
      const unit = m[2];
      const factor = { s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }[unit];
      if (!Number.isFinite(value) || value <= 0 || !factor) return null;
      return value * factor;
    }

    function validateSecondIntervalRange(startIso, endIso, interval) {
      if (!startIso || !endIso) return null;
      const stepMs = parseIntervalMs(interval);
      if (!Number.isFinite(stepMs)) return "K线周期格式不正确";
      if (stepMs >= 60000) return null;
      const startMs = Date.parse(startIso);
      const endMs = Date.parse(endIso);
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return "起止时间格式不正确";
      if (endMs - startMs > SECOND_INTERVAL_MAX_SPAN_MS) {
        return "秒级K线区间不能超过31天";
      }
      return null;
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return `${(Number(v) * 100).toFixed(2)}%`;
    }

    function toDateTimeLocalValue(d) {
      if (!(d instanceof Date) || Number.isNaN(d.getTime())) return "";
      const pad = (x) => String(x).padStart(2, "0");
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function initializeDefaultTimeRange() {
      const now = new Date();
      const oneYearAgo = new Date(now.getTime() - 365 * 24 * 3600 * 1000);
      if (!startTimeEl.value) startTimeEl.value = toDateTimeLocalValue(oneYearAgo);
      if (!endTimeEl.value) endTimeEl.value = toDateTimeLocalValue(now);
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "msg error" : "msg";
    }

    function capSecondIntervalRange(notify = false) {
      if (!startTimeEl.value || !endTimeEl.value) return;
      const stepMs = parseIntervalMs(intervalEl.value.trim());
      if (!Number.isFinite(stepMs) || stepMs >= 60000) {
        endTimeEl.removeAttribute("max");
        return;
      }
      const start = new Date(startTimeEl.value);
      if (Number.isNaN(start.getTime())) {
        endTimeEl.removeAttribute("max");
        return;
      }
      const maxEnd = new Date(start.getTime() + SECOND_INTERVAL_MAX_SPAN_MS);
      const maxEndValue = toDateTimeLocalValue(maxEnd);
      endTimeEl.max = maxEndValue;
      const end = new Date(endTimeEl.value);
      if (Number.isNaN(end.getTime()) || end.getTime() <= maxEnd.getTime()) return;
      endTimeEl.value = maxEndValue;
      if (notify) {
        setStatus("已自动限制为31天：1s秒级K线区间最多31天。");
      }
    }

    function readPayload() {
      const startIso = startTimeEl.value ? new Date(startTimeEl.value).toISOString() : "";
      const endIso = endTimeEl.value ? new Date(endTimeEl.value).toISOString() : "";
      return {
        start_time: startIso,
        end_time: endIso,
        interval: intervalEl.value.trim(),
        top_k: Number(topKEl.value),
        max_symbols: Number(maxSymbolsEl.value),
        cache_only: cacheOnlyEl.value === "1",
        workers: Number(workersEl.value),
        symbol_filter: (symbolFilterEl.value || "").trim(),
      };
    }

    function sortRows(rows, table) {
      const st = sortState[table];
      const out = [...rows];
      out.sort((a, b) => {
        const av = a[st.key];
        const bv = b[st.key];
        if (st.type === "str") {
          const sa = String(av || "");
          const sb = String(bv || "");
          const cmp = sa.localeCompare(sb);
          return st.dir === "asc" ? cmp : -cmp;
        }
        const na = Number(av);
        const nb = Number(bv);
        const aa = Number.isFinite(na) ? na : -Infinity;
        const bb = Number.isFinite(nb) ? nb : -Infinity;
        return st.dir === "asc" ? aa - bb : bb - aa;
      });
      return out;
    }

    function applySymbolFilter(rows) {
      const q = (symbolFilterEl.value || "").trim().toUpperCase();
      if (!q) return rows;
      return rows.filter((x) => String(x.symbol || "").toUpperCase().includes(q));
    }

    function updateHeaderArrows() {
      const headers = document.querySelectorAll("th.sortable");
      headers.forEach((th) => {
        const table = th.dataset.table;
        const key = th.dataset.key;
        const label = th.dataset.label || th.textContent;
        const st = sortState[table];
        let arrow = "";
        if (st && st.key === key) {
          arrow = st.dir === "asc" ? " ▲" : " ▼";
        }
        th.textContent = `${label}${arrow}`;
      });
    }

    function renderTables() {
      const filteredVol = applySymbolFilter(rawVolRows);
      const filteredFund = applySymbolFilter(rawFundRows);
      const topK = Math.max(1, Number(topKEl.value) || 1);
      const volRows = sortRows(filteredVol, "volatility").slice(0, topK);
      const fundRows = sortRows(filteredFund, "funding").slice(0, topK);
      volBody.innerHTML = volRows.map((row) => `
        <tr>
          <td>${row.symbol}</td>
          <td>${fmtPct(row.volatility_annualized)}</td>
          <td>${fmtPct(row.price_return)}</td>
          <td>${row.candle_count}</td>
        </tr>
      `).join("");
      fundBody.innerHTML = fundRows.map((row) => `
        <tr>
          <td>${row.symbol}</td>
          <td>${fmtPct(row.total_rate)}</td>
          <td>${fmtPct(row.long_side_total_rate)}</td>
          <td>${row.event_count}</td>
        </tr>
      `).join("");
      updateHeaderArrows();
    }

    async function runRanking(isAuto = false) {
      const payload = readPayload();
      if (!payload.start_time || !payload.end_time) {
        setStatus("请先输入起止时间。", true);
        return;
      }
      const rangeErr = validateSecondIntervalRange(
        payload.start_time,
        payload.end_time,
        payload.interval
      );
      if (rangeErr) {
        setStatus(rangeErr, true);
        return;
      }
      runBtn.disabled = true;
      if (!isAuto) {
        setStatus("正在刷新排行...");
      }
      try {
        const resp = await fetch("/api/market_rankings", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        rawVolRows = Array.isArray(data.volatility && data.volatility.rows) ? data.volatility.rows : [];
        rawFundRows = Array.isArray(data.funding && data.funding.rows) ? data.funding.rows : [];
        renderTables();
        const source = payload.cache_only ? "本地缓存" : "缓存+增量拉取";
        const autoText = autoTimer ? `，自动刷新中(${Math.max(60, Number(refreshSecondsEl.value) || 60)}s)` : "";
        const topK = Math.max(1, Number(payload.top_k) || 1);
        setStatus(
          `完成：样本=${data.meta.symbols_considered}，波动率有效=${data.volatility.count}，资金费有效=${data.funding.count}，展示Top=${topK}（排序基于全量样本），耗时=${data.meta.elapsed_seconds}s，来源=${source}${autoText}。`
        );
      } catch (err) {
        setStatus(`刷新失败：${err.message}`, true);
      } finally {
        runBtn.disabled = false;
      }
    }

    function stopAutoRefresh() {
      if (autoTimer) {
        clearInterval(autoTimer);
        autoTimer = null;
      }
      autoBtn.textContent = "开启自动刷新";
    }

    function startAutoRefresh() {
      const sec = Math.max(60, Number(refreshSecondsEl.value) || 60);
      stopAutoRefresh();
      autoTimer = setInterval(() => runRanking(true), sec * 1000);
      autoBtn.textContent = "停止自动刷新";
      setStatus(`已开启自动刷新：每 ${sec}s 执行一次。`);
    }

    document.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const table = th.dataset.table;
        const key = th.dataset.key;
        const type = th.dataset.type || "num";
        const st = sortState[table];
        if (!st) return;
        if (st.key === key) {
          st.dir = st.dir === "asc" ? "desc" : "asc";
        } else {
          st.key = key;
          st.type = type;
          st.dir = type === "str" ? "asc" : "desc";
        }
        renderTables();
      });
    });

    symbolFilterEl.addEventListener("input", renderTables);
    topKEl.addEventListener("input", renderTables);
    intervalEl.addEventListener("change", () => capSecondIntervalRange(true));
    startTimeEl.addEventListener("change", () => capSecondIntervalRange(true));
    endTimeEl.addEventListener("change", () => capSecondIntervalRange(true));
    runBtn.addEventListener("click", () => runRanking(false));
    autoBtn.addEventListener("click", () => {
      if (autoTimer) {
        stopAutoRefresh();
        setStatus("已停止自动刷新。");
      } else {
        startAutoRefresh();
      }
    });

    initializeDefaultTimeRange();
    capSecondIntervalRange(false);
    updateHeaderArrows();
  </script>
</body>
</html>
"""

BASIS_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>现货 / 合约价差监控</title>
  <style>
    :root {
      --bg: #f8f7f4;
      --panel: #ffffff;
      --text: #171717;
      --muted: #6a6a6a;
      --line: #e5e1d8;
      --brand: #0f766e;
      --brand-soft: #e6f6f4;
      --danger: #b42318;
      --warn: #b45309;
      --good: #0b6b44;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      background: radial-gradient(circle at 10% 0%, #fffef8 0%, var(--bg) 45%, #f1efe8 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1380px;
      margin: 24px auto 48px;
      padding: 0 16px;
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 8px 30px rgba(13, 18, 30, 0.05);
    }
    .header h1 {
      margin: 0 0 6px;
      font-size: 28px;
      letter-spacing: 0.02em;
    }
    .header p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .header-links {
      margin-top: 12px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .header-links a {
      display: inline-flex;
      height: 34px;
      align-items: center;
      padding: 0 12px;
      border-radius: 10px;
      border: 1px solid var(--line);
      text-decoration: none;
      color: #0c4b46;
      background: var(--brand-soft);
      font-size: 13px;
      font-weight: 600;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    .field label {
      color: var(--muted);
      font-size: 13px;
    }
    .field input, .field select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      font-size: 14px;
      background: #fff;
    }
    .field input:focus, .field select:focus {
      outline: none;
      border-color: var(--brand);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 4px;
      min-width: 0;
    }
    .actions .msg {
      min-width: 0;
      flex: 1 1 260px;
    }
    button {
      height: 40px;
      border-radius: 10px;
      border: none;
      background: var(--brand);
      color: #fff;
      font-size: 14px;
      font-weight: 600;
      padding: 0 14px;
      cursor: pointer;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.7;
    }
    .msg {
      font-size: 13px;
      color: var(--muted);
    }
    .msg.error {
      color: var(--danger);
      font-weight: 600;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: linear-gradient(180deg, #fffdfa 0%, #f7fbfa 100%);
    }
    .kpi .k {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .kpi .v {
      font-size: 20px;
      font-weight: 700;
      color: #0c4b46;
    }
    .kpi .s {
      margin-top: 4px;
      font-size: 12px;
      color: var(--muted);
    }
    .formula-box {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .formula-card {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fcfffe;
    }
    .formula-card h3 {
      margin: 0 0 6px;
      font-size: 15px;
    }
    .formula-card p {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 1280px;
    }
    th, td {
      padding: 10px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      vertical-align: top;
      white-space: nowrap;
    }
    th:first-child, td:first-child {
      text-align: left;
    }
    th {
      background: #faf8f2;
      font-weight: 700;
      color: #3f3f3f;
      position: sticky;
      top: 0;
      z-index: 1;
      user-select: none;
    }
    th.sortable {
      cursor: pointer;
    }
    tr.triggered {
      background: #f6fffb;
    }
    .stack {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 3px;
      min-width: 0;
    }
    td:first-child .stack,
    td:nth-child(2) .stack,
    td:nth-child(3) .stack,
    td:nth-child(10) .stack,
    td:nth-child(12) .stack,
    td:nth-child(13) .stack {
      align-items: flex-start;
    }
    .main {
      font-weight: 600;
      color: var(--text);
    }
    .sub {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
      white-space: normal;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 0 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
    }
    .pill.good {
      color: var(--good);
      background: #e7f7ef;
      border-color: #c4ead4;
    }
    .pill.warn {
      color: var(--warn);
      background: #fff5e8;
      border-color: #f5d3a4;
    }
    .pill.danger {
      color: var(--danger);
      background: #fcebea;
      border-color: #f6c6c3;
    }
    .pill.neutral {
      color: var(--muted);
      background: #f4f2ec;
      border-color: #e5e1d8;
    }
    @media (max-width: 1080px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .summary { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .formula-box { grid-template-columns: 1fr; }
    }
    @media (max-width: 680px) {
      .grid { grid-template-columns: 1fr; }
      .summary { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>现货 / 永续价差监控台</h1>
      <p>默认按 Binance 批量行情接口聚合所有同时存在现货与永续合约的币种，使用可执行盘口计算两类套利价差：买现货/卖合约，以及借币卖现货/买合约。</p>
      <div class="header-links">
        <a href="/">返回策略测算页</a>
        <a href="/rankings">打开波动率 / 资金费率排行</a>
        <a href="/monitor">打开实盘网格监控页</a>
      </div>
    </section>

    <section class="card">
      <div class="grid">
        <div class="field">
          <label>合约类型</label>
          <select id="contract_type">
            <option value="usdm" selected>U本位永续</option>
            <option value="coinm">币本位永续</option>
          </select>
        </div>
        <div class="field">
          <label>现货配对</label>
          <select id="spot_quote_mode">
            <option value="major_stables" selected>稳定币优先（USDT/USDC/FDUSD/BUSD）</option>
            <option value="usdt">仅 USDT 现货</option>
          </select>
        </div>
        <div class="field">
          <label>预警阈值(%)</label>
          <input id="alert_threshold" type="number" step="0.1" min="0" value="1" />
        </div>
        <div class="field">
          <label>最小可执行价差(%)</label>
          <input id="min_spread" type="number" step="0.1" min="0" value="0" />
        </div>
        <div class="field">
          <label>方向筛选</label>
          <select id="direction_filter">
            <option value="all" selected>全部</option>
            <option value="triggered">仅已触发</option>
            <option value="long_spot_short_perp">仅 买现货 / 卖合约</option>
            <option value="short_spot_long_perp">仅 借币卖现货 / 买合约</option>
          </select>
        </div>
        <div class="field">
          <label>资金费方向</label>
          <select id="funding_filter">
            <option value="all" selected>全部</option>
            <option value="positive">仅正资金费</option>
            <option value="negative">仅负资金费</option>
            <option value="flat">仅接近0</option>
          </select>
        </div>
        <div class="field">
          <label>资金费协同</label>
          <select id="carry_filter">
            <option value="all" selected>全部</option>
            <option value="aligned">仅顺风</option>
            <option value="opposed">仅逆风</option>
            <option value="neutral">仅中性</option>
          </select>
        </div>
        <div class="field">
          <label>币种过滤</label>
          <input id="symbol_filter" type="text" placeholder="如 BTC, DOGE, 1000PEPE" />
        </div>
        <div class="field">
          <label>展示数量</label>
          <input id="top_k" type="number" step="1" min="10" max="1000" value="200" />
        </div>
        <div class="field">
          <label>自动刷新(秒，最小10)</label>
          <input id="refresh_seconds" type="number" step="1" min="10" max="3600" value="15" />
        </div>
        <div class="actions">
          <button id="run_btn" type="button">刷新快照</button>
          <button id="auto_btn" type="button">开启自动刷新</button>
          <span id="status" class="msg">等待刷新。</span>
        </div>
      </div>
    </section>

    <section class="card">
      <div class="summary" id="summary"></div>
    </section>

    <section class="card">
      <div class="formula-box">
        <div class="formula-card">
          <h3>套利价差口径</h3>
          <p>买现货 / 卖合约价差 = 合约买一 ÷ 现货卖一 - 1。<br />借币卖现货 / 买合约价差 = 现货买一 ÷ 合约卖一 - 1。<br />中位价差 = 合约中间价 ÷ 现货中间价 - 1。</p>
        </div>
        <div class="formula-card">
          <h3>资金费协同</h3>
          <p>当“买现货 / 卖合约”对应正资金费，或“借币卖现货 / 买合约”对应负资金费时，记为顺风。当前页展示的是 Binance `premiumIndex` 返回的最新资金费率与下一次结算时间。</p>
        </div>
      </div>
    </section>

    <section class="card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="sortable" data-key="base_asset" data-type="str" data-label="标的">标的</th>
              <th data-label="现货">现货</th>
              <th data-label="永续合约">永续合约</th>
              <th class="sortable" data-key="basis_mid" data-type="num" data-label="中位价差">中位价差</th>
              <th class="sortable" data-key="spread_long_spot_short_perp" data-type="num" data-label="买现货/卖合约">买现货 / 卖合约</th>
              <th class="sortable" data-key="spread_short_spot_long_perp" data-type="num" data-label="借币卖现货/买合约">借币卖现货 / 买合约</th>
              <th class="sortable" data-key="funding_rate" data-type="num" data-label="当前资金费">当前资金费</th>
              <th class="sortable" data-key="previous_funding_rate" data-type="num" data-label="上一期资金费">上一期资金费</th>
              <th class="sortable" data-key="previous_basis" data-type="num" data-label="上一期结算价差">上一期结算价差</th>
              <th data-label="借币来源">借币来源</th>
              <th class="sortable" data-key="carry_alignment_rank" data-type="num" data-label="协同度">协同度</th>
              <th class="sortable" data-key="best_spread" data-type="num" data-label="建议动作">建议动作</th>
              <th class="sortable" data-key="best_spread" data-type="num" data-label="状态">状态</th>
            </tr>
          </thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </section>
  </div>

  <script>
    const contractTypeEl = document.getElementById("contract_type");
    const spotQuoteModeEl = document.getElementById("spot_quote_mode");
    const alertThresholdEl = document.getElementById("alert_threshold");
    const minSpreadEl = document.getElementById("min_spread");
    const directionFilterEl = document.getElementById("direction_filter");
    const fundingFilterEl = document.getElementById("funding_filter");
    const carryFilterEl = document.getElementById("carry_filter");
    const symbolFilterEl = document.getElementById("symbol_filter");
    const topKEl = document.getElementById("top_k");
    const refreshSecondsEl = document.getElementById("refresh_seconds");
    const runBtn = document.getElementById("run_btn");
    const autoBtn = document.getElementById("auto_btn");
    const statusEl = document.getElementById("status");
    const summaryEl = document.getElementById("summary");
    const tbody = document.getElementById("tbody");

    let rawRows = [];
    let autoTimer = null;
    const sortState = { key: "best_spread", dir: "desc", type: "num" };
    const SNAPSHOT_TIMEOUT_MS = 15000;
    const DETAIL_BATCH_SIZE = 20;
    let detailLoadTimer = null;
    let loadedDetailKeys = new Set();
    let pendingDetailKeys = new Set();

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return Number(v).toLocaleString(undefined, {
        maximumFractionDigits: digits,
        minimumFractionDigits: digits,
      });
    }

    function fmtPrice(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      const n = Math.abs(Number(v));
      let digits = 8;
      if (n >= 1000) digits = 2;
      else if (n >= 1) digits = 4;
      else if (n >= 0.01) digits = 6;
      return fmtNum(v, digits);
    }

    function fmtPct(v, digits = 2) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return `${(Number(v) * 100).toFixed(digits)}%`;
    }

    function fmtSignedPct(v, digits = 2) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      const n = Number(v);
      const sign = n > 0 ? "+" : "";
      return `${sign}${(n * 100).toFixed(digits)}%`;
    }

    function fmtDateTime(v) {
      if (!v) return "-";
      const d = new Date(v);
      if (Number.isNaN(d.getTime())) return String(v);
      return d.toLocaleString();
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "msg error" : "msg";
    }

    function markSnapshotDirty(text = "参数已更新，请点击“刷新快照”重新拉取。") {
      clearDetailState();
      rawRows = [];
      renderTable();
      setStatus(text);
    }

    function readPayload() {
      return {
        contract_type: (contractTypeEl.value || "usdm").trim(),
        spot_quote_mode: (spotQuoteModeEl.value || "major_stables").trim(),
      };
    }

    function currentThreshold() {
      return Math.max(0, Number(alertThresholdEl.value) || 0) / 100;
    }

    function currentMinSpread() {
      return Math.max(0, Number(minSpreadEl.value) || 0) / 100;
    }

    function isTriggered(row, threshold) {
      return Number(row.best_spread) >= threshold;
    }

    function rowDetailKey(row) {
      return `${contractTypeEl.value}:${row.futures_symbol || ""}:${row.spot_symbol || ""}`;
    }

    function clearDetailState() {
      loadedDetailKeys = new Set();
      pendingDetailKeys = new Set();
      if (detailLoadTimer) {
        clearTimeout(detailLoadTimer);
        detailLoadTimer = null;
      }
    }

    function getFilteredRows(threshold = currentThreshold(), minSpread = currentMinSpread()) {
      return rawRows.filter((row) => passesFilters(row, threshold, minSpread));
    }

    function getRenderableRows(threshold = currentThreshold(), minSpread = currentMinSpread()) {
      return sortRows(getFilteredRows(threshold, minSpread));
    }

    function borrowInfo(row) {
      if (row.borrow === undefined) {
        return { main: "加载中", subs: ["补充借币来源与上一期结算快照"] };
      }
      const borrow = row.borrow || {};
      if (borrow.status === "not_required") {
        return { main: "不需要借币", subs: ["当前建议不是借币卖现货"] };
      }
      if (borrow.status === "missing_api_key") {
        return { main: "未配置 API Key", subs: ["安全模式下仅需 BINANCE_API_KEY"] };
      }
      const subs = [];
      const cross = borrow.cross || {};
      if (borrow.status === "safe_readonly") {
        if (cross.supported) {
          const parts = ["Cross 理论可借"];
          if (cross.restricted) parts.push("当前受限");
          if (cross.next_hourly_interest_rate !== null && cross.next_hourly_interest_rate !== undefined) {
            parts.push(`@ ${fmtSignedPct(cross.next_hourly_interest_rate, 4)}/h`);
          }
          subs.push(parts.join(" "));
        } else if (cross.supported === false) {
          subs.push("Cross Margin 理论上不可借");
        } else {
          subs.push("暂未拿到 Cross Margin 可借状态");
        }
        const isolated = borrow.isolated || {};
        if (isolated.next_hourly_interest_rate !== null && isolated.next_hourly_interest_rate !== undefined) {
          subs.push(`逐仓参考利率 ${fmtSignedPct(isolated.next_hourly_interest_rate, 4)}/h`);
        }
        subs.push("逐仓支持 / VIP Loan / 账户额度未查询");
        if (borrow.note) subs.push(borrow.note);
        if (borrow.error) subs.push(`注: ${borrow.error}`);
        return { main: "理论可借", subs };
      }
      if (cross.supported) {
        const parts = [];
        if (cross.max_borrow !== null && cross.max_borrow !== undefined) {
          parts.push(`Cross max ${fmtNum(cross.max_borrow, 4)} ${escapeHtml(row.base_asset || "")}`);
        }
        if (cross.next_hourly_interest_rate !== null && cross.next_hourly_interest_rate !== undefined) {
          parts.push(`@ ${fmtSignedPct(cross.next_hourly_interest_rate, 4)}/h`);
        }
        subs.push(parts.length ? parts.join(" ") : "Cross Margin 可借");
      } else if (cross.supported === false) {
        subs.push("Cross Margin 不支持或额度为 0");
      }
      const isolated = borrow.isolated || {};
      if (isolated.supported) {
        const parts = [];
        if (isolated.symbol) parts.push(`Isolated ${escapeHtml(isolated.symbol)}`);
        if (isolated.max_borrow !== null && isolated.max_borrow !== undefined) {
          parts.push(`max ${fmtNum(isolated.max_borrow, 4)} ${escapeHtml(row.base_asset || "")}`);
        }
        if (isolated.next_hourly_interest_rate !== null && isolated.next_hourly_interest_rate !== undefined) {
          parts.push(`@ ${fmtSignedPct(isolated.next_hourly_interest_rate, 4)}/h`);
        }
        subs.push(parts.join(" "));
      }
      const vip = borrow.vip || {};
      if (vip.available) {
        const parts = [];
        if (vip.max_limit !== null && vip.max_limit !== undefined) {
          parts.push(`VIP max ${fmtNum(vip.max_limit, 2)} USD`);
        }
        if (vip.flexible_daily_interest_rate !== null && vip.flexible_daily_interest_rate !== undefined) {
          parts.push(`@ ${fmtSignedPct(vip.flexible_daily_interest_rate, 4)}/d`);
        }
        subs.push(parts.join(" "));
      }
      if (!subs.length) {
        subs.push(borrow.error || "暂未拿到可借额度");
      } else if (borrow.error) {
        subs.push(`注: ${borrow.error}`);
      }
      if (borrow.note) {
        subs.push(borrow.note);
      }
      return { main: "可借来源", subs };
    }

    function previousFundingInfo(row) {
      if (row.previous_funding_rate === undefined) {
        return { main: "加载中", subs: ["正在补上一期结算数据"] };
      }
      if (row.previous_funding_rate === null || !row.previous_funding_time) {
        return { main: "-", subs: ["暂无上一期资金费记录"] };
      }
      return {
        main: fmtSignedPct(row.previous_funding_rate, 4),
        subs: [fmtDateTime(row.previous_funding_time)],
      };
    }

    function previousBasisInfo(row) {
      if (row.previous_basis === undefined) {
        return { main: "加载中", subs: ["等待 spot 1m 收盘价"] };
      }
      if (row.previous_basis === null) {
        return { main: "-", subs: ["缺少结算时刻现货/Mark 价格"] };
      }
      return {
        main: fmtSignedPct(row.previous_basis),
        subs: [
          `Mark ${fmtPrice(row.previous_funding_mark_price)}`,
          `Spot ${fmtPrice(row.previous_spot_close)}`,
        ],
      };
    }

    function scheduleVisibleDetailsLoad() {
      if (detailLoadTimer) clearTimeout(detailLoadTimer);
      detailLoadTimer = setTimeout(loadVisibleDetails, 0);
    }

    function mergeDetailRows(detailRows, targets) {
      const detailByCompositeKey = new Map();
      (targets || []).forEach((row) => {
        const detail = detailRows[row.futures_symbol];
        if (detail) {
          detailByCompositeKey.set(rowDetailKey(row), detail);
        }
      });
      rawRows = rawRows.map((row) => {
        const detail = detailByCompositeKey.get(rowDetailKey(row)) || detailRows[row.futures_symbol];
        return detail ? { ...row, ...detail } : row;
      });
    }

    function sortRows(rows) {
      const out = [...rows];
      out.sort((a, b) => {
        const av = a[sortState.key];
        const bv = b[sortState.key];
        if (sortState.type === "str") {
          const sa = String(av || "");
          const sb = String(bv || "");
          const cmp = sa.localeCompare(sb);
          return sortState.dir === "asc" ? cmp : -cmp;
        }
        const na = Number(av);
        const nb = Number(bv);
        const aa = Number.isFinite(na) ? na : -Infinity;
        const bb = Number.isFinite(nb) ? nb : -Infinity;
        return sortState.dir === "asc" ? aa - bb : bb - aa;
      });
      return out;
    }

    function passesFilters(row, threshold, minSpread) {
      if (Number(row.best_spread) < minSpread) return false;

      const q = (symbolFilterEl.value || "").trim().toUpperCase();
      if (q) {
        const text = `${row.base_asset || ""} ${row.spot_symbol || ""} ${row.futures_symbol || ""}`.toUpperCase();
        if (!text.includes(q)) return false;
      }

      const direction = directionFilterEl.value;
      if (direction === "triggered" && !isTriggered(row, threshold)) return false;
      if (
        direction === "long_spot_short_perp" &&
        String(row.arbitrage_side || "") !== "long_spot_short_perp"
      ) return false;
      if (
        direction === "short_spot_long_perp" &&
        String(row.arbitrage_side || "") !== "short_spot_long_perp"
      ) return false;

      const fundingFilter = fundingFilterEl.value;
      const funding = Number(row.funding_rate);
      if (fundingFilter === "positive" && !(funding > 0)) return false;
      if (fundingFilter === "negative" && !(funding < 0)) return false;
      if (fundingFilter === "flat" && Math.abs(funding) > 1e-9) return false;

      const carryFilter = carryFilterEl.value;
      if (carryFilter !== "all" && String(row.carry_alignment || "") !== carryFilter) return false;

      return true;
    }

    function updateHeaderArrows() {
      document.querySelectorAll("th.sortable").forEach((th) => {
        const label = th.dataset.label || th.textContent;
        let arrow = "";
        if (th.dataset.key === sortState.key) {
          arrow = sortState.dir === "asc" ? " ▲" : " ▼";
        }
        th.textContent = `${label}${arrow}`;
      });
    }

    function renderSummary(rows, threshold) {
      const triggered = rows.filter((row) => isTriggered(row, threshold));
      const longSide = triggered.filter((row) => row.arbitrage_side === "long_spot_short_perp");
      const shortSide = triggered.filter((row) => row.arbitrage_side === "short_spot_long_perp");
      const aligned = rows.filter((row) => row.carry_alignment === "aligned");
      const avgBest = rows.length
        ? rows.reduce((sum, row) => sum + (Number(row.best_spread) || 0), 0) / rows.length
        : 0;
      const cards = [
        ["展示币种", rows.length, `当前阈值 ${fmtPct(threshold)}`],
        ["触发预警", triggered.length, `可执行价差 >= ${fmtPct(threshold)}`],
        ["买现货 / 卖合约", longSide.length, "合约相对现货更贵"],
        ["借币卖现货 / 买合约", shortSide.length, "现货相对合约更贵"],
        ["资金费顺风", aligned.length, "价差方向与资金费方向一致"],
        ["平均最优价差", fmtPct(avgBest), rows.length ? "过滤后样本均值" : "暂无样本"],
      ];
      summaryEl.innerHTML = cards.map(([k, v, s]) => `
        <div class="kpi">
          <div class="k">${escapeHtml(k)}</div>
          <div class="v">${typeof v === "number" ? escapeHtml(String(v)) : v}</div>
          <div class="s">${escapeHtml(s)}</div>
        </div>
      `).join("");
      if (!rows.length) {
        summaryEl.innerHTML += `
          <div class="kpi">
            <div class="k">过滤结果</div>
            <div class="v">0</div>
            <div class="s">放宽筛选后可查看更多币种</div>
          </div>
        `;
      }
    }

    function carryPill(row) {
      const label = escapeHtml(row.carry_alignment_label || "中性");
      const cls = row.carry_alignment === "aligned"
        ? "good"
        : row.carry_alignment === "opposed"
          ? "danger"
          : "neutral";
      return `<span class="pill ${cls}">${label}</span>`;
    }

    function statusPill(row, threshold) {
      if (isTriggered(row, threshold)) {
        return `<span class="pill good">触发</span>`;
      }
      if (Number(row.best_spread) > 0) {
        return `<span class="pill warn">观察</span>`;
      }
      return `<span class="pill neutral">无优势</span>`;
    }

    function renderTable() {
      const threshold = currentThreshold();
      const minSpread = currentMinSpread();
      const topK = Math.max(1, Number(topKEl.value) || 1);
      const filtered = getFilteredRows(threshold, minSpread);
      const rows = getRenderableRows(threshold, minSpread).slice(0, topK);
      renderSummary(filtered, threshold);
      if (!rows.length) {
        tbody.innerHTML = `
          <tr>
            <td colspan="13">
              <div class="stack" style="align-items:flex-start;">
                <span class="main">没有匹配结果</span>
                <span class="sub">可以降低阈值、放宽方向筛选，或切换现货配对规则。</span>
              </div>
            </td>
          </tr>
        `;
        updateHeaderArrows();
        return;
      }

      tbody.innerHTML = rows.map((row) => {
        const triggered = isTriggered(row, threshold);
        const actionNote = row.arbitrage_side === "short_spot_long_perp"
          ? "注意借币利息"
          : row.arbitrage_side === "long_spot_short_perp"
            ? "可同时观察资金费收入"
            : "当前不足以覆盖执行成本";
        const previousFunding = previousFundingInfo(row);
        const previousBasis = previousBasisInfo(row);
        const borrow = borrowInfo(row);
        return `
          <tr class="${triggered ? "triggered" : ""}">
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(row.base_asset || row.futures_symbol || "-")}</span>
                <span class="sub">${escapeHtml(row.basis_regime_label || "")}</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(row.spot_symbol || "-")}</span>
                <span class="sub">买一 ${fmtPrice(row.spot_bid)} / 卖一 ${fmtPrice(row.spot_ask)}</span>
                <span class="sub">中价 ${fmtPrice(row.spot_mid)} · ${escapeHtml(row.spot_quote_asset || "")}</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(row.futures_symbol || "-")}</span>
                <span class="sub">买一 ${fmtPrice(row.futures_bid)} / 卖一 ${fmtPrice(row.futures_ask)}</span>
                <span class="sub">中价 ${fmtPrice(row.futures_mid)} · Mark ${fmtPrice(row.mark_price)}</span>
              </div>
            </td>
            <td><div class="stack"><span class="main">${fmtSignedPct(row.basis_mid)}</span><span class="sub">合约中价 vs 现货中价</span></div></td>
            <td><div class="stack"><span class="main">${fmtPct(row.spread_long_spot_short_perp)}</span><span class="sub">合约买一 / 现货卖一 - 1</span></div></td>
            <td><div class="stack"><span class="main">${fmtPct(row.spread_short_spot_long_perp)}</span><span class="sub">现货买一 / 合约卖一 - 1</span></div></td>
            <td>
              <div class="stack">
                <span class="main">${fmtSignedPct(row.funding_rate, 4)}</span>
                <span class="sub">下次 ${escapeHtml(fmtDateTime(row.next_funding_time))}</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(previousFunding.main)}</span>
                ${previousFunding.subs.map((text) => `<span class="sub">${escapeHtml(text)}</span>`).join("")}
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(previousBasis.main)}</span>
                ${previousBasis.subs.map((text) => `<span class="sub">${escapeHtml(text)}</span>`).join("")}
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(borrow.main)}</span>
                ${borrow.subs.map((text) => `<span class="sub">${escapeHtml(text)}</span>`).join("")}
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${carryPill(row)}</span>
                <span class="sub">${escapeHtml(row.carry_alignment_note || "")}</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${escapeHtml(row.strategy_label || "继续观察")}</span>
                <span class="sub">${escapeHtml(actionNote)}</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="main">${statusPill(row, threshold)}</span>
                <span class="sub">最优可执行价差 ${fmtPct(row.best_spread)}</span>
              </div>
            </td>
          </tr>
        `;
      }).join("");
      updateHeaderArrows();
      scheduleVisibleDetailsLoad();
    }

    async function loadVisibleDetails() {
      detailLoadTimer = null;
      if (!rawRows.length) return 0;
      const topK = Math.max(1, Number(topKEl.value) || 1);
      const visibleRows = getRenderableRows().slice(0, Math.min(topK, DETAIL_BATCH_SIZE));
      const targets = visibleRows.filter((row) => {
        const key = rowDetailKey(row);
        return !loadedDetailKeys.has(key) && !pendingDetailKeys.has(key);
      });
      if (!targets.length) return 0;

      targets.forEach((row) => pendingDetailKeys.add(rowDetailKey(row)));
      try {
        const resp = await fetch("/api/basis_enrich", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            contract_type: (contractTypeEl.value || "usdm").trim(),
            items: targets.map((row) => ({
              futures_symbol: row.futures_symbol,
              spot_symbol: row.spot_symbol,
              base_asset: row.base_asset,
              arbitrage_side: row.arbitrage_side,
            })),
          }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const detailRows = data.rows || {};
        mergeDetailRows(detailRows, targets);
        targets.forEach((row) => {
          const key = rowDetailKey(row);
          pendingDetailKeys.delete(key);
          loadedDetailKeys.add(key);
        });
        renderTable();
        return Object.keys(detailRows).length;
      } catch (err) {
        targets.forEach((row) => pendingDetailKeys.delete(rowDetailKey(row)));
        console.error("basis_enrich_failed", err);
        setStatus(`快照已完成，但补充上一期结算/借币数据失败：${err.message}`, true);
        return 0;
      }
    }

    async function runSnapshot(isAuto = false) {
      runBtn.disabled = true;
      runBtn.textContent = "刷新中...";
      if (!isAuto) {
        setStatus("正在拉取现货 / 永续快照...");
      }
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), SNAPSHOT_TIMEOUT_MS);
      try {
        const payload = readPayload();
        if (!isAuto) payload.refresh = true;
        const resp = await fetch("/api/basis_monitor", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
          signal: controller.signal,
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        clearDetailState();
        rawRows = Array.isArray(data.rows) ? data.rows : [];
        renderTable();
        await loadVisibleDetails();
        const source = data.meta && data.meta.stale
          ? "上次成功快照回退"
          : data.meta && data.meta.cached
            ? "内存缓存"
            : "Binance 批量接口";
        const warning = data.meta && data.meta.warning ? `，警告=${data.meta.warning}` : "";
        const autoText = autoTimer ? `，自动刷新中(${Math.max(10, Number(refreshSecondsEl.value) || 10)}s)` : "";
        setStatus(
          `完成：匹配=${data.meta.rows}，合约样本=${data.meta.futures_symbols_considered}，耗时=${data.meta.elapsed_seconds}s，来源=${source}${autoText}${warning}`
        );
      } catch (err) {
        if (controller.signal.aborted) {
          setStatus(`刷新超时（>${Math.round(SNAPSHOT_TIMEOUT_MS / 1000)}s），请重试。`, true);
        } else {
          setStatus(`刷新失败：${err.message}`, true);
        }
      } finally {
        clearTimeout(timeoutId);
        runBtn.disabled = false;
        runBtn.textContent = "刷新快照";
      }
    }

    function stopAutoRefresh() {
      if (autoTimer) {
        clearInterval(autoTimer);
        autoTimer = null;
      }
      autoBtn.textContent = "开启自动刷新";
    }

    function startAutoRefresh() {
      const sec = Math.max(10, Number(refreshSecondsEl.value) || 10);
      stopAutoRefresh();
      autoTimer = setInterval(() => runSnapshot(true), sec * 1000);
      autoBtn.textContent = "停止自动刷新";
      setStatus(`已开启自动刷新：每 ${sec}s 拉取一次快照。`);
    }

    document.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        const type = th.dataset.type || "num";
        if (sortState.key === key) {
          sortState.dir = sortState.dir === "asc" ? "desc" : "asc";
        } else {
          sortState.key = key;
          sortState.type = type;
          sortState.dir = type === "str" ? "asc" : "desc";
        }
        renderTable();
      });
    });

    [
      alertThresholdEl,
      minSpreadEl,
      directionFilterEl,
      fundingFilterEl,
      carryFilterEl,
      symbolFilterEl,
      topKEl,
    ].forEach((el) => {
      el.addEventListener(el.tagName === "INPUT" ? "input" : "change", renderTable);
    });
    contractTypeEl.addEventListener("change", () => {
      if (autoTimer) {
        runSnapshot(true);
        return;
      }
      markSnapshotDirty("合约类型已更新，请点击“刷新快照”。");
    });
    spotQuoteModeEl.addEventListener("change", () => {
      if (autoTimer) {
        runSnapshot(true);
        return;
      }
      markSnapshotDirty("现货配对规则已更新，请点击“刷新快照”。");
    });
    runBtn.addEventListener("click", () => runSnapshot(false));
    autoBtn.addEventListener("click", () => {
      if (autoTimer) {
        stopAutoRefresh();
        setStatus("已停止自动刷新。");
      } else {
        startAutoRefresh();
      }
    });

    updateHeaderArrows();
    renderTable();
    setStatus("等待手动刷新。");
  </script>
</body>
</html>
"""


MONITOR_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>实盘网格监控</title>
  <style>
    :root {
      --bg: #f5f3ee;
      --panel: #ffffff;
      --line: #e2ddd2;
      --text: #171717;
      --muted: #6c685f;
      --brand: #0b6f68;
      --brand-soft: #e6f6f4;
      --good: #0f7b45;
      --warn: #b76e00;
      --bad: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      color: var(--text);
      background: radial-gradient(circle at top left, #fffef8 0%, var(--bg) 46%, #ece8de 100%);
    }
    .wrap { max-width: 1380px; margin: 24px auto 48px; padding: 0 16px; display: grid; gap: 16px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 34px rgba(16, 24, 40, 0.05);
    }
    .header h1 { margin: 0 0 6px; font-size: 28px; }
    .header p { margin: 0; color: var(--muted); font-size: 14px; }
    .header-links, .toolbar { margin-top: 12px; display: flex; gap: 10px; flex-wrap: wrap; }
    .header-links a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      height: 38px;
      padding: 0 14px;
      border-radius: 10px;
      border: 1px solid var(--line);
      text-decoration: none;
      color: #0f423f;
      background: var(--brand-soft);
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }
    button.primary {
      background: var(--brand);
      border-color: var(--brand);
      color: #fff;
    }
    .toolbar label { display: flex; flex-direction: column; gap: 6px; font-size: 12px; color: var(--muted); min-width: 160px; }
    .toolbar .inline-check {
      min-width: 220px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      justify-content: center;
    }
    .toolbar .inline-check .check-row {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text);
      font-size: 13px;
      line-height: 1.4;
    }
    .toolbar .inline-check input[type="checkbox"] {
      width: 16px;
      height: 16px;
      margin: 0;
    }
    .toolbar input, .toolbar select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      background: #fff;
      color: var(--text);
      font-size: 14px;
    }
    .editor-toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .runtime-guard-toolbar {
      margin-top: 0;
      margin-bottom: 8px;
    }
    .runtime-guard-toolbar label {
      min-width: 220px;
    }
    .editor-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(320px, 0.9fr);
      gap: 16px;
      align-items: start;
    }
    .editor-box {
      width: 100%;
      min-height: 360px;
      border-radius: 12px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      background: #fcfbf7;
      color: var(--text);
      font-size: 13px;
      line-height: 1.55;
      font-family: "SFMono-Regular", "Menlo", "Monaco", "Consolas", monospace;
      resize: vertical;
    }
    .param-guide {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fcfbf7;
      overflow: hidden;
    }
    .param-guide .tiny {
      padding: 12px 14px 0;
    }
    .param-guide-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    .param-guide-table th,
    .param-guide-table td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    .param-guide-table th {
      background: #f5f1e8;
      color: var(--muted);
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .param-guide-table td code {
      font-size: 12px;
      color: #0f423f;
    }
    .param-guide-scroll {
      max-height: 420px;
      overflow: auto;
    }
    .strategy-guide-body {
      padding: 12px 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .strategy-guide-card {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: linear-gradient(180deg, #fffefb 0%, #fbf8f2 100%);
      padding: 14px;
    }
    .strategy-guide-card h3 {
      margin: 0 0 8px;
      font-size: 16px;
    }
    .strategy-guide-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 10px;
    }
    .strategy-guide-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 9px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      font-size: 12px;
    }
    .strategy-guide-lead {
      margin: 0;
      color: var(--muted);
      line-height: 1.6;
      font-size: 13px;
    }
    .strategy-guide-section {
      border-top: 1px solid var(--line);
      padding-top: 10px;
    }
    .strategy-guide-section:first-of-type {
      border-top: none;
      padding-top: 0;
    }
    .strategy-guide-section h4 {
      margin: 0 0 8px;
      font-size: 13px;
      color: #0f423f;
    }
    .strategy-guide-list {
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    .strategy-guide-list li + li {
      margin-top: 6px;
    }
    .strategy-guide-note {
      margin: 0;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.6;
    }
    .strategy-guide-empty {
      padding: 16px 4px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }
    .strategy-guide-callout {
      border: 1px dashed var(--line);
      border-radius: 10px;
      background: rgba(255, 255, 255, 0.8);
      padding: 10px 12px;
    }
    .meta { font-size: 13px; color: var(--muted); }
    .alert-stack {
      display: flex;
      flex-direction: column;
      gap: 10px;
      margin-top: 12px;
    }
    .alert-item {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      background: #fcfbf7;
    }
    .alert-item.critical {
      border-color: rgba(180, 35, 24, 0.28);
      background: #fff4f2;
    }
    .alert-item.warning {
      border-color: rgba(183, 110, 0, 0.28);
      background: #fff9ef;
    }
    .alert-item.info {
      border-color: rgba(15, 118, 110, 0.22);
      background: #f2fbfa;
    }
    .alert-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 6px;
    }
    .alert-title {
      font-size: 14px;
      font-weight: 800;
      color: var(--text);
    }
    .alert-severity {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid transparent;
    }
    .alert-item.critical .alert-severity {
      color: var(--bad);
      background: rgba(180, 35, 24, 0.08);
      border-color: rgba(180, 35, 24, 0.12);
    }
    .alert-item.warning .alert-severity {
      color: var(--warn);
      background: rgba(183, 110, 0, 0.08);
      border-color: rgba(183, 110, 0, 0.12);
    }
    .alert-item.info .alert-severity {
      color: var(--brand);
      background: rgba(15, 118, 110, 0.08);
      border-color: rgba(15, 118, 110, 0.12);
    }
    .alert-detail,
    .alert-action,
    .alert-ts {
      font-size: 13px;
      line-height: 1.6;
      color: var(--muted);
    }
    .alert-action {
      margin-top: 6px;
      color: var(--text);
    }
    .alert-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
    }
    .alert-action-btn {
      appearance: none;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      color: #0f423f;
      font-size: 12px;
      font-weight: 700;
      height: 32px;
      padding: 0 12px;
      cursor: pointer;
    }
    .alert-action-btn.primary {
      background: var(--brand-soft);
      border-color: rgba(15, 118, 110, 0.24);
      color: var(--brand);
    }
    .alert-action-btn:disabled {
      cursor: not-allowed;
      opacity: 0.55;
    }
    .alert-empty {
      border: 1px dashed var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
      background: #fcfbf7;
    }
    .status-row { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .metric {
      background: linear-gradient(180deg, #fffefc 0%, #faf8f2 100%);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      min-height: 110px;
    }
    .metric .label { font-size: 12px; color: var(--muted); margin-bottom: 10px; }
    .metric .value { font-size: 24px; font-weight: 800; letter-spacing: -0.02em; }
    .metric .sub { font-size: 12px; color: var(--muted); margin-top: 8px; line-height: 1.5; }
    .metric-lines {
      margin-top: 8px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .metric-line {
      font-size: 12px;
      line-height: 1.55;
      color: var(--muted);
    }
    .metric-line strong {
      color: var(--text);
      font-size: 13px;
      font-weight: 800;
    }
    .metric-line .inline-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 42px;
      padding: 2px 8px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.08);
      color: var(--brand);
      font-weight: 700;
      margin-right: 6px;
    }
    .good { color: var(--good); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .grid-2 { display: grid; grid-template-columns: 1.3fr 1fr; gap: 16px; }
    .panel-title {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
    }
    .panel-title h2 { margin: 0; font-size: 18px; }
    .chart {
      width: 100%;
      height: 220px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: linear-gradient(180deg, #fff 0%, #f7f5ef 100%);
      overflow: hidden;
      position: relative;
    }
    .chart svg { width: 100%; height: 100%; display: block; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { font-size: 12px; color: var(--muted); font-weight: 700; }
    tbody tr:hover { background: #faf8f3; }
    .table-wrap { overflow: auto; }
    .tiny { font-size: 12px; color: var(--muted); }
    .empty {
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 12px;
      color: var(--muted);
      font-size: 13px;
      text-align: center;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: #f0ece3;
      color: #574c36;
    }
    @media (max-width: 980px) {
      .status-row { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid-2 { grid-template-columns: 1fr; }
      .editor-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      .status-row { grid-template-columns: 1fr; }
      .toolbar label { min-width: 100%; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>实盘网格监控台</h1>
      <p>实时查看循环网格的交易笔数、累计成交额、收益估算、当前持仓与挂单，并同步展示最近的补单/撤单动作。</p>
      <div class="header-links">
        <a href="/">返回策略测算页</a>
        <a href="/basis">打开现货/合约价差监控</a>
        <a href="/spot_runner">打开现货执行台</a>
        <a href="/rankings">打开排行榜</a>
        <a href="/strategies">打开策略总览</a>
      </div>
      <div class="toolbar">
        <label>交易对
          <select id="symbol"></select>
        </label>
        <label>刷新秒数
          <input id="refresh_sec" type="number" min="2" step="1" value="5" />
        </label>
        <label>策略预设
          <select id="strategy_preset"></select>
        </label>
        <button id="refresh_btn" class="primary">立即刷新</button>
        <button id="toggle_btn">暂停自动刷新</button>
        <button id="start_strategy_btn" class="primary">启动策略</button>
        <button id="stop_strategy_btn">停止策略</button>
        <label class="inline-check" title="停止策略后撤销当前交易对全部未成交委托">
          <span class="check-row">
            <input id="stop_cancel_orders" type="checkbox" />
            <span>停止时撤销全部委托</span>
          </span>
        </label>
        <label class="inline-check" title="停止策略后启动 maker 跟价平仓：多仓挂卖一、空仓挂买一，未成交会撤旧单并重挂，直到仓位归零">
          <span class="check-row">
            <input id="stop_close_positions" type="checkbox" />
            <span>停止时按买一/卖一平仓</span>
          </span>
        </label>
      </div>
      <div id="meta" class="meta">等待首轮数据...</div>
      <div class="meta">监控币种列表可在 <a href="/strategies">策略总览页</a> 手动添加和删除。</div>
      <div id="strategy_action_meta" class="meta"></div>
      <div id="strategy_preset_meta" class="meta"></div>
      <div id="alert_box" class="alert-stack">
        <div class="alert-empty">等待首轮数据，拿到 runner 状态后会在这里直接提示“保证金不足 / 频率超限 / 停买停空 / 进程退出”等关键问题。</div>
      </div>
    </section>

    <section class="card">
      <div class="panel-title">
        <h2>策略参数编辑</h2>
        <div class="tiny">载入当前运行参数或所选预设，直接修改 JSON 后应用到当前交易对</div>
      </div>
      <div class="editor-toolbar">
        <button id="load_running_params_btn">载入运行参数</button>
        <button id="load_preset_params_btn">载入预设参数</button>
        <button id="apply_params_btn" class="primary">应用参数并启动</button>
      </div>
      <div class="toolbar runtime-guard-toolbar">
        <label>起始交易时间
          <input id="monitor_run_start_time" type="datetime-local" />
        </label>
        <label>结束交易时间
          <input id="monitor_run_end_time" type="datetime-local" />
        </label>
        <label>滚动 60 分钟亏损阈值
          <input id="monitor_rolling_hourly_loss_limit" type="number" min="0" step="0.01" />
        </label>
        <label>累计成交额阈值
          <input id="monitor_max_cumulative_notional" type="number" min="0" step="0.01" />
        </label>
      </div>
      <div class="tiny">这四项会和下方 JSON 同步；时间按当前浏览器所在时区录入，提交时自动转成 UTC 时间戳。</div>
      <div id="runner_params_meta" class="meta">先载入运行参数或预设参数，再按需要修改 JSON。</div>
      <div class="editor-grid">
        <textarea id="runner_params_editor" class="editor-box" spellcheck="false"></textarea>
        <div class="param-guide">
          <div class="tiny">右侧直接解释这份 JSON 会怎样下单、何时移中心、何时暂停或撤单。完整文档在仓库 `docs` 目录下的 `STRATEGY_EXECUTION_GUIDE.md`。</div>
          <div class="param-guide-scroll">
            <div id="runner_params_guide_body" class="strategy-guide-body">
              <div class="strategy-guide-empty">先载入参数，右侧再显示当前策略的执行说明。</div>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="card">
      <div class="panel-title">
        <h2>自定义币安式网格策略</h2>
        <div class="tiny">按当前选中交易对预览，并保存成可直接启动的固定中心静态网格近似策略</div>
      </div>
      <div class="toolbar">
        <label>策略名称
          <input id="custom_grid_name" type="text" placeholder="例如 OPN 合约网格 A" />
        </label>
        <label>方向
          <select id="custom_grid_direction">
            <option value="neutral">中性</option>
            <option value="long">做多</option>
            <option value="short">做空</option>
          </select>
        </label>
        <label>网格模式
          <select id="custom_grid_level_mode">
            <option value="arithmetic">等差（固定价差）</option>
            <option value="geometric">等比（固定百分比）</option>
          </select>
        </label>
        <label>网格数
          <input id="custom_grid_n" type="number" min="1" step="1" value="12" />
        </label>
        <label>最低价
          <input id="custom_grid_min_price" type="number" min="0" step="0.0000001" placeholder="最低价" />
        </label>
        <label>最高价
          <input id="custom_grid_max_price" type="number" min="0" step="0.0000001" placeholder="最高价" />
        </label>
        <label>最大投入保证金
          <input id="custom_grid_margin_amount" type="number" min="0" step="0.01" value="500" />
        </label>
        <label>杠杆
          <input id="custom_grid_leverage" type="number" min="1" step="0.1" value="2" />
        </label>
        <label>条件下移
          <input id="custom_grid_roll_enabled" type="checkbox" />
        </label>
        <label>检查周期（分钟）
          <input id="custom_grid_roll_interval_minutes" type="number" min="1" step="1" value="5" />
        </label>
        <label>成交阈值（笔）
          <input id="custom_grid_roll_trade_threshold" type="number" min="0" step="1" value="100" />
        </label>
        <label>距上沿比例阈值
          <input id="custom_grid_roll_upper_distance_ratio" type="number" min="0" max="1" step="0.01" value="0.30" />
        </label>
        <label>每次下移层数
          <input id="custom_grid_roll_shift_levels" type="number" min="1" step="1" value="1" />
        </label>
        <button id="custom_grid_preview_btn" class="primary">预览网格</button>
        <button id="custom_grid_save_btn">保存为策略</button>
        <button id="custom_grid_load_btn">载入已选策略</button>
        <button id="custom_grid_update_btn">更新已选策略</button>
        <button id="custom_grid_delete_btn" class="danger">删除已选策略</button>
      </div>
      <div id="custom_grid_status" class="meta">按当前选中的交易对生成预览。保存后会出现在上方“策略预设”下拉里。已保存的自定义策略支持载入、编辑和删除。条件下移目前只对做多静态网格生效。</div>
      <div class="meta">网格模式说明：等差表示相邻网格的价格差固定，适合你已经明确绝对价差区间；等比表示相邻网格的百分比间距固定，价格越高格距越大，更适合跨度较大的区间。无论哪种模式，预览和保存后的策略都按每格固定名义金额分配，不按固定币数分配。</div>
      <div id="custom_grid_summary" class="meta"></div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>格子</th>
              <th>方向</th>
              <th>区间</th>
              <th>开仓</th>
              <th>平仓</th>
              <th>数量</th>
              <th>名义</th>
              <th>启动状态</th>
              <th>当前活动委托</th>
            </tr>
          </thead>
          <tbody id="custom_grid_preview_body"></tbody>
        </table>
      </div>
    </section>

    <section class="status-row" id="summary"></section>

    <section class="grid-2">
      <div class="card">
        <div class="panel-title">
          <h2>成交额曲线</h2>
          <div class="tiny">当前会话真实成交累计</div>
        </div>
        <div class="chart" id="trade_chart"></div>
      </div>
      <div class="card">
        <div class="panel-title">
          <h2>中价/循环状态</h2>
          <div class="tiny">最近 30 轮 loop_runner</div>
        </div>
        <div class="chart" id="loop_chart"></div>
      </div>
    </section>

    <section class="card">
      <div class="panel-title">
        <h2>小时损益拆解</h2>
        <div class="tiny" id="hourly_meta">最近 24 小时</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>小时</th>
              <th>成交额</th>
              <th>笔数</th>
              <th>净损益</th>
              <th>已实现</th>
              <th>手续费</th>
              <th>资金费</th>
              <th>涨跌幅</th>
              <th>振幅</th>
              <th>买/卖额</th>
            </tr>
          </thead>
          <tbody id="hourly_body"></tbody>
        </table>
      </div>
    </section>

    <section class="grid-2">
      <div class="card">
        <div class="panel-title">
          <h2>当前挂单</h2>
          <div class="tiny" id="open_order_meta"></div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>方向</th>
                <th>持仓侧</th>
                <th>价格</th>
                <th>数量</th>
                <th>名义</th>
                <th>类型</th>
                <th>时间</th>
              </tr>
            </thead>
            <tbody id="open_orders_body"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="panel-title">
          <h2>当前持仓</h2>
          <div class="tiny">账户实时读取</div>
        </div>
        <div id="position_box"></div>
      </div>
    </section>

    <section class="grid-2">
      <div class="card">
        <div class="panel-title">
          <h2>最近成交</h2>
          <div class="tiny">最近 50 笔真实成交</div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>方向</th>
                <th>价格</th>
                <th>数量</th>
                <th>名义</th>
                <th>已实现</th>
                <th>手续费</th>
                <th>Maker</th>
              </tr>
            </thead>
            <tbody id="trades_body"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="panel-title">
          <h2>最近循环</h2>
          <div class="tiny">补单、撤单和错误</div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>轮次</th>
                <th>中价</th>
                <th>仓位名义</th>
                <th>挂单</th>
                <th>动作</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody id="events_body"></tbody>
          </table>
        </div>
      </div>
    </section>
  </div>

  <script>
    const summaryEl = document.getElementById("summary");
    const tradeChartEl = document.getElementById("trade_chart");
    const loopChartEl = document.getElementById("loop_chart");
    const openOrdersBody = document.getElementById("open_orders_body");
    const positionBox = document.getElementById("position_box");
    const tradesBody = document.getElementById("trades_body");
    const eventsBody = document.getElementById("events_body");
    const hourlyBody = document.getElementById("hourly_body");
    const hourlyMetaEl = document.getElementById("hourly_meta");
    const metaEl = document.getElementById("meta");
    const alertBoxEl = document.getElementById("alert_box");
    const openOrderMetaEl = document.getElementById("open_order_meta");
    const symbolEl = document.getElementById("symbol");
    const refreshSecEl = document.getElementById("refresh_sec");
    const refreshBtn = document.getElementById("refresh_btn");
    const toggleBtn = document.getElementById("toggle_btn");
    const strategyPresetEl = document.getElementById("strategy_preset");
    const startStrategyBtn = document.getElementById("start_strategy_btn");
    const stopStrategyBtn = document.getElementById("stop_strategy_btn");
    const stopCancelOrdersEl = document.getElementById("stop_cancel_orders");
    const stopClosePositionsEl = document.getElementById("stop_close_positions");
    const strategyActionMetaEl = document.getElementById("strategy_action_meta");
    const strategyPresetMetaEl = document.getElementById("strategy_preset_meta");
    const loadRunningParamsBtn = document.getElementById("load_running_params_btn");
    const loadPresetParamsBtn = document.getElementById("load_preset_params_btn");
    const applyParamsBtn = document.getElementById("apply_params_btn");
    const runnerParamsMetaEl = document.getElementById("runner_params_meta");
    const runnerParamsEditorEl = document.getElementById("runner_params_editor");
    const runnerParamsGuideBodyEl = document.getElementById("runner_params_guide_body");
    const monitorRunStartTimeEl = document.getElementById("monitor_run_start_time");
    const monitorRunEndTimeEl = document.getElementById("monitor_run_end_time");
    const monitorRollingHourlyLossLimitEl = document.getElementById("monitor_rolling_hourly_loss_limit");
    const monitorMaxCumulativeNotionalEl = document.getElementById("monitor_max_cumulative_notional");
    const customGridNameEl = document.getElementById("custom_grid_name");
    const customGridDirectionEl = document.getElementById("custom_grid_direction");
    const customGridLevelModeEl = document.getElementById("custom_grid_level_mode");
    const customGridNEl = document.getElementById("custom_grid_n");
    const customGridMinPriceEl = document.getElementById("custom_grid_min_price");
    const customGridMaxPriceEl = document.getElementById("custom_grid_max_price");
    const customGridMarginAmountEl = document.getElementById("custom_grid_margin_amount");
    const customGridLeverageEl = document.getElementById("custom_grid_leverage");
    const customGridRollEnabledEl = document.getElementById("custom_grid_roll_enabled");
    const customGridRollIntervalEl = document.getElementById("custom_grid_roll_interval_minutes");
    const customGridRollTradeThresholdEl = document.getElementById("custom_grid_roll_trade_threshold");
    const customGridRollUpperDistanceRatioEl = document.getElementById("custom_grid_roll_upper_distance_ratio");
    const customGridRollShiftLevelsEl = document.getElementById("custom_grid_roll_shift_levels");
    const customGridPreviewBtn = document.getElementById("custom_grid_preview_btn");
    const customGridSaveBtn = document.getElementById("custom_grid_save_btn");
    const customGridLoadBtn = document.getElementById("custom_grid_load_btn");
    const customGridUpdateBtn = document.getElementById("custom_grid_update_btn");
    const customGridDeleteBtn = document.getElementById("custom_grid_delete_btn");
    const customGridStatusEl = document.getElementById("custom_grid_status");
    const customGridSummaryEl = document.getElementById("custom_grid_summary");
    const customGridPreviewBody = document.getElementById("custom_grid_preview_body");
    const DEFAULT_MONITOR_SYMBOLS = ["NIGHTUSDT", "OPNUSDT", "ROBOUSDT", "KATUSDT"];
    const LOCAL_STRATEGY_PRESETS = [
      {
        key: "volume_long_v4",
        label: "量优先做多 v4",
        description: "当前实盘主策略。偏多滚动微网格，保留成交量，带分钟熔断和库存分层。",
        startable: true,
        kind: "one_way",
        config: {
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 70.0,
          base_position_notional: 420.0,
          up_trigger_steps: 6,
          down_trigger_steps: 4,
          shift_steps: 4,
          pause_buy_position_notional: 750.0,
          max_position_notional: 900.0,
          buy_pause_amp_trigger_ratio: 0.0075,
          buy_pause_down_return_trigger_ratio: -0.0035,
          freeze_shift_abs_return_trigger_ratio: 0.005,
          inventory_tier_start_notional: 600.0,
          inventory_tier_end_notional: 750.0,
          inventory_tier_buy_levels: 4,
          inventory_tier_sell_levels: 12,
          inventory_tier_per_order_notional: 70.0,
          inventory_tier_base_position_notional: 280.0,
        },
      },
      {
        key: "volatility_defensive_v1",
        label: "高波动防守 v1",
        description: "高振幅市场的保守预设。更轻底仓、更早停买、更慢追跌，并收紧分钟级熔断，优先控制损耗。",
        startable: true,
        kind: "one_way",
        config: {
          buy_levels: 4,
          sell_levels: 12,
          per_order_notional: 45.0,
          base_position_notional: 120.0,
          up_trigger_steps: 5,
          down_trigger_steps: 7,
          shift_steps: 3,
          pause_buy_position_notional: 300.0,
          max_position_notional: 420.0,
          buy_pause_amp_trigger_ratio: 0.0055,
          buy_pause_down_return_trigger_ratio: -0.0025,
          freeze_shift_abs_return_trigger_ratio: 0.0035,
          inventory_tier_start_notional: 220.0,
          inventory_tier_end_notional: 320.0,
          inventory_tier_buy_levels: 2,
          inventory_tier_sell_levels: 14,
          inventory_tier_per_order_notional: 45.0,
          inventory_tier_base_position_notional: 90.0,
        },
      },
      {
        key: "adaptive_volatility_v1",
        label: "自适应刷量/防守 v1",
        description: "自动识别稳定与高波动状态。稳定时用量优先做多 v4，高波动或下跌扩振时切到高波动防守 v1，并带确认周期避免来回抖动。",
        startable: true,
        kind: "one_way",
        config: {
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 70.0,
          base_position_notional: 420.0,
          up_trigger_steps: 6,
          down_trigger_steps: 4,
          shift_steps: 4,
          pause_buy_position_notional: 750.0,
          max_position_notional: 900.0,
          buy_pause_amp_trigger_ratio: 0.0075,
          buy_pause_down_return_trigger_ratio: -0.0035,
          freeze_shift_abs_return_trigger_ratio: 0.005,
          inventory_tier_start_notional: 600.0,
          inventory_tier_end_notional: 750.0,
          inventory_tier_buy_levels: 4,
          inventory_tier_sell_levels: 12,
          inventory_tier_per_order_notional: 70.0,
          inventory_tier_base_position_notional: 280.0,
          auto_regime_enabled: true,
          auto_regime_confirm_cycles: 2,
          auto_regime_stable_15m_max_amplitude_ratio: 0.02,
          auto_regime_stable_60m_max_amplitude_ratio: 0.05,
          auto_regime_stable_60m_return_floor_ratio: -0.01,
          auto_regime_defensive_15m_amplitude_ratio: 0.035,
          auto_regime_defensive_60m_amplitude_ratio: 0.08,
          auto_regime_defensive_15m_return_ratio: -0.015,
          auto_regime_defensive_60m_return_ratio: -0.03,
        },
      },
      {
        key: "bard_volume_long_v2",
        label: "BARD 量优先做多 v2",
        description: "BARDUSDT 专用做多预设。加入启动门禁和首轮 warm-start，避免带仓或带旧挂单时一上来补到底仓。",
        startable: true,
        kind: "one_way",
        symbol: "BARDUSDT",
        config: {
          symbol: "BARDUSDT",
          buy_levels: 5,
          sell_levels: 11,
          per_order_notional: 40.0,
          base_position_notional: 120.0,
          flat_start_enabled: true,
          warm_start_enabled: true,
          step_price: 0.0002,
          up_trigger_steps: 2,
          down_trigger_steps: 2,
          shift_steps: 2,
          pause_buy_position_notional: 420.0,
          max_position_notional: 560.0,
          buy_pause_amp_trigger_ratio: 0.0048,
          buy_pause_down_return_trigger_ratio: -0.002,
          freeze_shift_abs_return_trigger_ratio: 0.004,
          inventory_tier_start_notional: 260.0,
          inventory_tier_end_notional: 380.0,
          inventory_tier_buy_levels: 2,
          inventory_tier_sell_levels: 14,
          inventory_tier_per_order_notional: 35.0,
          inventory_tier_base_position_notional: 80.0,
          autotune_symbol_enabled: false,
          excess_inventory_reduce_only_enabled: true,
          sleep_seconds: 5.0,
        },
      },
      {
        key: "xaut_long_adaptive_v1",
        label: "XAUT 自适应做多 v1",
        description: "仅用于 XAUTUSDT 的三态自适应做多。平稳时刷量，扩振时转防守，极端波动时立即撤买单并只保留卖单减仓。",
        startable: true,
        kind: "one_way",
        symbol: "XAUTUSDT",
        config: {
          symbol: "XAUTUSDT",
          strategy_mode: "one_way_long",
          step_price: 7.5,
          buy_levels: 6,
          sell_levels: 10,
          per_order_notional: 80.0,
          base_position_notional: 320.0,
          up_trigger_steps: 5,
          down_trigger_steps: 4,
          shift_steps: 3,
          pause_buy_position_notional: 520.0,
          max_position_notional: 680.0,
          buy_pause_amp_trigger_ratio: 0.0060,
          buy_pause_down_return_trigger_ratio: -0.0045,
          freeze_shift_abs_return_trigger_ratio: 0.0048,
          inventory_tier_start_notional: 420.0,
          inventory_tier_end_notional: 520.0,
          inventory_tier_buy_levels: 3,
          inventory_tier_sell_levels: 12,
          inventory_tier_per_order_notional: 70.0,
          inventory_tier_base_position_notional: 160.0,
          autotune_symbol_enabled: false,
          excess_inventory_reduce_only_enabled: false,
        },
      },
      {
        key: "xaut_short_adaptive_v1",
        label: "XAUT 自适应做空 v1",
        description: "仅用于 XAUTUSDT 的三态自适应做空。平稳时刷量，扩振时转防守，极端波动时立即撤卖单并只保留买单回补减仓。",
        startable: true,
        kind: "one_way",
        symbol: "XAUTUSDT",
        config: {
          symbol: "XAUTUSDT",
          strategy_mode: "one_way_short",
          step_price: 7.6,
          buy_levels: 10,
          sell_levels: 6,
          per_order_notional: 80.0,
          base_position_notional: 260.0,
          up_trigger_steps: 4,
          down_trigger_steps: 5,
          shift_steps: 3,
          pause_short_position_notional: 600.0,
          max_short_position_notional: 660.0,
          inventory_tier_start_notional: 360.0,
          inventory_tier_end_notional: 500.0,
          inventory_tier_buy_levels: 14,
          inventory_tier_sell_levels: 2,
          inventory_tier_per_order_notional: 65.0,
          inventory_tier_base_position_notional: 120.0,
          short_cover_pause_amp_trigger_ratio: 0.0060,
          short_cover_pause_down_return_trigger_ratio: -0.0045,
          autotune_symbol_enabled: false,
          excess_inventory_reduce_only_enabled: false,
        },
      },
      {
        key: "volume_short_v1",
        label: "量优先做空 v1",
        description: "偏空滚动微网格。结构上镜像量优先做多 v4，适合 OPN 这类偏弱下跌窗口，优先在反抽中开空、在回落中回补。",
        startable: true,
        kind: "one_way",
        config: {
          strategy_mode: "one_way_short",
          step_price: 0.00002,
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 70.0,
          base_position_notional: 420.0,
          up_trigger_steps: 3,
          down_trigger_steps: 4,
          shift_steps: 3,
          pause_short_position_notional: 900.0,
          max_short_position_notional: 900.0,
          inventory_tier_start_notional: 600.0,
          inventory_tier_end_notional: 750.0,
          inventory_tier_buy_levels: 12,
          inventory_tier_sell_levels: 4,
          inventory_tier_per_order_notional: 70.0,
          inventory_tier_base_position_notional: 280.0,
          sleep_seconds: 10.0,
        },
      },
      {
        key: "volume_short_v1_aggressive",
        label: "量优先做空 v1（激进）",
        description: "激进空头滚动微网格。沿用 OPN 的高量空头参数，适合明显弱势下跌窗口，优先提升成交量。",
        startable: true,
        kind: "one_way",
        config: {
          strategy_mode: "one_way_short",
          step_price: 0.00002,
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 70.0,
          base_position_notional: 420.0,
          up_trigger_steps: 3,
          down_trigger_steps: 4,
          shift_steps: 3,
          pause_short_position_notional: 900.0,
          max_short_position_notional: 900.0,
          inventory_tier_start_notional: 600.0,
          inventory_tier_end_notional: 750.0,
          inventory_tier_buy_levels: 12,
          inventory_tier_sell_levels: 4,
          inventory_tier_per_order_notional: 70.0,
          inventory_tier_base_position_notional: 280.0,
          sleep_seconds: 10.0,
        },
      },
      {
        key: "night_volume_short_v1",
        label: "NIGHT 专用做空高换手",
        description: "针对 NIGHTUSDT 的低价高换手做空预设。仍保留更快轮询和更近的首笔往返，但降低单笔、底仓和总空仓上限，并在急跌放量时暂停追着买回。",
        startable: true,
        kind: "one_way",
        config: {
          strategy_mode: "one_way_short",
          step_price: 0.00002,
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 45.0,
          base_position_notional: 180.0,
          up_trigger_steps: 4,
          down_trigger_steps: 4,
          shift_steps: 2,
          pause_short_position_notional: 450.0,
          max_short_position_notional: 600.0,
          inventory_tier_start_notional: 300.0,
          inventory_tier_end_notional: 450.0,
          inventory_tier_buy_levels: 10,
          inventory_tier_sell_levels: 4,
          inventory_tier_per_order_notional: 45.0,
          inventory_tier_base_position_notional: 90.0,
          sleep_seconds: 10.0,
          autotune_symbol_enabled: false,
          short_cover_pause_amp_trigger_ratio: 0.004,
          short_cover_pause_down_return_trigger_ratio: -0.0018,
          freeze_shift_abs_return_trigger_ratio: 0.0045,
        },
      },
      {
        key: "volume_short_v1_conservative",
        label: "量优先做空 v1（保守）",
        description: "保守空头滚动微网格。更轻底仓、更低单笔和更慢追涨开空，适合 NIGHT 这类偏震荡币种试空。",
        startable: true,
        kind: "one_way",
        config: {
          strategy_mode: "one_way_short",
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 45.0,
          base_position_notional: 180.0,
          up_trigger_steps: 5,
          down_trigger_steps: 7,
          shift_steps: 3,
          pause_short_position_notional: 450.0,
          max_short_position_notional: 600.0,
          inventory_tier_start_notional: 300.0,
          inventory_tier_end_notional: 450.0,
          inventory_tier_buy_levels: 10,
          inventory_tier_sell_levels: 4,
          inventory_tier_per_order_notional: 45.0,
          inventory_tier_base_position_notional: 90.0,
          short_cover_pause_amp_trigger_ratio: 0.0045,
          short_cover_pause_down_return_trigger_ratio: -0.002,
        },
      },
      {
        key: "xaut_volume_short_v1",
        label: "XAUT 专用做空高换手",
        description: "面向 XAUTUSDT 的高换手空头预设。维持 1000 USDT 空仓上限，收紧动态步长到现价约 0.017%，优先把小时成交额顶到 1 万附近。",
        startable: true,
        kind: "one_way",
        symbol: "XAUTUSDT",
        config: {
          strategy_mode: "one_way_short",
          step_price: 0.00002,
          buy_levels: 10,
          sell_levels: 10,
          per_order_notional: 100.0,
          base_position_notional: 240.0,
          up_trigger_steps: 2,
          down_trigger_steps: 3,
          shift_steps: 2,
          pause_short_position_notional: 850.0,
          max_short_position_notional: 1000.0,
          inventory_tier_start_notional: 650.0,
          inventory_tier_end_notional: 850.0,
          inventory_tier_buy_levels: 14,
          inventory_tier_sell_levels: 6,
          inventory_tier_per_order_notional: 100.0,
          inventory_tier_base_position_notional: 150.0,
          sleep_seconds: 5.0,
          autotune_symbol_enabled: true,
        },
      },
      {
        key: "defensive_quasi_neutral_aggressive_v1",
        label: "准中性降损激进版",
        description: "基于 ROBO 最近实盘运行参数固化出的激进准中性版本。仍以做多为主，但提高卖侧卸仓能力、放宽总上限，适合趋势不明时保量控损。",
        startable: true,
        kind: "one_way",
        config: {
          strategy_mode: "one_way_long",
          step_price: 0.00001,
          buy_levels: 8,
          sell_levels: 16,
          per_order_notional: 180.0,
          base_position_notional: 300.0,
          up_trigger_steps: 6,
          down_trigger_steps: 4,
          shift_steps: 4,
          pause_buy_position_notional: 1200.0,
          max_position_notional: 1500.0,
          buy_pause_amp_trigger_ratio: 0.0075,
          buy_pause_down_return_trigger_ratio: -0.0035,
          freeze_shift_abs_return_trigger_ratio: 0.005,
          inventory_tier_start_notional: 800.0,
          inventory_tier_end_notional: 1200.0,
          inventory_tier_buy_levels: 6,
          inventory_tier_sell_levels: 18,
          inventory_tier_per_order_notional: 180.0,
          inventory_tier_base_position_notional: 180.0,
          max_new_orders: 30,
          max_total_notional: 5000.0,
          sleep_seconds: 5.0,
          autotune_symbol_enabled: false,
        },
      },
      {
        key: "volume_neutral_target_v1",
        label: "量优先中性 v1",
        description: "单向模式下的目标净仓中性策略。每 3 分钟重定中心，按上下 0.5% / 1% / 2% 三档目标仓位曲线挂买卖单，并按小时缩放目标仓位，优先控下跌损耗。",
        startable: true,
        kind: "target_neutral",
        config: {
          strategy_mode: "inventory_target_neutral",
          buy_levels: 3,
          sell_levels: 3,
          per_order_notional: 50.0,
          base_position_notional: 0.0,
          up_trigger_steps: 1,
          down_trigger_steps: 1,
          shift_steps: 1,
          pause_buy_position_notional: 900.0,
          pause_short_position_notional: 900.0,
          max_position_notional: 900.0,
          max_short_position_notional: 900.0,
          buy_pause_amp_trigger_ratio: 0.009,
          buy_pause_down_return_trigger_ratio: -0.005,
          freeze_shift_abs_return_trigger_ratio: 0.006,
          neutral_center_interval_minutes: 3,
          neutral_band1_offset_ratio: 0.005,
          neutral_band2_offset_ratio: 0.01,
          neutral_band3_offset_ratio: 0.02,
          neutral_band1_target_ratio: 0.20,
          neutral_band2_target_ratio: 0.50,
          neutral_band3_target_ratio: 1.00,
          neutral_hourly_scale_enabled: true,
          neutral_hourly_scale_stable: 1.0,
          neutral_hourly_scale_transition: 0.85,
          neutral_hourly_scale_defensive: 0.65,
        },
      },
      {
        key: "defensive_quasi_neutral_v1",
        label: "准中性降损",
        description: "单向兼容的降损预设。减少底仓和买侧权重，增加卖侧卸仓速度，适合量够后控损耗。",
        startable: true,
        kind: "one_way",
        config: {
          buy_levels: 6,
          sell_levels: 12,
          per_order_notional: 80.0,
          base_position_notional: 160.0,
          up_trigger_steps: 6,
          down_trigger_steps: 4,
          shift_steps: 4,
          pause_buy_position_notional: 700.0,
          max_position_notional: 850.0,
          buy_pause_amp_trigger_ratio: 0.0075,
          buy_pause_down_return_trigger_ratio: -0.0035,
          freeze_shift_abs_return_trigger_ratio: 0.005,
          inventory_tier_start_notional: 500.0,
          inventory_tier_end_notional: 650.0,
          inventory_tier_buy_levels: 4,
          inventory_tier_sell_levels: 14,
          inventory_tier_per_order_notional: 80.0,
          inventory_tier_base_position_notional: 80.0,
        },
      },
      {
        key: "neutral_hedge_v1",
        label: "真中性 Hedge",
        description: "双向中性微网格。Long/Short 两边独立限仓，适合量够之后压损耗。",
        startable: true,
        kind: "hedge",
        config: {
          strategy_mode: "hedge_neutral",
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 35,
          base_position_notional: 140,
          up_trigger_steps: 6,
          down_trigger_steps: 6,
          shift_steps: 4,
          pause_buy_position_notional: 450,
          pause_short_position_notional: 450,
          max_position_notional: 500,
          max_short_position_notional: 500,
        },
      },
      {
        key: "synthetic_neutral_v1",
        label: "单向合成中性",
        description: "单向持仓下的合成中性微网格。内部维护虚拟 long/short 两本账，适合不方便切 Hedge Mode 的账户。",
        startable: true,
        kind: "synthetic",
        config: {
          strategy_mode: "synthetic_neutral",
          buy_levels: 8,
          sell_levels: 8,
          per_order_notional: 35,
          base_position_notional: 140,
          up_trigger_steps: 6,
          down_trigger_steps: 6,
          shift_steps: 4,
          pause_buy_position_notional: 450,
          pause_short_position_notional: 450,
          max_position_notional: 500,
          max_short_position_notional: 500,
        },
      },
    ];

    let timer = null;
    let paused = false;
    let latestMonitorData = null;
    let monitorLoadPromise = null;
    let strategyActionPending = false;
    let runnerPresets = [];
    let presetsLoaded = false;
    let latestCustomGridPreview = null;
    let monitorSymbols = [];
    let latestRunnerEditorConfig = null;
    const RUNNER_PARAM_EXPLAIN = {
      strategy_profile: "策略模板标识。用于区分量优先做空、做多、防守或自定义策略。",
      strategy_mode: "策略方向/模式。one_way_long 做多，one_way_short 做空，neutral/synthetic 是中性或合成中性。",
      symbol: "当前交易对。",
      step_price: "相邻网格之间的固定价差。越小越贴近盘口、成交更密，但更容易因为手续费和反复换手产生磨损。",
      buy_levels: "买单层数。层数越多，向下承接更深，但资金占用也更高。",
      sell_levels: "卖单层数。层数越多，向上卸仓或开空覆盖更广。",
      per_order_notional: "每笔挂单的目标名义金额（U）。直接决定单笔成交额大小。",
      base_position_notional: "基础持仓目标名义。做多会优先建立底仓，做空则对应基础空仓。",
      center_price: "固定中心价；为空时由运行时根据中价和偏移逻辑动态计算。",
      fixed_center_enabled: "是否启用固定中心价。开启后不会按常规触发逻辑自动迁移中心。",
      fixed_center_roll_enabled: "固定中心启用时，是否允许中心按规则缓慢滚动。",
      excess_inventory_reduce_only_enabled: "库存超标时只允许减仓，不再继续扩仓。",
      autotune_symbol_enabled: "是否允许按币种内置调参规则覆盖当前手动参数。你要精确手动调参时通常建议关闭。",
      up_trigger_steps: "价格向上偏离中心多少格后，触发中心上移。",
      down_trigger_steps: "价格向下偏离中心多少格后，触发中心下移。",
      shift_steps: "每次触发后中心移动多少格。越大越跟价，越小越稳。",
      pause_buy_position_notional: "做多模式下达到该持仓名义后暂停继续买入。",
      pause_short_position_notional: "做空模式下达到该空仓名义后暂停继续开空。",
      max_position_notional: "做多模式总持仓上限。",
      max_short_position_notional: "做空模式总空仓上限。",
      buy_pause_amp_trigger_ratio: "短时间振幅过大时，暂停做多开仓的阈值。",
      buy_pause_down_return_trigger_ratio: "短时间跌幅过大时，暂停做多开仓的阈值。",
      freeze_shift_abs_return_trigger_ratio: "短时波动过大时冻结中心位移，避免不停追价。",
      short_cover_pause_amp_trigger_ratio: "做空模式下，振幅过大时暂停买回补空。",
      short_cover_pause_down_return_trigger_ratio: "做空模式下，快速下跌时暂停追着买回补空。",
      auto_regime_enabled: "是否启用市场状态自动切换。",
      auto_regime_confirm_cycles: "市场状态切换前需要连续满足条件的轮数。",
      auto_regime_stable_15m_max_amplitude_ratio: "稳定行情判定的 15 分钟振幅阈值。",
      auto_regime_stable_60m_max_amplitude_ratio: "稳定行情判定的 60 分钟振幅阈值。",
      auto_regime_stable_60m_return_floor_ratio: "稳定行情判定的 60 分钟最低涨跌幅阈值。",
      auto_regime_defensive_15m_amplitude_ratio: "防守行情判定的 15 分钟振幅阈值。",
      auto_regime_defensive_60m_amplitude_ratio: "防守行情判定的 60 分钟振幅阈值。",
      auto_regime_defensive_15m_return_ratio: "防守行情判定的 15 分钟涨跌幅阈值。",
      auto_regime_defensive_60m_return_ratio: "防守行情判定的 60 分钟涨跌幅阈值。",
      neutral_center_interval_minutes: "中性策略中心重算周期。",
      neutral_band1_offset_ratio: "中性模式第一层带宽偏移比例。",
      neutral_band2_offset_ratio: "中性模式第二层带宽偏移比例。",
      neutral_band3_offset_ratio: "中性模式第三层带宽偏移比例。",
      neutral_band1_target_ratio: "中性模式第一层目标仓位比例。",
      neutral_band2_target_ratio: "中性模式第二层目标仓位比例。",
      neutral_band3_target_ratio: "中性模式第三层目标仓位比例。",
      neutral_hourly_scale_enabled: "是否按小时级市场状态动态缩放中性仓位。",
      neutral_hourly_scale_stable: "稳定行情时的中性仓位缩放系数。",
      neutral_hourly_scale_transition: "过渡行情时的中性仓位缩放系数。",
      neutral_hourly_scale_defensive: "防守行情时的中性仓位缩放系数。",
      inventory_tier_start_notional: "持仓达到该名义后开始进入库存分层控制。",
      inventory_tier_end_notional: "达到该持仓名义后，分层参数完全生效。",
      inventory_tier_buy_levels: "库存分层生效后的买单层数。",
      inventory_tier_sell_levels: "库存分层生效后的卖单层数。",
      inventory_tier_per_order_notional: "库存分层生效后的单笔名义金额。",
      inventory_tier_base_position_notional: "库存分层生效后的基础底仓/基础空仓名义。",
      margin_type: "保证金模式。KEEP 表示沿用账户当前保证金设置。",
      leverage: "杠杆倍数。",
      max_plan_age_seconds: "计划最大允许年龄，超过就不执行，避免使用过旧计划。",
      max_mid_drift_steps: "计划生成到下单之间，允许中价漂移的最大格数。",
      maker_retries: "遇到 post-only 拒单时的重试次数。",
      max_new_orders: "单轮最多新增多少笔挂单。",
      max_total_notional: "单轮新增挂单总名义上限。",
      sleep_seconds: "循环轮询周期。越小越跟价，但撤改单更频繁。",
      run_start_time: "允许开始交易的时间。未到时间前会停止交易、撤策略单并进入清仓逻辑。",
      run_end_time: "允许结束交易的时间。超过时间后会停止交易、撤策略单并进入清仓逻辑。",
      rolling_hourly_loss_limit: "最近 60 分钟滚动亏损阈值。达到后会自动停机、撤单并清仓。",
      max_cumulative_notional: "累计成交额阈值。达到后会自动停机、撤单并清仓。",
      cancel_stale: "是否撤掉与当前目标计划不一致的旧单。",
      apply: "是否真实下单。关闭时仅做 dry-run。",
      reset_state: "启动时是否重置本地状态文件。",
      state_path: "运行状态文件路径。",
      plan_json: "最近一次计划输出 JSON 路径。",
      submit_report_json: "最近一次下单提交报告路径。",
      summary_jsonl: "循环事件日志文件路径。",
      custom_grid_enabled: "是否启用自定义网格模式。",
      custom_grid_roll_enabled: "仅做多静态网格有效。开启后会按时间桶检查成交是否足够活跃、价格是否已明显远离上沿，满足后把整个区间下移。",
      custom_grid_roll_interval_minutes: "条件下移的检查周期。每个时间桶最多检查一次。",
      custom_grid_roll_trade_threshold: "自上次成功下移后，至少累计多少笔成交才允许再次触发。",
      custom_grid_roll_upper_distance_ratio: "当前价格距上沿还剩多少比例的梯子层数时，才允许触发下移。",
      custom_grid_roll_shift_levels: "每次触发时整个静态梯子向下移动多少层。"
    };
    const ALERT_PARAM_KEYS = {
      margin_insufficient: [
        "base_position_notional",
        "per_order_notional",
        "buy_levels",
        "sell_levels",
        "pause_buy_position_notional",
        "pause_short_position_notional",
        "max_position_notional",
        "max_short_position_notional",
        "max_new_orders",
      ],
      rate_limited: [
        "sleep_seconds",
        "max_new_orders",
        "maker_retries",
      ],
      post_only_rejected: [
        "step_price",
        "maker_retries",
        "sleep_seconds",
      ],
      validation_failed: [
        "strategy_mode",
        "margin_type",
        "leverage",
      ],
      buy_paused: [
        "buy_levels",
        "per_order_notional",
        "base_position_notional",
        "pause_buy_position_notional",
        "inventory_tier_buy_levels",
        "inventory_tier_per_order_notional",
        "excess_inventory_reduce_only_enabled",
      ],
      short_paused: [
        "sell_levels",
        "per_order_notional",
        "base_position_notional",
        "pause_short_position_notional",
        "inventory_tier_sell_levels",
        "inventory_tier_per_order_notional",
        "excess_inventory_reduce_only_enabled",
      ],
      buy_cap_applied: [
        "buy_levels",
        "per_order_notional",
        "max_position_notional",
        "max_new_orders",
      ],
      short_cap_applied: [
        "sell_levels",
        "per_order_notional",
        "max_short_position_notional",
        "max_new_orders",
      ],
      volatility_buy_pause: [
        "buy_pause_amp_trigger_ratio",
        "buy_pause_down_return_trigger_ratio",
        "sleep_seconds",
      ],
      shift_frozen: [
        "freeze_shift_abs_return_trigger_ratio",
        "up_trigger_steps",
        "down_trigger_steps",
        "shift_steps",
      ],
    };
    const ALERT_SUGGESTION_LABELS = {
      margin_insufficient: "保证金不足",
      rate_limited: "频率超限",
      post_only_rejected: "Post only 拒单",
      buy_paused: "停买",
      short_paused: "停空",
      buy_cap_applied: "多仓硬上限裁剪",
      short_cap_applied: "空仓硬上限裁剪",
      volatility_buy_pause: "分钟级停买",
      shift_frozen: "冻结中心迁移",
    };

    async function loadMonitorSymbols(preferredSymbol = "") {
      try {
        const resp = await fetch("/api/symbol_lists?list_type=monitor");
        const data = await resp.json();
        if (!resp.ok || !data.ok || !Array.isArray(data.symbols)) {
          throw new Error(data.error || `HTTP ${resp.status}`);
        }
        monitorSymbols = data.symbols.slice();
      } catch (err) {
        monitorSymbols = DEFAULT_MONITOR_SYMBOLS.slice();
      }
      symbolEl.innerHTML = monitorSymbols
        .map((symbol) => `<option value="${escapeHtml(symbol)}">${escapeHtml(symbol)}</option>`)
        .join("");
      const normalizedPreferred = String(preferredSymbol || "").trim().toUpperCase();
      if (normalizedPreferred && monitorSymbols.includes(normalizedPreferred)) {
        symbolEl.value = normalizedPreferred;
      } else if (monitorSymbols.length) {
        symbolEl.value = monitorSymbols[0];
      } else {
        symbolEl.value = "";
      }
    }

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      return Number(v).toLocaleString("zh-CN", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    }

    function fmtMoney(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v);
      const sign = num > 0 ? "+" : "";
      return sign + fmtNum(num, 4);
    }

    function fmtWanVolume(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v);
      const wan = num / 10000;
      if (Math.abs(wan) >= 100) return `${wan.toFixed(0)}万`;
      if (Math.abs(wan) >= 10) return `${wan.toFixed(1)}万`;
      return `${wan.toFixed(2)}万`;
    }

    function formatStartupInventory(summary, quantityDigits = 4, notionalDigits = 4) {
      const direction = String(summary.strategy_direction || "");
      const longQty = Number(summary.startup_long_qty || 0);
      const shortQty = Number(summary.startup_short_qty || 0);
      const netQty = Number(summary.startup_net_qty || 0);
      const longNotional = Number(summary.startup_long_notional || 0);
      const shortNotional = Number(summary.startup_short_notional || 0);
      const netNotional = Number(summary.startup_net_notional || 0);
      if (direction === "long") {
        return `多仓 ${fmtNum(longQty, quantityDigits)} / ${fmtNum(longNotional, notionalDigits)}U`;
      }
      if (direction === "short") {
        return `空仓 ${fmtNum(shortQty, quantityDigits)} / ${fmtNum(shortNotional, notionalDigits)}U`;
      }
      return `多 ${fmtNum(longQty, quantityDigits)} / 空 ${fmtNum(shortQty, quantityDigits)} / 净 ${signedNum(netQty, quantityDigits)} · ${signedNum(netNotional, notionalDigits)}U`;
    }

    function getPresetByKey(key) {
      return (runnerPresets || []).find((item) => item.key === key) || null;
    }

    function getSelectedCustomPreset() {
      const preset = getPresetByKey(strategyPresetEl.value || "");
      return preset && preset.custom ? preset : null;
    }

    function normalizeRunnerEditorConfig(rawConfig, strategyProfile = "") {
      const source = (rawConfig && typeof rawConfig === "object") ? rawConfig : {};
      const config = {};
      Object.entries(source).forEach(([key, value]) => {
        if (value !== undefined) config[key] = value;
      });
      const selectedSymbol = symbolEl.value.trim().toUpperCase() || String(config.symbol || "NIGHTUSDT").toUpperCase();
      config.symbol = selectedSymbol;
      if (strategyProfile && !config.strategy_profile) {
        config.strategy_profile = strategyProfile;
      }
      return normalizeRunnerRuntimePaths(config, selectedSymbol);
    }

    function outputSlugForSymbol(symbol) {
      return String(symbol || "")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "") || "symbol";
    }

    function defaultRunnerRuntimePathsForSymbol(symbol) {
      const slug = outputSlugForSymbol(symbol);
      return {
        state_path: `output/${slug}_loop_state.json`,
        plan_json: `output/${slug}_loop_latest_plan.json`,
        submit_report_json: `output/${slug}_loop_latest_submit.json`,
        summary_jsonl: `output/${slug}_loop_events.jsonl`,
      };
    }

    function normalizeRunnerRuntimePaths(config, symbol) {
      const normalizedSymbol = String(symbol || (config && config.symbol) || "NIGHTUSDT").trim().toUpperCase() || "NIGHTUSDT";
      const runtimePaths = defaultRunnerRuntimePathsForSymbol(normalizedSymbol);
      const normalized = { ...(config || {}), symbol: normalizedSymbol };
      Object.entries(runtimePaths).forEach(([key, expected]) => {
        const rawValue = String(normalized[key] || "").trim();
        if (!rawValue) {
          normalized[key] = expected;
          return;
        }
        if (!rawValue.startsWith("output/") || !rawValue.includes("_loop_")) {
          return;
        }
        if (rawValue !== expected) {
          normalized[key] = expected;
        }
      });
      return normalized;
    }

    async function fetchMonitorSnapshot(symbol, { updateLatest = true } = {}) {
      const normalizedSymbol = String(symbol || "").trim().toUpperCase() || "NIGHTUSDT";
      const resp = await fetch(`/api/loop_monitor?symbol=${encodeURIComponent(normalizedSymbol)}`);
      const data = await resp.json();
      if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
      if (updateLatest) {
        latestMonitorData = data;
      }
      return data;
    }

    function alertParamKeys(code) {
      const normalizedCode = String(code || "").trim();
      return (ALERT_PARAM_KEYS[normalizedCode] || []).slice();
    }

    function alertSupportsSuggestion(code) {
      return Boolean(ALERT_SUGGESTION_LABELS[String(code || "").trim()]);
    }

    function formatAlertCodeLabel(code) {
      return ALERT_SUGGESTION_LABELS[String(code || "").trim()] || String(code || "当前告警");
    }

    function scaleNumber(value, ratio, digits = 4, minValue = null) {
      const num = Number(value);
      if (!Number.isFinite(num)) return value;
      const scaled = num * ratio;
      const clamped = minValue === null ? scaled : Math.max(minValue, scaled);
      return Number(clamped.toFixed(digits));
    }

    function reduceInteger(value, delta, minValue = 1) {
      const num = Number(value);
      if (!Number.isFinite(num)) return value;
      return Math.max(minValue, Math.round(num - delta));
    }

    function readRunnerEditorConfigFromTextarea() {
      const payload = JSON.parse(runnerParamsEditorEl.value || "{}");
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        throw new Error("参数编辑器内容必须是 JSON 对象。");
      }
      const selectedSymbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
      return normalizeRunnerRuntimePaths(payload, selectedSymbol);
    }

    function readRuntimeGuardConfigFromInputs() {
      return {
        run_start_time: fromLocalInputValue(monitorRunStartTimeEl.value),
        run_end_time: fromLocalInputValue(monitorRunEndTimeEl.value),
        rolling_hourly_loss_limit: monitorRollingHourlyLossLimitEl.value ? Number(monitorRollingHourlyLossLimitEl.value) : null,
        max_cumulative_notional: monitorMaxCumulativeNotionalEl.value ? Number(monitorMaxCumulativeNotionalEl.value) : null,
      };
    }

    function syncRuntimeGuardInputsFromConfig(config) {
      const source = (config && typeof config === "object" && !Array.isArray(config)) ? config : {};
      monitorRunStartTimeEl.value = toLocalInputValue(source.run_start_time);
      monitorRunEndTimeEl.value = toLocalInputValue(source.run_end_time);
      monitorRollingHourlyLossLimitEl.value = source.rolling_hourly_loss_limit ?? "";
      monitorMaxCumulativeNotionalEl.value = source.max_cumulative_notional ?? "";
    }

    function mergeRuntimeGuardConfig(payload) {
      const source = (payload && typeof payload === "object" && !Array.isArray(payload)) ? payload : {};
      return {
        ...source,
        ...readRuntimeGuardConfigFromInputs(),
      };
    }

    function syncRuntimeGuardInputsToEditor() {
      let payload = latestRunnerEditorConfig ? { ...latestRunnerEditorConfig } : {};
      try {
        payload = readRunnerEditorConfigFromTextarea();
      } catch (_err) {
      }
      const nextConfig = normalizeRunnerRuntimePaths(
        mergeRuntimeGuardConfig(payload),
        symbolEl.value.trim().toUpperCase() || "NIGHTUSDT"
      );
      latestRunnerEditorConfig = nextConfig;
      runnerParamsEditorEl.value = JSON.stringify(nextConfig, null, 2);
      renderRunnerParamGuide(nextConfig);
      runnerParamsMetaEl.textContent = "运行保护参数已同步到 JSON，可继续修改后应用。";
    }

    async function ensureEditorConfigForAlert() {
      const selectedSymbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
      try {
        const payload = readRunnerEditorConfigFromTextarea();
        const configSymbol = String(payload.symbol || "").trim().toUpperCase();
        if (!configSymbol || configSymbol === selectedSymbol) {
          latestRunnerEditorConfig = payload;
          return payload;
        }
      } catch (_err) {
      }
      await loadRunningConfigToEditor(true);
      return readRunnerEditorConfigFromTextarea();
    }

    function focusRunnerEditorOnKey(key) {
      if (!key) return false;
      const marker = `"${String(key)}"`;
      const text = runnerParamsEditorEl.value || "";
      const idx = text.indexOf(marker);
      runnerParamsEditorEl.focus();
      if (idx < 0) return false;
      runnerParamsEditorEl.setSelectionRange(idx, idx + marker.length);
      const linesBefore = text.slice(0, idx).split("\\n").length - 1;
      runnerParamsEditorEl.scrollTop = Math.max(0, (linesBefore - 2) * 24);
      return true;
    }

    async function locateAlertParams(code) {
      const keys = alertParamKeys(code);
      if (!keys.length) {
        runnerParamsMetaEl.textContent = `告警“${formatAlertCodeLabel(code)}”更偏运行环境或账户状态，没有直接对应的参数可定位。`;
        return;
      }
      const config = await ensureEditorConfigForAlert();
      const primaryKey = keys.find((key) => Object.prototype.hasOwnProperty.call(config, key)) || keys[0];
      const located = focusRunnerEditorOnKey(primaryKey);
      runnerParamsMetaEl.textContent = `已定位“${formatAlertCodeLabel(code)}”相关参数：${keys.join("、")}${located ? "" : "（当前 JSON 里未显式写出首个参数，但建议优先检查它）"}。`;
      renderRunnerParamGuide(config);
    }

    function buildAlertSuggestion(code, currentConfig) {
      const normalizedCode = String(code || "").trim();
      const nextConfig = JSON.parse(JSON.stringify(currentConfig || {}));
      const changedKeys = [];
      const applyChange = (key, value) => {
        if (value === undefined) return;
        if (nextConfig[key] === value) return;
        nextConfig[key] = value;
        changedKeys.push(key);
      };
      if (normalizedCode === "margin_insufficient") {
        applyChange("autotune_symbol_enabled", false);
        applyChange("base_position_notional", scaleNumber(nextConfig.base_position_notional, 0.75, 4, 20));
        applyChange("per_order_notional", scaleNumber(nextConfig.per_order_notional, 0.8, 4, 5));
        applyChange("buy_levels", reduceInteger(nextConfig.buy_levels, 2, 2));
        applyChange("sell_levels", reduceInteger(nextConfig.sell_levels, 1, 2));
        applyChange("pause_buy_position_notional", scaleNumber(nextConfig.pause_buy_position_notional, 0.82, 4, 50));
        applyChange("pause_short_position_notional", scaleNumber(nextConfig.pause_short_position_notional, 0.82, 4, 50));
        applyChange("max_position_notional", scaleNumber(nextConfig.max_position_notional, 0.82, 4, 80));
        applyChange("max_short_position_notional", scaleNumber(nextConfig.max_short_position_notional, 0.82, 4, 80));
        applyChange("max_new_orders", reduceInteger(nextConfig.max_new_orders, 4, 4));
      } else if (normalizedCode === "rate_limited") {
        applyChange("sleep_seconds", Math.max(10, Math.round(Number(nextConfig.sleep_seconds || 5) + 3)));
        applyChange("max_new_orders", reduceInteger(nextConfig.max_new_orders, 4, 4));
        applyChange("maker_retries", reduceInteger(nextConfig.maker_retries, 1, 1));
      } else if (normalizedCode === "post_only_rejected") {
        applyChange("step_price", scaleNumber(nextConfig.step_price, 1.5, 7, 0.0000001));
        applyChange("maker_retries", reduceInteger(nextConfig.maker_retries, 1, 1));
        applyChange("sleep_seconds", Math.max(6, Math.round(Number(nextConfig.sleep_seconds || 5))));
      } else if (normalizedCode === "buy_paused") {
        applyChange("autotune_symbol_enabled", false);
        applyChange("buy_levels", reduceInteger(nextConfig.buy_levels, 2, 2));
        applyChange("per_order_notional", scaleNumber(nextConfig.per_order_notional, 0.88, 4, 5));
        applyChange("base_position_notional", scaleNumber(nextConfig.base_position_notional, 0.9, 4, 20));
        applyChange("inventory_tier_buy_levels", reduceInteger(nextConfig.inventory_tier_buy_levels, 1, 1));
        applyChange("inventory_tier_per_order_notional", scaleNumber(nextConfig.inventory_tier_per_order_notional, 0.88, 4, 5));
        applyChange("excess_inventory_reduce_only_enabled", true);
      } else if (normalizedCode === "short_paused") {
        applyChange("autotune_symbol_enabled", false);
        applyChange("sell_levels", reduceInteger(nextConfig.sell_levels, 2, 2));
        applyChange("per_order_notional", scaleNumber(nextConfig.per_order_notional, 0.88, 4, 5));
        applyChange("base_position_notional", scaleNumber(nextConfig.base_position_notional, 0.9, 4, 20));
        applyChange("inventory_tier_sell_levels", reduceInteger(nextConfig.inventory_tier_sell_levels, 2, 2));
        applyChange("inventory_tier_per_order_notional", scaleNumber(nextConfig.inventory_tier_per_order_notional, 0.88, 4, 5));
        applyChange("excess_inventory_reduce_only_enabled", true);
      } else if (normalizedCode === "buy_cap_applied") {
        applyChange("buy_levels", reduceInteger(nextConfig.buy_levels, 1, 2));
        applyChange("per_order_notional", scaleNumber(nextConfig.per_order_notional, 0.92, 4, 5));
        applyChange("max_new_orders", reduceInteger(nextConfig.max_new_orders, 2, 4));
      } else if (normalizedCode === "short_cap_applied") {
        applyChange("sell_levels", reduceInteger(nextConfig.sell_levels, 1, 2));
        applyChange("per_order_notional", scaleNumber(nextConfig.per_order_notional, 0.92, 4, 5));
        applyChange("max_new_orders", reduceInteger(nextConfig.max_new_orders, 2, 4));
      } else if (normalizedCode === "volatility_buy_pause") {
        applyChange("buy_pause_amp_trigger_ratio", scaleNumber(nextConfig.buy_pause_amp_trigger_ratio, 1.15, 6, 0.0001));
        const downReturn = Number(nextConfig.buy_pause_down_return_trigger_ratio);
        if (Number.isFinite(downReturn) && downReturn < 0) {
          applyChange("buy_pause_down_return_trigger_ratio", Number((downReturn * 1.15).toFixed(6)));
        }
        applyChange("sleep_seconds", Math.max(6, Math.round(Number(nextConfig.sleep_seconds || 5) + 1)));
      } else if (normalizedCode === "shift_frozen") {
        applyChange("freeze_shift_abs_return_trigger_ratio", scaleNumber(nextConfig.freeze_shift_abs_return_trigger_ratio, 1.15, 6, 0.0001));
        applyChange("shift_steps", Math.max(1, Math.round(Number(nextConfig.shift_steps || 1))));
      } else {
        return null;
      }
      return { config: normalizeRunnerRuntimePaths(nextConfig, nextConfig.symbol || symbolEl.value), changedKeys };
    }

    async function applyAlertSuggestion(code) {
      if (!alertSupportsSuggestion(code)) {
        runnerParamsMetaEl.textContent = `告警“${formatAlertCodeLabel(code)}”暂时没有安全的通用建议参数，请先手动检查当前配置。`;
        return;
      }
      const config = await ensureEditorConfigForAlert();
      const suggestion = buildAlertSuggestion(code, config);
      if (!suggestion || !suggestion.changedKeys.length) {
        runnerParamsMetaEl.textContent = `告警“${formatAlertCodeLabel(code)}”当前没有可自动生成的建议参数。`;
        return;
      }
      setRunnerEditorConfig(
        suggestion.config,
        `已按“${formatAlertCodeLabel(code)}”生成建议参数：${suggestion.changedKeys.join("、")}。请检查后再点“应用参数并启动”。`
      );
      focusRunnerEditorOnKey(suggestion.changedKeys[0]);
    }

    const RUNNER_PROFILE_GUIDE_NOTES = {
      volume_long_v4: {
        summary: "偏多滚动微网格，目标是让盘口附近不断有承接买单和减仓卖单，优先把成交密度做高。",
        focus: [
          "这不是中性策略，本质上仍然依赖多仓库存来提供上方卖单。",
          "更适合稳定或偏强市场；遇到持续下跌时，会靠停买和库存分层减轻继续接仓。"
        ],
      },
      volatility_defensive_v1: {
        summary: "同样是 one_way_long，但把底仓、单笔和停买阈值都压低，优先控制回撤而不是冲量。",
        focus: [
          "上方卖单更多、下方买单更少，意味着反弹时更快卸仓，回落时更慢继续接。",
        ],
      },
      adaptive_volatility_v1: {
        summary: "这不是固定参数集。运行时会先判定市场状态，再在 volume_long_v4 和 volatility_defensive_v1 两套参数之间切换。",
        focus: [
          "当 15m/60m 振幅或跌幅触发防守条件时，会连续确认后切换到防守档；稳定后再切回量优先档。",
        ],
      },
      bard_volume_long_v2: {
        summary: "BARDUSDT 的量优先做多 v2。核心不是简单把底仓调轻，而是把启动阶段拆成 flat-start 和 warm-start 两层保护。",
        focus: [
          "如果账户里还有反向仓位或遗留挂单，启动会直接拒绝，不再把旧状态硬吞进新策略。",
          "如果启动时已经有同向多仓，首轮会先禁掉 bootstrap，只让网格顺着现有库存继续运转。",
        ],
      },
      xaut_long_adaptive_v1: {
        summary: "XAUT 专用三态做多。除了 normal / defensive 外，还会在极端下跌或扩振时进入 reduce_only，立即撤买单，只保留卖单减仓。",
        focus: [
          "这套只允许在 XAUTUSDT 上使用，目的是把高波动时的继续接仓速度压到最低。",
        ],
      },
      xaut_short_adaptive_v1: {
        summary: "XAUT 专用三态做空。极端上冲或扩振时进入 reduce_only，立即撤卖单，只保留买单回补减空仓。",
        focus: [
          "结构上镜像 xaut_long_adaptive_v1，但风险触发方向反过来。",
        ],
      },
      volume_short_v1: {
        summary: "one_way_short 镜像网格。上方卖单负责开空，下方买单负责回补，适合弱势或冲高回落窗口。",
        focus: [
          "买单只用于平已有空仓，不会反手开多。",
        ],
      },
      volume_short_v1_aggressive: {
        summary: "在 volume_short_v1 的基础上强调换手，接受更高的反复挂单频率来换取更多成交。",
        focus: [
          "更适合方向明确的弱势段；横盘里会更容易被手续费磨损。",
        ],
      },
      night_volume_short_v1: {
        summary: "针对 NIGHTUSDT 的高换手做空版。仍然偏快，但比原先明显收了单笔、底仓和总空仓，并加入急跌停回补保护。",
        focus: [
          "比通用空头版仍然更快移中心、更快轮询，但不会像之前那样一边放大仓位一边追着回补。",
        ],
      },
      volume_short_v1_conservative: {
        summary: "保守型做空网格。底仓和单笔更轻，上移中心更慢，先保证空头库存不要扩太快。",
        focus: [
          "适合先试空或高波动震荡段，不适合把量推到极限。",
        ],
      },
      xaut_volume_short_v1: {
        summary: "XAUTUSDT 的专用空头高换手方案。仍然是 one_way_short，但把空仓上限钉在 1000 USDT，并把自动步长压到更紧的 0.017%。",
        focus: [
          "目标是把小时成交额拉到 1 万附近，所以更依赖 XAUT 自身短周期波动；如果行情突然变钝，量会明显回落。",
        ],
      },
      defensive_quasi_neutral_aggressive_v1: {
        summary: "名字叫准中性，但实现上仍然是 one_way_long。所谓“准中性”是通过少买、多卖、轻底仓来削弱方向偏置。",
        focus: [
          "卖侧层数远多于买侧，目的是已有多仓一旦反弹就更快拆掉，同时保留一定成交量。",
        ],
      },
      defensive_quasi_neutral_v1: {
        summary: "one_way_long 的降损版本。和激进版相比更轻仓、更慢，适合量已经够但想压损耗。",
        focus: [
          "同样不是真中性，只是比量优先做多更少接、更多卖。",
        ],
      },
      volume_neutral_target_v1: {
        summary: "单向账户里的目标净仓中性策略。不是传统一格一格的网格，而是按离中心的偏移带直接指定净仓目标。",
        focus: [
          "更像“仓位曲线执行器”：价格偏下就逐步做净多，价格偏上就逐步做净空。",
        ],
      },
      neutral_hedge_v1: {
        summary: "双向持仓模式下的真中性策略。LONG 和 SHORT 两条腿各自有独立的开仓/止盈网格。",
        focus: [
          "这套要求账户本身是 hedge mode，否则提交器会直接拒绝。",
        ],
      },
      synthetic_neutral_v1: {
        summary: "单向账户里的合成中性。内部维护一套虚拟 long/short 账本，再把双边计划折成单向实际委托。",
        focus: [
          "优点是不需要切 hedge mode；代价是实际净仓和虚拟账本需要持续对齐。",
        ],
      },
    };
    const AUTOTUNE_STEP_HINTS = {
      volume_long_v4: { stepRatio: 0.0004, minTicks: 2 },
      volatility_defensive_v1: { stepRatio: 0.0008, minTicks: 4 },
      xaut_volume_short_v1: { stepRatio: 0.00017, minTicks: 2 },
      volume_neutral_target_v1: { stepRatio: 0.0006, minTicks: 3 },
      neutral_hedge_v1: { stepRatio: 0.0005, minTicks: 3 },
      synthetic_neutral_v1: { stepRatio: 0.0005, minTicks: 3 },
    };

    function asGuideNumber(value) {
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    }

    function fmtGuideNumber(value, digits = 4) {
      const num = asGuideNumber(value);
      if (num === null) return "--";
      return num.toLocaleString("zh-CN", {
        minimumFractionDigits: 0,
        maximumFractionDigits: digits,
      });
    }

    function fmtGuidePrice(value) {
      return fmtGuideNumber(value, 7);
    }

    function fmtGuideNotional(value) {
      return `${fmtGuideNumber(value, 4)}U`;
    }

    function fmtGuidePctFromRatio(value, digits = 2) {
      const num = asGuideNumber(value);
      if (num === null) return "--";
      return `${fmtGuideNumber(num * 100, digits)}%`;
    }

    function guideStepMoveText(stepPrice, steps, centerPrice = null) {
      const step = asGuideNumber(stepPrice);
      const count = Math.max(Number(steps) || 0, 0);
      if (!(step > 0) || count <= 0) return "未配置";
      const move = step * count;
      let text = `${count} 格（${count} × ${fmtGuidePrice(step)} = ${fmtGuidePrice(move)}）`;
      const center = asGuideNumber(centerPrice);
      if (center && center > 0) {
        text += `，约中心价的 ${fmtGuidePctFromRatio(move / center)}`;
      }
      return text;
    }

    function guideHtmlList(items) {
      const rows = (items || []).filter(Boolean);
      if (!rows.length) return "";
      return `
        <ul class="strategy-guide-list">
          ${rows.map((item) => `<li>${escapeHtml(String(item))}</li>`).join("")}
        </ul>
      `;
    }

    function guideHtmlSection(title, items) {
      const rows = (items || []).filter(Boolean);
      if (!rows.length) return "";
      return `
        <section class="strategy-guide-section">
          <h4>${escapeHtml(title)}</h4>
          ${guideHtmlList(rows)}
        </section>
      `;
    }

    function getGuidePreset(config) {
      const profile = String((config || {}).strategy_profile || "").trim();
      return getPresetByKey(profile) || null;
    }

    function getGuideMode(config, preset = null) {
      return String(
        (config && config.strategy_mode)
        || (preset && preset.config && preset.config.strategy_mode)
        || "one_way_long"
      ).trim() || "one_way_long";
    }

    function buildModeExecutionLines(config, mode) {
      const stepPrice = asGuideNumber(config.step_price);
      const buyLevels = Math.max(Number(config.buy_levels) || 0, 0);
      const sellLevels = Math.max(Number(config.sell_levels) || 0, 0);
      const perOrderNotional = asGuideNumber(config.per_order_notional);
      const basePositionNotional = Math.max(asGuideNumber(config.base_position_notional) || 0, 0);
      if (mode === "one_way_short") {
        return [
          `以上下方运行时中心价为轴，向上每隔 ${fmtGuidePrice(stepPrice)} 挂 ${sellLevels} 层卖单开空，向下每隔 ${fmtGuidePrice(stepPrice)} 挂 ${buyLevels} 层买单回补空仓。`,
          `单笔目标名义 ${fmtGuideNotional(perOrderNotional)}；如果当前空仓低于基础空仓 ${fmtGuideNotional(basePositionNotional)}，会先在卖一附近挂 bootstrap 卖单补到基础空仓。`,
          "买单只按现有空仓数量生成，作用是回补，不会因为买单而反手开多。",
        ];
      }
      if (mode === "hedge_neutral") {
        return [
          `LONG 腿会在中心下方每隔 ${fmtGuidePrice(stepPrice)} 挂 ${buyLevels} 层买单、中心上方挂 ${sellLevels} 层卖单；SHORT 腿会镜像再做一套。`,
          `两边都以单笔 ${fmtGuideNotional(perOrderNotional)} 运转；如果 LONG / SHORT 任一边低于基础仓位 ${fmtGuideNotional(basePositionNotional)}，都会各自补 bootstrap 单。`,
          "这套是双腿独立运行，LONG 的止盈不会替 SHORT 平仓，反之亦然。",
        ];
      }
      if (mode === "synthetic_neutral") {
        return [
          `内部先按双向 hedge 网格生成计划：下方买入视为补 LONG、上方卖出视为补 SHORT，再把两边订单折算成单向账户可提交的委托。`,
          `每边的目标单笔仍是 ${fmtGuideNotional(perOrderNotional)}，基础仓位按 ${fmtGuideNotional(basePositionNotional)} 维护，但仓位判断依赖虚拟 long/short 账本而不是账户原生双向持仓。`,
          "如果实际净仓和虚拟账本偏离，runner 会先同步账本，再继续按 synthetic 计划下单。",
        ];
      }
      if (mode === "inventory_target_neutral") {
        return [
          `这套不是传统 buy_levels / sell_levels 网格，而是每隔 ${Math.max(Number(config.neutral_center_interval_minutes) || 0, 0)} 分钟取最新闭合 K 线收盘价做中心。`,
          `价格跌到中心下方的 0.5% / 1% / 2% 时，会把目标净多仓逐步拉到最大多仓的 ${fmtGuidePctFromRatio(config.neutral_band1_target_ratio)} / ${fmtGuidePctFromRatio(config.neutral_band2_target_ratio)} / ${fmtGuidePctFromRatio(config.neutral_band3_target_ratio)}；涨到上方时对称转成净空。`,
          `如果当前净仓和“此刻目标净仓”不一致，会先在买一或卖一附近挂 bootstrap 单把净仓拉向目标，再在三档带宽位置挂后续单。`,
        ];
      }
      return [
        `以上下方运行时中心价为轴，向下每隔 ${fmtGuidePrice(stepPrice)} 挂 ${buyLevels} 层买单，向上每隔 ${fmtGuidePrice(stepPrice)} 挂 ${sellLevels} 层卖单。`,
        `单笔目标名义 ${fmtGuideNotional(perOrderNotional)}；如果当前多仓低于基础底仓 ${fmtGuideNotional(basePositionNotional)}，会先在买一附近补 bootstrap 买单。`,
        "卖单只按现有多仓数量生成，作用是减仓/止盈，不会为了卖出而主动开空。",
      ];
    }

    function buildCenterBehaviorLines(config, mode) {
      const centerPrice = asGuideNumber(config.center_price);
      const stepPrice = asGuideNumber(config.step_price);
      if (config.custom_grid_enabled) {
        return [
          "当前 JSON 已启用 custom_grid_enabled，中心价来自你定义的固定网格区间中点，不再按普通微网格触发规则移动。",
        ];
      }
      if (config.fixed_center_enabled) {
        const lines = [
          centerPrice && centerPrice > 0
            ? `当前启用了固定中心价，中心直接锁在 ${fmtGuidePrice(centerPrice)}，不会再按 up/down trigger 自动追价。`
            : "当前启用了固定中心价；如果 JSON 里没显式写 center_price，实际会沿用状态文件中的固定中心。",
        ];
        if (config.fixed_center_roll_enabled) {
          const triggerSteps = asGuideNumber(config.fixed_center_roll_trigger_steps) || 1;
          const confirmCycles = Math.max(Number(config.fixed_center_roll_confirm_cycles) || 3, 1);
          const shiftSteps = Math.max(Number(config.fixed_center_roll_shift_steps) || 1, 1);
          lines.push(
            `虽然中心固定，但已开启 fixed_center_roll：同方向偏离累计 ${guideStepMoveText(stepPrice, triggerSteps, centerPrice)} 并连续确认 ${confirmCycles} 轮后，中心再滚动 ${guideStepMoveText(stepPrice, shiftSteps, centerPrice)}。`
          );
        }
        return lines;
      }
      if (mode === "inventory_target_neutral") {
        const lines = [
          `目标仓位中性策略直接使用最近一根闭合 ${Math.max(Number(config.neutral_center_interval_minutes) || 0, 0)} 分钟 K 线收盘价作为新中心，不使用 up/down trigger / shift 迁移规则。`,
        ];
        if (asGuideNumber(config.freeze_shift_abs_return_trigger_ratio) > 0) {
          lines.push(
            `freeze_shift_abs_return_trigger_ratio=${fmtGuidePctFromRatio(config.freeze_shift_abs_return_trigger_ratio)} 在这个模式里不负责中心迁移，因为中心重算完全走定时 K 线。`
          );
        }
        return lines;
      }
      const lines = [
        `当中价相对中心上涨达到 ${guideStepMoveText(stepPrice, config.up_trigger_steps, centerPrice)} 时，中心会上移 ${guideStepMoveText(stepPrice, config.shift_steps, centerPrice)}；下跌达到 ${guideStepMoveText(stepPrice, config.down_trigger_steps, centerPrice)} 时，中心下移同样的步长。`,
        "这不是一次只挪一档；同一轮里会持续移动，直到中价重新回到触发带内。",
      ];
      if (asGuideNumber(config.freeze_shift_abs_return_trigger_ratio) > 0) {
        lines.push(
          `如果最近 1 分钟绝对涨跌达到 ${fmtGuidePctFromRatio(config.freeze_shift_abs_return_trigger_ratio)}，这一轮会冻结中心迁移，避免在急拉急杀里不停追价。`
        );
      }
      return lines;
    }

    function buildInventoryTierLines(config, mode) {
      const start = asGuideNumber(config.inventory_tier_start_notional);
      if (!(start > 0)) {
        return ["当前未启用库存分层，挂单层数、单笔名义和基础仓位会一直保持这一份 JSON 的主参数。"]; 
      }
      const end = Math.max(asGuideNumber(config.inventory_tier_end_notional) || start, start);
      const currentLabel = mode === "one_way_short" ? "空仓名义" : "持仓名义";
      return [
        `当${currentLabel}从 ${fmtGuideNotional(start)} 增加到 ${fmtGuideNotional(end)} 之间时，runner 会线性过渡参数，而不是一步切换。`,
        `买层 ${Math.max(Number(config.buy_levels) || 0, 0)} -> ${Math.max(Number(config.inventory_tier_buy_levels) || 0, 0)}，卖层 ${Math.max(Number(config.sell_levels) || 0, 0)} -> ${Math.max(Number(config.inventory_tier_sell_levels) || 0, 0)}。`,
        `单笔名义 ${fmtGuideNotional(config.per_order_notional)} -> ${fmtGuideNotional(config.inventory_tier_per_order_notional)}，基础仓位 ${fmtGuideNotional(config.base_position_notional)} -> ${fmtGuideNotional(config.inventory_tier_base_position_notional)}。`,
      ];
    }

    function buildPauseAndCapLines(config, mode) {
      const lines = [];
      const buyPause = asGuideNumber(config.pause_buy_position_notional);
      const shortPause = asGuideNumber(config.pause_short_position_notional);
      const maxLong = asGuideNumber(config.max_position_notional);
      const maxShort = asGuideNumber(config.max_short_position_notional);
      if (mode === "one_way_short") {
        if (shortPause > 0) {
          lines.push(`当空仓名义达到 ${fmtGuideNotional(shortPause)} 时，会清掉 bootstrap_short 和新的卖单，只保留买回补空单。`);
        }
        if (maxShort > 0) {
          lines.push(`如果这一轮计划中的新增空仓会把总空仓推过 ${fmtGuideNotional(maxShort)}，多出来的卖单会被裁剪，不会整轮直接放行。`);
        }
        if (asGuideNumber(config.short_cover_pause_amp_trigger_ratio) > 0 && asGuideNumber(config.short_cover_pause_down_return_trigger_ratio) !== null) {
          lines.push(
            `最近 1 分钟如果同时满足“振幅 >= ${fmtGuidePctFromRatio(config.short_cover_pause_amp_trigger_ratio)}”且“收跌 <= ${fmtGuidePctFromRatio(config.short_cover_pause_down_return_trigger_ratio)}”，BUY 回补单会被暂停，避免在急跌里过早回补。`
          );
        }
      } else if (mode === "hedge_neutral" || mode === "synthetic_neutral" || mode === "inventory_target_neutral") {
        if (buyPause > 0) {
          lines.push(`LONG 侧名义达到 ${fmtGuideNotional(buyPause)} 时，LONG 开仓买单会被清掉，只保留 LONG 卖出减仓单。`);
        }
        if (shortPause > 0) {
          lines.push(`SHORT 侧名义达到 ${fmtGuideNotional(shortPause)} 时，SHORT 开仓卖单会被清掉，只保留 SHORT 买回减仓单。`);
        }
        if (maxLong > 0) {
          lines.push(`LONG 侧新增计划会被裁剪到总多仓不超过 ${fmtGuideNotional(maxLong)}。`);
        }
        if (maxShort > 0) {
          lines.push(`SHORT 侧新增计划会被裁剪到总空仓不超过 ${fmtGuideNotional(maxShort)}。`);
        }
      } else {
        if (buyPause > 0) {
          lines.push(`当多仓名义达到 ${fmtGuideNotional(buyPause)} 时，会清掉 bootstrap 和所有买单，只保留上方卖单卸仓。`);
        }
        if (maxLong > 0) {
          lines.push(`如果这一轮计划中的新增买单会把总多仓推过 ${fmtGuideNotional(maxLong)}，买单会按剩余额度裁剪，而不是整轮全部撤掉。`);
        }
      }
      if (asGuideNumber(config.min_mid_price_for_buys) > 0) {
        lines.push(`当中价跌到 ${fmtGuidePrice(config.min_mid_price_for_buys)} 以下时，LONG 开仓买单会被整体暂停。`);
      }
      if (asGuideNumber(config.buy_pause_amp_trigger_ratio) > 0 && asGuideNumber(config.buy_pause_down_return_trigger_ratio) !== null) {
        lines.push(
          `最近 1 分钟如果同时满足“振幅 >= ${fmtGuidePctFromRatio(config.buy_pause_amp_trigger_ratio)}”且“收跌 <= ${fmtGuidePctFromRatio(config.buy_pause_down_return_trigger_ratio)}”，LONG 开仓买单会被暂停。`
        );
      }
      if (config.excess_inventory_reduce_only_enabled) {
        if (mode === "one_way_short") {
          lines.push("已启用 excess_inventory_reduce_only：如果现有空仓已经高于目标基础空仓，会清掉新的卖单，只允许继续买回减仓。");
        } else if (mode === "one_way_long") {
          lines.push("已启用 excess_inventory_reduce_only：如果现有多仓已经高于目标底仓，会清掉新的买单，只允许继续卖出减仓。");
        }
      }
      return lines;
    }

    function buildExecutionGuardLines(config) {
      const lines = [
        `每 ${fmtGuideNumber(config.sleep_seconds, 2)} 秒重算一次目标计划，再和当前挂单做 diff。`,
      ];
      if (config.run_start_time) {
        lines.push(`run_start_time=${fmtTs(config.run_start_time)} 之前不会继续交易；如果当前已有仓位或挂单，会先撤策略单并转入清仓。`);
      }
      if (config.run_end_time) {
        lines.push(`run_end_time=${fmtTs(config.run_end_time)} 之后会自动停止交易、撤策略单并转入清仓。`);
      }
      if (asGuideNumber(config.rolling_hourly_loss_limit) > 0) {
        lines.push(`最近 60 分钟滚动亏损达到 ${fmtGuideNotional(config.rolling_hourly_loss_limit)} 时，会直接停机并执行撤单清仓。`);
      }
      if (asGuideNumber(config.max_cumulative_notional) > 0) {
        lines.push(`累计成交额达到 ${fmtGuideNotional(config.max_cumulative_notional)} 时，会直接停机并执行撤单清仓。`);
      }
      if (config.cancel_stale === false) {
        lines.push("当前 cancel_stale=false：如果新计划和旧挂单不一致，提交器会直接拒绝执行，而不是替你撤单。");
      } else {
        lines.push("当前 cancel_stale=true：如果某个价位/方向不再需要，会撤掉旧单；但同价位总量不变时会保留原单。");
      }
      lines.push("同价位同方向的单子现在按“价位桶”比较：目标总量增加时只补差额，不会为了补量把原排队单撤掉重挂。");
      lines.push("只有当同价位目标总量减少，或者这个价位整个被新计划删除时，旧单才会被判成 stale 并撤掉。");
      if (asGuideNumber(config.max_plan_age_seconds) > 0) {
        lines.push(`计划生成后超过 ${fmtGuideNumber(config.max_plan_age_seconds, 0)} 秒就不提交，防止旧计划继续下单。`);
      }
      if (asGuideNumber(config.max_mid_drift_steps) !== null) {
        lines.push(`如果实时中价相对计划生成时漂移超过 ${fmtGuideNumber(config.max_mid_drift_steps, 2)} 格，这一轮会拒绝提交，避免计划和盘口偏离太大。`);
      }
      if (asGuideNumber(config.maker_retries) !== null) {
        lines.push(`post-only 被交易所拒单时，最多会按 maker_retries=${fmtGuideNumber(config.maker_retries, 0)} 再尝试调价重提。`);
      }
      return lines;
    }

    function buildAutotuneAndSpecialNotes(config, preset, mode) {
      const lines = [];
      const profileKey = String((config || {}).strategy_profile || (preset && preset.key) || "").trim();
      const autotuneEnabled = Boolean(config.autotune_symbol_enabled);
      if (autotuneEnabled) {
        const hint = AUTOTUNE_STEP_HINTS[profileKey] || AUTOTUNE_STEP_HINTS.volume_long_v4;
        lines.push(
          `启动前 web 端会按币种自动校准步长：step_price 至少取 max(中价 × ${fmtGuidePctFromRatio(hint.stepRatio, 4)}, 最小 tick × ${hint.minTicks}, 当前点差 × 2)。`
        );
        lines.push("如果 per_order_notional 太小，不够满足交易所最小成交额/最小下单量，也会被自动抬高；base_position_notional 至少会被抬到两笔单笔名义。");
      } else if ("autotune_symbol_enabled" in (config || {})) {
        lines.push("当前已关闭 autotune_symbol_enabled：启动时不会再按币种自动改步长和单笔名义，当前 JSON 基本会原样执行。");
      }

      if (Boolean(config.auto_regime_enabled) && mode === "one_way_long") {
        lines.push(
          `auto_regime 已开启：15m 振幅 <= ${fmtGuidePctFromRatio(config.auto_regime_stable_15m_max_amplitude_ratio)} 且 60m 振幅 <= ${fmtGuidePctFromRatio(config.auto_regime_stable_60m_max_amplitude_ratio)}、60m 涨跌 >= ${fmtGuidePctFromRatio(config.auto_regime_stable_60m_return_floor_ratio)} 时，视为 stable。`
        );
        lines.push(
          `15m 振幅 >= ${fmtGuidePctFromRatio(config.auto_regime_defensive_15m_amplitude_ratio)}、60m 振幅 >= ${fmtGuidePctFromRatio(config.auto_regime_defensive_60m_amplitude_ratio)}，或 15m / 60m 跌幅分别 <= ${fmtGuidePctFromRatio(config.auto_regime_defensive_15m_return_ratio)} / ${fmtGuidePctFromRatio(config.auto_regime_defensive_60m_return_ratio)} 时，候选切到 defensive。`
        );
        lines.push(`候选状态连续满足 ${Math.max(Number(config.auto_regime_confirm_cycles) || 1, 1)} 轮后，才会真正切换到另一套参数，避免来回抖动。`);
      }

      if (mode === "inventory_target_neutral") {
        lines.push("注意：inventory_target_neutral 当前真正生效的是 neutral_center_interval_minutes、三档 band offset / target ratio、max_position_notional / max_short_position_notional 和 neutral_hourly_scale。");
        lines.push("buy_levels、sell_levels、per_order_notional、base_position_notional、up_trigger_steps、down_trigger_steps、shift_steps 在这个模式里不会决定实际挂单位置。");
      }

      return lines;
    }

    function renderRunnerParamGuide(config) {
      const source = (config && typeof config === "object" && !Array.isArray(config)) ? config : {};
      if (!Object.keys(source).length) {
        runnerParamsGuideBodyEl.innerHTML = '<div class="strategy-guide-empty">先载入参数，右侧再显示当前策略的执行说明。</div>';
        return;
      }
      const preset = getGuidePreset(source);
      const profileKey = String(source.strategy_profile || (preset && preset.key) || "").trim();
      const presetNote = RUNNER_PROFILE_GUIDE_NOTES[profileKey] || null;
      const mode = getGuideMode(source, preset);
      const title = (preset && preset.label) || profileKey || mode;
      const lead = (presetNote && presetNote.summary) || (preset && preset.description) || "当前策略没有单独的预设说明，以下内容按真实执行模式和当前 JSON 推导。";
      const metaPills = [
        `profile: ${profileKey || "未指定"}`,
        `mode: ${mode}`,
        `symbol: ${String(source.symbol || symbolEl.value || "-").toUpperCase()}`,
        `轮询: ${fmtGuideNumber(source.sleep_seconds, 2)}s`,
      ];
      const summarySections = [
        guideHtmlSection("策略定位", (presetNote && presetNote.focus) || []),
        guideHtmlSection("这份参数会怎样下单", buildModeExecutionLines(source, mode)),
        guideHtmlSection("中心价如何移动", buildCenterBehaviorLines(source, mode)),
        guideHtmlSection("何时暂停、减仓或限额", buildPauseAndCapLines(source, mode)),
        guideHtmlSection("库存分层如何改网格", buildInventoryTierLines(source, mode)),
        guideHtmlSection("撤单、补单和执行保护", buildExecutionGuardLines(source)),
        guideHtmlSection("自动调参 / 特殊注意", buildAutotuneAndSpecialNotes(source, preset, mode)),
      ].filter(Boolean).join("");

      runnerParamsGuideBodyEl.innerHTML = `
        <div class="strategy-guide-card">
          <h3>${escapeHtml(title)}</h3>
          <div class="strategy-guide-meta">
            ${metaPills.map((item) => `<span class="strategy-guide-pill">${escapeHtml(item)}</span>`).join("")}
          </div>
          <p class="strategy-guide-lead">${escapeHtml(lead)}</p>
          ${summarySections}
          <div class="strategy-guide-section">
            <div class="strategy-guide-callout">
              <p class="strategy-guide-note">详细版文档已经落到仓库 <code>docs/STRATEGY_EXECUTION_GUIDE.md</code> 文件。右侧说明只解释当前这一份 JSON 真正会怎样运行。</p>
            </div>
          </div>
        </div>
      `;
    }

    function syncRunnerParamGuideFromEditor() {
      try {
        const payload = JSON.parse(runnerParamsEditorEl.value || "{}");
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
          throw new Error("参数编辑器内容必须是 JSON 对象");
        }
        latestRunnerEditorConfig = normalizeRunnerRuntimePaths(payload, symbolEl.value.trim().toUpperCase() || "NIGHTUSDT");
        syncRuntimeGuardInputsFromConfig(latestRunnerEditorConfig);
        renderRunnerParamGuide(latestRunnerEditorConfig);
      } catch (err) {
        runnerParamsGuideBodyEl.innerHTML = `
          <div class="strategy-guide-empty">JSON 解析失败：${escapeHtml(String(err))}</div>
        `;
      }
    }

    function setRunnerEditorConfig(rawConfig, sourceLabel = "") {
      latestRunnerEditorConfig = normalizeRunnerEditorConfig(rawConfig, rawConfig && rawConfig.strategy_profile ? String(rawConfig.strategy_profile) : "");
      runnerParamsEditorEl.value = JSON.stringify(latestRunnerEditorConfig, null, 2);
      syncRuntimeGuardInputsFromConfig(latestRunnerEditorConfig);
      runnerParamsMetaEl.textContent = sourceLabel || "参数已载入，可直接修改 JSON 后应用。";
      renderRunnerParamGuide(latestRunnerEditorConfig);
    }

    async function loadRunningConfigToEditor(forceRefresh = true) {
      const selectedSymbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
      runnerParamsMetaEl.textContent = `正在载入 ${selectedSymbol} 的运行参数...`;
      let monitorData = latestMonitorData;
      if (
        forceRefresh
        || !monitorData
        || String((monitorData || {}).symbol || "").trim().toUpperCase() !== selectedSymbol
      ) {
        try {
          monitorData = await fetchMonitorSnapshot(selectedSymbol);
          populatePresetOptions(monitorData);
        } catch (err) {
          runnerParamsMetaEl.textContent = `载入失败: ${err}`;
          return;
        }
      }
      const runnerConfig = ((((monitorData || {}).runner || {}).config) || {});
      if (!runnerConfig || !Object.keys(runnerConfig).length) {
        runnerParamsMetaEl.textContent = "当前没有可载入的运行参数，请先启动一次策略。";
        return;
      }
      const configSymbol = String(runnerConfig.symbol || "").trim().toUpperCase();
      if (configSymbol && configSymbol !== selectedSymbol) {
        runnerParamsMetaEl.textContent = `载入失败: 返回了 ${configSymbol} 的配置，当前页面选中的是 ${selectedSymbol}`;
        return;
      }
      const profile = String(runnerConfig.strategy_profile || "");
      setRunnerEditorConfig(
        normalizeRunnerRuntimePaths({ ...runnerConfig, ...(profile ? { strategy_profile: profile } : {}) }, selectedSymbol),
        `已载入 ${selectedSymbol} 的运行参数。`
      );
    }

    function loadPresetConfigToEditor() {
      const preset = getPresetByKey(strategyPresetEl.value || "");
      if (!preset) {
        runnerParamsMetaEl.textContent = "请先选择一个策略预设。";
        return;
      }
      setRunnerEditorConfig(
        { ...((preset && preset.config) || {}), strategy_profile: preset.key },
        `已载入预设 ${preset.label}，可按当前交易对继续修改。`
      );
    }

    async function applyRunnerParams() {
      const selectedPreset = getPresetByKey(strategyPresetEl.value);
      const fallbackProfile = selectedPreset
        ? selectedPreset.key
        : String((((latestMonitorData || {}).runner || {}).config || {}).strategy_profile || "volume_long_v4");
      let payload;
      try {
        payload = readRunnerEditorConfigFromTextarea();
      } catch (err) {
        runnerParamsMetaEl.textContent = `JSON 解析失败: ${err}`;
        return;
      }
      const selectedSymbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
      payload = mergeRuntimeGuardConfig(payload);
      payload.symbol = selectedSymbol;
      if (!payload.strategy_profile) {
        payload.strategy_profile = fallbackProfile;
      }
      applyParamsBtn.disabled = true;
      loadRunningParamsBtn.disabled = true;
      loadPresetParamsBtn.disabled = true;
      runnerParamsMetaEl.textContent = `正在应用 ${selectedSymbol} 参数并重启策略...`;
      try {
        const resp = await fetch("/api/runner/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        runnerParamsMetaEl.textContent = `参数已应用到 ${selectedSymbol}${data.restarted ? "，策略已重启" : (data.already_running ? "，策略已在运行" : "，策略已启动")}`;
        await loadMonitor();
        const appliedConfig = ((((data || {}).runner || {}).config) || payload);
        setRunnerEditorConfig(appliedConfig, `已载入 ${selectedSymbol} 当前生效参数。`);
      } catch (err) {
        runnerParamsMetaEl.textContent = `应用失败: ${err}`;
      } finally {
        applyParamsBtn.disabled = false;
        loadRunningParamsBtn.disabled = false;
        loadPresetParamsBtn.disabled = false;
      }
    }

    function syncCustomGridActionButtons() {
      const selectedCustom = getSelectedCustomPreset();
      const hasCustom = Boolean(selectedCustom);
      customGridLoadBtn.disabled = strategyActionPending || !hasCustom;
      customGridUpdateBtn.disabled = strategyActionPending || !hasCustom;
      customGridDeleteBtn.disabled = strategyActionPending || !hasCustom;
    }

    function populatePresetOptions(data) {
      const selectedSymbol = String(symbolEl.value || "").trim().toUpperCase();
      const presets = (((data && data.runner_presets) && data.runner_presets.length)
        ? data.runner_presets
        : LOCAL_STRATEGY_PRESETS).filter((item) => {
          const presetSymbol = String((item && item.symbol) || "").trim().toUpperCase();
          return !selectedSymbol || !presetSymbol || presetSymbol === selectedSymbol;
        });
      runnerPresets = presets;
      const currentProfile = String((((data || {}).runner || {}).config || {}).strategy_profile || "volume_long_v4");
      const existingValue = strategyPresetEl.value || currentProfile;
      strategyPresetEl.innerHTML = presets.map((item) => `
        <option value="${escapeHtml(item.key)}">${escapeHtml(item.label)}</option>
      `).join("");
      const preferredValue = presets.some((item) => item.key === existingValue) ? existingValue : currentProfile;
      if (preferredValue) strategyPresetEl.value = preferredValue;
      presetsLoaded = true;
      renderPresetMeta(data);
      syncCustomGridActionButtons();
    }

    function renderPresetMeta(data) {
      const runnerCfg = (((data || {}).runner || {}).config || {});
      const currentProfile = String(runnerCfg.strategy_profile || "volume_long_v4");
      const selectedProfile = String(strategyPresetEl.value || currentProfile);
      const currentPreset = getPresetByKey(currentProfile);
      const selectedPreset = getPresetByKey(selectedProfile);
      const currentText = currentPreset ? currentPreset.label : currentProfile;
      const selectedText = selectedPreset ? selectedPreset.label : selectedProfile;
      const selectedDesc = selectedPreset ? selectedPreset.description : "未选择策略预设";
      const startableText = selectedPreset && !selectedPreset.startable ? " · 当前只展示模板，不能直接启动" : "";
      const customText = selectedPreset && selectedPreset.custom ? " · 可载入/更新/删除" : "";
      strategyPresetMetaEl.textContent =
        `当前运行: ${currentText} · 已选择: ${selectedText} · ${selectedDesc}${startableText}${customText}`;
      syncCustomGridActionButtons();
    }

    function setCustomGridStatus(message, isError = false) {
      customGridStatusEl.textContent = message;
      customGridStatusEl.className = isError ? "meta bad" : "meta";
    }

    function clearCustomGridPreview() {
      latestCustomGridPreview = null;
      customGridSummaryEl.textContent = "";
      customGridPreviewBody.innerHTML = '<tr><td colspan="9" class="empty">先生成预览，再决定是否保存为策略</td></tr>';
    }

    function loadCustomGridPresetToForm() {
      const preset = getSelectedCustomPreset();
      if (!preset) {
        setCustomGridStatus("当前选择的不是自定义策略，无法载入到表单。", true);
        return;
      }
      const params = preset.grid_preview_params || {};
      const config = preset.config || {};
      customGridNameEl.value = String(preset.label || "");
      customGridDirectionEl.value = String(params.strategy_direction || "neutral");
      customGridLevelModeEl.value = String(params.grid_level_mode || "arithmetic");
      customGridNEl.value = String(params.n ?? 12);
      customGridMinPriceEl.value = String(params.min_price ?? "");
      customGridMaxPriceEl.value = String(params.max_price ?? "");
      customGridMarginAmountEl.value = String(params.margin_amount ?? "");
      customGridLeverageEl.value = String(params.leverage ?? 2);
      customGridRollEnabledEl.checked = Boolean(config.custom_grid_roll_enabled);
      customGridRollIntervalEl.value = String(config.custom_grid_roll_interval_minutes ?? 5);
      customGridRollTradeThresholdEl.value = String(config.custom_grid_roll_trade_threshold ?? 100);
      customGridRollUpperDistanceRatioEl.value = String(config.custom_grid_roll_upper_distance_ratio ?? 0.30);
      customGridRollShiftLevelsEl.value = String(config.custom_grid_roll_shift_levels ?? 1);
      latestCustomGridPreview = null;
      customGridSummaryEl.textContent = "";
      customGridPreviewBody.innerHTML = '<tr><td colspan="9" class="empty">已载入已选自定义策略。点击“预览网格”可查看当前盘面下的最新委托明细。</td></tr>';
      setCustomGridStatus(`已载入 ${preset.label}。修改参数后可直接点“更新已选策略”。`);
    }

    function readCustomGridPayload() {
      return {
        contract_type: "usdm",
        symbol: (symbolEl.value || "NIGHTUSDT").trim().toUpperCase(),
        name: (customGridNameEl.value || "").trim(),
        strategy_direction: String(customGridDirectionEl.value || "neutral"),
        grid_level_mode: String(customGridLevelModeEl.value || "arithmetic"),
        n: Number(customGridNEl.value || 0),
        min_price: Number(customGridMinPriceEl.value || 0),
        max_price: Number(customGridMaxPriceEl.value || 0),
        margin_amount: Number(customGridMarginAmountEl.value || 0),
        leverage: Number(customGridLeverageEl.value || 0),
        custom_grid_roll_enabled: Boolean(customGridRollEnabledEl.checked),
        custom_grid_roll_interval_minutes: Number(customGridRollIntervalEl.value || 5),
        custom_grid_roll_trade_threshold: Number(customGridRollTradeThresholdEl.value || 100),
        custom_grid_roll_upper_distance_ratio: Number(customGridRollUpperDistanceRatioEl.value || 0.30),
        custom_grid_roll_shift_levels: Number(customGridRollShiftLevelsEl.value || 1),
      };
    }

    function renderCustomGridPreview(result) {
      latestCustomGridPreview = result;
      const summary = (result && result.summary) || {};
      const rows = (result && result.rows) || [];
      const warnings = Array.isArray(summary.warnings) ? summary.warnings.filter(Boolean) : [];
      const liqParts = [];
      if (summary.startup_long_liquidation_price) liqParts.push(`启动多仓爆仓价 ${fmtNum(summary.startup_long_liquidation_price, 7)}`);
      if (summary.startup_short_liquidation_price) liqParts.push(`启动空仓爆仓价 ${fmtNum(summary.startup_short_liquidation_price, 7)}`);
      if (summary.full_long_liquidation_price) liqParts.push(`满格多仓爆仓价 ${fmtNum(summary.full_long_liquidation_price, 7)}`);
      if (summary.full_short_liquidation_price) liqParts.push(`满格空仓爆仓价 ${fmtNum(summary.full_short_liquidation_price, 7)}`);
      customGridSummaryEl.textContent = [
        `现价 ${fmtNum(summary.current_price || 0, 7)}`,
        `网格 ${fmtNum(summary.grid_count || 0, 0)}`,
        `现价启动底仓 ${formatStartupInventory(summary, 4, 4)}`,
        `启动净仓 ${fmtNum(summary.startup_net_notional || 0, 4)}`,
        `活动买/卖 ${fmtNum(summary.active_buy_orders || 0, 0)} / ${fmtNum(summary.active_sell_orders || 0, 0)}`,
        `满格多/空 ${fmtNum(summary.full_long_entry_notional || 0, 4)} / ${fmtNum(summary.full_short_entry_notional || 0, 4)}`,
        warnings.join(" · "),
        liqParts.join(" · "),
      ].filter(Boolean).join(" · ");
      if (!rows.length) {
        customGridPreviewBody.innerHTML = '<tr><td colspan="9" class="empty">当前参数下没有生成有效网格</td></tr>';
        return;
      }
      customGridPreviewBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${fmtNum(row.idx || 0, 0)}</td>
          <td>${escapeHtml(row.grid_side || "--")}</td>
          <td>${fmtNum(row.lower_price || 0, 7)} ~ ${fmtNum(row.upper_price || 0, 7)}</td>
          <td>${escapeHtml(row.entry_side || "--")} @ ${fmtNum(row.entry_price || 0, 7)}</td>
          <td>${escapeHtml(row.exit_side || "--")} @ ${fmtNum(row.exit_price || 0, 7)}</td>
          <td>${fmtNum(row.qty || 0, 4)}</td>
          <td>${fmtNum(row.entry_notional || 0, 4)}</td>
          <td>${escapeHtml(row.startup_state || "--")}</td>
          <td>${escapeHtml(row.active_order_side || "--")} @ ${fmtNum(row.active_order_price || 0, 7)}</td>
        </tr>
      `).join("");
    }

    async function runCustomGridPreview() {
      const payload = readCustomGridPayload();
      setCustomGridStatus(`正在为 ${payload.symbol} 生成网格预览...`);
      customGridPreviewBtn.disabled = true;
      customGridSaveBtn.disabled = true;
      try {
        const resp = await fetch("/api/grid_preview", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        renderCustomGridPreview(data);
        const warnings = Array.isArray(data.summary && data.summary.warnings) ? data.summary.warnings.filter(Boolean) : [];
        setCustomGridStatus(
          warnings.length
            ? `预览已生成。注意：${warnings.join("；")}`
            : `预览已生成。可以继续保存为策略预设。`
        );
      } catch (err) {
        clearCustomGridPreview();
        setCustomGridStatus(`预览失败: ${err}`, true);
      } finally {
        customGridPreviewBtn.disabled = false;
        customGridSaveBtn.disabled = false;
      }
    }

    async function saveCustomGridStrategy() {
      const payload = readCustomGridPayload();
      setCustomGridStatus(`正在保存 ${payload.symbol} 自定义网格策略...`);
      customGridPreviewBtn.disabled = true;
      customGridSaveBtn.disabled = true;
      try {
        const resp = await fetch("/api/runner/presets/create_grid", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        if (data.preview) renderCustomGridPreview(data.preview);
        await loadMonitor();
        strategyPresetEl.value = data.preset_key;
        renderPresetMeta(latestMonitorData);
        setCustomGridStatus(`策略已保存为 ${data.preset && data.preset.label ? data.preset.label : data.preset_key}。现在可以直接点上方“启动策略/重启策略”。`);
      } catch (err) {
        setCustomGridStatus(`保存失败: ${err}`, true);
      } finally {
        customGridPreviewBtn.disabled = false;
        customGridSaveBtn.disabled = false;
        syncCustomGridActionButtons();
      }
    }

    async function updateCustomGridStrategy() {
      const preset = getSelectedCustomPreset();
      if (!preset) {
        setCustomGridStatus("请选择一个自定义策略后再更新。", true);
        return;
      }
      const payload = { ...readCustomGridPayload(), preset_key: preset.key };
      setCustomGridStatus(`正在更新 ${payload.symbol} 自定义网格策略...`);
      customGridPreviewBtn.disabled = true;
      customGridSaveBtn.disabled = true;
      customGridLoadBtn.disabled = true;
      customGridUpdateBtn.disabled = true;
      customGridDeleteBtn.disabled = true;
      try {
        const resp = await fetch("/api/runner/presets/update_grid", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        if (data.preview) renderCustomGridPreview(data.preview);
        await loadMonitor();
        strategyPresetEl.value = data.preset_key;
        renderPresetMeta(latestMonitorData);
        setCustomGridStatus(`策略已更新：${data.preset && data.preset.label ? data.preset.label : data.preset_key}。如需生效到运行中的 runner，请点上方“启动策略/重启策略”。`);
      } catch (err) {
        setCustomGridStatus(`更新失败: ${err}`, true);
      } finally {
        customGridPreviewBtn.disabled = false;
        customGridSaveBtn.disabled = false;
        syncCustomGridActionButtons();
      }
    }

    async function deleteCustomGridStrategy() {
      const preset = getSelectedCustomPreset();
      if (!preset) {
        setCustomGridStatus("请选择一个自定义策略后再删除。", true);
        return;
      }
      if (!confirm(`确认删除自定义策略“${preset.label}”吗？`)) return;
      setCustomGridStatus(`正在删除 ${preset.label}...`);
      customGridPreviewBtn.disabled = true;
      customGridSaveBtn.disabled = true;
      customGridLoadBtn.disabled = true;
      customGridUpdateBtn.disabled = true;
      customGridDeleteBtn.disabled = true;
      try {
        const resp = await fetch("/api/runner/presets/delete_grid", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            preset_key: preset.key,
            symbol: (symbolEl.value || "").trim().toUpperCase(),
          }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        clearCustomGridPreview();
        customGridNameEl.value = "";
        await loadMonitor();
        renderPresetMeta(latestMonitorData);
        setCustomGridStatus(`策略已删除：${preset.label}`);
      } catch (err) {
        setCustomGridStatus(`删除失败: ${err}`, true);
      } finally {
        customGridPreviewBtn.disabled = false;
        customGridSaveBtn.disabled = false;
        syncCustomGridActionButtons();
      }
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v) * 100;
      const sign = num > 0 ? "+" : "";
      return sign + num.toFixed(4) + "%";
    }

    function fmtTs(v) {
      if (!v) return "--";
      const dt = new Date(v);
      if (Number.isNaN(dt.getTime())) return String(v);
      return dt.toLocaleString("zh-CN", { hour12: false });
    }

    function toLocalInputValue(value) {
      if (!value) return "";
      const dt = new Date(value);
      if (Number.isNaN(dt.getTime())) return "";
      const pad = (num) => String(num).padStart(2, "0");
      return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
    }

    function fromLocalInputValue(value) {
      const raw = String(value || "").trim();
      if (!raw) return null;
      const dt = new Date(raw);
      if (Number.isNaN(dt.getTime())) return null;
      return dt.toISOString();
    }

    function escapeHtml(s) {
      return String(s ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function statusClass(v) {
      if (v > 0) return "good";
      if (v < 0) return "bad";
      return "";
    }

    function buildCompetitionRewardTargetCard(targets) {
      const payload = (targets && typeof targets === "object") ? targets : {};
      const tiers = Array.isArray(payload.tiers) ? payload.tiers : [];
      const message = String(payload.message || "").trim();
      if (!tiers.length && !message) {
        return null;
      }
      const rows = tiers.map((item) => {
        const volumes = (item && item.volumes_by_loss_rate) || {};
        const rewardText = item.reward_value_usdt === null || item.reward_value_usdt === undefined
          ? "--"
          : fmtNum(item.reward_value_usdt, 2);
        return `
          <div class="metric-line">
            <strong><span class="inline-badge">${escapeHtml(item.rank_label || "--")}</span>${escapeHtml(item.segment_label || "")}</strong><br />
            奖励 ${rewardText}U · 万3 ${fmtWanVolume(volumes["3"])} · 万4 ${fmtWanVolume(volumes["4"])} · 万5 ${fmtWanVolume(volumes["5"])}
          </div>
        `;
      }).join("");
      const rewardPriceText = payload.reward_price_usdt === null || payload.reward_price_usdt === undefined
        ? "--"
        : fmtNum(payload.reward_price_usdt, 4);
      return {
        label: "奖励回本量",
        value: "20 / 50 / 200",
        cls: "",
        sub: `按当前奖励折 USDT 估算 · 奖励币种 ${String(payload.reward_unit || "--").toUpperCase()} · 现价 ${rewardPriceText}`,
        bodyHtml: `<div class="metric-lines">${rows || `<div class="metric-line">${escapeHtml(message || "当前没有可用的奖励换手量目标。")}</div>`}</div>`,
      };
    }

    function renderCards(data) {
      const trade = data.trade_summary || {};
      const income = data.income_summary || {};
      const audit = data.audit || {};
      const pos = data.position || {};
      const accountAssets = data.account_assets || {};
      const usdtAsset = accountAssets.USDT || null;
      const bnbAsset = accountAssets.BNB || null;
      const market = data.market || {};
      const loop = (data.local && data.local.loop_summary) || {};
      const latestLoop = loop.latest || {};
      const runner = data.runner || {};
      const runnerCfg = runner.config || {};
      const competitionWindow = data.competition_window || {};
      const competitionRewardTargets = data.competition_reward_targets || {};
      const currentPreset = getPresetByKey(String(runnerCfg.strategy_profile || "volume_long_v4"));
      const selectedPreset = getPresetByKey(String(strategyPresetEl.value || runnerCfg.strategy_profile || "volume_long_v4"));
      const risk = data.risk_controls || {};
      const strategyMode = String(risk.strategy_mode || runnerCfg.strategy_mode || "one_way_long");
      const effectiveProfile = String(risk.effective_strategy_profile || runnerCfg.strategy_profile || "volume_long_v4");
      const effectiveProfileLabel = String(risk.effective_strategy_label || effectiveProfile || "--");
      const autoRegimeEnabled = Boolean(risk.auto_regime_enabled);
      const autoRegimeRegime = String(risk.auto_regime_regime || "--");
      const autoRegimePending = `${fmtNum(risk.auto_regime_pending_count || 0, 0)} / ${fmtNum(risk.auto_regime_confirm_cycles || 0, 0)}`;
      const xautAdaptiveEnabled = Boolean(risk.xaut_adaptive_enabled);
      const xautAdaptiveState = String(risk.xaut_adaptive_state || "--");
      const xautAdaptiveCandidateState = String(risk.xaut_adaptive_candidate_state || "--");
      const xautAdaptivePending = fmtNum(risk.xaut_adaptive_pending_count || 0, 0);
      const neutralHourlyEnabled = Boolean(risk.neutral_hourly_scale_enabled);
      const neutralHourlyRegime = String(risk.neutral_hourly_regime || "--");
      const neutralHourlyScale = fmtNum(risk.neutral_hourly_scale_ratio || 0, 2);
      const isOneWayShort = strategyMode === "one_way_short";
      const isHedge = strategyMode === "hedge_neutral";
      const isSyntheticNeutral = strategyMode === "synthetic_neutral";
      const isTargetNeutral = strategyMode === "inventory_target_neutral";
      const isNeutralMode = isHedge || isSyntheticNeutral || isTargetNeutral;
      const modeLabel = isHedge ? "双向中性" : (isSyntheticNeutral ? "单向合成中性" : (isTargetNeutral ? "单向目标中性" : (isOneWayShort ? "单向做空" : "单向做多")));
      const longQty = isSyntheticNeutral ? (pos.virtual_long_qty || pos.long_qty || 0) : (pos.long_qty || 0);
      const shortQty = isSyntheticNeutral ? (pos.virtual_short_qty || pos.short_qty || 0) : (pos.short_qty || 0);
      const netQty = isSyntheticNeutral ? (pos.virtual_net_qty || pos.position_amt || 0) : (pos.position_amt || 0);
      const centerSource = risk.center_source || {};
      const isCustomGridCenter = String(centerSource.reason || "").startsWith("custom_grid_");
      const centerPriceText = fmtNum(centerSource.center_price || 0, 7);
      const centerLabelBase = isCustomGridCenter ? "梯子参考价" : "中心价";
      const centerLabelText = centerSource.interval ? `${centerLabelBase}: ${centerPriceText} (${centerSource.interval})` : `${centerLabelBase}: ${centerPriceText}`;
      const strategyRunning = Boolean(loop.is_alive) && (runner.configured ? Boolean(runner.is_running) : true);
      const strategyDetail = [
        `预设: ${currentPreset ? currentPreset.label : (runnerCfg.strategy_profile || "--")}`,
        `生效子策略: ${effectiveProfileLabel}`,
        `模式: ${modeLabel}`,
        `最近事件: ${fmtTs(data.session && data.session.last_event)}`,
        `PID: ${runner.pid || "--"}`,
        isOneWayShort ? `停空: ${risk.short_paused ? "是" : "否"}` : `停买: ${risk.buy_paused ? "是" : "否"}`,
        isNeutralMode ? `停空: ${risk.short_paused ? "是" : "否"}` : (isOneWayShort ? `空裁单: ${risk.short_cap_applied ? "是" : "否"}` : `硬裁单: ${risk.buy_cap_applied ? "是" : "否"}`),
      ].join(" · ");
      const riskStatusClass = risk.buy_paused || risk.buy_cap_applied || risk.short_paused || risk.short_cap_applied ? "warn" : "";
      const riskValue = isNeutralMode
        ? `多仓软/硬 ${fmtNum(risk.pause_buy_position_notional, 4)} / ${fmtNum(risk.max_position_notional, 4)} · 空仓软/硬 ${fmtNum(risk.pause_short_position_notional, 4)} / ${fmtNum(risk.max_short_position_notional, 4)}`
        : (isOneWayShort
            ? `软停空 ${fmtNum(risk.pause_short_position_notional, 4)} / 硬上限 ${fmtNum(risk.max_short_position_notional, 4)}`
            : `软停买 ${fmtNum(risk.pause_buy_position_notional, 4)} / 硬上限 ${fmtNum(risk.max_position_notional, 4)}`);
        const riskDetail = [
        isNeutralMode ? `当前多/空: ${fmtNum(risk.current_long_notional, 4)} / ${fmtNum(risk.current_short_notional, 4)}` : (isOneWayShort ? `当前空仓: ${fmtNum(risk.current_short_notional, 4)}` : `当前净仓: ${fmtNum(risk.current_long_notional, 4)}`),
        isNeutralMode ? `多/空剩余空间: ${fmtNum(risk.remaining_headroom, 4)} / ${fmtNum(risk.remaining_short_headroom, 4)}` : (isOneWayShort ? `剩余空间: ${fmtNum(risk.remaining_short_headroom, 4)}` : `剩余空间: ${fmtNum(risk.remaining_headroom, 4)}`),
        isNeutralMode ? `计划多/空: ${fmtNum(risk.planned_buy_notional, 4)} / ${fmtNum(risk.planned_short_notional, 4)}` : (isOneWayShort ? `计划卖空: ${fmtNum(risk.planned_short_notional, 4)}` : `计划买单: ${fmtNum(risk.planned_buy_notional, 4)}`),
        isNeutralMode ? `多/空预算: ${fmtNum(risk.buy_budget_notional, 4)} / ${fmtNum(risk.short_budget_notional, 4)}` : (isOneWayShort ? `卖空预算: ${fmtNum(risk.short_budget_notional, 4)}` : `买单预算: ${fmtNum(risk.buy_budget_notional, 4)}`),
        `运行状态: ${risk.runtime_status || "--"}`,
        `滚动亏损: ${fmtNum(risk.rolling_hourly_loss || 0, 4)} / ${fmtNum(risk.rolling_hourly_loss_limit || 0, 4)}`,
        `累计成交额: ${fmtNum(risk.cumulative_gross_notional || 0, 4)} / ${fmtNum(risk.max_cumulative_notional || 0, 4)}`,
        `停止原因: ${risk.stop_reason || "--"}`,
        `自适应状态: ${autoRegimeEnabled ? `${autoRegimeRegime} (${autoRegimePending})` : "关闭"}`,
        `XAUT 三态: ${xautAdaptiveEnabled ? `${xautAdaptiveState} -> ${xautAdaptiveCandidateState} (pending ${xautAdaptivePending})` : "关闭"}`,
        `中性小时缩放: ${neutralHourlyEnabled ? `${neutralHourlyRegime} x${neutralHourlyScale}` : "关闭"}`,
        `分层: ${risk.inventory_tier_active ? `${Math.round((risk.inventory_tier_ratio || 0) * 100)}%` : "未激活"}`,
        `分钟停买: ${risk.volatility_buy_pause ? "是" : "否"}`,
        `冻轴: ${risk.shift_frozen ? "是" : "否"}`,
      ].join(" · ");
      const runnerLabel = runner.is_running ? `PID ${runner.pid}` : (runner.pid ? `PID ${runner.pid} 未运行` : "未检测到");
      const runnerDetail = [
        `运行时长: ${runner.elapsed || "--"}`,
        `apply: ${runnerCfg.apply ? "是" : "否"}`,
        `间隔: ${fmtNum(runnerCfg.sleep_seconds, 0)}s`,
        `杠杆: ${runnerCfg.leverage ? `${runnerCfg.leverage}x` : "--"}`,
      ].join(" · ");
      const statsWindowLabel = competitionWindow.label
        ? `${competitionWindow.label} · 起点 ${fmtTs(competitionWindow.stats_start_at || competitionWindow.activity_start_at)}`
        : `会话起点 ${fmtTs(data.session && data.session.start)}`;
      const cards = [
        { label: "策略状态", value: strategyRunning ? "运行中" : "未活跃", cls: strategyRunning ? "good" : "warn", sub: strategyDetail },
        { label: "执行进程", value: runnerLabel, cls: runner.is_running ? "good" : "warn", sub: runnerDetail },
        { label: "统计区间", value: statsWindowLabel, cls: "", sub: competitionWindow.activity_end_at ? `结束时间: ${fmtTs(competitionWindow.activity_end_at)}` : "未匹配到交易赛窗口，默认按当前会话统计" },
        { label: "风控硬限制", value: riskValue, cls: riskStatusClass, sub: riskDetail },
        { label: "会话成交笔数", value: fmtNum(trade.trade_count || 0, 0), cls: "", sub: `Maker: ${fmtNum(trade.maker_count || 0, 0)} · 买/卖: ${fmtNum(trade.buy_count || 0, 0)} / ${fmtNum(trade.sell_count || 0, 0)}` },
        { label: "累计成交额", value: fmtNum(trade.gross_notional || 0, 4), cls: "", sub: `买入: ${fmtNum(trade.buy_notional || 0, 4)} · 卖出: ${fmtNum(trade.sell_notional || 0, 4)} · 来源: ${(audit.trade_source && audit.trade_source.source) || "--"}` },
        { label: "净收益估算", value: fmtMoney(trade.net_pnl_estimate || 0), cls: statusClass(trade.net_pnl_estimate || 0), sub: `已实现: ${fmtMoney(trade.realized_pnl || 0)} · 浮盈: ${fmtMoney(pos.unrealized_pnl || 0)}` },
        { label: "手续费 / 资金费", value: `${fmtMoney(-(trade.commission || 0))} / ${fmtMoney(income.funding_fee || 0)}`, cls: "", sub: `左侧为折算 USDT 后的累计手续费 · 原始资产: ${escapeHtml(JSON.stringify(trade.commission_raw_by_asset || {}))}` },
        { label: "审计日志", value: `成交 ${fmtNum(audit.trade_row_count || 0, 0)} / 资金费 ${fmtNum(audit.income_row_count || 0, 0)}`, cls: "", sub: `委托事件: ${fmtNum(audit.order_event_count || 0, 0)} · 提交轮次: ${fmtNum(audit.submit_event_count || 0, 0)} · 计划轮次: ${fmtNum(audit.plan_event_count || 0, 0)}` },
        { label: "当前仓位", value: isNeutralMode ? `净 ${fmtNum(netQty, 0)}` : fmtNum(pos.position_amt || 0, 0), cls: "", sub: isNeutralMode ? `Long/Short: ${fmtNum(longQty, 0)} / ${fmtNum(shortQty, 0)} · ${centerLabelText} · 中价名义: ${fmtNum((longQty + shortQty) * (market.mid_price || 0), 4)}` : `开仓均价: ${fmtNum(pos.entry_price || 0, 7)} · ${centerLabelText} · 持仓名义: ${fmtNum(Math.abs(pos.position_amt || 0) * (market.mid_price || 0), 4)}` },
        { label: "当前挂单数", value: fmtNum((data.open_orders || []).length, 0), cls: "", sub: `买/卖: ${fmtNum((data.open_orders || []).filter(x => x.side === "BUY").length, 0)} / ${fmtNum((data.open_orders || []).filter(x => x.side === "SELL").length, 0)}` },
        { label: "合约 USDT", value: usdtAsset ? fmtNum(usdtAsset.wallet_balance || 0, 4) : "--", cls: "", sub: usdtAsset ? `可用: ${fmtNum(usdtAsset.available_balance || 0, 4)} · 可提: ${fmtNum(usdtAsset.max_withdraw_amount || 0, 4)}` : "未读到 USDT 资产" },
        { label: "合约 BNB", value: bnbAsset ? fmtNum(bnbAsset.wallet_balance || 0, 6) : "--", cls: "", sub: bnbAsset ? `可用: ${fmtNum(bnbAsset.available_balance || 0, 6)} · 可提: ${fmtNum(bnbAsset.max_withdraw_amount || 0, 6)}` : "未读到 BNB 资产" },
        { label: "市场", value: `${fmtNum(market.bid_price || 0, 7)} / ${fmtNum(market.ask_price || 0, 7)}`, cls: "", sub: `中价: ${fmtNum(market.mid_price || 0, 7)} · Funding: ${fmtPct(market.funding_rate || 0)}` },
      ];
      const rewardTargetCard = buildCompetitionRewardTargetCard(competitionRewardTargets);
      if (rewardTargetCard) {
        cards.splice(6, 0, rewardTargetCard);
      }
      summaryEl.innerHTML = cards.map((item) => {
        const label = String(item.label || "");
        const value = String(item.value || "--");
        const cls = String(item.cls || "");
        const sub = String(item.sub || "");
        const bodyHtml = String(item.bodyHtml || "");
        return `
          <div class="metric">
            <div class="label">${escapeHtml(label)}</div>
            <div class="value ${cls}">${escapeHtml(value)}</div>
            <div class="sub">${escapeHtml(sub)}</div>
            ${bodyHtml}
          </div>
        `;
      }).join("");
      startStrategyBtn.disabled = strategyActionPending || Boolean(selectedPreset && !selectedPreset.startable);
      startStrategyBtn.textContent = runner.is_running ? "重启策略" : "启动策略";
      stopStrategyBtn.disabled = strategyActionPending || !Boolean(runner.is_running);
    }

    function formatAlertSeverity(severity) {
      if (severity === "critical") return "严重";
      if (severity === "warning") return "警告";
      return "提示";
    }

    function renderAlerts(data) {
      const alerts = Array.isArray(data.alerts) ? data.alerts : [];
      if (!alerts.length) {
        alertBoxEl.innerHTML = '<div class="alert-empty">当前没有明确的运行告警。这里会直接提示保证金不足、频率超限、停买/停空、进程退出等关键问题。</div>';
        return;
      }
      alertBoxEl.innerHTML = alerts.map((item) => {
        const severity = ["critical", "warning", "info"].includes(item.severity) ? item.severity : "info";
        const detail = String(item.detail || "").trim();
        const action = String(item.action || "").trim();
        const code = String(item.code || "").trim();
        const ts = item.ts ? fmtTs(item.ts) : "";
        const actionButtons = [];
        if (code && alertParamKeys(code).length) {
          actionButtons.push(
            `<button type="button" class="alert-action-btn" data-alert-action="locate" data-alert-code="${escapeHtml(code)}">定位参数</button>`
          );
        }
        if (code && alertSupportsSuggestion(code)) {
          actionButtons.push(
            `<button type="button" class="alert-action-btn primary" data-alert-action="suggest" data-alert-code="${escapeHtml(code)}">生成建议参数</button>`
          );
        }
        return `
          <div class="alert-item ${severity}">
            <div class="alert-head">
              <div class="alert-title">${escapeHtml(String(item.title || "运行提示"))}</div>
              <span class="alert-severity">${escapeHtml(formatAlertSeverity(severity))}</span>
            </div>
            ${detail ? `<div class="alert-detail">${escapeHtml(detail)}</div>` : ""}
            ${action ? `<div class="alert-action">建议：${escapeHtml(action)}</div>` : ""}
            ${actionButtons.length ? `<div class="alert-actions">${actionButtons.join("")}</div>` : ""}
            ${ts ? `<div class="alert-ts">时间：${escapeHtml(ts)}</div>` : ""}
          </div>
        `;
      }).join("");
    }

    function renderPosition(data) {
      const rawPos = data.position;
      const pos = rawPos || {};
      const risk = data.risk_controls || {};
      const accountAssets = data.account_assets || {};
      const usdtAsset = accountAssets.USDT || null;
      const bnbAsset = accountAssets.BNB || null;
      const runnerCfg = (data.runner && data.runner.config) || {};
      const currentPreset = getPresetByKey(String(runnerCfg.strategy_profile || "volume_long_v4"));
      const strategyMode = String(risk.strategy_mode || runnerCfg.strategy_mode || "one_way_long");
      const effectiveProfile = String(risk.effective_strategy_profile || runnerCfg.strategy_profile || "volume_long_v4");
      const effectiveProfileLabel = String(risk.effective_strategy_label || effectiveProfile || "--");
      const xautAdaptiveEnabled = Boolean(risk.xaut_adaptive_enabled);
      const xautAdaptiveState = String(risk.xaut_adaptive_state || "--");
      const xautAdaptiveCandidateState = String(risk.xaut_adaptive_candidate_state || "--");
      const xautAdaptivePending = fmtNum(risk.xaut_adaptive_pending_count || 0, 0);
      const isOneWayShort = strategyMode === "one_way_short";
      const isHedge = strategyMode === "hedge_neutral";
      const isSyntheticNeutral = strategyMode === "synthetic_neutral";
      const isTargetNeutral = strategyMode === "inventory_target_neutral";
      const isNeutralMode = isHedge || isSyntheticNeutral || isTargetNeutral;
      const modeLabel = isHedge ? "双向中性" : (isSyntheticNeutral ? "单向合成中性" : (isTargetNeutral ? "单向目标中性" : (isOneWayShort ? "单向做空" : "单向做多")));
      const virtualLongQty = isSyntheticNeutral ? (pos.virtual_long_qty || pos.long_qty || 0) : (pos.long_qty || 0);
      const virtualShortQty = isSyntheticNeutral ? (pos.virtual_short_qty || pos.short_qty || 0) : (pos.short_qty || 0);
      const virtualNetQty = isSyntheticNeutral ? (pos.virtual_net_qty || pos.position_amt || 0) : (pos.position_amt || 0);
      const targetBands = risk.inventory_target_bands || {};
      const centerSource = risk.center_source || {};
      const isCustomGridCenter = String(centerSource.reason || "").startsWith("custom_grid_");
      const neutralHourly = risk.neutral_hourly_scale || {};
      if (!rawPos) {
        positionBox.innerHTML = '<div class="empty">没有可用的账户持仓信息。请确认 Web UI 启动时已加载 Binance API 环境变量。</div>';
        return;
      }
      const items = [
        ["当前预设", currentPreset ? currentPreset.label : (runnerCfg.strategy_profile || "--")],
        ["当前子策略", effectiveProfileLabel],
        ["策略模式", modeLabel],
        ["净仓数量", fmtNum(virtualNetQty, 0)],
        ["Long 仓位", fmtNum(virtualLongQty, 0)],
        ["Short 仓位", fmtNum(virtualShortQty, 0)],
        ["交易所净仓", fmtNum(pos.position_amt || 0, 0)],
        [isCustomGridCenter ? "当前梯子参考价" : "当前中心价格", centerSource.center_price ? `${fmtNum(centerSource.center_price || 0, 7)}${centerSource.interval ? ` (${centerSource.interval})` : ""}` : "--"],
        ["开仓均价", fmtNum(pos.entry_price || 0, 7)],
        ["保本价", fmtNum(pos.break_even_price || 0, 7)],
        ["浮动盈亏", fmtMoney(pos.unrealized_pnl || 0)],
        ["杠杆", pos.leverage ? `${pos.leverage}x` : "--"],
        ["单向模式", pos.one_way_mode ? "是" : "否"],
        ["Multi-Assets", pos.multi_assets_margin ? "是" : "否"],
        ["可用余额", fmtNum(pos.available_balance || 0, 4)],
        ["钱包余额", fmtNum(pos.wallet_balance || 0, 4)],
        ["合约 USDT 余额", usdtAsset ? fmtNum(usdtAsset.wallet_balance || 0, 4) : "--"],
        ["合约 USDT 可用", usdtAsset ? fmtNum(usdtAsset.available_balance || 0, 4) : "--"],
        ["合约 USDT 可提", usdtAsset ? fmtNum(usdtAsset.max_withdraw_amount || 0, 4) : "--"],
        ["合约 BNB 余额", bnbAsset ? fmtNum(bnbAsset.wallet_balance || 0, 6) : "--"],
        ["合约 BNB 可用", bnbAsset ? fmtNum(bnbAsset.available_balance || 0, 6) : "--"],
        ["合约 BNB 可提", bnbAsset ? fmtNum(bnbAsset.max_withdraw_amount || 0, 6) : "--"],
        ["每笔名义", fmtNum(runnerCfg.per_order_notional, 4)],
        ["底仓目标名义", fmtNum(runnerCfg.base_position_notional, 4)],
        ["软停买阈值", fmtNum(risk.pause_buy_position_notional, 4)],
        ["软停空阈值", fmtNum(risk.pause_short_position_notional, 4)],
        ["多仓硬上限", fmtNum(risk.max_position_notional, 4)],
        ["空仓硬上限", fmtNum(risk.max_short_position_notional, 4)],
        ["当前多仓名义", fmtNum(risk.current_long_notional, 4)],
        ["当前空仓名义", fmtNum(risk.current_short_notional, 4)],
        ["剩余上限空间", fmtNum(risk.remaining_headroom, 4)],
        ["空仓剩余空间", fmtNum(risk.remaining_short_headroom, 4)],
        ["运行状态", risk.runtime_status || "--"],
        ["起始时间", risk.run_start_time ? fmtTs(risk.run_start_time) : "--"],
        ["结束时间", risk.run_end_time ? fmtTs(risk.run_end_time) : "--"],
        ["滚动亏损", fmtNum(risk.rolling_hourly_loss, 4)],
        ["滚动亏损阈值", fmtNum(risk.rolling_hourly_loss_limit, 4)],
        ["累计成交额", fmtNum(risk.cumulative_gross_notional, 4)],
        ["累计成交额阈值", fmtNum(risk.max_cumulative_notional, 4)],
        ["停止原因", risk.stop_reason || "--"],
        ["本轮买单预算", fmtNum(risk.buy_budget_notional, 4)],
        ["本轮卖空预算", fmtNum(risk.short_budget_notional, 4)],
        ["本轮计划买单", fmtNum(risk.planned_buy_notional, 4)],
        ["本轮计划卖空", fmtNum(risk.planned_short_notional, 4)],
        ["库存分层", risk.inventory_tier_active ? `${Math.round((risk.inventory_tier_ratio || 0) * 100)}%` : "未激活"],
        ["生效买/卖格", `${fmtNum(risk.effective_buy_levels, 0)} / ${fmtNum(risk.effective_sell_levels, 0)}`],
        ["生效单笔名义", fmtNum(risk.effective_per_order_notional, 4)],
        ["生效底仓目标", fmtNum(risk.effective_base_position_notional, 4)],
        ["停买状态", risk.buy_paused ? "是" : "否"],
        ["停空状态", risk.short_paused ? "是" : "否"],
        ["分钟停买", risk.volatility_buy_pause ? "是" : "否"],
        ["冻结重心平移", risk.shift_frozen ? "是" : "否"],
        ["最新1m涨跌", fmtPct(risk.market_guard_return_ratio || 0)],
        ["最新1m振幅", fmtPct(risk.market_guard_amplitude_ratio || 0)],
        ["自适应状态", risk.auto_regime_enabled ? `${risk.auto_regime_regime || "--"} (${fmtNum(risk.auto_regime_pending_count || 0, 0)} / ${fmtNum(risk.auto_regime_confirm_cycles || 0, 0)})` : "关闭"],
        ["自适应原因", risk.auto_regime_reason || "--"],
        ["XAUT 三态状态", xautAdaptiveEnabled ? `${xautAdaptiveState} -> ${xautAdaptiveCandidateState} (pending ${xautAdaptivePending})` : "关闭"],
        ["XAUT 三态原因", risk.xaut_adaptive_reason || "--"],
        ["中性目标带", (targetBands.offsets && targetBands.target_ratios) ? `${targetBands.offsets.map((v, i) => `${(Number(v) * 100).toFixed(1)}%:${Math.round(Number((targetBands.target_ratios || [])[i] || 0) * 100)}%`).join(' / ')}` : "--"],
        ["中性基础目标带", (targetBands.offsets && targetBands.base_target_ratios) ? `${targetBands.offsets.map((v, i) => `${(Number(v) * 100).toFixed(1)}%:${Math.round(Number((targetBands.base_target_ratios || [])[i] || 0) * 100)}%`).join(' / ')}` : "--"],
        ["当前目标净仓", fmtNum(targetBands.current_target_notional, 4)],
        ["小时缩放", neutralHourly.enabled ? `${neutralHourly.regime || "--"} · x${fmtNum(neutralHourly.scale || 0, 2)}` : "关闭"],
        ["小时缩放原因", neutralHourly.reason || "--"],
        ["中心来源", centerSource.interval ? `${centerSource.interval} · ${fmtTs(centerSource.candle && centerSource.candle.close_time)}` : "--"],
        ["硬裁单状态", risk.buy_cap_applied ? "是" : "否"],
        ["空仓裁单状态", risk.short_cap_applied ? "是" : "否"],
      ];
      if (isSyntheticNeutral) {
        items.splice(4, 0, ["虚拟净仓漂移", fmtNum(pos.ledger_drift_qty || 0, 0)]);
      }
      positionBox.innerHTML = `<table><tbody>${items.map(([k, v]) => `<tr><th>${escapeHtml(k)}</th><td>${escapeHtml(v)}</td></tr>`).join("")}</tbody></table>`;
    }

    function renderOpenOrders(data) {
      const rows = (data.open_orders || []).slice().sort((a, b) => Number(b.price || 0) - Number(a.price || 0));
      openOrderMetaEl.textContent = `当前 ${rows.length} 笔未成交委托`;
      if (!rows.length) {
        openOrdersBody.innerHTML = '<tr><td colspan="7" class="empty">当前没有挂单</td></tr>';
        return;
      }
      openOrdersBody.innerHTML = rows.map((row) => `
        <tr>
          <td><span class="pill ${row.side === "SELL" ? "bad" : "good"}">${escapeHtml(row.side)}</span></td>
          <td>${escapeHtml(row.position_side || "BOTH")}</td>
          <td>${fmtNum(row.price || 0, 7)}</td>
          <td>${fmtNum(row.orig_qty || 0, 0)}</td>
          <td>${fmtNum((row.price || 0) * (row.orig_qty || 0), 4)}</td>
          <td>${row.reduce_only ? "减仓" : "开仓/补仓"}</td>
          <td>${row.time ? new Date(Number(row.time)).toLocaleTimeString("zh-CN", { hour12: false }) : "--"}</td>
        </tr>
      `).join("");
    }

    function renderHourlyStats(data) {
      const hourly = (data.hourly_summary || {});
      const rows = (hourly.rows || []);
      const competitionWindow = data.competition_window || {};
      const statsStartText = competitionWindow.stats_start_at
        ? ` · 统计起点 ${fmtTs(competitionWindow.stats_start_at)}`
        : "";
      hourlyMetaEl.textContent = `最近 ${hourly.row_count || 0} / ${hourly.available_hours || 0} 小时${statsStartText}`;
      if (!rows.length) {
        hourlyBody.innerHTML = '<tr><td colspan="10" class="empty">当前没有小时级统计数据</td></tr>';
        return;
      }
      hourlyBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${fmtTs(row.hour_start)}</td>
          <td>${fmtNum(row.gross_notional || 0, 4)}</td>
          <td>${fmtNum(row.trade_count || 0, 0)}</td>
          <td class="${statusClass(Number(row.net_after_fees_and_funding || 0))}">${fmtMoney(row.net_after_fees_and_funding || 0)}</td>
          <td class="${statusClass(Number(row.realized_pnl || 0))}">${fmtMoney(row.realized_pnl || 0)}</td>
          <td class="bad">${fmtMoney(-Math.abs(Number(row.commission || 0)))}</td>
          <td class="${statusClass(Number(row.funding_fee || 0))}">${fmtMoney(row.funding_fee || 0)}</td>
          <td class="${statusClass(Number(row.return_ratio || 0))}">${fmtPct(row.return_ratio || 0)}</td>
          <td>${fmtPct(row.amplitude_ratio || 0)}</td>
          <td>${fmtNum(row.buy_notional || 0, 1)} / ${fmtNum(row.sell_notional || 0, 1)}</td>
        </tr>
      `).join("");
    }

    function renderTrades(data) {
      const rows = ((data.trade_summary && data.trade_summary.recent_trades) || []).slice().reverse();
      if (!rows.length) {
        tradesBody.innerHTML = '<tr><td colspan="8" class="empty">当前会话还没有真实成交</td></tr>';
        return;
      }
      tradesBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${row.time ? new Date(Number(row.time)).toLocaleTimeString("zh-CN", { hour12: false }) : "--"}</td>
          <td>${escapeHtml(row.side || "--")}</td>
          <td>${fmtNum(row.price || 0, 7)}</td>
          <td>${fmtNum(row.qty || 0, 0)}</td>
          <td>${fmtNum((Number(row.price || 0) * Number(row.qty || 0)), 4)}</td>
          <td class="${statusClass(Number(row.realizedPnl || 0))}">${fmtMoney(row.realizedPnl || 0)}</td>
          <td class="bad">${fmtMoney(-Math.abs(Number(row.commission_usdt || row.commission || 0)))}</td>
          <td>${row.maker ? "是" : "否"}</td>
        </tr>
      `).join("");
    }

    function renderEvents(data) {
      const rows = ((((data.local || {}).loop_summary || {}).recent) || []).slice().reverse();
      if (!rows.length) {
        eventsBody.innerHTML = '<tr><td colspan="7" class="empty">还没有 loop_runner 事件，请先启动循环执行器</td></tr>';
        return;
      }
      eventsBody.innerHTML = rows.map((row) => {
        const status = row.error_message ? `<span class="bad">错误</span>` : (row.executed ? `<span class="good">已执行</span>` : `<span class="warn">观察</span>`);
        const action = row.error_message
          ? escapeHtml(row.error_message)
          : `补单 ${fmtNum(row.placed_count || 0, 0)} / 撤单 ${fmtNum(row.canceled_count || 0, 0)} · 成交落盘 ${fmtNum(row.trade_audit_appended || 0, 0)} · 分层 ${row.inventory_tier_active ? `${Math.round((Number(row.inventory_tier_ratio || 0)) * 100)}%` : "否"} · 停买 ${row.buy_paused ? "是" : "否"} · 分钟停买 ${row.volatility_buy_pause ? "是" : "否"} · 冻轴 ${row.shift_frozen ? "是" : "否"} · 硬裁单 ${row.buy_cap_applied ? "是" : "否"}`;
        return `
          <tr>
            <td>${fmtTs(row.ts)}</td>
            <td>${fmtNum(row.cycle || 0, 0)}</td>
            <td>${fmtNum(row.mid_price || 0, 7)}</td>
            <td>${fmtNum(row.current_long_notional || 0, 4)}</td>
            <td>${fmtNum(row.open_order_count || 0, 0)}</td>
            <td>${action}</td>
            <td>${status}</td>
          </tr>
        `;
      }).join("");
    }

    function makeSparkline(points, valueKey, color, fillColor) {
      if (!points || !points.length) return '<div class="empty">暂无可视化数据</div>';
      const width = 800;
      const height = 220;
      const padX = 24;
      const padY = 18;
      const values = points.map((item) => Number(item[valueKey] || 0));
      const min = Math.min(...values);
      const max = Math.max(...values);
      const spread = max - min || 1;
      const stepX = points.length > 1 ? (width - padX * 2) / (points.length - 1) : 0;
      const coords = points.map((item, idx) => {
        const x = padX + idx * stepX;
        const y = height - padY - ((Number(item[valueKey] || 0) - min) / spread) * (height - padY * 2);
        return [x, y];
      });
      const line = coords.map(([x, y]) => `${x},${y}`).join(" ");
      const area = `M ${padX} ${height - padY} ` + coords.map(([x, y]) => `L ${x} ${y}`).join(" ") + ` L ${coords[coords.length - 1][0]} ${height - padY} Z`;
      const latest = values[values.length - 1];
      return `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
          <path d="${area}" fill="${fillColor}" stroke="none"></path>
          <polyline points="${line}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
          <line x1="${padX}" y1="${height - padY}" x2="${width - padX}" y2="${height - padY}" stroke="#ddd6c8" stroke-width="1"></line>
          <text x="${padX}" y="18" fill="#6c685f" font-size="12">min ${min.toFixed(4)}</text>
          <text x="${width - padX}" y="18" fill="#6c685f" font-size="12" text-anchor="end">max ${max.toFixed(4)}</text>
          <text x="${width - padX}" y="${height - 8}" fill="${color}" font-size="12" text-anchor="end">latest ${latest.toFixed(4)}</text>
        </svg>
      `;
    }

    function formatStopActionSummary(summary) {
      if (!summary) return "";
      const parts = [];
      if (summary.cancel_open_orders_requested) {
        parts.push(`撤单 ${fmtNum(summary.cancel_success_count || 0, 0)} / ${fmtNum(summary.cancel_attempted_count || 0, 0)}`);
      }
      if (summary.close_all_positions_requested) {
        parts.push(`待平仓方向 ${fmtNum(summary.close_attempted_count || 0, 0)}`);
        if (summary.flatten_started) {
          parts.push("跟价平仓已启动");
        } else if (summary.flatten_already_running) {
          parts.push("跟价平仓已在运行");
        }
      }
      if (summary.warnings && summary.warnings.length) {
        parts.push(`提示: ${summary.warnings.join("；")}`);
      }
      if (summary.cancel_errors && summary.cancel_errors.length) {
        parts.push(`撤单错误 ${summary.cancel_errors.length}`);
      }
      if (summary.close_errors && summary.close_errors.length) {
        parts.push(`平仓错误 ${summary.close_errors.length}`);
      }
      return parts.join(" · ");
    }

    function renderCharts(data) {
      const tradeSeries = ((data.trade_summary && data.trade_summary.series) || []).map((item) => ({
        notional: Number(item.cumulative_notional || 0),
      }));
      tradeChartEl.innerHTML = makeSparkline(tradeSeries, "notional", "#0b6f68", "rgba(11,111,104,0.14)");

      const eventSeries = ((((data.local || {}).loop_summary || {}).recent) || []).filter((item) => item && item.mid_price !== undefined);
      loopChartEl.innerHTML = makeSparkline(eventSeries, "mid_price", "#8b5a2b", "rgba(139,90,43,0.12)");
    }

    async function loadMonitor() {
      if (monitorLoadPromise) {
        return monitorLoadPromise;
      }
      monitorLoadPromise = (async () => {
        const symbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
        if (!symbol) {
          metaEl.textContent = "监控币种列表为空，请先到策略总览页添加币种。";
          return;
        }
        metaEl.textContent = `正在刷新 ${symbol} ...`;
        try {
          const data = await fetchMonitorSnapshot(symbol);
          populatePresetOptions(data);
          renderCards(data);
          renderAlerts(data);
          renderPosition(data);
          renderOpenOrders(data);
          renderHourlyStats(data);
          renderTrades(data);
          renderEvents(data);
          renderCharts(data);
          if (!runnerParamsEditorEl.value.trim()) {
            await loadRunningConfigToEditor(false);
          }
          const warnings = (data.warnings || []).join(" | ");
          metaEl.textContent = `最后刷新: ${fmtTs(data.ts)}${warnings ? ` · 警告: ${warnings}` : ""}`;
        } catch (err) {
          alertBoxEl.innerHTML = '<div class="alert-empty">当前没拿到监控快照，所以还无法判断是参数问题、账户问题还是 Binance 接口异常。</div>';
          metaEl.textContent = `刷新失败: ${err}`;
        }
      })();
      try {
        await monitorLoadPromise;
      } finally {
        monitorLoadPromise = null;
      }
    }

    async function controlStrategy(action) {
      if (strategyActionPending) return;
      const selectedPreset = getPresetByKey(strategyPresetEl.value);
      if (action === "start" && (!selectedPreset || !selectedPreset.startable)) {
        strategyActionMetaEl.textContent = selectedPreset
          ? `${selectedPreset.label} 当前是模板预设，页面已展示参数，但还不能直接启动。`
          : "请选择可启动的策略预设";
        return;
      }
      strategyActionPending = true;
      strategyActionMetaEl.textContent = action === "start" ? "正在启动策略..." : "正在停止策略...";
      startStrategyBtn.disabled = true;
      stopStrategyBtn.disabled = true;
      try {
        const selectedSymbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
        const payload = action === "start"
          ? {
              ...(selectedPreset ? (selectedPreset.config || {}) : {}),
              symbol: selectedSymbol,
              strategy_profile: selectedPreset ? selectedPreset.key : ((((latestMonitorData || {}).runner || {}).config || {}).strategy_profile || "volume_long_v4"),
            }
          : {
              symbol: selectedSymbol,
              cancel_open_orders: Boolean(stopCancelOrdersEl && stopCancelOrdersEl.checked),
              close_all_positions: Boolean(stopClosePositionsEl && stopClosePositionsEl.checked),
            };
        const resp = await fetch(`/api/runner/${action}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        strategyActionMetaEl.textContent = action === "start"
          ? `策略已启动${data.restarted ? "（已重启应用新配置）" : (data.already_running ? "（已在运行）" : "")}`
          : `策略已停止${data.already_stopped ? "（原本就未运行）" : ""}${formatStopActionSummary(data.post_stop_actions) ? ` · ${formatStopActionSummary(data.post_stop_actions)}` : ""}`;
      } catch (err) {
        strategyActionMetaEl.textContent = `${action === "start" ? "启动" : "停止"}失败: ${err}`;
      } finally {
        strategyActionPending = false;
        await loadMonitor();
      }
    }

    function restartTimer() {
      if (timer) clearInterval(timer);
      timer = null;
      if (paused) return;
      const sec = Math.max(2, Number(refreshSecEl.value || 5));
      timer = setInterval(loadMonitor, sec * 1000);
    }

    refreshBtn.addEventListener("click", () => {
      loadMonitor();
      restartTimer();
    });
    toggleBtn.addEventListener("click", () => {
      paused = !paused;
      toggleBtn.textContent = paused ? "恢复自动刷新" : "暂停自动刷新";
      restartTimer();
    });
    startStrategyBtn.addEventListener("click", () => controlStrategy("start"));
    stopStrategyBtn.addEventListener("click", () => controlStrategy("stop"));
    refreshSecEl.addEventListener("change", restartTimer);
    strategyPresetEl.addEventListener("change", () => renderPresetMeta(latestMonitorData));
    loadRunningParamsBtn.addEventListener("click", loadRunningConfigToEditor);
    loadPresetParamsBtn.addEventListener("click", loadPresetConfigToEditor);
    applyParamsBtn.addEventListener("click", applyRunnerParams);
    runnerParamsEditorEl.addEventListener("input", syncRunnerParamGuideFromEditor);
    [monitorRunStartTimeEl, monitorRunEndTimeEl, monitorRollingHourlyLossLimitEl, monitorMaxCumulativeNotionalEl]
      .forEach((el) => el.addEventListener("change", syncRuntimeGuardInputsToEditor));
    alertBoxEl.addEventListener("click", async (event) => {
      const button = event.target.closest("[data-alert-action]");
      if (!button) return;
      const action = String(button.getAttribute("data-alert-action") || "").trim();
      const code = String(button.getAttribute("data-alert-code") || "").trim();
      if (!action || !code) return;
      button.disabled = true;
      try {
        if (action === "locate") {
          await locateAlertParams(code);
        } else if (action === "suggest") {
          await applyAlertSuggestion(code);
        }
      } catch (err) {
        runnerParamsMetaEl.textContent = `告警动作失败: ${err}`;
      } finally {
        button.disabled = false;
      }
    });
    customGridPreviewBtn.addEventListener("click", runCustomGridPreview);
    customGridSaveBtn.addEventListener("click", saveCustomGridStrategy);
    customGridLoadBtn.addEventListener("click", loadCustomGridPresetToForm);
    customGridUpdateBtn.addEventListener("click", updateCustomGridStrategy);
    customGridDeleteBtn.addEventListener("click", deleteCustomGridStrategy);
    symbolEl.addEventListener("change", () => {
      clearCustomGridPreview();
      loadMonitor();
      restartTimer();
    });

    async function initMonitorPage() {
      await loadMonitorSymbols(symbolEl.value);
      clearCustomGridPreview();
      await loadMonitor();
      restartTimer();
    }

    initMonitorPage();
  </script>
</body>
</html>
"""


SPOT_RUNNER_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>现货比赛执行台</title>
  <style>
    :root {
      --bg: #f6f3ed;
      --panel: #ffffff;
      --line: #e3dbcd;
      --text: #1d1d1d;
      --muted: #6d675d;
      --brand: #0f766e;
      --brand-soft: #e7f6f4;
      --good: #0f7b45;
      --warn: #b76e00;
      --bad: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      color: var(--text);
      background: radial-gradient(circle at top left, #fffef8 0%, var(--bg) 48%, #ece7dc 100%);
    }
    .wrap { max-width: 1480px; margin: 24px auto 48px; padding: 0 16px; display: grid; gap: 16px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 34px rgba(16, 24, 40, 0.05);
    }
    .header h1 { margin: 0 0 6px; font-size: 30px; }
    .header p { margin: 0; color: var(--muted); font-size: 14px; line-height: 1.6; }
    .header-links, .toolbar, .actions { margin-top: 12px; display: flex; gap: 10px; flex-wrap: wrap; }
    .header-links a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      height: 38px;
      padding: 0 14px;
      border-radius: 10px;
      border: 1px solid var(--line);
      text-decoration: none;
      color: #0f423f;
      background: var(--brand-soft);
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }
    button.primary {
      background: var(--brand);
      border-color: var(--brand);
      color: #fff;
    }
    .toolbar label, .fields label {
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 12px;
      color: var(--muted);
      min-width: 150px;
    }
    .toolbar input, .toolbar select, .fields input, .fields select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      background: #fff;
      color: var(--text);
      font-size: 14px;
    }
    .meta, .hint { color: var(--muted); font-size: 13px; }
    .summary {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
    }
    .metric {
      background: linear-gradient(180deg, #fffefc 0%, #faf8f2 100%);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      min-height: 108px;
    }
    .metric .label { font-size: 12px; color: var(--muted); margin-bottom: 10px; }
    .metric .value { font-size: 24px; font-weight: 800; letter-spacing: -0.02em; }
    .metric .sub { font-size: 12px; color: var(--muted); margin-top: 8px; line-height: 1.5; }
    .good { color: var(--good); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .table-wrap { overflow: auto; border: 1px solid var(--line); border-radius: 12px; }
    .fields {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-top: 14px;
    }
    .preview-summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 12px;
      margin-bottom: 12px;
    }
    .pill {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
      background: #fcfffe;
    }
    .pill .k { font-size: 12px; color: var(--muted); margin-bottom: 4px; }
    .pill .v { font-size: 16px; font-weight: 700; color: #0c4b46; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; min-width: 720px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { font-size: 12px; color: var(--muted); font-weight: 700; }
    .empty {
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 12px;
      color: var(--muted);
      font-size: 13px;
      text-align: center;
    }
    .hidden { display: none; }
    @media (max-width: 1180px) {
      .summary { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .fields { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid-2 { grid-template-columns: 1fr; }
    }
    @media (max-width: 680px) {
      .summary, .fields, .preview-summary { grid-template-columns: 1fr; }
      .toolbar label { min-width: 100%; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>现货比赛执行台</h1>
      <p>隔离于现有合约 runner。支持 `Spot V1 单向做多静态网格` 与 `现货量优先移中心` 两种模式，页面直接展示累计成交额、已实现收益、回收损耗、库存与停买状态。</p>
      <div class="header-links">
        <a href="/">返回测算页</a>
        <a href="/monitor">打开合约实盘监控</a>
        <a href="/basis">打开现货/合约价差监控</a>
        <a href="/spot_strategies">打开现货总览</a>
        <a href="/strategies">打开策略总览</a>
      </div>
      <div class="toolbar">
        <label>交易对
          <select id="symbol"></select>
        </label>
        <label>手动筛选 / 直接输入
          <input id="symbol_search" type="text" placeholder="输入币种，如 BARDUSDT" spellcheck="false" />
        </label>
        <label>策略模式
          <select id="strategy_mode">
            <option value="spot_volume_shift_long">现货量优先移中心</option>
            <option value="spot_one_way_long">Spot V1 单向做多静态网格</option>
          </select>
        </label>
        <label>自动刷新秒数
          <input id="refresh_seconds" type="number" min="2" step="1" value="5" />
        </label>
        <label>循环秒数
          <input id="sleep_seconds" type="number" min="2" step="1" value="10" />
        </label>
        <button id="preview_btn">预览静态网格</button>
        <button id="refresh_btn">刷新状态</button>
        <button id="start_btn" class="primary">启动策略</button>
        <button id="stop_btn">停止策略</button>
        <button id="toggle_btn">暂停自动刷新</button>
      </div>
      <div class="fields" id="static_fields">
        <label>价格网格
          <select id="grid_level_mode">
            <option value="arithmetic">等差</option>
            <option value="geometric">等比</option>
          </select>
        </label>
        <label>最低价
          <input id="min_price" type="number" min="0" step="0.0000001" />
        </label>
        <label>最高价
          <input id="max_price" type="number" min="0" step="0.0000001" />
        </label>
        <label>网格数 N
          <input id="n" type="number" min="1" step="1" />
        </label>
        <label>总预算（quote）
          <input id="total_quote_budget" type="number" min="0" step="0.01" />
        </label>
      </div>
      <div class="fields" id="shift_fields">
        <label>总预算（quote）
          <input id="shift_total_quote_budget" type="number" min="0" step="0.01" />
        </label>
        <label>带宽比例
          <input id="grid_band_ratio" type="number" min="0.001" step="0.001" />
        </label>
        <label>攻击买/卖格
          <input id="attack_levels" type="text" />
        </label>
        <label>攻击单笔金额
          <input id="attack_per_order_notional" type="number" min="0.01" step="0.01" />
        </label>
        <label>防守买/卖格
          <input id="defense_levels" type="text" />
        </label>
        <label>防守单笔金额
          <input id="defense_per_order_notional" type="number" min="0.01" step="0.01" />
        </label>
        <label>库存软/硬上限
          <input id="inventory_limits" type="text" />
        </label>
        <label>下移触发/步长
          <input id="center_shift_ratios" type="text" />
        </label>
        <label>停买阈值（振幅/跌幅）
          <input id="pause_thresholds" type="text" />
        </label>
        <label>回收年龄/损耗
          <input id="recycle_settings" type="text" />
        </label>
      </div>
      <div class="fields" id="runtime_guard_fields">
        <label>起始时间
          <input id="run_start_time" type="datetime-local" />
        </label>
        <label>结束时间
          <input id="run_end_time" type="datetime-local" />
        </label>
        <label>滚动 60 分钟亏损阈值
          <input id="rolling_hourly_loss_limit" type="number" min="0" step="0.01" />
        </label>
        <label>累计成交额阈值
          <input id="max_cumulative_notional" type="number" min="0" step="0.01" />
        </label>
      </div>
      <div class="actions">
        <span id="status" class="meta">等待状态加载...</span>
      </div>
      <p class="hint">说明：现货量优先移中心模式会根据成交和库存动态调整买卖；停止时只撤掉这个 spot runner 自己挂出的单，不会碰你账户里的其他现货委托。</p>
    </section>

    <section class="card">
      <div id="summary" class="summary"></div>
    </section>

    <section class="grid-2">
      <section class="card">
        <h2>风险与库存</h2>
        <div id="risk_summary" class="preview-summary"></div>
      </section>
      <section class="card" id="preview_card">
        <h2>静态网格预览</h2>
        <div id="preview_summary" class="preview-summary"></div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>格子</th>
                <th>买入价</th>
                <th>卖出价</th>
                <th>目标数量</th>
                <th>当前持有</th>
                <th>当前动作</th>
              </tr>
            </thead>
            <tbody id="preview_body"></tbody>
          </table>
        </div>
      </section>
    </section>

    <section class="grid-2">
      <section class="card">
        <h2>策略挂单</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>方向</th>
                <th>价格</th>
                <th>数量</th>
                <th>名义</th>
                <th>客户端ID</th>
              </tr>
            </thead>
            <tbody id="orders_body"></tbody>
          </table>
        </div>
      </section>
      <section class="card">
        <h2>最近成交</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>方向</th>
                <th>价格</th>
                <th>数量</th>
                <th>成交额</th>
                <th>已实现</th>
                <th>角色</th>
              </tr>
            </thead>
            <tbody id="trades_body"></tbody>
          </table>
        </div>
      </section>
    </section>

    <section class="card">
      <h2>最近循环</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>轮次</th>
              <th>模式</th>
              <th>中价</th>
              <th>库存(base)</th>
              <th>累计成交额</th>
              <th>已实现 / 浮盈</th>
              <th>买/卖委托</th>
              <th>补单/撤单</th>
            </tr>
          </thead>
          <tbody id="events_body"></tbody>
        </table>
      </div>
    </section>
  </div>

  <script>
    const symbolEl = document.getElementById("symbol");
    const symbolSearchEl = document.getElementById("symbol_search");
    const strategyModeEl = document.getElementById("strategy_mode");
    const gridLevelModeEl = document.getElementById("grid_level_mode");
    const minPriceEl = document.getElementById("min_price");
    const maxPriceEl = document.getElementById("max_price");
    const nEl = document.getElementById("n");
    const totalQuoteBudgetEl = document.getElementById("total_quote_budget");
    const shiftTotalQuoteBudgetEl = document.getElementById("shift_total_quote_budget");
    const gridBandRatioEl = document.getElementById("grid_band_ratio");
    const attackLevelsEl = document.getElementById("attack_levels");
    const attackPerOrderNotionalEl = document.getElementById("attack_per_order_notional");
    const defenseLevelsEl = document.getElementById("defense_levels");
    const defensePerOrderNotionalEl = document.getElementById("defense_per_order_notional");
    const inventoryLimitsEl = document.getElementById("inventory_limits");
    const centerShiftRatiosEl = document.getElementById("center_shift_ratios");
    const pauseThresholdsEl = document.getElementById("pause_thresholds");
    const recycleSettingsEl = document.getElementById("recycle_settings");
    const runStartTimeEl = document.getElementById("run_start_time");
    const runEndTimeEl = document.getElementById("run_end_time");
    const rollingHourlyLossLimitEl = document.getElementById("rolling_hourly_loss_limit");
    const maxCumulativeNotionalEl = document.getElementById("max_cumulative_notional");
    const sleepSecondsEl = document.getElementById("sleep_seconds");
    const refreshSecondsEl = document.getElementById("refresh_seconds");
    const staticFieldsEl = document.getElementById("static_fields");
    const shiftFieldsEl = document.getElementById("shift_fields");
    const previewCardEl = document.getElementById("preview_card");
    const previewBtn = document.getElementById("preview_btn");
    const refreshBtn = document.getElementById("refresh_btn");
    const startBtn = document.getElementById("start_btn");
    const stopBtn = document.getElementById("stop_btn");
    const toggleBtn = document.getElementById("toggle_btn");
    const statusEl = document.getElementById("status");
    const summaryEl = document.getElementById("summary");
    const riskSummaryEl = document.getElementById("risk_summary");
    const previewSummaryEl = document.getElementById("preview_summary");
    const previewBody = document.getElementById("preview_body");
    const ordersBody = document.getElementById("orders_body");
    const tradesBody = document.getElementById("trades_body");
    const eventsBody = document.getElementById("events_body");

    let latestSnapshot = null;
    let latestPreview = null;
    let availableSpotSymbols = [];
    let timer = null;
    let paused = false;
    let actionPending = false;

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      return Number(v).toLocaleString("zh-CN", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v) * 100;
      return `${num > 0 ? "+" : ""}${num.toFixed(2)}%`;
    }

    function fmtMoney(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v);
      return `${num > 0 ? "+" : ""}${fmtNum(num, 4)}`;
    }

    function fmtTs(v) {
      if (!v) return "--";
      const dt = new Date(v);
      if (Number.isNaN(dt.getTime())) return String(v);
      return dt.toLocaleString("zh-CN", { hour12: false });
    }

    function toLocalInputValue(value) {
      if (!value) return "";
      const dt = new Date(value);
      if (Number.isNaN(dt.getTime())) return "";
      const pad = (num) => String(num).padStart(2, "0");
      return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
    }

    function fromLocalInputValue(value) {
      const raw = String(value || "").trim();
      if (!raw) return null;
      const dt = new Date(raw);
      if (Number.isNaN(dt.getTime())) return null;
      return dt.toISOString();
    }

    function escapeHtml(s) {
      return String(s ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function statusClass(v) {
      const num = Number(v || 0);
      if (num > 0) return "good";
      if (num < 0) return "bad";
      return "";
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "meta bad" : "meta";
    }

    function parsePair(text, fallbackA, fallbackB) {
      const raw = String(text || "").replaceAll("，", ",").replaceAll("/", ",");
      const parts = raw.split(",").map((x) => x.trim()).filter(Boolean);
      return [Number(parts[0] || fallbackA), Number(parts[1] || fallbackB)];
    }

    function normalizeSymbol(value) {
      return String(value || "").trim().toUpperCase();
    }

    function getExactSymbolMatch(value) {
      const normalized = normalizeSymbol(value);
      if (!normalized) return "";
      return availableSpotSymbols.find((symbol) => symbol === normalized) || "";
    }

    function getSelectedSymbol() {
      return getExactSymbolMatch(symbolSearchEl.value) || normalizeSymbol(symbolEl.value) || "BARDUSDT";
    }

    function readForm() {
      const strategyMode = String(strategyModeEl.value || "spot_volume_shift_long");
      const [attackBuyLevels, attackSellLevels] = parsePair(attackLevelsEl.value, 14, 22);
      const [defenseBuyLevels, defenseSellLevels] = parsePair(defenseLevelsEl.value, 8, 20);
      const [softLimit, hardLimit] = parsePair(inventoryLimitsEl.value, 350, 500);
      const [shiftTrigger, shiftStep] = parsePair(centerShiftRatiosEl.value, 0.012, 0.006);
      const [pauseAmp, pauseDown] = parsePair(pauseThresholdsEl.value, 0.045, -0.022);
      const [recycleAge, recycleLoss] = parsePair(recycleSettingsEl.value, 40, 0.006);
      return {
        symbol: getSelectedSymbol(),
        strategy_mode: strategyMode,
        grid_level_mode: String(gridLevelModeEl.value || "arithmetic"),
        min_price: Number(minPriceEl.value || 0),
        max_price: Number(maxPriceEl.value || 0),
        n: Number(nEl.value || 0),
        total_quote_budget: Number(strategyMode === "spot_one_way_long" ? totalQuoteBudgetEl.value : shiftTotalQuoteBudgetEl.value || 0),
        sleep_seconds: Number(sleepSecondsEl.value || 10),
        cancel_stale: true,
        apply: true,
        reset_state: true,
        grid_band_ratio: Number(gridBandRatioEl.value || 0.045),
        attack_buy_levels: Number(attackBuyLevels || 14),
        attack_sell_levels: Number(attackSellLevels || 22),
        attack_per_order_notional: Number(attackPerOrderNotionalEl.value || 20),
        defense_buy_levels: Number(defenseBuyLevels || 8),
        defense_sell_levels: Number(defenseSellLevels || 20),
        defense_per_order_notional: Number(defensePerOrderNotionalEl.value || 12),
        inventory_soft_limit_notional: Number(softLimit || 350),
        inventory_hard_limit_notional: Number(hardLimit || 500),
        center_shift_trigger_ratio: Number(shiftTrigger || 0.012),
        center_shift_confirm_cycles: 3,
        center_shift_step_ratio: Number(shiftStep || 0.006),
        buy_pause_amp_trigger_ratio: Number(pauseAmp || 0.045),
        buy_pause_down_return_trigger_ratio: Number(pauseDown || -0.022),
        freeze_shift_abs_return_trigger_ratio: 0.03,
        inventory_recycle_age_minutes: Number(recycleAge || 40),
        inventory_recycle_loss_tolerance_ratio: Number(recycleLoss || 0.006),
        inventory_recycle_min_profit_ratio: 0.001,
        max_single_cycle_new_orders: 8,
        run_start_time: fromLocalInputValue(runStartTimeEl.value),
        run_end_time: fromLocalInputValue(runEndTimeEl.value),
        rolling_hourly_loss_limit: rollingHourlyLossLimitEl.value ? Number(rollingHourlyLossLimitEl.value) : null,
        max_cumulative_notional: maxCumulativeNotionalEl.value ? Number(maxCumulativeNotionalEl.value) : null,
      };
    }

    function populateSymbols(symbols, preferred) {
      const normalizedSymbols = Array.from(new Set((symbols || []).map((symbol) => normalizeSymbol(symbol)).filter(Boolean)));
      availableSpotSymbols = normalizedSymbols;
      const prev = normalizeSymbol(symbolEl.value);
      const manual = normalizeSymbol(symbolSearchEl.value);
      const filteredSymbols = manual
        ? normalizedSymbols.filter((symbol) => symbol.includes(manual))
        : normalizedSymbols;
      const target = getExactSymbolMatch(preferred) || getExactSymbolMatch(manual) || prev || "BARDUSDT";
      if (!filteredSymbols.length) {
        symbolEl.innerHTML = '<option value="">没有匹配币种</option>';
        symbolEl.value = "";
        return;
      }
      symbolEl.innerHTML = filteredSymbols.map((symbol) => `<option value="${symbol}">${symbol}</option>`).join("");
      if (filteredSymbols.includes(target)) {
        symbolEl.value = target;
      } else if (filteredSymbols.includes("BARDUSDT")) {
        symbolEl.value = "BARDUSDT";
      } else if (filteredSymbols.includes("BTCUSDT")) {
        symbolEl.value = "BTCUSDT";
      } else if (filteredSymbols.length) {
        symbolEl.value = filteredSymbols[0];
      }
    }

    async function loadSymbols() {
      try {
        const resp = await fetch("/api/symbols?market_type=spot");
        const data = await resp.json();
        if (!resp.ok || !data.ok || !Array.isArray(data.symbols)) {
          throw new Error(data.error || `HTTP ${resp.status}`);
        }
        populateSymbols(data.symbols, getSelectedSymbol() || (latestSnapshot && latestSnapshot.symbol) || "BARDUSDT");
      } catch (err) {
        populateSymbols(["BARDUSDT", "SAHARAUSDT", "NIGHTUSDT", "CFGUSDT"], getSelectedSymbol() || (latestSnapshot && latestSnapshot.symbol) || "BARDUSDT");
        setStatus(`现货交易对加载失败，已使用默认列表：${err}`, true);
      }
    }

    function toggleModeFields() {
      const isStatic = String(strategyModeEl.value || "") === "spot_one_way_long";
      staticFieldsEl.classList.toggle("hidden", !isStatic);
      shiftFieldsEl.classList.toggle("hidden", isStatic);
      previewCardEl.classList.toggle("hidden", !isStatic);
      previewBtn.disabled = !isStatic;
    }

    function renderSummary(snapshot) {
      const runner = (snapshot && snapshot.runner) || {};
      const config = (snapshot && snapshot.config) || {};
      const balances = (snapshot && snapshot.balances) || {};
      const market = (snapshot && snapshot.market) || {};
      const state = (snapshot && snapshot.state) || {};
      const trade = (snapshot && snapshot.trade_summary) || {};
      const risk = (snapshot && snapshot.risk_controls) || {};
      const warnings = Array.isArray(snapshot && snapshot.warnings) ? snapshot.warnings : [];
      const cards = [
        ["策略状态", runner.is_running ? "运行中" : "未运行", runner.is_running ? "good" : "warn", `PID: ${runner.pid || "--"} · 模式: ${risk.mode || config.strategy_mode || "--"}`],
        ["累计成交额", fmtNum(trade.gross_notional, 4), "", `买/卖: ${fmtNum(trade.buy_notional, 4)} / ${fmtNum(trade.sell_notional, 4)}`],
        ["净收益估算", fmtMoney(trade.net_pnl_estimate), statusClass(trade.net_pnl_estimate), `已实现: ${fmtMoney(trade.realized_pnl)} · 浮盈: ${fmtMoney(trade.unrealized_pnl)}`],
        ["手续费 / 回收损耗", `${fmtMoney(-(trade.commission_quote || 0))} / ${fmtMoney(-(trade.recycle_loss_abs || 0))}`, "bad", `回收已实现: ${fmtMoney(trade.recycle_realized_pnl || 0)}`],
        ["当前库存", `${fmtNum(state.inventory_qty || state.managed_base_qty, 8)} ${escapeHtml(balances.base_asset || "BASE")}`, "", `均价: ${fmtNum(state.inventory_avg_cost, 8)} · 中心: ${fmtNum(risk.center_price, 8)}`],
        ["账户余额", `${fmtNum(balances.quote_free, 4)} ${escapeHtml(balances.quote_asset || "QUOTE")}`, "", `Base: ${fmtNum(balances.base_free, 8)} ${escapeHtml(balances.base_asset || "BASE")} · 警告: ${warnings.join(" | ") || "无"}`],
      ];
      summaryEl.innerHTML = cards.map(([label, value, cls, sub]) => `
        <div class="metric">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value ${cls}">${escapeHtml(value)}</div>
          <div class="sub">${escapeHtml(sub)}</div>
        </div>
      `).join("");

      const riskPills = [
        ["风控模式", risk.mode || "--"],
        ["运行状态", risk.runtime_status || "--"],
        ["停买", risk.buy_paused ? "是" : "否"],
        ["软/硬库存上限", `${fmtNum(risk.inventory_soft_limit_notional, 2)} / ${fmtNum(risk.inventory_hard_limit_notional, 2)}`],
        ["1m 涨跌 / 振幅", `${fmtPct(risk.market_guard_return_ratio)} / ${fmtPct(risk.market_guard_amplitude_ratio)}`],
        ["滚动亏损 / 阈值", `${fmtMoney(-(risk.rolling_hourly_loss || 0))} / ${fmtNum(risk.rolling_hourly_loss_limit, 2)}`],
        ["累计成交额 / 阈值", `${fmtNum(risk.cumulative_gross_notional, 2)} / ${fmtNum(risk.max_cumulative_notional, 2)}`],
        ["停止原因", risk.stop_reason || "--"],
      ];
      riskSummaryEl.innerHTML = riskPills.map(([k, v]) => `<div class="pill"><div class="k">${escapeHtml(k)}</div><div class="v">${escapeHtml(v)}</div></div>`).join("");
      startBtn.textContent = runner.is_running ? "重启策略" : "启动策略";
      stopBtn.disabled = actionPending || !runner.is_running;
      startBtn.disabled = actionPending;
    }

    function renderPreview(preview, snapshot) {
      latestPreview = preview;
      const summary = (preview && preview.summary) || {};
      const rows = Array.isArray(preview && preview.rows) ? preview.rows : [];
      const stateCells = (((snapshot || {}).state || {}).cells) || {};
      const pills = [
        ["当前价格", fmtNum(summary.current_price, 8)],
        ["预算", fmtNum(summary.position_budget_notional, 4)],
        ["活动买/卖", `${fmtNum(summary.active_buy_orders, 0)} / ${fmtNum(summary.active_sell_orders, 0)}`],
        ["提示", Array.isArray(summary.warnings) && summary.warnings.length ? summary.warnings.join("；") : "静态模式不自动预建底仓"],
      ];
      previewSummaryEl.innerHTML = pills.map(([k, v]) => `<div class="pill"><div class="k">${escapeHtml(k)}</div><div class="v">${escapeHtml(v)}</div></div>`).join("");
      if (!rows.length) {
        previewBody.innerHTML = '<tr><td colspan="6" class="empty">当前参数下没有有效网格</td></tr>';
        return;
      }
      previewBody.innerHTML = rows.map((row) => {
        const cellState = stateCells[String(row.idx)] || {};
        const heldQty = Number(cellState.position_qty || 0);
        const action = heldQty > 0 ? `SELL @ ${fmtNum(row.exit_price, 8)}` : (row.active_order_side ? `${row.active_order_side} @ ${fmtNum(row.active_order_price, 8)}` : "等待激活");
        return `
          <tr>
            <td>${fmtNum(row.idx, 0)}</td>
            <td>${fmtNum(row.entry_price, 8)}</td>
            <td>${fmtNum(row.exit_price, 8)}</td>
            <td>${fmtNum(row.qty, 8)}</td>
            <td>${fmtNum(heldQty, 8)}</td>
            <td>${escapeHtml(action)}</td>
          </tr>
        `;
      }).join("");
    }

    function renderOrders(snapshot) {
      const rows = Array.isArray(snapshot && snapshot.open_orders) ? snapshot.open_orders : [];
      if (!rows.length) {
        ordersBody.innerHTML = '<tr><td colspan="5" class="empty">当前没有策略挂单</td></tr>';
        return;
      }
      ordersBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${escapeHtml(row.side || "--")}</td>
          <td>${fmtNum(row.price, 8)}</td>
          <td>${fmtNum(row.origQty, 8)}</td>
          <td>${fmtNum(Number(row.price || 0) * Number(row.origQty || 0), 4)}</td>
          <td>${escapeHtml(row.clientOrderId || "--")}</td>
        </tr>
      `).join("");
    }

    function renderTrades(snapshot) {
      const rows = (((snapshot || {}).trade_summary || {}).recent_trades || []).slice().reverse();
      if (!rows.length) {
        tradesBody.innerHTML = '<tr><td colspan="7" class="empty">当前还没有策略成交</td></tr>';
        return;
      }
      tradesBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${fmtTs(row.time)}</td>
          <td>${escapeHtml(row.side || "--")}</td>
          <td>${fmtNum(row.price, 8)}</td>
          <td>${fmtNum(row.qty, 8)}</td>
          <td>${fmtNum(row.notional, 4)}</td>
          <td class="${statusClass(row.realized_pnl)}">${fmtMoney(row.realized_pnl)}</td>
          <td>${escapeHtml(row.role || "--")}</td>
        </tr>
      `).join("");
    }

    function renderEvents(snapshot) {
      const rows = Array.isArray(snapshot && snapshot.events) ? snapshot.events.slice().reverse() : [];
      if (!rows.length) {
        eventsBody.innerHTML = '<tr><td colspan="9" class="empty">还没有现货 runner 循环记录</td></tr>';
        return;
      }
      eventsBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${fmtTs(row.ts)}</td>
          <td>${fmtNum(row.cycle, 0)}</td>
          <td>${escapeHtml(row.mode || row.strategy_mode || "--")}</td>
          <td>${fmtNum(row.mid_price, 8)}</td>
          <td>${fmtNum(row.inventory_qty || row.managed_base_qty, 8)}</td>
          <td>${fmtNum(row.gross_notional, 4)}</td>
          <td class="${statusClass((row.realized_pnl || 0) + (row.unrealized_pnl || 0))}">${fmtMoney(row.realized_pnl)} / ${fmtMoney(row.unrealized_pnl)}</td>
          <td>${fmtNum(row.active_buy_orders, 0)} / ${fmtNum(row.active_sell_orders, 0)}</td>
          <td>${fmtNum(row.placed_count, 0)} / ${fmtNum(row.canceled_count, 0)}</td>
        </tr>
      `).join("");
    }

    function syncFormFromConfig(snapshot) {
      const config = (snapshot && snapshot.config) || {};
      strategyModeEl.value = String(config.strategy_mode || "spot_volume_shift_long");
      if (config.grid_level_mode) gridLevelModeEl.value = String(config.grid_level_mode);
      if (config.min_price !== undefined) minPriceEl.value = String(config.min_price);
      if (config.max_price !== undefined) maxPriceEl.value = String(config.max_price);
      if (config.n !== undefined) nEl.value = String(config.n);
      if (config.total_quote_budget !== undefined) {
        totalQuoteBudgetEl.value = String(config.total_quote_budget);
        shiftTotalQuoteBudgetEl.value = String(config.total_quote_budget);
      }
      if (config.sleep_seconds !== undefined) sleepSecondsEl.value = String(config.sleep_seconds);
      gridBandRatioEl.value = String(config.grid_band_ratio ?? 0.045);
      attackLevelsEl.value = `${config.attack_buy_levels ?? 14},${config.attack_sell_levels ?? 22}`;
      attackPerOrderNotionalEl.value = String(config.attack_per_order_notional ?? 20);
      defenseLevelsEl.value = `${config.defense_buy_levels ?? 8},${config.defense_sell_levels ?? 20}`;
      defensePerOrderNotionalEl.value = String(config.defense_per_order_notional ?? 12);
      inventoryLimitsEl.value = `${config.inventory_soft_limit_notional ?? 350},${config.inventory_hard_limit_notional ?? 500}`;
      centerShiftRatiosEl.value = `${config.center_shift_trigger_ratio ?? 0.012},${config.center_shift_step_ratio ?? 0.006}`;
      pauseThresholdsEl.value = `${config.buy_pause_amp_trigger_ratio ?? 0.045},${config.buy_pause_down_return_trigger_ratio ?? -0.022}`;
      recycleSettingsEl.value = `${config.inventory_recycle_age_minutes ?? 40},${config.inventory_recycle_loss_tolerance_ratio ?? 0.006}`;
      runStartTimeEl.value = toLocalInputValue(config.run_start_time);
      runEndTimeEl.value = toLocalInputValue(config.run_end_time);
      rollingHourlyLossLimitEl.value = config.rolling_hourly_loss_limit ?? "";
      maxCumulativeNotionalEl.value = config.max_cumulative_notional ?? "";
      toggleModeFields();
    }

    async function loadStatus(silent = false) {
      try {
        const selectedSymbol = getSelectedSymbol();
        const qs = new URLSearchParams();
        if (selectedSymbol) qs.set("symbol", selectedSymbol);
        const resp = await fetch(`/api/spot_runner/status?${qs.toString()}`);
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        latestSnapshot = data.snapshot || {};
        syncFormFromConfig(latestSnapshot);
        await loadSymbols();
        renderSummary(latestSnapshot);
        renderOrders(latestSnapshot);
        renderTrades(latestSnapshot);
        renderEvents(latestSnapshot);
        if (latestPreview && String(strategyModeEl.value || "") === "spot_one_way_long") {
          renderPreview(latestPreview, latestSnapshot);
        }
        if (!silent) {
          const runner = (latestSnapshot && latestSnapshot.runner) || {};
          setStatus(`状态已刷新：${runner.is_running ? "运行中" : "未运行"} · ${latestSnapshot.symbol || "--"}`);
        }
      } catch (err) {
        if (!silent) setStatus(`状态刷新失败：${err}`, true);
      }
    }

    async function previewGrid() {
      const form = readForm();
      if (form.strategy_mode !== "spot_one_way_long") {
        setStatus("只有静态模式支持页面预览。", true);
        return;
      }
      setStatus(`正在生成 ${form.symbol} 的现货静态网格预览...`);
      previewBtn.disabled = true;
      try {
        const resp = await fetch("/api/grid_preview", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            market_type: "spot",
            contract_type: "",
            symbol: form.symbol,
            strategy_direction: "long",
            grid_level_mode: form.grid_level_mode,
            min_price: form.min_price,
            max_price: form.max_price,
            n: form.n,
            margin_amount: form.total_quote_budget,
            leverage: 1,
          }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        renderPreview(data, latestSnapshot);
        setStatus(`预览已更新：${form.symbol} · N=${form.n}`);
      } catch (err) {
        previewSummaryEl.innerHTML = "";
        previewBody.innerHTML = '<tr><td colspan="6" class="empty">预览失败</td></tr>';
        setStatus(`预览失败：${err}`, true);
      } finally {
        previewBtn.disabled = false;
      }
    }

    async function controlRunner(action) {
      if (actionPending) return;
      actionPending = true;
      startBtn.disabled = true;
      stopBtn.disabled = true;
      setStatus(action === "start" ? "正在启动现货策略..." : "正在停止现货策略...");
      try {
        const resp = await fetch(`/api/spot_runner/${action}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(action === "start" ? readForm() : { symbol: getSelectedSymbol() }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        if (action === "start") {
          const cleanup = (data.cleanup && data.cleanup.canceled) ? `，启动前已清理 ${data.cleanup.canceled} 笔旧单` : "";
          setStatus(`现货策略已启动${data.restarted ? "（已重启）" : ""}${cleanup}`);
        } else {
          const cleanup = (data.cleanup && data.cleanup.canceled) ? `，并撤掉 ${data.cleanup.canceled} 笔策略挂单` : "";
          setStatus(`现货策略已停止${data.already_stopped ? "（原本就未运行）" : ""}${cleanup}`);
        }
      } catch (err) {
        setStatus(`${action === "start" ? "启动" : "停止"}失败：${err}`, true);
      } finally {
        actionPending = false;
        await loadStatus(true);
      }
    }

    function restartTimer() {
      if (timer) clearInterval(timer);
      timer = null;
      if (paused) return;
      const sec = Math.max(2, Number(refreshSecondsEl.value || 5));
      timer = setInterval(() => loadStatus(true), sec * 1000);
    }

    previewBtn.addEventListener("click", previewGrid);
    refreshBtn.addEventListener("click", () => {
      loadStatus();
      restartTimer();
    });
    startBtn.addEventListener("click", () => controlRunner("start"));
    stopBtn.addEventListener("click", () => controlRunner("stop"));
    toggleBtn.addEventListener("click", () => {
      paused = !paused;
      toggleBtn.textContent = paused ? "恢复自动刷新" : "暂停自动刷新";
      restartTimer();
    });
    refreshSecondsEl.addEventListener("change", restartTimer);
    strategyModeEl.addEventListener("change", toggleModeFields);
    symbolEl.addEventListener("change", () => loadStatus());
    symbolSearchEl.addEventListener("input", () => {
      const cursor = symbolSearchEl.selectionStart;
      const normalized = normalizeSymbol(symbolSearchEl.value);
      symbolSearchEl.value = normalized;
      if (cursor !== null) symbolSearchEl.setSelectionRange(cursor, cursor);
      populateSymbols(availableSpotSymbols, normalized || symbolEl.value || "BARDUSDT");
      const exactMatch = getExactSymbolMatch(normalized);
      if (exactMatch) {
        symbolEl.value = exactMatch;
      }
    });
    symbolSearchEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        const exactMatch = getExactSymbolMatch(symbolSearchEl.value);
        if (exactMatch) {
          symbolEl.value = exactMatch;
          loadStatus();
        } else {
          setStatus(`没有找到完全匹配的现货币种：${normalizeSymbol(symbolSearchEl.value)}`, true);
        }
      }
    });

    toggleModeFields();
    loadSymbols().then(() => loadStatus());
    restartTimer();
  </script>
</body>
</html>
"""


SPOT_STRATEGIES_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>现货策略总览</title>
  <style>
    :root {
      --bg: #f4efe3;
      --panel: rgba(255,253,248,0.95);
      --line: #ddd4c3;
      --text: #22201a;
      --muted: #6c685f;
      --good: #0b6f68;
      --bad: #b04b37;
      --warn: #8b5a2b;
      --accent: #163d36;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top, #faf7ee 0%, var(--bg) 55%, #efe6d4 100%);
      color: var(--text);
      font-family: "SF Pro SC", "PingFang SC", "Helvetica Neue", sans-serif;
    }
    .wrap { max-width: 1440px; margin: 0 auto; padding: 24px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 20px;
      box-shadow: 0 10px 30px rgba(48, 37, 17, 0.06);
    }
    .hero h1 { margin: 0 0 8px; font-size: 32px; }
    .hero p, .meta { margin: 0; color: var(--muted); line-height: 1.6; }
    .links, .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 14px;
      align-items: center;
    }
    .links a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid #bfd4ca;
      background: #edf6f1;
      color: var(--accent);
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
    }
    label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
    }
    input {
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      font: inherit;
    }
    #symbols_input { width: min(580px, 70vw); }
    #refresh_sec { width: 90px; }
    #summary {
      margin: 16px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .grid {
      margin-top: 18px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }
    .strategy-card {
      display: grid;
      gap: 14px;
    }
    .topline {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .title h2 { margin: 0; font-size: 24px; }
    .title p { margin: 8px 0 0; color: var(--muted); font-size: 14px; line-height: 1.6; }
    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
    }
    .badge.good { background: rgba(11,111,104,0.12); color: var(--good); }
    .badge.warn { background: rgba(139,90,43,0.14); color: var(--warn); }
    .badge.bad { background: rgba(176,75,55,0.12); color: var(--bad); }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: linear-gradient(180deg, #fff 0%, #faf5ea 100%);
    }
    .metric .label { font-size: 12px; color: var(--muted); font-weight: 700; }
    .metric .value { margin-top: 6px; font-size: 26px; font-weight: 800; }
    .metric .sub { margin-top: 6px; font-size: 12px; color: var(--muted); line-height: 1.5; }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .sections {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .section {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: #fff;
    }
    .section h3 {
      margin: 0 0 10px;
      font-size: 15px;
    }
    .section p {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    .manager-grid {
      margin-top: 18px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }
    .manager-block {
      display: grid;
      gap: 12px;
    }
    .manager-block h2 {
      margin: 0;
      font-size: 18px;
    }
    .manager-input {
      width: min(260px, 100%);
    }
    .chip-list {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      font-size: 13px;
      font-weight: 700;
    }
    .chip button {
      padding: 6px 10px;
      font-size: 12px;
      border-radius: 999px;
    }
    .points {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
    }
    .point {
      padding: 6px 9px;
      border-radius: 999px;
      background: #f5efe3;
      border: 1px solid var(--line);
      font-size: 12px;
      font-weight: 700;
      color: #4f4738;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      padding: 9px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      width: 120px;
      color: var(--muted);
      font-weight: 700;
    }
    .empty {
      margin-top: 16px;
      padding: 24px;
      border: 1px dashed var(--line);
      border-radius: 18px;
      text-align: center;
      color: var(--muted);
      background: rgba(255,255,255,0.75);
    }
    @media (max-width: 1180px) {
      .grid { grid-template-columns: 1fr; }
      .manager-grid { grid-template-columns: 1fr; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .sections { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .metrics { grid-template-columns: 1fr; }
      .topline { flex-direction: column; }
      .badges { justify-content: flex-start; }
      #symbols_input { width: 100%; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card hero">
      <h1>现货策略总览</h1>
      <p>按币种汇总当前现货 runner 的累计成交额、净收益估算、库存、停买状态、当前挂单和最近成交，适合同时盯多个交易赛币种。</p>
      <div class="links">
        <a href="/">返回测算页</a>
        <a href="/spot_runner">打开现货执行台</a>
        <a href="/monitor">打开合约实盘监控</a>
        <a href="/strategies">打开合约总览</a>
      </div>
      <div class="toolbar">
        <label>币种列表
          <input id="symbols_input" type="text" value="SAHARAUSDT,NIGHTUSDT,CFGUSDT" />
        </label>
        <label>自动刷新（秒）
          <input id="refresh_sec" type="number" min="3" step="1" value="8" />
        </label>
        <button id="refresh_btn" class="primary">立即刷新</button>
        <button id="toggle_btn">暂停自动刷新</button>
      </div>
      <div id="meta" class="meta">等待首轮数据...</div>
      <div id="summary"></div>
    </section>
    <div id="cards" class="grid"></div>
  </div>

  <script>
    const cardsEl = document.getElementById("cards");
    const metaEl = document.getElementById("meta");
    const summaryEl = document.getElementById("summary");
    const refreshBtn = document.getElementById("refresh_btn");
    const toggleBtn = document.getElementById("toggle_btn");
    const refreshSecEl = document.getElementById("refresh_sec");
    const symbolsInputEl = document.getElementById("symbols_input");
    let timer = null;
    let paused = false;

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\\"/g, "&quot;");
    }

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      return Number(v).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }

    function fmtMoney(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v);
      return `${num > 0 ? "+" : ""}${fmtNum(num, 4)}`;
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v) * 100;
      return `${num > 0 ? "+" : ""}${num.toFixed(2)}%`;
    }

    function fmtTs(v) {
      if (!v) return "--";
      const dt = new Date(v);
      return Number.isNaN(dt.getTime()) ? String(v) : dt.toLocaleString("zh-CN", { hour12: false });
    }

    function statusClass(v) {
      const num = Number(v || 0);
      if (num > 0) return "good";
      if (num < 0) return "bad";
      return "warn";
    }

    function parseSymbols() {
      return String(symbolsInputEl.value || "")
        .split(",")
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
    }

    function getOrderedPoints(openOrders, side) {
      const rows = (openOrders || []).filter((item) => item.side === side);
      rows.sort((a, b) => side === "BUY" ? Number(b.price || 0) - Number(a.price || 0) : Number(a.price || 0) - Number(b.price || 0));
      return rows.slice(0, 6).map((item) => Number(item.price || 0));
    }

    function renderCard(snapshot) {
      const runner = snapshot.runner || {};
      const config = snapshot.config || {};
      const risk = snapshot.risk_controls || {};
      const state = snapshot.state || {};
      const trade = snapshot.trade_summary || {};
      const market = snapshot.market || {};
      const openOrders = snapshot.open_orders || [];
      const isRunning = Boolean(runner.is_running);
      const inventoryQty = Number(state.inventory_qty || state.managed_base_qty || 0);
      const hasExposure = inventoryQty > 0 || openOrders.length > 0;
      const stateLabel = isRunning ? "运行中" : (hasExposure ? "停机但仍有库存/挂单" : "未运行");
      const stateClass = isRunning ? "good" : (hasExposure ? "warn" : "bad");
      const buyPoints = getOrderedPoints(openOrders, "BUY");
      const sellPoints = getOrderedPoints(openOrders, "SELL");
      const latestTrade = ((trade.recent_trades || []).slice(-1)[0]) || {};
      return `
        <section class="card strategy-card">
          <div class="topline">
            <div class="title">
              <h2>${escapeHtml(snapshot.symbol || "--")}</h2>
              <p>模式 ${escapeHtml(risk.mode || config.strategy_mode || "--")} · 中心 ${escapeHtml(fmtNum(risk.center_price || state.center_price || 0, 8))} · 最近刷新 ${escapeHtml(fmtTs((snapshot.latest_event || {}).ts))}</p>
            </div>
            <div class="badges">
              <span class="badge ${stateClass}">${escapeHtml(stateLabel)}</span>
              <span class="badge warn">停买 ${risk.buy_paused ? "是" : "否"}</span>
              <span class="badge good">挂单 ${escapeHtml(fmtNum(openOrders.length, 0))}</span>
            </div>
          </div>
          <div class="metrics">
            <div class="metric">
              <div class="label">累计成交额</div>
              <div class="value">${escapeHtml(fmtNum(trade.gross_notional || 0, 4))}</div>
              <div class="sub">买/卖 ${escapeHtml(fmtNum(trade.buy_notional || 0, 4))} / ${escapeHtml(fmtNum(trade.sell_notional || 0, 4))}</div>
            </div>
            <div class="metric">
              <div class="label">净收益估算</div>
              <div class="value ${statusClass(Number(trade.net_pnl_estimate || 0))}">${escapeHtml(fmtMoney(trade.net_pnl_estimate || 0))}</div>
              <div class="sub">已实现 ${escapeHtml(fmtMoney(trade.realized_pnl || 0))} · 浮盈 ${escapeHtml(fmtMoney(trade.unrealized_pnl || 0))}</div>
            </div>
            <div class="metric">
              <div class="label">当前库存</div>
              <div class="value">${escapeHtml(fmtNum(inventoryQty, 8))}</div>
              <div class="sub">均价 ${escapeHtml(fmtNum(state.inventory_avg_cost || 0, 8))} · 名义 ${escapeHtml(fmtNum(inventoryQty * Number(market.mid_price || 0), 4))}</div>
            </div>
            <div class="metric">
              <div class="label">回收损耗 / 手续费</div>
              <div class="value bad">${escapeHtml(fmtMoney(-(trade.recycle_loss_abs || 0)))} / ${escapeHtml(fmtMoney(-(trade.commission_quote || 0)))}</div>
              <div class="sub">回收已实现 ${escapeHtml(fmtMoney(trade.recycle_realized_pnl || 0))}</div>
            </div>
          </div>
          <div class="sections">
            <div class="section">
              <h3>当前行为</h3>
              <p>风控模式 ${escapeHtml(risk.mode || "--")}，停买 ${risk.buy_paused ? "是" : "否"}，软/硬库存 ${escapeHtml(fmtNum(risk.inventory_soft_limit_notional || 0, 2))} / ${escapeHtml(fmtNum(risk.inventory_hard_limit_notional || 0, 2))}。</p>
              <p style="margin-top:8px;">1m 涨跌 ${escapeHtml(fmtPct(risk.market_guard_return_ratio || 0))}，1m 振幅 ${escapeHtml(fmtPct(risk.market_guard_amplitude_ratio || 0))}，平移次数 ${escapeHtml(fmtNum(risk.center_shift_count || 0, 0))}。</p>
            </div>
            <div class="section">
              <h3>买点</h3>
              <p>当前活动买格 ${escapeHtml(fmtNum(risk.effective_buy_levels || 0, 0))}，优先在中心下方吸收回踩。</p>
              <div class="points">${buyPoints.length ? buyPoints.map((price) => `<span class="point">BUY ${fmtNum(price, 8)}</span>`).join("") : '<span class="point">当前没有活动买单</span>'}</div>
            </div>
            <div class="section">
              <h3>卖点</h3>
              <p>当前活动卖格 ${escapeHtml(fmtNum(risk.effective_sell_levels || 0, 0))}，优先回收库存并维持周转。</p>
              <div class="points">${sellPoints.length ? sellPoints.map((price) => `<span class="point">SELL ${fmtNum(price, 8)}</span>`).join("") : '<span class="point">当前没有活动卖单</span>'}</div>
            </div>
          </div>
          <table>
            <tbody>
              <tr><th>策略模式</th><td>${escapeHtml(String(config.strategy_mode || "--"))}</td><th>当前中价</th><td>${escapeHtml(fmtNum(market.mid_price || 0, 8))}</td></tr>
              <tr><th>当前盘口</th><td>${escapeHtml(fmtNum(market.bid_price || 0, 8))} / ${escapeHtml(fmtNum(market.ask_price || 0, 8))}</td><th>中心价</th><td>${escapeHtml(fmtNum(risk.center_price || state.center_price || 0, 8))}</td></tr>
              <tr><th>账户余额</th><td>${escapeHtml(fmtNum(((snapshot.balances || {}).quote_free) || 0, 4))} ${escapeHtml(((snapshot.balances || {}).quote_asset) || "QUOTE")}</td><th>Base 余额</th><td>${escapeHtml(fmtNum(((snapshot.balances || {}).base_free) || 0, 8))} ${escapeHtml(((snapshot.balances || {}).base_asset) || "BASE")}</td></tr>
              <tr><th>最近成交</th><td>${escapeHtml(latestTrade.side || "--")} ${escapeHtml(fmtNum(latestTrade.notional || 0, 4))}</td><th>成交时间</th><td>${escapeHtml(fmtTs(latestTrade.time))}</td></tr>
              <tr><th>最后刷新</th><td colspan="3">${escapeHtml(fmtTs(new Date().toISOString()))}</td></tr>
            </tbody>
          </table>
        </section>
      `;
    }

    async function loadSpotStrategies() {
      const symbols = parseSymbols();
      if (!symbols.length) {
        cardsEl.innerHTML = '<div class="empty">请先输入至少一个币种</div>';
        summaryEl.textContent = "";
        return;
      }
      metaEl.textContent = `正在刷新 ${symbols.length} 个现货币种...`;
      try {
        const results = await Promise.all(symbols.map(async (symbol) => {
          const resp = await fetch(`/api/spot_runner/status?symbol=${encodeURIComponent(symbol)}`);
          const data = await resp.json();
          if (!resp.ok || !data.ok) throw new Error(`${symbol}: ${data.error || `HTTP ${resp.status}`}`);
          return data.snapshot || {};
        }));
        results.sort((a, b) => Number(((b.trade_summary || {}).gross_notional) || 0) - Number(((a.trade_summary || {}).gross_notional) || 0));
        const runningCount = results.filter((item) => Boolean((item.runner || {}).is_running)).length;
        const exposureCount = results.filter((item) => Number(((item.state || {}).inventory_qty) || ((item.state || {}).managed_base_qty) || 0) > 0 || ((item.open_orders || []).length > 0)).length;
        const totalVolume = results.reduce((acc, item) => acc + Number(((item.trade_summary || {}).gross_notional) || 0), 0);
        const totalPnl = results.reduce((acc, item) => acc + Number(((item.trade_summary || {}).net_pnl_estimate) || 0), 0);
        summaryEl.textContent = `当前拉取 ${results.length} 个现货币种；正在运行 ${runningCount} 个；仍有库存或挂单暴露 ${exposureCount} 个；累计成交额 ${fmtNum(totalVolume, 4)}；净收益估算 ${fmtMoney(totalPnl)}。`;
        cardsEl.innerHTML = results.map(renderCard).join("");
        metaEl.textContent = `最后刷新：${fmtTs(new Date().toISOString())}`;
      } catch (err) {
        cardsEl.innerHTML = `<div class="empty">加载失败：${escapeHtml(err)}</div>`;
        metaEl.textContent = `刷新失败：${err}`;
      }
    }

    function restartTimer() {
      if (timer) clearInterval(timer);
      timer = null;
      if (paused) return;
      const sec = Math.max(3, Number(refreshSecEl.value || 8));
      timer = setInterval(loadSpotStrategies, sec * 1000);
    }

    refreshBtn.addEventListener("click", () => {
      loadSpotStrategies();
      restartTimer();
    });
    toggleBtn.addEventListener("click", () => {
      paused = !paused;
      toggleBtn.textContent = paused ? "恢复自动刷新" : "暂停自动刷新";
      restartTimer();
    });
    refreshSecEl.addEventListener("change", restartTimer);
    symbolsInputEl.addEventListener("change", () => {
      loadSpotStrategies();
      restartTimer();
    });

    loadSpotStrategies();
    restartTimer();
  </script>
</body>
</html>
"""


STRATEGIES_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>当前策略总览</title>
  <style>
    :root {
      --bg: #f4efe3;
      --panel: rgba(255,253,248,0.95);
      --line: #ddd4c3;
      --text: #22201a;
      --muted: #6c685f;
      --good: #0b6f68;
      --bad: #b04b37;
      --warn: #8b5a2b;
      --accent: #163d36;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top, #faf7ee 0%, var(--bg) 55%, #efe6d4 100%);
      color: var(--text);
      font-family: "SF Pro SC", "PingFang SC", "Helvetica Neue", sans-serif;
    }
    .wrap { max-width: 1440px; margin: 0 auto; padding: 24px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 20px;
      box-shadow: 0 10px 30px rgba(48, 37, 17, 0.06);
    }
    .hero h1 { margin: 0 0 8px; font-size: 32px; }
    .hero p, .meta { margin: 0; color: var(--muted); line-height: 1.6; }
    .links, .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 14px;
      align-items: center;
    }
    .links a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid #bfd4ca;
      background: #edf6f1;
      color: var(--accent);
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
    }
    label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
    }
    input {
      width: 90px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      font: inherit;
    }
    #summary {
      margin: 16px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .grid {
      margin-top: 18px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }
    .strategy-card {
      display: grid;
      gap: 14px;
    }
    .topline {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .title h2 { margin: 0; font-size: 24px; }
    .title p { margin: 8px 0 0; color: var(--muted); font-size: 14px; line-height: 1.6; }
    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
    }
    .badge.good { background: rgba(11,111,104,0.12); color: var(--good); }
    .badge.warn { background: rgba(139,90,43,0.14); color: var(--warn); }
    .badge.bad { background: rgba(176,75,55,0.12); color: var(--bad); }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: linear-gradient(180deg, #fff 0%, #faf5ea 100%);
    }
    .metric .label { font-size: 12px; color: var(--muted); font-weight: 700; }
    .metric .value { margin-top: 6px; font-size: 26px; font-weight: 800; }
    .metric .sub { margin-top: 6px; font-size: 12px; color: var(--muted); line-height: 1.5; }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .sections {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .section {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: #fff;
    }
    .section h3 {
      margin: 0 0 10px;
      font-size: 15px;
    }
    .section p {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    .points {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
    }
    .point {
      padding: 6px 9px;
      border-radius: 999px;
      background: #f5efe3;
      border: 1px solid var(--line);
      font-size: 12px;
      font-weight: 700;
      color: #4f4738;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      padding: 9px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      width: 120px;
      color: var(--muted);
      font-weight: 700;
    }
    .empty {
      margin-top: 16px;
      padding: 24px;
      border: 1px dashed var(--line);
      border-radius: 18px;
      text-align: center;
      color: var(--muted);
      background: rgba(255,255,255,0.75);
    }
    @media (max-width: 1180px) {
      .grid { grid-template-columns: 1fr; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .sections { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .metrics { grid-template-columns: 1fr; }
      .topline { flex-direction: column; }
      .badges { justify-content: flex-start; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card hero">
      <h1>当前策略总览</h1>
      <p>按币种列出当前生效策略、详细解释、当前买点与卖点、仓位状态和风控原因。适合快速判断“现在为什么这样挂单”。</p>
      <div class="links">
        <a href="/">返回策略测算页</a>
        <a href="/monitor">打开单币监控页</a>
        <a href="/basis">打开现货/合约价差监控</a>
      </div>
      <div class="toolbar">
        <label>自动刷新（秒）
          <input id="refresh_sec" type="number" min="3" step="1" value="8" />
        </label>
        <button id="refresh_btn" class="primary">立即刷新</button>
        <button id="toggle_btn">暂停自动刷新</button>
      </div>
      <div id="meta" class="meta">等待首轮数据...</div>
      <div id="summary"></div>
    </section>
    <section class="card">
      <div class="topline">
        <div class="title">
          <h2>币种列表管理</h2>
          <p>监控/总览列表会同时作用于 `/monitor` 和本页。交易赛列表会作用于交易赛相关测算与报表。</p>
        </div>
      </div>
      <div class="manager-grid">
        <div class="manager-block">
          <h2>监控/总览币种</h2>
          <div class="toolbar">
            <label>新增币种
              <input id="monitor_symbol_input" class="manager-input" type="text" placeholder="例如 XAUTUSDT" />
            </label>
            <button id="monitor_symbol_add_btn" class="primary">添加</button>
          </div>
          <div id="monitor_symbol_meta" class="meta">可手动维护 `/monitor` 和 `/strategies` 使用的币种。</div>
          <div id="monitor_symbol_chips" class="chip-list"></div>
        </div>
        <div class="manager-block">
          <h2>交易赛币种</h2>
          <div class="toolbar">
            <label>新增币种
              <input id="competition_symbol_input" class="manager-input" type="text" placeholder="例如 XAUTUSDT" />
            </label>
            <button id="competition_symbol_add_btn" class="primary">添加</button>
          </div>
          <div id="competition_symbol_meta" class="meta">可手动维护交易赛回测、报表和相关接口允许的币种。</div>
          <div id="competition_symbol_chips" class="chip-list"></div>
        </div>
      </div>
    </section>
    <div id="cards" class="grid"></div>
  </div>

  <script>
    const DEFAULT_MONITOR_SYMBOLS = ["NIGHTUSDT", "OPNUSDT", "ROBOUSDT", "KATUSDT"];
    const DEFAULT_COMPETITION_SYMBOLS = ["ENSOUSDT", "OPNUSDT", "ROBOUSDT", "KATUSDT", "BASEDUSDT"];
    const cardsEl = document.getElementById("cards");
    const metaEl = document.getElementById("meta");
    const summaryEl = document.getElementById("summary");
    const refreshBtn = document.getElementById("refresh_btn");
    const toggleBtn = document.getElementById("toggle_btn");
    const refreshSecEl = document.getElementById("refresh_sec");
    const monitorSymbolInputEl = document.getElementById("monitor_symbol_input");
    const competitionSymbolInputEl = document.getElementById("competition_symbol_input");
    const monitorSymbolAddBtn = document.getElementById("monitor_symbol_add_btn");
    const competitionSymbolAddBtn = document.getElementById("competition_symbol_add_btn");
    const monitorSymbolMetaEl = document.getElementById("monitor_symbol_meta");
    const competitionSymbolMetaEl = document.getElementById("competition_symbol_meta");
    const monitorSymbolChipsEl = document.getElementById("monitor_symbol_chips");
    const competitionSymbolChipsEl = document.getElementById("competition_symbol_chips");
    let timer = null;
    let paused = false;
    let strategySymbols = [];
    let strategiesLoadPromise = null;

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;");
    }

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      return Number(v).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }

    function fmtMoney(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v);
      return `${num > 0 ? "+" : ""}${fmtNum(num, 4)}`;
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
      const num = Number(v) * 100;
      return `${num > 0 ? "+" : ""}${num.toFixed(2)}%`;
    }

    function fmtTs(v) {
      if (!v) return "--";
      const dt = new Date(v);
      return Number.isNaN(dt.getTime()) ? String(v) : dt.toLocaleString("zh-CN", { hour12: false });
    }

    function statusClass(v) {
      const num = Number(v || 0);
      if (num > 0) return "good";
      if (num < 0) return "bad";
      return "warn";
    }

    function normalizeSymbolInput(value) {
      return String(value || "").trim().toUpperCase();
    }

    function setListMeta(element, message, isError = false) {
      element.textContent = message;
      element.className = isError ? "meta bad" : "meta";
    }

    function renderSymbolChips(element, listType, symbols) {
      if (!symbols.length) {
        element.innerHTML = '<div class="empty">当前列表为空</div>';
        return;
      }
      element.innerHTML = symbols.map((symbol) => `
        <span class="chip">
          <span>${escapeHtml(symbol)}</span>
          <button data-list-type="${escapeHtml(listType)}" data-symbol="${escapeHtml(symbol)}">删除</button>
        </span>
      `).join("");
    }

    async function loadSymbolLists() {
      const resp = await fetch("/api/symbol_lists");
      const data = await resp.json();
      if (!resp.ok || !data.ok || !data.lists) {
        throw new Error(data.error || `HTTP ${resp.status}`);
      }
      const monitorSymbols = Array.isArray(data.lists.monitor)
        ? data.lists.monitor.slice()
        : DEFAULT_MONITOR_SYMBOLS.slice();
      const competitionSymbols = Array.isArray(data.lists.competition)
        ? data.lists.competition.slice()
        : DEFAULT_COMPETITION_SYMBOLS.slice();
      strategySymbols = monitorSymbols;
      renderSymbolChips(monitorSymbolChipsEl, "monitor", monitorSymbols);
      renderSymbolChips(competitionSymbolChipsEl, "competition", competitionSymbols);
      setListMeta(monitorSymbolMetaEl, `当前共 ${monitorSymbols.length} 个监控/总览币种。`);
      setListMeta(competitionSymbolMetaEl, `当前共 ${competitionSymbols.length} 个交易赛币种。`);
    }

    async function mutateSymbolList(listType, action, symbol, metaEl) {
      const normalizedSymbol = normalizeSymbolInput(symbol);
      if (!normalizedSymbol) {
        setListMeta(metaEl, "请输入币种，例如 XAUTUSDT。", true);
        return;
      }
      setListMeta(metaEl, `${action === "add" ? "正在添加" : "正在删除"} ${normalizedSymbol} ...`);
      try {
        const resp = await fetch("/api/symbol_lists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ list_type: listType, action, symbol: normalizedSymbol }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
        await loadSymbolLists();
        setListMeta(metaEl, `${normalizedSymbol} 已${action === "add" ? "添加到" : "从"}${listType === "monitor" ? "监控/总览" : "交易赛"}列表。`);
        await loadStrategies();
      } catch (err) {
        setListMeta(metaEl, `${action === "add" ? "添加" : "删除"}失败：${err}`, true);
      }
    }

    function getPreset(snapshot, key) {
      return ((snapshot.runner_presets || []).find((item) => item.key === key)) || null;
    }

    function getOrderedPoints(openOrders, side) {
      const rows = (openOrders || []).filter((item) => item.side === side);
      rows.sort((a, b) => side === "BUY" ? Number(b.price || 0) - Number(a.price || 0) : Number(a.price || 0) - Number(b.price || 0));
      return rows.slice(0, 6).map((item) => Number(item.price || 0));
    }

    function explainPreset(snapshot) {
      const runnerCfg = ((snapshot.runner || {}).config || {});
      const risk = snapshot.risk_controls || {};
      const effectiveKey = String(risk.effective_strategy_profile || runnerCfg.strategy_profile || "");
      const effectivePreset = getPreset(snapshot, effectiveKey);
      const description = (effectivePreset && effectivePreset.description) || "暂无策略描述";
      const strategyMode = String(risk.strategy_mode || runnerCfg.strategy_mode || "one_way_long");
      const centerSource = risk.center_source || {};
      const referenceName = String(centerSource.reason || "").startsWith("custom_grid_") ? "梯子参考价" : "中心价";
      const referenceText = centerSource.center_price ? `${referenceName} ${fmtNum(centerSource.center_price, 7)}` : "当前参考价";
      const buyLevels = Number(risk.effective_buy_levels || runnerCfg.buy_levels || 0);
      const sellLevels = Number(risk.effective_sell_levels || runnerCfg.sell_levels || 0);
      const perOrder = Number(risk.effective_per_order_notional || runnerCfg.per_order_notional || 0);
      const basePosition = Number(risk.effective_base_position_notional || runnerCfg.base_position_notional || 0);
      if (runnerCfg.custom_grid_enabled) {
        const direction = String(runnerCfg.custom_grid_direction || "long");
        const liveMinPrice = Number(risk.custom_grid_runtime_min_price || runnerCfg.custom_grid_min_price || 0);
        const liveMaxPrice = Number(risk.custom_grid_runtime_max_price || runnerCfg.custom_grid_max_price || 0);
        const rangeText = `${fmtNum(liveMinPrice, 7)} - ${fmtNum(liveMaxPrice, 7)}，${fmtNum(runnerCfg.custom_grid_n, 0)} 格`;
        if (direction === "neutral") {
          return {
            description,
            buy: `固定梯子 ${rangeText}。现价以下挂买单，逐步把净仓往多侧推。`,
            sell: `现价以上挂卖单，逐步把净仓往空侧推；若一侧净仓过重，会先回到目标再恢复正常双边。`,
          };
        }
        if (direction === "short") {
          return {
            description,
            buy: `固定梯子 ${rangeText}。现价以下优先挂回补买单；若空仓超额，会先只减仓。`,
            sell: `现价以上挂卖出开空单；若空仓不足，会先补到启动空仓再进入正常网格。`,
          };
        }
        return {
          description,
          buy: `固定梯子 ${rangeText}。现价以下的格子挂买单；若当前多仓已经高于启动底仓，买单会先暂停。`,
          sell: `现价以上的格子挂卖单。超额仓位时会优先减仓，回到目标底仓后再恢复正常双边网格。`,
        };
      }
      if (strategyMode === "inventory_target_neutral") {
        const bands = risk.inventory_target_bands || {};
        const offsets = (bands.offsets || []).map((v) => `${(Number(v) * 100).toFixed(1)}%`);
        const ratios = (bands.target_ratios || []).map((v) => `${Math.round(Number(v) * 100)}%`);
        const bandText = offsets.length ? offsets.map((v, idx) => `${v}:${ratios[idx] || "--"}`).join(" / ") : "--";
        return {
          description,
          buy: `${referenceText} 下方按目标带 ${bandText} 提高净多目标，并在当前价下方挂买单去贴近目标仓位。`,
          sell: `${referenceText} 上方按同样的目标带提高净空目标，并在当前价上方挂卖单去贴近目标仓位。`,
        };
      }
      if (strategyMode === "one_way_short") {
        return {
          description,
          buy: `${referenceText} 下方分 ${buyLevels} 档挂回补买单，优先兑现已有空仓。`,
          sell: `${referenceText} 上方分 ${sellLevels} 档挂卖出开空单，每档约 ${fmtNum(perOrder, 2)}U，基础空仓目标约 ${fmtNum(basePosition, 2)}U。`,
        };
      }
      if (strategyMode === "synthetic_neutral") {
        return {
          description,
          buy: `${referenceText} 下方买单同时承担“开多/平空”的职责，内部按虚拟 long/short 账本归因。`,
          sell: `${referenceText} 上方卖单同时承担“平多/开空”的职责，单向账户里模拟双边库存。`,
        };
      }
      return {
        description,
        buy: `${referenceText} 下方分 ${buyLevels} 档挂买单，每档约 ${fmtNum(perOrder, 2)}U，基础多仓目标约 ${fmtNum(basePosition, 2)}U。`,
        sell: `${referenceText} 上方分 ${sellLevels} 档挂卖单，对已有多仓逐级减仓或止盈。`,
      };
    }

    function currentBehavior(snapshot) {
      const runnerCfg = ((snapshot.runner || {}).config || {});
      const risk = snapshot.risk_controls || {};
      const strategyMode = String(risk.strategy_mode || runnerCfg.strategy_mode || "one_way_long");
      const parts = [];
      if (risk.xaut_adaptive_enabled) {
        parts.push(`XAUT 三态：${risk.xaut_adaptive_state || "--"}，候选 ${risk.xaut_adaptive_candidate_state || "--"}，原因 ${risk.xaut_adaptive_reason || "--"}`);
        if (String(risk.xaut_adaptive_state || "") === "reduce_only") {
          parts.push(strategyMode === "one_way_short" ? "已撤掉卖单，只保留买单回补减空仓。" : "已撤掉买单，只保留卖单减仓。");
        }
      }
      if (risk.auto_regime_enabled) {
        parts.push(`自适应状态：${risk.auto_regime_regime || "--"}，当前子策略 ${risk.effective_strategy_label || risk.effective_strategy_profile || "--"}`);
      }
      if (risk.inventory_tier_active) {
        parts.push(`库存分层已激活，当前强度 ${Math.round(Number(risk.inventory_tier_ratio || 0) * 100)}%，买/卖格 ${fmtNum(risk.effective_buy_levels || 0, 0)} / ${fmtNum(risk.effective_sell_levels || 0, 0)}`);
      }
      if (risk.buy_paused) {
        parts.push(`买侧暂停：${(risk.pause_reasons || []).join("；") || "达到风控条件"}`);
      }
      if (risk.short_paused) {
        parts.push(`空侧暂停：${(risk.short_pause_reasons || []).join("；") || "达到风控条件"}`);
      }
      if (risk.volatility_buy_pause) {
        parts.push("分钟级高波动停买已触发");
      }
      if (risk.shift_frozen) {
        parts.push("分钟级大波动冻结了中心/重心平移");
      }
      if (runnerCfg.custom_grid_enabled) {
        if (risk.custom_grid_roll_enabled) {
          parts.push(
            `条件下移已开启：每 ${fmtNum(risk.custom_grid_roll_interval_minutes || 0, 0)} 分钟检查，成交 ${fmtNum(risk.custom_grid_roll_trades_since_last_roll || 0, 0)} / ${fmtNum(risk.custom_grid_roll_trade_threshold || 0, 0)}，距上沿 ${fmtNum(risk.custom_grid_roll_levels_above_current || 0, 0)} / ${fmtNum(risk.custom_grid_roll_required_levels_above || 0, 0)} 层`
          );
          if (risk.custom_grid_roll_last_applied_at) {
            parts.push(
              `最近一次下移 ${fmtTs(risk.custom_grid_roll_last_applied_at)}：${fmtNum(risk.custom_grid_roll_last_old_min_price, 7)} - ${fmtNum(risk.custom_grid_roll_last_old_max_price, 7)} -> ${fmtNum(risk.custom_grid_roll_last_new_min_price, 7)} - ${fmtNum(risk.custom_grid_roll_last_new_max_price, 7)}`
            );
          }
        } else if (String(runnerCfg.custom_grid_direction || "long") === "long") {
          parts.push("条件下移未开启");
        }
      }
      return parts.length ? parts.join("。") : "当前没有额外风控接管，按预设规则正常运行。";
    }

    function renderCard(snapshot) {
      const runner = snapshot.runner || {};
      const runnerCfg = runner.config || {};
      const risk = snapshot.risk_controls || {};
      const position = snapshot.position || {};
      const market = snapshot.market || {};
      const trade = snapshot.trade_summary || {};
      const income = snapshot.income_summary || {};
      const explain = explainPreset(snapshot);
      const buyPoints = getOrderedPoints(snapshot.open_orders || [], "BUY");
      const sellPoints = getOrderedPoints(snapshot.open_orders || [], "SELL");
      const requestedKey = String(runnerCfg.strategy_profile || "");
      const requestedPreset = getPreset(snapshot, requestedKey);
      const requestedLabel = requestedPreset ? requestedPreset.label : (requestedKey || "--");
      const effectiveLabel = String(risk.effective_strategy_label || risk.effective_strategy_profile || requestedLabel || "--");
      const isRunning = Boolean(runner.is_running);
      const hasExposure = Math.abs(Number(position.position_amt || 0)) > 0 || (snapshot.open_orders || []).length > 0;
      const stateLabel = isRunning ? "运行中" : (hasExposure ? "停机但仍有仓位/挂单" : "未运行");
      const stateClass = isRunning ? "good" : (hasExposure ? "warn" : "bad");
      const currentPrice = Number(market.mid_price || ((Number(market.bid_price || 0) + Number(market.ask_price || 0)) / 2) || 0);
      const positionNotional = Math.abs(Number(position.position_amt || 0)) * currentPrice;
      return `
        <section class="card strategy-card">
          <div class="topline">
            <div class="title">
              <h2>${escapeHtml(snapshot.symbol || "--")}</h2>
              <p>${escapeHtml(explain.description)}</p>
            </div>
            <div class="badges">
              <span class="badge ${stateClass}">${escapeHtml(stateLabel)}</span>
              <span class="badge warn">请求预设 ${escapeHtml(requestedLabel)}</span>
              <span class="badge good">当前子策略 ${escapeHtml(effectiveLabel)}</span>
            </div>
          </div>
          <div class="metrics">
            <div class="metric">
              <div class="label">当前仓位</div>
              <div class="value">${escapeHtml(fmtNum(position.position_amt || 0, 0))}</div>
              <div class="sub">持仓名义 ${escapeHtml(fmtNum(positionNotional, 4))}U</div>
            </div>
            <div class="metric">
              <div class="label">当前挂单</div>
              <div class="value">${escapeHtml(fmtNum((snapshot.open_orders || []).length, 0))}</div>
              <div class="sub">买/卖 ${escapeHtml(fmtNum((snapshot.open_orders || []).filter((x) => x.side === "BUY").length, 0))} / ${escapeHtml(fmtNum((snapshot.open_orders || []).filter((x) => x.side === "SELL").length, 0))}</div>
            </div>
            <div class="metric">
              <div class="label">累计成交额</div>
              <div class="value">${escapeHtml(fmtNum(trade.gross_notional || 0, 4))}</div>
              <div class="sub">已实现 ${escapeHtml(fmtMoney(trade.realized_pnl || 0))}</div>
            </div>
            <div class="metric">
              <div class="label">净收益估算</div>
              <div class="value ${statusClass(Number(trade.net_pnl_estimate || 0))}">${escapeHtml(fmtMoney(trade.net_pnl_estimate || 0))}</div>
              <div class="sub">手续费 ${escapeHtml(fmtMoney(-(trade.commission || 0)))} · 资金费 ${escapeHtml(fmtMoney(income.funding_fee || 0))}</div>
            </div>
          </div>
          <div class="sections">
            <div class="section">
              <h3>当前行为</h3>
              <p>${escapeHtml(currentBehavior(snapshot))}</p>
            </div>
            <div class="section">
              <h3>买点</h3>
              <p>${escapeHtml(explain.buy)}</p>
              <div class="points">${buyPoints.length ? buyPoints.map((price) => `<span class="point">BUY ${fmtNum(price, 7)}</span>`).join("") : '<span class="point">当前没有活动买单</span>'}</div>
            </div>
            <div class="section">
              <h3>卖点</h3>
              <p>${escapeHtml(explain.sell)}</p>
              <div class="points">${sellPoints.length ? sellPoints.map((price) => `<span class="point">SELL ${fmtNum(price, 7)}</span>`).join("") : '<span class="point">当前没有活动卖单</span>'}</div>
            </div>
          </div>
          <table>
            <tbody>
              <tr><th>策略模式</th><td>${escapeHtml(String(risk.strategy_mode || runnerCfg.strategy_mode || "--"))}</td><th>当前中价</th><td>${escapeHtml(fmtNum(currentPrice, 7))}</td></tr>
              <tr><th>中心/参考价</th><td>${escapeHtml(risk.center_source && risk.center_source.center_price ? fmtNum(risk.center_source.center_price, 7) : "--")}</td><th>来源</th><td>${escapeHtml((risk.center_source && (risk.center_source.reason || risk.center_source.interval)) || "--")}</td></tr>
              <tr><th>买/卖格</th><td>${escapeHtml(fmtNum(risk.effective_buy_levels || 0, 0))} / ${escapeHtml(fmtNum(risk.effective_sell_levels || 0, 0))}</td><th>每笔名义</th><td>${escapeHtml(fmtNum(risk.effective_per_order_notional || 0, 4))}U</td></tr>
              <tr><th>底仓目标</th><td>${escapeHtml(fmtNum(risk.effective_base_position_notional || 0, 4))}U</td><th>停买/停空</th><td>${risk.buy_paused ? "是" : "否"} / ${risk.short_paused ? "是" : "否"}</td></tr>
              <tr><th>当前多/空名义</th><td>${escapeHtml(fmtNum(risk.current_long_notional || 0, 4))} / ${escapeHtml(fmtNum(risk.current_short_notional || 0, 4))}U</td><th>剩余空间</th><td>${escapeHtml(fmtNum(risk.remaining_headroom || 0, 4))} / ${escapeHtml(fmtNum(risk.remaining_short_headroom || 0, 4))}U</td></tr>
              <tr><th>分钟波动</th><td>涨跌 ${escapeHtml(fmtPct(risk.market_guard_return_ratio || 0))}</td><th>分钟振幅</th><td>${escapeHtml(fmtPct(risk.market_guard_amplitude_ratio || 0))}</td></tr>
              <tr><th>最后刷新</th><td colspan="3">${escapeHtml(fmtTs(snapshot.ts))}</td></tr>
            </tbody>
          </table>
        </section>
      `;
    }

    async function fetchStrategySnapshots(symbols, concurrency = 2) {
      const normalizedConcurrency = Math.max(1, Number(concurrency || 1));
      const results = new Array(symbols.length);
      let nextIndex = 0;

      async function worker() {
        while (true) {
          const currentIndex = nextIndex;
          nextIndex += 1;
          if (currentIndex >= symbols.length) {
            return;
          }
          const symbol = symbols[currentIndex];
          const resp = await fetch(`/api/loop_monitor?symbol=${encodeURIComponent(symbol)}`);
          const data = await resp.json();
          if (!resp.ok || !data.ok) {
            throw new Error(`${symbol}: ${data.error || `HTTP ${resp.status}`}`);
          }
          results[currentIndex] = data;
        }
      }

      await Promise.all(Array.from({ length: Math.min(normalizedConcurrency, symbols.length) }, () => worker()));
      return results;
    }

    async function loadStrategies() {
      if (strategiesLoadPromise) {
        return strategiesLoadPromise;
      }
      strategiesLoadPromise = (async () => {
        metaEl.textContent = "正在刷新全部策略...";
        try {
          const symbols = strategySymbols.slice();
          if (!symbols.length) {
            summaryEl.textContent = "当前监控/总览币种列表为空。";
            cardsEl.innerHTML = '<div class="empty">请先在上方“币种列表管理”里添加至少一个监控币种。</div>';
            metaEl.textContent = `最后刷新：${fmtTs(new Date().toISOString())}`;
            return;
          }
          const results = await fetchStrategySnapshots(symbols, 2);
          const runningCount = results.filter((item) => Boolean((item.runner || {}).is_running)).length;
          const exposureCount = results.filter((item) => Math.abs(Number(((item.position || {}).position_amt) || 0)) > 0 || (item.open_orders || []).length > 0).length;
          summaryEl.textContent = `当前拉取 ${results.length} 个币种；正在运行 ${runningCount} 个；仍有仓位或挂单暴露 ${exposureCount} 个。`;
          cardsEl.innerHTML = results.map(renderCard).join("");
          metaEl.textContent = `最后刷新：${fmtTs(new Date().toISOString())}`;
        } catch (err) {
          cardsEl.innerHTML = `<div class="empty">加载失败：${escapeHtml(err)}</div>`;
          metaEl.textContent = `刷新失败：${err}`;
        }
      })();
      try {
        await strategiesLoadPromise;
      } finally {
        strategiesLoadPromise = null;
      }
    }

    function restartTimer() {
      if (timer) clearInterval(timer);
      timer = null;
      if (paused) return;
      const sec = Math.max(3, Number(refreshSecEl.value || 8));
      timer = setInterval(loadStrategies, sec * 1000);
    }

    refreshBtn.addEventListener("click", () => {
      loadStrategies();
      restartTimer();
    });
    toggleBtn.addEventListener("click", () => {
      paused = !paused;
      toggleBtn.textContent = paused ? "恢复自动刷新" : "暂停自动刷新";
      restartTimer();
    });
    refreshSecEl.addEventListener("change", restartTimer);
    monitorSymbolAddBtn.addEventListener("click", () => {
      mutateSymbolList("monitor", "add", monitorSymbolInputEl.value, monitorSymbolMetaEl);
      monitorSymbolInputEl.value = "";
    });
    competitionSymbolAddBtn.addEventListener("click", () => {
      mutateSymbolList("competition", "add", competitionSymbolInputEl.value, competitionSymbolMetaEl);
      competitionSymbolInputEl.value = "";
    });
    monitorSymbolChipsEl.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) return;
      mutateSymbolList("monitor", "remove", target.dataset.symbol || "", monitorSymbolMetaEl);
    });
    competitionSymbolChipsEl.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) return;
      mutateSymbolList("competition", "remove", target.dataset.symbol || "", competitionSymbolMetaEl);
    });

    async function initStrategiesPage() {
      try {
        await loadSymbolLists();
      } catch (err) {
        strategySymbols = DEFAULT_MONITOR_SYMBOLS.slice();
        renderSymbolChips(monitorSymbolChipsEl, "monitor", strategySymbols);
        renderSymbolChips(competitionSymbolChipsEl, "competition", DEFAULT_COMPETITION_SYMBOLS.slice());
        setListMeta(monitorSymbolMetaEl, `加载失败，已回退默认监控列表：${err}`, true);
        setListMeta(competitionSymbolMetaEl, `加载失败，已回退默认交易赛列表：${err}`, true);
      }
      await loadStrategies();
      restartTimer();
    }

    initStrategiesPage();
  </script>
</body>
</html>
"""


def _safe_float(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    return number


def _safe_int(value: Any, name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    return number


def _safe_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off", ""}:
            return False
    raise ValueError(f"{name} must be boolean")


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "on"}


def _safe_datetime(value: Any, name: str) -> datetime:
    if value is None:
        raise ValueError(f"{name} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} is required")
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{name} must be ISO datetime string") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_metric(value: float) -> float | None:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _percentile(values: list[float], ratio: float) -> float:
    if not values:
        raise ValueError("values cannot be empty")
    clipped = min(max(float(ratio), 0.0), 1.0)
    if len(values) == 1:
        return float(values[0])
    position = clipped * (len(values) - 1)
    left = int(math.floor(position))
    right = int(math.ceil(position))
    if left == right:
        return float(values[left])
    weight = position - left
    return float(values[left]) * (1.0 - weight) + float(values[right]) * weight


def _sample_n_values(n_min: int, n_max: int, max_count: int = 16) -> list[int]:
    if n_min <= 0 or n_max <= 0 or n_min > n_max:
        raise ValueError("invalid n range")
    if max_count <= 0:
        raise ValueError("max_count must be > 0")
    if n_max - n_min + 1 <= max_count:
        return list(range(n_min, n_max + 1))
    values = {n_min, n_max}
    steps = max_count - 2
    for index in range(1, steps + 1):
        ratio = index / (steps + 1)
        sampled = int(round(n_min + ratio * (n_max - n_min)))
        values.add(max(n_min, min(n_max, sampled)))
    return sorted(values)


def _build_candidate_ranges(candles: list[Candle]) -> list[tuple[float, float, str]]:
    low_prices = sorted(float(item.low) for item in candles)
    high_prices = sorted(float(item.high) for item in candles)
    close_prices = sorted(float(item.close) for item in candles)
    last_close = float(candles[-1].close)
    median_close = _percentile(close_prices, 0.5)

    raw_ranges: list[tuple[float, float, str]] = []
    for low_q, high_q in (
        (0.01, 0.99),
        (0.03, 0.97),
        (0.05, 0.95),
        (0.10, 0.90),
        (0.15, 0.85),
        (0.20, 0.80),
        (0.25, 0.75),
    ):
        raw_ranges.append(
            (
                _percentile(low_prices, low_q),
                _percentile(high_prices, high_q),
                f"quantile_{int(low_q * 100)}_{int(high_q * 100)}",
            )
        )

    for width in (0.02, 0.03, 0.05, 0.08, 0.12, 0.18):
        raw_ranges.append(
            (
                max(0.0000001, last_close * (1.0 - width)),
                last_close * (1.0 + width),
                f"last_close_{int(width * 100)}pct",
            )
        )
        raw_ranges.append(
            (
                max(0.0000001, median_close * (1.0 - width)),
                median_close * (1.0 + width),
                f"median_close_{int(width * 100)}pct",
            )
        )

    deduped: dict[tuple[float, float], tuple[float, float, str]] = {}
    for min_price, max_price, tag in raw_ranges:
        if min_price <= 0 or max_price <= min_price:
            continue
        mid = (min_price + max_price) / 2.0
        if mid <= 0:
            continue
        if (max_price - min_price) / mid < 0.004:
            continue
        key = (round(min_price, 6), round(max_price, 6))
        deduped[key] = (min_price, max_price, tag)

    return sorted(deduped.values(), key=lambda item: item[1] - item[0])[:16]


def _competition_sort_key(
    *,
    trade_volume: float,
    target_trade_volume: float,
    net_profit: float,
    max_drawdown: float,
) -> tuple[float, float, float, float, float, float]:
    if target_trade_volume > 0:
        meets_target = 1.0 if trade_volume >= target_trade_volume else 0.0
        coverage = min(1.0, trade_volume / target_trade_volume)
    else:
        meets_target = 1.0
        coverage = 1.0
    loss = max(0.0, -net_profit)
    profit = max(0.0, net_profit)
    return (
        meets_target,
        coverage,
        -loss,
        profit,
        trade_volume,
        -max_drawdown,
    )


def _mode_preference_score(mode: str) -> float:
    order = {
        "equal_qty": 6.0,
        "equal": 5.5,
        "center_heavy": 5.0,
        "edge_heavy": 4.6,
        "linear_reverse": 4.0,
        "linear": 3.8,
        "geometric_reverse": 3.4,
        "geometric": 3.2,
        "quadratic_reverse": 3.0,
        "quadratic": 2.8,
    }
    return order.get(mode.strip().lower(), 0.0)


def _ordered_suggestion_modes(modes: list[str]) -> list[str]:
    deduped: list[str] = []
    for mode in modes:
        cleaned = mode.strip().lower()
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return sorted(deduped, key=lambda item: (_mode_preference_score(item), item), reverse=True)


def _range_reason(coverage: float, net_profit: float, mode: str) -> str:
    mode_hint = "中性分配" if _mode_preference_score(mode) >= 5.0 else "方向性分配"
    if coverage >= 1.0 and net_profit >= 0:
        return f"达成目标且净收益为正（{mode_hint}）"
    if coverage >= 1.0 and net_profit < 0:
        return f"达成目标，优先控制损失（{mode_hint}）"
    if coverage < 1.0 and net_profit >= 0:
        return f"未达目标但净收益为正，可继续加密（{mode_hint}）"
    return f"优先提升成交额，亏损可控（{mode_hint}）"


def _load_market_symbols(
    *,
    market_type: str,
    contract_type: str | None = None,
    refresh: bool = False,
) -> list[str]:
    normalized_market_type = normalize_market_type(market_type)
    if normalized_market_type == "spot":
        return load_or_fetch_spot_symbols(cache_dir="data", refresh=refresh)
    return load_or_fetch_futures_symbols(
        contract_type=normalize_contract_type(contract_type or "usdm"),
        cache_dir="data",
        refresh=refresh,
    )


def _validate_market_symbol(
    *,
    symbol: str,
    market_type: str,
    contract_type: str | None = None,
) -> None:
    try:
        supported_symbols = set(
            _load_market_symbols(
                market_type=market_type,
                contract_type=contract_type,
                refresh=False,
            )
        )
    except Exception:
        supported_symbols = set()
    if supported_symbols and symbol not in supported_symbols:
        raise ValueError(f"unsupported symbol: {symbol}")


def _infer_settle_asset(symbol: str, contract_type: str) -> str:
    if normalize_contract_type(contract_type) != "coinm":
        return "U"
    text = str(symbol or "").upper().strip()
    m = re.match(r"^([A-Z0-9]+)USD(?:_PERP)?$", text)
    if m:
        return m.group(1)
    return text or "BASE"


def _normalize_spot_quote_mode(value: Any) -> str:
    mode = str(value or "major_stables").strip().lower()
    if mode not in {"major_stables", "usdt"}:
        raise ValueError("spot_quote_mode must be one of: major_stables, usdt")
    return mode


def _spot_quote_priority(mode: str) -> tuple[str, ...]:
    normalized = _normalize_spot_quote_mode(mode)
    if normalized == "usdt":
        return ("USDT",)
    return STABLE_SPOT_QUOTES


def _infer_base_quote_from_symbol(symbol: str, contract_type: str) -> tuple[str, str]:
    text = str(symbol or "").upper().strip()
    normalized_contract_type = normalize_contract_type(contract_type)
    if normalized_contract_type == "coinm":
        match = re.match(r"^([A-Z0-9]+)USD(?:_PERP)?$", text)
        if match:
            return match.group(1), "USD"
        return text, "USD"
    for quote_asset in STABLE_SPOT_QUOTES:
        if text.endswith(quote_asset):
            return text[: -len(quote_asset)], quote_asset
    return text, ""


def _pick_spot_market_for_futures_symbol(
    futures_symbol: str,
    contract_type: str,
    spot_by_symbol: dict[str, dict[str, str]],
    spot_by_base: dict[str, list[dict[str, str]]],
    quote_priority: tuple[str, ...],
) -> dict[str, str] | None:
    base_asset, futures_quote_asset = _infer_base_quote_from_symbol(futures_symbol, contract_type)
    quote_index = {quote: idx for idx, quote in enumerate(quote_priority)}

    exact = spot_by_symbol.get(futures_symbol)
    if exact and exact.get("quote_asset") in quote_index:
        return exact

    candidates = spot_by_base.get(base_asset, [])
    if not candidates:
        return None

    same_quote = next(
        (
            item
            for item in candidates
            if item.get("quote_asset") == futures_quote_asset and item.get("quote_asset") in quote_index
        ),
        None,
    )
    if same_quote is not None:
        return same_quote

    eligible = [item for item in candidates if item.get("quote_asset") in quote_index]
    if not eligible:
        return None
    eligible.sort(key=lambda item: (quote_index[item["quote_asset"]], item["symbol"]))
    return eligible[0]


def _basis_regime(mid_basis: float) -> tuple[str, str]:
    if mid_basis > 1e-12:
        return "perp_over_spot", "合约偏贵"
    if mid_basis < -1e-12:
        return "spot_over_perp", "现货偏贵"
    return "flat", "基本平水"


def _basis_carry_alignment(arbitrage_side: str, funding_rate: float | None) -> tuple[str, str, str, int]:
    if arbitrage_side not in {"long_spot_short_perp", "short_spot_long_perp"}:
        return "neutral", "中性", "当前无明确资金费优势", 1
    if funding_rate is None or abs(funding_rate) <= 1e-12:
        return "neutral", "中性", "资金费接近 0", 1
    if arbitrage_side == "long_spot_short_perp":
        if funding_rate > 0:
            return "aligned", "顺风", "做空合约可收资金费", 2
        return "opposed", "逆风", "做空合约需支付资金费", 0
    if funding_rate < 0:
        return "aligned", "顺风", "做多合约可收资金费", 2
    return "opposed", "逆风", "做多合约需支付资金费", 0


def _build_basis_rows(
    futures_symbols: list[str],
    spot_markets: list[dict[str, str]],
    spot_tickers: list[dict[str, Any]],
    futures_tickers: list[dict[str, Any]],
    premium_rows: list[dict[str, Any]],
    contract_type: str = "usdm",
    spot_quote_mode: str = "major_stables",
) -> list[dict[str, Any]]:
    quote_priority = _spot_quote_priority(spot_quote_mode)
    quote_set = set(quote_priority)
    spot_by_symbol = {
        item["symbol"]: item
        for item in spot_markets
        if item.get("symbol") and item.get("quote_asset") in quote_set
    }
    spot_by_base: dict[str, list[dict[str, str]]] = {}
    for item in spot_markets:
        symbol = item.get("symbol")
        base_asset = item.get("base_asset")
        quote_asset = item.get("quote_asset")
        if not symbol or not base_asset or quote_asset not in quote_set:
            continue
        spot_by_base.setdefault(base_asset, []).append(item)

    spot_ticker_map = {item["symbol"]: item for item in spot_tickers if item.get("symbol")}
    futures_ticker_map = {item["symbol"]: item for item in futures_tickers if item.get("symbol")}
    premium_map = {item["symbol"]: item for item in premium_rows if item.get("symbol")}

    rows: list[dict[str, Any]] = []
    for futures_symbol in futures_symbols:
        futures_ticker = futures_ticker_map.get(futures_symbol)
        if futures_ticker is None:
            continue
        spot_market = _pick_spot_market_for_futures_symbol(
            futures_symbol=futures_symbol,
            contract_type=contract_type,
            spot_by_symbol=spot_by_symbol,
            spot_by_base=spot_by_base,
            quote_priority=quote_priority,
        )
        if spot_market is None:
            continue
        spot_ticker = spot_ticker_map.get(spot_market["symbol"])
        if spot_ticker is None:
            continue

        spot_bid = float(spot_ticker["bid_price"])
        spot_ask = float(spot_ticker["ask_price"])
        futures_bid = float(futures_ticker["bid_price"])
        futures_ask = float(futures_ticker["ask_price"])
        if spot_bid <= 0 or spot_ask <= 0 or futures_bid <= 0 or futures_ask <= 0:
            continue

        spot_mid = (spot_bid + spot_ask) / 2.0
        futures_mid = (futures_bid + futures_ask) / 2.0
        if spot_mid <= 0 or futures_mid <= 0:
            continue

        basis_mid = futures_mid / spot_mid - 1.0
        spread_long_spot_short_perp = futures_bid / spot_ask - 1.0
        spread_short_spot_long_perp = spot_bid / futures_ask - 1.0
        best_spread = max(spread_long_spot_short_perp, spread_short_spot_long_perp)

        if spread_long_spot_short_perp >= spread_short_spot_long_perp and spread_long_spot_short_perp > 0:
            arbitrage_side = "long_spot_short_perp"
            strategy_label = "买现货 / 卖合约"
        elif spread_short_spot_long_perp > 0:
            arbitrage_side = "short_spot_long_perp"
            strategy_label = "借币卖现货 / 买合约"
        else:
            arbitrage_side = "watch"
            strategy_label = "继续观察"

        premium = premium_map.get(futures_symbol, {})
        funding_rate = premium.get("funding_rate")
        next_funding_time_ms = premium.get("next_funding_time")
        next_funding_time = (
            datetime.fromtimestamp(next_funding_time_ms / 1000, tz=timezone.utc).isoformat()
            if isinstance(next_funding_time_ms, int) and next_funding_time_ms > 0
            else None
        )
        basis_regime, basis_regime_label = _basis_regime(basis_mid)
        carry_alignment, carry_alignment_label, carry_alignment_note, carry_alignment_rank = (
            _basis_carry_alignment(arbitrage_side, funding_rate)
        )

        rows.append(
            {
                "base_asset": spot_market["base_asset"],
                "spot_symbol": spot_market["symbol"],
                "spot_quote_asset": spot_market["quote_asset"],
                "spot_bid": spot_bid,
                "spot_ask": spot_ask,
                "spot_mid": spot_mid,
                "futures_symbol": futures_symbol,
                "futures_bid": futures_bid,
                "futures_ask": futures_ask,
                "futures_mid": futures_mid,
                "mark_price": premium.get("mark_price") or futures_mid,
                "index_price": premium.get("index_price"),
                "basis_mid": basis_mid,
                "spread_long_spot_short_perp": spread_long_spot_short_perp,
                "spread_short_spot_long_perp": spread_short_spot_long_perp,
                "best_spread": best_spread,
                "arbitrage_side": arbitrage_side,
                "strategy_label": strategy_label,
                "basis_regime": basis_regime,
                "basis_regime_label": basis_regime_label,
                "funding_rate": funding_rate,
                "next_funding_time": next_funding_time,
                "carry_alignment": carry_alignment,
                "carry_alignment_label": carry_alignment_label,
                "carry_alignment_note": carry_alignment_note,
                "carry_alignment_rank": carry_alignment_rank,
            }
        )

    rows.sort(key=lambda item: item["best_spread"], reverse=True)
    return rows


def _funding_margin_snapshot(
    position_notional: float,
    reference_price: float,
    account_equity: float,
    margin_ratio: float = FUNDING_MARGIN_RATIO,
) -> dict[str, float | None]:
    abs_notional = abs(float(position_notional))
    safe_ratio = max(float(margin_ratio), 0.0)
    minimum_margin = abs_notional * safe_ratio
    withdrawable_amount = max(float(account_equity) - minimum_margin, 0.0)

    liquidation_price: float | None = None
    if abs_notional > 0.0 and reference_price > 0.0:
        if position_notional > 0:
            liquidation_price = max(reference_price * (1.0 - safe_ratio), 0.0)
        elif position_notional < 0:
            liquidation_price = reference_price * (1.0 + safe_ratio)

    return {
        "minimum_margin": minimum_margin,
        "liquidation_price": liquidation_price,
        "withdrawable_amount": withdrawable_amount,
    }


def _series_max_drawdown(nav: list[float]) -> float:
    if not nav:
        return 0.0
    peak = nav[0]
    max_dd = 0.0
    for value in nav:
        if value > peak:
            peak = value
        if peak <= 0:
            continue
        dd = (peak - value) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _expected_candle_count(start_time: datetime, end_time: datetime, interval: str) -> int:
    span_ms = int((end_time - start_time).total_seconds() * 1000)
    if span_ms <= 0:
        return 0
    step_ms = parse_interval_ms(interval)
    if step_ms <= 0:
        return 0
    return max(span_ms // step_ms, 0)


def _enforce_seconds_interval_limit(start_time: datetime, end_time: datetime, interval: str) -> None:
    step_ms = parse_interval_ms(interval)
    if step_ms >= 60_000:
        return
    span = end_time - start_time
    if span > SECOND_INTERVAL_MAX_SPAN:
        raise ValueError(
            f"interval={interval} 时，时间区间不能超过 {SECOND_INTERVAL_MAX_SPAN.days} 天"
        )


def _filter_cached_candles_by_time(candles: list, start_time: datetime, end_time: datetime) -> list:
    st = start_time.astimezone(timezone.utc)
    et = end_time.astimezone(timezone.utc)
    return [x for x in candles if st <= x.open_time < et]


def _filter_cached_funding_by_time(items: list, start_time: datetime, end_time: datetime) -> list:
    st = start_time.astimezone(timezone.utc)
    et = end_time.astimezone(timezone.utc)
    return [x for x in items if st <= x.ts < et]


def _load_candles_for_ranking(
    symbol: str,
    interval: str,
    start_time: datetime,
    end_time: datetime,
    contract_type: str,
    cache_only: bool,
    refresh: bool,
) -> list:
    path = cache_file_path(
        symbol,
        interval,
        cache_dir="data",
        contract_type=contract_type,
    )
    if cache_only:
        if not path.exists():
            return []
        candles = load_candles_from_csv(path)
        return _filter_cached_candles_by_time(candles, start_time, end_time)
    try:
        candles = load_or_fetch_candles(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            cache_dir="data",
            contract_type=contract_type,
            refresh=refresh,
        )
        if candles:
            return candles
    except Exception:
        pass
    if not path.exists():
        return []
    candles = load_candles_from_csv(path)
    return _filter_cached_candles_by_time(candles, start_time, end_time)


def _load_funding_for_ranking(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    contract_type: str,
    cache_only: bool,
    refresh: bool,
) -> list:
    path = funding_cache_file_path(symbol, cache_dir="data", contract_type=contract_type)
    if cache_only:
        if not path.exists():
            return []
        items = load_funding_rates_from_csv(path)
        return _filter_cached_funding_by_time(items, start_time, end_time)
    try:
        items = load_or_fetch_funding_rates(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            cache_dir="data",
            contract_type=contract_type,
            refresh=refresh,
        )
        if items:
            return items
    except Exception:
        pass
    if not path.exists():
        return []
    items = load_funding_rates_from_csv(path)
    return _filter_cached_funding_by_time(items, start_time, end_time)


def _normalize_market_rankings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    contract_type = normalize_contract_type(payload.get("contract_type", "usdm"))
    start_time = _safe_datetime(payload.get("start_time"), "start_time")
    end_time = _safe_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1h")).strip()
    top_k = _safe_int(payload.get("top_k", 30), "top_k")
    max_symbols = _safe_int(payload.get("max_symbols", 0), "max_symbols")
    workers = _safe_int(payload.get("workers", 16), "workers")
    cache_ttl_seconds = _safe_int(payload.get("cache_ttl_seconds", 60), "cache_ttl_seconds")
    cache_only = _safe_bool(payload.get("cache_only", True), "cache_only")
    refresh = _safe_bool(payload.get("refresh", False), "refresh")
    symbol_filter = str(payload.get("symbol_filter", "")).strip().upper()

    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    if top_k <= 0 or top_k > 500:
        raise ValueError("top_k must be in [1,500]")
    if max_symbols < 0 or max_symbols > 5000:
        raise ValueError("max_symbols must be in [0,5000]")
    if workers <= 0 or workers > 128:
        raise ValueError("workers must be in [1,128]")
    if cache_ttl_seconds < 0 or cache_ttl_seconds > 3600:
        raise ValueError("cache_ttl_seconds must be in [0,3600]")
    _enforce_seconds_interval_limit(start_time, end_time, interval)

    return {
        "contract_type": contract_type,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "top_k": top_k,
        "max_symbols": max_symbols,
        "workers": workers,
        "cache_ttl_seconds": cache_ttl_seconds,
        "cache_only": cache_only,
        "refresh": refresh,
        "symbol_filter": symbol_filter,
    }


def _run_market_rankings(params: dict[str, Any]) -> dict[str, Any]:
    key_obj = {
        "contract_type": params["contract_type"],
        "start_time": params["start_time"].isoformat(),
        "end_time": params["end_time"].isoformat(),
        "interval": params["interval"],
        "max_symbols": params["max_symbols"],
        "cache_only": params["cache_only"],
        "symbol_filter": params["symbol_filter"],
    }
    cache_key = json.dumps(key_obj, sort_keys=True, ensure_ascii=False)
    now_ts = time.time()
    if not params["refresh"] and params["cache_ttl_seconds"] > 0:
        with RANKING_CACHE_LOCK:
            hit = RANKING_CACHE.get(cache_key)
        if hit and now_ts - float(hit.get("ts", 0)) <= params["cache_ttl_seconds"]:
            cached_result = dict(hit["result"])
            meta = dict(cached_result.get("meta", {}))
            meta["cached"] = True
            cached_result["meta"] = meta
            return cached_result

    t0 = time.time()
    try:
        symbols = load_or_fetch_futures_symbols(
            contract_type=params["contract_type"],
            cache_dir="data",
            refresh=False,
        )
    except Exception:
        symbols = ["BTCUSD_PERP", "ETHUSD_PERP"] if params["contract_type"] == "coinm" else ["BTCUSDT", "ETHUSDT"]
    if params["symbol_filter"]:
        symbols = [s for s in symbols if params["symbol_filter"] in s.upper()]
    if params["max_symbols"] > 0:
        symbols = symbols[: params["max_symbols"]]

    interval_ms = parse_interval_ms(params["interval"])
    periods_per_year = (365.0 * 24.0 * 3600.0 * 1000.0) / interval_ms
    worker_count = min(max(1, params["workers"]), max(1, len(symbols)))

    def _volatility_worker(symbol: str) -> dict[str, Any] | None:
        try:
            candles = _load_candles_for_ranking(
                symbol=symbol,
                interval=params["interval"],
                start_time=params["start_time"],
                end_time=params["end_time"],
                contract_type=params["contract_type"],
                cache_only=params["cache_only"],
                refresh=params["refresh"],
            )
            if len(candles) < 2:
                return None
            closes = [x.close for x in candles if x.close > 0]
            if len(closes) < 2:
                return None
            log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
            if not log_returns:
                return None
            avg = sum(log_returns) / len(log_returns)
            var = (
                sum((x - avg) ** 2 for x in log_returns) / (len(log_returns) - 1)
                if len(log_returns) > 1
                else 0.0
            )
            std = math.sqrt(max(var, 0.0))
            ann = std * math.sqrt(periods_per_year)
            price_return = closes[-1] / closes[0] - 1.0
            return {
                "symbol": symbol,
                "volatility_annualized": ann,
                "volatility_std": std,
                "price_return": price_return,
                "candle_count": len(candles),
            }
        except Exception:
            return None

    def _funding_worker(symbol: str) -> dict[str, Any] | None:
        try:
            rates = _load_funding_for_ranking(
                symbol=symbol,
                start_time=params["start_time"],
                end_time=params["end_time"],
                contract_type=params["contract_type"],
                cache_only=params["cache_only"],
                refresh=params["refresh"],
            )
            if not rates:
                return None
            total_rate = sum(x.rate for x in rates)
            return {
                "symbol": symbol,
                "total_rate": total_rate,
                "long_side_total_rate": -total_rate,
                "event_count": len(rates),
            }
        except Exception:
            return None

    vol_rows: list[dict[str, Any]] = []
    fund_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        vol_futures = [executor.submit(_volatility_worker, symbol) for symbol in symbols]
        fund_futures = [executor.submit(_funding_worker, symbol) for symbol in symbols]
        for future in as_completed(vol_futures):
            row = future.result()
            if row:
                vol_rows.append(row)
        for future in as_completed(fund_futures):
            row = future.result()
            if row:
                fund_rows.append(row)

    vol_rows.sort(key=lambda x: x["volatility_annualized"], reverse=True)
    fund_rows.sort(key=lambda x: x["total_rate"], reverse=True)
    result = {
        "ok": True,
        "volatility": {
            "count": len(vol_rows),
            "rows": vol_rows,
        },
        "funding": {
            "count": len(fund_rows),
            "rows": fund_rows,
        },
        "meta": {
            "cached": False,
            "symbols_considered": len(symbols),
            "contract_type": params["contract_type"],
            "interval": params["interval"],
            "requested_top_k": params["top_k"],
            "start_time": params["start_time"].isoformat(),
            "end_time": params["end_time"].isoformat(),
            "cache_only": params["cache_only"],
            "symbol_filter": params["symbol_filter"],
            "elapsed_seconds": round(time.time() - t0, 2),
        },
    }
    if params["cache_ttl_seconds"] > 0:
        with RANKING_CACHE_LOCK:
            RANKING_CACHE[cache_key] = {"ts": now_ts, "result": result}
    return result


def _normalize_basis_monitor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    contract_type = normalize_contract_type(payload.get("contract_type", "usdm"))
    spot_quote_mode = _normalize_spot_quote_mode(payload.get("spot_quote_mode", "major_stables"))
    max_symbols = _safe_int(payload.get("max_symbols", 0), "max_symbols")
    cache_ttl_seconds = _safe_int(payload.get("cache_ttl_seconds", 5), "cache_ttl_seconds")
    refresh = _safe_bool(payload.get("refresh", False), "refresh")

    if max_symbols < 0 or max_symbols > 5000:
        raise ValueError("max_symbols must be in [0,5000]")
    if cache_ttl_seconds < 0 or cache_ttl_seconds > 300:
        raise ValueError("cache_ttl_seconds must be in [0,300]")

    return {
        "contract_type": contract_type,
        "spot_quote_mode": spot_quote_mode,
        "max_symbols": max_symbols,
        "cache_ttl_seconds": cache_ttl_seconds,
        "refresh": refresh,
    }


def _run_basis_monitor(params: dict[str, Any]) -> dict[str, Any]:
    key_obj = {
        "contract_type": params["contract_type"],
        "spot_quote_mode": params["spot_quote_mode"],
        "max_symbols": params["max_symbols"],
    }
    cache_key = json.dumps(key_obj, sort_keys=True, ensure_ascii=False)
    now_ts = time.time()
    stale_hit: dict[str, Any] | None = None
    with BASIS_CACHE_LOCK:
        stale_hit = BASIS_CACHE.get(cache_key)
    if (
        stale_hit
        and not params["refresh"]
        and params["cache_ttl_seconds"] > 0
        and now_ts - float(stale_hit.get("ts", 0)) <= params["cache_ttl_seconds"]
    ):
        cached_result = dict(stale_hit["result"])
        meta = dict(cached_result.get("meta", {}))
        meta["cached"] = True
        meta["stale"] = False
        cached_result["meta"] = meta
        return cached_result

    t0 = time.time()
    try:
        spot_markets = load_or_fetch_spot_markets(cache_dir="data", refresh=False)
        futures_symbols = load_or_fetch_futures_symbols(
            contract_type=params["contract_type"],
            cache_dir="data",
            refresh=False,
        )
        if params["max_symbols"] > 0:
            futures_symbols = futures_symbols[: params["max_symbols"]]

        spot_tickers = fetch_spot_book_tickers()
        futures_tickers = fetch_futures_book_tickers(contract_type=params["contract_type"])
        premium_rows = fetch_futures_premium_index(contract_type=params["contract_type"])
        rows = _build_basis_rows(
            futures_symbols=futures_symbols,
            spot_markets=spot_markets,
            spot_tickers=spot_tickers,
            futures_tickers=futures_tickers,
            premium_rows=premium_rows,
            contract_type=params["contract_type"],
            spot_quote_mode=params["spot_quote_mode"],
        )
        result = {
            "ok": True,
            "rows": rows,
            "meta": {
                "cached": False,
                "stale": False,
                "warning": "",
                "contract_type": params["contract_type"],
                "spot_quote_mode": params["spot_quote_mode"],
                "rows": len(rows),
                "spot_markets_considered": len(spot_markets),
                "futures_symbols_considered": len(futures_symbols),
                "snapshot_time": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": round(time.time() - t0, 2),
            },
        }
        with BASIS_CACHE_LOCK:
            BASIS_CACHE[cache_key] = {"ts": now_ts, "result": result}
        return result
    except Exception as exc:
        if stale_hit:
            cached_result = dict(stale_hit["result"])
            meta = dict(cached_result.get("meta", {}))
            meta["cached"] = True
            meta["stale"] = True
            meta["warning"] = f"{type(exc).__name__}: {exc}"
            cached_result["meta"] = meta
            return cached_result
        raise


def _chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("size must be > 0")
    return [items[i : i + size] for i in range(0, len(items), size)]


def _spot_snapshot_bucket_ms(target_ms: int) -> int:
    if target_ms <= 0:
        return 0
    return ((target_ms - 1) // 60_000) * 60_000


def _load_spot_close_before_funding(symbol: str, funding_time_ms: int) -> float | None:
    bucket_ms = _spot_snapshot_bucket_ms(funding_time_ms)
    cache_key = f"{symbol.upper()}:{bucket_ms}"
    now_ts = time.time()
    with SPOT_SNAPSHOT_CACHE_LOCK:
        hit = SPOT_SNAPSHOT_CACHE.get(cache_key)
    if hit and now_ts - float(hit.get("ts", 0)) <= 24 * 3600:
        return hit.get("close")

    close_price: float | None = None
    for start_ms in (bucket_ms, max(bucket_ms - 60_000, 0)):
        candles = fetch_spot_klines(
            symbol=symbol,
            interval="1m",
            start_ms=start_ms,
            end_ms=start_ms + 60_000,
            limit=1,
        )
        if candles:
            close_price = float(candles[-1].close)
            break

    with SPOT_SNAPSHOT_CACHE_LOCK:
        SPOT_SNAPSHOT_CACHE[cache_key] = {"ts": now_ts, "close": close_price}
    return close_price


def _load_latest_funding_snapshots(
    contract_type: str,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    unique_symbols = sorted({str(x).upper().strip() for x in symbols if str(x).strip()})
    if not unique_symbols:
        return {}
    cache_key = json.dumps(
        {"contract_type": contract_type, "symbols": [] if contract_type == "usdm" else unique_symbols},
        sort_keys=True,
        ensure_ascii=False,
    )
    now_ts = time.time()
    with LAST_FUNDING_CACHE_LOCK:
        hit = LAST_FUNDING_CACHE.get(cache_key)
    if hit and now_ts - float(hit.get("ts", 0)) <= 60:
        data = hit.get("rows", {})
        return {symbol: data.get(symbol) for symbol in unique_symbols if symbol in data}

    latest: dict[str, dict[str, Any]] = {}
    if contract_type == "usdm":
        rows = fetch_recent_funding_records(contract_type="usdm", limit=1000)
        for item in rows:
            symbol = item["symbol"]
            current = latest.get(symbol)
            if current is None or int(item["funding_time"]) > int(current["funding_time"]):
                latest[symbol] = item
        missing = [symbol for symbol in unique_symbols if symbol not in latest]
        for symbol in missing:
            rows = fetch_recent_funding_records(contract_type="usdm", symbol=symbol, limit=2)
            if not rows:
                continue
            latest[symbol] = max(rows, key=lambda item: int(item["funding_time"]))
    else:
        worker_count = min(max(1, len(unique_symbols)), 8)

        def _worker(symbol: str) -> tuple[str, dict[str, Any] | None]:
            rows = fetch_recent_funding_records(contract_type=contract_type, symbol=symbol, limit=2)
            if not rows:
                return symbol, None
            return symbol, max(rows, key=lambda item: int(item["funding_time"]))

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_worker, symbol) for symbol in unique_symbols]
            for future in as_completed(futures):
                symbol, row = future.result()
                if row is not None:
                    latest[symbol] = row

    with LAST_FUNDING_CACHE_LOCK:
        LAST_FUNDING_CACHE[cache_key] = {"ts": now_ts, "rows": latest}
    return {symbol: latest.get(symbol) for symbol in unique_symbols if symbol in latest}


def _borrow_not_required_payload() -> dict[str, Any]:
    return {
        "status": "not_required",
        "needs_borrow": False,
        "cross": None,
        "isolated": None,
        "vip": None,
        "error": "",
    }


def _load_borrow_reference_data(
    assets: list[str],
    spot_symbols: list[str],
) -> dict[str, Any]:
    api_key = load_binance_api_key()
    if api_key is None:
        return {
            "status": "missing_api_key",
            "mode": "safe",
            "errors": ["未配置 BINANCE_API_KEY"],
        }

    lookup_mode = load_binance_borrow_lookup_mode()
    credentials = load_binance_api_credentials()
    full_mode = lookup_mode == "full" and credentials is not None
    api_secret = credentials[1] if credentials is not None else ""
    cache_key = json.dumps(
        {
            "assets": sorted(set(assets)),
            "spot_symbols": sorted(set(spot_symbols)),
            "mode": "full" if full_mode else "safe",
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    now_ts = time.time()
    with BORROW_LOOKUP_CACHE_LOCK:
        hit = BORROW_LOOKUP_CACHE.get(cache_key)
    if hit and now_ts - float(hit.get("ts", 0)) <= 60:
        return hit["data"]

    unique_assets = sorted({x for x in assets if x})
    unique_symbols = sorted({x for x in spot_symbols if x})
    data: dict[str, Any] = {
        "status": "ok" if full_mode else "safe_readonly",
        "mode": "full" if full_mode else "safe",
        "all_assets": {},
        "restricted_open_long": set(),
        "restricted_max_collateral": set(),
        "inventory_margin": {},
        "inventory_isolated": {},
        "isolated_symbols": set(),
        "cross_rates": {},
        "isolated_rates": {},
        "cross_max": {},
        "isolated_max": {},
        "vip_loanable": {},
        "vip_rates": {},
        "errors": [],
    }
    if lookup_mode == "full" and not full_mode:
        data["errors"].append("未配置 BINANCE_API_SECRET，已回退到安全模式")

    try:
        data["all_assets"] = {row["asset"]: row for row in fetch_margin_all_assets(api_key)}
    except Exception as exc:
        data["errors"].append(f"allAssets: {type(exc).__name__}: {exc}")

    try:
        restricted = fetch_margin_restricted_assets(api_key)
        data["restricted_open_long"] = set(restricted.get("open_long_restricted", []))
        data["restricted_max_collateral"] = set(restricted.get("max_collateral_exceeded", []))
    except Exception as exc:
        data["errors"].append(f"restricted-asset: {type(exc).__name__}: {exc}")

    try:
        for chunk in _chunked(unique_assets, 20):
            for row in fetch_margin_next_hourly_interest_rates(chunk, True, api_key):
                data["isolated_rates"][row["asset"]] = row["next_hourly_interest_rate"]
    except Exception as exc:
        data["errors"].append(f"next-hourly-interest-rate(isolated): {type(exc).__name__}: {exc}")

    if not full_mode:
        with BORROW_LOOKUP_CACHE_LOCK:
            BORROW_LOOKUP_CACHE[cache_key] = {"ts": now_ts, "data": data}
        return data

    try:
        inventory = fetch_margin_available_inventory(api_key, margin_type="MARGIN")
        data["inventory_margin"] = inventory.get("assets", {})
    except Exception as exc:
        data["errors"].append(f"available-inventory(MARGIN): {type(exc).__name__}: {exc}")

    try:
        inventory = fetch_margin_available_inventory(api_key, margin_type="ISOLATED")
        data["inventory_isolated"] = inventory.get("assets", {})
    except Exception as exc:
        data["errors"].append(f"available-inventory(ISOLATED): {type(exc).__name__}: {exc}")

    try:
        isolated_rows = fetch_margin_isolated_all_pairs(api_key, api_secret)
        data["isolated_symbols"] = {row["symbol"] for row in isolated_rows if row.get("is_margin_trade")}
    except Exception as exc:
        data["errors"].append(f"isolated/allPairs: {type(exc).__name__}: {exc}")

    try:
        for chunk in _chunked(unique_assets, 20):
            for row in fetch_margin_next_hourly_interest_rates(chunk, False, api_key):
                data["cross_rates"][row["asset"]] = row["next_hourly_interest_rate"]
    except Exception as exc:
        data["errors"].append(f"next-hourly-interest-rate(cross): {type(exc).__name__}: {exc}")

    worker_count = min(max(1, len(unique_assets)), 8)
    try:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(fetch_margin_max_borrowable, asset, api_key, api_secret)
                for asset in unique_assets
            ]
            for future in as_completed(futures):
                row = future.result()
                data["cross_max"][row["asset"]] = row
    except Exception as exc:
        data["errors"].append(f"maxBorrowable(cross): {type(exc).__name__}: {exc}")

    isolated_targets = [symbol for symbol in unique_symbols if symbol in data["isolated_symbols"]]
    if isolated_targets:
        try:
            with ThreadPoolExecutor(max_workers=min(max(1, len(isolated_targets)), 8)) as executor:
                futures = [
                    executor.submit(
                        fetch_margin_max_borrowable,
                        _infer_base_quote_from_symbol(symbol, "usdm")[0],
                        api_key,
                        api_secret,
                        symbol,
                    )
                    for symbol in isolated_targets
                ]
                for future in as_completed(futures):
                    row = future.result()
                    isolated_symbol = row.get("isolated_symbol")
                    if isolated_symbol:
                        data["isolated_max"][isolated_symbol] = row
        except Exception as exc:
            data["errors"].append(f"maxBorrowable(isolated): {type(exc).__name__}: {exc}")

    try:
        for chunk in _chunked(unique_assets, 10):
            for row in fetch_vip_borrow_interest_rate(api_key, api_secret, chunk):
                data["vip_rates"][row["asset"]] = row
    except Exception as exc:
        data["errors"].append(f"vip/request/interestRate: {type(exc).__name__}: {exc}")

    for asset in unique_assets:
        try:
            rows = fetch_vip_loanable_assets_data(api_key, api_secret, asset)
            if rows:
                data["vip_loanable"][asset] = rows[0]
        except Exception as exc:
            data["errors"].append(f"vip/loanable/data({asset}): {type(exc).__name__}: {exc}")

    with BORROW_LOOKUP_CACHE_LOCK:
        BORROW_LOOKUP_CACHE[cache_key] = {"ts": now_ts, "data": data}
    return data


def _build_borrow_payload_for_item(item: dict[str, Any], refs: dict[str, Any]) -> dict[str, Any]:
    if item["arbitrage_side"] != "short_spot_long_perp":
        return _borrow_not_required_payload()
    if refs.get("status") == "missing_api_key":
        return {
            "status": "missing_api_key",
            "mode": "safe",
            "needs_borrow": True,
            "cross": None,
            "isolated": None,
            "vip": None,
            "note": "安全模式下仅需 BINANCE_API_KEY",
            "error": "未配置 BINANCE_API_KEY",
        }

    asset = item["base_asset"]
    spot_symbol = item["spot_symbol"]
    asset_meta = refs.get("all_assets", {}).get(asset)
    restricted = asset in refs.get("restricted_open_long", set()) or asset in refs.get(
        "restricted_max_collateral", set()
    )
    cross_row = refs.get("cross_max", {}).get(asset, {})
    isolated_row = refs.get("isolated_max", {}).get(spot_symbol, {})
    vip_row = refs.get("vip_loanable", {}).get(asset, {})
    vip_rate_row = refs.get("vip_rates", {}).get(asset, {})
    safe_mode = refs.get("mode") != "full"
    note = (
        "安全模式：仅显示理论可借与参考利率，不查询账户额度、库存、逐仓支持与 VIP Loan。"
        if safe_mode
        else ""
    )
    return {
        "status": refs.get("status") or ("ok" if not refs.get("errors") else "partial"),
        "mode": refs.get("mode", "full"),
        "needs_borrow": True,
        "cross": {
            "supported": bool(asset_meta.get("is_borrowable")) if asset_meta else None,
            "restricted": restricted,
            "inventory": None if safe_mode else refs.get("inventory_margin", {}).get(asset),
            "max_borrow": None if safe_mode else cross_row.get("amount"),
            "borrow_limit": None if safe_mode else cross_row.get("borrow_limit"),
            "next_hourly_interest_rate": refs.get("cross_rates", {}).get(asset),
        },
        "isolated": {
            "supported": None if safe_mode else spot_symbol in refs.get("isolated_symbols", set()),
            "restricted": restricted,
            "inventory": None if safe_mode else refs.get("inventory_isolated", {}).get(asset),
            "max_borrow": None if safe_mode else isolated_row.get("amount"),
            "borrow_limit": None if safe_mode else isolated_row.get("borrow_limit"),
            "next_hourly_interest_rate": refs.get("isolated_rates", {}).get(asset),
            "symbol": spot_symbol,
        },
        "vip": {
            "available": None if safe_mode else asset in refs.get("vip_loanable", {}),
            "max_limit": None if safe_mode else vip_row.get("max_limit"),
            "min_limit": None if safe_mode else vip_row.get("min_limit"),
            "flexible_daily_interest_rate": (
                None
                if safe_mode
                else (
                    vip_rate_row.get("flexible_daily_interest_rate")
                    or vip_row.get("flexible_daily_interest_rate")
                )
            ),
            "flexible_yearly_interest_rate": (
                None
                if safe_mode
                else (
                    vip_rate_row.get("flexible_yearly_interest_rate")
                    or vip_row.get("flexible_yearly_interest_rate")
                )
            ),
        },
        "note": note,
        "error": "; ".join(refs.get("errors", [])[:3]),
    }


def _normalize_basis_enrich_payload(payload: dict[str, Any]) -> dict[str, Any]:
    contract_type = normalize_contract_type(payload.get("contract_type", "usdm"))
    items_raw = payload.get("items", [])
    if not isinstance(items_raw, list):
        raise ValueError("items must be a list")
    if not items_raw:
        return {"contract_type": contract_type, "items": []}
    if len(items_raw) > 20:
        raise ValueError("items length must be <= 20")

    items: list[dict[str, Any]] = []
    for item in items_raw:
        if not isinstance(item, dict):
            raise ValueError("each item must be an object")
        futures_symbol = str(item.get("futures_symbol", "")).upper().strip()
        spot_symbol = str(item.get("spot_symbol", "")).upper().strip()
        base_asset = str(item.get("base_asset", "")).upper().strip()
        arbitrage_side = str(item.get("arbitrage_side", "")).strip().lower()
        if not futures_symbol or not spot_symbol or not base_asset:
            raise ValueError("each item must include futures_symbol, spot_symbol, base_asset")
        if arbitrage_side not in {"long_spot_short_perp", "short_spot_long_perp", "watch"}:
            arbitrage_side = "watch"
        items.append(
            {
                "futures_symbol": futures_symbol,
                "spot_symbol": spot_symbol,
                "base_asset": base_asset,
                "arbitrage_side": arbitrage_side,
            }
        )
    return {"contract_type": contract_type, "items": items}


def _run_basis_enrich(params: dict[str, Any]) -> dict[str, Any]:
    if not params["items"]:
        return {"ok": True, "rows": {}, "meta": {"rows": 0, "elapsed_seconds": 0.0}}

    now_ts = time.time()
    details: dict[str, dict[str, Any]] = {}
    pending_items: list[dict[str, Any]] = []
    for item in params["items"]:
        cache_key = json.dumps(
            {
                "contract_type": params["contract_type"],
                "futures_symbol": item["futures_symbol"],
                "spot_symbol": item["spot_symbol"],
                "arbitrage_side": item["arbitrage_side"],
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        with DETAIL_CACHE_LOCK:
            hit = DETAIL_CACHE.get(cache_key)
        if hit and now_ts - float(hit.get("ts", 0)) <= 60:
            details[item["futures_symbol"]] = hit["detail"]
        else:
            pending_items.append(item)

    if not pending_items:
        return {
            "ok": True,
            "rows": details,
            "meta": {"rows": len(details), "cached_only": True, "elapsed_seconds": 0.0},
        }

    t0 = time.time()
    funding_map = _load_latest_funding_snapshots(
        params["contract_type"],
        [item["futures_symbol"] for item in pending_items],
    )
    borrow_refs = _load_borrow_reference_data(
        [item["base_asset"] for item in pending_items if item["arbitrage_side"] == "short_spot_long_perp"],
        [item["spot_symbol"] for item in pending_items if item["arbitrage_side"] == "short_spot_long_perp"],
    )

    for item in pending_items:
        futures_symbol = item["futures_symbol"]
        funding = funding_map.get(futures_symbol)
        previous_funding_time_ms = int(funding["funding_time"]) if funding else None
        previous_spot_close: float | None = None
        previous_mark_price = float(funding["mark_price"]) if funding and funding.get("mark_price") else None
        previous_basis: float | None = None
        detail_error = ""
        try:
            previous_spot_close = (
                _load_spot_close_before_funding(item["spot_symbol"], previous_funding_time_ms)
                if previous_funding_time_ms
                else None
            )
            previous_basis = (
                previous_mark_price / previous_spot_close - 1.0
                if previous_mark_price and previous_spot_close and previous_spot_close > 0
                else None
            )
        except Exception as exc:
            detail_error = f"{type(exc).__name__}: {exc}"

        borrow_payload = _build_borrow_payload_for_item(item, borrow_refs)
        if detail_error:
            merged_error = "; ".join(x for x in [borrow_payload.get("error", ""), detail_error] if x)
            borrow_payload["error"] = merged_error

        detail = {
            "previous_funding_time": (
                datetime.fromtimestamp(previous_funding_time_ms / 1000, tz=timezone.utc).isoformat()
                if previous_funding_time_ms
                else None
            ),
            "previous_funding_rate": funding.get("funding_rate") if funding else None,
            "previous_funding_mark_price": previous_mark_price,
            "previous_spot_close": previous_spot_close,
            "previous_basis": previous_basis,
            "borrow": borrow_payload,
        }
        details[futures_symbol] = detail
        cache_key = json.dumps(
            {
                "contract_type": params["contract_type"],
                "futures_symbol": item["futures_symbol"],
                "spot_symbol": item["spot_symbol"],
                "arbitrage_side": item["arbitrage_side"],
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        with DETAIL_CACHE_LOCK:
            DETAIL_CACHE[cache_key] = {"ts": now_ts, "detail": detail}

    return {
        "ok": True,
        "rows": details,
        "meta": {
            "rows": len(details),
            "cached_only": False,
            "elapsed_seconds": round(time.time() - t0, 2),
        },
    }


def _normalize_compare_payload(payload: dict[str, Any]) -> dict[str, Any]:
    market_type = normalize_market_type(payload.get("market_type", "futures"))
    contract_type = (
        normalize_contract_type(payload.get("contract_type", "usdm"))
        if market_type == "futures"
        else None
    )
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    start_time = _safe_datetime(payload.get("start_time"), "start_time")
    end_time = _safe_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1h")).strip()
    strategy_direction = str(payload.get("strategy_direction", "long")).strip().lower()
    grid_level_mode = str(payload.get("grid_level_mode", "arithmetic")).strip().lower()
    include_funding = _safe_bool(payload.get("include_funding", True), "include_funding")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    refresh = bool(payload.get("refresh", False))

    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    _enforce_seconds_interval_limit(start_time, end_time, interval)
    if fee_rate < 0 or slippage < 0:
        raise ValueError("fee_rate/slippage must be >= 0")
    if strategy_direction not in set(supported_strategy_directions()):
        raise ValueError(
            f"unsupported strategy_direction: {strategy_direction}; "
            f"supported: {','.join(supported_strategy_directions())}"
        )
    if grid_level_mode not in set(supported_grid_level_modes()):
        raise ValueError(
            f"unsupported grid_level_mode: {grid_level_mode}; "
            f"supported: {','.join(supported_grid_level_modes())}"
        )
    if market_type == "spot":
        strategy_direction = "long"
        include_funding = False
    _validate_market_symbol(symbol=symbol, market_type=market_type, contract_type=contract_type)

    raw_layers = payload.get("layers")
    if not isinstance(raw_layers, list) or not raw_layers:
        raise ValueError("layers must be a non-empty list")

    layers = []
    supported_modes = set(supported_allocation_modes())
    for i, item in enumerate(raw_layers):
        if not isinstance(item, dict):
            raise ValueError(f"layers[{i}] must be an object")
        min_price = _safe_float(item.get("min_price"), f"layers[{i}].min_price")
        max_price = _safe_float(item.get("max_price"), f"layers[{i}].max_price")
        n = _safe_int(item.get("n", item.get("suggest_n")), f"layers[{i}].n")
        notional = _safe_float(item.get("notional"), f"layers[{i}].notional")
        grid_span = str(item.get("grid_span", "-"))
        layer_mode = str(item.get("layer_mode", "equal")).strip().lower()
        if min_price <= 0 or max_price <= 0 or min_price >= max_price:
            raise ValueError(f"layers[{i}] has invalid price range")
        if n <= 0:
            raise ValueError(f"layers[{i}].n must be > 0")
        if notional <= 0:
            raise ValueError(f"layers[{i}].notional must be > 0")
        if layer_mode not in supported_modes:
            raise ValueError(
                f"layers[{i}].layer_mode unsupported: {layer_mode}; "
                f"supported: {','.join(sorted(supported_modes))}"
            )
        layers.append(
            {
                "min_price": min_price,
                "max_price": max_price,
                "n": n,
                "notional": notional,
                "grid_span": grid_span,
                "layer_mode": layer_mode,
            }
        )

    baseline_raw = payload.get("baseline", {})
    if baseline_raw is None:
        baseline_raw = {}
    if not isinstance(baseline_raw, dict):
        raise ValueError("baseline must be object")

    baseline: dict[str, Any] = {
        "n": baseline_raw.get("n"),
        "allocation_mode": baseline_raw.get("allocation_mode"),
        "grid_level_mode": baseline_raw.get("grid_level_mode"),
    }
    float_keys = [
        "total_buy_notional",
        "net_profit",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "calmar",
        "trade_volume",
        "total_fees",
        "funding_pnl",
        "avg_capital_usage",
        "realized_pnl",
        "unrealized_pnl",
    ]
    int_keys = ["trade_count", "funding_event_count"]
    for key in float_keys:
        if key in baseline_raw and baseline_raw[key] is not None:
            baseline[key] = _safe_float(baseline_raw.get(key), f"baseline.{key}")
    for key in int_keys:
        if key in baseline_raw and baseline_raw[key] is not None:
            baseline[key] = _safe_int(baseline_raw.get(key), f"baseline.{key}")

    return {
        "market_type": market_type,
        "contract_type": contract_type,
        "symbol": symbol,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "strategy_direction": strategy_direction,
        "grid_level_mode": grid_level_mode,
        "include_funding": include_funding,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "refresh": refresh,
        "layers": layers,
        "baseline": baseline,
    }


def _normalize_funding_breakdown_payload(payload: dict[str, Any]) -> dict[str, Any]:
    contract_type = normalize_contract_type(payload.get("contract_type", "usdm"))
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    min_price = _safe_float(payload.get("min_price"), "min_price")
    max_price = _safe_float(payload.get("max_price"), "max_price")
    n = _safe_int(payload.get("n"), "n")
    allocation_mode = str(payload.get("allocation_mode", "equal")).strip().lower()
    strategy_direction = str(payload.get("strategy_direction", "long")).strip().lower()
    grid_level_mode = str(payload.get("grid_level_mode", "arithmetic")).strip().lower()
    total_buy_notional = _safe_float(payload.get("total_buy_notional"), "total_buy_notional")
    start_time = _safe_datetime(payload.get("start_time"), "start_time")
    end_time = _safe_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1h")).strip()
    include_funding = _safe_bool(payload.get("include_funding", True), "include_funding")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    refresh = bool(payload.get("refresh", False))
    fixed_per_grid_qty_raw = payload.get("fixed_per_grid_qty")
    fixed_per_grid_qty = None
    if fixed_per_grid_qty_raw is not None:
        qty = _safe_float(fixed_per_grid_qty_raw, "fixed_per_grid_qty")
        if qty > 0:
            fixed_per_grid_qty = qty

    if min_price <= 0 or max_price <= 0 or min_price >= max_price:
        raise ValueError("invalid min_price/max_price")
    if n <= 0:
        raise ValueError("n must be > 0")
    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")
    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    _enforce_seconds_interval_limit(start_time, end_time, interval)
    if fee_rate < 0 or slippage < 0:
        raise ValueError("fee_rate/slippage must be >= 0")
    if strategy_direction not in set(supported_strategy_directions()):
        raise ValueError(
            f"unsupported strategy_direction: {strategy_direction}; "
            f"supported: {','.join(supported_strategy_directions())}"
        )
    if grid_level_mode not in set(supported_grid_level_modes()):
        raise ValueError(
            f"unsupported grid_level_mode: {grid_level_mode}; "
            f"supported: {','.join(supported_grid_level_modes())}"
        )
    supported_modes = set(supported_allocation_modes())
    if allocation_mode not in supported_modes:
        raise ValueError(
            f"unsupported allocation_mode: {allocation_mode}; "
            f"supported: {','.join(sorted(supported_modes))}"
        )
    try:
        supported_symbols = set(
            load_or_fetch_futures_symbols(
                contract_type=contract_type,
                cache_dir="data",
                refresh=False,
            )
        )
        if supported_symbols and symbol not in supported_symbols:
            raise ValueError(f"unsupported symbol: {symbol}")
    except Exception:
        pass

    return {
        "contract_type": contract_type,
        "symbol": symbol,
        "min_price": min_price,
        "max_price": max_price,
        "n": n,
        "allocation_mode": allocation_mode,
        "strategy_direction": strategy_direction,
        "grid_level_mode": grid_level_mode,
        "total_buy_notional": total_buy_notional,
        "fixed_per_grid_qty": fixed_per_grid_qty,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "include_funding": include_funding,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "refresh": refresh,
    }


def _run_funding_breakdown(params: dict[str, Any]) -> dict[str, Any]:
    cache_path = str(
        cache_file_path(
            params["symbol"],
            params["interval"],
            cache_dir="data",
            contract_type=params["contract_type"],
        )
    )
    funding_cache_path = (
        str(
            funding_cache_file_path(
                params["symbol"],
                cache_dir="data",
                contract_type=params["contract_type"],
            )
        )
        if params["include_funding"]
        else None
    )
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        start_time=params["start_time"],
        end_time=params["end_time"],
        cache_dir="data",
        contract_type=params["contract_type"],
        refresh=params["refresh"],
    )
    funding_rates = (
        load_or_fetch_funding_rates(
            symbol=params["symbol"],
            start_time=params["start_time"],
            end_time=params["end_time"],
            cache_dir="data",
            contract_type=params["contract_type"],
            refresh=params["refresh"],
        )
        if params["include_funding"]
        else []
    )

    result = run_backtest(
        candles=candles,
        min_price=params["min_price"],
        max_price=params["max_price"],
        n=params["n"],
        total_buy_notional=params["total_buy_notional"],
        grid_level_mode=params["grid_level_mode"],
        allocation_mode=params["allocation_mode"],
        strategy_direction=params["strategy_direction"],
        fee_rate=params["fee_rate"],
        slippage=params["slippage"],
        funding_rates=funding_rates,
        fixed_per_grid_qty=params["fixed_per_grid_qty"],
        capture_funding_events=True,
        capture_trades=False,
    )
    settle_asset = _infer_settle_asset(params["symbol"], params["contract_type"])
    initial_asset_mode = (
        "settle"
        if (params.get("fixed_per_grid_qty") is not None or params.get("allocation_mode") == "equal_qty")
        else "quote"
    )
    initial_asset_qty_raw = sum(abs(float(x)) for x in (result.per_grid_qty or []))
    initial_asset_quote_raw = (
        initial_asset_qty_raw * result.start_price if result.start_price > 0 else None
    )
    if initial_asset_mode == "settle":
        initial_asset_qty = initial_asset_qty_raw
        initial_asset_quote = initial_asset_quote_raw
        equity_base_quote = (
            initial_asset_quote
            if initial_asset_quote is not None and initial_asset_quote > 0
            else params["total_buy_notional"]
        )
        initial_asset_qty_approx = initial_asset_qty
    else:
        initial_asset_qty = None
        initial_asset_quote = params["total_buy_notional"]
        if initial_asset_quote <= 0:
            initial_asset_quote = sum(abs(float(x)) for x in (result.per_grid_notionals or []))
        equity_base_quote = initial_asset_quote
        initial_asset_qty_approx = (
            (initial_asset_quote / result.start_price)
            if initial_asset_quote > 0 and result.start_price > 0
            else None
        )
    rows = []
    for event in result.funding_events or []:
        account_equity_quote = equity_base_quote + event.net_pnl
        net_to_initial = None
        equity_to_initial = None
        if initial_asset_mode == "settle":
            if initial_asset_qty and initial_asset_qty > 0 and event.mark_price > 0:
                net_to_initial = (event.net_pnl / event.mark_price) / initial_asset_qty
                equity_to_initial = (account_equity_quote / event.mark_price) / initial_asset_qty
        else:
            if equity_base_quote > 0:
                net_to_initial = event.net_pnl / equity_base_quote
                equity_to_initial = account_equity_quote / equity_base_quote
        margin_snapshot = _funding_margin_snapshot(
            position_notional=event.position_notional,
            reference_price=event.mark_price,
            account_equity=account_equity_quote,
            margin_ratio=FUNDING_MARGIN_RATIO,
        )
        rows.append(
            {
                "ts": event.ts.isoformat(),
                "rate": event.rate,
                "reference_price": event.mark_price,
                "position_notional": event.position_notional,
                "net_pnl": event.net_pnl,
                "account_equity": account_equity_quote,
                "initial_asset_mode": initial_asset_mode,
                "initial_asset_qty": initial_asset_qty,
                "initial_asset_qty_approx": initial_asset_qty_approx,
                "initial_asset_quote": initial_asset_quote,
                "initial_asset_start_price": result.start_price,
                "net_to_initial": net_to_initial,
                "equity_to_initial": equity_to_initial,
                "pnl": event.pnl,
                "cumulative_pnl": event.cumulative_pnl,
                "minimum_margin": margin_snapshot["minimum_margin"],
                "liquidation_price": margin_snapshot["liquidation_price"],
                "withdrawable_amount": margin_snapshot["withdrawable_amount"],
            }
        )
    return {
        "ok": True,
        "rows": rows,
        "summary": {
            "funding_pnl": result.funding_pnl,
            "funding_event_count": result.funding_event_count,
            "total_fees": result.total_fees,
            "net_profit": result.net_profit,
            "total_return": result.total_return,
            "period_low": result.period_low,
            "period_high": result.period_high,
            "period_amplitude": result.period_amplitude,
            "margin_ratio": FUNDING_MARGIN_RATIO,
            "initial_asset_mode": initial_asset_mode,
            "initial_asset_qty": initial_asset_qty,
            "initial_asset_qty_approx": initial_asset_qty_approx,
            "initial_asset_quote": initial_asset_quote,
            "initial_asset_quote_base": equity_base_quote,
            "settle_asset": settle_asset,
            "start_price": result.start_price,
        },
        "data": {
            "candles": len(candles),
            "cache_file": cache_path,
            "funding_cache_file": funding_cache_path,
            "symbol": params["symbol"],
            "contract_type": params["contract_type"],
            "interval": params["interval"],
            "strategy_direction": params["strategy_direction"],
            "grid_level_mode": params["grid_level_mode"],
            "start_time": params["start_time"].isoformat(),
            "end_time": params["end_time"].isoformat(),
            "include_funding": params["include_funding"],
            "funding_events": len(rows),
            "margin_ratio": FUNDING_MARGIN_RATIO,
        },
    }


def _normalize_grid_preview_payload(payload: dict[str, Any]) -> dict[str, Any]:
    market_type = normalize_market_type(payload.get("market_type", "futures"))
    contract_type = (
        normalize_contract_type(payload.get("contract_type", "usdm"))
        if market_type == "futures"
        else None
    )
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    strategy_direction = str(payload.get("strategy_direction", "neutral")).strip().lower()
    grid_level_mode = str(payload.get("grid_level_mode", "arithmetic")).strip().lower()
    min_price = _safe_float(payload.get("min_price"), "min_price")
    max_price = _safe_float(payload.get("max_price"), "max_price")
    n = _safe_int(payload.get("n"), "n")
    margin_amount = _safe_float(payload.get("margin_amount"), "margin_amount")
    leverage = _safe_float(payload.get("leverage", 1.0), "leverage")

    if min_price <= 0 or max_price <= 0 or min_price >= max_price:
        raise ValueError("invalid min_price/max_price")
    if n <= 0:
        raise ValueError("n must be > 0")
    if margin_amount <= 0:
        raise ValueError("margin_amount must be > 0")
    if leverage <= 0:
        raise ValueError("leverage must be > 0")
    if strategy_direction not in set(supported_strategy_directions()):
        raise ValueError(
            f"unsupported strategy_direction: {strategy_direction}; "
            f"supported: {','.join(supported_strategy_directions())}"
        )
    if grid_level_mode not in set(supported_grid_level_modes()):
        raise ValueError(
            f"unsupported grid_level_mode: {grid_level_mode}; "
            f"supported: {','.join(supported_grid_level_modes())}"
        )
    if market_type == "spot":
        strategy_direction = "long"
        leverage = 1.0
    _validate_market_symbol(symbol=symbol, market_type=market_type, contract_type=contract_type)

    return {
        "market_type": market_type,
        "contract_type": contract_type,
        "symbol": symbol,
        "strategy_direction": strategy_direction,
        "grid_level_mode": grid_level_mode,
        "min_price": min_price,
        "max_price": max_price,
        "n": n,
        "margin_amount": margin_amount,
        "leverage": leverage,
    }


def _estimated_grid_liquidation_snapshot(
    *,
    side: str,
    avg_entry_price: float,
    position_qty: float,
    allocated_margin: float,
    maintenance_margin_ratio: float = GRID_PREVIEW_MAINTENANCE_MARGIN_RATIO,
) -> dict[str, float | None]:
    qty = max(float(position_qty), 0.0)
    entry_price = max(float(avg_entry_price), 0.0)
    margin = max(float(allocated_margin), 0.0)
    mmr = max(float(maintenance_margin_ratio), 0.0)
    if qty <= 0.0 or entry_price <= 0.0:
        return {
            "entry_notional": 0.0,
            "allocated_margin": margin,
            "minimum_margin": 0.0,
            "withdrawable_amount": margin,
            "liquidation_price": None,
        }

    entry_notional = qty * entry_price
    minimum_margin = entry_notional * mmr
    withdrawable_amount = max(margin - minimum_margin, 0.0)

    normalized_side = str(side).strip().lower()
    liquidation_price: float | None = None
    if normalized_side == "long":
        denominator = qty * max(1.0 - mmr, 1e-9)
        liquidation_price = max((entry_notional - margin) / denominator, 0.0)
    elif normalized_side == "short":
        denominator = qty * (1.0 + mmr)
        liquidation_price = max((entry_notional + margin) / denominator, 0.0)

    return {
        "entry_notional": entry_notional,
        "allocated_margin": margin,
        "minimum_margin": minimum_margin,
        "withdrawable_amount": withdrawable_amount,
        "liquidation_price": liquidation_price,
    }


def _run_grid_preview(params: dict[str, Any]) -> dict[str, Any]:
    market_type = params["market_type"]
    contract_type = params["contract_type"]
    symbol = params["symbol"]
    if market_type == "spot":
        symbol_info = fetch_spot_symbol_config(symbol)
        book_rows = fetch_spot_book_tickers(symbol=symbol)
        if book_rows:
            book = book_rows[0]
            bid_price = _safe_float(book.get("bid_price"), "bid_price")
            ask_price = _safe_float(book.get("ask_price"), "ask_price")
            current_price = (bid_price + ask_price) / 2.0
        else:
            current_price = fetch_spot_latest_price(symbol)
            bid_price = current_price
            ask_price = current_price
    else:
        symbol_info = fetch_futures_symbol_config(symbol, contract_type=contract_type)
        book_rows = fetch_futures_book_tickers(contract_type=contract_type, symbol=symbol)
        if book_rows:
            book = book_rows[0]
            bid_price = _safe_float(book.get("bid_price"), "bid_price")
            ask_price = _safe_float(book.get("ask_price"), "ask_price")
            current_price = (bid_price + ask_price) / 2.0
        else:
            current_price = fetch_futures_latest_price(symbol, contract_type=contract_type)
            bid_price = current_price
            ask_price = current_price

    preview_time = datetime.now(timezone.utc)
    preview_candle = Candle(
        open_time=preview_time,
        close_time=preview_time,
        open=current_price,
        high=current_price,
        low=current_price,
        close=current_price,
    )
    position_budget_notional = (
        params["margin_amount"] if market_type == "spot" else params["margin_amount"] * params["leverage"]
    )
    result = run_backtest(
        candles=[preview_candle],
        min_price=params["min_price"],
        max_price=params["max_price"],
        n=params["n"],
        total_buy_notional=position_budget_notional,
        grid_level_mode=params["grid_level_mode"],
        allocation_mode="equal",
        strategy_direction=params["strategy_direction"],
        fee_rate=0.0,
        slippage=0.0,
        funding_rates=[],
        bootstrap_positions=market_type != "spot",
        capture_trades=False,
        capture_funding_events=False,
        capture_curves=False,
    )

    rows: list[dict[str, Any]] = []
    startup_long_qty = 0.0
    startup_short_qty = 0.0
    full_long_qty = 0.0
    full_short_qty = 0.0
    full_long_entry_notional = 0.0
    full_short_entry_notional = 0.0
    active_buy_orders = 0
    active_sell_orders = 0

    if market_type == "spot":
        for idx in range(result.n):
            lower_price = result.grid_levels[idx]
            upper_price = result.grid_levels[idx + 1]
            qty = result.per_grid_qty[idx]
            entry_notional = result.per_grid_notionals[idx]
            full_long_qty += qty
            full_long_entry_notional += entry_notional
            active_order_side: str | None = None
            active_order_price: float | None = None
            startup_state = "等待重新站回该格上方后激活"
            if lower_price < current_price:
                active_order_side = "BUY"
                active_order_price = lower_price
                startup_state = "待买入"
                active_buy_orders += 1
            rows.append(
                {
                    "idx": idx + 1,
                    "grid_side": "long",
                    "lower_price": lower_price,
                    "upper_price": upper_price,
                    "entry_side": "BUY",
                    "entry_price": lower_price,
                    "exit_side": "SELL",
                    "exit_price": upper_price,
                    "entry_notional": entry_notional,
                    "qty": qty,
                    "startup_state": startup_state,
                    "active_order_side": active_order_side,
                    "active_order_price": active_order_price,
                }
            )
    else:
        for idx in range(result.n):
            grid_side = result.grid_sides[idx]
            lower_price = result.grid_levels[idx]
            upper_price = result.grid_levels[idx + 1]
            qty = result.per_grid_qty[idx]
            entry_notional = result.per_grid_notionals[idx]

            if grid_side == "long":
                entry_side = "BUY"
                entry_price = lower_price
                exit_side = "SELL"
                exit_price = upper_price
                is_bootstrapped = upper_price > current_price
                startup_state = "预建多仓待卖出" if is_bootstrapped else "待买入"
                active_order_side = "SELL" if is_bootstrapped else "BUY"
                active_order_price = upper_price if is_bootstrapped else lower_price
                if is_bootstrapped:
                    startup_long_qty += qty
                full_long_qty += qty
                full_long_entry_notional += entry_notional
            else:
                entry_side = "SELL"
                entry_price = upper_price
                exit_side = "BUY"
                exit_price = lower_price
                is_bootstrapped = lower_price < current_price
                startup_state = "预建空仓待回补" if is_bootstrapped else "待卖出开空"
                active_order_side = "BUY" if is_bootstrapped else "SELL"
                active_order_price = lower_price if is_bootstrapped else upper_price
                if is_bootstrapped:
                    startup_short_qty += qty
                full_short_qty += qty
                full_short_entry_notional += entry_notional

            if active_order_side == "BUY":
                active_buy_orders += 1
            else:
                active_sell_orders += 1

            rows.append(
                {
                    "idx": idx + 1,
                    "grid_side": grid_side,
                    "lower_price": lower_price,
                    "upper_price": upper_price,
                    "entry_side": entry_side,
                    "entry_price": entry_price,
                    "exit_side": exit_side,
                    "exit_price": exit_price,
                    "entry_notional": entry_notional,
                    "qty": qty,
                    "startup_state": startup_state,
                    "active_order_side": active_order_side,
                    "active_order_price": active_order_price,
                }
            )

    startup_long_notional = startup_long_qty * current_price
    startup_short_notional = startup_short_qty * current_price
    startup_abs_notional = startup_long_notional + startup_short_notional
    full_abs_notional = full_long_entry_notional + full_short_entry_notional

    startup_long_margin = (
        params["margin_amount"] * (startup_long_notional / startup_abs_notional)
        if startup_abs_notional > 0 and startup_long_notional > 0
        else 0.0
    )
    startup_short_margin = (
        params["margin_amount"] * (startup_short_notional / startup_abs_notional)
        if startup_abs_notional > 0 and startup_short_notional > 0
        else 0.0
    )
    full_long_margin = (
        params["margin_amount"] * (full_long_entry_notional / full_abs_notional)
        if full_abs_notional > 0 and full_long_entry_notional > 0
        else 0.0
    )
    full_short_margin = (
        params["margin_amount"] * (full_short_entry_notional / full_abs_notional)
        if full_abs_notional > 0 and full_short_entry_notional > 0
        else 0.0
    )

    startup_long_snapshot = _estimated_grid_liquidation_snapshot(
        side="long",
        avg_entry_price=current_price,
        position_qty=startup_long_qty,
        allocated_margin=startup_long_margin,
    )
    startup_short_snapshot = _estimated_grid_liquidation_snapshot(
        side="short",
        avg_entry_price=current_price,
        position_qty=startup_short_qty,
        allocated_margin=startup_short_margin,
    )
    full_long_avg_entry = (full_long_entry_notional / full_long_qty) if full_long_qty > 0 else 0.0
    full_short_avg_entry = (full_short_entry_notional / full_short_qty) if full_short_qty > 0 else 0.0
    full_long_snapshot = _estimated_grid_liquidation_snapshot(
        side="long",
        avg_entry_price=full_long_avg_entry,
        position_qty=full_long_qty,
        allocated_margin=full_long_margin,
    )
    full_short_snapshot = _estimated_grid_liquidation_snapshot(
        side="short",
        avg_entry_price=full_short_avg_entry,
        position_qty=full_short_qty,
        allocated_margin=full_short_margin,
    )
    warnings: list[str] = []
    strategy_direction = params["strategy_direction"]
    if market_type == "spot":
        if current_price >= params["max_price"]:
            warnings.append("现价已高于现货网格上沿；当前会以下方买单为主，不会自动补出现货底仓。")
        elif current_price <= params["min_price"]:
            warnings.append("现价已低于现货网格下沿；V1 不自动回补历史应持仓位，需等待价格回到区间后再逐步激活买单。")
    elif strategy_direction == "long":
        if current_price >= params["max_price"]:
            warnings.append("现价已高于做多网格上沿；当前会退化为下方接回撤买单，卖单不会以 maker 活动委托形式保留。")
        elif current_price <= params["min_price"]:
            warnings.append("现价已低于做多网格下沿；启动时会预建接近满格多仓，活动委托将以上方卖单为主。")
    elif strategy_direction == "short":
        if current_price <= params["min_price"]:
            warnings.append("现价已低于做空网格下沿；当前会退化为上方等反弹卖空，买单不会以 maker 活动委托形式保留。")
        elif current_price >= params["max_price"]:
            warnings.append("现价已高于做空网格上沿；启动时会预建接近满格空仓，活动委托将以下方买单为主。")
    else:
        if current_price < params["min_price"] or current_price > params["max_price"]:
            warnings.append("现价已跑出中性网格区间；当前会偏向单边预建仓和单边活动委托，建议先重设区间。")

    return {
        "ok": True,
        "summary": {
            "market_type": market_type,
            "contract_type": contract_type,
            "symbol": symbol,
            "strategy_direction": params["strategy_direction"],
            "grid_level_mode": params["grid_level_mode"],
            "allocation_mode": "equal",
            "current_price": current_price,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "grid_count": params["n"],
            "min_price": params["min_price"],
            "max_price": params["max_price"],
            "margin_amount": params["margin_amount"],
            "leverage": params["leverage"],
            "position_budget_notional": position_budget_notional,
            "neutral_anchor_price": result.neutral_anchor_price,
            "long_grid_count": sum(1 for side in result.grid_sides if side == "long"),
            "short_grid_count": sum(1 for side in result.grid_sides if side == "short"),
            "active_buy_orders": active_buy_orders,
            "active_sell_orders": active_sell_orders,
            "startup_long_qty": startup_long_qty,
            "startup_short_qty": startup_short_qty,
            "startup_net_qty": startup_long_qty - startup_short_qty,
            "startup_long_notional": startup_long_notional,
            "startup_short_notional": startup_short_notional,
            "startup_net_notional": startup_long_notional - startup_short_notional,
            "full_long_qty": full_long_qty,
            "full_short_qty": full_short_qty,
            "full_long_entry_notional": full_long_entry_notional,
            "full_short_entry_notional": full_short_entry_notional,
            "startup_long_liquidation_price": startup_long_snapshot["liquidation_price"],
            "startup_short_liquidation_price": startup_short_snapshot["liquidation_price"],
            "full_long_liquidation_price": full_long_snapshot["liquidation_price"],
            "full_short_liquidation_price": full_short_snapshot["liquidation_price"],
            "maintenance_margin_ratio": GRID_PREVIEW_MAINTENANCE_MARGIN_RATIO,
            "warnings": warnings,
            "symbol_info": {
                "tick_size": symbol_info.get("tick_size"),
                "step_size": symbol_info.get("step_size"),
                "min_qty": symbol_info.get("min_qty"),
                "min_notional": symbol_info.get("min_notional"),
            },
        },
        "rows": rows,
    }


def _run_layer_compare(params: dict[str, Any]) -> dict[str, Any]:
    cache_path = str(
        cache_file_path(
            params["symbol"],
            params["interval"],
            cache_dir="data",
            contract_type=params.get("contract_type") or "usdm",
            market_type=params["market_type"],
        )
    )
    funding_cache_path = (
        str(
            funding_cache_file_path(
                params["symbol"],
                cache_dir="data",
                contract_type=params.get("contract_type") or "usdm",
                market_type=params["market_type"],
            )
        )
        if params["include_funding"] and params["market_type"] == "futures"
        else None
    )
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        start_time=params["start_time"],
        end_time=params["end_time"],
        cache_dir="data",
        contract_type=params.get("contract_type") or "usdm",
        market_type=params["market_type"],
        refresh=params["refresh"],
    )
    funding_rates = (
        load_or_fetch_funding_rates(
            symbol=params["symbol"],
            start_time=params["start_time"],
            end_time=params["end_time"],
            cache_dir="data",
            contract_type=params.get("contract_type") or "usdm",
            market_type=params["market_type"],
            refresh=params["refresh"],
        )
        if params["include_funding"] and params["market_type"] == "futures"
        else []
    )
    if not candles:
        raise ValueError("No candle data")

    combined_equity = [0.0] * len(candles)
    combined_capital = [0.0] * len(candles)

    total_buy_notional = 0.0
    net_profit = 0.0
    total_fees = 0.0
    funding_pnl = 0.0
    funding_event_count = 0
    trade_count = 0
    trade_volume = 0.0
    realized_pnl = 0.0
    unrealized_pnl = 0.0
    layer_total_n = 0

    for layer in params["layers"]:
        result = run_backtest(
            candles=candles,
            min_price=layer["min_price"],
            max_price=layer["max_price"],
            n=layer["n"],
            total_buy_notional=layer["notional"],
            grid_level_mode=params["grid_level_mode"],
            allocation_mode=layer["layer_mode"],
            strategy_direction=params["strategy_direction"],
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            funding_rates=funding_rates,
            bootstrap_positions=params["market_type"] != "spot",
            capture_trades=False,
            capture_curves=True,
        )
        total_buy_notional += layer["notional"]
        net_profit += result.net_profit
        total_fees += result.total_fees
        funding_pnl += result.funding_pnl
        funding_event_count += result.funding_event_count
        trade_count += result.trade_count
        trade_volume += result.trade_volume
        realized_pnl += result.realized_pnl
        unrealized_pnl += result.unrealized_pnl
        layer_total_n += layer["n"]

        equity_curve = result.equity_curve or []
        capital_curve = result.capital_usage_curve or []
        if len(equity_curve) != len(candles) or len(capital_curve) != len(candles):
            raise RuntimeError("Layer curve length mismatch")
        for i, value in enumerate(equity_curve):
            combined_equity[i] += value
        for i, value in enumerate(capital_curve):
            combined_capital[i] += value

    if total_buy_notional <= 0:
        raise ValueError("total layered notional must be > 0")

    total_return = net_profit / total_buy_notional
    backtest_days = (candles[-1].close_time - candles[0].open_time).total_seconds() / 86_400
    if backtest_days > 0 and total_return > -1.0:
        annualized = (1.0 + total_return) ** (365.0 / backtest_days) - 1.0
    else:
        annualized = -1.0 if total_return <= -1.0 else 0.0

    nav = [total_buy_notional + x for x in combined_equity]
    max_drawdown = _series_max_drawdown(nav)
    if max_drawdown > 0:
        calmar = annualized / max_drawdown
    else:
        calmar = math.inf if annualized > 0 else 0.0

    avg_capital_usage = mean(combined_capital) / total_buy_notional if combined_capital else 0.0
    max_capital_usage = max(combined_capital) / total_buy_notional if combined_capital else 0.0

    start_price = candles[0].open
    end_price = candles[-1].close
    period_low = min(x.low for x in candles)
    period_high = max(x.high for x in candles)
    period_amplitude = ((period_high - period_low) / period_low) if period_low > 0 else 0.0
    underlying_return = (end_price / start_price - 1.0) if start_price > 0 else 0.0

    layered = {
        "layer_count": len(params["layers"]),
        "layer_total_n": layer_total_n,
        "strategy_direction": params["strategy_direction"],
        "grid_level_mode": params["grid_level_mode"],
        "total_buy_notional": total_buy_notional,
        "net_profit": net_profit,
        "total_return": total_return,
        "annualized_return": annualized,
        "max_drawdown": max_drawdown,
        "calmar": _safe_metric(calmar),
        "trade_count": trade_count,
        "trade_volume": trade_volume,
        "total_fees": total_fees,
        "funding_pnl": funding_pnl,
        "funding_event_count": funding_event_count,
        "avg_capital_usage": avg_capital_usage,
        "max_capital_usage": max_capital_usage,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "start_time": candles[0].open_time.isoformat(),
        "end_time": candles[-1].close_time.isoformat(),
        "start_price": start_price,
        "end_price": end_price,
        "underlying_return": underlying_return,
        "period_low": period_low,
        "period_high": period_high,
        "period_amplitude": period_amplitude,
    }

    baseline = dict(params.get("baseline", {}))
    comparison: dict[str, Any] = {}
    compare_keys = [
        "net_profit",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "calmar",
        "trade_count",
        "trade_volume",
        "total_fees",
        "funding_pnl",
        "funding_event_count",
        "avg_capital_usage",
        "realized_pnl",
        "unrealized_pnl",
    ]
    for key in compare_keys:
        base = baseline.get(key)
        lay = layered.get(key)
        if base is None or lay is None:
            comparison[f"{key}_delta"] = None
            continue
        try:
            comparison[f"{key}_delta"] = float(lay) - float(base)
        except (TypeError, ValueError):
            comparison[f"{key}_delta"] = None

    return {
        "ok": True,
        "baseline": baseline,
        "layered": layered,
        "comparison": comparison,
        "data": {
            "candles": len(candles),
            "cache_file": cache_path,
            "funding_cache_file": funding_cache_path,
            "symbol": params["symbol"],
            "market_type": params["market_type"],
            "contract_type": params["contract_type"],
            "interval": params["interval"],
            "strategy_direction": params["strategy_direction"],
            "grid_level_mode": params["grid_level_mode"],
            "start_time": params["start_time"].isoformat(),
            "end_time": params["end_time"].isoformat(),
            "include_funding": params["include_funding"],
            "funding_events": len(funding_rates),
        },
    }


def _create_job(params: dict[str, Any], job_kind: str = "optimize") -> str:
    job_id = uuid.uuid4().hex
    now = time.time()
    with JOBS_LOCK:
        # Keep job store bounded.
        expired_ids = []
        for jid, job in JOBS.items():
            age = now - float(job.get("updated_at", now))
            if age > 6 * 3600 and job.get("status") in {"done", "failed"}:
                expired_ids.append(jid)
        for jid in expired_ids:
            JOBS.pop(jid, None)

        JOBS[job_id] = {
            "job_id": job_id,
            "job_kind": job_kind,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "progress": 0.0,
            "processed": 0,
            "total": 0,
            "eta_seconds": None,
            "message": "任务排队中",
            "params": params,
            "result": None,
            "error": None,
        }
    return job_id


def _update_job(job_id: str, **updates: Any) -> None:
    with JOBS_LOCK:
        current = JOBS.get(job_id)
        if not current:
            return
        current.update(updates)
        current["updated_at"] = time.time()


def _get_job_snapshot(job_id: str) -> dict[str, Any] | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        # shallow copy is enough for response rendering
        return dict(job)


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    calc_mode = str(payload.get("calc_mode", "optimize")).strip().lower()
    if calc_mode not in {"optimize", "fixed"}:
        raise ValueError("calc_mode must be optimize or fixed")
    market_type = normalize_market_type(payload.get("market_type", "futures"))
    contract_type = (
        normalize_contract_type(payload.get("contract_type", "usdm"))
        if market_type == "futures"
        else None
    )
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    if not symbol:
        raise ValueError("symbol is required")
    min_price = _safe_float(payload.get("min_price"), "min_price")
    max_price = _safe_float(payload.get("max_price"), "max_price")
    max_buy_raw = payload.get("max_buy_notional", payload.get("total_buy_notional", 0))
    total_buy_notional = _safe_float(max_buy_raw, "max_buy_notional")
    strategy_direction = str(payload.get("strategy_direction", "long")).strip().lower()
    grid_level_mode = str(payload.get("grid_level_mode", "arithmetic")).strip().lower()
    fixed_n = _safe_int(payload.get("fixed_n", 0), "fixed_n")
    fixed_buy_unit = str(payload.get("fixed_buy_unit", "notional")).strip().lower()
    fixed_per_grid_notional = _safe_float(
        payload.get("fixed_per_grid_notional", 0), "fixed_per_grid_notional"
    )
    fixed_per_grid_qty = _safe_float(payload.get("fixed_per_grid_qty", 0), "fixed_per_grid_qty")
    include_funding = _safe_bool(payload.get("include_funding", True), "include_funding")
    start_time = _safe_datetime(payload.get("start_time"), "start_time")
    end_time = _safe_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1h")).strip()
    n_min = _safe_int(payload.get("n_min", 5), "n_min")
    n_max = _safe_int(payload.get("n_max", 200), "n_max")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    funding_buffer = _safe_float(payload.get("funding_buffer", 0.0), "funding_buffer")
    target_trade_volume = _safe_float(
        payload.get("target_trade_volume", 0.0), "target_trade_volume"
    )
    top_k = _safe_int(payload.get("top_k", 5), "top_k")
    refresh = bool(payload.get("refresh", False))

    raw_modes = payload.get("allocation_modes", "equal,linear")
    if isinstance(raw_modes, str):
        allocation_modes = [x.strip().lower() for x in raw_modes.split(",") if x.strip()]
    elif isinstance(raw_modes, list):
        allocation_modes = [str(x).strip().lower() for x in raw_modes if str(x).strip()]
    else:
        raise ValueError("allocation_modes must be string or list")

    objective = str(payload.get("objective", "calmar")).strip().lower()
    if objective not in {
        "calmar",
        "net_profit",
        "total_return",
        "annualized_return",
        "gross_trade_notional",
        "competition_volume",
    }:
        raise ValueError("unsupported objective")
    min_trade_count = _safe_int(payload.get("min_trade_count", 0), "min_trade_count")
    min_avg_capital_usage = _safe_float(
        payload.get("min_avg_capital_usage", 0.0), "min_avg_capital_usage"
    )

    if min_price >= max_price:
        raise ValueError("min_price must be less than max_price")
    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    _enforce_seconds_interval_limit(start_time, end_time, interval)
    if fee_rate < 0 or slippage < 0 or funding_buffer < 0:
        raise ValueError("fee/slippage/funding_buffer must be >= 0")
    if min_trade_count < 0:
        raise ValueError("min_trade_count must be >= 0")
    if min_avg_capital_usage < 0 or min_avg_capital_usage > 1:
        raise ValueError("min_avg_capital_usage must be in [0,1]")
    if target_trade_volume < 0:
        raise ValueError("target_trade_volume must be >= 0")
    if strategy_direction not in set(supported_strategy_directions()):
        raise ValueError(
            f"unsupported strategy_direction: {strategy_direction}; "
            f"supported: {','.join(supported_strategy_directions())}"
        )
    if market_type == "spot":
        strategy_direction = "long"
        include_funding = False
    if grid_level_mode not in set(supported_grid_level_modes()):
        raise ValueError(
            f"unsupported grid_level_mode: {grid_level_mode}; "
            f"supported: {','.join(supported_grid_level_modes())}"
        )
    _validate_market_symbol(symbol=symbol, market_type=market_type, contract_type=contract_type)

    if calc_mode == "optimize":
        if fixed_buy_unit not in {"notional", "qty"}:
            raise ValueError("fixed_buy_unit must be notional or qty")
        if total_buy_notional <= 0:
            raise ValueError("max_buy_notional must be > 0")
        if n_min <= 0 or n_max <= 0 or n_min > n_max:
            raise ValueError("invalid n range")
        if not allocation_modes:
            raise ValueError("allocation_modes cannot be empty")
        supported = set(supported_allocation_modes())
        unknown = [x for x in allocation_modes if x not in supported]
        if unknown:
            raise ValueError(
                f"unsupported allocation_modes: {','.join(unknown)}; "
                f"supported: {','.join(sorted(supported))}"
            )
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
    else:
        if fixed_n <= 0:
            raise ValueError("fixed_n must be > 0")
        if fixed_buy_unit not in {"notional", "qty"}:
            raise ValueError("fixed_buy_unit must be notional or qty")
        if fixed_buy_unit == "notional":
            if fixed_per_grid_notional <= 0:
                raise ValueError("fixed_per_grid_notional must be > 0")
            total_buy_notional = fixed_n * fixed_per_grid_notional
            fixed_per_grid_qty = 0.0
            allocation_modes = ["equal"]
        else:
            if fixed_per_grid_qty <= 0:
                raise ValueError("fixed_per_grid_qty must be > 0")
            total_buy_notional = 0.0
            levels = build_grid_levels(
                min_price=min_price,
                max_price=max_price,
                n=fixed_n,
                grid_level_mode=grid_level_mode,
            )
            for i in range(fixed_n):
                total_buy_notional += levels[i] * fixed_per_grid_qty
            fixed_per_grid_notional = 0.0
            allocation_modes = ["equal_qty"]
        n_min = fixed_n
        n_max = fixed_n
        if top_k <= 0:
            top_k = 1

    return {
        "calc_mode": calc_mode,
        "market_type": market_type,
        "contract_type": contract_type,
        "symbol": symbol,
        "min_price": min_price,
        "max_price": max_price,
        "total_buy_notional": total_buy_notional,
        "strategy_direction": strategy_direction,
        "grid_level_mode": grid_level_mode,
        "fixed_n": fixed_n,
        "fixed_buy_unit": fixed_buy_unit,
        "fixed_per_grid_notional": fixed_per_grid_notional,
        "fixed_per_grid_qty": fixed_per_grid_qty,
        "include_funding": include_funding,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "n_min": n_min,
        "n_max": n_max,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "funding_buffer": funding_buffer,
        "target_trade_volume": target_trade_volume,
        "allocation_modes": allocation_modes,
        "objective": objective,
        "min_trade_count": min_trade_count,
        "min_avg_capital_usage": min_avg_capital_usage,
        "top_k": top_k,
        "refresh": refresh,
    }


def _normalize_suggest_payload(payload: dict[str, Any]) -> dict[str, Any]:
    params = _normalize_payload(payload)
    if params["calc_mode"] != "optimize":
        raise ValueError("suggest_range only supports optimize mode")
    params["allocation_modes"] = _ordered_suggestion_modes(params["allocation_modes"])
    params["top_k"] = min(10, max(1, params["top_k"]))
    return params


def _normalize_short_volume_candidates_payload(payload: dict[str, Any]) -> dict[str, Any]:
    market_type = normalize_market_type(payload.get("market_type", "futures"))
    if market_type != "futures":
        raise ValueError("short_volume_candidates only supports futures")
    contract_type = normalize_contract_type(payload.get("contract_type", "usdm"))
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    if not symbol:
        raise ValueError("symbol is required")
    start_time = _safe_datetime(payload.get("start_time"), "start_time")
    end_time = _safe_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1m")).strip()
    total_buy_notional = _safe_float(
        payload.get("total_buy_notional", payload.get("max_buy_notional", 0)),
        "total_buy_notional",
    )
    include_funding = _safe_bool(payload.get("include_funding", True), "include_funding")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    refresh = bool(payload.get("refresh", False))

    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    _enforce_seconds_interval_limit(start_time, end_time, interval)
    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")
    if fee_rate < 0 or slippage < 0:
        raise ValueError("fee_rate/slippage must be >= 0")
    _validate_market_symbol(symbol=symbol, market_type=market_type, contract_type=contract_type)

    return {
        "market_type": market_type,
        "contract_type": contract_type,
        "symbol": symbol,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "total_buy_notional": total_buy_notional,
        "include_funding": include_funding,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "refresh": refresh,
    }


def _run_short_volume_candidates(
    params: dict[str, Any],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    cache_path = str(
        cache_file_path(
            params["symbol"],
            params["interval"],
            cache_dir="data",
            contract_type=params["contract_type"],
            market_type=params["market_type"],
        )
    )
    funding_cache_path = str(
        funding_cache_file_path(
            params["symbol"],
            cache_dir="data",
            contract_type=params["contract_type"],
            market_type=params["market_type"],
        )
    )
    report = build_short_volume_candidate_report(
        symbol=params["symbol"],
        start_time=params["start_time"],
        end_time=params["end_time"],
        interval=params["interval"],
        total_buy_notional=params["total_buy_notional"],
        contract_type=params["contract_type"],
        cache_dir="data",
        include_funding=params["include_funding"],
        fee_rate=params["fee_rate"],
        slippage=params["slippage"],
        refresh=params["refresh"],
        progress_callback=progress_callback,
    )
    report["ok"] = True
    report["data"] = {
        "market_type": params["market_type"],
        "contract_type": params["contract_type"],
        "symbol": params["symbol"],
        "interval": params["interval"],
        "start_time": params["start_time"].isoformat(),
        "end_time": params["end_time"].isoformat(),
        "total_buy_notional": params["total_buy_notional"],
        "include_funding": params["include_funding"],
        "cache_file": cache_path,
        "funding_cache_file": funding_cache_path if params["include_funding"] else None,
    }
    return report


def _run_range_suggestion(params: dict[str, Any]) -> dict[str, Any]:
    cache_path = str(
        cache_file_path(
            params["symbol"],
            params["interval"],
            cache_dir="data",
            contract_type=params.get("contract_type") or "usdm",
            market_type=params["market_type"],
        )
    )
    funding_cache_path = (
        str(
            funding_cache_file_path(
                params["symbol"],
                cache_dir="data",
                contract_type=params.get("contract_type") or "usdm",
                market_type=params["market_type"],
            )
        )
        if params["include_funding"] and params["market_type"] == "futures"
        else None
    )
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        start_time=params["start_time"],
        end_time=params["end_time"],
        cache_dir="data",
        contract_type=params.get("contract_type") or "usdm",
        market_type=params["market_type"],
        refresh=params["refresh"],
    )
    funding_rates = (
        load_or_fetch_funding_rates(
            symbol=params["symbol"],
            start_time=params["start_time"],
            end_time=params["end_time"],
            cache_dir="data",
            contract_type=params.get("contract_type") or "usdm",
            market_type=params["market_type"],
            refresh=params["refresh"],
        )
        if params["include_funding"] and params["market_type"] == "futures"
        else []
    )
    if len(candles) < 10:
        raise ValueError("Not enough candle data for range suggestion")

    range_candidates = _build_candidate_ranges(candles)
    if not range_candidates:
        raise ValueError("Unable to build candidate ranges from current data")
    n_values = _sample_n_values(params["n_min"], params["n_max"], max_count=16)

    suggestions: list[dict[str, Any]] = []
    for min_price, max_price, source in range_candidates:
        optimization = optimize_grid_count(
            candles=candles,
            min_price=min_price,
            max_price=max_price,
            total_buy_notional=params["total_buy_notional"],
            n_min=n_values[0],
            n_max=n_values[-1],
            n_values=n_values,
            grid_level_mode=params["grid_level_mode"],
            strategy_direction=params["strategy_direction"],
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            funding_buffer=params["funding_buffer"],
            allocation_modes=params["allocation_modes"],
            objective="competition_volume",
            target_trade_volume=params["target_trade_volume"],
            min_trade_count=params["min_trade_count"],
            min_avg_capital_usage=params["min_avg_capital_usage"],
            top_k=max(10, len(params["allocation_modes"])),
            funding_rates=funding_rates,
            bootstrap_positions=params["market_type"] != "spot",
        )
        if not optimization.top_results:
            continue

        ranked_candidates = sorted(
            optimization.top_results,
            key=lambda item: (
                _competition_sort_key(
                    trade_volume=float(item.gross_trade_notional),
                    target_trade_volume=float(params["target_trade_volume"]),
                    net_profit=float(item.net_profit),
                    max_drawdown=float(item.max_drawdown),
                ),
                _mode_preference_score(item.allocation_mode),
            ),
            reverse=True,
        )
        best = ranked_candidates[0]
        gross_trade_notional = float(best.gross_trade_notional)
        target_trade_volume = float(params["target_trade_volume"])
        volume_coverage = (
            gross_trade_notional / target_trade_volume if target_trade_volume > 0 else 1.0
        )
        if params["grid_level_mode"] == "geometric" and min_price > 0 and best.n > 0:
            step_pct = (max_price / min_price) ** (1.0 / best.n) - 1.0
        else:
            mid_price = (min_price + max_price) / 2.0
            step_pct = ((max_price - min_price) / best.n) / mid_price if mid_price > 0 and best.n > 0 else 0.0
        min_grid_notional = min(best.per_grid_notionals) if best.per_grid_notionals else 0.0
        max_grid_notional = max(best.per_grid_notionals) if best.per_grid_notionals else 0.0
        avg_grid_notional = (
            sum(best.per_grid_notionals) / len(best.per_grid_notionals)
            if best.per_grid_notionals
            else 0.0
        )
        suggestions.append(
            {
                "min_price": min_price,
                "max_price": max_price,
                "range_width_pct": ((max_price - min_price) / ((max_price + min_price) / 2.0))
                if (max_price + min_price) > 0
                else 0.0,
                "recommended_n": best.n,
                "recommended_mode": best.allocation_mode,
                "step_pct": step_pct,
                "avg_per_grid_notional": avg_grid_notional,
                "min_per_grid_notional": min_grid_notional,
                "max_per_grid_notional": max_grid_notional,
                "gross_trade_notional": gross_trade_notional,
                "trade_volume": gross_trade_notional,
                "target_trade_volume": target_trade_volume,
                "volume_coverage": volume_coverage,
                "net_profit": best.net_profit,
                "max_drawdown": best.max_drawdown,
                "trade_count": best.trade_count,
                "total_fees": best.total_fees,
                "source": source,
                "reason": _range_reason(
                    coverage=volume_coverage,
                    net_profit=best.net_profit,
                    mode=best.allocation_mode,
                ),
            }
        )

    suggestions = sorted(
        suggestions,
        key=lambda item: _competition_sort_key(
            trade_volume=float(item["gross_trade_notional"]),
            target_trade_volume=float(params["target_trade_volume"]),
            net_profit=float(item["net_profit"]),
            max_drawdown=float(item["max_drawdown"]),
        ),
        reverse=True,
    )[: params["top_k"]]

    return {
        "ok": True,
        "suggestions": suggestions,
        "data": {
            "candles": len(candles),
            "cache_file": cache_path,
            "funding_cache_file": funding_cache_path,
            "funding_events": len(funding_rates),
            "symbol": params["symbol"],
            "market_type": params["market_type"],
            "contract_type": params["contract_type"],
            "interval": params["interval"],
            "strategy_direction": params["strategy_direction"],
            "grid_level_mode": params["grid_level_mode"],
            "start_time": params["start_time"].isoformat(),
            "end_time": params["end_time"].isoformat(),
            "include_funding": params["include_funding"],
            "target_trade_volume": params["target_trade_volume"],
        },
    }


def _run_optimizer(
    params: dict[str, Any],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    cache_path = str(
        cache_file_path(
            params["symbol"],
            params["interval"],
            cache_dir="data",
            contract_type=params.get("contract_type") or "usdm",
            market_type=params["market_type"],
        )
    )
    funding_cache_path = (
        str(
            funding_cache_file_path(
                params["symbol"],
                cache_dir="data",
                contract_type=params.get("contract_type") or "usdm",
                market_type=params["market_type"],
            )
        )
        if params["include_funding"] and params["market_type"] == "futures"
        else None
    )
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        start_time=params["start_time"],
        end_time=params["end_time"],
        cache_dir="data",
        contract_type=params.get("contract_type") or "usdm",
        market_type=params["market_type"],
        refresh=params["refresh"],
    )
    funding_rates = (
        load_or_fetch_funding_rates(
            symbol=params["symbol"],
            start_time=params["start_time"],
            end_time=params["end_time"],
            cache_dir="data",
            contract_type=params.get("contract_type") or "usdm",
            market_type=params["market_type"],
            refresh=params["refresh"],
        )
        if params["include_funding"] and params["market_type"] == "futures"
        else []
    )
    expected_candles = _expected_candle_count(
        params["start_time"], params["end_time"], params["interval"]
    )
    candle_coverage = (len(candles) / expected_candles) if expected_candles > 0 else 1.0

    def _candidate_payload(item) -> dict[str, Any]:
        target_trade_volume = float(params["target_trade_volume"])
        gross_trade_notional = float(item.gross_trade_notional)
        volume_coverage = gross_trade_notional / target_trade_volume if target_trade_volume > 0 else 1.0
        plan_rows = []
        for idx in range(item.n):
            plan_rows.append(
                {
                    "idx": idx + 1,
                    "grid_side": item.grid_sides[idx],
                    "buy_price": item.grid_levels[idx],
                    "sell_price": item.grid_levels[idx + 1],
                    "buy_notional": item.per_grid_notionals[idx],
                    "qty": item.per_grid_qty[idx],
                }
            )
        return {
            "market_type": params["market_type"],
            "n": item.n,
            "allocation_mode": item.allocation_mode,
            "grid_level_mode": params["grid_level_mode"],
            "strategy_direction": item.strategy_direction,
            "neutral_anchor_price": item.neutral_anchor_price,
            "score": _safe_metric(item.score),
            "net_profit": item.net_profit,
            "total_return": item.total_return,
            "annualized_return": item.annualized_return,
            "max_drawdown": item.max_drawdown,
            "calmar": _safe_metric(item.calmar),
            "total_fees": item.total_fees,
            "funding_pnl": item.funding_pnl,
            "funding_event_count": item.funding_event_count,
            "trade_count": item.trade_count,
            "gross_trade_notional": gross_trade_notional,
            "trade_volume": gross_trade_notional,
            "target_trade_volume": target_trade_volume,
            "volume_coverage": volume_coverage,
            "win_rate": item.win_rate,
            "avg_capital_usage": item.avg_capital_usage,
            "max_capital_usage": item.max_capital_usage,
            "total_buy_notional": item.capital_base_notional,
            "entry_notional_sum": sum(item.per_grid_notionals),
            "realized_pnl": item.realized_pnl,
            "unrealized_pnl": item.unrealized_pnl,
            "final_position_qty": item.final_position_qty,
            "final_position_notional": item.final_position_notional,
            "start_time": item.start_time.isoformat(),
            "end_time": item.end_time.isoformat(),
            "start_price": item.start_price,
            "end_price": item.end_price,
            "underlying_return": item.underlying_return,
            "period_low": item.period_low,
            "period_high": item.period_high,
            "period_amplitude": item.period_amplitude,
            "plan": plan_rows,
        }

    if params["calc_mode"] == "fixed":
        fixed_mode = "equal_qty" if params["fixed_buy_unit"] == "qty" else "equal"
        if progress_callback:
            progress_callback({"processed": 0, "total": 1, "status": "started", "n": params["fixed_n"], "mode": fixed_mode})
        fixed_result = run_backtest(
            candles=candles,
            min_price=params["min_price"],
            max_price=params["max_price"],
            n=params["fixed_n"],
            total_buy_notional=params["total_buy_notional"],
            grid_level_mode=params["grid_level_mode"],
            allocation_mode=fixed_mode,
            strategy_direction=params["strategy_direction"],
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            funding_rates=funding_rates,
            fixed_per_grid_qty=(
                params["fixed_per_grid_qty"] if params["fixed_buy_unit"] == "qty" else None
            ),
            bootstrap_positions=params["market_type"] != "spot",
            capture_trades=False,
        )
        if progress_callback:
            progress_callback(
                {"processed": 1, "total": 1, "status": "tested", "n": params["fixed_n"], "mode": fixed_mode}
            )
        fixed_result.score = objective_value(
            fixed_result,
            params["objective"],
            target_trade_volume=params["target_trade_volume"],
        )

        if fixed_result.trade_count < params["min_trade_count"]:
            return {
                "ok": True,
                "best": None,
                "top": [],
                "plan": [],
                "data": {
                    "candles": len(candles),
                    "expected_candles": expected_candles,
                    "candle_coverage": candle_coverage,
                    "cache_file": cache_path,
                    "funding_cache_file": funding_cache_path,
                    "funding_events": len(funding_rates),
                    "include_funding": params["include_funding"],
                    "market_type": params["market_type"],
                    "contract_type": params["contract_type"],
                    "symbol": params["symbol"],
                    "strategy_direction": params["strategy_direction"],
                    "grid_level_mode": params["grid_level_mode"],
                },
                "search": {
                    "mode": "fixed",
                    "tested": 1,
                    "skipped_by_cost": 0,
                    "objective": params["objective"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_trade_count": params["min_trade_count"],
                    "min_avg_capital_usage": params["min_avg_capital_usage"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_step_ratio_for_cost": min_step_ratio_for_cost(
                        fee_rate=params["fee_rate"],
                        slippage=params["slippage"],
                        funding_buffer=params["funding_buffer"],
                    ),
                },
                "error": "Fixed result filtered by min_trade_count",
            }

        if fixed_result.avg_capital_usage < params["min_avg_capital_usage"]:
            return {
                "ok": True,
                "best": None,
                "top": [],
                "plan": [],
                "data": {
                    "candles": len(candles),
                    "expected_candles": expected_candles,
                    "candle_coverage": candle_coverage,
                    "cache_file": cache_path,
                    "funding_cache_file": funding_cache_path,
                    "funding_events": len(funding_rates),
                    "include_funding": params["include_funding"],
                    "market_type": params["market_type"],
                    "contract_type": params["contract_type"],
                    "symbol": params["symbol"],
                    "strategy_direction": params["strategy_direction"],
                    "grid_level_mode": params["grid_level_mode"],
                },
                "search": {
                    "mode": "fixed",
                    "tested": 1,
                    "skipped_by_cost": 0,
                    "objective": params["objective"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_trade_count": params["min_trade_count"],
                    "min_avg_capital_usage": params["min_avg_capital_usage"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_step_ratio_for_cost": min_step_ratio_for_cost(
                        fee_rate=params["fee_rate"],
                        slippage=params["slippage"],
                        funding_buffer=params["funding_buffer"],
                    ),
                },
                "error": "Fixed result filtered by min_avg_capital_usage",
            }

        best = _candidate_payload(fixed_result)
        top = [best]
        tested = 1
        skipped_by_cost = 0
    else:
        optimization = optimize_grid_count(
            candles=candles,
            min_price=params["min_price"],
            max_price=params["max_price"],
            total_buy_notional=params["total_buy_notional"],
            n_min=params["n_min"],
            n_max=params["n_max"],
            grid_level_mode=params["grid_level_mode"],
            strategy_direction=params["strategy_direction"],
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            funding_buffer=params["funding_buffer"],
            allocation_modes=params["allocation_modes"],
            objective=params["objective"],
            target_trade_volume=params["target_trade_volume"],
            min_trade_count=params["min_trade_count"],
            min_avg_capital_usage=params["min_avg_capital_usage"],
            top_k=params["top_k"],
            funding_rates=funding_rates,
            bootstrap_positions=params["market_type"] != "spot",
            progress_callback=progress_callback,
        )

        if optimization.best is None:
            return {
                "ok": True,
                "best": None,
                "top": [],
                "plan": [],
                "data": {
                    "candles": len(candles),
                    "expected_candles": expected_candles,
                    "candle_coverage": candle_coverage,
                    "cache_file": cache_path,
                    "funding_cache_file": funding_cache_path,
                    "funding_events": len(funding_rates),
                    "include_funding": params["include_funding"],
                    "market_type": params["market_type"],
                    "contract_type": params["contract_type"],
                    "symbol": params["symbol"],
                    "strategy_direction": params["strategy_direction"],
                    "grid_level_mode": params["grid_level_mode"],
                },
                "search": {
                    "mode": "optimize",
                    "tested": optimization.tested,
                    "skipped_by_cost": optimization.skipped_by_cost,
                    "objective": params["objective"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_trade_count": params["min_trade_count"],
                    "min_avg_capital_usage": params["min_avg_capital_usage"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_step_ratio_for_cost": min_step_ratio_for_cost(
                        fee_rate=params["fee_rate"],
                        slippage=params["slippage"],
                        funding_buffer=params["funding_buffer"],
                    ),
                },
            }

        best = _candidate_payload(optimization.best)
        top = [_candidate_payload(x) for x in optimization.top_results]
        tested = optimization.tested
        skipped_by_cost = optimization.skipped_by_cost

    return {
        "ok": True,
        "best": best,
        "top": top,
        "plan": best["plan"],
        "data": {
            "candles": len(candles),
            "expected_candles": expected_candles,
            "candle_coverage": candle_coverage,
            "cache_file": cache_path,
            "funding_cache_file": funding_cache_path,
            "funding_events": len(funding_rates),
            "include_funding": params["include_funding"],
            "market_type": params["market_type"],
            "contract_type": params["contract_type"],
            "symbol": params["symbol"],
            "strategy_direction": params["strategy_direction"],
            "grid_level_mode": params["grid_level_mode"],
            "calc_mode": params["calc_mode"],
            "total_buy_notional": params["total_buy_notional"],
            "target_trade_volume": params["target_trade_volume"],
            "start_time": params["start_time"].isoformat(),
            "end_time": params["end_time"].isoformat(),
        },
        "search": {
            "mode": params["calc_mode"],
            "tested": tested,
            "skipped_by_cost": skipped_by_cost,
            "objective": params["objective"],
            "target_trade_volume": params["target_trade_volume"],
            "min_trade_count": params["min_trade_count"],
            "min_avg_capital_usage": params["min_avg_capital_usage"],
            "target_trade_volume": params["target_trade_volume"],
            "min_step_ratio_for_cost": min_step_ratio_for_cost(
                fee_rate=params["fee_rate"],
                slippage=params["slippage"],
                funding_buffer=params["funding_buffer"],
            ),
        },
    }


def _run_job_worker(job_id: str) -> None:
    snapshot = _get_job_snapshot(job_id)
    if not snapshot:
        return
    params = snapshot["params"]
    job_kind = str(snapshot.get("job_kind", "optimize")).strip().lower()
    start_ts = time.time()
    backtest_start_ts = start_ts

    def _progress(event: dict[str, Any]) -> None:
        nonlocal backtest_start_ts
        processed = int(event.get("processed", 0))
        total = int(event.get("total", 0))
        mode = event.get("mode")
        n = event.get("n")
        status = event.get("status", "")

        if status == "started":
            backtest_start_ts = time.time()

        ratio = 0.0
        if total > 0:
            ratio = min(1.0, max(0.0, processed / total))

        elapsed = max(0.001, time.time() - backtest_start_ts)
        eta_seconds: int | None = None
        if ratio > 0.0001 and ratio < 1.0:
            eta_seconds = int(elapsed * (1.0 - ratio) / ratio)

        progress_value = 0.15 + 0.8 * ratio
        message = str(event.get("message", "")).strip()
        if not message:
            if job_kind == "short_volume_candidates":
                message = f"生成候选中 {processed}/{total}"
                if n is not None:
                    message += f" (N={n}"
                    if mode:
                        message += f", mode={mode}"
                    message += ")"
            else:
                message = f"回测中 {processed}/{total}"
                if n is not None:
                    message += f" (N={n}"
                    if mode:
                        message += f", mode={mode}"
                    message += ")"

        _update_job(
            job_id,
            status="running",
            progress=progress_value,
            processed=processed,
            total=total,
            eta_seconds=eta_seconds,
            message=message,
        )

    try:
        if job_kind == "short_volume_candidates":
            load_message = "加载K线并生成空头刷量候选..."
        else:
            load_message = "加载K线和资金费率数据..." if params.get("include_funding") else "加载K线数据..."
        _update_job(job_id, status="running", progress=0.05, message=load_message)
        if job_kind == "short_volume_candidates":
            result = _run_short_volume_candidates(params, progress_callback=_progress)
        else:
            result = _run_optimizer(params, progress_callback=_progress)
        search_info = result.get("search", {})
        processed = int(search_info.get("tested", 0) or 0)
        total = int(search_info.get("total", processed) or processed)
        total_elapsed = int(time.time() - start_ts)
        _update_job(
            job_id,
            status="done",
            progress=1.0,
            processed=processed,
            total=total,
            eta_seconds=0,
            message=(
                f"候选生成完成，用时 {total_elapsed}s"
                if job_kind == "short_volume_candidates"
                else f"测算完成，用时 {total_elapsed}s"
            ),
            result=result,
            error=None,
        )
    except Exception as exc:  # pragma: no cover
        _update_job(
            job_id,
            status="failed",
            progress=1.0,
            eta_seconds=None,
            message="测算失败",
            error=f"{type(exc).__name__}: {exc}",
        )


def _safe_positive_int_query(value: Any, default: int) -> int:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


def _run_loop_monitor_query(query: dict[str, list[str]]) -> dict[str, Any]:
    symbol = str(query.get("symbol", ["NIGHTUSDT"])[0]).upper().strip() or "NIGHTUSDT"
    summary_limit = min(_safe_positive_int_query(query.get("summary_limit", ["500"])[0], 500), 2000)
    runner = read_symbol_runner_process(symbol)
    if isinstance(runner, dict):
        runner = dict(runner)
        runner_config = runner.get("config", {})
        if isinstance(runner_config, dict):
            normalized_runner_config = dict(runner_config)
            config_symbol = str(normalized_runner_config.get("symbol", "")).upper().strip()
            if not config_symbol or config_symbol == symbol:
                normalized_runner_config["symbol"] = symbol
                runner["config"] = _normalize_runner_runtime_paths(normalized_runner_config, symbol)
    runner_config = runner.get("config", {}) if isinstance(runner, dict) else {}
    if str(runner_config.get("symbol", "")).upper().strip() == symbol:
        default_paths = {
            "events_path": str(runner_config.get("summary_jsonl", "")).strip(),
            "plan_path": str(runner_config.get("plan_json", "")).strip(),
            "submit_report_path": str(runner_config.get("submit_report_json", "")).strip(),
        }
    else:
        runtime_paths = _default_runtime_paths_for_symbol(symbol)
        default_paths = {
            "events_path": runtime_paths["summary_jsonl"],
            "plan_path": runtime_paths["plan_json"],
            "submit_report_path": runtime_paths["submit_report_json"],
        }
    events_path = str(query.get("events_path", [default_paths["events_path"]])[0]).strip() or default_paths["events_path"]
    plan_path = str(query.get("plan_path", [default_paths["plan_path"]])[0]).strip() or default_paths["plan_path"]
    submit_report_path = (
        str(query.get("submit_report_path", [default_paths["submit_report_path"]])[0]).strip()
        or default_paths["submit_report_path"]
    )
    snapshot = build_monitor_snapshot(
        symbol=symbol,
        events_path=events_path,
        plan_path=plan_path,
        submit_report_path=submit_report_path,
        summary_limit=summary_limit,
        runner_process=runner,
    )
    snapshot["runner_presets"] = _runner_preset_summaries(symbol)
    return snapshot


class _Handler(BaseHTTPRequestHandler):
    server_version = "grid-web/0.1"

    def _request_path(self) -> str:
        return urlparse(self.path).path

    def _request_client_ip(self) -> str:
        forwarded = str(self.headers.get("X-Forwarded-For", "")).strip()
        if forwarded and os.environ.get("GRID_WEB_TRUST_PROXY", "").strip().lower() in {"1", "true", "yes"}:
            return forwarded.split(",", 1)[0].strip()
        return str(self.client_address[0]).strip()

    def _send_common_headers(self, content_type: str, content_length: int) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(content_length))
        for key, value in _security_headers().items():
            self.send_header(key, value)

    def _send_text(self, body: str, status: int, extra_headers: dict[str, str] | None = None) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self._send_common_headers("text/plain; charset=utf-8", len(data))
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def _send_unauthorized(self) -> None:
        self._send_text(
            "Unauthorized",
            status=HTTPStatus.UNAUTHORIZED,
            extra_headers={"WWW-Authenticate": f'Basic realm="{WEB_AUTH_REALM}", charset="UTF-8"'},
        )

    def _send_forbidden(self) -> None:
        self._send_text("Forbidden", status=HTTPStatus.FORBIDDEN)

    def _authorize_request(self) -> bool:
        if self._request_path() in {"/api/health", "/hub", "/hub.html", "/portal", "/portal.html"}:
            return True

        allowed_networks = _parse_allowed_networks(os.environ.get("GRID_WEB_ALLOWED_CIDRS"))
        if not _client_ip_allowed(self._request_client_ip(), allowed_networks):
            self._send_forbidden()
            return False

        credentials = _load_web_auth_credentials()
        if credentials is None:
            return True
        username, password = credentials
        if _basic_auth_header_matches(self.headers.get("Authorization"), username, password):
            return True
        self._send_unauthorized()
        return False

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._send_common_headers("application/json; charset=utf-8", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str, status: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self._send_common_headers("text/html; charset=utf-8", len(data))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if not self._authorize_request():
            return
        if path in {"/hub", "/hub.html", "/portal", "/portal.html"}:
            self._send_html(SERVER_HUB_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/", "/index.html"}:
            self._send_html(HTML_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/rankings", "/rankings.html"}:
            self._send_html(RANKING_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/competition_board", "/competition_board.html"}:
            self._send_html(COMPETITION_BOARD_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/basis", "/basis.html"}:
            self._send_html(BASIS_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/monitor", "/monitor.html"}:
            self._send_html(MONITOR_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/spot_runner", "/spot_runner.html"}:
            self._send_html(SPOT_RUNNER_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/spot_strategies", "/spot_strategies.html"}:
            self._send_html(SPOT_STRATEGIES_PAGE, status=HTTPStatus.OK)
            return
        if path in {"/strategies", "/strategies.html"}:
            self._send_html(STRATEGIES_PAGE, status=HTTPStatus.OK)
            return
        if path == "/api/health":
            self._send_json({"ok": True}, status=HTTPStatus.OK)
            return
        if path == "/api/competition_board":
            refresh = str(query.get("refresh", ["0"])[0]).strip() == "1"
            try:
                snapshot = build_competition_board_snapshot(refresh=refresh)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json({"ok": True, "snapshot": snapshot}, status=HTTPStatus.OK)
            return
        if path == "/api/spot_runner/status":
            symbol = str(query.get("symbol", [SPOT_RUNNER_DEFAULT_CONFIG["symbol"]])[0]).upper().strip()
            try:
                snapshot = _build_spot_runner_snapshot(symbol or None)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json({"ok": True, "snapshot": snapshot}, status=HTTPStatus.OK)
            return
        if path.startswith("/api/loop_monitor"):
            try:
                payload = _run_loop_monitor_query(query)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(payload, status=HTTPStatus.OK)
            return
        if path == "/api/symbol_lists":
            list_type_raw = str(query.get("list_type", [""])[0]).strip()
            try:
                if list_type_raw:
                    list_type = normalize_symbol_list_type(list_type_raw)
                    self._send_json(
                        {
                            "ok": True,
                            "list_type": list_type,
                            "symbols": get_symbol_list(list_type),
                        },
                        status=HTTPStatus.OK,
                    )
                else:
                    self._send_json({"ok": True, "lists": load_symbol_lists()}, status=HTTPStatus.OK)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=400)
            return
        if path.startswith("/api/symbols"):
            refresh = str(query.get("refresh", ["0"])[0]).strip() == "1"
            market_type_raw = str(query.get("market_type", ["futures"])[0]).strip()
            contract_type_raw = str(query.get("contract_type", ["usdm"])[0]).strip()
            try:
                market_type = normalize_market_type(market_type_raw)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            if market_type == "futures":
                try:
                    contract_type = normalize_contract_type(contract_type_raw)
                except ValueError as exc:
                    self._send_json({"ok": False, "error": str(exc)}, status=400)
                    return
            else:
                contract_type = None
            try:
                symbols = _load_market_symbols(
                    market_type=market_type,
                    contract_type=contract_type,
                    refresh=refresh,
                )
                self._send_json(
                    {
                        "ok": True,
                        "market_type": market_type,
                        "contract_type": contract_type,
                        "symbols": symbols,
                        "source": "binance_or_cache",
                    },
                    status=HTTPStatus.OK,
                )
            except Exception as exc:
                if market_type == "spot":
                    fallback_symbols = ["BTCUSDT", "ETHUSDT"]
                else:
                    fallback_symbols = (
                        ["BTCUSD_PERP", "ETHUSD_PERP"]
                        if contract_type == "coinm"
                        else ["BTCUSDT", "ETHUSDT"]
                    )
                self._send_json(
                    {
                        "ok": True,
                        "market_type": market_type,
                        "contract_type": contract_type,
                        "symbols": fallback_symbols,
                        "source": "fallback",
                        "warning": f"{type(exc).__name__}: {exc}",
                    },
                    status=HTTPStatus.OK,
                )
            return
        if path == "/api/price":
            symbol = str(query.get("symbol", [""])[0]).upper().strip()
            market_type_raw = str(query.get("market_type", ["futures"])[0]).strip()
            contract_type_raw = str(query.get("contract_type", ["usdm"])[0]).strip()
            try:
                market_type = normalize_market_type(market_type_raw)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            if market_type == "futures":
                try:
                    contract_type = normalize_contract_type(contract_type_raw)
                except ValueError as exc:
                    self._send_json({"ok": False, "error": str(exc)}, status=400)
                    return
            else:
                contract_type = None
            if not symbol:
                if market_type == "spot":
                    symbol = "BTCUSDT"
                else:
                    symbol = "BTCUSD_PERP" if contract_type == "coinm" else "BTCUSDT"
            if not symbol:
                self._send_json({"ok": False, "error": "symbol is required"}, status=400)
                return
            try:
                if market_type == "spot":
                    price = fetch_spot_latest_price(symbol)
                else:
                    price = fetch_futures_latest_price(symbol, contract_type=contract_type)
                self._send_json(
                    {
                        "ok": True,
                        "market_type": market_type,
                        "contract_type": contract_type,
                        "symbol": symbol,
                        "price": price,
                        "source": "binance_ticker",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    },
                    status=HTTPStatus.OK,
                )
            except Exception as exc:
                fallback = read_latest_cached_close(
                    symbol=symbol,
                    cache_dir="data",
                    contract_type=contract_type or "usdm",
                    market_type=market_type,
                )
                if fallback is not None:
                    self._send_json(
                        {
                            "ok": True,
                            "market_type": market_type,
                            "contract_type": contract_type,
                            "symbol": symbol,
                            "price": fallback,
                            "source": "cache_fallback",
                            "warning": f"{type(exc).__name__}: {exc}",
                            "ts": datetime.now(timezone.utc).isoformat(),
                        },
                        status=HTTPStatus.OK,
                    )
                else:
                    self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=502)
            return
        if path.startswith("/api/job/"):
            job_id = path.split("/api/job/", 1)[1].strip()
            if not job_id:
                self._send_json({"ok": False, "error": "Missing job id"}, status=400)
                return
            snapshot = _get_job_snapshot(job_id)
            if not snapshot:
                self._send_json({"ok": False, "error": "Job not found"}, status=404)
                return
            payload = {
                "ok": True,
                "job_id": snapshot["job_id"],
                "status": snapshot["status"],
                "progress": snapshot.get("progress", 0.0),
                "processed": snapshot.get("processed", 0),
                "total": snapshot.get("total", 0),
                "eta_seconds": snapshot.get("eta_seconds"),
                "message": snapshot.get("message"),
                "error": snapshot.get("error"),
                "result": snapshot.get("result") if snapshot.get("status") == "done" else None,
            }
            self._send_json(payload, status=HTTPStatus.OK)
            return
        self._send_json({"ok": False, "error": "Not Found"}, status=HTTPStatus.NOT_FOUND)

    def do_HEAD(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if not self._authorize_request():
            return
        if path in {"/hub", "/hub.html", "/portal", "/portal.html"}:
            body = SERVER_HUB_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/", "/index.html"}:
            body = HTML_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/rankings", "/rankings.html"}:
            body = RANKING_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/competition_board", "/competition_board.html"}:
            body = COMPETITION_BOARD_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/basis", "/basis.html"}:
            body = BASIS_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/monitor", "/monitor.html"}:
            body = MONITOR_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/spot_runner", "/spot_runner.html"}:
            body = SPOT_RUNNER_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/spot_strategies", "/spot_strategies.html"}:
            body = SPOT_STRATEGIES_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path in {"/strategies", "/strategies.html"}:
            body = STRATEGIES_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("text/html; charset=utf-8", len(body))
            self.end_headers()
            return
        if path == "/api/health":
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("application/json; charset=utf-8", len(body))
            self.end_headers()
            return
        if (
            path == "/api/symbol_lists"
            or path.startswith("/api/symbols")
            or path == "/api/price"
            or path.startswith("/api/loop_monitor")
            or path == "/api/grid_preview"
            or path == "/api/competition_board"
            or path == "/api/spot_runner/status"
        ):
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._send_common_headers("application/json; charset=utf-8", len(body))
            self.end_headers()
            return
        if self.path == "/api/symbols":
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        for key, value in _security_headers().items():
            self.send_header(key, value)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if not self._authorize_request():
            return
        if path in {"/api/spot_runner/start", "/api/spot_runner/stop"}:
            try:
                content_len = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json({"ok": False, "error": "Invalid Content-Length"}, status=400)
                return
            if content_len < 0 or content_len > 1024 * 1024:
                self._send_json({"ok": False, "error": "Invalid payload size"}, status=400)
                return
            payload: dict[str, Any] = {}
            if content_len > 0:
                raw = self.rfile.read(content_len)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
                    return
                if not isinstance(payload, dict):
                    self._send_json({"ok": False, "error": "JSON body must be object"}, status=400)
                    return
            try:
                if path.endswith("/start"):
                    config = _normalize_spot_runner_payload(payload)
                    result = _start_spot_runner_process(config)
                else:
                    result = _stop_spot_runner_process(payload.get("symbol"))
                self._send_json({"ok": True, **result}, status=200)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
            return
        if path in {"/api/runner/start", "/api/runner/stop"}:
            try:
                content_len = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json({"ok": False, "error": "Invalid Content-Length"}, status=400)
                return
            if content_len < 0 or content_len > 1024 * 1024:
                self._send_json({"ok": False, "error": "Invalid payload size"}, status=400)
                return
            payload: dict[str, Any] = {}
            if content_len > 0:
                raw = self.rfile.read(content_len)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
                    return
                if not isinstance(payload, dict):
                    self._send_json({"ok": False, "error": "JSON body must be object"}, status=400)
                    return
            try:
                if path.endswith("/start"):
                    config = _resolve_runner_start_config(payload)
                    result = _start_runner_process(config)
                else:
                    result = _stop_runner_process(
                        payload.get("symbol"),
                        cancel_open_orders=_safe_bool(payload.get("cancel_open_orders", False), "cancel_open_orders"),
                        close_all_positions=_safe_bool(payload.get("close_all_positions", False), "close_all_positions"),
                    )
                self._send_json({"ok": True, **result}, status=200)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
            return
        if path == "/api/symbol_lists":
            try:
                content_len = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json({"ok": False, "error": "Invalid Content-Length"}, status=400)
                return
            if content_len <= 0 or content_len > 1024 * 1024:
                self._send_json({"ok": False, "error": "Invalid payload size"}, status=400)
                return
            raw = self.rfile.read(content_len)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
                return
            if not isinstance(payload, dict):
                self._send_json({"ok": False, "error": "JSON body must be object"}, status=400)
                return
            try:
                list_type = normalize_symbol_list_type(payload.get("list_type"))
                action = str(payload.get("action", "replace")).strip().lower()
                if action == "replace":
                    symbols_value = payload.get("symbols")
                    if not isinstance(symbols_value, list):
                        raise ValueError("symbols must be list when action=replace")
                    symbols = set_symbol_list(list_type, symbols_value)
                else:
                    symbols = update_symbol_list(
                        list_type,
                        action=action,
                        symbol=payload.get("symbol"),
                    )
                self._send_json(
                    {
                        "ok": True,
                        "list_type": list_type,
                        "symbols": symbols,
                        "lists": load_symbol_lists(),
                    },
                    status=HTTPStatus.OK,
                )
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
            return

        if path == "/api/competition_entries":
            try:
                content_len = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json({"ok": False, "error": "Invalid Content-Length"}, status=400)
                return
            if content_len <= 0 or content_len > 1024 * 1024:
                self._send_json({"ok": False, "error": "Invalid payload size"}, status=400)
                return
            raw = self.rfile.read(content_len)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
                return
            if not isinstance(payload, dict):
                self._send_json({"ok": False, "error": "JSON body must be object"}, status=400)
                return
            action = str(payload.get("action", "upsert")).strip().lower() or "upsert"
            try:
                if action == "delete":
                    result = delete_competition_entry(str(payload.get("entry_id", payload.get("id", ""))))
                elif action == "upsert":
                    result = upsert_competition_entry(payload)
                else:
                    self._send_json({"ok": False, "error": "Unsupported action"}, status=400)
                    return
                snapshot = build_competition_board_snapshot(refresh=False)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json({"ok": True, **result, "snapshot": snapshot}, status=200)
            return

        if path not in {
            "/api/optimize",
            "/api/short_volume_candidates",
            "/api/grid_preview",
            "/api/runner/presets/create_grid",
            "/api/runner/presets/update_grid",
            "/api/runner/presets/delete_grid",
            "/api/layer_compare",
            "/api/suggest_range",
            "/api/funding_breakdown",
            "/api/market_rankings",
            "/api/basis_monitor",
            "/api/basis_enrich",
        }:
            self._send_json({"ok": False, "error": "Not Found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            content_len = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json({"ok": False, "error": "Invalid Content-Length"}, status=400)
            return
        if content_len <= 0 or content_len > 1024 * 1024:
            self._send_json({"ok": False, "error": "Invalid payload size"}, status=400)
            return

        raw = self.rfile.read(content_len)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
            return
        if not isinstance(payload, dict):
            self._send_json({"ok": False, "error": "JSON body must be object"}, status=400)
            return

        if path == "/api/optimize":
            try:
                params = _normalize_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            job_id = _create_job(params, job_kind="optimize")
            worker = threading.Thread(target=_run_job_worker, args=(job_id,), daemon=True)
            worker.start()
            self._send_json({"ok": True, "job_id": job_id}, status=202)
            return

        if path == "/api/short_volume_candidates":
            try:
                params = _normalize_short_volume_candidates_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            job_id = _create_job(params, job_kind="short_volume_candidates")
            worker = threading.Thread(target=_run_job_worker, args=(job_id,), daemon=True)
            worker.start()
            self._send_json({"ok": True, "job_id": job_id}, status=202)
            return

        if path == "/api/grid_preview":
            try:
                params = _normalize_grid_preview_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_grid_preview(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/runner/presets/create_grid":
            try:
                params = _normalize_grid_strategy_create_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _create_custom_grid_runner_preset(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/runner/presets/update_grid":
            preset_key = str(payload.get("preset_key", "")).strip()
            if not preset_key:
                self._send_json({"ok": False, "error": "preset_key is required"}, status=400)
                return
            try:
                params = _normalize_grid_strategy_create_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _update_custom_grid_runner_preset(preset_key, params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/runner/presets/delete_grid":
            preset_key = str(payload.get("preset_key", "")).strip()
            symbol = str(payload.get("symbol", "")).upper().strip()
            if not preset_key:
                self._send_json({"ok": False, "error": "preset_key is required"}, status=400)
                return
            try:
                result = _delete_custom_grid_runner_preset(preset_key, symbol or None)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/layer_compare":
            try:
                params = _normalize_compare_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_layer_compare(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/suggest_range":
            try:
                params = _normalize_suggest_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_range_suggestion(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/funding_breakdown":
            try:
                params = _normalize_funding_breakdown_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_funding_breakdown(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/market_rankings":
            try:
                params = _normalize_market_rankings_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_market_rankings(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/basis_monitor":
            try:
                params = _normalize_basis_monitor_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_basis_monitor(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if path == "/api/basis_enrich":
            try:
                params = _normalize_basis_enrich_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_basis_enrich(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Web UI for grid optimizer.")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), _Handler)
    print(f"Grid Web UI running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
