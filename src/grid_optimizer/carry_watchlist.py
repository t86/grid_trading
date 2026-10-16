from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

DEFAULT_WATCHLIST_PATH = Path("output/carry_watchlist.json")
DEFAULT_MONITOR_STATE_PATH = Path("output/carry_watchlist_monitor_state.json")
DEFAULT_BARK_PATH = Path("output/carry_watchlist_bark.json")
_SHANGHAI = timezone(timedelta(hours=8))
_BASE = {"usdm": "https://fapi.binance.com/fapi/v1", "coinm": "https://dapi.binance.com/dapi/v1"}
_CATALOG: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_CATALOG_LOCK = threading.Lock()
CONDITIONS = {"funding_cross_negative", "funding_below", "funding_above", "change_above", "change_below", "price_above", "price_below"}


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("监控配置格式异常")
    return payload


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def _locked(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(path.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _default_rule(symbol: str) -> dict[str, Any]:
    return {"id": "legacy-" + symbol, "revision": "1", "symbol": symbol, "market": "usdm", "condition": "funding_cross_negative", "threshold": 0.0, "frequency": "daily", "interval_minutes": 30, "confirmations": 1, "channel": "bark", "enabled": True}


def load_rules(path: Path = DEFAULT_WATCHLIST_PATH) -> list[dict[str, Any]]:
    payload = _load_json(path)
    if "rules" in payload:
        return payload["rules"]
    return [_default_rule(s) for s in sorted(set(payload.get("symbols", [])))]


def load_watchlist(path: Path = DEFAULT_WATCHLIST_PATH) -> list[str]:
    return sorted({r["symbol"] for r in load_rules(path) if r["market"] == "usdm"})


def update_watchlist(symbol: str, watched: bool, path: Path = DEFAULT_WATCHLIST_PATH) -> list[str]:
    symbol = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z0-9_]{3,40}", symbol):
        raise ValueError("合约名称无效")
    with _locked(path):
        rules = load_rules(path)
        if watched and not any(r["symbol"] == symbol and r["market"] == "usdm" for r in rules):
            rules.append(_default_rule(symbol))
        elif not watched:
            rules = [r for r in rules if not (r["symbol"] == symbol and r["market"] == "usdm")]
        _save_json(path, {"version": 2, "rules": rules})
    return load_watchlist(path)


def _public_get(market: str, route: str) -> Any:
    response = requests.get(_BASE[market] + "/" + route, timeout=12)
    response.raise_for_status()
    return response.json()


def fetch_contracts(market: str) -> list[dict[str, Any]]:
    if market not in _BASE:
        raise ValueError("请选择 U 本位或币本位")
    with _CATALOG_LOCK:
        cached = _CATALOG.get(market)
        if cached and time.monotonic() - cached[0] < 600:
            return cached[1]
    raw = _public_get(market, "exchangeInfo")
    rows = [{"symbol": r["symbol"], "market": market, "base_asset": r.get("baseAsset", ""), "quote_asset": r.get("quoteAsset", ""), "perpetual": r.get("contractType") == "PERPETUAL"} for r in raw["symbols"] if (r.get("status") or r.get("contractStatus")) == "TRADING"]
    with _CATALOG_LOCK:
        _CATALOG[market] = (time.monotonic(), rows)
    return rows


def search_contracts(query: str = "", market: str = "all") -> dict[str, Any]:
    if market not in {"all", "usdm", "coinm"}:
        raise ValueError("未知合约市场")
    rows, errors = [], []
    markets = list(_BASE) if market == "all" else [market]
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = {m: pool.submit(fetch_contracts, m) for m in markets}
        for m, future in pending.items():
            try:
                rows.extend(future.result())
            except Exception:
                errors.append(f"{m} 合约列表暂时不可用，请重试")
    q = query.upper().strip()[:60]
    matches = sorted([r for r in rows if q in r["symbol"] or q in r["base_asset"]], key=lambda r: (r["symbol"], r["market"]))
    return {"ok": bool(rows), "rows": matches, "total": len(matches), "errors": errors}


def save_rule(raw: dict[str, Any], path: Path = DEFAULT_WATCHLIST_PATH) -> dict[str, Any]:
    market = str(raw.get("market", "usdm"))
    symbol = str(raw.get("symbol", "")).upper().strip()
    contract = next((r for r in fetch_contracts(market) if r["symbol"] == symbol), None)
    if contract is None:
        raise ValueError("请从搜索结果选择正在交易的合约")
    condition = raw.get("condition")
    if condition not in CONDITIONS:
        raise ValueError("未知监控条件")
    if str(condition).startswith("funding_") and not contract["perpetual"]:
        raise ValueError("交割合约没有资金费率，请选择价格或涨跌幅条件")
    threshold = float(raw.get("threshold", 0))
    if not math.isfinite(threshold):
        raise ValueError("阈值必须是有效数字")
    if str(condition).startswith("price_") and threshold <= 0:
        raise ValueError("价格阈值必须大于零")
    frequency, channel = raw.get("frequency", "daily"), raw.get("channel", "bark")
    if frequency not in {"daily", "repeat", "edge"} or channel not in {"bark", "web", "both"}:
        raise ValueError("通知频率或渠道无效")
    interval, confirmations = int(raw.get("interval_minutes", 30)), int(raw.get("confirmations", 1))
    if not 5 <= interval <= 10080 or not 1 <= confirmations <= 12:
        raise ValueError("重复间隔须为 5–10080 分钟，连续确认次数须为 1–12")
    rule = {"id": str(raw.get("id") or uuid.uuid4().hex), "revision": uuid.uuid4().hex, "market": market, "symbol": symbol, "condition": condition, "threshold": threshold, "frequency": frequency, "interval_minutes": interval, "confirmations": confirmations, "channel": channel, "enabled": bool(raw.get("enabled", True))}
    with _locked(path):
        rules = load_rules(path)
        if raw.get("id") and not any(r["id"] == rule["id"] for r in rules):
            raise ValueError("规则已被删除，请刷新")
        if not raw.get("id") and len(rules) >= 200:
            raise ValueError("最多支持 200 条监控规则")
        rules = [rule if r["id"] == rule["id"] else r for r in rules] if raw.get("id") else rules + [rule]
        _save_json(path, {"version": 2, "rules": rules})
    return rule


def change_rule(rule_id: str, action: str, path: Path = DEFAULT_WATCHLIST_PATH) -> None:
    with _locked(path):
        rules = load_rules(path)
        target = next((r for r in rules if r["id"] == rule_id), None)
        if not target:
            raise ValueError("规则不存在")
        if action == "delete":
            rules.remove(target)
        elif action == "toggle":
            target.update(enabled=not target["enabled"], revision=uuid.uuid4().hex)
        else:
            raise ValueError("未知操作")
        _save_json(path, {"version": 2, "rules": rules})


def _bark_endpoint() -> str:
    return str(_load_json(DEFAULT_BARK_PATH).get("endpoint") or os.environ.get("GRID_CARRY_BARK_ENDPOINT") or "").strip().rstrip("/")


def bark_configured() -> bool:
    return bool(_bark_endpoint())


def save_bark_endpoint(endpoint: str) -> None:
    endpoint = endpoint.strip().rstrip("/")
    if not re.fullmatch(r"https://api\.day\.app/[A-Za-z0-9_-]{8,128}", endpoint):
        raise ValueError("请输入完整 Bark 地址：https://api.day.app/你的推送密钥")
    _save_json(DEFAULT_BARK_PATH, {"endpoint": endpoint})


def _send_bark(title: str, body: str) -> dict[str, Any]:
    endpoint = _bark_endpoint()
    if not endpoint:
        return {"sent": False, "error": "Bark 未配置"}
    try:
        response = requests.post(endpoint, json={"title": title, "body": body, "group": "grid-carry-watchlist", "isArchive": "1"}, timeout=12)
        response.raise_for_status()
        if response.json().get("code") != 200:
            return {"sent": False, "error": "Bark 未接受通知"}
        return {"sent": True, "error": None}
    except Exception:
        # Exception text can contain the secret endpoint. Never log it.
        return {"sent": False, "error": "Bark 发送失败，请检查配置或网络"}


def rule_label(rule: dict[str, Any]) -> str:
    threshold, condition = rule["threshold"], rule["condition"]
    return {"funding_cross_negative": "资金费率正转负", "funding_below": f"单期资金费率 < {threshold:g}%", "funding_above": f"单期资金费率 > {threshold:g}%", "change_above": f"24h 涨跌幅 > {threshold:g}%", "change_below": f"24h 涨跌幅 < {threshold:g}%", "price_above": f"最新价 > {threshold:g}", "price_below": f"最新价 < {threshold:g}"}[condition]


def _market_snapshot(market: str, funding: bool, ticker: bool) -> dict[str, Any]:
    result: dict[str, Any] = {"premium": {}, "ticker": {}, "errors": []}
    for key, needed, route in (("premium", funding, "premiumIndex"), ("ticker", ticker, "ticker/24hr")):
        if needed:
            try:
                rows = _public_get(market, route)
                result[key] = {r["symbol"]: r for r in rows}
            except Exception:
                result["errors"].append(f"{market} {key} 行情读取失败")
    return result


def _evaluate(rule: dict[str, Any], snapshot: dict[str, Any], previous: dict[str, Any], now: datetime) -> tuple[bool | None, float | None]:
    condition = rule["condition"]
    funding = condition.startswith("funding_")
    row = snapshot["premium" if funding else "ticker"].get(rule["symbol"], {})
    field = "lastFundingRate" if funding else "priceChangePercent" if condition.startswith("change_") else "lastPrice"
    try:
        value, timestamp = float(row[field]), float(row["time" if funding else "closeTime"]) / 1000
        if not math.isfinite(value) or not -60 <= now.timestamp() - timestamp <= 900:
            return None, None
    except (KeyError, TypeError, ValueError):
        return None, None
    if funding:
        value *= 100
    if condition == "funding_cross_negative":
        # Preserve a positive baseline across a zero sample; keep the event
        # active during negative rates for optional repeated reminders.
        matched = value < 0 and (previous.get("last_nonzero", 0) > 0 or previous.get("matched", False))
    else:
        matched = value < rule["threshold"] if condition.endswith("below") else value > rule["threshold"]
    return matched, value


def watchlist_payload(path: Path = DEFAULT_WATCHLIST_PATH, state_path: Path = DEFAULT_MONITOR_STATE_PATH) -> dict[str, Any]:
    state = _load_json(state_path)
    rules = load_rules(path)
    states = state.get("rules", {})
    return {"ok": True, "rules": [{**r, "label": rule_label(r), "state": states.get(r["id"], {}) if states.get(r["id"], {}).get("revision") == r["revision"] else {}} for r in rules], "symbols": load_watchlist(path), "bark_configured": bark_configured(), "updated_at": state.get("updated_at"), "errors": state.get("errors", []), "events": state.get("events", [])[-50:][::-1]}


def check_watchlist(*, watchlist_path: Path = DEFAULT_WATCHLIST_PATH, state_path: Path = DEFAULT_MONITOR_STATE_PATH, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    rules = [r for r in load_rules(watchlist_path) if r["enabled"]]
    markets = {r["market"] for r in rules}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {m: pool.submit(_market_snapshot, m, any(r["market"] == m and r["condition"].startswith("funding_") for r in rules), any(r["market"] == m and not r["condition"].startswith("funding_") for r in rules)) for m in markets}
        snapshots = {m: f.result() for m, f in futures.items()}
    errors = [e for s in snapshots.values() for e in s["errors"]]
    with _locked(state_path):
        state = _load_json(state_path)
        states, events = state.get("rules", {}), state.get("events", [])
        day = now.astimezone(_SHANGHAI).date().isoformat()
        sent = 0
        for rule in rules:
            # A user may pause/delete/edit while the market request is running.
            if not any(r["id"] == rule["id"] and r["revision"] == rule["revision"] and r["enabled"] for r in load_rules(watchlist_path)):
                continue
            prev = states.get(rule["id"], {})
            if prev.get("revision") != rule["revision"]:
                prev = {"revision": rule["revision"]}
                # Existing default observations retain their last sampled sign.
                old = state.get("symbols", {}).get(rule["symbol"], {}) if rule["id"].startswith("legacy-") else {}
                if isinstance(old.get("last_rate"), (int, float)):
                    prev["last_nonzero"] = old["last_rate"] * 100
                    prev["last_notice_day"] = old.get("last_alert_attempt_day")
            matched, value = _evaluate(rule, snapshots[rule["market"]], prev, now)
            current = {**prev, "checked_at": now.isoformat(), "value": value, "error": None}
            if matched is None:
                current.update(error="行情缺失或超过15分钟，暂停判断", streak=0)
                states[rule["id"]] = current
                continue
            streak = int(prev.get("streak", 0)) + 1 if matched else 0
            confirmed = matched and streak >= rule["confirmations"]
            current.update(matched=matched, streak=streak, confirmed=confirmed)
            if not matched:
                current["notified_for_match"] = False
            if value != 0:
                current["last_nonzero"] = value
            due = confirmed
            if rule["frequency"] == "daily":
                due = due and prev.get("last_notice_day") != day
            elif rule["frequency"] == "repeat":
                due = due and now.timestamp() - prev.get("last_notice_ts", 0) >= rule["interval_minutes"] * 60
            else:
                due = due and not prev.get("notified_for_match", False)
            if due:
                if rule["channel"] == "bark" and not bark_configured():
                    # No delivery was attempted. Configuring Bark later should
                    # allow the next check to notify, even in daily/edge mode.
                    current["last_notification"] = {"error": "Bark 未配置"}
                    states[rule["id"]] = current
                    continue
                title = f"合约监控：{rule['symbol']}"
                unit = "" if rule["condition"].startswith("price_") else "%"
                body = f"{rule['market']} · {rule_label(rule)}；当前值 {value:g}{unit}。{now.astimezone(_SHANGHAI).strftime('%m-%d %H:%M')} 北京时间"
                event = {"rule_id": rule["id"], "symbol": rule["symbol"], "time": now.isoformat(), "body": body, "channel": rule["channel"], "sent": False}
                # Reserve the notification before the network call. A timeout or
                # process restart cannot produce repeated posts in the interval.
                current.update(last_notice_day=day, last_notice_ts=now.timestamp(), notified_for_match=True)
                states[rule["id"]] = current
                _save_json(state_path, {**state, "rules": states, "events": events[-200:]})
                if rule["channel"] in {"bark", "both"}:
                    event.update(_send_bark(title, body))
                    sent += int(event["sent"])
                events.append(event)
                current["last_notification"] = event
            states[rule["id"]] = current
        _save_json(state_path, {"version": 2, "rules": states, "events": events[-200:], "updated_at": now.isoformat(), "errors": errors})
    return {"ok": not errors, "watch_count": len(rules), "bark_configured": bark_configured(), "bark_sent": sent, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor selected Binance futures conditions")
    parser.add_argument("--watchlist-path", default=str(DEFAULT_WATCHLIST_PATH))
    parser.add_argument("--state-path", default=str(DEFAULT_MONITOR_STATE_PATH))
    args = parser.parse_args()
    print(json.dumps(check_watchlist(watchlist_path=Path(args.watchlist_path), state_path=Path(args.state_path)), ensure_ascii=False))


if __name__ == "__main__":
    main()
