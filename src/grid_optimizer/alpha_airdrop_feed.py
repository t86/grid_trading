"""Public official Alpha airdrop sources; no Telegram account or trading access."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .alpha_airdrop_monitor import (
    DEFAULT_ACCOUNTS, DEFAULT_BARK_CONFIG_PATH, _airdrop_scheduled_at,
    _extract_matches_for_account, _fetch_account_entries,
    _load_state, _match_alpha_airdrop_post, _parse_created_at,
    _strip_html, _rate_limit_reset_from_exception, load_bark_config, send_bark_notification,
)
from .alpha_competition_metrics import _list_articles, _text_descendants
from .audit import exclusive_json_state_lock, write_json
from .notifications import send_alert_email

UTC = timezone.utc
STATE_PATH = Path("output/alpha_airdrop_feed.json")
TELEGRAM_CHANNEL = "binance_wallet_announcements"
CMS_BASE = "https://www.binance.com/bapi/composite/v1/public/cms/article"


def parse_telegram_posts(html: str) -> list[dict[str, Any]]:
    posts = []
    for block in re.split(r'(?=<div class="tgme_widget_message [^"]*" data-post=)', html):
        ident = re.search(r'data-post="' + TELEGRAM_CHANNEL + r'/(\d+)"', block)
        published = re.search(r'<time datetime="([^"]+)"', block)
        text = re.search(r'<div class="tgme_widget_message_text js-message_text"[^>]*>(.*?)</div>', block, re.S)
        if ident and published and text:
            posts.append({"id_str": ident[1], "created_at": published[1],
                          "full_text": _strip_html(text[1]), "source": "telegram",
                          "account": TELEGRAM_CHANNEL,
                          "source_url": f"https://t.me/{TELEGRAM_CHANNEL}/{ident[1]}"})
    if not posts:
        raise ValueError("Telegram page has no readable posts")
    return posts


def fetch_telegram_posts() -> list[dict[str, Any]]:
    response = requests.get(f"https://t.me/s/{TELEGRAM_CHANNEL}", timeout=(5, 10))
    response.raise_for_status()
    return parse_telegram_posts(response.text)


def fetch_cms_posts() -> list[dict[str, Any]]:
    def get(path, params):
        response = requests.get(f"{CMS_BASE}/{path}/query", params=params, timeout=(5, 10))
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "000000" or not isinstance(payload.get("data"), dict):
            raise ValueError("Binance CMS rejected request")
        return payload["data"]
    posts, seen = [], set()
    for catalog in (93, 49):
        data = get("list", {"type": 1, "catalogId": catalog, "pageNo": 1, "pageSize": 50})
        for item in _list_articles(data, target_catalog_id=catalog):
            title, code = str(item.get("title") or ""), str(item.get("code") or "")
            if code in seen or not re.search(r"alpha", title, re.I) or not re.search(r"airdrop|空投", title, re.I):
                continue
            seen.add(code)
            detail = get("detail", {"articleCode": code})
            body = detail.get("body") or ""
            try:
                body = " ".join(_text_descendants(json.loads(body)))
            except (ValueError, TypeError):
                body = _strip_html(str(body))
            published = datetime.fromtimestamp(int(item["releaseDate"])/1000, tz=UTC)
            posts.append({"id_str": code, "created_at": published.isoformat(),
                          "full_text": f"{title} {body}", "source": "cms", "account": "Binance",
                          "source_url": f"https://www.binance.com/en/support/announcement/detail/{code}"})
            if len(posts) >= 3:
                return posts
    return posts


def normalize_post(raw: dict[str, Any], *, now: datetime) -> dict[str, Any] | None:
    match = _match_alpha_airdrop_post(raw, now=now, tz_offset_hours=8)
    if match is None:
        return None
    text = match["text"]
    symbol = next((s for s in re.findall(r"[（(]([A-Z][A-Z0-9]{1,14})[）)]", text)
                   if s not in {"UTC", "GMT", "FCFS"}), None)
    quantity = re.search(r"airdrop of\s+([\d,.]+)\s+([A-Z][A-Z0-9]{0,14})\s+tokens", text)
    if quantity is None:
        quantity = re.search(r"(?:领取|申领)\s*([\d,.]+)\s*个?\s*([A-Z][A-Z0-9]{0,14})\s*(?:代币|空投)", text)
    if quantity:
        symbol = quantity[2]
    cost = re.search(r"consume\s+(\d+)\s+(?:Binance\s+)?Alpha Points|消耗\s*(\d+)\s*个?\s*(?:币安\s*)?Alpha\s*积分", text, re.I)
    drop = re.search(r"decrease by\s+(\d+)\s+points every\s+(\d+)\s+minutes", text, re.I)
    chinese_drop = re.search(r"每\s*(\d+)\s*分钟.{0,12}?(?:降低|下降|减少)\s*(\d+)\s*分", text)
    scheduled = _airdrop_scheduled_at(match)
    published = _parse_created_at(match["created_at"])
    # Date-only previews still need to share the same event identity as details.
    date_match = _airdrop_scheduled_at({**match, "time_hint_text": match["time_hint_text"] or
                                      ("00:00 (UTC+8)" if re.search(r"[\u4e00-\u9fff]", text) else "00:00 (UTC)")})
    event_day = (scheduled or date_match or published).astimezone(timezone(timedelta(hours=8))).date().isoformat()
    live = bool(re.search(r"now live|claim now|现已上线|现已开放", text, re.I))
    stage = "live" if live else "details" if scheduled or quantity else "preview"
    return {**match, "symbol": symbol, "quantity": f"{quantity[1]} {quantity[2]}" if quantity else None,
            "points_cost": int(cost[1] or cost[2]) if cost else None,
            "threshold_drop": f"{drop[1]}/{drop[2]}" if drop else f"{chinese_drop[2]}/{chinese_drop[1]}" if chinese_drop else None,
            "scheduled_at": scheduled.isoformat() if scheduled else None,
            "stage": stage, "event_id": f"{event_day}:{symbol or (scheduled.isoformat() if scheduled else raw['id_str'])}",
            "source": raw.get("source", "x"), "source_url": raw.get("source_url") or raw.get("tweet_url"),
            "account": raw.get("account", "Binance")}


def _version(event: dict[str, Any]) -> str:
    values = [event.get(k) for k in ("stage", "points_threshold", "scheduled_at", "quantity", "points_cost", "threshold_drop")]
    return hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).hexdigest()[:16]


def _save(path: Path, state: dict[str, Any]) -> None:
    write_json(path, state)


def ingest_posts(state: dict[str, Any], posts: list[dict[str, Any]], *, now: datetime) -> None:
    events = state.setdefault("events", {})
    for raw in sorted(posts, key=lambda p: str(p.get("created_at") or "")):
        item = normalize_post(raw, now=now)
        if item is None:
            continue
        key = item["event_id"]
        if key in events and item["scheduled_at"] and events[key].get("scheduled_at") not in (None, item["scheduled_at"]):
            key = f"{key}:{item['scheduled_at']}"
        # Link unnamed teasers only when an exact start time uniquely identifies an event.
        if key not in events and item["scheduled_at"]:
            aliases = [k for k, e in events.items() if e.get("scheduled_at") == item["scheduled_at"]
                       and (not e.get("symbol") or not item.get("symbol") or e.get("symbol") == item.get("symbol"))]
            if len(aliases) == 1:
                key = aliases[0]
        event = events.setdefault(key, {"first_seen_at": now.isoformat(), "sources": [], "notices": []})
        ref = {"source": item["source"], "url": item["source_url"], "published_at": item["created_at"],
               "first_seen_at": now.isoformat()}
        if not any(s["url"] == ref["url"] for s in event["sources"]):
            event["sources"].append(ref)
            event["sources"] = event["sources"][-20:]
        digest = hashlib.sha256(item["text"].encode()).hexdigest()[:16]
        observed = event.setdefault("observed_posts", {})
        if observed.get(item["source_url"]) == digest:
            continue
        observed[item["source_url"]] = digest
        published = _parse_created_at(item["created_at"])
        latest = _parse_created_at(event.get("created_at") or "")
        if latest is not None and published < latest:
            continue
        before = _version(event)
        old_stage = event.get("stage", "preview")
        # Incomplete followups must not erase known claim conditions.
        event.update({k: v for k, v in item.items() if v is not None})
        if old_stage == "live":
            event["stage"] = "live"
        if event["stage"] != "live":
            event["stage"] = "details" if event.get("scheduled_at") or event.get("quantity") else "preview"
        event["event_id"] = key
        after = _version(event)
        if before == after:
            continue
        event["updated_at"] = now.isoformat()
        scheduled = _parse_created_at(event.get("scheduled_at") or "")
        fresh = timedelta(0) <= now - published <= timedelta(minutes=10)
        useful = scheduled is not None and scheduled > now
        if not event["notices"] or event["notices"][-1]["version"] != after:
            event["notices"].append({"version": after, "kind": event["stage"], "detected_at": now.isoformat(),
                                     "suppressed": not (fresh or useful)})
            event["notices"] = event["notices"][-30:]
    for event in events.values():
        scheduled = _parse_created_at(event.get("scheduled_at") or "")
        if scheduled and timedelta(0) <= now - scheduled <= timedelta(minutes=2):
            version = f"opening:{scheduled.isoformat()}"
            if not any(n["version"] == version for n in event["notices"]):
                event["notices"].append({"version": version, "kind": "opening", "detected_at": now.isoformat(), "suppressed": False})
    # Bound runtime state, independent of how long the daemon has been running.
    state["events"] = dict(sorted(events.items(), key=lambda kv: kv[1]["first_seen_at"], reverse=True)[:200])


def deliver_notifications(state: dict[str, Any], *, now: datetime,
                          bark_config_path: Path = DEFAULT_BARK_CONFIG_PATH,
                          alert_config_path: Path | None = None) -> None:
    config = load_bark_config(bark_config_path)
    labels = {"preview": "空投预告", "details": "空投条件更新", "live": "空投已上线", "opening": "空投开始提醒"}
    for event in state.get("events", {}).values():
        for notice in event["notices"]:
            if notice["suppressed"]:
                continue
            detected = _parse_created_at(notice["detected_at"])
            if now - detected > timedelta(minutes=10):
                notice["suppressed"] = True
                continue
            # If several revisions arrive during an outage, only deliver the newest conditions.
            if notice != event["notices"][-1]:
                continue
            schedule = _parse_created_at(event.get("scheduled_at") or "")
            bj_time = schedule.astimezone(timezone(timedelta(hours=8))).strftime("%m-%d %H:%M") if schedule else "待公布"
            summary = (f"{labels[notice['kind']]} · {event.get('symbol') or '币种待公布'}\n"
                       f"北京时间 {bj_time}，门槛 {event.get('points_threshold', '待公布')}，"
                       f"数量 {event.get('quantity', '待公布')}，消耗积分 {event.get('points_cost', '待公布')}\n"
                       "先到先得活动请到官方 App 核验资格和剩余奖池。\n" + event["text"])
            if config["enabled"] and not notice.get("bark_sent_at"):
                attempted = _parse_created_at(notice.get("bark_attempted_at") or "")
                if attempted and now - attempted < timedelta(seconds=30):
                    continue
                notice["bark_attempted_at"] = now.isoformat()
                result = send_bark_notification(
                    bark_endpoint_or_key=config.get("bark_endpoint", ""), post={**event, "text": summary,
                        "tweet_url": event["source_url"], "notification_sequence": 1,
                        "alert_title": f"{labels[notice['kind']]} {event.get('symbol') or 'Alpha'} · {bj_time}",
                        "alert_body": summary[:500]},
                    bark_base_url=config.get("bark_base_url", "https://api.day.app"),
                    bark_level=config.get("bark_level", "critical"), bark_sound=config.get("bark_sound", "alarm"),
                    bark_call=config.get("bark_call", True), timeout_seconds=10)
                # Never persist errors/URLs from the notifier: they may contain the device key.
                notice["bark_error"] = None if result.get("sent") else "Bark 发送失败，将重试"
                if result.get("sent"):
                    notice["bark_sent_at"] = now.isoformat()
            if alert_config_path is not None and not notice.get("email_sent_at"):
                result = send_alert_email(subject=f"{labels[notice['kind']]} {event.get('symbol') or 'Alpha'}", body=summary,
                                          config_path=alert_config_path)
                if result.get("sent"):
                    notice["email_sent_at"] = now.isoformat()


def check_airdrop_feed(*, now: datetime | None = None, state_path: Path = STATE_PATH,
                       notify: bool = True, enable_x: bool = False,
                       bark_config_path: Path = DEFAULT_BARK_CONFIG_PATH,
                       alert_config_path: Path | None = None) -> dict[str, Any]:
    current = now or datetime.now(UTC)
    state = _load_state(state_path)
    sources = state.setdefault("sources", {})
    loaders = [("telegram", fetch_telegram_posts, 0), ("cms", fetch_cms_posts, 300)]
    if enable_x:
        def fetch_x():
            posts = []
            for account in DEFAULT_ACCOUNTS:
                for p in _extract_matches_for_account(account, _fetch_account_entries(account), now=current, tz_offset_hours=8):
                    posts.append({**p, "id_str": p["tweet_id"], "full_text": p["text"], "source": "x", "source_url": p["tweet_url"]})
            return posts
        loaders.append(("x", fetch_x, 300))
    for name, loader, interval in loaders:
        previous = sources.get(name, {})
        retry_at = _parse_created_at(previous.get("retry_at") or "")
        checked = _parse_created_at(previous.get("checked_at") or "")
        if retry_at and current < retry_at or checked and (current - checked).total_seconds() < interval:
            continue
        try:
            posts = loader()
            if now is None:
                current = datetime.now(UTC)
            ingest_posts(state, posts, now=current)
            sources[name] = {"ok": True, "checked_at": current.isoformat(), "posts": len(posts), "failures": 0}
        except Exception as exc:
            failures = int(previous.get("failures", 0)) + 1
            retry_seconds = min(1800, 30 * 2**min(failures, 6))
            reset_at = _rate_limit_reset_from_exception(exc)
            response = getattr(exc, "response", None)
            retry_after = str(getattr(response, "headers", {}).get("Retry-After", ""))
            if retry_after.isdigit():
                retry_seconds = max(retry_seconds, int(retry_after))
            elif retry_after:
                reset_at = _parse_created_at(retry_after) or reset_at
            retry_at = current + timedelta(seconds=retry_seconds)
            if reset_at is not None:
                retry_at = max(retry_at, reset_at)
            sources[name] = {"ok": False, "checked_at": current.isoformat(), "failures": failures,
                             "error": f"{type(exc).__name__}: 采集失败（保留上次数据）",
                             "retry_at": retry_at.isoformat()}
        if notify:
            deliver_notifications(state, now=current, bark_config_path=bark_config_path, alert_config_path=alert_config_path)
        state["checked_at"] = current.isoformat()
        _save(state_path, state)  # Telegram alerts are persisted before slower fallback fetches.
    ingest_posts(state, [], now=current)
    if notify:
        deliver_notifications(state, now=current, bark_config_path=bark_config_path, alert_config_path=alert_config_path)
    state["checked_at"] = current.isoformat()
    _save(state_path, state)
    return feed_payload(state_path, now=current)


def feed_payload(path: Path = STATE_PATH, *, now: datetime | None = None) -> dict[str, Any]:
    state = _load_state(path)
    current = now or datetime.now(UTC)
    checked = _parse_created_at(state.get("checked_at") or "")
    return {"ok": True, "checked_at": state.get("checked_at"), "sources": state.get("sources", {}),
            "stale": checked is None or current - checked > timedelta(minutes=3),
            "events": sorted(state.get("events", {}).values(), key=lambda e: e.get("created_at", ""), reverse=True),
            "bark_configured": load_bark_config()["enabled"], "poll_seconds": 30,
            "telegram_mode": "public_page"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor official Telegram/CMS Alpha airdrop announcements")
    parser.add_argument("--state-path", type=Path, default=STATE_PATH)
    parser.add_argument("--bark-config-path", type=Path, default=DEFAULT_BARK_CONFIG_PATH)
    parser.add_argument("--alert-config-path", type=Path)
    parser.add_argument("--enable-x", action="store_true")
    parser.add_argument("--no-notify", action="store_true", help="Collect without sending Bark/email")
    args = parser.parse_args()
    with exclusive_json_state_lock(args.state_path, timeout_seconds=0):
        result = check_airdrop_feed(state_path=args.state_path, bark_config_path=args.bark_config_path,
                                   alert_config_path=args.alert_config_path, enable_x=args.enable_x, notify=not args.no_notify)
    print(json.dumps({"checked_at": result["checked_at"], "events": len(result["events"]), "sources": result["sources"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
