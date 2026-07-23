from __future__ import annotations

import argparse
import json
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from .notifications import send_alert_email

DEFAULT_ACCOUNTS: tuple[str, ...] = ("binancezh", "BinanceWallet")
DEFAULT_STATE_PATH = Path("output/alpha_airdrop_monitor_state.json")
DEFAULT_ALERT_CONFIG_PATH = Path("output/alert_notifier_config.json")
DEFAULT_BARK_CONFIG_PATH = Path("output/alpha_airdrop_monitor_bark.json")
DEFAULT_TZ_OFFSET_HOURS = 8
DEFAULT_REQUEST_TIMEOUT_SECONDS = 20
DEFAULT_BARK_URL = "https://api.day.app"
DEFAULT_BARK_LEVEL = "critical"
DEFAULT_BARK_SOUND = "alarm"
NITTER_RSS_BASE_URL = "https://nitter.net"

_POINTS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"at least\s+(\d{2,6})\s+binance alpha points", re.IGNORECASE),
    re.compile(r"(\d{2,6})\s+binance alpha points", re.IGNORECASE),
    re.compile(r"至少\s*(\d{2,6})\s*个?\s*币安?\s*alpha\s*积分", re.IGNORECASE),
    re.compile(r"持有\s*(?:至少)?\s*(\d{2,6})\s*个?\s*币安?\s*alpha\s*积分", re.IGNORECASE),
    re.compile(r"(\d{2,6})\s*个?\s*alpha\s*积分", re.IGNORECASE),
)
_TIME_HINT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b\d{1,2}:\d{2}\b"),
    re.compile(r"today at \d{1,2}:\d{2}", re.IGNORECASE),
    re.compile(r"今天\s*\d{1,2}:\d{2}"),
)
_TIME_EXTRACT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"today at\s+(\d{1,2}:\d{2}(?:\s*\(utc(?:[+-]\d+)?\))?)", re.IGNORECASE),
    re.compile(r"tomorrow at\s+(\d{1,2}:\d{2}(?:\s*\(utc(?:[+-]\d+)?\))?)", re.IGNORECASE),
    re.compile(r"今天\s*(\d{1,2}:\d{2}(?:（UTC[+-]\d+）|\(UTC[+-]\d+\))?)"),
    re.compile(r"明天\s*(\d{1,2}:\d{2}(?:（UTC[+-]\d+）|\(UTC[+-]\d+\))?)"),
    re.compile(r"\b(\d{1,2}:\d{2}\s*\(utc(?:[+-]\d+)?\))", re.IGNORECASE),
    re.compile(r"\b(\d{1,2}:\d{2})\b"),
)
_RELATIVE_SCHEDULE_DAY_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"\btomorrow\b", re.IGNORECASE), 1),
    (re.compile(r"明天"), 1),
    (re.compile(r"后天"), 2),
)
_EXPLICIT_SCHEDULE_DAY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?<!\d)(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]?"),
    re.compile(
        r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
        r"sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\s+(\d{1,2})\b",
        re.IGNORECASE,
    ),
)
_EN_MONTHS: dict[str, int] = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


@dataclass(slots=True)
class AccountCheckResult:
    account: str
    fetched: bool
    matches: list[dict[str, Any]]
    error: str | None = None


def _normalize_text(value: str) -> str:
    cleaned = unescape(str(value or ""))
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _parse_created_at(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in (
        "%a %b %d %H:%M:%S %z %Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ):
        try:
            normalized = raw[:-3] + "+0000" if raw.endswith(" GMT") else raw
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _is_today_in_tz(post_time: datetime, *, now: datetime, tz_offset_hours: int) -> bool:
    tz = timezone(timedelta(hours=tz_offset_hours))
    return post_time.astimezone(tz).date() == now.astimezone(tz).date()


def _has_current_or_future_schedule(text: str, *, post_time: datetime, now: datetime, tz_offset_hours: int) -> bool:
    tz = timezone(timedelta(hours=tz_offset_hours))
    post_day = post_time.astimezone(tz).date()
    today = now.astimezone(tz).date()

    for pattern, day_offset in _RELATIVE_SCHEDULE_DAY_PATTERNS:
        if pattern.search(text):
            return post_day + timedelta(days=day_offset) >= today

    chinese_date = _EXPLICIT_SCHEDULE_DAY_PATTERNS[0].search(text)
    if chinese_date:
        month = int(chinese_date.group(1))
        day = int(chinese_date.group(2))
        try:
            scheduled_day = post_day.replace(month=month, day=day)
        except ValueError:
            return False
        if scheduled_day < post_day:
            scheduled_day = scheduled_day.replace(year=scheduled_day.year + 1)
        return scheduled_day >= today

    english_date = _EXPLICIT_SCHEDULE_DAY_PATTERNS[1].search(text)
    if english_date:
        month_text = english_date.group(0).split()[0].rstrip(".").casefold()
        month = _EN_MONTHS.get(month_text[:3], 0)
        day = int(english_date.group(1))
        try:
            scheduled_day = post_day.replace(month=month, day=day)
        except ValueError:
            return False
        if scheduled_day < post_day:
            scheduled_day = scheduled_day.replace(year=scheduled_day.year + 1)
        return scheduled_day >= today

    return False


def _extract_points_threshold(text: str) -> int | None:
    for pattern in _POINTS_PATTERNS:
        match = pattern.search(text)
        if match:
            return int(match.group(1))
    return None


def _has_time_hint(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TIME_HINT_PATTERNS)


def _extract_time_hint_text(text: str) -> str | None:
    for pattern in _TIME_EXTRACT_PATTERNS:
        match = pattern.search(text)
        if match:
            return _normalize_text(match.group(1))
    return None


def _is_eligible_alpha_airdrop_announcement(text: str, normalized: str) -> bool:
    return (
        "claim" in normalized
        and ("eligible" in normalized or "符合" in text)
        and ("alpha points" in normalized or "alpha 积分" in normalized or "Alpha 积分" in text)
    )


def _match_alpha_airdrop_post(entry: dict[str, Any], *, now: datetime, tz_offset_hours: int) -> dict[str, Any] | None:
    tweet_id = str(entry.get("id_str") or entry.get("id") or "").strip()
    text = _normalize_text(entry.get("full_text") or entry.get("text") or "")
    created_at = _parse_created_at(str(entry.get("created_at") or ""))
    if not tweet_id or not text or created_at is None:
        return None
    posted_today = _is_today_in_tz(created_at, now=now, tz_offset_hours=tz_offset_hours)
    scheduled_now_or_later = _has_current_or_future_schedule(
        text,
        post_time=created_at,
        now=now,
        tz_offset_hours=tz_offset_hours,
    )
    if not posted_today and not scheduled_now_or_later:
        return None

    normalized = text.casefold()
    if "alpha" not in normalized:
        return None
    if "airdrop" not in normalized and "空投" not in text:
        return None
    points_threshold = _extract_points_threshold(text)
    if points_threshold is None and not _is_eligible_alpha_airdrop_announcement(text, normalized):
        return None

    action_hit = any(
        token in normalized
        for token in ("claim", "trade", "points", "today", "get ready", "first-come", "first come")
    ) or any(token in text for token in ("领取", "交易", "积分", "今天", "准备"))
    if not action_hit:
        return None

    return {
        "tweet_id": tweet_id,
        "created_at": created_at.astimezone(UTC).isoformat(),
        "text": text,
        "points_threshold": points_threshold,
        "has_time_hint": _has_time_hint(text),
        "time_hint_text": _extract_time_hint_text(text),
    }


def _extract_entries_from_syndication_html(html: str) -> list[dict[str, Any]]:
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.S)
    if not match:
        return []
    payload = json.loads(match.group(1))
    entries = (((payload.get("props") or {}).get("pageProps") or {}).get("timeline") or {}).get("entries") or []
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict)]


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return _normalize_text(text)


def _extract_entries_from_nitter_rss(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    entries: list[dict[str, Any]] = []
    for item in root.findall("./channel/item"):
        title = _normalize_text(item.findtext("title") or "")
        description = _strip_html(item.findtext("description") or "")
        text = title or description
        if description and description not in text:
            text = _normalize_text(f"{text} {description}")
        link = _normalize_text(item.findtext("link") or "")
        tweet_id_match = re.search(r"/status/(\d+)", link)
        tweet_id = tweet_id_match.group(1) if tweet_id_match else ""
        created_at = _parse_created_at(item.findtext("pubDate") or "")
        if not tweet_id or not text or created_at is None:
            continue
        entries.append(
            {
                "type": "tweet",
                "entry_id": f"tweet-{tweet_id}",
                "content": {
                    "tweet": {
                        "id_str": tweet_id,
                        "created_at": created_at.strftime("%a %b %d %H:%M:%S %z %Y"),
                        "full_text": text,
                    }
                },
            }
        )
    return entries


def _fetch_account_entries(account: str, *, timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS) -> list[dict[str, Any]]:
    url = f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{account}"
    response = requests.get(
        url,
        timeout=timeout_seconds,
        headers={"User-Agent": "Mozilla/5.0 (compatible; grid-optimizer-alpha-monitor/1.0)"},
    )
    response.raise_for_status()
    return _extract_entries_from_syndication_html(response.text)


def _fetch_account_entries_from_nitter_rss(
    account: str,
    *,
    timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    url = f"{NITTER_RSS_BASE_URL}/{account}/rss"
    response = requests.get(
        url,
        timeout=timeout_seconds,
        headers={"User-Agent": "Mozilla/5.0 (compatible; grid-optimizer-alpha-monitor/1.0)"},
    )
    response.raise_for_status()
    return _extract_entries_from_nitter_rss(response.text)


def _merge_entries(primary: list[dict[str, Any]], secondary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in [*primary, *secondary]:
        tweet = ((entry.get("content") or {}).get("tweet") or {}) if isinstance(entry, dict) else {}
        tweet_id = str(tweet.get("id_str") or tweet.get("id") or entry.get("entry_id") or "").strip()
        if tweet_id and tweet_id in seen:
            continue
        if tweet_id:
            seen.add(tweet_id)
        merged.append(entry)
    return merged


def _extract_matches_for_account(account: str, entries: list[dict[str, Any]], *, now: datetime, tz_offset_hours: int) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        if str(entry.get("type") or "").strip() != "tweet":
            continue
        tweet = ((entry.get("content") or {}).get("tweet") or {})
        if not isinstance(tweet, dict):
            continue
        matched = _match_alpha_airdrop_post(tweet, now=now, tz_offset_hours=tz_offset_hours)
        if matched is None:
            continue
        matched["account"] = account
        matched["tweet_url"] = f"https://x.com/{account}/status/{matched['tweet_id']}"
        matches.append(matched)
    return matches


def _load_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_match_key(post: dict[str, Any]) -> str:
    return f"{post.get('account')}:{post.get('tweet_id')}"


def _select_notification_candidates(
    matches: list[dict[str, Any]],
    state: dict[str, Any],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for match in matches:
        key = _build_match_key(match)
        current = state.get(key) if isinstance(state.get(key), dict) else {}
        count = int(current.get("notification_count") or 0)
        if count >= 3:
            continue
        candidate = dict(match)
        candidate["bark_notification_sent"] = bool(current.get("bark_notification_sent"))
        candidate["notification_sequence"] = count + 1
        candidate["detected_at"] = now.astimezone(UTC).isoformat()
        candidates.append(candidate)
    return candidates


def _update_state_for_candidates(state: dict[str, Any], candidates: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    updated = dict(state)
    for candidate in candidates:
        key = _build_match_key(candidate)
        current = updated.get(key) if isinstance(updated.get(key), dict) else {}
        updated[key] = {
            **current,
            "notification_count": int(current.get("notification_count") or 0) + 1,
            "last_notified_at": now.astimezone(UTC).isoformat(),
            "tweet_url": candidate.get("tweet_url"),
            "created_at": candidate.get("created_at"),
            "points_threshold": candidate.get("points_threshold"),
            "text": candidate.get("text"),
        }
    return updated


def _update_state_for_bark_candidates(
    state: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    updated = dict(state)
    for candidate in candidates:
        key = _build_match_key(candidate)
        current = updated.get(key) if isinstance(updated.get(key), dict) else {}
        updated[key] = {
            **current,
            "bark_notification_sent": True,
            "bark_last_notified_at": now.astimezone(UTC).isoformat(),
        }
    return updated


def _build_email_subject(post: dict[str, Any]) -> str:
    return _build_alert_headline(post)


def _build_email_body(post: dict[str, Any]) -> str:
    lines = [
        "Binance Alpha 空投监控命中",
        "",
        f"账号: @{post.get('account')}",
        f"积分门槛: {post.get('points_threshold')}",
        f"发布时间(UTC): {post.get('created_at')}",
        f"提醒次数: 第 {post.get('notification_sequence')}/3 次提醒",
        f"链接: {post.get('tweet_url')}",
        "",
        "正文:",
        str(post.get("text") or ""),
    ]
    return "\n".join(lines).strip() + "\n"


def _format_points_bucket(points_threshold: Any) -> str:
    try:
        points = int(points_threshold)
    except (TypeError, ValueError):
        return "xxx"
    if points < 100:
        return str(points)
    return f"{str(points)[0]}xx"


def _normalize_headline_time(time_hint: str) -> str:
    match = re.search(r"(\d{1,2}:\d{2})", str(time_hint or ""))
    return match.group(1) if match else ""


def _build_alert_headline(post: dict[str, Any]) -> str:
    bucket = _format_points_bucket(post.get("points_threshold"))
    time_text = _normalize_headline_time(str(post.get("time_hint_text") or ""))
    if time_text:
        return f"！！！空投 {bucket} {time_text}"
    return f"！！！空投 {bucket}"


def _normalize_bark_base_url(value: str) -> str:
    text = str(value or "").strip().rstrip("/")
    return text or DEFAULT_BARK_URL


def _extract_bark_key(value: str) -> str:
    text = str(value or "").strip().rstrip("/")
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        parsed = urlparse(text)
        parts = [part for part in parsed.path.split("/") if part]
        if parts and parts[0] == "push":
            parts = parts[1:]
        return parts[0] if parts else ""
    return text


def _build_bark_title(post: dict[str, Any]) -> str:
    return _build_alert_headline(post)


def _build_bark_body(post: dict[str, Any]) -> str:
    summary = str(post.get("text") or "").strip()
    if len(summary) > 160:
        summary = summary[:157] + "..."
    return f"积分门槛 {post.get('points_threshold')}，第 {post.get('notification_sequence')}/3 次提醒\n{summary}"


def _is_airdrop_time_passed(post: dict[str, Any], *, now: datetime, tz_offset_hours: int) -> bool:
    time_hint = str(post.get("time_hint_text") or "")
    time_match = re.search(r"(\d{1,2}):(\d{2})", time_hint)
    if time_match is None:
        return False

    try:
        created_at = datetime.fromisoformat(str(post.get("created_at") or ""))
    except ValueError:
        return False
    if created_at.tzinfo is None:
        return False

    offset_match = re.search(r"utc\s*([+-]\d+)?", time_hint, re.IGNORECASE)
    offset_hours = int(offset_match.group(1) or 0) if offset_match else tz_offset_hours
    schedule_tz = timezone(timedelta(hours=offset_hours))
    scheduled_day = created_at.astimezone(schedule_tz).date()
    text = str(post.get("text") or "")
    chinese_date = _EXPLICIT_SCHEDULE_DAY_PATTERNS[0].search(text)
    english_date = _EXPLICIT_SCHEDULE_DAY_PATTERNS[1].search(text)
    try:
        if chinese_date:
            scheduled_day = scheduled_day.replace(month=int(chinese_date.group(1)), day=int(chinese_date.group(2)))
        elif english_date:
            month_text = english_date.group(0).split()[0].rstrip(".").casefold()
            scheduled_day = scheduled_day.replace(month=_EN_MONTHS[month_text[:3]], day=int(english_date.group(1)))
        else:
            for pattern, day_offset in _RELATIVE_SCHEDULE_DAY_PATTERNS:
                if pattern.search(text):
                    scheduled_day += timedelta(days=day_offset)
                    break
        if (chinese_date or english_date) and scheduled_day < created_at.astimezone(schedule_tz).date():
            scheduled_day = scheduled_day.replace(year=scheduled_day.year + 1)
        scheduled_at = datetime(
            scheduled_day.year,
            scheduled_day.month,
            scheduled_day.day,
            int(time_match.group(1)),
            int(time_match.group(2)),
            tzinfo=schedule_tz,
        )
    except (KeyError, ValueError):
        return False
    return now.astimezone(schedule_tz) > scheduled_at


def send_bark_notification(
    *,
    bark_endpoint_or_key: str,
    post: dict[str, Any],
    bark_base_url: str = DEFAULT_BARK_URL,
    bark_level: str = DEFAULT_BARK_LEVEL,
    bark_sound: str = DEFAULT_BARK_SOUND,
    bark_call: bool = True,
    timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    key = _extract_bark_key(bark_endpoint_or_key)
    if not key:
        return {"sent": False, "error": "bark_disabled", "url": None}
    base_url = _normalize_bark_base_url(bark_base_url)
    url = f"{base_url}/{key}"
    payload = {
        "title": _build_bark_title(post),
        "body": _build_bark_body(post),
        "url": str(post.get("tweet_url") or ""),
        "group": "binance-alpha-airdrop",
        "level": bark_level,
        "sound": bark_sound,
        "call": "1" if bark_call else "0",
        "isArchive": "1",
    }
    result = {"sent": False, "error": None, "url": url}
    try:
        response = requests.post(url, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        result["sent"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def load_bark_config(path: Path | None = None) -> dict[str, Any]:
    config_path = Path(path or DEFAULT_BARK_CONFIG_PATH)
    file_config = _load_state(config_path)
    env_endpoint = str(os.environ.get("GRID_ALPHA_AIRDROP_BARK_ENDPOINT", "")).strip()
    env_base_url = str(os.environ.get("GRID_ALPHA_AIRDROP_BARK_BASE_URL", "")).strip()
    env_level = str(os.environ.get("GRID_ALPHA_AIRDROP_BARK_LEVEL", "")).strip()
    env_sound = str(os.environ.get("GRID_ALPHA_AIRDROP_BARK_SOUND", "")).strip()
    env_call = str(os.environ.get("GRID_ALPHA_AIRDROP_BARK_CALL", "")).strip().lower()
    return {
        "enabled": bool(env_endpoint or file_config.get("bark_endpoint")),
        "bark_endpoint": env_endpoint or str(file_config.get("bark_endpoint") or "").strip(),
        "bark_base_url": env_base_url or str(file_config.get("bark_base_url") or DEFAULT_BARK_URL).strip(),
        "bark_level": env_level or str(file_config.get("bark_level") or DEFAULT_BARK_LEVEL).strip(),
        "bark_sound": env_sound or str(file_config.get("bark_sound") or DEFAULT_BARK_SOUND).strip(),
        "bark_call": env_call not in {"0", "false", "no"} if env_call else bool(file_config.get("bark_call", True)),
        "config_path": str(config_path),
    }


def _send_notifications(
    candidates: list[dict[str, Any]],
    *,
    alert_config_path: Path | None,
    bark_config_path: Path | None,
    now: datetime,
    tz_offset_hours: int,
) -> tuple[int, list[dict[str, Any]], int, list[dict[str, Any]]]:
    bark_config = load_bark_config(bark_config_path)
    email_sent = 0
    results: list[dict[str, Any]] = []
    bark_sent = 0
    bark_results: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate.get("bark_notification_sent"):
            bark_result = {"sent": False, "error": "bark_already_sent", "url": None}
        elif _is_airdrop_time_passed(candidate, now=now, tz_offset_hours=tz_offset_hours):
            bark_result = {"sent": False, "error": "airdrop_time_passed", "url": None}
        else:
            bark_result = send_bark_notification(
                bark_endpoint_or_key=str(bark_config.get("bark_endpoint") or ""),
                bark_base_url=str(bark_config.get("bark_base_url") or DEFAULT_BARK_URL),
                bark_level=str(bark_config.get("bark_level") or DEFAULT_BARK_LEVEL),
                bark_sound=str(bark_config.get("bark_sound") or DEFAULT_BARK_SOUND),
                bark_call=bool(bark_config.get("bark_call", True)),
                post=candidate,
            )
        bark_results.append(bark_result)
        if bark_result.get("sent"):
            bark_sent += 1
        result = send_alert_email(
            subject=_build_email_subject(candidate),
            body=_build_email_body(candidate),
            config_path=alert_config_path,
        )
        results.append(result)
        if result.get("sent"):
            email_sent += 1
    return email_sent, results, bark_sent, bark_results


def check_alpha_airdrop_posts(
    *,
    accounts: tuple[str, ...] = DEFAULT_ACCOUNTS,
    now: datetime | None = None,
    tz_offset_hours: int = DEFAULT_TZ_OFFSET_HOURS,
    state_path: Path = DEFAULT_STATE_PATH,
    alert_config_path: Path | None = DEFAULT_ALERT_CONFIG_PATH,
    bark_config_path: Path | None = DEFAULT_BARK_CONFIG_PATH,
) -> dict[str, Any]:
    current_now = now or datetime.now(UTC)
    state = _load_state(state_path)
    account_results: list[AccountCheckResult] = []
    matches: list[dict[str, Any]] = []
    errors: list[str] = []

    for account in accounts:
        account_errors: list[str] = []
        entries: list[dict[str, Any]] = []
        fetched = False
        try:
            entries = _fetch_account_entries(account)
            fetched = True
        except Exception as exc:
            account_errors.append(f"syndication {type(exc).__name__}: {exc}")
        try:
            rss_entries = _fetch_account_entries_from_nitter_rss(account)
            entries = _merge_entries(entries, rss_entries)
            fetched = True
        except Exception as exc:
            account_errors.append(f"nitter_rss {type(exc).__name__}: {exc}")

        account_matches = _extract_matches_for_account(
            account,
            entries,
            now=current_now,
            tz_offset_hours=tz_offset_hours,
        )
        error = f"{account}: {'; '.join(account_errors)}" if account_errors else None
        if error:
            errors.append(error)
        account_results.append(AccountCheckResult(account=account, fetched=fetched, matches=account_matches, error=error))
        matches.extend(account_matches)

    candidates = _select_notification_candidates(matches, state, now=current_now)
    emails_sent, email_results, bark_sent, bark_results = _send_notifications(
        candidates,
        alert_config_path=alert_config_path,
        bark_config_path=bark_config_path,
        now=current_now,
        tz_offset_hours=tz_offset_hours,
    )
    sent_candidates = [
        candidate
        for candidate, result in zip(candidates, email_results, strict=False)
        if result.get("sent")
    ]
    new_state = _update_state_for_candidates(state, sent_candidates, now=current_now)
    bark_sent_candidates = [
        candidate
        for candidate, result in zip(candidates, bark_results, strict=False)
        if result.get("sent")
    ]
    new_state = _update_state_for_bark_candidates(new_state, bark_sent_candidates, now=current_now)
    _save_state(state_path, new_state)
    return {
        "checked_at": current_now.astimezone(UTC).isoformat(),
        "accounts": [
            {
                "account": item.account,
                "fetched": item.fetched,
                "matches": item.matches,
                "error": item.error,
            }
            for item in account_results
        ],
        "matches": matches,
        "notifications": candidates,
        "emails_sent": emails_sent,
        "bark_sent": bark_sent,
        "bark_results": bark_results,
        "errors": errors,
        "state_path": str(state_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor Binance X accounts for Alpha airdrop posts.")
    parser.add_argument(
        "--accounts",
        type=str,
        default=",".join(DEFAULT_ACCOUNTS),
        help="Comma-separated X account screen names.",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        default=str(DEFAULT_STATE_PATH),
        help="Path to notification state json.",
    )
    parser.add_argument(
        "--alert-config-path",
        type=str,
        default=str(DEFAULT_ALERT_CONFIG_PATH),
        help="Path to alert notifier config json.",
    )
    parser.add_argument(
        "--bark-config-path",
        type=str,
        default="output/alpha_airdrop_monitor_bark.json",
        help="Path to Bark config json.",
    )
    parser.add_argument(
        "--tz-offset-hours",
        type=int,
        default=DEFAULT_TZ_OFFSET_HOURS,
        help="Day boundary timezone offset, default UTC+8.",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print the check result as JSON.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    accounts = tuple(part.strip() for part in str(args.accounts or "").split(",") if part.strip())
    result = check_alpha_airdrop_posts(
        accounts=accounts or DEFAULT_ACCOUNTS,
        tz_offset_hours=int(args.tz_offset_hours),
        state_path=Path(args.state_path),
        alert_config_path=Path(args.alert_config_path) if str(args.alert_config_path or "").strip() else None,
        bark_config_path=Path(args.bark_config_path) if str(args.bark_config_path or "").strip() else None,
    )
    if args.print_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "checked_at": result["checked_at"],
                    "matches": len(result["matches"]),
                    "emails_sent": result["emails_sent"],
                    "bark_sent": result["bark_sent"],
                    "errors": result["errors"],
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
