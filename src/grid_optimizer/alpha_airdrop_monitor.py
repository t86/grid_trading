from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any

import requests

from .notifications import send_alert_email

DEFAULT_ACCOUNTS: tuple[str, ...] = ("binancezh", "BinanceWallet")
DEFAULT_STATE_PATH = Path("output/alpha_airdrop_monitor_state.json")
DEFAULT_ALERT_CONFIG_PATH = Path("output/alert_notifier_config.json")
DEFAULT_TZ_OFFSET_HOURS = 8
DEFAULT_REQUEST_TIMEOUT_SECONDS = 20

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
    for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _is_today_in_tz(post_time: datetime, *, now: datetime, tz_offset_hours: int) -> bool:
    tz = timezone(timedelta(hours=tz_offset_hours))
    return post_time.astimezone(tz).date() == now.astimezone(tz).date()


def _extract_points_threshold(text: str) -> int | None:
    for pattern in _POINTS_PATTERNS:
        match = pattern.search(text)
        if match:
            return int(match.group(1))
    return None


def _has_time_hint(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TIME_HINT_PATTERNS)


def _match_alpha_airdrop_post(entry: dict[str, Any], *, now: datetime, tz_offset_hours: int) -> dict[str, Any] | None:
    tweet_id = str(entry.get("id_str") or entry.get("id") or "").strip()
    text = _normalize_text(entry.get("full_text") or entry.get("text") or "")
    created_at = _parse_created_at(str(entry.get("created_at") or ""))
    if not tweet_id or not text or created_at is None:
        return None
    if not _is_today_in_tz(created_at, now=now, tz_offset_hours=tz_offset_hours):
        return None

    normalized = text.casefold()
    if "alpha" not in normalized:
        return None
    if "airdrop" not in normalized and "空投" not in text:
        return None
    points_threshold = _extract_points_threshold(text)
    if points_threshold is None:
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


def _fetch_account_entries(account: str, *, timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS) -> list[dict[str, Any]]:
    url = f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{account}"
    response = requests.get(
        url,
        timeout=timeout_seconds,
        headers={"User-Agent": "Mozilla/5.0 (compatible; grid-optimizer-alpha-monitor/1.0)"},
    )
    response.raise_for_status()
    return _extract_entries_from_syndication_html(response.text)


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
            "notification_count": int(current.get("notification_count") or 0) + 1,
            "last_notified_at": now.astimezone(UTC).isoformat(),
            "tweet_url": candidate.get("tweet_url"),
            "created_at": candidate.get("created_at"),
            "points_threshold": candidate.get("points_threshold"),
            "text": candidate.get("text"),
        }
    return updated


def _build_email_subject(post: dict[str, Any]) -> str:
    return (
        f"[grid][alpha-airdrop] @{post.get('account')} "
        f"Alpha 空投提醒 {post.get('notification_sequence')}/3"
    )


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


def _send_notifications(
    candidates: list[dict[str, Any]],
    *,
    alert_config_path: Path | None,
) -> tuple[int, list[dict[str, Any]]]:
    sent = 0
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        result = send_alert_email(
            subject=_build_email_subject(candidate),
            body=_build_email_body(candidate),
            config_path=alert_config_path,
        )
        results.append(result)
        if result.get("sent"):
            sent += 1
    return sent, results


def check_alpha_airdrop_posts(
    *,
    accounts: tuple[str, ...] = DEFAULT_ACCOUNTS,
    now: datetime | None = None,
    tz_offset_hours: int = DEFAULT_TZ_OFFSET_HOURS,
    state_path: Path = DEFAULT_STATE_PATH,
    alert_config_path: Path | None = DEFAULT_ALERT_CONFIG_PATH,
) -> dict[str, Any]:
    current_now = now or datetime.now(UTC)
    state = _load_state(state_path)
    account_results: list[AccountCheckResult] = []
    matches: list[dict[str, Any]] = []
    errors: list[str] = []

    for account in accounts:
        try:
            entries = _fetch_account_entries(account)
            account_matches = _extract_matches_for_account(
                account,
                entries,
                now=current_now,
                tz_offset_hours=tz_offset_hours,
            )
            account_results.append(
                AccountCheckResult(account=account, fetched=True, matches=account_matches, error=None)
            )
            matches.extend(account_matches)
        except Exception as exc:
            error = f"{account}: {type(exc).__name__}: {exc}"
            account_results.append(AccountCheckResult(account=account, fetched=False, matches=[], error=error))
            errors.append(error)

    candidates = _select_notification_candidates(matches, state, now=current_now)
    emails_sent, email_results = _send_notifications(
        candidates,
        alert_config_path=alert_config_path,
    )
    sent_candidates = [
        candidate
        for candidate, result in zip(candidates, email_results, strict=False)
        if result.get("sent")
    ]
    new_state = _update_state_for_candidates(state, sent_candidates, now=current_now)
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
                    "errors": result["errors"],
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
