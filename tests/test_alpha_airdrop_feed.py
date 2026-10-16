import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from grid_optimizer.alpha_airdrop_feed import (
    parse_telegram_posts, normalize_post, ingest_posts, deliver_notifications,
    check_airdrop_feed, feed_payload, fetch_cms_posts,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 5, 2, tzinfo=UTC)
DETAIL = (
    "Binance Alpha is the first platform to feature Concrete (CT), with trading starting "
    "on September 30, 2026, at 08:00 (UTC). Users with at least 223 Binance Alpha Points "
    "can claim an airdrop of 200 CT tokens on a first-come, first-served basis. "
    "The score threshold will automatically decrease by 5 points every 5 minutes. "
    "Claiming the airdrop will consume 15 Binance Alpha Points."
)


def post(text=DETAIL, *, source="telegram", created=NOW, ident="1814"):
    return {"id_str": ident, "created_at": created.isoformat(), "full_text": text,
            "source": source, "account": "BinanceWallet",
            "source_url": f"https://t.me/binance_wallet_announcements/{ident}"}


def test_telegram_parser_reads_only_the_post_not_quoted_message():
    html = '''<div class="tgme_widget_message text_not_supported_wrap" data-post="binance_wallet_announcements/1814">
    <div class="tgme_widget_message_reply_text">old quoted airdrop</div>
    <div class="tgme_widget_message_text js-message_text">Binance Alpha<br>CT &amp; rewards</div>
    <time datetime="2026-09-30T05:01:18+00:00"></time></div>'''
    parsed = parse_telegram_posts(html)
    assert len(parsed) == 1
    assert parsed[0]["full_text"] == "Binance Alpha CT & rewards"
    assert parsed[0]["source_url"].endswith("/1814")
    assert parsed[0]["created_at"] == "2026-09-30T05:01:18+00:00"


def test_parse_detects_empty_challenge_instead_of_reporting_healthy():
    import pytest
    with pytest.raises(ValueError):
        parse_telegram_posts("<html>challenge</html>")


def test_normalize_extracts_claim_conditions_and_beijing_time():
    item = normalize_post(post(), now=NOW)
    assert item["symbol"] == "CT"
    assert item["quantity"] == "200 CT"
    assert item["points_cost"] == 15
    assert item["scheduled_at"] == "2026-09-30T08:00:00+00:00"
    assert item["stage"] == "details"
    assert item["threshold_drop"] == "5/5"


def test_preview_without_points_is_not_discarded():
    item = normalize_post(post("Binance Alpha will feature Concrete (CT) on September 30. "
                               "An airdrop is coming; further details soon."), now=NOW)
    assert item is not None
    assert item["points_threshold"] is None
    assert item["stage"] == "preview"


def test_cross_source_dedup_and_condition_change():
    state = {}
    ingest_posts(state, [post()], now=NOW)
    event = next(iter(state["events"].values()))
    assert len(event["notices"]) == 1
    ingest_posts(state, [post(source="cms", ident="abcd", created=NOW+timedelta(seconds=5))], now=NOW)
    assert len(event["notices"]) == 1
    assert len(event["sources"]) == 2
    ingest_posts(state, [post(DETAIL.replace("223", "218"), created=NOW+timedelta(minutes=1))], now=NOW+timedelta(minutes=1))
    assert len(event["notices"]) == 2
    assert event["points_threshold"] == 218
    # Re-reading the old Telegram post must not revert the newer conditions.
    ingest_posts(state, [post()], now=NOW+timedelta(minutes=2))
    assert event["points_threshold"] == 218
    assert len(event["notices"]) == 2


def test_same_post_edit_is_detected_even_if_publication_time_unchanged():
    state = {}
    ingest_posts(state, [post()], now=NOW)
    ingest_posts(state, [post(DETAIL.replace("223", "218"))], now=NOW+timedelta(seconds=30))
    assert next(iter(state["events"].values()))["points_threshold"] == 218


def test_old_bootstrap_posts_are_visible_but_do_not_alert():
    state = {}
    ingest_posts(state, [post()], now=NOW+timedelta(hours=6))
    event = next(iter(state["events"].values()))
    assert event["notices"][0]["suppressed"] is True


def test_preview_then_details_then_opening_only_once():
    state = {}
    ingest_posts(state, [post("Binance Alpha will feature Concrete (CT) on September 30. "
                               "An airdrop is coming; further details soon.")], now=NOW)
    ingest_posts(state, [post(created=NOW+timedelta(seconds=30))], now=NOW+timedelta(seconds=30))
    ingest_posts(state, [post(created=NOW+timedelta(seconds=30))], now=NOW+timedelta(seconds=60))
    opening = datetime(2026, 9, 30, 8, 0, tzinfo=UTC)
    ingest_posts(state, [], now=opening)
    ingest_posts(state, [], now=opening+timedelta(seconds=30))
    event = next(iter(state["events"].values()))
    assert [n["kind"] for n in event["notices"]] == ["preview", "details", "opening"]


def test_future_date_is_used_as_event_date_not_publication_date():
    item = normalize_post(post(DETAIL.replace("September 30", "October 1")), now=NOW)
    assert item["event_id"].startswith("2026-10-01:")


def test_unnamed_same_time_is_linked_to_named_announcement():
    state = {}
    ingest_posts(state, [post("Please get ready to claim the Binance Alpha airdrop today at "
                               "08:00 (UTC). Users with at least 223 Binance Alpha Points can claim.")], now=NOW)
    ingest_posts(state, [post(created=NOW+timedelta(seconds=30))], now=NOW+timedelta(seconds=30))
    assert len(state["events"]) == 1
    assert next(iter(state["events"].values()))["symbol"] == "CT"


def test_notification_failure_retries_and_success_deduplicates():
    state = {}
    ingest_posts(state, [post()], now=NOW)
    with patch("grid_optimizer.alpha_airdrop_feed.load_bark_config", return_value={"enabled": True}), \
         patch("grid_optimizer.alpha_airdrop_feed.send_bark_notification", side_effect=[{"sent": False}, {"sent": True}]) as bark, \
         patch("grid_optimizer.alpha_airdrop_feed.send_alert_email", return_value={"sent": False, "error": "email_disabled"}):
        deliver_notifications(state, now=NOW)
        deliver_notifications(state, now=NOW+timedelta(seconds=30))
        deliver_notifications(state, now=NOW+timedelta(seconds=60))
    assert bark.call_count == 2
    assert next(iter(state["events"].values()))["notices"][0]["bark_sent_at"]


def test_telegram_notifies_before_slow_fallback_and_source_failure_isolated():
    order = []
    def tg():
        return [post()]
    def cms():
        order.append("cms")
        raise RuntimeError("network")
    def deliver(state, **kwargs):
        order.append("notify")
    with TemporaryDirectory() as d, \
         patch("grid_optimizer.alpha_airdrop_feed.fetch_telegram_posts", side_effect=tg), \
         patch("grid_optimizer.alpha_airdrop_feed.fetch_cms_posts", side_effect=cms), \
         patch("grid_optimizer.alpha_airdrop_feed.deliver_notifications", side_effect=deliver):
        result = check_airdrop_feed(now=NOW, state_path=Path(d)/"feed.json")
    assert order.index("notify") < order.index("cms")
    assert result["sources"]["telegram"]["ok"] is True
    assert result["sources"]["cms"]["ok"] is False
    assert len(result["events"]) == 1


def test_feed_payload_is_read_only_and_never_returns_notifier_secrets():
    with TemporaryDirectory() as d:
        result = feed_payload(Path(d)/"missing.json", now=NOW)
        assert result["events"] == []
        assert result["stale"] is True


def test_installer_uses_24h_fast_multi_source_service():
    script = Path("deploy/oracle/install_alpha_airdrop_monitor.sh").read_text()
    assert "grid_optimizer.alpha_airdrop_feed" in script
    assert "OnUnitInactiveSec=30s" in script
    assert "OnCalendar=" not in script
    assert "--bark-config-path" in script
    assert "--state-path ${STATE_PATH}" in script


def test_same_symbol_two_start_times_are_not_combined():
    state = {}
    ingest_posts(state, [post()], now=NOW)
    ingest_posts(state, [post(DETAIL.replace("08:00", "10:00"), ident="1815")], now=NOW)
    assert len(state["events"]) == 2


def test_repeated_old_copy_does_not_undo_same_timestamp_edit():
    state = {}
    ingest_posts(state, [post(), post(source="cms", ident="abcd")], now=NOW)
    ingest_posts(state, [post(DETAIL.replace("223", "218")), post(source="cms", ident="abcd")], now=NOW)
    assert next(iter(state["events"].values()))["points_threshold"] == 218


def test_chinese_conditions_extract_and_dedup_with_english():
    cn = "币安 Alpha 将于今天 16:00（UTC+8）开放 Concrete（CT）空投。持有至少 223 个币安 Alpha 积分的用户可以领取 200 CT 代币空投。领取将消耗 15 个 Alpha 积分。奖池未领完时每5分钟门槛降低5分。"
    item = normalize_post(post(cn, source="cms"), now=NOW)
    assert item["quantity"] == "200 CT"
    assert item["points_cost"] == 15
    assert item["threshold_drop"] == "5/5"
    state = {}
    ingest_posts(state, [post(), post(cn, source="cms", ident="cn")], now=NOW)
    assert len(next(iter(state["events"].values()))["notices"]) == 1


def test_rate_limit_retry_after_is_honored_without_stalling_other_source():
    import requests
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "900"
    with TemporaryDirectory() as d, \
         patch("grid_optimizer.alpha_airdrop_feed.fetch_telegram_posts", side_effect=requests.HTTPError(response=response)) as tg, \
         patch("grid_optimizer.alpha_airdrop_feed.fetch_cms_posts", return_value=[]):
        path = Path(d)/"state.json"
        check_airdrop_feed(now=NOW, state_path=path, notify=False)
        result = check_airdrop_feed(now=NOW+timedelta(seconds=30), state_path=path, notify=False)
    assert tg.call_count == 1
    assert result["sources"]["cms"]["ok"] is True
    assert result["sources"]["telegram"]["retry_at"] == (NOW+timedelta(seconds=900)).isoformat()


def test_cms_list_detail_content_and_duplicate_catalogs():
    def get(url, *, params, timeout):
        if "/detail/" in url:
            data = {"body": json.dumps({"node": "text", "text": DETAIL})}
        else:
            data = {"articles": [{"code": "abc", "title": "Concrete (CT) Binance Alpha Airdrop", "releaseDate": int(NOW.timestamp()*1000)}]}
        return Mock(json=lambda: {"code": "000000", "data": data}, raise_for_status=lambda: None)
    with patch("grid_optimizer.alpha_airdrop_feed.requests.get", side_effect=get):
        posts = fetch_cms_posts()
    assert len(posts) == 1
    assert normalize_post(posts[0], now=NOW)["points_threshold"] == 223


def test_airdrop_page_and_api_remain_authenticated_routes():
    from grid_optimizer.web import _Handler
    for path in ("/alpha-airdrops", "/api/alpha-airdrops"):
        handler = _Handler.__new__(_Handler)
        handler.path = path
        handler._authorize_request = Mock(return_value=False)
        handler._send_json = Mock()
        handler._send_html = Mock()
        handler.do_GET()
        handler._send_json.assert_not_called()
        handler._send_html.assert_not_called()
        handler._authorize_request.return_value = True
        handler.do_GET()
        assert handler._send_json.called if "/api/" in path else handler._send_html.called


def test_bark_http_success_with_rejected_payload_is_not_counted_as_sent():
    from grid_optimizer.alpha_airdrop_monitor import send_bark_notification
    with patch("grid_optimizer.alpha_airdrop_monitor.requests.post", return_value=Mock(json=lambda: {"code": 400}, raise_for_status=lambda: None)):
        result = send_bark_notification(bark_endpoint_or_key="test-only-key", post={})
    assert result["sent"] is False
