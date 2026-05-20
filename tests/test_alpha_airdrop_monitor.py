from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.alpha_airdrop_monitor import (
    AccountCheckResult,
    _build_alert_headline,
    _build_bark_title,
    _extract_bark_key,
    _build_email_body,
    _build_match_key,
    _extract_entries_from_syndication_html,
    _extract_entries_from_nitter_rss,
    _extract_time_hint_text,
    _is_today_in_tz,
    _load_state,
    load_bark_config,
    _match_alpha_airdrop_post,
    _select_notification_candidates,
    _update_state_for_candidates,
    check_alpha_airdrop_posts,
    send_bark_notification,
)


class AlphaAirdropMonitorTests(unittest.TestCase):
    def test_extract_entries_from_syndication_html_reads_next_data(self) -> None:
        payload = {
            "props": {
                "pageProps": {
                    "timeline": {
                        "entries": [
                            {
                                "type": "tweet",
                                "entry_id": "tweet-1",
                                "content": {
                                    "tweet": {
                                        "id_str": "1",
                                        "created_at": "Thu May 15 01:00:00 +0000 2026",
                                        "full_text": "Alpha Points 225 claim now",
                                    }
                                },
                            }
                        ]
                    }
                }
            }
        }
        html = (
            '<html><body><script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(payload, ensure_ascii=False)
            + "</script></body></html>"
        )

        entries = _extract_entries_from_syndication_html(html)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["entry_id"], "tweet-1")

    def test_extract_entries_from_nitter_rss_reads_zest_airdrop(self) -> None:
        rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Binance Alpha will be the first platform to feature Zest Protocol (ZEST) on May 19.

Eligible users can claim their airdrop using Binance Alpha Points on the Alpha Events page once trading opens.</title>
      <link>https://nitter.net/BinanceWallet/status/2056730000000000000#m</link>
      <pubDate>Mon, 18 May 2026 11:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

        entries = _extract_entries_from_nitter_rss(rss)
        tweet = entries[0]["content"]["tweet"]
        matched = _match_alpha_airdrop_post(
            tweet,
            now=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc),
            tz_offset_hours=8,
        )

        self.assertEqual(tweet["id_str"], "2056730000000000000")
        self.assertIsNotNone(matched)
        self.assertIn("Zest Protocol", matched["text"])

    def test_match_alpha_airdrop_post_requires_points_threshold(self) -> None:
        now = datetime(2026, 5, 15, 6, 0, tzinfo=timezone.utc)
        matched = _match_alpha_airdrop_post(
            {
                "id_str": "100",
                "created_at": "Thu May 15 01:00:00 +0000 2026",
                "full_text": (
                    "Please get ready to claim the Binance Alpha airdrop and trade today at 9:00 (UTC). "
                    "Users with at least 225 Binance Alpha Points can claim the token."
                ),
            },
            now=now,
            tz_offset_hours=8,
        )
        not_matched = _match_alpha_airdrop_post(
            {
                "id_str": "101",
                "created_at": "Thu May 15 01:00:00 +0000 2026",
                "full_text": "Binance Alpha token news today, trade opens soon.",
            },
            now=now,
            tz_offset_hours=8,
        )

        self.assertIsNotNone(matched)
        self.assertEqual(matched["points_threshold"], 225)
        self.assertIsNone(not_matched)

    def test_match_alpha_airdrop_post_supports_chinese_points_threshold(self) -> None:
        now = datetime(2026, 5, 15, 6, 0, tzinfo=timezone.utc)

        matched = _match_alpha_airdrop_post(
            {
                "id_str": "102",
                "created_at": "Thu May 15 07:30:00 +0000 2026",
                "full_text": (
                    "请大家准备今天 17:00（UTC+8）领取币安 Alpha 空投并交易！"
                    "持有至少 225 个币安 Alpha 积分的用户可申领代币空投。"
                ),
            },
            now=now,
            tz_offset_hours=8,
        )

        self.assertIsNotNone(matched)
        self.assertEqual(matched["points_threshold"], 225)
        self.assertEqual(matched["time_hint_text"], "17:00（UTC+8）")

    def test_match_alpha_airdrop_post_allows_future_schedule_from_previous_day(self) -> None:
        now = datetime(2026, 5, 15, 6, 0, tzinfo=timezone.utc)

        matched = _match_alpha_airdrop_post(
            {
                "id_str": "103",
                "created_at": "Thu May 14 07:30:00 +0000 2026",
                "full_text": (
                    "请大家准备明天 17:00（UTC+8）领取币安 Alpha 空投并交易！"
                    "持有至少 225 个币安 Alpha 积分的用户可申领代币空投。"
                ),
            },
            now=now,
            tz_offset_hours=8,
        )

        self.assertIsNotNone(matched)
        self.assertEqual(matched["points_threshold"], 225)
        self.assertEqual(matched["time_hint_text"], "17:00（UTC+8）")

    def test_match_alpha_airdrop_post_allows_future_eligible_airdrop_without_threshold(self) -> None:
        now = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

        matched = _match_alpha_airdrop_post(
            {
                "id_str": "104",
                "created_at": "Sun May 17 11:00:00 +0000 2026",
                "full_text": (
                    "Binance Alpha will be the first platform to feature Zest Protocol (ZEST) on May 19. "
                    "Eligible users can claim their airdrop using Binance Alpha Points on the Alpha Events page "
                    "once trading opens. Further details will be announced soon."
                ),
            },
            now=now,
            tz_offset_hours=8,
        )

        self.assertIsNotNone(matched)
        self.assertIsNone(matched["points_threshold"])

    def test_extract_time_hint_text_prefers_explicit_schedule(self) -> None:
        self.assertEqual(
            _extract_time_hint_text("Please get ready to claim today at 9:00 (UTC)."),
            "9:00 (UTC)",
        )
        self.assertEqual(
            _extract_time_hint_text("请大家准备今天 17:00（UTC+8）领取币安 Alpha 空投并交易！"),
            "17:00（UTC+8）",
        )

    def test_is_today_in_tz_uses_utc_plus_8_day_boundary(self) -> None:
        now = datetime(2026, 5, 15, 1, 0, tzinfo=timezone.utc)
        same_day_utc = datetime(2026, 5, 14, 16, 30, tzinfo=timezone.utc)
        previous_day_utc = datetime(2026, 5, 14, 15, 30, tzinfo=timezone.utc)

        self.assertTrue(_is_today_in_tz(same_day_utc, now=now, tz_offset_hours=8))
        self.assertFalse(_is_today_in_tz(previous_day_utc, now=now, tz_offset_hours=8))

    def test_select_notification_candidates_limits_same_post_to_three_sends(self) -> None:
        post = {
            "account": "binancezh",
            "tweet_id": "100",
            "created_at": "2026-05-15T01:00:00+00:00",
            "text": "Alpha Points 225 claim now trade today",
            "points_threshold": 225,
            "tweet_url": "https://x.com/binancezh/status/100",
        }
        state = {
            _build_match_key(post): {
                "notification_count": 2,
                "last_notified_at": "2026-05-15T06:00:00+00:00",
            }
        }

        candidates = _select_notification_candidates([post], state, now=datetime(2026, 5, 15, 6, 10, tzinfo=timezone.utc))
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["notification_sequence"], 3)

        updated = _update_state_for_candidates(state, candidates, now=datetime(2026, 5, 15, 6, 10, tzinfo=timezone.utc))
        self.assertEqual(updated[_build_match_key(post)]["notification_count"], 3)

        candidates_after_limit = _select_notification_candidates([post], updated, now=datetime(2026, 5, 15, 6, 20, tzinfo=timezone.utc))
        self.assertEqual(candidates_after_limit, [])

    def test_build_email_body_contains_summary(self) -> None:
        post = {
            "account": "BinanceWallet",
            "tweet_id": "200",
            "created_at": "2026-05-15T01:00:00+00:00",
            "text": "Users with at least 225 Binance Alpha Points can claim the token on a first-come basis.",
            "points_threshold": 225,
            "tweet_url": "https://x.com/BinanceWallet/status/200",
            "notification_sequence": 1,
        }

        body = _build_email_body(post)

        self.assertIn("Binance Alpha 空投监控命中", body)
        self.assertIn("BinanceWallet", body)
        self.assertIn("225", body)
        self.assertIn("第 1/3 次提醒", body)
        self.assertIn("https://x.com/BinanceWallet/status/200", body)

    def test_build_alert_headline_uses_points_bucket_and_time(self) -> None:
        self.assertEqual(
            _build_alert_headline({"points_threshold": 225, "time_hint_text": "18:00（UTC+8）"}),
            "！！！空投 2xx 18:00",
        )
        self.assertEqual(
            _build_alert_headline({"points_threshold": 95}),
            "！！！空投 95",
        )

    def test_extract_bark_key_supports_full_url_or_plain_key(self) -> None:
        self.assertEqual(_extract_bark_key("Ui2sPsKqsS4uvJsYP6tvwm"), "Ui2sPsKqsS4uvJsYP6tvwm")
        self.assertEqual(
            _extract_bark_key("https://api.day.app/Ui2sPsKqsS4uvJsYP6tvwm/"),
            "Ui2sPsKqsS4uvJsYP6tvwm",
        )

    def test_load_bark_config_prefers_file_and_env(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "bark.json"
            config_path.write_text(
                json.dumps({"bark_endpoint": "https://api.day.app/testkey/", "bark_base_url": "https://api.day.app"}),
                encoding="utf-8",
            )

            config = load_bark_config(config_path)

            self.assertTrue(config["enabled"])
            self.assertEqual(config["bark_endpoint"], "https://api.day.app/testkey/")

    def test_send_bark_notification_posts_json_payload(self) -> None:
        post = {
            "account": "binancezh",
            "tweet_id": "200",
            "created_at": "2026-05-15T01:00:00+00:00",
            "text": "Users with at least 225 Binance Alpha Points can claim the token.",
            "points_threshold": 225,
            "tweet_url": "https://x.com/binancezh/status/200",
            "notification_sequence": 1,
            "time_hint_text": "17:00（UTC+8）",
        }
        captured: dict[str, object] = {}

        class DummyResponse:
            def raise_for_status(self) -> None:
                return None

        def fake_post(url, json=None, timeout=None):
            captured["url"] = url
            captured["json"] = json
            captured["timeout"] = timeout
            return DummyResponse()

        with patch("grid_optimizer.alpha_airdrop_monitor.requests.post", side_effect=fake_post):
            result = send_bark_notification(
                bark_endpoint_or_key="https://api.day.app/Ui2sPsKqsS4uvJsYP6tvwm/",
                post=post,
            )

        self.assertTrue(result["sent"])
        self.assertEqual(captured["url"], "https://api.day.app/Ui2sPsKqsS4uvJsYP6tvwm")
        self.assertEqual(captured["json"]["url"], "https://x.com/binancezh/status/200")
        self.assertEqual(captured["json"]["title"], "！！！空投 2xx 17:00")

    def test_build_bark_title_uses_time_hint_when_available(self) -> None:
        self.assertEqual(
            _build_bark_title({"account": "binancezh", "points_threshold": 225, "time_hint_text": "17:00（UTC+8）"}),
            "！！！空投 2xx 17:00",
        )
        self.assertEqual(
            _build_bark_title({"account": "binancezh", "points_threshold": 225}),
            "！！！空投 2xx",
        )

    def test_load_state_ignores_invalid_json(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{bad json", encoding="utf-8")

            state = _load_state(state_path)

            self.assertEqual(state, {})

    def test_check_alpha_airdrop_posts_sends_email_and_updates_state(self) -> None:
        html = (
            '<script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(
                {
                    "props": {
                        "pageProps": {
                            "timeline": {
                                "entries": [
                                    {
                                        "type": "tweet",
                                        "entry_id": "tweet-300",
                                        "content": {
                                            "tweet": {
                                                "id_str": "300",
                                                "created_at": "Thu May 15 01:00:00 +0000 2026",
                                                "full_text": (
                                                    "Please get ready to claim the Binance Alpha airdrop today. "
                                                    "Users with at least 225 Binance Alpha Points can claim the token and trade."
                                                ),
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }
                },
                ensure_ascii=False,
            )
            + "</script>"
        )

        class DummyResponse:
            def __init__(self, text: str) -> None:
                self.text = text
                self.status_code = 200

            def raise_for_status(self) -> None:
                return None

        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "alpha_state.json"
            bark_path = Path(tmpdir) / "bark.json"
            bark_path.write_text(
                json.dumps({"bark_endpoint": "https://api.day.app/Ui2sPsKqsS4uvJsYP6tvwm/"}, ensure_ascii=False),
                encoding="utf-8",
            )
            now = datetime(2026, 5, 15, 6, 0, tzinfo=timezone.utc)
            sent_subjects: list[str] = []
            bark_calls: list[str] = []

            def fake_get(*args, **kwargs):
                return DummyResponse(html)

            def fake_send_alert_email(*, subject: str, body: str, config_path=None):
                sent_subjects.append(subject)
                return {"sent": True, "subject": subject, "body": body}

            class DummyBarkResponse:
                def raise_for_status(self) -> None:
                    return None

            def fake_post(url, json=None, timeout=None):
                bark_calls.append(url)
                return DummyBarkResponse()

            with (
                patch("grid_optimizer.alpha_airdrop_monitor.requests.get", side_effect=fake_get),
                patch("grid_optimizer.alpha_airdrop_monitor.requests.post", side_effect=fake_post),
                patch("grid_optimizer.alpha_airdrop_monitor.send_alert_email", side_effect=fake_send_alert_email),
            ):
                result = check_alpha_airdrop_posts(now=now, state_path=state_path, bark_config_path=bark_path)

            self.assertEqual(len(sent_subjects), 2)
            self.assertEqual(len(bark_calls), 2)
            self.assertEqual(result["emails_sent"], 2)
            self.assertEqual(result["bark_sent"], 2)
            self.assertEqual(len(result["matches"]), 2)
            saved_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_state["binancezh:300"]["notification_count"], 1)
            self.assertEqual(saved_state["BinanceWallet:300"]["notification_count"], 1)

    def test_check_alpha_airdrop_posts_uses_nitter_rss_when_syndication_is_rate_limited(self) -> None:
        rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Binance Alpha will be the first platform to feature Zest Protocol (ZEST) on May 19.

Eligible users can claim their airdrop using Binance Alpha Points on the Alpha Events page once trading opens.</title>
      <link>https://nitter.net/BinanceWallet/status/2056730000000000000#m</link>
      <pubDate>Mon, 18 May 2026 11:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

        class DummyResponse:
            def __init__(self, text: str, status_code: int = 200) -> None:
                self.text = text
                self.status_code = status_code

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP {self.status_code}")

        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "alpha_state.json"
            now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
            sent_subjects: list[str] = []

            def fake_get(url, *args, **kwargs):
                if "syndication.twitter.com" in str(url):
                    return DummyResponse("", status_code=429)
                return DummyResponse(rss)

            def fake_send_alert_email(*, subject: str, body: str, config_path=None):
                sent_subjects.append(subject)
                return {"sent": True, "subject": subject, "body": body}

            with (
                patch("grid_optimizer.alpha_airdrop_monitor.requests.get", side_effect=fake_get),
                patch("grid_optimizer.alpha_airdrop_monitor.requests.post"),
                patch("grid_optimizer.alpha_airdrop_monitor.send_alert_email", side_effect=fake_send_alert_email),
            ):
                result = check_alpha_airdrop_posts(
                    accounts=("BinanceWallet",),
                    now=now,
                    state_path=state_path,
                    bark_config_path=Path(tmpdir) / "missing_bark.json",
                )

        self.assertEqual(result["emails_sent"], 1)
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(sent_subjects, ["！！！空投 xxx"])
        self.assertIn("BinanceWallet", result["matches"][0]["account"])

    def test_check_alpha_airdrop_posts_records_fetch_errors(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "alpha_state.json"

            def fake_get(*args, **kwargs):
                raise RuntimeError("network down")

            with patch("grid_optimizer.alpha_airdrop_monitor.requests.get", side_effect=fake_get):
                result = check_alpha_airdrop_posts(
                    now=datetime(2026, 5, 15, 6, 0, tzinfo=timezone.utc),
                    state_path=state_path,
                )

            self.assertEqual(len(result["errors"]), 2)
            self.assertEqual(result["emails_sent"], 0)

    def test_account_check_result_shape_stays_simple(self) -> None:
        result = AccountCheckResult(
            account="binancezh",
            fetched=True,
            matches=[],
            error=None,
        )

        self.assertEqual(result.account, "binancezh")


if __name__ == "__main__":
    unittest.main()
