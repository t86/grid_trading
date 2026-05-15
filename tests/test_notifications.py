from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.notifications import send_alert_email


class NotificationsTests(unittest.TestCase):
    def test_send_alert_email_filters_by_subject_regex(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "alert.json"
            status_path = Path(tmpdir) / "status.json"
            config_path.write_text(
                json.dumps(
                    {
                        "email_to": ["11655255@qq.com"],
                        "email_from": "11655255@qq.com",
                        "mode": "smtp",
                        "smtp_host": "smtp.qq.com",
                        "smtp_port": 587,
                        "smtp_username": "11655255@qq.com",
                        "smtp_password": "secret",
                        "subject_allow_regex": r"^！！！空投",
                        "status_path": str(status_path),
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch("grid_optimizer.notifications._send_via_smtp") as mock_send:
                result = send_alert_email(
                    subject="[grid][114] running_status_overview 整点总览",
                    body="noise",
                    config_path=config_path,
                )

            self.assertFalse(result["sent"])
            self.assertEqual(result["error"], "alert_email_filtered_by_subject_regex")
            mock_send.assert_not_called()

    def test_send_alert_email_allows_matching_subject_regex(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "alert.json"
            status_path = Path(tmpdir) / "status.json"
            config_path.write_text(
                json.dumps(
                    {
                        "email_to": ["11655255@qq.com"],
                        "email_from": "11655255@qq.com",
                        "mode": "smtp",
                        "smtp_host": "smtp.qq.com",
                        "smtp_port": 587,
                        "smtp_username": "11655255@qq.com",
                        "smtp_password": "secret",
                        "subject_allow_regex": r"^！！！空投",
                        "status_path": str(status_path),
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch("grid_optimizer.notifications._send_via_smtp") as mock_send:
                result = send_alert_email(
                    subject="！！！空投 2xx 18:00",
                    body="alpha",
                    config_path=config_path,
                )

            self.assertTrue(result["sent"])
            mock_send.assert_called_once()


if __name__ == "__main__":
    unittest.main()
