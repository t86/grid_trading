from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from .alpha_competition_metrics import (
    CompetitionRound, CompetitionRule, calculate_thresholds,
    decode_competition_rule, encode_competition_rule,
)


DEFAULT_HISTORY_PATH = Path("/home/ubuntu/.local/share/binance-alpha-volume-alert/competition_history.json")
_SYMBOL_RE = re.compile(r"[A-Z0-9_]{1,32}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("time must include a timezone")
    return value.astimezone(timezone.utc)


def _parse_utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("endUtc must be an ISO timestamp")
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        raise ValueError("endUtc must be an ISO timestamp") from None


def _positive_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number")
    number = float(value)
    if not math.isfinite(number) or number <= 0 or number > 1_000_000_000_000:
        raise ValueError(f"{name} must be a positive number")
    return number


def _article_code(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in {"binance.com", "www.binance.com"}:
        raise ValueError("articleUrl must be an HTTPS Binance announcement")
    code = parsed.path.rstrip("/").split("/")[-1]
    if not re.fullmatch(r"[A-Za-z0-9_-]{3,100}", code):
        raise ValueError("articleUrl has no announcement code")
    return code


class CompetitionHistoryStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def _load(self) -> dict[str, dict[str, Any]]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            raise ValueError("competition history is unreadable") from exc
        if not isinstance(payload, dict) or payload.get("version") != 1 or not isinstance(payload.get("rows"), dict):
            raise ValueError("competition history has an invalid format")
        rows = payload["rows"]
        if any(not isinstance(key, str) or not isinstance(row, dict) for key, row in rows.items()):
            raise ValueError("competition history has an invalid row")
        return rows

    def _save(self, rows: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=self.path.parent,
                prefix=f".{self.path.name}.", suffix=".tmp", delete=False,
            ) as handle:
                temporary = Path(handle.name)
                json.dump({"version": 1, "rows": rows}, handle, ensure_ascii=False, allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def archive_rules(self, rules: list[CompetitionRule], *, now: datetime) -> None:
        _utc(now)
        with self._lock:
            rows = self._load()
            changed = False
            for rule in rules:
                for round_ in rule.rounds:
                    identity = f"{rule.article_code}:{round_.number}"
                    metadata = {
                        "id": identity,
                        "articleCode": rule.article_code,
                        "symbol": rule.symbol,
                        "name": rule.name,
                        "round": round_.number,
                        "startUtc": _utc(round_.start_utc).isoformat(),
                        "endUtc": _utc(round_.end_utc).isoformat(),
                        "winnerCount": rule.winner_count,
                        "articleUrl": rule.article_url,
                        "source": "official_rule",
                        "rule": encode_competition_rule(rule),
                    }
                    existing = rows.get(identity, {})
                    merged = {**existing, **metadata}
                    if not existing:
                        merged.update(finalThreshold=None, note="", lastObservation=None)
                    if merged != existing:
                        rows[identity] = merged
                        changed = True
            if changed:
                self._save(rows)

    def capture_rows(self, metrics_rows: list[dict[str, Any]], *, now: datetime) -> None:
        observed_at = _utc(now)
        with self._lock:
            rows = self._load()
            changed = False
            for metric in metrics_rows:
                if metric.get("status") != "active":
                    continue
                try:
                    identity = f"{_article_code(str(metric.get('articleUrl') or ''))}:{int(metric.get('round'))}"
                except (TypeError, ValueError):
                    continue
                record = rows.get(identity)
                if record is None:
                    continue
                values = {
                    key: metric.get(key)
                    for key in ("weightedVolume", "referenceThreshold", "safeThreshold", "leaderboardThreshold")
                    if isinstance(metric.get(key), (int, float))
                    and not isinstance(metric.get(key), bool)
                    and math.isfinite(metric[key])
                }
                if not values:
                    continue
                previous = record.get("lastObservation") or {}
                if previous.get("observedAtUtc"):
                    age = observed_at - _parse_utc(previous["observedAtUtc"])
                    if age.total_seconds() < 60:
                        continue
                record["lastObservation"] = {
                    **values,
                    "volumeSource": metric.get("volumeSource"),
                    "observedAtUtc": observed_at.isoformat(),
                }
                changed = True
            if changed:
                self._save(rows)

    def snapshot(self, *, now: datetime) -> dict[str, Any]:
        current = _utc(now)
        with self._lock:
            rows = [
                {key: value for key, value in row.items() if key != "rule"}
                for row in self._load().values()
                if _parse_utc(row.get("endUtc")) <= current
            ]
        rows.sort(key=lambda row: (row["endUtc"], row["symbol"], row["round"]), reverse=True)
        return {"generatedAtUtc": current.isoformat(timespec="seconds"), "rows": rows}

    def pending_reference(self, *, now: datetime) -> tuple[str, CompetitionRule, CompetitionRound] | None:
        current = _utc(now)
        with self._lock:
            for row in sorted(self._load().values(), key=lambda item: item["endUtc"]):
                if _parse_utc(row["endUtc"]) > current or row.get("referenceThreshold") is not None:
                    continue
                if row.get("referenceRetryAtUtc") and _parse_utc(row["referenceRetryAtUtc"]) > current:
                    continue
                try:
                    rule = decode_competition_rule(row.get("rule"))
                    round_ = next(round_ for round_ in rule.rounds if round_.number == row["round"])
                except (ValueError, TypeError, StopIteration):
                    continue
                return row["id"], rule, round_
        return None

    @staticmethod
    def _set_reference(row: dict[str, Any], weighted_volume: float, source: str, now: datetime) -> None:
        thresholds = calculate_thresholds(weighted_volume=weighted_volume, winner_count=row["winnerCount"])
        row.update(
            finalWeightedVolume=weighted_volume,
            referenceThreshold=thresholds.reference,
            referenceSource=source,
            referenceCalculatedAtUtc=_utc(now).isoformat(),
        )
        row.pop("referenceError", None)
        row.pop("referenceRetryAtUtc", None)

    def save_reference(self, identity: str, *, weighted_volume: float, source: str, now: datetime) -> None:
        with self._lock:
            rows = self._load()
            row = rows.get(identity)
            if row is None or _parse_utc(row["endUtc"]) > _utc(now):
                raise ValueError("ended competition round is required")
            self._set_reference(row, weighted_volume, source, now)
            self._save(rows)

    def mark_reference_unavailable(self, identity: str, *, now: datetime) -> None:
        with self._lock:
            rows = self._load()
            if identity not in rows:
                return
            rows[identity]["referenceError"] = "完整轮次 K 线暂不可用"
            rows[identity]["referenceRetryAtUtc"] = (_utc(now) + timedelta(minutes=10)).isoformat()
            self._save(rows)

    def save_final(self, payload: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("history entry must be an object")
        current = _utc(now)
        threshold = _positive_number(payload.get("finalThreshold"), "finalThreshold")
        with self._lock:
            rows = self._load()
            identity = payload.get("id")
            if identity is not None:
                if not isinstance(identity, str) or identity not in rows:
                    raise ValueError("unknown history entry")
                row = dict(rows[identity])
            else:
                symbol = str(payload.get("symbol") or "").strip().upper()
                if _SYMBOL_RE.fullmatch(symbol) is None:
                    raise ValueError("symbol is invalid")
                round_ = payload.get("round")
                if isinstance(round_, bool) or not isinstance(round_, int) or not 1 <= round_ <= 20:
                    raise ValueError("round must be between 1 and 20")
                end = _parse_utc(payload.get("endUtc"))
                winner_count = payload.get("winnerCount")
                if isinstance(winner_count, bool) or not isinstance(winner_count, int) or winner_count <= 0:
                    raise ValueError("winnerCount must be positive")
                article_url = str(payload.get("articleUrl") or "").strip()
                code = _article_code(article_url)
                identity = f"{code}:{round_}" if code else f"manual:{symbol}:{end.date().isoformat()}:{round_}"
                if identity in rows:
                    raise ValueError("history entry already exists; edit the existing row")
                name = str(payload.get("name") or symbol).strip()
                if not name or len(name) > 100:
                    raise ValueError("name is invalid")
                row = {
                    "id": identity,
                    "articleCode": code or None,
                    "symbol": symbol,
                    "name": name,
                    "round": round_,
                    "startUtc": None,
                    "endUtc": end.isoformat(),
                    "winnerCount": winner_count,
                    "articleUrl": article_url or None,
                    "source": "manual",
                    "lastObservation": None,
                }
            if _parse_utc(row["endUtc"]) > current:
                raise ValueError("competition round has not ended")
            note = str(payload.get("note", row.get("note") or "")).strip()
            if len(note) > 500:
                raise ValueError("note is too long")
            row.update(finalThreshold=threshold, note=note, updatedAtUtc=current.isoformat())
            if "rewardValueU" in payload:
                value = payload["rewardValueU"]
                row["rewardValueU"] = None if value is None else _positive_number(value, "rewardValueU")
            if payload.get("weightedVolume") is not None:
                volume = _positive_number(payload["weightedVolume"], "weightedVolume")
                self._set_reference(row, volume, "manual_volume", current)
            rows[identity] = row
            self._save(rows)
            return dict(row)

    def delete(self, identity: str) -> None:
        with self._lock:
            rows = self._load()
            row = rows.get(identity)
            if row is None:
                raise ValueError("unknown history entry")
            if row.get("source") == "manual":
                del rows[identity]
            else:
                row["finalThreshold"] = None
                row["rewardValueU"] = None
                row["note"] = ""
                row.pop("updatedAtUtc", None)
            self._save(rows)
