from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from .alpha_market import AlphaMarketClient, AlphaToken, _safe_float, _safe_int


DEFAULT_SYMBOLS = ("ZEST", "QAIT", "O", "PRL", "CAP")
MARKET = AlphaMarketClient()


def fetch_tokens() -> dict[str, AlphaToken]:
    return MARKET.fetch_tokens()


def fetch_ticker(pair: str) -> dict[str, Any]:
    return MARKET.fetch_ticker(pair)


def fetch_klines(pair: str, interval: str, limit: int) -> list[list[Any]]:
    return MARKET.fetch_klines(pair, interval=interval, limit=limit)


@dataclass(frozen=True)
class VolumeSpike:
    token: AlphaToken
    close_time_ms: int
    latest_quote_volume: float
    quote_volume_1h: float
    baseline_quote_volume: float
    multiple: float
    latest_base_volume: float
    trades: int
    last_price: float
    ticker_quote_volume_24h: float
    trigger_reason: str


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def find_spike(
    token: AlphaToken,
    *,
    interval: str,
    baseline_candles: int,
    spike_multiple: float,
    min_quote_volume: float,
    absolute_min_quote_volume: float,
) -> VolumeSpike | None:
    klines = fetch_klines(token.pair, interval, max(baseline_candles, 60) + 3)
    if len(klines) < baseline_candles + 1:
        return None

    latest = klines[-2] if len(klines) >= 2 else klines[-1]
    closed_rows = klines[:-1] if len(klines) >= 2 else klines
    quote_volume_1h = sum(_safe_float(row[7]) for row in closed_rows[-60:])
    baseline_rows = klines[-(baseline_candles + 2):-2]
    baseline_values = [_safe_float(row[7]) for row in baseline_rows if _safe_float(row[7]) > 0]
    if not baseline_values:
        baseline_values = [_safe_float(row[7]) for row in baseline_rows]
    baseline = sum(baseline_values) / max(1, len(baseline_values))
    latest_quote_volume = _safe_float(latest[7])
    if baseline <= 0:
        multiple = float("inf") if latest_quote_volume > 0 else 0.0
    else:
        multiple = latest_quote_volume / baseline

    trigger_reason = ""
    if absolute_min_quote_volume > 0:
        if latest_quote_volume >= absolute_min_quote_volume:
            trigger_reason = f"latest 1m quote volume >= {absolute_min_quote_volume:,.2f} USDT"
        else:
            return None
    else:
        if latest_quote_volume >= min_quote_volume and multiple >= spike_multiple:
            trigger_reason = (
                f"latest 1m quote volume >= {min_quote_volume:,.2f} USDT "
                f"and multiple >= {spike_multiple:.2f}x"
            )
        else:
            return None

    ticker = fetch_ticker(token.pair)
    return VolumeSpike(
        token=token,
        close_time_ms=_safe_int(latest[6]),
        latest_quote_volume=latest_quote_volume,
        quote_volume_1h=quote_volume_1h,
        baseline_quote_volume=baseline,
        multiple=multiple,
        latest_base_volume=_safe_float(latest[5]),
        trades=_safe_int(latest[8]),
        last_price=_safe_float(ticker.get("lastPrice"), token.price),
        ticker_quote_volume_24h=_safe_float(ticker.get("quoteVolume"), token.volume_24h),
        trigger_reason=trigger_reason,
    )


def format_spike(spike: VolumeSpike) -> str:
    t = spike.token
    mult = "inf" if spike.multiple == float("inf") else f"{spike.multiple:.2f}x"
    return (
        f"{t.symbol} ({t.name}, {t.alpha_id})\n"
        f"pair: {t.pair}\n"
        f"trigger: {spike.trigger_reason}\n"
        f"closed_utc: {_ms_to_iso(spike.close_time_ms)}\n"
        f"latest_1m_quote_volume: {spike.latest_quote_volume:,.2f} USDT\n"
        f"latest_1h_quote_volume: {spike.quote_volume_1h:,.2f} USDT\n"
        f"baseline_avg_quote_volume: {spike.baseline_quote_volume:,.2f} USDT\n"
        f"multiple: {mult}\n"
        f"latest_1m_base_volume: {spike.latest_base_volume:,.6f} {t.symbol}\n"
        f"trades: {spike.trades:,}\n"
        f"last_price: {spike.last_price:.10g}\n"
        f"ticker_quote_volume_24h: {spike.ticker_quote_volume_24h:,.2f} USDT"
    )


def send_email(subject: str, body: str) -> None:
    host = os.environ.get("SMTP_HOST")
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASSWORD")
    mail_from = os.environ.get("ALERT_EMAIL_FROM") or user
    mail_to = os.environ.get("ALERT_EMAIL_TO")
    port = int(os.environ.get("SMTP_PORT", "465"))

    missing = [
        name for name, value in {
            "SMTP_HOST": host,
            "SMTP_USER": user,
            "SMTP_PASSWORD": password,
            "ALERT_EMAIL_FROM/SMTP_USER": mail_from,
            "ALERT_EMAIL_TO": mail_to,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing email env vars: {', '.join(missing)}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg.set_content(body)

    if port == 465:
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=20) as smtp:
            smtp.login(user, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(user, password)
            smtp.send_message(msg)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Alert when Binance Alpha symbols get sudden volume spikes.")
    parser.add_argument("--symbols", default=os.environ.get("ALPHA_SYMBOLS", ",".join(DEFAULT_SYMBOLS)),
                        help="Comma-separated Alpha token symbols, for example ZEST,QAIT,O,PRL,CAP.")
    parser.add_argument("--interval", default=os.environ.get("ALPHA_INTERVAL", "1m"))
    parser.add_argument("--baseline-candles", type=int, default=int(os.environ.get("ALPHA_BASELINE_CANDLES", "20")))
    parser.add_argument("--spike-multiple", type=float, default=float(os.environ.get("ALPHA_SPIKE_MULTIPLE", "3")))
    parser.add_argument("--min-quote-volume", type=float, default=float(os.environ.get("ALPHA_MIN_QUOTE_VOLUME", "1000")))
    parser.add_argument("--absolute-min-quote-volume", type=float,
                        default=float(os.environ.get("ALPHA_ABSOLUTE_MIN_QUOTE_VOLUME", "60000")),
                        help="Trigger when the latest closed 1m quote volume reaches this USDT amount; set 0 to disable.")
    parser.add_argument("--cooldown-minutes", type=int, default=int(os.environ.get("ALPHA_COOLDOWN_MINUTES", "30")))
    parser.add_argument("--state-file", default=os.environ.get("ALPHA_STATE_FILE", "/tmp/binance_alpha_volume_alert_state.json"))
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen and do not send email.")
    return parser


def parse_args() -> argparse.Namespace:
    return _build_parser().parse_args()


def parse_args_from(args: list[str]) -> argparse.Namespace:
    return _build_parser().parse_args(args)


def run(args: argparse.Namespace) -> int:
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No symbols configured.")

    tokens = fetch_tokens()
    state_path = Path(args.state_file)
    state = load_state(state_path)
    sent = state.setdefault("sent", {})
    cooldown_ms = args.cooldown_minutes * 60 * 1000
    now_ms = int(time.time() * 1000)

    spikes: list[VolumeSpike] = []
    for symbol in symbols:
        token = tokens.get(symbol)
        if token is None:
            print(f"skip {symbol}: not found in Binance Alpha token list", file=sys.stderr)
            continue
        spike = find_spike(
            token,
            interval=args.interval,
            baseline_candles=args.baseline_candles,
            spike_multiple=args.spike_multiple,
            min_quote_volume=args.min_quote_volume,
            absolute_min_quote_volume=args.absolute_min_quote_volume,
        )
        if spike is None:
            continue
        last_sent_ms = _safe_int(sent.get(symbol, {}).get("sent_at_ms"))
        last_close_ms = _safe_int(sent.get(symbol, {}).get("close_time_ms"))
        if spike.close_time_ms == last_close_ms or now_ms - last_sent_ms < cooldown_ms:
            continue
        spikes.append(spike)

    if not spikes:
        print("No Alpha volume spikes detected.")
        save_state(state_path, state)
        return 0

    body = "\n\n".join(format_spike(spike) for spike in spikes)
    subject = (
        f"Binance Alpha volume spike 1m>={args.absolute_min_quote_volume:,.0f}: "
        + ", ".join(spike.token.symbol for spike in spikes)
    )
    print(body)
    if args.dry_run:
        print("\nDry run: email not sent.")
    else:
        send_email(subject, body)
        print(f"\nEmail sent: {subject}")

    for spike in spikes:
        sent[spike.token.symbol] = {
            "sent_at_ms": now_ms,
            "close_time_ms": spike.close_time_ms,
            "multiple": spike.multiple,
            "latest_quote_volume": spike.latest_quote_volume,
        }
    save_state(state_path, state)
    return 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
