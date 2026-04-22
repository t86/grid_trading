from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone

from grid_optimizer.data import fetch_spot_user_trades
from grid_optimizer.spot_trade_summary import format_spot_trade_summary, summarize_spot_trades


def _fetch_remote_trades(ssh_host: str, symbol: str, start_ms: int, end_ms: int, limit: int) -> list[dict]:
    remote_py = f"""
import json, os
from grid_optimizer.data import fetch_spot_user_trades
trades = fetch_spot_user_trades(
    symbol={symbol!r},
    api_key=os.environ["BINANCE_API_KEY"],
    api_secret=os.environ["BINANCE_API_SECRET"],
    start_time_ms={int(start_ms)},
    end_time_ms={int(end_ms)},
    limit={int(limit)},
)
print(json.dumps(trades, ensure_ascii=False))
""".strip()
    encoded = base64.b64encode(remote_py.encode("utf-8")).decode("ascii")
    remote_cmd = (
        "bash -lc '"
        "if [ -x /home/ubuntu/releases/wangge-118edc7/.venv/bin/python ]; then "
        "APP=/home/ubuntu/releases/wangge-118edc7; "
        "else APP=/home/ubuntu/wangge; "
        "fi; "
        "cd \"$APP\"; "
        "export $(sudo cat /etc/grid-web.env | xargs); "
        f"printf %s {encoded} | base64 -d | \"$APP/.venv/bin/python\""
        "'"
    )
    cmd = ["ssh", ssh_host, remote_cmd]
    output = subprocess.check_output(cmd, text=True)
    data = json.loads(output)
    if not isinstance(data, list):
        raise RuntimeError(f"{ssh_host} returned non-list trades payload")
    return data


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Human-readable Binance spot trade summary.")
    parser.add_argument("--symbol", default="BARDUSDT")
    parser.add_argument("--ssh-host", help="SSH host alias to read /etc/grid-web.env from")
    parser.add_argument("--api-key", default=os.environ.get("BINANCE_API_KEY", ""))
    parser.add_argument("--api-secret", default=os.environ.get("BINANCE_API_SECRET", ""))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--recent-limit", type=int, default=10)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    api_key = str(args.api_key or "").strip()
    api_secret = str(args.api_secret or "").strip()
    now = datetime.now(timezone.utc)
    start_ms = int((now - timedelta(hours=24)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    if args.ssh_host:
        trades = _fetch_remote_trades(str(args.ssh_host), str(args.symbol).upper().strip(), start_ms, end_ms, int(args.limit))
    else:
        if not api_key or not api_secret:
            raise SystemExit("Provide --ssh-host or --api-key/--api-secret")
        trades = fetch_spot_user_trades(
            symbol=str(args.symbol).upper().strip(),
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            limit=int(args.limit),
        )
    if not args.ssh_host and (not api_key or not api_secret):
        raise SystemExit("Provide --ssh-host or --api-key/--api-secret")
    summary = summarize_spot_trades(trades, recent_limit=int(args.recent_limit))
    print(format_spot_trade_summary(summary, symbol=str(args.symbol).upper().strip()))


if __name__ == "__main__":
    main()
