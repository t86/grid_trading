from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


WINDOWS_MS: dict[str, int] = {
    "10m": 10 * 60_000,
    "1h": 60 * 60_000,
    "24h": 24 * 60 * 60_000,
}


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _trade_quote_qty(trade: dict[str, Any]) -> float:
    return _safe_float(trade.get("quoteQty"))


def summarize_spot_trades(
    trades: list[dict[str, Any]],
    *,
    now_ms: int | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    normalized = sorted(
        [item for item in trades if isinstance(item, dict)],
        key=lambda item: int(item.get("time", 0) or 0),
        reverse=True,
    )
    windows: dict[str, dict[str, Any]] = {}
    for label, span_ms in WINDOWS_MS.items():
        rows = [item for item in normalized if int(item.get("time", 0) or 0) >= now_ms - span_ms]
        buy_notional = sum(_trade_quote_qty(item) for item in rows if bool(item.get("isBuyer")))
        sell_notional = sum(_trade_quote_qty(item) for item in rows if not bool(item.get("isBuyer")))
        gross_notional = buy_notional + sell_notional
        net_notional = buy_notional - sell_notional
        windows[label] = {
            "trade_count": len(rows),
            "gross_notional": gross_notional,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "net_buy_notional": max(net_notional, 0.0),
            "net_sell_notional": max(-net_notional, 0.0),
        }

    recent_trades = [
        {
            "time": int(item.get("time", 0) or 0),
            "side": "BUY" if bool(item.get("isBuyer")) else "SELL",
            "price": _safe_float(item.get("price")),
            "qty": _safe_float(item.get("qty")),
            "quote_qty": _trade_quote_qty(item),
            "commission": _safe_float(item.get("commission")),
            "commission_asset": str(item.get("commissionAsset", "") or ""),
        }
        for item in normalized[: max(int(recent_limit), 0)]
    ]
    return {"windows": windows, "recent_trades": recent_trades}


def format_spot_trade_summary(summary: dict[str, Any], *, symbol: str) -> str:
    lines = [f"{symbol} spot trade summary"]
    for label in ("10m", "1h", "24h"):
        item = summary["windows"][label]
        lines.append(
            f"{label:>3}  gross={item['gross_notional']:.4f}  buy={item['buy_notional']:.4f}  "
            f"sell={item['sell_notional']:.4f}  trades={item['trade_count']}"
        )
        if item["net_buy_notional"] > 0:
            lines.append(f"     net=BUY {item['net_buy_notional']:.4f}")
        elif item["net_sell_notional"] > 0:
            lines.append(f"     net=SELL {item['net_sell_notional']:.4f}")
        else:
            lines.append("     net=FLAT 0.0000")

    lines.append("")
    lines.append("Recent trades")
    if not summary["recent_trades"]:
        lines.append("  (none)")
        return "\n".join(lines)
    for item in summary["recent_trades"]:
        ts = datetime.fromtimestamp(item["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append(
            f"  {ts}  {item['side']:>4}  price={item['price']:.8f}  qty={item['qty']:.4f}  quote={item['quote_qty']:.4f}"
        )
    return "\n".join(lines)
