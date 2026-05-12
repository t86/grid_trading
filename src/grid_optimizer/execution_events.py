from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any
import json
import threading
import time

from .data import _futures_trade_base_url, _http_api_key_request_json, normalize_contract_type


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class MarketTick:
    symbol: str
    bid_price: float
    ask_price: float
    mid_price: float
    exchange_time: int | None = None
    source: str = "bookTicker"

    @classmethod
    def from_book_ticker(cls, payload: dict[str, Any]) -> "MarketTick":
        symbol = str(payload.get("s") or payload.get("symbol") or "").upper().strip()
        bid = _safe_float(payload.get("b", payload.get("bidPrice")))
        ask = _safe_float(payload.get("a", payload.get("askPrice")))
        if not symbol:
            raise ValueError("symbol is required")
        if bid <= 0 or ask <= 0:
            raise ValueError("bid/ask must be positive")
        return cls(
            symbol=symbol,
            bid_price=bid,
            ask_price=ask,
            mid_price=(bid + ask) / 2.0,
            exchange_time=_safe_int(payload.get("T")),
        )


@dataclass(frozen=True)
class ExecutionEvent:
    kind: str
    symbol: str
    event_time: int | None
    transaction_time: int | None
    order_id: int | None
    client_order_id: str
    side: str
    execution_type: str
    order_status: str
    order_type: str = ""
    time_in_force: str = ""
    position_side: str = "BOTH"
    original_qty: float = 0.0
    original_price: float = 0.0
    average_price: float = 0.0
    last_filled_qty: float = 0.0
    cumulative_filled_qty: float = 0.0
    last_filled_price: float = 0.0
    commission: float = 0.0
    commission_asset: str = ""
    realized_pnl: float = 0.0

    @property
    def dedupe_key(self) -> tuple[Any, ...]:
        return (
            self.symbol,
            self.order_id,
            self.client_order_id,
            self.execution_type,
            self.order_status,
            self.event_time,
            self.transaction_time,
            self.cumulative_filled_qty,
            self.last_filled_qty,
            self.last_filled_price,
        )


def _dec(value: float | int | str | Decimal) -> Decimal:
    return Decimal(str(value))


def _floor_index(price: Decimal, lower: Decimal, step: Decimal) -> int:
    return int(((price - lower) / step).to_integral_value(rounding=ROUND_FLOOR))


def _ceil_index(price: Decimal, lower: Decimal, step: Decimal) -> int:
    return int(((price - lower) / step).to_integral_value(rounding=ROUND_CEILING))


def detect_crossed_grid_levels(
    *,
    last_price: float,
    current_price: float,
    lower_price: float,
    upper_price: float,
    step: float,
) -> list[dict[str, Any]]:
    last = _dec(last_price)
    current = _dec(current_price)
    lower = _dec(lower_price)
    upper = _dec(upper_price)
    tick = _dec(step)
    if tick <= 0:
        raise ValueError("step must be positive")
    if lower >= upper:
        raise ValueError("lower_price must be below upper_price")
    if last == current:
        return []
    crossings: list[dict[str, Any]] = []
    if current > last:
        start = max(_floor_index(last, lower, tick) + 1, 1)
        stop_exclusive = min(_floor_index(current, lower, tick), _floor_index(upper, lower, tick))
        for index in range(start, stop_exclusive):
            price = lower + tick * Decimal(index)
            if lower < price < upper:
                crossings.append({"side": "SELL", "index": index, "price": float(price)})
        return crossings

    start = min(_ceil_index(last, lower, tick) - 1, _floor_index(upper, lower, tick) - 1)
    stop_inclusive = max(_ceil_index(current, lower, tick) + 1, 1)
    for index in range(start, stop_inclusive - 1, -1):
        price = lower + tick * Decimal(index)
        if lower < price < upper:
            crossings.append({"side": "BUY", "index": index, "price": float(price)})
    return crossings


class ExecutionEventStore:
    def __init__(self, *, max_events: int = 10_000) -> None:
        self._events: deque[ExecutionEvent] = deque(maxlen=max(int(max_events), 1))
        self._seen: set[tuple[Any, ...]] = set()
        self._lock = threading.RLock()

    def add(self, event: ExecutionEvent) -> bool:
        with self._lock:
            key = event.dedupe_key
            if key in self._seen:
                return False
            if len(self._events) == self._events.maxlen and self._events:
                old = self._events[0]
                self._seen.discard(old.dedupe_key)
            self._events.append(event)
            self._seen.add(key)
            return True

    def snapshot(self) -> list[ExecutionEvent]:
        with self._lock:
            return list(self._events)


class AccountPositionStore:
    def __init__(self) -> None:
        self._positions: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._last_update_at = 0.0

    def update_from_account_update(self, payload: dict[str, Any]) -> int:
        account = payload.get("a")
        if not isinstance(account, dict):
            return 0
        positions = account.get("P")
        if not isinstance(positions, list):
            return 0
        observed_at = time.monotonic()
        event_time = _safe_int(payload.get("E"))
        changed = 0
        with self._lock:
            for item in positions:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("s") or "").upper().strip()
                if not symbol:
                    continue
                position_side = str(item.get("ps") or "BOTH").upper().strip() or "BOTH"
                self._positions[(symbol, position_side)] = {
                    "symbol": symbol,
                    "positionSide": position_side,
                    "positionAmt": str(item.get("pa") or "0"),
                    "entryPrice": str(item.get("ep") or "0"),
                    "breakEvenPrice": str(item.get("bep") or "0"),
                    "unRealizedProfit": str(item.get("up") or "0"),
                    "marginType": str(item.get("mt") or ""),
                    "isolatedWallet": str(item.get("iw") or "0"),
                    "event_time": event_time,
                    "observed_at": observed_at,
                    "source": "user_data_stream",
                }
                changed += 1
            if changed:
                self._last_update_at = observed_at
        return changed

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._positions.values()]

    def last_update_age_seconds(self) -> float | None:
        with self._lock:
            if self._last_update_at <= 0:
                return None
            return max(time.monotonic() - self._last_update_at, 0.0)


class OpenOrderStateStore:
    def __init__(self) -> None:
        self._orders: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._last_update_at = 0.0

    def update_from_execution_event(self, event: ExecutionEvent) -> None:
        client_order_id = str(event.client_order_id or "").strip()
        if not client_order_id:
            return
        now = time.monotonic()
        with self._lock:
            if event.kind in {"ORDER_FILLED", "ORDER_CANCELED", "ORDER_EXPIRED"}:
                self._orders.pop(client_order_id, None)
            else:
                self._orders[client_order_id] = {
                    "symbol": event.symbol,
                    "orderId": event.order_id,
                    "clientOrderId": client_order_id,
                    "side": event.side,
                    "type": event.order_type,
                    "timeInForce": event.time_in_force,
                    "positionSide": event.position_side,
                    "origQty": str(event.original_qty),
                    "price": str(event.original_price),
                    "executedQty": str(event.cumulative_filled_qty),
                    "status": event.order_status,
                    "executionType": event.execution_type,
                    "event_time": event.event_time,
                    "transaction_time": event.transaction_time,
                    "observed_at": now,
                    "source": "user_data_stream",
                }
            self._last_update_at = now

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._orders.values()]

    def last_update_age_seconds(self) -> float | None:
        with self._lock:
            if self._last_update_at <= 0:
                return None
            return max(time.monotonic() - self._last_update_at, 0.0)


def _kind_from_order_update(execution_type: str, order_status: str) -> str:
    status = order_status.upper()
    execution = execution_type.upper()
    if status == "FILLED":
        return "ORDER_FILLED"
    if status == "PARTIALLY_FILLED":
        return "ORDER_PARTIALLY_FILLED"
    if status == "CANCELED":
        return "ORDER_CANCELED"
    if status == "EXPIRED":
        return "ORDER_EXPIRED"
    if execution == "NEW" or status == "NEW":
        return "ORDER_NEW"
    return f"ORDER_{status or execution or 'UPDATE'}"


def normalize_order_trade_update(payload: dict[str, Any]) -> ExecutionEvent | None:
    order = payload.get("o")
    if not isinstance(order, dict):
        return None
    symbol = str(order.get("s") or "").upper().strip()
    if not symbol:
        return None
    execution_type = str(order.get("x") or "").upper().strip()
    order_status = str(order.get("X") or "").upper().strip()
    return ExecutionEvent(
        kind=_kind_from_order_update(execution_type, order_status),
        symbol=symbol,
        event_time=_safe_int(payload.get("E")),
        transaction_time=_safe_int(payload.get("T")),
        order_id=_safe_int(order.get("i")),
        client_order_id=str(order.get("c") or "").strip(),
        side=str(order.get("S") or "").upper().strip(),
        execution_type=execution_type,
        order_status=order_status,
        order_type=str(order.get("o") or "").upper().strip(),
        time_in_force=str(order.get("f") or "").upper().strip(),
        position_side=str(order.get("ps") or "BOTH").upper().strip() or "BOTH",
        original_qty=_safe_float(order.get("q")),
        original_price=_safe_float(order.get("p")),
        average_price=_safe_float(order.get("ap")),
        last_filled_qty=_safe_float(order.get("l")),
        cumulative_filled_qty=_safe_float(order.get("z")),
        last_filled_price=_safe_float(order.get("L")),
        commission=_safe_float(order.get("n")),
        commission_asset=str(order.get("N") or "").upper().strip(),
        realized_pnl=_safe_float(order.get("rp")),
    )


class FuturesListenKeyClient:
    def __init__(self, *, api_key: str, contract_type: str = "usdm") -> None:
        if not str(api_key or "").strip():
            raise ValueError("api_key is required")
        self.api_key = str(api_key).strip()
        self.contract_type = normalize_contract_type(contract_type)

    def _url(self) -> str:
        return f"{_futures_trade_base_url(self.contract_type)}/fapi/v1/listenKey"

    def create(self) -> str:
        data = _http_api_key_request_json(self._url(), {}, self.api_key, method="POST")
        if not isinstance(data, dict) or not str(data.get("listenKey") or "").strip():
            raise RuntimeError("Unexpected futures listenKey response")
        return str(data["listenKey"]).strip()

    def keepalive(self, listen_key: str) -> None:
        key = str(listen_key or "").strip()
        if not key:
            raise ValueError("listen_key is required")
        _http_api_key_request_json(self._url(), {"listenKey": key}, self.api_key, method="PUT")

    def close(self, listen_key: str) -> None:
        key = str(listen_key or "").strip()
        if not key:
            raise ValueError("listen_key is required")
        _http_api_key_request_json(self._url(), {"listenKey": key}, self.api_key, method="DELETE")


class FuturesUserDataStream:
    def __init__(
        self,
        *,
        api_key: str,
        contract_type: str = "usdm",
        event_store: ExecutionEventStore | None = None,
        account_position_store: AccountPositionStore | None = None,
        open_order_state_store: OpenOrderStateStore | None = None,
        listen_key_client: FuturesListenKeyClient | None = None,
        keepalive_interval_seconds: float = 30 * 60,
    ) -> None:
        self.listen_key_client = listen_key_client or FuturesListenKeyClient(
            api_key=api_key,
            contract_type=contract_type,
        )
        self.contract_type = normalize_contract_type(contract_type)
        self.event_store = event_store or ExecutionEventStore()
        self.account_position_store = account_position_store or AccountPositionStore()
        self.open_order_state_store = open_order_state_store or OpenOrderStateStore()
        self.keepalive_interval_seconds = max(float(keepalive_interval_seconds), 60.0)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws_app: Any | None = None
        self._listen_key: str | None = None
        self._connection_state = "idle"
        self._last_error: str | None = None
        self._last_message_at = 0.0
        self._last_keepalive_at = 0.0

    def start(self) -> None:
        thread = self._thread
        if thread is not None and thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever, name="futures-user-data-stream", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        app = self._ws_app
        if app is not None:
            try:
                app.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        listen_key = self._listen_key
        if listen_key:
            try:
                self.listen_key_client.close(listen_key)
            except Exception:
                pass
        self._thread = None
        self._ws_app = None
        self._listen_key = None

    def snapshot_events(self) -> list[ExecutionEvent]:
        return self.event_store.snapshot()

    def snapshot_account_positions(self) -> list[dict[str, Any]]:
        return self.account_position_store.snapshot()

    def snapshot_open_orders(self) -> list[dict[str, Any]]:
        return self.open_order_state_store.snapshot()

    def open_order_state_age_seconds(self) -> float | None:
        return self.open_order_state_store.last_update_age_seconds()

    def status(self) -> dict[str, Any]:
        with self._lock:
            last_message_age_seconds = (
                max(time.monotonic() - self._last_message_at, 0.0)
                if self._last_message_at > 0
                else None
            )
            last_keepalive_age_seconds = (
                max(time.monotonic() - self._last_keepalive_at, 0.0)
                if self._last_keepalive_at > 0
                else None
            )
            return {
                "connection_state": self._connection_state,
                "last_error": self._last_error,
                "last_message_age_seconds": last_message_age_seconds,
                "last_keepalive_age_seconds": last_keepalive_age_seconds,
                "last_account_update_age_seconds": self.account_position_store.last_update_age_seconds(),
                "last_open_order_update_age_seconds": self.open_order_state_store.last_update_age_seconds(),
                "listen_key_active": bool(self._listen_key),
                "event_count": len(self.event_store.snapshot()),
                "account_position_count": len(self.account_position_store.snapshot()),
                "open_order_count": len(self.open_order_state_store.snapshot()),
            }

    def _base_stream_url(self) -> str:
        if self.contract_type == "coinm":
            return "wss://dstream.binance.com/ws"
        return "wss://fstream.binance.com/ws"

    def _user_stream_url(self, listen_key: str) -> str:
        key = str(listen_key or "").strip()
        if not key:
            raise ValueError("listen_key is required")
        if self.contract_type == "coinm":
            return f"wss://dstream.binance.com/private/ws/{key}"
        return f"wss://fstream.binance.com/private/ws/{key}"

    def _run_forever(self) -> None:
        import websocket

        backoff_seconds = 1.0
        while not self._stop_event.is_set():
            try:
                listen_key = self.listen_key_client.create()
                with self._lock:
                    self._listen_key = listen_key
                    self._last_keepalive_at = time.monotonic()
                app = websocket.WebSocketApp(
                    self._user_stream_url(listen_key),
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                with self._lock:
                    self._ws_app = app
                    self._connection_state = "connecting"
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._on_error(None, exc)
            finally:
                with self._lock:
                    self._ws_app = None
            if self._stop_event.is_set():
                break
            self._maybe_keepalive()
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _maybe_keepalive(self) -> None:
        listen_key = self._listen_key
        if not listen_key:
            return
        now = time.monotonic()
        if now - self._last_keepalive_at < self.keepalive_interval_seconds:
            return
        self.listen_key_client.keepalive(listen_key)
        self._last_keepalive_at = now

    def _on_open(self, _ws: Any) -> None:
        with self._lock:
            self._connection_state = "connected"
            self._last_error = None

    def _on_message(self, _ws: Any, raw_message: str) -> None:
        payload = json.loads(raw_message)
        if not isinstance(payload, dict):
            return
        event_type = str(payload.get("e") or "").strip()
        event: ExecutionEvent | None = None
        if event_type == "ORDER_TRADE_UPDATE" or isinstance(payload.get("o"), dict):
            event = normalize_order_trade_update(payload)
        if event is not None:
            self.event_store.add(event)
            self.open_order_state_store.update_from_execution_event(event)
        if event_type == "ACCOUNT_UPDATE" or isinstance(payload.get("a"), dict):
            self.account_position_store.update_from_account_update(payload)
        with self._lock:
            self._last_message_at = time.monotonic()

    def _on_error(self, _ws: Any, exc: Exception) -> None:
        with self._lock:
            self._last_error = str(exc)
            self._connection_state = "error"

    def _on_close(self, _ws: Any, _status_code: int | None, _msg: str | None) -> None:
        with self._lock:
            if not self._stop_event.is_set():
                self._connection_state = "disconnected"
