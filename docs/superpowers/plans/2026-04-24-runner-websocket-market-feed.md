# Runner Websocket Market Feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a process-local websocket market feed to each futures loop runner, fall back cleanly to REST, and stop per-cycle `bookTicker` / `premiumIndex` polling from dominating IP request weight.

**Architecture:** Keep the current one-symbol-per-process runner model. Extend `data.py` with a long-lived symbol-config cache, a unified market snapshot resolver, and a small threaded `FuturesMarketStream`; then wire `loop_runner.py` to prefer websocket snapshots for planning and execution while preserving REST fallback when the stream is cold, stale, or disconnected.

**Tech Stack:** Python, `unittest`, existing Binance REST helpers, new `websocket-client` dependency, current synchronous runner loop

---

## File Map

- Modify: `/Volumes/WORK/binance/wangge/pyproject.toml`
  - Add the websocket client dependency used by the process-local stream.
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/data.py`
  - Add in-memory symbol config cache, unified market snapshot resolution, and the `FuturesMarketStream` implementation.
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/loop_runner.py`
  - Start and stop the market stream in `main()`, replace direct market REST reads with the unified snapshot resolver, and keep execution retry behavior intact.
- Create: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`
  - Lock the cache, snapshot fallback, stale detection, and stream message merge behavior.
- Modify: `/Volumes/WORK/binance/wangge/tests/test_loop_runner.py`
  - Lock the runner’s websocket-first planning/execution behavior and REST fallback compatibility.

---

### Task 1: Lock the market snapshot and symbol-config contract with failing tests

**Files:**
- Create: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`

- [ ] **Step 1: Write failing cache and snapshot tests**

```python
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from grid_optimizer.data import (
    clear_futures_market_data_caches,
    fetch_futures_symbol_config,
    resolve_futures_market_snapshot,
)


class FuturesMarketSnapshotTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_futures_market_data_caches()

    @patch("grid_optimizer.data._http_get_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_symbol_config_uses_long_lived_cache(self, mock_time, mock_http) -> None:
        mock_http.side_effect = [
            {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "pricePrecision": 2, "quantityPrecision": 3, "filters": []}]},
            {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "pricePrecision": 4, "quantityPrecision": 5, "filters": []}]},
        ]

        mock_time.return_value = 1_000.0
        first = fetch_futures_symbol_config("BTCUSDT")

        mock_time.return_value = 1_600.0
        second = fetch_futures_symbol_config("BTCUSDT")

        mock_time.return_value = 30_000.0
        third = fetch_futures_symbol_config("BTCUSDT")

        self.assertEqual(first["price_precision"], 2)
        self.assertEqual(second["price_precision"], 2)
        self.assertEqual(third["price_precision"], 4)

    @patch("grid_optimizer.data.build_futures_rest_market_snapshot")
    def test_resolve_futures_market_snapshot_prefers_fresh_stream_snapshot(self, mock_rest) -> None:
        stream = SimpleNamespace(
            snapshot=lambda max_age_seconds=None: {
                "symbol": "BTCUSDT",
                "bid_price": 10.0,
                "ask_price": 10.2,
                "mark_price": 10.1,
                "funding_rate": 0.0001,
                "next_funding_time": 1234567890,
                "source": "websocket",
            }
        )

        snapshot = resolve_futures_market_snapshot("BTCUSDT", stream=stream)

        self.assertEqual(snapshot["source"], "websocket")
        mock_rest.assert_not_called()

    @patch("grid_optimizer.data.build_futures_rest_market_snapshot")
    def test_resolve_futures_market_snapshot_falls_back_to_rest_when_stream_is_unavailable(self, mock_rest) -> None:
        mock_rest.return_value = {
            "symbol": "BTCUSDT",
            "bid_price": 9.9,
            "ask_price": 10.1,
            "mark_price": 10.0,
            "funding_rate": 0.0,
            "next_funding_time": None,
            "source": "rest",
        }
        stream = SimpleNamespace(snapshot=lambda max_age_seconds=None: None)

        snapshot = resolve_futures_market_snapshot("BTCUSDT", stream=stream)

        self.assertEqual(snapshot["source"], "rest")
```

- [ ] **Step 2: Run the new test file to verify it fails**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_data_market_stream.py -q
```

Expected: FAIL with `ImportError` / `AttributeError` because `clear_futures_market_data_caches` and `resolve_futures_market_snapshot` do not exist yet.

---

### Task 2: Implement symbol-config caching and REST-backed market snapshot resolution

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/data.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`

- [ ] **Step 1: Add the new cache state and clearing helper**

```python
FUTURES_SYMBOL_CONFIG_CACHE_TTL_SECONDS = 6 * 3600.0
_FUTURES_MARKET_CACHE_LOCK = threading.RLock()
_FUTURES_SYMBOL_CONFIG_CACHE: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}


def _get_cached_market_response(cache: dict[Any, tuple[float, Any]], key: Any, ttl_seconds: float) -> Any | None:
    now = time.time()
    with _FUTURES_MARKET_CACHE_LOCK:
        entry = cache.get(key)
        if entry is None:
            return None
        loaded_at, payload = entry
        if now - loaded_at > ttl_seconds:
            cache.pop(key, None)
            return None
        return deepcopy(payload)


def _store_cached_market_response(cache: dict[Any, tuple[float, Any]], key: Any, payload: Any) -> Any:
    cached_payload = deepcopy(payload)
    with _FUTURES_MARKET_CACHE_LOCK:
        cache[key] = (time.time(), cached_payload)
    return deepcopy(cached_payload)


def clear_futures_market_data_caches() -> None:
    with _FUTURES_MARKET_CACHE_LOCK:
        _FUTURES_SYMBOL_CONFIG_CACHE.clear()
```

- [ ] **Step 2: Wrap `fetch_futures_symbol_config()` with the new TTL cache**

```python
def fetch_futures_symbol_config(symbol: str, contract_type: str = "usdm") -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    normalized_contract = normalize_contract_type(contract_type)
    cache_key = (normalized_contract, normalized_symbol)
    cached = _get_cached_market_response(
        _FUTURES_SYMBOL_CONFIG_CACHE,
        cache_key,
        FUTURES_SYMBOL_CONFIG_CACHE_TTL_SECONDS,
    )
    if cached is not None:
        return cached

    data = _http_get_json(_market_api_urls(normalized_contract)["exchange_info"], {"symbol": normalized_symbol})
    symbols_raw = data.get("symbols", [])
    item = next(
        candidate
        for candidate in symbols_raw
        if isinstance(candidate, dict) and str(candidate.get("symbol", "")).upper().strip() == normalized_symbol
    )
    payload = {
        "symbol": normalized_symbol,
        "status": str(item.get("status") or item.get("contractStatus") or "").upper().strip(),
        "contract_type": str(item.get("contractType", "")).upper().strip(),
        "price_precision": int(item["pricePrecision"]) if str(item.get("pricePrecision", "")).strip() else None,
        "quantity_precision": int(item["quantityPrecision"]) if str(item.get("quantityPrecision", "")).strip() else None,
        "tick_size": _safe_positive_float((item.get("filters") or [{}])[0].get("tickSize")),
        "min_notional": _safe_positive_float((item.get("filters") or [{}])[0].get("notional")),
    }
    return _store_cached_market_response(_FUTURES_SYMBOL_CONFIG_CACHE, cache_key, payload)
```

- [ ] **Step 3: Add a REST builder plus unified resolver**

```python
def build_futures_rest_market_snapshot(symbol: str, contract_type: str = "usdm") -> dict[str, Any]:
    book_rows = fetch_futures_book_tickers(contract_type=contract_type, symbol=symbol)
    premium_rows = fetch_futures_premium_index(contract_type=contract_type, symbol=symbol)
    if not book_rows or not premium_rows:
        raise RuntimeError(f"incomplete market snapshot for {symbol}")
    book = book_rows[0]
    premium = premium_rows[0]
    return {
        "symbol": str(symbol).upper().strip(),
        "bid_price": _safe_float(book.get("bid_price")),
        "ask_price": _safe_float(book.get("ask_price")),
        "mark_price": _safe_float(premium.get("mark_price")),
        "funding_rate": _safe_float(premium.get("funding_rate")),
        "next_funding_time": premium.get("next_funding_time"),
        "book_time": book.get("time"),
        "mark_time": premium.get("time"),
        "snapshot_at": time.monotonic(),
        "source": "rest",
    }


def resolve_futures_market_snapshot(
    symbol: str,
    *,
    contract_type: str = "usdm",
    stream: Any | None = None,
    max_snapshot_age_seconds: float = 3.0,
) -> dict[str, Any]:
    if stream is not None:
        snapshot = stream.snapshot(max_age_seconds=max_snapshot_age_seconds)
        if snapshot is not None:
            return snapshot
    return build_futures_rest_market_snapshot(symbol, contract_type=contract_type)
```

- [ ] **Step 4: Re-run the focused data snapshot tests**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_data_market_stream.py -q
```

Expected: PASS for the new cache/snapshot resolver tests.

- [ ] **Step 5: Commit the cache/resolver slice**

```bash
git add src/grid_optimizer/data.py tests/test_data_market_stream.py
git commit -m "Cache futures symbol config and resolve market snapshots"
```

---

### Task 3: Lock websocket stream merge and stale behavior with failing tests

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`

- [ ] **Step 1: Add failing `FuturesMarketStream` tests**

```python
from grid_optimizer.data import FuturesMarketStream


class FuturesMarketStreamLifecycleTests(unittest.TestCase):
    @patch("grid_optimizer.data.time.monotonic")
    def test_market_stream_merges_book_and_mark_messages_into_one_snapshot(self, mock_monotonic) -> None:
        mock_monotonic.side_effect = [10.0, 10.0, 10.0]
        stream = FuturesMarketStream("BTCUSDT")

        stream._handle_book_ticker_message({"s": "BTCUSDT", "b": "10.0", "a": "10.2", "T": 100})
        stream._handle_mark_price_message({"s": "BTCUSDT", "p": "10.1", "r": "0.0001", "T": 101, "E": 102, "n": 1234567890})

        snapshot = stream.snapshot(max_age_seconds=3.0)

        self.assertEqual(snapshot["symbol"], "BTCUSDT")
        self.assertEqual(snapshot["bid_price"], 10.0)
        self.assertEqual(snapshot["mark_price"], 10.1)

    @patch("grid_optimizer.data.time.monotonic")
    def test_market_stream_returns_none_when_snapshot_is_stale(self, mock_monotonic) -> None:
        mock_monotonic.side_effect = [10.0, 10.0, 20.0]
        stream = FuturesMarketStream("BTCUSDT")
        stream._handle_book_ticker_message({"s": "BTCUSDT", "b": "10.0", "a": "10.2", "T": 100})
        stream._handle_mark_price_message({"s": "BTCUSDT", "p": "10.1", "r": "0.0001", "T": 101, "E": 102, "n": 1234567890})

        self.assertIsNone(stream.snapshot(max_age_seconds=3.0))
```

- [ ] **Step 2: Run the stream tests to verify they fail**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_data_market_stream.py -q
```

Expected: FAIL because `FuturesMarketStream` does not exist yet.

---

### Task 4: Implement the process-local websocket market stream and add the dependency

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/pyproject.toml`
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/data.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`

- [ ] **Step 1: Add the websocket dependency**

```toml
[project]
dependencies = [
  "requests>=2.31",
  "websocket-client>=1.8,<2",
]
```

- [ ] **Step 2: Implement `FuturesMarketStream` in `data.py`**

```python
class FuturesMarketStream:
    def __init__(self, symbol: str, *, contract_type: str = "usdm") -> None:
        self.symbol = str(symbol).upper().strip()
        self.contract_type = normalize_contract_type(contract_type)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._latest_snapshot: dict[str, Any] | None = None
        self._latest_snapshot_at = 0.0
        self._last_error: str | None = None
        self._connection_state = "idle"

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever, name=f"{self.symbol}-market-stream", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._thread = None

    def snapshot(self, *, max_age_seconds: float | None = None) -> dict[str, Any] | None:
        with self._lock:
            if self._latest_snapshot is None:
                return None
            if max_age_seconds is not None and (time.monotonic() - self._latest_snapshot_at) > max_age_seconds:
                return None
            return deepcopy(self._latest_snapshot)
```

- [ ] **Step 3: Implement websocket message parsing with explicit book and mark handlers**

```python
def _run_forever(self) -> None:
    import websocket

    stream_name = f"{self.symbol.lower()}@bookTicker/{self.symbol.lower()}@markPrice"
    url = f"wss://fstream.binance.com/stream?streams={stream_name}"
    backoff_seconds = 1.0
    while not self._stop_event.is_set():
        app = websocket.WebSocketApp(url, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close)
        self._connection_state = "connecting"
        app.run_forever(ping_interval=20, ping_timeout=10)
        if self._stop_event.is_set():
            break
        self._connection_state = "disconnected"
        time.sleep(backoff_seconds)
        backoff_seconds = min(backoff_seconds * 2.0, 15.0)


def _on_message(self, _ws: Any, raw_message: str) -> None:
    payload = json.loads(raw_message)
    stream_name = str(payload.get("stream", ""))
    data = payload.get("data") or {}
    if stream_name.endswith("@bookTicker"):
        self._handle_book_ticker_message(data)
    elif stream_name.endswith("@markPrice"):
        self._handle_mark_price_message(data)


def _on_error(self, _ws: Any, exc: Exception) -> None:
    with self._lock:
        self._last_error = str(exc)
        self._connection_state = "error"


def _on_close(self, _ws: Any, _status_code: int | None, _msg: str | None) -> None:
    with self._lock:
        if not self._stop_event.is_set():
            self._connection_state = "disconnected"


def _handle_book_ticker_message(self, payload: dict[str, Any]) -> None:
    self._merge_snapshot(
        bid_price=_safe_float(payload.get("b")),
        ask_price=_safe_float(payload.get("a")),
        book_time=int(payload.get("T") or 0) or None,
    )


def _handle_mark_price_message(self, payload: dict[str, Any]) -> None:
    self._merge_snapshot(
        mark_price=_safe_float(payload.get("p")),
        funding_rate=_safe_float(payload.get("r")),
        next_funding_time=int(payload.get("n") or 0) or None,
        mark_time=int(payload.get("E") or payload.get("T") or 0) or None,
    )
```

- [ ] **Step 4: Re-run the focused stream tests until they pass**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_data_market_stream.py -q
```

Expected: PASS for cache, resolver, stream merge, and stale-snapshot tests.

- [ ] **Step 5: Commit the websocket stream slice**

```bash
git add pyproject.toml src/grid_optimizer/data.py tests/test_data_market_stream.py
git commit -m "Add futures market websocket stream"
```

---

### Task 5: Lock websocket-first runner behavior with failing tests

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/tests/test_loop_runner.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_loop_runner.py`

- [ ] **Step 1: Add failing planning and execution tests**

```python
    @patch("grid_optimizer.loop_runner.resolve_futures_market_snapshot")
    @patch("grid_optimizer.loop_runner.fetch_futures_premium_index")
    @patch("grid_optimizer.loop_runner.fetch_futures_book_tickers")
    def test_generate_plan_report_uses_market_snapshot_resolver_when_stream_available(
        self,
        mock_book_tickers,
        mock_premium_index,
        mock_market_snapshot,
        mock_symbol_config,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_market_guard,
    ) -> None:
        mock_market_snapshot.return_value = {
            "symbol": "BARDUSDT",
            "bid_price": 0.3120,
            "ask_price": 0.3122,
            "mark_price": 0.3121,
            "funding_rate": 0.0001,
            "next_funding_time": 1234567890,
            "source": "websocket",
        }
        mock_symbol_config.return_value = {"tick_size": 0.0001, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0}
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {"multiAssetsMargin": False, "positions": [{"symbol": "BARDUSDT", "positionAmt": "0", "entryPrice": "0"}]}
        mock_open_orders.return_value = []
        mock_market_guard.return_value = {"buy_pause_active": False, "buy_pause_reasons": [], "shift_frozen": False}
        with TemporaryDirectory() as tmpdir:
            args = self._base_one_way_long_args(tmpdir, symbol="BARDUSDT")
        report = generate_plan_report(args)
        self.assertEqual(report["mid_price"], 0.3121)
        mock_book_tickers.assert_not_called()
        mock_premium_index.assert_not_called()

    @patch("grid_optimizer.loop_runner.resolve_futures_market_snapshot")
    def test_execute_plan_report_uses_market_snapshot_resolver_for_initial_quote_and_retry(
        self,
        mock_market_snapshot,
        mock_validate_plan_report,
        mock_load_credentials,
        mock_position_mode,
        mock_account_info,
        mock_open_orders,
        mock_change_leverage,
        mock_post_order,
        _mock_update_inventory_refs,
        _mock_update_refs,
    ) -> None:
        mock_market_snapshot.side_effect = [
            {"symbol": "KATUSDT", "bid_price": 0.49, "ask_price": 0.51, "mark_price": 0.50, "funding_rate": 0.0, "next_funding_time": None, "source": "websocket"},
            {"symbol": "KATUSDT", "bid_price": 0.48, "ask_price": 0.50, "mark_price": 0.49, "funding_rate": 0.0, "next_funding_time": None, "source": "websocket"},
        ]
        mock_validate_plan_report.return_value = {
            "ok": True,
            "errors": [],
            "actions": {"place_count": 1, "cancel_count": 0, "cancel_orders": [], "place_orders": [{"role": "entry", "side": "BUY", "qty": 11.0, "price": 0.50}]},
        }
        mock_load_credentials.return_value = ("key", "secret")
        mock_position_mode.return_value = {"dualSidePosition": False}
        mock_account_info.return_value = {"multiAssetsMargin": False, "positions": [{"symbol": "KATUSDT", "positionAmt": "0", "entryPrice": "0"}]}
        mock_open_orders.return_value = []
        mock_change_leverage.return_value = {"leverage": 2}
        mock_post_order.side_effect = [
            RuntimeError("Binance API error -5022: Post only order will be rejected."),
            {"orderId": 124, "clientOrderId": "cid-124"},
        ]
        args = Namespace(symbol="KATUSDT", strategy_mode="one_way_long", max_new_orders=20, max_total_notional=1000.0, cancel_stale=False, max_plan_age_seconds=30, max_mid_drift_steps=4.0, plan_json="output/katusdt_loop_latest_plan.json", apply=True, margin_type="KEEP", leverage=2, maker_retries=1, recv_window=5000, state_path="output/katusdt_loop_state.json")
        plan_report = {"symbol": "KATUSDT", "strategy_mode": "one_way_long", "mid_price": 0.50, "step_price": 0.01, "open_order_count": 0, "current_long_qty": 0.0, "current_short_qty": 0.0, "actual_net_qty": 0.0, "symbol_info": {"tick_size": 0.01, "min_qty": 0.1, "min_notional": 5.0}}
        report = execute_plan_report(args, plan_report)
        self.assertTrue(report["executed"])
        self.assertEqual(mock_market_snapshot.call_count, 2)
```

- [ ] **Step 2: Run the runner tests to verify they fail**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_loop_runner.py -q
```

Expected: FAIL because `loop_runner.py` still calls direct REST market helpers.

---

### Task 6: Wire the runner to the websocket-first market snapshot path

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/loop_runner.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_loop_runner.py`

- [ ] **Step 1: Import the new data-layer helpers and define runner-side snapshot constants**

```python
from .data import (
    FuturesMarketStream,
    resolve_futures_market_snapshot,
)

RUNNER_MARKET_SNAPSHOT_MAX_AGE_SECONDS = 3.0
```

- [ ] **Step 2: Replace `generate_plan_report()` market REST calls with the unified resolver**

```python
def generate_plan_report(args: argparse.Namespace) -> dict[str, Any]:
    symbol = args.symbol.upper().strip()
    symbol_info = fetch_futures_symbol_config(symbol)
    market_snapshot = resolve_futures_market_snapshot(
        symbol,
        stream=getattr(args, "market_stream", None),
        max_snapshot_age_seconds=RUNNER_MARKET_SNAPSHOT_MAX_AGE_SECONDS,
    )
    bid_price = _safe_float(market_snapshot.get("bid_price"))
    ask_price = _safe_float(market_snapshot.get("ask_price"))
    premium = {
        "funding_rate": market_snapshot.get("funding_rate"),
        "mark_price": market_snapshot.get("mark_price"),
        "next_funding_time": market_snapshot.get("next_funding_time"),
    }
```

- [ ] **Step 3: Replace `execute_plan_report()` REST-only book fetches with the unified resolver**

```python
initial_market_snapshot = resolve_futures_market_snapshot(
    symbol,
    stream=getattr(args, "market_stream", None),
    max_snapshot_age_seconds=RUNNER_MARKET_SNAPSHOT_MAX_AGE_SECONDS,
)
market_fetcher = _build_execution_market_snapshot_fetcher(
    symbol=symbol,
    initial_book=initial_market_snapshot,
    snapshot_loader=lambda: resolve_futures_market_snapshot(
        symbol,
        stream=getattr(args, "market_stream", None),
        max_snapshot_age_seconds=RUNNER_MARKET_SNAPSHOT_MAX_AGE_SECONDS,
    ),
)
```

- [ ] **Step 4: Start and stop the stream in `main()` without making websocket availability mandatory**

```python
market_stream: FuturesMarketStream | None = None
try:
    market_stream = FuturesMarketStream(args.symbol)
    market_stream.start()
    args.market_stream = market_stream
except Exception as exc:
    args.market_stream = None
    print(f"[market-stream] disabled for {args.symbol}: {exc}")

try:
    while True:
        plan_report = generate_plan_report(args)
        submit_report = execute_plan_report(args, plan_report)
        _write_json(plan_path, plan_report)
        _write_json(submit_report_path, submit_report)
        if args.iterations and cycle >= args.iterations:
            break
finally:
    if market_stream is not None:
        market_stream.stop()
```

- [ ] **Step 5: Re-run the focused runner tests until they pass**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_loop_runner.py -q
```

Expected: PASS for the new websocket-first planning/execution tests and the existing runner suite.

- [ ] **Step 6: Commit the runner wiring slice**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "Use websocket market snapshots in loop runner"
```

---

### Task 7: Verify targeted coverage before any deploy decision

**Files:**
- Test: `/Volumes/WORK/binance/wangge/tests/test_data_market_stream.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_data_signed_cache.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_loop_runner.py`

- [ ] **Step 1: Run the targeted pytest suite**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_data_market_stream.py tests/test_data_signed_cache.py tests/test_loop_runner.py -q
```

Expected: all tests pass.

- [ ] **Step 2: Run a syntax check on the modified modules**

Run:

```bash
PYTHONPATH=src python -m py_compile src/grid_optimizer/data.py src/grid_optimizer/loop_runner.py
```

Expected: exit code `0`.

- [ ] **Step 3: Check the diff is scoped correctly before talking about deployment**

Run:

```bash
git diff -- src/grid_optimizer/data.py src/grid_optimizer/loop_runner.py tests/test_data_market_stream.py tests/test_loop_runner.py pyproject.toml
```

Expected: only websocket market feed, symbol-config cache, and runner wiring changes appear.
