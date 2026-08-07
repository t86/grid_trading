# Alpha 交易赛门槛面板 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `/alpha/` 改成已确认的上下分区 B 方案，自动展示当前轮早鸟倍速、加权总交易量、获奖人数、平均量及 `0.4/0.6/1.0` 三档估算门槛，同时完整保留现有行情与 `Check Alert` 行为。

**Architecture:** 将 110 上未版本化的 Alpha 行情与告警逻辑迁入当前仓库，抽出共享市场客户端；交易赛规则、K 线加权、两层缓存和逐币种错误隔离放在独立指标模块；轻量 `ThreadingHTTPServer` 入口只负责 API、Basic Auth 和页面渲染。生产部署使用新的仓库检出目录和可回滚 systemd 安装脚本，不覆盖 110 上已有的脏工作树。

**Tech Stack:** Python 3.10+、标准库 `http.server` / `dataclasses` / `datetime`、`requests`、pytest、原生 HTML/CSS/JavaScript、systemd、现有 Nginx `/alpha/` 反向代理。

---

## 文件结构

- Create: `src/grid_optimizer/alpha_market.py` — Binance Alpha token、交易对、ticker 和 K 线公共客户端。
- Create: `src/grid_optimizer/alpha_volume_alert.py` — 版本化当前生产告警行为，供 CLI 和 Dashboard 的 `Check Alert` 复用。
- Create: `src/grid_optimizer/alpha_competition_metrics.py` — 公告解析、规则发现、规则缓存、K 线加权、门槛计算和 60 秒聚合缓存。
- Create: `src/grid_optimizer/alpha_competition_dashboard.py` — 现有行情快照、Basic Auth、三个 API 路由和 B 方案页面。
- Create: `tests/fixtures/alpha_competition_articles.json` — QUID、GRVT、CAP、PRL、O 的固定官方规则样例。
- Create: `tests/test_alpha_market.py` — 交易对优先级和 K 线时间参数。
- Create: `tests/test_alpha_volume_alert.py` — 当前告警默认值、冷却和 dry-run 回归。
- Create: `tests/test_alpha_competition_metrics.py` — 规则、轮次、倍速、加权量、门槛、缓存与错误隔离。
- Create: `tests/test_alpha_competition_dashboard.py` — API、Basic Auth、现有快照行为和页面结构。
- Create: `deploy/oracle/install_alpha_competition_dashboard.sh` — 安装/更新服务、备份旧 unit、失败自动回滚。
- Create: `tests/test_install_alpha_competition_dashboard.py` — 安装脚本合同。
- Modify: `pyproject.toml` — 增加 Dashboard 和告警 CLI 入口。

不修改 `src/grid_optimizer/web.py`，不触碰网格策略、订单、仓位或冻结账本。

## 已核对的生产基线

- `dashboard.py`：451 行，SHA-256 `80a2636caabbb001b9263b0302ac04ea39d0e2b2b09807bb00fdbf9aae31926d`。
- `binance_alpha_volume_alert.py`：387 行，SHA-256 `8b351fe7222a9a06d135b764403a9b9ceaffd21b18ce617fccd008a507a686c8`。
- 当前 unit 从 `/home/ubuntu/binance-alpha-volume-alert/dashboard.py` 启动并监听 `0.0.0.0:8796`。
- Nginx 的 `/alpha/` 明确代理到 `http://127.0.0.1:8796/`，因此新 unit 只监听 loopback 即可，不改 Nginx。
- 110 的 `/home/ubuntu/wangge` 位于 `codex/strategy-workspace-controller`、commit `c96822cdb1497cbfaf663e25133756bc57f7b26a`，且有未跟踪文件；不得在该目录 pull、checkout 或清理。
- `ALPHA_SYMBOLS` 继续由 `/home/ubuntu/.config/binance-alpha-volume-alert.env` 提供；Basic Auth 继续复用 `GRID_WEB_USERNAME` / `GRID_WEB_PASSWORD`。

### Task 0: 准备隔离的本地测试环境

**Files:**
- Verify only: `pyproject.toml`

- [ ] **Step 1: 创建项目虚拟环境并安装测试依赖**

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e . pytest
```

Expected: `.venv/bin/python -c 'import grid_optimizer, requests, pytest'` exits 0。`.venv` 已被 Git 忽略；不得生成或提交无关 `uv.lock`。

- [ ] **Step 2: 记录干净基线**

```bash
git status --short --branch
git log -1 --oneline
```

Expected: 工作树干净并位于 `main`。

### Task 1: 版本化现有 Alpha 市场客户端与告警行为

**Files:**
- Create: `src/grid_optimizer/alpha_market.py`
- Create: `src/grid_optimizer/alpha_volume_alert.py`
- Create: `tests/test_alpha_market.py`
- Create: `tests/test_alpha_volume_alert.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: 写共享市场客户端的失败测试**

```python
from unittest.mock import Mock

from grid_optimizer.alpha_market import AlphaMarketClient


def test_trading_pairs_prefer_usdt_and_fall_back_to_usdc() -> None:
    client = AlphaMarketClient()
    payload = {"code": "000000", "data": {"symbols": [
        {"status": "TRADING", "baseAsset": "ALPHA_1", "quoteAsset": "USDC", "symbol": "ALPHA_1USDC"},
        {"status": "TRADING", "baseAsset": "ALPHA_1", "quoteAsset": "USDT", "symbol": "ALPHA_1USDT"},
        {"status": "TRADING", "baseAsset": "ALPHA_2", "quoteAsset": "USDC", "symbol": "ALPHA_2USDC"},
    ]}}
    assert client.parse_trading_pairs(payload) == {"ALPHA_1": "ALPHA_1USDT", "ALPHA_2": "ALPHA_2USDC"}


def test_fetch_klines_sends_verified_time_parameter_names() -> None:
    client = AlphaMarketClient()
    client._get_json = Mock(return_value={"code": "000000", "data": [[1000, "1", "1", "1", "1", "1", 1999, "12", 1]]})
    client.fetch_klines("ALPHA_1075USDC", interval="1h", limit=200, start_time_ms=1000, end_time_ms=2000)
    client._get_json.assert_called_once_with(
        "/bapi/defi/v1/public/alpha-trade/klines",
        {"symbol": "ALPHA_1075USDC", "interval": "1h", "limit": 200, "startTime": 1000, "endTime": 2000},
    )
```

- [ ] **Step 2: 运行测试并确认因模块不存在而失败**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_market.py -q`

Expected: `ModuleNotFoundError: grid_optimizer.alpha_market`。

- [ ] **Step 3: 实现共享客户端的稳定边界**

```python
@dataclass(frozen=True)
class AlphaToken:
    symbol: str
    alpha_id: str
    name: str
    chain_name: str
    price: float
    volume_24h: float
    count_24h: int
    pair: str


class AlphaMarketClient:
    def __init__(self, *, timeout_seconds: float = 20.0, session: requests.Session | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        for attempt in range(5):
            response = self.session.get(
                f"https://www.binance.com{path}",
                params=params or {},
                timeout=self.timeout_seconds,
                headers={"Accept": "application/json", "User-Agent": "wangge-alpha-dashboard/1.0"},
            )
            if response.status_code in {429, 502, 503, 504} and attempt < 4:
                time.sleep(2**attempt)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"request retries exhausted: {path}")

    def fetch_klines(self, pair: str, *, interval: str, limit: int,
                     start_time_ms: int | None = None, end_time_ms: int | None = None) -> list[list[Any]]:
        params: dict[str, Any] = {"symbol": pair, "interval": interval, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        payload = self._get_json("/bapi/defi/v1/public/alpha-trade/klines", params)
        if payload.get("code") != "000000" or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"unexpected Alpha klines response for {pair}")
        return payload["data"]
```

同类中按下面的确定性规则实现剩余方法；`_safe_float/_safe_int` 沿用生产脚本的 `None/""/异常 -> default` 行为：

```python
@staticmethod
def parse_trading_pairs(payload: dict[str, Any]) -> dict[str, str]:
    data = payload.get("data")
    if payload.get("code") != "000000" or not isinstance(data, dict):
        raise RuntimeError("unexpected Alpha exchange-info response")
    candidates: dict[str, tuple[int, str]] = {}
    for item in data.get("symbols", []):
        base = str(item.get("baseAsset", "")).upper().strip()
        quote = str(item.get("quoteAsset", "")).upper().strip()
        pair = str(item.get("symbol", "")).upper().strip()
        if item.get("status") != "TRADING" or not base or not pair or quote not in {"USDT", "USDC"}:
            continue
        candidate = (0 if quote == "USDT" else 1, pair)
        if base not in candidates or candidate < candidates[base]:
            candidates[base] = candidate
    return {base: value[1] for base, value in candidates.items()}

def fetch_trading_pairs(self) -> dict[str, str]:
    return self.parse_trading_pairs(self._get_json(
        "/bapi/defi/v1/public/alpha-trade/get-exchange-info"
    ))

def fetch_tokens(self) -> dict[str, AlphaToken]:
    payload = self._get_json(
        "/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    )
    if payload.get("code") != "000000" or not isinstance(payload.get("data"), list):
        raise RuntimeError("unexpected Alpha token-list response")
    pairs = self.fetch_trading_pairs()
    tokens: dict[str, AlphaToken] = {}
    for item in payload["data"]:
        symbol = str(item.get("symbol", "")).upper().strip()
        alpha_id = str(item.get("alphaId", "")).upper().strip()
        pair = pairs.get(alpha_id, "")
        if not symbol or not alpha_id or not pair:
            continue
        token = AlphaToken(symbol, alpha_id, str(item.get("name") or symbol),
                           str(item.get("chainName") or ""), _safe_float(item.get("price")),
                           _safe_float(item.get("volume24h")), _safe_int(item.get("count24h")), pair)
        if symbol not in tokens or token.volume_24h > tokens[symbol].volume_24h:
            tokens[symbol] = token
    return tokens

def fetch_ticker(self, pair: str) -> dict[str, Any]:
    payload = self._get_json("/bapi/defi/v1/public/alpha-trade/ticker", {"symbol": pair})
    if payload.get("code") != "000000" or not isinstance(payload.get("data"), dict):
        raise RuntimeError(f"unexpected Alpha ticker response for {pair}")
    return payload["data"]
```

`fetch_tokens()` 必须合并 exchange-info 的实际 USDT/USDC pair；不得重新假设 `alphaId + USDT`。

- [ ] **Step 4: 用生产 SHA 门禁移植告警脚本**

Run: `ssh srv-43-156-35-110 'sha256sum /home/ubuntu/binance-alpha-volume-alert/binance_alpha_volume_alert.py'`

Expected: SHA 仍为 `8b351fe7222a9a06d135b764403a9b9ceaffd21b18ce617fccd008a507a686c8`；不一致时停止并审阅差异。

用 `apply_patch` 创建 `alpha_volume_alert.py`，保留生产文件的 `VolumeSpike`、`find_spike()`、`format_spike()`、`send_email()`、状态文件、`parse_args_from()`、`run()` 和 `main()`；只把市场调用替换为：

```python
from .alpha_market import AlphaMarketClient, AlphaToken

MARKET = AlphaMarketClient()

def fetch_tokens() -> dict[str, AlphaToken]:
    return MARKET.fetch_tokens()

def fetch_ticker(pair: str) -> dict[str, Any]:
    return MARKET.fetch_ticker(pair)

def fetch_klines(pair: str, interval: str, limit: int) -> list[list[Any]]:
    return MARKET.fetch_klines(pair, interval=interval, limit=limit)
```

- [ ] **Step 5: 写告警兼容测试并增加 CLI**

```python
def test_alert_parser_preserves_production_defaults() -> None:
    args = parse_args_from(["--symbols", "QUID,GRVT"])
    assert (args.interval, args.baseline_candles, args.absolute_min_quote_volume, args.cooldown_minutes) == (
        "1m", 20, 60000.0, 30,
    )
```

另加两个回归：形成中的最后一根 K 线不参与 1m 告警；相同 close time 或冷却期内不重复发送。在 `pyproject.toml` 增加：

```toml
grid-alpha-volume-alert = "grid_optimizer.alpha_volume_alert:main"
grid-alpha-competition-dashboard = "grid_optimizer.alpha_competition_dashboard:main"
```

- [ ] **Step 6: 验证并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_market.py tests/test_alpha_volume_alert.py -q
git add pyproject.toml src/grid_optimizer/alpha_market.py src/grid_optimizer/alpha_volume_alert.py tests/test_alpha_market.py tests/test_alpha_volume_alert.py
git commit -m "feat: version alpha market and alert service"
```

Expected: all selected tests pass。

### Task 2: 固定官方规则夹具并实现公告解析

**Files:**
- Create: `tests/fixtures/alpha_competition_articles.json`
- Create: `tests/test_alpha_competition_metrics.py`
- Create: `src/grid_optimizer/alpha_competition_metrics.py`

- [ ] **Step 1: 创建五币种固定夹具**

夹具保存从官方 CMS 响应归一化出的稳定事实；pytest helper 再生成 CMS 形状的 `code/title/publishDate/body`，body 节点文本包含两轮 Promotion Period、top-N 和 Day 1–7。锁定这些 expected：

```json
{
  "QUID":{"code":"18d7255a59f74b3d90139c755cc806dd","publishDate":1785927611774,"winnerCount":2500,"rounds":[["2026-08-05 13:00","2026-08-12 13:00"],["2026-08-12 13:00","2026-08-19 13:00"]],"multipliers":[3.5,3.0,2.5,2.0,1.8,1.3,1.0]},
  "GRVT":{"code":"7344e0bd9d244ff89c1a1642800da931","publishDate":1785841206003,"winnerCount":2500,"rounds":[["2026-08-04 13:00","2026-08-11 13:00"],["2026-08-11 13:00","2026-08-18 13:00"]],"multipliers":[3.0,3.0,2.5,2.0,1.8,1.3,1.0]},
  "CAP":{"code":"b935934fe980452fa96722ecaf30abde","publishDate":1785411007454,"winnerCount":2000,"rounds":[["2026-07-30 13:00","2026-08-06 13:00"],["2026-08-06 13:00","2026-08-13 13:00"]],"multipliers":[2.0,2.0,1.8,1.8,1.5,1.5,1.0]},
  "PRL":{"code":"21f8a7399c2542e68452f266ecb0a8ef","publishDate":1785319516682,"winnerCount":2000,"rounds":[["2026-07-29 11:00","2026-08-05 11:00"],["2026-08-05 11:00","2026-08-12 11:00"]],"multipliers":[2.0,2.0,1.8,1.8,1.5,1.5,1.0]},
  "O":{"code":"cc164ab36d114ea2a123309c9bbed74f","publishDate":1784804407255,"winnerCount":2160,"rounds":[["2026-07-23 13:00","2026-07-30 13:00"],["2026-07-30 13:00","2026-08-06 13:00"]],"multipliers":[3.0,3.0,2.5,2.0,1.8,1.3,1.0]}
}
```

测试 helper 必须用 fixture 的完整值构造正文，而不是绕过 parser：

```python
def cms_payload(symbol: str, item: dict[str, Any]) -> dict[str, Any]:
    name = {"QUID": "Squid", "GRVT": "Grvt", "CAP": "Cap", "PRL": "Perle", "O": "o1.exchange"}[symbol]
    periods = " ".join(
        f"{index}st {symbol} Trading Competition Promotion Period: {start} (UTC) to {end} (UTC)"
        for index, (start, end) in enumerate(item["rounds"], start=1)
    ).replace("2st ", "2nd ")
    days = " ".join(
        f"Day {index} Eligible Date and Time (UTC) {multiplier}x"
        for index, multiplier in enumerate(item["multipliers"], start=1)
    )
    text = f"{periods} General Rules: The top {item['winnerCount']:,} users by purchase volume. {days}"
    return {
        "code": item["code"],
        "title": f"Binance Alpha Trading Competition: Trade {name} ({symbol}) and Share $200K Worth of Rewards",
        "publishDate": item["publishDate"],
        "body": json.dumps({"node": "document", "children": [{"node": "text", "text": text}]}),
    }
```

每个 fixture 对象必须包含上面 helper 使用的 `publishDate`，采用已核对公告响应中的毫秒值。

- [ ] **Step 2: 写解析失败测试**

```python
def test_parse_quid_article_extracts_rounds_winners_and_multipliers(article_payloads) -> None:
    rule = parse_competition_article(article_payloads["QUID"], expected_symbol="QUID")
    assert rule.winner_count == 2500
    assert rule.rounds[0].start_utc == datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)
    assert rule.multipliers == (3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)

def test_short_symbol_o_requires_exact_parenthesized_title(article_payloads) -> None:
    payload = dict(article_payloads["O"])
    payload["title"] = "Binance Alpha Trading Competition: Trade Other (OTHER) and Share Rewards"
    with pytest.raises(RuleParseError, match="title symbol"):
        parse_competition_article(payload, expected_symbol="O")

def test_rule_rejects_missing_day_or_non_seven_day_round(article_payloads) -> None:
    with pytest.raises(RuleParseError):
        parse_competition_article(without_day(article_payloads["QUID"], 7), expected_symbol="QUID")
```

- [ ] **Step 3: 运行并确认模块不存在**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -k parse -q`

Expected: collection failure。

- [ ] **Step 4: 实现模型和解析器**

```python
class RuleParseError(ValueError):
    pass

@dataclass(frozen=True)
class CompetitionRound:
    number: int
    start_utc: datetime
    end_utc: datetime

@dataclass(frozen=True)
class CompetitionRule:
    symbol: str
    name: str
    article_code: str
    title: str
    article_url: str
    published_at_utc: datetime
    winner_count: int
    rounds: tuple[CompetitionRound, ...]
    multipliers: tuple[float, ...]
```

`parse_competition_article()` 必须：递归抽取 `node == "text"`；HTML entity 解码并折叠空白；精确验证 `(<SYMBOL>) and`；解析带正确 ordinal suffix 的 Promotion Period；用 `The top\s+([\d,]+)\s+users` 同时支持 `2000` 和 `2,160`；在每个 Day 区块内提取第一个 `Nx`；验证 round number 唯一、start/end 有序、每轮恰好七天、Day 1–7 齐全且正倍速。七个连续 24 小时活动日由 round start 与 day index 构造，使用 `[start,end)`，因此完整覆盖当前轮且无重叠。构造公告 URL：

```python
article_url = f"https://www.binance.com/en/support/announcement/detail/{data['code']}"
```

- [ ] **Step 5: 运行解析测试并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -k parse -q
git add src/grid_optimizer/alpha_competition_metrics.py tests/fixtures/alpha_competition_articles.json tests/test_alpha_competition_metrics.py
git commit -m "feat: parse alpha competition rules"
```

### Task 3: 实现 CMS 自动发现和 6 小时规则缓存

**Files:**
- Modify: `src/grid_optimizer/alpha_competition_metrics.py`
- Modify: `tests/test_alpha_competition_metrics.py`

- [ ] **Step 1: 写最新公告、分页和 last-known-good 测试**

```python
def test_provider_selects_latest_exact_symbol_article(article_payloads) -> None:
    provider = fake_provider_with_articles([
        list_item(article_payloads["O"], release_date=1),
        list_item(article_payloads["QUID"], release_date=3),
        list_item(article_payloads["QUID"], code="older", release_date=2),
    ], article_payloads)
    assert provider.fetch_rule("QUID").article_code == article_payloads["QUID"]["code"]

def test_rule_cache_returns_stale_last_success_after_network_failure(tmp_path, quid_rule) -> None:
    cache = CompetitionRuleCache(tmp_path / "rules.json", ttl=timedelta(hours=6))
    cache.store(quid_rule, fetched_at=datetime(2026, 8, 7, 0, 0, tzinfo=timezone.utc))
    result = cache.get("QUID", now=datetime(2026, 8, 7, 7, 0, tzinfo=timezone.utc), loader=raising_loader)
    assert result.rule == quid_rule
    assert result.stale is True
```

再覆盖：第一页缺失时分页回溯；超过 60 天停止；损坏缓存忽略并重抓；新增 symbol cache miss 立即抓取。

- [ ] **Step 2: 实现 provider**

```python
class BinanceCompetitionRuleProvider:
    def fetch_rule(self, symbol: str, *, now: datetime | None = None) -> CompetitionRule:
        target = symbol.upper().strip()
        cutoff_ms = int(((now or datetime.now(timezone.utc)) - timedelta(days=60)).timestamp() * 1000)
        candidates: list[dict[str, Any]] = []
        for page_no in range(1, 21):
            data = self._get_data("/bapi/composite/v1/public/cms/article/list/query", {
                "type": 1, "catalogId": 93, "pageNo": page_no, "pageSize": 50,
            })
            articles = data.get("articles", []) if isinstance(data, dict) else []
            candidates.extend(item for item in articles if _title_matches_symbol(item, target))
            oldest = min((_article_release_ms(item) for item in articles), default=cutoff_ms)
            if candidates or not articles or oldest < cutoff_ms:
                break
        if not candidates:
            raise RuleParseError(f"no recent competition announcement for {target}")
        latest = max(candidates, key=_article_release_ms)
        detail = self._get_data(
            "/bapi/composite/v1/public/cms/article/detail/query", {"articleCode": latest["code"]}
        )
        return parse_competition_article(detail, expected_symbol=target)
```

`_get_data()` 使用 `requests.Session`、20 秒超时、官方 code 校验；标题同时验证固定前缀与精确 symbol。

- [ ] **Step 3: 实现原子规则缓存**

```python
@dataclass(frozen=True)
class CachedRuleResult:
    rule: CompetitionRule
    stale: bool

class CompetitionRuleCache:
    def __init__(self, path: Path, *, ttl: timedelta = timedelta(hours=6)) -> None:
        self.path = path
        self.ttl = ttl

    def get(self, symbol: str, *, now: datetime, loader: Callable[[str], CompetitionRule]) -> CachedRuleResult:
        state = self._load()
        cached = state.get(symbol)
        if cached is not None and now - cached["fetched_at"] < self.ttl:
            return CachedRuleResult(cached["rule"], False)
        try:
            rule = loader(symbol)
        except Exception:
            if cached is None:
                raise
            return CachedRuleResult(cached["rule"], True)
        state[symbol] = {"fetched_at": now, "rule": rule}
        self._save(state)
        return CachedRuleResult(rule, False)

    def store(self, rule: CompetitionRule, *, fetched_at: datetime) -> None:
        state = self._load()
        state[rule.symbol] = {"fetched_at": fetched_at, "rule": rule}
        self._save(state)
```

`_load/_save` 显式编解码 dataclass；JSON 带 `version: 1`；写同目录临时文件后 `os.replace()`。

- [ ] **Step 4: 验证并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -k 'provider or rule_cache' -q
git add src/grid_optimizer/alpha_competition_metrics.py tests/test_alpha_competition_metrics.py
git commit -m "feat: discover and cache alpha competition rules"
```

### Task 4: 实现当前轮、早鸟倍速、加权量与三档门槛

**Files:**
- Modify: `src/grid_optimizer/alpha_competition_metrics.py`
- Modify: `tests/test_alpha_competition_metrics.py`

- [ ] **Step 1: 写半开区间和非自然日加权测试**

```python
def test_select_round_uses_half_open_boundaries(quid_rule) -> None:
    first = select_round(quid_rule, datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc))
    second = select_round(quid_rule, datetime(2026, 8, 12, 13, 0, tzinfo=timezone.utc))
    ended = select_round(quid_rule, datetime(2026, 8, 19, 13, 0, tzinfo=timezone.utc))
    assert (first.status, first.round.number, first.day, first.multiplier) == ("active", 1, 1, 3.5)
    assert (second.status, second.round.number, second.day, second.multiplier) == ("active", 2, 1, 3.5)
    assert ended.status == "ended"

def test_weighted_volume_groups_11_utc_day_and_deduplicates_open_time(prl_rule) -> None:
    rows = [
        kline("2026-08-05 11:00", quote_volume=100),
        kline("2026-08-05 11:00", quote_volume=999),
        kline("2026-08-06 10:00", quote_volume=200),
        kline("2026-08-06 11:00", quote_volume=300),
    ]
    result = weight_kline_volume(prl_rule.rounds[1], prl_rule.multipliers, rows)
    assert result == pytest.approx(100 * 2.0 + 200 * 2.0 + 300 * 2.0)

def test_thresholds_exclude_personal_1_2x() -> None:
    metrics = calculate_thresholds(weighted_volume=4_800_000, winner_count=2500)
    assert (metrics.average, metrics.watch, metrics.reference, metrics.safe) == pytest.approx(
        (1920, 768, 1152, 1920)
    )
```

- [ ] **Step 2: 写 K 线参数和官方总量优先测试**

```python
def test_volume_provider_uses_start_time_end_time_and_limit_200(quid_rule) -> None:
    market = FakeMarketClient(rows=[kline("2026-08-05 13:00", quote_volume=100)])
    result = CompetitionVolumeProvider(market=market).fetch(quid_rule, quid_rule.rounds[0], NOW)
    assert result.source == "alpha_kline_estimate"
    assert market.calls[0] == {
        "pair": "ALPHA_1075USDC", "interval": "1h", "limit": 200,
        "start_time_ms": int(quid_rule.rounds[0].start_utc.timestamp() * 1000),
        "end_time_ms": int(NOW.timestamp() * 1000),
    }

def test_verified_official_total_wins_without_klines(quid_rule) -> None:
    official = lambda rule, round_, now: OfficialVolumeSnapshot(9_000_000, now)
    market = FakeMarketClient(rows=[])
    result = CompetitionVolumeProvider(market=market, official_fetcher=official).fetch(
        quid_rule, quid_rule.rounds[0], NOW
    )
    assert (result.source, result.weighted_volume, market.calls) == ("official", 9_000_000, [])
```

默认 `official_fetcher=None`，代表当前没有已验证的官方总量字段；不得猜测接口或把奖励池数值当交易量。

- [ ] **Step 3: 运行并确认计算函数缺失**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -k 'round or weighted or threshold or volume_provider' -q`

Expected: selected tests fail on missing functions。

- [ ] **Step 4: 实现纯计算函数**

```python
@dataclass(frozen=True)
class Thresholds:
    average: float
    watch: float
    reference: float
    safe: float

@dataclass(frozen=True)
class OfficialVolumeSnapshot:
    weighted_volume: float
    updated_at_utc: datetime

@dataclass(frozen=True)
class VolumeSnapshot:
    weighted_volume: float
    source: str
    updated_at_utc: datetime

@dataclass(frozen=True)
class RoundSelection:
    status: str
    round: CompetitionRound | None
    day: int | None
    multiplier: float | None

def select_round(rule: CompetitionRule, now: datetime) -> RoundSelection:
    ordered = sorted(rule.rounds, key=lambda item: item.start_utc)
    if now < ordered[0].start_utc:
        return RoundSelection("upcoming", None, None, None)
    for item in ordered:
        if item.start_utc <= now < item.end_utc:
            day = int((now - item.start_utc).total_seconds() // 86400) + 1
            return RoundSelection("active", item, day, rule.multipliers[day - 1])
    if any(left.end_utc <= now < right.start_utc for left, right in zip(ordered, ordered[1:])):
        return RoundSelection("between_rounds", None, None, None)
    return RoundSelection("ended", None, None, None)

def weight_kline_volume(round_: CompetitionRound, multipliers: tuple[float, ...],
                        rows: list[list[Any]]) -> float:
    seen: set[int] = set()
    total = 0.0
    for row in rows:
        open_ms = int(row[0])
        if open_ms in seen:
            continue
        seen.add(open_ms)
        opened = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
        if not round_.start_utc <= opened < round_.end_utc:
            continue
        day_index = int((opened - round_.start_utc).total_seconds() // 86400)
        total += float(row[7]) * multipliers[day_index]
    return total

def calculate_thresholds(*, weighted_volume: float, winner_count: int) -> Thresholds:
    if weighted_volume < 0 or winner_count <= 0:
        raise ValueError("weighted volume and winner count must be valid")
    average = weighted_volume / winner_count
    return Thresholds(average=average, watch=average * 0.4, reference=average * 0.6, safe=average)
```

- [ ] **Step 5: 实现 volume provider**

`CompetitionVolumeProvider.fetch()` 先调用 injected verified official fetcher；返回 `None` 才用 `rule.symbol` 获取实际 token pair，并用 `startTime/endTime/limit=200` 请求 1h K 线。`end_time_ms=min(now, round.end)`；形成中的 K 线按当前 quote volume 计入；`updated_at_utc` 使用成功请求时间，不使用未来 candle close time。

- [ ] **Step 6: 验证并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -k 'round or weighted or threshold or volume_provider' -q
git add src/grid_optimizer/alpha_competition_metrics.py tests/test_alpha_competition_metrics.py
git commit -m "feat: calculate alpha competition thresholds"
```

### Task 5: 实现 60 秒结果缓存和逐币种错误隔离

**Files:**
- Modify: `src/grid_optimizer/alpha_competition_metrics.py`
- Modify: `tests/test_alpha_competition_metrics.py`

- [ ] **Step 1: 写服务级缓存与隔离测试**

```python
def test_service_keeps_symbol_order_and_isolates_one_rule_failure() -> None:
    service = make_service(rules={"QUID": quid_rule, "GRVT": RuntimeError("cms down")})
    payload = service.collect(["QUID", "GRVT"], now=NOW)
    assert [row["symbol"] for row in payload["rows"]] == ["QUID", "GRVT"]
    assert payload["rows"][0]["status"] == "active"
    assert payload["rows"][1]["status"] == "rule_unavailable"
    assert payload["rows"][1]["weightedVolume"] is None

def test_service_caches_volume_for_sixty_seconds() -> None:
    service, volume = make_counting_service()
    service.collect(["QUID"], now=NOW)
    service.collect(["QUID"], now=NOW + timedelta(seconds=59))
    service.collect(["QUID"], now=NOW + timedelta(seconds=60))
    assert volume.calls == 2

def test_inactive_round_does_not_fetch_volume() -> None:
    service, volume = make_counting_service()
    row = service.collect(["QUID"], now=BEFORE)["rows"][0]
    assert row["status"] == "upcoming"
    assert row["weightedVolume"] is None
    assert volume.calls == 0
```

- [ ] **Step 2: 运行并确认 service 缺失**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -k service -q`

- [ ] **Step 3: 实现线程安全服务**

```python
class CompetitionMetricsService:
    def __init__(self, *, rule_provider, rule_cache, volume_provider,
                 volume_ttl: timedelta = timedelta(seconds=60)) -> None:
        self.rule_provider = rule_provider
        self.rule_cache = rule_cache
        self.volume_provider = volume_provider
        self.volume_ttl = volume_ttl
        self._volume_cache: dict[tuple[str, int], tuple[datetime, VolumeSnapshot]] = {}
        self._lock = threading.Lock()

    def collect(self, symbols: list[str], *, now: datetime | None = None) -> dict[str, Any]:
        current = now or datetime.now(timezone.utc)
        rows, errors = [], []
        with self._lock:
            for symbol in symbols:
                try:
                    cached_rule = self.rule_cache.get(
                        symbol, now=current,
                        loader=lambda value: self.rule_provider.fetch_rule(value, now=current),
                    )
                    rows.append(self._build_row(cached_rule, current))
                except Exception as exc:
                    message = f"{symbol}: {exc}"
                    errors.append(message)
                    rows.append(_unavailable_row(symbol, "rule_unavailable", message))
        return {"generatedAtUtc": current.isoformat(timespec="seconds"), "rows": rows, "errors": errors}
```

`_build_row()` 对非 active 状态返回规则元数据但量和门槛均为 `None`。active 按 `(symbol, round.number)` 缓存 60 秒；volume 失败且有旧值时返回旧值并设 `stale=true`，没有旧值时返回 `volume_unavailable`，不能返回默认零。

输出字段固定为：`symbol/name/round/day/roundStartUtc/roundEndUtc/currentMultiplier/weightedVolume/volumeSource/volumeUpdatedAtUtc/winnerCount/averageVolume/watchThreshold/referenceThreshold/safeThreshold/articleUrl/stale/status/error`。

- [ ] **Step 4: 验证并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_metrics.py -q
git add src/grid_optimizer/alpha_competition_metrics.py tests/test_alpha_competition_metrics.py
git commit -m "feat: serve cached alpha competition metrics"
```

### Task 6: 迁移 Dashboard 后端并增加 `/api/competition`

**Files:**
- Create: `src/grid_optimizer/alpha_competition_dashboard.py`
- Create: `tests/test_alpha_competition_dashboard.py`

- [ ] **Step 1: 写现有行为和新端点失败测试**

```python
def test_snapshot_preserves_production_fields(fake_market) -> None:
    row = collect_snapshot(["QUID"], market=fake_market)["rows"][0]
    assert set(row) >= {
        "symbol", "name", "alphaId", "pair", "lastPrice", "latest1mQuoteVolume",
        "previous1mQuoteVolume", "delta1mQuoteVolume", "latest1hQuoteVolume",
        "baselineQuoteVolume", "multiple", "trades", "closedUtc",
        "tickerQuoteVolume24h", "priceChangePercent24h",
    }

def test_api_competition_uses_configured_symbol_order(http_server) -> None:
    response = http_server.get("/api/competition", auth=True)
    assert response.status_code == 200
    assert [row["symbol"] for row in response.json()["rows"]] == ["QUID", "GRVT", "O", "PRL", "CAP"]

def test_all_routes_require_basic_auth(http_server) -> None:
    for method, path in [("GET", "/"), ("GET", "/api/snapshot"),
                         ("GET", "/api/competition"), ("POST", "/api/check")]:
        assert http_server.request(method, path, auth=False).status_code == 401

def test_check_alert_delegates_to_versioned_alert_module() -> None:
    with patch("grid_optimizer.alpha_competition_dashboard.alert.run", return_value=0) as run:
        result = check_alert_once()
    assert result["ok"] is True
    run.assert_called_once()
```

`http_server` fixture 在 `127.0.0.1:0` 启动真实 `ThreadingHTTPServer`，后台线程运行 `serve_forever()`，并注入 fake competition service；fixture teardown 必须调用 `shutdown()` 和 `server_close()`，确保测试不残留端口或线程。

- [ ] **Step 2: 运行并确认 Dashboard 模块不存在**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_dashboard.py -q`

- [ ] **Step 3: 移植现有快照、告警和 Basic Auth**

重新验证 `dashboard.py` SHA；用 `apply_patch` 创建模块，保留 `_symbols_from_env()`、`collect_snapshot()`、`check_alert_once()`、Basic Auth、`/api/snapshot` 与 `/api/check`。导入改为：

```python
from . import alpha_volume_alert as alert
from .alpha_market import AlphaMarketClient
from .alpha_competition_metrics import (
    BinanceCompetitionRuleProvider, CompetitionMetricsService,
    CompetitionRuleCache, CompetitionVolumeProvider,
)
```

`collect_snapshot(symbols, market=None)` 支持注入客户端；生产默认用 `AlphaMarketClient`，仍按 `multiple` 降序。

- [ ] **Step 4: 构造惰性 service 并接路由**

```python
def competition_service() -> CompetitionMetricsService:
    global _COMPETITION_SERVICE
    with _COMPETITION_SERVICE_LOCK:
        if _COMPETITION_SERVICE is None:
            market = AlphaMarketClient()
            cache_path = Path(os.environ.get(
                "ALPHA_COMPETITION_RULE_CACHE",
                "/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json",
            ))
            _COMPETITION_SERVICE = CompetitionMetricsService(
                rule_provider=BinanceCompetitionRuleProvider(),
                rule_cache=CompetitionRuleCache(cache_path),
                volume_provider=CompetitionVolumeProvider(market=market),
            )
        return _COMPETITION_SERVICE
```

`Handler.do_GET()` 增加：

```python
if parsed.path == "/api/competition":
    self._send_json(competition_service().collect(_symbols_from_env()))
    return
```

新端点走同一 `_require_auth()` 和 `Cache-Control: no-store, max-age=0`。

- [ ] **Step 5: 验证并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_dashboard.py -k 'snapshot or api or auth' -q
git add src/grid_optimizer/alpha_competition_dashboard.py tests/test_alpha_competition_dashboard.py
git commit -m "feat: add alpha competition dashboard API"
```

### Task 7: 实现 B 方案桌面表格和移动卡片

**Files:**
- Modify: `src/grid_optimizer/alpha_competition_dashboard.py`
- Modify: `tests/test_alpha_competition_dashboard.py`

- [ ] **Step 1: 写 HTML/CSS/JS 合同失败测试**

```python
def test_page_has_competition_section_above_market_section() -> None:
    assert INDEX_HTML.index('id="competitionSection"') < INDEX_HTML.index('id="marketSection"')
    for marker in ('id="competitionRows"', 'class="competition-table"',
                   'data-label="观察线"', 'data-label="参考线"', 'data-label="安全线"',
                   'Alpha K线估算', '不含新锐交易者个人 1.2x 加成',
                   'formatCountdown', 'volumeUpdatedAtUtc', 'articleUrl'):
        assert marker in INDEX_HTML

def test_page_refreshes_two_apis_independently() -> None:
    assert "Promise.allSettled" in INDEX_HTML
    assert "api/snapshot" in INDEX_HTML and "api/competition" in INDEX_HTML
    assert "renderMarket" in INDEX_HTML and "renderCompetition" in INDEX_HTML

def test_mobile_rows_become_cards() -> None:
    assert ".competition-table tbody tr" in INDEX_HTML
    assert "content: attr(data-label)" in INDEX_HTML
```

- [ ] **Step 2: 运行并确认上方区域不存在**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_dashboard.py -k 'page or mobile' -q`

- [ ] **Step 3: 增加上方交易赛结构**

```html
<section id="competitionSection" class="section-block">
  <div class="section-heading">
    <div><h2>Alpha 交易赛门槛</h2><p>按当前轮累计；门槛为总量启发式估算，不含新锐交易者个人 1.2x 加成。</p></div>
    <div id="competitionGenerated" class="source-time">加载中…</div>
  </div>
  <div class="table-wrap competition-wrap">
    <table class="competition-table">
      <thead><tr><th>币种</th><th>轮次</th><th>当前倍速</th><th>加权总量</th><th>获奖人数</th><th>平均量</th><th>观察线</th><th>参考线</th><th>安全线</th><th>来源</th></tr></thead>
      <tbody id="competitionRows"></tbody>
    </table>
  </div>
  <div id="competitionErrors" class="errors"></div>
</section>
<section id="marketSection" class="section-block">
  <!-- 把现有 KPI、行情表、status 和 errors 原样放入 -->
</section>
```

- [ ] **Step 4: 增加移动卡片 CSS**

```css
.competition-table { min-width: 1120px; }
.source-badge { display:inline-flex; border-radius:999px; padding:3px 8px; background:#e8f1ff; color:#175cd3; font-weight:700; }
.source-badge.official { background:#e7f6ec; color:#067647; }
.source-badge.stale { background:#fff4e5; color:var(--warn); }
.threshold-watch { color:#175cd3; }
.threshold-reference { color:var(--warn); font-weight:700; }
.threshold-safe { color:var(--good); font-weight:700; }

@media (max-width: 760px) {
  .competition-wrap { overflow:visible; background:transparent; border:0; }
  .competition-table { min-width:0; display:block; }
  .competition-table thead { display:none; }
  .competition-table tbody { display:grid; gap:10px; }
  .competition-table tbody tr { display:block; background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:8px 12px; }
  .competition-table tbody td { display:grid; grid-template-columns:92px minmax(0,1fr); gap:12px; width:100%; padding:8px 0; text-align:right; white-space:normal; overflow-wrap:anywhere; }
  .competition-table tbody td::before { content:attr(data-label); color:var(--muted); text-align:left; font-size:12px; font-weight:700; }
}
```

- [ ] **Step 5: 独立刷新并渲染两个 API**

实现 `escapeHtml()`、`formatU()`、`formatCountdown()`、`renderCompetition()` 和 `renderMarket()`；每个 competition `td` 带中文 `data-label`。轮次单元格显示 `第 N 轮 · Day N · 剩余时间`；来源单元格显示 `官方` 或 `Alpha K线估算`、`volumeUpdatedAtUtc`、stale 提示和可点击的 `articleUrl` 官方公告链接。非 active 状态显示明确文本，不显示 `0 U`。

```javascript
async function refresh() {
  refreshBtn.disabled = true;
  const [marketResult, competitionResult] = await Promise.allSettled([
    fetchJson('api/snapshot'), fetchJson('api/competition'),
  ]);
  if (marketResult.status === 'fulfilled') renderMarket(marketResult.value);
  else statusEl.textContent = `行情刷新失败: ${marketResult.reason}`;
  if (competitionResult.status === 'fulfilled') renderCompetition(competitionResult.value);
  else competitionErrorsEl.textContent = `交易赛刷新失败: ${competitionResult.reason}`;
  refreshBtn.disabled = false;
}
```

`Check Alert` 仍只 POST `api/check`；完成后调用 `refresh()`，不能让 competition 请求失败遮盖告警结果。

- [ ] **Step 6: 验证并提交**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_competition_dashboard.py -q
git add src/grid_optimizer/alpha_competition_dashboard.py tests/test_alpha_competition_dashboard.py
git commit -m "feat: add responsive alpha threshold layout"
```

### Task 8: 增加 systemd 安装与失败回滚脚本

**Files:**
- Create: `deploy/oracle/install_alpha_competition_dashboard.sh`
- Create: `tests/test_install_alpha_competition_dashboard.py`

- [ ] **Step 1: 写安装脚本合同测试**

```python
def test_installer_runs_repo_module_and_keeps_existing_env_files() -> None:
    text = Path("deploy/oracle/install_alpha_competition_dashboard.sh").read_text()
    assert "-m grid_optimizer.alpha_competition_dashboard" in text
    assert "/home/ubuntu/.config/wangge/grid_web_controller.env" in text
    assert "/home/ubuntu/.config/binance-alpha-volume-alert.env" in text
    assert "ALPHA_COMPETITION_RULE_CACHE" in text
    assert "systemctl is-active --quiet" in text
    assert "rollback" in text
```

- [ ] **Step 2: 运行并确认脚本不存在**

Run: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_install_alpha_competition_dashboard.py -q`

- [ ] **Step 3: 实现安装和回滚**

```bash
#!/usr/bin/env bash
set -euo pipefail
APP_DIR="${APP_DIR:-/home/ubuntu/wangge-alpha-dashboard}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
SERVICE_NAME="${SERVICE_NAME:-binance-alpha-dashboard}"
SERVICE_USER="${SERVICE_USER:-ubuntu}"
HOST="${ALPHA_DASHBOARD_HOST:-127.0.0.1}"
PORT="${ALPHA_DASHBOARD_PORT:-8796}"
RULE_CACHE="${ALPHA_COMPETITION_RULE_CACHE:-/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json}"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
BACKUP_PATH="${UNIT_PATH}.backup.$(date -u +%Y%m%dT%H%M%SZ)"

test -d "${APP_DIR}/src/grid_optimizer"
test -x "${PYTHON_BIN}"
sudo install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" "$(dirname "${RULE_CACHE}")"
if sudo test -f "${UNIT_PATH}"; then sudo cp -a "${UNIT_PATH}" "${BACKUP_PATH}"; fi
rollback() {
  if sudo test -f "${BACKUP_PATH}"; then
    sudo cp -a "${BACKUP_PATH}" "${UNIT_PATH}"
    sudo systemctl daemon-reload
    sudo systemctl restart "${SERVICE_NAME}.service"
  fi
}
trap rollback ERR
```

写入 unit 时固定保留两个现有 `EnvironmentFile`，设置 `PYTHONPATH=${APP_DIR}/src` 和 `ALPHA_COMPETITION_RULE_CACHE`，以 `${PYTHON_BIN} -m grid_optimizer.alpha_competition_dashboard --host ${HOST} --port ${PORT}` 启动。随后 `daemon-reload`、enable、restart、`systemctl is-active --quiet`；成功后移除 ERR trap。末尾只输出 unit/backup 路径和 status，不输出环境变量值。

- [ ] **Step 4: 校验并提交**

```bash
bash -n deploy/oracle/install_alpha_competition_dashboard.sh
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_install_alpha_competition_dashboard.py -q
git add deploy/oracle/install_alpha_competition_dashboard.sh tests/test_install_alpha_competition_dashboard.py
git commit -m "ops: install alpha competition dashboard"
```

### Task 9: 全量验证、本地视觉验收和生产部署

**Files:**
- Verify: all files listed above
- Modify only if verification exposes a defect: the directly responsible file and test

- [ ] **Step 1: 运行聚焦测试**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest \
  tests/test_alpha_market.py tests/test_alpha_volume_alert.py \
  tests/test_alpha_competition_metrics.py tests/test_alpha_competition_dashboard.py \
  tests/test_install_alpha_competition_dashboard.py -q
```

Expected: all focused tests pass。

- [ ] **Step 2: 运行完整验证**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest -q
python3 -m compileall -q src/grid_optimizer/alpha_market.py src/grid_optimizer/alpha_volume_alert.py src/grid_optimizer/alpha_competition_metrics.py src/grid_optimizer/alpha_competition_dashboard.py
bash -n deploy/oracle/install_alpha_competition_dashboard.sh
git diff --check
```

Expected: all checks pass。若全套有与本改动无关的既有失败，记录精确测试名和基线证据，不跳过聚焦测试。

- [ ] **Step 3: 本地启动并验证 API**

```bash
ALPHA_SYMBOLS=QUID,GRVT,O,PRL,CAP ALPHA_DASHBOARD_HOST=127.0.0.1 \
ALPHA_DASHBOARD_PORT=8796 ALPHA_COMPETITION_RULE_CACHE=/tmp/wangge-alpha-rules.json \
PYTHONPATH=src .venv/bin/python -m grid_optimizer.alpha_competition_dashboard
```

Expected: `/api/snapshot` 与 `/api/competition` 都返回 JSON；competition 顺序为 `QUID,GRVT,O,PRL,CAP`；QUID pair 为 `ALPHA_1075USDC`；每币种都有 active/inactive/error 明确状态。

- [ ] **Step 4: 浏览器桌面和移动验收**

用约 `1440×900` 和 `390×844` 视口验证：桌面交易赛表在上、行情表在下；移动交易赛逐币种成卡；无文字竖排、裁切或重叠；任一 API 失败时另一块仍显示；Refresh 更新两块；Check Alert 只请求 `/api/check`。截图保留作验收证据，不加入 Git。

- [ ] **Step 5: 确认 main 和 origin 关系**

```bash
git status --short --branch
git fetch origin
git rev-list --left-right --count origin/main...main
git log --oneline --decorate -10
```

Expected: clean `main`，只有本功能提交，无无关文件。`git rev-list` 左侧必须为 `0`；若 origin/main 已领先或双方分叉，停止部署并先按项目规则同步，禁止 force push。

- [ ] **Step 6: 推送并创建隔离生产检出**

```bash
git push origin main
ssh srv-43-156-35-110 'git ls-remote --exit-code https://github.com/t86/grid_trading.git refs/heads/main'
ssh srv-43-156-35-110 'git clone --branch main --single-branch https://github.com/t86/grid_trading.git /home/ubuntu/wangge-alpha-dashboard'
```

若隔离目录已存在，只执行：

```bash
ssh srv-43-156-35-110 'cd /home/ubuntu/wangge-alpha-dashboard && git fetch origin main && git merge --ff-only origin/main'
```

Expected: 隔离目录是 clean main；旧 `/home/ubuntu/wangge` branch、commit 和未跟踪文件完全不变。

- [ ] **Step 7: 远端先测再改 unit**

```bash
ssh srv-43-156-35-110 'cd /home/ubuntu/wangge-alpha-dashboard && python3 -m venv .venv && .venv/bin/python -m pip install -e . pytest && PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_alpha_market.py tests/test_alpha_volume_alert.py tests/test_alpha_competition_metrics.py tests/test_alpha_competition_dashboard.py tests/test_install_alpha_competition_dashboard.py -q'
```

Expected: focused tests pass；systemd 尚未变化。

- [ ] **Step 8: 安装服务并验证认证 API**

```bash
ssh srv-43-156-35-110 'cd /home/ubuntu/wangge-alpha-dashboard && APP_DIR=/home/ubuntu/wangge-alpha-dashboard bash deploy/oracle/install_alpha_competition_dashboard.sh'
ssh srv-43-156-35-110 'set -a; . /home/ubuntu/.config/wangge/grid_web_controller.env; set +a; curl -fsS -u "$GRID_WEB_USERNAME:$GRID_WEB_PASSWORD" http://127.0.0.1:8796/api/snapshot >/tmp/alpha-snapshot.json; curl -fsS -u "$GRID_WEB_USERNAME:$GRID_WEB_PASSWORD" http://127.0.0.1:8796/api/competition >/tmp/alpha-competition.json; python3 -m json.tool /tmp/alpha-snapshot.json >/dev/null; python3 -m json.tool /tmp/alpha-competition.json >/dev/null; systemctl is-active binance-alpha-dashboard.service'
```

Expected: JSON valid，service `active`。验证后只删除两个明确的临时响应：

```bash
ssh srv-43-156-35-110 'rm -- /tmp/alpha-snapshot.json /tmp/alpha-competition.json'
```

- [ ] **Step 9: 验证公网页面和日志**

在已认证浏览器打开 `http://43.156.35.110/alpha/` 重复两种视口检查。没有用户明确授权时不得触发真实邮件；仅在 `ALPHA_DASHBOARD_DRY_RUN=1` 时点击 Check Alert。检查：

```bash
ssh srv-43-156-35-110 'journalctl -u binance-alpha-dashboard.service --since "10 minutes ago" --no-pager | tail -100'
```

Expected: no traceback；五币种逐行显示结果或明确错误；原行情仍更新。

- [ ] **Step 10: 记录成功证据或回滚**

成功时记录本地/远端 commit、unit backup 路径、API 验证时间和两种视口结果。失败时使用安装脚本输出的精确 backup 路径恢复旧 unit，执行 `systemctl daemon-reload && systemctl restart binance-alpha-dashboard.service` 并确认旧页面恢复。保留隔离目录供诊断，不删除旧源码或 backup。

## 完成定义

- QUID、GRVT、O、PRL、CAP 按 `ALPHA_SYMBOLS` 顺序出现。
- 当前轮、Day、早鸟倍速、获奖人数来自最新可验证公告。
- 加权总量按 1h K 线 open time、官方日界和当日倍速累计。
- 平均量及 `0.4/0.6/1.0` 正确，页面明确标注估算且不含个人 1.2x。
- 规则缓存 6 小时、成交量缓存 60 秒；网络失败保留 last-known-good 并标记 stale。
- 单币种失败不影响其他币种；单 API 失败不影响另一块页面。
- 桌面上下分区、移动卡片无挤压；原行情和 Check Alert 行为不变。
- 代码、测试和部署脚本均在 `main`，110 旧脏工作树未被覆盖，回滚路径已验证。
