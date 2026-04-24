# Runner Websocket Market Feed Design

## Goal

为当前 futures `loop_runner` 增加一个进程内 websocket 行情源，先把每轮高频市场 REST 里的 `bookTicker` 和 `premiumIndex` 挪走，并同时把 `exchangeInfo(symbol)` 做成长 TTL 缓存，让单机同 IP 下的多币并发更接近可用状态。

本次设计的目标不是“彻底重构 runner”，而是交付一个最小、可回退、可逐步上线的市场数据层改造，使 `114` 上后续 `4-6` 个币的并发压测成为可能。

## Scope

本次只覆盖 futures runner 自身的市场数据读取路径：

- 为每个 `loop_runner` 进程新增一个只订阅当前 `symbol` 的 websocket 行情源
- websocket 提供：
  - 最佳买一卖一
  - mark price
  - funding rate
  - next funding time
- `generate_plan_report()` 优先读取 websocket 快照，缺失时回退 REST
- `execute_plan_report()` 优先读取 websocket 快照，缺失时回退 REST
- `fetch_futures_symbol_config()` 增加进程内长 TTL 缓存
- 为 websocket 冷启动、断线、快照过期、REST 回退补测试

## Non-Goals

本次明确不做：

- 不新增跨进程共享 market sidecar
- 不把多个 runner 合并成单进程统一调度器
- 不改 signed account / open orders / position mode 的缓存策略
- 不改 monitor 页面或 web service 的接口协议
- 不引入完整 async/await runner 重写
- 不在本次直接承诺“多赛道全开”，只完成进入压测所需的最小改造

## Current State

当前 `loop_runner` 是每个 symbol 独立一个同步死循环：

1. `generate_plan_report()` 每轮会读：
   - `fetch_futures_symbol_config(symbol)`
   - `fetch_futures_book_tickers(symbol=symbol)`
   - `fetch_futures_premium_index(symbol=symbol)`
2. `execute_plan_report()` 执行前会再读一次 `bookTicker`
3. maker 重试时会按 attempt 再次读盘口

前一轮已经把 `execute_plan_report()` 的首次重复盘口读取去掉，但剩余问题仍然明显：

- `generate_plan_report()` 依然每轮打市场 REST
- `fetch_futures_symbol_config(symbol)` 实际是 `exchangeInfo(symbol)`，本质是静态元数据，不应每轮请求
- 当前代码没有任何 websocket 基础设施
- runner 是同步结构，如果直接引入 async runner，会把改动面抬得太大

## Recommended Approach

采用每个 runner 进程自带一个轻量 websocket 行情流，而不是先做共享 sidecar。

推荐原因：

1. 最贴合当前“一币一进程”的运行模型，不需要改 systemd / runner wrapper 形态
2. 故障边界清楚，某个 symbol 的 websocket 断开不会影响其他 runner
3. 回退路径简单，可以随时退回当前 REST 行为
4. 改动集中在 `data.py` 和 `loop_runner.py`，便于快速上线和验证

不推荐本次直接做共享 sidecar，因为那会同时引入：

- 新常驻服务
- 进程间通信
- 部署编排和服务监控
- 多 symbol 生命周期管理

这些都不属于本次“先把 114 的多币压测跑起来”的最短路径。

## Dependency Choice

本次新增依赖建议使用 `websocket-client`，不使用 `websockets`。

原因：

- 当前 runner 是同步/线程模型，`websocket-client` 更容易以后台线程方式嵌入
- 不需要把 `loop_runner` 改成 async 主循环
- 重连、回调、关闭都可以封装在一个小对象里完成

计划在 `pyproject.toml` 中新增：

```toml
dependencies = [
  "requests>=2.31",
  "websocket-client>=1.8,<2",
]
```

## Architecture

### 1. Process-Local Market Stream

在 `src/grid_optimizer/data.py` 中新增一个小型行情流对象，建议命名为：

- `FuturesMarketStream`

职责：

- 只服务单个 `symbol`
- 在后台线程订阅 Binance USD-M websocket
- 维护最近一次完整市场快照
- 提供线程安全的读取接口
- 提供健康状态、最后更新时间、最后错误

建议内部状态：

- `symbol`
- `thread`
- `stop_event`
- `latest_snapshot`
- `latest_snapshot_at`
- `last_error`
- `last_message_at`
- `connection_state`
- `lock`

### 2. Stream Payload Shape

统一为 runner 内部使用的标准结构：

```python
{
  "symbol": "BTCUSDC",
  "bid_price": 0.0,
  "ask_price": 0.0,
  "mark_price": 0.0,
  "funding_rate": 0.0,
  "next_funding_time": 0,
  "book_time": 0,
  "mark_time": 0,
  "snapshot_at": 0.0,
  "source": "websocket",
}
```

其中：

- `bid_price/ask_price` 来自 `bookTicker`
- `mark_price/funding_rate/next_funding_time` 来自 `markPrice`
- `snapshot_at` 使用本地 `time.monotonic()`

### 3. REST Fallback

新增一个统一的市场快照获取器，优先级固定为：

1. websocket 快照可用且未过期
2. REST 回退读取
3. 如果两者都不可用，则抛错

所谓“可用且未过期”定义为：

- book 数据存在
- mark 数据存在
- 本地快照年龄不超过短 TTL

建议 TTL：

- websocket 市场快照：`<= 3s`
- 执行期 maker retry 的快照重用：保留当前 `0.25s`
- `symbol_config` 长缓存：`6h`

## Data Flow

### Runner Startup

1. `main()` 解析参数
2. 为当前 symbol 初始化 `FuturesMarketStream`
3. 先尝试启动 websocket
4. websocket 尚未准备好时，runner 仍可依赖 REST 启动
5. 将 stream 对象挂到 runner 可复用上下文中

### generate_plan_report()

现状：

- 每轮直接读 `symbol_config + bookTicker + premiumIndex`

改造后：

1. `symbol_config` 从长 TTL 缓存读取
2. 市场快照优先从 websocket 读取
3. 若 websocket 冷启动或过期，则回退一次 REST
4. 计划生成继续只消费统一的 `bid/ask/mark/funding` 结构，不感知底层来源

### execute_plan_report()

现状：

- 执行前再拉一次 `bookTicker`
- maker 重试再拉盘口

改造后：

1. 执行入口优先读取统一市场快照
2. 首次执行使用当前快照
3. maker retry 时优先重新读取 websocket 最新快照
4. 只有 websocket 不可用时才回退 REST

## Binance Stream Choice

每个 runner 订阅两个 symbol 级流：

- `<symbol>@bookTicker`
- `<symbol>@markPrice`

不订阅 depth，不订阅全市场 multiplex。

原因：

- 我们只需要买一卖一和 mark/funding 数据
- 这两个流已经足够覆盖当前 `bookTicker + premiumIndex` 的 runner 用途
- 单 symbol 双流的消息量对当前进程模型可接受

## Failure Handling

### Websocket Cold Start

runner 刚启动时，websocket 可能尚未收到首条消息。

处理策略：

- 允许短时间直接回退 REST
- 不阻塞整个 runner 启动

### Disconnect / Reconnect

当 websocket 断线：

- 标记 `connection_state=disconnected`
- 记录 `last_error`
- 后台线程按退避重连
- runner 读取行情时自动回退 REST

### Stale Snapshot

如果快照超过 TTL：

- 不继续把旧 websocket 数据当成新行情
- 直接走 REST 回退
- 同时保留 stale 标记用于调试

### Partial Snapshot

如果只收到 `bookTicker` 或只收到 `markPrice`：

- 不视为可用完整快照
- 继续回退 REST

## Testing Strategy

本次按 TDD 做三层测试。

### Unit Tests

新增针对市场流状态机和统一快照读取器的测试：

- websocket 快照完整时优先返回 websocket
- websocket 缺字段时回退 REST
- websocket 过期时回退 REST
- websocket 重连前后状态切换正确
- `symbol_config` 长缓存命中时不重复打 `exchangeInfo`

### Runner Tests

扩展 `tests/test_loop_runner.py`：

- `generate_plan_report()` 在 websocket 可用时不再调用 `fetch_futures_book_tickers`
- `generate_plan_report()` 在 websocket 可用时不再调用 `fetch_futures_premium_index`
- `execute_plan_report()` 首次执行优先用 websocket 快照
- maker retry 时优先使用刷新后的 websocket 快照
- websocket 不可用时仍保持当前 REST 兼容行为

### Local Integration Check

实现后本地至少要验证：

- runner 在无 websocket 网络异常时仍可运行
- runner 在 websocket 可用时计划和执行仍能推进
- 关键测试文件全部通过

## Rollout Plan

### Phase 1

本次只落地：

- 单 runner websocket 行情源
- REST 回退
- `symbol_config` 长缓存

### Phase 2

在 `114` 上做并发压测：

- `SOONUSDT`
- `CHIPUSDT`
- 再加 `2-4` 个冲刺赛币种

观测：

- `-1003` 是否重新出现
- cycle 是否稳定推进
- websocket 断线时是否平滑退回 REST

### Phase 3

只有在 Phase 2 仍不够时，再考虑：

- 跨进程共享 market sidecar
- 更彻底的 REST 预算控制

## Risks

主要风险有四个：

1. `websocket-client` 在线上环境的稳定性和 TLS 表现需要验证
2. 单 runner 双流虽然可控，但多 runner 并发后仍需观察连接数和消息抖动
3. websocket 数据结构和 REST 字段命名不同，标准化层必须严谨
4. 如果实现时把 runner 和 websocket 生命周期耦合过深，会让回退逻辑变脆

本次设计用“进程内独立对象 + 明确回退 + 不改主循环形态”来压低这些风险。

## Success Criteria

达到以下标准就算这次设计实现成功：

- 单个 runner 在 websocket 正常时，不再每轮调用 `bookTicker` 和 `premiumIndex`
- `fetch_futures_symbol_config(symbol)` 不再每轮请求
- websocket 断线时 runner 不会直接停掉，而是回退到 REST
- `114` 上 `SOON + CHIP + 2-4` 个冲刺赛币的并发压测不立即触发新一轮 `-1003`

