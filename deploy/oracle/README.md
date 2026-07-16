# Oracle Deployment

## 1) One-time setup on Oracle VM

Use Ubuntu 22.04+ (Always Free Arm instance is fine).

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip rsync openssh-server
```

If you use UFW:

```bash
sudo ufw allow 22/tcp
sudo ufw allow 8787/tcp
sudo ufw reload
```

Make sure the deploy user has passwordless `sudo` (required by systemd install step).

## 2) Supported Deployment Entry Points

Routine deployment only uses the server-local update script.

The only supported deployment commands are:

```bash
ssh srv-43-131-232-150 '/usr/local/bin/grid-web-update'
ssh srv-43-155-136-111 '/usr/local/bin/grid-web-update'
ssh srv-43-155-163-114 '/usr/local/bin/grid-web-update'
```

For additional web instances on the same host, install a dedicated systemd unit and a matching
wrapper such as `/usr/local/bin/grid-web-api2-update`, then use that wrapper for routine updates.

The server-local `/usr/local/bin/grid-web-update` should match the tracked script at
`deploy/oracle/grid-web-update.sh`.

`grid-web-update` is responsible for:

- pulling the latest `main`
- recreating `.venv` when needed
- reinstalling the package in editable mode
- restarting `grid-web`
- checking the local `/api/health` endpoint

Routine deployment standard:

- routine deployment must use a server-local wrapper such as `/usr/local/bin/grid-web-update`
  or `/usr/local/bin/grid-web-api2-update`
- the wrapper must update code by `git pull --ff-only origin main` against a clean working tree
- do not use `scp`, `rsync`, ad hoc `cp`, or manual file copy to push code onto the server
- do not patch production code directly on the server and then keep running from a dirty tree

Do not use GitHub Actions, `rsync`, or ad hoc copy commands for routine deployment.

## 2.1) Installing A Second Web Instance

Use the same tracked installer, but pass a different service name, port, app directory, and env file:

```bash
APP_DIR=/home/ubuntu/wangge_api2 \
SERVICE_WORKING_DIR=/home/ubuntu/wangge_api2 \
SERVICE_NAME=grid-web-api2 \
SERVICE_DESCRIPTION="Grid Optimizer Web Service (API2)" \
GRID_WEB_PORT=8788 \
SYSTEMD_ENV_FILE=/home/ubuntu/.config/wangge/grid_web_api2.env \
PYTHONPATH_VALUE=/home/ubuntu/wangge_api2/src \
UPDATE_WRAPPER_NAME=grid-web-api2-update \
deploy/oracle/install_or_update.sh
```

If the code checkout and the runtime/output directory are different, also pass:

```bash
APP_DIR=/home/ubuntu/releases/wangge-118edc7-api2 \
SERVICE_WORKING_DIR=/home/ubuntu/wangge_api2 \
EXEC_PYTHON_BIN=/home/ubuntu/releases/wangge-118edc7-api2/.venv/bin/python \
PYTHONPATH_VALUE=/home/ubuntu/releases/wangge-118edc7-api2/src \
deploy/oracle/install_or_update.sh
```

Recommended contents for the instance env file:

- `BINANCE_API_KEY` / `BINANCE_API_SECRET`
- `GRID_WEB_USERNAME` / `GRID_WEB_PASSWORD` when the instance should require auth
- any host-local extras such as `BINANCE_BORROW_LOOKUP_MODE=safe`

After install, the routine update entrypoint becomes:

```bash
ssh <host> '/usr/local/bin/grid-web-api2-update'
```

## 3) Runner Ownership Rule

On production Oracle hosts, strategy processes must be started as `ubuntu`, not `root`.

Required rule:

- `grid-web` / `wangge-web` may be managed by `systemd`, but the service itself must run as `ubuntu`.
- All strategy runner and flatten runner operations should go through the web UI or the authenticated local API while logged in as `ubuntu`.
- Do not start `grid_optimizer.loop_runner` or `grid_optimizer.maker_flatten_runner` with `sudo`, `root`, or a root shell.

Why this matters:

- The web UI and local control plane operate as `ubuntu`.
- If a runner is started by `root`, the process and related pid files can become root-owned.
- Once that happens, the UI may fail to stop/restart the runner with `PermissionError`, and manual sudo cleanup is required.

Safe examples:

```bash
ssh srv-43-155-163-114 'set -a; source /home/ubuntu/.config/wangge/binance_api_env.env; curl -u "$GRID_WEB_USERNAME:$GRID_WEB_PASSWORD" http://127.0.0.1:8788/api/health'
```

Unsafe examples:

```bash
ssh srv-43-155-163-114 'sudo python3 -m grid_optimizer.loop_runner ...'
ssh srv-43-155-163-114 'sudo /home/ubuntu/wangge/.venv/bin/python -m grid_optimizer.maker_flatten_runner ...'
```

If someone already started a runner as `root`, fix it in this order:

1. `sudo` stop or kill the wrong process.
2. Remove the stale root-owned pid file under `output/`.
3. Re-start via the `ubuntu`-owned web service or UI.

## 4) Post-Deploy Checks

After each deployment, check on the target server:

```bash
sudo systemctl status grid-web --no-pager
sudo journalctl -u grid-web -n 100 --no-pager
```

## 4.1) Saved Runner Restart Helper

For hosts that run multiple Binance accounts, do not hand-type `nohup env BINANCE_API_KEY=...`.
Use the tracked helper script instead:

```bash
chmod +x deploy/oracle/manage_saved_runner.sh
APP_DIR=/home/ubuntu/wangge_api2 \
PYTHON_BIN=/home/ubuntu/wangge_api2/.venv/bin/python \
PYTHONPATH_VALUE=/home/ubuntu/wangge_api2/src \
GRID_API_ENV_FILE=/home/ubuntu/.config/wangge/binance_api_env_api2.env \
deploy/oracle/manage_saved_runner.sh restart BASEDUSDT
```

If you want a host-local fixed runner entrypoint, install it through `deploy/oracle/install_or_update.sh`:

```bash
APP_DIR=/home/ubuntu/wangge_api2 \
SERVICE_WORKING_DIR=/home/ubuntu/wangge_api2 \
SERVICE_NAME=grid-web-api2 \
SERVICE_DESCRIPTION="Grid Optimizer Web Service (API2)" \
GRID_WEB_PORT=8789 \
SYSTEMD_ENV_FILE=/home/ubuntu/.config/wangge/grid_web_api2.env \
PYTHONPATH_VALUE=/home/ubuntu/wangge_api2/src \
UPDATE_WRAPPER_NAME=grid-web-api2-update \
INSTALL_RUNNER_WRAPPER=1 \
RUNNER_ENV_FILE=/home/ubuntu/.config/wangge/binance_api_env_api2.env \
RUNNER_WRAPPER_NAME=grid-saved-runner-api2 \
deploy/oracle/install_or_update.sh
```

After that, routine runner operations become:

```bash
ssh <host> '/usr/local/bin/grid-saved-runner-api2 restart SOONUSDT'
ssh <host> '/usr/local/bin/grid-saved-runner-api2 status SOONUSDT'
```

Spot competition runners with APP-loss prestart gate must use the tracked
systemd unit from `deploy/oracle/install_runner_systemd.sh`. Gate rejection exits
with status `2`, and the unit must include `RestartPreventExitStatus=2`; this
prevents `Restart=always` from turning a rejected low-loss gate into an
automatic Binance audit loop or a later unattended start.

## 4.3) Web Health Watchdog

For hosts where `grid-web` may stay `active` but stop answering local HTTP requests, install the
tracked web watchdog. It probes local health endpoints every minute and restarts the service after
repeated failures.

Primary or controller host:

```bash
APP_DIR=/home/ubuntu/wangge \
RUNNER_CODE_DIR=/home/ubuntu/wangge \
SERVICE_NAME=grid-web \
HEALTHCHECK_URL=http://127.0.0.1:8788/api/health \
STATUS_URL=http://127.0.0.1:8788/api/running_status?scope=local \
deploy/oracle/install_web_watchdog.sh
```

Controller host on port `8787`:

```bash
APP_DIR=/home/ubuntu/wangge \
RUNNER_CODE_DIR=/home/ubuntu/wangge \
SERVICE_NAME=grid-web-controller \
HEALTHCHECK_URL=http://127.0.0.1:8787/api/health \
STATUS_URL=http://127.0.0.1:8787/api/running_status?scope=cross \
deploy/oracle/install_web_watchdog.sh
```

API2 host:

```bash
APP_DIR=/home/ubuntu/wangge_api2 \
RUNNER_CODE_DIR=/home/ubuntu/wangge_api2_repo \
SERVICE_NAME=grid-web-api2 \
HEALTHCHECK_URL=http://127.0.0.1:8789/api/health \
STATUS_URL=http://127.0.0.1:8789/api/running_status?scope=local \
deploy/oracle/install_web_watchdog.sh
```

Optional knobs:

- `FAILURE_THRESHOLD=3`: restart only after 3 consecutive failures
- `ON_UNIT_ACTIVE_SEC=1min`: watchdog frequency
- `AUTH_USERNAME` / `AUTH_PASSWORD`: for protected local status endpoints
- `ALERT_EMAIL_TO=you@example.com`: send an alert email on automatic restart

Verify after install:

```bash
sudo systemctl status grid-web-health-watchdog.timer --no-pager
sudo journalctl -u grid-web-health-watchdog.service -n 50 --no-pager
```

## 4.4) Controller Cross Watchdog

For the `110` controller, install a second watchdog that checks the aggregated cross payload instead
of only the local web process. This catches "controller is up but one upstream node is missing"
incidents early.

```bash
APP_DIR=/home/ubuntu/wangge \
RUNNER_CODE_DIR=/home/ubuntu/wangge \
AUTH_USERNAME=admin \
AUTH_PASSWORD='your-basic-auth-password' \
HOST_LABEL=110 \
CONTROLLER_CROSS_STATUS_URL='http://127.0.0.1:8787/api/running_status?scope=cross' \
deploy/oracle/install_controller_cross_watchdog.sh
```

Notes:

- The watchdog writes state under `/var/tmp/grid-controller-cross-watchdog/state.json`.
- It logs to journald under `grid-controller-cross-watchdog.service`.
- If alert email is configured through `output/alert_notifier_config.json` or `GRID_ALERT_*`, it
  sends an email after repeated failures and only re-alerts on a new summary or after recovery.

## 4.2) Saved Runner systemd Template

For production runners that should survive process exits and host reboots, install the symbol-level
systemd template from the checked-out repo. This keeps the same saved runner control JSON, but
routes web/wrapper start-stop operations through `grid-loop@SYMBOL.service`.

Primary host:

```bash
APP_DIR=/home/ubuntu/wangge \
RUNNER_CODE_DIR=/home/ubuntu/wangge \
PYTHON_BIN=/home/ubuntu/wangge/.venv/bin/python \
PYTHONPATH_VALUE=/home/ubuntu/wangge/src \
GRID_API_ENV_FILE=/home/ubuntu/.config/wangge/binance_api_env.env \
WEB_SERVICE_NAME=grid-web \
RUNNER_WRAPPER_NAME=grid-saved-runner \
SYMBOLS=SOONUSDT \
START_NOW=1 \
deploy/oracle/install_runner_systemd.sh
```

API2 host:

```bash
APP_DIR=/home/ubuntu/wangge_api2 \
RUNNER_CODE_DIR=/home/ubuntu/wangge_api2_repo \
PYTHON_BIN=/home/ubuntu/wangge_api2_repo/.venv/bin/python \
PYTHONPATH_VALUE=/home/ubuntu/wangge_api2_repo/src \
GRID_API_ENV_FILE=/home/ubuntu/.config/wangge/binance_api_env_api2.env \
WEB_SERVICE_NAME=grid-web-api2 \
RUNNER_WRAPPER_NAME=grid-saved-runner-api2 \
SYMBOLS=SOONUSDT \
START_NOW=1 \
deploy/oracle/install_runner_systemd.sh
```

The installer also enables `grid-loop-watchdog@SYMBOL.timer`. When a control config exists, the
watchdog starts an inactive runner and restarts an active runner whose
`output/<symbol>_loop_events.jsonl` has stopped updating. 对带终止 intent 的运行，watchdog 会先校验
`futures_lifecycle_intent_v2` 内嵌的 `futures_run_contract_snapshot_v3` 及摘要，再按本节后文的
`active` / `handoff_pending` / `completed_current` / `completed_old` / `invalid` 语义决定是否续做；不能只根据进程
状态或自然日标记复活。

Recommended pattern:

- keep each account in its own env file, for example `binance_api_env.env` and `binance_api_env_api2.env`
- keep each web instance in its own env file, for example `grid_web.env` and `grid_web_api2.env`
- keep the env file out of Git and set permissions to `0600`
- always restart saved runners through `manage_saved_runner.sh`, not ad hoc inline env commands
- when installed, prefer the host-local saved runner wrapper over hand-typed repo paths
- always update extra web instances through their dedicated wrapper, for example `grid-web-api2-update`
- when the code lives in a release directory, point `PYTHON_BIN` and `PYTHONPATH_VALUE` at that release explicitly

## 5) Access

- use the port configured in `grid-web.service`
- health check path: `/api/health`

For production/public internet, place Nginx/Caddy in front with TLS and basic auth.

## 6) Existing Server Checks

Before changing deployment plumbing on an existing server, verify the live systemd unit:

```bash
sudo systemctl cat grid-web.service
```

Confirm at least:

- `WorkingDirectory`
- `ExecStart`
- `EnvironmentFile`

All current production servers are expected to run from `/home/ubuntu/wangge`.

Also note that preserving `output/` keeps runner state and control JSON files. That is usually
required, but it also means a code deployment does not automatically refresh saved runtime
configuration. When debugging "server still uses old config" issues, inspect the persisted files
under `output/` in addition to the code version.

When restarting a saved runner through `/api/runner/start`, prefer sending the full payload copied
from the live `output/*_control.json` and then editing only the fields you intend to change. Partial
payloads are easy to start from, but they can silently drop live overrides such as
`take_profit_min_profit_ratio`, inventory guards, or preset-specific caps.

## 7) Competition Volume-Farming Ops Stack (稳健 model)

> **当前分支发布门禁：不得部署。** 源码已实现显式注册 symbol 的
> `guard -> coordinator -> runner` 每轮单动作闭环，包括定向库存/maker flow 恢复、类型化临时
> 亏损租约的签发/消费/回执/回收，以及目标/截止时间的唯一终止排空所有者。严格
> `STABLE` 下的每日窗口滚动由 `BASELINE_REBASE` 唯一动作原子更新运行契约/所有者，并调度受栅栏的唯一
> 重启；外部 Web 触发器、磨损守卫和状态重对齐对已注册 symbol 只观察、委托或拒绝直写。
>
> 但当前没有任何生产 symbol 完成注册或所有权切换，本分支也未部署。冻结账本专用修复闭环、
> 真实生产基线/订单/账本迁移、宿主机外 cron/systemd/`output/ops` 盘点、协调器心跳/严重告警、
> 受管 runner 的 `Restart=no` 门禁和按 symbol 的原子切换/回滚演练仍是硬阻塞。下面的旧生产
> 部署示例只用于理解现有环境，不得据此部署本分支或注册 symbol。
>
> 当前注册门禁会拒绝带有正数/无效冻结库存摘要或 lot、或仍启用 `best_quote_maker_volume_reduce_freeze_enabled`
> 冻结创建能力的 symbol，防止不完整的冻结修复动作取得所有权；
> 这只是安全拒绝，不等于冻结账本修复已经实现。扁平受管字段漂移（包括遗留原始 `allow_loss=true`）
> 和损坏的普通运行回执 journal 会进入单独的本地状态修复轮次，下一轮再恢复交易，不会要求人工清理布尔状态。

Hedge `best_quote_maker_volume` farming (REUSDT, and now ARXUSDT / OUSDT) is supervised by a small
server-side automation stack. Two of its pieces are tracked, symbol-generic package modules — deploy
them by `git pull`, not by hand-editing `output/ops/*.py`:

- `python -m grid_optimizer.competition_target_gate`：观察 live control JSON 中的
  `max_cumulative_notional` 和磨损阈值，然后原子提交
  `output/<symbol>_terminal_intent.json`。它不停止/重启服务、不撤单、不平仓；loop runner 是
  maker-only 生命周期排空的唯一所有者。显式目标运行必须同时提供
  `runtime_guard_stats_start_time` 和 `run_end_time`，成交额、目标进度与损耗统一按半开区间
  `[runtime_guard_stats_start_time, run_end_time)` 统计。旧部署可能残留
  `output/<symbol>_target_gate_done_YYYYMMDD.flag`，但显式契约路径既不读取也不创建它，且不把它
  作为权威。
- `python -m grid_optimizer.competition_health_monitor`：未注册 symbol 仍保留旧的健康监督行为；
  它不提高预算追目标，可以处理“运行中但无挂单”重启、记录平衡对冲死锁，并使用带滞回的
  磨损调节器。对包含恢复所有权的已注册 symbol，`--enforce` 会被收窄为
  `observe_only_recovery_managed_symbol`：它只输出观测，不重启运行器、不直写 `quote_offset_ticks`，
  也不执行 managed pair reduce。这些原因统一交由协调器仲裁。

- `python -m grid_optimizer.competition_state_realign`：未注册 symbol 仍可使用旧 BQ 账本重对齐流程，
  并保留冻结库存。对已注册 symbol，检测到恢复所有权后只返回 deferred/observer 结果，
  不停止或启动运行器、不撤单、不改写账本。冻结账本修复必须等专用类型化动作闭环完成后，
  再由协调器按账本权限执行，不得用该旧脚本绕过。

- Web 成交量触发器对已注册 symbol 不再直接 stop/start。低于停止阈值只持久化绑定
  symbol/run-contract/generation 的 120 秒类型化观察，由协调器选择一次 `SAFETY_CONVERGE`；
  成交量恢复或 TTL 到期后仍由协调器退出安全状态。运行器本地 runtime/protective guard 也只写
  类型化信号，TTL 为 `max(120s, sleep + jitter + 60s轮询 + 30s余量)` 且最长 15 分钟，
  本地 plan/action/submit 只保留 normal `LIMIT + GTX` reduce，不直接撤单、平仓、停止或重启。

- `/api/runner/stop`、`/api/runner/quick_flatten`、`/api/runner/frozen_inventory` 遇到显式恢复
  envelope 时，在任何 systemd、撤单、平仓或账本副作用前返回 409。未注册旧 symbol 保持兼容。
  当前分支不提供 HTTP break-glass；生产所有权切换前必须另行设计、审计并演练人工紧急通道。

`install_bq_volume_recovery_guard.sh` 将守卫安装为 1 分钟 timer 驱动的 `Type=oneshot`，并为每次执行
设置默认 45 秒、可通过 `TIMEOUT_START_SEC` 调整的 `TimeoutStartSec`。默认超时必须短于
`ON_UNIT_ACTIVE_SEC`，这样网络调用挂死会被 systemd 终止为失败，后续 timer 轮次仍能重新启动守卫；
进程级监督只负责终止/告警守卫进程，不得代替协调器对任何 symbol 下单、撤单或停启 runner。

当前“守卫一轮已经完成”的心跳边界是：主循环结束后最终写入的
`output/bq_volume_recovery_guard_state.json` 的 mtime 已推进，并且
`output/bq_volume_recovery_guard_events.jsonl` 中存在本轮各 symbol 的最新结果记录。timer 已触发、
oneshot 仍在运行或仅有进程 PID 都不算完成心跳。service 出现 `timeout` / `failed`，或 state mtime
未按预期推进且事件日志缺少完整本轮记录，都必须触发严重告警。该超时、告警、下一轮自动重入及
state/event 心跳推进仍是生产 symbol 注册前必须在隔离环境演练的发布门禁，不能因为已有超时配置就视为可上线。

The gate and health monitor read exchange `userTrades` for the true wear
(`-realized_pnl / gross_notional * 1e4`), never the runner's own `loss_per_10k` field. 其中目标闸门
对显式目标运行使用固定的 `min(当前时间, run_end_time)` 查询截止点；成交按 ID 去重，缺失 ID、
字段无效或分页无进展时失败关闭，不能退回自然日口径或用不完整统计宣称达标。窗口内暂时查询失败会记录
可见错误并继续 maker 循环；到达截止时间仍不可观测时，结果明确记为
`TARGET_UNMET_DEADLINE / observation_unavailable_at_deadline`，随后按冻结的退出契约收敛，不能无限等成交统计恢复。

### Why this supersedes the per-symbol pace controllers

The hand-maintained `output/ops/<symbol>_pace_controller.py` chases the daily target by **boosting**
`cycle_budget`. On a high-volatility pair that just churns quotes that never fill and bleeds wear (see
the ARX 2026-07-02 night retrospective in `docs/STRATEGY_EXECUTION_GUIDE.md`). The health monitor's
governor is brake-only: it protects volume by never stopping, and protects wear by widening the
offset — the same design REUSDT has run stably. Prefer the health monitor for ARX/OUSDT; keep a pace
controller only if you deliberately want target-chasing on a calm, liquid pair.

The health monitor also folds in the job of the standalone `bq_liveness_watchdog.py` (liveness) plus
the deadlock self-heal, so those become redundant once it is wired.

### Cron wiring (per host, run from the app dir as `ubuntu`)

旧 `daily_reset`（08:00 目标/参数滚动 + 直接重启）仍可存在于各账户的 `output/ops/`，但只能用于
未注册 symbol。对已注册 symbol，`roll_competition_window.py` 只在严格 `STABLE` 且无 lease、清理义务、
激活/执行阶段时，在同一 symbol 锁内由 `BASELINE_REBASE` 原子更新新窗口的运行契约/所有者，并写入唯一待处理、
受执行栅栏保护的 `RUNNER_RESTART`。滚动脚本本身不调用 systemd；守卫下一轮执行该重启并等待当前代际
回执。非 `STABLE` 或有未完成义务时返回 `deferred` 且零写入；不清空守卫状态、租约、订单清单或成交回执。
目标闸门中的旧 `--wear-stop` / `--first` 参数仅为兼容旧 cron 的弃用解析参数，不再具有授权能力。
是否启用磨损退出只取决于运行契约同时固化的 `lifecycle_wear_stop_per_10k` 与
`lifecycle_wear_stop_min_gross_notional`；两者缺失时磨损退出关闭，不能由进程启动参数临时打开或关闭。

```cron
# --- ARXUSDT ---
*/8 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/arx_tgt_gate.lock .venv/bin/python -m grid_optimizer.competition_target_gate --symbol ARXUSDT --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --tick 0.0001 --enforce >> output/arxusdt_target_gate.log 2>&1
*/10 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/arx_health.lock .venv/bin/python -m grid_optimizer.competition_health_monitor --symbol ARXUSDT --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --first 3000 --hard-wear 1.6 --brake-wear 2.0 --release-wear 1.0 --max-offset 4 --enforce >> output/arxusdt_health_monitor.log 2>&1
6-56/10 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/arx_realign.lock .venv/bin/python -m grid_optimizer.competition_state_realign --symbol ARXUSDT --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --threshold-qty 150 --enforce >> output/arx_auto_realign.log 2>&1

# --- OUSDT ---
*/8 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/o_tgt_gate.lock .venv/bin/python -m grid_optimizer.competition_target_gate --symbol OUSDT --service grid-loop@OUSDT.service --workdir /home/ubuntu/wangge --tick 0.0001 --enforce >> output/ousdt_target_gate.log 2>&1
*/10 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/o_health.lock .venv/bin/python -m grid_optimizer.competition_health_monitor --symbol OUSDT --service grid-loop@OUSDT.service --workdir /home/ubuntu/wangge --first 3000 --hard-wear 1.6 --brake-wear 2.0 --release-wear 1.0 --max-offset 4 --enforce >> output/ousdt_health_monitor.log 2>&1
```

在 API2 主机（150）上，旧未注册路径的 `--workdir` 为 `/home/ubuntu/wangge_api2`，并使用该主机的环境文件。
去掉 `--enforce` 可在无副作用下观察决策。上述 cron 是未注册 symbol 的旧环境示例，不是恢复协调器的上线清单；
在冻结账本、迁移和原子切换门禁完成前，不得为已注册 symbol 复制或启用这些 cron。

Built-in production-safety behaviour of the target gate (no cron flags needed):

- 只有**正数**目标才能提交 `target_reached`。control JSON 不可读或显式配置 `<= 0` 目标时校验
  失败，不发布 intent。仅磨损运行必须省略目标字段，并仍提供同样完整的统计窗口与退出契约；
  同时在运行契约中提供正数 `lifecycle_wear_stop_per_10k` 与
  `lifecycle_wear_stop_min_gross_notional` 后，`wear_limit_breached` 才能提交自己的 intent。
- 正目标必须有完整且有效的 `runtime_guard_stats_start_time`、`run_end_time` 与退出契约；缺少或
  非法时在查询交易所或执行任何进程/订单副作用前返回 `invalid_run_contract`。
- `--enforce` 只发布带版本、可幂等的 `futures_lifecycle_intent_v2`。intent 内嵌完整
  `futures_run_contract_snapshot_v3`，并绑定 `futures-run-contract-v3-<digest>`；运行器和 watchdog
  都重新规范化并复算摘要，字段被改写、摘要不匹配或状态未知时可见失败关闭。
- 重试保留第一份 pending 契约而不替换它。未完成的旧 intent 始终按其中冻结的预算、等待时间、
  退出策略和有效单笔排空上限续跑，即使当前 control 已改变；自动路径不调用 systemctl 或交易所
  订单变更 API。
- dry-run（省略 `--enforce`）只打印拟提交的 intent。
- `--place-tp-now` 仍是显式人工维护动作。它的 `--qty-step`（默认 `1`）向下截断 frozen-short TP
  数量，并把数量限制在实际保留空仓以内。
- 自然日 `target_gate_done_YYYYMMDD.flag` 不是显式运行契约的完成或停机权威。新路径不创建该
  marker，也不能让旧 marker 阻止当前契约统计或 intent 提交。

The `--first`/`--brake-wear`/`--hard-wear` values above are aligned to the ARX v2 wear budget
(soft 0.9 / hard 1.6 per 10k); tune per campaign and per the reward economics, not by copy-paste.

### Runtime-guard stop semantics and the revival matrix (deploy checklist)

A runtime-guard stop (`max_actual_net_notional_hit`, rolling loss limits, …) encodes a risk
decision. After the 2026-07-04/05 ARX incidents, exactly one durable owner may decide lifecycle
recovery. 活动终止 intent 的运行器死亡不是“预期停机”，watchdog 必须恢复同一冻结契约；只有
当前契约已经完成，或其他注册 stop reason 仍有效时，才保持停机：

| Path | Revives? |
|---|---|
| systemd | 未注册 runner 按现有 `Restart=on-failure` drop-in 处理；已注册 runner 未通过切换门禁前不得启用，切换后必须为 `Restart=no`，崩溃只生成 `RUNNER_RECOVER` 观测 |
| `runner_watchdog.sh` + `active` intent | 必须续做同一冻结契约：runner inactive 时启动，事件缺失或陈旧时重启；这是排空恢复工作，不是预期停机 |
| `runner_watchdog.sh` + `handoff_pending` | 旧退出所有者已归档但新运行尚未写出第一条正常事件，必须启动/重启完成接管；正常事件先落盘再原子确认 handoff，确认后不再凭永久 history 授权复活 |
| `runner_watchdog.sh` + acknowledged handoff | 交接已经完成，恢复按当前 intent、事件和 stop reason 判断；旧 stop/history 不再具有复活权限 |
| `runner_watchdog.sh` + `completed_current` intent | 当前运行契约已经 `completed` / `stopped_clean` / `stopped_preserved`，保持预期停机，不启动或重启 |
| `runner_watchdog.sh` + `completed_old` intent | 已完成 intent 属于旧运行契约，不阻止当前新契约启动；后续由新契约路径归档 |
| `runner_watchdog.sh` + `invalid` intent/snapshot | 摘要、schema、symbol、action 或状态不合法时可见失败关闭，在任何 systemctl 副作用前退出 |
| `runner_watchdog.sh` + no intent | 未注册 symbol 继续按现有事件/stop reason 门禁判断；已注册 symbol 的普通重启必须来自当前协调器 `RUNNER_RECOVER` 执行栅栏，自然日 done marker 不具有授权 |
| `competition_state_realign` | 已注册 symbol 只观察/deferred，不重启、撤单或改账本；未注册 symbol 保留“只重启自己停止的运行器”旧语义 |
| `competition_health_monitor` | 已注册 symbol 只输出 `observe_only_recovery_managed_symbol`，不重启或直写配置；未注册 symbol 保留旧监督语义 |
| 人工 / 每日滚动 | 人工不属于自动恢复权限；已注册 symbol 的每日滚动只原子写入新契约和受栅栏 `RUNNER_RESTART`，不直接复活 |

以下 `Restart=on-failure` drop-in 说明只对未注册的现有 runner 有效
(`/etc/systemd/system/grid-loop@.service.d/80-runtime-guard-stop.conf`, written by
`install_runner_systemd.sh`):

- It applies to **every** `grid-loop@` runner on the host, not only competition symbols. Crash
  recovery is unchanged (non-zero exits still restart); what changes is that clean exits — runtime
  guard stops, `after_end_window`, cumulative-cap stops — are no longer blind-revived by
  `Restart=always`. Liveness for healthy runners is covered by the watchdog timer and the health
  monitor, and scheduled revival by the daily-reset crons.
- Verify after install: `systemctl show -p Restart 'grid-loop@ARXUSDT.service'` must print
  `Restart=on-failure`.

恢复协调器的生产切换是另一套门禁：先验证协调器心跳、进程级监督、严重告警、主机外执行器盘点和回滚路径，
再对单个 symbol 原子转移所有权，并将该受管 runner 校验为 `Restart=no`。不得让 `Restart=on-failure` 与协调器同时拥有重启权。

Stale-plan guard fallback (`loop_runner`): the runtime guard normally reads the ledger-scoped
`strategy_actual_net_notional` from the latest plan. Only when a **non-empty** persisted plan
snapshot is stale (`generated_at` older than 180s, or missing from the snapshot) does it re-read
**account-level** exposure from live positionRisk; a missing plan file keeps legacy no-network
semantics — a guard-stopped runner stops writing plans, and trusting the pre-stop snapshot
latches the stop across every restart even on a flat account. The live reading includes frozen
inventory, which is deliberate (fail-closed; the state ledger cannot be trusted at that moment —
external fills are what desynchronized it). Operating rule that follows: **a symbol holding a
frozen reservoir must keep `max_actual_net_notional` at reservoir + normal-farming headroom**
(OUSDT: guard 1500 vs ~720 frozen short reservoir — check the live reservoir size before ever
lowering that guard, or the fallback will hold stops on the reservoir alone and conflict with
"frozen inventory must not affect normal farming").
