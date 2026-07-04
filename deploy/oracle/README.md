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
`output/<symbol>_loop_events.jsonl` has stopped updating.

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

Hedge `best_quote_maker_volume` farming (REUSDT, and now ARXUSDT / OUSDT) is supervised by a small
server-side automation stack. Two of its pieces are tracked, symbol-generic package modules — deploy
them by `git pull`, not by hand-editing `output/ops/*.py`:

- `python -m grid_optimizer.competition_target_gate` — stop + flatten at the target
  (`max_cumulative_notional` from the live control JSON) or past `--first` with wear above
  `--wear-stop`. It flattens **managed only**, keeps frozen inventory, and rests a `FROZENTP*` BUY
  limit on the kept frozen shorts (priced so no lot is underwater at the fill). A per-symbol
  `output/<symbol>_target_gate_done_YYYYMMDD.flag` prevents repeats within a UTC day.
- `python -m grid_optimizer.competition_health_monitor` — the stable supervisor. It **never boosts
  budget to chase a target**; it only (1) restarts an "active but not placing" runner, (2) *detects*
  the balanced-hedge deadlock and logs it (`would_unstick` / `blocked_by_config`) — it does **not**
  auto market-reduce managed positions unless the operator explicitly passes
  `--enable-deadlock-unstick` (OFF by default, and production cron leaves it off, per the "no
  automatic managed pair-reduce" policy) — and (3) runs a hysteretic wear governor that raises
  `quote_offset_ticks` when recent wear is hot and lowers it when wear recovers. Every restart checks
  the systemctl return code and is recorded, so a failed restart is retried, not silently assumed.

Both read exchange `userTrades` for the true wear (`-realized_pnl / gross_notional * 1e4`), never the
runner's own `loss_per_10k` field.

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

`daily_reset` (the 08:00 target/param roll + restart) stays a per-account script under `output/ops/`
because its config body differs per machine; the two modules below are symbol-generic. Target keeps
its own auto-stop disabled for pure farming via `--wear-stop 999999 --first 999999999` (the campaign
convention); drop those overrides to re-enable target/wear auto-stop.

```cron
# --- ARXUSDT ---
*/8 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/arx_tgt_gate.lock .venv/bin/python -m grid_optimizer.competition_target_gate --symbol ARXUSDT --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --tick 0.0001 --wear-stop 999999 --first 999999999 --enforce >> output/arxusdt_target_gate.log 2>&1
*/10 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/arx_health.lock .venv/bin/python -m grid_optimizer.competition_health_monitor --symbol ARXUSDT --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --first 3000 --hard-wear 1.6 --brake-wear 2.0 --release-wear 1.0 --max-offset 4 --enforce >> output/arxusdt_health_monitor.log 2>&1

# --- OUSDT ---
*/8 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/o_tgt_gate.lock .venv/bin/python -m grid_optimizer.competition_target_gate --symbol OUSDT --service grid-loop@OUSDT.service --workdir /home/ubuntu/wangge --tick 0.0001 --wear-stop 999999 --first 999999999 --enforce >> output/ousdt_target_gate.log 2>&1
*/10 * * * * cd /home/ubuntu/wangge && set -a && . /home/ubuntu/.config/wangge/binance_api_env.env && set +a && flock -n /tmp/o_health.lock .venv/bin/python -m grid_optimizer.competition_health_monitor --symbol OUSDT --service grid-loop@OUSDT.service --workdir /home/ubuntu/wangge --first 3000 --hard-wear 1.6 --brake-wear 2.0 --release-wear 1.0 --max-offset 4 --enforce >> output/ousdt_health_monitor.log 2>&1
```

On the API2 host (150) use `--workdir /home/ubuntu/wangge_api2` and that host's env file. Validate a
new symbol without side effects by dropping `--enforce` (both modules then only print the JSON
decision). Confirm `crontab -l` and tail the `output/*_health_monitor.log` for a few cycles before
trusting it unattended.

Built-in production-safety behaviour of the target gate (no cron flags needed):

- It fires the target stop only for a **positive** target. A missing/unreadable control JSON or a
  `<= 0` target reports `config_error` and does nothing, so a 0 target can never stop+flatten on a
  `vol >= 0` read.
- After `systemctl stop` it **confirms the service is inactive** before cancelling orders or
  flattening; if the stop fails it reports `ABORTED_STOP_FAILED` and touches nothing.
- Dry-run (`--enforce` omitted) performs **no** exchange side effects, including the `FROZENTP`
  cleanup.
- `--qty-step` (default `1`) truncates the frozen-short TP qty to the symbol lot step, and the qty is
  clamped to the actual kept short so the BUY can never over-buy into a long. Set it if a symbol uses
  a non-integer contract step.

The `--first`/`--brake-wear`/`--hard-wear` values above are aligned to the ARX v2 wear budget
(soft 0.9 / hard 1.6 per 10k); tune per campaign and per the reward economics, not by copy-paste.
