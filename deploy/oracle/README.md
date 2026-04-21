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
