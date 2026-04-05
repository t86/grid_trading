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

The server-local `/usr/local/bin/grid-web-update` should match the tracked script at
`deploy/oracle/grid-web-update.sh`.

`grid-web-update` is responsible for:

- pulling the latest `main`
- recreating `.venv` when needed
- reinstalling the package in editable mode
- restarting `grid-web`
- checking the local `/api/health` endpoint

Do not use GitHub Actions, `rsync`, or ad hoc copy commands for routine deployment.

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
