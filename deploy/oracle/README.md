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

## 3) Post-Deploy Checks

After each deployment, check on the target server:

```bash
sudo systemctl status grid-web --no-pager
sudo journalctl -u grid-web -n 100 --no-pager
```

## 4) Access

- use the port configured in `grid-web.service`
- health check path: `/api/health`

For production/public internet, place Nginx/Caddy in front with TLS and basic auth.

## 5) Existing Server Checks

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
