# Oracle Always Free + GitHub Actions Deployment

This project includes:
- `deploy/oracle/install_or_update.sh`: remote install/update script (venv + systemd).
- `.github/workflows/deploy-oracle.yml`: CI/CD workflow to deploy `main` to Oracle VM.

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

## 2) Add GitHub repository secrets

Required:
- `ORACLE_HOST`: public IP or domain
- `ORACLE_USER`: ssh user (for example `ubuntu`)
- `ORACLE_SSH_KEY`: private key content for that user

Optional:
- `ORACLE_PORT`: ssh port (default `22`)
- `ORACLE_APP_DIR`: deploy directory (default `/home/<ORACLE_USER>/grid_trading`)
- `GRID_WEB_PORT`: web port (default `8787`)
- `SERVICE_NAME`: systemd service name (default `grid-web`)

## 3) Trigger deployment

Deployment runs automatically on push to `main`, or manually from GitHub Actions:
- Workflow: `Deploy to Oracle Always Free`

After successful run:
- Service name: `grid-web` (or your `SERVICE_NAME`)
- Check on server:

```bash
sudo systemctl status grid-web --no-pager
sudo journalctl -u grid-web -n 100 --no-pager
```

## 4) Access

- `http://<ORACLE_HOST>:8787`
- Health check: `http://<ORACLE_HOST>:8787/api/health`

For production/public internet, place Nginx/Caddy in front with TLS and basic auth.

## 5) Existing Server Checks

Before updating an existing server, verify the live systemd unit instead of assuming the
default app directory from this README:

```bash
sudo systemctl cat grid-web.service
```

Confirm at least:

- `WorkingDirectory`
- `ExecStart`
- `EnvironmentFile`

Some hosts may still serve from a legacy directory such as `/home/ubuntu/wangge` rather than
`/home/<user>/grid_trading`. If you sync code to the wrong directory, deployment can appear to
succeed while the service continues running old code from the real working directory.

Also note that preserving `output/` keeps runner state and control JSON files. That is usually
required, but it also means a code deployment does not automatically refresh saved runtime
configuration. When debugging "server still uses old config" issues, inspect the persisted files
under `output/` in addition to the code version.
