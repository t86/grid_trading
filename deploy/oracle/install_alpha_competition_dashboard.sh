#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ubuntu/wangge-alpha-dashboard}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
SERVICE_NAME="${SERVICE_NAME:-binance-alpha-dashboard}"
SERVICE_USER="${SERVICE_USER:-ubuntu}"
HOST="${ALPHA_DASHBOARD_HOST:-127.0.0.1}"
PORT="${ALPHA_DASHBOARD_PORT:-8796}"
RULE_CACHE="${ALPHA_COMPETITION_RULE_CACHE:-/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json}"
DISCOVERY_CACHE="${ALPHA_COMPETITION_DISCOVERY_CACHE:-/home/ubuntu/.cache/binance-alpha-volume-alert/competition_discovery.json}"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
BACKUP_PATH="${UNIT_PATH}.backup.$(date -u +%Y%m%dT%H%M%SZ).$$"
UNIT_STAGE="${UNIT_PATH}.stage.$$.${RANDOM}"

fail() {
  printf '%s\n' "$1" >&2
  exit 2
}

validate_absolute_path() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^/[A-Za-z0-9._/@+-]+$ ]]; then
    fail "${name} must be an absolute path without whitespace or systemd metacharacters."
  fi
}

if ! command -v sudo >/dev/null 2>&1; then
  fail "sudo is required for systemd installation."
fi
if ! command -v realpath >/dev/null 2>&1; then
  fail "realpath is required for safe path validation."
fi
if [[ ! "$SERVICE_NAME" =~ ^[A-Za-z0-9_][A-Za-z0-9_.@-]*$ ]]; then
  fail "SERVICE_NAME contains unsafe characters."
fi
if [[ "$SERVICE_NAME" == *.service ]]; then
  fail "SERVICE_NAME must not include the .service suffix."
fi
if [[ ! "$SERVICE_USER" =~ ^[A-Za-z0-9_][A-Za-z0-9_.@-]*$ ]]; then
  fail "SERVICE_USER contains unsafe characters."
fi
if [[ ! "$HOST" =~ ^[A-Za-z0-9_.:-]+$ ]]; then
  fail "ALPHA_DASHBOARD_HOST contains unsafe characters."
fi
if [[ ! "$PORT" =~ ^[0-9]{1,5}$ ]] || (( 10#$PORT < 1 || 10#$PORT > 65535 )); then
  fail "ALPHA_DASHBOARD_PORT must be an integer from 1 to 65535."
fi
validate_absolute_path "APP_DIR" "$APP_DIR"
validate_absolute_path "PYTHON_BIN" "$PYTHON_BIN"
validate_absolute_path "ALPHA_COMPETITION_RULE_CACHE" "$RULE_CACHE"
validate_absolute_path "ALPHA_COMPETITION_DISCOVERY_CACHE" "$DISCOVERY_CACHE"

if [ ! -d "${APP_DIR}/src/grid_optimizer" ]; then
  fail "APP_DIR does not contain src/grid_optimizer."
fi
if [ ! -f "$PYTHON_BIN" ] || [ ! -x "$PYTHON_BIN" ]; then
  fail "PYTHON_BIN is not executable."
fi

if ! APP_DIR="$(realpath "$APP_DIR")"; then
  fail "APP_DIR could not be canonicalized."
fi
PYTHON_NAME="$(basename "$PYTHON_BIN")"
if ! PYTHON_DIR="$(realpath "$(dirname "$PYTHON_BIN")")"; then
  fail "PYTHON_BIN could not be canonicalized."
fi
PYTHON_BIN="${PYTHON_DIR}/${PYTHON_NAME}"
if ! RULE_CACHE="$(realpath -m "$RULE_CACHE")"; then
  fail "ALPHA_COMPETITION_RULE_CACHE could not be canonicalized."
fi
if ! DISCOVERY_CACHE="$(realpath -m "$DISCOVERY_CACHE")"; then
  fail "ALPHA_COMPETITION_DISCOVERY_CACHE could not be canonicalized."
fi
validate_absolute_path "APP_DIR" "$APP_DIR"
validate_absolute_path "PYTHON_BIN" "$PYTHON_BIN"
validate_absolute_path "ALPHA_COMPETITION_RULE_CACHE" "$RULE_CACHE"
validate_absolute_path "ALPHA_COMPETITION_DISCOVERY_CACHE" "$DISCOVERY_CACHE"

APP_HOME="$(dirname "$APP_DIR")"
CACHE_DIR="$(dirname "$RULE_CACHE")"
DISCOVERY_CACHE_DIR="$(dirname "$DISCOVERY_CACHE")"
EXPECTED_CACHE_DIR="${APP_HOME}/.cache/binance-alpha-volume-alert"
if [ "$CACHE_DIR" = "/" ] || [ "$DISCOVERY_CACHE_DIR" = "/" ]; then
  fail "Alpha competition caches must not use the filesystem root."
fi
if [ "$CACHE_DIR" != "$EXPECTED_CACHE_DIR" ] || [ "$DISCOVERY_CACHE_DIR" != "$EXPECTED_CACHE_DIR" ]; then
  fail "Alpha competition caches must be directly under the dedicated app-home cache directory."
fi
if [ -L "${APP_HOME}/.cache" ] || [ -L "$EXPECTED_CACHE_DIR" ]; then
  fail "The dedicated cache path must not contain symlinked cache directories."
fi
if [ -e "$CACHE_DIR" ] && [ ! -d "$CACHE_DIR" ]; then
  fail "The dedicated cache path exists but is not a directory."
fi

HAD_OLD_UNIT=0
WAS_ENABLE_STATE="disabled"
WAS_ACTIVE_STATE="inactive"
if sudo test -e "$UNIT_PATH" || sudo test -L "$UNIT_PATH"; then
  HAD_OLD_UNIT=1
  if ENABLE_OUTPUT="$(sudo systemctl is-enabled "${SERVICE_NAME}.service" 2>/dev/null)"; then
    ENABLE_RC=0
  else
    ENABLE_RC=$?
  fi
  case "${ENABLE_RC}:${ENABLE_OUTPUT}" in
    "0:enabled") WAS_ENABLE_STATE="enabled" ;;
    "1:disabled") WAS_ENABLE_STATE="disabled" ;;
    "1:masked" | "1:bad") fail "Unsupported existing unit state." ;;
    *) fail "Could not determine the existing service enable state safely." ;;
  esac

  if ACTIVE_OUTPUT="$(sudo systemctl is-active "${SERVICE_NAME}.service" 2>/dev/null)"; then
    ACTIVE_RC=0
  else
    ACTIVE_RC=$?
  fi
  case "${ACTIVE_RC}:${ACTIVE_OUTPUT}" in
    "0:active") WAS_ACTIVE_STATE="active" ;;
    "3:inactive") WAS_ACTIVE_STATE="inactive" ;;
    "3:failed") WAS_ACTIVE_STATE="failed" ;;
    *) fail "Could not determine the existing service active state safely." ;;
  esac
fi

if ! sudo bash -s -- "$APP_HOME" "$EXPECTED_CACHE_DIR" "$SERVICE_USER" <<'SCRIPT'
set -euo pipefail
app_home="$1"
expected_cache_dir="$2"
service_user="$3"

cd -P -- "$app_home"
[ "$(pwd -P)" = "$app_home" ]
if [ -L .cache ]; then
  exit 1
fi
if [ ! -e .cache ]; then
  mkdir -- .cache
elif [ ! -d .cache ]; then
  exit 1
fi
cd -P -- .cache
[ "$(pwd -P)" = "${app_home}/.cache" ]
if [ -L binance-alpha-volume-alert ]; then
  exit 1
fi
cache_created=0
if [ ! -e binance-alpha-volume-alert ]; then
  mkdir -- binance-alpha-volume-alert
  cache_created=1
elif [ ! -d binance-alpha-volume-alert ]; then
  exit 1
fi
cd -P -- binance-alpha-volume-alert
[ "$(pwd -P)" = "$expected_cache_dir" ]
if (( cache_created )); then
  chown -- "${service_user}:${service_user}" .
  chmod 0750 .
fi
SCRIPT
then
  fail "The dedicated cache directory could not be prepared safely."
fi

if [ "$(realpath "$EXPECTED_CACHE_DIR")" != "$EXPECTED_CACHE_DIR" ]; then
  fail "The dedicated cache directory changed after preparation."
fi
PROBE_NAME=".alpha-dashboard-install-write-probe.$$"
if ! sudo -u "$SERVICE_USER" sh -c '
set -eu
cache_dir="$1"
probe_name="$2"
cd -P -- "$cache_dir"
[ "$(pwd -P)" = "$cache_dir" ]
cleanup() {
  rm -f -- "$probe_name"
}
trap cleanup EXIT HUP INT TERM
(set -C; : > "$probe_name") 2>/dev/null
rm -- "$probe_name"
trap - EXIT HUP INT TERM
' sh "$EXPECTED_CACHE_DIR" "$PROBE_NAME"; then
  fail "SERVICE_USER cannot write the dedicated cache directory safely."
fi
if [ "$(realpath "$EXPECTED_CACHE_DIR")" != "$EXPECTED_CACHE_DIR" ]; then
  fail "The dedicated cache directory changed during the write probe."
fi

if (( HAD_OLD_UNIT )); then
  sudo cp -a -- "$UNIT_PATH" "$BACKUP_PATH"
fi

UNIT_TMP="$(mktemp)"
INSTALL_STARTED=0
cleanup() {
  local exit_code=$?
  trap - EXIT
  if ! rm -f "$UNIT_TMP"; then
    printf 'Warning: failed to clean local unit temporary file.\n' >&2
  fi
  if ! sudo rm -f -- "$UNIT_STAGE" >/dev/null 2>&1; then
    printf 'Warning: failed to clean unit stage: %s\n' "$UNIT_STAGE" >&2
  fi
  exit "$exit_code"
}
trap cleanup EXIT

rollback() {
  local exit_code="${1:-1}"
  trap - ERR
  set +e
  if (( INSTALL_STARTED )); then
    if (( HAD_OLD_UNIT )); then
      if ! sudo rm -f -- "$UNIT_PATH"; then
        printf 'Rollback failed at remove-current-unit: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
      if ! sudo cp -a -- "$BACKUP_PATH" "$UNIT_PATH"; then
        if ! sudo install -m 0644 "$UNIT_TMP" "$UNIT_STAGE" || ! sudo mv "$UNIT_STAGE" "$UNIT_PATH"; then
          printf 'Warning: failed to retain the new unit after restore failure.\n' >&2
        fi
        printf 'Rollback failed at restore-unit: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
      if ! sudo systemctl daemon-reload; then
        printf 'Rollback failed at daemon-reload: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
      case "$WAS_ENABLE_STATE" in
        enabled)
          if ! sudo systemctl enable "${SERVICE_NAME}.service"; then
            printf 'Rollback failed at restore-enable-state: %s\n' "$UNIT_PATH" >&2
            exit "$exit_code"
          fi
          ;;
        disabled)
          if ! sudo systemctl disable "${SERVICE_NAME}.service"; then
            printf 'Rollback failed at restore-enable-state: %s\n' "$UNIT_PATH" >&2
            exit "$exit_code"
          fi
          ;;
      esac
      if [ "$WAS_ACTIVE_STATE" = "active" ]; then
        if ! sudo systemctl restart "${SERVICE_NAME}.service"; then
          printf 'Rollback failed at restore-active-state: %s\n' "$UNIT_PATH" >&2
          exit "$exit_code"
        fi
      elif ! sudo systemctl stop "${SERVICE_NAME}.service"; then
        printf 'Rollback failed at restore-active-state: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
    else
      if ! sudo rm -f -- "$UNIT_PATH"; then
        printf 'Rollback failed at remove-new-unit: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
      if ! sudo systemctl daemon-reload; then
        printf 'Rollback failed at daemon-reload: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
      if ! sudo systemctl disable --now "${SERVICE_NAME}.service"; then
        printf 'Rollback failed at disable-new-unit: %s\n' "$UNIT_PATH" >&2
        exit "$exit_code"
      fi
    fi
    printf 'Rollback completed for unit: %s\n' "$UNIT_PATH" >&2
  fi
  exit "$exit_code"
}
trap 'rollback $?' ERR

cat >"$UNIT_TMP" <<EOF
[Unit]
Description=Binance Alpha competition dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=-/home/ubuntu/.config/wangge/grid_web_controller.env
EnvironmentFile=-/home/ubuntu/.config/binance-alpha-volume-alert.env
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${APP_DIR}/src
Environment=ALPHA_COMPETITION_RULE_CACHE=${RULE_CACHE}
Environment=ALPHA_COMPETITION_DISCOVERY_CACHE=${DISCOVERY_CACHE}
ExecStart=${PYTHON_BIN} -m grid_optimizer.alpha_competition_dashboard --host ${HOST} --port ${PORT}
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

INSTALL_STARTED=1
sudo install -m 0644 "$UNIT_TMP" "$UNIT_STAGE"
sudo mv "$UNIT_STAGE" "$UNIT_PATH"
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}.service"
sudo systemctl restart "${SERVICE_NAME}.service"
sudo systemctl is-active --quiet "${SERVICE_NAME}.service"

trap - ERR
INSTALL_STARTED=0
printf 'Installed unit: %s\n' "$UNIT_PATH"
if (( HAD_OLD_UNIT )); then
  printf 'Backup unit: %s\n' "$BACKUP_PATH"
else
  printf 'Backup unit: none\n'
fi
printf 'Service status: active\n'
