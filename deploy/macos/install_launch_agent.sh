#!/bin/zsh
set -euo pipefail

LABEL="${LABEL:-com.tl.grid-optimizer.web}"
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
AGENT_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$AGENT_DIR/${LABEL}.plist"
LOG_DIR="$ROOT_DIR/output/launchd"
LAUNCHD_LOG_DIR="$HOME/Library/Logs/grid-optimizer-web"
RUNTIME_DIR="$HOME/Library/Application Support/grid-optimizer-web"
STDOUT_LOG="$LAUNCHD_LOG_DIR/stdout.log"
STDERR_LOG="$LAUNCHD_LOG_DIR/stderr.log"
START_SCRIPT="$ROOT_DIR/deploy/macos/start_grid_web.sh"
WRAPPER_SCRIPT="$RUNTIME_DIR/start_grid_web.sh"
PYTHON_BIN="${PYTHON_BIN:-/opt/anaconda3/bin/python3}"
GRID_WEB_HOST="${GRID_WEB_HOST:-127.0.0.1}"
GRID_WEB_PORT="${GRID_WEB_PORT:-8787}"
GRID_API_ENV_FILE="${GRID_API_ENV_FILE:-/tmp/binance_api_env.sh}"
USER_UID="$(id -u)"

mkdir -p "$AGENT_DIR" "$LOG_DIR" "$LAUNCHD_LOG_DIR" "$RUNTIME_DIR"
chmod +x "$START_SCRIPT"

cat > "$WRAPPER_SCRIPT" <<EOF
#!/bin/zsh
set -euo pipefail
cd "${ROOT_DIR}"
export PYTHONPATH="${ROOT_DIR}/src"
export GRID_API_ENV_FILE="${GRID_API_ENV_FILE}"
if [[ -f "${GRID_API_ENV_FILE}" ]]; then
  source "${GRID_API_ENV_FILE}"
fi
exec "${PYTHON_BIN}" -m grid_optimizer.web --host "${GRID_WEB_HOST}" --port "${GRID_WEB_PORT}"
EOF

chmod +x "$WRAPPER_SCRIPT"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${WRAPPER_SCRIPT}</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHON_BIN</key>
    <string>${PYTHON_BIN}</string>
    <key>GRID_WEB_HOST</key>
    <string>${GRID_WEB_HOST}</string>
    <key>GRID_WEB_PORT</key>
    <string>${GRID_WEB_PORT}</string>
    <key>GRID_API_ENV_FILE</key>
    <string>${GRID_API_ENV_FILE}</string>
  </dict>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${STDOUT_LOG}</string>

  <key>StandardErrorPath</key>
  <string>${STDERR_LOG}</string>
</dict>
</plist>
EOF

launchctl bootout "gui/${USER_UID}" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/${USER_UID}" "$PLIST_PATH"
launchctl enable "gui/${USER_UID}/${LABEL}" >/dev/null 2>&1 || true
launchctl kickstart -k "gui/${USER_UID}/${LABEL}"

echo "Installed: ${PLIST_PATH}"
echo "Label: ${LABEL}"
echo "Wrapper: ${WRAPPER_SCRIPT}"
echo "API env: ${GRID_API_ENV_FILE}"
echo "Logs: ${STDOUT_LOG} / ${STDERR_LOG}"
