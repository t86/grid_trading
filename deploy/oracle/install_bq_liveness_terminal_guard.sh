#!/usr/bin/env bash
set -euo pipefail

current="$(crontab -l 2>/dev/null || true)"
updated="$(printf '%s\n' "$current" | sed -E 's#\.venv/bin/python +(/home/ubuntu/[^ ]+/)?output/ops/bq_liveness_watchdog\.py#\.venv/bin/python -m grid_optimizer.bq_liveness_terminal_guard#g')"

if [[ "$updated" == "$current" ]]; then
  echo "No legacy bq liveness cron entry found or already updated."
  exit 0
fi

printf '%s\n' "$updated" | crontab -
echo "Installed target-terminal-aware BQ liveness cron entry."
