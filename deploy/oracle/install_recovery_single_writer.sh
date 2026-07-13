#!/usr/bin/env bash
set -euo pipefail

# Install the ARX/O recovery ownership boundary on the host's existing cron.
# This deliberately touches only legacy, untracked actuator scripts.  Target
# gates remain enabled because they are terminal risk stops, not recovery
# writers.  Health, budget, and liveness jobs remain installed; the tracked
# modules become observe-only after the regular pull-based code update.

current="$(crontab -l 2>/dev/null || true)"
updated="$(printf '%s\n' "$current" | awk '
  /^# recovery-single-writer disabled:/ { print; next }
  /output\/ops\/arxusdt_ledger_drift_monitor\.py/ {
    print "# recovery-single-writer disabled: " $0
    next
  }
  /output\/ops\/ousdt_ledger_drift_monitor\.py/ {
    print "# recovery-single-writer disabled: " $0
    next
  }
  /output\/ops\/rollover_daily_window\.py/ && /--symbols OUSDT,ARXUSDT/ {
    print "# recovery-single-writer disabled: " $0
    next
  }
  { print }
')"

if [ "$updated" != "$current" ]; then
  printf '%s\n' "$updated" | crontab -
fi

printf '%s\n' "recovery-single-writer: legacy ARX/O actuators disabled; target gates preserved"
