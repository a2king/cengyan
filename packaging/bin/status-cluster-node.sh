#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "$0")/cluster-env.sh" "$@"
if [[ -f "$PID_FILE" ]] && matches_process "$(cat "$PID_FILE")"; then
    echo "$INSTANCE running: PID=$(cat "$PID_FILE") log=$LOG_FILE"
else
    echo "$INSTANCE: no matching managed process (foreground/systemd: check supervisor)"
    exit 1
fi
