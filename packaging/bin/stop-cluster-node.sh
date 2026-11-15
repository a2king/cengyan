#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "$0")/cluster-env.sh" "$@"
[[ -f "$PID_FILE" ]] || { echo "$INSTANCE: no managed PID (foreground/systemd: stop via supervisor)"; exit 0; }
pid="$(cat "$PID_FILE")"
if ! matches_process "$pid"; then
    echo "[ERROR] Stale/mismatched PID file: $PID_FILE; refusing to signal another process" >&2
    exit 1
fi
kill -TERM "$pid"
for ((i=0; i<60; i++)); do
    if ! matches_process "$pid"; then
        rm -f -- "$PID_FILE"
        echo "$INSTANCE stopped"
        exit 0
    fi
    sleep 1
done
echo "[ERROR] $INSTANCE has not stopped in 60 seconds; no automatic kill -9. Check $LOG_FILE" >&2
exit 1
