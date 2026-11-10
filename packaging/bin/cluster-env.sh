#!/usr/bin/env bash
# Shared by cluster role commands. Do not execute directly.
set -euo pipefail
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
APP_HOME="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
ROLE="${1:-}"
case "$ROLE" in storage|compute) ;; *) echo 'Usage: command storage|compute [config]' >&2; exit 2 ;; esac
CONFIG="${2:-$APP_HOME/config/$ROLE.properties}"
[[ -f "$CONFIG" ]] || { echo "[ERROR] Config not found: $CONFIG" >&2; exit 1; }
CONFIG="$(CDPATH= cd -- "$(dirname -- "$CONFIG")" && pwd)/$(basename -- "$CONFIG")"
NODE_ID="$(awk -F= '$1 ~ /^[[:space:]]*node_id[[:space:]]*$/ {sub(/^[^=]*=/, ""); gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print; exit}' "$CONFIG")"
[[ "$NODE_ID" =~ ^[A-Za-z0-9_-]+$ ]] || { echo '[ERROR] Invalid node_id' >&2; exit 1; }
INSTANCE="$ROLE-$NODE_ID"
PID_FILE="$APP_HOME/pid/$INSTANCE.pid"
LOG_FILE="$APP_HOME/logs/$INSTANCE.log"
JAR="$APP_HOME/cengyan-$ROLE.jar"

matches_process() {
    [[ "$1" =~ ^[0-9]+$ ]] && [[ "$1" -gt 1 ]] && kill -0 "$1" 2>/dev/null || return 1
    [[ -r "/proc/$1/cmdline" ]] || return 1
    local found_jar=false found_config=false arg
    # /proc cmdline is NUL-delimited. read -d works on CentOS 7 Bash 4.2,
    # unlike mapfile -d (Bash 4.4+); no empty array expansion under nounset.
    while IFS= read -r -d '' arg; do
        [[ "$arg" != "$JAR" ]] || found_jar=true
        [[ "$arg" != "$CONFIG" ]] || found_config=true
    done < "/proc/$1/cmdline"
    [[ "$found_jar" == true && "$found_config" == true ]]
}
