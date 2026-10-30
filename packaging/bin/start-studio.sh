#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
APP_HOME="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$APP_HOME/pid"
LOG_DIR="$APP_HOME/logs"
PID_FILE="$PID_DIR/studio.pid"
LOG_FILE="$LOG_DIR/studio.log"

source "$SCRIPT_DIR/check-java.sh"
require_java21

[[ -f "$APP_HOME/cengyan-studio.jar" ]] || { echo "[ERROR] 未找到 $APP_HOME/cengyan-studio.jar" >&2; exit 1; }

STUDIO_HOST=0.0.0.0
STUDIO_PORT=19670
DB_HOST=127.0.0.1
DB_PORT=19669
if [[ -f "$APP_HOME/config/studio.properties" ]]; then
    value_for() {
        awk -F= -v key="$1" '$1 ~ "^[[:space:]]*" key "[[:space:]]*$" {sub(/^[[:space:]]*/, "", $2); sub(/[[:space:]]*$/, "", $2); print $2; exit}' "$APP_HOME/config/studio.properties"
    }
    [[ -n "$(value_for host)" ]] && STUDIO_HOST="$(value_for host)"
    [[ -n "$(value_for port)" ]] && STUDIO_PORT="$(value_for port)"
    [[ -n "$(value_for db_host)" ]] && DB_HOST="$(value_for db_host)"
    [[ -n "$(value_for db_port)" ]] && DB_PORT="$(value_for db_port)"
fi

mkdir -p "$PID_DIR" "$LOG_DIR"

if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
        echo "[ERROR] Web 控制台已经在运行，PID=$old_pid。" >&2
        exit 1
    fi
    rm -f "$PID_FILE"
fi

cd "$APP_HOME"
nohup "$JAVA_EXE" -jar "$APP_HOME/cengyan-studio.jar" \
    --host "$STUDIO_HOST" \
    --port "$STUDIO_PORT" \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    >>"$LOG_FILE" 2>&1 < /dev/null &
pid=$!
echo "$pid" > "$PID_FILE"

sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
    echo "[ERROR] Web 控制台启动失败，请查看日志：$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
fi

echo "Web 控制台已后台启动，PID=$pid，日志：$LOG_FILE"
