#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
APP_HOME="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$APP_HOME/pid"
LOG_DIR="$APP_HOME/logs"
PID_FILE="$PID_DIR/server.pid"
LOG_FILE="$LOG_DIR/server.log"

source "$SCRIPT_DIR/check-java.sh"
require_java21

[[ -f "$APP_HOME/cengyan-server.jar" ]] || { echo "[ERROR] 未找到 $APP_HOME/cengyan-server.jar" >&2; exit 1; }
[[ -f "$APP_HOME/config/cengyan.properties" ]] || { echo "[ERROR] 未找到 $APP_HOME/config/cengyan.properties" >&2; exit 1; }

mkdir -p "$PID_DIR" "$LOG_DIR"

if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
        echo "[ERROR] 图数据库已经在运行，PID=$old_pid。" >&2
        exit 1
    fi
    rm -f "$PID_FILE"
fi

cd "$APP_HOME"
nohup "$JAVA_EXE" -jar "$APP_HOME/cengyan-server.jar" \
    "$APP_HOME/config/cengyan.properties" \
    >>"$LOG_FILE" 2>&1 < /dev/null &
pid=$!
echo "$pid" > "$PID_FILE"

sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
    echo "[ERROR] 图数据库启动失败，请查看日志：$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
fi

echo "图数据库已后台启动，PID=$pid，日志：$LOG_FILE"
