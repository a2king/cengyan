#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
APP_HOME="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$APP_HOME/pid/studio.pid"

if [[ ! -f "$PID_FILE" ]]; then
    echo "Web 控制台未运行。"
    exit 0
fi

pid="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
    echo "[WARN] PID 文件无效，已删除：$PID_FILE" >&2
    rm -f "$PID_FILE"
    exit 0
fi

if ! kill -0 "$pid" 2>/dev/null; then
    echo "Web 控制台进程已退出，清理 PID 文件。"
    rm -f "$PID_FILE"
    exit 0
fi

kill "$pid"
for _ in {1..30}; do
    if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "Web 控制台已停止。"
        exit 0
    fi
    sleep 1
done

echo "[WARN] Web 控制台未在 30 秒内退出，发送强制终止信号。" >&2
kill -9 "$pid" 2>/dev/null || true
rm -f "$PID_FILE"
echo "Web 控制台已停止。"
