#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

"$SCRIPT_DIR/start-server.sh"
sleep 2
"$SCRIPT_DIR/start-studio.sh"
echo "图数据库和 Web 控制台已后台启动。"
