#!/usr/bin/env bash
set -euo pipefail
# Foreground by default (systemd/container compatible); --daemon enables PID/log management.
MODE=foreground
if [[ "${1:-}" == --daemon ]]; then MODE=daemon; shift; fi
source "$(dirname -- "$0")/cluster-env.sh" "$@"
source "$SCRIPT_DIR/check-java.sh"
require_java21
LOG_CONFIG="$APP_HOME/config/logback.xml"
[[ -r "$LOG_CONFIG" ]] || { echo "[ERROR] Logback config not found/readable: $LOG_CONFIG" >&2; exit 1; }
[[ -f "$JAR" ]] || { echo "[ERROR] JAR not found: $JAR" >&2; exit 1; }
command -v flock >/dev/null || { echo '[ERROR] Install util-linux (flock).' >&2; exit 1; }
umask 077
mkdir -p "$APP_HOME/pid" "$APP_HOME/logs" "$APP_HOME/tmp/$INSTANCE"
exec 9>"$APP_HOME/pid/$INSTANCE.lock"
flock -n 9 || { echo "[ERROR] $INSTANCE already starting/running" >&2; exit 1; }
if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE")"
    if matches_process "$old_pid"; then echo "[ERROR] $INSTANCE running: PID=$old_pid" >&2; exit 1; fi
    if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
        echo '[ERROR] PID belongs to another process; check PID file manually.' >&2; exit 1
    fi
    rm -f -- "$PID_FILE"
fi
# Options are whitespace-delimited JVM arguments, never evaluated as shell code.
read -r -a options <<< "${JAVA_OPTS:--Xms256m -Xmx1g}"
if [[ "$ROLE" == storage ]]; then extra="${STORAGE_JAVA_OPTS:-}"; else extra="${COMPUTE_JAVA_OPTS:-}"; fi
read -r -a extra_options <<< "$extra"
# Bash <= 4.3 treats an empty array as unset under nounset. The conditional
# expansion emits zero arguments for an empty array, not an empty JVM argument.
cd "$APP_HOME"
if [[ "$MODE" == foreground ]]; then
    exec "$JAVA_EXE" ${options[@]+"${options[@]}"} ${extra_options[@]+"${extra_options[@]}"} "-Dlogback.configurationFile=$LOG_CONFIG" "-Djava.io.tmpdir=$APP_HOME/tmp/$INSTANCE" -jar "$JAR" "$CONFIG"
fi
nohup "$JAVA_EXE" ${options[@]+"${options[@]}"} ${extra_options[@]+"${extra_options[@]}"} "-Dlogback.configurationFile=$LOG_CONFIG" "-Djava.io.tmpdir=$APP_HOME/tmp/$INSTANCE" -jar "$JAR" "$CONFIG" >>"$LOG_FILE" 2>&1 < /dev/null &
pid=$!
echo "$pid" > "$PID_FILE"
sleep 2
if ! matches_process "$pid"; then
    rm -f -- "$PID_FILE"
    echo "[ERROR] $INSTANCE exited. Check $LOG_FILE" >&2; exit 1
fi
echo "$INSTANCE started: PID=$pid log=$LOG_FILE (process alive; not a Raft readiness check)"
