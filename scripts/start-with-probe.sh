#!/bin/bash

set -euo pipefail

PID_FILE="${SECFLOW_MAIN_PID_FILE:-/tmp/secflow-main.pid}"
STARTED_AT_FILE="${SECFLOW_MAIN_STARTED_AT_FILE:-/tmp/secflow-main.started_at}"
SERVICE_NAME="${SECFLOW_PROBE_SERVICE_NAME:-secflow-app}"

PYTHON_BIN="$(command -v python3 || command -v python)"
rm -f "${PID_FILE}" "${STARTED_AT_FILE}"

start_probe() {
    echo "[${SERVICE_NAME}] starting independent probe process"
    "${PYTHON_BIN}" -m app.probe_process &
    probe_pid=$!
    echo "[${SERVICE_NAME}] probe pid=${probe_pid}"
    # 保护 probe 不被 OOM killer 杀掉
    echo -1000 > /proc/${probe_pid}/oom_score_adj 2>/dev/null || true
}

start_probe

echo "[${SERVICE_NAME}] starting main process: $*"
"$@" &
main_pid=$!
# 保护 main 进程, 优先杀任务子进程
main_oom_score="${SECFLOW_MAIN_OOM_SCORE:--500}"
echo "${main_oom_score}" > /proc/${main_pid}/oom_score_adj 2>/dev/null || true

printf '%s\n' "${main_pid}" > "${PID_FILE}"
date +%s > "${STARTED_AT_FILE}"
echo "[${SERVICE_NAME}] main pid=${main_pid} probe pid=${probe_pid}"

terminate_children() {
    kill -TERM "${main_pid}" 2>/dev/null || true
    kill -TERM "${probe_pid}" 2>/dev/null || true
}

cleanup() {
    local exit_code="$1"
    echo "[${SERVICE_NAME}] stopping with exit_code=${exit_code}"
    rm -f "${PID_FILE}" "${STARTED_AT_FILE}"
    kill -TERM "${probe_pid}" 2>/dev/null || true
    wait "${probe_pid}" 2>/dev/null || true
    exit "${exit_code}"
}

trap 'terminate_children' TERM INT

set +e
while kill -0 "${main_pid}" 2>/dev/null; do
    if ! kill -0 "${probe_pid}" 2>/dev/null; then
        echo "[${SERVICE_NAME}] probe exited unexpectedly; restarting"
        start_probe
    fi
    sleep 1
done
wait "${main_pid}"
main_status=$?
set -e

cleanup "${main_status}"
