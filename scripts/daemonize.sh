#!/usr/bin/env bash
set -euo pipefail

role="${1:-scheduler}"
mkdir -p logs data

case "$role" in
  scheduler)
    nohup scripts/run_scheduler.sh > logs/scheduler.log 2>&1 &
    echo "$!" > data/scheduler.pid
    ;;
  worker)
    worker_name="${2:-worker-1}"
    nohup taskrunner worker --id "$worker_name" > "logs/${worker_name}.log" 2>&1 &
    echo "$!" > "data/${worker_name}.pid"
    ;;
  *)
    echo "usage: scripts/daemonize.sh scheduler|worker [worker-id]" >&2
    exit 2
    ;;
esac
