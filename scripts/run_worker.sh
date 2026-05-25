#!/usr/bin/env bash
set -euo pipefail

export TASKRUNNER_SCHEDULER_HOST="${TASKRUNNER_SCHEDULER_HOST:-127.0.0.1}"
export TASKRUNNER_WORKER_CAPACITY="${TASKRUNNER_WORKER_CAPACITY:-2}"

exec taskrunner worker \
  --queues "${TASKRUNNER_WORKER_QUEUES:-default,cpu,io}" \
  --labels "${TASKRUNNER_WORKER_LABELS:-cpu}"
