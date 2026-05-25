#!/usr/bin/env bash
set -euo pipefail

export TASKRUNNER_API_HOST="${TASKRUNNER_API_HOST:-0.0.0.0}"
export TASKRUNNER_SCHEDULER_HOST="${TASKRUNNER_SCHEDULER_HOST:-0.0.0.0}"
export TASKRUNNER_SQLITE_PATH="${TASKRUNNER_SQLITE_PATH:-data/taskrunner.db}"

exec taskrunner scheduler
