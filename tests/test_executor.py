from __future__ import annotations

import pytest

from taskrunner.shared.models import TaskEnvelope, TaskKind, TaskSpec, TaskStatus
from taskrunner.workers.executor import TaskExecutor


@pytest.mark.asyncio
async def test_executor_runs_async_task() -> None:
    executor = TaskExecutor(max_processes=1, max_threads=2)
    try:
        task = TaskEnvelope(
            TaskSpec(
                target="sample_tasks.io_job:run",
                kind=TaskKind.ASYNC,
                payload={"url": "local", "delay": 0.01},
            )
        )
        result = await executor.execute(task, "worker-1")
        assert result.status == TaskStatus.SUCCEEDED
        assert result.result["url"] == "local"
    finally:
        executor.shutdown()


@pytest.mark.asyncio
async def test_executor_captures_failures_for_retry() -> None:
    executor = TaskExecutor(max_processes=1, max_threads=2)
    try:
        task = TaskEnvelope(
            TaskSpec(
                target="sample_tasks.failing_job:run",
                kind=TaskKind.IO,
                payload={"message": "expected"},
            )
        )
        result = await executor.execute(task, "worker-1")
        assert result.status == TaskStatus.FAILED
        assert "expected" in result.error
    finally:
        executor.shutdown()
