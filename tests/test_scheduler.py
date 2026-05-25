from __future__ import annotations

import asyncio

import pytest

from taskrunner.scheduler.engine import SchedulerEngine, build_worker_info
from taskrunner.shared.config import Settings
from taskrunner.shared.models import TaskResult, TaskSpec, TaskStatus


@pytest.mark.asyncio
async def test_scheduler_requeues_failed_task_and_marks_worker_offline(tmp_path) -> None:
    config = Settings(
        sqlite_path=tmp_path / "taskrunner.db",
        heartbeat_interval_seconds=0.01,
        heartbeat_timeout_seconds=0.02,
        lease_seconds=0.05,
    )
    engine = SchedulerEngine(config)
    await engine.start_background_services()
    try:
        worker = await engine.register_worker(build_worker_info("worker-1", capacity=1))
        task = await engine.submit(TaskSpec(target="sample_tasks.failing_job:run", max_retries=1))
        reserved = await engine.reserve_task(worker.worker_id, timeout=0)
        assert reserved.task_id == task.task_id

        await engine.report_result(
            TaskResult(
                task_id=task.task_id,
                worker_id=worker.worker_id,
                status=TaskStatus.FAILED,
                started_at=1.0,
                finished_at=2.0,
                error="expected failure",
            )
        )
        assert engine.queue.get(task.task_id).status == TaskStatus.RETRYING

        await asyncio.sleep(0.05)
        assert engine.workers["worker-1"].status == "offline"
    finally:
        await engine.stop_background_services()
