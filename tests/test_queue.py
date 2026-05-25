from __future__ import annotations

from time import sleep

from taskrunner.queue.broker import InMemoryTaskQueue
from taskrunner.scheduler.engine import build_worker_info
from taskrunner.shared.models import TaskEnvelope, TaskSpec, TaskStatus


def test_priority_delay_retry_and_dead_letter() -> None:
    queue = InMemoryTaskQueue(max_depth=10)
    worker = build_worker_info("worker-1", queues={"default"}, labels={"gpu"}, capacity=1)

    slow = queue.submit(
        TaskEnvelope(
            TaskSpec(target="sample_tasks.io_job:run", priority=50, delay_seconds=0.05)
        )
    )
    fast = queue.submit(TaskEnvelope(TaskSpec(target="sample_tasks.io_job:run", priority=1)))

    assert queue.reserve(worker, set(), timeout=0).task_id == fast.task_id
    assert queue.reserve(worker, set(), timeout=0) is None

    sleep(0.06)
    assert queue.reserve(worker, set(), timeout=0).task_id == slow.task_id
    retried = queue.retry(slow.task_id, "network")
    assert retried.status == TaskStatus.RETRYING
    retried = queue.retry(slow.task_id, "network")
    retried = queue.retry(slow.task_id, "network")
    retried = queue.retry(slow.task_id, "network")
    assert retried.status == TaskStatus.DEAD_LETTERED


def test_affinity_and_dependencies_gate_reservation() -> None:
    queue = InMemoryTaskQueue(max_depth=10)
    cpu_worker = build_worker_info("worker-cpu", queues={"default"}, labels={"cpu"}, capacity=1)
    gpu_worker = build_worker_info("worker-gpu", queues={"default"}, labels={"gpu"}, capacity=1)
    task = queue.submit(
        TaskEnvelope(
            TaskSpec(
                target="sample_tasks.image_job:run",
                affinity={"gpu"},
                depends_on={"task-parent"},
            )
        )
    )

    assert queue.reserve(cpu_worker, {"task-parent"}, timeout=0) is None
    assert queue.reserve(gpu_worker, set(), timeout=0) is None
    assert queue.reserve(gpu_worker, {"task-parent"}, timeout=0).task_id == task.task_id
