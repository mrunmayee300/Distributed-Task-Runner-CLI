from __future__ import annotations

import asyncio
import os
import socket
from contextlib import suppress
from dataclasses import asdict
from time import time
from typing import Any

from taskrunner.database.store import SQLiteStore
from taskrunner.queue.broker import InMemoryTaskQueue
from taskrunner.shared.config import Settings, settings
from taskrunner.shared.events import EventBus
from taskrunner.shared.locks import DistributedLockManager
from taskrunner.shared.models import (
    ClusterEvent,
    EventType,
    QueueSnapshot,
    TaskEnvelope,
    TaskResult,
    TaskSpec,
    TaskStatus,
    WorkerInfo,
    WorkerStatus,
)


class SchedulerEngine:
    def __init__(
        self,
        config: Settings = settings,
        queue: InMemoryTaskQueue | None = None,
        store: SQLiteStore | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self.config = config
        self.queue = queue or InMemoryTaskQueue(max_depth=config.max_queue_depth)
        self.store = store or SQLiteStore(config.sqlite_path)
        self.event_bus = event_bus or EventBus()
        self.locks = DistributedLockManager()
        self.workers: dict[str, WorkerInfo] = {}
        self.completed_tasks: set[str] = set()
        self._results: dict[str, TaskResult] = {}
        self._drain_event = asyncio.Event()
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._services_started = False

    async def start_background_services(self) -> None:
        if self._services_started:
            return
        self._services_started = True
        for coro in (self._heartbeat_reaper(), self._lease_reaper(), self._pressure_reporter()):
            task = asyncio.create_task(coro)
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def stop_background_services(self) -> None:
        for task in list(self._background_tasks):
            task.cancel()
        for task in list(self._background_tasks):
            with suppress(asyncio.CancelledError):
                await task
        self._services_started = False

    async def submit(self, spec: TaskSpec) -> TaskEnvelope:
        async with self.locks.acquire(f"queue:{spec.queue}"):
            task = self.queue.submit(TaskEnvelope(spec=spec.normalized()))
            self.store.upsert_task(task)
        await self.event_bus.publish(
            ClusterEvent(
                EventType.TASK_SUBMITTED,
                {"task_id": task.task_id, "queue": task.spec.queue},
            )
        )
        self._drain_event.set()
        return task

    async def submit_batch(self, specs: list[TaskSpec]) -> list[TaskEnvelope]:
        tasks = [TaskEnvelope(spec=spec.normalized()) for spec in specs]
        submitted = self.queue.submit_many(tasks)
        for task in submitted:
            self.store.upsert_task(task)
        await self.event_bus.publish(
            ClusterEvent(EventType.TASK_SUBMITTED, {"batch_size": len(submitted)})
        )
        self._drain_event.set()
        return submitted

    async def register_worker(self, worker: WorkerInfo) -> WorkerInfo:
        worker.status = WorkerStatus.IDLE
        worker.last_heartbeat = time()
        self.workers[worker.worker_id] = worker
        self.store.upsert_worker(worker)
        await self.event_bus.publish(
            ClusterEvent(EventType.WORKER_REGISTERED, {"worker_id": worker.worker_id})
        )
        self._drain_event.set()
        return worker

    async def heartbeat(self, worker_id: str, running_tasks: int) -> WorkerInfo | None:
        worker = self.workers.get(worker_id)
        if worker is None:
            return None
        worker.last_heartbeat = time()
        worker.running_tasks = running_tasks
        worker.status = WorkerStatus.BUSY if running_tasks else WorkerStatus.IDLE
        self.store.upsert_worker(worker)
        await self.event_bus.publish(
            ClusterEvent(
                EventType.WORKER_HEARTBEAT,
                {"worker_id": worker.worker_id, "running_tasks": running_tasks},
            )
        )
        return worker

    async def reserve_task(self, worker_id: str, timeout: float = 1.0) -> TaskEnvelope | None:
        worker = self.workers.get(worker_id)
        if worker is None or worker.status == WorkerStatus.OFFLINE:
            return None
        task = await asyncio.to_thread(
            self.queue.reserve,
            worker,
            self.completed_tasks,
            self.config.lease_seconds,
            timeout,
        )
        if task is None:
            return None
        worker.running_tasks += 1
        worker.status = WorkerStatus.BUSY
        self.store.upsert_worker(worker)
        self.store.upsert_task(task)
        await self.event_bus.publish(
            ClusterEvent(
                EventType.TASK_DISPATCHED,
                {"task_id": task.task_id, "worker_id": worker_id, "queue": task.spec.queue},
            )
        )
        return task

    async def report_result(self, result: TaskResult) -> TaskEnvelope:
        task = self.queue.get(result.task_id)
        worker = self.workers.get(result.worker_id)
        if worker is not None:
            worker.running_tasks = max(0, worker.running_tasks - 1)
            worker.status = WorkerStatus.IDLE if worker.running_tasks == 0 else WorkerStatus.BUSY
            self.store.upsert_worker(worker)

        if result.status == TaskStatus.SUCCEEDED:
            task = self.queue.complete(result.task_id, TaskStatus.SUCCEEDED)
            self.completed_tasks.add(result.task_id)
            event_type = EventType.TASK_FINISHED
        elif result.status == TaskStatus.CANCELLED:
            task = self.queue.complete(result.task_id, TaskStatus.CANCELLED, result.error)
            event_type = EventType.TASK_FAILED
        else:
            task = self.queue.retry(result.task_id, result.error or "task failed")
            event_type = EventType.TASK_FAILED

        self._results[result.task_id] = result
        self.store.upsert_task(task, result)
        self.store.append_log(
            result.task_id,
            "ERROR" if result.error else "INFO",
            result.error or f"completed in {result.duration_seconds:.3f}s",
        )
        await self.event_bus.publish(
            ClusterEvent(
                event_type,
                {
                    "task_id": result.task_id,
                    "worker_id": result.worker_id,
                    "status": str(task.status),
                    "duration_seconds": result.duration_seconds,
                },
            )
        )
        self._drain_event.set()
        return task

    async def cancel_task(self, task_id: str) -> TaskEnvelope:
        task = self.queue.cancel(task_id)
        self.store.upsert_task(task)
        await self.event_bus.publish(
            ClusterEvent(
                EventType.TASK_FAILED,
                {"task_id": task_id, "status": str(TaskStatus.CANCELLED)},
            )
        )
        return task

    async def retry_task(self, task_id: str) -> TaskEnvelope:
        task = self.queue.retry(task_id, "manual retry")
        self.store.upsert_task(task)
        self._drain_event.set()
        return task

    def queue_snapshot(self) -> QueueSnapshot:
        return self.queue.snapshot()

    def metrics(self) -> dict[str, Any]:
        queue = self.queue.snapshot()
        workers = list(self.workers.values())
        return {
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "queue": asdict(queue),
            "workers": [worker.to_dict() for worker in workers],
            "tasks": self.store.task_counts(),
            "results": len(self._results),
        }

    def autoscaling_recommendation(self) -> dict[str, Any]:
        snapshot = self.queue.snapshot()
        online_workers = [
            worker for worker in self.workers.values() if worker.status != WorkerStatus.OFFLINE
        ]
        total_capacity = sum(worker.capacity for worker in online_workers) or 1
        backlog_ratio = int((snapshot.queued + snapshot.delayed) / total_capacity)
        desired = max(1, min(32, backlog_ratio + len(online_workers)))
        return {
            "current_workers": len(online_workers),
            "desired_workers": desired,
            "pressure": snapshot.pressure,
        }

    async def _heartbeat_reaper(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            now = time()
            for worker in list(self.workers.values()):
                if (
                    worker.status != WorkerStatus.OFFLINE
                    and now - worker.last_heartbeat > self.config.heartbeat_timeout_seconds
                ):
                    worker.status = WorkerStatus.OFFLINE
                    self.store.upsert_worker(worker)
                    self.store.mark_worker_offline(worker.worker_id)

    async def _lease_reaper(self) -> None:
        while True:
            await asyncio.sleep(max(1.0, self.config.lease_seconds / 3))
            expired = await asyncio.to_thread(self.queue.requeue_expired_leases)
            for task in expired:
                self.store.upsert_task(task)

    async def _pressure_reporter(self) -> None:
        while True:
            await asyncio.sleep(5)
            snapshot = self.queue.snapshot()
            if snapshot.pressure >= 0.8:
                await self.event_bus.publish(
                    ClusterEvent(EventType.QUEUE_PRESSURE, {"pressure": snapshot.pressure})
                )


def build_worker_info(
    worker_id: str,
    queues: set[str] | None = None,
    labels: set[str] | None = None,
    capacity: int = 1,
) -> WorkerInfo:
    return WorkerInfo(
        worker_id=worker_id,
        host=socket.gethostname(),
        pid=os.getpid(),
        queues=queues or {"default"},
        labels=labels or set(),
        capacity=capacity,
        status=WorkerStatus.IDLE,
    )
