from __future__ import annotations

import heapq
import threading
from collections import defaultdict
from time import monotonic, time
from typing import Iterable

from taskrunner.shared.models import QueueSnapshot, TaskEnvelope, TaskStatus, WorkerInfo


class QueueFullError(RuntimeError):
    pass


class TaskNotFoundError(KeyError):
    pass


class InMemoryTaskQueue:
    """Thread-safe priority queue with delayed tasks, leases, acks, retries, and DLQ."""

    def __init__(self, max_depth: int = 10_000) -> None:
        self.max_depth = max_depth
        self._condition = threading.Condition(threading.RLock())
        self._ready: dict[str, list[tuple[int, float, int, str]]] = defaultdict(list)
        self._delayed: list[tuple[float, int, str]] = []
        self._tasks: dict[str, TaskEnvelope] = {}
        self._inflight: dict[str, TaskEnvelope] = {}
        self._dead_letter: dict[str, TaskEnvelope] = {}
        self._cancelled: set[str] = set()
        self._sequence = 0

    def submit(self, task: TaskEnvelope) -> TaskEnvelope:
        with self._condition:
            if self.depth >= self.max_depth:
                raise QueueFullError(f"queue depth {self.depth} reached max {self.max_depth}")
            task.spec = task.spec.normalized()
            task.status = TaskStatus.QUEUED
            task.available_at = time() + task.spec.delay_seconds
            self._tasks[task.task_id] = task
            self._push(task)
            self._condition.notify_all()
            return task

    def submit_many(self, tasks: Iterable[TaskEnvelope]) -> list[TaskEnvelope]:
        submitted: list[TaskEnvelope] = []
        with self._condition:
            for task in tasks:
                if self.depth >= self.max_depth:
                    raise QueueFullError(f"queue depth {self.depth} reached max {self.max_depth}")
                task.spec = task.spec.normalized()
                task.status = TaskStatus.QUEUED
                task.available_at = time() + task.spec.delay_seconds
                self._tasks[task.task_id] = task
                self._push(task)
                submitted.append(task)
            self._condition.notify_all()
        return submitted

    def reserve(
        self,
        worker: WorkerInfo,
        completed_dependencies: set[str] | None = None,
        lease_seconds: float = 30.0,
        timeout: float | None = None,
    ) -> TaskEnvelope | None:
        deadline = None if timeout is None else monotonic() + timeout
        completed = completed_dependencies or set()
        with self._condition:
            while True:
                self._promote_delayed_locked()
                task = self._pop_eligible_locked(worker, completed)
                if task is not None:
                    task.status = TaskStatus.RUNNING
                    task.assigned_worker = worker.worker_id
                    task.lease_deadline = time() + lease_seconds
                    self._inflight[task.task_id] = task
                    return task
                if timeout == 0:
                    return None
                remaining = None if deadline is None else max(0.0, deadline - monotonic())
                if remaining == 0:
                    return None
                self._condition.wait(timeout=remaining if remaining is not None else 0.5)

    def ack(self, task_id: str) -> TaskEnvelope:
        with self._condition:
            task = self._inflight.pop(task_id, None)
            if task is None:
                task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            task.status = TaskStatus.ACKED
            task.lease_deadline = None
            return task

    def complete(self, task_id: str, status: TaskStatus, error: str | None = None) -> TaskEnvelope:
        with self._condition:
            task = self._inflight.pop(task_id, None) or self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            task.status = status
            task.last_error = error
            task.lease_deadline = None
            if status == TaskStatus.DEAD_LETTERED:
                self._dead_letter[task_id] = task
            self._condition.notify_all()
            return task

    def retry(self, task_id: str, reason: str) -> TaskEnvelope:
        with self._condition:
            task = self._inflight.pop(task_id, None) or self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            task.attempts += 1
            task.last_error = reason
            task.assigned_worker = None
            task.lease_deadline = None
            if task.attempts > task.spec.max_retries:
                task.status = TaskStatus.DEAD_LETTERED
                self._dead_letter[task.task_id] = task
            else:
                task.status = TaskStatus.RETRYING
                task.available_at = time() + (task.spec.retry_backoff_seconds * task.attempts)
                self._push(task)
            self._condition.notify_all()
            return task

    def cancel(self, task_id: str) -> TaskEnvelope:
        with self._condition:
            task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            task.status = TaskStatus.CANCELLED
            self._cancelled.add(task_id)
            self._inflight.pop(task_id, None)
            self._condition.notify_all()
            return task

    def requeue_expired_leases(self) -> list[TaskEnvelope]:
        now = time()
        expired: list[TaskEnvelope] = []
        with self._condition:
            for task_id, task in list(self._inflight.items()):
                if task.lease_deadline is not None and task.lease_deadline < now:
                    self._inflight.pop(task_id, None)
                    task.assigned_worker = None
                    expired.append(self.retry(task_id, "lease expired"))
            self._condition.notify_all()
        return expired

    def batch(self, batch_key: str, max_items: int = 100) -> list[TaskEnvelope]:
        with self._condition:
            return [
                task
                for task in self._tasks.values()
                if task.spec.batch_key == batch_key and task.status == TaskStatus.QUEUED
            ][:max_items]

    def get(self, task_id: str) -> TaskEnvelope:
        with self._condition:
            task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            return task

    def snapshot(self) -> QueueSnapshot:
        with self._condition:
            self._promote_delayed_locked()
            queued = sum(len(queue) for queue in self._ready.values())
            delayed = len(self._delayed)
            inflight = len(self._inflight)
            dead = len(self._dead_letter)
            active_depth = queued + delayed + inflight
            return QueueSnapshot(
                queued=queued,
                delayed=delayed,
                inflight=inflight,
                dead_lettered=dead,
                pressure=active_depth / max(1, self.max_depth),
            )

    @property
    def depth(self) -> int:
        return len(self._tasks) - len(
            [
                task
                for task in self._tasks.values()
                if task.status
                in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.DEAD_LETTERED}
            ]
        )

    def _push(self, task: TaskEnvelope) -> None:
        self._sequence += 1
        if task.available_at > time():
            heapq.heappush(self._delayed, (task.available_at, self._sequence, task.task_id))
        else:
            heapq.heappush(
                self._ready[task.spec.queue],
                (task.spec.priority, task.created_at, self._sequence, task.task_id),
            )

    def _promote_delayed_locked(self) -> None:
        now = time()
        while self._delayed and self._delayed[0][0] <= now:
            _, _, task_id = heapq.heappop(self._delayed)
            task = self._tasks.get(task_id)
            if task and task.status not in {TaskStatus.CANCELLED, TaskStatus.DEAD_LETTERED}:
                heapq.heappush(
                    self._ready[task.spec.queue],
                    (task.spec.priority, task.created_at, self._sequence, task.task_id),
                )

    def _pop_eligible_locked(
        self,
        worker: WorkerInfo,
        completed_dependencies: set[str],
    ) -> TaskEnvelope | None:
        best: tuple[int, float, int, str] | None = None
        best_queue: str | None = None
        for queue_name in worker.queues:
            queue = self._ready.get(queue_name)
            if not queue:
                continue
            while queue and queue[0][3] in self._cancelled:
                heapq.heappop(queue)
            if not queue:
                continue
            candidate = queue[0]
            task = self._tasks[candidate[3]]
            if not worker.accepts(task):
                continue
            if task.spec.depends_on and not task.spec.depends_on <= completed_dependencies:
                continue
            if best is None or candidate < best:
                best = candidate
                best_queue = queue_name
        if best is None or best_queue is None:
            return None
        heapq.heappop(self._ready[best_queue])
        return self._tasks[best[3]]
