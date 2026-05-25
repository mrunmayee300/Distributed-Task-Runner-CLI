from __future__ import annotations

import json
from typing import Any

from taskrunner.shared.models import TaskEnvelope, TaskKind, TaskResult, TaskSpec, TaskStatus, WorkerInfo, WorkerStatus


def spec_to_proto(spec: TaskSpec, pb2: Any) -> Any:
    payload = {key: json.dumps(value, default=str) for key, value in spec.payload.items()}
    return pb2.TaskSpec(
        target=spec.target,
        payload=payload,
        kind=str(spec.kind),
        queue=spec.queue,
        priority=spec.priority,
        delay_seconds=spec.delay_seconds,
        max_retries=spec.max_retries,
        retry_backoff_seconds=spec.retry_backoff_seconds,
        timeout_seconds=spec.timeout_seconds,
        affinity=sorted(spec.affinity),
        depends_on=sorted(spec.depends_on),
        batch_key=spec.batch_key or "",
    )


def spec_from_proto(message: Any) -> TaskSpec:
    payload: dict[str, Any] = {}
    for key, value in message.payload.items():
        try:
            payload[key] = json.loads(value)
        except json.JSONDecodeError:
            payload[key] = value
    return TaskSpec(
        target=message.target,
        payload=payload,
        kind=TaskKind(message.kind or TaskKind.IO),
        queue=message.queue or "default",
        priority=message.priority or 100,
        delay_seconds=message.delay_seconds,
        max_retries=message.max_retries,
        retry_backoff_seconds=message.retry_backoff_seconds or 1.5,
        timeout_seconds=message.timeout_seconds or 60.0,
        affinity=set(message.affinity),
        depends_on=set(message.depends_on),
        batch_key=message.batch_key or None,
    ).normalized()


def task_to_proto(task: TaskEnvelope, pb2: Any) -> Any:
    return pb2.TaskEnvelope(
        task_id=task.task_id,
        spec=spec_to_proto(task.spec, pb2),
        status=str(task.status),
        attempts=task.attempts,
        assigned_worker=task.assigned_worker or "",
        last_error=task.last_error or "",
        created_at=task.created_at,
        available_at=task.available_at,
    )


def task_from_proto(message: Any) -> TaskEnvelope:
    return TaskEnvelope(
        task_id=message.task_id,
        spec=spec_from_proto(message.spec),
        status=TaskStatus(message.status or TaskStatus.PENDING),
        attempts=message.attempts,
        assigned_worker=message.assigned_worker or None,
        last_error=message.last_error or None,
        created_at=message.created_at,
        available_at=message.available_at,
    )


def worker_to_proto(worker: WorkerInfo, pb2: Any) -> Any:
    return pb2.WorkerInfo(
        worker_id=worker.worker_id,
        host=worker.host,
        pid=worker.pid,
        queues=sorted(worker.queues),
        capacity=worker.capacity,
        labels=sorted(worker.labels),
        status=str(worker.status),
        running_tasks=worker.running_tasks,
    )


def worker_from_proto(message: Any) -> WorkerInfo:
    return WorkerInfo(
        worker_id=message.worker_id,
        host=message.host,
        pid=message.pid,
        queues=set(message.queues or ["default"]),
        capacity=message.capacity or 1,
        labels=set(message.labels),
        status=WorkerStatus(message.status or WorkerStatus.STARTING),
        running_tasks=message.running_tasks,
    )


def result_to_proto(result: TaskResult, pb2: Any) -> Any:
    return pb2.TaskResult(
        task_id=result.task_id,
        worker_id=result.worker_id,
        status=str(result.status),
        result_json=json.dumps(result.result, default=str),
        error=result.error or "",
        started_at=result.started_at,
        finished_at=result.finished_at,
    )


def result_from_proto(message: Any) -> TaskResult:
    result = None
    if message.result_json:
        try:
            result = json.loads(message.result_json)
        except json.JSONDecodeError:
            result = message.result_json
    return TaskResult(
        task_id=message.task_id,
        worker_id=message.worker_id,
        status=TaskStatus(message.status or TaskStatus.FAILED),
        started_at=message.started_at,
        finished_at=message.finished_at,
        result=result,
        error=message.error or None,
    )
