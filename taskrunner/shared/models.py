from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from time import time
from typing import Any
from uuid import uuid4


class TaskKind(StrEnum):
    CPU = "cpu"
    IO = "io"
    ASYNC = "async"
    SCHEDULED = "scheduled"


class TaskStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    ACKED = "acked"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    DEAD_LETTERED = "dead_lettered"


class WorkerStatus(StrEnum):
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    DRAINING = "draining"
    OFFLINE = "offline"


class EventType(StrEnum):
    TASK_SUBMITTED = "task.submitted"
    TASK_DISPATCHED = "task.dispatched"
    TASK_FINISHED = "task.finished"
    TASK_FAILED = "task.failed"
    WORKER_REGISTERED = "worker.registered"
    WORKER_HEARTBEAT = "worker.heartbeat"
    QUEUE_PRESSURE = "queue.pressure"


@dataclass(slots=True)
class TaskSpec:
    target: str
    payload: dict[str, Any] = field(default_factory=dict)
    kind: TaskKind = TaskKind.IO
    queue: str = "default"
    priority: int = 100
    delay_seconds: float = 0.0
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5
    timeout_seconds: float = 60.0
    affinity: set[str] = field(default_factory=set)
    depends_on: set[str] = field(default_factory=set)
    batch_key: str | None = None

    def normalized(self) -> TaskSpec:
        return TaskSpec(
            target=self.target,
            payload=self.payload,
            kind=TaskKind(self.kind),
            queue=self.queue or "default",
            priority=max(0, int(self.priority)),
            delay_seconds=max(0.0, float(self.delay_seconds)),
            max_retries=max(0, int(self.max_retries)),
            retry_backoff_seconds=max(0.0, float(self.retry_backoff_seconds)),
            timeout_seconds=max(0.1, float(self.timeout_seconds)),
            affinity=set(self.affinity),
            depends_on=set(self.depends_on),
            batch_key=self.batch_key,
        )


@dataclass(slots=True)
class TaskEnvelope:
    spec: TaskSpec
    task_id: str = field(default_factory=lambda: f"task-{uuid4().hex}")
    created_at: float = field(default_factory=time)
    available_at: float = field(default_factory=time)
    attempts: int = 0
    status: TaskStatus = TaskStatus.PENDING
    last_error: str | None = None
    assigned_worker: str | None = None
    lease_deadline: float | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["spec"]["kind"] = str(self.spec.kind)
        data["spec"]["affinity"] = sorted(self.spec.affinity)
        data["spec"]["depends_on"] = sorted(self.spec.depends_on)
        data["status"] = str(self.status)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskEnvelope:
        spec_data = dict(data["spec"])
        spec_data["kind"] = TaskKind(spec_data.get("kind", TaskKind.IO))
        spec_data["affinity"] = set(spec_data.get("affinity", []))
        spec_data["depends_on"] = set(spec_data.get("depends_on", []))
        spec = TaskSpec(**spec_data).normalized()
        return cls(
            spec=spec,
            task_id=data["task_id"],
            created_at=float(data.get("created_at", time())),
            available_at=float(data.get("available_at", time())),
            attempts=int(data.get("attempts", 0)),
            status=TaskStatus(data.get("status", TaskStatus.PENDING)),
            last_error=data.get("last_error"),
            assigned_worker=data.get("assigned_worker"),
            lease_deadline=data.get("lease_deadline"),
        )


@dataclass(slots=True)
class TaskResult:
    task_id: str
    worker_id: str
    status: TaskStatus
    started_at: float
    finished_at: float
    result: Any = None
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        return max(0.0, self.finished_at - self.started_at)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = str(self.status)
        data["duration_seconds"] = self.duration_seconds
        return data


@dataclass(slots=True)
class WorkerInfo:
    worker_id: str
    host: str
    pid: int
    queues: set[str]
    capacity: int
    labels: set[str] = field(default_factory=set)
    status: WorkerStatus = WorkerStatus.STARTING
    running_tasks: int = 0
    started_at: float = field(default_factory=time)
    last_heartbeat: float = field(default_factory=time)

    @property
    def load(self) -> float:
        if self.capacity <= 0:
            return 1.0
        return min(1.0, self.running_tasks / self.capacity)

    def accepts(self, task: TaskEnvelope) -> bool:
        queue_ok = task.spec.queue in self.queues
        affinity_ok = not task.spec.affinity or bool(task.spec.affinity & self.labels)
        return queue_ok and affinity_ok and self.status in {WorkerStatus.IDLE, WorkerStatus.BUSY}

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["queues"] = sorted(self.queues)
        data["labels"] = sorted(self.labels)
        data["status"] = str(self.status)
        data["load"] = self.load
        return data


@dataclass(slots=True)
class QueueSnapshot:
    queued: int
    delayed: int
    inflight: int
    dead_lettered: int
    pressure: float


@dataclass(slots=True)
class ClusterEvent:
    event_type: EventType
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: f"evt-{uuid4().hex}")
    created_at: float = field(default_factory=time)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["event_type"] = str(self.event_type)
        return data
