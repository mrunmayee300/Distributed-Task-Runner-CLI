from __future__ import annotations

import grpc

from taskrunner.grpc.codec import result_to_proto, spec_to_proto, task_from_proto, worker_to_proto
from taskrunner.grpc.protos import taskrunner_pb2, taskrunner_pb2_grpc
from taskrunner.shared.config import settings
from taskrunner.shared.models import TaskEnvelope, TaskResult, TaskSpec, WorkerInfo


class SchedulerClient:
    def __init__(self, target: str | None = None) -> None:
        self.target = target or settings.grpc_target
        self._channel: grpc.aio.Channel | None = None
        self._stub: taskrunner_pb2_grpc.SchedulerStub | None = None

    async def __aenter__(self) -> "SchedulerClient":
        self._channel = grpc.aio.insecure_channel(self.target)
        self._stub = taskrunner_pb2_grpc.SchedulerStub(self._channel)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._channel is not None:
            await self._channel.close()

    @property
    def stub(self) -> taskrunner_pb2_grpc.SchedulerStub:
        if self._stub is None:
            raise RuntimeError("client must be used as an async context manager")
        return self._stub

    async def submit(self, spec: TaskSpec) -> TaskEnvelope:
        response = await self.stub.SubmitTask(
            taskrunner_pb2.SubmitTaskRequest(spec=spec_to_proto(spec, taskrunner_pb2))
        )
        return task_from_proto(response.task)

    async def register_worker(self, worker: WorkerInfo) -> None:
        await self.stub.RegisterWorker(
            taskrunner_pb2.RegisterWorkerRequest(worker=worker_to_proto(worker, taskrunner_pb2))
        )

    async def heartbeat(self, worker_id: str, running_tasks: int) -> bool:
        response = await self.stub.Heartbeat(
            taskrunner_pb2.HeartbeatRequest(worker_id=worker_id, running_tasks=running_tasks)
        )
        return response.known

    async def reserve(self, worker_id: str, timeout_seconds: float = 1.0) -> TaskEnvelope | None:
        response = await self.stub.ReserveTask(
            taskrunner_pb2.ReserveTaskRequest(worker_id=worker_id, timeout_seconds=timeout_seconds)
        )
        if not response.found:
            return None
        return task_from_proto(response.task)

    async def report_result(self, result: TaskResult) -> TaskEnvelope:
        response = await self.stub.ReportResult(
            taskrunner_pb2.ReportResultRequest(result=result_to_proto(result, taskrunner_pb2))
        )
        return task_from_proto(response.task)

    async def cancel(self, task_id: str) -> TaskEnvelope:
        response = await self.stub.CancelTask(taskrunner_pb2.TaskMutationRequest(task_id=task_id))
        return task_from_proto(response.task)

    async def retry(self, task_id: str) -> TaskEnvelope:
        response = await self.stub.RetryTask(taskrunner_pb2.TaskMutationRequest(task_id=task_id))
        return task_from_proto(response.task)
