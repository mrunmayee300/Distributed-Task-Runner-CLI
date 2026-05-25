from __future__ import annotations

import grpc

from taskrunner.grpc.codec import (
    result_from_proto,
    spec_from_proto,
    task_to_proto,
    worker_from_proto,
    worker_to_proto,
)
from taskrunner.grpc.protos import taskrunner_pb2, taskrunner_pb2_grpc
from taskrunner.scheduler.engine import SchedulerEngine
from taskrunner.shared.config import Settings


class SchedulerServicer(taskrunner_pb2_grpc.SchedulerServicer):
    def __init__(self, engine: SchedulerEngine) -> None:
        self.engine = engine

    async def SubmitTask(self, request, context):  # noqa: N802, ANN001, ANN201
        task = await self.engine.submit(spec_from_proto(request.spec))
        return taskrunner_pb2.SubmitTaskResponse(task=task_to_proto(task, taskrunner_pb2))

    async def RegisterWorker(self, request, context):  # noqa: N802, ANN001, ANN201
        worker = await self.engine.register_worker(worker_from_proto(request.worker))
        return taskrunner_pb2.RegisterWorkerResponse(worker=worker_to_proto(worker, taskrunner_pb2))

    async def Heartbeat(self, request, context):  # noqa: N802, ANN001, ANN201
        worker = await self.engine.heartbeat(request.worker_id, request.running_tasks)
        if worker is None:
            return taskrunner_pb2.HeartbeatResponse(known=False)
        return taskrunner_pb2.HeartbeatResponse(
            known=True,
            worker=worker_to_proto(worker, taskrunner_pb2),
        )

    async def ReserveTask(self, request, context):  # noqa: N802, ANN001, ANN201
        task = await self.engine.reserve_task(request.worker_id, request.timeout_seconds or 1.0)
        if task is None:
            return taskrunner_pb2.ReserveTaskResponse(found=False)
        return taskrunner_pb2.ReserveTaskResponse(
            found=True,
            task=task_to_proto(task, taskrunner_pb2),
        )

    async def ReportResult(self, request, context):  # noqa: N802, ANN001, ANN201
        task = await self.engine.report_result(result_from_proto(request.result))
        return taskrunner_pb2.ReportResultResponse(task=task_to_proto(task, taskrunner_pb2))

    async def CancelTask(self, request, context):  # noqa: N802, ANN001, ANN201
        task = await self.engine.cancel_task(request.task_id)
        return taskrunner_pb2.ReportResultResponse(task=task_to_proto(task, taskrunner_pb2))

    async def RetryTask(self, request, context):  # noqa: N802, ANN001, ANN201
        task = await self.engine.retry_task(request.task_id)
        return taskrunner_pb2.ReportResultResponse(task=task_to_proto(task, taskrunner_pb2))


async def serve_grpc(engine: SchedulerEngine, config: Settings) -> None:
    server = grpc.aio.server(options=(("grpc.so_reuseport", 0),))
    taskrunner_pb2_grpc.add_SchedulerServicer_to_server(SchedulerServicer(engine), server)
    server.add_insecure_port(f"{config.scheduler_host}:{config.scheduler_grpc_port}")
    await server.start()
    await server.wait_for_termination()
