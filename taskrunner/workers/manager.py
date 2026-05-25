from __future__ import annotations

import asyncio
import os
import signal
import socket
from contextlib import suppress
from typing import Protocol
from uuid import uuid4

from taskrunner.grpc.client import SchedulerClient
from taskrunner.shared.config import Settings, settings
from taskrunner.shared.logging import configure_logging, get_logger
from taskrunner.shared.models import TaskEnvelope, TaskResult, WorkerInfo, WorkerStatus
from taskrunner.workers.executor import TaskExecutor
from taskrunner.workers.ipc import IPCChannels

logger = get_logger(__name__)


class SchedulerTransport(Protocol):
    async def register_worker(self, worker: WorkerInfo) -> None: ...

    async def heartbeat(self, worker_id: str, running_tasks: int) -> bool: ...

    async def reserve(self, worker_id: str, timeout_seconds: float = 1.0) -> TaskEnvelope | None: ...

    async def report_result(self, result: TaskResult) -> TaskEnvelope: ...


class WorkerRuntime:
    def __init__(
        self,
        worker_id: str | None = None,
        queues: set[str] | None = None,
        labels: set[str] | None = None,
        capacity: int | None = None,
        config: Settings = settings,
        executor: TaskExecutor | None = None,
        transport: SchedulerTransport | None = None,
    ) -> None:
        self.config = config
        self.worker = WorkerInfo(
            worker_id=worker_id or f"worker-{uuid4().hex[:8]}",
            host=socket.gethostname(),
            pid=os.getpid(),
            queues=queues or {"default"},
            labels=labels or set(),
            capacity=capacity or config.worker_capacity,
        )
        self.executor = executor or TaskExecutor(max_processes=self.worker.capacity)
        self.transport = transport
        self.ipc = IPCChannels()
        self._stop = asyncio.Event()
        self._running: set[asyncio.Task[None]] = set()
        self._completed = 0
        self._failed = 0

    async def run(self) -> None:
        configure_logging(self.config.log_level)
        if self.transport is not None:
            await self._run_with_transport(self.transport)
            return
        async with SchedulerClient(self.config.grpc_target) as client:
            await self._run_with_transport(client)

    async def _run_with_transport(self, transport: SchedulerTransport) -> None:
        self._install_signal_handlers()
        await transport.register_worker(self.worker)
        heartbeat = asyncio.create_task(self._heartbeat_loop(transport))
        consumers = [asyncio.create_task(self._consume_loop(transport)) for _ in range(self.worker.capacity)]
        try:
            await self._stop.wait()
        finally:
            self.worker.status = WorkerStatus.DRAINING
            for task in consumers:
                task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            for task in consumers:
                with suppress(asyncio.CancelledError):
                    await task
            await self._wait_for_running_tasks()
            self.executor.shutdown()
            self.ipc.close()

    async def _consume_loop(self, transport: SchedulerTransport) -> None:
        while not self._stop.is_set():
            task = await transport.reserve(self.worker.worker_id, timeout_seconds=1.0)
            if task is None:
                await asyncio.sleep(0.1)
                continue
            execution = asyncio.create_task(self._execute_and_report(transport, task))
            self._running.add(execution)
            execution.add_done_callback(self._running.discard)
            await execution

    async def _execute_and_report(self, transport: SchedulerTransport, task: TaskEnvelope) -> None:
        self.ipc.task_queue.put(task.to_dict())
        self.ipc.write_shared_counters(len(self._running), self._completed, self._failed)
        result = await self.executor.execute(task, self.worker.worker_id)
        await transport.report_result(result)
        self.ipc.result_queue.put(result.to_dict())
        if result.error:
            self._failed += 1
        else:
            self._completed += 1
        self.ipc.write_shared_counters(len(self._running), self._completed, self._failed)

    async def _heartbeat_loop(self, transport: SchedulerTransport) -> None:
        while not self._stop.is_set():
            known = await transport.heartbeat(self.worker.worker_id, len(self._running))
            if not known:
                await transport.register_worker(self.worker)
            await asyncio.sleep(self.config.heartbeat_interval_seconds)

    async def _wait_for_running_tasks(self) -> None:
        if not self._running:
            return
        await asyncio.gather(*self._running, return_exceptions=True)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stop.set)

    def request_shutdown(self) -> None:
        self._stop.set()


async def run_worker(
    worker_id: str | None = None,
    queues: set[str] | None = None,
    labels: set[str] | None = None,
    capacity: int | None = None,
) -> None:
    await WorkerRuntime(worker_id=worker_id, queues=queues, labels=labels, capacity=capacity).run()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
