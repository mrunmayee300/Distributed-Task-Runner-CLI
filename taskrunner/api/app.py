from __future__ import annotations

import asyncio
from dataclasses import asdict
from contextlib import suppress
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from taskrunner.monitoring.metrics import MetricsAggregator
from taskrunner.scheduler.engine import SchedulerEngine
from taskrunner.shared.config import settings
from taskrunner.shared.models import TaskKind, TaskSpec


class SubmitTaskBody(BaseModel):
    target: str
    payload: dict[str, Any] = Field(default_factory=dict)
    kind: TaskKind = TaskKind.IO
    queue: str = "default"
    priority: int = 100
    delay_seconds: float = 0.0
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5
    timeout_seconds: float = 60.0
    affinity: set[str] = Field(default_factory=set)
    depends_on: set[str] = Field(default_factory=set)
    batch_key: str | None = None

    def to_spec(self) -> TaskSpec:
        return TaskSpec(**self.model_dump()).normalized()


def create_app(engine: SchedulerEngine | None = None) -> FastAPI:
    scheduler = engine or SchedulerEngine(settings)
    metrics = MetricsAggregator(scheduler)
    app = FastAPI(
        title="Distributed Task Runner Monitoring API",
        version="0.1.0",
        description="Async control plane for queues, workers, tasks, metrics, and live events.",
    )

    @app.on_event("startup")
    async def _startup() -> None:
        await scheduler.start_background_services()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await scheduler.stop_background_services()

    @app.post("/tasks", status_code=202)
    async def submit_task(body: SubmitTaskBody) -> dict[str, Any]:
        task = await scheduler.submit(body.to_spec())
        return task.to_dict()

    @app.post("/tasks/batch", status_code=202)
    async def submit_batch(tasks: list[SubmitTaskBody]) -> list[dict[str, Any]]:
        submitted = await scheduler.submit_batch([task.to_spec() for task in tasks])
        return [task.to_dict() for task in submitted]

    @app.get("/tasks")
    async def tasks(limit: int = 100) -> list[dict[str, Any]]:
        return scheduler.store.recent_tasks(limit)

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, Any]:
        try:
            task = await scheduler.cancel_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        return task.to_dict()

    @app.post("/tasks/{task_id}/retry")
    async def retry_task(task_id: str, background_tasks: BackgroundTasks) -> dict[str, Any]:
        try:
            task = await scheduler.retry_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        background_tasks.add_task(scheduler.store.append_log, task_id, "INFO", "manual retry requested")
        return task.to_dict()

    @app.get("/workers")
    async def workers() -> list[dict[str, Any]]:
        return scheduler.store.workers()

    @app.get("/queues")
    async def queues() -> dict[str, Any]:
        return asdict(scheduler.queue_snapshot())

    @app.get("/logs/{entity_id}")
    async def logs(entity_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return scheduler.store.logs(entity_id, limit)

    @app.get("/metrics")
    async def metric_snapshot() -> dict[str, Any]:
        return metrics.snapshot()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "metrics": metrics.snapshot()}

    @app.websocket("/events")
    async def events(websocket: WebSocket) -> None:
        await websocket.accept()
        keepalive = asyncio.create_task(_websocket_keepalive(websocket))
        try:
            async for event in scheduler.event_bus.subscribe(replay=True):
                await websocket.send_json(event.to_dict())
        except WebSocketDisconnect:
            return
        finally:
            keepalive.cancel()
            with suppress(asyncio.CancelledError):
                await keepalive

    return app


async def _websocket_keepalive(websocket: WebSocket) -> None:
    while True:
        await asyncio.sleep(30)
        await websocket.send_json({"event_type": "system.keepalive"})


app = create_app()
