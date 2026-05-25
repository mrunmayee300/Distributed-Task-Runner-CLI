from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import typer

from taskrunner.grpc.client import SchedulerClient
from taskrunner.scheduler.service import run_scheduler
from taskrunner.shared.config import settings
from taskrunner.shared.models import TaskKind, TaskSpec
from taskrunner.workers.manager import run_worker

app = typer.Typer(
    name="taskrunner",
    help="Distributed Task Runner CLI for scheduler, workers, queues, logs, and monitoring.",
)


@app.command()
def scheduler() -> None:
    """Run the master scheduler node with gRPC and FastAPI services."""
    asyncio.run(run_scheduler(settings))


@app.command()
def worker(
    worker_id: str | None = typer.Option(None, "--id", help="Stable worker id."),
    queues: str = typer.Option("default", "--queues", help="Comma-separated queues."),
    labels: str = typer.Option("", "--labels", help="Comma-separated worker labels for affinity."),
    capacity: int | None = typer.Option(None, "--capacity", help="Concurrent task slots."),
) -> None:
    """Run a worker node that auto-registers and heartbeats."""
    asyncio.run(
        run_worker(
            worker_id=worker_id,
            queues=_csv_set(queues),
            labels=_csv_set(labels),
            capacity=capacity,
        )
    )


@app.command()
def submit(
    task_path: Path = typer.Argument(..., help="Python task file or import target."),
    function: str = typer.Option("run", "--function", "-f", help="Function name to execute."),
    payload: str = typer.Option("{}", "--payload", "-p", help="JSON payload passed as kwargs."),
    kind: TaskKind = typer.Option(TaskKind.IO, "--kind", help="cpu, io, async, or scheduled."),
    queue: str = typer.Option("default", "--queue", "-q"),
    priority: int = typer.Option(100, "--priority", help="Lower values run first."),
    delay: float = typer.Option(0.0, "--delay", help="Delay in seconds."),
    retries: int = typer.Option(3, "--retries"),
    affinity: str = typer.Option("", "--affinity", help="Comma-separated worker labels."),
) -> None:
    """Submit a task through the gRPC scheduler API."""
    target = _target(task_path, function)
    spec = TaskSpec(
        target=target,
        payload=json.loads(payload),
        kind=kind,
        queue=queue,
        priority=priority,
        delay_seconds=delay,
        max_retries=retries,
        affinity=_csv_set(affinity),
    )
    task = asyncio.run(_submit(spec))
    typer.echo(json.dumps(task.to_dict(), indent=2, default=str))


@app.command()
def workers() -> None:
    """List known workers."""
    typer.echo(json.dumps(_api_get("/workers"), indent=2, default=str))


@app.command()
def queues() -> None:
    """Show queue depth, delay, in-flight, and DLQ metrics."""
    typer.echo(json.dumps(_api_get("/queues"), indent=2, default=str))


@app.command()
def logs(entity_id: str = typer.Argument(..., help="Worker id or task id.")) -> None:
    """Fetch centralized logs for a worker or task."""
    typer.echo(json.dumps(_api_get(f"/logs/{entity_id}"), indent=2, default=str))


@app.command()
def retry(task_id: str) -> None:
    """Retry a failed or dead-lettered task."""
    task = asyncio.run(_retry(task_id))
    typer.echo(json.dumps(task.to_dict(), indent=2, default=str))


@app.command()
def cancel(task_id: str) -> None:
    """Cancel a queued or running task."""
    task = asyncio.run(_cancel(task_id))
    typer.echo(json.dumps(task.to_dict(), indent=2, default=str))


@app.command()
def monitor(interval: float = typer.Option(2.0, "--interval", "-i")) -> None:
    """Print live monitoring snapshots from the FastAPI backend."""
    try:
        while True:
            typer.echo(json.dumps(_api_get("/metrics"), indent=2, default=str))
            time.sleep(interval)
    except KeyboardInterrupt:
        typer.echo("monitor stopped")


async def _submit(spec: TaskSpec):
    async with SchedulerClient(settings.grpc_target) as client:
        return await client.submit(spec)


async def _retry(task_id: str):
    async with SchedulerClient(settings.grpc_target) as client:
        return await client.retry(task_id)


async def _cancel(task_id: str):
    async with SchedulerClient(settings.grpc_target) as client:
        return await client.cancel(task_id)


def _api_get(path: str) -> Any:
    url = f"http://{settings.api_host}:{settings.api_port}{path}"
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=5) as response:  # noqa: S310 - local operator endpoint.
            return json.loads(response.read().decode())
    except URLError as exc:
        raise typer.BadParameter(f"cannot reach monitoring API at {url}: {exc}") from exc


def _target(task_path: Path, function: str) -> str:
    text = str(task_path)
    if ":" in text and not task_path.exists():
        return text
    return f"{task_path}:{function}"


def _csv_set(value: str) -> set[str]:
    return {part.strip() for part in value.split(",") if part.strip()}


if __name__ == "__main__":
    app()
