from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

import uvicorn

from taskrunner.api.app import create_app
from taskrunner.grpc.server import serve_grpc
from taskrunner.scheduler.engine import SchedulerEngine
from taskrunner.shared.config import Settings, settings
from taskrunner.shared.logging import configure_logging, get_logger

logger = get_logger(__name__)


async def run_scheduler(config: Settings = settings) -> None:
    configure_logging(config.log_level)
    engine = SchedulerEngine(config)
    await engine.start_background_services()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    grpc_task = asyncio.create_task(serve_grpc(engine, config))
    api_config = uvicorn.Config(
        create_app(engine),
        host=config.api_host,
        port=config.api_port,
        log_config=None,
        loop="asyncio",
    )
    api_server = uvicorn.Server(api_config)
    api_task = asyncio.create_task(api_server.serve())

    logger.info(
        "scheduler started",
        extra={
            "taskrunner_grpc": config.grpc_target,
            "taskrunner_api": f"{config.api_host}:{config.api_port}",
        },
    )
    await stop.wait()
    api_server.should_exit = True
    grpc_task.cancel()
    with suppress(asyncio.CancelledError):
        await grpc_task
    await api_task
    await engine.stop_background_services()


def main() -> None:
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
