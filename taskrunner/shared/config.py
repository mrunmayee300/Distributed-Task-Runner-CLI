from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SCHEDULER_HOST = "127.0.0.1"
DEFAULT_SCHEDULER_GRPC_PORT = 50051
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 8080
DEFAULT_SQLITE_PATH = Path("data/taskrunner.db")
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 2.0
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 10.0
DEFAULT_LEASE_SECONDS = 30.0
DEFAULT_MAX_QUEUE_DEPTH = 10_000
DEFAULT_WORKER_CAPACITY = max(1, (os.cpu_count() or 2) - 1)
DEFAULT_LOG_LEVEL = "INFO"


@dataclass(frozen=True, slots=True)
class Settings:
    scheduler_host: str = DEFAULT_SCHEDULER_HOST
    scheduler_grpc_port: int = DEFAULT_SCHEDULER_GRPC_PORT
    api_host: str = DEFAULT_API_HOST
    api_port: int = DEFAULT_API_PORT
    sqlite_path: Path = DEFAULT_SQLITE_PATH
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    heartbeat_timeout_seconds: float = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS
    lease_seconds: float = DEFAULT_LEASE_SECONDS
    max_queue_depth: int = DEFAULT_MAX_QUEUE_DEPTH
    worker_capacity: int = DEFAULT_WORKER_CAPACITY
    log_level: str = DEFAULT_LOG_LEVEL

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            scheduler_host=os.getenv("TASKRUNNER_SCHEDULER_HOST", DEFAULT_SCHEDULER_HOST),
            scheduler_grpc_port=int(
                os.getenv("TASKRUNNER_SCHEDULER_GRPC_PORT", str(DEFAULT_SCHEDULER_GRPC_PORT))
            ),
            api_host=os.getenv("TASKRUNNER_API_HOST", DEFAULT_API_HOST),
            api_port=int(os.getenv("TASKRUNNER_API_PORT", str(DEFAULT_API_PORT))),
            sqlite_path=Path(os.getenv("TASKRUNNER_SQLITE_PATH", str(DEFAULT_SQLITE_PATH))),
            heartbeat_interval_seconds=float(
                os.getenv(
                    "TASKRUNNER_HEARTBEAT_INTERVAL_SECONDS",
                    str(DEFAULT_HEARTBEAT_INTERVAL_SECONDS),
                )
            ),
            heartbeat_timeout_seconds=float(
                os.getenv(
                    "TASKRUNNER_HEARTBEAT_TIMEOUT_SECONDS",
                    str(DEFAULT_HEARTBEAT_TIMEOUT_SECONDS),
                )
            ),
            lease_seconds=float(os.getenv("TASKRUNNER_LEASE_SECONDS", str(DEFAULT_LEASE_SECONDS))),
            max_queue_depth=int(
                os.getenv("TASKRUNNER_MAX_QUEUE_DEPTH", str(DEFAULT_MAX_QUEUE_DEPTH))
            ),
            worker_capacity=int(
                os.getenv("TASKRUNNER_WORKER_CAPACITY", str(DEFAULT_WORKER_CAPACITY))
            ),
            log_level=os.getenv("TASKRUNNER_LOG_LEVEL", DEFAULT_LOG_LEVEL),
        )

    @property
    def grpc_target(self) -> str:
        return f"{self.scheduler_host}:{self.scheduler_grpc_port}"


settings = Settings.from_env()
