from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    scheduler_host: str = "127.0.0.1"
    scheduler_grpc_port: int = 50051
    api_host: str = "127.0.0.1"
    api_port: int = 8080
    sqlite_path: Path = Path("data/taskrunner.db")
    heartbeat_interval_seconds: float = 2.0
    heartbeat_timeout_seconds: float = 10.0
    lease_seconds: float = 30.0
    max_queue_depth: int = 10_000
    worker_capacity: int = max(1, (os.cpu_count() or 2) - 1)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            scheduler_host=os.getenv("TASKRUNNER_SCHEDULER_HOST", cls.scheduler_host),
            scheduler_grpc_port=int(os.getenv("TASKRUNNER_SCHEDULER_GRPC_PORT", cls.scheduler_grpc_port)),
            api_host=os.getenv("TASKRUNNER_API_HOST", cls.api_host),
            api_port=int(os.getenv("TASKRUNNER_API_PORT", cls.api_port)),
            sqlite_path=Path(os.getenv("TASKRUNNER_SQLITE_PATH", str(cls.sqlite_path))),
            heartbeat_interval_seconds=float(
                os.getenv("TASKRUNNER_HEARTBEAT_INTERVAL_SECONDS", cls.heartbeat_interval_seconds)
            ),
            heartbeat_timeout_seconds=float(
                os.getenv("TASKRUNNER_HEARTBEAT_TIMEOUT_SECONDS", cls.heartbeat_timeout_seconds)
            ),
            lease_seconds=float(os.getenv("TASKRUNNER_LEASE_SECONDS", cls.lease_seconds)),
            max_queue_depth=int(os.getenv("TASKRUNNER_MAX_QUEUE_DEPTH", cls.max_queue_depth)),
            worker_capacity=int(os.getenv("TASKRUNNER_WORKER_CAPACITY", cls.worker_capacity)),
            log_level=os.getenv("TASKRUNNER_LOG_LEVEL", cls.log_level),
        )

    @property
    def grpc_target(self) -> str:
        return f"{self.scheduler_host}:{self.scheduler_grpc_port}"


settings = Settings.from_env()
