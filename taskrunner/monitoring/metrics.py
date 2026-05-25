from __future__ import annotations

import os
import resource
from dataclasses import asdict
from time import time
from typing import Any

from taskrunner.scheduler.engine import SchedulerEngine


class MetricsAggregator:
    def __init__(self, engine: SchedulerEngine) -> None:
        self.engine = engine
        self.started_at = time()

    def snapshot(self) -> dict[str, Any]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        queue = self.engine.queue_snapshot()
        metrics = self.engine.metrics()
        metrics.update(
            {
                "uptime_seconds": time() - self.started_at,
                "process": {
                    "pid": os.getpid(),
                    "max_rss_kb": usage.ru_maxrss,
                    "user_cpu_seconds": usage.ru_utime,
                    "system_cpu_seconds": usage.ru_stime,
                },
                "queue": asdict(queue),
                "autoscaling": self.engine.autoscaling_recommendation(),
            }
        )
        return metrics
