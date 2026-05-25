from __future__ import annotations

import os
from dataclasses import asdict
from time import time
from typing import Any

from taskrunner.scheduler.engine import SchedulerEngine

try:
    import resource
except ModuleNotFoundError:  # Windows development hosts do not ship the Linux resource module.
    resource = None


class MetricsAggregator:
    def __init__(self, engine: SchedulerEngine) -> None:
        self.engine = engine
        self.started_at = time()

    def snapshot(self) -> dict[str, Any]:
        usage = resource.getrusage(resource.RUSAGE_SELF) if resource else None
        queue = self.engine.queue_snapshot()
        metrics = self.engine.metrics()
        metrics.update(
            {
                "uptime_seconds": time() - self.started_at,
                "process": {
                    "pid": os.getpid(),
                    "max_rss_kb": usage.ru_maxrss if usage else None,
                    "user_cpu_seconds": usage.ru_utime if usage else None,
                    "system_cpu_seconds": usage.ru_stime if usage else None,
                },
                "queue": asdict(queue),
                "autoscaling": self.engine.autoscaling_recommendation(),
            }
        )
        return metrics
