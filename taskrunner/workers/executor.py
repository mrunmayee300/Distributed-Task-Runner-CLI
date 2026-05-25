from __future__ import annotations

import asyncio
import importlib.util
import inspect
import multiprocessing as mp
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from time import time
from typing import Any, Callable

from taskrunner.shared.models import TaskEnvelope, TaskKind, TaskResult, TaskStatus


def _load_callable(target: str) -> Callable[..., Any]:
    module_path, _, function_name = target.partition(":")
    if not module_path or not function_name:
        raise ValueError("target must use 'path/to/file.py:function_name' or 'module:function_name'")

    if module_path.endswith(".py") or Path(module_path).exists():
        path = Path(module_path).resolve()
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot import task module {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    else:
        module = __import__(module_path, fromlist=[function_name])

    func = getattr(module, function_name, None)
    if not callable(func):
        raise AttributeError(f"{target} is not callable")
    return func


def _run_sync_target(target: str, payload: dict[str, Any]) -> Any:
    func = _load_callable(target)
    return func(**payload)


class TaskExecutor:
    def __init__(self, max_processes: int | None = None, max_threads: int = 16) -> None:
        self.max_processes = max_processes or max(1, (mp.cpu_count() or 2) - 1)
        self._process_pool = ProcessPoolExecutor(max_workers=self.max_processes)
        self._thread_pool = ThreadPoolExecutor(max_workers=max_threads)
        self._shutdown = threading.Event()

    async def execute(self, task: TaskEnvelope, worker_id: str) -> TaskResult:
        started = time()
        try:
            result = await asyncio.wait_for(
                self._execute_by_kind(task),
                timeout=task.spec.timeout_seconds,
            )
            status = TaskStatus.SUCCEEDED
            error = None
        except asyncio.CancelledError:
            status = TaskStatus.CANCELLED
            result = None
            error = "cancelled"
            raise
        except Exception as exc:  # noqa: BLE001 - task failures must be captured.
            status = TaskStatus.FAILED
            result = None
            error = f"{type(exc).__name__}: {exc}"
        return TaskResult(
            task_id=task.task_id,
            worker_id=worker_id,
            status=status,
            started_at=started,
            finished_at=time(),
            result=result,
            error=error,
        )

    async def _execute_by_kind(self, task: TaskEnvelope) -> Any:
        loop = asyncio.get_running_loop()
        if task.spec.kind == TaskKind.CPU:
            return await loop.run_in_executor(
                self._process_pool,
                _run_sync_target,
                task.spec.target,
                task.spec.payload,
            )
        if task.spec.kind == TaskKind.ASYNC:
            func = _load_callable(task.spec.target)
            value = func(**task.spec.payload)
            if inspect.isawaitable(value):
                return await value
            return value
        return await loop.run_in_executor(
            self._thread_pool,
            _run_sync_target,
            task.spec.target,
            task.spec.payload,
        )

    def shutdown(self) -> None:
        if self._shutdown.is_set():
            return
        self._shutdown.set()
        self._thread_pool.shutdown(wait=True, cancel_futures=True)
        self._process_pool.shutdown(wait=True, cancel_futures=True)
