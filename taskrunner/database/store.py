from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from time import time
from typing import Any

from taskrunner.shared.models import TaskEnvelope, TaskResult, WorkerInfo, WorkerStatus


class SQLiteStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self.initialize()

    def initialize(self) -> None:
        with self._lock, self._connection:
            self._connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    queue TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    attempts INTEGER NOT NULL,
                    target TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    result_json TEXT,
                    error TEXT,
                    assigned_worker TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_status_queue ON tasks(status, queue);

                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    queues_json TEXT NOT NULL,
                    labels_json TEXT NOT NULL,
                    capacity INTEGER NOT NULL,
                    running_tasks INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    last_heartbeat REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                """
            )

    def upsert_task(self, task: TaskEnvelope, result: TaskResult | None = None) -> None:
        result_json = json.dumps(result.to_dict(), default=str) if result else None
        spec_json = json.dumps(task.to_dict()["spec"], default=str)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO tasks (
                    task_id, status, queue, priority, attempts, target, payload_json,
                    spec_json, result_json, error, assigned_worker, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status,
                    attempts=excluded.attempts,
                    result_json=COALESCE(excluded.result_json, tasks.result_json),
                    error=excluded.error,
                    assigned_worker=excluded.assigned_worker,
                    updated_at=excluded.updated_at
                """,
                (
                    task.task_id,
                    str(task.status),
                    task.spec.queue,
                    task.spec.priority,
                    task.attempts,
                    task.spec.target,
                    json.dumps(task.spec.payload, default=str),
                    spec_json,
                    result_json,
                    task.last_error,
                    task.assigned_worker,
                    task.created_at,
                    time(),
                ),
            )

    def upsert_worker(self, worker: WorkerInfo) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO workers (
                    worker_id, host, pid, queues_json, labels_json, capacity,
                    running_tasks, status, started_at, last_heartbeat
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    host=excluded.host,
                    pid=excluded.pid,
                    queues_json=excluded.queues_json,
                    labels_json=excluded.labels_json,
                    capacity=excluded.capacity,
                    running_tasks=excluded.running_tasks,
                    status=excluded.status,
                    last_heartbeat=excluded.last_heartbeat
                """,
                (
                    worker.worker_id,
                    worker.host,
                    worker.pid,
                    json.dumps(sorted(worker.queues)),
                    json.dumps(sorted(worker.labels)),
                    worker.capacity,
                    worker.running_tasks,
                    str(worker.status),
                    worker.started_at,
                    worker.last_heartbeat,
                ),
            )

    def mark_worker_offline(self, worker_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE workers SET status = ?, last_heartbeat = ? WHERE worker_id = ?",
                (str(WorkerStatus.OFFLINE), time(), worker_id),
            )

    def append_log(self, entity_id: str, level: str, message: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO logs(entity_id, level, message, created_at) VALUES (?, ?, ?, ?)",
                (entity_id, level.upper(), message, time()),
            )

    def recent_tasks(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT task_id, status, queue, priority, attempts, target, payload_json,
                   result_json, error, assigned_worker, created_at, updated_at
            FROM tasks
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def workers(self) -> list[dict[str, Any]]:
        rows = self._fetch_all("SELECT * FROM workers ORDER BY worker_id", ())
        for row in rows:
            row["queues"] = json.loads(row.pop("queues_json"))
            row["labels"] = json.loads(row.pop("labels_json"))
        return rows

    def logs(self, entity_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT entity_id, level, message, created_at
            FROM logs
            WHERE entity_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (entity_id, limit),
        )

    def task_counts(self) -> dict[str, int]:
        rows = self._fetch_all("SELECT status, COUNT(*) AS count FROM tasks GROUP BY status", ())
        return {row["status"]: int(row["count"]) for row in rows}

    def _fetch_all(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
