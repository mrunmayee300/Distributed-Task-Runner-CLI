from __future__ import annotations

from fastapi.testclient import TestClient

from taskrunner.api.app import create_app
from taskrunner.scheduler.engine import SchedulerEngine
from taskrunner.shared.config import Settings


def test_api_submit_and_metrics(tmp_path) -> None:
    config = Settings(sqlite_path=tmp_path / "taskrunner.db")
    engine = SchedulerEngine(config)
    with TestClient(create_app(engine)) as client:
        response = client.post(
            "/tasks",
            json={
                "target": "sample_tasks.io_job:run",
                "payload": {"url": "local", "delay": 0.01},
                "kind": "async",
                "priority": 10,
            },
        )
        assert response.status_code == 202
        task_id = response.json()["task_id"]

        tasks = client.get("/tasks").json()
        assert tasks[0]["task_id"] == task_id

        metrics = client.get("/metrics").json()
        assert metrics["queue"]["queued"] == 1
