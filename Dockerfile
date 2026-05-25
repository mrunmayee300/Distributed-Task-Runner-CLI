FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TASKRUNNER_API_HOST=0.0.0.0 \
    TASKRUNNER_SCHEDULER_HOST=0.0.0.0 \
    TASKRUNNER_SQLITE_PATH=/var/lib/taskrunner/taskrunner.db

WORKDIR /app
RUN useradd --create-home --shell /bin/bash taskrunner \
    && mkdir -p /var/lib/taskrunner \
    && chown -R taskrunner:taskrunner /var/lib/taskrunner

COPY pyproject.toml README.md ./
COPY taskrunner ./taskrunner
COPY sample_tasks ./sample_tasks

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir ".[dev]" \
    && python -m grpc_tools.protoc -I . \
      --python_out=. \
      --grpc_python_out=. \
      taskrunner/grpc/protos/taskrunner.proto

USER taskrunner
EXPOSE 50051 8080
CMD ["taskrunner", "scheduler"]
