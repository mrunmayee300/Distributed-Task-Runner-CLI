.PHONY: install proto test lint format run-scheduler run-worker docker-up docker-down clean

install:
	python -m pip install --upgrade pip
	python -m pip install -e ".[dev]"

proto:
	python -m grpc_tools.protoc -I . \
		--python_out=. \
		--grpc_python_out=. \
		taskrunner/grpc/protos/taskrunner.proto

test:
	pytest -q

lint:
	ruff check .

format:
	ruff format .
	ruff check --fix .

run-scheduler:
	taskrunner scheduler

run-worker:
	taskrunner worker --queues default,cpu,io --labels cpu

docker-up:
	docker compose up --build

docker-down:
	docker compose down --remove-orphans

clean:
	rm -rf .pytest_cache .ruff_cache build dist *.egg-info data logs
