from __future__ import annotations

import multiprocessing as mp
import os
import socket
import struct
from contextlib import closing
from multiprocessing import shared_memory
from multiprocessing.connection import Connection
from typing import Any


class IPCChannels:
    """Demonstrates queue, pipe, shared memory, and sockets inside a worker node."""

    def __init__(self) -> None:
        self.task_queue: mp.Queue[dict[str, Any]] = mp.Queue()
        self.result_queue: mp.Queue[dict[str, Any]] = mp.Queue()
        self.parent_pipe, self.child_pipe = mp.Pipe(duplex=True)
        self.shared_metrics = shared_memory.SharedMemory(create=True, size=64)
        self.shared_metrics.buf[:64] = bytes(64)
        self.socket_path = self._default_socket_path()
        self._listener: socket.socket | None = None

    def write_shared_counters(self, running: int, completed: int, failed: int) -> None:
        self.shared_metrics.buf[:24] = struct.pack("!QQQ", running, completed, failed)

    def read_shared_counters(self) -> tuple[int, int, int]:
        return struct.unpack("!QQQ", self.shared_metrics.buf[:24])

    def start_socket_listener(self) -> socket.socket:
        if os.name == "nt":
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
        else:
            with contextlib_suppress_file(self.socket_path):
                pass
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(self.socket_path)
        listener.listen(16)
        self._listener = listener
        return listener

    def send_pipe(self, connection: Connection, payload: dict[str, Any]) -> None:
        connection.send(payload)

    def recv_pipe(self, connection: Connection) -> dict[str, Any]:
        return connection.recv()

    def close(self) -> None:
        if self._listener:
            self._listener.close()
        self.parent_pipe.close()
        self.child_pipe.close()
        self.shared_metrics.close()
        self.shared_metrics.unlink()
        if os.name != "nt":
            with contextlib_suppress_file(self.socket_path):
                pass

    def _default_socket_path(self) -> str:
        if os.name == "nt":
            return "127.0.0.1:0"
        return f"/tmp/taskrunner-{os.getpid()}.sock"


class contextlib_suppress_file:
    def __init__(self, path: str) -> None:
        self.path = path

    def __enter__(self) -> None:
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            return None

    def __exit__(self, *_: object) -> bool:
        return False


def socket_healthcheck(host: str, port: int, timeout: float = 1.0) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0
