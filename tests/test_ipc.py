from __future__ import annotations

from taskrunner.workers.ipc import IPCChannels


def test_shared_memory_and_pipe_channels() -> None:
    channels = IPCChannels()
    try:
        channels.write_shared_counters(running=2, completed=5, failed=1)
        assert channels.read_shared_counters() == (2, 5, 1)

        channels.send_pipe(channels.parent_pipe, {"kind": "heartbeat"})
        assert channels.recv_pipe(channels.child_pipe) == {"kind": "heartbeat"}
    finally:
        channels.close()
