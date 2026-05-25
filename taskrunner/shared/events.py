from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import suppress

from taskrunner.shared.models import ClusterEvent


class EventBus:
    """In-process pub/sub used by the scheduler, API, and monitoring service."""

    def __init__(self, replay_size: int = 1_000) -> None:
        self._subscribers: set[asyncio.Queue[ClusterEvent]] = set()
        self._replay: deque[ClusterEvent] = deque(maxlen=replay_size)
        self._lock = asyncio.Lock()

    async def publish(self, event: ClusterEvent) -> None:
        async with self._lock:
            self._replay.append(event)
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            with suppress(asyncio.QueueFull):
                subscriber.put_nowait(event)

    async def subscribe(self, replay: bool = True) -> AsyncIterator[ClusterEvent]:
        queue: asyncio.Queue[ClusterEvent] = asyncio.Queue(maxsize=1_000)
        async with self._lock:
            self._subscribers.add(queue)
            history = list(self._replay) if replay else []
        try:
            for event in history:
                yield event
            while True:
                yield await queue.get()
        finally:
            async with self._lock:
                self._subscribers.discard(queue)
