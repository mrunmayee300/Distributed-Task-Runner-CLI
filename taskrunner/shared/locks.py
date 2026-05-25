from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class DistributedLockManager:
    """Scheduler-local lock manager with the same interface a Redis lock would expose."""

    def __init__(self) -> None:
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    @asynccontextmanager
    async def acquire(self, key: str, timeout: float = 10.0) -> AsyncIterator[None]:
        lock = self._locks[key]
        await asyncio.wait_for(lock.acquire(), timeout=timeout)
        try:
            yield
        finally:
            lock.release()
