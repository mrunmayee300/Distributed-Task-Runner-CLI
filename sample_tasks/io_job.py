from __future__ import annotations

import asyncio


async def run(url: str = "https://example.com", delay: float = 0.2) -> dict[str, str | float]:
    await asyncio.sleep(delay)
    return {"url": url, "simulated_latency_seconds": delay}
