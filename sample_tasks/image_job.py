from __future__ import annotations

import hashlib
from pathlib import Path


def run(path: str = "README.md", rounds: int = 100_000) -> dict[str, str | int]:
    data = Path(path).read_bytes() if Path(path).exists() else path.encode()
    digest = data
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return {"path": path, "rounds": rounds, "sha256": digest.hex()}
