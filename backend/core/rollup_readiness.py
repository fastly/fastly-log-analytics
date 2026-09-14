"""Per-service, in-process flag: has this durable-serving pod confirmed its
local closed-hour rollup cache reflects at least one full backfill pass?

Deliberately NOT persisted anywhere (no Postgres/SQLite row, no file) — it
exists only to bridge the pod-restart bootstrap window described in
docs/superpowers/specs/2026-09-09-partial-hour-speed-layer-design.md Part 1.
A restart naturally resets it to False, which is correct: a fresh pod must
re-confirm coverage before trusting local rollups again.
"""

from __future__ import annotations

import threading

_lock = threading.Lock()
_ready: set[str] = set()


def rollup_coverage_ready(service_id: str) -> bool:
    with _lock:
        return service_id in _ready


def mark_rollup_coverage_ready(service_id: str) -> None:
    with _lock:
        _ready.add(service_id)


def reset_rollup_coverage_ready(service_id: str | None = None) -> None:
    """Test-only: clear one service's flag, or every service's when omitted."""
    with _lock:
        if service_id is None:
            _ready.clear()
        else:
            _ready.discard(service_id)
