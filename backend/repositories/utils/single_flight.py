"""Thread-safe single-flight request coalescing.

Dedupes concurrent, IDENTICAL in-flight work: when two callers ask for the
same ``key`` at (nearly) the same time, only the first ("leader") actually
runs the builder; every other caller ("follower") blocks on the leader's
result and reuses it instead of redoing the work.

This is NOT a cache. Entries are removed the instant the leader finishes
(success or failure), so a call that starts after the previous one for the
same key has already completed always becomes its own leader and redoes the
work. That is the deliberate difference from a TTL response memo (see
``backend.repositories.utils.response_cache``): a memo is populated only
AFTER a request completes, so a second caller that arrives WHILE the first
is still running finds nothing to hit and reruns the full pipeline anyway.
Single-flight covers exactly that overlap window.

Motivating case: ``/api/network-health``'s ``core`` and ``map`` sections are
fired as separate, genuinely concurrent HTTP requests (see
``frontend/app/network/page.tsx`` — the 3-way parallel-POST split) for the
identical service/window/filters. Each request gets its own pooled DuckDB
connection (``backend/core/duckdb_pool.py``), and DuckDB temp tables are
connection-scoped, so a `CREATE TEMP TABLE` built by one request literally
cannot be read by the other. The builder passed to :func:`coalesce` must
therefore return plain, process-shareable Python data (row lists, dicts) —
never a temp-table name, cursor, or anything tied to the connection that
produced it.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

_registry_lock = threading.Lock()
_registry: dict[str, _InFlight] = {}


class _InFlight[T]:
    __slots__ = ("event", "result", "error")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: T | None = None
        self.error: BaseException | None = None


def coalesce[T](key: str, build: Callable[[], T]) -> tuple[T, bool]:
    """Run ``build()`` for ``key``, sharing the result with concurrent callers.

    Returns ``(result, is_leader)``. Exactly one concurrent caller per key
    becomes the leader (the one that actually invokes ``build``); every other
    caller blocks until the leader finishes and receives the SAME result
    object rather than re-running ``build``. If ``build`` raises, every
    waiter (including the leader) sees the same exception re-raised — no
    caller silently gets a different, possibly-partial result.

    Callers must design ``build`` so its return value is safe to hand to a
    caller other than the one that produced it — e.g. plain data, not a
    connection-scoped resource such as a DuckDB temp table.
    """
    with _registry_lock:
        existing = _registry.get(key)
        if existing is not None:
            slot = existing
            is_leader = False
        else:
            slot = _InFlight()
            _registry[key] = slot
            is_leader = True

    if not is_leader:
        slot.event.wait()
        if slot.error is not None:
            raise slot.error
        return slot.result, False  # type: ignore[return-value]

    try:
        result = build()
        slot.result = result
        return result, True
    except BaseException as e:
        slot.error = e
        raise
    finally:
        with _registry_lock:
            if _registry.get(key) is slot:
                del _registry[key]
        slot.event.set()
