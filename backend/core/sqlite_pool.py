"""Generic thread-local SQLite connection pool.

Shared shape extracted from the three near-identical pools that previously
lived in :mod:`backend.core.metadata.base`,
:mod:`backend.core.metadata.usage_log_db`, and
:mod:`backend.core.share_db.connection`. Each of those is now a thin wrapper
around an instance of :class:`ThreadLocalPool` configured for its DB.

What the pool owns
------------------
* A per-thread connection cache (``threading.local`` with a ``.conns`` dict).
* A process-wide registry of every connection handed out, across threads, so
  test fixtures can drain connections opened on TestClient worker threads
  that are otherwise invisible to the main thread's thread-local.
* A per-(process, db_path) initialised-paths set so schema init runs at most
  once for a given file.
* An init lock around the cold-start connect+PRAGMA window. ``PRAGMA
  journal_mode=WAL`` requires an exclusive writer lock to switch from the
  default delete journal mode; without serialising the cold path, concurrent
  first-opens race and one raises ``database is locked``.
* A canonical PRAGMA preamble applied once on every fresh connection.

Customisation hooks
-------------------
``path_fn(key) -> str``
    Resolve the absolute on-disk path for the given key. Caller is
    responsible for validating ``key`` (the per-service pools raise
    :class:`InvalidServiceIdError` here); the pool does not catch.

``schema_fn(con) -> None``
    Apply schema. Called inside the init lock the first time a (process,
    path) is seen.

``connect_fn(path) -> sqlite3.Connection``
    Open a connection. Default uses ``sqlite3.connect(path,
    timeout=connect_timeout, factory=InstrumentedConnection)`` so statements
    show up in the Live Query Monitor. Override to plug in corruption
    self-heal — :mod:`backend.core.share_db.connection` wraps a
    quarantine-on-corruption routine here.

``on_borrow_fn(con) -> sqlite3.Connection | None``
    Optional hook called on every cached-borrow. Return the connection to
    keep using it; return ``None`` to evict the cache entry and reopen.
    :mod:`backend.core.share_db.connection` uses this to re-assert
    ``PRAGMA foreign_keys=ON`` per borrow (SQLite resets it if any caller
    toggles it during the connection's lifetime).

``init_lock_provider() -> threading.Lock``
    Callable that returns the lock to use for the cold-start window. The
    pool calls this on every cold-open so module-level monkeypatching of
    the lock (used by :mod:`tests.core.test_metadata_db_concurrency`) keeps
    working. If omitted, an internal lock owned by the pool is used.

Behavior preserved across the three callers
-------------------------------------------
* Default ``sqlite3.connect`` keyword arguments — no ``isolation_level``
  (autocommit-off, implicit BEGIN), no ``check_same_thread`` override
  (defaults to True; safe because every connection is per-thread).
* Per-borrow service_id stamping for the Live Query Monitor's ``service``
  column (set on the :class:`InstrumentedConnection` subclass; the C-typed
  base rejects arbitrary attribute assignment).
* Connection registered in ``_all_connections`` BEFORE PRAGMAs run, so a
  mid-PRAGMA exception still leaves the handle reachable for cleanup.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from collections.abc import Callable, Sequence
from typing import Any

logger = logging.getLogger(__name__)

# Canonical PRAGMA preamble. All three pre-extraction pools applied the
# same five PRAGMAs; share_db ordered ``busy_timeout`` before ``cache_size``
# while the metadata pools ordered them the other way round. The unified
# order matches the metadata pools (cache_size then busy_timeout); both
# PRAGMAs are non-transactional so the swap is observationally a no-op.
DEFAULT_PRAGMAS: tuple[str, ...] = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA foreign_keys=ON",
    "PRAGMA cache_size=-64000",  # 64MB page cache
    "PRAGMA busy_timeout=30000",  # 30s, belt-and-suspenders alongside timeout=
)


def _default_connect(path: str, timeout: float) -> sqlite3.Connection:
    """Default connect: ``sqlite3.connect`` wrapped in InstrumentedConnection."""
    # Local import — sqlite_profiler imports back through backend.core and
    # we don't want a circular at module-load time.
    from backend.utils.sqlite_profiler import InstrumentedConnection

    return sqlite3.connect(path, timeout=timeout, factory=InstrumentedConnection)


class ThreadLocalPool:
    """Process-wide thread-local SQLite connection pool.

    Construct one instance per logical DB family (per-service metadata,
    per-service usage_log, global share_db) and call :meth:`get` on every
    request to fetch a thread-local connection.
    """

    def __init__(
        self,
        *,
        name: str,
        path_fn: Callable[[Any], str],
        schema_fn: Callable[[sqlite3.Connection], None],
        connect_fn: Callable[[str], sqlite3.Connection] | None = None,
        on_borrow_fn: Callable[[sqlite3.Connection], sqlite3.Connection | None] | None = None,
        init_lock_provider: Callable[[], threading.Lock] | None = None,
        init_lock_timeout: float = 10.0,
        connect_timeout: float = 30.0,
        pragmas: Sequence[str] = DEFAULT_PRAGMAS,
        stamp_service_id: bool = True,
        local_attr: str = "conns",
    ) -> None:
        self._name = name
        self._path_fn = path_fn
        self._schema_fn = schema_fn
        self._connect_fn = connect_fn
        self._on_borrow_fn = on_borrow_fn
        self._init_lock_timeout = init_lock_timeout
        self._connect_timeout = connect_timeout
        self._pragmas = tuple(pragmas)
        self._stamp_service_id = stamp_service_id
        self._local_attr = local_attr

        # Owned lock used when no external provider is supplied. When an
        # external provider IS supplied, the pool reads its lock through
        # the provider on every call so monkeypatched module-level locks
        # still take effect.
        self._owned_lock = threading.Lock()
        self._init_lock_provider = init_lock_provider or (lambda: self._owned_lock)

        self._local = threading.local()
        self._initialized: set[str] = set()
        self._all_connections: list[sqlite3.Connection] = []
        self._all_connections_lock = threading.Lock()

    # ── Per-thread cache ────────────────────────────────────────────────

    def _conns(self) -> dict[Any, sqlite3.Connection]:
        if not hasattr(self._local, self._local_attr):
            setattr(self._local, self._local_attr, {})
        return getattr(self._local, self._local_attr)

    # ── Public surface ─────────────────────────────────────────────────

    def get(self, key: Any) -> sqlite3.Connection:
        """Return a thread-local connection for ``key``.

        Cold path is serialised through the init lock so concurrent
        first-opens of a brand-new file don't race on ``PRAGMA
        journal_mode=WAL``.
        """
        pool = self._conns()
        cached = pool.get(key)
        if cached is not None:
            if self._on_borrow_fn is None:
                return cached
            rebound = self._on_borrow_fn(cached)
            if rebound is not None:
                return rebound
            # Hook signalled the cached connection is unusable (e.g. share_db's
            # PRAGMA foreign_keys=ON raised ProgrammingError on a closed
            # connection). Drop it and fall through to reopen.
            pool.pop(key, None)

        path = self._path_fn(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        init_lock = self._init_lock_provider()
        if not init_lock.acquire(timeout=self._init_lock_timeout):
            raise sqlite3.OperationalError(
                f"{self._name}._init_lock contended >{self._init_lock_timeout:g}s for {key}"
                " — another thread is stuck inside connect+PRAGMA"
            )
        try:
            con = self._open(path)
            if self._stamp_service_id:
                # InstrumentedConnection allows attribute assignment; the
                # C-typed sqlite3.Connection base does not. Wrap in try
                # so plain-Connection callers (e.g. share_db pre-flip)
                # still work during incremental migration.
                try:
                    con._service_id = key  # type: ignore[attr-defined]
                except AttributeError:
                    pass
            # Register BEFORE PRAGMAs/schema: any exception below must
            # still leave the handle reachable for close_all to drain.
            with self._all_connections_lock:
                self._all_connections.append(con)
            try:
                con.row_factory = sqlite3.Row
                for pragma in self._pragmas:
                    con.execute(pragma)
                if path not in self._initialized:
                    self._schema_fn(con)
                    self._initialized.add(path)
            except Exception:
                try:
                    con.close()
                except Exception:
                    pass
                raise
        finally:
            init_lock.release()

        pool[key] = con
        return con

    def open_readonly(self, key: Any, *, timeout: float = 5.0) -> sqlite3.Connection:
        """Open a short-lived read-only connection (no pool, no PRAGMAs).

        ``mode=ro`` guarantees the open call cannot acquire the writer
        lock — a slow reader on this path can never block a concurrent
        writer. File-must-exist semantics: raises ``OperationalError``
        when the file isn't there yet.
        """
        path = self._path_fn(key)
        uri = f"file:{path}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=timeout)
        con.row_factory = sqlite3.Row
        return con

    def close_all(self) -> None:
        """Close every connection handed out, across every thread.

        Used by pytest fixtures to drain TestClient worker-thread
        connections. The calling thread's own ``_local`` entries are
        cleared too — they would have pointed at closed handles otherwise.
        """
        with self._all_connections_lock:
            for con in self._all_connections:
                try:
                    con.close()
                except Exception:
                    pass
            self._all_connections.clear()
        if hasattr(self._local, self._local_attr):
            getattr(self._local, self._local_attr).clear()

    def teardown(self, key: Any) -> None:
        """Close any thread-local connection and discard the init marker.

        File deletion is the caller's responsibility — pools that back
        per-service files (metadata, usage_log) layer ``os.remove`` on top;
        pools that back a singleton file (share_db) don't.
        """
        pool = self._conns()
        con = pool.pop(key, None)
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        try:
            path = self._path_fn(key)
        except Exception:
            return
        self._initialized.discard(path)

    def reset(self) -> None:
        """Drop the in-memory init cache and close all connections.

        Pytest fixtures that swap the data dir per-test rely on this to
        avoid carrying over a connection bound to the previous test's path.
        """
        self.close_all()
        self._initialized.clear()

    # ── Internal helpers ───────────────────────────────────────────────

    def _open(self, path: str) -> sqlite3.Connection:
        if self._connect_fn is not None:
            return self._connect_fn(path)
        return _default_connect(path, self._connect_timeout)
