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

``initialized_provider() -> set[str]``
    Callable that returns the path-set used to gate one-shot schema init.
    Same rationale as ``init_lock_provider`` — pytest fixtures (see
    :mod:`tests.conftest`) monkeypatch a module-level ``_initialized`` and
    expect the swap to take effect. If omitted, an internal set owned by
    the pool is used.

``local_provider() -> threading.local``
    Callable that returns the per-thread cache anchor. Same rationale as
    above. If omitted, an internal ``threading.local`` owned by the pool
    is used.

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
import time
import weakref
from collections.abc import Callable, Sequence
from pathlib import Path
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

# WAL preamble used by the small singleton caches that don't go through
# ThreadLocalPool (rdns_cache, ngwaf_bot_cache). Smaller cache than the
# pool default since these are tiny tables, and shorter busy_timeout
# since the writers fail fast and retry rather than blocking 30s.
SMALL_CACHE_PRAGMAS: tuple[str, ...] = (
    # busy_timeout FIRST: PRAGMA journal_mode=WAL needs an exclusive lock to
    # switch modes and does NOT honor the connect-level timeout=, so the busy
    # handler must be armed before the mode switch or a concurrent cold-open
    # returns SQLITE_BUSY immediately ("database is locked").
    "PRAGMA busy_timeout=10000",
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA cache_size=-16000",  # 16MB
)


def open_small_cache_db(
    path: str | Path,
    *,
    ddl: str,
    check_same_thread: bool = True,
    timeout: float = 10.0,
) -> sqlite3.Connection:
    """Open a raw (non-pooled) SQLite connection for one of the small
    singleton caches (rdns_cache, ngwaf_bot_cache), applying
    :data:`SMALL_CACHE_PRAGMAS` and running ``ddl``.

    Creates the parent directory if needed, then connects, applies the WAL
    preamble in order, and runs ``ddl`` via ``executescript`` so both the
    multi-statement (ngwaf) and single-statement (rdns) schemas work. This is
    the connect+mkdir+pragma-loop+DDL boilerplate the two cache modules
    previously inlined; the per-caller ``check_same_thread`` keyword is the
    only difference between them and is preserved here.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    # Use robust retry loop with exponential backoff to absorb concurrent cold-opens gracefully.
    delay = 0.05
    max_delay = 1.0
    deadline = time.monotonic() + timeout

    while True:
        try:
            con = sqlite3.connect(str(p), timeout=timeout, check_same_thread=check_same_thread)
            for pragma in SMALL_CACHE_PRAGMAS:
                if pragma.lower().startswith("pragma journal_mode="):
                    cur = con.execute("PRAGMA journal_mode")
                    row = cur.fetchone()
                    if row and row[0].lower() == "wal":
                        continue
                con.execute(pragma)
            con.executescript(ddl)
            con.commit()
            return con
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "locked" in err_msg or "busy" in err_msg:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(delay, remaining))
                delay = min(delay * 2.0, max_delay)
            else:
                raise


def remove_sqlite_db_files(path: str, *, name: str = "sqlite_pool") -> None:
    """Delete a SQLite DB file and its WAL/SHM/journal siblings.

    Safe to call even if any of the four files are missing — missing
    paths are silently ignored, and ``OSError`` is logged at DEBUG so a
    permission glitch on one sibling doesn't blow up the caller's
    teardown loop.

    Used by every SQLite-backed teardown path (per-service metadata,
    per-service usage_log, singleton metric_snapshots). Caller is
    responsible for closing connections to ``path`` before invoking.
    """
    log = logging.getLogger(name) if name != "sqlite_pool" else logger
    for suffix in ("", "-wal", "-shm", "-journal"):
        target = path + suffix
        try:
            if os.path.exists(target):
                os.remove(target)
        except OSError as e:
            log.debug("[%s] could not remove %s: %s", name, target, e)


def _connection_is_open(con: sqlite3.Connection) -> bool:
    """True if ``con`` is still usable.

    A closed ``sqlite3.Connection`` raises ``ProgrammingError`` on attribute
    access; we read the cheap ``total_changes`` property to detect a handle
    that was closed out from under the thread-local cache by ``close_all`` /
    ``teardown`` running on another thread. Negligible cost (a C-level property
    read) on the otherwise-hot cached-borrow path.
    """
    try:
        _ = con.total_changes
        return True
    except sqlite3.ProgrammingError:
        return False


def _default_connect(path: str, timeout: float) -> sqlite3.Connection:
    """Default connect: ``sqlite3.connect`` wrapped in InstrumentedConnection."""
    # Local import — sqlite_profiler imports back through backend.core and
    # we don't want a circular at module-load time.
    from backend.utils.sqlite_profiler import InstrumentedConnection

    return sqlite3.connect(
        path,
        timeout=timeout,
        factory=InstrumentedConnection,
        check_same_thread=False,
    )


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
        initialized_provider: Callable[[], set[str]] | None = None,
        local_provider: Callable[[], threading.local] | None = None,
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

        # Owned state used when no external provider is supplied. When a
        # provider IS supplied, the pool reads through it on every call so
        # monkeypatched module-level state still takes effect (a fixture
        # rebinding ``module._initialized = set()`` to clear cross-test
        # state would otherwise be invisible to the pool).
        self._owned_lock = threading.Lock()
        self._owned_initialized: set[str] = set()
        self._owned_local = threading.local()

        # Per-key locking: when no external init_lock_provider is supplied,
        # the pool creates a separate lock per key so that cold-opens for
        # different DB files (different service IDs) don't serialize against
        # each other. The single-lock contention was the root cause of the
        # "metadata_db._init_lock contended >10s" errors in production —
        # a slow cold-open for one service blocked every other service.
        self._init_lock_provider = init_lock_provider
        self._key_locks: dict[Any, threading.Lock] = {}
        self._key_locks_guard = threading.Lock()

        self._initialized_provider = initialized_provider or (lambda: self._owned_initialized)
        self._local_provider = local_provider or (lambda: self._owned_local)

        # Each entry pairs the connection with a weakref to the thread that
        # opened it. Connections are check_same_thread=False (so dead-owner
        # handles can be drained from another thread), which makes the owner
        # weakref load-bearing: a handle may ONLY be closed from a foreign
        # thread once its owner has exited — closing a live foreign thread's
        # connection is a use-after-free → segfault. Once the owner dies the
        # handle is pure dead weight (an unclosed file + an up-to-64MB page
        # cache); _reap_dead_thread_connections drops those on cold-open so a
        # churning caller (the per-tick cron executor was the 2026-06-22 OOM
        # cause) can't accumulate orphaned connections.
        # See [[backend-oom-restart-loop]].
        self._all_connections: list[tuple[sqlite3.Connection, weakref.ref[threading.Thread]]] = []
        self._all_connections_lock = threading.Lock()

    # ── Init lock ───────────────────────────────────────────────────────

    def _get_init_lock(self, key: Any) -> threading.Lock:
        """Return the init lock for ``key``.

        When an external ``init_lock_provider`` was supplied (test fixtures
        that need to monkeypatch a single global lock), delegate to it.
        Otherwise create one lock per key so different DB files cold-open
        concurrently.
        """
        if self._init_lock_provider is not None:
            return self._init_lock_provider()
        with self._key_locks_guard:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    # ── Per-thread cache ────────────────────────────────────────────────

    def _conns(self) -> dict[Any, sqlite3.Connection]:
        local = self._local_provider()
        if not hasattr(local, self._local_attr):
            setattr(local, self._local_attr, {})
        return getattr(local, self._local_attr)

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
            if not _connection_is_open(cached):
                # Closed out from under this thread — e.g. close_all/teardown
                # ran on another thread (now possible: cron bodies share a
                # long-lived watchdog thread pool instead of a per-tick
                # thread, so a worker can outlive a reset of its connection).
                # Drop the stale entry and fall through to reopen.
                pool.pop(key, None)
            elif self._on_borrow_fn is None:
                return cached
            else:
                rebound = self._on_borrow_fn(cached)
                if rebound is not None:
                    return rebound
                # Hook signalled the cached connection is unusable (e.g. share_db's
                # PRAGMA foreign_keys=ON raised ProgrammingError on a closed
                # connection). Drop it and fall through to reopen.
                try:
                    cached.close()
                except Exception:
                    pass
                pool.pop(key, None)

        path = self._path_fn(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        init_lock = self._get_init_lock(key)
        if not init_lock.acquire(timeout=self._init_lock_timeout):
            raise sqlite3.OperationalError(
                f"{self._name}._init_lock contended >{self._init_lock_timeout:g}s for {key}"
                " — another thread is stuck inside connect+PRAGMA"
            )
        try:
            # Opportunistic reap on the cold path: close handles whose owning
            # thread has exited. Cheap (bounded by live count) and timely —
            # thread churn IS a cold-open, so this runs exactly when orphans
            # would otherwise pile up, keeping the count ≈ live threads.
            self._reap_dead_thread_connections()
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
            owner = weakref.ref(threading.current_thread())
            with self._all_connections_lock:
                self._all_connections.append((con, owner))
            try:
                con.row_factory = sqlite3.Row
                for pragma in self._pragmas:
                    if pragma.lower().startswith("pragma journal_mode="):
                        cur = con.execute("PRAGMA journal_mode")
                        row = cur.fetchone()
                        if row and row[0].lower() == "wal":
                            continue
                    con.execute(pragma)
                initialized = self._initialized_provider()
                if path not in initialized:
                    self._schema_fn(con)
                    initialized.add(path)
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
        con = sqlite3.connect(uri, uri=True, timeout=timeout, check_same_thread=False)
        con.row_factory = sqlite3.Row
        return con

    def _reap_dead_thread_connections(self) -> None:
        """Close + drop connections whose owning thread has exited.

        ThreadLocalPool pins every connection in ``_all_connections`` until
        shutdown — so a handle whose creating thread has died can never be used
        again, yet it keeps a file open (3 fds in WAL) and an up-to-
        ``cache_size`` page cache alive. Closing it here (from a foreign thread)
        is safe precisely because the owner has exited; a LIVE owner's handle
        is left untouched (a cross-thread close mid-use would segfault).
        A churning caller (cron_task formerly spawned a fresh executor thread
        per tick) leaked ~2 such handles per tick → multi-GB RSS + fd
        exhaustion → OOM (prod 2026-06-22, see [[backend-oom-restart-loop]]).
        Reaping the dead-owner entries keeps the live count tracking live
        threads. Takes the registry lock itself; safe to call from the cold
        path while holding ``init_lock`` (distinct locks, no inversion).
        """
        with self._all_connections_lock:
            live: list[tuple[sqlite3.Connection, weakref.ref[threading.Thread]]] = []
            for con, owner in self._all_connections:
                t = owner()
                if t is not None and t.is_alive():
                    live.append((con, owner))
                    continue
                try:
                    con.close()
                except Exception:
                    pass
            self._all_connections[:] = live

    def close_all(self) -> None:
        """Close connections owned by THIS thread or by a thread that has
        already exited; leave a different live thread's connection in place.

        Used by pytest fixtures to drain TestClient worker-thread connections.
        Connections are ``check_same_thread=False``, so a cross-thread
        ``con.close()`` no longer raises — which means closing one out from
        under a DIFFERENT, still-alive thread mid-use is a use-after-free and
        segfaults the interpreter (the 2026-06-23 xdist worker crashes:
        telemetry-proxy / ngwaf-sync streaming threads were live when a
        per-test ``reset()`` ran). Dead-owner handles are safe to close from
        any thread (no one can touch them); live foreign handles are retained
        and reaped later once their owner exits (see
        :meth:`_reap_dead_thread_connections`). The calling thread's own
        ``_local`` entries are cleared too — they'd point at closed handles.
        """
        cur = threading.current_thread()
        with self._all_connections_lock:
            retained: list[tuple[sqlite3.Connection, weakref.ref[threading.Thread]]] = []
            for con, owner in self._all_connections:
                t = owner()
                if t is None or t is cur or not t.is_alive():
                    try:
                        con.close()
                    except Exception:
                        pass
                else:
                    retained.append((con, owner))
            self._all_connections[:] = retained
        local = self._local_provider()
        if hasattr(local, self._local_attr):
            getattr(local, self._local_attr).clear()

    def teardown(self, key: Any) -> None:
        """Close any thread-local connection and discard the init marker.

        File deletion is the caller's responsibility — pools that back
        per-service files (metadata, usage_log) layer ``os.remove`` on top;
        pools that back a singleton file (share_db) don't.
        """
        pool = self._conns()
        con = pool.pop(key, None)

        cur = threading.current_thread()
        with self._all_connections_lock:
            retained = []
            for c, owner in self._all_connections:
                matches = c is con or getattr(c, "_service_id", object()) == key
                t = owner()
                # Only close cross-thread when the owner has exited (or it's
                # ours): a check_same_thread=False close of a live foreign
                # thread's connection is a use-after-free → segfault. A live
                # foreign owner keeps its handle and reaps it on exit.
                safe_to_close = t is None or t is cur or not t.is_alive()
                if matches and safe_to_close:
                    try:
                        c.close()
                    except Exception:
                        pass
                else:
                    retained.append((c, owner))
            self._all_connections[:] = retained

        try:
            path = self._path_fn(key)
        except Exception:
            return
        self._initialized_provider().discard(path)
        with self._key_locks_guard:
            self._key_locks.pop(key, None)

    def reset(self) -> None:
        """Drop the in-memory init cache and close all connections.

        Pytest fixtures that swap the data dir per-test rely on this to
        avoid carrying over a connection bound to the previous test's path.
        """
        self.close_all()
        self._initialized_provider().clear()
        with self._key_locks_guard:
            self._key_locks.clear()

    # ── Internal helpers ───────────────────────────────────────────────

    def _open(self, path: str) -> sqlite3.Connection:
        if self._connect_fn is not None:
            return self._connect_fn(path)
        return _default_connect(path, self._connect_timeout)
