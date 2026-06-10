"""Per-service DuckDB connection pool.

Each API request previously opened a fresh DuckDB connection, ran ~10 PRAGMAs,
configured S3 + iceberg, and called ``update_iceberg_view`` to bind the per-
service view onto the new connection. Steady-state cost was ~50ms of setup
plus another ~45ms on first-query overhead — paid by every request.

This module caches read-only connections in a per-service pool. A request
checks out a fully-configured connection, runs its queries, then returns it.
The view binding is re-validated on checkout via the existing fast-path
fingerprint (``_view_cache``); a cache hit is a few-µs dict lookup, so the
hot path checkout is genuinely cheap.

The pool is opt-in via ``DUCKDB_CONNECTION_POOL`` env var (default on); set
to ``"0"`` to disable and fall back to the always-fresh-connection path.
Exists primarily so tests and ops have an emergency switch if a pooling
regression slips through.

Lifecycle:
  * Pool is created lazily on first checkout for a service.
  * Idle connections are stored in a LIFO queue (recently-used first, so the
    OS page cache stays hot on the file descriptors that are currently warm).
  * Pool size is bounded by ``max_size`` (default 8 per service). When the
    pool is empty and ``in_use < max_size``, the next checkout creates a new
    connection. When ``in_use == max_size``, waiters block on a Condition.
  * If a request returns a connection that errored mid-query, the connection
    is discarded (closed) rather than returned to the pool — the next
    checkout creates a fresh one.
  * On checkin, we DROP any temp tables the request created (sweep against
    ``information_schema``) so a long-lived pool connection doesn't slowly
    accumulate state across requests. A leaked temp table from a prior
    request would otherwise show up as ``CATALOG ENTRY ALREADY EXISTS`` if a
    later request happened to pick the same uuid (improbable, but
    deterministic at scale).

Concurrency:
  * Multiple connections to the same DuckDB file on the same process are safe
    — they share the in-memory database state.
  * All connections open with ``read_only=False`` (``get_connection`` forces
    this) so cron write connections never conflict with pool connections.

Failure handling:
  * If view rebind fails on checkout, we discard the connection and try a
    fresh one. After ``max_retries`` consecutive failures we surface
    ``DBBusyError`` to the caller (which becomes a 503 in deps.py).
"""

from __future__ import annotations

import collections
import logging
import os
import queue
import threading
import time
from contextlib import contextmanager

import duckdb

logger = logging.getLogger(__name__)


def _pool_enabled() -> bool:
    return os.getenv("DUCKDB_CONNECTION_POOL", "1").lower() not in ("0", "false", "no", "off")


def _pool_max_size() -> int:
    raw = os.getenv("DUCKDB_POOL_MAX_SIZE", "8")
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 8


def _pool_conn_memory_limit() -> str | None:
    """Optional per-pool-connection memory cap.

    Without this, every pool connection inherits the process-wide DuckDB
    memory_limit derived from physical RAM (~60%), so 8 concurrent queries
    against a large dataset can each balloon to multi-GB. Set
    ``DUCKDB_POOL_CONN_MEMORY_LIMIT`` (e.g. ``256MB`` or ``1GB``) to enforce
    a per-connection ceiling — DuckDB spills intermediate state to its
    temp directory when over the limit instead of growing RSS unbounded.

    Returns the env-var value (passed through verbatim — DuckDB accepts
    ``256MB`` / ``2GB`` / ``104857600`` etc.) or ``None`` to keep the default.
    """
    return os.getenv("DUCKDB_POOL_CONN_MEMORY_LIMIT") or None


def _pool_conn_threads() -> int | None:
    """Optional per-pool-connection DuckDB thread count.

    Each pool connection defaults to ``min(cpu_count, 8)`` DuckDB threads.
    With ``DUCKDB_POOL_MAX_SIZE=8`` concurrent queries that means
    ``8 connections × 8 threads = 64 threads`` competing for ~8 physical
    cores — context-switching dominates and per-query latency degrades
    well past linear queueing. Set ``DUCKDB_POOL_CONN_THREADS`` to a smaller
    value (commonly ``cpu_count // pool_max_size``) to trade single-query
    throughput for better tail-latency under sustained load.

    Returns the int value (>=1) or ``None`` to keep the default.
    """
    raw = os.getenv("DUCKDB_POOL_CONN_THREADS")
    if not raw:
        return None
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return None


# Per-connection state tracking. DuckDB connection objects are slotted
# C types — they don't accept arbitrary attribute assignment — so we
# keep our metadata in a module-level dict keyed by id(con). Entries are
# cleared when the connection is closed/discarded.
#
# Fingerprint = the ``_view_cache`` tuple at the time the view
# was last bound to this connection. The tuple is replaced (not mutated)
# when the cache rotates, so identity is a sufficient fresh-check.
_conn_state: dict[int, dict] = {}
_conn_state_lock = threading.Lock()


def _set_conn_state(con: duckdb.DuckDBPyConnection, **kv) -> None:
    with _conn_state_lock:
        state = _conn_state.setdefault(id(con), {})
        state.update(kv)


def _get_conn_state(con: duckdb.DuckDBPyConnection, key: str, default=None):
    with _conn_state_lock:
        return _conn_state.get(id(con), {}).get(key, default)


def _forget_conn(con: duckdb.DuckDBPyConnection) -> None:
    with _conn_state_lock:
        _conn_state.pop(id(con), None)


def _safe_buffer_mtime(src: dict | None) -> float | None:
    """Return mtime of the service's buffer dir, or None if it can't be read.

    Used as part of the pool's checkout fingerprint so that the sync cron
    removing buffer parquet files (without touching ``_view_cache``) still
    invalidates pooled connections. Any add/remove inside the dir bumps the
    dir's own mtime — so a single stat is enough.
    """
    if src is None:
        return None
    try:
        from backend.core.iceberg._core import _buffer_dir

        path = _buffer_dir(src)
        return os.path.getmtime(path)
    except Exception:
        return None


_WAIT_SAMPLES_MAX = 1024  # ~last 17 minutes at 1 req/s; ~3.5 minutes at 5 req/s


class _Pool:
    """Per-service pool. Not exposed directly — use ``checkout_connection``."""

    def __init__(self, service_key: str, max_size: int):
        self.service_key = service_key
        self.max_size = max_size
        # LIFO so the most-recently-used connection (warmest in any OS / DuckDB
        # internal caches) is the next checkout.
        self._idle: queue.LifoQueue = queue.LifoQueue(maxsize=max_size)
        self._lock = threading.RLock()
        # ``in_use`` is the count of connections currently checked out plus
        # connections idle in the queue. Bounded by ``max_size``.
        self._in_use = 0
        self._cond = threading.Condition(self._lock)
        # Cumulative counters for diagnostics — exposed via ``stats()``.
        self._created_total = 0
        self._reused_total = 0
        self._discarded_total = 0
        # Phase 6 in-process sampler — last ``_WAIT_SAMPLES_MAX`` checkout
        # wait times in milliseconds. Companion to the OTel histogram
        # (``app.thread_wait_ms``) so the admin UI can render p50/p95/p99
        # without parsing docker logs. Bounded deque so memory stays flat
        # regardless of throughput.
        self._wait_samples: collections.deque[float] = collections.deque(maxlen=_WAIT_SAMPLES_MAX)
        self._wait_samples_lock = threading.Lock()

    def acquire(self, src: dict, max_wait: float) -> duckdb.DuckDBPyConnection:
        # Phase 6 telemetry: time how long this checkout spends WAITING for
        # an idle connection (the saturated path). Both fast-path (idle
        # ready) and fresh-build paths record ~0 ms here; only contention
        # with cron / another request shows up as non-zero. ADR-03 reads
        # the p95 of ``app.thread_wait_ms`` to decide cron isolation
        # strategy (separate pool vs separate process).
        t_acquire_start = time.monotonic()
        deadline = t_acquire_start + max_wait
        reused_con: duckdb.DuckDBPyConnection | None = None
        waited = False
        with self._cond:
            while True:
                # Fast path: idle connection available
                try:
                    reused_con = self._idle.get_nowait()
                    self._reused_total += 1
                    break  # fall through to UNLOCKED _prepare_checkout
                except queue.Empty:
                    pass

                # Capacity available: build a new one outside the lock
                if self._in_use < self.max_size:
                    self._in_use += 1
                    self._created_total += 1
                    break  # fall through to the unlocked build path

                # Saturated: wait for a return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    wait_ms = (time.monotonic() - t_acquire_start) * 1000.0
                    try:
                        from backend.core.request_telemetry import thread_wait_histogram

                        thread_wait_histogram().record(
                            wait_ms,
                            {"service": self.service_key, "outcome": "timeout"},
                        )
                    except Exception:
                        pass
                    self._record_wait_sample(wait_ms)
                    raise _PoolBusy(f"pool for {self.service_key} saturated at {self.max_size}")
                waited = True
                self._cond.wait(timeout=remaining)

        # Record the (possibly zero) wait time so Phase 6 has a population
        # of samples — even fast-path checkouts contribute, so the median
        # tracks total request-path cost rather than just contention.
        wait_ms = (time.monotonic() - t_acquire_start) * 1000.0
        try:
            from backend.core.request_telemetry import thread_wait_histogram

            thread_wait_histogram().record(
                wait_ms,
                {
                    "service": self.service_key,
                    "outcome": "reused" if reused_con is not None else "created",
                    "waited": str(waited).lower(),
                },
            )
        except Exception:
            # OTel SDK not initialised (tests) or histogram creation failed —
            # never let telemetry instrumentation break a checkout.
            pass
        self._record_wait_sample(wait_ms)

        # Outside lock. Both branches can call ``update_iceberg_view`` which
        # may take seconds when an Iceberg snapshot reload or S3 manifest read
        # is required; holding the pool's Condition lock across that call
        # deadlocks every concurrent waiter, the ``max_wait`` cap can't fire
        # because waiters block on the threading lock (not ``_cond.wait``),
        # and the FastAPI thread pool then fills with stuck checkouts until
        # the backend stops accepting new connections.
        if reused_con is not None:
            # _prepare_checkout calls _discard on failure (decrements in_use,
            # notifies waiter) before re-raising — no extra cleanup needed.
            return self._prepare_checkout(reused_con, src)

        # Build fresh. _in_use was already incremented; if the build raises
        # we MUST decrement and notify a waiter, hence the try.
        try:
            from backend.core.duckdb import get_connection

            con = get_connection(source=src, read_only=True, max_wait=max_wait)
            _set_conn_state(con, service_key=self.service_key)
            # Apply per-connection overrides once at build time — DuckDB
            # persists session settings for the connection's lifetime, so
            # subsequent checkouts of this same connection inherit them.
            mem_limit = _pool_conn_memory_limit()
            if mem_limit:
                try:
                    con.execute(f"SET memory_limit = '{mem_limit}'")
                except Exception as e:
                    logger.warning(
                        "[pool] %s: failed to apply DUCKDB_POOL_CONN_MEMORY_LIMIT=%r: %s",
                        self.service_key,
                        mem_limit,
                        e,
                    )
            conn_threads = _pool_conn_threads()
            if conn_threads is not None:
                try:
                    con.execute(f"SET threads = {conn_threads}")
                except Exception as e:
                    logger.warning(
                        "[pool] %s: failed to apply DUCKDB_POOL_CONN_THREADS=%d: %s",
                        self.service_key,
                        conn_threads,
                        e,
                    )
            self._stamp_fingerprint(con, src)
            return con
        except Exception:
            with self._cond:
                self._in_use -= 1
                self._cond.notify()
            raise

    def release(self, con: duckdb.DuckDBPyConnection, *, errored: bool = False) -> None:
        """Return a connection to the pool. Pass ``errored=True`` to discard
        instead — the next checkout will build fresh."""
        if errored:
            self._discard(con)
            return
        try:
            self._cleanup_temp_tables(con)
        except Exception as e:
            # Cleanup failure means the connection is in unknown state — discard.
            logger.debug("[pool] %s: cleanup failed, discarding: %s", self.service_key, e)
            self._discard(con)
            return
        with self._cond:
            try:
                self._idle.put_nowait(con)
                self._cond.notify()
                return
            except queue.Full:
                # Pool already at max idle (shouldn't happen given in_use cap,
                # but defensive). Close this one and free the slot.
                pass
        # Outside lock: close
        try:
            con.close()
        except Exception:
            pass
        with self._cond:
            self._in_use -= 1
            self._cond.notify()

    def _discard(self, con: duckdb.DuckDBPyConnection) -> None:
        _forget_conn(con)
        try:
            con.close()
        except Exception:
            pass
        with self._cond:
            self._in_use -= 1
            self._discarded_total += 1
            self._cond.notify()

    def _prepare_checkout(self, con: duckdb.DuckDBPyConnection, src: dict) -> duckdb.DuckDBPyConnection:
        """Re-validate the view binding before handing the connection out.

        Two checks make up the fingerprint:

          1. The iceberg ``_view_cache`` tuple for this service.
             The tuple is replaced (not mutated) when the cache rotates, so
             identity is a sufficient check that the SQL we'd bind matches
             what we bound last time.

          2. mtime of the buffer directory. The sync cron's commit step
             DELETES buffer parquet files without calling update_iceberg_view —
             so the view-cache tuple keeps looking "fresh" while the files
             it references are gone. mtime catches that: any add/remove in
             the dir bumps it. Cost ~1 syscall (~µs).

        If either differs from what we last stamped, rebind. If the rebind
        fails, discard the connection and let the caller retry.
        """
        try:
            from backend.core.iceberg import view as iceberg_view

            current = iceberg_view._view_cache.get(self.service_key)
            stamped_view = _get_conn_state(con, "view_fingerprint")
            stamped_buf = _get_conn_state(con, "buffer_mtime")
            current_buf = _safe_buffer_mtime(src)
            if current is not None and current is stamped_view and current_buf == stamped_buf:
                # View AND underlying buffer set match what we bound last
                # time — nothing to do.
                return con
            iceberg_view.update_iceberg_view(con, src)
            self._stamp_fingerprint(con, src)
            return con
        except Exception as e:
            logger.warning("[pool] %s: view refresh on checkout failed, discarding: %s", self.service_key, e)
            self._discard(con)
            raise

    def warm_idle(self, src: dict) -> None:
        """Rebind every idle connection to the latest cached view.

        Called by writer-side cron jobs (sync, commit) after they mutate
        state that invalidates the per-service _view_cache fingerprint.
        Drains the idle queue under the lock, binds the cached view DDL
        on each conn via _try_fast_path_view (which handles the CREATE OR
        REPLACE VIEW → TEMP VIEW translation), re-stamps the fingerprint,
        then returns every conn to the queue. Sequential because TEMP
        VIEWs are per-connection in DuckDB and a single connection handle
        is not safe to call from multiple threads.

        Drain-then-return rather than pop-bind-put-per-conn because _idle
        is a LIFO queue: pop-then-put returns the same conn on the next
        pop, so we'd just keep warming one slot.

        Bookkeeping: _in_use is unchanged across drain + return because
        drained conns are conceptually "held by warm_idle" — same slot
        in the invariant `_in_use == checked_out + idle_count`. A
        concurrent acquirer that arrives mid-warm either builds a new
        conn (if _in_use < max_size) or waits on _cond, identical to
        today's behavior.
        """
        from backend.core.iceberg import view as iceberg_view

        drained: list[duckdb.DuckDBPyConnection] = []
        with self._cond:
            while True:
                try:
                    drained.append(self._idle.get_nowait())
                except queue.Empty:
                    break
        if not drained:
            return

        for con in drained:
            try:
                iceberg_view._try_fast_path_view(con, src)
                self._stamp_fingerprint(con, src)
            except Exception as e:
                logger.warning(
                    "[pool] %s: warm_idle bind failed (will rebind on next checkout): %s",
                    self.service_key,
                    e,
                )

        with self._cond:
            for con in drained:
                try:
                    self._idle.put_nowait(con)
                    self._cond.notify()
                except queue.Full:
                    # Should not happen — we drained this same queue under the
                    # same lock with no intervening puts. Defensive close.
                    try:
                        con.close()
                    except Exception:
                        pass
                    self._in_use -= 1
                    self._cond.notify()

    def _stamp_fingerprint(self, con: duckdb.DuckDBPyConnection, src: dict | None = None) -> None:
        try:
            from backend.core.iceberg import view as iceberg_view

            current = iceberg_view._view_cache.get(self.service_key)
            buf_mtime = _safe_buffer_mtime(src) if src is not None else None
            _set_conn_state(
                con,
                view_fingerprint=current,
                buffer_mtime=buf_mtime,
            )
        except Exception:
            _set_conn_state(con, view_fingerprint=None, buffer_mtime=None)

    def _cleanup_temp_tables(self, con: duckdb.DuckDBPyConnection) -> None:
        """Drop any t_<uuid>-style temp tables left behind by repositories
        whose ``temp_table`` context manager exited cleanly does the DROP
        itself; this is belt-and-suspenders for the failure paths."""
        try:
            rows = con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' AND temporary = true"
            ).fetchall()
        except Exception:
            return
        for (name,) in rows:
            try:
                con.execute(f"DROP TABLE IF EXISTS {name}")
            except Exception:
                # Best-effort — if a single table fails to drop, keep going.
                pass

    def _record_wait_sample(self, wait_ms: float) -> None:
        """Append a checkout wait-time sample to the bounded ring buffer.

        Lock-protected so concurrent acquirers don't trample the deque's
        internal state (CPython's deque IS thread-safe for single ops, but
        we also read+sort it from ``_wait_stats`` which would race).
        """
        with self._wait_samples_lock:
            self._wait_samples.append(wait_ms)

    def _wait_stats(self) -> dict:
        """Return percentile summary over the recent-samples ring buffer.

        Computed on-read (sort a snapshot, no continuous histogram) — at
        ~1024 samples this is well under 1 ms. Returns zeros when the
        buffer is empty so the admin UI can render a stable shape from
        boot (no conditional rendering churn). Counts are emitted so the
        operator can tell whether a green p95 reflects "no contention"
        or "no samples yet".
        """
        with self._wait_samples_lock:
            snap = list(self._wait_samples)
        n = len(snap)
        if n == 0:
            return {"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}
        snap.sort()

        # Nearest-rank percentile — fine at this sample count.
        def _pct(p: float) -> float:
            idx = min(n - 1, max(0, int(round(p * (n - 1)))))
            return round(snap[idx], 2)

        return {
            "count": n,
            "p50_ms": _pct(0.50),
            "p95_ms": _pct(0.95),
            "p99_ms": _pct(0.99),
            "max_ms": round(snap[-1], 2),
            "mean_ms": round(sum(snap) / n, 2),
        }

    def stats(self) -> dict:
        with self._cond:
            base = {
                "service": self.service_key,
                "max_size": self.max_size,
                "in_use": self._in_use,
                "idle": self._idle.qsize(),
                "created_total": self._created_total,
                "reused_total": self._reused_total,
                "discarded_total": self._discarded_total,
            }
        # Wait-stats snapshot OUTSIDE the pool lock — its own lock guards
        # the sample deque, and the call would otherwise tie checkout
        # waiters up behind a sort.
        base["wait"] = self._wait_stats()
        return base


class _PoolBusy(Exception):
    """Raised when the pool is saturated and the wait deadline elapsed."""


_pools: dict[str, _Pool] = {}
_pools_lock = threading.Lock()


def _get_pool(service_key: str, max_size: int | None = None) -> _Pool:
    if max_size is None:
        max_size = _pool_max_size()
    with _pools_lock:
        pool = _pools.get(service_key)
        if pool is None:
            pool = _Pool(service_key, max_size=max_size)
            _pools[service_key] = pool
        return pool


@contextmanager
def checkout_connection(src: dict, max_wait: float = 10.0):
    """Yield a fully-configured DuckDB connection from the per-service pool.

    Falls back to the legacy always-fresh path when ``DUCKDB_CONNECTION_POOL``
    is disabled. Returns the connection to the pool on clean exit; discards
    it on any exception so a poisoned connection doesn't get reused.
    """
    if not _pool_enabled():
        from backend.core.duckdb import get_connection

        con = get_connection(source=src, read_only=True, max_wait=max_wait)
        try:
            yield con
        finally:
            try:
                con.close()
            except Exception:
                pass
        return

    service_key = src.get("name") or src.get("service_id") or "default"
    pool = _get_pool(service_key)
    con = pool.acquire(src, max_wait=max_wait)
    errored = False
    try:
        yield con
    except Exception:
        errored = True
        raise
    finally:
        pool.release(con, errored=errored)


def warm_pool_for_service(service_key: str, src: dict) -> None:
    """Warm the per-service pool's idle connections to the latest view.

    Called by writer-side cron jobs (sync, commit) after they mutate state
    that invalidates _view_cache. No-op if no pool exists yet (no readers
    have queried this service).
    """
    with _pools_lock:
        pool = _pools.get(service_key)
    if pool is None:
        return
    pool.warm_idle(src)


def get_all_stats() -> list[dict]:
    """Diagnostics: return current pool state for every service."""
    with _pools_lock:
        return [pool.stats() for pool in _pools.values()]


def shutdown_all() -> None:
    """Close every idle connection across every pool. Called on app shutdown
    so DuckDB releases its file handles cleanly."""
    with _pools_lock:
        pools = list(_pools.values())
        _pools.clear()
    for pool in pools:
        while True:
            try:
                con = pool._idle.get_nowait()
            except queue.Empty:
                break
            _forget_conn(con)
            try:
                con.close()
            except Exception:
                pass
