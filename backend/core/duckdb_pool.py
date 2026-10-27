"""Per-service DuckDB connection pool.

Caches read-only connections per service in a LIFO pool (bounded by ``max_size``,
default 12), re-validating view bindings via ``_view_cache`` + buffer mtime on
checkout. Opt-in via ``DUCKDB_CONNECTION_POOL`` (default on; ``"0"`` disables).

Notes:
  * File-backed safety is single-process only (ADR-18); durable serving uses
    independent in-memory DuckDB instances attached read-only to DuckLake.
  * Heavy routes (``is_heavy_route``) acquire a ``DUCKDB_POOL_HEAVY_MAX``
    semaphore slot (default ``max_size - 1``) so light requests never starve.
  * On checkin, errored or draining connections are discarded; optional temp
    table sweep (``DUCKDB_POOL_SWEEP``) drops leftover ``temp.main`` tables.
"""

from __future__ import annotations

import collections
import logging
import os
import queue
import threading
import time
from contextlib import contextmanager
from typing import Any

import duckdb

from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)

_TRUTHY = ("1", "true", "yes", "on")
_FALSY = ("0", "false", "no", "off")
_DEFAULT_HEAVY_ROUTE_SUFFIXES = (
    "/aggregates",
    "/bundle",
    "/network-health",
    "/insights",
    "/insights/cache-collapse-detail",
    "/slow-urls",
)
_WAIT_SAMPLES_MAX = 1024  # ~last 17 minutes at 1 req/s; ~3.5 minutes at 5 req/s


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, scale: float = 1.0) -> float:
    try:
        return max(0.0, float(os.getenv(name, str(default))) * scale)
    except (TypeError, ValueError):
        return default * scale


def _close_quietly(con: duckdb.DuckDBPyConnection, label: str) -> None:
    try:
        con.close()
    except Exception:
        if log_once_per(label, 60):
            logger.debug(label, exc_info=True)


def _pool_enabled() -> bool:
    return os.getenv("DUCKDB_CONNECTION_POOL", "1").lower() not in _FALSY


def _pool_max_size() -> int:
    return _env_int("DUCKDB_POOL_MAX_SIZE", 12)


def _pool_heavy_cap(max_size: int) -> int:
    """Max pool slots heavy requests may hold at once (clamped to ``[1, max_size]``)."""
    raw = os.getenv("DUCKDB_POOL_HEAVY_MAX")
    if raw:
        try:
            return min(max_size, max(1, int(raw)))
        except (TypeError, ValueError):
            pass
    return max(1, max_size - 1)


def is_heavy_route(route_template: str | None) -> bool:
    """True for routes that should take a heavy bulkhead slot."""
    if not route_template:
        return False
    raw = os.getenv("DUCKDB_POOL_HEAVY_ROUTES")
    suffixes = tuple(s.strip() for s in raw.split(",") if s.strip()) if raw else _DEFAULT_HEAVY_ROUTE_SUFFIXES
    return route_template.rstrip("/").endswith(suffixes)


def _slow_checkout_ms() -> float:
    """Checkouts that wait at least this long are logged (0 disables; default 500 ms)."""
    return _env_float("DUCKDB_POOL_SLOW_CHECKOUT_MS", 500.0)


def _pool_conn_memory_limit() -> str | None:
    """Optional per-pool-connection memory cap (``DUCKDB_POOL_CONN_MEMORY_LIMIT``)."""
    return os.getenv("DUCKDB_POOL_CONN_MEMORY_LIMIT") or None


def _pool_api_rebind_lock_timeout_s() -> float:
    """Per-service lake-view rebind lock timeout for API checkouts (ADR-03 §28, default 0.5s)."""
    return _env_float("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", 500.0, scale=0.001)


def _pool_warm_at_boot_enabled() -> bool:
    """Whether the scheduler pre-acquires pool connections at startup."""
    return os.getenv("DUCKDB_POOL_WARM_AT_BOOT", "0").lower() in _TRUTHY


def _pool_warm_at_boot_count() -> int:
    """How many idle pool conns to pre-acquire per service at startup (default 4)."""
    return _env_int("DUCKDB_POOL_WARM_AT_BOOT_COUNT", 4)


def _pool_sweep_enabled() -> bool:
    """Whether release() drops leftover ``TEMP`` tables on checkin."""
    return os.getenv("DUCKDB_POOL_SWEEP", "0").lower() in _TRUTHY


# Per-connection metadata keyed by id(con) since DuckDB connections are slotted C types.
_conn_state: dict[int, dict] = {}
_conn_state_lock = threading.Lock()


def _set_conn_state(con: duckdb.DuckDBPyConnection, **kv) -> None:
    with _conn_state_lock:
        _conn_state.setdefault(id(con), {}).update(kv)


def _get_conn_state(con: duckdb.DuckDBPyConnection, key: str, default=None):
    with _conn_state_lock:
        return _conn_state.get(id(con), {}).get(key, default)


def _forget_conn(con: duckdb.DuckDBPyConnection) -> None:
    with _conn_state_lock:
        _conn_state.pop(id(con), None)


def _safe_buffer_mtime(src: dict | None) -> float | tuple[float | None, ...] | None:
    """Return mtime of the service's buffer dir(s), or None if unavailable."""
    if src is None:
        return None
    try:
        from backend import config as svcconfig

        if svcconfig.is_durable_serving_mode(src):
            return None
        from backend.core.storage._core import _buffer_dir

        if src.get("name", "").endswith("::rum"):
            mtimes: list[float | None] = []
            for tbl in ("client_vitals", "client_errors"):
                try:
                    mtimes.append(os.path.getmtime(_buffer_dir(src, table_name=tbl)))
                except Exception:
                    mtimes.append(None)
                    if log_once_per("_safe_buffer_mtime: mtimes.append failed (ignored)", 60):
                        logger.debug("_safe_buffer_mtime: mtimes.append failed (ignored)", exc_info=True)
            return tuple(mtimes) if any(m is not None for m in mtimes) else None
        return os.path.getmtime(_buffer_dir(src))
    except Exception:
        return None


def _percentile_summary(samples: collections.deque[float], lock: threading.Lock) -> dict:
    """Nearest-rank percentile snapshot over a bounded sample deque."""
    with lock:
        snap = list(samples)
    n = len(snap)
    if n == 0:
        return {"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}
    snap.sort()
    pct = lambda p: round(snap[min(n - 1, max(0, int(round(p * (n - 1)))))], 2)  # noqa: E731
    return {
        "count": n,
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "max_ms": round(snap[-1], 2),
        "mean_ms": round(sum(snap) / n, 2),
    }


class _Pool:
    """Per-service pool. Not exposed directly — use ``checkout_connection``."""

    def __init__(self, service_key: str, max_size: int):
        self.service_key = service_key
        self.max_size = max_size
        self._idle: queue.LifoQueue = queue.LifoQueue(maxsize=max_size)
        self._lock = threading.RLock()
        self._in_use = 0  # checked out + idle in queue (bounded by max_size)
        self._cond = threading.Condition(self._lock)
        self._draining = False
        self._created_total = 0
        self._reused_total = 0
        self._discarded_total = 0
        self._saturated_rejects_total = 0  # SRE-12: user-facing saturation rejects (→ 503)
        self._drain_rejects_total = 0  # SRE-12: transient recycle-drain rejects
        self._last_warmed_at: str | None = None  # SRE-16: wall-clock of last completed view warm
        self._wait_samples: collections.deque[float] = collections.deque(maxlen=_WAIT_SAMPLES_MAX)
        self._wait_samples_lock = threading.Lock()
        self._rebind_wait_samples: collections.deque[float] = collections.deque(maxlen=_WAIT_SAMPLES_MAX)
        self._rebind_wait_samples_lock = threading.Lock()
        self.heavy_cap = _pool_heavy_cap(max_size)
        self._heavy_sem = threading.BoundedSemaphore(self.heavy_cap)
        self._heavy_in_use = 0
        self._heavy_lock = threading.Lock()
        self._heavy_wait_samples: collections.deque[float] = collections.deque(maxlen=_WAIT_SAMPLES_MAX)
        self._heavy_wait_samples_lock = threading.Lock()

    def acquire_heavy_slot(self, max_wait: float) -> None:
        """Take a heavy slot, waiting up to ``max_wait`` seconds; raises ``_PoolBusy``."""
        t0 = time.monotonic()
        if not self._heavy_sem.acquire(timeout=max_wait):
            with self._cond:
                self._saturated_rejects_total += 1
            raise _PoolBusy(f"heavy-request slots exhausted ({self.heavy_cap}) after {max_wait:.1f}s")
        with self._heavy_lock:
            self._heavy_in_use += 1
        with self._heavy_wait_samples_lock:
            self._heavy_wait_samples.append((time.monotonic() - t0) * 1000.0)

    def release_heavy_slot(self) -> None:
        with self._heavy_lock:
            self._heavy_in_use -= 1
        self._heavy_sem.release()

    def _record_wait_telemetry(self, wait_ms: float, outcome: str, waited: bool | None = None) -> None:
        try:
            from backend.core.request_telemetry import thread_wait_histogram

            attrs = {"service": self.service_key, "outcome": outcome}
            if waited is not None:
                attrs["waited"] = str(waited).lower()
            thread_wait_histogram().record(wait_ms, attrs)
        except Exception:
            if log_once_per("acquire: thread_wait_histogram failed (ignored)", 60):
                logger.debug("acquire: thread_wait_histogram failed (ignored)", exc_info=True)
        self._record_wait_sample(wait_ms)

    def acquire(
        self,
        src: dict,
        max_wait: float,
        *,
        skip_view_update: bool = False,
        reuse_only: bool = False,
    ) -> duckdb.DuckDBPyConnection:
        t_acquire_start = time.monotonic()
        deadline = t_acquire_start + max_wait
        reused_con: duckdb.DuckDBPyConnection | None = None
        waited = False
        with self._cond:
            while True:
                # Recycle drain: queue waiters (unless reuse_only) until end_drain notifies.
                if self._draining:
                    if reuse_only:
                        raise _PoolBusy(f"pool for {self.service_key} draining for recycle")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._drain_rejects_total += 1
                        raise _PoolBusy(f"pool for {self.service_key} draining for recycle")
                    self._cond.wait(timeout=remaining)
                    continue

                try:
                    reused_con = self._idle.get_nowait()
                    self._reused_total += 1
                    break
                except queue.Empty:
                    pass

                # Reuse-only (best-effort parallel): bail immediately if no idle conn is ready.
                if reuse_only:
                    raise _PoolBusy(f"pool for {self.service_key} has no idle connection (reuse_only)")

                if self._in_use < self.max_size:
                    self._in_use += 1
                    self._created_total += 1
                    break

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._record_wait_telemetry((time.monotonic() - t_acquire_start) * 1000.0, "timeout")
                    self._saturated_rejects_total += 1
                    raise _PoolBusy(f"pool for {self.service_key} saturated at {self.max_size}")
                waited = True
                self._cond.wait(timeout=remaining)

        self._record_wait_telemetry(
            (time.monotonic() - t_acquire_start) * 1000.0,
            "reused" if reused_con is not None else "created",
            waited=waited,
        )

        # Outside lock: view rebind / fresh build may take seconds on S3/DuckLake I/O.
        if reused_con is not None:
            from backend.core.storage._core import _ducklake_attach

            if not _ducklake_attach(reused_con, src, read_only=True):
                self._discard(reused_con)
                raise RuntimeError("Failed to re-attach DuckLake")
            return reused_con if skip_view_update else self._prepare_checkout(reused_con, src)

        try:
            from backend.core.duckdb import get_connection

            con = get_connection(source=src, read_only=True, max_wait=max_wait)
            _set_conn_state(con, service_key=self.service_key)
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
            self._stamp_fingerprint(con, src)
            return con
        except Exception:
            with self._cond:
                self._in_use -= 1
                self._cond.notify()
            raise

    def release(self, con: duckdb.DuckDBPyConnection, *, errored: bool = False) -> None:
        """Return a connection to the pool, or discard it if ``errored=True`` or draining."""
        if errored or self._draining:
            self._discard(con)
            return

        # Never DETACH lake here: file-backed connections share one DuckDB instance,
        # so detaching evicts lake from sibling connections mid-query (Trap #35).
        if _pool_sweep_enabled():
            try:
                rows = con.execute(
                    "SELECT table_name FROM duckdb_tables() "
                    "WHERE temporary = true AND table_name LIKE 't\\_%' ESCAPE '\\'"
                ).fetchall()
                for (name,) in rows:
                    try:
                        con.execute(f'DROP TABLE IF EXISTS "{name}"')
                    except Exception as e:
                        logger.warning("[pool] %s: temp sweep failed to drop %s: %s", self.service_key, name, e)
            except Exception as e:
                logger.warning("[pool] %s: temp sweep query failed: %s", self.service_key, e)
        with self._cond:
            try:
                self._idle.put_nowait(con)
                self._cond.notify()
                return
            except queue.Full:
                pass
        _close_quietly(con, "release: con.close failed (ignored)")
        with self._cond:
            self._in_use -= 1
            self._cond.notify()

    def _discard(self, con: duckdb.DuckDBPyConnection) -> None:
        _forget_conn(con)
        _close_quietly(con, "_discard: con.close failed (ignored)")
        with self._cond:
            self._in_use -= 1
            self._discarded_total += 1
            self._cond.notify()

    def _drain_idle_locked(self, label: str, decrement_in_use: bool = True) -> int:
        closed = 0
        while True:
            try:
                con = self._idle.get_nowait()
            except queue.Empty:
                break
            _forget_conn(con)
            _close_quietly(con, label)
            if decrement_in_use:
                self._in_use -= 1
            closed += 1
        return closed

    def begin_drain(self) -> None:
        """Enter draining mode for a DuckDB instance recycle (idempotent)."""
        with self._cond:
            self._draining = True
            self._drain_idle_locked("begin_drain: con.close failed (ignored)")
            self._cond.notify_all()

    def wait_drained(self, timeout: float) -> bool:
        """Block until ``_in_use == 0`` or ``timeout`` (seconds) elapses."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._in_use > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    def end_drain(self) -> None:
        """Leave draining mode; resume normal acquires (idempotent)."""
        with self._cond:
            self._draining = False
            self._cond.notify_all()

    def discard_idle(self) -> int:
        """Close every idle connection so the next checkout builds fresh (e.g. on credential rotation)."""
        with self._cond:
            closed = self._drain_idle_locked("discard_idle: con.close failed (ignored)")
            self._cond.notify_all()
        return closed

    @staticmethod
    def _strip_view_timing(fp: Any) -> Any:
        if isinstance(fp, tuple) and len(fp) >= 7 and isinstance(fp[4], (int, float)) and isinstance(fp[5], bool):
            return fp[:4] + fp[6:]
        return fp

    def _current_view_fp(self) -> Any:
        from backend.core.storage import _core as storage_view

        if self.service_key.endswith("::rum"):
            v = self._strip_view_timing(storage_view._view_cache.get(f"{self.service_key}::client_vitals"))
            e = self._strip_view_timing(storage_view._view_cache.get(f"{self.service_key}::client_errors"))
            return (v, e) if (v is not None or e is not None) else None
        return self._strip_view_timing(storage_view._view_cache.get(self.service_key))

    def _prepare_checkout(self, con: duckdb.DuckDBPyConnection, src: dict) -> duckdb.DuckDBPyConnection:
        """Re-validate ``_view_cache`` fingerprint + buffer mtime before handing ``con`` out."""
        try:
            from backend.core.storage import _core as storage_view

            current = self._current_view_fp()
            if (
                current is not None
                and current == _get_conn_state(con, "view_fingerprint")
                and _safe_buffer_mtime(src) == _get_conn_state(con, "buffer_mtime")
            ):
                return con
            t_rebind_start = time.monotonic()
            try:
                storage_view.update_storage_view(con, src, lock_timeout=_pool_api_rebind_lock_timeout_s())
            finally:
                self._record_rebind_wait_sample((time.monotonic() - t_rebind_start) * 1000.0)
            self._stamp_fingerprint(con, src)
            return con
        except Exception as e:
            logger.warning("[pool] %s: view refresh on checkout failed, discarding: %s", self.service_key, e)
            self._discard(con)
            raise

    def warm_idle(self, src: dict) -> None:
        """Rebind every idle connection to the latest cached view (drain-then-requeue on LIFO)."""
        from backend.core.storage import _core as storage_view

        drained: list[duckdb.DuckDBPyConnection] = []
        with self._cond:
            if self._draining:
                return
            while True:
                try:
                    drained.append(self._idle.get_nowait())
                except queue.Empty:
                    break
        if not drained:
            return

        for con in drained:
            try:
                if self.service_key.endswith("::rum"):
                    storage_view._try_fast_path_view(con, src, target_table="client_vitals")
                    storage_view._try_fast_path_view(con, src, target_table="client_errors")
                else:
                    storage_view._try_fast_path_view(con, src)
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
                    _close_quietly(con, "warm_idle: con.close failed (ignored)")
                    self._in_use -= 1
                    self._cond.notify()
        try:
            from backend.utils.date_utils import iso_z_now

            self._last_warmed_at = iso_z_now()
        except Exception:
            if log_once_per("warm_idle: iso_z_now failed (ignored)", 60):
                logger.debug("warm_idle: iso_z_now failed (ignored)", exc_info=True)

    def _stamp_fingerprint(self, con: duckdb.DuckDBPyConnection, src: dict | None = None) -> None:
        try:
            _set_conn_state(
                con,
                view_fingerprint=self._current_view_fp(),
                buffer_mtime=_safe_buffer_mtime(src) if src is not None else None,
            )
        except Exception:
            _set_conn_state(con, view_fingerprint=None, buffer_mtime=None)

    def _record_wait_sample(self, wait_ms: float) -> None:
        with self._wait_samples_lock:
            self._wait_samples.append(wait_ms)

    def _record_rebind_wait_sample(self, wait_ms: float) -> None:
        with self._rebind_wait_samples_lock:
            self._rebind_wait_samples.append(wait_ms)

    def _wait_stats(self) -> dict:
        return _percentile_summary(self._wait_samples, self._wait_samples_lock)

    def _rebind_wait_stats(self) -> dict:
        return _percentile_summary(self._rebind_wait_samples, self._rebind_wait_samples_lock)

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
                "saturated_rejects_total": self._saturated_rejects_total,
                "drain_rejects_total": self._drain_rejects_total,
                "last_warmed_at": self._last_warmed_at,
            }
        base["wait"] = self._wait_stats()
        base["rebind_wait"] = self._rebind_wait_stats()
        base["heavy"] = {
            "cap": self.heavy_cap,
            "in_use": self._heavy_in_use,
            "wait": _percentile_summary(self._heavy_wait_samples, self._heavy_wait_samples_lock),
        }
        return base


class _PoolBusy(Exception):
    """Raised when the pool is saturated and the wait deadline elapsed."""


_pools: dict[str, _Pool] = {}
_retired_pools: dict[str, _Pool] = {}
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
def checkout_connection(
    src: dict,
    max_wait: float = 10.0,
    *,
    skip_view_update: bool = False,
    reuse_only: bool = False,
    heavy: bool = False,
    label: str | None = None,
):
    """Yield an instrumented DuckDB connection from the per-service pool (or fresh if disabled)."""
    if not _pool_enabled():
        from backend.core.duckdb import get_connection

        if reuse_only:
            raise _PoolBusy("pool disabled, no idle connection to reuse (reuse_only)")
        raw_con = get_connection(source=src, read_only=True, max_wait=max_wait)
        wrapped = _instrument(raw_con, service_key=src.get("name") or src.get("service_id"))
        try:
            yield wrapped
        finally:
            _close_quietly(raw_con, "checkout_connection: raw_con.close failed (ignored)")
        return

    service_key = src.get("name") or src.get("service_id") or "default"
    pool = _get_pool(service_key)
    t_start = time.monotonic()
    heavy_deadline = t_start + max_wait
    if heavy:
        pool.acquire_heavy_slot(max_wait)
    t_slot = time.monotonic()
    try:
        raw_con = pool.acquire(
            src,
            max_wait=max(0.0, heavy_deadline - time.monotonic()) if heavy else max_wait,
            skip_view_update=skip_view_update,
            reuse_only=reuse_only,
        )
    except BaseException:
        if heavy:
            pool.release_heavy_slot()
        raise
    t_done = time.monotonic()
    threshold = _slow_checkout_ms()
    if threshold and (t_done - t_start) * 1000.0 >= threshold:
        logger.warning(
            "[pool] slow checkout: service=%s route=%s heavy=%s slot_wait_ms=%d pool_wait_ms=%d open=%d idle=%d heavy_in_use=%d/%d",
            service_key,
            label,
            heavy,
            (t_slot - t_start) * 1000,
            (t_done - t_slot) * 1000,
            pool._in_use,
            pool._idle.qsize(),
            pool._heavy_in_use,
            pool.heavy_cap,
        )
    wrapped = _instrument(raw_con, service_key=service_key)
    errored = False
    try:
        yield wrapped
    except Exception:
        errored = True
        raise
    finally:
        try:
            pool.release(raw_con, errored=errored)
        finally:
            if heavy:
                pool.release_heavy_slot()


def _instrument(raw_con, *, service_key: str | None):
    """Wrap a raw DuckDB connection in the live-query monitor proxy (fail-open)."""
    try:
        from backend.core.request_telemetry import InstrumentedDuckDBConnection

        return InstrumentedDuckDBConnection(raw_con, service_id=service_key)
    except Exception:
        if log_once_per("DuckDB live-instrumentation skipped", 60):
            logger.debug("DuckDB live-instrumentation skipped", exc_info=True)
        return raw_con


def warm_pool_for_service(service_key: str, src: dict) -> None:
    """Warm the per-service pool's idle connections to the latest view (no-op if no pool)."""
    with _pools_lock:
        pool = _pools.get(service_key)
    if pool is not None:
        pool.warm_idle(src)


def warm_pool_at_startup(service_key: str, src: dict, count: int = 4) -> int:
    """Pre-acquire up to ``count`` pool connections at boot and return them to idle."""
    pool = _get_pool(service_key)
    n = min(count, pool.max_size)
    built: list[duckdb.DuckDBPyConnection] = []
    for i in range(n):
        try:
            built.append(pool.acquire(src, max_wait=30.0))
        except Exception as e:
            logger.warning("[pool] %s: warm_pool_at_startup conn %d/%d failed: %s", service_key, i + 1, n, e)
            break
    for con in built:
        pool.release(con)
    return len(built)


def get_all_stats() -> list[dict]:
    """Diagnostics: return current pool state for every service."""
    with _pools_lock:
        return [pool.stats() for pool in _pools.values()]


def begin_drain_pools(service_keys) -> dict[str, _Pool]:
    """Blue/Green swap: mark pools for ``service_keys`` draining and pop from ``_pools``."""
    retired = {}
    with _pools_lock:
        for k in service_keys:
            pool = _pools.pop(k, None)
            if pool is not None:
                pool.begin_drain()
                retired[k] = _retired_pools[k] = pool
    return retired


def wait_pools_drained(pools_or_keys, timeout: float) -> bool:
    """Wait (up to ``timeout`` total) for every drained pool to reach in_use==0."""
    if isinstance(pools_or_keys, dict):
        targets = list(pools_or_keys.values())
    elif isinstance(pools_or_keys, list) and all(isinstance(p, _Pool) for p in pools_or_keys):
        targets = pools_or_keys
    else:
        with _pools_lock:
            targets = [p for k in pools_or_keys if (p := (_retired_pools.get(k) or _pools.get(k))) is not None]

    deadline = time.monotonic() + timeout
    ok = True
    for pool in targets:
        if not pool.wait_drained(max(0.0, deadline - time.monotonic())):
            ok = False
    return ok


def end_drain_pools(pools_or_keys) -> None:
    """Take every existing pool out of draining mode (no-op for swapped pools)."""
    if isinstance(pools_or_keys, dict):
        with _pools_lock:
            for k in pools_or_keys:
                _retired_pools.pop(k, None)
        targets = list(pools_or_keys.values())
    elif isinstance(pools_or_keys, list) and all(isinstance(p, _Pool) for p in pools_or_keys):
        with _pools_lock:
            for k, p in list(_retired_pools.items()):
                if p in pools_or_keys:
                    _retired_pools.pop(k, None)
        targets = pools_or_keys
    else:
        with _pools_lock:
            targets = [
                _p
                for k in pools_or_keys
                if isinstance(k, str) and (_p := (_retired_pools.pop(k, None) or _pools.get(k))) is not None
            ]
    for pool in targets:
        pool.end_drain()


def reset_pool_for_service(service_key: str) -> int:
    """Close all idle pooled connections for one service (e.g. on credential change)."""
    with _pools_lock:
        pool = _pools.get(service_key)
    return 0 if pool is None else pool.discard_idle()


def shutdown_all() -> None:
    """Close every idle connection across every pool on app shutdown."""
    with _pools_lock:
        pools = list(_pools.values())
        _pools.clear()
    for pool in pools:
        pool._drain_idle_locked("shutdown_all: con.close failed (ignored)", decrement_in_use=False)


close_all_pools = shutdown_all
close_all_service_pools = close_all_pools


def get_pool_status() -> dict[str, Any]:
    """Return status and connection metrics across all active and retired pools."""
    with _pools_lock:
        pools_info = {
            k: {
                "in_use": p._in_use,
                "idle": p._idle.qsize(),
                "max_size": p.max_size,
                "created_total": p._created_total,
                "reused_total": p._reused_total,
                "draining": p._draining,
            }
            for k, p in _pools.items()
        }
        retired_info = {
            k: {"in_use": p._in_use, "idle": p._idle.qsize(), "draining": p._draining}
            for k, p in _retired_pools.items()
        }
    return {"pools": pools_info, "retired_pools": retired_info}


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("core.duckdb_pool._conn_state", _conn_state)
