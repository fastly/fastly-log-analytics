"""Periodic in-process DuckDB instance recycle — bounds the object-cache leak.

DuckDB's ``enable_object_cache`` caches parquet metadata keyed by filename and
never evicts entries for deleted files. Under continuous file churn (sync writes
new buffer parquet every tick, commit deletes them; compaction rewrites committed
files) the cache grows unbounded and the container OOMs (~175 MB/min on prod).
The cache can't be cleared in place (no ``pragma_clear_cache`` in DuckDB 1.5.3),
so we periodically destroy + rebuild the per-file DuckDB instance: when the LAST
connection to a db file closes, DuckDB frees the instance and the cache with it
(confirmed on the prod container — RSS reclaimed). ``object_cache`` stays ON for
its query-latency benefit; it simply re-warms lazily after each recycle.

Mechanism (all seams live in [duckdb.py](duckdb.py) + [duckdb_pool.py](duckdb_pool.py)):
  1. acquire each owning service's iceberg rebind/write lock (sorted, timeout) so
     no writer is mid-catalog-op when we destroy the instance,
  2. raise a fail-open barrier so new ``duckdb.connect`` calls for the file pause,
  3. drain the connection pool(s) to ``in_use == 0`` and close idle conns,
  4. ``gc.collect()`` then wait for the weakref liveness set to reach 0 (the
     authoritative "instance has no connections" signal),
  5. always (``finally``) end the drain, clear the barrier, release the locks.

Safe by construction: the barrier is fail-open (a connection open never waits
longer than its cap), every wait is timeout-bounded, and any timeout aborts the
cycle cleanly (a no-op that leaves everything running; the next tick retries).
Disabled by default (``DUCKDB_RECYCLE_INTERVAL_MIN=0``); enabled in prod via
docker-compose env. See [[backend-oom-restart-loop]].
"""

from __future__ import annotations

import gc
import logging
import os
import time

from backend import config as svcconfig
from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool

logger = logging.getLogger(__name__)


# ── Config knobs (DUCKDB_ prefix; risky → OFF, matching duckdb_pool.py) ───────


def recycle_interval_min() -> float:
    """Interval (minutes) between recycles. 0 = disabled (the scheduler then
    never registers the job). Default 0."""
    raw = os.getenv("DUCKDB_RECYCLE_INTERVAL_MIN", "0")
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.0


def _recycle_rss_threshold_bytes() -> int:
    """Only recycle when current RSS exceeds this (MB → bytes); 0 = always
    recycle on the interval. Lets a quiet process skip needless cache drops."""
    raw = os.getenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "0")
    try:
        return max(0, int(raw)) * 1024 * 1024
    except (TypeError, ValueError):
        return 0


def _recycle_drain_timeout_s() -> float:
    raw = os.getenv("DUCKDB_RECYCLE_DRAIN_TIMEOUT_MS", "5000")
    try:
        return max(0.1, float(raw) / 1000.0)
    except (TypeError, ValueError):
        return 5.0


def _recycle_grace_s() -> float:
    raw = os.getenv("DUCKDB_RECYCLE_GRACE_MS", "3000")
    try:
        return max(0.0, float(raw) / 1000.0)
    except (TypeError, ValueError):
        return 3.0


def _recycle_lock_timeout_s() -> float:
    raw = os.getenv("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "2000")
    try:
        return max(0.0, float(raw) / 1000.0)
    except (TypeError, ValueError):
        return 2.0


# ── Recycle ──────────────────────────────────────────────────────────────────


def _sources_by_db_path() -> dict[str, list[dict]]:
    """Group every configured service's source dict by its abspath DuckDB file.

    Services sharing a db file share the DuckDB instance + its object cache, so
    they must be drained together for the instance to be destroyed.
    """
    groups: dict[str, list[dict]] = {}
    for cfg in svcconfig.list_configs():
        try:
            src = _db._source_from_config(cfg)
        except Exception:
            continue
        groups.setdefault(_db.db_path_for_source(src), []).append(src)

        # If RUM is enabled for the service, register the isolated RUM source for recycling too
        rum_cfg = cfg.get("rum") or {}
        if rum_cfg.get("enabled") or cfg.get("rum_enabled"):
            try:
                from backend.core.duckdb import rum_source_for

                rum_src = rum_source_for(src)
                groups.setdefault(_db.db_path_for_source(rum_src), []).append(rum_src)
            except Exception:
                pass
    return groups


def _service_key(src: dict) -> str:
    return src.get("name") or src.get("service_id") or "default"


def _lock_key(src: dict) -> str:
    # Matches iceberg.view._get_service_lock keying (source.get("name", "default")).
    return src.get("name", "default")


def _recycle_db_path(db_path: str, sources: list[dict]) -> dict:
    """Recycle the DuckDB instance for one db file. Returns a status dict."""
    from backend.core.iceberg.view import _get_service_lock

    drain_timeout = _recycle_drain_timeout_s()
    grace = _recycle_grace_s()
    lock_timeout = _recycle_lock_timeout_s()
    service_keys = [_service_key(s) for s in sources]

    # Acquire write/rebind locks in a deterministic global order (sorted lock
    # keys) so recycle can never deadlock against a writer holding one of them.
    lock_keys = sorted({_lock_key(s) for s in sources})
    acquired: list = []
    rss_before = _db.current_rss_bytes()
    try:
        for lk in lock_keys:
            lock = _get_service_lock(lk)
            if not lock.acquire(timeout=lock_timeout):
                logger.info(
                    "[recycle] %s: skip — write lock %r busy (a sync/commit is in flight)",
                    db_path,
                    lk,
                )
                return {"db_path": db_path, "status": "skipped_locked", "lock": lk}
            acquired.append(lock)

        # Locks held → raise the barrier so no NEW connection opens for this file.
        _db.set_recycle_barrier(db_path, True)
        try:
            _pool.begin_drain_pools(service_keys)
            drained = _pool.wait_pools_drained(service_keys, drain_timeout)

            # gc so closed-but-not-yet-collected conn wrappers leave the WeakSet.
            gc.collect()
            deadline = time.monotonic() + grace
            while _db.live_connection_count(db_path) > 0 and time.monotonic() < deadline:
                time.sleep(0.05)
                gc.collect()
            live = _db.live_connection_count(db_path)
        finally:
            _pool.end_drain_pools(service_keys)
            _db.set_recycle_barrier(db_path, False)
    finally:
        for lock in reversed(acquired):
            try:
                lock.release()
            except RuntimeError:
                pass

    rss_after = _db.current_rss_bytes()
    freed = (rss_before - rss_after) if (rss_before is not None and rss_after is not None) else None
    status = "recycled" if (drained and live == 0) else "incomplete"
    result = {
        "db_path": db_path,
        "status": status,
        "drained": drained,
        "live_after": live,
        "rss_before": rss_before,
        "rss_after": rss_after,
        "freed_bytes": freed,
    }
    freed_mb = f"{freed / 1e6:.0f}MB" if freed is not None else "n/a"
    logger.info(
        "[recycle] %s: %s (drained=%s live_after=%d freed=%s)",
        db_path,
        status,
        drained,
        live,
        freed_mb,
    )
    return result


def recycle_once(reason: str = "interval") -> str:
    """Recycle every distinct DuckDB instance once. Returns a one-line detail.

    Never raises — each db file is recycled independently and abort-safe.
    """
    groups = _sources_by_db_path()
    if not groups:
        return "no services configured"
    results = []
    for db_path, sources in groups.items():
        try:
            results.append(_recycle_db_path(db_path, sources))
        except Exception as e:  # defensive — a recycle must never crash the job
            logger.warning("[recycle] %s: errored: %s", db_path, e, exc_info=True)
            results.append({"db_path": db_path, "status": "error", "error": str(e)})
    recycled = sum(1 for r in results if r.get("status") == "recycled")
    freed = sum(r.get("freed_bytes") or 0 for r in results if (r.get("freed_bytes") or 0) > 0)
    return f"{reason}: recycled {recycled}/{len(results)} instance(s), freed ~{freed / 1e6:.0f}MB"


# The ``@global_job``-decorated scheduler entry (``run_duckdb_recycle``) lives in
# backend/cron/jobs/duckdb_recycle.py — it imports ``recycle_once`` +
# ``_recycle_rss_threshold_bytes`` from here. Keeping the decorator in the cron
# layer is what keeps this core module free of a ``backend.cron`` import (the
# "Core does not depend on routers" contract: core → cron → routers).
