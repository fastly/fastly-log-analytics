"""Periodic DuckDB connection-pool recycle and RSS memory guard cron job."""

from __future__ import annotations

import gc
import logging
import os
import signal
import sys as _sys
import time

from backend import config as svcconfig
from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool
from backend.cron.scheduler import global_job

logger = logging.getLogger("backend.scheduler")


def _env_mb_bytes(name: str) -> int:
    try:
        return max(0, int(os.getenv(name, "0"))) * 1024 * 1024
    except (TypeError, ValueError):
        return 0


def _env_ms_to_s(name: str, default_ms: str, min_s: float, fallback_s: float) -> float:
    try:
        return max(min_s, float(os.getenv(name, default_ms)) / 1000.0)
    except (TypeError, ValueError):
        return fallback_s


def recycle_interval_min() -> float:
    try:
        return max(0.0, float(os.getenv("DUCKDB_RECYCLE_INTERVAL_MIN", "0")))
    except (TypeError, ValueError):
        return 0.0


def _recycle_rss_threshold_bytes() -> int:
    return _env_mb_bytes("DUCKDB_RECYCLE_RSS_THRESHOLD_MB")


def _recycle_drain_timeout_s() -> float:
    return _env_ms_to_s("DUCKDB_RECYCLE_DRAIN_TIMEOUT_MS", "5000", 0.1, 5.0)


def _recycle_grace_s() -> float:
    return _env_ms_to_s("DUCKDB_RECYCLE_GRACE_MS", "3000", 0.0, 3.0)


def _recycle_lock_timeout_s() -> float:
    return _env_ms_to_s("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "2000", 0.0, 2.0)


def _sources_by_db_path() -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for cfg in svcconfig.list_configs():
        try:
            src = _db._source_from_config(cfg)
        except Exception:
            continue
        groups.setdefault(_db.db_path_for_source(src), []).append(src)
        if svcconfig.is_rum_enabled(cfg):
            try:
                from backend.core.duckdb import rum_source_for

                rum_src = rum_source_for(src)
                groups.setdefault(_db.db_path_for_source(rum_src), []).append(rum_src)
            except Exception:
                logger.debug("_sources_by_db_path: groups.setdefault failed (ignored)", exc_info=True)
    return groups


def _service_key(src: dict) -> str:
    return src.get("name") or src.get("service_id") or "default"


def _lock_key(src: dict) -> str:
    return src.get("name", "default")


def _trim_memory() -> None:
    gc.collect()
    try:
        import ctypes

        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        logger.debug("_trim_memory: ctypes.CDLL failed (ignored)", exc_info=True)


def _recycle_db_path(db_path: str, sources: list[dict]) -> dict:
    from backend.core.storage._core import _get_service_lock

    drain_timeout, grace, lock_timeout = _recycle_drain_timeout_s(), _recycle_grace_s(), _recycle_lock_timeout_s()
    service_keys = [_service_key(s) for s in sources]
    lock_keys = sorted({_lock_key(s) for s in sources})
    acquired: list = []
    rss_before = _db.current_rss_bytes()
    try:
        for lk in lock_keys:
            lock = _get_service_lock(lk)
            if not lock.acquire(timeout=lock_timeout):
                logger.info("[recycle] %s: skip — write lock %r busy (a sync/commit is in flight)", db_path, lk)
                return {"db_path": db_path, "status": "skipped_locked", "lock": lk}
            acquired.append(lock)
        _db.set_recycle_barrier(db_path, True)
        retired_pools = {}
        try:
            retired_pools = _pool.begin_drain_pools(service_keys)
            drained = _pool.wait_pools_drained(retired_pools, drain_timeout)
            _trim_memory()
            deadline = time.monotonic() + grace
            while _db.live_connection_count(db_path) > 0 and time.monotonic() < deadline:
                time.sleep(0.05)
                _trim_memory()
            live = _db.live_connection_count(db_path)
        finally:
            _pool.end_drain_pools(retired_pools or service_keys)
            _db.set_recycle_barrier(db_path, False)
    finally:
        for lock in reversed(acquired):
            try:
                lock.release()
            except RuntimeError:
                pass
    _trim_memory()
    rss_after = _db.current_rss_bytes()
    freed = (rss_before - rss_after) if (rss_before is not None and rss_after is not None) else None
    status = "recycled" if (drained and live == 0) else "incomplete"
    freed_mb = f"{freed / 1e6:.0f}MB" if freed is not None else "n/a"
    logger.info("[recycle] %s: %s (drained=%s live_after=%d freed=%s)", db_path, status, drained, live, freed_mb)
    return {
        "db_path": db_path,
        "status": status,
        "drained": drained,
        "live_after": live,
        "rss_before": rss_before,
        "rss_after": rss_after,
        "freed_bytes": freed,
    }


def recycle_once(reason: str = "interval") -> str:
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
    incomplete = sum(1 for r in results if r.get("status") in ("incomplete", "skipped_locked", "error"))
    extra = f" (incomplete: {incomplete})" if incomplete else ""
    return f"{reason}: recycled {recycled}/{len(results)} instance(s), freed ~{freed / 1e6:.0f}MB{extra}"


def graceful_restart_rss_threshold_bytes() -> int:
    return _env_mb_bytes("BACKEND_GRACEFUL_RESTART_RSS_MB")


def maybe_graceful_restart() -> bool:
    threshold = graceful_restart_rss_threshold_bytes()
    if threshold <= 0:
        return False
    try:
        from backend.core.duckdb import current_rss_bytes

        rss = current_rss_bytes()
    except Exception:
        return False
    if rss is None or rss < threshold:
        return False
    logger.warning(
        "\U0001f501 [memguard] RSS %.0fMB >= %.0fMB restart threshold — sending "
        "SIGTERM for a graceful restart (uvicorn drains + exits 0; docker "
        "restart:unless-stopped brings up a fresh process). STOPGAP for the "
        "unresolved memory growth; see backend-oom-restart-loop.",
        rss / 1e6,
        threshold / 1e6,
    )
    os.kill(os.getpid(), signal.SIGTERM)
    return True


def _maybe_adjust_recycle_schedule(*, expedite: bool) -> None:
    try:
        from backend.cron.scheduler import get_scheduler

        job = get_scheduler().get_job("duckdb_recycle")
        if not job:
            return
        baseline_mins = max(1.0, recycle_interval_min() or 60.0)
        target_mins = min(2.0, baseline_mins) if expedite else baseline_mins
        current_interval_s = getattr(
            getattr(getattr(job, "trigger", None), "interval", None), "total_seconds", lambda: 0
        )()
        if int(current_interval_s) != int(target_mins * 60):
            job.reschedule("interval", minutes=target_mins)
            logger.info(
                "⚡ [duckdb_recycle] Dynamically adjusted recycle interval to %.1fm (expedite=%s)",
                target_mins,
                expedite,
            )
    except Exception as e:
        logger.debug("[duckdb_recycle] Could not adjust recycle schedule: %s", e)


@global_job("duckdb_recycle", color="35", tag="recycle", label="DuckDB recycle")
def run_duckdb_recycle() -> str:
    if maybe_graceful_restart():
        return "graceful restart triggered (RSS over restart threshold)"
    threshold = _recycle_rss_threshold_bytes()
    rss = _db.current_rss_bytes()
    if threshold > 0 and rss is not None and rss < threshold:
        if rss >= int(threshold * 0.8):
            _maybe_adjust_recycle_schedule(expedite=True)
        elif rss < int(threshold * 0.5):
            _maybe_adjust_recycle_schedule(expedite=False)
        return f"skipped: RSS {rss / (1024 * 1024):.0f}MB < threshold {threshold / (1024 * 1024):.0f}MB"
    res = recycle_once(reason="interval")
    rss_after = _db.current_rss_bytes()
    if "incomplete" in res:
        _maybe_adjust_recycle_schedule(expedite=True)
    elif threshold > 0 and rss_after is not None:
        if rss_after >= int(threshold * 0.8):
            _maybe_adjust_recycle_schedule(expedite=True)
        elif rss_after < int(threshold * 0.5):
            _maybe_adjust_recycle_schedule(expedite=False)
    elif threshold == 0:
        _maybe_adjust_recycle_schedule(expedite=False)
    return res


_self_mod = _sys.modules[__name__]
_sys.modules["backend.core.duckdb_recycle"] = _self_mod
if (_core_pkg := _sys.modules.get("backend.core")) is not None:
    _core_pkg.duckdb_recycle = _self_mod  # type: ignore[attr-defined]
