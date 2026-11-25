"""Admin status helpers and SSE publishers.

Holds the in-process / Valkey sync-status publishers, the ingest-ledger
summary, list, purge and retry helpers, the in-process APScheduler schedule
and active-run snapshots, and the cached status payload served at
``/api/admin/celery/status`` and on the ``celery-status`` SSE channel. Ingest
runs in ``backend.high_scale.worker``; the module and route names predate it.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from typing import Any

from backend.config import SSE_BACKPLANE
from backend.utils.bounded_cache import CacheRegistry, SWRTTLCache

logger = logging.getLogger(__name__)


# ── In-process pub/sub primitive & sync-status publisher ─────────────────────

REPLAY_BUFFER_SIZE = 60


class _InProcessPublisher:
    """Per-``service_id`` fan-out of dict payloads to async subscribers."""

    def __init__(self, replay_size: int = REPLAY_BUFFER_SIZE) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()
        self._replay: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=replay_size))

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind the publisher to the FastAPI asyncio loop.

        Must be called from inside a running event loop (typically the
        FastAPI ``lifespan`` startup phase). Cross-thread ``publish()`` calls
        before this binding are silently dropped.
        """
        self._loop = loop

    def publish(self, service_id: str, payload: dict) -> None:
        """Fan ``payload`` out to every subscriber for ``service_id``.

        Safe to call from any thread (APScheduler workers, threads spawned by
        the cron, etc.). Returns immediately; queue inserts happen on the
        asyncio loop.
        """
        with self._lock:
            self._replay[service_id].append(payload)
            queues = list(self._subscribers.get(service_id, ()))
        loop = self._loop
        if loop is None:
            return
        for q in queues:
            try:
                loop.call_soon_threadsafe(self._enqueue, q, payload)
            except RuntimeError:
                return

    @staticmethod
    def _enqueue(q: asyncio.Queue, payload: dict) -> None:
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    async def subscribe(self, service_id: str) -> AsyncIterator[dict]:
        """Async iterator that yields each published payload for ``service_id``
        until the caller stops iterating (e.g. SSE client disconnects → the
        endpoint generator is closed)."""
        q: asyncio.Queue = asyncio.Queue(maxsize=4)
        with self._lock:
            replay = list(self._replay.get(service_id, ()))
            self._subscribers[service_id].add(q)
        try:
            for item in replay:
                yield item
            while True:
                yield await q.get()
        finally:
            with self._lock:
                subs = self._subscribers.get(service_id)
                if subs is not None:
                    subs.discard(q)
                    if not subs:
                        self._subscribers.pop(service_id, None)

    def subscriber_count(self, service_id: str) -> int:
        """Inspection helper for tests / debug surfaces."""
        with self._lock:
            return len(self._subscribers.get(service_id, ()))


class SyncStatusPublisher(_InProcessPublisher):
    def __init__(self) -> None:
        super().__init__(replay_size=0)


publisher: Any
if SSE_BACKPLANE == "valkey":
    from backend.utils.telemetry import ValkeyPublisher

    publisher = ValkeyPublisher()
else:
    publisher = SyncStatusPublisher()


# ── APScheduler/ledger status snapshot & sampler ──


# Admin pages poll status every few seconds; the ledger totals are the slow part
# (one metadata scan per service) and move slowly, so reuse them with stale-while-revalidate.
_LEDGER_SUMMARY_CACHE: SWRTTLCache = SWRTTLCache(maxsize=1, ttl_seconds=30.0, stale_ttl_seconds=300.0)
_LEDGER_SUMMARY_LOCK = threading.Lock()
_LEDGER_BG_GUARD = threading.Lock()
_ledger_bg_refreshing = False
CacheRegistry.register("celery_app._LEDGER_SUMMARY_CACHE", _LEDGER_SUMMARY_CACHE)


def ingest_ledger_summary() -> dict[str, int]:
    """Aggregate ingest_ledger status counts across all configured services."""
    global _ledger_bg_refreshing
    cached: dict[str, int] | None = _LEDGER_SUMMARY_CACHE.get("ledger")
    if cached is not None:
        return dict(cached)

    stale: dict[str, int] | None = (
        _LEDGER_SUMMARY_CACHE.get_stale("ledger") if isinstance(_LEDGER_SUMMARY_CACHE, SWRTTLCache) else None
    )
    if stale is not None:
        with _LEDGER_BG_GUARD:
            should_spawn = not _ledger_bg_refreshing
            if should_spawn:
                _ledger_bg_refreshing = True
        if should_spawn:

            def _refresh_ledger() -> None:
                global _ledger_bg_refreshing
                try:
                    with _LEDGER_SUMMARY_LOCK:
                        fresh = _read_ingest_ledger_summary()
                        _LEDGER_SUMMARY_CACHE["ledger"] = dict(fresh)
                except Exception as e:
                    logger.debug("[admin_status] background ledger refresh failed: %s", e)
                finally:
                    try:
                        from backend.core.metadata import pg_pool

                        pg_pool.release_thread_connection()
                    except Exception:
                        pass
                    with _LEDGER_BG_GUARD:
                        _ledger_bg_refreshing = False

            threading.Thread(target=_refresh_ledger, name="celery-ledger-swr", daemon=True).start()
        return dict(stale)

    with _LEDGER_SUMMARY_LOCK:
        cached = _LEDGER_SUMMARY_CACHE.get("ledger")
        if cached is not None:
            return dict(cached)
        ledger = _read_ingest_ledger_summary()
        _LEDGER_SUMMARY_CACHE["ledger"] = dict(ledger)
        return dict(ledger)


def prewarm_celery_status() -> None:
    """Best-effort startup pre-warm of the ingest-ledger summary."""
    try:
        ingest_ledger_summary()
    except Exception:
        logger.debug("[admin_status] celery status pre-warm skipped", exc_info=True)
    finally:
        try:
            from backend.core.metadata import pg_pool

            pg_pool.release_thread_connection()
        except Exception:
            pass


def _read_ingest_ledger_summary() -> dict[str, int]:
    ledger: dict[str, int] = {}
    try:
        from backend.config import list_configs
        from backend.core.metadata import pg_pool
        from backend.core.metadata.base import get_con

        sids: list[str] = []
        for svc in list_configs():
            sid = svc.get("service_id") or svc.get("name")
            if sid and sid not in sids:
                sids.append(str(sid))
        if not sids:
            return ledger

        if pg_pool.is_postgres() and len(sids) > 1:
            con = get_con(sids[0])
            cur = con.cursor()
            placeholders = ", ".join("?" for _ in sids)
            cur.execute(
                f"SELECT status, count(*) FROM ingest_ledger WHERE service_id IN ({placeholders}) GROUP BY status",
                tuple(sids),
            )
            for status, count in cur.fetchall():
                ledger[status] = ledger.get(status, 0) + int(count)
            return ledger

        for sid in sids:
            try:
                con = get_con(sid)
                cur = con.cursor()
                cur.execute(
                    "SELECT status, count(*) FROM ingest_ledger WHERE service_id = ? GROUP BY status",
                    (sid,),
                )
                for status, count in cur.fetchall():
                    ledger[status] = ledger.get(status, 0) + int(count)
            except Exception:
                continue
    except Exception as e:
        logger.debug("[admin_status] ledger summary failed: %s", e)
    return ledger


def inprocess_schedule_entries() -> list[dict[str, Any]]:
    """List registered in-process APScheduler jobs as {name, trigger, next_run_at} dicts."""
    entries: list[dict[str, Any]] = []
    try:
        import importlib

        sched = importlib.import_module("backend.cron.scheduler").get_scheduler()
        inner_sched = getattr(sched, "_sched", None) or getattr(sched, "scheduler", None)
        for job in inner_sched.get_jobs() if inner_sched is not None else []:
            next_run = getattr(job, "next_run_time", None)
            next_ts: float | None = None
            if next_run is not None:
                try:
                    next_ts = float(next_run.timestamp())
                except Exception:
                    next_ts = None
            entries.append(
                {
                    "name": str(job.id),
                    "trigger": str(job.trigger),
                    "next_run_at": next_ts,
                }
            )
        entries.sort(key=lambda item: item["name"])
    except Exception as e:
        logger.debug("[admin_status] inprocess_schedule_entries failed: %s", e)
    return entries


def inprocess_active_runs_entries() -> list[dict[str, Any]]:
    """Return currently executing in-process cron runs from cron_progress."""
    out: list[dict[str, Any]] = []
    try:
        from backend.cron.scheduler import list_active_runs

        for item in list_active_runs():
            run_id = item.get("run_id")
            if run_id is None:
                continue
            started_at = item.get("started_at")
            out.append(
                {
                    "run_id": int(run_id),
                    "service_id": str(item["service_id"]) if item.get("service_id") else None,
                    "task": str(item["task"]) if item.get("task") else None,
                    "started_at": float(started_at) if isinstance(started_at, (int, float)) else None,
                }
            )
    except Exception as e:
        logger.debug("[admin_status] inprocess_active_runs_entries failed: %s", e)
    return out


def get_celery_status() -> dict[str, Any]:
    """Full status payload for the admin endpoint / SSE feeder."""
    return {
        "ledger": ingest_ledger_summary(),
        "inprocess_schedules": inprocess_schedule_entries(),
        "inprocess_active_runs": inprocess_active_runs_entries(),
    }


_LEDGER_PROBLEM_STATUSES = ("dead_letter", "quarantined")
_LEDGER_VALID_STATUSES = ("discovered", "claimed", "committed", "quarantined", "dead_letter")


def _is_rum_object_key(object_key: str) -> bool:
    return object_key.startswith("raw/rum/") or "/raw/rum/" in object_key


def _resolve_ledger_service_ids(service_id: str | None = None) -> list[str]:
    if service_id:
        return [service_id]
    from backend.config import list_configs

    seen: list[str] = []
    for svc in list_configs():
        sid = svc.get("service_id") or svc.get("name")
        if sid and sid not in seen:
            seen.append(sid)
    return seen


def list_ingest_ledger_entries(
    status: str | None = "problem",
    service_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List rows from ``ingest_ledger`` across configured services (or a single
    service), defaulting to problem rows (``dead_letter`` + ``quarantined``)."""
    from backend.core.metadata.base import get_con

    norm_status = (status or "problem").strip().lower()
    entries: list[dict[str, Any]] = []
    total = 0
    seen_keys: set[tuple[str, str]] = set()
    per_service_limit = max(1, int(offset) + int(limit))

    for sid in _resolve_ledger_service_ids(service_id):
        try:
            con = get_con(sid)
            cur = con.cursor()
            if norm_status == "all":
                where_sql = "WHERE service_id = ?"
                params: tuple[Any, ...] = (sid,)
            elif norm_status in _LEDGER_VALID_STATUSES:
                where_sql = "WHERE service_id = ? AND status = ?"
                params = (sid, norm_status)
            else:
                where_sql = "WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')"
                params = (sid,)

            cur.execute(f"SELECT count(*) FROM ingest_ledger {where_sql}", params)
            count_row = cur.fetchone()
            total += int(count_row[0]) if count_row and count_row[0] is not None else 0

            cur.execute(
                f"SELECT service_id, object_key, status, attempts, error_kind, last_error, "
                f"claimed_by, claimed_at, discovered_at, committed_at, next_attempt_at, "
                f"originating_task, originating_run_id, size_bytes "
                f"FROM ingest_ledger {where_sql} "
                f"ORDER BY COALESCE(claimed_at, discovered_at, committed_at, 0) DESC, object_key DESC "
                f"LIMIT ?",
                (*params, per_service_limit),
            )
            for row in cur.fetchall():
                row_sid = str(row[0])
                obj_key = str(row[1])
                if (row_sid, obj_key) in seen_keys:
                    continue
                seen_keys.add((row_sid, obj_key))
                entries.append(
                    {
                        "service_id": row_sid,
                        "object_key": obj_key,
                        "source_type": "rum" if _is_rum_object_key(obj_key) else "request",
                        "status": str(row[2]),
                        "attempts": int(row[3] or 0),
                        "error_kind": row[4],
                        "last_error": row[5],
                        "claimed_by": row[6],
                        "claimed_at": float(row[7]) if row[7] is not None else None,
                        "discovered_at": float(row[8]) if row[8] is not None else None,
                        "committed_at": float(row[9]) if row[9] is not None else None,
                        "next_attempt_at": float(row[10]) if row[10] is not None else None,
                        "originating_task": row[11],
                        "originating_run_id": int(row[12]) if row[12] is not None else None,
                        "size_bytes": int(row[13]) if row[13] is not None else None,
                    }
                )
        except Exception as e:
            logger.debug("[admin_status] list_ingest_ledger_entries failed for %s: %s", sid, e)
            continue

    entries.sort(
        key=lambda item: (
            float(item.get("claimed_at") or item.get("discovered_at") or item.get("committed_at") or 0.0),
            str(item.get("object_key") or ""),
        ),
        reverse=True,
    )
    return {
        "items": entries[offset : offset + limit],
        "total": total,
    }


def purge_ingest_ledger_entries(
    items: list[dict[str, str]] | None = None,
    status: str | None = None,
    service_id: str | None = None,
) -> dict[str, Any]:
    """Delete ``dead_letter`` / ``quarantined`` rows from ``ingest_ledger``.
    Never deletes ``discovered``, ``claimed``, or ``committed`` rows."""
    from backend.core.metadata.base import get_con

    affected = 0
    if items:
        by_service: dict[str, list[str]] = {}
        for item in items:
            sid = (item.get("service_id") or "").strip()
            key = (item.get("object_key") or "").strip()
            if sid and key:
                by_service.setdefault(sid, []).append(key)
        for sid, keys in by_service.items():
            try:
                con = get_con(sid)
                cur = con.cursor()
                for key in keys:
                    cur.execute(
                        "DELETE FROM ingest_ledger "
                        "WHERE service_id = ? AND object_key = ? "
                        "AND status IN ('dead_letter', 'quarantined')",
                        (sid, key),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        affected += int(cur.rowcount)
                con.commit()
            except Exception as e:
                logger.warning("[admin_status] purge_ingest_ledger_entries failed for %s: %s", sid, e)
    else:
        norm_status = (status or "problem").strip().lower()
        for sid in _resolve_ledger_service_ids(service_id):
            try:
                con = get_con(sid)
                cur = con.cursor()
                if norm_status in _LEDGER_PROBLEM_STATUSES:
                    cur.execute(
                        "DELETE FROM ingest_ledger WHERE service_id = ? AND status = ?",
                        (sid, norm_status),
                    )
                else:
                    cur.execute(
                        "DELETE FROM ingest_ledger WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')",
                        (sid,),
                    )
                if cur.rowcount and cur.rowcount > 0:
                    affected += int(cur.rowcount)
                con.commit()
            except Exception as e:
                logger.warning("[admin_status] bulk purge_ingest_ledger_entries failed for %s: %s", sid, e)

    return {"ok": True, "affected": affected}


def retry_ingest_ledger_entries(
    items: list[dict[str, str]] | None = None,
    status: str | None = None,
    service_id: str | None = None,
) -> dict[str, Any]:
    """Reset ``dead_letter`` / ``quarantined`` rows in ``ingest_ledger`` back to
    ``discovered`` (with ``attempts=0`` and cleared error fields) for ingest retry."""
    from backend.core.metadata.base import get_con

    affected = 0

    if items:
        by_service: dict[str, list[str]] = {}
        for item in items:
            sid = (item.get("service_id") or "").strip()
            key = (item.get("object_key") or "").strip()
            if sid and key:
                by_service.setdefault(sid, []).append(key)
        for sid, keys in by_service.items():
            try:
                con = get_con(sid)
                cur = con.cursor()
                for key in keys:
                    cur.execute(
                        "UPDATE ingest_ledger "
                        "SET status = 'discovered', attempts = 0, last_error = NULL, error_kind = NULL, "
                        "claimed_by = NULL, claimed_at = NULL, next_attempt_at = NULL "
                        "WHERE service_id = ? AND object_key = ? "
                        "AND status IN ('dead_letter', 'quarantined')",
                        (sid, key),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        affected += int(cur.rowcount)
                con.commit()
            except Exception as e:
                logger.warning("[admin_status] retry_ingest_ledger_entries failed for %s: %s", sid, e)
    else:
        norm_status = (status or "problem").strip().lower()
        for sid in _resolve_ledger_service_ids(service_id):
            try:
                con = get_con(sid)
                cur = con.cursor()
                if norm_status in _LEDGER_PROBLEM_STATUSES:
                    cur.execute(
                        "UPDATE ingest_ledger "
                        "SET status = 'discovered', attempts = 0, last_error = NULL, error_kind = NULL, "
                        "claimed_by = NULL, claimed_at = NULL, next_attempt_at = NULL "
                        "WHERE service_id = ? AND status = ?",
                        (sid, norm_status),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        affected += int(cur.rowcount)
                    con.commit()
                else:
                    cur.execute(
                        "UPDATE ingest_ledger "
                        "SET status = 'discovered', attempts = 0, last_error = NULL, error_kind = NULL, "
                        "claimed_by = NULL, claimed_at = NULL, next_attempt_at = NULL "
                        "WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')",
                        (sid,),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        affected += int(cur.rowcount)
                    con.commit()
            except Exception as e:
                logger.warning("[admin_status] bulk retry_ingest_ledger_entries failed for %s: %s", sid, e)

    return {"ok": True, "affected": affected}


_CACHE_TTL_S = 4.0
_cache: dict = {"at": 0.0, "value": None}
_lock = asyncio.Lock()


async def sample_celery_status_cached():
    now = time.monotonic()
    if _cache["value"] is not None and now - _cache["at"] < _CACHE_TTL_S:
        return _cache["value"]
    async with _lock:
        now = time.monotonic()
        if _cache["value"] is not None and now - _cache["at"] < _CACHE_TTL_S:
            return _cache["value"]
        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(None, get_celery_status)
        _cache["value"] = value
        _cache["at"] = time.monotonic()
        return value
