"""Celery/RedBeat/ledger status snapshot, shared by the admin route, the
admin SSE feeder, and the health snapshot.

Lives at backend top level (beside ``celery_app``) so non-router callers
(``celery_status_sampler``, ``routers/admin/health``) don't import through
the routers package.

Design notes:
- One module-level sync Redis client, created lazily and reused — the old
  per-call ``Redis.from_url`` churned a TCP connect per admin poll.
- ``SCAN``-based key discovery only; ``KEYS`` is O(keyspace) and blocks the
  broker for every other client.
- ``broker_reachable`` distinguishes "idle" from "Valkey is down" — the old
  shape collapsed both to empty dicts.
- RedBeat entries are hashes at ``redbeat:<name>`` whose ``definition`` field
  is JSON (celery-redbeat's storage format); ``redbeat::schedule`` and
  ``redbeat::lock`` are internal statics, not entries.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

_redis_lock = threading.Lock()
_redis_client: Any = None

# Queues the app actually uses: Celery's default queue plus the explicit
# routed queues from backend/celery_app.py. Keep in sync with task_routes.
KNOWN_QUEUES = ("celery", "q.ingest", "q.control")

_REDBEAT_STATIC_PREFIX = "redbeat::"  # schedule zset + lock, not entries
_REDBEAT_ENTRY_PREFIX = "redbeat:"


def _get_redis():
    """Lazily create (and cache) one sync Redis client for status reads."""
    global _redis_client
    with _redis_lock:
        if _redis_client is None:
            import redis

            broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            _redis_client = redis.Redis.from_url(
                broker_url,
                socket_connect_timeout=1.0,
                socket_timeout=2.0,
                decode_responses=True,
            )
        return _redis_client


def celery_queue_depths() -> tuple[dict[str, int], bool]:
    """Return ({queue: depth}, broker_reachable).

    Checks the known queue names directly (LLEN on a missing key is 0) and
    SCANs for any additional ``q.*`` queues a future route might add.
    """
    queues: dict[str, int] = {}
    try:
        r = _get_redis()
        names = set(KNOWN_QUEUES)
        for key in r.scan_iter(match="q.*", count=200):
            names.add(key)
        for q in sorted(names):
            try:
                if r.type(q) in ("list", "none"):
                    queues[q] = int(r.llen(q))
            except Exception:
                continue
        return queues, True
    except Exception as e:
        logger.debug("[celery_status] broker unreachable for queue depths: %s", e)
        return {}, False


def redbeat_schedule_entries() -> list[dict[str, str]]:
    """List RedBeat schedule entries as {name, task} dicts."""
    entries: list[dict[str, str]] = []
    try:
        r = _get_redis()
        for key in r.scan_iter(match=f"{_REDBEAT_ENTRY_PREFIX}*", count=500):
            if key.startswith(_REDBEAT_STATIC_PREFIX):
                continue
            try:
                definition = r.hget(key, "definition")
                task = ""
                if definition:
                    task = json.loads(definition).get("task", "")
                entries.append({"name": key[len(_REDBEAT_ENTRY_PREFIX) :], "task": task})
            except Exception:
                continue
    except Exception as e:
        logger.debug("[celery_status] broker unreachable for redbeat entries: %s", e)
    return entries


def ingest_ledger_summary() -> dict[str, int]:
    """Aggregate ingest_ledger status counts across all configured services."""
    ledger: dict[str, int] = {}
    try:
        from backend.config import list_configs
        from backend.core.metadata.base import get_con

        for svc in list_configs():
            sid = svc.get("service_id") or svc.get("name")
            if not sid:
                continue
            try:
                con = get_con(sid)
                cur = con.cursor()
                cur.execute("SELECT status, count(*) FROM ingest_ledger GROUP BY status")
                for status, count in cur.fetchall():
                    ledger[status] = ledger.get(status, 0) + count
            except Exception:
                continue
    except Exception as e:
        logger.debug("[celery_status] ledger summary failed: %s", e)
    return ledger


def get_celery_status(timeout: float = 1.0) -> dict[str, Any]:
    """Full status payload for the admin endpoint / SSE feeder."""
    from backend.celery_app import app

    workers_error: str | None = None
    try:
        i = app.control.inspect(timeout=timeout)
        active = i.active() or {}
        stats = i.stats() or {}
        registered = i.registered() or {}
        scheduled = i.scheduled() or {}
    except Exception as e:
        workers_error = str(e)[:200]
        active = {}
        stats = {}
        registered = {}
        scheduled = {}

    queues, broker_reachable = celery_queue_depths()
    schedules = redbeat_schedule_entries() if broker_reachable else []
    ledger = ingest_ledger_summary()

    return {
        "broker_reachable": broker_reachable,
        "workers": {
            "active_tasks": active,
            "stats": stats,
            "registered": registered,
            "scheduled": scheduled,
            "count": len(stats) if stats else 0,
            **({"error": workers_error} if workers_error else {}),
        },
        "queues": queues,
        "schedules": schedules,
        "ledger": ledger,
    }
