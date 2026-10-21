"""Low-level Valkey/Redis broker queue-depth probe for Celery queues.

Extracted from ``backend/celery_status.py`` so ``backend.core.ingest`` and cron
jobs can inspect queue depths without importing ``backend.celery_status`` (which
imports ``backend.cron.scheduler`` for in-process schedule inspection and would
transitively couple ``backend.core`` to ``backend.routers``).
"""

from __future__ import annotations

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


def _get_redis():
    """Lazily create (and cache) one sync Redis client for status reads."""
    global _redis_client
    from backend import config as svcconfig

    if not svcconfig.is_high_scale_mode() and not os.environ.get("CELERY_BROKER_URL"):
        raise ConnectionError("Celery broker not configured in standard mode")
    with _redis_lock:
        if _redis_client is None:
            import redis

            broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            _redis_client = redis.Redis.from_url(
                broker_url,
                socket_connect_timeout=0.25,
                socket_timeout=0.5,
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
