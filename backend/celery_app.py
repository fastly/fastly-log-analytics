"""Celery application for the distributed ingest / cron data plane, plus
broker queue-depth probing and SSE in-process/sync-status publishers.

Queues:
- ``q.ingest``  — file-level ingest work (discovery dispatch, convert, sweep).
- ``q.control`` — cron-family jobs wrapped by ``@cron_task`` / ``global_job``.

Anything not matched by ``task_routes`` lands on the default ``celery``
queue; workers must consume all three (see docker-compose.multipod.yml and
deploy/chart worker args). tests/routers/test_admin_celery_status.py pins
that every route key matches a registered task and that the advertised
queue set covers every routed queue.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys as _sys
import threading
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from typing import Any

from celery import Celery
from celery.signals import setup_logging, worker_process_init

from backend.config import SSE_BACKPLANE

logger = logging.getLogger(__name__)

broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")

app = Celery("fla_worker", broker=broker_url)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # No result backend: task return values are unused; recording them in the
    # broker would just grow keys. Outcomes are tracked in cron_runs and the
    # ingest_ledger instead.
    task_ignore_result=True,
    # At-least-once semantics: a worker killed mid-task must not silently
    # lose the message. Tasks are idempotent (ledger claim + delete-then-
    # insert convert), so redelivery is safe.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Long-running converts must not let one worker hoard the queue.
    worker_prefetch_multiplier=1,
    # Hard cap a wedged FOS download / DuckDB insert (prefork pool only;
    # the threads pool does not support time limits).
    task_time_limit=15 * 60,
    task_soft_time_limit=13 * 60,
    broker_connection_retry_on_startup=True,
    # Redis-broker redelivery window. Must exceed task_time_limit so a
    # still-running task isn't redelivered to a second worker mid-flight.
    broker_transport_options={"visibility_timeout": 30 * 60},
    include=[
        "backend.core.ingest",
        "backend.cron.jobs.sync",
        "backend.cron.jobs.compaction",
        "backend.cron.jobs.metadata",
        "backend.cron.jobs.insights_prewarmer",
        "backend.cron.jobs.metric_snapshot",
        "backend.cron.jobs.rum_sync",
        "backend.cron.jobs.rum_commit",
    ],
    task_routes={
        "backend.core.ingest.*": {"queue": "q.ingest"},
    },
)


@setup_logging.connect
def _configure_worker_logging(**_kwargs):
    """Workers share the backend's structlog format so their logs carry the
    same processors/fields as the API process (and log shipping filters
    keep working)."""
    from backend.utils.structlog_config import configure_structlog

    configure_structlog()


@worker_process_init.connect
def _worker_process_init(**_kwargs):
    # Re-run per forked child (prefork pool) — signal handlers registered in
    # the parent don't carry logging config into children.
    from backend.utils.structlog_config import configure_structlog

    configure_structlog()

    # Fail the worker fast on incoherent celery-mode config (missing broker,
    # file-based DuckLake catalog) instead of degrading invisibly — same
    # guard the backend lifespan runs.
    from backend.config import validate_deployment_mode

    validate_deployment_mode()

    # Start Prometheus metrics server for worker if configured
    if os.environ.get("OTEL_EXPORTER") == "prometheus":
        try:
            from prometheus_client import start_http_server

            logging.info("Starting Prometheus metrics server on port 8000")
            start_http_server(8000)
        except Exception as e:
            logging.warning(f"Failed to start prometheus_client: {e}")

    # Workers issue metadata queries (ingest ledger, cron_runs) and may boot
    # before — or without — the API pod, so they cannot rely on the backend
    # lifespan having created the Postgres schema. Idempotent and
    # race-tolerant; no-op when METADATA_DSN is unset.
    from backend.core.metadata.pg_schema import ensure_pg_schema

    ensure_pg_schema()


# ── Low-level Valkey/Redis broker queue-depth probe (folded from celery_queue.py) ──

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

            _broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            _redis_client = redis.Redis.from_url(
                _broker_url,
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

_self_mod = _sys.modules[__name__]
for _alias in (
    "backend.celery_queue",
    "backend._in_process_publisher",
    "backend.sync_status_publisher",
):
    _sys.modules[_alias] = _self_mod
if "backend" in _sys.modules:
    _backend_pkg = _sys.modules["backend"]
    _backend_pkg.celery_queue = _self_mod  # type: ignore[attr-defined]
    _backend_pkg._in_process_publisher = _self_mod  # type: ignore[attr-defined]
    _backend_pkg.sync_status_publisher = _self_mod  # type: ignore[attr-defined]
