"""Celery application for the distributed ingest / cron data plane.

Queues:
- ``q.ingest``  — file-level ingest work (discovery dispatch, convert, sweep).
- ``q.control`` — cron-family jobs wrapped by ``@cron_task`` / ``global_job``.

Anything not matched by ``task_routes`` lands on the default ``celery``
queue; workers must consume all three (see docker-compose.multipod.yml and
deploy/chart worker args). tests/routers/test_admin_celery_status.py pins
that every route key matches a registered task and that the advertised
queue set covers every routed queue.
"""

import os

from celery import Celery
from celery.signals import setup_logging, worker_process_init

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
        "backend.cron.jobs.commit",
        "backend.cron.jobs.compaction",
        "backend.cron.jobs.optimize",
        "backend.cron.jobs.expire",
        "backend.cron.jobs.metadata",
        "backend.cron.jobs.duckdb_recycle",
        "backend.cron.jobs.insights_prewarmer",
        "backend.cron.jobs.metric_snapshot",
        "backend.cron.jobs.rum_sync",
        "backend.cron.jobs.rum_commit",
        "backend.cron.jobs.rum_ledger",
    ],
    task_routes={
        "backend.core.ingest.*": {"queue": "q.ingest"},
        "backend.cron.jobs.*": {"queue": "q.control"},
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
    from backend.config import validate_ingest_mode

    validate_ingest_mode()

    # Workers issue metadata queries (ingest ledger, cron_runs) and may boot
    # before — or without — the API pod, so they cannot rely on the backend
    # lifespan having created the Postgres schema. Idempotent and
    # race-tolerant; no-op when METADATA_DSN is unset.
    from backend.core.metadata.pg_schema import ensure_pg_schema

    ensure_pg_schema()
