"""Minute-cadence sampler for the admin metric-history trend lines.

Pulls five operational vitals from the surfaces they already live on
(no new instrumentation) and stamps them to
:mod:`backend.core.metric_snapshots`:

- ``pool_wait_p95_ms``    (per-service)
- ``cron_duration_ms``    (per-service, per-task; latest terminal run)
- ``ingest_lag_s``        (per-service)
- ``active_query_count``  (global)
- ``cpu_load_1m`` / ``mem_used_pct`` / ``disk_used_pct``  (global)

Failure of one metric is logged and skipped; the others still get
recorded.

Registered as a global APScheduler interval job (every 60 s) from
:meth:`Scheduler._sync_jobs` — see ``metric_snapshot`` block there.
"""

from __future__ import annotations

import logging
import os
import shutil

from backend.core import metric_snapshots

logger = logging.getLogger("backend.scheduler")


def _safe_record(
    metric: str,
    value: float,
    *,
    service_id: str | None = None,
    task: str | None = None,
    **attributes: str,
) -> None:
    """Wrap record_snapshot so one bad metric can't poison the rest of the tick."""
    try:
        metric_snapshots.record_snapshot(metric, value, service_id=service_id, task=task)
    except Exception as e:
        logger.debug("[metric_snapshot] record %s failed: %s", metric, e)
    try:
        from backend.core import operational_metrics

        if metric in {"celery_active_tasks", "celery_active_workers"}:
            operational_metrics.record(metric, value)
        elif metric.startswith("celery_queue_depth_"):
            operational_metrics.record(
                "celery_queue_depth",
                value,
                queue=metric.removeprefix("celery_queue_depth_"),
            )
        elif metric == "celery_queue_depth":
            operational_metrics.record(metric, value, queue="all")
        elif metric == "celery_broker_reachable":
            operational_metrics.record(metric, value)
        elif metric.startswith("ingest_ledger_"):
            operational_metrics.record(
                "ingest_ledger_rows",
                value,
                status=metric.removeprefix("ingest_ledger_"),
            )
        elif metric.startswith("high_scale_"):
            if service_id is not None:
                attributes["service_id"] = service_id
            if task is not None:
                attributes["task"] = task
            operational_metrics.record(metric, value, **attributes)
    except Exception as e:
        logger.debug("[metric_snapshot] operational metric %s failed: %s", metric, e)


def _sample_pool_wait() -> None:
    try:
        from backend.core import duckdb_pool

        for stats in duckdb_pool.get_all_stats():
            wait = stats.get("wait") or {}
            p95 = wait.get("p95_ms")
            if p95 is not None and stats.get("service"):
                _safe_record("pool_wait_p95_ms", float(p95), service_id=stats["service"])
    except Exception as e:
        logger.debug("[metric_snapshot] pool_wait sample failed: %s", e)


def _sample_cron_duration() -> None:
    try:
        from backend import config as svcconfig
        from backend.core.metadata.base import get_con

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                con = get_con(service_id)
                rows = con.execute(
                    """
                    SELECT task, duration_s
                    FROM cron_runs
                    WHERE status IN ('success', 'error')
                      AND duration_s IS NOT NULL
                      AND id IN (
                          SELECT max(id) FROM cron_runs
                          WHERE status IN ('success', 'error')
                          GROUP BY task
                      )
                    """
                ).fetchall()
                for r in rows:
                    task = r["task"]
                    secs = r["duration_s"]
                    if task and secs is not None:
                        _safe_record("cron_duration_ms", float(secs) * 1000.0, service_id=service_id, task=task)
            except Exception as e:
                logger.debug("[metric_snapshot] cron_duration for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] cron_duration sample failed: %s", e)


def _sample_ingest_lag() -> None:
    try:
        from datetime import UTC, datetime

        from backend import config as svcconfig
        from backend.core import metadata as metadata_db
        from backend.utils.date_utils import parse_iso_utc

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                latest = metadata_db.get_latest_ingest_ts(service_id)
                if not latest:
                    continue
                latest_dt = parse_iso_utc(latest)
                if latest_dt is None:
                    continue
                lag_s = max(0.0, (datetime.now(UTC) - latest_dt).total_seconds())
                _safe_record("ingest_lag_s", lag_s, service_id=service_id)
            except Exception as e:
                logger.debug("[metric_snapshot] ingest_lag for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] ingest_lag sample failed: %s", e)


def _sample_active_queries() -> None:
    try:
        from backend.core.query_registry import query_registry

        summary = query_registry.summary()
        _safe_record("active_query_count", float(summary.get("active_total", 0)))
    except Exception as e:
        logger.debug("[metric_snapshot] active_query sample failed: %s", e)


def _sample_ducklake_admission() -> None:
    try:
        from backend.core.ducklake_admission import get_admission_stats

        for service_id, stats in get_admission_stats().items():
            acquisitions = stats.get("acquisitions", 0.0)
            if acquisitions <= 0:
                continue
            _safe_record(
                "ducklake_admission_wait_avg_ms",
                stats.get("wait_ms_total", 0.0) / acquisitions,
                service_id=service_id,
            )
            _safe_record(
                "ducklake_admission_hold_avg_ms",
                stats.get("hold_ms_total", 0.0) / acquisitions,
                service_id=service_id,
            )
            _safe_record(
                "ducklake_admission_timeouts",
                stats.get("timeouts", 0.0),
                service_id=service_id,
            )
    except Exception as e:
        logger.debug("[metric_snapshot] ducklake_admission sample failed: %s", e)


def _sample_os_vitals() -> None:
    # CPU load (1-minute moving average).
    try:
        load1, _, _ = os.getloadavg()
        _safe_record("cpu_load_1m", float(load1))
    except Exception as e:
        logger.debug("[metric_snapshot] cpu_load sample failed: %s", e)

    # Memory used percentage (Linux /proc/meminfo). Dev macOS has no
    # /proc — the open fails and we skip the metric gracefully.
    try:
        with open("/proc/meminfo") as f:
            meminfo: dict[str, int] = {}
            for line in f:
                k, _, rest = line.partition(":")
                v = rest.strip().split()
                if v and v[0].isdigit():
                    meminfo[k.strip()] = int(v[0]) * 1024
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", 0)
        if total:
            _safe_record("mem_used_pct", round((1 - avail / total) * 100, 2))
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug("[metric_snapshot] mem sample failed: %s", e)

    # Disk used percentage on the data mount (prod) or root (dev fallback).
    for path, metric in (("/app/data", "disk_used_pct"), ("/", "disk_used_pct_root")):
        try:
            d = shutil.disk_usage(path)
            if d.total:
                _safe_record(metric, round(d.used / d.total * 100, 2))
        except FileNotFoundError:
            continue
        except Exception as e:
            logger.debug("[metric_snapshot] disk sample for %s failed: %s", path, e)


def _sample_celery_queues() -> None:
    try:
        from backend.celery_status import KNOWN_QUEUES, celery_queue_depths

        queues, broker_reachable = celery_queue_depths()
        _safe_record("celery_broker_reachable", 1.0 if broker_reachable else 0.0)
        total_depth = 0.0
        for queue in sorted(set(KNOWN_QUEUES) | set(queues)):
            depth = float(queues.get(queue, 0))
            total_depth += depth
            _safe_record(f"celery_queue_depth_{queue}", depth)
        _safe_record("celery_queue_depth", total_depth)
    except Exception as e:
        logger.debug("[metric_snapshot] celery_queues sample failed: %s", e)


def _sample_ingest_ledger() -> None:
    try:
        from backend.celery_status import ingest_ledger_summary

        for status, count in ingest_ledger_summary().items():
            _safe_record(f"ingest_ledger_{status}", float(count))
    except Exception as e:
        logger.debug("[metric_snapshot] ingest_ledger sample failed: %s", e)


def _sample_high_scale() -> None:
    if os.getenv("HIGH_SCALE_ENABLED", "").lower() not in {"1", "true", "yes", "on"}:
        return
    try:
        from backend import config as svcconfig
        from backend.high_scale.postgres_control import PostgresControlPlane

        control = PostgresControlPlane()
        configured_domains = {
            domain.strip() for domain in os.getenv("HIGH_SCALE_DOMAINS", "").split(",") if domain.strip()
        }
        source_statuses = (
            "discovered",
            "claimed",
            "appended",
            "archived",
            "acknowledged",
            "source_deleted",
            "source_missing",
        )
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            snapshot = control.operational_snapshot(service_id)
            domains = configured_domains | {
                row["domain"] for row in snapshot["source_objects"] + snapshot["source_age"] + snapshot["publications"]
            }
            for domain in domains:
                for status in source_statuses:
                    _safe_record(
                        "high_scale_source_objects",
                        0.0,
                        service_id=service_id,
                        domain=domain,
                        status=status,
                    )
                _safe_record(
                    "high_scale_oldest_active_age_seconds",
                    0.0,
                    service_id=service_id,
                    domain=domain,
                )
                _safe_record(
                    "high_scale_pending_manifests",
                    0.0,
                    service_id=service_id,
                    domain=domain,
                )
                _safe_record(
                    "high_scale_publication_lag_seconds",
                    0.0,
                    service_id=service_id,
                    domain=domain,
                )
                _safe_record(
                    "high_scale_rows_published_total",
                    0.0,
                    service_id=service_id,
                    domain=domain,
                )
            for row in snapshot["source_objects"]:
                _safe_record(
                    "high_scale_source_objects",
                    float(row["count"]),
                    service_id=service_id,
                    domain=row["domain"],
                    status=row["status"],
                )
            for row in snapshot["source_age"]:
                _safe_record(
                    "high_scale_oldest_active_age_seconds",
                    row["age_seconds"],
                    service_id=service_id,
                    domain=row["domain"],
                )
            for row in snapshot["publications"]:
                _safe_record(
                    "high_scale_pending_manifests",
                    float(row["pending"]),
                    service_id=service_id,
                    domain=row["domain"],
                )
                _safe_record(
                    "high_scale_publication_lag_seconds",
                    row["lag_seconds"],
                    service_id=service_id,
                    domain=row["domain"],
                )
                _safe_record(
                    "high_scale_rows_published_total",
                    float(row["published_rows"]),
                    service_id=service_id,
                    domain=row["domain"],
                )
    except Exception as e:
        logger.debug("[metric_snapshot] high_scale sample failed: %s", e)


def _sample_celery_workers() -> None:
    try:
        from backend.celery_app import app

        i = app.control.inspect()
        stats = i.stats() or {}
        _safe_record("celery_active_workers", float(len(stats)))

        active = i.active() or {}
        active_tasks = sum(len(tasks) for tasks in active.values())
        _safe_record("celery_active_tasks", float(active_tasks))
    except Exception as e:
        logger.debug("[metric_snapshot] celery_workers sample failed: %s", e)


from backend.cron.decorators import global_job


@global_job("metric_snapshot", color="32", tag="metric_snapshot", label="Metric snapshot")
def _run_metric_snapshot() -> None:
    """Sample every vital once. Designed to finish in <100 ms on a single-service deploy.

    Not decorated with @cron_task because it doesn't open a DuckDB
    connection or hit FOS and the standard watchdog / usage-log envelope
    would be net overhead for a job that writes ~5 SQLite rows.
    """
    _sample_pool_wait()
    _sample_cron_duration()
    _sample_ingest_lag()
    _sample_active_queries()
    _sample_ducklake_admission()
    _sample_high_scale()
    _sample_os_vitals()
    _sample_celery_queues()
    _sample_ingest_ledger()
    _sample_celery_workers()
