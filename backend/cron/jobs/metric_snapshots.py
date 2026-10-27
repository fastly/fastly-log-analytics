"""Operational metric snapshot storage, OTel observable gauges, and sampling cron job."""

from __future__ import annotations

import logging
import os
import shutil
import sys as _sys
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

from opentelemetry.metrics import CallbackOptions, Observation

from backend import config as svcconfig  # noqa: F401
from backend.core import duckdb as _db  # noqa: F401
from backend.core import duckdb_pool as _pool  # noqa: F401
from backend.core.request_telemetry import get_meter
from backend.cron.scheduler import global_job
from backend.utils.date_utils import iso_z, iso_z_now, parse_relative_time_window

_os = os
_shutil = shutil
logger = logging.getLogger("backend.scheduler")

_EMPTY = ""


def _pg_connection():
    from backend.core.metadata.base import get_pg_readonly_connection

    return get_pg_readonly_connection()


def _pg_write_connection():
    from backend.core.metadata.base import get_pg_thread_connection

    return get_pg_thread_connection()


def record_snapshot(
    metric: str, value: float, *, service_id: str | None = None, task: str | None = None, ts: str | None = None
) -> None:
    if not metric:
        raise ValueError("metric is required")
    try:
        _pg_write_connection().execute(
            "INSERT INTO metric_snapshots (ts, metric, service_id, task, value) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (metric, service_id, task, ts) DO UPDATE SET value = EXCLUDED.value",
            (ts or iso_z_now(), metric, service_id or _EMPTY, task or _EMPTY, float(value)),
        )
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres insert failed for %s: %s", metric, e)


def get_batch(*, since: datetime | str) -> dict:
    if isinstance(since, str):
        since = parse_relative_time_window(since)
    try:
        with closing(_pg_connection()) as con:
            rows = con.execute(
                "SELECT metric, service_id, task, ts, value FROM metric_snapshots WHERE ts >= %s ORDER BY ts ASC",
                (iso_z(since),),
            ).fetchall()
        return _group_batch_rows(rows)
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres batch read failed: %s", e)
        return {}


def last_snapshot_age_s() -> float | None:
    try:
        with closing(_pg_connection()) as con:
            row = con.execute("SELECT max(ts) AS latest FROM metric_snapshots").fetchone()
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres liveness read failed: %s", e)
        return None
    latest = row["latest"] if row else None
    if not latest:
        return None
    from backend.utils.date_utils import parse_iso_utc

    dt = parse_iso_utc(latest)
    return max(0.0, (datetime.now(UTC) - dt).total_seconds()) if dt is not None else None


def purge_old(retention_days: int = 30) -> int:
    if retention_days <= 0:
        return 0
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    try:
        cur = _pg_write_connection().execute("DELETE FROM metric_snapshots WHERE ts < %s", (cutoff,))
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres purge failed: %s", e)
        return 0


def close_all_connections() -> None:
    pass


_MAX_BATCH_POINTS_PER_SERIES = 240


def _downsample_rows(series_rows: list, max_points: int = _MAX_BATCH_POINTS_PER_SERIES) -> list[dict]:
    n = len(series_rows)
    if n <= max_points or max_points < 4:
        return [{"ts": r["ts"], "value": r["value"]} for r in series_rows]
    step = (n - 1) / float(max_points - 3)
    indices = {
        0,
        n - 1,
        min(range(n), key=lambda idx: series_rows[idx]["value"]),
        max(range(n), key=lambda idx: series_rows[idx]["value"]),
        *(int(round(i * step)) for i in range(max_points - 2)),
    }
    return [{"ts": series_rows[idx]["ts"], "value": series_rows[idx]["value"]} for idx in sorted(indices)]


def _group_batch_rows(rows) -> dict[str, list[dict]]:
    raw_grouped: dict[str, list] = {}
    for r in rows:
        key = r["metric"]
        if r["service_id"]:
            key = f"{key}|{r['service_id']}" + (f"|{r['task']}" if r["task"] else "")
        raw_grouped.setdefault(key, []).append(r)
    return {key: _downsample_rows(series_rows) for key, series_rows in raw_grouped.items()}


def teardown() -> None:
    try:
        _pg_write_connection().execute("TRUNCATE TABLE metric_snapshots")
    except Exception:
        logger.debug("teardown: con.execute failed (ignored)", exc_info=True)


_lock = threading.Lock()
_values: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}
_instruments: dict[str, Any] = {}
_METRICS = {
    "celery_queue_depth": ("Celery broker queue depth", "{messages}"),
    "celery_broker_reachable": ("Celery broker reachability", "1"),
    "celery_active_workers": ("Active Celery workers", "{workers}"),
    "celery_active_tasks": ("Active Celery tasks", "{tasks}"),
    "ingest_ledger_rows": ("Ingest ledger rows by state", "{rows}"),
    "high_scale_source_objects": ("High-scale source objects by state", "{objects}"),
    "high_scale_oldest_active_age_seconds": ("Age of the oldest active high-scale source object", "s"),
    "high_scale_publication_lag_seconds": ("Age of the oldest pending high-scale publication", "s"),
    "high_scale_pending_manifests": ("Pending high-scale publication manifests", "{manifests}"),
    "high_scale_rows_published_total": ("High-scale rows published", "{rows}"),
    "high_scale_disk_mode_level": ("High-scale serving-volume disk mode (0 normal .. 3 emergency)", "1"),
    "high_scale_disk_free_fraction": ("High-scale serving-volume free-space fraction", "1"),
}


def _instrument(metric: str) -> Any:
    if (instrument := _instruments.get(metric)) is not None:
        return instrument
    description, unit = _METRICS[metric]

    def callback(_options: CallbackOptions, metric_name: str = metric) -> list[Observation]:
        with _lock:
            samples = dict(_values.get(metric_name, {}))
        return [Observation(value, dict(attributes)) for attributes, value in samples.items()]

    instrument = get_meter().create_observable_gauge(
        name=f"fla_{metric}", callbacks=[callback], description=description, unit=unit
    )
    _instruments[metric] = instrument
    return instrument


def record(metric: str, value: float, **attributes: str) -> None:
    if metric not in _METRICS:
        raise ValueError(f"unsupported operational metric: {metric}")
    key = tuple(sorted((name, str(val)) for name, val in attributes.items()))
    with _lock:
        _values.setdefault(metric, {})[key] = float(value)
    _instrument(metric)


def reset_for_tests() -> None:
    with _lock:
        _values.clear()
        _instruments.clear()


metric_snapshots = _sys.modules[__name__]


def _safe_record(
    metric: str, value: float, *, service_id: str | None = None, task: str | None = None, **attributes: str
) -> bool:
    recorded = False
    try:
        metric_snapshots.record_snapshot(metric, value, service_id=service_id, task=task)
        recorded = True
    except Exception as e:
        logger.debug("[metric_snapshot] record %s failed: %s", metric, e)
    try:
        operational_metrics = _sys.modules[__name__]
        if metric in {"celery_active_tasks", "celery_active_workers", "celery_broker_reachable"}:
            operational_metrics.record(metric, value)
        elif metric.startswith("celery_queue_depth_"):
            operational_metrics.record("celery_queue_depth", value, queue=metric.removeprefix("celery_queue_depth_"))
        elif metric == "celery_queue_depth":
            operational_metrics.record(metric, value, queue="all")
        elif metric.startswith("ingest_ledger_"):
            operational_metrics.record("ingest_ledger_rows", value, status=metric.removeprefix("ingest_ledger_"))
        elif metric.startswith("high_scale_"):
            if service_id is not None:
                attributes["service_id"] = service_id
            if task is not None:
                attributes["task"] = task
            operational_metrics.record(metric, value, **attributes)
    except Exception as e:
        logger.debug("[metric_snapshot] operational metric %s failed: %s", metric, e)
    return recorded


def _sample_pool_wait() -> int:
    count = 0
    try:
        from backend.core import duckdb_pool

        for stats in duckdb_pool.get_all_stats():
            p95 = (stats.get("wait") or {}).get("p95_ms")
            if (
                p95 is not None
                and stats.get("service")
                and _safe_record("pool_wait_p95_ms", float(p95), service_id=stats["service"])
            ):
                count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] pool_wait sample failed: %s", e)
    return count


def _sample_cron_duration() -> int:
    count = 0
    try:
        from backend import config as svcconfig
        from backend.core.metadata.base import get_con

        sql_by_svc = (
            "SELECT task, duration_s FROM cron_runs WHERE service_id = ? AND status IN ('success', 'warning', 'error') "
            "AND duration_s IS NOT NULL AND id IN (SELECT max(id) FROM cron_runs WHERE service_id = ? "
            "AND status IN ('success', 'warning', 'error') GROUP BY service_id, task)"
        )
        sql_legacy = (
            "SELECT task, duration_s FROM cron_runs WHERE status IN ('success', 'warning', 'error') "
            "AND duration_s IS NOT NULL AND id IN (SELECT max(id) FROM cron_runs "
            "WHERE status IN ('success', 'warning', 'error') GROUP BY task)"
        )
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                con = get_con(service_id)
                try:
                    rows = con.execute(sql_by_svc, (service_id, service_id)).fetchall()
                except Exception as probe_exc:
                    if "service_id" not in str(probe_exc):
                        raise
                    rows = con.execute(sql_legacy).fetchall()
                for r in rows:
                    if r["task"] and r["duration_s"] is not None:
                        if _safe_record(
                            "cron_duration_ms", float(r["duration_s"]) * 1000.0, service_id=service_id, task=r["task"]
                        ):
                            count += 1
            except Exception as e:
                logger.debug("[metric_snapshot] cron_duration for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] cron_duration sample failed: %s", e)
    return count


def _sample_ingest_lag() -> int:
    count = 0
    try:
        from backend import config as svcconfig
        from backend.core import metadata as metadata_db
        from backend.utils.date_utils import parse_iso_utc

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                latest = metadata_db.get_latest_ingest_ts(service_id)
                latest_dt = parse_iso_utc(latest) if latest else None
                if latest_dt is not None:
                    lag_s = max(0.0, (datetime.now(UTC) - latest_dt).total_seconds())
                    if _safe_record("ingest_lag_s", lag_s, service_id=service_id):
                        count += 1
            except Exception as e:
                logger.debug("[metric_snapshot] ingest_lag for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] ingest_lag sample failed: %s", e)
    return count


def _sample_active_queries() -> int:
    try:
        from backend.core.request_telemetry import query_registry

        summary = query_registry.summary()
        return 1 if _safe_record("active_query_count", float(summary.get("active_total", 0))) else 0
    except Exception as e:
        logger.debug("[metric_snapshot] active_query sample failed: %s", e)
        return 0


def _sample_ducklake_admission() -> int:
    count = 0
    try:
        from backend.core.storage._ducklake import get_admission_stats

        for service_id, stats in get_admission_stats().items():
            acquisitions = stats.get("acquisitions", 0.0)
            if acquisitions <= 0:
                continue
            for metric, val in (
                ("ducklake_admission_wait_avg_ms", stats.get("wait_ms_total", 0.0) / acquisitions),
                ("ducklake_admission_hold_avg_ms", stats.get("hold_ms_total", 0.0) / acquisitions),
                ("ducklake_admission_timeouts", stats.get("timeouts", 0.0)),
            ):
                if _safe_record(metric, val, service_id=service_id):
                    count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] ducklake_admission sample failed: %s", e)
    return count


def _sample_os_vitals() -> int:
    count = 0
    try:
        load1, _, _ = _os.getloadavg()
        if _safe_record("cpu_load_1m", float(load1)):
            count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] cpu_load sample failed: %s", e)
    try:
        with open("/proc/meminfo") as f:
            meminfo: dict[str, int] = {}
            for line in f:
                k, _, rest = line.partition(":")
                v = rest.strip().split()
                if v and v[0].isdigit():
                    meminfo[k.strip()] = int(v[0]) * 1024
        total, avail = meminfo.get("MemTotal", 0), meminfo.get("MemAvailable", 0)
        if total and _safe_record("mem_used_pct", round((1 - avail / total) * 100, 2)):
            count += 1
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug("[metric_snapshot] mem sample failed: %s", e)
    for path, metric in (("/app/data", "disk_used_pct"), ("/", "disk_used_pct_root")):
        try:
            d = _shutil.disk_usage(path)
            if d.total and _safe_record(metric, round(d.used / d.total * 100, 2)):
                count += 1
        except FileNotFoundError:
            continue
        except Exception as e:
            logger.debug("[metric_snapshot] disk sample for %s failed: %s", path, e)
    return count


def _sample_celery_queues() -> int:
    count = 0
    try:
        from backend.celery_status import KNOWN_QUEUES, celery_queue_depths

        queues, broker_reachable = celery_queue_depths()
        if _safe_record("celery_broker_reachable", 1.0 if broker_reachable else 0.0):
            count += 1
        total_depth = 0.0
        for queue in sorted(set(KNOWN_QUEUES) | set(queues)):
            depth = float(queues.get(queue, 0))
            total_depth += depth
            if _safe_record(f"celery_queue_depth_{queue}", depth):
                count += 1
        if _safe_record("celery_queue_depth", total_depth):
            count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] celery_queues sample failed: %s", e)
    return count


def _sample_ingest_ledger() -> int:
    count = 0
    try:
        from backend.celery_status import ingest_ledger_summary

        for status, count_val in ingest_ledger_summary().items():
            if _safe_record(f"ingest_ledger_{status}", float(count_val)):
                count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] ingest_ledger sample failed: %s", e)
    return count


def _sample_high_scale() -> int:

    if not svcconfig.is_high_scale_mode() or (
        _os.getenv("HIGH_SCALE_ENABLED", "").lower() not in {"1", "true", "yes", "on"}
        and svcconfig.DEPLOYMENT_MODE != "high_scale"
    ):
        return 0
    count = 0
    try:
        from backend.high_scale.postgres_control import PostgresControlPlane

        control = PostgresControlPlane()
        configured_domains = {d.strip() for d in _os.getenv("HIGH_SCALE_DOMAINS", "").split(",") if d.strip()}
        source_statuses = (
            "discovered",
            "claimed",
            "appended",
            "archived",
            "acknowledged",
            "source_deleted",
            "source_missing",
        )
        pub_metrics = (
            ("high_scale_pending_manifests", "pending"),
            ("high_scale_publication_lag_seconds", "lag_seconds"),
            ("high_scale_rows_published_total", "published_rows"),
        )
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            snapshot = control.operational_snapshot(service_id)
            domains = configured_domains | {
                row["domain"] for row in snapshot["source_objects"] + snapshot["source_age"] + snapshot["publications"]
            }
            present_status = {(row["domain"], row["status"]) for row in snapshot["source_objects"]}
            present_age = {row["domain"] for row in snapshot["source_age"]}
            present_pub = {row["domain"] for row in snapshot["publications"]}
            try:
                operational_metrics = _sys.modules[__name__]
                for domain in domains:
                    for status in source_statuses:
                        if (domain, status) not in present_status:
                            operational_metrics.record(
                                "high_scale_source_objects", 0.0, service_id=service_id, domain=domain, status=status
                            )
                    if domain not in present_age:
                        operational_metrics.record(
                            "high_scale_oldest_active_age_seconds", 0.0, service_id=service_id, domain=domain
                        )
                    if domain not in present_pub:
                        for m_name, _ in pub_metrics:
                            operational_metrics.record(m_name, 0.0, service_id=service_id, domain=domain)
            except Exception as e:
                logger.debug("[metric_snapshot] high_scale zero-seed failed: %s", e)
            for row in snapshot["source_objects"]:
                if _safe_record(
                    "high_scale_source_objects",
                    float(row["count"]),
                    service_id=service_id,
                    domain=row["domain"],
                    status=row["status"],
                ):
                    count += 1
            for row in snapshot["source_age"]:
                if _safe_record(
                    "high_scale_oldest_active_age_seconds",
                    row["age_seconds"],
                    service_id=service_id,
                    domain=row["domain"],
                ):
                    count += 1
            for row in snapshot["publications"]:
                for m_name, col in pub_metrics:
                    val = row[col] if col == "lag_seconds" else float(row[col])
                    if _safe_record(m_name, val, service_id=service_id, domain=row["domain"]):
                        count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] high_scale sample failed: %s", e)
    return count


def _sample_high_scale_disk() -> int:

    if not svcconfig.is_high_scale_mode():
        return 0
    count = 0
    try:
        from backend.core.clickhouse import get_clickhouse_client
        from backend.high_scale.archive_models import MODE_LEVELS, sample_disk_state

        client = get_clickhouse_client()
        if client is None:
            return 0
        snapshot = sample_disk_state(client)
        if _safe_record("high_scale_disk_mode_level", float(MODE_LEVELS[snapshot.state.mode])):
            count += 1
        if _safe_record("high_scale_disk_free_fraction", snapshot.state.free_fraction):
            count += 1
    except Exception as e:
        import structlog

        structlog.get_logger(__name__).warning("high_scale.disk_sample_failed", error_kind=type(e).__name__)
    return count


def _sample_duckdb_size() -> int:
    count = 0
    try:
        from backend import config as svcconfig

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                db_path = svcconfig.duckdb_path(service_id)
                if _os.path.exists(db_path) and _safe_record(
                    "duckdb_file_bytes", float(_os.path.getsize(db_path)), service_id=service_id
                ):
                    count += 1
            except Exception as e:
                logger.debug("[metric_snapshot] duckdb_size for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] duckdb_size sample failed: %s", e)
    return count


def _sample_celery_workers() -> int:
    count = 0
    try:
        from backend.celery_app import app

        i = app.control.inspect(timeout=0.5)
        stats = i.stats() or {}
        if _safe_record("celery_active_workers", float(len(stats))):
            count += 1
        active = (i.active() or {}) if stats else {}
        if _safe_record("celery_active_tasks", float(sum(len(tasks) for tasks in active.values()))):
            count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] celery_workers sample failed: %s", e)
    return count


@global_job("metric_snapshot", color="32", tag="metric_snapshot", label="Metric snapshot")
def _run_metric_snapshot() -> str:
    total = sum(
        fn()
        for fn in (
            _sample_pool_wait,
            _sample_cron_duration,
            _sample_ingest_lag,
            _sample_duckdb_size,
            _sample_active_queries,
            _sample_ducklake_admission,
            _sample_high_scale,
            _sample_high_scale_disk,
            _sample_os_vitals,
            _sample_celery_queues,
            _sample_ingest_ledger,
            _sample_celery_workers,
        )
    )
    return f"sampled={total} metrics"


_self_mod = _sys.modules[__name__]
_sys.modules["backend.core.metric_snapshots"] = _self_mod
_sys.modules["backend.cron.jobs.metric_snapshot"] = _self_mod
if (_core_pkg := _sys.modules.get("backend.core")) is not None:
    _core_pkg.metric_snapshots = _self_mod  # type: ignore[attr-defined]
if (_jobs_pkg := _sys.modules.get("backend.cron.jobs")) is not None:
    _jobs_pkg.metric_snapshot = _self_mod  # type: ignore[attr-defined]
