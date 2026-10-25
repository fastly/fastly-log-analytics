"""Minute-cadence operational-vital snapshots.

Backs the trend lines on the admin System Health card / Trends tab. Each
row is one numeric sample: ``(ts, metric, service_id?, task?, value)``.
All snapshots are stored in PostgreSQL standard mode in the ``metric_snapshots``
table.

Public surface
--------------
- :func:`record_snapshot` — sampler writes one row per metric per tick.
- :func:`get_history` — read a single time-series (one ``metric`` /
  ``service_id`` / ``task``); the symmetric single-series counterpart to
  :func:`get_batch`.
- :func:`get_batch` — admin endpoint reads many series in one call.
- :func:`purge_old` — daily cleanup cron drops rows past retention.
- :func:`teardown` / :func:`close_all_connections` — test helpers.
"""

from __future__ import annotations

import logging
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

from opentelemetry.metrics import CallbackOptions, Observation

from backend.core.request_telemetry import get_meter
from backend.utils.date_utils import iso_z, iso_z_now, parse_relative_time_window

logger = logging.getLogger(__name__)

_EMPTY = ""


def _pg_connection():
    from backend.core.metadata.pg_connection import get_pg_readonly_connection

    return get_pg_readonly_connection()


def _pg_write_connection():
    from backend.core.metadata.pg_connection import get_pg_thread_connection

    return get_pg_thread_connection()


# ── Write path ───────────────────────────────────────────────────────────────


def record_snapshot(
    metric: str,
    value: float,
    *,
    service_id: str | None = None,
    task: str | None = None,
    ts: str | None = None,
) -> None:
    """Insert one metric sample. Idempotent on ``(metric, service_id, task, ts)``."""
    if not metric:
        raise ValueError("metric is required")
    ts_str = ts or iso_z_now()
    try:
        con = _pg_write_connection()
        con.execute(
            """
            INSERT INTO metric_snapshots (ts, metric, service_id, task, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (metric, service_id, task, ts)
            DO UPDATE SET value = EXCLUDED.value
            """,
            (ts_str, metric, service_id or _EMPTY, task or _EMPTY, float(value)),
        )
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres insert failed for %s: %s", metric, e)


# ── Read path ────────────────────────────────────────────────────────────────


def get_batch(*, since: datetime | str) -> dict:
    """Return every series newer than ``since``, grouped by series key.

    Series key shape: ``"{metric}"`` for global, ``"{metric}|{service_id}"``
    for per-service, ``"{metric}|{service_id}|{task}"`` for per-task. The
    admin Trends page does one round-trip; the frontend partitions
    by metric prefix.
    """
    if isinstance(since, str):
        since = parse_relative_time_window(since)
    try:
        with closing(_pg_connection()) as con:
            rows = con.execute(
                """
                SELECT metric, service_id, task, ts, value
                FROM metric_snapshots
                WHERE ts >= %s
                ORDER BY ts ASC
                """,
                (iso_z(since),),
            ).fetchall()
        return _group_batch_rows(rows)
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres batch read failed: %s", e)
        return {}


# ── Liveness ─────────────────────────────────────────────────────────────────


def last_snapshot_age_s() -> float | None:
    """Seconds since the most recent metric_snapshots row, or ``None`` if empty.

    SRE-06: the minute-cadence sampler (:mod:`backend.cron.jobs.metric_snapshot`)
    is a *global* APScheduler interval job, so a stale ``max(ts)`` is a direct
    witness that the scheduler thread has stopped ticking.
    """
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
    if dt is None:
        return None
    return max(0.0, (datetime.now(UTC) - dt).total_seconds())


# ── Retention ────────────────────────────────────────────────────────────────


def purge_old(retention_days: int = 30) -> int:
    """Delete rows older than ``retention_days``. Returns the row count."""
    if retention_days <= 0:
        return 0
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    try:
        con = _pg_write_connection()
        cur = con.execute("DELETE FROM metric_snapshots WHERE ts < %s", (cutoff,))
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres purge failed: %s", e)
        return 0


# ── Test / teardown helpers ──────────────────────────────────────────────────


def close_all_connections() -> None:
    """Close any thread-local connection (no-op under Postgres)."""
    pass


_MAX_BATCH_POINTS_PER_SERIES = 240


def _downsample_rows(series_rows: list, max_points: int = _MAX_BATCH_POINTS_PER_SERIES) -> list[dict]:
    n = len(series_rows)
    if n <= max_points or max_points < 4:
        return [{"ts": r["ts"], "value": r["value"]} for r in series_rows]
    step = (n - 1) / float(max_points - 3)
    indices = {int(round(i * step)) for i in range(max_points - 2)}
    indices.add(0)
    indices.add(n - 1)
    min_idx = min(range(n), key=lambda idx: series_rows[idx]["value"])
    max_idx = max(range(n), key=lambda idx: series_rows[idx]["value"])
    indices.add(min_idx)
    indices.add(max_idx)
    return [{"ts": series_rows[idx]["ts"], "value": series_rows[idx]["value"]} for idx in sorted(indices)]


def _group_batch_rows(rows) -> dict[str, list[dict]]:
    raw_grouped: dict[str, list] = {}
    for r in rows:
        key = r["metric"]
        svc = r["service_id"]
        if svc:
            key = f"{key}|{svc}"
            task = r["task"]
            if task:
                key = f"{key}|{task}"
        raw_grouped.setdefault(key, []).append(r)
    return {key: _downsample_rows(series_rows) for key, series_rows in raw_grouped.items()}


def teardown() -> None:
    """Clean up snapshots for test isolation."""
    try:
        con = _pg_write_connection()
        con.execute("TRUNCATE TABLE metric_snapshots")
    except Exception:
        logger.debug("teardown: con.execute failed (ignored)", exc_info=True)


# ── Latest operational samples exported through the configured OTel meter ────

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
    instrument = _instruments.get(metric)
    if instrument is not None:
        return instrument

    description, unit = _METRICS[metric]

    def callback(_options: CallbackOptions, metric_name: str = metric) -> list[Observation]:
        with _lock:
            samples = dict(_values.get(metric_name, {}))
        return [Observation(value, dict(attributes)) for attributes, value in samples.items()]

    instrument = get_meter().create_observable_gauge(
        name=f"fla_{metric}",
        callbacks=[callback],
        description=description,
        unit=unit,
    )
    _instruments[metric] = instrument
    return instrument


def record(metric: str, value: float, **attributes: str) -> None:
    """Publish the latest value for an operational metric and its labels."""
    if metric not in _METRICS:
        raise ValueError(f"unsupported operational metric: {metric}")
    key = tuple(sorted((name, str(value)) for name, value in attributes.items()))
    with _lock:
        _values.setdefault(metric, {})[key] = float(value)
    _instrument(metric)


def reset_for_tests() -> None:
    """Clear cached samples and instruments between isolated unit tests."""
    with _lock:
        _values.clear()
        _instruments.clear()
