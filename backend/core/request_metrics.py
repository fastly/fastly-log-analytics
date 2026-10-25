"""Authoritative request extents from the durable analytics serving view
and backend-owned, coalescing request-status reconciliation (ADR-18)."""

from __future__ import annotations

import logging
import sys as _sys
import threading
import time
from typing import Any

from backend import config
from backend.celery_app import publisher
from backend.cron_progress import compute_sync_status_cached
from backend.high_scale.registry_bootstrap import get_high_scale_service_registry
from backend.utils.date_utils import parse_iso_utc

logger = logging.getLogger(__name__)

QUERY_TIMEOUT_S = 30.0
RECONCILE_INTERVAL_S = 15.0
STOP_TIMEOUT_S = 5.0
FAILURE_BACKOFF_BASE_S = 15.0
FAILURE_BACKOFF_MAX_S = 300.0

_refresh_lock = threading.Lock()
_query_lock = threading.Lock()
_active_query: Any | None = None
_cancelled = threading.Event()


def _read_durable_rum_metrics(con: Any, source: dict) -> dict[str, Any] | None:
    """Read RUM extents from the same durable serving connection as requests."""
    from backend.core.iceberg._ducklake import ducklake_table_name

    tables: list[str] = []
    for table_name in (
        ducklake_table_name(source, "client_vitals"),
        ducklake_table_name(source, "client_errors"),
    ):
        try:
            con.execute(f'SELECT 1 FROM lake."{table_name}" LIMIT 0')
        except Exception:
            continue
        tables.append(table_name)
    if not tables:
        return None

    union_sql = " UNION ALL ".join(f'SELECT req_id, cid, timestamp FROM lake."{table}"' for table in tables)
    row = con.execute(
        "SELECT COUNT(DISTINCT COALESCE(NULLIF(req_id, ''), "
        "concat(cid, '_', CAST(epoch(timestamp) AS BIGINT)))), "
        f"MAX(timestamp) FROM ({union_sql}) AS rum_rows"
    ).fetchone()
    return {
        "latest_log_at": row[1].isoformat() if row and row[1] is not None else None,
        "total_rows": int(row[0] or 0) if row else 0,
    }


def interrupt_request_metrics() -> None:
    """Interrupt only this observer's dedicated connection, never a reader."""
    with _query_lock:
        if _active_query is not None:
            _cancelled.set()
            _active_query.interrupt()


def _interrupt_until_finished(finished: threading.Event) -> None:
    # An interrupt arriving just before execute() starts need not cancel that
    # future statement. Repeat after the deadline until the owning pass exits.
    while not finished.is_set():
        interrupt_request_metrics()
        finished.wait(0.1)


def refresh_durable_request_metrics(
    source: dict, *, stop_event: threading.Event | None = None, extra_status: dict | None = None
) -> dict | None:
    """Read one current count/min/max, then persist; failures never write zeros.

    Concurrent callers coalesce instead of queueing scans. The status refresh
    and the lifespan observer share this seam, so neither can overwrite a newer
    request observation with local-file estimates. RUM retains its own extent.
    """
    global _active_query
    if not config.is_durable_serving_mode(source):
        raise ValueError("request metrics observation requires durable serving mode")
    if not _refresh_lock.acquire(blocking=False):
        return None
    con = None
    timer = None
    finished = threading.Event()
    try:
        from backend.core.duckdb import _safe_table_name, open_serving_connection
        from backend.core.metadata.cron_log import latest_cron_per_task
        from backend.core.query_instrumentation import InstrumentedDuckDBConnection
        from backend.utils.telemetry import process_context_scope

        service_id = source["name"]
        with process_context_scope("request_metrics_observer"):
            if stop_event is not None and stop_event.is_set():
                return None
            # Same fresh, read-only, durable-only view as analytics. In
            # particular do NOT use get_sync_status's local parquet fast path.
            con = open_serving_connection(source, max_wait=5, skip_view_update=False)
            if stop_event is not None and stop_event.is_set():
                return None
            with _query_lock:
                _cancelled.clear()
                _active_query = con
            timer = threading.Timer(QUERY_TIMEOUT_S, _interrupt_until_finished, args=(finished,))
            timer.daemon = True
            timer.start()
            row = (
                InstrumentedDuckDBConnection(con, service_id=service_id)
                .execute(f'SELECT count(*), min(timestamp), max(timestamp) FROM lake."{_safe_table_name(service_id)}"')
                .fetchone()
            )
            if row is None:
                raise RuntimeError("request metrics aggregate returned no result")
            if _cancelled.is_set() or (stop_event is not None and stop_event.is_set()):
                raise InterruptedError("request metrics observation cancelled")
            cron = latest_cron_per_task(service_id)
            current = config.get_status(service_id) or {}
            rum = _read_durable_rum_metrics(con, source)
            if rum is None:
                rum = (extra_status or {}).get("rum", current.get("rum"))
            rum = dict(rum or {})
            rum.setdefault("last_sync_at", None)
            rum_cron_times: list[str] = []
            for task in (
                cron.get("rum_discovery", {}),
                cron.get("rum_sync", {}),
                cron.get("ledger_rum_sweep", {}),
            ):
                started_at = task.get("started_at")
                if isinstance(started_at, str):
                    rum_cron_times.append(started_at)
            if rum_cron_times:

                def cron_timestamp(value: str) -> float:
                    parsed = parse_iso_utc(value)
                    return parsed.timestamp() if parsed is not None else float("-inf")

                latest_rum_cron = max(
                    rum_cron_times,
                    key=cron_timestamp,
                )
                current_rum_sync = rum.get("last_sync_at")
                current_rum_dt = parse_iso_utc(current_rum_sync) if current_rum_sync else None
                latest_rum_cron_dt = parse_iso_utc(latest_rum_cron)
                if current_rum_dt is None or (latest_rum_cron_dt is not None and latest_rum_cron_dt > current_rum_dt):
                    rum["last_sync_at"] = latest_rum_cron
            rum_rows = (rum or {}).get("total_rows") or 0
            earliest = row[1].isoformat() if row[1] is not None else None
            latest = row[2].isoformat() if row[2] is not None else None
            observation = {
                "earliest_log_at": earliest,
                "latest_log_at": latest,
                "local_rows": int(row[0]) + rum_rows,
                "rum": rum,
                "request": {
                    "latest_log_at": latest,
                    "total_rows": int(row[0]),
                    "last_sync_at": cron.get("log_discovery", {}).get("started_at")
                    or cron.get("sync", {}).get("started_at"),
                },
            }
            if _cancelled.is_set() or (stop_event is not None and stop_event.is_set()):
                raise InterruptedError("request metrics observation cancelled")
            # Heavy refresh extras (RUM/schema/etc.) are merged under this same
            # lock; a competing config read-modify-write cannot restore an old
            # request snapshot after the observer has persisted a new one.
            config.update_status(service_id, {**(extra_status or {}), **observation})
            # Include RUM in change detection without overwriting its producer.
            return {**observation, "rum": rum}
    finally:
        finished.set()
        if timer is not None:
            timer.cancel()
            timer.join()
        with _query_lock:
            _active_query = None
        try:
            if con is not None:
                con.close()
        finally:
            _refresh_lock.release()


class RequestMetricsObserver:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._pass_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._published: dict[str, dict] = {}
        self._failure_backoff_s: dict[str, float] = {}
        self._next_attempt_mono: dict[str, float] = {}

    def reconcile(self) -> None:
        if not self._pass_lock.acquire(blocking=False):
            return
        try:
            for cfg in config.list_configs():
                if self._stop.is_set():
                    break
                source = config.config_to_source(cfg)
                if not config.is_durable_serving_mode(source):
                    continue
                service_id = source["name"]
                if get_high_scale_service_registry().resolve(service_id) is not None:
                    continue
                if time.monotonic() < self._next_attempt_mono.get(service_id, 0.0):
                    continue
                try:
                    observed = refresh_durable_request_metrics(source, stop_event=self._stop)
                    if observed is None:
                        continue
                    self._failure_backoff_s.pop(service_id, None)
                    self._next_attempt_mono.pop(service_id, None)
                    if observed == self._published.get(service_id):
                        continue
                    snapshot = compute_sync_status_cached(service_id)
                    if snapshot is not None and not self._stop.is_set():
                        # The shared refresh has already persisted this success.
                        publisher.publish(service_id, snapshot)
                        self._published[service_id] = observed
                        logger.info(
                            "request_metrics.published service=%s rows=%s",
                            service_id,
                            observed["request"]["total_rows"],
                        )
                except Exception:
                    delay = min(
                        self._failure_backoff_s.get(service_id, FAILURE_BACKOFF_BASE_S),
                        FAILURE_BACKOFF_MAX_S,
                    )
                    self._failure_backoff_s[service_id] = min(delay * 2, FAILURE_BACKOFF_MAX_S)
                    self._next_attempt_mono[service_id] = time.monotonic() + delay
                    logger.warning("request_metrics.refresh_failed service=%s", service_id, exc_info=True)
        finally:
            self._pass_lock.release()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    self.reconcile()
                except Exception:
                    logger.warning("request_metrics.reconcile_failed", exc_info=True)
                # No queued ticks: a slow pass coalesces all intervening batches.
                self._stop.wait(RECONCILE_INTERVAL_S)
        finally:
            from backend.core.metadata.base import release_thread_connection

            release_thread_connection()

    def start(self) -> None:
        if not config.is_durable_serving_mode():
            return
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._published.clear()
            self._failure_backoff_s.clear()
            self._next_attempt_mono.clear()
            self._thread = threading.Thread(target=self._run, name="request-metrics-observer", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop.set()
            interrupt_request_metrics()
            if self._thread is not None:
                self._thread.join(timeout=STOP_TIMEOUT_S)
                if self._thread.is_alive():
                    # Keep the handle: start() must not create an overlapping
                    # observer while a dependency is still unwinding.
                    logger.warning("request_metrics.stop_timeout")
                else:
                    self._thread = None


observer = RequestMetricsObserver()

_self_mod = _sys.modules[__name__]
_sys.modules["backend.request_metrics_observer"] = _self_mod
if "backend" in _sys.modules:
    _sys.modules["backend"].request_metrics_observer = _self_mod  # type: ignore[attr-defined]
