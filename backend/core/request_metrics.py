"""Authoritative request extents from the durable analytics serving view."""

from __future__ import annotations

import threading
from typing import Any

from backend import config

QUERY_TIMEOUT_S = 30.0
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
                .execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {_safe_table_name(service_id)}")
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
            rum["last_sync_at"] = (
                rum.get("last_sync_at")
                or cron.get("rum_discovery", {}).get("started_at")
                or cron.get("rum_sync", {}).get("started_at")
                or cron.get("ledger_rum_sweep", {}).get("started_at")
            )
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
