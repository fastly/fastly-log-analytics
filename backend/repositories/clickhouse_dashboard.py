"""Archived experimental evaluator for direct, offline library comparisons.

ADR-20 rejected this measured prototype for dashboard serving. No live endpoint
imports or routes through this module; CLICKHOUSE_ENABLED enables diagnostic
index/replay tooling, never dashboard dispatch. Retain the bounded hybrid
evaluation and its correctness checks to reproduce the experimental comparison.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from uuid import uuid4

import structlog

from backend import config
from backend.core.clickhouse_client import ClickHouseClient, get_clickhouse_client
from backend.core.clickhouse_export import source_catalog_identity
from backend.core.clickhouse_publication import readiness
from backend.core.iceberg import get_arrow_schema
from backend.core.iceberg._ducklake import ducklake_table_name
from backend.core.iceberg.view import _finalize_view_sql
from backend.repositories import dashboard
from backend.repositories._base import SectionTimer, _safe_table
from backend.utils.date_utils import parse_iso_utc, safe_iso

logger = structlog.get_logger(__name__)

_INTERVALS = {"1 second": "1 SECOND", "1 minute": "1 MINUTE", "1 hour": "1 HOUR", "1 day": "1 DAY"}
_WHERE = (
    "service_id={service:String} AND generation={generation:String} "
    "AND timestamp >= {start:DateTime64(6, 'UTC')} AND timestamp <= {end:DateTime64(6, 'UTC')} "
    "AND ip IS NOT NULL AND ip != '' AND url IS NOT NULL "
    "AND url != '/rum-beacon' AND url NOT LIKE '/rum-beacon?%'"
)


class HybridUnavailable(RuntimeError):
    """An attempted hybrid request failed; never report a success-shaped zero."""


class _Ineligible(ValueError):
    """A healthy source does not support this bounded prototype."""


def query_slice(
    client: ClickHouseClient, service: str, generation: str, start: datetime, end: datetime, interval: str
) -> dict:
    params = {"service": service, "generation": generation, "start": start, "end": end}
    # FINAL must precede every aggregate: publication retries can still have
    # duplicate physical rows until background merges run.
    total = client.execute("SELECT count(country) AS n FROM log_facts FINAL WHERE " + _WHERE, params)[0]["n"]
    top = client.execute(
        "SELECT country AS value, count() AS count FROM log_facts FINAL WHERE "
        + _WHERE
        + " AND country IS NOT NULL AND country != '' GROUP BY country ORDER BY count DESC LIMIT 10",
        params,
    )
    series = client.execute(
        f"SELECT toStartOfInterval(timestamp, INTERVAL {_INTERVALS[interval]}, 'UTC') AS bucket, "
        "count() AS value FROM log_facts FINAL WHERE " + _WHERE + " GROUP BY bucket ORDER BY bucket",
        params,
    )
    return {
        "country": {"top": [{"value": r["value"], "count": int(r["count"])} for r in top], "total": int(total)},
        "time_series": [
            {"time": safe_iso(parse_iso_utc(str(r["bucket"]))), "value": float(r["value"])} for r in series
        ],
    }


def _ineligible(kwargs: dict, sections: set[str] | None) -> str | None:
    if config.load_clickhouse_config() is None:
        return "disabled"
    if not config.is_durable_serving_mode(kwargs["src"]):
        return "non_durable_connection"
    if kwargs.get("fields_filter") != ["country"]:
        return "unsupported_fields"
    if sections not in (None, {"core", "topten"}) or any(
        kwargs.get(flag) is False
        for flag in ("include_time_series", "include_conn_requests", "include_map_data", "include_top_n")
    ):
        return "unsupported_sections"
    if kwargs["chart_metric"] != "requests":
        return "unsupported_metric"
    if kwargs["chart_interval"] not in _INTERVALS:
        return "unsupported_interval"
    if kwargs["filters"]:
        return "unsupported_filters"
    if not kwargs["start_time"] or not kwargs["end_time"]:
        return "unbounded_window"
    # Source-level manual-import/Path-A clamps live in the view, independently
    # of the already-applied RequestContext invite clamp. Do not bypass them.
    if kwargs["src"].get("time_range"):
        return "source_time_scope"
    return None


@contextmanager
def pinned_view(con, source: dict) -> Iterator[tuple[str, int]]:
    """A unique normalized view on the exclusive ephemeral serving connection.

    Never replace logs_<service>, or touch the shared view/schema caches.
    AT VERSION pins all DuckLake reads even if a writer commits meanwhile.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        row = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()
        if row is None or row[0] is None:
            raise _Ineligible("source_snapshot_unavailable")
        snapshot = int(row[0])
        if snapshot < 0:
            raise ValueError("invalid source snapshot")
        table = ducklake_table_name(source)
        if _safe_table(table) != f"logs_{table}":
            raise ValueError("invalid source table")
        if not con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND schema_name='main' AND table_name=?",
            [table],
        ).fetchone():
            raise _Ineligible("unsupported_schema")
        raw_sql = f'SELECT * FROM lake."{table}" AT (VERSION => {snapshot})'
        columns = {c[0] for c in con.execute(raw_sql + " LIMIT 0").description}
        if not {"timestamp", "country", "ip", "url", "conn_requests"} <= columns:
            raise _Ineligible("unsupported_schema")
        cfg = config.load_config(source.get("service_id") or source["name"])
        schema = get_arrow_schema(cfg.get("log_fields", {}) if cfg else None)
        sql = _finalize_view_sql(raw_sql, source, "logs", {f.name for f in schema}, columns)
        view = _safe_table("ch_request_" + uuid4().hex)
        con.execute(f"CREATE TEMP VIEW {view} AS {sql}")
        yield view, snapshot
    finally:
        # Rolls back only this read transaction, including its temporary DDL.
        con.execute("ROLLBACK")


def get_aggregates(*, sections: set[str] | None = None, **kwargs) -> dict:
    """Dispatch AFTER the router has enforced tenancy and clamped the window."""
    reason = _ineligible(kwargs, sections)
    if reason is None:
        start = parse_iso_utc(kwargs["start_time"])
        end = parse_iso_utc(kwargs["end_time"])
        if start is None or end is None or start > end:
            reason = "invalid_window"
    if reason is not None:
        logger.info("clickhouse.dashboard_dispatch", engine="ducklake", reason=reason)
        return dashboard.get_aggregates(**kwargs, _dispatch_reason=reason)

    timer = SectionTimer()
    try:
        con, src = kwargs["con"], kwargs["src"]
        path = con.execute("SELECT path FROM duckdb_databases() WHERE database_name=current_database()").fetchone()
        if path is None or path[0]:
            reason = "non_ephemeral_connection"
        else:
            with pinned_view(con, src) as (view, snapshot):
                client = get_clickhouse_client()
                if client is None:
                    raise HybridUnavailable("ClickHouse disabled during request")
                assert start is not None and end is not None
                ready = timer.call(
                    "clickhouse:readiness",
                    lambda: readiness(
                        src["service_id"],
                        start=start,
                        end=end,
                        source_snapshot=snapshot,
                        catalog_identity=source_catalog_identity(),
                        source_table=ducklake_table_name(src),
                        client=client,
                    ),
                )
                if ready.eligible:
                    assert ready.generation is not None
                    selected = timer.call(
                        "clickhouse:time_series_country",
                        lambda: query_slice(
                            client, src["service_id"], ready.generation, start, end, kwargs["chart_interval"]
                        ),
                    )
                    result = dashboard.get_aggregates(
                        **kwargs, _query_table=view, _selected_aggregates=selected, _dispatch_reason="hybrid"
                    )
                    result["section_timings"].extend(timer.entries)
                    logger.info("clickhouse.dashboard_dispatch", engine="hybrid", reason="ready")
                    return result
                reason = ready.reason
    except _Ineligible as exc:
        reason = str(exc)
    except Exception as exc:
        logger.error(
            "clickhouse.dashboard_dispatch", engine="hybrid", reason="unavailable", error_kind=type(exc).__name__
        )
        raise HybridUnavailable("Hybrid dashboard temporarily unavailable") from None
    logger.info("clickhouse.dashboard_dispatch", engine="ducklake", reason=reason)
    result = dashboard.get_aggregates(**kwargs, _dispatch_reason=reason)
    result.setdefault("section_timings", []).extend(timer.entries)
    return result
