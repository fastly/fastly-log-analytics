"""Streaming content-security repository — who is pulling your content, and from where.

Backs the Streaming → Content Security tab: top countries / referers / hosts and edge-vs-shield bandwidth, optionally narrowed to one CMCD content
id (``cmcd_cid``).
"""

from __future__ import annotations

import time as _time
from typing import Any

import duckdb

from backend.models.common import FiltersDict, FilterSpec
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories.cmcd import _bucket_expr, _pad_timeseries
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    bucket_time_to_minute,
    cache_get,
    cache_put,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import BoundedTTLCache

_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128
_response_cache: BoundedTTLCache = BoundedTTLCache(maxsize=_RESPONSE_CACHE_MAXSIZE, ttl_seconds=_RESPONSE_CACHE_TTL)

# Leaderboard column → response section.
_TOP_N_SECTIONS = {
    "country": "top_countries",
    "referer": "top_referers",
    "host": "top_hosts",
}

# Columns the tab can use; each is optional and its section degrades to empty.
_OPTIONAL_COLS = ("country", "referer", "host", "resp_bytes", "edge", "cmcd_cid")

# Cap on the content-id picker's option list.
_CONTENT_ID_OPTIONS_LIMIT = 100


def _response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    content_id: str | None,
    bucket_seconds: int,
    top_n: int,
) -> str:
    # Key field order is load-bearing (serialized as-is): s, e, f, cid, bs, tn.
    payload = {
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "cid": content_id,
        "bs": bucket_seconds,
        "tn": top_n,
    }
    return digest_cache_key(payload, src)


def get_content_security(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    content_id: str | None = None,
    bucket_seconds: int = 300,
    top_n: int = 10,
) -> dict[str, Any]:
    """Return content-security aggregates for the window.

    Leaderboards count only edge log lines (``edge = true``) when the column
    exists: a cache miss also produces a shield log line for the same client
    request, which would otherwise double-count it. Bandwidth splits
    ``resp_bytes`` by that same flag.
    """
    timer = SectionTimer()
    section_timings = timer.entries
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t)

    fields = {col: bool(actual_cols) and col in actual_cols for col in _OPTIONAL_COLS}
    if not actual_cols:
        return {"available": False, "fields": fields, "section_timings": section_timings, **runner.telemetry()}

    cache_key = _response_cache_key(src, start_time, end_time, filters, content_id, bucket_seconds, top_n)
    cached = cache_get(_response_cache, cache_key)
    if cached is not None:
        return {**cached, "section_timings": section_timings, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    has_edge = fields["edge"]
    has_bytes = fields["resp_bytes"]
    has_cid = fields["cmcd_cid"]
    edge_where = "edge = true" if has_edge else "1=1"

    results: dict[str, Any] = {
        "available": True,
        "fields": fields,
        "content_id": content_id,
        "has_shield_split": has_edge,
        **runner.telemetry(),
    }

    # ── Content-id picker options: global filters only, never the content filter ──
    results["content_ids"] = []
    if has_cid:
        _t = _time.perf_counter()
        base_params, base_where = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
        rows = runner.execute(
            f"""
            SELECT cmcd_cid, COUNT(*) AS requests
            FROM {table_name}
            WHERE {base_where} AND {edge_where} AND cmcd_cid IS NOT NULL AND cmcd_cid != ''
            GROUP BY 1
            ORDER BY requests DESC, cmcd_cid
            LIMIT {_CONTENT_ID_OPTIONS_LIMIT}
            """,
            base_params or None,
        ).fetchall()
        results["content_ids"] = [{"content_id": r[0], "requests": r[1]} for r in rows]
        timer.mark("content_ids", _t)

    # The content id goes through build_where_clause like any other filter, so
    # it is escaped there, and on a schema without cmcd_cid it matches nothing.
    scoped_filters: FiltersDict = dict(filters)
    if content_id:
        scoped_filters["cmcd_cid"] = FilterSpec(mode="include", values=[content_id])

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, scoped_filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t)

    temp_cols = ["timestamp", *[c for c in _OPTIONAL_COLS if c != "cmcd_cid" and fields[c]]]

    _t = _time.perf_counter()
    with runner.temp_table(temp_cols, actual_cols, table_name, where_clause, params) as t:
        timer.mark("temp_table_create", _t)
        if t is None:
            return {
                "available": False,
                "fields": fields,
                "reason": "Data temporarily unavailable — view refresh failed. Retry in a moment.",
                **runner.telemetry(),
            }

        bytes_agg = "SUM(resp_bytes)" if has_bytes else "NULL"

        # ── Leaderboards ──────────────────────────────────────────────────
        for col, section in _TOP_N_SECTIONS.items():
            results[section] = []
            if not fields[col]:
                continue
            _t = _time.perf_counter()
            rows = runner.execute(
                f"""
                SELECT "{col}" AS value, COUNT(*) AS requests, {bytes_agg} AS bytes
                FROM {t}
                WHERE {edge_where} AND "{col}" IS NOT NULL AND "{col}" != ''
                GROUP BY 1
                ORDER BY requests DESC, value
                LIMIT {int(top_n)}
                """
            ).fetchall()
            results[section] = [
                {"value": r[0], "requests": r[1], "bytes": int(r[2]) if r[2] is not None else None} for r in rows
            ]
            timer.mark(section, _t)

        # ── Edge vs shield bandwidth ──────────────────────────────────────
        results["bandwidth_ts"] = []
        if has_bytes:
            _t = _time.perf_counter()
            if has_edge:
                split = (
                    "COALESCE(SUM(resp_bytes) FILTER (WHERE edge = true), 0) AS edge_bytes, "
                    "COALESCE(SUM(resp_bytes) FILTER (WHERE edge IS DISTINCT FROM true), 0) AS shield_bytes"
                )
            else:
                split = "COALESCE(SUM(resp_bytes), 0) AS edge_bytes, NULL AS shield_bytes"
            rows = runner.execute(
                f"""
                SELECT strftime({_bucket_expr(bucket_seconds * 1000)}, '%Y-%m-%d %H:%M:%S') AS bucket, {split}
                FROM {t}
                GROUP BY 1
                """
            ).fetchall()
            by_bucket = {
                r[0]: {"edge_bytes": int(r[1]), "shield_bytes": int(r[2]) if r[2] is not None else None} for r in rows
            }
            default = {"edge_bytes": 0, "shield_bytes": 0 if has_edge else None}
            results["bandwidth_ts"] = _pad_timeseries(start_time, end_time, bucket_seconds, by_bucket, default)
            timer.mark("bandwidth_ts", _t)

    results["section_timings"] = section_timings
    cache_put(_response_cache, cache_key, results, strip=("section_timings",))
    return results


from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("content_security._response_cache", _response_cache)
