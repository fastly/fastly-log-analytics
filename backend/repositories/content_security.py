"""Streaming content-security repository — who is pulling your content, and from where.

Backs the Streaming → Content Security tab: top countries / referers / hosts, top subscriber ids by distinct
client IPs (``cs_sub_id``, Streaming → Token), every proxy type / description (``p_type`` / ``p_desc``, Group I), and
edge-vs-shield bandwidth, the enforcement outcome (``cs_status`` / ``cs_mode`` / ``cs_rl_mode``, the block, throttle
and rate-limit result the operator's VCL stamps on each request), optionally narrowed to one content id (``cs_content_id``, Streaming → Token → Content ID).

Subscriber ids are returned raw; analyst masking happens in the router, after
this module's response cache (which is shared across roles).
"""

from __future__ import annotations

import datetime as _dt
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

# Uncapped breakdown column → response section. client.geo.proxy_type and
# client.geo.proxy_description each have ~10 possible values, so listing every
# one is cheap; "" is logged for a non-proxy IP and is excluded.
_ALL_VALUES_SECTIONS = {
    "p_type": "proxy_types",
    "p_desc": "proxy_descriptions",
}

# Columns the tab can use; each is optional and its section degrades to empty.
_OPTIONAL_COLS = (
    "country",
    "referer",
    "host",
    "resp_bytes",
    "edge",
    "cs_content_id",
    "ip",
    "cs_sub_id",
    "cs_status",
    "cs_mode",
    "cs_rl_mode",
    *_ALL_VALUES_SECTIONS,
)

# Cap on the content-id picker's option list.
_CONTENT_ID_OPTIONS_LIMIT = 100


def _enforced_expr(fields: dict[str, bool]) -> str:
    """SQL predicate: the row's status was enforced rather than only recorded (WARN)."""
    block = "cs_mode = 'ENFORCE'" if fields["cs_mode"] else "false"
    rate = "cs_rl_mode = 'ENFORCE'" if fields["cs_rl_mode"] else "false"
    return f"((cs_status IN ('blocked', 'throttled') AND {block}) OR (cs_status = 'rate_limited' AND {rate}))"


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
    has_cid = fields["cs_content_id"]
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
            SELECT cs_content_id, COUNT(*) AS requests
            FROM {table_name}
            WHERE {base_where} AND {edge_where} AND cs_content_id IS NOT NULL AND cs_content_id != ''
            GROUP BY 1
            ORDER BY requests DESC, cs_content_id
            LIMIT {_CONTENT_ID_OPTIONS_LIMIT}
            """,
            base_params or None,
        ).fetchall()
        results["content_ids"] = [{"content_id": r[0], "requests": r[1]} for r in rows]
        timer.mark("content_ids", _t)

    # The content id goes through build_where_clause like any other filter, so
    # it is escaped there, and on a schema without cs_content_id it matches nothing.
    scoped_filters: FiltersDict = dict(filters)
    if content_id:
        scoped_filters["cs_content_id"] = FilterSpec(mode="include", values=[content_id])

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, scoped_filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t)

    temp_cols = ["timestamp", *[c for c in _OPTIONAL_COLS if c != "cs_content_id" and fields[c]]]

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
        # Users = distinct (subscriber id, client IP) pairs; a request with no
        # subscriber id counts by its IP alone. Without the token column every
        # request is the IP-only case.
        if fields["ip"] and fields["cs_sub_id"]:
            users_agg = "COUNT(DISTINCT (COALESCE(NULLIF(cs_sub_id, ''), ''), ip))"
        elif fields["ip"]:
            users_agg = "COUNT(DISTINCT ip)"
        else:
            users_agg = "NULL"

        # ── Leaderboards ──────────────────────────────────────────────────
        for col, section in _TOP_N_SECTIONS.items():
            results[section] = []
            if not fields[col]:
                continue
            _t = _time.perf_counter()
            rows = runner.execute(
                f"""
                SELECT "{col}" AS value, {users_agg} AS users, COUNT(*) AS requests, {bytes_agg} AS bytes
                FROM {t}
                WHERE {edge_where} AND "{col}" IS NOT NULL AND "{col}" != ''
                GROUP BY 1
                ORDER BY requests DESC, value
                LIMIT {int(top_n)}
                """
            ).fetchall()
            results[section] = [
                {"value": r[0], "users": r[1], "requests": r[2], "bytes": int(r[3]) if r[3] is not None else None}
                for r in rows
            ]
            timer.mark(section, _t)

        # ── Every proxy type / description (no top-N cap) ───────────────
        for col, section in _ALL_VALUES_SECTIONS.items():
            results[section] = []
            if not fields[col]:
                continue
            _t = _time.perf_counter()
            rows = runner.execute(
                f"""
                SELECT "{col}" AS value, {users_agg} AS users, COUNT(*) AS requests, {bytes_agg} AS bytes
                FROM {t}
                WHERE {edge_where} AND "{col}" IS NOT NULL AND "{col}" != ''
                GROUP BY 1
                ORDER BY requests DESC, value
                """
            ).fetchall()
            results[section] = [
                {"value": r[0], "users": r[1], "requests": r[2], "bytes": int(r[3]) if r[3] is not None else None}
                for r in rows
            ]
            timer.mark(section, _t)

        # ── Subscriber ids shared across the most client IPs ─────────────
        results["top_subscribers"] = []
        if fields["cs_sub_id"] and fields["ip"]:
            _t = _time.perf_counter()
            countries_agg = 'COUNT(DISTINCT "country")' if fields["country"] else "NULL"
            rows = runner.execute(
                f"""
                SELECT cs_sub_id AS subscriber_id,
                       COUNT(DISTINCT ip) AS distinct_ips,
                       {countries_agg} AS distinct_countries,
                       COUNT(*) AS requests,
                       {bytes_agg} AS bytes,
                       MAX(timestamp) AS last_seen
                FROM {t}
                WHERE {edge_where} AND cs_sub_id IS NOT NULL AND cs_sub_id != ''
                GROUP BY 1
                ORDER BY distinct_ips DESC, requests DESC, subscriber_id
                LIMIT {int(top_n)}
                """
            ).fetchall()
            results["top_subscribers"] = [
                {
                    "subscriber_id": r[0],
                    "distinct_ips": r[1],
                    "distinct_countries": r[2],
                    "requests": r[3],
                    "bytes": int(r[4]) if r[4] is not None else None,
                    # TIMESTAMPTZ comes back in the session zone; send UTC.
                    "last_seen": r[5].astimezone(_dt.UTC).isoformat() if r[5] is not None else None,
                }
                for r in rows
            ]
            timer.mark("top_subscribers", _t)

        # ── Enforcement outcomes (block / throttle / rate limit) ──────────
        # ``cs_status`` is set by the operator's VCL. A status only counts as
        # *enforced* when its mode column says ENFORCE (cs_mode for blocked /
        # throttled, cs_rl_mode for rate_limited); WARN rows are recorded but
        # the request was served, so the report must not claim they were stopped.
        results["enforcement"] = []
        results["enforcement_ts"] = []
        if fields["cs_status"]:
            _t = _time.perf_counter()
            enforced_expr = _enforced_expr(fields)
            rows = runner.execute(
                f"""
                SELECT cs_status AS status,
                       COUNT(*) AS requests,
                       COUNT(*) FILTER (WHERE {enforced_expr}) AS enforced,
                       {users_agg} AS users,
                       {bytes_agg} AS bytes
                FROM {t}
                WHERE {edge_where} AND cs_status IS NOT NULL AND cs_status != ''
                GROUP BY 1
                ORDER BY requests DESC, status
                """
            ).fetchall()
            results["enforcement"] = [
                {
                    "status": r[0],
                    "requests": r[1],
                    "enforced": r[2],
                    "users": r[3],
                    "bytes": int(r[4]) if r[4] is not None else None,
                }
                for r in rows
            ]
            rows = runner.execute(
                f"""
                SELECT strftime({_bucket_expr(bucket_seconds * 1000)}, '%Y-%m-%d %H:%M:%S') AS bucket,
                       cs_status AS status, COUNT(*) AS requests
                FROM {t}
                WHERE {edge_where} AND cs_status IS NOT NULL AND cs_status != ''
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            ).fetchall()
            results["enforcement_ts"] = [{"bucket": r[0], "status": r[1], "requests": r[2]} for r in rows]
            timer.mark("enforcement", _t)

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


# Cap on the per-subscriber (IP, country, host) list; a heavily shared credential
# can fan out to thousands of IPs, and the dialog only needs the most recent ones.
_SUBSCRIBER_IPS_LIMIT = 500


def get_subscriber_ips(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    subscriber_id: str,
    content_id: str | None = None,
) -> dict[str, Any]:
    """Return every (client IP, country, host) combination one subscriber id was seen from, most recent first.

    One row per distinct combination, so an IP seen from two countries (or on
    two hosts) is two rows. Edge log lines only, as in
    :func:`get_content_security`. Returns the raw subscriber id and IPs — the
    route is admin-only.
    """
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()
    fields = {col: bool(actual_cols) and col in actual_cols for col in _OPTIONAL_COLS}
    base = {"subscriber_id": subscriber_id, "fields": fields, "ips": [], "truncated": False}
    if not (fields["cs_sub_id"] and fields["ip"]):
        return {**base, "available": False, **runner.telemetry()}

    # Subscriber + content ids go through build_where_clause like any other filter, so they are escaped there.
    scoped_filters: FiltersDict = dict(filters)
    scoped_filters["cs_sub_id"] = FilterSpec(mode="include", values=[subscriber_id])
    if content_id:
        scoped_filters["cs_content_id"] = FilterSpec(mode="include", values=[content_id])
    params, where_clause = build_where_clause(start_time, end_time, scoped_filters, actual_cols, inline_params=True)

    edge_where = "edge = true" if fields["edge"] else "1=1"
    # '' and NULL are the same "unknown" value — one row, not two.
    country_col = """NULLIF("country", '')""" if fields["country"] else "NULL"
    host_col = """NULLIF("host", '')""" if fields["host"] else "NULL"
    bytes_agg = "SUM(resp_bytes)" if fields["resp_bytes"] else "NULL"
    rows = runner.execute(
        f"""
        SELECT ip, {country_col} AS country, {host_col} AS host, COUNT(*) AS requests, {bytes_agg} AS bytes,
               MAX(timestamp) AS last_seen
        FROM {_safe_table(src["name"])}
        WHERE {where_clause} AND {edge_where} AND ip IS NOT NULL AND ip != ''
        GROUP BY 1, 2, 3
        ORDER BY last_seen DESC, requests DESC, ip, country NULLS LAST, host NULLS LAST
        LIMIT {_SUBSCRIBER_IPS_LIMIT + 1}
        """,
        params or None,
    ).fetchall()
    return {
        **base,
        "available": True,
        "ips": [
            {
                "ip": r[0],
                "country": r[1],
                "host": r[2],
                "requests": r[3],
                "bytes": int(r[4]) if r[4] is not None else None,
                # TIMESTAMPTZ comes back in the session zone; send UTC.
                "last_seen": r[5].astimezone(_dt.UTC).isoformat() if r[5] is not None else None,
            }
            for r in rows[:_SUBSCRIBER_IPS_LIMIT]
        ],
        "truncated": len(rows) > _SUBSCRIBER_IPS_LIMIT,
        **runner.telemetry(),
    }


from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("content_security._response_cache", _response_cache)
