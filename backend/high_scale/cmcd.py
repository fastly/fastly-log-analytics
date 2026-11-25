"""High-scale CMCD projection and aggregate queries."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import (
    normalize_cache_bound,
    parse_time_window,
    range_value,
)
from backend.high_scale.archive import build_cmcd_projection_key
from backend.models.analytics import CmcdRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    client_cache_token,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    CMCD_TS_DEFAULTS,
    empty_cmcd_aggregates,
    enrich_asn_rows,
    opt_float,
    pad_timeseries_buckets,
)
from backend.utils.bounded_cache import CacheRegistry

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService


def project_cmcd(request_event: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_fields = request_event.get("cmcd")
    if raw_fields is None:
        return None
    if not isinstance(raw_fields, Mapping):
        raise ValueError("CMCD fields must be a mapping")
    if not any(v not in (None, "") for v in raw_fields.values()):
        return None
    request_event_id = request_event.get("event_id")
    if not isinstance(request_event_id, str) or not request_event_id:
        raise ValueError("CMCD projection requires request_event_id")
    service_id = request_event.get("service_id")
    if not isinstance(service_id, str) or not service_id:
        raise ValueError("CMCD projection requires service_id")
    projection = {
        "projection_key": build_cmcd_projection_key({"request_event_id": request_event_id}),
        "request_event_id": request_event_id,
        "service_id": service_id,
        "timestamp": request_event.get("timestamp"),
        "cmcd_version": request_event.get("cmcd_version"),
        "sid": raw_fields.get("sid") or raw_fields.get("cmcd_sid") or "",
        "country": _country(request_event.get("country")),
        "asn": _asn(request_event.get("asn")),
        "fields": dict(raw_fields),
    }
    return projection


def _country(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _asn(value: object) -> int | None:
    """A positive AS number, or None: the analytics group by ASN, never by a malformed label."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip()) or None
    return None


_HS_CMCD_CACHE_TTL = float(os.getenv("HS_CMCD_CACHE_TTL_SECONDS", "60"))
_memo = ResponseMemo(ttl=_HS_CMCD_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_cmcd_aggregates_memo", _memo.cache)
_DEFAULT_WINDOW = timedelta(days=1)
_BUCKET_FORMAT = "%Y-%m-%d %H:%M:%S"
_DURATION_BUCKETS = {1: "<30s", 2: "30s–2m", 3: "2–5m", 4: "5–10m", 5: "10–30m", 6: "30m–1h", 7: ">1h"}
_BITRATE_LADDER_TIERS = {
    1: "< 1 Mbps (SD)",
    2: "1–2.5 Mbps (720p)",
    3: "2.5–5 Mbps (1080p)",
    4: "5–10 Mbps (1440p)",
    5: "10+ Mbps (4K)",
}
_FILTER_COLUMNS = {"country": "country", "asn": "asn"}
_EVENTS_SELECT = """
    SELECT event_timestamp AS ts,
           cmcd['sid'] AS sid,
           cmcd['cid'] AS cid,
           cmcd['ot'] AS ot,
           cmcd['sf'] AS sf,
           cmcd['st'] AS st,
           country,
           {asn_expr},
           toFloat64OrNull(cmcd['bl']) AS bl,
           toFloat64OrNull(cmcd['br']) AS br,
           toFloat64OrNull(cmcd['mtp']) AS mtp,
           toFloat64OrNull(cmcd['tb']) AS tb,
           if(cmcd['bs'] = '', NULL, lower(cmcd['bs']) IN ('true', '1')) AS bs,
           if(cmcd['su'] = '', NULL, lower(cmcd['su']) IN ('true', '1')) AS su
    FROM {from_table}
    WHERE {where}
"""
_EVENTS = _EVENTS_SELECT.replace("{from_table}", "cmcd_projection_facts").replace("{asn_expr}", "asn")
_BUCKET = (
    "formatDateTime(toStartOfInterval({column}, INTERVAL {{bucket_seconds:UInt32}} SECOND), '%Y-%m-%d %H:%i:%S', 'UTC')"
)
_VIDEO = "ot = 'v'"


def _asn_names(service_id: str, asns: list[int]) -> dict[int, str]:
    from backend.core import fos_client as _db

    return _db.get_asn_names(service_id, asns)


def _asn_label(asn: int, names: Mapping[int, str]) -> str:
    from backend.core import fos_client as _db

    return _db.format_asn_label(asn, names.get(asn, ""))


def _spec(config: Any) -> tuple[str, list[Any]]:
    if isinstance(config, Mapping):
        return str(config.get("mode", "include")), list(config.get("values") or [])
    return str(getattr(config, "mode", "include")), list(getattr(config, "values", None) or [])


def _filter_clauses(filters: Mapping[str, Any]) -> tuple[list[str], dict[str, Any], bool]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    request_filters: dict[str, Any] = {}
    direct_filters: list[tuple[int, str, str, list[Any]]] = []
    for i, (name, config) in enumerate(filters.items()):
        if name == "filter_mode":
            continue
        mode, values = _spec(config)
        if not values:
            continue
        if name not in _FILTER_COLUMNS:
            request_filters[name] = config
            continue
        direct_filters.append((i, name, mode, values))

    use_request_facts = bool(request_filters)
    if use_request_facts:
        from ._common import request_filter_clauses

        req_clauses, req_params = request_filter_clauses(request_filters, prefix="cmcd_req_flt")
        params.update(req_params)
        clauses.extend(req_clauses)

    asn_expr = "toInt32OrNull(nullIf(custom_fields['asn'], ''))" if use_request_facts else "asn"
    for i, name, mode, values in direct_filters:
        keys: list[str] = []
        for j, value in enumerate(values):
            param = f"flt_{i}_{j}"
            if name == "asn":
                if not str(value).strip().isdigit():
                    raise ValueError("asn filter values must be numeric AS numbers")
                params[param] = int(str(value).strip())
                keys.append(f"{{{param}:UInt32}}")
            else:
                params[param] = str(value)
                keys.append(f"{{{param}:String}}")
        operator = "NOT IN" if mode == "exclude" else "IN"
        col = asn_expr if name == "asn" else _FILTER_COLUMNS[name]
        clauses.append(f"{col} {operator} ({', '.join(keys)})")
    return clauses, params, use_request_facts


def _cmcd_scope(
    service: HighScaleService, req: CmcdRequest, start: datetime, end: datetime
) -> tuple[str, str, str, dict[str, Any]]:
    filter_clauses, filter_params, use_request_facts = _filter_clauses(req.filters or {})
    domain = "request" if use_request_facts else "cmcd"
    from_table = "request_facts" if use_request_facts else "cmcd_projection_facts"
    asn_expr = "toInt32OrNull(nullIf(custom_fields['asn'], '')) AS asn" if use_request_facts else "asn"
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' AND publication_state='visible')",
        "event_timestamp >= {start:DateTime64(3)}",
        "event_timestamp < {end:DateTime64(3)}",
        "cmcd['sid'] != ''",
        *filter_clauses,
    ]
    params = {
        "service_id": service.service_id,
        "start": start,
        "end": end,
        "bucket_seconds": int(req.bucket_seconds),
        "top_n": int(req.top_n),
        **filter_params,
    }
    return from_table, asn_expr, " AND ".join(clauses), params


def _round(value: Any) -> float | None:
    return opt_float(value, 2)


def _pad(
    start: datetime, end: datetime, bucket_seconds: int, rows: dict[str, dict[str, Any]], default: dict[str, Any]
) -> list[dict[str, Any]]:
    return pad_timeseries_buckets(start, end, bucket_seconds, rows, default, end_inclusive=False)


def _sessions_ts(rows: Iterable[dict[str, Any]], new_sessions: Mapping[str, int]) -> dict[str, dict[str, Any]]:
    return {
        str(row["bucket"]): {
            "concurrent_sessions": int(row["concurrent_sessions"] or 0),
            "rebuffer_session_pct": _round(row.get("rebuffer_session_pct")),
            "new_sessions": new_sessions.get(str(row["bucket"]), 0),
        }
        for row in rows
    }


def _cmcd_cache_bound(value: str | datetime | None) -> str | None:
    """Floor a CMCD window bound to the UTC minute so warm reloads within the same minute hit cache."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo is not None else value
        return dt.strftime("%Y-%m-%dT%H:%M")
    stripped = str(value).strip()
    if not stripped:
        return None
    try:
        parsed = range_value(stripped)
        if parsed is not None:
            dt = parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed
            return dt.strftime("%Y-%m-%dT%H:%M")
    except (ValueError, TypeError):
        pass
    return bucket_time_to_minute(normalize_cache_bound(stripped))


def _cmcd_cache_bounds(
    start_time: str | datetime | None,
    end_time: str | datetime | None,
) -> tuple[str | None, str | None]:
    """Bucket >= 1 minute windows to the UTC minute; preserve exact bounds for sub-minute windows."""
    try:
        start_dt = range_value(start_time)
        end_dt = range_value(end_time)
    except (ValueError, TypeError):
        return normalize_cache_bound(start_time), normalize_cache_bound(end_time)
    if start_dt is not None and end_dt is not None:
        s_utc = start_dt.astimezone(UTC) if start_dt.tzinfo is not None else start_dt.replace(tzinfo=UTC)
        e_utc = end_dt.astimezone(UTC) if end_dt.tzinfo is not None else end_dt.replace(tzinfo=UTC)
        if abs((e_utc - s_utc).total_seconds()) < 60:
            return normalize_cache_bound(start_time), normalize_cache_bound(end_time)
    return _cmcd_cache_bound(start_time), _cmcd_cache_bound(end_time)


def cmcd_aggregates(
    service: HighScaleService,
    req: CmcdRequest,
    start_time: str | None,
    end_time: str | None,
    sections: set[Any] | None,
    mask_ips: bool,
) -> dict[str, Any]:
    """Return the standard CMCD response shape; ``mask_ips`` is moot because no section emits an IP."""
    cache_start, cache_end = _cmcd_cache_bounds(start_time, end_time)
    digest = digest_cache_key(
        {
            "ep": "hs_cmcd_aggregates",
            "cid": client_cache_token(service.client),
            "s": cache_start,
            "e": cache_end,
            "b": int(req.bucket_seconds),
            "n": int(req.top_n),
            "sec": sorted(str(s) for s in sections) if sections else None,
            "m": bool(mask_ips),
            "f": serialize_filters_for_key(req.filters or {}),
        },
        {"name": service.service_id, "service_id": service.service_id},
    )
    memo_key = f"{service.service_id}:{digest}"
    if _HS_CMCD_CACHE_TTL > 0:
        cached_resp = _memo.get_with_telemetry(memo_key)
        if cached_resp is not None:
            return dict(cached_resp)

    def _compute() -> dict[str, Any]:
        if _HS_CMCD_CACHE_TTL > 0 and (cached_inner := _memo.get_with_telemetry(memo_key)) is not None:
            return dict(cached_inner)
        payload = _cmcd_aggregates_compute(service, req, start_time, end_time, sections, mask_ips)
        if _HS_CMCD_CACHE_TTL > 0:
            _memo.put(memo_key, payload, strip=("section_timings",))
        return payload

    if _HS_CMCD_CACHE_TTL > 0:
        return _memo.coalesce(f"hs_cmcd_agg:{memo_key}", _compute)
    return _memo.result(_compute(), state="bypass")


def _cmcd_aggregates_compute(
    service: HighScaleService,
    req: CmcdRequest,
    start_time: str | None,
    end_time: str | None,
    sections: set[Any] | None,
    mask_ips: bool,
) -> dict[str, Any]:
    start, end = parse_time_window(start_time, end_time, _DEFAULT_WINDOW, utc=True)
    from_table, asn_expr, where, params = _cmcd_scope(service, req, start, end)
    events = _EVENTS_SELECT.format(from_table=from_table, asn_expr=asn_expr, where=where)
    execute = service.client.execute

    def wanted(name: str) -> bool:
        return sections is None or name in sections

    trace = HighScaleSections(service.service_id, "cmcd")
    want_overview = wanted("overview")
    probe_select = [
        "count() AS events",
        "uniqExact(sid) AS sessions",
        "countIf(bl IS NOT NULL) AS has_bl",
        "countIf(br IS NOT NULL) AS has_br",
        "countIf(bs IS NOT NULL) AS has_bs",
        "countIf(mtp IS NOT NULL) AS has_mtp",
        "countIf(ot != '') AS has_ot",
        "countIf(sf != '') AS has_sf",
        "countIf(st != '') AS has_st",
        "countIf(su IS NOT NULL) AS has_su",
        "countIf(tb IS NOT NULL) AS has_tb",
        "countIf(cid != '') AS has_cid",
        "countIf(country != '') AS has_country",
        "countIf(asn IS NOT NULL) AS has_asn",
    ]
    if want_overview:
        probe_select += [
            "uniqExactIf(sid, bs = 1) AS rebuffer_sessions",
            "avgIf(br, ot = 'v') AS avg_br_v",
            "avg(br) AS avg_br_all",
            "avgIf(bl, ot = 'v') AS avg_bl_v",
            "avg(bl) AS avg_bl_all",
            "quantileTDigestIf(0.5)(mtp, ot = 'v') AS med_mtp_v",
            "quantileTDigest(0.5)(mtp) AS med_mtp_all",
        ]
    probe = trace.run(
        "probe",
        lambda: execute(f"/* cmcd:probe */ SELECT {', '.join(probe_select)} FROM ({events})", params),
        source="projected_fact",
        activity="overhead",
    )[0]
    if not int(probe["events"] or 0):
        return empty_cmcd_aggregates(available=True, section_timings=trace.timings())
    has = {
        key: bool(int(probe.get(f"has_{key}") or 0))
        for key in ("bl", "br", "bs", "mtp", "ot", "sf", "st", "su", "tb", "cid")
    }
    # cmcd_bs is a sparse flag (1/true only on starvation, empty/NULL otherwise), so buffer-length
    # telemetry proves starvation tracking is active even in healthy windows with zero starvations.
    has_bs_effective = has["bs"] or has["bl"]
    has_country = bool(int(probe["has_country"] or 0))
    has_asn = bool(int(probe["has_asn"] or 0))
    has_video_filter = has["ot"]
    video = _VIDEO if has_video_filter else "1"
    result: dict[str, Any] = {"available": True, "has_data": True}
    tasks: list[tuple[str, Callable[[], dict[str, Any]]]] = []
    want_ts = {
        "sessions_ts": wanted("sessions_ts"),
        "buffer_health_ts": wanted("buffer_health_ts") and has["bl"],
        "bitrate_ts": wanted("bitrate_ts") and has["br"],
        "throughput_ts": wanted("throughput_ts") and has["mtp"],
        "startup_ts": wanted("startup_ts") and has["su"],
    }
    want_duration_dist = wanted("session_duration_dist")
    overview_extras: dict[str, Any] = {}
    need_peak_query = want_overview and not want_ts["sessions_ts"]
    need_duration_query = want_overview and not want_duration_dist
    if need_peak_query or need_duration_query:

        def _run_overview_extras() -> dict[str, Any]:
            if need_peak_query:
                peak = execute(
                    "/* cmcd:overview_peak */ SELECT max(viewers) AS peak_viewers FROM ("
                    "SELECT uniqExact(sid) AS viewers FROM (" + events + ") "
                    f"GROUP BY {_BUCKET.format(column='ts')})",
                    params,
                )
                overview_extras["peak_viewers"] = int((peak[0].get("peak_viewers") if peak else 0) or 0)
            if need_duration_query:
                duration = execute(
                    "/* cmcd:overview_duration */ SELECT avg(duration) AS avg_duration FROM ("
                    "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                    "FROM (" + events + ") GROUP BY sid HAVING duration > 0)",
                    params,
                )
                overview_extras["avg_duration"] = duration[0].get("avg_duration") if duration else None
            return {}

        tasks.append(("overview", _run_overview_extras))
    if any(want_ts.values()):

        def _run_timeseries() -> dict[str, Any]:
            ts_out: dict[str, Any] = {}
            select = [f"{_BUCKET.format(column='ts')} AS bucket"]
            if want_ts["sessions_ts"]:
                rebuffer_session_expr = (
                    "uniqExactIf(sid, bs = 1) * 100.0 / nullIf(uniqExact(sid), 0) AS rebuffer_session_pct"
                    if has_bs_effective
                    else "CAST(NULL AS Nullable(Float64)) AS rebuffer_session_pct"
                )
                select += [
                    "uniqExact(sid) AS concurrent_sessions",
                    rebuffer_session_expr,
                ]
            if want_ts["buffer_health_ts"]:
                starvation_expr = (
                    "countIf(bs = 1) * 100.0 / nullIf(count(), 0) AS starvation_rate"
                    if has_bs_effective
                    else "CAST(NULL AS Nullable(Float64)) AS starvation_rate"
                )
                select += [
                    f"quantileTDigestIf(0.5)(bl, {video}) AS p50_buffer",
                    f"quantileTDigestIf(0.95)(bl, {video}) AS p95_buffer",
                    starvation_expr,
                ]
            if want_ts["bitrate_ts"]:
                select += [
                    f"avgIf(br, {video}) AS avg_bitrate",
                    f"avgIf(br / tb, {video} AND tb > 0) AS utilization_ratio",
                ]
            if want_ts["throughput_ts"]:
                select += [
                    f"quantileTDigestIf(0.5)(mtp, {video}) AS p50_throughput",
                    f"quantileTDigestIf(0.95)(mtp, {video}) AS p95_throughput",
                    f"quantileTDigestIf(0.99)(mtp, {video}) AS p99_throughput",
                ]
            if want_ts["startup_ts"]:
                select.append("countIf(su = 1) * 100.0 / nullIf(count(), 0) AS startup_ratio")
            rows = execute(
                f"/* cmcd:timeseries */ SELECT {', '.join(select)} FROM ({events}) GROUP BY bucket ORDER BY bucket",
                params,
            )
            by_bucket = {str(row["bucket"]): row for row in rows}

            def padded(field_map: dict[str, tuple[str, Any]], default: dict[str, Any]) -> list[dict[str, Any]]:
                return _pad(
                    start,
                    end,
                    int(req.bucket_seconds),
                    {
                        bucket: {out: convert(row.get(src)) for out, (src, convert) in field_map.items()}
                        for bucket, row in by_bucket.items()
                    },
                    default,
                )

            if want_ts["sessions_ts"] and want_overview:
                overview_extras["peak_viewers"] = max(
                    (int(row.get("concurrent_sessions") or 0) for row in rows),
                    default=0,
                )
            if want_ts["sessions_ts"]:
                ts_out["_sessions_ts_rows"] = rows
            if want_ts["buffer_health_ts"]:
                ts_out["buffer_health_ts"] = padded(
                    {
                        "p50_buffer": ("p50_buffer", _round),
                        "p95_buffer": ("p95_buffer", _round),
                        "starvation_rate": ("starvation_rate", _round),
                    },
                    CMCD_TS_DEFAULTS["buffer_health_ts"],
                )
            if want_ts["bitrate_ts"]:
                ts_out["bitrate_ts"] = padded(
                    {"avg_bitrate": ("avg_bitrate", _round), "utilization_ratio": ("utilization_ratio", _round)},
                    CMCD_TS_DEFAULTS["bitrate_ts"],
                )
            if want_ts["throughput_ts"]:
                ts_out["throughput_ts"] = padded(
                    {
                        "p50": ("p50_throughput", _round),
                        "p95": ("p95_throughput", _round),
                        "p99": ("p99_throughput", _round),
                    },
                    CMCD_TS_DEFAULTS["throughput_ts"],
                )
            if want_ts["startup_ts"]:
                ts_out["startup_ts"] = padded(
                    {"startup_ratio": ("startup_ratio", _round)},
                    CMCD_TS_DEFAULTS["startup_ts"],
                )
            return ts_out

        tasks.append(("timeseries", _run_timeseries))

    # Consolidate per-session aggregations (new_sessions + session_duration_dist) into one scan.
    if want_ts["sessions_ts"] and want_duration_dist:

        def _run_sessions() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:sessions */ WITH sess AS ("
                "SELECT sid, min(ts) AS first_ts, "
                "(toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                f"FROM ({events}) GROUP BY sid) "
                f"SELECT 'new_sessions' AS kind, {_BUCKET.format(column='first_ts')} AS k, "
                "toUInt64(count()) AS session_count, toFloat64(0) AS positive_duration_sum, "
                "toUInt64(0) AS positive_duration_count FROM sess GROUP BY k "
                "UNION ALL "
                "SELECT 'duration' AS kind, toString(multiIf(duration < 30, 1, duration < 120, 2, "
                "duration < 300, 3, duration < 600, 4, duration < 1800, 5, duration < 3600, 6, 7)) AS k, "
                "toUInt64(count()) AS session_count, toFloat64(sumIf(duration, duration > 0)) AS positive_duration_sum, "
                "toUInt64(countIf(duration > 0)) AS positive_duration_count FROM sess GROUP BY k",
                params,
            )
            new_sessions: dict[str, int] = {}
            dur_rows: list[dict[str, Any]] = []
            for row in rows:
                if row.get("kind") == "new_sessions":
                    new_sessions[str(row["k"])] = int(row.get("session_count") or 0)
                elif row.get("kind") == "duration":
                    dur_rows.append(row)
            dur_rows.sort(key=lambda r: int(r["k"]))
            if want_overview:
                pos_count = sum(int(row.get("positive_duration_count") or 0) for row in dur_rows)
                if pos_count > 0:
                    pos_sum = sum(float(row.get("positive_duration_sum") or 0.0) for row in dur_rows)
                    overview_extras["avg_duration"] = pos_sum / pos_count
                else:
                    overview_extras["avg_duration"] = None
            return {
                "_new_sessions": new_sessions,
                "session_duration_dist": [
                    {
                        "duration_bucket": _DURATION_BUCKETS[int(row["k"])],
                        "session_count": int(row["session_count"]),
                    }
                    for row in dur_rows
                ],
            }

        tasks.append(("sessions", _run_sessions))
    elif want_ts["sessions_ts"]:

        def _run_new_sessions() -> dict[str, Any]:
            first_seen = execute(
                "/* cmcd:new_sessions */ SELECT "
                f"{_BUCKET.format(column='first_ts')} AS bucket, count() AS new_sessions FROM ("
                f"SELECT sid, min(ts) AS first_ts FROM ({events}) GROUP BY sid) GROUP BY bucket",
                params,
            )
            return {"_new_sessions": {str(row["bucket"]): int(row["new_sessions"] or 0) for row in first_seen}}

        tasks.append(("new_sessions", _run_new_sessions))
    elif want_duration_dist:

        def _run_session_duration() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:session_duration_dist */ SELECT sort_order, count() AS session_count, "
                "sumIf(duration, duration > 0) AS positive_duration_sum, "
                "countIf(duration > 0) AS positive_duration_count FROM ("
                "SELECT duration, multiIf(duration < 30, 1, duration < 120, 2, duration < 300, 3, duration < 600, 4, "
                "duration < 1800, 5, duration < 3600, 6, 7) AS sort_order FROM ("
                "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                f"FROM ({events}) GROUP BY sid)) GROUP BY sort_order ORDER BY sort_order",
                params,
            )
            if want_overview:
                pos_count = sum(int(row.get("positive_duration_count") or 0) for row in rows)
                if pos_count > 0:
                    pos_sum = sum(float(row.get("positive_duration_sum") or 0.0) for row in rows)
                    overview_extras["avg_duration"] = pos_sum / pos_count
                else:
                    overview_extras["avg_duration"] = None
            return {
                "session_duration_dist": [
                    {
                        "duration_bucket": _DURATION_BUCKETS[int(row["sort_order"])],
                        "session_count": int(row["session_count"]),
                    }
                    for row in rows
                ]
            }

        tasks.append(("session_duration", _run_session_duration))

    # Consolidate requested breakdown queries into one UNION ALL query.
    want_content = wanted("top_content") and has["cid"]
    want_country = wanted("rebuffer_by_country") and has_country and has_bs_effective
    want_asn = wanted("rebuffer_by_asn") and has_asn and has_bs_effective
    want_ot = wanted("object_type_dist") and has["ot"]
    want_sf = wanted("streaming_format_dist") and has["sf"]
    want_ladder = wanted("bitrate_ladder") and has["br"]
    want_st = wanted("stream_type_dist") and has["st"]
    breakdown_branches: list[str] = []
    if want_content:
        breakdown_branches.append(
            "SELECT * FROM (SELECT 'content' AS kind, cid AS k, toUInt64(count()) AS session_count, "
            "toUInt64(0) AS request_count, "
            "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
            "toNullable(sumIf(avg_br * br_reqs, avg_br IS NOT NULL) / nullIf(sumIf(br_reqs, avg_br IS NOT NULL), 0)) AS avg_br, "
            "toNullable(sumIf(avg_bl * bl_reqs, avg_bl IS NOT NULL) / nullIf(sumIf(bl_reqs, avg_bl IS NOT NULL), 0)) AS avg_bl, "
            "CAST(NULL, 'Nullable(Float64)') AS median_mtp FROM ("
            "SELECT cid, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
            f"avgIf(br, {video}) AS avg_br, countIf({video} AND br IS NOT NULL) AS br_reqs, "
            f"avgIf(bl, {video}) AS avg_bl, countIf({video} AND bl IS NOT NULL) AS bl_reqs "
            f"FROM ({events}) WHERE cid != '' GROUP BY cid, sid) "
            "GROUP BY cid ORDER BY session_count DESC, k LIMIT {top_n:UInt32})"
        )
    if want_country:
        breakdown_branches.append(
            "SELECT * FROM (SELECT 'country' AS kind, country AS k, toUInt64(count()) AS session_count, "
            "toUInt64(0) AS request_count, "
            "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
            "CAST(NULL, 'Nullable(Float64)') AS avg_br, CAST(NULL, 'Nullable(Float64)') AS avg_bl, "
            "toNullable(sumIf(median_mtp * mtp_reqs, median_mtp IS NOT NULL) / nullIf(sumIf(mtp_reqs, median_mtp IS NOT NULL), 0)) AS median_mtp FROM ("
            "SELECT country, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
            f"quantileTDigestIf(0.5)(mtp, {video}) AS median_mtp, countIf({video} AND mtp IS NOT NULL) AS mtp_reqs "
            f"FROM ({events}) WHERE country != '' GROUP BY country, sid) "
            "GROUP BY country ORDER BY session_count DESC, k LIMIT {top_n:UInt32})"
        )
    if want_asn:
        breakdown_branches.append(
            "SELECT * FROM (SELECT 'asn' AS kind, toString(asn) AS k, toUInt64(count()) AS session_count, "
            "toUInt64(0) AS request_count, "
            "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
            "toNullable(sumIf(avg_br * br_reqs, avg_br IS NOT NULL) / nullIf(sumIf(br_reqs, avg_br IS NOT NULL), 0)) AS avg_br, "
            "toNullable(sumIf(avg_bl * bl_reqs, avg_bl IS NOT NULL) / nullIf(sumIf(bl_reqs, avg_bl IS NOT NULL), 0)) AS avg_bl, "
            "CAST(NULL, 'Nullable(Float64)') AS median_mtp FROM ("
            "SELECT asn, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
            f"avgIf(br, {video}) AS avg_br, countIf({video} AND br IS NOT NULL) AS br_reqs, "
            f"avgIf(bl, {video}) AS avg_bl, countIf({video} AND bl IS NOT NULL) AS bl_reqs "
            f"FROM ({events}) WHERE asn IS NOT NULL GROUP BY asn, sid) "
            "GROUP BY asn ORDER BY session_count DESC, k LIMIT {top_n:UInt32})"
        )
    if want_ot:
        breakdown_branches.append(
            "SELECT 'ot' AS kind, ot AS k, toUInt64(count()) AS session_count, toUInt64(count()) AS request_count, "
            "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, CAST(NULL, 'Nullable(Float64)') AS avg_br, "
            "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS median_mtp "
            f"FROM ({events}) WHERE ot != '' GROUP BY ot"
        )
    if want_sf:
        breakdown_branches.append(
            "SELECT 'sf' AS kind, sf AS k, toUInt64(uniqExact(sid)) AS session_count, toUInt64(count()) AS request_count, "
            "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, CAST(NULL, 'Nullable(Float64)') AS avg_br, "
            "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS median_mtp "
            f"FROM ({events}) WHERE sf != '' GROUP BY sf"
        )
    if want_ladder:
        reb_expr = (
            "toNullable(countIf(bs = 1) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
            if has_bs_effective
            else "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, "
        )
        breakdown_branches.append(
            "SELECT 'ladder' AS kind, "
            "toString(multiIf(br < 1000, 1, br < 2500, 2, br < 5000, 3, br < 10000, 4, 5)) AS k, "
            "toUInt64(uniqExactIf(sid, sid != '')) AS session_count, toUInt64(count()) AS request_count, "
            f"{reb_expr}"
            "toNullable(avg(br)) AS avg_br, CAST(NULL, 'Nullable(Float64)') AS avg_bl, "
            "CAST(NULL, 'Nullable(Float64)') AS median_mtp "
            f"FROM ({events}) WHERE ({video}) AND br IS NOT NULL AND br > 0 GROUP BY k"
        )
    if want_st:
        breakdown_branches.append(
            "SELECT 'st' AS kind, st AS k, toUInt64(uniqExactIf(sid, sid != '')) AS session_count, "
            "toUInt64(count()) AS request_count, "
            "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, CAST(NULL, 'Nullable(Float64)') AS avg_br, "
            "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS median_mtp "
            f"FROM ({events}) WHERE st != '' GROUP BY st"
        )
    if breakdown_branches:

        def _run_breakdowns() -> dict[str, Any]:
            rows = execute("/* cmcd:breakdowns */ " + " UNION ALL ".join(breakdown_branches), params)
            by_kind: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                by_kind.setdefault(str(row.get("kind") or ""), []).append(row)
            out: dict[str, Any] = {}
            if want_content:
                c_rows = sorted(
                    by_kind.get("content", []),
                    key=lambda r: (-int(r["session_count"]), str(r["k"])),
                )
                out["top_content"] = [
                    {
                        "content_id": row["k"],
                        "session_count": int(row["session_count"]),
                        "rebuffer_rate": _round(row.get("rebuffer_rate")),
                        "avg_bitrate": _round(row.get("avg_br")),
                        "avg_buffer_length": _round(row.get("avg_bl")),
                    }
                    for row in c_rows
                ]
            if want_country:
                co_rows = sorted(
                    by_kind.get("country", []),
                    key=lambda r: (-int(r["session_count"]), str(r["k"])),
                )
                out["rebuffer_by_country"] = [
                    {
                        "country": row["k"],
                        "rebuffer_rate": _round(row.get("rebuffer_rate")),
                        "session_count": int(row["session_count"]),
                        "median_throughput": _round(row.get("median_mtp")),
                    }
                    for row in co_rows
                ]
            if want_asn:
                a_rows = sorted(
                    by_kind.get("asn", []),
                    key=lambda r: (-int(r["session_count"]), int(r["k"])),
                )
                out["rebuffer_by_asn"] = [
                    {
                        "asn": int(row["k"]),
                        "label": "",
                        "rebuffer_rate": _round(row.get("rebuffer_rate")),
                        "session_count": int(row["session_count"]),
                        "avg_bitrate": _round(row.get("avg_br")),
                        "avg_buffer_length": _round(row.get("avg_bl")),
                    }
                    for row in a_rows
                ]
            if want_ot:
                ot_rows = sorted(
                    by_kind.get("ot", []),
                    key=lambda r: (-int(r["session_count"]), str(r["k"])),
                )
                out["object_type_dist"] = [
                    {"object_type": row["k"], "request_count": int(row["session_count"])} for row in ot_rows
                ]
            if want_sf:
                sf_rows = sorted(
                    by_kind.get("sf", []),
                    key=lambda r: (-int(r["session_count"]), str(r["k"])),
                )
                out["streaming_format_dist"] = [
                    {"streaming_format": row["k"], "session_count": int(row["session_count"])} for row in sf_rows
                ]
            if want_ladder:
                ladder_rows = sorted(
                    by_kind.get("ladder", []),
                    key=lambda r: int(r["k"]),
                )
                out["bitrate_ladder"] = [
                    {
                        "tier": _BITRATE_LADDER_TIERS[int(row["k"])],
                        "request_count": int(row.get("request_count") or 0),
                        "session_count": int(row["session_count"]),
                        "avg_bitrate": _round(row.get("avg_br")),
                        "rebuffer_rate": _round(row.get("rebuffer_rate")) if has_bs_effective else None,
                    }
                    for row in ladder_rows
                    if int(row["k"]) in _BITRATE_LADDER_TIERS
                ]
            if want_st:
                st_rows = sorted(
                    by_kind.get("st", []),
                    key=lambda r: (-int(r["session_count"]), str(r["k"])),
                )
                out["stream_type_dist"] = [
                    {
                        "stream_type": row["k"],
                        "session_count": int(row["session_count"]),
                        "request_count": int(row.get("request_count") or 0),
                    }
                    for row in st_rows
                ]
            return out

        tasks.append(("breakdowns", _run_breakdowns))
    for d in trace.run_parallel(tasks, source="projected_fact", max_workers=8).values():
        result.update(d)
    if want_ts["sessions_ts"]:
        ts_rows = result.pop("_sessions_ts_rows", [])
        new_sessions = result.pop("_new_sessions", {})
        result["sessions_ts"] = _pad(
            start,
            end,
            int(req.bucket_seconds),
            _sessions_ts(ts_rows, new_sessions),
            CMCD_TS_DEFAULTS["sessions_ts"],
        )
    if want_overview:
        active_sessions = int(probe["sessions"] or 0)
        rebuffer_sessions = int(probe.get("rebuffer_sessions") or 0)
        rebuffer_rate = (rebuffer_sessions * 100.0 / active_sessions) if active_sessions > 0 else None
        avg_duration = overview_extras.get("avg_duration")
        result["overview"] = {
            "active_sessions": active_sessions,
            "rebuffer_rate": _round(rebuffer_rate) if has_bs_effective else None,
            "avg_bitrate": _round(probe.get("avg_br_v" if has_video_filter else "avg_br_all")) if has["br"] else None,
            "avg_buffer_length": (
                _round(probe.get("avg_bl_v" if has_video_filter else "avg_bl_all")) if has["bl"] else None
            ),
            "median_throughput": (
                _round(probe.get("med_mtp_v" if has_video_filter else "med_mtp_all")) if has["mtp"] else None
            ),
            "peak_viewers": int(overview_extras.get("peak_viewers") or 0),
            "avg_session_duration": int(round(float(avg_duration))) if avg_duration else None,
        }
    if "rebuffer_by_asn" in result:

        def _label_asns() -> None:
            enrich_asn_rows(
                result["rebuffer_by_asn"],
                service.service_id,
                asn_key="asn",
                name_lookup=_asn_names,
            )

        trace.run("asn_labels", _label_asns, activity="overhead")
    result["section_timings"] = trace.timings()
    return result


if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "cmcd", sys.modules[__name__])
