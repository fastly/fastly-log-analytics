"""CMCD streaming-QoE analytics for high-scale services, computed from ``cmcd_projection_facts``.

Every section of the standard CMCD response is derived from the projection's CMCD key/value
map plus the request geography (country, ASN) the projection carries. Sections whose CMCD key
never appears in the window are omitted, exactly as the standard path omits sections whose
column is missing; ClickHouse failures propagate rather than becoming empty charts.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._projection_utils import range_value
from backend.high_scale.registry import HighScaleService
from backend.models.cmcd import CmcdRequest

_DEFAULT_WINDOW = timedelta(days=1)
_BUCKET_FORMAT = "%Y-%m-%d %H:%M:%S"
_DURATION_BUCKETS = {
    1: "<30s",
    2: "30s–2m",
    3: "2–5m",
    4: "5–10m",
    5: "10–30m",
    6: "30m–1h",
    7: ">1h",
}
_FILTER_COLUMNS = {"country": "country", "asn": "asn"}

# One row per CMCD event with each key parsed once. Flags are NULL when the key is absent so
# "never reported" stays distinguishable from "reported false".
_EVENTS = """
    SELECT event_timestamp AS ts,
           cmcd['sid'] AS sid,
           cmcd['cid'] AS cid,
           cmcd['ot'] AS ot,
           cmcd['sf'] AS sf,
           country,
           asn,
           toFloat64OrNull(cmcd['bl']) AS bl,
           toFloat64OrNull(cmcd['br']) AS br,
           toFloat64OrNull(cmcd['mtp']) AS mtp,
           toFloat64OrNull(cmcd['tb']) AS tb,
           if(cmcd['bs'] = '', NULL, lower(cmcd['bs']) IN ('true', '1')) AS bs,
           if(cmcd['su'] = '', NULL, lower(cmcd['su']) IN ('true', '1')) AS su
    FROM cmcd_projection_facts
    WHERE {where}
"""
_BUCKET = (
    "formatDateTime(toStartOfInterval({column}, INTERVAL {{bucket_seconds:UInt32}} SECOND), '%Y-%m-%d %H:%i:%S', 'UTC')"
)
_VIDEO = "ot = 'v'"


def _asn_names(service_id: str, asns: list[int]) -> dict[int, str]:
    from backend.core import duckdb as _db

    return _db.get_asn_names(service_id, asns)


def _asn_label(asn: int, names: Mapping[int, str]) -> str:
    from backend.core import duckdb as _db

    return _db.format_asn_label(asn, names.get(asn, ""))


def _spec(config: Any) -> tuple[str, list[Any]]:
    if isinstance(config, Mapping):
        return str(config.get("mode", "include")), list(config.get("values") or [])
    return str(getattr(config, "mode", "include")), list(getattr(config, "values", None) or [])


def _filter_clauses(filters: Mapping[str, Any]) -> tuple[list[str], dict[str, Any]]:
    unsupported = sorted(name for name, config in filters.items() if _spec(config)[1] and name not in _FILTER_COLUMNS)
    if unsupported:
        raise ValueError(
            f"high-scale CMCD projections cannot filter on: {', '.join(unsupported)} (supported: country, asn)"
        )
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, (name, config) in enumerate(filters.items()):
        mode, values = _spec(config)
        if not values:
            continue
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
        clauses.append(f"{_FILTER_COLUMNS[name]} {operator} ({', '.join(keys)})")
    return clauses, params


def _scope(service: HighScaleService, req: CmcdRequest, start: datetime, end: datetime) -> tuple[str, dict[str, Any]]:
    filter_clauses, filter_params = _filter_clauses(req.filters or {})
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain='cmcd' AND publication_state='visible')",
        "event_timestamp >= {start:DateTime64(3)}",
        "event_timestamp <= {end:DateTime64(3)}",
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
    return " AND ".join(clauses), params


def _round(value: Any) -> float | None:
    return None if value is None else round(float(value), 2)


def _pad(
    start: datetime, end: datetime, bucket_seconds: int, rows: dict[str, dict[str, Any]], default: dict[str, Any]
) -> list[dict[str, Any]]:
    first = int(start.timestamp())
    last = int(end.timestamp())
    cursor = first - first % bucket_seconds
    final = last - last % bucket_seconds
    padded: list[dict[str, Any]] = []
    while cursor <= final:
        bucket = datetime.fromtimestamp(cursor, tz=UTC).strftime(_BUCKET_FORMAT)
        padded.append({"bucket": bucket, **rows.get(bucket, default)})
        cursor += bucket_seconds
    return padded


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _window(start_time: str | None, end_time: str | None) -> tuple[datetime, datetime]:
    end = _utc(range_value(end_time)) if end_time else datetime.now(UTC)
    start = _utc(range_value(start_time)) if start_time else end - _DEFAULT_WINDOW
    return start, end


def _sessions_ts(rows: Iterable[dict[str, Any]], new_sessions: Mapping[str, int]) -> dict[str, dict[str, Any]]:
    return {
        str(row["bucket"]): {
            "concurrent_sessions": int(row["concurrent_sessions"] or 0),
            "rebuffer_session_pct": _round(row.get("rebuffer_session_pct")),
            "new_sessions": new_sessions.get(str(row["bucket"]), 0),
        }
        for row in rows
    }


def cmcd_aggregates(
    service: HighScaleService,
    req: CmcdRequest,
    start_time: str | None,
    end_time: str | None,
    sections: set[Any] | None,
    mask_ips: bool,
) -> dict[str, Any]:
    """Return the standard CMCD response shape; ``mask_ips`` is moot because no section emits an IP."""
    start, end = _window(start_time, end_time)
    where, params = _scope(service, req, start, end)
    events = _EVENTS.format(where=where)
    execute = service.client.execute

    def wanted(name: str) -> bool:
        return sections is None or name in sections

    probe = execute(
        "/* cmcd:probe */ SELECT count() AS events, uniqExact(sid) AS sessions, "
        "countIf(bl IS NOT NULL) AS has_bl, countIf(br IS NOT NULL) AS has_br, countIf(bs IS NOT NULL) AS has_bs, "
        "countIf(mtp IS NOT NULL) AS has_mtp, countIf(ot != '') AS has_ot, countIf(sf != '') AS has_sf, "
        "countIf(su IS NOT NULL) AS has_su, countIf(tb IS NOT NULL) AS has_tb, countIf(cid != '') AS has_cid, "
        "countIf(country != '') AS has_country, countIf(asn IS NOT NULL) AS has_asn "
        f"FROM ({events})",
        params,
    )[0]
    if not int(probe["events"] or 0):
        return {"available": True, "has_data": False}
    has = {key: bool(int(probe[f"has_{key}"] or 0)) for key in ("bl", "br", "bs", "mtp", "ot", "sf", "su", "tb", "cid")}
    has_country = bool(int(probe["has_country"] or 0))
    has_asn = bool(int(probe["has_asn"] or 0))
    has_video_filter = has["ot"]
    video = _VIDEO if has_video_filter else "1"

    result: dict[str, Any] = {"available": True, "has_data": True}

    if wanted("overview"):
        row = execute(
            "/* cmcd:overview */ SELECT uniqExact(sid) AS active_sessions, "
            "if(countIf(bs IS NOT NULL) = 0, NULL, "
            "uniqExactIf(sid, bs = 1) * 100.0 / nullIf(uniqExact(sid), 0)) AS rebuffer_rate, "
            f"avgIf(br, {video}) AS avg_bitrate, avgIf(bl, {video}) AS avg_buffer_length, "
            f"quantileTDigestIf(0.5)(mtp, {video}) AS median_throughput FROM ({events})",
            params,
        )[0]
        peak = execute(
            "/* cmcd:overview_peak */ SELECT max(viewers) AS peak_viewers FROM ("
            "SELECT uniqExact(sid) AS viewers FROM (" + events + ") "
            f"GROUP BY {_BUCKET.format(column='ts')})",
            params,
        )
        duration = execute(
            "/* cmcd:overview_duration */ SELECT avg(duration) AS avg_duration FROM ("
            "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
            "FROM (" + events + ") GROUP BY sid HAVING duration > 0)",
            params,
        )
        avg_duration = duration[0].get("avg_duration") if duration else None
        result["overview"] = {
            "active_sessions": int(row["active_sessions"] or 0),
            "rebuffer_rate": _round(row.get("rebuffer_rate")) if has["bs"] else None,
            "avg_bitrate": _round(row.get("avg_bitrate")) if has["br"] else None,
            "avg_buffer_length": _round(row.get("avg_buffer_length")) if has["bl"] else None,
            "median_throughput": _round(row.get("median_throughput")) if has["mtp"] else None,
            "peak_viewers": int((peak[0].get("peak_viewers") if peak else 0) or 0),
            "avg_session_duration": int(round(float(avg_duration))) if avg_duration else None,
        }

    want_ts = {
        "sessions_ts": wanted("sessions_ts"),
        "buffer_health_ts": wanted("buffer_health_ts") and has["bl"],
        "bitrate_ts": wanted("bitrate_ts") and has["br"],
        "throughput_ts": wanted("throughput_ts") and has["mtp"],
        "startup_ts": wanted("startup_ts") and has["su"],
    }
    if any(want_ts.values()):
        select = [f"{_BUCKET.format(column='ts')} AS bucket"]
        if want_ts["sessions_ts"]:
            select += [
                "uniqExact(sid) AS concurrent_sessions",
                "if(countIf(bs IS NOT NULL) = 0, NULL, "
                "uniqExactIf(sid, bs = 1) * 100.0 / nullIf(uniqExact(sid), 0)) AS rebuffer_session_pct",
            ]
        if want_ts["buffer_health_ts"]:
            select += [
                f"quantileTDigestIf(0.5)(bl, {video}) AS p50_buffer",
                f"quantileTDigestIf(0.95)(bl, {video}) AS p95_buffer",
                "if(countIf(bs IS NOT NULL) = 0, NULL, countIf(bs = 1) * 100.0 / nullIf(count(), 0)) "
                "AS starvation_rate",
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
            select.append(
                "if(countIf(su IS NOT NULL) = 0, NULL, countIf(su = 1) * 100.0 / nullIf(count(), 0)) AS startup_ratio"
            )
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

        if want_ts["sessions_ts"]:
            first_seen = execute(
                "/* cmcd:new_sessions */ SELECT "
                f"{_BUCKET.format(column='first_ts')} AS bucket, count() AS new_sessions FROM ("
                f"SELECT sid, min(ts) AS first_ts FROM ({events}) GROUP BY sid) GROUP BY bucket",
                params,
            )
            new_sessions = {str(row["bucket"]): int(row["new_sessions"] or 0) for row in first_seen}
            result["sessions_ts"] = _pad(
                start,
                end,
                int(req.bucket_seconds),
                _sessions_ts(rows, new_sessions),
                {"concurrent_sessions": 0, "rebuffer_session_pct": None, "new_sessions": 0},
            )
        if want_ts["buffer_health_ts"]:
            result["buffer_health_ts"] = padded(
                {
                    "p50_buffer": ("p50_buffer", _round),
                    "p95_buffer": ("p95_buffer", _round),
                    "starvation_rate": ("starvation_rate", _round),
                },
                {"p50_buffer": None, "p95_buffer": None, "starvation_rate": None},
            )
        if want_ts["bitrate_ts"]:
            result["bitrate_ts"] = padded(
                {"avg_bitrate": ("avg_bitrate", _round), "utilization_ratio": ("utilization_ratio", _round)},
                {"avg_bitrate": None, "utilization_ratio": None},
            )
        if want_ts["throughput_ts"]:
            result["throughput_ts"] = padded(
                {
                    "p50": ("p50_throughput", _round),
                    "p95": ("p95_throughput", _round),
                    "p99": ("p99_throughput", _round),
                },
                {"p50": None, "p95": None, "p99": None},
            )
        if want_ts["startup_ts"]:
            result["startup_ts"] = padded(
                {"startup_ratio": ("startup_ratio", _round)},
                {"startup_ratio": None},
            )

    if wanted("top_content") and has["cid"]:
        rows = execute(
            "/* cmcd:top_content */ SELECT cid AS content_id, count() AS session_count, "
            "sum(had_rebuffer) * 100.0 / nullIf(count(), 0) AS rebuffer_rate, avg(avg_br) AS avg_br, "
            "avg(avg_bl) AS avg_bl FROM ("
            "SELECT cid, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
            f"avgIf(br, {video}) AS avg_br, avgIf(bl, {video}) AS avg_bl "
            f"FROM ({events}) WHERE cid != '' GROUP BY cid, sid) "
            "GROUP BY cid ORDER BY session_count DESC, content_id LIMIT {top_n:UInt32}",
            params,
        )
        result["top_content"] = [
            {
                "content_id": row["content_id"],
                "session_count": int(row["session_count"]),
                "rebuffer_rate": _round(row.get("rebuffer_rate")),
                "avg_bitrate": _round(row.get("avg_br")),
                "avg_buffer_length": _round(row.get("avg_bl")),
            }
            for row in rows
        ]

    if wanted("rebuffer_by_country") and has_country and has["bs"]:
        rows = execute(
            "/* cmcd:rebuffer_by_country */ SELECT country, "
            "sum(had_rebuffer) * 100.0 / nullIf(count(), 0) AS rebuffer_rate, count() AS session_count, "
            "avg(median_mtp) AS median_mtp FROM ("
            "SELECT country, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
            f"quantileTDigestIf(0.5)(mtp, {video}) AS median_mtp "
            f"FROM ({events}) WHERE country != '' GROUP BY country, sid) "
            "GROUP BY country ORDER BY session_count DESC, country LIMIT {top_n:UInt32}",
            params,
        )
        result["rebuffer_by_country"] = [
            {
                "country": row["country"],
                "rebuffer_rate": _round(row.get("rebuffer_rate")),
                "session_count": int(row["session_count"]),
                "median_throughput": _round(row.get("median_mtp")),
            }
            for row in rows
        ]

    if wanted("rebuffer_by_asn") and has_asn and has["bs"]:
        rows = execute(
            "/* cmcd:rebuffer_by_asn */ SELECT asn, "
            "sum(had_rebuffer) * 100.0 / nullIf(count(), 0) AS rebuffer_rate, count() AS session_count, "
            "avg(avg_br) AS avg_br, avg(avg_bl) AS avg_bl FROM ("
            "SELECT asn, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
            f"avgIf(br, {video}) AS avg_br, avgIf(bl, {video}) AS avg_bl "
            f"FROM ({events}) WHERE asn IS NOT NULL GROUP BY asn, sid) "
            "GROUP BY asn ORDER BY session_count DESC, asn LIMIT {top_n:UInt32}",
            params,
        )
        names = _asn_names(service.service_id, [int(row["asn"]) for row in rows])
        result["rebuffer_by_asn"] = [
            {
                "asn": int(row["asn"]),
                "label": _asn_label(int(row["asn"]), names),
                "rebuffer_rate": _round(row.get("rebuffer_rate")),
                "session_count": int(row["session_count"]),
                "avg_bitrate": _round(row.get("avg_br")),
                "avg_buffer_length": _round(row.get("avg_bl")),
            }
            for row in rows
        ]

    if wanted("object_type_dist") and has["ot"]:
        rows = execute(
            "/* cmcd:object_type_dist */ SELECT ot AS object_type, count() AS request_count "
            f"FROM ({events}) WHERE ot != '' GROUP BY ot ORDER BY request_count DESC, ot",
            params,
        )
        result["object_type_dist"] = [
            {"object_type": row["object_type"], "request_count": int(row["request_count"])} for row in rows
        ]

    if wanted("streaming_format_dist") and has["sf"]:
        rows = execute(
            "/* cmcd:streaming_format_dist */ SELECT sf AS streaming_format, uniqExact(sid) AS session_count "
            f"FROM ({events}) WHERE sf != '' GROUP BY sf ORDER BY session_count DESC, sf",
            params,
        )
        result["streaming_format_dist"] = [
            {"streaming_format": row["streaming_format"], "session_count": int(row["session_count"])} for row in rows
        ]

    if wanted("session_duration_dist"):
        rows = execute(
            "/* cmcd:session_duration_dist */ SELECT sort_order, count() AS session_count FROM ("
            "SELECT multiIf(duration < 30, 1, duration < 120, 2, duration < 300, 3, duration < 600, 4, "
            "duration < 1800, 5, duration < 3600, 6, 7) AS sort_order FROM ("
            "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
            f"FROM ({events}) GROUP BY sid)) GROUP BY sort_order ORDER BY sort_order",
            params,
        )
        result["session_duration_dist"] = [
            {"duration_bucket": _DURATION_BUCKETS[int(row["sort_order"])], "session_count": int(row["session_count"])}
            for row in rows
        ]

    return result
