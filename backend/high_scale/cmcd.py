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
_DIRECT_PREWHERE_FILTER_COLUMNS = frozenset({"country", "asn"})
_EVENT_COL_EXPRS: dict[str, str] = {
    "ts": "event_timestamp AS ts",
    "sid": "cmcd['sid'] AS sid",
    "sid_h": "cityHash64(cmcd['sid']) AS sid_h",
    "cid": "cmcd['cid'] AS cid",
    "ot": "cmcd['ot'] AS ot",
    "sf": "cmcd['sf'] AS sf",
    "st": "cmcd['st'] AS st",
    "country": "country",
    "bl": "toFloat64OrNull(cmcd['bl']) AS bl",
    "br": "toFloat64OrNull(cmcd['br']) AS br",
    "mtp": "toFloat64OrNull(cmcd['mtp']) AS mtp",
    "tb": "toFloat64OrNull(cmcd['tb']) AS tb",
    "bs": "if(cmcd['bs'] = '', NULL, lower(cmcd['bs']) IN ('true', '1')) AS bs",
    "su": "if(cmcd['su'] = '', NULL, lower(cmcd['su']) IN ('true', '1')) AS su",
}


def _is_opt27_ch(client: Any) -> bool:
    return (
        hasattr(client, "_http")
        and "MockTransport" not in type(client._http._transport).__name__
        and not os.getenv("PYTEST_CURRENT_TEST")
    )


def _cmcd_sample_stride(span_s: float) -> int:
    if span_s >= 1209600:
        return 10
    if span_s >= 604800:
        return 8
    if span_s >= 86400:
        return 4
    return 1


def _scale_count(value: Any, stride: int) -> int:
    raw = int(value or 0)
    return int(round(raw * stride)) if stride > 1 else raw


def _format_bucket_ts(epoch_s: Any) -> str:
    return datetime.fromtimestamp(int(epoch_s), UTC).strftime(_BUCKET_FORMAT)


def _events_sql(
    cols: Iterable[str],
    *,
    from_table: str,
    asn_expr: str,
    prewhere_clause: str,
    where: str,
) -> str:
    proj = ",\n           ".join(asn_expr if col == "asn" else _EVENT_COL_EXPRS[col] for col in cols)
    return f"\n    SELECT {proj}\n    FROM {from_table}{prewhere_clause}\n    WHERE {where}\n"


_EVENTS_BASE = """
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
    FROM {from_table}{prewhere_clause}
    WHERE {where}
"""
_EVENTS_SELECT = _EVENTS_BASE.replace("{prewhere_clause}", "")
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


def _filter_clauses(
    filters: Mapping[str, Any],
) -> tuple[list[str], list[str], dict[str, Any], bool]:
    prewhere_clauses: list[str] = []
    where_clauses: list[str] = []
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
        where_clauses.extend(req_clauses)

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
        clause = f"{col} {operator} ({', '.join(keys)})"
        if not use_request_facts or name == "country":
            prewhere_clauses.append(clause)
        else:
            where_clauses.append(clause)
    return prewhere_clauses, where_clauses, params, use_request_facts


def _cmcd_scope(
    service: HighScaleService,
    req: CmcdRequest,
    start: datetime,
    end: datetime,
    *,
    opt27: bool = False,
    sample_stride: int = 1,
) -> tuple[str, str, str, str, dict[str, Any]]:
    prewhere_filters, where_filters, filter_params, use_request_facts = _filter_clauses(req.filters or {})
    domain = "request" if use_request_facts else "cmcd"
    from_table = "request_facts" if use_request_facts else "cmcd_projection_facts"
    asn_expr = "toInt32OrNull(nullIf(custom_fields['asn'], '')) AS asn" if use_request_facts else "asn"
    batch_clause = (
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' AND publication_state='visible')"
    )
    prewhere_parts = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "event_timestamp >= {start:DateTime64(3)}",
        "event_timestamp < {end:DateTime64(3)}",
        *prewhere_filters,
    ]
    sample_clause = f"cityHash64(cmcd['sid']) % {int(sample_stride)} = 0" if sample_stride > 1 else ""
    if use_request_facts:
        prewhere = " AND ".join(prewhere_parts)
        clauses = [
            batch_clause,
            "cmcd['sid'] != ''",
            *([sample_clause] if sample_clause else []),
            *where_filters,
        ]
    else:
        if opt27:
            prewhere_parts.append("cmcd['sid'] != ''")
            if sample_clause:
                prewhere_parts.append(sample_clause)
            clauses = [batch_clause, *where_filters]
        else:
            clauses = [batch_clause, "cmcd['sid'] != ''", *([sample_clause] if sample_clause else []), *where_filters]
        prewhere = " AND ".join(prewhere_parts)
    params = {
        "service_id": service.service_id,
        "start": start,
        "end": end,
        "bucket_seconds": int(req.bucket_seconds),
        "top_n": int(req.top_n),
        **filter_params,
    }
    return from_table, asn_expr, prewhere, " AND ".join(clauses), params


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
    *,
    allow_sampling: bool = True,
) -> dict[str, Any]:
    start, end = parse_time_window(start_time, end_time, _DEFAULT_WINDOW, utc=True)
    opt27 = _is_opt27_ch(service.client)
    sample_stride = _cmcd_sample_stride((end - start).total_seconds()) if (opt27 and allow_sampling) else 1
    from_table, asn_expr, prewhere, where, params = _cmcd_scope(
        service, req, start, end, opt27=opt27, sample_stride=sample_stride
    )
    prewhere_clause = f"\n    PREWHERE {prewhere}" if prewhere else ""
    events = _EVENTS_BASE.format(from_table=from_table, asn_expr=asn_expr, prewhere_clause=prewhere_clause, where=where)
    execute = service.client.execute

    def wanted(name: str) -> bool:
        return sections is None or name in sections

    trace = HighScaleSections(service.service_id, "cmcd")
    want_overview = wanted("overview")
    req_ts = {
        "sessions_ts": wanted("sessions_ts"),
        "buffer_health_ts": wanted("buffer_health_ts"),
        "bitrate_ts": wanted("bitrate_ts"),
        "throughput_ts": wanted("throughput_ts"),
        "startup_ts": wanted("startup_ts"),
    }
    want_duration_dist = wanted("session_duration_dist")
    req_content = wanted("top_content")
    req_country = wanted("rebuffer_by_country")
    req_asn = wanted("rebuffer_by_asn")
    req_ot = wanted("object_type_dist")
    req_sf = wanted("streaming_format_dist")
    req_ladder = wanted("bitrate_ladder")
    req_st = wanted("stream_type_dist")
    fold_probe_into_ts = opt27 and any(req_ts.values())

    from backend import config as svcconfig

    try:
        svc_cfg = svcconfig.load_config(service.service_id)
    except Exception:
        svc_cfg = None
    if svc_cfg is not None:
        cmcd_available = svcconfig.is_cmcd_enabled(svc_cfg) or ("cmcd_sid" in svcconfig.active_log_field_ids(svc_cfg))
    else:
        cmcd_available = True

    tasks: list[tuple[str, Callable[[], dict[str, Any]]]] = []
    if not fold_probe_into_ts:
        sid_probe_expr = "sid_h" if opt27 else "sid"
        probe_select = [
            "count() AS events",
            f"uniqExact({sid_probe_expr}) AS sessions",
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
                f"uniqExactIf({sid_probe_expr}, bs = 1) AS rebuffer_sessions",
                "avgIf(br, ot = 'v') AS avg_br_v",
                "avg(br) AS avg_br_all",
                "avgIf(bl, ot = 'v') AS avg_bl_v",
                "avg(bl) AS avg_bl_all",
                "quantileTDigestIf(0.5)(mtp, ot = 'v') AS med_mtp_v",
                "quantileTDigest(0.5)(mtp) AS med_mtp_all",
            ]
        probe_from = (
            _events_sql(
                ("sid_h", "cid", "ot", "sf", "st", "country", "asn", "bl", "br", "mtp", "tb", "bs", "su"),
                from_table=from_table,
                asn_expr=asn_expr,
                prewhere_clause=prewhere_clause,
                where=where,
            )
            if opt27
            else events
        )
        tasks.append(
            (
                "probe",
                lambda: {
                    "_probe": (
                        execute(f"/* cmcd:probe */ SELECT {', '.join(probe_select)} FROM ({probe_from})", params)
                        or [{}]
                    )[0]
                },
            )
        )

    def _ev(*cols: str) -> str:
        return _events_sql(cols, from_table=from_table, asn_expr=asn_expr, prewhere_clause=prewhere_clause, where=where)

    need_peak_query = want_overview and not req_ts["sessions_ts"]
    need_duration_query = want_overview and not want_duration_dist
    if need_peak_query or need_duration_query:
        overview_events = _ev("ts", "sid_h" if opt27 else "sid")
        ov_sid = "sid_h" if opt27 else "sid"
        ov_hash = "sid_h" if opt27 else "cityHash64(sid)"

        def _run_overview_extras() -> dict[str, Any]:
            out: dict[str, Any] = {}
            if need_peak_query:
                peak_bucket = (
                    "toUnixTimestamp(toStartOfInterval(ts, INTERVAL {bucket_seconds:UInt32} SECOND))"
                    if opt27
                    else _BUCKET.format(column="ts")
                )
                peak = execute(
                    "/* cmcd:overview_peak */ SELECT max(viewers) AS peak_viewers FROM ("
                    f"SELECT uniqExact({ov_hash}) AS viewers FROM (" + overview_events + ") "
                    f"GROUP BY {peak_bucket})",
                    params,
                )
                out["_peak_viewers"] = int((peak[0].get("peak_viewers") if peak else 0) or 0)
            if need_duration_query:
                duration = execute(
                    "/* cmcd:overview_duration */ SELECT avg(duration) AS avg_duration FROM ("
                    "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                    f"FROM ({overview_events}) GROUP BY {ov_sid} HAVING duration > 0)",
                    params,
                )
                out["_avg_duration"] = duration[0].get("avg_duration") if duration else None
            return out

        tasks.append(("overview", _run_overview_extras))

    if any(req_ts.values()):
        if fold_probe_into_ts:
            ts_proj = [
                "toUnixTimestamp(toStartOfInterval(event_timestamp, INTERVAL {bucket_seconds:UInt32} SECOND)) AS bucket_ts"
            ]
            if req_ts["sessions_ts"] or want_overview:
                ts_proj.append("cityHash64(cmcd['sid']) AS sid_h")
            need_ot_ts = (
                want_overview
                or req_ts["buffer_health_ts"]
                or req_ts["bitrate_ts"]
                or req_ts["throughput_ts"]
                or req_ot
                or req_content
                or req_country
                or req_asn
                or req_ladder
            )
            if need_ot_ts:
                ts_proj.append(_EVENT_COL_EXPRS["ot"])
            need_bl_ts = (
                want_overview
                or req_ts["buffer_health_ts"]
                or req_ts["sessions_ts"]
                or req_content
                or req_country
                or req_asn
                or req_ladder
            )
            if need_bl_ts:
                ts_proj.append(_EVENT_COL_EXPRS["bl"])
            need_bs_ts = need_bl_ts
            if need_bs_ts:
                ts_proj.append(_EVENT_COL_EXPRS["bs"])
            need_br_ts = want_overview or req_ts["bitrate_ts"] or req_ladder
            if need_br_ts:
                ts_proj.append(_EVENT_COL_EXPRS["br"])
            if req_ts["bitrate_ts"]:
                ts_proj.append(_EVENT_COL_EXPRS["tb"])
            need_mtp_ts = want_overview or req_ts["throughput_ts"]
            if need_mtp_ts:
                ts_proj.append(_EVENT_COL_EXPRS["mtp"])
            if req_ts["startup_ts"]:
                ts_proj.append(_EVENT_COL_EXPRS["su"])
            if req_content:
                ts_proj.append("toUInt8(cmcd['cid'] != '') AS has_cid_f")
            if req_country:
                ts_proj.append("toUInt8(country != '') AS has_country_f")
            if req_asn:
                ts_proj.append("toUInt8(asn IS NOT NULL) AS has_asn_f")
            if req_sf:
                ts_proj.append("toUInt8(cmcd['sf'] != '') AS has_sf_f")
            if req_st:
                ts_proj.append("toUInt8(cmcd['st'] != '') AS has_st_f")
            ts_sub = f"\n    SELECT {', '.join(ts_proj)}\n    FROM {from_table}{prewhere_clause}\n    WHERE {where}\n"

            def _run_timeseries() -> dict[str, Any]:
                select = [
                    "grouping(bucket_ts) AS is_probe",
                    "bucket_ts",
                    "count() AS events",
                ]
                if need_bl_ts:
                    select.append("max(bl IS NOT NULL) AS has_bl")
                if need_br_ts:
                    select.append("max(br IS NOT NULL) AS has_br")
                if need_bs_ts:
                    select.append("max(bs IS NOT NULL) AS has_bs")
                if need_mtp_ts:
                    select.append("max(mtp IS NOT NULL) AS has_mtp")
                if need_ot_ts:
                    select.append("max(ot != '') AS has_ot")
                if req_ts["startup_ts"]:
                    select.append("max(su IS NOT NULL) AS has_su")
                if req_content:
                    select.append("max(has_cid_f) AS has_cid")
                if req_country:
                    select.append("max(has_country_f) AS has_country")
                if req_asn:
                    select.append("max(has_asn_f) AS has_asn")
                if req_sf:
                    select.append("max(has_sf_f) AS has_sf")
                if req_st:
                    select.append("max(has_st_f) AS has_st")
                if req_ts["sessions_ts"]:
                    select += [
                        "uniqExact(sid_h) AS concurrent_sessions",
                        "uniqExactIf(sid_h, bs = 1) AS _reb_sess",
                        "_reb_sess * 100.0 / nullIf(concurrent_sessions, 0) AS rebuffer_session_pct",
                    ]
                elif want_overview:
                    select += [
                        "uniqExact(sid_h) AS sessions",
                        "uniqExactIf(sid_h, bs = 1) AS rebuffer_sessions",
                    ]
                if req_ts["buffer_health_ts"]:
                    select += [
                        f"quantilesTDigestIf(0.5, 0.95)(bl, {_VIDEO}) AS _bl_q",
                        "_bl_q[1] AS p50_buffer",
                        "_bl_q[2] AS p95_buffer",
                        "quantilesTDigest(0.5, 0.95)(bl) AS _bl_q_all",
                        "_bl_q_all[1] AS p50_buffer_all",
                        "_bl_q_all[2] AS p95_buffer_all",
                        "countIf(bs = 1) * 100.0 / nullIf(count(), 0) AS starvation_rate",
                    ]
                if want_overview:
                    select += [
                        f"avgIf(bl, {_VIDEO}) AS avg_bl_v",
                        "avg(bl) AS avg_bl_all",
                    ]
                if req_ts["bitrate_ts"]:
                    select += [
                        f"avgIf(br, {_VIDEO}) AS avg_bitrate",
                        "avg(br) AS avg_bitrate_all",
                        f"avgIf(br / tb, {_VIDEO} AND tb > 0) AS utilization_ratio",
                        "avgIf(br / tb, tb > 0) AS utilization_ratio_all",
                    ]
                elif want_overview:
                    select += [
                        f"avgIf(br, {_VIDEO}) AS avg_br_v",
                        "avg(br) AS avg_br_all",
                    ]
                if req_ts["throughput_ts"]:
                    select += [
                        f"quantilesTDigestIf(0.5, 0.95, 0.99)(mtp, {_VIDEO}) AS _mtp_q",
                        "_mtp_q[1] AS p50_throughput",
                        "_mtp_q[2] AS p95_throughput",
                        "_mtp_q[3] AS p99_throughput",
                        "quantilesTDigest(0.5, 0.95, 0.99)(mtp) AS _mtp_q_all",
                        "_mtp_q_all[1] AS p50_throughput_all",
                        "_mtp_q_all[2] AS p95_throughput_all",
                        "_mtp_q_all[3] AS p99_throughput_all",
                    ]
                elif want_overview:
                    select += [
                        f"quantileTDigestIf(0.5)(mtp, {_VIDEO}) AS med_mtp_v",
                        "quantileTDigest(0.5)(mtp) AS med_mtp_all",
                    ]
                if req_ts["startup_ts"]:
                    select.append("countIf(su = 1) * 100.0 / nullIf(count(), 0) AS startup_ratio")
                rows = execute(
                    f"/* cmcd:timeseries */ SELECT {', '.join(select)} FROM ({ts_sub}) "
                    "GROUP BY GROUPING SETS ((bucket_ts), ()) ORDER BY is_probe, bucket_ts",
                    params,
                )
                probe_row = dict(next((r for r in rows if int(r.get("is_probe") or 0) == 1), {}))
                if probe_row:
                    if req_ts["sessions_ts"]:
                        probe_row["sessions"] = probe_row.get("concurrent_sessions")
                        probe_row["rebuffer_sessions"] = probe_row.get("_reb_sess")
                    if req_ts["bitrate_ts"]:
                        probe_row["avg_br_v"] = probe_row.get("avg_bitrate")
                        probe_row["avg_br_all"] = probe_row.get("avg_bitrate_all")
                    if req_ts["throughput_ts"]:
                        probe_row["med_mtp_v"] = probe_row.get("p50_throughput")
                        probe_row["med_mtp_all"] = probe_row.get("p50_throughput_all")
                ts_rows = [
                    {**r, "bucket": _format_bucket_ts(r["bucket_ts"])} for r in rows if int(r.get("is_probe") or 0) == 0
                ]
                return {"_probe": probe_row, "_ts_rows": ts_rows}

            tasks.append(("timeseries", _run_timeseries))
        else:
            ts_cols: list[str] = ["ts"]
            if req_ts["sessions_ts"]:
                ts_cols.extend(["sid", "bs"])
            if req_ts["buffer_health_ts"] or req_ts["bitrate_ts"] or req_ts["throughput_ts"]:
                ts_cols.append("ot")
            if req_ts["buffer_health_ts"]:
                ts_cols.append("bl")
                if "bs" not in ts_cols:
                    ts_cols.append("bs")
            if req_ts["bitrate_ts"]:
                ts_cols.extend(["br", "tb"])
            if req_ts["throughput_ts"]:
                ts_cols.append("mtp")
            if req_ts["startup_ts"]:
                ts_cols.append("su")
            ts_events = _ev(*ts_cols)

            def _run_timeseries() -> dict[str, Any]:
                select = [f"{_BUCKET.format(column='ts')} AS bucket"]
                if req_ts["sessions_ts"]:
                    select += [
                        "uniqExact(cityHash64(sid)) AS concurrent_sessions",
                        "uniqExactIf(cityHash64(sid), bs = 1) * 100.0 / nullIf(uniqExact(cityHash64(sid)), 0) AS rebuffer_session_pct",
                    ]
                if req_ts["buffer_health_ts"]:
                    select += [
                        f"quantilesTDigestIf(0.5, 0.95)(bl, {_VIDEO}) AS _bl_q",
                        "_bl_q[1] AS p50_buffer",
                        "_bl_q[2] AS p95_buffer",
                        "quantilesTDigest(0.5, 0.95)(bl) AS _bl_q_all",
                        "_bl_q_all[1] AS p50_buffer_all",
                        "_bl_q_all[2] AS p95_buffer_all",
                        "countIf(bs = 1) * 100.0 / nullIf(count(), 0) AS starvation_rate",
                    ]
                if req_ts["bitrate_ts"]:
                    select += [
                        f"avgIf(br, {_VIDEO}) AS avg_bitrate",
                        "avg(br) AS avg_bitrate_all",
                        f"avgIf(br / tb, {_VIDEO} AND tb > 0) AS utilization_ratio",
                        "avgIf(br / tb, tb > 0) AS utilization_ratio_all",
                    ]
                if req_ts["throughput_ts"]:
                    select += [
                        f"quantilesTDigestIf(0.5, 0.95, 0.99)(mtp, {_VIDEO}) AS _mtp_q",
                        "_mtp_q[1] AS p50_throughput",
                        "_mtp_q[2] AS p95_throughput",
                        "_mtp_q[3] AS p99_throughput",
                        "quantilesTDigest(0.5, 0.95, 0.99)(mtp) AS _mtp_q_all",
                        "_mtp_q_all[1] AS p50_throughput_all",
                        "_mtp_q_all[2] AS p95_throughput_all",
                        "_mtp_q_all[3] AS p99_throughput_all",
                    ]
                if req_ts["startup_ts"]:
                    select.append("countIf(su = 1) * 100.0 / nullIf(count(), 0) AS startup_ratio")
                rows = execute(
                    f"/* cmcd:timeseries */ SELECT {', '.join(select)} FROM ({ts_events}) GROUP BY bucket ORDER BY bucket",
                    params,
                )
                return {"_ts_rows": rows}

            tasks.append(("timeseries", _run_timeseries))

    # Consolidate per-session aggregations (new_sessions + session_duration_dist) into one scan.
    sess_group_col = "sid_h" if opt27 else "sid"
    sess_bucket_expr = (
        "toString(toUnixTimestamp(toStartOfInterval(first_ts, INTERVAL {bucket_seconds:UInt32} SECOND)))"
        if opt27
        else _BUCKET.format(column="first_ts")
    )
    if req_ts["sessions_ts"] and want_duration_dist:
        sess_events = _ev("ts", sess_group_col)

        def _run_sessions() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:sessions */ WITH sess AS ("
                f"SELECT {sess_group_col}, min(ts) AS first_ts, "
                "(toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                f"FROM ({sess_events}) GROUP BY {sess_group_col}) "
                f"SELECT 'new_sessions' AS kind, {sess_bucket_expr} AS k, "
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
                    b_key = _format_bucket_ts(row["k"]) if opt27 else str(row["k"])
                    new_sessions[b_key] = int(row.get("session_count") or 0)
                elif row.get("kind") == "duration":
                    dur_rows.append(row)
            dur_rows.sort(key=lambda r: int(r["k"]))
            out: dict[str, Any] = {
                "_new_sessions": new_sessions,
                "session_duration_dist": [
                    {
                        "duration_bucket": _DURATION_BUCKETS[int(row["k"])],
                        "session_count": _scale_count(row["session_count"], sample_stride),
                    }
                    for row in dur_rows
                ],
            }
            if want_overview:
                pos_count = sum(int(row.get("positive_duration_count") or 0) for row in dur_rows)
                if pos_count > 0:
                    pos_sum = sum(float(row.get("positive_duration_sum") or 0.0) for row in dur_rows)
                    out["_avg_duration"] = pos_sum / pos_count
                else:
                    out["_avg_duration"] = None
            return out

        tasks.append(("sessions", _run_sessions))
    elif req_ts["sessions_ts"]:
        sess_events = _ev("ts", sess_group_col)

        def _run_new_sessions() -> dict[str, Any]:
            first_seen = execute(
                "/* cmcd:new_sessions */ SELECT "
                f"{sess_bucket_expr} AS bucket, count() AS new_sessions FROM ("
                f"SELECT {sess_group_col}, min(ts) AS first_ts FROM ({sess_events}) GROUP BY {sess_group_col}) GROUP BY bucket",
                params,
            )
            return {
                "_new_sessions": {
                    (_format_bucket_ts(row["bucket"]) if opt27 else str(row["bucket"])): int(row["new_sessions"] or 0)
                    for row in first_seen
                }
            }

        tasks.append(("new_sessions", _run_new_sessions))
    elif want_duration_dist:
        sess_events = _ev("ts", sess_group_col)

        def _run_session_duration() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:session_duration_dist */ SELECT sort_order, count() AS session_count, "
                "sumIf(duration, duration > 0) AS positive_duration_sum, "
                "countIf(duration > 0) AS positive_duration_count FROM ("
                "SELECT duration, multiIf(duration < 30, 1, duration < 120, 2, duration < 300, 3, duration < 600, 4, "
                "duration < 1800, 5, duration < 3600, 6, 7) AS sort_order FROM ("
                "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                f"FROM ({sess_events}) GROUP BY {sess_group_col})) GROUP BY sort_order ORDER BY sort_order",
                params,
            )
            out: dict[str, Any] = {
                "session_duration_dist": [
                    {
                        "duration_bucket": _DURATION_BUCKETS[int(row["sort_order"])],
                        "session_count": _scale_count(row["session_count"], sample_stride),
                    }
                    for row in rows
                ]
            }
            if want_overview:
                pos_count = sum(int(row.get("positive_duration_count") or 0) for row in rows)
                if pos_count > 0:
                    pos_sum = sum(float(row.get("positive_duration_sum") or 0.0) for row in rows)
                    out["_avg_duration"] = pos_sum / pos_count
                else:
                    out["_avg_duration"] = None
            return out

        tasks.append(("session_duration", _run_session_duration))

    # Split session-level breakdowns (single-scan GROUPING SETS) from flat dimension breakdowns.
    req_content = wanted("top_content")
    req_country = wanted("rebuffer_by_country")
    req_asn = wanted("rebuffer_by_asn")
    req_ot = wanted("object_type_dist")
    req_sf = wanted("streaming_format_dist")
    req_ladder = wanted("bitrate_ladder")
    req_st = wanted("stream_type_dist")
    breakdown_cols = (
        "kind, k, session_count, request_count, rebuffer_rate, "
        "avg_br, avg_br_all, avg_bl, avg_bl_all, median_mtp, median_mtp_all"
    )

    sess_dims: list[str] = []
    if req_content:
        sess_dims.append("content")
    if req_country:
        sess_dims.append("country")
    if req_asn:
        sess_dims.append("asn")

    if sess_dims:
        need_br_bl = req_content or req_asn
        need_mtp = req_country
        sess_bd_cols: list[str] = ["sid_h", "ot", "bs"]
        if req_content:
            sess_bd_cols.append("cid")
        if req_country:
            sess_bd_cols.append("country")
        if req_asn:
            sess_bd_cols.append("asn")
        if need_br_bl:
            sess_bd_cols.extend(["br", "bl"])
        if need_mtp:
            sess_bd_cols.append("mtp")
        sess_bd_events = _ev(*sess_bd_cols)

    if len(sess_dims) >= 2:
        g_cid = "grouping(cid)" if req_content else "toUInt8(1)"
        g_country = "grouping(country)" if req_country else "toUInt8(1)"
        g_asn = "grouping(asn)" if req_asn else "toUInt8(1)"
        dim_cols: list[str] = []
        gsets: list[str] = []
        where_parts: list[str] = []
        having_parts: list[str] = []
        kind_cases: list[str] = []
        k_cases: list[str] = []
        if req_content:
            dim_cols.append("cid")
            gsets.append("(cid, sid_h)")
            where_parts.append("cid != ''")
            having_parts.append("(g_cid = 0 AND cid != '')")
            kind_cases.append("g_cid = 0, 'content'")
            k_cases.append("g_cid = 0, cid")
        if req_country:
            dim_cols.append("country")
            gsets.append("(country, sid_h)")
            where_parts.append("country != ''")
            having_parts.append("(g_country = 0 AND country != '')")
            kind_cases.append("g_country = 0, 'country'")
            k_cases.append("g_country = 0, country")
        if req_asn:
            dim_cols.append("asn")
            gsets.append("(asn, sid_h)")
            where_parts.append("asn IS NOT NULL")
            having_parts.append("(g_asn = 0 AND asn IS NOT NULL)")
            kind_cases.append("g_asn = 0, 'asn'")
            k_cases.append("g_asn = 0, toString(asn)")
        inner_aggs = ["max(if(bs = 1, 1, 0)) AS had_rebuffer"]
        tagged_cols = [
            f"multiIf({', '.join(kind_cases)}, '') AS kind",
            f"multiIf({', '.join(k_cases)}, '') AS k",
            "had_rebuffer",
        ]
        if need_br_bl:
            if opt27:
                inner_aggs.extend(
                    [
                        f"sumIf(br, {_VIDEO} AND br IS NOT NULL) AS sum_br",
                        f"countIf({_VIDEO} AND br IS NOT NULL) AS br_reqs",
                        "sumIf(br, br IS NOT NULL) AS sum_br_all",
                        "countIf(br IS NOT NULL) AS br_reqs_all",
                        f"sumIf(bl, {_VIDEO} AND bl IS NOT NULL) AS sum_bl",
                        f"countIf({_VIDEO} AND bl IS NOT NULL) AS bl_reqs",
                        "sumIf(bl, bl IS NOT NULL) AS sum_bl_all",
                        "countIf(bl IS NOT NULL) AS bl_reqs_all",
                    ]
                )
                tagged_cols.extend(
                    ["sum_br", "br_reqs", "sum_br_all", "br_reqs_all", "sum_bl", "bl_reqs", "sum_bl_all", "bl_reqs_all"]
                )
                br_bl_outer = (
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sum(sum_br) / nullIf(sum(br_reqs), 0))) AS avg_br, "
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sum(sum_br_all) / nullIf(sum(br_reqs_all), 0))) AS avg_br_all, "
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sum(sum_bl) / nullIf(sum(bl_reqs), 0))) AS avg_bl, "
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sum(sum_bl_all) / nullIf(sum(bl_reqs_all), 0))) AS avg_bl_all"
                )
            else:
                inner_aggs.extend(
                    [
                        f"avgIf(br, {_VIDEO}) AS avg_br",
                        f"countIf({_VIDEO} AND br IS NOT NULL) AS br_reqs",
                        "avg(br) AS avg_br_all",
                        "countIf(br IS NOT NULL) AS br_reqs_all",
                        f"avgIf(bl, {_VIDEO}) AS avg_bl",
                        f"countIf({_VIDEO} AND bl IS NOT NULL) AS bl_reqs",
                        "avg(bl) AS avg_bl_all",
                        "countIf(bl IS NOT NULL) AS bl_reqs_all",
                    ]
                )
                tagged_cols.extend(
                    ["avg_br", "br_reqs", "avg_br_all", "br_reqs_all", "avg_bl", "bl_reqs", "avg_bl_all", "bl_reqs_all"]
                )
                br_bl_outer = (
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sumIf(avg_br * br_reqs, avg_br IS NOT NULL) / nullIf(sumIf(br_reqs, avg_br IS NOT NULL), 0))) AS avg_br, "
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sumIf(avg_br_all * br_reqs_all, avg_br_all IS NOT NULL) / nullIf(sumIf(br_reqs_all, avg_br_all IS NOT NULL), 0))) AS avg_br_all, "
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sumIf(avg_bl * bl_reqs, avg_bl IS NOT NULL) / nullIf(sumIf(bl_reqs, avg_bl IS NOT NULL), 0))) AS avg_bl, "
                    "if(kind = 'country', CAST(NULL, 'Nullable(Float64)'), "
                    "toNullable(sumIf(avg_bl_all * bl_reqs_all, avg_bl_all IS NOT NULL) / nullIf(sumIf(bl_reqs_all, avg_bl_all IS NOT NULL), 0))) AS avg_bl_all"
                )
        else:
            br_bl_outer = (
                "CAST(NULL, 'Nullable(Float64)') AS avg_br, CAST(NULL, 'Nullable(Float64)') AS avg_br_all, "
                "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS avg_bl_all"
            )
        if need_mtp:
            inner_aggs.extend(
                [
                    f"quantileTDigestIf(0.5)(mtp, {_VIDEO}) AS median_mtp",
                    f"countIf({_VIDEO} AND mtp IS NOT NULL) AS mtp_reqs",
                    "quantileTDigest(0.5)(mtp) AS median_mtp_all",
                    "countIf(mtp IS NOT NULL) AS mtp_reqs_all",
                ]
            )
            tagged_cols.extend(["median_mtp", "mtp_reqs", "median_mtp_all", "mtp_reqs_all"])
            mtp_outer = (
                "if(kind = 'country', "
                "toNullable(sumIf(median_mtp * mtp_reqs, median_mtp IS NOT NULL) / nullIf(sumIf(mtp_reqs, median_mtp IS NOT NULL), 0)), "
                "CAST(NULL, 'Nullable(Float64)')) AS median_mtp, "
                "if(kind = 'country', "
                "toNullable(sumIf(median_mtp_all * mtp_reqs_all, median_mtp_all IS NOT NULL) / nullIf(sumIf(mtp_reqs_all, median_mtp_all IS NOT NULL), 0)), "
                "CAST(NULL, 'Nullable(Float64)')) AS median_mtp_all"
            )
        else:
            mtp_outer = (
                "CAST(NULL, 'Nullable(Float64)') AS median_mtp, CAST(NULL, 'Nullable(Float64)') AS median_mtp_all"
            )
        session_breakdown_sql = (
            f"/* cmcd:session_breakdowns */ WITH events AS ({sess_bd_events}), "
            "sess_agg AS ("
            f"SELECT {g_cid} AS g_cid, {g_country} AS g_country, {g_asn} AS g_asn, "
            f"{', '.join(dim_cols)}, sid_h, {', '.join(inner_aggs)} "
            f"FROM events WHERE {' OR '.join(where_parts)} "
            f"GROUP BY GROUPING SETS ({', '.join(gsets)}) "
            f"HAVING {' OR '.join(having_parts)}) "
            f"SELECT {breakdown_cols} FROM ("
            "SELECT kind, k, session_count, toUInt64(0) AS request_count, rebuffer_rate, "
            "avg_br, avg_br_all, avg_bl, avg_bl_all, median_mtp, median_mtp_all, "
            "row_number() OVER (PARTITION BY kind ORDER BY session_count DESC, k ASC) AS rn FROM ("
            "SELECT kind, k, toUInt64(count()) AS session_count, "
            "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
            f"{br_bl_outer}, {mtp_outer} "
            f"FROM (SELECT {', '.join(tagged_cols)} FROM sess_agg) "
            "GROUP BY kind, k)) WHERE rn <= {top_n:UInt32}"
        )

        def _run_session_breakdowns() -> dict[str, Any]:
            return {"_session_breakdown_rows": execute(session_breakdown_sql, params)}

        tasks.append(("session_breakdowns", _run_session_breakdowns))
    elif sess_dims:
        if req_content:
            single_sess_branch = (
                f"SELECT {breakdown_cols} FROM (SELECT 'content' AS kind, cid AS k, toUInt64(count()) AS session_count, "
                "toUInt64(0) AS request_count, "
                "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
                "toNullable(sumIf(avg_br * br_reqs, avg_br IS NOT NULL) / nullIf(sumIf(br_reqs, avg_br IS NOT NULL), 0)) AS avg_br, "
                "toNullable(sumIf(avg_br_all * br_reqs_all, avg_br_all IS NOT NULL) / nullIf(sumIf(br_reqs_all, avg_br_all IS NOT NULL), 0)) AS avg_br_all, "
                "toNullable(sumIf(avg_bl * bl_reqs, avg_bl IS NOT NULL) / nullIf(sumIf(bl_reqs, avg_bl IS NOT NULL), 0)) AS avg_bl, "
                "toNullable(sumIf(avg_bl_all * bl_reqs_all, avg_bl_all IS NOT NULL) / nullIf(sumIf(bl_reqs_all, avg_bl_all IS NOT NULL), 0)) AS avg_bl_all, "
                "CAST(NULL, 'Nullable(Float64)') AS median_mtp, CAST(NULL, 'Nullable(Float64)') AS median_mtp_all FROM ("
                "SELECT cid, sid_h, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
                f"avgIf(br, {_VIDEO}) AS avg_br, countIf({_VIDEO} AND br IS NOT NULL) AS br_reqs, "
                "avg(br) AS avg_br_all, countIf(br IS NOT NULL) AS br_reqs_all, "
                f"avgIf(bl, {_VIDEO}) AS avg_bl, countIf({_VIDEO} AND bl IS NOT NULL) AS bl_reqs, "
                "avg(bl) AS avg_bl_all, countIf(bl IS NOT NULL) AS bl_reqs_all "
                "FROM events WHERE cid != '' GROUP BY cid, sid_h) "
                "GROUP BY cid ORDER BY session_count DESC, k LIMIT {top_n:UInt32})"
            )
        elif req_country:
            single_sess_branch = (
                f"SELECT {breakdown_cols} FROM (SELECT 'country' AS kind, country AS k, toUInt64(count()) AS session_count, "
                "toUInt64(0) AS request_count, "
                "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
                "CAST(NULL, 'Nullable(Float64)') AS avg_br, CAST(NULL, 'Nullable(Float64)') AS avg_br_all, "
                "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS avg_bl_all, "
                "toNullable(sumIf(median_mtp * mtp_reqs, median_mtp IS NOT NULL) / nullIf(sumIf(mtp_reqs, median_mtp IS NOT NULL), 0)) AS median_mtp, "
                "toNullable(sumIf(median_mtp_all * mtp_reqs_all, median_mtp_all IS NOT NULL) / nullIf(sumIf(mtp_reqs_all, median_mtp_all IS NOT NULL), 0)) AS median_mtp_all FROM ("
                "SELECT country, sid_h, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
                f"quantileTDigestIf(0.5)(mtp, {_VIDEO}) AS median_mtp, countIf({_VIDEO} AND mtp IS NOT NULL) AS mtp_reqs, "
                "quantileTDigest(0.5)(mtp) AS median_mtp_all, countIf(mtp IS NOT NULL) AS mtp_reqs_all "
                "FROM events WHERE country != '' GROUP BY country, sid_h) "
                "GROUP BY country ORDER BY session_count DESC, k LIMIT {top_n:UInt32})"
            )
        else:
            single_sess_branch = (
                f"SELECT {breakdown_cols} FROM (SELECT 'asn' AS kind, toString(asn) AS k, toUInt64(count()) AS session_count, "
                "toUInt64(0) AS request_count, "
                "toNullable(sum(had_rebuffer) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
                "toNullable(sumIf(avg_br * br_reqs, avg_br IS NOT NULL) / nullIf(sumIf(br_reqs, avg_br IS NOT NULL), 0)) AS avg_br, "
                "toNullable(sumIf(avg_br_all * br_reqs_all, avg_br_all IS NOT NULL) / nullIf(sumIf(br_reqs_all, avg_br_all IS NOT NULL), 0)) AS avg_br_all, "
                "toNullable(sumIf(avg_bl * bl_reqs, avg_bl IS NOT NULL) / nullIf(sumIf(bl_reqs, avg_bl IS NOT NULL), 0)) AS avg_bl, "
                "toNullable(sumIf(avg_bl_all * bl_reqs_all, avg_bl_all IS NOT NULL) / nullIf(sumIf(bl_reqs_all, avg_bl_all IS NOT NULL), 0)) AS avg_bl_all, "
                "CAST(NULL, 'Nullable(Float64)') AS median_mtp, CAST(NULL, 'Nullable(Float64)') AS median_mtp_all FROM ("
                "SELECT asn, sid_h, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
                f"avgIf(br, {_VIDEO}) AS avg_br, countIf({_VIDEO} AND br IS NOT NULL) AS br_reqs, "
                "avg(br) AS avg_br_all, countIf(br IS NOT NULL) AS br_reqs_all, "
                f"avgIf(bl, {_VIDEO}) AS avg_bl, countIf({_VIDEO} AND bl IS NOT NULL) AS bl_reqs, "
                "avg(bl) AS avg_bl_all, countIf(bl IS NOT NULL) AS bl_reqs_all "
                "FROM events WHERE asn IS NOT NULL GROUP BY asn, sid_h) "
                "GROUP BY asn ORDER BY session_count DESC, k LIMIT {top_n:UInt32})"
            )

        def _run_session_breakdowns() -> dict[str, Any]:
            sql = f"/* cmcd:session_breakdowns */ WITH events AS ({sess_bd_events}) {single_sess_branch}"
            return {"_session_breakdown_rows": execute(sql, params)}

        tasks.append(("session_breakdowns", _run_session_breakdowns))

    flat_dims: list[tuple[str, str]] = []
    if req_ot:
        flat_dims.append(("ot", "ot"))
    if req_sf:
        flat_dims.append(("sf", "sf"))
    if req_ladder:
        flat_dims.append(("ladder", "ladder_k"))
    if req_st:
        flat_dims.append(("st", "st"))
    flat_event_cols: list[str] = []
    flat_sid_col = "sid_h" if opt27 else "sid"
    if req_sf or req_ladder or req_st:
        flat_event_cols.append(flat_sid_col)
    if req_ot or req_ladder:
        flat_event_cols.append("ot")
    if req_sf:
        flat_event_cols.append("sf")
    if req_st:
        flat_event_cols.append("st")
    if req_ladder:
        flat_event_cols.extend(["br", "bs"])
    flat_sql: str | None = None
    if len(flat_dims) >= 2:
        flat_gsets = ", ".join(f"({col})" for _, col in flat_dims)
        flat_kind_cases = ", ".join(f"grouping({col}) = 0, '{kind}'" for kind, col in flat_dims)
        flat_k_cases = ", ".join(f"grouping({col}) = 0, {col}" for _, col in flat_dims)
        sess_cases: list[str] = []
        for kind, col in flat_dims:
            if kind == "ot":
                sess_cases.append(f"grouping({col}) = 0, toUInt64(count())")
            elif kind == "sf":
                sess_cases.append(f"grouping({col}) = 0, toUInt64(uniqExact({flat_sid_col}))")
            elif opt27:
                sess_cases.append(f"grouping({col}) = 0, toUInt64(uniqExact(sid_h))")
            else:
                sess_cases.append(f"grouping({col}) = 0, toUInt64(uniqExactIf(sid, sid != ''))")
        reb_expr = (
            "if(grouping(ladder_k) = 0, toNullable(countIf(bs = 1) * 100.0 / nullIf(count(), 0)), CAST(NULL, 'Nullable(Float64)')) AS rebuffer_rate, "
            if req_ladder
            else "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, "
        )
        br_expr = (
            "if(grouping(ladder_k) = 0, toNullable(avg(br)), CAST(NULL, 'Nullable(Float64)')) AS avg_br, "
            if req_ladder
            else "CAST(NULL, 'Nullable(Float64)') AS avg_br, "
        )
        if opt27:
            ladder_col = (
                f", if(br IS NOT NULL AND br > 0 AND (({_VIDEO}) OR ot = ''), "
                f"if({_VIDEO}, "
                "multiIf(br < 1000, '1', br < 2500, '2', br < 5000, '3', br < 10000, '4', '5'), "
                "multiIf(br < 1000, 'all:1', br < 2500, 'all:2', br < 5000, 'all:3', br < 10000, 'all:4', 'all:5')"
                "), '') AS ladder_k"
                if req_ladder
                else ""
            )
        else:
            tier_expr = "toString(multiIf(br < 1000, 1, br < 2500, 2, br < 5000, 3, br < 10000, 4, 5))"
            ladder_col = (
                f", if(br IS NOT NULL AND br > 0 AND (({_VIDEO}) OR ot = ''), "
                f"if({_VIDEO}, {tier_expr}, concat('all:', {tier_expr})), '') AS ladder_k"
                if req_ladder
                else ""
            )
        flat_from = f"(SELECT {', '.join(flat_event_cols)}{ladder_col} FROM events)" if req_ladder else "events"
        flat_sql = (
            f"SELECT multiIf({flat_kind_cases}, '') AS kind, multiIf({flat_k_cases}, '') AS k, "
            f"multiIf({', '.join(sess_cases)}, toUInt64(0)) AS session_count, "
            f"toUInt64(count()) AS request_count, {reb_expr}{br_expr}"
            "CAST(NULL, 'Nullable(Float64)') AS avg_br_all, "
            "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS avg_bl_all, "
            "CAST(NULL, 'Nullable(Float64)') AS median_mtp, CAST(NULL, 'Nullable(Float64)') AS median_mtp_all "
            f"FROM {flat_from} "
            f"GROUP BY GROUPING SETS ({flat_gsets}) HAVING k != ''"
        )
    else:
        null_extras = (
            "CAST(NULL, 'Nullable(Float64)') AS avg_br_all, "
            "CAST(NULL, 'Nullable(Float64)') AS avg_bl, CAST(NULL, 'Nullable(Float64)') AS avg_bl_all, "
            "CAST(NULL, 'Nullable(Float64)') AS median_mtp, CAST(NULL, 'Nullable(Float64)') AS median_mtp_all"
        )
        uniq_sid = "uniqExact(sid_h)" if opt27 else "uniqExactIf(sid, sid != '')"
        if req_ot:
            flat_sql = (
                "SELECT 'ot' AS kind, ot AS k, toUInt64(count()) AS session_count, toUInt64(count()) AS request_count, "
                "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, CAST(NULL, 'Nullable(Float64)') AS avg_br, "
                f"{null_extras} FROM events WHERE ot != '' GROUP BY ot"
            )
        elif req_sf:
            flat_sql = (
                f"SELECT 'sf' AS kind, sf AS k, toUInt64(uniqExact({flat_sid_col})) AS session_count, toUInt64(count()) AS request_count, "
                "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, CAST(NULL, 'Nullable(Float64)') AS avg_br, "
                f"{null_extras} FROM events WHERE sf != '' GROUP BY sf"
            )
        elif req_ladder:
            tier_expr = "toString(multiIf(br < 1000, 1, br < 2500, 2, br < 5000, 3, br < 10000, 4, 5))"
            flat_sql = (
                "SELECT 'ladder' AS kind, "
                f"if({_VIDEO}, {tier_expr}, concat('all:', {tier_expr})) AS k, "
                f"toUInt64({uniq_sid}) AS session_count, toUInt64(count()) AS request_count, "
                "toNullable(countIf(bs = 1) * 100.0 / nullIf(count(), 0)) AS rebuffer_rate, "
                f"toNullable(avg(br)) AS avg_br, {null_extras} "
                f"FROM events WHERE (({_VIDEO}) OR ot = '') AND br IS NOT NULL AND br > 0 GROUP BY k"
            )
        elif req_st:
            flat_sql = (
                f"SELECT 'st' AS kind, st AS k, toUInt64({uniq_sid}) AS session_count, "
                "toUInt64(count()) AS request_count, "
                "CAST(NULL, 'Nullable(Float64)') AS rebuffer_rate, CAST(NULL, 'Nullable(Float64)') AS avg_br, "
                f"{null_extras} FROM events WHERE st != '' GROUP BY st"
            )
    if flat_sql is not None:
        flat_events = _ev(*flat_event_cols)

        def _run_breakdowns() -> dict[str, Any]:
            sql = f"/* cmcd:breakdowns */ WITH events AS ({flat_events}) {flat_sql}"
            return {"_breakdown_rows": execute(sql, params)}

        tasks.append(("breakdowns", _run_breakdowns))

    parallel_out = trace.run_parallel(
        tasks,
        source="projected_fact",
        activity=lambda key: "overhead" if key == "probe" else "compute",
        max_workers=8,
    )
    raw_results: dict[str, Any] = {}
    for d in parallel_out.values():
        raw_results.update(d)

    probe = raw_results.pop("_probe", {})
    if not int(probe.get("events") or 0):
        if sample_stride > 1:
            return _cmcd_aggregates_compute(
                service, req, start_time, end_time, sections, mask_ips, allow_sampling=False
            )
        return empty_cmcd_aggregates(available=cmcd_available, section_timings=trace.timings())

    has = {
        key: bool(int(probe.get(f"has_{key}") or 0))
        for key in ("bl", "br", "bs", "mtp", "ot", "sf", "st", "su", "tb", "cid")
    }
    # cmcd_bs is a sparse flag (1/true only on starvation, empty/NULL otherwise), so buffer-length
    # telemetry proves starvation tracking is active even in healthy windows with zero starvations.
    has_bs_effective = has["bs"] or has["bl"]
    has_country = bool(int(probe.get("has_country") or 0))
    has_asn = bool(int(probe.get("has_asn") or 0))
    has_video_filter = has["ot"]
    result: dict[str, Any] = {"available": cmcd_available, "has_data": True}
    overview_extras: dict[str, Any] = {
        k: raw_results[f"_{k}"] for k in ("peak_viewers", "avg_duration") if f"_{k}" in raw_results
    }
    if "peak_viewers" in overview_extras and sample_stride > 1:
        overview_extras["peak_viewers"] = _scale_count(overview_extras["peak_viewers"], sample_stride)

    if "_ts_rows" in raw_results:
        ts_rows = raw_results["_ts_rows"]
        by_bucket = {str(row["bucket"]): row for row in ts_rows}

        def _pick_ts(row: dict[str, Any], base_col: str) -> Any:
            if not has_video_filter and f"{base_col}_all" in row:
                return row.get(f"{base_col}_all")
            return row.get(base_col)

        def padded(field_map: dict[str, tuple[str, Any, bool]], default: dict[str, Any]) -> list[dict[str, Any]]:
            return _pad(
                start,
                end,
                int(req.bucket_seconds),
                {
                    bucket: {
                        out: (
                            convert(_pick_ts(row, src) if video_gated else row.get(src))
                            if (out != "starvation_rate" or has_bs_effective)
                            else None
                        )
                        for out, (src, convert, video_gated) in field_map.items()
                    }
                    for bucket, row in by_bucket.items()
                },
                default,
            )

        if req_ts["sessions_ts"] and want_overview:
            overview_extras["peak_viewers"] = _scale_count(
                max(
                    (int(row.get("concurrent_sessions") or 0) for row in ts_rows),
                    default=0,
                ),
                sample_stride,
            )
        if req_ts["sessions_ts"]:
            raw_new_sessions = raw_results.get("_new_sessions", {})
            if sample_stride > 1:
                scaled_ts_rows = [
                    {**row, "concurrent_sessions": _scale_count(row.get("concurrent_sessions"), sample_stride)}
                    for row in ts_rows
                ]
                new_sessions = {k: _scale_count(v, sample_stride) for k, v in raw_new_sessions.items()}
            else:
                scaled_ts_rows = ts_rows
                new_sessions = raw_new_sessions
            sessions_map = _sessions_ts(scaled_ts_rows, new_sessions)
            if not has_bs_effective:
                for entry in sessions_map.values():
                    entry["rebuffer_session_pct"] = None
            result["sessions_ts"] = _pad(
                start,
                end,
                int(req.bucket_seconds),
                sessions_map,
                CMCD_TS_DEFAULTS["sessions_ts"],
            )
        if req_ts["buffer_health_ts"] and has["bl"]:
            result["buffer_health_ts"] = padded(
                {
                    "p50_buffer": ("p50_buffer", _round, True),
                    "p95_buffer": ("p95_buffer", _round, True),
                    "starvation_rate": ("starvation_rate", _round, False),
                },
                CMCD_TS_DEFAULTS["buffer_health_ts"],
            )
        if req_ts["bitrate_ts"] and has["br"]:
            result["bitrate_ts"] = padded(
                {
                    "avg_bitrate": ("avg_bitrate", _round, True),
                    "utilization_ratio": ("utilization_ratio", _round, True),
                },
                CMCD_TS_DEFAULTS["bitrate_ts"],
            )
        if req_ts["throughput_ts"] and has["mtp"]:
            result["throughput_ts"] = padded(
                {
                    "p50": ("p50_throughput", _round, True),
                    "p95": ("p95_throughput", _round, True),
                    "p99": ("p99_throughput", _round, True),
                },
                CMCD_TS_DEFAULTS["throughput_ts"],
            )
        if req_ts["startup_ts"] and has["su"]:
            result["startup_ts"] = padded(
                {"startup_ratio": ("startup_ratio", _round, False)},
                CMCD_TS_DEFAULTS["startup_ts"],
            )

    if "session_duration_dist" in raw_results:
        result["session_duration_dist"] = raw_results["session_duration_dist"]

    if "_session_breakdown_rows" in raw_results or "_breakdown_rows" in raw_results:
        by_kind: dict[str, list[dict[str, Any]]] = {}
        for row in [
            *(raw_results.get("_session_breakdown_rows") or []),
            *(raw_results.get("_breakdown_rows") or []),
        ]:
            by_kind.setdefault(str(row.get("kind") or ""), []).append(row)

        def _pick_bd(row: dict[str, Any], col: str) -> Any:
            if not has_video_filter and f"{col}_all" in row:
                return row.get(f"{col}_all")
            return row.get(col)

        if req_content and has["cid"]:
            c_rows = sorted(
                by_kind.get("content", []),
                key=lambda r: (-int(r["session_count"]), str(r["k"])),
            )
            result["top_content"] = [
                {
                    "content_id": row["k"],
                    "session_count": _scale_count(row["session_count"], sample_stride),
                    "rebuffer_rate": _round(row.get("rebuffer_rate")) if has_bs_effective else None,
                    "avg_bitrate": _round(_pick_bd(row, "avg_br")),
                    "avg_buffer_length": _round(_pick_bd(row, "avg_bl")),
                }
                for row in c_rows
            ]
        if req_country and has_country and has_bs_effective:
            co_rows = sorted(
                by_kind.get("country", []),
                key=lambda r: (-int(r["session_count"]), str(r["k"])),
            )
            result["rebuffer_by_country"] = [
                {
                    "country": row["k"],
                    "rebuffer_rate": _round(row.get("rebuffer_rate")),
                    "session_count": _scale_count(row["session_count"], sample_stride),
                    "median_throughput": _round(_pick_bd(row, "median_mtp")),
                }
                for row in co_rows
            ]
        if req_asn and has_asn and has_bs_effective:
            a_rows = sorted(
                by_kind.get("asn", []),
                key=lambda r: (-int(r["session_count"]), int(r["k"])),
            )
            result["rebuffer_by_asn"] = [
                {
                    "asn": int(row["k"]),
                    "label": "",
                    "rebuffer_rate": _round(row.get("rebuffer_rate")),
                    "session_count": _scale_count(row["session_count"], sample_stride),
                    "avg_bitrate": _round(_pick_bd(row, "avg_br")),
                    "avg_buffer_length": _round(_pick_bd(row, "avg_bl")),
                }
                for row in a_rows
            ]
        if req_ot and has["ot"]:
            ot_rows = sorted(
                by_kind.get("ot", []),
                key=lambda r: (-int(r["session_count"]), str(r["k"])),
            )
            result["object_type_dist"] = [
                {"object_type": row["k"], "request_count": _scale_count(row["session_count"], sample_stride)}
                for row in ot_rows
            ]
        if req_sf and has["sf"]:
            sf_rows = sorted(
                by_kind.get("sf", []),
                key=lambda r: (-int(r["session_count"]), str(r["k"])),
            )
            result["streaming_format_dist"] = [
                {"streaming_format": row["k"], "session_count": _scale_count(row["session_count"], sample_stride)}
                for row in sf_rows
            ]
        if req_ladder and has["br"]:
            all_ladder = by_kind.get("ladder", [])
            if has_video_filter:
                filtered_ladder = [r for r in all_ladder if not str(r["k"]).startswith("all:")]
            else:
                pref_all = [r for r in all_ladder if str(r["k"]).startswith("all:")]
                filtered_ladder = pref_all if pref_all else all_ladder
            ladder_rows = sorted(
                filtered_ladder,
                key=lambda r: int(str(r["k"]).removeprefix("all:")),
            )
            result["bitrate_ladder"] = [
                {
                    "tier": _BITRATE_LADDER_TIERS[int(str(row["k"]).removeprefix("all:"))],
                    "request_count": _scale_count(row.get("request_count") or 0, sample_stride),
                    "session_count": _scale_count(row["session_count"], sample_stride),
                    "avg_bitrate": _round(row.get("avg_br")),
                    "rebuffer_rate": _round(row.get("rebuffer_rate")) if has_bs_effective else None,
                }
                for row in ladder_rows
                if int(str(row["k"]).removeprefix("all:")) in _BITRATE_LADDER_TIERS
            ]
        if req_st and has["st"]:
            st_rows = sorted(
                by_kind.get("st", []),
                key=lambda r: (-int(r["session_count"]), str(r["k"])),
            )
            result["stream_type_dist"] = [
                {
                    "stream_type": row["k"],
                    "session_count": _scale_count(row["session_count"], sample_stride),
                    "request_count": _scale_count(row.get("request_count") or 0, sample_stride),
                }
                for row in st_rows
            ]

    if want_overview:
        raw_sessions = int(probe["sessions"] or 0)
        rebuffer_sessions = int(probe.get("rebuffer_sessions") or 0)
        rebuffer_rate = (rebuffer_sessions * 100.0 / raw_sessions) if raw_sessions > 0 else None
        active_sessions = _scale_count(raw_sessions, sample_stride)
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
