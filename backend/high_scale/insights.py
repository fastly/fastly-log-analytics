"""Insights (anomaly detection) for high-scale services, computed natively in ClickHouse.

Detections reuse the standard registry's row processors, severity rules and card text, fed by the
ClickHouse queries in :mod:`backend.high_scale.insight_queries`, so a card means the same thing on
both data planes. A detection whose required log field the service never emits is omitted, as on the
standard path; one whose query fails is reported as an ``error`` card rather than as "clean".
"""

from __future__ import annotations

import contextvars
import urllib.parse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.clickhouse_client import ClickHouseError
from backend.core.share_db.validation import mask_ip
from backend.high_scale._common import visible_fact_clauses
from backend.high_scale._filters import build_clickhouse_filters
from backend.high_scale.insight_queries import (
    AUTH_REGEX,
    BOT_UA_REGEX,
    COLUMNS,
    COMPRESSIBLE_REGEX,
    DETECTIONS,
    POP_CODE,
    PROBE_REGEX,
    Detection,
    botnet_grouping,
    impossible_distance,
    repeated_patterns_fp,
)
from backend.high_scale.registry import HighScaleService
from backend.models.insights import CacheCollapseDetailResponse, InsightsResponse
from backend.repositories.insights import InsightDefinition, registry  # noqa: F401  (registers the detections)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc

_MAX_WORKERS = 12
_FIELD_SAMPLE_ROWS = 1000
_BASE_FIELDS = frozenset({"timestamp", "url", "country", "ip"})
_ASN_INSIGHT_MARKERS = ("asn", "metro")

_INSIGHTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
_CacheRegistry.register("high_scale.insights._INSIGHTS_CACHE", _INSIGHTS_CACHE)


def _window(
    window_hours: float, baseline_hours: float, clamp_start: str | None, clamp_end: str | None
) -> tuple[datetime, datetime, datetime]:
    """``(baseline_start, window_start, now)``, honouring the analyst clamp exactly like the standard path."""
    now = datetime.now(UTC)
    if clamp_end:
        ceiling = parse_iso_utc(clamp_end)
        if ceiling and ceiling < now:
            now = ceiling
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        floor = parse_iso_utc(clamp_start)
        if floor:
            baseline_start = max(baseline_start, floor)
            window_start = max(window_start, floor)
    return baseline_start, window_start, now


def _base_sql(detection: Detection, insight_id: str) -> str:
    projection = ", ".join(f"{COLUMNS[column]} AS {column}" for column in detection.columns)
    lower = "{window_start:DateTime64(3)}" if detection.window_only else "{baseline_start:DateTime64(3)}"
    row_filter = f" AND ({detection.row_filter})" if detection.row_filter else ""
    scope = " AND ".join(visible_fact_clauses("request"))
    return (
        f"/* insight:{insight_id} */ WITH base AS (SELECT {projection}, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
        "event_timestamp >= {window_start:DateTime64(3)} AS is_w "
        f"FROM request_facts WHERE {scope} AND event_timestamp >= {lower} "
        f"AND event_timestamp <= {{window_end:DateTime64(3)}}{row_filter}) {detection.sql}"
    )


def _fingerprint_column(present: set[str]) -> str | None:
    for column in ("ja4", "ja3"):
        if column in present:
            return column
    return None


def _detection_for(insight_id: str, present: set[str]) -> Detection | None:
    """The ClickHouse query for ``insight_id``, or None when this service cannot compute it."""
    fingerprint = _fingerprint_column(present)
    if insight_id == "botnet_grouping":
        return botnet_grouping(fingerprint) if fingerprint else None
    if insight_id == "repeated_patterns_fp":
        return repeated_patterns_fp(fingerprint) if fingerprint else None
    if insight_id == "impossible_distance":
        from backend.utils.pop_utils import get_pop_lat_lon_map

        pops = {
            code: (float(lat), float(lon))
            for code, (lat, lon) in get_pop_lat_lon_map().items()
            if lat is not None and lon is not None and POP_CODE.fullmatch(code)
        }
        if not pops or not fingerprint:
            return None
        return impossible_distance(pops, fingerprint, edge_only="edge" in present)
    return DETECTIONS.get(insight_id)


def _make_investigate_url(
    service_id: str, window_start: datetime, now: datetime
) -> Callable[[dict[str, Any] | None], str]:
    def build(filters: dict[str, Any] | None = None) -> str:
        pairs: list[tuple[str, str]] = [
            ("service", service_id),
            ("start", window_start.isoformat()),
            ("end", now.isoformat()),
        ]
        pairs.extend((f"filter_{column}", str(value)) for column, value in (filters or {}).items() if value is not None)
        return "/dashboard?" + urllib.parse.urlencode(pairs)

    return build


def _severity(items: list[dict[str, Any]]) -> str:
    if not items:
        return "clean"
    return "critical" if any(item.get("severity") == "critical" for item in items) else "warning"


def _summary(definition: InsightDefinition, items: list[dict[str, Any]]) -> str:
    if not items:
        return f"No {definition.title.lower()} detected"
    if definition.id == "error_spikes":
        return f"{len(items)} URLs with elevated server error rates"
    if definition.id == "botnet_grouping":
        return f"{len(items)} fingerprints with suspicious IP spread"
    return f"{len(items)} anomalies detected"


def _card(definition: InsightDefinition, severity: str, summary: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": definition.id,
        "title": definition.title,
        "description": definition.description,
        "category": str(definition.category),
        "severity": severity,
        "summary": summary,
        "items": items,
    }


def _failure_message(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ClickHouseError) else type(exc).__name__


def _asn_context(service_id: str, rows: list[tuple[Any, ...]], insight_id: str) -> dict[str, Any]:
    if not rows or not any(marker in insight_id for marker in _ASN_INSIGHT_MARKERS):
        return {}
    from backend.core import duckdb as _db

    context: dict[str, Any] = {"asn_names": _db.get_asn_names(service_id, [row[0] for row in rows if row])}
    if "metro" in insight_id:
        context["dma_map"] = _db._get_dma_map()
    return context


def insights_endpoint(
    service: HighScaleService,
    req: Any,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
) -> InsightsResponse:
    window_hours = float(req.window_size_hrs)
    baseline_hours = float(req.baseline_hours)
    req_filters = getattr(req, "filters", None)
    filter_clause, filter_params = build_clickhouse_filters(req_filters) if req_filters else ("", {})
    from backend.repositories.utils.response_cache import bucket_time_to_minute

    if clamp_cache_key is not None:
        key_clamp = clamp_cache_key
    elif clamp_start or clamp_end:
        key_clamp = f"{bucket_time_to_minute(clamp_start) or ''}:{bucket_time_to_minute(clamp_end) or ''}"
    else:
        key_clamp = ""
    cache_key = (
        service.service_id,
        id(service.client),
        window_hours,
        baseline_hours,
        key_clamp,
        int(mask_ips),
        filter_clause,
        tuple(sorted((k, str(v)) for k, v in filter_params.items())),
    )
    use_cache = isinstance(service, HighScaleService)
    if use_cache and not force_refresh:
        cached = _INSIGHTS_CACHE.get(cache_key)
        if cached is not None:
            return InsightsResponse.with_telemetry(**cached, is_cached=True, cache_age_seconds=0)

    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    scope = " AND ".join(visible_fact_clauses("request"))
    if filter_clause:
        scope = f"{scope} AND ({filter_clause})"
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "probe_regex": PROBE_REGEX,
        "compress_regex": COMPRESSIBLE_REGEX,
        "auth_regex": AUTH_REGEX,
        "bot_ua_regex": BOT_UA_REGEX,
        **filter_params,
    }
    in_range = "event_timestamp >= {baseline_start:DateTime64(3)} AND event_timestamp <= {window_end:DateTime64(3)}"

    history_sql = (
        "/* insight:history */ SELECT min(event_timestamp) AS earliest, "
        "countIf(event_timestamp >= {window_start:DateTime64(3)}) AS window_total "
        f"FROM request_facts WHERE {scope} AND {in_range}"
    )
    sample_sql = (
        "/* insight:fields */ SELECT groupUniqArrayArray(mapKeys(custom_fields)) AS fields FROM ("
        f"SELECT custom_fields FROM request_facts WHERE {scope} AND {in_range} "
        f"ORDER BY event_timestamp DESC LIMIT {_FIELD_SAMPLE_ROWS})"
    )
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-insights-meta") as meta_pool:
        fut_history = meta_pool.submit(
            contextvars.copy_context().run, lambda: service.client.execute(history_sql, params)
        )
        fut_sample = meta_pool.submit(
            contextvars.copy_context().run, lambda: service.client.execute(sample_sql, params)
        )
        history = fut_history.result()
        sample = fut_sample.result()
    earliest = history[0].get("earliest") if history else None
    window_total = int(history[0].get("window_total") or 0) if history else 0
    earliest_dt = parse_iso_utc(earliest) if isinstance(earliest, str) else earliest
    if isinstance(earliest_dt, datetime):
        earliest_dt = earliest_dt.astimezone(UTC) if earliest_dt.tzinfo else earliest_dt.replace(tzinfo=UTC)
        history_hours = (now - earliest_dt).total_seconds() / 3600.0
    else:
        history_hours = 0.0
    present = set(_BASE_FIELDS)
    present.update(sample[0].get("fields") or [] if sample else [])
    fingerprint = _fingerprint_column(present) or "ja3"
    make_investigate_url = _make_investigate_url(service.service_id, window_start, now)

    def run(definition: InsightDefinition, detection: Detection) -> dict[str, Any]:
        if detection.needs_baseline and history_hours < baseline_hours:
            available = max(0.1, round(history_hours, 1))
            return _card(
                definition,
                "info",
                f"Requires {int(baseline_hours)}h of historical data (only {available}h available)",
                [],
            )
        try:
            sql = _base_sql(detection, definition.id)
            if filter_clause:
                sql = sql.replace(
                    "AND event_timestamp <= {window_end:DateTime64(3)}",
                    f"AND event_timestamp <= {{window_end:DateTime64(3)}} AND ({filter_clause})",
                )
            rows = [tuple(row.values()) for row in service.client.execute(sql, params)]
            context = {
                "window_hours": window_hours,
                "baseline_hours": baseline_hours,
                "fp_col": fingerprint,
                "actual_cols": sorted(present),
                "mask_ips": mask_ips,
                **_asn_context(service.service_id, rows, definition.id),
            }
            items: list[dict[str, Any]] = []
            for row in rows:
                item = definition.row_processor(row, definition, context) if definition.row_processor else {}
                if "investigate_url" not in item:
                    item["investigate_url"] = make_investigate_url(item.get("meta", {}).get("filters"))
                items.append(item)
        except Exception as exc:
            return _card(definition, "error", f"Query failed: {_failure_message(exc)}", [])
        severity = definition.severity_logic(items) if items and definition.severity_logic else _severity(items)
        return _card(definition, severity, _summary(definition, items), items)

    candidates: list[tuple[InsightDefinition, Detection]] = []
    for definition in registry.get_all():
        detection = _detection_for(definition.id, present)
        if detection is not None and all(column in present for column in definition.required_fields):
            candidates.append((definition, detection))
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="hs-insights") as pool:
        # Each task runs in a copy of this request's context so its ClickHouse calls stay attributed to the request.
        futures = [pool.submit(contextvars.copy_context().run, lambda pair=pair: run(*pair)) for pair in candidates]
        cards = [future.result() for future in futures]

    computed_at = now.isoformat()
    payload = {
        "insights": cards,
        "window_start": window_start.isoformat(),
        "window_end": computed_at,
        "baseline_start": baseline_start.isoformat(),
        "baseline_end": window_start.isoformat(),
        "computed_at": computed_at,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "window_total_requests": window_total,
    }
    if use_cache and not any(c.get("severity") == "error" for c in cards):
        _INSIGHTS_CACHE[cache_key] = payload
    return InsightsResponse.with_telemetry(**payload)


def _bucket_interval(total_hours: float) -> str:
    if total_hours <= 2:
        return "INTERVAL 1 MINUTE"
    if total_hours <= 12:
        return "INTERVAL 5 MINUTE"
    if total_hours <= 48:
        return "INTERVAL 15 MINUTE"
    if total_hours <= 168:
        return "INTERVAL 1 HOUR"
    if total_hours <= 720:
        return "INTERVAL 4 HOUR"
    return "INTERVAL 1 DAY"


def cache_collapse_detail_endpoint(
    service: HighScaleService,
    req: Any,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool = False,
) -> CacheCollapseDetailResponse:
    """Hit/miss timeline, disposition breakdown and recent misses for one URL, as on the standard path."""
    window_hours = float(req.window_size_hrs)
    baseline_hours = float(req.baseline_hours)
    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    scope = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "url = {url:String}",
            "event_timestamp >= {baseline_start:DateTime64(3)}",
            "event_timestamp <= {window_end:DateTime64(3)}",
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "url": req.url,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
    }
    cache = "custom_fields['cache']"
    hit, miss, passed = (f"startsWith({cache}, '{kind}')" for kind in ("HIT", "MISS", "PASS"))
    rates = service.client.execute(
        "/* insight:cache_collapse_rates */ SELECT "
        f"countIf(is_w AND {hit}) AS w_hits, countIf(is_w AND {miss}) AS w_miss, "
        f"countIf(is_w AND {passed}) AS w_pass, countIf(is_w) AS w_total, "
        f"countIf(is_b AND {hit}) AS b_hits, countIf(is_b AND {miss}) AS b_miss, "
        f"countIf(is_b AND {passed}) AS b_pass, countIf(is_b) AS b_total "
        "FROM (SELECT custom_fields, event_timestamp >= {window_start:DateTime64(3)} AS is_w, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b "
        f"FROM request_facts WHERE {scope})",
        params,
    )[0]
    w_hits, w_miss, w_pass, w_total, b_hits, b_miss, b_pass, b_total = (
        int(rates[key] or 0)
        for key in ("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total")
    )
    w_cacheable, b_cacheable = w_hits + w_miss, b_hits + b_miss
    w_rate = w_hits / w_cacheable if w_cacheable else 0.0
    b_rate = b_hits / b_cacheable if b_cacheable else 0.0

    timeline_rows = service.client.execute(
        "/* insight:cache_collapse_timeline */ SELECT "
        f"toStartOfInterval(event_timestamp, {_bucket_interval(window_hours + baseline_hours)}) AS bucket, "
        f"count() AS total_requests, countIf({hit}) AS real_hits, countIf({miss}) AS misses "
        f"FROM (SELECT event_timestamp, custom_fields FROM request_facts WHERE {scope}) "
        "GROUP BY bucket ORDER BY bucket ASC",
        params,
    )
    timeline = []
    for row in timeline_rows:
        real_hits, misses = int(row["real_hits"] or 0), int(row["misses"] or 0)
        cacheable = real_hits + misses
        timeline.append(
            {
                "bucket": _iso(row["bucket"]),
                "expected_hits": float(cacheable * b_rate),
                "real_hits": real_hits,
                "misses": misses,
                "total_requests": int(row["total_requests"] or 0),
                "hit_rate": float(real_hits / cacheable) if cacheable else 0.0,
            }
        )

    miss_rows = service.client.execute(
        "/* insight:cache_collapse_misses */ SELECT event_timestamp AS timestamp, "
        f"{cache} AS cache, custom_fields['pop'] AS pop, client_ip AS ip, toInt32OrNull(custom_fields['status']) AS status "
        f"FROM request_facts WHERE {scope} AND {miss} ORDER BY event_timestamp DESC LIMIT 50",
        params,
    )
    recent_misses = [
        {
            "timestamp": _iso(row["timestamp"]),
            "cache": row.get("cache") or "UNKNOWN",
            "pop": row.get("pop") or None,
            "ip": mask_ip(row["ip"]) if mask_ips and row.get("ip") else row.get("ip"),
            "status": int(row["status"]) if row.get("status") is not None else None,
        }
        for row in miss_rows
    ]
    return CacheCollapseDetailResponse.with_telemetry(
        url=req.url,
        has_data=bool(w_total + b_total),
        timeline=timeline,
        recent_misses=recent_misses,
        breakdown={
            "hits": w_hits,
            "misses": w_miss,
            "passes": w_pass,
            "other": max(0, w_total - w_hits - w_miss - w_pass),
        },
        baseline_hit_rate=float(b_rate) * 100,
        window_hit_rate=float(w_rate) * 100,
        baseline_pass_rate=float(b_pass / b_total) * 100 if b_total else 0.0,
        window_pass_rate=float(w_pass / w_total) * 100 if w_total else 0.0,
    )


from backend.high_scale._common import format_iso_utc_or_empty as _iso
