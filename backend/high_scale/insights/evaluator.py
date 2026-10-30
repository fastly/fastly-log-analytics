"""High-scale ClickHouse insight evaluation and cache-collapse detail endpoints."""

from __future__ import annotations

import contextvars
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

from backend.core.clickhouse import ClickHouseError
from backend.high_scale._common import (
    build_clickhouse_filters,
    visible_fact_clauses,
)
from backend.high_scale._common import format_iso_utc_or_empty as _iso_or_empty
from backend.models.analytics import CacheCollapseDetailResponse, InsightsResponse
from backend.repositories.insights import InsightDefinition, registry  # noqa: F401  (registers the detections)
from backend.repositories.insights.shaping import (
    AUTH_PATHS as _AUTH_PATHS,  # noqa: F401
)
from backend.repositories.insights.shaping import (
    AUTH_REGEX,
    BOT_UA_REGEX,
    COMPRESSIBLE_REGEX,
    PROBE_REGEX,
    build_error_insight_card,
    build_insufficient_baseline_card,
    compute_available_history_hours,
    evaluate_insight_severity,
    resolve_clamp_cache_key,
    shape_cache_collapse_detail,
    shape_insight_card_from_rows,
)
from backend.repositories.insights.shaping import (
    BOT_UA_TOKENS as _BOT_UA_TOKENS,  # noqa: F401
)
from backend.repositories.insights.shaping import (
    COMPRESSIBLE_EXTENSIONS as _COMPRESSIBLE_EXTENSIONS,  # noqa: F401
)
from backend.repositories.insights.shaping import (
    build_insight_card as _card,  # noqa: F401
)
from backend.repositories.insights.shaping import (
    clamp_insight_window as _window,
)
from backend.repositories.insights.shaping import (
    fingerprint_column as _fingerprint_column,
)
from backend.repositories.insights.shaping import (
    format_insight_summary as _summary,  # noqa: F401
)
from backend.repositories.insights.shaping import (
    make_investigate_url_builder as _make_investigate_url,
)
from backend.repositories.utils.response_shaping import (
    build_insight_asn_context,
    build_insights_envelope,
    select_insight_bucket_interval,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

from .sql_queries import Detection, _base_sql, _detection_for

_MAX_WORKERS = 12
_FIELD_SAMPLE_ROWS = 1000
_BASE_FIELDS = frozenset({"timestamp", "url", "country", "ip"})
_ASN_INSIGHT_MARKERS = ("asn", "metro")
_INSIGHTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
_CacheRegistry.register("high_scale.insights._INSIGHTS_CACHE", _INSIGHTS_CACHE)


def _severity(items: list[dict[str, Any]]) -> str:
    return evaluate_insight_severity(None, items)


def _failure_message(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ClickHouseError) else type(exc).__name__


def _asn_context(service_id: str, rows: list[tuple[Any, ...]], insight_id: str) -> dict[str, Any]:
    if not rows or not any(marker in insight_id for marker in _ASN_INSIGHT_MARKERS):
        return {}
    return build_insight_asn_context(service_id, rows, insight_id, include_dma="metro" in insight_id)


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
    key_clamp = resolve_clamp_cache_key(clamp_start, clamp_end, clamp_cache_key)
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
    from backend.high_scale.ingest_controller import HighScaleService

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
    history_hours = compute_available_history_hours(now, earliest)
    present = set(_BASE_FIELDS)
    present.update(sample[0].get("fields") or [] if sample else [])
    fingerprint = _fingerprint_column(present) or "ja3"
    make_investigate_url = _make_investigate_url(service.service_id, window_start, now)

    def run(definition: InsightDefinition, detection: Detection) -> dict[str, Any]:
        if detection.needs_baseline and history_hours < baseline_hours:
            return build_insufficient_baseline_card(
                definition.id,
                definition.title,
                definition.description,
                str(definition.category),
                baseline_hours,
                history_hours,
            )
        try:
            sql = _base_sql(detection, definition.id)
            if filter_clause:
                sql = sql.replace(
                    "AND event_timestamp <= {window_end:DateTime64(3)}",
                    f"AND event_timestamp <= {{window_end:DateTime64(3)}} AND ({filter_clause})",
                )
            rows = [tuple(row.values()) for row in service.client.execute(sql, params)]
            return shape_insight_card_from_rows(
                definition,
                rows,
                service_id=service.service_id,
                window_hours=window_hours,
                baseline_hours=baseline_hours,
                fp_col=fingerprint,
                actual_cols=sorted(present),
                mask_ips=mask_ips,
                investigate_url_builder=make_investigate_url,
            )
        except Exception as exc:
            return build_error_insight_card(
                definition.id,
                definition.title,
                str(definition.category),
                _failure_message(exc),
                description=definition.description,
            )

    candidates: list[tuple[InsightDefinition, Detection]] = []
    for definition in registry.get_all():
        detection = _detection_for(definition.id, present)
        if detection is not None and all(column in present for column in definition.required_fields):
            candidates.append((definition, detection))
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="hs-insights") as pool:
        futures = [pool.submit(contextvars.copy_context().run, lambda pair=pair: run(*pair)) for pair in candidates]
        cards = [future.result() for future in futures]
    computed_at = now.isoformat()
    payload = build_insights_envelope(
        cards,
        window_start=window_start.isoformat(),
        window_end=computed_at,
        baseline_start=baseline_start.isoformat(),
        baseline_end=window_start.isoformat(),
        computed_at=computed_at,
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        window_total_requests=window_total,
    )
    if use_cache and not any(c.get("severity") == "error" for c in cards):
        _INSIGHTS_CACHE[cache_key] = payload
    return InsightsResponse.with_telemetry(**payload)


compute_clickhouse_insights = insights_endpoint


def _bucket_interval(total_hours: float) -> str:
    return select_insight_bucket_interval(total_hours, clickhouse=True)


def cache_collapse_detail_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None, mask_ips: bool = False
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
    rates_row = service.client.execute(
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
    rates = tuple(
        int(rates_row[key] or 0)
        for key in ("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total")
    )
    timeline_rows = service.client.execute(
        "/* insight:cache_collapse_timeline */ SELECT "
        f"toStartOfInterval(event_timestamp, {_bucket_interval(window_hours + baseline_hours)}) AS bucket, "
        f"count() AS total_requests, countIf({hit}) AS real_hits, countIf({miss}) AS misses "
        f"FROM (SELECT event_timestamp, custom_fields FROM request_facts WHERE {scope}) "
        "GROUP BY bucket ORDER BY bucket ASC",
        params,
    )
    miss_rows = service.client.execute(
        "/* insight:cache_collapse_misses */ SELECT event_timestamp AS timestamp, "
        f"{cache} AS cache, custom_fields['pop'] AS pop, client_ip AS ip, toInt32OrNull(custom_fields['status']) AS status "
        f"FROM request_facts WHERE {scope} AND {miss} ORDER BY event_timestamp DESC LIMIT 50",
        params,
    )
    return CacheCollapseDetailResponse.with_telemetry(
        **shape_cache_collapse_detail(
            req.url,
            rates,  # type: ignore[arg-type]
            timeline_rows,
            miss_rows,
            mask_ips=mask_ips,
            include_has_data=True,
            iso_formatter=_iso_or_empty,
        )
    )
