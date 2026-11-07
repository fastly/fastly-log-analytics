"""High-scale ClickHouse insight evaluation and cache-collapse detail endpoints."""

from __future__ import annotations

import contextvars
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import wait as _wait_futures
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

from backend.core.clickhouse import ClickHouseError
from backend.core.request_telemetry import HighScaleSections
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
    build_insight_card,
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

_MAX_WORKERS = 4
_FIELD_SAMPLE_ROWS = 1000
_BASE_FIELDS = frozenset({"timestamp", "url", "country", "ip"})
_ASN_INSIGHT_MARKERS = ("asn", "metro")
_INSIGHTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
_INSIGHTS_TELEMETRY_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
# Results with error cards are kept briefly so retries and prewarms reuse the cards that did succeed.
_INSIGHTS_PARTIAL_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=60)
_CacheRegistry.register("high_scale.insights._INSIGHTS_CACHE", _INSIGHTS_CACHE)
_CacheRegistry.register("high_scale.insights._INSIGHTS_TELEMETRY_CACHE", _INSIGHTS_TELEMETRY_CACHE)
_CacheRegistry.register("high_scale.insights._INSIGHTS_PARTIAL_CACHE", _INSIGHTS_PARTIAL_CACHE)
# One evaluation scans the whole baseline once per detection (minutes on a busy service), so page requests
# never run it themselves: they get the last result at once while a single background evaluation per
# request shape refreshes it, and a cold shape answers with pending cards after a short wait.
_STALE_MAX_SECONDS = 24 * 3600
_COLD_WAIT_SECONDS = 5.0
_META_WAIT_SECONDS = 15.0
# A failed scan is raised to the next page request only while it is this recent; after that, retry.
_FAILURE_REPLAY_SECONDS = 60.0
_INSIGHTS_LAST_RESULT: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=_STALE_MAX_SECONDS)
_INSIGHTS_REFRESH_FAILURES: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=_STALE_MAX_SECONDS)
_CacheRegistry.register("high_scale.insights._INSIGHTS_LAST_RESULT", _INSIGHTS_LAST_RESULT)
_CacheRegistry.register("high_scale.insights._INSIGHTS_REFRESH_FAILURES", _INSIGHTS_REFRESH_FAILURES)
# Shapes with no result yet (a page is waiting) get their own pool, so prewarms and stale refreshes
# queued in _REFRESH_POOL never hold them back.
_COLD_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-insights-cold")
_REFRESH_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-insights-refresh")
_JOBS_LOCK = threading.Lock()


@dataclass
class _Job:
    context: contextvars.Context
    run: Callable[[], None] = lambda: None
    started: float | None = None
    promoted: bool = False
    future: Future = field(default_factory=Future)
    meta_ready: threading.Event = field(default_factory=threading.Event)
    preview: dict[str, Any] | None = None


_INSIGHTS_JOBS: dict[tuple[Any, ...], _Job] = {}


def _severity(items: list[dict[str, Any]]) -> str:
    return evaluate_insight_severity(None, items)


def _failure_message(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ClickHouseError) else type(exc).__name__


def _asn_context(service_id: str, rows: list[tuple[Any, ...]], insight_id: str) -> dict[str, Any]:
    if not rows or not any(marker in insight_id for marker in _ASN_INSIGHT_MARKERS):
        return {}
    return build_insight_asn_context(service_id, rows, insight_id, include_dma="metro" in insight_id)


def _evaluate(
    service: HighScaleService,
    window_hours: float,
    baseline_hours: float,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool,
    filter_clause: str,
    filter_params: dict[str, Any],
    on_meta: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[dict[str, Any], bool]:
    """Run every detection once; returns the envelope and whether any card is an error card."""
    trace = HighScaleSections(service.service_id, "insights")
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
        fut_history = trace.submit(
            meta_pool, "history", lambda: service.client.execute(history_sql, params), source="fact"
        )
        fut_sample = trace.submit(
            meta_pool, "fields", lambda: service.client.execute(sample_sql, params), source="fact"
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
            rows = [
                tuple(row.values())
                for row in trace.run(definition.id, lambda: service.client.execute(sql, params), source="fact")
            ]
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
    if on_meta is not None:
        pending_cards = [
            build_insufficient_baseline_card(
                definition.id,
                definition.title,
                definition.description,
                str(definition.category),
                baseline_hours,
                history_hours,
            )
            if detection.needs_baseline and history_hours < baseline_hours
            else build_insight_card(
                insight_id=definition.id,
                title=definition.title,
                description=definition.description,
                category=str(definition.category),
                severity="info",
                summary="Evaluating in the background; results appear when the scan finishes.",
                items=[],
            )
            | {"placeholder": True}
            for definition, detection in candidates
        ]
        on_meta(
            build_insights_envelope(
                pending_cards,
                window_start=window_start.isoformat(),
                window_end=now.isoformat(),
                baseline_start=baseline_start.isoformat(),
                baseline_end=window_start.isoformat(),
                computed_at="",
                window_hours=window_hours,
                baseline_hours=baseline_hours,
                window_total_requests=window_total,
                pending=True,
            )
        )
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
    return payload, any(c.get("severity") == "error" for c in cards)


def _store(cache_key: tuple[Any, ...], payload: dict[str, Any], partial: bool, queries: list[Any]) -> None:
    cached_queries = [{**q, "is_cached": True} if isinstance(q, dict) else q for q in queries]
    if partial:
        _INSIGHTS_PARTIAL_CACHE[cache_key] = (payload, cached_queries)
    else:
        _INSIGHTS_CACHE[cache_key] = payload
        _INSIGHTS_TELEMETRY_CACHE[cache_key] = cached_queries
        _INSIGHTS_PARTIAL_CACHE.pop(cache_key, None)
    _INSIGHTS_LAST_RESULT[cache_key] = (payload, cached_queries, time.monotonic())
    _INSIGHTS_REFRESH_FAILURES.pop(cache_key, None)


def _start_or_join(
    cache_key: tuple[Any, ...], evaluate: Callable[..., tuple[dict[str, Any], bool]], *, interactive: bool
) -> _Job:
    """Return the evaluation for ``cache_key``, starting one in the background if none runs.

    ``interactive`` (a page waiting on a shape with no result) queues it on the cold pool; a job still
    queued on the refresh pool is also queued there, and whichever thread picks it up first runs it.
    """
    from backend.utils.telemetry import clear_queries, get_queries

    job = _Job(context=contextvars.copy_context())

    def publish(preview: dict[str, Any]) -> None:
        job.preview = preview
        job.meta_ready.set()

    def refresh() -> None:
        with _JOBS_LOCK:
            if job.started is not None:
                return
            job.started = time.monotonic()
        clear_queries()
        try:
            payload, partial = evaluate(on_meta=publish)
            queries = list(get_queries())
            _store(cache_key, payload, partial, queries)
            job.future.set_result((payload, queries))
        except BaseException as exc:
            _INSIGHTS_REFRESH_FAILURES[cache_key] = (exc, time.monotonic())
            job.future.set_exception(exc)
        finally:
            job.meta_ready.set()
            with _JOBS_LOCK:
                if _INSIGHTS_JOBS.get(cache_key) is job:
                    del _INSIGHTS_JOBS[cache_key]

    job.run = refresh
    with _JOBS_LOCK:
        existing = _INSIGHTS_JOBS.get(cache_key)
        if existing is None:
            _INSIGHTS_JOBS[cache_key] = job
            created = True
        elif interactive and existing.started is None and not existing.promoted:
            existing.promoted = True
            job, created = existing, False
        else:
            return existing
    try:
        (_COLD_POOL if interactive else _REFRESH_POOL).submit(job.context.copy().run, job.run)
    except BaseException:
        if created:
            with _JOBS_LOCK:
                _INSIGHTS_JOBS.pop(cache_key, None)
        raise
    return job


def _job_result(cache_key: tuple[Any, ...], job: _Job) -> InsightsResponse:
    try:
        payload, queries = job.future.result()
    except BaseException:
        _INSIGHTS_REFRESH_FAILURES.pop(cache_key, None)
        raise
    return InsightsResponse.with_telemetry(
        **payload, debug_queries=[q.copy() if isinstance(q, dict) else q for q in queries]
    )


def insights_endpoint(
    service: HighScaleService,
    req: Any,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
) -> InsightsResponse:
    from backend.utils.telemetry import get_queries

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

    def evaluate(on_meta: Callable[[dict[str, Any]], None] | None = None) -> tuple[dict[str, Any], bool]:
        return _evaluate(
            service,
            window_hours,
            baseline_hours,
            clamp_start,
            clamp_end,
            mask_ips,
            filter_clause,
            filter_params,
            on_meta,
        )

    if not isinstance(service, HighScaleService):
        get_queries()
        payload, _ = evaluate()
        return InsightsResponse.with_telemetry(**payload)
    if force_refresh:
        return _job_result(cache_key, _start_or_join(cache_key, evaluate, interactive=False))

    cached = _INSIGHTS_CACHE.get(cache_key)
    cached_telemetry = _INSIGHTS_TELEMETRY_CACHE.get(cache_key)
    if cached is None and (partial := _INSIGHTS_PARTIAL_CACHE.get(cache_key)) is not None:
        cached, cached_telemetry = partial
    if cached is not None:
        cached_queries = [q.copy() if isinstance(q, dict) else q for q in (cached_telemetry or [])]
        return InsightsResponse.with_telemetry(
            **cached, is_cached=True, cache_age_seconds=0, debug_queries=cached_queries
        )
    if (last := _INSIGHTS_LAST_RESULT.get(cache_key)) is not None:
        payload, last_queries, stored_at = last
        failure, _ = _INSIGHTS_REFRESH_FAILURES.get(cache_key) or (None, 0.0)
        _start_or_join(cache_key, evaluate, interactive=False)
        return InsightsResponse.with_telemetry(
            **payload,
            is_cached=True,
            cache_age_seconds=int(time.monotonic() - stored_at),
            refreshing=True,
            refresh_error=_failure_message(failure) if isinstance(failure, Exception) else None,
            debug_queries=[q.copy() if isinstance(q, dict) else q for q in last_queries],
        )
    if (failed := _INSIGHTS_REFRESH_FAILURES.pop(cache_key, None)) is not None:
        failure, failed_at = failed
        if time.monotonic() - failed_at < _FAILURE_REPLAY_SECONDS:
            raise failure
    job = _start_or_join(cache_key, evaluate, interactive=True)
    # The wait counts from when the scan starts, not from when it was queued.
    began = job.started if job.started is not None else time.monotonic()
    _wait_futures([job.future], timeout=max(0.0, began + _COLD_WAIT_SECONDS - time.monotonic()))
    if job.future.done():
        return _job_result(cache_key, job)
    if job.started is not None:
        job.meta_ready.wait(max(0.0, job.started + _COLD_WAIT_SECONDS + _META_WAIT_SECONDS - time.monotonic()))
        if job.future.done():
            return _job_result(cache_key, job)
    if job.preview is not None:
        return InsightsResponse.with_telemetry(**job.preview)
    # Queued, or still reading history and fields: no detections are known yet.
    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    return InsightsResponse.with_telemetry(
        **build_insights_envelope(
            [],
            window_start=window_start.isoformat(),
            window_end=now.isoformat(),
            baseline_start=baseline_start.isoformat(),
            baseline_end=window_start.isoformat(),
            computed_at="",
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            pending=True,
        )
    )


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
