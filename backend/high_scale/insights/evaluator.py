"""High-scale ClickHouse insight evaluation and cache-collapse detail endpoints."""

from __future__ import annotations

import contextvars
import inspect
import json
import logging
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import wait as _wait_futures
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

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
from backend.repositories.utils.response_cache import client_cache_token
from backend.repositories.utils.response_shaping import (
    build_insight_asn_context,
    build_insights_envelope,
    select_insight_bucket_interval,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

from .batches import DetectionBatch, plan_execution
from .rollup import ROLLUP_DETECTIONS, baseline_hours_span, coverage_sql, covered_span, hour_totals, rollup_base_sql
from .sql_queries import Detection, _base_sql, _detection_for, rewrite_materialized_cf

logger = logging.getLogger(__name__)

_MAX_WORKERS = 3
_BACKGROUND_MAX_WORKERS = 2
_BACKGROUND_QUERY_MAX_THREADS = str(max(1, int(os.getenv("INSIGHTS_BG_MAX_THREADS", "2") or "2")))
_INSIGHT_MARKER_RE = re.compile(r"^\s*/\*\s*insight:\w+\s*\*/\s*")
_MULTIPLEX_GROUPS: tuple[tuple[str, frozenset[str]], ...] = ()
_FIELD_SAMPLE_ROWS = 1000
_BASE_FIELDS = frozenset({"timestamp", "url", "country", "ip"})
_ASN_INSIGHT_MARKERS = ("asn", "metro")
_INSIGHTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
_INSIGHTS_TELEMETRY_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
# Results with error cards are kept briefly so retries and prewarms reuse the cards that did succeed.
_INSIGHTS_PARTIAL_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=60)
_INSIGHTS_HISTORY_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60)
_INSIGHTS_COVERAGE_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=900)
_CacheRegistry.register("high_scale.insights._INSIGHTS_CACHE", _INSIGHTS_CACHE)
_CacheRegistry.register("high_scale.insights._INSIGHTS_TELEMETRY_CACHE", _INSIGHTS_TELEMETRY_CACHE)
_CacheRegistry.register("high_scale.insights._INSIGHTS_PARTIAL_CACHE", _INSIGHTS_PARTIAL_CACHE)
_CacheRegistry.register("high_scale.insights._INSIGHTS_HISTORY_CACHE", _INSIGHTS_HISTORY_CACHE)
_CacheRegistry.register("high_scale.insights._INSIGHTS_COVERAGE_CACHE", _INSIGHTS_COVERAGE_CACHE)
# One evaluation scans the whole baseline once per detection (minutes on a busy service), so page requests
# never run it themselves: they get the last result at once while a single background evaluation per
# request shape refreshes it, and a cold shape answers with pending cards after a short wait.
_STALE_MAX_SECONDS = 24 * 3600
_COLD_WAIT_SECONDS = 2.0
_META_WAIT_SECONDS = 15.0
# A failed scan is raised to the next page request only while it is this recent; after that, retry.
_FAILURE_REPLAY_SECONDS = 60.0
_INSIGHTS_LAST_RESULT: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=_STALE_MAX_SECONDS)
_INSIGHTS_REFRESH_FAILURES: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=_STALE_MAX_SECONDS)
_CacheRegistry.register("high_scale.insights._INSIGHTS_LAST_RESULT", _INSIGHTS_LAST_RESULT)
_CacheRegistry.register("high_scale.insights._INSIGHTS_REFRESH_FAILURES", _INSIGHTS_REFRESH_FAILURES)
# Shapes with no result yet (a page is waiting) get their own pool, so prewarms and stale refreshes
# queued in _REFRESH_POOL never hold them back.
_COLD_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hs-insights-cold")
_REFRESH_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hs-insights-refresh")
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


def _use_materialized_cf(client: Any) -> bool:
    """Return True only when the ClickHouse client confirms materialized ``_cf_*`` columns are ready."""
    checker = getattr(client, "_has_materialized_cf_columns", None)
    if callable(checker):
        return bool(checker())
    return bool(getattr(client, "_enable_cf_rewrite_in_tests", False))


def _failure_message(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ClickHouseError) else type(exc).__name__


def _asn_context(service_id: str, rows: list[tuple[Any, ...]], insight_id: str) -> dict[str, Any]:
    if not rows or not any(marker in insight_id for marker in _ASN_INSIGHT_MARKERS):
        return {}
    return build_insight_asn_context(service_id, rows, insight_id, include_dma="metro" in insight_id)


def _rollup_spans(
    history: dict[str, Any], coverage: list[dict[str, Any]], baseline_start: datetime, window_start: datetime
) -> dict[str, tuple[datetime, datetime]]:
    """Per rollup shape, the baseline hours its rows can replace (absent when none match the raw facts)."""
    hours = history.get("hours")
    if not hours or not coverage:
        return {}
    raw = hour_totals(zip(hours, history.get("hour_rows") or [], history.get("hour_checksums") or [], strict=True))
    by_shape: dict[str, list[tuple[object, object, object]]] = {}
    for row in coverage:
        by_shape.setdefault(str(row["shape"]), []).append((row["hour"], row["row_count"], row["batch_checksum"]))
    spans: dict[str, tuple[datetime, datetime]] = {}
    for shape, rows in by_shape.items():
        if span := covered_span(baseline_start, window_start, raw, hour_totals(rows)):
            spans[shape] = span
    return spans


def _evaluate(
    service: HighScaleService,
    window_hours: float,
    baseline_hours: float,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool,
    filter_clause: str,
    filter_params: dict[str, Any],
    enabled_fields: frozenset[str],
    on_meta: Callable[[dict[str, Any]], None] | None = None,
    *,
    max_workers: int = _MAX_WORKERS,
    query_settings: Mapping[str, str] | None = None,
    bypass_history_cache: bool = False,
) -> tuple[dict[str, Any], bool]:
    """Run every detection once; returns the envelope and whether any card is an error card.

    The log field settings (``enabled_fields``) decide which insights run; the sampled ``present``
    columns only shape each detection's SQL, so rows logged before a field was enabled can't break it.
    """
    trace = HighScaleSections(service.service_id, "insights")
    accepts_settings = bool(query_settings) and "settings" in inspect.signature(service.client.execute).parameters

    def _exec(sql: str, q_params: dict[str, Any]) -> list[dict[str, Any]]:
        if accepts_settings and query_settings:
            return service.client.execute(sql, q_params, settings=query_settings)
        return service.client.execute(sql, q_params)

    use_cf = _use_materialized_cf(service.client)
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
    in_range = "event_timestamp >= {baseline_start:DateTime64(3)} AND event_timestamp < {window_end:DateTime64(3)}"
    # The rollup has no per-request columns, so a filtered evaluation always reads raw facts.
    use_rollup = not filter_clause
    if use_rollup:
        # Per-hour row counts and batch checksums let the rollup replace exactly the hours it matches.
        history_sql = (
            "/* insight:history */ SELECT min(earliest) AS earliest, sum(window_total) AS window_total, "
            "groupArray(hour) AS hours, groupArray(row_count) AS hour_rows, "
            "groupArray(batch_checksum) AS hour_checksums FROM ("
            "SELECT toUInt32(toUnixTimestamp(toStartOfHour(event_timestamp))) AS hour, "
            "min(event_timestamp) AS earliest, "
            "countIf(event_timestamp >= {window_start:DateTime64(3)}) AS window_total, "
            "count() AS row_count, sum(cityHash64(batch_id)) AS batch_checksum "
            f"FROM request_facts PREWHERE {scope} AND {in_range} GROUP BY hour)"
        )
    else:
        history_sql = (
            "/* insight:history */ SELECT min(event_timestamp) AS earliest, "
            "countIf(event_timestamp >= {window_start:DateTime64(3)}) AS window_total "
            f"FROM request_facts PREWHERE {scope} AND {in_range}"
        )
    coverage_start, coverage_end = baseline_hours_span(baseline_start, window_start)
    coverage_params = {**params, "coverage_start": coverage_start, "coverage_end": coverage_end}

    def read_coverage(cache_key: tuple[Any, ...]) -> list[dict[str, Any]]:
        try:
            rows = _exec(coverage_sql(), coverage_params)
        except Exception as exc:
            logger.warning("insights rollup coverage unavailable for %s: %s", service.service_id, exc)
            return []
        _INSIGHTS_COVERAGE_CACHE[cache_key] = rows
        return rows

    sample_sql = (
        "/* insight:fields */ SELECT groupUniqArrayArray(mapKeys(custom_fields)) AS fields FROM ("
        f"SELECT custom_fields FROM request_facts PREWHERE {scope} AND {in_range} "
        f"ORDER BY event_timestamp DESC LIMIT {_FIELD_SAMPLE_ROWS})"
    )
    # Pending cards need only history and fields; the coverage read finishes behind them.
    history_cache_key = (
        service.service_id,
        client_cache_token(service.client),
        use_rollup,
        filter_clause,
        tuple(sorted((k, str(v)) for k, v in filter_params.items())),
        int(baseline_start.timestamp()) // 60,
        int(window_start.timestamp()) // 60,
        int(now.timestamp()) // 60,
    )
    cached_history = None if bypass_history_cache else _INSIGHTS_HISTORY_CACHE.get(history_cache_key)
    meta_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-insights-meta")
    fut_coverage = None
    cached_coverage: list[dict[str, Any]] | None = None
    try:
        fut_history = (
            trace.submit(meta_pool, "history", lambda: _exec(history_sql, params), source="fact")
            if cached_history is None
            else None
        )
        fut_sample = trace.submit(meta_pool, "fields", lambda: _exec(sample_sql, params), source="fact")
        if fut_history is not None:
            history = fut_history.result()
            _INSIGHTS_HISTORY_CACHE[history_cache_key] = history
        else:
            history = cached_history or []
        sample = fut_sample.result()
        earliest = history[0].get("earliest") if history else None
        window_total = int(history[0].get("window_total") or 0) if history else 0
        history_hours = compute_available_history_hours(now, earliest)
        if use_rollup and coverage_start < coverage_end and history_hours >= baseline_hours and history[0].get("hours"):
            cov_start_ts = int(coverage_start.timestamp())
            cov_end_ts = int(coverage_end.timestamp())
            cov_pairs = tuple(
                sorted(
                    (int(h), str(cs))
                    for h, cs in zip(
                        history[0].get("hours") or [],
                        history[0].get("hour_checksums") or [],
                        strict=False,
                    )
                    if cov_start_ts <= int(h) < cov_end_ts
                )
            )
            coverage_cache_key = (
                service.service_id,
                client_cache_token(service.client),
                cov_start_ts,
                cov_end_ts,
                cov_pairs,
            )
            cached_coverage = None if bypass_history_cache else _INSIGHTS_COVERAGE_CACHE.get(coverage_cache_key)
            if cached_coverage is None:
                fut_coverage = trace.submit(
                    meta_pool,
                    "rollup_coverage",
                    lambda: read_coverage(coverage_cache_key),
                    source="projection",
                )
    finally:
        meta_pool.shutdown(wait=False)
    present = set(_BASE_FIELDS)
    present.update(sample[0].get("fields") or [] if sample else [])
    enabled_set = set(enabled_fields)
    fingerprint = _fingerprint_column(enabled_set) or "ja3"
    make_investigate_url = _make_investigate_url(service.service_id, window_start, now)

    def _shape(definition: InsightDefinition, rows: list[tuple[Any, ...]]) -> dict[str, Any]:
        return shape_insight_card_from_rows(
            definition,
            rows,
            service_id=service.service_id,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            fp_col=fingerprint,
            actual_cols=sorted(enabled_set),
            mask_ips=mask_ips,
            investigate_url_builder=make_investigate_url,
        )

    @dataclass
    class _QueryUnit:
        key: str
        sql: str
        query_params: dict[str, Any]
        source: Literal["fact", "mixed"]
        finish: Callable[[list[dict[str, Any]]], list[tuple[str, dict[str, Any]]]]
        on_error: Callable[[Exception], list[tuple[str, dict[str, Any]]]]
        run_fn: Callable[[], list[tuple[str, dict[str, Any]]]] | None = None

    def _error_card(definition: InsightDefinition, msg: str) -> tuple[str, dict[str, Any]]:
        return (
            definition.id,
            build_error_insight_card(
                definition.id,
                definition.title,
                str(definition.category),
                msg,
                description=definition.description,
            ),
        )

    def _prepare_standalone(definition: InsightDefinition, detection: Detection) -> _QueryUnit:
        shape = ROLLUP_DETECTIONS.get(definition.id)
        span = rollup_spans.get(shape) if shape else None
        query_params = params
        source: Literal["fact", "mixed"] = "fact"
        if shape and span:
            sql = rollup_base_sql(
                detection, definition.id, shape, baseline_hours=baseline_hours, window_hours=window_hours
            )
            query_params = {**params, "rollup_start": span[0], "rollup_end": span[1]}
            source = "mixed"
        else:
            sql = _base_sql(detection, definition.id, baseline_hours=baseline_hours, window_hours=window_hours)
        if filter_clause:
            sql = sql.replace(
                "AND event_timestamp < {window_end:DateTime64(3)}",
                f"AND event_timestamp < {{window_end:DateTime64(3)}} AND ({filter_clause})",
            )
        sql = rewrite_materialized_cf(sql, query_params, use_cf=use_cf)

        def _finish(raw_rows: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
            try:
                return [(definition.id, _shape(definition, [tuple(row.values()) for row in raw_rows]))]
            except Exception as exc:
                return [_error_card(definition, _failure_message(exc))]

        return _QueryUnit(
            key=definition.id,
            sql=sql,
            query_params=query_params,
            source=source,
            finish=_finish,
            on_error=lambda exc: [_error_card(definition, _failure_message(exc))],
        )

    def _prepare_batch(batch: DetectionBatch, members: list[tuple[InsightDefinition, Detection]]) -> _QueryUnit:
        active_ids = {definition.id for definition, _ in members}
        span = rollup_spans.get(batch.rollup_shape) if batch.rollup_shape else None
        build_kwargs: dict[str, Any] = {"present": enabled_set, "rollup_span": span, "filter_clause": filter_clause}
        build_params = inspect.signature(batch.build_sql).parameters
        if "baseline_hours" in build_params:
            build_kwargs["baseline_hours"] = baseline_hours
        if "window_hours" in build_params:
            build_kwargs["window_hours"] = window_hours
        query_params = (
            {**params, "rollup_start": span[0], "rollup_end": span[1]} if (batch.rollup_shape and span) else params
        )

        def _on_error(exc: Exception) -> list[tuple[str, dict[str, Any]]]:
            msg = _failure_message(exc)
            return [_error_card(definition, msg) for definition, _ in members]

        def _finish(raw_rows: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
            try:
                per_detector = batch.extract_rows(
                    raw_rows, active_ids, window_hours=window_hours, baseline_hours=baseline_hours
                )
            except Exception as exc:
                return _on_error(exc)
            out: list[tuple[str, dict[str, Any]]] = []
            for definition, _ in members:
                try:
                    out.append((definition.id, _shape(definition, per_detector.get(definition.id, []))))
                except Exception as exc:
                    out.append(_error_card(definition, _failure_message(exc)))
            return out

        if batch.build_candidates_sql is not None:
            cand_kwargs: dict[str, Any] = {
                "present": enabled_set,
                "rollup_span": span,
                "filter_clause": filter_clause,
            }
            if "window_hours" in inspect.signature(batch.build_candidates_sql).parameters:
                cand_kwargs["window_hours"] = window_hours
            cand_sql = rewrite_materialized_cf(
                batch.build_candidates_sql(active_ids, **cand_kwargs),
                params,
                use_cf=use_cf,
            )

            def _run_with_candidates() -> list[tuple[str, dict[str, Any]]]:
                try:
                    cand_rows = trace.run(
                        f"batch_{batch.name}_candidates",
                        lambda: _exec(cand_sql, params),
                        source="fact",
                    )
                    parsed = batch.parse_candidates(active_ids, cand_rows) if batch.parse_candidates else None
                    if parsed is None:
                        return _finish([])
                    cand_params: dict[str, Any] = {}
                    built_sql, built_source = batch.build_sql(
                        active_ids,
                        **build_kwargs,
                        candidates=parsed,
                        candidate_params=cand_params,
                    )
                    merged_params = {**query_params, **cand_params}
                    built_sql = rewrite_materialized_cf(built_sql, merged_params, use_cf=use_cf)
                    raw_rows = trace.run(
                        f"batch_{batch.name}", lambda: _exec(built_sql, merged_params), source=built_source
                    )
                except Exception as exc:
                    return _on_error(exc)
                return _finish(raw_rows)

            return _QueryUnit(
                key=f"batch_{batch.name}",
                sql="",
                query_params=query_params,
                source="fact",
                finish=_finish,
                on_error=_on_error,
                run_fn=_run_with_candidates,
            )

        sql, source = batch.build_sql(active_ids, **build_kwargs)
        sql = rewrite_materialized_cf(sql, query_params, use_cf=use_cf)
        return _QueryUnit(
            key=f"batch_{batch.name}",
            sql=sql,
            query_params=query_params,
            source=source,
            finish=_finish,
            on_error=_on_error,
        )

    def _run_unit(unit: _QueryUnit) -> list[tuple[str, dict[str, Any]]]:
        if unit.run_fn is not None:
            return unit.run_fn()
        try:
            raw_rows = trace.run(unit.key, lambda: _exec(unit.sql, unit.query_params), source=unit.source)
        except Exception as exc:
            return unit.on_error(exc)
        return unit.finish(raw_rows)

    def _run_group(group_name: str, group_units: list[_QueryUnit]) -> list[tuple[str, dict[str, Any]]]:
        if len(group_units) == 1:
            return _run_unit(group_units[0])
        if group_name.startswith("group_rollup_") and not all(unit.source == "mixed" for unit in group_units):
            out_individual: list[tuple[str, dict[str, Any]]] = []
            for unit in group_units:
                out_individual.extend(_run_unit(unit))
            return out_individual
        combined_params = dict(params)
        branches: list[str] = []
        for idx, unit in enumerate(group_units):
            branch_sql = _INSIGHT_MARKER_RE.sub("", unit.sql, count=1).strip()
            for pname in ("rollup_start", "rollup_end"):
                if pname in unit.query_params:
                    aliased = f"{pname}_{idx}"
                    branch_sql = branch_sql.replace(f"{{{pname}:", f"{{{aliased}:")
                    combined_params[aliased] = unit.query_params[pname]
            branches.append(f"SELECT '{unit.key}' AS _q, formatRow('JSONEachRow', *) AS _r FROM ({branch_sql})")
        combined_sql = f"/* insight:{group_name} */ " + " UNION ALL ".join(branches)
        group_source: Literal["fact", "mixed"] = (
            "mixed" if any(unit.source == "mixed" for unit in group_units) else "fact"
        )
        try:
            raw_rows = trace.run(group_name, lambda: _exec(combined_sql, combined_params), source=group_source)
            by_key: dict[str, list[dict[str, Any]]] = {}
            for row in raw_rows:
                tag = str(row.get("_q") or "")
                raw_json = row.get("_r")
                decoded = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                if isinstance(decoded, dict):
                    by_key.setdefault(tag, []).append(decoded)
        except Exception:
            out: list[tuple[str, dict[str, Any]]] = []
            for unit in group_units:
                out.extend(_run_unit(unit))
            return out
        out = []
        for unit in group_units:
            out.extend(unit.finish(by_key.get(unit.key, [])))
        return out

    candidates: list[tuple[InsightDefinition, Detection]] = []
    for definition in registry.get_all():
        detection = _detection_for(definition.id, enabled_fields)
        if detection is not None and all(column in enabled_fields for column in definition.required_fields):
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
    cards_by_id: dict[str, dict[str, Any]] = {}
    eligible: list[tuple[InsightDefinition, Detection]] = []
    for definition, detection in candidates:
        if detection.needs_baseline and history_hours < baseline_hours:
            cards_by_id[definition.id] = build_insufficient_baseline_card(
                definition.id,
                definition.title,
                definition.description,
                str(definition.category),
                baseline_hours,
                history_hours,
            )
        else:
            eligible.append((definition, detection))
    batches, standalone = plan_execution(eligible, enabled_set)

    def _group_name_for(unit_key: str) -> str:
        for name, members in _MULTIPLEX_GROUPS:
            if unit_key in members:
                return name
        return unit_key

    rollup_dependent_groups: set[str] = set()
    for batch, _ in batches:
        if batch.rollup_shape is not None:
            rollup_dependent_groups.add(_group_name_for(f"batch_{batch.name}"))
    for definition, _ in standalone:
        if definition.id in ROLLUP_DETECTIONS:
            rollup_dependent_groups.add(_group_name_for(definition.id))

    rollup_spans: dict[str, tuple[datetime, datetime]] = {}
    early_grouped: dict[str, list[_QueryUnit]] = {}
    deferred_batches: list[tuple[DetectionBatch, list[tuple[InsightDefinition, Detection]]]] = []
    deferred_standalone: list[tuple[InsightDefinition, Detection]] = []
    for batch, members in batches:
        gname = _group_name_for(f"batch_{batch.name}")
        if gname in rollup_dependent_groups:
            deferred_batches.append((batch, members))
        else:
            early_grouped.setdefault(gname, []).append(_prepare_batch(batch, members))
    for definition, detection in standalone:
        gname = _group_name_for(definition.id)
        if gname in rollup_dependent_groups:
            deferred_standalone.append((definition, detection))
        else:
            early_grouped.setdefault(gname, []).append(_prepare_standalone(definition, detection))

    with ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="hs-insights") as pool:
        futures = [
            pool.submit(contextvars.copy_context().run, partial(_run_group, g, u)) for g, u in early_grouped.items()
        ]
        coverage = fut_coverage.result() if fut_coverage is not None else (cached_coverage or [])
        rollup_spans = _rollup_spans(history[0] if history else {}, coverage, baseline_start, window_start)

        deferred_grouped: dict[str, list[_QueryUnit]] = {}
        for batch, members in deferred_batches:
            gname = _group_name_for(f"batch_{batch.name}")
            deferred_grouped.setdefault(gname, []).append(_prepare_batch(batch, members))
        for definition, detection in deferred_standalone:
            gname = _group_name_for(definition.id)
            deferred_grouped.setdefault(gname, []).append(_prepare_standalone(definition, detection))
        for g, u in deferred_grouped.items():
            if g.startswith("group_rollup_") and not all(unit.source == "mixed" for unit in u):
                for unit in u:
                    futures.append(pool.submit(contextvars.copy_context().run, partial(_run_unit, unit)))
            else:
                futures.append(pool.submit(contextvars.copy_context().run, partial(_run_group, g, u)))

        for future in futures:
            for insight_id, card in future.result():
                cards_by_id[insight_id] = card
    cards = [cards_by_id[definition.id] for definition, _ in candidates]
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
    from backend import config as svcconfig

    enabled_fields = frozenset(svcconfig.active_log_field_ids(svcconfig.load_config(service.service_id) or {}))
    cache_key = (
        service.service_id,
        id(service.client),
        window_hours,
        baseline_hours,
        key_clamp,
        int(mask_ips),
        filter_clause,
        tuple(sorted((k, str(v)) for k, v in filter_params.items())),
        tuple(sorted(enabled_fields)),
    )
    from backend.high_scale.ingest_controller import HighScaleService

    def evaluate(
        on_meta: Callable[[dict[str, Any]], None] | None = None,
        *,
        max_workers: int = _MAX_WORKERS,
        query_settings: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        return _evaluate(
            service,
            window_hours,
            baseline_hours,
            clamp_start,
            clamp_end,
            mask_ips,
            filter_clause,
            filter_params,
            enabled_fields,
            on_meta,
            max_workers=max_workers,
            query_settings=query_settings,
            bypass_history_cache=force_refresh,
        )

    def evaluate_bg(on_meta: Callable[[dict[str, Any]], None] | None = None) -> tuple[dict[str, Any], bool]:
        return evaluate(
            on_meta,
            max_workers=_BACKGROUND_MAX_WORKERS,
            query_settings={"max_threads": _BACKGROUND_QUERY_MAX_THREADS},
        )

    if not isinstance(service, HighScaleService):
        get_queries()
        payload, _ = evaluate_bg() if force_refresh else evaluate()
        return InsightsResponse.with_telemetry(**payload)
    if force_refresh:
        return _job_result(cache_key, _start_or_join(cache_key, evaluate_bg, interactive=False))

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
        _start_or_join(cache_key, evaluate_bg, interactive=False)
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
            "event_timestamp < {window_end:DateTime64(3)}",
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "url": req.url,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
    }
    use_cf = _use_materialized_cf(service.client)
    cache = "custom_fields['cache']"
    hit, miss, passed = (f"startsWith({cache}, '{kind}')" for kind in ("HIT", "MISS", "PASS"))
    is_w = "event_timestamp >= {window_start:DateTime64(3)}"
    is_b = "event_timestamp < {window_start:DateTime64(3)}"
    rates_sql = rewrite_materialized_cf(
        "/* insight:cache_collapse_rates */ SELECT "
        f"countIf({is_w} AND {hit}) AS w_hits, countIf({is_w} AND {miss}) AS w_miss, "
        f"countIf({is_w} AND {passed}) AS w_pass, countIf({is_w}) AS w_total, "
        f"countIf({is_b} AND {hit}) AS b_hits, countIf({is_b} AND {miss}) AS b_miss, "
        f"countIf({is_b} AND {passed}) AS b_pass, countIf({is_b}) AS b_total "
        f"FROM request_facts PREWHERE {scope}",
        params,
        use_cf=use_cf,
    )
    timeline_sql = rewrite_materialized_cf(
        "/* insight:cache_collapse_timeline */ SELECT "
        f"toStartOfInterval(event_timestamp, {_bucket_interval(window_hours + baseline_hours)}) AS bucket, "
        f"count() AS total_requests, countIf({hit}) AS real_hits, countIf({miss}) AS misses "
        f"FROM request_facts PREWHERE {scope} GROUP BY bucket ORDER BY bucket ASC",
        params,
        use_cf=use_cf,
    )
    miss_sql = rewrite_materialized_cf(
        "/* insight:cache_collapse_misses */ SELECT event_timestamp AS timestamp, "
        f"{cache} AS cache, custom_fields['pop'] AS pop, client_ip AS ip, toInt32OrNull(custom_fields['status']) AS status "
        f"FROM request_facts PREWHERE {scope} WHERE {miss} ORDER BY event_timestamp DESC LIMIT 50",
        params,
        use_cf=use_cf,
    )
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="hs-cache-collapse") as pool:
        fut_rates = pool.submit(contextvars.copy_context().run, service.client.execute, rates_sql, params)
        fut_timeline = pool.submit(contextvars.copy_context().run, service.client.execute, timeline_sql, params)
        fut_misses = pool.submit(contextvars.copy_context().run, service.client.execute, miss_sql, params)
        rates_row = fut_rates.result()[0]
        timeline_rows = fut_timeline.result()
        miss_rows = fut_misses.result()
    rates = tuple(
        int(rates_row[key] or 0)
        for key in ("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total")
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
