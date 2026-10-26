"""Offline measurement reporting. Unknown evidence is never a zero or a pass."""

from __future__ import annotations

import json
import math
import re
import statistics
import time
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from backend.models._base import ClickHouseQueryObservation
from backend.models.dashboard import HistoryObservation, ReaderCoverage
from pydantic import TypeAdapter, ValidationError

from backend.models.common import FilterSpec

# fmt: off
_FILTERS = TypeAdapter(dict[str, FilterSpec])
BUDGETS = json.loads(Path(__file__).with_name("performance_budgets.json").read_text())
CACHE_STATES = frozenset({"hit", "miss", "bypass", "unknown"})
SOURCES = frozenset({"rollup", "live", "projection", "fact", "projected_fact", "mixed", "unknown"})
COMPONENTS = ("aggregates", "top_bots", "health", "quality", "pop_health", "transport", "shielding_analysis")
QUEUE_SECTIONS = frozenset({"connection_acquire", "executor_wait", "admission_wait", "single_flight_wait"})
OVERHEAD_SECTIONS = frozenset({"get_schema_cols", "build_where_clause", "request_setup", "response_assembly"})
_CORRELATION_FIELDS = ("query_id", "request_id", "page_load_id", "service_id", "component", "section_id", "parent_section_id")
_CORRELATION_VALUE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_COVERAGE_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$")
_TS_KEYS = {"window_start", "window_end", "live_start", "live_end", "active_hour_start", "trailing_start", "earliest_event_at", "latest_event_at"}


def _duration(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value >= 0 else None


def _correlation_value(value: Any) -> bool:
    return isinstance(value, str) and bool(_CORRELATION_VALUE.fullmatch(value))


def _obs_current(reused: bool, state: str) -> bool | None:
    return False if reused else True if state in {"miss", "bypass"} else None


def _section(row: Mapping[str, Any], cache_state: str) -> dict[str, Any]:
    name = str(row["section"])
    duration = _duration(row.get("time_ms") if "time_ms" in row else row.get("elapsed_ms"))
    source = row.get("source")
    source_evidence = "explicit" if isinstance(source, str) and source in SOURCES and source != "unknown" else "unknown"
    if source_evidence == "unknown":
        source = "unknown"
        if "rollup" in name and not name.endswith("_cache_hit"):
            source, source_evidence = "rollup", "section_name"
    activity = row.get("activity")
    if not isinstance(activity, str) or activity not in {"compute", "queue", "overhead", "cache", "skipped", "unknown"}:
        activity = (
            "queue" if name in QUEUE_SECTIONS else "overhead" if name in OVERHEAD_SECTIONS
            else "cache" if name.endswith("_cache_hit")
            else "skipped" if ("temp_skipped" in name or name.endswith("_skipped")) else "unknown"
        )
    current = False if cache_state == "hit" else (row.get("current_execution") if isinstance(row.get("current_execution"), bool) else None)
    gaps = [
        *(["duration_unknown"] if duration is None else []),
        *(["source_unverified"] if source_evidence != "explicit" and activity not in {"queue", "overhead", "cache", "skipped"} else []),
        *(["execution_freshness_unknown"] if current is None else []),
    ]
    dispatch, tk = row.get("executor_dispatch"), row.get("timing_kind")
    return {
        "section": name, "ms": duration, "source": source, "source_evidence": source_evidence, "activity": activity,
        "executor_dispatch": dispatch if dispatch in ("inline", "thread_pool") else "unknown",
        "executor_wait_ms": _duration(row.get("executor_wait_ms")),
        "timing_kind": tk if tk in ("client_section_wall", "client_operation_wall") else "unknown",
        "current_execution": current,
        **{key: row[key] for key in _CORRELATION_FIELDS if _correlation_value(row.get(key))},
        "gaps": gaps,
    }


def _query_operations(raw: Any, reused: bool, state: str) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(raw, list):
        return [], []
    operations: list[dict[str, Any]] = []
    gaps: list[str] = []
    for row in raw[:256]:
        if not isinstance(row, dict) or not isinstance(row.get("clickhouse"), dict):
            continue
        try:
            parsed = ClickHouseQueryObservation.model_validate(row["clickhouse"], strict=True).model_dump(exclude_unset=True)
            if any(v is not None and not _correlation_value(v) for k, v in parsed.items() if k in _CORRELATION_FIELDS or k == "section"):
                raise ValueError("Invalid correlation field")
        except (ValidationError, ValueError):
            gaps.append("query_operation_invalid")
            continue
        current = _obs_current(reused or row.get("is_cached") is True, state)
        operations.append({"observation": parsed, "current_observation": current})
        gaps.append("server_cpu_unobserved")
        if parsed.get("server_elapsed_ms") is None or parsed.get("server_elapsed_source") != "statistics.elapsed":
            gaps.append("server_wall_time_unobserved")
        if current is not True:
            gaps.append("query_operation_historical_or_unknown")
        if any(not parsed.get(k) for k in ("request_id", "service_id", "section_id", "section")):
            gaps.append("query_correlation_incomplete")
        if parsed["outcome"] != "success":
            gaps.append("query_operation_failed")
        elif any(parsed.get(k) is None for k in ("admission_wait_ms", "network_ms", "decode_ms")):
            gaps.append("query_phase_unverified")
    if len(raw) > 256:
        gaps.append("query_operations_truncated")
    return operations, gaps


def _valid_evidence_timestamps(value: Any) -> bool:
    if isinstance(value, dict):
        return all((v is None or isinstance(v, str) and bool(_COVERAGE_TIMESTAMP.fullmatch(v))) if k in _TS_KEYS else _valid_evidence_timestamps(v) for k, v in value.items())
    return all(_valid_evidence_timestamps(v) for v in value) if isinstance(value, list) else True


def _history(raw: Any, reused: bool, state: str) -> tuple[dict[str, Any] | None, list[str]]:
    if raw is None:
        return None, ["history_evidence_missing"]
    try:
        parsed = HistoryObservation.model_validate(raw, strict=True).model_dump(exclude_unset=True)
        if not _valid_evidence_timestamps(parsed):
            raise ValueError("Invalid history timestamp")
    except (ValidationError, ValueError):
        return None, ["history_evidence_invalid"]
    current = _obs_current(reused, state)
    reaches = parsed.get("raw_extent_reaches_window_start")
    gaps = [
        "history_continuity_unverified",
        *(["history_historical_or_unknown"] if current is not True else []),
        *(["history_extent_unverified"] if parsed.get("extent_source") != "live_query" or not parsed.get("earliest_event_at") or not parsed.get("latest_event_at") else []),
        *(["history_raw_prefix_uncovered"] if reaches is False else ["history_raw_prefix_unverified"] if reaches is None else []),
        *(["history_window_rows_unverified"] if parsed.get("window_count_source") not in {"raw", "filtered_temp"} or parsed.get("window_rows") is None else []),
        *(["history_empty_window"] if parsed.get("window_rows") == 0 else []),
    ]
    return {"observation": parsed, "current_observation": current}, gaps


def _reader_coverage(raw: Any, reused: bool, state: str) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(raw, list) or not raw:
        return [], ["reader_coverage_missing"]
    observations, gaps = [], []
    for row in raw[:32]:
        try:
            parsed = ReaderCoverage.model_validate(row, strict=True).model_dump(exclude_unset=True)
            if not _valid_evidence_timestamps(parsed):
                raise ValueError("Invalid coverage timestamp")
        except (ValidationError, ValueError):
            gaps.append("reader_coverage_invalid")
            continue
        current = _obs_current(reused, state)
        observations.append({"observation": parsed, "current_observation": current})
        if current is not True:
            gaps.append("reader_coverage_historical_or_unknown")
        if parsed.get("live_top_up") in {"failed", "unavailable", "missing_column", "unverified"}:
            gaps.append(f"reader_live_top_up_{parsed['live_top_up']}")
        if parsed.get("outcome") in {None, "unknown", "read_failed"}:
            gaps.append("reader_outcome_unverified")
        if parsed.get("closed_hour_heal") in {"failed", "unavailable"}:
            gaps.append("reader_closed_hour_heal_incomplete")
        if any(a.get("reason") == "planned" and a.get("coverage_fraction") is not None and a["coverage_fraction"] < 1 for a in parsed.get("attempts", [])):
            gaps.append("reader_closed_hours_uncovered")
        scalar = parsed.get("scalar_plan") or {}
        if scalar.get("reason") == "boundary_changed":
            gaps.append("reader_coverage_boundary_changed")
        if (scalar.get("trailing") or {}).get("missing_hours"):
            gaps.append("reader_closed_hours_uncovered")
        top_n = parsed.get("top_n_plan") or {}
        if top_n:
            gaps.append("reader_field_complete_coverage_unverified")
            if top_n.get("rolled_read") == "failed":
                gaps.append("reader_rolled_read_failed")
            if top_n.get("outside_heal_horizon_hours"):
                gaps.append("reader_older_hours_not_heal_checked")
    if len(raw) > 32:
        gaps.append("reader_coverage_truncated")
    return observations, gaps


def describe_response(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"components": [], "gaps": ["response_not_object"]}
    components: list[dict[str, Any]] = []

    def visit(body: dict[str, Any], path: str, enclosing_hit: bool = False) -> None:
        meta = body.get("_response_cache") if isinstance(body.get("_response_cache"), dict) else {}
        state = meta.get("state")
        hit_marker = body.get("_is_cached") is True or body.get("is_cached") is True
        conflict = isinstance(state, str) and state in {"miss", "bypass"} and hit_marker
        if conflict or not isinstance(state, str) or state not in CACHE_STATES:
            state = "unknown" if conflict else ("hit" if hit_marker else "unknown")
        prewarmer = meta.get("prewarmer") if isinstance(meta.get("prewarmer"), bool) else None
        sf = meta.get("single_flight")
        single_flight = sf if isinstance(sf, str) and sf in {"leader", "follower"} else None
        shared = single_flight == "follower"
        cached_execution = enclosing_hit or hit_marker or state == "hit" or shared
        raw = body.get("_section_timings", body.get("section_timings"))
        sections = [
            _section(row, "hit" if cached_execution else state)
            for row in (raw if isinstance(raw, list) else [])
            if isinstance(row, dict) and isinstance(row.get("section"), str)
        ]
        wrapper = any(isinstance(body.get(name), (dict, list)) for name in COMPONENTS)
        raw_readers = body.get("_reader_coverage", body.get("reader_coverage"))
        readers, reader_gaps = ([], []) if raw_readers is None and wrapper else _reader_coverage(raw_readers, cached_execution, state)
        raw_history = body.get("_history", body.get("history"))
        history, history_gaps = (None, []) if raw_history is None and wrapper else _history(raw_history, cached_execution, state)
        queries, query_gaps = _query_operations(body.get("_debug_queries", body.get("debug_queries")), cached_execution, state)
        gaps = [
            *(["response_cache_conflict"] if conflict else []), *reader_gaps, *history_gaps, *query_gaps,
            *(["response_cache_unknown"] if state == "unknown" else []),
            *(["shared_execution"] if shared else []), *(["prewarmer_unknown"] if prewarmer is None else []),
            *(["section_timings_missing"] if not sections else []),
            *(["historical_or_unverified_timings"] if cached_execution and sections else []),
        ]
        components.append({
            "component": path, "cache_state": state, "prewarmer": prewarmer, "ttl_seconds": _duration(meta.get("ttl_seconds")),
            "single_flight": single_flight, "wait_ms": _duration(meta.get("wait_ms")), "execution_reused": cached_execution,
            "lookup_current": not enclosing_hit and not shared, "sections": sections, "reader_coverage": readers,
            "history": history, "query_operations": queries, "gaps": gaps,
        })
        for name in COMPONENTS:
            child = body.get(name)
            if isinstance(child, (dict, list)):
                inherit = enclosing_hit or (cached_execution and meta.get("scope") != "self")
                sidecars = body.get("_component_observations")
                sidecar = sidecars.get(name) if isinstance(sidecars, dict) else None
                envelope = child if isinstance(child, dict) else sidecar
                visit(envelope if isinstance(envelope, dict) else {}, name if path == "response" else f"{path}.{name}", inherit)
                if isinstance(child, list) and not isinstance(sidecar, dict):
                    components[-1]["gaps"].append("component_observation_envelope_missing")

    visit(payload, "response")
    gaps = {gap for c in components for gap in c["gaps"]} | {gap for c in components for s in c["sections"] for gap in s["gaps"]}
    query_sections = {
        q["observation"].get("section_id")
        for c in components for q in c["query_operations"]
        if q["current_observation"] is True and q["observation"].get("section_id")
    }
    for c in components:
        for s in c["sections"]:
            if s["source"] in {"fact", "projection", "projected_fact", "mixed"} and s["current_execution"] is True and s.get("section_id") not in query_sections:
                gaps.add("section_query_correlation_missing")
    return {"components": components, "gaps": sorted(gaps)}


def distribution(values: Iterable[float]) -> dict[str, Any]:
    samples = list(values)
    if any(_duration(v) is None for v in samples):
        raise ValueError("Samples must be finite nonnegative numbers")
    samples.sort()
    count = len(samples)
    pct = lambda f: samples[max(0, math.ceil(f * count) - 1)] if count else None  # noqa: E731
    return {
        "count": count, "method": "nearest_rank", "min": samples[0] if count else None,
        "mean": statistics.fmean(samples) if count else None, "p50": pct(0.50), "p90": pct(0.90), "p95": pct(0.95), "p99": pct(0.99),
        "max": samples[-1] if count else None, "stddev": statistics.pstdev(samples) if count else None,
        "p95_sample_floor_met": count >= BUDGETS["minimum_samples_per_state"], "p99_sample_floor_met": count >= 100,
        "qualification": "descriptive_only",
    }


def active_filter_count(filters: Mapping[str, Any]) -> int:
    try:
        parsed = _FILTERS.validate_python(filters)
    except ValidationError:
        raise ValueError("Invalid filter shape") from None
    if any(not k.strip() or not s.values or any(v is None or (isinstance(v, str) and not v.strip()) for v in s.values) for k, s in parsed.items()):
        raise ValueError("Each filter must have a named dimension and active values")
    return len(parsed)


def visible_results_budget_ms(window: str, filters: Mapping[str, Any]) -> int:
    windows = BUDGETS["visible_results_p95_ms"]
    if window not in windows:
        raise ValueError(f"Unsupported window: {window}")
    return int(windows[window]["filtered" if active_filter_count(filters) else "unfiltered"])


def rank_sections(samples: Iterable[dict[str, Any]], budget_ms: float = 500) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    seen: set[tuple[str, str]] = set()
    for sample in samples:
        for component in sample.get("response", {}).get("components", []):
            if component["cache_state"] == "hit":
                continue
            for section in component["sections"]:
                duration = section["ms"]
                sample_id = sample.get("sample_id")
                if duration is None or section["current_execution"] is False or not sample_id:
                    continue
                dims = {
                    **{k: sample.get(k) for k in ("environment", "role", "endpoint", "window", "payload")},
                    "component": component["component"], "cache_state": component["cache_state"],
                    "section": section["section"], "source": section["source"], "activity": section["activity"],
                }
                key = json.dumps(dims, sort_keys=True, separators=(",", ":"))
                if (sample_id, key) in seen:
                    continue
                seen.add((sample_id, key))
                group = groups.setdefault(key, {**dims, "values": [], "gaps": set(), "over_budget_count": 0})
                group["values"].append(duration)
                group["gaps"].update(component["gaps"] + section["gaps"])
                group["over_budget_count"] += duration >= budget_ms
    ranked = []
    for group in groups.values():
        group["distribution_ms"] = distribution(group.pop("values"))
        group["gaps"] = sorted(group["gaps"])
        group["repeatable"] = group["over_budget_count"] >= 2
        if group["over_budget_count"]:
            ranked.append(group)
    return sorted(ranked, key=lambda row: (row["repeatable"], row["distribution_ms"]["p95"]), reverse=True)


def sample_cache_pair(
    request: Callable[[dict, float], Any], payload: dict, *, max_seconds: float = 120,
    max_samples: int = 4, monotonic: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if not math.isfinite(max_seconds) or not 0 < max_seconds <= 600:
        raise ValueError("max_seconds must be finite and between 0 and 600")
    if isinstance(max_samples, bool) or not 2 <= max_samples <= 10:
        raise ValueError("max_samples must be between 2 and 10")
    started, deadline = monotonic(), monotonic() + max_seconds
    expiry_ready: dict[str, float] = {}
    samples: list[dict] = []
    components: dict[str, dict] = {}
    latest: dict[str, dict] = {}
    gaps: set[str] = set()
    for index in range(1, max_samples + 1):
        remaining = deadline - monotonic()
        if remaining <= 0:
            gaps.add("observation_deadline_exceeded")
            break
        request_started = monotonic()
        try:
            observation = describe_response(request(deepcopy(payload), remaining))
        except Exception as exc:  # noqa: BLE001 — exception text may contain credentials
            samples.append({"sample": index, "error": type(exc).__name__})
            gaps.add("request_failed")
            break
        samples.append({"sample": index, "started_seconds": request_started - started, "response": observation})
        if monotonic() > deadline:
            gaps.add("observation_deadline_exceeded")
            break
        rows = observation["components"]
        if not rows or (components and {r["component"] for r in rows} != set(components)):
            gaps.add("response_components_missing" if not rows else "component_set_changed")
            break
        latest = {row["component"]: row for row in rows}
        for name, row in latest.items():
            entry = components.setdefault(name, {"status": "incomplete"})
            if entry["status"] == "observed" or not row["lookup_current"]:
                continue
            state, ttl = row["cache_state"], row["ttl_seconds"]
            expired = name in expiry_ready and request_started >= expiry_ready[name]
            if name not in expiry_ready and ttl is not None and ttl > 0:
                expiry_ready[name] = monotonic() + ttl
            if state == "bypass":
                entry["status"] = "bypass"
            elif state == "miss" and expired and not row["execution_reused"]:
                entry.update(status="incomplete", miss_sample=index)
            elif state == "hit" and "miss_sample" in entry:
                entry.update(status="observed", hit_sample=index)
            elif entry["status"] == "bypass":
                entry["status"] = "incomplete"
        if all(row["status"] in {"observed", "bypass"} for row in components.values()):
            break
        pending = [name for name, row in components.items() if row["status"] == "incomplete"]
        if any("miss_sample" in components[n] and latest[n]["cache_state"] == "miss" for n in pending):
            continue
        ttls = [latest[n]["ttl_seconds"] for n in pending]
        if any(ttl is None or ttl <= 0 for ttl in ttls):
            gaps.add("cache_ttl_unknown")
        known_ttls = [ttl for ttl in ttls if ttl is not None and ttl > 0]
        if not known_ttls:
            break
        wait = max(known_ttls) + 0.01
        if wait >= deadline - monotonic():
            gaps.add("expiry_wait_exceeds_deadline")
            break
        if index < max_samples:
            sleep(wait)
    incomplete = [name for name, row in components.items() if row["status"] == "incomplete"]
    if incomplete:
        gaps.add("miss_hit_pair_unobserved")
    complete = bool(components) and not incomplete and not gaps
    applicable = any(row["status"] == "observed" for row in components.values())
    return {
        "status": "observed" if complete and applicable else "not_applicable" if complete else "incomplete",
        "components": components, "samples": samples, "gaps": sorted(gaps), "database_cache_state": "unknown",
    }
