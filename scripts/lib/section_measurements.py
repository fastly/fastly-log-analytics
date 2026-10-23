"""Offline measurement reporting. Unknown evidence is never a zero or a pass."""

from __future__ import annotations

import json
import math
import re
import statistics
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from backend.models.common import FilterSpec
from backend.models.reader_coverage import ReaderCoverage

_FILTERS = TypeAdapter(dict[str, FilterSpec])
BUDGETS = json.loads(Path(__file__).with_name("performance_budgets.json").read_text())
CACHE_STATES = frozenset({"hit", "miss", "bypass", "unknown"})
SOURCES = frozenset({"rollup", "live", "projection", "fact", "projected_fact", "unknown"})
COMPONENTS = ("aggregates", "top_bots", "health", "quality", "pop_health", "transport", "shielding_analysis")
QUEUE_SECTIONS = frozenset({"connection_acquire", "executor_wait", "admission_wait", "single_flight_wait"})
OVERHEAD_SECTIONS = frozenset({"get_schema_cols", "build_where_clause", "request_setup", "response_assembly"})


def _duration(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value >= 0 else None


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
        if name in QUEUE_SECTIONS:
            activity = "queue"
        elif name in OVERHEAD_SECTIONS:
            activity = "overhead"
        elif name.endswith("_cache_hit"):
            activity = "cache"
        elif "temp_skipped" in name or name.endswith("_skipped"):
            activity = "skipped"
        else:
            activity = "unknown"
    current = row.get("current_execution")
    if not isinstance(current, bool):
        current = None
    if cache_state == "hit":
        current = False
    gaps = []
    if duration is None:
        gaps.append("duration_unknown")
    if source_evidence != "explicit" and activity not in {"queue", "overhead", "cache", "skipped"}:
        gaps.append("source_unverified")
    if current is None:
        gaps.append("execution_freshness_unknown")
    return {
        "section": name,
        "ms": duration,
        "source": source,
        "source_evidence": source_evidence,
        "activity": activity,
        "current_execution": current,
        "gaps": gaps,
    }


_COVERAGE_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$")


def _reader_coverage(raw: Any, reused: bool, state: str) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(raw, list) or not raw:
        return [], ["reader_coverage_missing", "history_evidence_missing"]
    observations = []
    gaps = ["history_evidence_missing"]
    for row in raw[:2]:
        try:
            parsed = ReaderCoverage.model_validate(row, strict=True).model_dump(exclude_unset=True)
            timestamps = [parsed.get(key) for key in ("window_start", "window_end", "live_start", "live_end")]
            timestamps.extend(attempt.get("active_hour_start") for attempt in parsed.get("attempts", []))
            if any(value is not None and not _COVERAGE_TIMESTAMP.fullmatch(value) for value in timestamps):
                raise ValueError("Invalid coverage timestamp")
        except (ValidationError, ValueError):
            gaps.append("reader_coverage_invalid")
            continue
        current = False if reused else True if state in {"miss", "bypass"} else None
        observations.append({"observation": parsed, "current_observation": current})
        if current is not True:
            gaps.append("reader_coverage_historical_or_unknown")
        if parsed.get("live_top_up") in {"failed", "unavailable", "missing_column"}:
            gaps.append(f"reader_live_top_up_{parsed['live_top_up']}")
        if parsed.get("outcome") in {None, "unknown", "read_failed"}:
            gaps.append("reader_outcome_unverified")
    if len(raw) > 2:
        gaps.append("reader_coverage_truncated")
    return observations, gaps


def describe_response(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"components": [], "gaps": ["response_not_object"]}
    components: list[dict[str, Any]] = []

    def visit(body: dict[str, Any], path: str, enclosing_hit: bool = False) -> None:
        metadata = body.get("_response_cache")
        metadata = metadata if isinstance(metadata, dict) else {}
        state = metadata.get("state")
        hit_marker = body.get("_is_cached") is True or body.get("is_cached") is True
        conflict = isinstance(state, str) and state in {"miss", "bypass"} and hit_marker
        if not isinstance(state, str) or state not in CACHE_STATES:
            state = "hit" if hit_marker else "unknown"
        if conflict:
            state = "unknown"
        prewarmer = metadata.get("prewarmer")
        if not isinstance(prewarmer, bool):
            prewarmer = None
        single_flight = metadata.get("single_flight")
        if not isinstance(single_flight, str) or single_flight not in {"leader", "follower"}:
            single_flight = None
        shared = single_flight == "follower"
        cached_execution = enclosing_hit or hit_marker or state == "hit" or shared
        raw = body.get("_section_timings", body.get("section_timings"))
        sections = [
            _section(row, "hit" if cached_execution else state)
            for row in (raw if isinstance(raw, list) else [])
            if isinstance(row, dict) and isinstance(row.get("section"), str)
        ]
        raw_readers = body.get("_reader_coverage", body.get("reader_coverage"))
        readers: list[dict[str, Any]]
        if raw_readers is None and any(isinstance(body.get(name), dict) for name in COMPONENTS):
            readers, reader_gaps = [], ["history_evidence_missing"]
        else:
            readers, reader_gaps = _reader_coverage(raw_readers, cached_execution, state)
        gaps = ["response_cache_conflict"] if conflict else []
        gaps.extend(reader_gaps)
        if state == "unknown":
            gaps.append("response_cache_unknown")
        if shared:
            gaps.append("shared_execution")
        if prewarmer is None:
            gaps.append("prewarmer_unknown")
        if not sections:
            gaps.append("section_timings_missing")
        if cached_execution and sections:
            gaps.append("historical_or_unverified_timings")
        components.append(
            {
                "component": path,
                "cache_state": state,
                "prewarmer": prewarmer,
                "ttl_seconds": _duration(metadata.get("ttl_seconds")),
                "single_flight": single_flight,
                "wait_ms": _duration(metadata.get("wait_ms")),
                "execution_reused": cached_execution,
                "lookup_current": not enclosing_hit and not shared,
                "sections": sections,
                "reader_coverage": readers,
                "gaps": gaps,
            }
        )
        for name in COMPONENTS:
            child = body.get(name)
            if isinstance(child, dict):
                inherit = enclosing_hit or (cached_execution and metadata.get("scope") != "self")
                visit(child, name if path == "response" else f"{path}.{name}", inherit)

    visit(payload, "response")
    gaps = {gap for component in components for gap in component["gaps"]}
    gaps.update(gap for component in components for section in component["sections"] for gap in section["gaps"])
    return {"components": components, "gaps": sorted(gaps)}


def distribution(values: Iterable[float]) -> dict[str, Any]:
    samples = list(values)
    if any(_duration(value) is None for value in samples):
        raise ValueError("Samples must be finite nonnegative numbers")
    samples.sort()
    count = len(samples)

    def percentile(fraction: float) -> float | None:
        return samples[max(0, math.ceil(fraction * count) - 1)] if count else None

    return {
        "count": count,
        "method": "nearest_rank",
        "min": samples[0] if count else None,
        "mean": statistics.fmean(samples) if count else None,
        "p50": percentile(0.50),
        "p90": percentile(0.90),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": samples[-1] if count else None,
        "stddev": statistics.pstdev(samples) if count else None,
        "p95_sample_floor_met": count >= BUDGETS["minimum_samples_per_state"],
        "p99_sample_floor_met": count >= 100,
        "qualification": "descriptive_only",
    }


def active_filter_count(filters: Mapping[str, Any]) -> int:
    try:
        parsed = _FILTERS.validate_python(filters)
    except ValidationError:
        raise ValueError("Invalid filter shape") from None
    if any(
        not key.strip()
        or not spec.values
        or any(value is None or (isinstance(value, str) and not value.strip()) for value in spec.values)
        for key, spec in parsed.items()
    ):
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
                if duration is None or section["current_execution"] is False:
                    continue
                sample_id = sample.get("sample_id")
                if not sample_id:
                    continue
                dimensions = {key: sample.get(key) for key in ("environment", "role", "endpoint", "window", "payload")}
                dimensions.update(
                    component=component["component"],
                    cache_state=component["cache_state"],
                    section=section["section"],
                    source=section["source"],
                    activity=section["activity"],
                )
                key = json.dumps(dimensions, sort_keys=True, separators=(",", ":"))
                identity = (sample_id, key)
                if identity in seen:
                    continue
                seen.add(identity)
                group = groups.setdefault(key, {**dimensions, "values": [], "gaps": set(), "over_budget_count": 0})
                group["values"].append(duration)
                group["gaps"].update(component["gaps"] + section["gaps"])
                group["over_budget_count"] += duration >= budget_ms
    ranked = []
    for group in groups.values():
        stats = distribution(group.pop("values"))
        group["distribution_ms"] = stats
        group["gaps"] = sorted(group["gaps"])
        group["repeatable"] = group["over_budget_count"] >= 2
        if group["over_budget_count"]:
            ranked.append(group)
    return sorted(ranked, key=lambda row: (row["repeatable"], row["distribution_ms"]["p95"]), reverse=True)
