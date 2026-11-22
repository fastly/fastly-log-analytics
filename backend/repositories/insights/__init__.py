from __future__ import annotations

import threading
from collections.abc import Callable
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache


class InsightCategory(StrEnum):
    """Thematic section an insight belongs to on the Anomaly Detection page."""

    security = "security"
    origin = "origin"
    edge = "edge"
    traffic = "traffic"
    network = "network"


class InsightDefinition(BaseModel):
    """Definition for an insight anomaly check."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    title: str
    category: InsightCategory
    description: str = ""
    sql_template: str
    visual_type: str = "table"  # "table", "chart", "map"
    required_fields: list[str] = Field(default_factory=list)

    row_processor: Callable[[Any, InsightDefinition, dict[str, Any]], dict] | None = None
    severity_logic: Callable[[list[dict]], str] | None = None


class InsightsRegistry:
    """Registry for all insight definitions."""

    def __init__(self) -> None:
        self._definitions: dict[str, InsightDefinition] = {}

    def register(self, definition: InsightDefinition) -> None:
        """Register a new insight definition."""
        self._definitions[definition.id] = definition

    def get_all(self) -> list[InsightDefinition]:
        """Return all registered insight definitions."""
        return list(self._definitions.values())

    def get(self, insight_id: str) -> InsightDefinition | None:
        """Get an insight definition by ID."""
        return self._definitions.get(insight_id)


# Singleton registry instance
registry: InsightsRegistry = InsightsRegistry()

# Side-effect import: registers all insight definitions with the singleton registry
from . import definitions as _definitions  # noqa: E402, F401
from .shaping import (  # noqa: E402
    build_error_insight_card,
    build_insight_card,
    build_insights_cache_key,
    build_insufficient_baseline_card,
    clamp_insight_window,
    compute_available_history_hours,
    compute_cache_collapse_rates,
    compute_clamped_insight_windows,
    evaluate_insight_severity,
    fingerprint_column,
    format_insight_summary,
    make_investigate_url_builder,
    process_insight_rows,
    resolve_clamp_cache_key,
    shape_cache_collapse_detail,
    shape_cache_collapse_recent_misses,
    shape_insight_card_from_rows,
)

INSIGHTS_CACHE_TTL = 300  # seconds
_insights_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=500, ttl_seconds=INSIGHTS_CACHE_TTL)
_insights_cache_lock = threading.Lock()


def check_insights_cache(
    src: dict,
    window_hours: float = 24,
    baseline_hours: float = 168,
    clamp_start: str | None = None,
    clamp_end: str | None = None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
    filters: dict | None = None,
) -> dict | None:
    if INSIGHTS_CACHE_TTL <= 0 or force_refresh:
        return None
    cache_key = build_insights_cache_key(
        src["name"],
        window_hours=window_hours,
        baseline_hours=baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        filters=filters,
    )
    with _insights_cache_lock:
        entry = _insights_cache.get(cache_key)
    if entry is not None:
        cached = entry[1].copy()
        cached["is_cached"] = True
        return cached
    return None


__all__ = [
    "INSIGHTS_CACHE_TTL",
    "InsightCategory",
    "InsightDefinition",
    "InsightsRegistry",
    "_insights_cache",
    "build_error_insight_card",
    "build_insight_card",
    "build_insights_cache_key",
    "build_insufficient_baseline_card",
    "check_insights_cache",
    "clamp_insight_window",
    "compute_available_history_hours",
    "compute_cache_collapse_rates",
    "compute_clamped_insight_windows",
    "evaluate_insight_severity",
    "fingerprint_column",
    "format_insight_summary",
    "make_investigate_url_builder",
    "process_insight_rows",
    "registry",
    "resolve_clamp_cache_key",
    "shape_cache_collapse_detail",
    "shape_cache_collapse_recent_misses",
    "shape_insight_card_from_rows",
]
