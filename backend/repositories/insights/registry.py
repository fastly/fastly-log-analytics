from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class InsightCategory(StrEnum):
    """Thematic section an insight belongs to on the Anomaly Detection page.

    The lowercase machine key is the ONLY thing on the API contract; the
    human-facing section label, icon, and ordering live in the frontend
    (``frontend/lib/insight-sections.ts``) so a section can be renamed
    without touching the API or the insight definitions.

    ``network`` (Phase 3) is the dedicated Network Path section: ISP/ASN
    reachability, RTT/packet-loss, and metro/region delivery quality
    (asn_concentration, asn_metro_performance, region_latency, and the
    Phase-3 network_asn_health insight). See the design plan §2/§3.
    """

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
    # Required, no default — a registration that forgets its category fails
    # Pydantic validation at import time. That import-time failure is the
    # entire point: it forces every new insight to declare which section it
    # belongs to instead of silently landing in a wrong/fallback bucket.
    category: InsightCategory
    description: str = ""
    sql_template: str
    visual_type: str = "table"  # "table", "chart", "map"
    required_fields: list[str] = Field(default_factory=list)

    # Optional callback to process a raw row from DuckDB into a standardized item dict
    # signature: (row: tuple, definition: InsightDefinition, context: dict) -> dict
    row_processor: Callable[[Any, InsightDefinition, dict[str, Any]], dict] | None = None

    # Optional logic to determine overall severity for the insight based on its items
    # signature: (items: list[dict]) -> str
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
registry = InsightsRegistry()
