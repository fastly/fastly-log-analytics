"""Reference incremental aggregates used to define high-scale semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from backend.high_scale._projection_utils import range_value
from backend.high_scale.archive_models import ServingWatermark


@dataclass(frozen=True)
class EventBatch:
    batch_id: str
    service_id: str
    domain: str
    events: tuple[dict[str, Any], ...]
    owner_epoch: int
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None


@dataclass(frozen=True)
class AggregateReceipt:
    batch_id: str
    applied: bool
    rows_applied: int
    duplicate: bool


@dataclass(frozen=True)
class AggregateResponse:
    service_id: str
    domain: str
    request_count: int
    top_values: tuple[tuple[str, int], ...]
    coverage: float
    watermark: ServingWatermark
    freshness_lag_seconds: float
    exact: bool
    approximation_error: float | None
    counters: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class AggregateRequest:
    service_id: str
    domain: str
    start: datetime | None = None
    end: datetime | None = None
    dimension: str | None = None

    def validate(self) -> None:
        if not self.service_id.strip():
            raise ValueError("service id is required")
        if self.domain not in {"request", "rum_vitals", "rum_errors", "cmcd"}:
            raise ValueError("unsupported aggregate domain")
        if (self.start is None) != (self.end is None):
            raise ValueError("aggregate range must include both start and end")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("aggregate range is inverted")


def _dimension_for(domain: str) -> str:
    return {
        "request": "url",
        "rum_vitals": "metric_name",
        "rum_errors": "error_message",
        "cmcd": "cmcd_session",
    }.get(domain, "")


def _event_timestamp(event: dict[str, Any]) -> datetime | None:
    value = event.get("timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return range_value(value).astimezone(UTC)
    return None
