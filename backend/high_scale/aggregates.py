"""Reference incremental aggregates used to define high-scale semantics."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

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


class AggregateStore:
    """Idempotent aggregate reference store for local tests and replay logic."""

    def __init__(self, *, top_n_limit: int = 1000) -> None:
        if top_n_limit <= 0:
            raise ValueError("top_n_limit must be positive")
        self._top_n_limit = top_n_limit
        self._batches: set[str] = set()
        self._counts: dict[tuple[str, str], int] = defaultdict(int)
        self._status_counts: dict[str, Counter[str]] = defaultdict(Counter)
        self._top_values: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        self._watermarks: dict[tuple[str, str], ServingWatermark] = {}

    def apply(self, batch: EventBatch) -> AggregateReceipt:
        if batch.domain not in {"request", "rum_vitals", "rum_errors", "cmcd"}:
            raise ValueError("unsupported aggregate domain")
        if batch.batch_id in self._batches:
            return AggregateReceipt(batch.batch_id, False, 0, True)
        if batch.owner_epoch < 0:
            raise ValueError("owner epoch must be non-negative")
        self._batches.add(batch.batch_id)
        key = (batch.service_id, batch.domain)
        self._counts[key] += len(batch.events)
        for event in batch.events:
            if batch.domain == "request":
                status = str(event.get("status_code", "unknown"))
                self._status_counts[batch.service_id][status] += 1
                self._top_values[(batch.service_id, "url")][str(event.get("url", ""))] += 1
            elif batch.domain == "rum_vitals":
                self._top_values[(batch.service_id, "metric_name")][str(event.get("metric_name", ""))] += 1
            elif batch.domain == "rum_errors":
                self._top_values[(batch.service_id, "error_message")][str(event.get("error_message", ""))] += 1
            elif batch.domain == "cmcd":
                self._top_values[(batch.service_id, "cmcd_session")][str(event.get("sid", ""))] += 1
        event_id = str(batch.events[-1].get("event_id")) if batch.events else None
        self._watermarks[key] = ServingWatermark(
            service_id=batch.service_id,
            domain=batch.domain,
            owner_epoch=batch.owner_epoch,
            coverage_start=batch.coverage_start,
            coverage_end=batch.coverage_end,
            last_accepted_cursor=batch.batch_id,
            last_archived_event_id=event_id,
            last_visible_event_id=event_id,
            exact=True,
        )
        return AggregateReceipt(batch.batch_id, True, len(batch.events), False)

    def request_count(self, service_id: str) -> int:
        return self._counts[(service_id, "request")]

    def status_counts(self, service_id: str) -> dict[str, int]:
        return dict(self._status_counts[service_id])

    def response(self, service_id: str, domain: str, *, now: datetime | None = None) -> AggregateResponse:
        if (service_id, domain) not in self._watermarks:
            watermark = ServingWatermark(service_id, domain, 0, None, None, None, None, None, True)
            request_count = 0
        else:
            watermark = self._watermarks[(service_id, domain)]
            request_count = self._counts[(service_id, domain)]
        watermark.validate()
        values = self._top_values[(service_id, _dimension_for(domain))].most_common(self._top_n_limit)
        observed = now or datetime.now(UTC)
        freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
        return AggregateResponse(
            service_id,
            domain,
            request_count,
            tuple(values),
            1.0,
            watermark,
            freshness,
            True,
            None,
        )


def _dimension_for(domain: str) -> str:
    return {
        "request": "url",
        "rum_vitals": "metric_name",
        "rum_errors": "error_message",
        "cmcd": "cmcd_session",
    }.get(domain, "")
