"""Shared selected-period storage billing interval model and calculator."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel

GIB_BYTES = 1024**3
HOUR_MS = 3_600_000

StorageBillingKind = Literal["live", "deleted", "minimum_billed"]


class StorageBillingInterval(BaseModel):
    """One billable object interval in epoch milliseconds."""

    id: str
    dataset: str
    kind: StorageBillingKind
    bytes: int
    created_at: int | None = None
    deleted_at: int | None = None
    billed_from: int
    billed_until: int
    complete: bool
    provenance: str


class StorageBillingSummary(BaseModel):
    """Aggregated GiB-hours across a selected [start_ms, end_ms) period."""

    live_gb_hours: float
    deleted_gb_hours: float
    minimum_billed_gb_hours: float
    total_gb_hours: float
    complete: bool
    provenance: list[str]


def _coerce_interval(item: StorageBillingInterval | Mapping[str, Any]) -> StorageBillingInterval:
    if isinstance(item, StorageBillingInterval):
        return item
    return StorageBillingInterval.model_validate(item)


def calculate_storage_billing(
    intervals: Sequence[StorageBillingInterval | Mapping[str, Any]],
    start_ms: int,
    end_ms: int,
) -> StorageBillingSummary:
    """Integrate billable byte-time over ``[start_ms, end_ms)`` in GiB-hours."""
    if end_ms < start_ms:
        raise ValueError(f"end_ms ({end_ms}) must be >= start_ms ({start_ms})")
    if end_ms == start_ms:
        return StorageBillingSummary(
            live_gb_hours=0.0,
            deleted_gb_hours=0.0,
            minimum_billed_gb_hours=0.0,
            total_gb_hours=0.0,
            complete=True,
            provenance=[],
        )

    seen_ids: set[str] = set()
    live_gb_hours = 0.0
    deleted_gb_hours = 0.0
    minimum_billed_gb_hours = 0.0
    complete = True
    provenance: set[str] = set()

    for raw in intervals:
        interval = _coerce_interval(raw)
        if interval.id in seen_ids:
            continue
        seen_ids.add(interval.id)

        if interval.billed_until <= interval.billed_from:
            continue

        overlap_start = max(interval.billed_from, start_ms)
        overlap_end = min(interval.billed_until, end_ms)
        if overlap_end <= overlap_start:
            continue

        clamped_bytes = max(0, int(interval.bytes))
        gb_hours = (clamped_bytes / GIB_BYTES) * ((overlap_end - overlap_start) / HOUR_MS)

        if interval.kind == "live":
            live_gb_hours += gb_hours
        elif interval.kind == "deleted":
            deleted_gb_hours += gb_hours
        else:
            minimum_billed_gb_hours += gb_hours

        if not interval.complete:
            complete = False
        if interval.provenance:
            provenance.add(interval.provenance)

    total_gb_hours = live_gb_hours + deleted_gb_hours + minimum_billed_gb_hours
    return StorageBillingSummary(
        live_gb_hours=live_gb_hours,
        deleted_gb_hours=deleted_gb_hours,
        minimum_billed_gb_hours=minimum_billed_gb_hours,
        total_gb_hours=total_gb_hours,
        complete=complete,
        provenance=sorted(provenance),
    )
