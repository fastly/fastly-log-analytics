"""Pydantic models for saved views."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SavedView(BaseModel):
    id: str | None = None
    service_id: str
    name: str
    filters_json: str
    time_range_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    page: str | None = None
    created_at: str | None = None


class SavedViewRecord(BaseModel):
    """Wire-safe read shape for ``GET /api/views/{service_id}`` rows.

    Distinct from the strict ``SavedView`` request body: all-Optional +
    ``extra="allow"`` so a partial row (or a future column) can never 500
    the read path; ``response_model_exclude_unset`` keeps the emitted key
    set byte-identical to the producer's."""

    model_config = ConfigDict(extra="allow")

    id: str | None = None
    service_id: str | None = None
    name: str | None = None
    filters_json: str | None = None
    time_range_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    page: str | None = None
    created_at: str | None = None


class ViewSaveResponse(BaseModel):
    """Ack from ``POST /api/views/`` — id of the created row."""

    id: str
    status: str
