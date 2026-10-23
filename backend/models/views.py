"""Pydantic models for saved views."""

from __future__ import annotations

from pydantic import BaseModel

from backend.models._base import WireSafeModel


class SavedView(WireSafeModel):
    id: str | None = None
    service_id: str
    name: str
    filters_json: str
    time_range_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    page: str | None = None
    created_at: str | None = None


class SavedViewRecord(SavedView):
    """Wire-safe read shape for ``GET /api/views/{service_id}`` rows.

    Distinct from the strict ``SavedView`` request body: ``id`` is guaranteed
    non-null on persisted rows; ``extra="allow"`` + ``response_model_exclude_unset``
    is inherited from ``SavedView`` (``WireSafeModel``)."""

    id: str


class ViewSaveResponse(BaseModel):
    """Ack from ``POST /api/views/`` — id of the created row."""

    id: str
    status: str
