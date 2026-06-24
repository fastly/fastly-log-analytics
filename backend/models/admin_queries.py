"""Pydantic models for the admin Live Query Monitor.

Carved out of ``backend.routers.admin_queries`` so the schemas live with
the other ``backend.models.*`` modules and OpenAPI generation has one
canonical location to find them.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class SnapshotResponse(BaseModel):
    """Incremental snapshot of the in-process query registry.

    ``last_seq`` is the high-water mark the client should send back as
    ``since_seq`` on its next poll so the registry can stream only new rows.
    """

    last_seq: int
    active: list[dict]
    completed: list[dict]


class SummaryResponse(BaseModel):
    """Cheap counts that power the live-monitor tab badge."""

    active_total: int
    by_db_type: dict[str, int]
    longest_ms: float


class CancelResponse(BaseModel):
    """Outcome of ``POST /api/admin/queries/{qid}/cancel``."""

    state: Literal["cancelled", "not_found", "already_finished", "connection_gone"]
    query_id: int
