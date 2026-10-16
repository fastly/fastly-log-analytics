"""Small read-response models for routes that previously returned bare dicts."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from backend.models._base import JsonRows, WireSafeModel


class DebugStateResponse(WireSafeModel):
    """``GET /api/debug/state``."""

    debug_responses_enabled: bool


class SchemaResponse(WireSafeModel):
    """``GET /api/schema``: cached or live column list plus the safe table name."""

    schema_: list[dict[str, Any]] = Field(alias="schema")
    table_name: str


class AuditLogsResponse(WireSafeModel):
    """``GET /api/audit-logs``. ``_debug_calls`` rides along as an extra."""

    total: int
    page: int
    per_page: int
    entries: JsonRows


class CmcdStatusResponse(WireSafeModel):
    """``GET /api/services/{id}/cmcd/status``; only ``enabled`` is set when CMCD is off."""

    enabled: bool
    mode: str | None = None
    version: int | None = None
    enabled_at: str | None = None
