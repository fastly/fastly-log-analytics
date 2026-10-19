"""Small read-response models for routes that previously returned bare dicts."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from backend.models._base import WireSafeModel


class DebugStateResponse(WireSafeModel):
    """``GET /api/debug/state``."""

    debug_responses_enabled: bool


class SchemaColumnRow(WireSafeModel):
    """One column of ``get_schema``: name/type always; the SUMMARIZE stats only on the sampled path."""

    name: str
    type: str
    min: str | None = None
    max: str | None = None
    approx_unique: int | None = None
    null_percentage: float | None = None
    count: int | None = None


class SchemaResponse(WireSafeModel):
    """``GET /api/schema``: cached or live column list plus the safe table name."""

    schema_: list[SchemaColumnRow] = Field(alias="schema")
    table_name: str


class AuditLogEntry(WireSafeModel):
    """One ``metadata.get_audit_logs`` row (``details`` is the json-decoded payload)."""

    id: int
    timestamp: str
    event_type: str
    details: dict[str, Any]
    actor: str | None
    source: str


class AuditLogsResponse(WireSafeModel):
    """``GET /api/audit-logs``. ``_debug_calls`` rides along as an extra."""

    total: int
    page: int
    per_page: int
    entries: list[AuditLogEntry]


class CmcdStatusResponse(WireSafeModel):
    """``GET /api/services/{id}/cmcd/status``; only ``enabled`` is set when CMCD is off."""

    enabled: bool
    mode: str | None = None
    version: int | None = None
    enabled_at: str | None = None
