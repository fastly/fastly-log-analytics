"""Response models for the Control Room JSON routes (``routers/control_room.py``).

``WireSafeModel`` (extra="allow", unset fields omitted) keeps each payload
identical to the dict the handler builds. The five mutation stubs always
raise 501, so they have no success model.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.models._base import WireSafeModel


class MutationStub(BaseModel):
    """Generic request body for Phase 0 mutation stubs."""

    action: str = ""
    payload: dict[str, Any] = {}


class ControlRoomActionResponse(WireSafeModel):
    status: str | None = None
    message: str | None = None


class CorrelateRequest(BaseModel):
    dimension: str
    window_minutes: int = Field(default=5, ge=1, le=60)
    limit: int = Field(default=10, ge=1, le=100)


class WizardStepRequest(BaseModel):
    """Request body for recording a wizard step completion."""

    step: str
    details: dict[str, Any] = {}


class ControlRoomTabResponse(WireSafeModel):
    """Canned per-tab payload (``TAB_STUBS``); the metric keys differ per tab."""

    tab: str
    status: str | None = None


class RealtimeSeedResponse(WireSafeModel):
    ticks: list[dict[str, Any]]


class LogFieldAuditFeature(WireSafeModel):
    name: str
    required_fields: list[str]
    status: str
    missing_fields: list[str]


class LogFieldAuditResponse(WireSafeModel):
    service_id: str
    enabled_fields: list[str]
    features: list[LogFieldAuditFeature]


class CorrelatorTopRow(WireSafeModel):
    value: str | int | float | None
    count: int


class CorrelatorFreshness(WireSafeModel):
    latest_log_at: str | None = None
    lag_seconds: int


class CorrelateResponse(WireSafeModel):
    dimension: str
    top: list[CorrelatorTopRow]
    freshness: CorrelatorFreshness


class WizardStateResponse(WireSafeModel):
    service_id: str
    completed_steps: list[str]
    is_complete: bool


class WizardStepResponse(WireSafeModel):
    recorded: bool
    step: str
