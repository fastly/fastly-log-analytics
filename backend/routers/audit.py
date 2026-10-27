"""Log-field audit and audit-log backed onboarding wizard router."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from backend.core.metadata.state import list_audit, record_audit
from backend.deps import ServiceId, require_service_access
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.common import (
    LogFieldAuditFeature,
    LogFieldAuditResponse,
    WizardStateResponse,
    WizardStepRequest,
    WizardStepResponse,
)
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["audit"], responses=DEFAULT_ERROR_RESPONSES)
audit_router = router

FEATURE_REQUIRED_FIELDS: dict[str, list[str]] = {
    "Dashboard": ["status", "cache_status", "url", "method"],
    "Performance": ["time_elapsed", "time_to_first_byte"],
    "Origin": ["is_cacheable", "origin_host"],
    "Security": ["client_ip", "geo_country", "user_agent"],
    "Sessions": ["session_id", "url"],
    "Network": ["pop", "geo_country"],
    "Insights": ["status", "url", "client_ip", "user_agent"],
}


@audit_router.get(
    "/services/{service_id}/log-field-audit",
    response_model=LogFieldAuditResponse,
    response_model_exclude_unset=True,
)
@query_errors()
def log_field_audit(
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> LogFieldAuditResponse:
    """Check which log fields are enabled and which features they support."""
    from backend import config as svcconfig
    from backend.core.log_fields import resolve_enabled_fields

    cfg = svcconfig.load_config(service_id)
    log_fields_cfg: dict[str, Any] | None = cfg.get("log_fields") if cfg else None
    enabled = resolve_enabled_fields(log_fields_cfg or {})
    enabled_ids = {f if isinstance(f, str) else f.get("id", f) for f in enabled}

    features: list[LogFieldAuditFeature] = []
    for feature_name, required in FEATURE_REQUIRED_FIELDS.items():
        missing = [f for f in required if f not in enabled_ids]
        features.append(
            LogFieldAuditFeature(
                name=feature_name,
                required_fields=required,
                status="ok" if not missing else "missing_fields",
                missing_fields=missing,
            )
        )

    return LogFieldAuditResponse(
        service_id=service_id,
        enabled_fields=sorted(enabled_ids),
        features=features,
    )


@audit_router.get(
    "/services/{service_id}/control-room/wizard/state",
    response_model=WizardStateResponse,
    response_model_exclude_unset=True,
)
@query_errors()
def wizard_state(
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> WizardStateResponse:
    """Return current wizard state from audit log."""
    entries = list_audit(service_id, limit=100)
    wizard_entries = [e for e in entries if e.get("event_type", "").startswith("wizard_")]
    completed_steps = [
        e["event_type"].replace("wizard_step_", "")
        for e in wizard_entries
        if e["event_type"].startswith("wizard_step_")
    ]
    return WizardStateResponse(
        service_id=service_id,
        completed_steps=completed_steps,
        is_complete=any(e["event_type"] == "wizard_complete" for e in wizard_entries),
    )


@audit_router.post(
    "/services/{service_id}/control-room/wizard/step",
    response_model=WizardStepResponse,
    response_model_exclude_unset=True,
)
@query_errors()
def wizard_step(
    body: WizardStepRequest,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> WizardStepResponse:
    """Record a wizard step completion via audit log."""
    record_audit(
        service_id=service_id,
        event_type=f"wizard_step_{body.step}",
        details=body.details,
        actor="control_room_wizard",
    )
    return WizardStepResponse(recorded=True, step=body.step)
