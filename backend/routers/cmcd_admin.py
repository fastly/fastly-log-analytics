"""CMCD admin router — enable/disable/status for CMCD collection.

GET  /api/services/{service_id}/cmcd/status
POST /api/services/{service_id}/cmcd/enable
POST /api/services/{service_id}/cmcd/disable
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Path

from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.status_reads import CmcdDisableBody, CmcdEnableBody, CmcdStatusResponse
from backend.utils.router_utils import resolve_fastly_token as _resolve_token
from backend.utils.sse import stream_iac_reconcile

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/services", tags=["cmcd-admin"], responses=DEFAULT_ERROR_RESPONSES)


@router.get("/{service_id}/cmcd/status", response_model=CmcdStatusResponse, response_model_exclude_unset=True)
def cmcd_status(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    cmcd = cfg.get("cmcd")
    if not cmcd or not cmcd.get("enabled"):
        return {"enabled": False}
    return {
        "enabled": True,
        "mode": cmcd.get("mode", "query_string"),
        "version": cmcd.get("version", 1),
        "enabled_at": cmcd.get("enabled_at"),
    }


@router.post("/{service_id}/cmcd/enable")
def cmcd_enable(
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdEnableBody | None = None,
):
    token = body.token if body else ""
    mode = body.mode if body else "query_string"
    version = body.version if body else 1
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required (pass in JSON body or set in service config)"},
        )

    import datetime as _dt

    from backend import config as svcconfig

    svcconfig.mutate_config(
        service_id,
        lambda c: c.__setitem__(
            "cmcd",
            {
                "enabled": True,
                "mode": mode,
                "version": version,
                "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            },
        ),
    )

    def _done_event() -> dict:
        cfg = svcconfig.load_config(service_id) or {}
        return {
            "type": "done",
            "message": f"CMCD v{version} collection enabled.",
            "cmcd": cfg.get("cmcd", {}),
        }

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Enabling CMCD v{version} collection for {service_id}...",
        done_event=_done_event,
        log_label="cmcd_enable",
        logger_obj=logger,
    )


@router.post("/{service_id}/cmcd/disable")
def cmcd_disable(
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdDisableBody | None = None,
):
    token = body.token if body else ""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required"},
        )

    from backend import config as svcconfig

    def _disable(c: dict) -> None:
        if "cmcd" in c:
            c["cmcd"]["enabled"] = False

    svcconfig.mutate_config(service_id, _disable)

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Disabling CMCD collection for {service_id}...",
        done_event={"type": "done", "message": "CMCD collection disabled."},
        log_label="cmcd_disable",
        logger_obj=logger,
    )
