"""CMCD admin router — enable/disable/status for CMCD collection.

GET  /api/services/{service_id}/cmcd/status
POST /api/services/{service_id}/cmcd/enable
POST /api/services/{service_id}/cmcd/disable
"""

from __future__ import annotations

import json
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/services", tags=["cmcd-admin"], responses=DEFAULT_ERROR_RESPONSES)


class CmcdEnableBody(BaseModel):
    token: str = ""
    mode: Literal["query_string", "headers"] = "query_string"
    version: int = 1


class CmcdDisableBody(BaseModel):
    token: str = ""


def _resolve_token(service_id: str, override_token: str = "") -> str:
    if override_token:
        return override_token
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if cfg:
        return cfg.get("fastly_api_key", "") or ""
    return ""


@router.get("/{service_id}/cmcd/status")
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

    from backend.provision.cmcd_orchestrator import enable_cmcd
    from backend.provision.orchestrator import run_with_events

    def stream():
        yield json.dumps({"type": "status", "message": f"Enabling CMCD v{version} collection for {service_id}..."})

        try:
            for event in run_with_events(enable_cmcd, service_id, resolved_token, mode=mode, version=version):
                yield json.dumps(event)
            from backend import config as svcconfig

            cfg = svcconfig.load_config(service_id) or {}
            cmcd = cfg.get("cmcd", {})
            yield json.dumps(
                {
                    "type": "done",
                    "message": f"CMCD v{version} collection enabled.",
                    "cmcd": cmcd,
                }
            )
        except Exception as e:
            logger.exception("cmcd_enable failed for %s", service_id)
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


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

    from backend.provision.cmcd_orchestrator import disable_cmcd
    from backend.provision.orchestrator import run_with_events

    def stream():
        yield json.dumps({"type": "status", "message": f"Disabling CMCD collection for {service_id}..."})

        try:
            for event in run_with_events(disable_cmcd, service_id, resolved_token):
                yield json.dumps(event)
            yield json.dumps({"type": "done", "message": "CMCD collection disabled."})
        except Exception as e:
            logger.exception("cmcd_disable failed for %s", service_id)
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)
