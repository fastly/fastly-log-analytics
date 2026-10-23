"""FastAPI router for sharing domain deployment."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, HTTPException

from backend.config import list_configs
from backend.deps import require_admin
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.services import OkMessageResponse
from backend.models.share_admin import (
    SharingDeployRequest,
    SharingDeployResponse,
    SharingTeardownRequest,
)
from backend.provision.sharing_domain import delete_remote_frontend, deploy_remote_frontend

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/sharing",
    tags=["sharing"],
    dependencies=[Depends(require_admin)],
    responses=DEFAULT_ERROR_RESPONSES,
)


def _resolve_fastly_token(token_override: str | None) -> str:
    """Resolve Fastly API token from override, configs/*.json, or FASTLY_API_KEY env."""
    from backend.utils.router_utils import make_error, stored_fastly_token

    token = token_override
    if not token:
        for cfg in list_configs():
            key = stored_fastly_token(cfg)
            if key:
                token = key
                break
    if not token:
        env_token = os.environ.get("FASTLY_API_KEY")
        if env_token and env_token.strip():
            token = env_token.strip()
    if not token:
        raise HTTPException(
            status_code=400,
            detail=make_error("missing_api_token", "Fastly API Token is required"),
        )
    return token


@router.post("/deploy-frontend", response_model=SharingDeployResponse)
def deploy_frontend(payload: SharingDeployRequest):
    """Deploy a remote frontend service.

    Resolves the Fastly token by checking:
    1. token_override in request payload
    2. Any configs/*.json for fastly_api_key
    3. os.environ.get("FASTLY_API_KEY")

    Raises 400 if token is missing.
    """
    from backend.utils.router_utils import mutate_service_config, raise_internal

    token = _resolve_fastly_token(payload.token_override)

    try:
        res = deploy_remote_frontend(
            service_name=payload.service_name,
            domain_name=payload.domain_name,
            origin_host=payload.origin_host,
            origin_port=payload.origin_port,
            use_ssl=payload.use_ssl,
            token=token,
            override_host=payload.override_host,
        )

        # If a logging service ID is supplied, write remote frontend specs to its config
        if payload.service_id:
            remote_frontend_spec = {
                "service_id": res["service_id"],
                "version": res["version"],
                "domain_name": res["domain_name"],
                "origin_host": res["origin_host"],
            }
            try:
                mutate_service_config(
                    payload.service_id, lambda c: c.__setitem__("remote_frontend", remote_frontend_spec)
                )
            except Exception as e:
                logger.warning(f"Could not save remote_frontend to config of service {payload.service_id}: {e}")

        # Automatically start sharing with this public endpoint
        try:
            from backend.utils.tunnel import get_tunnel_manager

            get_tunnel_manager().start_sharing(public_endpoint=f"https://{res['domain_name']}")
        except Exception as e:
            logger.warning(f"Could not automatically activate sharing for {res['domain_name']}: {e}")

        return res
    except Exception as exc:
        raise_internal(logger, exc)


@router.post("/teardown-frontend", response_model=OkMessageResponse, response_model_exclude_unset=True)
def teardown_frontend(payload: SharingTeardownRequest):
    """Teardown a remote frontend service.

    Resolves Fastly API token, deletes the remote frontend on Fastly, and
    clears remote_frontend configuration on the logging service.
    """
    from backend.utils.router_utils import load_service_config, mutate_service_config, raise_internal

    token = _resolve_fastly_token(payload.token_override)

    try:
        cfg = load_service_config(payload.service_id)

        remote_frontend = cfg.get("remote_frontend")
        if not remote_frontend or not remote_frontend.get("service_id"):
            # Already torn down or never deployed
            return {"ok": True, "message": "No remote frontend configured or already torn down"}

        remote_service_id = remote_frontend["service_id"]
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

        # Clear remote frontend configuration and save
        mutate_service_config(payload.service_id, lambda c: c.pop("remote_frontend", None))

        return {"ok": True, "message": f"Successfully deleted remote frontend service {remote_service_id}"}
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal(logger, exc)
