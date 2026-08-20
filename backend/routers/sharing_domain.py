"""FastAPI router for sharing domain deployment."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.config import list_configs
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.provision.sharing_domain import delete_remote_frontend, deploy_remote_frontend

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sharing", tags=["sharing"], responses=DEFAULT_ERROR_RESPONSES)


class SharingDeployRequest(BaseModel):
    service_name: str
    domain_name: str
    origin_host: str = "34.123.30.195"
    origin_port: int = 80
    use_ssl: bool = False
    token_override: str | None = None
    override_host: str | None = None
    service_id: str | None = None


class SharingDeployResponse(BaseModel):
    service_id: str
    version: int
    domain_name: str
    origin_host: str


class SharingTeardownRequest(BaseModel):
    service_id: str
    token_override: str | None = None


@router.post("/deploy-frontend", response_model=SharingDeployResponse)
def deploy_frontend(payload: SharingDeployRequest):
    """Deploy a remote frontend service.

    Resolves the Fastly token by checking:
    1. token_override in request payload
    2. Any configs/*.json for fastly_api_key
    3. os.environ.get("FASTLY_API_KEY")

    Raises 400 if token is missing.
    """
    token = payload.token_override

    # 1. Try to load from configs/*.json if not overridden
    if not token:
        for cfg in list_configs():
            key = cfg.get("fastly_api_key")
            if key and key.strip():
                token = key.strip()
                break

    from backend.utils.router_utils import load_service_config, make_error, raise_internal

    # 2. Try to fallback to env if still not found
    if not token:
        env_token = os.environ.get("FASTLY_API_KEY")
        if env_token and env_token.strip():
            token = env_token.strip()

    if not token:
        raise HTTPException(
            status_code=400,
            detail=make_error("missing_api_token", "Fastly API Token is required"),
        )

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
            from backend import config as svcconfig

            try:
                cfg = load_service_config(payload.service_id)
                cfg["remote_frontend"] = {
                    "service_id": res["service_id"],
                    "version": res["version"],
                    "domain_name": res["domain_name"],
                    "origin_host": res["origin_host"],
                }
                svcconfig.save_config(payload.service_id, cfg)
            except Exception as e:
                logger.warning(f"Could not save remote_frontend to config of service {payload.service_id}: {e}")

        return res
    except Exception as exc:
        raise_internal(logger, exc)


@router.post("/teardown-frontend")
def teardown_frontend(payload: SharingTeardownRequest):
    """Teardown a remote frontend service.

    Resolves Fastly API token, deletes the remote frontend on Fastly, and
    clears remote_frontend configuration on the logging service.
    """
    token = payload.token_override

    # 1. Try to load from configs/*.json if not overridden
    if not token:
        for cfg in list_configs():
            key = cfg.get("fastly_api_key")
            if key and key.strip():
                token = key.strip()
                break

    from backend.utils.router_utils import load_service_config, make_error, raise_internal

    # 2. Try to fallback to env if still not found
    if not token:
        env_token = os.environ.get("FASTLY_API_KEY")
        if env_token and env_token.strip():
            token = env_token.strip()

    if not token:
        raise HTTPException(
            status_code=400,
            detail=make_error("missing_api_token", "Fastly API Token is required"),
        )

    try:
        from backend import config as svcconfig

        cfg = load_service_config(payload.service_id)

        remote_frontend = cfg.get("remote_frontend")
        if not remote_frontend or not remote_frontend.get("service_id"):
            # Already torn down or never deployed
            return {"ok": True, "message": "No remote frontend configured or already torn down"}

        remote_service_id = remote_frontend["service_id"]
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

        # Clear remote frontend configuration and save
        cfg.pop("remote_frontend", None)
        svcconfig.save_config(payload.service_id, cfg)

        return {"ok": True, "message": f"Successfully deleted remote frontend service {remote_service_id}"}
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal(logger, exc)
