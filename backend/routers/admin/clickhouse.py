"""Explicit replay controls; RemoteAccessMiddleware blocks all /api/admin paths."""

from typing import Literal

import structlog
from fastapi import HTTPException, Query, Response
from fastapi.responses import JSONResponse

from backend import config
from backend.core import metadata
from backend.core.clickhouse_client import get_clickhouse_client
from backend.core.clickhouse_export import FosArtifacts
from backend.core.clickhouse_manifest import PgManifest
from backend.core.clickhouse_publication import full_rebuild, replay_unpublished
from backend.core.clickhouse_schema import CLICKHOUSE_SCHEMA_VERSION, target_identity
from backend.models.admin_clickhouse import (
    ClickHouseReplayErrorResponse,
    ClickHouseReplayRequest,
    ClickHouseReplayResponse,
    ClickHouseStatusResponse,
)
from backend.models.errors import ErrorDetail
from backend.routers.admin._router import router
from backend.utils.router_utils import make_error

logger = structlog.get_logger(__name__)


def _service(service_id: str, *, writable: bool = False) -> dict:
    try:
        cfg = config.load_config(service_id)
    except Exception:
        raise HTTPException(
            503, detail=make_error("service_unavailable", "Service configuration unavailable")
        ) from None
    if not cfg:
        raise HTTPException(404, detail=make_error("service_not_found", "Service not found"))
    if writable and cfg.get("access_level", "read_write") != "read_write":
        raise HTTPException(403, detail=make_error("read_write_required", "Read-write service required"))
    return cfg


@router.get(
    "/admin/clickhouse/status",
    response_model=ClickHouseStatusResponse,
    responses={503: {"model": ClickHouseStatusResponse}},
)
def api_clickhouse_status(response: Response, service_id: str = Query(min_length=1, max_length=128)):
    _service(service_id)
    try:
        client = get_clickhouse_client()
        if client is None:
            return ClickHouseStatusResponse(service_id=service_id, enabled=False, health="disabled")
        client.health()
        status = PgManifest(bounded=True).admin_status(service_id, target_identity(client))
        if status["schema_version"] != CLICKHOUSE_SCHEMA_VERSION:
            response.status_code = 503
            return ClickHouseStatusResponse(service_id=service_id, enabled=True, health="unavailable", **status)
        return ClickHouseStatusResponse(service_id=service_id, enabled=True, health="ok", **status)
    except Exception as exc:
        logger.warning("clickhouse.admin_status", outcome="unavailable", error_kind=type(exc).__name__)
        response.status_code = 503
        return ClickHouseStatusResponse(service_id=service_id, enabled=True, health="unavailable")


@router.post(
    "/admin/clickhouse/replay",
    response_model=ClickHouseReplayResponse,
    responses={503: {"model": ClickHouseReplayErrorResponse}},
)
def api_clickhouse_replay(req: ClickHouseReplayRequest):
    cfg = _service(req.service_id, writable=True)
    if req.limit > 100:
        raise HTTPException(422, detail=make_error("invalid_replay_limit", "HTTP replay limit must be 1..100"))
    try:
        client = get_clickhouse_client()
        if client is None:
            raise HTTPException(409, detail=make_error("clickhouse_disabled", "ClickHouse prototype disabled"))
        store = PgManifest(bounded=True)
        target = target_identity(client)
        try:
            preview = store.replay_preview(
                req.service_id, dataset_id=req.dataset_id, generation=req.generation, target=target, limit=req.limit
            )
        except LookupError:
            raise HTTPException(
                404, detail=make_error("replay_not_found", "Replay reference not found for service")
            ) from None
        except ValueError:
            raise HTTPException(
                409,
                detail=make_error("replay_ineligible", "Replay reference expired, incompatible, or exceeds HTTP cap"),
            ) from None
        operation: Literal["rebuild", "resume"] = "rebuild" if req.dataset_id else "resume"
        reply = ClickHouseReplayResponse(
            service_id=req.service_id,
            dry_run=req.dry_run,
            operation=operation,
            generation=req.generation,
            limit=req.limit,
            **preview,
        )
        if req.dry_run:
            return reply
        # No export API: the loader only GETs pre-existing sealed artifacts.
        objects = FosArtifacts(config.config_to_source(cfg))
        metadata.record_audit(
            req.service_id,
            event_type="clickhouse_replay",
            details={"action": operation, "phase": "requested", "limit": req.limit},
        )
        try:
            if req.dataset_id:
                result = full_rebuild(
                    req.service_id, req.dataset_id, loader=objects.load, limit=req.limit, store=store, client=client
                )
            else:
                assert req.generation is not None
                result = replay_unpublished(
                    req.service_id,
                    generation=req.generation,
                    loader=objects.load,
                    limit=req.limit,
                    store=store,
                    client=client,
                )
        except Exception:
            metadata.record_audit(
                req.service_id,
                event_type="clickhouse_replay",
                details={"action": operation, "phase": "failed", "limit": req.limit},
            )
            raise
        metadata.record_audit(
            req.service_id,
            event_type="clickhouse_replay",
            details={
                "action": operation,
                "phase": "completed",
                "attempted": result.attempted,
                "published": result.published,
                "activated": result.activated,
            },
        )
        reply.generation = result.generation
        reply.attempted = result.attempted
        reply.published = result.published
        reply.activated = result.activated
        return reply
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("clickhouse.admin_replay", outcome="unavailable", error_kind=type(exc).__name__)
        # An empty BaseResponse envelope also prevents the telemetry backstop
        # from injecting FOS URLs after a loader failure, even with debug opt-in.
        error = ClickHouseReplayErrorResponse(
            detail=ErrorDetail.model_validate(
                make_error("replay_unavailable", "ClickHouse replay unavailable; inspect status before retrying")
            )
        )
        return JSONResponse(status_code=503, content=error.model_dump(by_alias=True))
