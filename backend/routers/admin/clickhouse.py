"""ClickHouse replay & High-Scale retention recommendation admin endpoints."""

from __future__ import annotations

from typing import Any, Literal

import structlog
from fastapi import Depends, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from backend import config
from backend.core import metadata
from backend.core.clickhouse import (
    CLICKHOUSE_SCHEMA_VERSION,
    FosArtifacts,
    PgManifest,
    full_rebuild,
    get_clickhouse_client,
    replay_unpublished,
    target_identity,
)
from backend.core.request_context import require_admin
from backend.high_scale.archive import (
    ServiceProfile,
    StorageProfile,
    latest_disk_snapshot,
    recommend_retention,
)
from backend.models.admin import (
    ClickHouseReplayErrorResponse,
    ClickHouseReplayRequest,
    ClickHouseReplayResponse,
    ClickHouseStatusResponse,
    HighScaleRetentionRecommendationResponse,
)
from backend.models.common import ErrorDetail
from backend.utils.router_utils import load_service_config_or_503, make_error, require_service_id

from ._router import router

_struct_logger = structlog.get_logger(__name__)


@router.get(
    "/admin/clickhouse/status",
    response_model=ClickHouseStatusResponse,
    responses={503: {"model": ClickHouseStatusResponse}},
    dependencies=[Depends(require_admin)],
)
def api_clickhouse_status(
    response: Response,
    service_id: str | None = Query(default=None, max_length=128),
) -> Any:
    service_id = require_service_id(service_id)
    load_service_config_or_503(service_id)
    try:
        client = get_clickhouse_client()
        if client is None:
            return ClickHouseStatusResponse.with_telemetry(service_id=service_id, enabled=False, health="disabled")
        client.health()
        status = PgManifest(bounded=True).admin_status(service_id, target_identity(client))
        if status["schema_version"] != CLICKHOUSE_SCHEMA_VERSION:
            response.status_code = 503
            return ClickHouseStatusResponse.with_telemetry(
                service_id=service_id, enabled=True, health="unavailable", **status
            )
        return ClickHouseStatusResponse.with_telemetry(service_id=service_id, enabled=True, health="ok", **status)
    except Exception as exc:
        _struct_logger.warning("clickhouse.admin_status", outcome="unavailable", error_kind=type(exc).__name__)
        response.status_code = 503
        return ClickHouseStatusResponse.with_telemetry(service_id=service_id, enabled=True, health="unavailable")


@router.post(
    "/admin/clickhouse/replay",
    response_model=ClickHouseReplayResponse,
    responses={503: {"model": ClickHouseReplayErrorResponse}},
    dependencies=[Depends(require_admin)],
)
def api_clickhouse_replay(req: ClickHouseReplayRequest) -> Any:
    cfg = load_service_config_or_503(req.service_id)
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
                if req.generation is None:
                    raise RuntimeError("req.generation is unexpectedly None")
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
        _struct_logger.warning("clickhouse.admin_replay", outcome="unavailable", error_kind=type(exc).__name__)
        error = ClickHouseReplayErrorResponse(
            detail=ErrorDetail.model_validate(
                make_error("replay_unavailable", "ClickHouse replay unavailable; inspect status before retrying")
            )
        )
        return JSONResponse(status_code=503, content=error.model_dump(by_alias=True))


@router.get(
    "/admin/high-scale/retention-recommendation",
    response_model=HighScaleRetentionRecommendationResponse,
)
def api_high_scale_retention_recommendation(
    service_id: str | None = Query(default=None, max_length=128),
    events_per_second: float = Query(gt=0, description="Observed or estimated request events per second"),
    average_row_bytes: float = Query(default=500.0, gt=0),
    total_bytes: int | None = Query(
        default=None, gt=0, description="Serving volume size; defaults to the sampled volume"
    ),
) -> HighScaleRetentionRecommendationResponse:
    """Recommend hot/warm retention for sizing; an explicit per-service setting always wins."""
    service_id = require_service_id(service_id)
    if not config.is_high_scale_mode():
        raise HTTPException(404, detail=make_error("high_scale_not_enabled", "High-scale mode is not enabled"))
    cfg = load_service_config_or_503(service_id)

    volume_source: Literal["request", "sampled"] = "request"
    volume_bytes = total_bytes
    if volume_bytes is None:
        snapshot = latest_disk_snapshot()
        if snapshot is None:
            raise HTTPException(
                409,
                detail=make_error(
                    "high_scale_volume_unknown",
                    "Serving volume size is not sampled yet; pass total_bytes",
                ),
            )
        volume_bytes = snapshot.total_bytes
        volume_source = "sampled"

    recommendation = recommend_retention(
        ServiceProfile(events_per_second=events_per_second, average_row_bytes=average_row_bytes),
        StorageProfile(total_bytes=volume_bytes),
    )
    explicit = ((cfg.get("provisioning") or {}).get("cron_sync") or {}).get("data_retention_days")
    explicit_days = explicit if isinstance(explicit, int) and not isinstance(explicit, bool) and explicit >= 0 else None
    source: Literal["override", "recommendation"] = "override" if explicit_days is not None else "recommendation"
    effective: float | None
    if explicit_days is None:
        effective = recommendation.warm_days
    else:
        effective = float(explicit_days) if explicit_days > 0 else None

    _struct_logger.info(
        "high_scale.retention_recommended",
        service_id=service_id,
        events_per_second=events_per_second,
        average_row_bytes=average_row_bytes,
        volume_total_bytes=volume_bytes,
        volume_total_source=volume_source,
        recommended_hot_days=round(recommendation.hot_days, 3),
        recommended_warm_days=round(recommendation.warm_days, 3),
        explicit_retention_days=explicit_days,
        source=source,
    )
    return HighScaleRetentionRecommendationResponse(
        service_id=service_id,
        recommended_hot_days=recommendation.hot_days,
        recommended_warm_days=recommendation.warm_days,
        reasoning=recommendation.reasoning,
        volume_total_bytes=volume_bytes,
        volume_total_source=volume_source,
        explicit_retention_days=explicit_days,
        effective_retention_days=effective,
        source=source,
    )
