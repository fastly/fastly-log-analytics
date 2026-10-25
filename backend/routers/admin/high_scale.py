"""Shared admin router helpers, ClickHouse replay/retention routes, and multiplexed SSE channel."""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import sys
import zipfile
from collections.abc import AsyncIterator
from typing import Any, Literal

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from backend import config
from backend.core import metadata
from backend.core.clickhouse_client import get_clickhouse_client
from backend.core.clickhouse_manifest import PgManifest
from backend.core.clickhouse_publication import FosArtifacts, full_rebuild, replay_unpublished
from backend.core.clickhouse_schema import CLICKHOUSE_SCHEMA_VERSION, target_identity
from backend.cron_runs_publisher import publisher as cron_runs_publisher
from backend.deps import get_service_id, require_admin
from backend.high_scale.archive_models import (
    ServiceProfile,
    StorageProfile,
    latest_disk_snapshot,
    recommend_retention,
)
from backend.models._base import DEFAULT_ERROR_RESPONSES, ErrorDetail
from backend.models.admin_status import (
    ClickHouseReplayErrorResponse,
    ClickHouseReplayRequest,
    ClickHouseReplayResponse,
    ClickHouseStatusResponse,
)
from backend.models.high_scale import HighScaleRetentionRecommendationResponse
from backend.sync_status_publisher import publisher as sync_status_publisher
from backend.utils.router_utils import load_service_config_or_503, make_error, require_service_id
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
    iter_with_disconnect_ping,
    sse_response,
)

logger = logging.getLogger(__name__)
_struct_logger = structlog.get_logger(__name__)

# Shared APIRouter instance for the admin package. Lives here so sub-modules
# can import ``router`` without triggering the package-init's sub-module
# imports (which would be a circular dependency).
router: APIRouter = APIRouter(prefix="/api", tags=["admin"], responses=DEFAULT_ERROR_RESPONSES)


# ---------------------------------------------------------------------------
# Shared helpers (folded from backend.routers.admin._helpers)
# ---------------------------------------------------------------------------


class _QueueFile:
    """File-like wrapper around a queue.Queue for streaming ZIP generation."""

    def __init__(self, q: queue.Queue[bytes | None]) -> None:
        self.q = q
        self.offset = 0

    def write(self, b: bytes) -> int:
        self.q.put(b)
        n = len(b)
        self.offset += n
        return n

    def flush(self) -> None:
        pass

    def tell(self) -> int:
        return self.offset


def stream_zip(
    context_label: str,
    service_name: str,
    populate: Any,
) -> Any:
    """Run a streaming ZIP producer in a daemon thread."""

    def zip_worker(q: queue.Queue[bytes | None]) -> None:
        from backend.utils.telemetry import (
            flush_usage_log as _flush,
        )
        from backend.utils.telemetry import (
            process_context_scope as _pcs,
        )
        from backend.utils.telemetry import (
            start_call_tracking as _sct,
        )

        _sct()
        with _pcs(context_label):
            try:
                from typing import cast as _cast

                with zipfile.ZipFile(_cast(Any, _QueueFile(q)), "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    populate(zf)
            except Exception:
                logger.error("Error in ZIP generation", exc_info=True)
            finally:
                try:
                    _flush(service_name)
                except Exception:
                    logger.debug("zip_worker: _flush failed (ignored)", exc_info=True)
                q.put(None)

    return _stream_from_worker(zip_worker)


def _fetch_file_to_zip(
    source: dict[str, Any],
    fos_client: Any,
    cdn: str,
    key: str,
    arcname: str,
    zf: zipfile.ZipFile,
    caller: str,
) -> bool:
    """Fetch a single S3 key into the zip via CDN with fallback to direct FOS."""
    import time as _t
    import urllib.parse
    import urllib.request

    from backend.utils.telemetry import record_cdn_call as _rcdn

    if cdn:
        url = f"{cdn}/{urllib.parse.quote(key)}"
        try:
            req = urllib.request.Request(url)
            if source.get("cdn_secret"):
                req.add_header("x-fastly-key", source["cdn_secret"])
            t0 = _t.time()
            bytes_read = 0
            cdn_headers = None
            with urllib.request.urlopen(req, timeout=30) as response:
                cdn_headers = response.headers
                with zf.open(arcname, "w", force_zip64=True) as dest:
                    while True:
                        chunk = response.read(65536)
                        if not chunk:
                            break
                        bytes_read += len(chunk)
                        dest.write(chunk)
            _rcdn(
                "GET",
                key,
                round((_t.time() - t0) * 1000, 2),
                headers=cdn_headers,
                bytes_count=bytes_read,
                caller=caller,
            )
            return True
        except ClientDisconnected:
            raise
        except Exception:
            logger.warning("CDN fetch failed for %s, falling back to FOS", key, exc_info=True)

    try:
        resp = fos_client.get_object(Bucket=source["bucket"], Key=key)
        with zf.open(arcname, "w", force_zip64=True) as dest:
            body = resp["Body"]
            while True:
                chunk = body.read(65536)
                if not chunk:
                    break
                dest.write(chunk)
        return True
    except ClientDisconnected:
        raise
    except Exception:
        logger.error("Error fetching %s from FOS", key, exc_info=True)
        return False


def attach_debug_db_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach ``_debug_sqlite`` and ``_debug_postgres`` when debug responses are enabled."""
    from backend.models.common import _debug_responses_enabled

    if _debug_responses_enabled():
        from backend.utils.telemetry import get_sqlite_queries

        ds = list(get_sqlite_queries())
        payload["_debug_sqlite"] = ds
        payload["_debug_postgres"] = ds
    return payload


def service_id_from_source(
    source: dict[str, Any],
    service_id: str | None = None,
    *,
    prefer_name: bool = False,
) -> str:
    """Resolve ``service_id`` from an explicit override or the ``source`` dict."""
    if service_id:
        return service_id
    if prefer_name:
        return str(source.get("name") or source.get("service_id") or "")
    return str(source.get("service_id") or source.get("name") or "")


# ---------------------------------------------------------------------------
# ClickHouse replay & High-Scale retention (folded from backend.routers.admin.clickhouse)
# ---------------------------------------------------------------------------


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
    cfg = load_service_config_or_503(req.service_id, writable=True)
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


# ---------------------------------------------------------------------------
# Multiplexed admin SSE channel (folded from backend.routers.admin.events)
# ---------------------------------------------------------------------------

from . import health as _health_mod  # noqa: E402
from .health import compute_sync_status_cached, sample_system_metrics_cached  # noqa: E402

_ORIG_COMPUTE_SYNC_STATUS_CACHED = compute_sync_status_cached
_ORIG_SAMPLE_SYSTEM_METRICS_CACHED = sample_system_metrics_cached

_ADMIN_EVENT_CHANNELS = frozenset({"sync-status", "cron-runs", "system-metrics", "share", "celery-status"})

_METRICS_SAMPLE_INTERVAL_SECONDS = 10.0
_SHARE_SAMPLE_INTERVAL_SECONDS = 10.0
_CELERY_SAMPLE_INTERVAL_SECONDS = 5.0

_FANIN_MAXSIZE = 16


def _offer(q: asyncio.Queue[dict[str, Any]], env: dict[str, Any]) -> None:
    """Non-blocking enqueue with drop-oldest."""
    if q.full():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        q.put_nowait(env)
    except asyncio.QueueFull:
        pass


async def _sync_status_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str) -> None:
    fn = (
        compute_sync_status_cached
        if compute_sync_status_cached is not _ORIG_COMPUTE_SYNC_STATUS_CACHED
        else _health_mod.compute_sync_status_cached
    )
    initial = await asyncio.to_thread(fn, service_id)
    if initial is not None:
        _offer(q, {"channel": "sync-status", "data": initial})
    async for payload in sync_status_publisher.subscribe(service_id):
        _offer(q, {"channel": "sync-status", "data": payload})


async def _cron_runs_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str) -> None:
    async for payload in cron_runs_publisher.subscribe(service_id):
        _offer(q, {"channel": "cron-runs", "data": payload})


async def _system_metrics_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str | None) -> None:
    def _resolve_sampler() -> Any:
        return (
            sample_system_metrics_cached
            if sample_system_metrics_cached is not _ORIG_SAMPLE_SYSTEM_METRICS_CACHED
            else _health_mod.sample_system_metrics_cached
        )

    initial = await _resolve_sampler()(service_id)
    _offer(q, {"channel": "system-metrics", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_METRICS_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await _resolve_sampler()(service_id)
        except Exception:
            logger.exception("system-metrics sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "system-metrics", "data": payload})
            last_payload = payload


async def _celery_status_feeder(q: asyncio.Queue[dict[str, Any]]) -> None:
    from backend.celery_status_sampler import sample_celery_status_cached

    last_payload = None
    try:
        last_payload = await sample_celery_status_cached()
        _offer(q, {"channel": "celery-status", "data": last_payload})
    except Exception:
        logger.exception("celery-status initial sample failed; will retry next tick")
    while True:
        await asyncio.sleep(_CELERY_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await sample_celery_status_cached()
        except Exception:
            logger.exception("celery-status sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "celery-status", "data": payload})
            last_payload = payload


async def _share_feeder(q: asyncio.Queue[dict[str, Any]]) -> None:
    from backend.utils.tunnel import build_share_live_payload

    initial = build_share_live_payload()
    _offer(q, {"channel": "share", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_SHARE_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = build_share_live_payload()
        except Exception:
            logger.exception("share sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "share", "data": payload})
            last_payload = payload


@router.get("/admin/events/stream")
async def admin_events_stream(
    request: Request,
    channels: str = Query(..., description="Comma-separated admin channels to multiplex."),
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Multiplex the requested admin channels onto one SSE connection."""
    requested = [c.strip() for c in channels.split(",") if c.strip()]
    if not requested:
        raise HTTPException(status_code=422, detail=make_error("channels_required"))
    unknown = sorted(c for c in requested if c not in _ADMIN_EVENT_CHANNELS)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=make_error("unknown_channel", f"unknown channel(s): {', '.join(unknown)}"),
        )
    wanted = set(requested)

    async def stream() -> AsyncIterator[str]:
        out_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_FANIN_MAXSIZE)
        feeders: list[asyncio.Task[Any]] = []
        if "system-metrics" in wanted:
            feeders.append(asyncio.ensure_future(_system_metrics_feeder(out_q, service_id)))
        if "share" in wanted:
            feeders.append(asyncio.ensure_future(_share_feeder(out_q)))
        if "celery-status" in wanted:
            feeders.append(asyncio.ensure_future(_celery_status_feeder(out_q)))
        if service_id:
            if "sync-status" in wanted:
                feeders.append(asyncio.ensure_future(_sync_status_feeder(out_q, service_id)))
            if "cron-runs" in wanted:
                feeders.append(asyncio.ensure_future(_cron_runs_feeder(out_q, service_id)))

        async def _q_iter() -> AsyncIterator[dict[str, Any]]:
            while True:
                yield await out_q.get()

        try:
            async for env in iter_with_disconnect_ping(_q_iter(), request, ping_seconds=15):
                yield json.dumps(env)
        finally:
            for t in feeders:
                t.cancel()
            await asyncio.gather(*feeders, return_exceptions=True)

    return sse_response(stream())


sys.modules["backend.routers.admin._helpers"] = sys.modules[__name__]
sys.modules["backend.routers.admin._router"] = sys.modules[__name__]
sys.modules["backend.routers.admin.clickhouse"] = sys.modules[__name__]
sys.modules["backend.routers.admin.events"] = sys.modules[__name__]
