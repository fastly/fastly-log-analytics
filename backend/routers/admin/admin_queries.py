"""Live Query Monitor, Usage-Logging, and System-Jobs admin endpoints."""

from __future__ import annotations

import csv
import io
import logging
import os
import sys
import time
from collections import deque
from collections.abc import Iterator
from typing import Any

from fastapi import Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from backend.core import metadata as _meta_mod
from backend.core.request_telemetry import query_registry
from backend.deps import get_service_id, get_source, require_admin
from backend.models.admin import (
    SystemJobsResponse,
    UsageLogAggregate,
    UsageLogEntry,
    UsageLoggingConfigResponse,
    UsageLoggingUpdateBody,
    UsageLogResponse,
)
from backend.models.admin_actions import (
    ActiveQueryRow,
    CancelResponse,
    QueryMonitorConfigResponse,
    SlowQueriesClearResponse,
    SlowQueriesCountResponse,
    SlowQueriesResponse,
    SnapshotResponse,
    SummaryResponse,
)
from backend.models.common import OkResponse
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.router_utils import (
    make_error,
    not_found,
    require_service_id,
)

from ._helpers import service_id_from_source
from ._router import router

logger = logging.getLogger(__name__)

admin_queries_router = router


def _enabled() -> bool:
    return os.environ.get("QUERY_MONITOR_ENABLED", "true").strip().lower() not in (
        "false",
        "0",
        "no",
        "off",
        "",
    )


def _ensure_enabled() -> None:
    if not _enabled():
        raise HTTPException(status_code=404, detail=not_found("query_monitor_disabled"))


_CANCEL_RATE_PER_SEC = 10
_CANCEL_WINDOW_S = 1.0
_cancel_history: dict[str, deque[float]] = {}


def _check_cancel_rate(admin_id: str) -> bool:
    now = time.monotonic()
    history = _cancel_history.setdefault(admin_id, deque(maxlen=_CANCEL_RATE_PER_SEC * 2))
    cutoff = now - _CANCEL_WINDOW_S
    while history and history[0] < cutoff:
        history.popleft()
    if len(history) >= _CANCEL_RATE_PER_SEC:
        return False
    history.append(now)
    return True


def _admin_id_from_request(request: Request) -> str:
    """Same logic as :func:`backend.core.request_context._build_attribution_from_request`
    — keep them in sync if the admin-id derivation ever moves."""
    from backend.utils.remote_access import client_ip

    return client_ip(request, default="unknown") or "admin"


@router.get(
    "/admin/queries",
    response_model=SnapshotResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def list_queries(
    since_seq: int = Query(0, ge=0),
    include_completed: bool = Query(False),
    cold_limit: int | None = Query(None, ge=1, le=400),
) -> SnapshotResponse:
    _ensure_enabled()
    snap = query_registry.snapshot(
        since_seq=since_seq,
        full_sql=False,
        include_completed=include_completed,
        all_active=True,
        cold_limit=cold_limit,
    )
    return SnapshotResponse(**snap)


@router.get(
    "/admin/queries/summary",
    response_model=SummaryResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def queries_summary() -> SummaryResponse:
    _ensure_enabled()
    return SummaryResponse(**query_registry.summary())


@router.get(
    "/admin/slow-queries/count",
    response_model=SlowQueriesCountResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def count_persisted_slow_queries(
    service_id: str = Depends(get_service_id),
    since_hours: int = Query(24, ge=1, le=24 * 30),
    threshold_ms: float = Query(1000.0, ge=0.0),
) -> SlowQueriesCountResponse:
    """Cheap row-count for the operations-overview card."""
    _ensure_enabled()
    service_id = require_service_id(service_id)
    since_utc = time.time() - since_hours * 3600
    return SlowQueriesCountResponse(
        count=_meta_mod.count_slow_queries(service_id, since_utc=since_utc, threshold_ms=threshold_ms),
        since_hours=since_hours,
        threshold_ms=threshold_ms,
    )


@router.get(
    "/admin/slow-queries",
    response_model=SlowQueriesResponse,
    response_model_exclude_unset=True,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def list_persisted_slow_queries(
    service_id: str = Depends(get_service_id),
    since_hours: int = Query(24, ge=1, le=24 * 30),
    threshold_ms: float = Query(100.0, ge=0.0),
    kind: str | None = Query(None, pattern="^(analyst|admin|cron|system)$"),
    db_type: str | None = Query(None, pattern="^(DuckDB|Postgres|ClickHouse|SQLite)$"),
    sort: str = Query("recent", pattern="^(recent|duration)$"),
    limit: int = Query(200, ge=1, le=2000),
) -> SlowQueriesResponse:
    """Persistent slow-SQL history from the PostgreSQL ``slow_queries`` table."""
    _ensure_enabled()
    service_id = require_service_id(service_id)
    since_utc = time.time() - since_hours * 3600
    rows = _meta_mod.list_slow_queries(
        service_id,
        since_utc=since_utc,
        threshold_ms=threshold_ms,
        kind=kind,
        db_type=db_type,
        sort_by_duration=(sort == "duration"),
        limit=limit,
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "query_id": r["query_id"],
                "db_type": r["db_type"],
                "service_id": r["service_id"],
                "sql_preview": r["sql_preview"],
                "sql": r["sql_full"],
                "sql_len": r["sql_len"],
                "started_at_utc": r["started_at_utc"],
                "ended_at_utc": r["ended_at_utc"],
                "duration_ms": r["duration_ms"],
                "outcome": r["outcome"],
                "error_type": r["error_type"],
                "error_message": r["error_message"],
                "peak_memory_mb": r["peak_memory_mb"],
                "attribution": {
                    "kind": r["attr_kind"],
                    "label": r["attr_label"],
                    "principal_id": r["attr_principal_id"],
                    "caller_qualname": r["attr_caller_qualname"],
                    "caller_file": r["attr_caller_file"],
                    "request_path": r["attr_request_path"],
                    "request_id": r["attr_request_id"],
                    "cron_job": r["attr_cron_job"],
                    "cron_run_id": r["attr_cron_run_id"],
                    "pool_slot": r["attr_pool_slot"],
                },
            }
        )
    return SlowQueriesResponse(rows=out, since_hours=since_hours, threshold_ms=threshold_ms)


@router.delete(
    "/admin/slow-queries",
    response_model=SlowQueriesClearResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def clear_persisted_slow_queries(
    request: Request,
    service_id: str = Depends(get_service_id),
) -> SlowQueriesClearResponse:
    """Clear persisted slow-SQL history for *service_id* and record an audit entry."""
    _ensure_enabled()
    service_id = require_service_id(service_id)
    deleted = _meta_mod.clear_slow_queries(service_id)
    admin_id = _admin_id_from_request(request)
    try:
        _meta_mod.record_audit(
            service_id,
            "slow_queries_cleared",
            {"deleted": deleted, "admin_id": admin_id},
            actor=admin_id,
        )
    except Exception:
        logger.warning("slow_queries_cleared audit write failed", exc_info=True)
    return SlowQueriesClearResponse(deleted=deleted, service_id=service_id)


@router.get(
    "/admin/queries/{qid}",
    response_model=ActiveQueryRow,
    response_model_exclude_unset=True,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def get_query(qid: int) -> ActiveQueryRow:
    """Fetch the full SQL + attribution for a single in-flight query."""
    _ensure_enabled()
    active = query_registry.get(qid)
    if active is None:
        raise HTTPException(status_code=404, detail=not_found("query_not_found"))
    snap = query_registry.snapshot(since_seq=qid - 1, full_sql=True)
    row: dict[str, Any] | None = next((r for r in snap["active"] if r["query_id"] == qid), None)
    if row is None:
        raise HTTPException(status_code=404, detail=not_found("query_not_found"))
    return ActiveQueryRow(**row)


@router.post(
    "/admin/queries/{qid}/cancel",
    response_model=CancelResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def cancel_query(qid: int, request: Request) -> CancelResponse:
    _ensure_enabled()
    admin_id = _admin_id_from_request(request)
    if not _check_cancel_rate(admin_id):
        raise HTTPException(
            status_code=429,
            detail=make_error("cancel_rate_limit_exceeded"),
            headers={"Retry-After": "1"},
        )
    active = query_registry.get(qid)
    svc_id = active.service_id if active is not None else None
    state = query_registry.cancel_query(qid, admin_id=admin_id)
    if state == "cancelled" and svc_id:
        try:
            _meta_mod.record_audit(
                svc_id,
                "query_cancelled",
                {"query_id": qid, "admin_id": admin_id},
                actor=admin_id,
            )
        except Exception:
            logger.warning("query_cancelled audit write failed", exc_info=True)
    return CancelResponse(state=state, query_id=qid)


@router.get(
    "/admin/app-config/query-monitor",
    response_model=QueryMonitorConfigResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def query_monitor_config() -> QueryMonitorConfigResponse:
    """Tiny config endpoint the frontend hits on mount to decide whether to
    render the Live Query Monitor tab."""
    return QueryMonitorConfigResponse(enabled=_enabled())


# ── Admin usage-logging + system-jobs endpoints (folded from admin_usage.py) ──


@router.get(
    "/admin/usage-logging",
    response_model=UsageLoggingConfigResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def get_usage_logging_settings() -> UsageLoggingConfigResponse:
    """Return the usage logging config (global defaults)."""
    from backend import config as svcconfig

    return UsageLoggingConfigResponse(**svcconfig.load_usage_logging_config())


@router.patch(
    "/admin/usage-logging",
    response_model=UsageLoggingConfigResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def update_usage_logging_settings(body: UsageLoggingUpdateBody) -> UsageLoggingConfigResponse:
    """Update the global usage logging config."""
    from backend import config as svcconfig

    updates = body.model_dump(exclude_unset=True)

    numeric_fields = (
        "retention_days",
        "class_a_rate_per_1k",
        "class_b_rate_per_10k",
        "cdn_egress_rate_per_gb",
        "storage_rate_per_gb_month",
        "min_billed_days",
    )
    for fld in numeric_fields:
        if fld not in updates:
            continue
        try:
            n = float(updates[fld])
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": f"{fld} must be a positive number", "field": fld},
            ) from exc
        if not (n > 0):
            raise HTTPException(
                status_code=400,
                detail={"error": f"{fld} must be a positive number", "field": fld},
            )

    if "billing_zone" in updates and updates["billing_zone"] is not None:
        zone = str(updates["billing_zone"])
        allowed_zones = ("MBZ100", "MBZ200", "Global")
        if zone not in allowed_zones:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": f"Invalid billing_zone '{zone}'. Must be one of: {', '.join(allowed_zones)}",
                    "field": "billing_zone",
                },
            )
        if "cdn_egress_rate_per_gb" not in updates:
            zone_rates = {"MBZ100": 0.08, "MBZ200": 0.12, "Global": 0.18}
            updates["cdn_egress_rate_per_gb"] = zone_rates[zone]

    current = svcconfig.load_usage_logging_config()
    current.update(updates)
    svcconfig.save_usage_logging_config(current)
    return UsageLoggingConfigResponse(**current)


@router.get(
    "/admin/usage-log",
    response_model=UsageLogResponse,
    dependencies=[Depends(require_admin)],
)
def usage_log_endpoint(
    source: dict[str, Any] = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    usage_type: str = Query(default=""),
    process_context: str = Query(default=""),
    operation_type: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
) -> Any:
    """Return paginated _usage_log entries with aggregates for cost analysis from metadata_db."""
    from backend import config as svcconfig
    from backend.core import metadata as metadata_db
    from backend.utils.date_utils import parse_date_window

    ul_cfg = svcconfig.load_usage_logging_config()
    rate_a = float(ul_cfg.get("class_a_rate_per_1k", 0.005))
    rate_b = float(ul_cfg.get("class_b_rate_per_10k", 0.01))
    rate_cdn = float(ul_cfg.get("cdn_egress_rate_per_gb", 0.12))

    start_str, end_str = parse_date_window(start, end)
    service_id = service_id_from_source(source, prefer_name=True)

    rows, total, agg_data = metadata_db.get_usage_logs(
        service_id=service_id,
        start=start_str,
        end=end_str,
        usage_type=usage_type,
        process_context=process_context,
        operation_type=operation_type,
        page=page,
        page_size=page_size,
    )

    total_a = agg_data["total_class_a"]
    total_b = agg_data["total_class_b"]
    total_cdn = agg_data["total_cdn_downloads"]
    cdn_bytes = agg_data["total_cdn_bytes"]
    fos_bytes = agg_data["total_fos_bytes"]

    cost_a = (total_a / 1000) * rate_a
    cost_b = (total_b / 10000) * rate_b
    cost_cdn = (cdn_bytes / (1024**3)) * rate_cdn

    entries = []
    for r in rows:
        op_class = r["operation_class"]
        op_count = int(r["count"] or 1) if "count" in r.keys() else 1
        b = r["bytes"]
        if op_class == "A":
            ec = (op_count / 1000) * rate_a
        elif op_class == "B":
            ec = (op_count / 10000) * rate_b
        elif op_class == "CDN":
            ec = ((b or 0) / (1024**3)) * rate_cdn
        else:
            ec = None

        entries.append(
            UsageLogEntry(
                id=int(r.get("id") or 0),
                timestamp=str(r["timestamp"]),
                operation_class=r["operation_class"],
                operation_type=r["operation_type"],
                url=r["url"],
                bytes=r["bytes"],
                duration_ms=r["duration_ms"],
                function_name=r["function_name"],
                process_context=r["process_context"],
                status=r["status"],
                estimated_cost=round(ec, 8) if ec is not None else None,
                count=op_count,
            )
        )

    aggregate = UsageLogAggregate(
        total_class_a=total_a,
        total_class_b=total_b,
        total_cdn_downloads=total_cdn,
        total_cdn_bytes=cdn_bytes,
        total_fos_bytes=fos_bytes,
        estimated_cost_class_a=round(cost_a, 6),
        estimated_cost_class_b=round(cost_b, 6),
        estimated_cost_cdn=round(cost_cdn, 6),
        estimated_cost_total=round(cost_a + cost_b + cost_cdn, 6),
        class_a_breakdown=agg_data["class_a_breakdown"],
        class_b_breakdown=agg_data["class_b_breakdown"],
    )

    return UsageLogResponse.with_telemetry(
        service_id=service_id,
        entries=entries,
        total=total,
        aggregate=aggregate,
    )


@router.get(
    "/admin/usage-log/export",
    dependencies=[Depends(require_admin)],
)
def usage_log_export(
    source: dict[str, Any] = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    usage_type: str = Query(default=""),
    process_context: str = Query(default=""),
    operation_type: str = Query(default=""),
    max_rows: int = Query(default=100_000, ge=1, le=1_000_000),
) -> StreamingResponse:
    """Export _usage_log as CSV from metadata_db."""
    from backend.core import metadata as metadata_db
    from backend.utils.date_utils import parse_date_window

    start_str, end_str = parse_date_window(start, end)
    service_id = service_id_from_source(source, prefer_name=True)

    CHUNK = 5_000

    def generate() -> Iterator[str]:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "timestamp",
                "service_id",
                "operation_class",
                "operation_type",
                "url",
                "bytes",
                "duration_ms",
                "function_name",
                "process_context",
                "status",
                "count",
            ]
        )
        buf.seek(0)
        yield buf.read()
        buf.seek(0)
        buf.truncate(0)

        for chunk in metadata_db.iter_usage_logs_chunks(
            service_id=service_id,
            start=start_str,
            end=end_str,
            usage_type=usage_type,
            process_context=process_context,
            operation_type=operation_type,
            chunk_size=CHUNK,
            max_rows=max_rows,
        ):
            for row in chunk:
                row_data = [
                    row["timestamp"],
                    row["service_id"],
                    row["operation_class"],
                    row["operation_type"],
                    row["url"],
                    row["bytes"],
                    row["duration_ms"],
                    row["function_name"],
                    row["process_context"],
                    row["status"],
                    row["count"] if "count" in row.keys() else 1,
                ]
                writer.writerow(
                    [
                        f"'{v}" if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@")) else v
                        for v in row_data
                    ]
                )
            buf.seek(0)
            yield buf.read()
            buf.seek(0)
            buf.truncate(0)

    headers = {"Content-Disposition": "attachment; filename=usage_log.csv"}
    return StreamingResponse(generate(), media_type="text/csv", headers=headers)


@router.delete(
    "/admin/usage-log",
    response_model=OkResponse,
    dependencies=[Depends(require_admin)],
)
def purge_usage_log_endpoint(source: dict[str, Any] = Depends(get_source)) -> OkResponse:
    """Delete all _usage_log entries for this service from metadata_db."""
    from backend.core import metadata as metadata_db

    metadata_db.clear_usage_log(service_id_from_source(source, prefer_name=True))
    return OkResponse(ok=True)


@router.get(
    "/admin/system-jobs",
    response_model=SystemJobsResponse,
    dependencies=[Depends(require_admin)],
)
def get_system_jobs_endpoint() -> Any:
    """Return status and schedule info for global background jobs."""
    from backend.cron.decorators import get_system_job_status
    from backend.cron.scheduler import get_scheduler

    statuses = get_system_job_status()
    result = []
    job_labels = {
        "bot_data_refresh": "Bot Data Refresh",
        "rdns_enrichment": "rDNS Enrichment",
        "share_audit_purge": "Share Audit Purge",
    }
    sched = get_scheduler()
    for job_id, label in job_labels.items():
        entry = {
            "id": job_id,
            "name": label,
            "next_run_at": None,
            **statuses.get(job_id, {"last_run_at": None, "status": None, "duration_s": None, "detail": ""}),
        }
        if sched is not None:
            try:
                job = sched.get_job(job_id)
            except Exception:
                job = None
            next_run = getattr(job, "next_run_time", None) if job else None
            if next_run:
                from backend.utils.date_utils import iso_z

                entry["next_run_at"] = iso_z(next_run)
        result.append(entry)

    return SystemJobsResponse.with_telemetry(jobs=result)


_CacheRegistry.register("routers.admin_queries._cancel_history", _cancel_history)

_self_mod = sys.modules[__name__]
sys.modules["backend.routers.admin_queries"] = _self_mod
sys.modules["backend.routers.admin_usage"] = _self_mod
if "backend.routers" in sys.modules:
    setattr(sys.modules["backend.routers"], "admin_queries", _self_mod)
    setattr(sys.modules["backend.routers"], "admin_usage", _self_mod)
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    sys.modules["backend"]._MODULE_ALIASES["backend.routers.admin_queries"] = __name__
    sys.modules["backend"]._MODULE_ALIASES["backend.routers.admin_usage"] = __name__
