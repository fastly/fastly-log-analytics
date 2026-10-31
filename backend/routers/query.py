"""SQL console router — ad-hoc query execution, schema reflection, and preset queries."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import (
    RequestContext,
    ServiceId,
    build_request_context,
    get_service_id,
    require_service_access,
)
from backend.models.analytics import (
    PresetQuery,
    QueryRequest,
    QueryResponse,
    QuerySchemaResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.repositories import query as query_repo
from backend.repositories.query import get_cached_presets, set_cached_presets
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_error, redact_error_text

logger = logging.getLogger(__name__)

repo = query_repo

router = APIRouter(prefix="/api", tags=["query"], responses=DEFAULT_ERROR_RESPONSES)
query_router = router


def _redact_paths(msg: str) -> str:
    return redact_error_text(msg)


def _console_masked(analyst_session: object) -> bool:
    """Masked identity unless the invite's policy EXPLICITLY sets ``mask_ips`` to false."""
    policy = getattr(analyst_session, "pii_policy", None)
    return not (isinstance(policy, dict) and policy.get("mask_ips") is False)


@router.post("/query", response_model=QueryResponse, response_model_exclude_unset=True)
def query_endpoint(
    req: QueryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail=make_error("empty_sql", "No SQL provided"))

    analyst_session = ctx.analyst_session
    audit_session_id = getattr(analyst_session, "session_id", "admin") if analyst_session else "admin"

    start_time, end_time = ctx.clamp(None, None)
    time_filter = (start_time, end_time) if start_time and end_time else None
    mask_ips = mask_ips_for(analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        query_client = None
        query_tables = None
        if analyst_session is not None:
            from backend.high_scale.postgres_control import get_console_clients

            try:
                console = get_console_clients().client_for(
                    high_scale_service.service_id, masked=_console_masked(analyst_session)
                )
            except Exception:
                logger.warning(
                    "analyst console identity unavailable for %s", high_scale_service.service_id, exc_info=True
                )
                console = None
            if console is None:
                raise HTTPException(
                    status_code=403,
                    detail=make_error(
                        "console_unavailable_for_analysts",
                        "The SQL console is not available to analysts on this high-scale deployment.",
                    ),
                )
            query_client, query_tables = console.client, console.tables
        from backend.high_scale.query import query_endpoint as hs_query

        try:
            return hs_query(
                high_scale_service,
                req,
                start_time,
                end_time,
                mask_ips,
                session_id=audit_session_id,
                client=query_client,
                tables=query_tables,
            )
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", _redact_paths(str(e))))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=make_error("query_failed", _redact_paths(str(e))),
            )

    from backend.core.storage import execute_with_stale_view_retry as _exec_stale_retry

    def _run_with_compaction_retry(con, src: dict, fn, *, table_name: str):
        for attempt in (1, 2):
            try:
                return _exec_stale_retry(con, src, fn, table_name=table_name)
            except PermissionError as e:
                raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", _redact_paths(str(e))))
            except Exception as e:
                msg = str(e)
                if attempt == 1 and "Cannot open file" in msg:
                    continue
                raise HTTPException(
                    status_code=400,
                    detail=make_error("query_failed", _redact_paths(msg)),
                )

    if req.dataset != "logs":
        from backend.core.duckdb import rum_source_for
        from backend.core.request_context import _ConnectionHolder

        rum_source = rum_source_for(ctx.source)
        holder = _ConnectionHolder(rum_source, read_only=True)
        try:
            with holder as con:

                def _run_rum(c):
                    return query_repo.execute_query(
                        con=c,
                        src=rum_source,
                        sql=sql,
                        max_rows=req.max_rows,
                        want_explain=req.explain,
                        session_id=audit_session_id,
                        service_id=ctx.service_id,
                        time_filter=time_filter,
                        mask_ips=mask_ips,
                        dataset=req.dataset,
                    )

                return _run_with_compaction_retry(con, rum_source, _run_rum, table_name=req.dataset)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=make_error("query_failed", _redact_paths(str(e))),
            )
    else:

        def _run(con):
            return query_repo.execute_query(
                con=con,
                src=ctx.source,
                sql=sql,
                max_rows=req.max_rows,
                want_explain=req.explain,
                session_id=audit_session_id,
                service_id=ctx.service_id,
                time_filter=time_filter,
                mask_ips=mask_ips,
                dataset="logs",
            )

        return _run_with_compaction_retry(ctx.con, ctx.source, _run, table_name="logs")


_FALLBACK_SCHEMA_COLUMNS: list[dict[str, str]] = [
    {"column": "timestamp", "type": "TIMESTAMPTZ"},
    {"column": "ip", "type": "VARCHAR"},
    {"column": "url", "type": "VARCHAR"},
    {"column": "host", "type": "VARCHAR"},
    {"column": "method", "type": "VARCHAR"},
    {"column": "status", "type": "INTEGER"},
    {"column": "cache", "type": "VARCHAR"},
    {"column": "country", "type": "VARCHAR"},
    {"column": "pop", "type": "VARCHAR"},
    {"column": "asn", "type": "INTEGER"},
    {"column": "ua", "type": "VARCHAR"},
]


@router.get("/query/schema/{service_id}", response_model=QuerySchemaResponse)
def query_schema_endpoint(
    service_id: ServiceId,
    ctx: RequestContext = Depends(build_request_context),
):
    """Reflect schema columns and types for the active service."""
    del service_id
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    name = ctx.source["name"]
    status = svcconfig.get_status(name) or {}
    schema = status.get("schema")

    high_scale_service = get_high_scale_service_registry().resolve(ctx.service_id)
    if not schema and high_scale_service is None:
        try:
            schema = _db.get_schema(ctx.con, ctx.source)
        except Exception as exc:
            logger.debug("[query_schema] live schema fetch failed for %s: %s", name, exc)
            schema = []

    if not schema:
        schema = list(_FALLBACK_SCHEMA_COLUMNS)

    if mask_ips_for(ctx.analyst_session):
        schema = [col for col in schema if col.get("column") != "cookie_session"]

    return {
        "source": name,
        "schema": schema,
        "debug_queries": [],
        "debug_calls": [],
    }


@router.get("/presets", response_model=list[PresetQuery], response_model_exclude_unset=True)
def presets_endpoint(
    service_id: str | None = Depends(get_service_id),
    _scope: str | None = Depends(require_service_access),
):
    from backend.core import duckdb as _db

    if not service_id:
        return []

    src = _db.get_source_for_service(service_id)
    if not src:
        return []

    format_hash = (src.get("log_fields") or {}).get("format_hash")
    cached = get_cached_presets(service_id, format_hash)
    if cached is not None:
        return cached

    result = query_repo.get_presets(src=src)
    set_cached_presets(service_id, format_hash, result)
    return result
