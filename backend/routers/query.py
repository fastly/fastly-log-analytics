"""Query router — SQL execution and preset queries."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.deps import ServiceId, get_service_id
from backend.models.dashboard import QueryRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.query import PresetQuery, QueryResponse, QuerySchemaResponse
from backend.repositories import query as repo
from backend.repositories._presets_cache import get_cached_presets, set_cached_presets
from backend.utils.auth import mask_ips_for
from backend.utils.error_redaction import redact_error_text
from backend.utils.router_utils import make_error

logger = logging.getLogger(__name__)


# DuckDB exception strings interpolate absolute file paths and SQL excerpts; every analyst-visible error
# body goes through backend.utils.error_redaction.redact_error_text (path strip + cid-salt scrub).


def _redact_paths(msg: str) -> str:
    return redact_error_text(msg)


router = APIRouter(prefix="/api", tags=["query"], responses=DEFAULT_ERROR_RESPONSES)


def _console_masked(analyst_session: object) -> bool:
    """Masked identity unless the invite's policy EXPLICITLY sets ``mask_ips`` to false.

    ``mask_ips_for`` treats a missing or malformed policy as unmasked, which is the existing behaviour of the other
    surfaces; on the free-form console a policy that is absent, ``None`` or revoked mid-session must fail closed.
    """
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

    # Stamp session + service onto the validator audit log line so a
    # rejection-rate spike from one analyst (attack-shaped probing) is
    # observable without grepping correlated logs. ctx.service_id is the
    # tenancy-enforced service (the one actually queried), so it attributes
    # the audit line correctly even for an analyst that passed no ?service.
    analyst_session = ctx.analyst_session
    audit_session_id = getattr(analyst_session, "session_id", "admin") if analyst_session else "admin"

    # H1: clamp this free-form query to the analyst's allowed window before it
    # touches the data. Admin (no analyst session) → (None, None) → no source
    # filter (full retained range). Unlike the sibling analytics endpoints this
    # one takes no caller-supplied range, so an analyst always gets exactly
    # their session window (or TimeBounds.clamp's now-1h..now default for an
    # open invite). The repo rebinds the log table to a window-filtered view —
    # see execute_query — so the clamp can't be aliased or aggregated away.
    start_time, end_time = ctx.clamp(None, None)
    time_filter = (start_time, end_time) if start_time and end_time else None

    # H2: mask result IPs by value when the invite carries mask_ips. The
    # analyst names the output columns here, so key-name masking is bypassable
    # — the repo masks any cell that parses as an IP.
    mask_ips = mask_ips_for(analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        query_client = None  # None = the service's own (privileged) client, admin sessions only
        query_tables = None
        # Analysts are an adversary and the console validates with DuckDB's parser while ClickHouse runs the
        # text, so name-based guards cannot be the only boundary (five independent reviews each found another
        # desync). An analyst's SQL therefore runs as a least-privilege ClickHouse identity for that service
        # and mask mode (mask_ips invites get the masked one): SELECT on its own views only, which fix the
        # service and, when masked, replace client IPs and session identifiers in the data itself (see
        # backend/high_scale/console_access.py). Without such an identity there is no console.
        if analyst_session is not None:
            from backend.high_scale.console_access import get_console_clients

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

    # Two-layer retry. The PermissionError → 403 path stays inline (it
    # short-circuits both retry classes — there's no point rebinding the
    # view on a validator rejection).
    #
    # Inner: ``execute_with_stale_view_retry`` (Phase 8 self-heal) catches
    # the "No files found that match the pattern …batch_<hash>.parquet"
    # error class — the iceberg view's cached SQL pointed at a buffer the
    # commit cycle has since swept. It clears the view cache + force-
    # rebinds before retrying once. Witnessed by analyst on /query at
    # 2026-06-10T15:42 UTC; the inline "Cannot open file" retry below
    # didn't catch it because that error class has a different message.
    #
    # Outer: the "Cannot open file" retry stays — local_compaction can
    # delete a file the read_parquet glob enumerated a moment ago; the
    # second attempt typically sees the post-compaction file set. This
    # race doesn't need a view rebind because the file delete isn't a
    # tombstone-style swap (no cached SQL points at it).
    from backend.core.iceberg import execute_with_stale_view_retry

    def _run_with_compaction_retry(con, src: dict, fn, *, table_name: str):
        for attempt in (1, 2):
            try:
                return execute_with_stale_view_retry(con, src, fn, table_name=table_name)
            except PermissionError as e:
                # SQL validator gate — message is controlled ("operation X not
                # permitted on table Y") so it's safe to echo for the admin.
                raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", _redact_paths(str(e))))
            except Exception as e:
                msg = str(e)
                if attempt == 1 and "Cannot open file" in msg:
                    continue  # compaction race — retry once
                # _redact_paths keeps the SQL diagnostic text the admin needs
                # while stripping the absolute file paths DuckDB interpolates
                # into IO Error messages.
                raise HTTPException(
                    status_code=400,
                    detail=make_error("query_failed", _redact_paths(msg)),
                )

    # Resolve active source and connection based on dataset.
    # For RUM datasets, checkout isolated RUM connection and source.
    if req.dataset != "logs":
        from backend.core.duckdb import rum_source_for
        from backend.deps import _ConnectionHolder

        rum_source = rum_source_for(ctx.source)
        holder = _ConnectionHolder(rum_source, read_only=True)
        try:
            with holder as con:

                def _run_rum(c):
                    return repo.execute_query(
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
            return repo.execute_query(
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
    from backend.high_scale.registry import get_high_scale_service_registry

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
def presets_endpoint(service_id: str | None = Depends(get_service_id)):
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

    # The presets are pure SQL templates derived from the service name;
    # they need no DuckDB connection. Skipping the get_connection
    # acquire collapses /api/presets from a multi-hundred-ms cold-tail
    # (extension install + _configure_fos + pool wait) to a pure
    # in-memory format on cache miss.
    result = repo.get_presets(src=src)
    set_cached_presets(service_id, format_hash, result)
    return result
