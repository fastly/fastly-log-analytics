"""Query router — SQL execution and preset queries."""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.deps import get_service_id
from backend.models.dashboard import QueryRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import query as repo
from backend.repositories._presets_cache import get_cached_presets, set_cached_presets
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_error

logger = logging.getLogger(__name__)


# DuckDB exception strings frequently interpolate absolute file paths
# ("Cannot open file '/srv/...'", "No such file or directory: '/var/...'").
# Strip them before echoing so the wire payload doesn't leak server-side
# directory layout while still preserving the SQL diagnostic text
# (Parser / Binder / Catalog Errors) the admin needs to fix their query.
#
# Conservative anchored prefix list: ONLY matches when the leading slash
# is preceded by a non-alphanumeric character (whitespace, quote, line
# start) AND the path root is one of the deploy-relevant Unix prefixes.
# Avoids stripping URL paths that happen to look like server paths
# ("https://example.com/var/foo" — the /var here is NOT a leak), column
# references in SQL ("/api/foo"), or qualified names. If a deployment
# uses a non-standard data root, append it here rather than reverting
# to the over-broad ``/[A-Za-z0-9_./-]+`` pattern.
# NB: ``app`` is the prod container WORKDIR (backend/Dockerfile → /app), so the
# real data root is /app/data/services/<sid>/... — without ``app`` here those
# absolute paths reach the analyst un-redacted in the query_failed envelope.
_ABS_PATH_RE = re.compile(r"(?<![A-Za-z0-9])/(?:srv|var|tmp|data|home|opt|usr|mnt|app)\b(?:/[A-Za-z0-9_./-]*)?")


def _redact_paths(msg: str) -> str:
    return _ABS_PATH_RE.sub("<path>", msg)


router = APIRouter(prefix="/api", tags=["query"], responses=DEFAULT_ERROR_RESPONSES)


@router.post("/query")
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

                for attempt in (1, 2):
                    try:
                        return execute_with_stale_view_retry(con, rum_source, _run_rum, table_name=req.dataset)
                    except PermissionError as e:
                        raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", str(e)))
                    except Exception as e:
                        msg = str(e)
                        if attempt == 1 and "Cannot open file" in msg:
                            continue  # compaction race — retry once
                        raise HTTPException(
                            status_code=400,
                            detail=make_error("query_failed", _redact_paths(msg)),
                        )
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

        for attempt in (1, 2):
            try:
                return execute_with_stale_view_retry(ctx.con, ctx.source, _run, table_name="logs")
            except PermissionError as e:
                # SQL validator gate — message is controlled ("operation X not
                # permitted on table Y") so it's safe to echo for the admin.
                raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", str(e)))
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


@router.get("/presets")
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
