"""Query router — SQL execution and preset queries."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.deps import get_service_id
from backend.core.request_context import RequestContext, build_request_context
from backend.models.dashboard import QueryRequest
from backend.repositories import query as repo

router = APIRouter(prefix="/api", tags=["query"])


@router.post("/query")
def query_endpoint(
    req: QueryRequest,
    request: Request,
    ctx: RequestContext = Depends(build_request_context),
    service_id: str | None = Depends(get_service_id),
):
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail={"error": "No SQL provided"})

    # Stamp session + service onto the validator audit log line so a
    # rejection-rate spike from one analyst (attack-shaped probing) is
    # observable without grepping correlated logs.
    analyst_session = getattr(request.state, "analyst_session", None)
    audit_session_id = analyst_session.session_id if analyst_session else "admin"

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

    def _run(con):
        return repo.execute_query(
            con=con,
            src=ctx.source,
            sql=sql,
            max_rows=req.max_rows,
            want_explain=req.explain,
            session_id=audit_session_id,
            service_id=service_id,
        )

    for attempt in (1, 2):
        try:
            return execute_with_stale_view_retry(ctx.con, ctx.source, _run)
        except PermissionError as e:
            raise HTTPException(status_code=403, detail={"error": str(e)})
        except Exception as e:
            msg = str(e)
            if attempt == 1 and "Cannot open file" in msg:
                continue  # compaction race — retry once
            raise HTTPException(status_code=400, detail={"error": msg})


@router.get("/presets")
def presets_endpoint(service_id: str | None = Depends(get_service_id)):
    from backend.core import duckdb as _db

    if not service_id:
        return []

    src = _db.get_source_for_service(service_id)
    if not src:
        return []

    con = None
    try:
        from backend.core.duckdb import get_connection

        # Presets are derived from log metadata, not the Iceberg view itself —
        # skip the view-update and open RO so this never blocks ingest.
        con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
        return repo.get_presets(src=src, con=con)
    except Exception:
        return repo.get_presets(src=src, con=None)
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass
