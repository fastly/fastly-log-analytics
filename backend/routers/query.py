"""Query router — SQL execution and preset queries."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.deps import AnalyticsDeps, get_service_id
from backend.models.dashboard import QueryRequest
from backend.repositories import query as repo

router = APIRouter(prefix="/api", tags=["query"])


@router.post("/query")
def query_endpoint(req: QueryRequest, deps: AnalyticsDeps = Depends()):
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail={"error": "No SQL provided"})

    # Single retry on "Cannot open file" — the local_compaction cron can
    # delete the file the read_parquet glob just enumerated. The race
    # window is sub-second; a single retry catches it transparently.
    # See architecture-review Finding #3.
    for attempt in (1, 2):
        try:
            return repo.execute_query(
                con=deps.con, src=deps.source, sql=sql, max_rows=req.max_rows, want_explain=req.explain
            )
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
