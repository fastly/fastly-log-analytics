"""Admin usage-logging endpoints (v2.0 file-size carve).

Carved out of ``backend/routers/admin.py`` so the main router file stays
under the 1500-line tech-debt threshold. The router instance + shared
helpers continue to live in ``admin.py``; this module just registers
its routes on the same router by importing it.

Endpoints here (all under /api/admin/usage-log* + /api/admin/system-jobs):
- GET    /api/admin/usage-logging
- PATCH  /api/admin/usage-logging
- GET    /api/admin/usage-log
- GET    /api/admin/usage-log/export
- DELETE /api/admin/usage-log
- GET    /api/admin/system-jobs

Cross-module symbol contract: ``admin.py`` registers this module's
routes by importing it for its side effects at the bottom of the file.
"""

from __future__ import annotations

import csv
import io

# Pull the shared router + helpers from the main admin module. Until
# ``backend.routers.admin`` is off the mypy override, its symbols come
# through as untyped — explicit annotation lets the @router decorators
# in this file resolve.
from fastapi import APIRouter, Depends, HTTPException, Query

from backend.deps import get_source
from backend.models.admin import (
    SystemJobsResponse,
    UsageLogAggregate,
    UsageLogEntry,
    UsageLoggingUpdateBody,
    UsageLogResponse,
)
from backend.routers import admin as _adm

router: APIRouter = _adm.router  # type: ignore


@router.get("/admin/usage-logging")
def get_usage_logging_settings():
    """Return the usage logging config (global defaults)."""
    from backend import config as svcconfig

    return svcconfig.load_usage_logging_config()


@router.patch("/admin/usage-logging")
def update_usage_logging_settings(body: UsageLoggingUpdateBody):
    """Update the global usage logging config."""
    from backend import config as svcconfig

    # exclude_unset preserves the partial-update contract: only fields
    # explicitly present in the request body are applied, so callers can
    # PATCH a single rate without nulling the rest.
    updates = body.model_dump(exclude_unset=True)

    # N-9: reject empty / non-positive numeric writes so an admin who hits
    # Save with empty inputs doesn't silently wipe the global rates to 0
    # (which would zero out cost estimates across every service). Boolean
    # toggles (enabled, track_duckdb_httpfs) are allowed through unchanged.
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
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail={"error": f"{fld} must be a positive number", "field": fld},
            )
        if not (n > 0):
            raise HTTPException(
                status_code=400,
                detail={"error": f"{fld} must be a positive number", "field": fld},
            )

    current = svcconfig.load_usage_logging_config()
    current.update(updates)
    svcconfig.save_usage_logging_config(current)
    return current


@router.get("/admin/usage-log", response_model=UsageLogResponse)
def usage_log_endpoint(
    source: dict = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    usage_type: str = Query(default=""),
    process_context: str = Query(default=""),
    operation_type: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
):
    """Return paginated _usage_log entries with aggregates for cost analysis from metadata_db (SQLite)."""
    from backend import config as svcconfig
    from backend.core import metadata as metadata_db
    from backend.utils.date_utils import parse_date_window

    ul_cfg = svcconfig.load_usage_logging_config()
    rate_a = float(ul_cfg.get("class_a_rate_per_1k", 0.005))
    rate_b = float(ul_cfg.get("class_b_rate_per_10k", 0.01))
    rate_cdn = float(ul_cfg.get("cdn_egress_rate_per_gb", 0.12))

    start_str, end_str = parse_date_window(start, end)
    service_id = source.get("name") or source.get("service_id", "")

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
        # `count` is 1 for observed proxy rows and N for reconciliation rows
        # written by fastly.reconciliation (one compact row per (hour, class)
        # gap vs Fastly's /stats/aggregate). The displayed estimated_cost has
        # to scale with N so the per-row cost matches the aggregate totals.
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


@router.get("/admin/usage-log/export")
def usage_log_export(
    source: dict = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    usage_type: str = Query(default=""),
    process_context: str = Query(default=""),
    operation_type: str = Query(default=""),
    max_rows: int = Query(default=100_000, ge=1, le=1_000_000),
):
    """Export _usage_log as CSV from metadata_db (SQLite).

    Paginated server-side in 5k chunks to keep the worker's resident set
    bounded — the previous implementation pulled the whole result into a
    single Python list before the generator started iterating, which
    materialised ~30+ MB per export and held the SQLite connection open
    the whole time. Now each chunk is fetched, written to the CSV
    generator, and released before the next page is read.

    ``max_rows`` caps the total to prevent an admin clicking the wrong
    filter from pulling tens of millions of rows; the default matches
    the prior implicit cap, the limit is the absolute ceiling.
    """

    from fastapi.responses import StreamingResponse as _StreamingResponse

    from backend.core import metadata as metadata_db
    from backend.utils.date_utils import parse_date_window

    start_str, end_str = parse_date_window(start, end)
    service_id = source.get("name") or source.get("service_id", "")

    CHUNK = 5_000

    def generate():
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
        # Flush the header before iterating rows so an empty result-set
        # still produces a valid header-only CSV (rather than an empty body).
        buf.seek(0)
        yield buf.read()
        buf.seek(0)
        buf.truncate(0)

        # Keyset (seek) pagination via iter_usage_logs_chunks: skips the
        # OFFSET-N scan cost that grew with page number. Each chunk costs
        # ~constant time regardless of how many chunks have already been
        # emitted, so a 100k-row export drops from O(N²) cumulative scan
        # work to O(N). The helper also handles the empty-result + short-
        # chunk termination so the loop here stays a single for/yield.
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
    return _StreamingResponse(generate(), media_type="text/csv", headers=headers)


@router.delete("/admin/usage-log")
def purge_usage_log_endpoint(source: dict = Depends(get_source)):
    """Delete all _usage_log entries for this service from metadata_db (SQLite)."""
    from backend.core import metadata as metadata_db

    service_id = source.get("name") or source.get("service_id", "")
    metadata_db.clear_usage_log(service_id)
    return {"ok": True}


@router.get("/admin/system-jobs", response_model=SystemJobsResponse)
def get_system_jobs_endpoint():
    """Return status and schedule info for global background jobs."""
    from backend.scheduler import get_scheduler
    from backend.utils.system_jobs import get_system_job_status

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
            # ``next_run_time`` is only set when the scheduler is running
            # AND the job has a future fire time. After ``scheduler.shutdown()``
            # (or when the job is paused) the attribute is absent or None,
            # so use getattr() to fail-soft rather than 500 the admin panel.
            next_run = getattr(job, "next_run_time", None) if job else None
            if next_run:
                from backend.utils.date_utils import iso_z

                entry["next_run_at"] = iso_z(next_run)
        result.append(entry)

    return SystemJobsResponse.with_telemetry(jobs=result)
