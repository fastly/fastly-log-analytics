"""Services management router — list, cron settings, cron logs, rename, logging settings, log fields."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.deps import get_service_id, get_source
from backend.models.services import LogFieldsUpdateRequest, ServicesListResponse
from backend.utils.router_utils import SSE_HEADERS as _SSE_HEADERS
from backend.utils.router_utils import sse_flush_preamble as _sse_flush

router = APIRouter(prefix="/api", tags=["services"])


@router.get("/services", response_model=ServicesListResponse)
def api_services_list(service_id: str | None = Depends(get_service_id)):
    from backend.services.service_manager import get_enriched_services

    _debug_queries: list[dict] = []
    result = get_enriched_services(service_id)

    return ServicesListResponse.with_telemetry(services=result, debug_queries=_debug_queries)


@router.get("/services/{service_id}/lake-info")
def get_service_lake_info(source: dict = Depends(get_source)):
    """Return Iceberg table range and calendar for a configured service."""
    from backend.models.lake import fetch_lake_info

    return fetch_lake_info(source, use_temp_cache=False)


@router.post("/services/{service_id}/cron-settings")
@router.patch("/services/{service_id}/cron-settings")
def api_service_cron_settings(service_id: str, body: dict):
    from backend import config as svcconfig

    def stream():
        yield from _sse_flush()
        try:
            yield f"data: {json.dumps({'type': 'progress', 'current': 0, 'total': 4})}\n\n"
            yield f"data: {json.dumps({'type': 'status', 'message': f'Applying configuration for {service_id}...'})}\n\n"
            yield f": {' ' * 256}\n\n"
            cfg = svcconfig.load_config(service_id)
            if not cfg:
                raise ValueError("Service not found")
            yield f"data: {json.dumps({'type': 'progress', 'current': 1, 'total': 4})}\n\n"
            yield f"data: {json.dumps({'type': 'status', 'message': 'Updating sync and compaction schedules...'})}\n\n"
            prov = cfg.setdefault("provisioning", {})
            for cron_key in ("cron_sync", "cron_compact", "cron_ngwaf"):
                if cron_key in body:
                    incoming = body[cron_key]
                    existing = prov.get(cron_key, {})
                    allowed_keys = [
                        "enabled",
                        "interval_mins",
                        "commit_interval_mins",
                        "log_enabled",
                        "log_retention_days",
                        "data_retention_days",
                        "cache_retention_days",
                        "delete_after",
                    ]
                    existing.update({k: incoming[k] for k in allowed_keys if k in incoming})
                    prov[cron_key] = existing

            yield f"data: {json.dumps({'type': 'progress', 'current': 2, 'total': 4})}\n\n"
            yield f"data: {json.dumps({'type': 'status', 'message': 'Saving settings to local database...'})}\n\n"
            svcconfig.save_config(service_id, cfg)
            yield f"data: {json.dumps({'type': 'progress', 'current': 3, 'total': 4})}\n\n"
            yield f"data: {json.dumps({'type': 'status', 'message': 'Synchronizing APScheduler...'})}\n\n"
            try:
                from backend.provision import _sync_crontab

                _sync_crontab()
            except Exception as e:
                yield f"data: {json.dumps({'type': 'status', 'message': f'Warning: Cron sync failed: {e}'})}\n\n"
            try:
                from backend.core import metadata_db

                metadata_db.record_audit(service_id=service_id, event_type="cron_settings_update", details=body)
            except Exception:
                pass
            yield f"data: {json.dumps({'type': 'progress', 'current': 4, 'total': 4})}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'message': 'Successfully applied changes.'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.delete("/services/{service_id}/time-range")
def api_service_clear_time_range(service_id: str):
    """Remove the persisted time_range filter from a service config.

    Once cleared, the cron will ingest all available data (bounded only by
    log_retention_days), and the FOS scan will use the incremental lookback
    optimization rather than scanning from the original import start date.
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    prov = cfg.get("provisioning", {})
    if "time_range" not in prov:
        return {"ok": True, "message": "No time_range was set."}
    del prov["time_range"]
    cfg["provisioning"] = prov
    svcconfig.save_config(service_id, cfg)
    try:
        from backend.core import metadata_db

        metadata_db.record_audit(service_id=service_id, event_type="time_range_cleared", details={})
    except Exception:
        pass
    return {"ok": True, "message": "time_range cleared. Cron will now ingest all available data."}


@router.get("/cron-runs/{run_id}/stream")
async def cron_logs_stream(run_id: int, service_id: str | None = Depends(get_service_id)):
    import asyncio
    import json

    from backend.cron_progress import get_progress

    async def stream():
        last_idx = 0
        retries = 0
        # 3 s is plenty: start_cron_run() and start_progress() run sequentially
        # in the admin endpoints (see backend/routers/admin.py), so the gap is
        # microseconds in practice. If we're still waiting after 3 s, the row
        # is orphaned (server restarted mid-run) and we should surface that
        # immediately so the UI can switch out of "Loading logs..." instead of
        # hanging for 30 s.
        max_retries = 6  # 6 × 0.5 s = 3 s
        for _line in _sse_flush():
            yield _line
        while True:
            evs = get_progress(run_id, last_idx)
            if evs is None:
                if last_idx == 0:
                    # Fall back to SQLite database if progress cache doesn't have it (completed / historical)
                    try:
                        from backend.core import metadata_db

                        if service_id:
                            con = metadata_db.get_con(service_id)
                            row = con.execute(
                                "SELECT status, log_output FROM cron_runs WHERE id = ?", (run_id,)
                            ).fetchone()
                            if row:
                                status = row["status"]
                                log_output = row["log_output"]
                                if status in ("done", "error") or log_output:
                                    if log_output:
                                        lines = log_output.split("\n")
                                        has_terminal = False
                                        for line in lines:
                                            if not line.strip():
                                                continue
                                            t = "status"
                                            msg = line
                                            if line.startswith("[") and "]" in line:
                                                bracket_end = line.find("]")
                                                bracket_content = line[1:bracket_end].lower()
                                                if bracket_content in ("status", "error", "done", "warning", "info"):
                                                    t = bracket_content
                                                    msg = line[bracket_end + 1 :].strip()
                                            if t in ("done", "error"):
                                                has_terminal = True
                                            yield f"data: {json.dumps({'type': t, 'message': msg})}\n\n"

                                        if not has_terminal and status in ("done", "error"):
                                            yield f"data: {json.dumps({'type': status, 'message': f'Run completed with status {status}.'})}\n\n"
                                    else:
                                        yield f"data: {json.dumps({'type': status, 'message': f'Run completed with status {status} (no logs recorded).'})}\n\n"

                                    yield f": {' ' * 512} (flush)\n\n"
                                    return
                    except Exception as e:
                        import logging

                        logger = logging.getLogger("backend.routers.services.core")
                        logger.error(f"Error fetching historical logs for run {run_id}: {e}")

                if last_idx == 0 and retries < max_retries:
                    retries += 1
                    yield f": {' ' * 512} (waiting for start...)\n\n"
                    await asyncio.sleep(0.5)
                    continue
                if last_idx == 0:
                    yield (
                        "data: "
                        + json.dumps(
                            {
                                "type": "error",
                                "message": (
                                    f"No live progress for run {run_id} (likely interrupted by a "
                                    "server restart). Trigger a new sync to start fresh."
                                ),
                            }
                        )
                        + "\n\n"
                    )
                break
            if evs:
                for ev in evs:
                    yield f"data: {json.dumps(ev)}\n\n"
                    if ev.get("type") in ("done", "error"):
                        yield f": {' ' * 512} (flush)\n\n"
                        return
                last_idx += len(evs)
            yield f": {' ' * 512} (keep-alive)\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.get("/cron-schedule")
def api_cron_schedule(source: dict = Depends(get_source)):
    from backend.core import metadata_db
    from backend.scheduler import get_scheduler

    sched = get_scheduler()
    service_id = source["name"]
    last_runs: dict[str, dict] = {}
    try:
        per_task = metadata_db.latest_cron_per_task(service_id)
        for task, info in per_task.items():
            last_runs[task] = {
                "last_run_time": info["started_at"],
                "last_run_status": info["status"],
                "last_run_duration_s": info["duration_s"],
                "last_run_summary": info["summary"],
            }
    except Exception:
        pass
    _TASK_MAP = {
        "sync_metadata": "metadata_sync",
        "sync": "sync",
        "full_sync": "full_sync",
        "gap_heal": "gap_heal",
        "commit": "commit",
        "optimize": "optimize",
        "local_compact": "local_compact",
        "expire": "expire",
        "alerts_evaluation": "alerts",
        "ngwaf_sync": "ngwaf_sync",
        "metadata_cleanup": "metadata_cleanup",
    }
    schedules = []
    for job in sched._sched.get_jobs():
        job_id = getattr(job, "id", "")
        if not job_id.endswith(f"_{service_id}"):
            continue
        if job_id.startswith("initial_sync"):
            continue
        task_name = job_id[: -len(f"_{service_id}")]
        db_task = _TASK_MAP.get(task_name)
        if db_task is None:
            continue
        from backend.utils.date_utils import iso_z

        next_run = iso_z(job.next_run_time) if job.next_run_time else None
        schedules.append({"task": db_task, "next_run_time": next_run, **last_runs.get(db_task, {})})
    existing = {s["task"] for s in schedules}
    for task, info in last_runs.items():
        if task not in existing and task in _TASK_MAP.values():
            schedules.append({"task": task, "next_run_time": None, **info})

    # Mark the alerts tile as "No alerts configured" when no alerts exist.
    # Two cases:
    #  1. The cron is unregistered AND there are no historical runs → synthesize
    #     a fresh placeholder so the UI tile doesn't silently vanish.
    #  2. The cron is unregistered but historical runs exist → the loop above
    #     already added an alerts entry with next_run_time=None; tag it with
    #     disabled_reason so the UI renders "No alerts configured" instead of
    #     the ambiguous "Next: Disabled" fallback.
    # Once an alert is created the alerts router calls scheduler.reload(), which
    # re-registers the job and overwrites this placeholder with a live entry.
    try:
        from backend.core import metadata_db

        if metadata_db.count_alerts(service_id) == 0:
            alerts_entry = next((s for s in schedules if s["task"] == "alerts"), None)
            if alerts_entry is None:
                schedules.append(
                    {
                        "task": "alerts",
                        "next_run_time": None,
                        "disabled_reason": "no_alerts_configured",
                    }
                )
            elif alerts_entry.get("next_run_time") is None:
                alerts_entry["disabled_reason"] = "no_alerts_configured"
    except Exception:
        pass

    return {"schedules": schedules}


@router.patch("/services/{service_id}/credentials")
def api_service_update_credentials(service_id: str, body: dict):
    """Rotate FOS credentials for a service.

    Two modes:
    - api_token (admin read_write only): auto-creates a new FOS key via the Fastly API,
      deletes the old one, and saves the result.
    - access_key + secret_key (all services): validates the supplied credentials against
      FOS then saves them using the correct field names for the service's access level.
    """
    import time

    import botocore.exceptions

    from backend import config as svcconfig
    from backend.core.duckdb import _get_fos_client

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    is_admin = cfg.get("access_level") == "read_write"
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket", "")
    endpoint = cfg.get("fos_endpoint") or f"{region}.object.fastlystorage.app"
    api_token = (body.get("api_token") or "").strip()
    access_key = (body.get("access_key") or "").strip()
    secret_key = (body.get("secret_key") or "").strip()
    if api_token:
        if not is_admin:
            raise HTTPException(
                status_code=403,
                detail={"error": "API token rotation is only available for admin (read_write) services"},
            )
        from backend.core.fastly.client import fastly

        desc = f"fos-log-analysis-{service_id}-{int(time.time())}"
        try:
            key = fastly(
                "POST",
                "/resources/object-storage/access-keys",
                {"permission": "read-write-objects", "description": desc, "buckets": [bucket]},
                token=api_token,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail={"error": f"Failed to create FOS key via Fastly API: {e}"})
        old_key_id = cfg.get("provisioning", {}).get("fos_key_id")
        if old_key_id and old_key_id != key["access_key"]:
            try:
                fastly(
                    "DELETE", f"/resources/object-storage/access-keys/{old_key_id}", token=api_token, expect_empty=True
                )
            except RuntimeError:
                pass
        cfg["fos_access_key_id"] = key["access_key"]
        cfg["fos_secret_access_key"] = key["secret_key"]
        cfg.setdefault("provisioning", {})["fos_key_id"] = key["access_key"]
        svcconfig.save_config(service_id, cfg)
        return {"ok": True, "message": "Key rotated via Fastly API", "access_key_id": key["access_key"]}
    if not access_key or not secret_key:
        raise HTTPException(
            status_code=400, detail={"error": "Provide either api_token (admin) or both access_key and secret_key"}
        )
    src = {
        "bucket": bucket,
        "endpoint": endpoint,
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "region": region,
        "storage_mode": "cloud",
    }
    try:
        client = _get_fos_client(src)
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            raise HTTPException(
                status_code=400, detail={"error": "Validation failed: access denied. Check the key and secret."}
            )
        raise HTTPException(status_code=400, detail={"error": f"Validation failed: {code}"})
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": f"Validation failed: {e}"})
    cfg["fos_access_key_id"] = access_key
    cfg["fos_secret_access_key"] = secret_key
    cfg.setdefault("provisioning", {})["fos_key_id"] = access_key
    svcconfig.save_config(service_id, cfg)
    return {"ok": True, "message": "Credentials updated successfully"}


@router.post("/services/{service_id}/rename")
def api_service_rename(service_id: str, body: dict):
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail={"error": "Name is required"})
    cfg["name"] = name
    svcconfig.save_config(service_id, cfg)
    return {"ok": True, "name": name}


from backend.models.services import LoggingSettingsResponse


@router.get("/services/{service_id}/logging-settings", response_model=LoggingSettingsResponse)
def api_service_logging_settings(service_id: str):
    import re
    import urllib.parse

    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    token = cfg.get("fastly_api_key", "")
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    try:
        from backend.core.fastly.client import fastly
        from backend.core.fastly.service import find_condition, get_active_version

        active_ver = get_active_version(service_id, token)
        if not active_ver:
            raise HTTPException(status_code=400, detail={"error": "No active version found"})
        encoded_name = urllib.parse.quote(endpoint_name, safe="")
        ep = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/s3/{encoded_name}", token=token)
        sample_rate = 100
        edge_only = False
        custom_condition = ""
        cond_name = ep.get("response_condition")
        if cond_name:
            cond = find_condition(cond_name, service_id, active_ver, token)
            stmt = cond.get("statement", "") if cond else ""
            m = re.search("randombool\\((\\d+),", stmt)
            if m:
                sample_rate = int(m.group(1))
            if "req.restarts == 0" in stmt:
                edge_only = True
            mc = re.search(" && \\((?!req\\.restarts == 0)(.+)\\)$", stmt)
            if mc:
                custom_condition = mc.group(1)
        prov = cfg.get("provisioning", {})
        if not custom_condition:
            custom_condition = prov.get("custom_condition", "")
        full_path = ep.get("path", "")
        prefix = ""
        m = re.match("^/?(.*?)/raw/", full_path)
        if m:
            prefix = m.group(1).strip("/")
        format_match = True
        try:
            lf_config = cfg.get("log_fields")
            from backend.provision import load_log_format

            target_format = load_log_format(lf_config)
            current_format = (ep.get("format") or "").strip()
            if current_format != target_format:
                format_match = False
        except Exception:
            pass
        from backend.models.services import LoggingSettingsResponse

        return LoggingSettingsResponse.with_telemetry(
            ok=True,
            prefix=prefix,
            period=ep.get("period", 60),
            sample_rate=sample_rate,
            edge_only=edge_only,
            custom_condition=custom_condition,
            format_match=format_match,
            version=active_ver,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"ok": False, "error": str(e)})


from backend.models.services import LogFieldsResponse


@router.get("/services/{service_id}/log-fields", response_model=LogFieldsResponse)
def api_service_log_fields_get(service_id: str):

    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core import log_fields as lf

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    log_fields_config = lf.get_lf_config(cfg)
    if not log_fields_config.get("groups"):
        log_fields_config = {"groups": lf.PRESETS["standard"]["groups"], "field_overrides": {}}
    waf_warning = False
    if "J" in log_fields_config.get("groups", []):
        try:
            src = _db.get_source_for_service(service_id)
            if src:
                # read_only: schema lookup + one SELECT against the view.
                c = _db.get_connection(source=src, read_only=True)
                try:
                    table_name = _db._safe_table_name(src["name"])
                    actual_cols = {col["name"] for col in _db.get_schema(c, src)}
                    if "waf" in actual_cols:
                        result = c.execute(
                            f"\n                            SELECT SUM(CASE WHEN waf = true THEN 1 ELSE 0 END), COUNT(*)\n                            FROM {table_name}\n                            WHERE timestamp >= now() - INTERVAL '7 days'\n                        "
                        ).fetchone()
                        if result and result[1] and (result[1] >= 10000) and ((result[0] or 0) == 0):
                            waf_warning = True
                finally:
                    c.close()
        except Exception:
            pass
    history = []
    try:
        from backend.core import metadata_db

        _, history_rows = metadata_db.get_audit_logs(service_id, event_type="log_format_change", page=1, per_page=20)
        for r in history_rows:
            d = r["details"] if isinstance(r["details"], dict) else {}
            history.append(
                {
                    "changed_at": r["timestamp"],
                    "preset": d.get("preset"),
                    "groups_before": d.get("groups_before") or [],
                    "groups_after": d.get("groups_after") or [],
                    "fields_added": d.get("fields_added") or [],
                    "fields_removed": d.get("fields_removed") or [],
                    "changed_by": r["actor"],
                }
            )
    except Exception:
        pass
    from backend.utils.telemetry import get_tracked_calls

    return {
        "log_fields": log_fields_config,
        "waf_warning": waf_warning,
        "history": history,
        "estimate": lf.estimate_log_line_bytes(log_fields_config),
        "line_budget_warning": lf.check_log_line_budget(log_fields_config),
        "_debug_queries": [],
        "_debug_calls": get_tracked_calls(),
    }


@router.post("/services/{service_id}/log-fields")
def api_service_log_fields_set(service_id: str, body: LogFieldsUpdateRequest):
    from datetime import datetime

    from backend import config as svcconfig
    from backend.core import log_fields as lf

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    new_lf = body.log_fields
    if not new_lf:
        raise HTTPException(status_code=400, detail={"error": "log_fields is required"})
    old_lf = cfg.get("log_fields", {})
    # MERGE GUARD (sibling of 2026-06-02 state_sync fix): preserve
    # existing custom_fields unless the caller explicitly provided a
    # non-empty replacement. The pre-existing guard only triggered when
    # the key was absent — an empty list "custom_fields":[] still
    # stripped scoring fields. Treat "absent OR empty" as "no change",
    # then if scoring is enabled re-inject the canonical entries from
    # code so the routing-table for ingest stays correct.
    incoming_custom = new_lf.get("custom_fields")
    if not incoming_custom and old_lf.get("custom_fields"):
        new_lf["custom_fields"] = old_lf["custom_fields"]
    if cfg.get("scoring", {}).get("enabled"):
        from backend.provision.session_scoring_orchestrator import (
            _SCORING_CUSTOM_FIELDS,
            _SCORING_FIELD_NAMES,
        )

        merged = list(new_lf.get("custom_fields") or [])
        merged = [cf for cf in merged if cf.get("name") not in _SCORING_FIELD_NAMES]
        merged.extend(dict(cf) for cf in _SCORING_CUSTOM_FIELDS)
        new_lf["custom_fields"] = merged
    new_lf["schema_version"] = 2
    old_groups = set(old_lf.get("groups", []))
    new_groups = set(new_lf.get("groups", []))
    old_fields = lf.resolve_enabled_fields(old_lf) if old_lf else set()
    new_fields = lf.resolve_enabled_fields(new_lf)
    fields_added = sorted(new_fields - old_fields)
    fields_removed = sorted(old_fields - new_fields)
    old_hash = old_lf.get("format_hash")
    new_hash = lf.format_hash(new_lf)
    if old_hash == new_hash:
        return {"ok": True, "message": "No changes detected"}
    new_lf["format_hash"] = new_hash
    new_lf["format_updated_at"] = datetime.now(UTC).isoformat()
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(service_id)
    except Exception:
        pass
    return {
        "ok": True,
        "estimate": lf.estimate_log_line_bytes(new_lf),
        "line_budget_warning": lf.check_log_line_budget(new_lf),
    }


# Security: was @router.get — moved to POST/PATCH so a cross-origin
# `<img src=...>` or `<link rel=preload>` can no longer trigger a
# state-changing Fastly logging-settings update. The frontend's useSSE
# helper handles POST-with-streaming-response transparently.
@router.post("/services/{service_id}/logging-settings/update")
@router.patch("/services/{service_id}/logging-settings/update")
def api_service_update_logging_settings(
    service_id: str,
    period: int | None = Query(default=None),
    sample_rate: int | None = Query(default=None),
    prefix: str | None = Query(default=None),
    edge_only: bool | None = Query(default=None),
    custom_condition: str | None = Query(default=None),
    update_format: bool = Query(default=False),
):
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    prov = cfg.setdefault("provisioning", {})
    old_period = int(cfg.get("log_period", 60))
    old_sample_rate = int(prov.get("sample_rate", 100))
    old_prefix = cfg.get("fos_prefix", "")
    old_edge_only = prov.get("edge_only", False)
    old_custom_condition = prov.get("custom_condition", "")

    if period is None:
        period = old_period
    if sample_rate is None:
        sample_rate = old_sample_rate
    if prefix is None:
        prefix = old_prefix
    if edge_only is None:
        edge_only = old_edge_only
    if custom_condition is None:
        custom_condition = old_custom_condition
    if not 1 <= period <= 86400:
        raise HTTPException(status_code=400, detail={"error": "Rotation period must be between 1 and 86400 seconds"})
    if not 1 <= sample_rate <= 100:
        raise HTTPException(status_code=400, detail={"error": "Sample rate must be between 1 and 100"})
    token = cfg.get("fastly_api_key", "")
    endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
    prefix = prefix.strip("/")
    path = f"/{prefix}/raw/%Y-%m-%d/%H/" if prefix else "/raw/%Y-%m-%d/%H/"

    def stream():
        yield from _sse_flush()
        try:
            from backend.provision import update_logging_endpoint

            update_cfg = {
                "logging_service_id": service_id,
                "endpoint_name": endpoint_name,
                "log_period": period,
                "fos_path": path,
                "sample_rate": sample_rate,
                "edge_only": edge_only,
                "custom_condition": custom_condition,
                "update_format": update_format,
            }
            for event in update_logging_endpoint(update_cfg, token):
                if event.get("type") == "done":
                    fresh_cfg = svcconfig.load_config(service_id) or cfg
                    fresh_prov = fresh_cfg.setdefault("provisioning", {})
                    fresh_prov["sample_rate"] = sample_rate
                    fresh_prov["edge_only"] = edge_only
                    fresh_prov["custom_condition"] = custom_condition
                    fresh_cfg["log_period"] = period
                    fresh_cfg["fos_prefix"] = prefix
                    if "cron_sync" in fresh_prov:
                        if period < 60:
                            fresh_prov["cron_sync"]["interval_seconds"] = period
                            fresh_prov["cron_sync"].pop("interval_mins", None)
                        else:
                            fresh_prov["cron_sync"]["interval_mins"] = max(1, period // 60)
                            fresh_prov["cron_sync"].pop("interval_seconds", None)
                    svcconfig.save_config(service_id, fresh_cfg)
                    if event.get("changed", False):
                        try:
                            _details = {}
                            if old_period != period:
                                _details["period"] = {"from": old_period, "to": period}
                            if old_sample_rate != sample_rate:
                                _details["sample_rate"] = {"from": old_sample_rate, "to": sample_rate}
                            if old_prefix != prefix:
                                _details["prefix"] = {"from": old_prefix, "to": prefix}
                            if old_edge_only != edge_only:
                                _details["edge_only"] = {"from": old_edge_only, "to": edge_only}
                            if old_custom_condition != custom_condition:
                                _details["custom_condition"] = {"from": old_custom_condition, "to": custom_condition}
                            if update_format:
                                _details["log_fields_deployed"] = True

                            from backend.core import metadata_db

                            metadata_db.record_audit(
                                service_id=service_id,
                                event_type="logging_settings_update",
                                details=_details,
                            )
                        except Exception:
                            pass
                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()
                    except Exception:
                        pass
                yield f"data: {json.dumps(event)}\n\n"
                yield f": {' ' * 256}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


from backend.models.services import AnalystInvite


@router.post("/services/{service_id}/generate-viewer-key", response_model=AnalystInvite)
def api_invite_analyst(service_id: str):
    from backend.provision import generate_analyst_invite
    from backend.utils.telemetry import get_tracked_calls

    try:
        result = generate_analyst_invite(service_id)
    except RuntimeError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail={"error": msg})
        if "read_write" in msg.lower():
            raise HTTPException(status_code=403, detail={"error": msg})
        raise HTTPException(status_code=400, detail={"error": msg})

    result["_debug_calls"] = get_tracked_calls()
    return result


@router.post("/services/{service_id}/ngwaf-sync")
def api_ngwaf_sync(service_id: str):
    """Manually trigger an NGWAF bot-sync run for a service, streamed as SSE."""
    import time

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.utils.bot_sources import build_matcher
    from backend.utils.ngwaf import fetch_verified_bots_paged
    from backend.utils.ngwaf_bot_cache import cleanup_old_bots, upsert_bots

    def stream():
        yield from _sse_flush()
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Service not found'})}\n\n"
            return
        workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
        if not workspace_id:
            yield f"data: {json.dumps({'type': 'error', 'message': 'No NGWAF workspace configured for this service'})}\n\n"
            return
        api_key = cfg.get("fastly_api_key", "")
        if not api_key:
            yield f"data: {json.dumps({'type': 'error', 'message': 'No Fastly API key stored for this service'})}\n\n"
            return
        src = get_source_for_service(service_id)
        if src is None:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Service source not found'})}\n\n"
            return
        try:
            run_id = start_cron_run(src, "ngwaf_sync")
        except RuntimeError as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
            return
        prov = cfg.get("provisioning", {})
        retention_days = int(prov.get("cron_ngwaf", {}).get("log_retention_days", 30))
        server_name_filter = cfg.get("server_name") or None
        matcher = build_matcher()
        from backend.utils.ngwaf import oldest_unenriched_timestamp

        from_ts = oldest_unenriched_timestamp(src)
        if not from_ts:
            summary = "All requests are already enriched. Nothing to sync."
            log_cron_run(src, "ngwaf_sync", 0.0, "success", summary=summary, run_id=run_id)
            yield f"data: {json.dumps({'type': 'done', 'message': summary})}\n\n"
            return
        until_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        yield f"data: {json.dumps({'type': 'status', 'message': f'Scanning {from_ts} → {until_ts}...'})}\n\n"
        yield f": {' ' * 256}\n\n"
        total_records = 0
        total_raw = 0
        page_num = 0
        start_time = time.time()
        max_runtime_secs = 240
        try:
            for page_records, page_latest_ts, raw_count in fetch_verified_bots_paged(
                api_key, workspace_id, from_ts, until_ts=until_ts
            ):
                page_num += 1
                total_raw += raw_count
                if server_name_filter:
                    page_records = [
                        r for r in page_records if not r.get("server_name") or r["server_name"] == server_name_filter
                    ]
                enriched: list[dict] = []
                for r in page_records:
                    ua = r.get("user_agent")
                    wk_matches = matcher(ua) if ua else ()
                    wk_match = wk_matches[0] if wk_matches else None
                    enriched.append(
                        {
                            **r,
                            "wellknown_bot_id": wk_match.get("id") if wk_match else None,
                            "wellknown_bot_name": wk_match.get("name") if wk_match else None,
                        }
                    )
                if enriched or page_latest_ts:
                    upsert_bots(enriched, workspace_id, page_latest_ts)
                total_records += len(enriched)
                yield f"data: {json.dumps({'type': 'status', 'message': f'Page {page_num}: {raw_count} API records, {len(enriched)} verified-bot ({total_records} total so far)...'})}\n\n"
                yield f": {' ' * 256}\n\n"
                if time.time() - start_time >= max_runtime_secs:
                    summary = f"Synced {total_records} bot record(s) from {total_raw} API records across {page_num} page(s) — budget reached, run again to continue."
                    break
            else:
                deleted = cleanup_old_bots(retention_days)
                summary = f"Synced {total_records} bot record(s) from {total_raw} API records across {page_num} page(s), cleaned {deleted} old row(s)."
            log_cron_run(src, "ngwaf_sync", time.time() - start_time, "success", summary=summary, run_id=run_id)
            yield f"data: {json.dumps({'type': 'done', 'message': summary})}\n\n"
        except Exception as e:
            log_cron_run(src, "ngwaf_sync", time.time() - start_time, "error", error_message=str(e), run_id=run_id)
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


from backend.models.custom_fields import (
    CustomFieldCreate,
    CustomFieldResponse,
    CustomFieldsListResponse,
    CustomFieldUpdate,
    VclLintRequest,
    VclLintResponse,
)


@router.get("/services/{service_id}/custom-fields", response_model=CustomFieldsListResponse)
def api_list_custom_fields(service_id: str):
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    from backend.core import log_fields as lf_module

    lf = lf_module.get_lf_config(cfg)
    return CustomFieldsListResponse(fields=lf.get("custom_fields", []))


def _check_iceberg_type_lock(
    service_id: str, field_name: str, new_duckdb_type: str | None = None, new_value_type: str | None = None
) -> None:
    """Ensure we don't mutate the core type of an existing field in the Iceberg table."""
    from fastapi import HTTPException

    from backend.core import duckdb as _db

    src = _db.get_source_for_service(service_id)
    if not src:
        return
    try:
        from backend.core.iceberg import _DUCKDB_TO_ICEBERG, _get_catalog, _table_identifier

        catalog = _get_catalog(src)
        identifier = _table_identifier(src)
        table = catalog.load_table(identifier)
        schema = table.schema()
        field = schema.find_field(field_name) if field_name in {f.name for f in schema.fields} else None
        if field and new_duckdb_type:
            target_iceberg_type = _DUCKDB_TO_ICEBERG.get(new_duckdb_type)
            if target_iceberg_type and field.field_type != target_iceberg_type:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "errors": [
                            f"Cannot change data type for '{field_name}' (currently {field.field_type}) after it has been created in the database. Please create a new field instead."
                        ]
                    },
                )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        pass


@router.post("/services/{service_id}/custom-fields", response_model=CustomFieldResponse)
def api_create_custom_field(service_id: str, body: CustomFieldCreate):
    from datetime import UTC, datetime

    from backend import config as svcconfig
    from backend import provision
    from backend.core import log_fields as lf_module

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    existing_names = [cf["name"] for cf in existing]
    field_dict = body.model_dump()
    errors = lf_module.validate_custom_field(field_dict, existing_names)
    hard_errors = [e for e in errors if not e.startswith("WARN:")]
    if hard_errors:
        raise HTTPException(status_code=422, detail={"errors": hard_errors})
    _check_iceberg_type_lock(
        service_id, field_dict["name"], field_dict.get("duckdb_type"), field_dict.get("value_type")
    )
    candidate_lf = {**lf, "custom_fields": existing + [field_dict]}
    fmt_errors = provision.validate_log_format(candidate_lf)
    if any("LOG_FORMAT_TOO_LONG" in e for e in fmt_errors):
        raise HTTPException(status_code=422, detail={"errors": fmt_errors})
    now = datetime.now(UTC).isoformat()
    field_dict["created_at"] = now
    field_dict["updated_at"] = now
    new_custom = existing + [field_dict]
    new_lf = {
        **lf,
        "custom_fields": new_custom,
        "groups": lf.get("groups", []),
        "field_overrides": lf.get("field_overrides", {}),
        "field_limits": lf.get("field_limits", {}),
        "format_hash": lf_module.format_hash(candidate_lf),
        "format_updated_at": now,
        "schema_version": 2,
    }
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    warnings = [e for e in errors if e.startswith("WARN:")]

    return {"ok": True, "field": field_dict, "warnings": warnings}


@router.patch("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_update_custom_field(service_id: str, field_name: str, body: CustomFieldUpdate):
    from datetime import UTC, datetime

    from backend import config as svcconfig
    from backend import provision
    from backend.core import duckdb as _db
    from backend.core import log_fields as lf_module

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    idx = next((i for i, cf in enumerate(existing) if cf["name"] == field_name), None)
    if idx is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    updates = body.model_dump(exclude_unset=True)
    changing_type = False
    old_field = existing[idx]
    if "duckdb_type" in updates and updates["duckdb_type"] != old_field.get("duckdb_type"):
        changing_type = True
    if "value_type" in updates and updates["value_type"] != old_field.get("value_type"):
        changing_type = True
    if changing_type:
        src = _db.get_source_for_service(service_id)
        if src:
            try:
                from backend.core.iceberg import _get_catalog, _table_identifier

                catalog = _get_catalog(src)
                identifier = _table_identifier(src)
                table = catalog.load_table(identifier)
                schema_fields = {f.name for f in table.schema().fields}
                if field_name in schema_fields:
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "errors": [
                                "Cannot change 'duckdb_type' or 'value_type' after the field has been created in the database. Please create a new field instead."
                            ]
                        },
                    )
            except Exception as e:
                if isinstance(e, HTTPException):
                    raise
                pass
    updated = {**existing[idx], **{k: v for k, v in updates.items() if v is not None}}
    updated["updated_at"] = datetime.now(UTC).isoformat()
    other_names = [cf["name"] for cf in existing if cf["name"] != field_name]
    errors = lf_module.validate_custom_field(updated, other_names)
    hard_errors = [e for e in errors if not e.startswith("WARN:")]
    if hard_errors:
        raise HTTPException(status_code=422, detail={"errors": hard_errors})
    new_custom = existing[:idx] + [updated] + existing[idx + 1 :]
    candidate_lf = {**lf, "custom_fields": new_custom}
    fmt_errors = provision.validate_log_format(candidate_lf)
    if any("LOG_FORMAT_TOO_LONG" in e for e in fmt_errors):
        raise HTTPException(status_code=422, detail={"errors": fmt_errors})
    now = datetime.now(UTC).isoformat()
    new_lf = {
        **lf,
        "custom_fields": new_custom,
        "groups": lf.get("groups", []),
        "field_overrides": lf.get("field_overrides", {}),
        "field_limits": lf.get("field_limits", {}),
        "format_hash": lf_module.format_hash(candidate_lf),
        "format_updated_at": now,
        "schema_version": 2,
    }
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    warnings = [e for e in errors if e.startswith("WARN:")]

    return {"ok": True, "field": updated, "warnings": warnings}


@router.delete("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_delete_custom_field(service_id: str, field_name: str):
    from datetime import UTC, datetime

    from backend import config as svcconfig
    from backend.core import log_fields as lf_module

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    field = next((cf for cf in existing if cf["name"] == field_name), None)
    if field is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    new_custom = [cf for cf in existing if cf["name"] != field_name]
    now = datetime.now(UTC).isoformat()
    new_lf = {
        **lf,
        "custom_fields": new_custom,
        "groups": lf.get("groups", []),
        "field_overrides": lf.get("field_overrides", {}),
        "field_limits": lf.get("field_limits", {}),
        "format_hash": lf_module.format_hash({**lf, "custom_fields": new_custom}),
        "format_updated_at": now,
        "schema_version": 2,
    }
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)

    return {"ok": True, "field": field}


@router.post("/services/{service_id}/custom-fields/validate-vcl", response_model=VclLintResponse)
def api_validate_custom_vcl(service_id: str, body: VclLintRequest):
    from backend import config as svcconfig
    from backend import provision
    from backend.core import log_fields as lf_module

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    lf = lf_module.get_lf_config(cfg)
    candidate = {
        "name": "lint_check",
        "label": "Lint Check",
        "vcl_log_expression": body.vcl_log_expression,
        "collection_stage": body.collection_stage,
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
        "enabled": True,
    }
    errors = []
    warnings = []
    expr_errors = lf_module.validate_custom_field(candidate, [])
    for e in expr_errors:
        warnings.append(e.removeprefix("WARN: ")) if e.startswith("WARN:") else errors.append(e)
    if body.log_fields_config:
        candidate_lf = {**body.log_fields_config, "custom_fields": lf.get("custom_fields", []) + [candidate]}
    else:
        candidate_lf = {**lf, "custom_fields": lf.get("custom_fields", []) + [candidate]}
    fmt_errors = provision.validate_log_format(candidate_lf)
    for e in fmt_errors:
        warnings.append(e) if e.startswith("WARN") else errors.append(e)
    fmt = provision.load_log_format(candidate_lf) if not errors else None
    return VclLintResponse(
        valid=len(errors) == 0, errors=errors, warnings=warnings, format_length=len(fmt) if fmt else None
    )


@router.get("/services/{service_id}/custom-fields/export")
def api_export_custom_fields(service_id: str):
    import json

    from backend import config as svcconfig
    from backend.core import log_fields as lf_module

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    lf = lf_module.get_lf_config(cfg)
    return StreamingResponse(
        iter([json.dumps({"custom_fields": lf.get("custom_fields", [])})]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=custom_fields_{service_id}.json"},
    )


@router.post("/services/{service_id}/custom-fields/import")
def api_import_custom_fields(service_id: str, body: dict):
    from datetime import UTC, datetime

    from backend import config as svcconfig
    from backend import provision
    from backend.core import log_fields as lf_module

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    fields_to_import = body.get("custom_fields", [])
    if not isinstance(fields_to_import, list):
        raise HTTPException(status_code=400, detail={"error": "custom_fields must be a list"})
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    existing_map = {cf["name"]: cf for cf in existing}
    locked_field_names: set[str] = set()
    try:
        from backend.core import duckdb as _db
        from backend.core.iceberg import _get_catalog, _table_identifier

        src = _db.get_source_for_service(service_id)
        if src:
            catalog = _get_catalog(src)
            table = catalog.load_table(_table_identifier(src))
            locked_field_names = {f.name for f in table.schema().fields}
    except Exception:
        pass
    new_custom_map = {**existing_map}
    now = datetime.now(UTC).isoformat()
    type_lock_errors: list[str] = []
    validation_errors: list[str] = []
    for field_dict in fields_to_import:
        if "name" not in field_dict:
            continue
        fname = field_dict["name"]
        existing_field = existing_map.get(fname, {})
        if fname in locked_field_names:
            if field_dict.get("duckdb_type") and field_dict["duckdb_type"] != existing_field.get("duckdb_type"):
                type_lock_errors.append(
                    f"Cannot change 'duckdb_type' of '{fname}': field is already committed to the database."
                )
                continue
            if field_dict.get("value_type") and field_dict["value_type"] != existing_field.get("value_type"):
                type_lock_errors.append(
                    f"Cannot change 'value_type' of '{fname}': field is already committed to the database."
                )
                continue
        # 019: Run the same validator the single-field add/update endpoints
        # use, so importing a custom-fields JSON cannot smuggle in a
        # field that the interactive editor would have rejected (bad
        # name, dangerous VCL expression, oversized byte limit, etc.).
        # WARN-level lines are advisory and don't block the write.
        other_names = [n for n in new_custom_map if n != fname]
        for err in lf_module.validate_custom_field(field_dict, other_names):
            if not err.startswith("WARN:"):
                validation_errors.append(f"{fname}: {err}")
        field_dict.pop("created_at", None)
        field_dict.pop("updated_at", None)
        field_dict["created_at"] = existing_field.get("created_at", now)
        field_dict["updated_at"] = now
        new_custom_map[fname] = field_dict
    if type_lock_errors:
        raise HTTPException(status_code=422, detail={"errors": type_lock_errors})
    if validation_errors:
        raise HTTPException(status_code=422, detail={"errors": validation_errors})
    new_custom = list(new_custom_map.values())
    candidate_lf = {**lf, "custom_fields": new_custom}
    fmt_errors = provision.validate_log_format(candidate_lf)
    if any("LOG_FORMAT_TOO_LONG" in e for e in fmt_errors):
        raise HTTPException(status_code=422, detail={"errors": fmt_errors})
    new_lf = {
        **lf,
        "custom_fields": new_custom,
        "format_hash": lf_module.format_hash(candidate_lf),
        "format_updated_at": now,
        "schema_version": 2,
    }
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    return {"ok": True, "imported_count": len(fields_to_import)}
