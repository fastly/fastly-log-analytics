"""Services management router — list, cron settings, cron logs, logging settings, log fields."""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Iterator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

from backend.deps import get_service_id, get_source
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.provision import CustomFieldsImportBody, ProvisionLakeInfoResponse
from backend.models.services import (
    CredentialsUpdateResponse,
    CronScheduleResponse,
    CustomFieldsImportResponse,
    LogFieldsUpdateRequest,
    LogFieldsUpdateResponse,
    OkMessageResponse,
    ServiceCredentialsBody,
    ServiceCronSettingsBody,
    ServicesListResponse,
)
from backend.repositories._base import SectionTimer
from backend.utils.auth import require_service_in_scope
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, load_service_config, make_error, raise_internal

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["services"], responses=DEFAULT_ERROR_RESPONSES)
# Short TTL on /api/cron-schedule. The page polls every 30 s (after
# perf item #33 throttled the cron-history pull), but a manual click
# can hit it within the window. The data is APScheduler state +
# latest_cron_per_task lookup + count_alerts — all near-pure reads
# that don't change between polls. 5 s TTL collapses the second poll
# from ~1 s to <10 ms without breaking the user's perception of
# "live" schedule data (schedules only change on config edits).
_CRON_SCHEDULE_TTL = 5.0
_cron_schedule_cache: dict[str, tuple[float, dict]] = {}


def clear_cron_schedule_cache(service_id: str | None = None) -> None:
    """Bust the /api/cron-schedule cache for a service (or all services)."""
    if service_id:
        _cron_schedule_cache.pop(service_id, None)
    else:
        _cron_schedule_cache.clear()


# N-2: fields safe to surface to a remote analyst on ``GET /api/services``.
# The full enriched dict contains operator infra strings (cdn_url,
# cdn_service_id, fos_bucket, fos_region, ngwaf_workspace_id) plus DuckDB
# internals (duckdb_size_bytes, cache_file_count, log_row_count) and per-
# service cron schedules — none of which the analyst frontend renders.
# Anything not in this set is stripped before the response leaves the
# router for analyst sessions.
_ANALYST_SAFE_SERVICE_FIELDS = frozenset(
    {
        "service_id",
        "name",
        "access_level",
        "is_active",
    }
)


def _trim_for_analyst(services: list[dict], allowed_ids: set[str]) -> list[dict]:
    out: list[dict] = []
    for svc in services:
        sid = svc.get("service_id", "")
        if sid not in allowed_ids:
            continue
        out.append({k: v for k, v in svc.items() if k in _ANALYST_SAFE_SERVICE_FIELDS})
    return out


def _require_service_scope(request: Request, service_id: str) -> None:
    """Defense-in-depth: reject a request when the analyst session attached
    to ``request.state`` does not include ``service_id`` in its allowed
    services. Admin sessions (``analyst_session is None``) pass through.

    Mirrors the read gate already in place on ``api_services_list`` and the
    write gate on ``api_service_update_credentials`` — this helper exists
    so every per-service endpoint can call one place instead of
    re-implementing the check. Thin wrapper over the shared
    :func:`backend.utils.auth.require_service_in_scope`.
    """
    require_service_in_scope(request, service_id)


@router.get("/services", response_model=ServicesListResponse)
def api_services_list(request: Request, service_id: str | None = Depends(get_service_id)):
    from backend.services.service_manager import get_enriched_services

    _debug_queries: list[dict] = []
    result = get_enriched_services(service_id)

    # N-2: analysts get a slim, whitelisted view scoped to their invite's
    # service_ids. Admins (analyst_session is None) see the full enriched
    # list, unchanged.
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        allowed = set(analyst_session.service_ids or [])
        result = _trim_for_analyst(result, allowed)

    return ServicesListResponse.with_telemetry(services=result, debug_queries=_debug_queries)


@router.get(
    "/services/{service_id}/lake-info",
    response_model=ProvisionLakeInfoResponse,
    response_model_exclude_unset=True,
)
def get_service_lake_info(source: dict = Depends(get_source)):
    """Return Iceberg table range and calendar for a configured service."""
    from backend.core.iceberg.lake_info import fetch_lake_info

    return fetch_lake_info(source, use_temp_cache=False)


# response_model intentionally omitted: SSE progress stream
# (EventSourceResponse), not a JSON body.
@router.post("/services/{service_id}/cron-settings")
def api_service_cron_settings(request: Request, service_id: str, body: ServiceCronSettingsBody):
    from backend import config as svcconfig

    _require_service_scope(request, service_id)

    # Capture once for the audit-log line below + the per-block iteration.
    # ``exclude_unset`` preserves the existing partial-update semantics
    # (only sent keys are written) — without it, every absent field would
    # appear as None and clobber the persisted value.
    body_payload = body.model_dump(exclude_unset=True)

    def stream():
        try:
            yield json.dumps({"type": "progress", "current": 0, "total": 4})
            yield json.dumps({"type": "status", "message": f"Applying configuration for {service_id}..."})
            cfg = svcconfig.load_config(service_id)
            if not cfg:
                raise ValueError("Service not found")
            yield json.dumps({"type": "progress", "current": 1, "total": 4})
            yield json.dumps({"type": "status", "message": "Updating sync and compaction schedules..."})
            prov = cfg.setdefault("provisioning", {})
            for cron_key in ("cron_sync", "cron_compact", "cron_ngwaf"):
                incoming = body_payload.get(cron_key)
                if incoming is None:
                    continue
                existing = prov.get(cron_key, {})
                # exclude_unset on the parent already filtered to sent
                # keys, but Pydantic dumps each nested block as a full
                # dict of its model fields — re-filter so we don't write
                # a None where the operator simply omitted a sub-field.
                existing.update({k: v for k, v in incoming.items() if v is not None})
                prov[cron_key] = existing

            # Handle RUM settings (top-level cfg, not provisioning)
            rum_incoming = body_payload.get("rum")
            if rum_incoming is not None:
                rum_config = cfg.setdefault("rum", {})
                rum_config.update({k: v for k, v in rum_incoming.items() if v is not None})
                if not rum_config.get("cid_salt"):
                    import secrets

                    rum_config["cid_salt"] = secrets.token_hex(32)
                cfg["rum"] = rum_config

            yield json.dumps({"type": "progress", "current": 2, "total": 4})
            yield json.dumps({"type": "status", "message": "Saving settings to local database..."})
            svcconfig.save_config(service_id, cfg)
            yield json.dumps({"type": "progress", "current": 3, "total": 4})
            yield json.dumps({"type": "status", "message": "Synchronizing APScheduler..."})
            try:
                from backend.provision import _sync_crontab

                _sync_crontab()
            except Exception as e:
                yield json.dumps({"type": "status", "message": f"Warning: Cron sync failed: {e}"})
            try:
                from backend.core import metadata as metadata_db

                metadata_db.record_audit(service_id=service_id, event_type="cron_settings_update", details=body_payload)
            except Exception:
                pass
            yield json.dumps({"type": "progress", "current": 4, "total": 4})
            yield json.dumps({"type": "done", "message": "Successfully applied changes."})
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.delete(
    "/services/{service_id}/time-range",
    response_model=OkMessageResponse,
    response_model_exclude_unset=True,
)
def api_service_clear_time_range(service_id: str):
    """Remove the persisted time_range filter from a service config.

    Once cleared, the cron will ingest all available data (bounded only by
    log_retention_days), and the FOS scan will use the incremental lookback
    optimization rather than scanning from the original import start date.
    """
    from backend import config as svcconfig

    cfg = load_service_config(service_id)
    prov = cfg.get("provisioning", {})
    if "time_range" not in prov:
        return {"ok": True, "message": "No time_range was set."}
    del prov["time_range"]
    cfg["provisioning"] = prov
    svcconfig.save_config(service_id, cfg)
    try:
        from backend.core import metadata as metadata_db

        metadata_db.record_audit(service_id=service_id, event_type="time_range_cleared", details={})
    except Exception:
        pass
    return {"ok": True, "message": "time_range cleared. Cron will now ingest all available data."}


# response_model intentionally omitted: SSE log stream (EventSourceResponse).
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
        while True:
            evs = get_progress(run_id, last_idx, service_id=service_id)
            if evs is None:
                if last_idx == 0:
                    # Fall back to SQLite database if progress cache doesn't have it (completed / historical)
                    try:
                        import sqlite3

                        from backend.core import metadata as metadata_db

                        if service_id:
                            row = metadata_db.get_cron_run_result(service_id, run_id)
                            if row:
                                status = row["status"]
                                # Normalize SQLite status values (success -> done, failed -> error) for SSE client compatibility
                                normalized_status = (
                                    "done"
                                    if status in ("success", "done")
                                    else "error"
                                    if status in ("failed", "error")
                                    else status
                                )
                                log_output = row["log_output"]
                                if normalized_status in ("done", "error") or log_output:
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
                                                if bracket_content in (
                                                    "status",
                                                    "error",
                                                    "done",
                                                    "warning",
                                                    "info",
                                                    "success",
                                                    "failed",
                                                ):
                                                    # Map sub-line types as well
                                                    t = (
                                                        "done"
                                                        if bracket_content in ("success", "done")
                                                        else "error"
                                                        if bracket_content in ("failed", "error")
                                                        else bracket_content
                                                    )
                                                    msg = line[bracket_end + 1 :].strip()
                                            if t in ("done", "error"):
                                                has_terminal = True
                                            yield json.dumps({"type": t, "message": msg})

                                        if not has_terminal and normalized_status in ("done", "error"):
                                            yield json.dumps(
                                                {
                                                    "type": normalized_status,
                                                    "message": f"Run completed with status {status}.",
                                                }
                                            )
                                    else:
                                        # No log output — generate a helpful message based on task type and status
                                        task_name = "unknown task"
                                        try:
                                            with sqlite3.connect(f"data/services/{service_id}.metadata.db") as con:
                                                con.row_factory = sqlite3.Row
                                                t_row = con.execute(
                                                    "SELECT task FROM cron_runs WHERE id = ?", (run_id,)
                                                ).fetchone()
                                                if t_row:
                                                    task_name = t_row["task"] or "unknown task"
                                        except Exception:
                                            pass

                                        msg = f"Run completed with status {status}."
                                        if normalized_status == "done" and task_name == "rum_sync":
                                            msg = "No new RUM logs found."
                                        elif normalized_status == "done":
                                            msg = f"{task_name} completed with no data."

                                        yield json.dumps({"type": normalized_status, "message": msg})
                                    return
                    except Exception as e:
                        import logging

                        logger = logging.getLogger("backend.routers.services.core")
                        logger.error(f"Error fetching historical logs for run {run_id}: {e}")

                if last_idx == 0 and retries < max_retries:
                    retries += 1
                    await asyncio.sleep(0.5)
                    continue
                if last_idx == 0:
                    yield json.dumps(
                        {
                            "type": "error",
                            "message": (
                                f"No live progress for run {run_id} (run orphaned — interrupted "
                                "by a server restart, or it exited without recording logs). "
                                "Trigger a new sync to start fresh."
                            ),
                        }
                    )
                break
            if evs:
                for ev in evs:
                    yield json.dumps(ev)
                    if ev.get("type") in ("done", "error"):
                        return
                last_idx += len(evs)
            await asyncio.sleep(0.5)

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.get("/cron-schedule", response_model=CronScheduleResponse, response_model_exclude_unset=True)
def api_cron_schedule(source: dict = Depends(get_source)):
    from backend.cron.schedule import build_cron_schedule_payload

    service_id = source["name"]
    now_mono = time.monotonic()
    cached = _cron_schedule_cache.get(service_id)
    if cached is not None and (now_mono - cached[0]) < _CRON_SCHEDULE_TTL:
        return cached[1]
    payload = build_cron_schedule_payload(source)
    _cron_schedule_cache[service_id] = (now_mono, payload)
    return payload


@router.patch(
    "/services/{service_id}/credentials",
    response_model=CredentialsUpdateResponse,
    response_model_exclude_unset=True,
)
def api_service_update_credentials(request: Request, service_id: str, body: ServiceCredentialsBody):
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

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
    is_admin = cfg.get("access_level") == "read_write"
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket", "")
    endpoint = cfg.get("fos_endpoint") or f"{region}.object.fastlystorage.app"
    api_token = body.api_token.strip()
    access_key = body.access_key.strip()
    secret_key = body.secret_key.strip()
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
            raise_internal(logger, e, code="fos_key_create_failed", status=400)
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
            status_code=400,
            detail=make_error(
                "fos_credentials_missing",
                "Provide either api_token (admin) or both access_key and secret_key",
            ),
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
                status_code=400,
                detail=make_error(
                    "fos_credentials_invalid",
                    "Validation failed: access denied. Check the key and secret.",
                ),
            )
        raise HTTPException(status_code=400, detail=make_error("fos_credentials_invalid", str(code)))
    except Exception as e:
        raise_internal(logger, e, code="fos_credentials_failed", status=400)
    cfg["fos_access_key_id"] = access_key
    cfg["fos_secret_access_key"] = secret_key
    cfg.setdefault("provisioning", {})["fos_key_id"] = access_key
    svcconfig.save_config(service_id, cfg)
    return {"ok": True, "message": "Credentials updated successfully"}


from backend.models.services import LoggingSettingsResponse
from backend.utils.bounded_cache import BoundedTTLCache

# Process-local response cache for /api/services/{service_id}/logging-settings.
# The endpoint chains 2-3 Fastly API calls (get_active_version → GET endpoint
# → find_condition) costing ~700ms cold. Per the perf audit it fires on
# every /alerts page nav and every tab refocus inside the alerts UI, so the
# same Fastly payload is fetched repeatedly within a single user session.
#
# Cached value shape: the full pre-pydantic dict that LoggingSettingsResponse
# wraps. We stamp ``"_is_cached": True`` on hits so the Debug Panel can
# distinguish cache vs cold and ``section_timings`` stays meaningful.
#
# Invalidation: ``api_service_update_logging_settings`` calls
# ``_logging_settings_cache.pop(service_id, None)`` after a successful
# Fastly mutation so the next read returns the user's own write, not the
# stale snapshot.
_LOGGING_SETTINGS_CACHE_TTL = 300.0  # 5 minutes
_logging_settings_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_LOGGING_SETTINGS_CACHE_TTL)


@router.get("/services/{service_id}/logging-settings", response_model=LoggingSettingsResponse)
def api_service_logging_settings(service_id: str):
    import re
    import time as _time
    import urllib.parse

    # Per-phase wall-clock for the two-three Fastly API round-trips this
    # endpoint makes. Per perf audit /api/services/{service_id}/logging-
    # settings is ~742 ms on the alerts page; section_timings tells us
    # how that splits between get_active_version / GET endpoint /
    # find_condition so the caching work targets the right call.
    timer = SectionTimer()
    section_timings = timer.entries

    cached_fields = _logging_settings_cache.get(service_id)
    if cached_fields is not None:
        return LoggingSettingsResponse.with_telemetry(
            ok=True,
            section_timings=[],
            is_cached=True,
            **cached_fields,
        )

    cfg = load_service_config(service_id)
    token = cfg.get("fastly_api_key", "")
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    try:
        from backend.core.fastly.client import fastly
        from backend.core.fastly.service import find_condition, get_active_version

        _t = _time.perf_counter()
        active_ver = get_active_version(service_id, token)
        timer.mark("get_active_version", _t)
        if not active_ver:
            raise HTTPException(status_code=400, detail={"error": "No active version found"})
        encoded_name = urllib.parse.quote(endpoint_name, safe="")
        _t = _time.perf_counter()
        ep = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/s3/{encoded_name}", token=token)
        timer.mark("get_logging_endpoint", _t)
        sample_rate = 100
        edge_only = False
        custom_condition = ""
        cond_name = ep.get("response_condition")
        if cond_name:
            _t = _time.perf_counter()
            cond = find_condition(cond_name, service_id, active_ver, token)
            timer.mark("find_condition", _t)
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

        # Cache only the business fields — telemetry (debug_queries,
        # debug_calls, section_timings, is_cached) is regenerated per
        # request so the Debug Panel keeps showing per-request data even
        # on cache hits.
        from backend.models.services import CmcdSettingsResponse

        cmcd_block = cfg.get("cmcd") or {}
        cacheable = {
            "prefix": prefix,
            "period": ep.get("period", 60),
            "sample_rate": sample_rate,
            "edge_only": edge_only,
            "custom_condition": custom_condition,
            "format_match": format_match,
            "version": active_ver,
            "cmcd": CmcdSettingsResponse(
                enabled=bool(cmcd_block.get("enabled")),
                mode=cmcd_block.get("mode"),
                version=cmcd_block.get("version"),
            ),
        }
        _logging_settings_cache[service_id] = cacheable

        return LoggingSettingsResponse.with_telemetry(
            ok=True,
            section_timings=section_timings,
            **cacheable,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(logger, e, code="logging_settings_save_failed")


from backend.models.services import LogFieldsResponse


@router.get("/services/{service_id}/log-fields", response_model=LogFieldsResponse)
def api_service_log_fields_get(service_id: str):

    from backend.core import field_registry as lf

    cfg = load_service_config(service_id)
    log_fields_config = lf.get_lf_config(cfg)
    if not log_fields_config.get("groups"):
        log_fields_config = {"groups": lf.PRESETS["standard"]["groups"], "field_overrides": {}}
    waf_warning = False
    if "J" in log_fields_config.get("groups", []):
        try:
            from backend.core import duckdb as _db

            src = _db.get_source_for_service(service_id)
            if src:
                c = _db.get_connection(source=src, read_only=True, max_wait=5.0, skip_view_update=True)
                try:
                    table_name = _db._safe_table_name(src["name"])
                    actual_cols = {col["name"] for col in _db.get_schema(c, src, stats=False)}
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
        from backend.core import metadata as metadata_db

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


@router.post(
    "/services/{service_id}/log-fields",
    response_model=LogFieldsUpdateResponse,
    response_model_exclude_unset=True,
)
def api_service_log_fields_set(request: Request, service_id: str, body: LogFieldsUpdateRequest):

    from backend import config as svcconfig
    from backend.core import field_registry as lf

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
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
    from backend.repositories._presets_cache import invalidate_presets_cache

    invalidate_presets_cache(service_id, old_hash)
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


# Security: was @router.get — moved to POST so a cross-origin
# `<img src=...>` or `<link rel=preload>` can no longer trigger a
# state-changing Fastly logging-settings update. The frontend's useSSE
# helper handles POST-with-streaming-response transparently.
# response_model intentionally omitted: SSE progress stream
# (EventSourceResponse via useSSE's POST-with-streaming), not a JSON body.
@router.post("/services/{service_id}/logging-settings/update")
def api_service_update_logging_settings(
    request: Request,
    service_id: str,
    period: int | None = Query(default=None),
    sample_rate: int | None = Query(default=None),
    prefix: str | None = Query(default=None),
    edge_only: bool | None = Query(default=None),
    custom_condition: str | None = Query(default=None),
    update_format: bool = Query(default=False),
    cmcd_enabled: bool | None = Query(default=None),
    cmcd_mode: str | None = Query(default=None),
    cmcd_version: int | None = Query(default=None),
):
    from backend import config as svcconfig

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
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
    if prefix and not re.match(r"^[A-Za-z0-9/_-]*$", prefix):
        raise HTTPException(
            status_code=400,
            detail={"error": "Invalid prefix. Use alphanumerics, /, _, -."},
        )
    token = cfg.get("fastly_api_key", "")
    endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
    prefix = prefix.strip("/")
    path = f"/{prefix}/raw/%Y-%m-%d/%H/" if prefix else "/raw/%Y-%m-%d/%H/"

    def stream():
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
                "cmcd_enabled": cmcd_enabled,
                "cmcd_mode": cmcd_mode,
                "cmcd_version": cmcd_version,
            }
            for event in update_logging_endpoint(update_cfg, token):
                if event.get("type") == "done":
                    # Reconcile the cached GET response with what Fastly
                    # just confirmed. Three paths:
                    #   changed=False → no diff applied; cached value still
                    #     matches deployed state. Leave it.
                    #   changed=True + update_format=True + known new ver →
                    #     pre-warm with the values we just deployed so the
                    #     next read (modal reopen, /alerts nav) skips the
                    #     2-3 round-trip Fastly fetch. update_format=true
                    #     deploys the target log_fields format, so
                    #     format_match holds.
                    #   anything else → pop and let the next read recompute.
                    new_ver = int(event.get("version") or 0)
                    if event.get("changed", False):
                        if update_format and new_ver:
                            from backend.models.services import CmcdSettingsResponse

                            _logging_settings_cache[service_id] = {
                                "prefix": prefix,
                                "period": period,
                                "sample_rate": sample_rate,
                                "edge_only": edge_only,
                                "custom_condition": custom_condition,
                                "format_match": True,
                                "version": new_ver,
                                "cmcd": CmcdSettingsResponse(
                                    enabled=bool(cmcd_enabled)
                                    if cmcd_enabled is not None
                                    else bool((cfg.get("cmcd") or {}).get("enabled")),
                                    mode=cmcd_mode if cmcd_mode is not None else (cfg.get("cmcd") or {}).get("mode"),
                                    version=cmcd_version
                                    if cmcd_version is not None
                                    else (cfg.get("cmcd") or {}).get("version"),
                                ),
                            }
                        else:
                            _logging_settings_cache.pop(service_id, None)
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
                            if cmcd_enabled is not None:
                                _details["cmcd_enabled"] = cmcd_enabled
                            if cmcd_mode is not None:
                                _details["cmcd_mode"] = cmcd_mode
                            if cmcd_version is not None:
                                _details["cmcd_version"] = cmcd_version
                            if update_format:
                                _details["log_fields_deployed"] = True

                            from backend.core import metadata as metadata_db

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
                yield json.dumps(event)
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


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


# response_model intentionally omitted: SSE progress stream.
@router.post("/services/{service_id}/ngwaf-sync")
def api_ngwaf_sync(service_id: str):
    """Manually trigger an NGWAF bot-sync run for a service, streamed as SSE."""
    import time

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.utils.bot_sources import build_matcher
    from backend.utils.ngwaf import fetch_verified_bots_paged
    from backend.utils.ngwaf_bot_cache import cleanup_old_bots, upsert_bots

    def stream() -> Iterator[str]:
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            yield json.dumps({"type": "error", "message": "Service not found"})
            return
        workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
        if not workspace_id:
            yield json.dumps({"type": "error", "message": "No NGWAF workspace configured for this service"})
            return
        api_key = cfg.get("fastly_api_key", "")
        if not api_key:
            yield json.dumps({"type": "error", "message": "No Fastly API key stored for this service"})
            return
        src = get_source_for_service(service_id)
        if src is None:
            yield json.dumps({"type": "error", "message": "Service source not found"})
            return
        try:
            run_id = start_cron_run(src, "ngwaf_sync")
        except RuntimeError as e:
            yield json.dumps({"type": "error", "message": str(e)})
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
            yield json.dumps({"type": "done", "message": summary})
            return
        from backend.utils.date_utils import iso_z_now

        until_ts = iso_z_now()
        yield json.dumps({"type": "status", "message": f"Scanning {from_ts} → {until_ts}..."})
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
                yield json.dumps(
                    {
                        "type": "status",
                        "message": f"Page {page_num}: {raw_count} API records, {len(enriched)} verified-bot ({total_records} total so far)...",
                    }
                )
                if time.time() - start_time >= max_runtime_secs:
                    summary = f"Synced {total_records} bot record(s) from {total_raw} API records across {page_num} page(s) — budget reached, run again to continue."
                    break
            else:
                deleted = cleanup_old_bots(retention_days)
                summary = f"Synced {total_records} bot record(s) from {total_raw} API records across {page_num} page(s), cleaned {deleted} old row(s)."
            log_cron_run(src, "ngwaf_sync", time.time() - start_time, "success", summary=summary, run_id=run_id)
            yield json.dumps({"type": "done", "message": summary})
        except Exception as e:
            log_cron_run(src, "ngwaf_sync", time.time() - start_time, "error", error_message=str(e), run_id=run_id)
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


# Same-identity re-export: these predicates moved to backend.core.field_registry
# so backend.provision.orchestrator (a layer below routers) can import them
# without creating a routers -> routers edge through provision. Kept here too
# so existing callers/tests that reference backend.routers.services.core.* keep
# working unchanged.
from backend.core.field_registry import (  # noqa: E402
    _filter_user_custom_fields,
    _is_system_field,
)
from backend.models.custom_fields import (
    CustomFieldCreate,
    CustomFieldResponse,
    CustomFieldsListResponse,
    CustomFieldUpdate,
    VclLintRequest,
    VclLintResponse,
)


@router.get("/services/{service_id}/custom-fields", response_model=CustomFieldsListResponse)
def api_list_custom_fields(request: Request, service_id: str) -> CustomFieldsListResponse:
    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
    from backend.core import field_registry as lf_module

    lf = lf_module.get_lf_config(cfg)
    # Filter out system-managed fields (CMCD, Scoring) from the custom fields list.
    # These are generated on-demand from feature toggles, not persisted in config.
    user_fields = _filter_user_custom_fields(lf.get("custom_fields", []))
    return CustomFieldsListResponse(fields=user_fields)  # type: ignore


def _check_iceberg_type_lock(
    service_id: str, field_name: str, new_duckdb_type: str | None = None, new_value_type: str | None = None
) -> None:
    """Ensure we don't mutate the core type of an existing field in the Iceberg table."""
    from fastapi import HTTPException

    from backend.core import duckdb as _db

    src = _db.get_source_for_service(service_id)
    if not src:
        return
    # Only Iceberg-backed services have a committed schema to lock against;
    # explicitly-local services don't, so there is nothing to verify. Treat a
    # missing storage_mode as the default ("cloud") so the lock still applies.
    if src.get("storage_mode", "cloud") == "local":
        return
    try:
        from pyiceberg.exceptions import NoSuchTableError

        from backend.core.iceberg import (  # type: ignore[attr-defined]
            _DUCKDB_TO_ICEBERG,
            _get_catalog,
            _table_identifier,
        )

        catalog = _get_catalog(src)
        identifier = _table_identifier(src)
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            # No committed Iceberg table yet → the field is effectively brand
            # new, nothing to lock. Safe to allow.
            return
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
    except HTTPException:
        raise
    except Exception as e:
        # We could NOT verify the field's committed type (catalog/connection
        # blip, etc.). Fail CLOSED: refuse the mutation rather than silently
        # letting a possibly type-incompatible change write a mismatched value
        # into the committed Iceberg schema. The caller can retry.
        raise HTTPException(
            status_code=503,
            detail={
                "errors": [
                    f"Could not verify the committed type for '{field_name}' "
                    f"({type(e).__name__}); the change was not applied. Please retry."
                ]
            },
        ) from e


def _locked_iceberg_field_names(src: dict | None, *, error_lead: str, error_tail: str) -> set[str]:
    """Return the committed Iceberg-schema field names for ``src``.

    Empty set for local services or when no table is committed yet. Fails
    CLOSED with a 503 (``{error_lead} ({ExcType}); {error_tail}``) on any
    catalog/connection error so a type change can't slip past an empty lock
    set and corrupt the committed schema. The two callers (update / import)
    pass their own lead/tail so their existing 503 detail strings are
    reproduced verbatim. (``_check_iceberg_type_lock`` keeps its own
    per-field type comparison rather than building on this name-set.)
    """
    if not src or src.get("storage_mode", "cloud") == "local":
        return set()
    try:
        from pyiceberg.exceptions import NoSuchTableError

        from backend.core.iceberg import (  # type: ignore[attr-defined]
            _get_catalog,
            _table_identifier,
        )

        catalog = _get_catalog(src)
        try:
            table = catalog.load_table(_table_identifier(src))
        except NoSuchTableError:
            # No committed table yet → nothing committed to lock against.
            return set()
        return {f.name for f in table.schema().fields}
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"errors": [f"{error_lead} ({type(e).__name__}); {error_tail}"]},
        ) from e


def _persist_log_fields(lf: dict, new_custom: list, candidate_lf: dict, now: str, *, full: bool = True) -> dict:
    """Build the canonical persisted ``log_fields`` dict for a custom-field write.

    ``full=True`` carries the create/update/delete shape (groups /
    field_overrides / field_limits echoed through from ``lf``); import passes
    ``full=False`` and omits those three keys exactly as it did inline. Key
    order matches the previous literals, and ``format_hash`` is computed over
    ``candidate_lf`` (the post-write field set).
    """
    from backend.core import field_registry as lf_module

    out: dict = {**lf, "custom_fields": new_custom}
    if full:
        out["groups"] = lf.get("groups", [])
        out["field_overrides"] = lf.get("field_overrides", {})
        out["field_limits"] = lf.get("field_limits", {})
    out["format_hash"] = lf_module.format_hash(candidate_lf)
    out["format_updated_at"] = now
    out["schema_version"] = 2
    return out


@router.post("/services/{service_id}/custom-fields", response_model=CustomFieldResponse)
def api_create_custom_field(request: Request, service_id: str, body: CustomFieldCreate):
    from datetime import UTC

    from backend import config as svcconfig
    from backend import provision
    from backend.core import field_registry as lf_module

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    existing_names = [cf["name"] for cf in existing]
    field_dict = body.model_dump()

    # Prevent creating system-managed fields
    if _is_system_field(field_dict.get("name", "")):
        raise HTTPException(
            status_code=422,
            detail={"errors": [f"Field '{field_dict['name']}' is system-managed and cannot be created manually."]},
        )

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
    new_lf = _persist_log_fields(lf, new_custom, candidate_lf, now)
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    warnings = [e for e in errors if e.startswith("WARN:")]

    return {"ok": True, "field": field_dict, "warnings": warnings}


@router.patch("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_update_custom_field(request: Request, service_id: str, field_name: str, body: CustomFieldUpdate):
    from datetime import UTC

    from backend import config as svcconfig
    from backend import provision
    from backend.core import duckdb as _db
    from backend.core import field_registry as lf_module

    _require_service_scope(request, service_id)

    # Prevent updating system-managed fields
    if _is_system_field(field_name):
        raise HTTPException(
            status_code=422, detail={"errors": [f"Field '{field_name}' is system-managed and cannot be edited."]}
        )

    cfg = load_service_config(service_id)
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
        # Only Iceberg-backed services have a committed schema to lock against.
        # Fail-closed 503 on catalog error lives in the helper; the 422 for an
        # already-committed field stays here so its message is unchanged.
        src = _db.get_source_for_service(service_id)
        schema_fields = _locked_iceberg_field_names(
            src,
            error_lead=f"Could not verify the committed schema for '{field_name}'",
            error_tail="the change was not applied. Please retry.",
        )
        if field_name in schema_fields:
            raise HTTPException(
                status_code=422,
                detail={
                    "errors": [
                        "Cannot change 'duckdb_type' or 'value_type' after the field has been created in the database. Please create a new field instead."
                    ]
                },
            )
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
    new_lf = _persist_log_fields(lf, new_custom, candidate_lf, now)
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    warnings = [e for e in errors if e.startswith("WARN:")]

    return {"ok": True, "field": updated, "warnings": warnings}


@router.delete("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_delete_custom_field(request: Request, service_id: str, field_name: str):
    from datetime import UTC

    from backend import config as svcconfig
    from backend.core import field_registry as lf_module

    _require_service_scope(request, service_id)

    # Prevent deleting system-managed fields
    if _is_system_field(field_name):
        raise HTTPException(
            status_code=422, detail={"errors": [f"Field '{field_name}' is system-managed and cannot be deleted."]}
        )

    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    field = next((cf for cf in existing if cf["name"] == field_name), None)
    if field is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    new_custom = [cf for cf in existing if cf["name"] != field_name]
    now = datetime.now(UTC).isoformat()
    new_lf = _persist_log_fields(lf, new_custom, {**lf, "custom_fields": new_custom}, now)
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)

    return {"ok": True, "field": field}


@router.post("/services/{service_id}/custom-fields/validate-vcl", response_model=VclLintResponse)
def api_validate_custom_vcl(request: Request, service_id: str, body: VclLintRequest):
    from backend import provision
    from backend.core import field_registry as lf_module

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
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
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        format_length=len(fmt) if fmt else None,
        format_length_limit=provision.fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX,
    )


# response_model intentionally omitted: streams a JSON file download
# (StreamingResponse with Content-Disposition), not an inline JSON body.
@router.get("/services/{service_id}/custom-fields/export")
def api_export_custom_fields(request: Request, service_id: str):
    import json

    from backend.core import field_registry as lf_module

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    # Export only user-defined custom fields, exclude system-managed fields
    user_fields = _filter_user_custom_fields(lf.get("custom_fields", []))
    return StreamingResponse(
        iter([json.dumps({"custom_fields": user_fields})]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=custom_fields_{service_id}.json"},
    )


@router.post(
    "/services/{service_id}/custom-fields/import",
    response_model=CustomFieldsImportResponse,
    response_model_exclude_unset=True,
)
def api_import_custom_fields(request: Request, service_id: str, body: CustomFieldsImportBody):
    from datetime import UTC

    from backend import config as svcconfig
    from backend import provision
    from backend.core import field_registry as lf_module

    _require_service_scope(request, service_id)

    cfg = load_service_config(service_id)
    # Pydantic guarantees ``custom_fields`` is a list (default []), so
    # the legacy ``isinstance(... , list)`` 400 check is now unreachable
    # — kept as a no-op for now to leave room for a future stricter
    # ``CustomFieldImportEntry`` model.
    fields_to_import = body.custom_fields
    lf = lf_module.get_lf_config(cfg)
    existing = lf.get("custom_fields", [])
    existing_map = {cf["name"]: cf for cf in existing}
    from backend.core import duckdb as _db

    src = _db.get_source_for_service(service_id)
    # Committed Iceberg field names to lock type changes against (fail-closed
    # 503 on catalog error lives in the helper). Empty for local/no-table.
    locked_field_names = _locked_iceberg_field_names(
        src,
        error_lead="Could not verify committed fields for import",
        error_tail="no changes were applied. Please retry.",
    )
    new_custom_map = {**existing_map}
    now = datetime.now(UTC).isoformat()
    type_lock_errors: list[str] = []
    validation_errors: list[str] = []
    for field_dict in fields_to_import:
        if "name" not in field_dict:
            continue
        fname = field_dict["name"]
        # Prevent importing system-managed fields
        if _is_system_field(fname):
            validation_errors.append(f"{fname}: cannot import system-managed field")
            continue
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
    new_lf = _persist_log_fields(lf, new_custom, candidate_lf, now, full=False)
    cfg["log_fields"] = new_lf
    svcconfig.save_config(service_id, cfg)
    return {"ok": True, "imported_count": len(fields_to_import)}


# R-1: register the two module caches so the autouse fixture in
# tests/conftest.py drains them via CacheRegistry.clear_all() instead
# of hand-clearing.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("services.core._cron_schedule_cache", _cron_schedule_cache)
_CacheRegistry.register("services.core._logging_settings_cache", _logging_settings_cache)
