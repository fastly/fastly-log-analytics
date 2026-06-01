"""Provisioning router — service list, validate, check-domain, teardown, execute."""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.utils.router_utils import SSE_HEADERS as _SSE_HEADERS
from backend.utils.router_utils import sse_flush_preamble as _sse_flush

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/provision", tags=["provision"])


def _check_domain_available(domain: str, timeout: int = 10) -> tuple[bool, str | None]:
    from backend.utils.telemetry import tracked_call

    with tracked_call("GET", domain, service="Domain Check"):
        try:
            req = urllib.request.Request(f"https://{domain}", method="GET")
            try:
                with urllib.request.urlopen(req, timeout=timeout):
                    return False, "Domain already in use (returned 200)"
            except urllib.error.HTTPError as exc:
                body = exc.read().decode(errors="replace")
                if "Please check that this domain has been added to a service." in body:
                    return True, None
                return False, "Domain already registered or in use"
            except urllib.error.URLError as exc:
                return True, f"DNS/Connection error (likely available): {exc.reason}"
        except Exception as exc:
            return False, str(exc)


from backend.models.admin import ProvisionService


@router.get("/services", response_model=list[ProvisionService])
def provision_list_services(token: str = Query(...)):
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly

    try:
        services = fastly("GET", "/service", token=token)
        existing_ids = set(svcconfig.list_service_ids())
        return [
            {"id": s["id"], "name": s["name"], "provisioned": s["id"] in existing_ids}
            for s in services
            if s.get("type", "vcl") == "vcl"
        ]
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})


@router.post("/validate")
def provision_validate(body: dict):
    from backend.core.fastly.client import fastly

    token = body.get("token")
    service_id = body.get("service_id")
    if not token or not service_id:
        raise HTTPException(status_code=400, detail={"error": "Token and Service ID are required"})

    from backend.utils.telemetry import get_tracked_calls

    try:
        # Check token info
        token_info = {}
        try:
            token_data = fastly("GET", "/tokens/self", token=token)
            token_info = {
                "id": token_data.get("id"),
                "name": token_data.get("name"),
                "user_id": token_data.get("user_id"),
                "type": "user" if token_data.get("user_id") else "automation",
            }
        except Exception as e:
            logger.warning("[provision-validate] Failed to fetch token info: %s", e)

        svc = fastly("GET", f"/service/{service_id}", token=token)
        svc_name = svc.get("name", service_id)

        safe_service_id = re.sub(r"[^a-zA-Z0-9]", "-", service_id)
        safe_service_id = re.sub(r"-+", "-", safe_service_id).strip("-")

        return {
            "service_name": svc_name,
            "token_info": token_info,
            "defaults": {
                "endpoint_name": "Fastly Object Storage Logs",
                "fos_region": "us-east-1",
                "fos_bucket_name": f"fos-{safe_service_id}-logs",
                "fos_prefix": "",
                "sample_rate": 100,
                "edge_only": True,
                "log_period": "1 minute",
                "cdn_service_name": f"Log Analysis CDN Service for {service_id}",
                "cdn_prefix": f"fos-{safe_service_id}-logs",
            },
            "_debug_calls": get_tracked_calls(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})


@router.get("/check-domain")
def provision_check_domain(prefix: str = Query(...)):
    if not prefix:
        raise HTTPException(status_code=400, detail={"error": "Prefix is required"})
    if not re.match(r"(?i)^([a-z0-9][a-z0-9-]*[a-z0-9]|[a-z0-9])$", prefix):
        return {
            "available": False,
            "reason": "Prefix must be alphanumeric and may contain hyphens (cannot start/end with hyphen)",
        }
    from backend.utils.telemetry import get_tracked_calls

    domain = f"{prefix}.global.ssl.fastly.net"
    available, reason = _check_domain_available(domain)
    result: dict = {"available": available}
    if reason:
        result["reason" if not available else "note"] = reason
    result["_debug_calls"] = get_tracked_calls()
    return result


@router.get("/check-fos")
def provision_check_fos(
    bucket: str = Query(...),
    region: str = Query(...),
    access_key: str = Query(...),
    secret_key: str = Query(...),
):
    """Validate FOS credentials by attempting to list objects."""
    import botocore.exceptions

    from backend.core.duckdb import _get_fos_client

    src = {
        "bucket": bucket,
        "endpoint": f"{region}.object.fastlystorage.app",
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "region": region,
        "storage_mode": "cloud",
    }

    from backend.utils.telemetry import get_tracked_calls

    try:
        client = _get_fos_client(src)
        # Attempt to list 1 object to verify read permissions
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return {"ok": True, "_debug_calls": get_tracked_calls()}
    except Exception as e:
        err_msg = str(e)
        if isinstance(e, botocore.exceptions.ClientError):
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                err_msg = "Access Denied. Please check your Access Key and Secret Key."
            elif code == "NoSuchBucket":
                err_msg = "Bucket not found. Please check the Bucket Name."
            elif code == "IllegalLocationConstraintException":
                err_msg = "Region mismatch. Please check the FOS Region."

        # boto3 sometimes raises EndpointConnectionError for bad regions
        if "EndpointConnectionError" in err_msg:
            err_msg = "Connection failed. Please verify the FOS Region is correct."

        return {"ok": False, "error": err_msg, "_debug_calls": get_tracked_calls()}


@router.get("/teardown")
def provision_teardown(
    token: str = Query(default=""),
    service_id: str | None = Query(default=None),
    remove_logging: bool = Query(default=True),
    remove_cdn: bool = Query(default=True),
    remove_bucket: bool = Query(default=True),
    remove_cache: bool = Query(default=True),
    remove_cron: bool = Query(default=False),
):
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.provision import _sync_crontab, perform_teardown

    state = None
    if service_id:
        svc_cfg = svcconfig.load_config(service_id)
        if svc_cfg:
            token = token or svc_cfg.get("fastly_api_key", "")
            prov = svc_cfg.get("provisioning", {})
            state = {
                "logging_service_id": service_id,
                "fos_bucket_name": svc_cfg.get("fos_bucket", ""),
                "fos_region": svc_cfg.get("fos_region", "us-east-1"),
                "fos_access_key": svc_cfg.get("fos_access_key_id", ""),
                "fos_secret_key": svc_cfg.get("fos_secret_access_key", ""),
                "fos_key_id": prov.get("fos_key_id", ""),
                "endpoint_name": prov.get("endpoint_name", "Fastly Object Storage Logs"),
                "cdn_service_id": prov.get("cdn_service_id", ""),
                "cdn_service_name": svc_cfg.get("name", service_id),
                "cdn_url": prov.get("cdn_url", ""),
                "cdn_secret": svc_cfg.get("cdn_secret", ""),
            }

    if not state:
        raise HTTPException(status_code=404, detail={"error": "No service config found."})

    opts = {
        "remove_logging": remove_logging,
        "remove_cdn": remove_cdn,
        "remove_bucket": remove_bucket,
    }

    def stream():
        def yj(data):
            yield f"data: {json.dumps(data)}\n\n"
            yield f": {' ' * 256}\n\n"

        # Initial padding to force flush
        yield from _sse_flush()

        sid = state.get("logging_service_id") or service_id

        try:
            # Stop scheduler jobs for this service immediately so no background sync
            # can write into the cache dir or attempt to fetch from FOS while we're deleting.
            if sid:
                yield from yj({"type": "status", "message": f"Stopping background sync for service {sid}..."})

                cfg_path = svcconfig.config_path(sid)
                if os.path.exists(cfg_path):
                    os.remove(cfg_path)
                    yield from yj({"type": "status", "message": f"Removed configs/{sid}.json"})

                # Sync crontab and reload scheduler to remove jobs immediately
                try:
                    _sync_crontab()
                    from backend.scheduler import get_scheduler

                    get_scheduler().reload()
                    if remove_cron:
                        yield from yj({"type": "status", "message": "Cron jobs updated"})
                except Exception as e:
                    if remove_cron:
                        yield from yj({"type": "status", "message": f"Warning: Failed to update cron jobs: {e}"})

                # Clear in-memory iceberg caches for this service.
                try:
                    from backend.core import iceberg as db_iceberg

                    db_iceberg.clear_source_caches(sid)
                except Exception:
                    pass

            yield from yj({"type": "status", "message": "Starting teardown of Fastly resources..."})
            for event in perform_teardown(state, token, opts=opts):
                yield from yj(event)
                # Small padding after each event
                yield f": {' ' * 256}\n\n"

            if remove_cache:
                import shutil

                # Remove the DuckDB file and WAL independently from the cache dir —
                # if one fails, the other still runs.
                db_path = svcconfig.duckdb_path(sid) if sid else _db.DUCKDB_PATH
                for f in [db_path, db_path + ".wal"]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                            _db.clear_initialization_state(f)
                        except Exception as e:
                            yield from yj(
                                {"type": "status", "message": f"Warning: could not remove {os.path.basename(f)}: {e}"}
                            )

                # Remove per-service cache dir (scoped by bucket name to avoid
                # wiping other services' caches).
                src_mock = {"bucket": state.get("fos_bucket_name", ""), "prefix": state.get("fos_prefix", "")}
                if src_mock["bucket"]:
                    svc_cache_dir = _db._cache_dir(src_mock)
                    if os.path.exists(svc_cache_dir):
                        try:
                            shutil.rmtree(svc_cache_dir)
                        except Exception as e:
                            yield from yj({"type": "status", "message": f"Warning: could not remove cache dir: {e}"})

                yield from yj({"type": "status", "message": "Removed local database and cache"})

            # Log teardown if the DB wasn't completely wiped
            if sid and not remove_cache:
                try:
                    from backend.core import metadata_db

                    metadata_db.record_audit(
                        service_id=sid,
                        event_type="teardown",
                        details={
                            "removed_logging": remove_logging,
                            "removed_cdn": remove_cdn,
                            "removed_bucket": remove_bucket,
                        },
                    )
                except Exception:
                    pass

            if sid:
                logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "logs", sid))
                if os.path.exists(logs_dir):
                    import shutil

                    try:
                        shutil.rmtree(logs_dir)
                        yield from yj({"type": "status", "message": "Removed logs directory"})
                    except Exception as e:
                        yield from yj({"type": "status", "message": f"Warning: could not remove logs directory: {e}"})

            _db.reload_default_source()
            yield from yj({"type": "done", "message": "Teardown complete."})
        except Exception as e:
            yield from yj({"type": "error", "message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.get("/lake-info")
def provision_lake_info(
    bucket: str = Query(...),
    region: str = Query(...),
    access_key: str = Query(...),
    secret_key: str = Query(...),
    prefix: str = Query(default=""),
    endpoint: str | None = Query(default=None),
    iceberg_metadata_location: str | None = Query(default=None),
):
    """Return Iceberg table range and calendar for a given bucket/credentials without registering it."""
    import hashlib

    from backend.models.lake import fetch_lake_info

    # Use a deterministic name to isolate catalog caches from real services.
    h = hashlib.md5(f"{bucket}:{prefix}".encode()).hexdigest()[:12]
    src = {
        "bucket": bucket,
        "region": region,
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "prefix": prefix,
        "endpoint": endpoint or f"{region}.object.fastlystorage.app",
        "name": f"_check_{h}",
        "storage_mode": "cloud",
        "iceberg_metadata_location": iceberg_metadata_location,
    }
    return fetch_lake_info(src, use_temp_cache=True)


@router.get("/execute")
def provision_execute(
    token: str = Query(...),
    service_id: str = Query(...),
    service_name: str | None = Query(default=None),
    endpoint_name: str = Query(default="Fastly Object Storage Logs"),
    fos_region: str = Query(default="us-east-1"),
    fos_bucket_name: str = Query(...),
    fos_prefix: str = Query(default=""),
    sample_rate: str = Query(default="100"),
    edge_only: bool = Query(default=True),
    custom_condition: str | None = Query(default=None),
    log_period: str = Query(default="1 minute"),
    cdn_service_name: str | None = Query(default=None),
    cdn_url: str | None = Query(default=None),
    cdn_shield: str = Query(default="none"),
    enable_cron_sync: bool = Query(default=True),
    delete_after: bool = Query(default=True),
    commit_interval_mins: int = Query(default=5),
    enable_cron_compact: bool = Query(default=True),
    log_retention_days: int = Query(default=30),
    log_fields: str | None = Query(default=None),
):
    import secrets

    from backend.core import duckdb as _db
    from backend.provision import parse_period
    from backend.utils.pop_utils import fetch_pop_locations

    fetch_pop_locations(token)

    if not service_name:
        from backend import config as svcconfig

        service_name = svcconfig.fetch_service_name(service_id, token) or service_id

    cfg = {
        "admin_token": token,
        "logging_service_id": service_id,
        "name": service_name,
        "endpoint_name": endpoint_name,
        "fos_region": fos_region,
        "fos_bucket_name": fos_bucket_name,
        "fos_prefix": fos_prefix,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "custom_condition": custom_condition,
        "log_period": log_period,
        "cdn_service_name": cdn_service_name,
        "cdn_url": cdn_url,
        "cdn_shield": cdn_shield,
        "enable_cron_sync": enable_cron_sync,
        "delete_after": delete_after,
        "commit_interval_mins": commit_interval_mins,
        "enable_cron_compact": enable_cron_compact,
        "log_retention_days": log_retention_days,
    }

    if log_fields:
        try:
            cfg["log_fields"] = json.loads(log_fields)
        except Exception:
            pass

    try:
        cfg["log_period"] = parse_period(cfg["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})

    bucket = cfg["fos_bucket_name"]
    if not re.match(r"^[A-Za-z0-9](?!.*--)[A-Za-z0-9-]{1,61}[A-Za-z0-9]$", bucket):
        raise HTTPException(
            status_code=400,
            detail={"error": f"Invalid bucket name: '{bucket}'. Use 3-63 alphanumeric characters or single hyphens."},
        )

    if cfg.get("cdn_url"):
        domain = cfg["cdn_url"].replace("https://", "")
        available, reason = _check_domain_available(domain, timeout=5)
        if not available and reason and "DNS" not in reason:
            raise HTTPException(status_code=400, detail={"error": f"Domain {domain} is unavailable: {reason}."})

    cfg["cdn_secret"] = secrets.token_urlsafe(24)

    def stream():
        # Initial padding to force flush
        yield from _sse_flush()

        error_already_emitted = False
        try:
            from backend.provision import provision

            for event in provision(cfg):
                if event.get("type") == "error":
                    error_already_emitted = True
                yield f"data: {json.dumps(event)}\n\n"
                # Small padding after each event
                yield f": {' ' * 256}\n\n"

                if event.get("type") == "done":
                    _db.reload_default_source()

                    try:
                        from backend.core import metadata_db

                        metadata_db.record_audit(
                            service_id=cfg["logging_service_id"],
                            event_type="provision",
                            details={
                                "bucket": cfg.get("fos_bucket_name"),
                                "prefix": cfg.get("fos_prefix"),
                                "region": cfg.get("fos_region"),
                                "sample_rate": cfg.get("sample_rate"),
                                "cdn_url": cfg.get("cdn_url"),
                                "cdn_shield": cfg.get("cdn_shield"),
                                "edge_only": cfg.get("edge_only"),
                                "log_period": cfg.get("log_period"),
                                "enable_cron_sync": cfg.get("enable_cron_sync"),
                                "delete_after": cfg.get("delete_after"),
                                "commit_interval_mins": cfg.get("commit_interval_mins"),
                                "enable_cron_compact": cfg.get("enable_cron_compact"),
                                "log_retention_days": cfg.get("log_retention_days"),
                                "log_fields": cfg.get("log_fields"),
                            },
                        )
                    except Exception:
                        pass

                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()

                        # Trigger an initial metadata sync
                        try:
                            from backend.scheduler import _run_metadata_sync

                            _run_metadata_sync(cfg["logging_service_id"])
                        except Exception as e:
                            logger.warning("[provision] Initial metadata sync after provision failed: %s", e)
                        except Exception:
                            pass
                    except Exception:
                        pass
        except Exception as e:
            if not error_already_emitted:
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/terraform/preview")
def provision_terraform_preview(body: dict):
    from backend.utils.terraform_gen import generate_terraform

    access_key = body.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = body.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    return generate_terraform(body, access_key, secret_key)


@router.post("/terraform/export")
def provision_terraform_export(body: dict):
    import io
    import zipfile

    from fastapi.responses import StreamingResponse

    from backend.utils.terraform_gen import generate_terraform

    access_key = body.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = body.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    tf_files = generate_terraform(body, access_key, secret_key)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, file_content in tf_files.items():
            if file_name == "instructions":
                zip_file.writestr("README.md", file_content)
            else:
                zip_file.writestr(file_name, file_content)

    zip_buffer.seek(0)

    headers = {"Content-Disposition": 'attachment; filename="fastly-log-analysis-terraform.zip"'}
    return StreamingResponse(zip_buffer, media_type="application/zip", headers=headers)


@router.post("/ingest")
def provision_ingest(body: dict):
    import secrets

    from backend.provision import ensure_fos_access_key, find_fos_key, parse_period, write_service_config
    from backend.utils.pop_utils import fetch_pop_locations

    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail={"error": "Token is required"})

    fetch_pop_locations(token)

    try:
        if body.get("log_period"):
            body["log_period"] = parse_period(body["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})

    # We skip bucket and service creation, and only ensure we have an access key
    # if one wasn't provided directly (though they really should be providing them or we generate one for the bucket)

    fos_access_key = body.get("fos_access_key")
    fos_secret_key = body.get("fos_secret_key")
    fos_key_id = ""

    if not fos_access_key or not fos_secret_key:
        # Try to find or create one
        desc = f"fos-log-analysis-{body.get('fos_bucket_name')}"
        existing = find_fos_key(desc, token)
        if existing:
            fos_access_key = existing["access_key"]
            fos_secret_key = existing["secret_key"]
            fos_key_id = existing["access_key"]
        else:
            try:
                new_key = ensure_fos_access_key(desc, body, token, buckets=[body.get("fos_bucket_name")])
                fos_access_key = new_key["access_key"]
                fos_secret_key = new_key["secret_key"]
                fos_key_id = new_key["id"]
            except Exception as e:
                raise HTTPException(status_code=400, detail={"error": f"Failed to ensure access key: {e}"})

    if not body.get("cdn_secret"):
        body["cdn_secret"] = secrets.token_urlsafe(24)

    # Store state just like execute does
    state = {
        "admin_token": token,
        "logging_service_id": body.get("service_id") or body.get("logging_service_id"),
        "name": body.get("service_name") or body.get("service_id"),
        "endpoint_name": body.get("endpoint_name", "Fastly Object Storage Logs"),
        "fos_region": body.get("fos_region", "us-east-1"),
        "fos_bucket_name": body.get("fos_bucket_name"),
        "fos_prefix": body.get("fos_prefix", ""),
        "fos_access_key_id": fos_access_key,
        "fos_secret_access_key": fos_secret_key,
        "sample_rate": body.get("sample_rate", "100"),
        "edge_only": body.get("edge_only", True),
        "custom_condition": body.get("custom_condition", ""),
        "log_period": body.get("log_period", 60),
        "cdn_service_name": body.get("cdn_service_name"),
        "cdn_url": body.get("cdn_url"),
        "cdn_shield": body.get("cdn_shield", "none"),
        "cdn_secret": body.get("cdn_secret"),
        "enable_cron_sync": body.get("enable_cron_sync", True),
        "delete_after": body.get("delete_after", True),
        "commit_interval_mins": body.get("commit_interval_mins", 5),
        "enable_cron_compact": body.get("enable_cron_compact", True),
        "log_retention_days": body.get("log_retention_days", 30),
        "provisioning": {"fos_key_id": fos_key_id},
    }

    if body.get("log_fields"):
        try:
            state["log_fields"] = (
                json.loads(body.get("log_fields"))
                if isinstance(body.get("log_fields"), str)
                else body.get("log_fields")
            )
        except Exception:
            pass

    write_service_config(state)

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        pass

    return {"ok": True, "service_id": state["logging_service_id"]}


@router.get("/check-config")
def provision_check_config(
    token: str = Query(...),
    service_id: str = Query(...),
    cdn_service_id: str = Query(...),
    bucket: str = Query(...),
):
    """Verify that both services are correctly configured for log analysis."""
    from backend.core.fastly.client import fastly

    results = {
        "logging_service": {"ok": False, "details": ""},
        "cdn_service": {"ok": False, "details": ""},
    }

    try:
        # 1. Check Logging Service
        try:
            active_ver_data = fastly("GET", f"/service/{service_id}/version/active", token=token)
            active_ver = active_ver_data.get("number")

            endpoints = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/s3", token=token)
            endpoint = next((ep for ep in endpoints if ep.get("bucket_name") == bucket), None)

            if not endpoint:
                results["logging_service"]["details"] = f"No S3 logging endpoint found pointing to bucket '{bucket}'"
            else:
                # Check for snippets
                snippets = fastly("GET", f"/service/{service_id}/version/{active_ver}/snippet", token=token)
                snip_names = {s.get("name") for s in snippets}

                # Core required snippets
                required_snippets = {
                    "Fastly Log Analysis Capture",
                    "Fastly Log Analysis Miss",
                    "Fastly Log Analysis Pass",
                }

                # Optional but highly recommended snippets (for Group L / Origin metrics)
                optional_snippets = {
                    "Fastly Log Analysis Origin Fetch",
                    "Fastly Log Analysis Origin Error",
                    "Fastly Log Analysis Origin Deliver",
                }

                missing_required = required_snippets - snip_names
                found_optional = optional_snippets & snip_names

                if missing_required:
                    results["logging_service"]["details"] = (
                        f"Found endpoint '{endpoint.get('name')}', but missing CORE snippets: {', '.join(missing_required)}"
                    )
                else:
                    msg = "Service has endpoint and core snippets"
                    if found_optional:
                        msg += f" (plus {len(found_optional)} origin metric snippets)"

                    # Check for condition
                    has_condition = endpoint.get("response_condition") is not None
                    if not has_condition:
                        results["logging_service"]["details"] = (
                            f"Warning: {msg}, but no response condition (sampling might be disabled)"
                        )
                        results["logging_service"]["ok"] = True
                    else:
                        results["logging_service"] = {
                            "ok": True,
                            "details": f"{msg} and active sampling condition",
                        }

        except Exception as e:
            results["logging_service"]["details"] = f"Error checking logging service: {e}"

        # 2. Check CDN Service
        try:
            # Check for required dictionaries
            dicts = fastly("GET", f"/service/{cdn_service_id}/version/active/dictionary", token=token)
            dict_names = {d.get("name") for d in dicts}
            missing_dicts = {"fos_credentials", "cdn_auth"} - dict_names

            if missing_dicts:
                results["cdn_service"]["details"] = f"Missing required dictionaries: {', '.join(missing_dicts)}"
            else:
                # Check for origin pointing to FOS
                backends = fastly("GET", f"/service/{cdn_service_id}/version/active/backend", token=token)
                fos_found = any(".fastlystorage.app" in b.get("address", "") for b in backends)

                if not fos_found:
                    results["cdn_service"]["details"] = (
                        "No backend found pointing to Fastly Object Storage (*.fastlystorage.app)"
                    )
                else:
                    # Check for CDN snippets
                    active_ver_cdn = fastly("GET", f"/service/{cdn_service_id}/version/active", token=token).get(
                        "number"
                    )
                    snippets_cdn = fastly(
                        "GET", f"/service/{cdn_service_id}/version/{active_ver_cdn}/snippet", token=token
                    )
                    snip_names_cdn = {s.get("name") for s in snippets_cdn}

                    required_cdn = {
                        "iceberg-metadata-pointer-ttl",
                        "cdn-swr-shield-disable",
                        "cdn-race-condition-generation",
                        "cdn-no-cache-404",
                    }
                    missing_cdn = required_cdn - snip_names_cdn

                    if missing_cdn:
                        results["cdn_service"]["details"] = (
                            f"Dictionaries/Backends OK, but missing snippets: {', '.join(missing_cdn)}"
                        )
                    else:
                        results["cdn_service"] = {
                            "ok": True,
                            "details": "Service fully configured with dictionaries, backends, and performance snippets",
                        }

        except Exception as e:
            results["cdn_service"]["details"] = f"Error checking CDN service: {e}"

    except Exception as e:
        logger.error("[provision-check-config] Global error: %s", e)

    return results


@router.get("/ngwaf-workspaces")
def provision_ngwaf_workspaces(service_id: str = Query(...), token: str = Query(default="")):
    """List NGWAF workspaces using the provided token or the stored API key."""
    import urllib.error

    from backend import config as svcconfig
    from backend.provision import fastly

    token = token.strip()
    if not token:
        token = svcconfig.get_fastly_api_key(service_id)
    if not token:
        raise HTTPException(status_code=400, detail={"error": "No API key stored for this service."})

    from backend.utils.router_utils import format_debug_request

    _ngwaf_url = "https://api.fastly.com/ngwaf/v1/workspaces"
    _req_headers = {"Fastly-Key": token, "Accept": "application/json"}

    try:
        req = urllib.request.Request(_ngwaf_url, headers=_req_headers)
        resp_status = None
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw_body = resp.read().decode()
            resp_status = resp.status
        data = json.loads(raw_body)
        logger.debug(
            "[ngwaf-workspaces] keys=%s raw_body_preview=%s",
            list(data.keys()) if isinstance(data, dict) else type(data),
            raw_body[:300],
        )
        # Handle both {"data": [...]} and {"workspaces": [...]} response shapes.
        # Use key-presence check to avoid falsy-empty-list bug.
        raw_list = data.get("data") if "data" in data else data.get("workspaces", [])
        logger.debug("[ngwaf-workspaces] raw_list length=%d", len(raw_list))
        workspaces = [
            {
                "id": w.get("id"),
                "name": w.get("name") or (w.get("attributes") or {}).get("name") or w.get("id"),
            }
            for w in raw_list
        ]
        result: dict = {"workspaces": workspaces}
        if not workspaces:
            # Check if this is an automation token, which cannot see NGWAF
            try:
                token_data = fastly("GET", "/tokens/self", token=token)
                if not token_data.get("user_id"):
                    result["error_hint"] = (
                        "You are using an Automation Token (Service Account). "
                        "Fastly Automation Tokens do not have access to Next-Gen WAF (NGWAF) data. "
                        "To use NGWAF features, please use a Personal API Token from a human user with Security permissions."
                    )
            except Exception:
                pass
            req_debug = format_debug_request("GET", _ngwaf_url, _req_headers)
            result["_debug_raw"] = f"{req_debug}\n\n--- Response ({resp_status}) ---\n{raw_body[:500]}"
        return result
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode()
        except Exception:
            pass
        logger.warning("[ngwaf-workspaces] HTTPError %s body=%s", exc.code, body[:300])
        if exc.code == 401:
            raise HTTPException(
                status_code=400, detail={"error": "Invalid API token or missing Edge Security permissions."}
            )
        raise HTTPException(status_code=400, detail={"error": f"NGWAF API error: {exc.code} — {body[:300]}"})
    except Exception as e:
        logger.warning("[ngwaf-workspaces] exception: %s", e)
        raise HTTPException(status_code=400, detail={"error": str(e)})


@router.patch("/services/{service_id}/ngwaf-workspace")
def provision_set_ngwaf_workspace(service_id: str, body: dict):
    """Persist the NGWAF workspace ID for a service and reload the scheduler."""
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})

    workspace_id = (body.get("ngwaf_workspace_id") or "").strip() or None
    cfg["ngwaf_workspace_id"] = workspace_id
    svcconfig.save_config(service_id, cfg)

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        pass

    return {"ok": True, "ngwaf_workspace_id": workspace_id}
