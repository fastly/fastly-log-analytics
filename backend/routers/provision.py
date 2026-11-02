"""Provisioning router — validate, execute, teardown, check-fos, terraform, ngwaf."""

from __future__ import annotations

import datetime as _dt
import io
import json
import logging
import os
import re
import secrets
import shutil
import time
import urllib.error
import urllib.request
import zipfile
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from backend.core.fastly.client import validate_rum_custom_condition
from backend.models.admin import ProvisionService
from backend.models.common import (
    DEFAULT_ERROR_RESPONSES,
    CheckFosRequest,
    NgwafWorkspaceCreateRequest,
    NgwafWorkspaceSetResponse,
    NgwafWorkspacesResponse,
    ProvisionCheckConfigResponse,
    ProvisionCheckDomainResponse,
    ProvisionCheckFosResponse,
    ProvisionConfigRequest,
    ProvisionExecuteRequest,
    ProvisionIngestResponse,
    ProvisionReconcileRequest,
    ProvisionTeardownRequest,
    ProvisionValidateRequest,
    ProvisionValidateResponse,
    TerraformPreviewResponse,
)
from backend.provision.fos_setup import _get_fos_s3_client
from backend.utils.router_utils import (
    make_error,
    raise_internal,
    require_json_content_type,
    require_service_id,
)
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/provision", tags=["provision"], responses=DEFAULT_ERROR_RESPONSES)
provision_router = router

_FASTLY_HTTP_ERROR_RE = re.compile(r"HTTP (\d+) ")


def _sse(type_: str, message: str | None = None, **extra: object) -> str:
    payload: dict[str, object] = {"type": type_}
    if message is not None:
        payload["message"] = message
    payload.update(extra)
    return json.dumps(payload)


def _invalidate_service_credentials(service_id: str) -> None:
    """Drop in-process FOS client + pooled DuckDB S3 SECRET caches for ``service_id``."""
    try:
        from backend.core import duckdb as _ddb
        from backend.core import duckdb_pool as _ddb_pool

        _ddb.clear_fos_client(service_id)
        _ddb_pool.reset_pool_for_service(service_id)
    except Exception:
        logger.warning("[provision] credential cache invalidation failed for %s", service_id, exc_info=True)


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


@provision_router.get("/services", response_model=list[ProvisionService])
def provision_list_services(token: str = Query(...)):
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly
    from backend.provision.fos_setup import object_storage_enabled

    existing_ids = set(svcconfig.list_service_ids())
    try:
        services = fastly("GET", "/service", token=token)
    except Exception as e:
        raise_internal(logger, e, code="list_services_failed", status=400)

    if not object_storage_enabled(token):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "object_storage_not_enabled",
                "Object Storage isn't enabled on this Fastly account, and it's "
                "required to store logs. Enable the Object Storage product for your "
                "account (in the Fastly control panel, or ask your Fastly account "
                "team), then click Fetch Services again.",
            ),
        )

    return [
        {"id": s["id"], "name": s["name"], "provisioned": s["id"] in existing_ids}
        for s in services
        if s.get("type", "vcl") == "vcl"
    ]


@provision_router.post("/validate", response_model=ProvisionValidateResponse, response_model_exclude_unset=True)
def provision_validate(body: ProvisionValidateRequest):
    from backend.core.fastly.client import fastly
    from backend.utils.telemetry import debug_envelope

    token, service_id = body.token, body.service_id
    if not token or not service_id:
        raise HTTPException(status_code=400, detail={"error": "Token and Service ID are required"})

    try:
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
        safe_service_id = re.sub(r"-+", "-", re.sub(r"[^a-zA-Z0-9]", "-", service_id)).strip("-")
        return debug_envelope(
            {
                "service_name": svc.get("name", service_id),
                "token_info": token_info,
                "defaults": {
                    "endpoint_name": "Fastly Object Storage Logs",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": f"fos-{safe_service_id.lower()}-logs",
                    "fos_prefix": "",
                    "sample_rate": 100,
                    "edge_only": True,
                    "log_period": "1 minute",
                    "cdn_service_name": f"Log Analysis CDN Service for {service_id}",
                    "cdn_prefix": f"fos-{safe_service_id}-logs",
                },
            },
            gated=True,
            is_cached=None,
            inject_calls=True,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(logger, e, code="provision_validate_failed", status=400)


@provision_router.get("/check-domain", response_model=ProvisionCheckDomainResponse, response_model_exclude_unset=True)
def provision_check_domain(prefix: str = Query(...), is_custom: bool = Query(False)):
    if not prefix:
        raise HTTPException(status_code=400, detail={"error": "Prefix is required"})
    if is_custom:
        domain = prefix.strip()
        if not domain or "." not in domain or not re.fullmatch(r"^[a-zA-Z0-9.-]+$", domain):
            return {
                "available": False,
                "reason": "Invalid custom domain. Must be non-empty, contain at least one dot, and consist only of letters, digits, hyphens, and dots.",
            }
    else:
        if not re.fullmatch(r"(?i)^([a-z0-9][a-z0-9-]*[a-z0-9]|[a-z0-9])$", prefix):
            return {
                "available": False,
                "reason": "Prefix must be alphanumeric and may contain hyphens (cannot start/end with hyphen)",
            }
        domain = f"{prefix}.global.ssl.fastly.net"

    from backend.utils.telemetry import debug_envelope

    available, reason = _check_domain_available(domain)
    result: dict[str, Any] = {"available": available}
    if reason:
        result["reason" if not available else "note"] = reason
    return debug_envelope(result, gated=True, is_cached=None, inject_calls=True)


@provision_router.post("/check-fos", response_model=ProvisionCheckFosResponse, response_model_exclude_unset=True)
def provision_check_fos(req: CheckFosRequest):
    """Validate FOS credentials against the same raw prefixes ingest reads."""
    import botocore.exceptions

    from backend.utils.telemetry import debug_envelope

    bucket, region, endpoint = req.bucket, req.region, f"{req.region}.object.fastlystorage.app"
    try:
        client = _get_fos_s3_client(
            req.access_key,
            req.secret_key,
            region,
            bucket_name=bucket,
            context="provision:check-fos",
        )
        representative: tuple[str, str] | None = None
        for prefix in ("raw/request/", "raw/rum/"):
            page = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            contents = page.get("Contents") or []
            if contents:
                key = str(contents[0]["Key"])
                client.get_object(Bucket=bucket, Key=key, Range="bytes=0-0")["Body"].close()
                representative = (prefix, key)
                break
        prefix, key = representative or ("raw/request/", "")
        logger.info(
            "[provision] FOS credential probe succeeded bucket=%s region=%s endpoint=%s prefix=%s key=%s",
            bucket,
            region,
            endpoint,
            prefix,
            key or "<none>",
        )
        return debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)
    except Exception as e:
        err_msg = str(e)
        logger.warning(
            "[provision] FOS credential probe failed bucket=%s region=%s endpoint=%s: %s",
            bucket,
            region,
            endpoint,
            err_msg[:200],
        )
        if isinstance(e, botocore.exceptions.ClientError):
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                err_msg = "Access Denied. Please check your Access Key and Secret Key."
            elif code == "NoSuchBucket":
                err_msg = "Bucket not found. Please check the Bucket Name."
            elif code == "IllegalLocationConstraintException":
                err_msg = "Region mismatch. Please check the FOS Region."
        if "EndpointConnectionError" in err_msg:
            err_msg = "Connection failed. Please verify the FOS Region is correct."
        return debug_envelope({"ok": False, "error": err_msg}, gated=True, is_cached=None, inject_calls=True)


@provision_router.post("/teardown", dependencies=[Depends(require_json_content_type)])
def provision_teardown(req: Request, body: ProvisionTeardownRequest | None = None):
    """Destructive service teardown over SSE."""
    body = body or ProvisionTeardownRequest()
    token, service_id = body.token, body.service_id
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core.vcl_generator import validate_destructive_token
    from backend.provision import _sync_crontab, perform_teardown

    state = None
    svc_cfg = svcconfig.load_config(service_id) if service_id else None
    if service_id and svc_cfg:
        prov = svc_cfg.get("provisioning", {})
        state = {
            "logging_service_id": service_id,
            "fos_bucket_name": svc_cfg.get("fos_bucket", ""),
            "fos_region": svc_cfg.get("fos_region", "us-east-1"),
            "fos_prefix": svc_cfg.get("fos_prefix", ""),
            "fos_access_key": svc_cfg.get("fos_access_key_id", ""),
            "fos_secret_key": svc_cfg.get("fos_secret_access_key", ""),
            "endpoint_name": prov.get("endpoint_name", "Fastly Object Storage Logs"),
            "cdn_service_id": prov.get("cdn_service_id", ""),
            "cdn_service_name": prov.get("cdn_service_name") or f"Log Analysis CDN Service for {service_id}",
            "cdn_url": prov.get("cdn_url", ""),
            "cdn_secret": svc_cfg.get("cdn_secret", ""),
            "scoring": svc_cfg.get("scoring") or {},
        }

    if not state:
        raise HTTPException(status_code=404, detail={"error": "No service config found."})

    validate_destructive_token(token, service_id=service_id or "")
    opts = {
        "remove_logging": body.remove_logging,
        "remove_rum": body.remove_rum,
        "remove_cdn": body.remove_cdn,
        "remove_bucket": body.remove_bucket,
        "remove_cloud_files": body.remove_cloud_files,
        "remove_scoring": body.remove_scoring,
        "remove_fos_tokens": body.remove_fos_tokens,
    }

    def stream():
        sid = state.get("logging_service_id") or service_id
        try:
            if sid:
                yield _sse("status", f"Stopping background sync for service {sid}...")
                cfg_path = svcconfig.config_path(sid)
                if os.path.exists(cfg_path):
                    os.remove(cfg_path)
                    from backend.utils.bounded_cache import CacheRegistry

                    CacheRegistry.clear("routers.bootstrap._bootstrap_cache")
                    yield _sse("status", f"Removed configs/{sid}.json")

                try:
                    _sync_crontab()
                    from backend.cron.scheduler import get_scheduler

                    get_scheduler().reload()
                    if body.remove_cron:
                        yield _sse("status", "Cron jobs updated")
                except Exception as e:
                    if body.remove_cron:
                        yield _sse("status", f"Warning: Failed to update cron jobs: {e}")

                try:
                    from backend.core import storage as db_storage

                    _cache_src = svcconfig.config_to_source(svc_cfg) if svc_cfg else {"name": sid}
                    db_storage.invalidate_service_caches(_cache_src)
                except Exception:
                    logger.debug("stream: db_storage.invalidate_service_caches failed (ignored)", exc_info=True)

                _invalidate_service_credentials(sid)

            yield _sse("status", "Starting teardown of Fastly resources...")
            for event in perform_teardown(state, token, opts=opts):
                yield json.dumps(event)

            if body.remove_cache:
                db_path = svcconfig.duckdb_path(sid) if sid else _db.DUCKDB_PATH
                for f in [db_path, db_path + ".wal"]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                            _db.clear_initialization_state(f)
                        except Exception as e:
                            yield _sse("status", f"Warning: could not remove {os.path.basename(f)}: {e}")

                src_mock = {"bucket": state.get("fos_bucket_name", ""), "prefix": state.get("fos_prefix", "")}
                if src_mock["bucket"]:
                    svc_cache_dir = _db._cache_dir(src_mock)
                    if os.path.exists(svc_cache_dir):
                        try:
                            for attempt in Retrying(
                                stop=stop_after_attempt(6),
                                wait=wait_fixed(0.5),
                                retry=retry_if_exception_type(OSError),
                                sleep=lambda s: time.sleep(s),
                                reraise=True,
                            ):
                                with attempt:
                                    try:
                                        shutil.rmtree(svc_cache_dir)
                                    except FileNotFoundError:
                                        pass
                        except OSError as rmtree_err:
                            yield _sse("status", f"Warning: could not remove cache dir: {rmtree_err}")

                if sid:
                    try:
                        from backend.core import metadata as metadata_db

                        metadata_db.teardown(sid)
                    except Exception as e:
                        yield _sse("status", f"Warning: could not clear metadata: {e}")

                yield _sse("status", "Removed local database and cache")

            if sid and not body.remove_cache:
                try:
                    from backend.core import metadata as metadata_db

                    metadata_db.record_audit(
                        service_id=sid,
                        event_type="teardown",
                        details={
                            "removed_logging": body.remove_logging,
                            "removed_cdn": body.remove_cdn,
                            "removed_bucket": body.remove_bucket,
                        },
                    )
                except Exception:
                    logger.warning("stream: metadata_db.record_audit failed (ignored)", exc_info=True)

            if sid:
                logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "logs", sid))
                if os.path.exists(logs_dir):
                    try:
                        shutil.rmtree(logs_dir)
                        yield _sse("status", "Removed logs directory")
                    except Exception as e:
                        yield _sse("status", f"Warning: could not remove logs directory: {e}")

            _db.reload_default_source()
            yield _sse("done", "Teardown complete.")
        except Exception as e:
            yield _sse("error", str(e))

    return sse_response(stream())


@provision_router.post("/execute")
def provision_execute(req: ProvisionExecuteRequest):
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.provision import parse_period
    from backend.utils.pop_utils import fetch_pop_locations

    fetch_pop_locations(req.token)
    service_name = req.service_name or svcconfig.fetch_service_name(req.service_id, req.token) or req.service_id

    cfg: dict[str, Any] = {
        "admin_token": req.token,
        "logging_service_id": req.service_id,
        "name": service_name,
        "endpoint_name": req.endpoint_name,
        "fos_region": req.fos_region,
        "fos_bucket_name": req.fos_bucket_name,
        "fos_prefix": req.fos_prefix,
        "sample_rate": req.sample_rate,
        "edge_only": req.edge_only,
        "custom_condition": req.custom_condition,
        "log_period": req.log_period,
        "cdn_service_name": req.cdn_service_name,
        "cdn_url": req.cdn_url,
        "cdn_shield": req.cdn_shield,
        "enable_cron_sync": req.enable_cron_sync,
        "delete_after": req.delete_after,
        "commit_interval_mins": req.commit_interval_mins,
        "enable_cron_compact": req.enable_cron_compact,
        "log_retention_days": req.log_retention_days,
        "rum_retention_days": req.rum_retention_days,
        "logging_enabled": req.logging_enabled,
        "rum_enabled": req.rum_enabled,
    }

    if req.log_fields:
        try:
            cfg["log_fields"] = json.loads(req.log_fields)
        except Exception:
            logger.debug("provision_execute: step failed (ignored)", exc_info=True)

    if req.cmcd_enabled:
        cfg["cmcd"] = {
            "enabled": True,
            "mode": req.cmcd_mode or "query_string",
            "version": req.cmcd_version if req.cmcd_version is not None else 1,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
        }

    if getattr(req, "rum_enabled", False):
        from backend.core.faro_versions import DEFAULT_FARO_VERSION

        rum_cond = (getattr(req, "rum_custom_condition", None) or "").strip()
        cond_err = validate_rum_custom_condition(rum_cond)
        if cond_err:
            raise HTTPException(status_code=400, detail=make_error("invalid_rum_condition", cond_err))
        cfg["rum"] = {
            "enabled": True,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            "faro_version": req.faro_version or DEFAULT_FARO_VERSION,
            "custom_condition": rum_cond,
        }
        if getattr(req, "rum_log_period", None) is not None:
            try:
                rum_period_val = parse_period(str(req.rum_log_period))
                cfg["rum"]["log_period"] = rum_period_val
                cfg["rum_log_period"] = rum_period_val
            except ValueError as e:
                raise HTTPException(status_code=400, detail=make_error("invalid_rum_log_period", str(e)))

    try:
        cfg["log_period"] = parse_period(cfg["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))

    bucket = cfg["fos_bucket_name"]
    if not re.fullmatch(r"^[A-Za-z0-9](?!.*--)[A-Za-z0-9-]{1,61}[A-Za-z0-9]$", bucket):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_bucket",
                f"Invalid bucket name: '{bucket}'. Use 3-63 alphanumeric characters or single hyphens.",
            ),
        )

    prefix = cfg.get("fos_prefix", "")
    if prefix and not re.fullmatch(r"^[A-Za-z0-9/_-]*$", prefix):
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_prefix", "Invalid prefix. Use alphanumerics, /, _, -."),
        )

    if cfg.get("cdn_url"):
        domain = cfg["cdn_url"].replace("https://", "")
        available, reason = _check_domain_available(domain, timeout=5)
        if not available and reason and "DNS" not in reason:
            raise HTTPException(
                status_code=400,
                detail=make_error("domain_unavailable", reason, domain=domain),
            )

    cfg["cdn_secret"] = secrets.token_urlsafe(24)

    def stream():
        error_already_emitted = False
        try:
            from backend.provision import provision

            for event in provision(cfg):
                if event.get("type") == "error":
                    error_already_emitted = True
                yield json.dumps(event)

                if event.get("type") == "done":
                    _db.reload_default_source()
                    from backend.utils.bounded_cache import CacheRegistry

                    CacheRegistry.clear("routers.bootstrap._bootstrap_cache")
                    _invalidate_service_credentials(cfg.get("logging_service_id") or cfg.get("service_id") or "")

                    try:
                        from backend.core import metadata as metadata_db

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
                        logger.warning("stream: metadata_db.record_audit failed (ignored)", exc_info=True)

                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()
                    except Exception:
                        logger.debug("stream: step failed (ignored)", exc_info=True)
        except Exception as e:
            if not error_already_emitted:
                yield _sse("error", str(e))

    return sse_response(stream())


@provision_router.post("/reconcile")
def provision_reconcile(body: ProvisionReconcileRequest):
    """Reconcile VCL state for a service and stream status via SSE."""
    if not body.service_id or not body.token:
        raise HTTPException(status_code=400, detail=make_error("service_id and token are required"))

    from backend.provision.declarative.reconciler import reconcile_vcl_state
    from backend.utils.sse import stream_sse_from_worker

    def worker(events):
        try:
            result = reconcile_vcl_state(
                service_id=body.service_id,
                token=body.token,
                dry_run=False,
                status_cb=lambda msg: events.put({"type": "status", "message": msg}),
            )
            events.put(
                {
                    "type": "reconciliation_result",
                    "service_id": result.service_id,
                    "activated_version": result.activated_version,
                    "changes_applied": result.changes_applied,
                    "duration_ms": result.duration_ms,
                    "error": result.error,
                }
            )
            events.put({"type": "done", "success": result.error is None})
        except Exception as e:
            events.put({"type": "error", "message": str(e)})

    return stream_sse_from_worker(worker)


def _render_terraform(body: ProvisionConfigRequest) -> dict[str, str]:
    from backend.utils.terraform_gen import generate_terraform

    cfg = body.model_dump(exclude_none=True)
    access_key = cfg.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = cfg.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    return generate_terraform(cfg, access_key, secret_key)


@provision_router.post("/terraform/preview", response_model=TerraformPreviewResponse)
def provision_terraform_preview(body: ProvisionConfigRequest):
    return _render_terraform(body)


@provision_router.post(
    "/terraform/export",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "ZIP archive of the generated Terraform files (Content-Disposition: attachment).",
            "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}},
        }
    },
)
def provision_terraform_export(body: ProvisionConfigRequest):
    tf_files = _render_terraform(body)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, file_content in tf_files.items():
            zip_file.writestr("README.md" if file_name == "instructions" else file_name, file_content)
    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="fastly-log-analysis-terraform.zip"'},
    )


@provision_router.post("/ingest", response_model=ProvisionIngestResponse, response_model_exclude_unset=True)
def provision_ingest(payload: ProvisionConfigRequest):
    from backend import config as svcconfig
    from backend.core.vcl_generator import validate_destructive_token
    from backend.provision import ensure_fos_access_key, find_fos_key, parse_period, write_service_config
    from backend.utils.pop_utils import fetch_pop_locations

    body: dict[str, Any] = payload.model_dump(exclude_none=False)
    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail={"error": "Token is required"})

    logging_service_id = body.get("service_id") or body.get("logging_service_id") or ""
    validate_destructive_token(token, service_id=logging_service_id)
    fetch_pop_locations(token)

    existing_cfg = svcconfig.load_config(logging_service_id) or {}
    existing_fos_proxy = existing_cfg.get("fos_proxy")
    if not isinstance(existing_fos_proxy, dict):
        existing_fos_proxy = {}

    try:
        if body.get("log_period"):
            body["log_period"] = parse_period(body["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))

    fos_access_key = body.get("fos_access_key")
    fos_secret_key = body.get("fos_secret_key")
    fos_key_id = ""

    if not fos_access_key or not fos_secret_key:
        desc = f"fos-log-analysis-{body.get('fos_bucket_name')}"
        existing = find_fos_key(desc, token)
        existing_secret = existing.get("secret_key") if existing else None
        if existing and existing_secret:
            fos_access_key = existing["access_key"]
            fos_secret_key = existing_secret
            fos_key_id = existing["access_key"]
        elif existing:
            raise HTTPException(
                status_code=409,
                detail=make_error(
                    "fos_key_secret_unavailable",
                    f"An access key '{desc}' already exists but its secret is not retrievable; "
                    "provide fos_access_key + fos_secret_key explicitly.",
                ),
            )
        else:
            try:
                bucket_name = body.get("fos_bucket_name")
                if not bucket_name:
                    raise HTTPException(status_code=400, detail={"error": "fos_bucket_name required"})
                new_key = ensure_fos_access_key(desc, body, token, buckets=[bucket_name])
                fos_access_key = new_key["access_key"]
                fos_secret_key = new_key["secret_key"]
                fos_key_id = new_key["id"]
            except Exception as e:
                raise_internal(logger, e, code="ensure_access_key_failed", status=400)

    if not body.get("cdn_secret"):
        body["cdn_secret"] = (
            existing_cfg.get("cdn_secret") or existing_fos_proxy.get("secret") or secrets.token_urlsafe(24)
        )

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
        "rum_retention_days": body.get("rum_retention_days", 30),
        "provisioning": {"fos_key_id": fos_key_id},
    }

    log_fields_raw = body.get("log_fields")
    if log_fields_raw:
        try:
            state["log_fields"] = json.loads(log_fields_raw) if isinstance(log_fields_raw, str) else log_fields_raw
        except Exception:
            logger.debug("provision_ingest: step failed (ignored)", exc_info=True)

    now_iso = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
    if any(k in body for k in ("cmcd_enabled", "cmcd_mode", "cmcd_version")) or isinstance(body.get("cmcd"), dict):
        existing_cmcd = body.get("cmcd", {}) if isinstance(body.get("cmcd"), dict) else {}
        state["cmcd"] = {
            "enabled": body.get("cmcd_enabled", existing_cmcd.get("enabled", False)),
            "mode": body.get("cmcd_mode") or existing_cmcd.get("mode", "query_string"),
            "version": body.get("cmcd_version")
            if body.get("cmcd_version") is not None
            else existing_cmcd.get("version", 1),
            "enabled_at": existing_cmcd.get("enabled_at") or now_iso,
        }

    scoring_keys = (
        "scoring_enabled",
        "scoring_domain",
        "scoring_request_secret",
        "scoring_exclude_url_regex",
        "scoring_enforce_status_code",
    )
    if any(k in body for k in scoring_keys) or isinstance(body.get("scoring"), dict):
        existing_scoring = body.get("scoring", {}) if isinstance(body.get("scoring"), dict) else {}
        state["scoring"] = {
            "enabled": body.get("scoring_enabled", existing_scoring.get("enabled", False)),
            "domain": body.get("scoring_domain") or existing_scoring.get("domain", ""),
            "request_secret": body.get("scoring_request_secret", existing_scoring.get("request_secret", "")),
            "exclude_url_regex": body.get("scoring_exclude_url_regex", existing_scoring.get("exclude_url_regex", "")),
            "enforce_status_code": body.get(
                "scoring_enforce_status_code", existing_scoring.get("enforce_status_code", 429)
            ),
            "enabled_at": existing_scoring.get("enabled_at") or now_iso,
        }

    if body.get("rum_enabled"):
        existing_rum = body.get("rum", {}) if isinstance(body.get("rum"), dict) else {}
        state["rum"] = {
            "enabled": True,
            "enabled_at": existing_rum.get("enabled_at") or now_iso,
            "custom_condition": body.get("rum_custom_condition") or existing_rum.get("custom_condition") or "",
        }
        on_disk_rum = existing_cfg.get("rum")
        faro_src: dict[str, Any] = (
            existing_rum if existing_rum.get("faro_version") else (on_disk_rum if isinstance(on_disk_rum, dict) else {})
        )
        if faro_src.get("faro_version"):
            for k in ("faro_version", "faro_content_hash", "faro_fos_etag_md5", "faro_last_upstream_check"):
                if k in faro_src:
                    state["rum"][k] = faro_src[k]

    write_service_config(state)

    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
        logger.info("[provision_ingest] Successfully reloaded scheduler with new intervals! 🔄")
    except Exception as e:
        logger.warning("[provision_ingest] Failed to reload scheduler after ingest: %s", e)

    _invalidate_service_credentials(state.get("logging_service_id") or "")

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        logger.warning("provision_ingest: _sync_crontab failed (ignored)", exc_info=True)

    return {"ok": True, "service_id": state["logging_service_id"]}


@provision_router.get("/check-config", response_model=ProvisionCheckConfigResponse, response_model_exclude_unset=True)
def provision_check_config(
    token: str = Query(...),
    service_id: str | None = Query(default=None),
    cdn_service_id: str = Query(...),
    bucket: str = Query(...),
):
    """Verify that both services are correctly configured for log analysis."""
    service_id = require_service_id(service_id)
    from backend.core.fastly.client import fastly

    results = {
        "logging_service": {"ok": False, "details": ""},
        "cdn_service": {"ok": False, "details": ""},
    }

    try:
        try:
            active_ver = fastly("GET", f"/service/{service_id}/version/active", token=token).get("number")
            endpoints = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/s3", token=token)
            endpoint = next((ep for ep in endpoints if ep.get("bucket_name") == bucket), None)

            if not endpoint:
                results["logging_service"]["details"] = f"No S3 logging endpoint found pointing to bucket '{bucket}'"
            else:
                snippets = fastly("GET", f"/service/{service_id}/version/{active_ver}/snippet", token=token)
                snip_names = {s.get("name") for s in snippets}
                required_snippets = {
                    "Fastly Log Analysis Capture",
                    "Fastly Log Analysis Miss",
                    "Fastly Log Analysis Pass",
                }
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
                    if endpoint.get("response_condition") is None:
                        results["logging_service"] = {
                            "ok": True,
                            "details": f"Warning: {msg}, but no response condition (sampling might be disabled)",
                        }
                    else:
                        results["logging_service"] = {"ok": True, "details": f"{msg} and active sampling condition"}
        except Exception as e:
            results["logging_service"]["details"] = f"Error checking logging service: {e}"

        try:
            dicts = fastly("GET", f"/service/{cdn_service_id}/version/active/dictionary", token=token)
            missing_dicts = {"fos_credentials", "cdn_auth"} - {d.get("name") for d in dicts}

            if missing_dicts:
                results["cdn_service"]["details"] = f"Missing required dictionaries: {', '.join(missing_dicts)}"
            else:
                backends = fastly("GET", f"/service/{cdn_service_id}/version/active/backend", token=token)
                if not any(".fastlystorage.app" in b.get("address", "") for b in backends):
                    results["cdn_service"]["details"] = (
                        "No backend found pointing to Fastly Object Storage (*.fastlystorage.app)"
                    )
                else:
                    active_ver_cdn = fastly("GET", f"/service/{cdn_service_id}/version/active", token=token).get(
                        "number"
                    )
                    snippets_cdn = fastly(
                        "GET", f"/service/{cdn_service_id}/version/{active_ver_cdn}/snippet", token=token
                    )
                    required_cdn = {
                        "cdn-swr-shield-disable",
                        "cdn-race-condition-generation",
                        "cdn-no-cache-404",
                    }
                    missing_cdn = required_cdn - {s.get("name") for s in snippets_cdn}
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


def _require_ngwaf_token(
    service_id: str,
    token: str,
    authorization: str | None,
    *,
    missing_message: str = "A Fastly API token is required.",
) -> str:
    """Resolve and validate a caller-supplied Fastly token for NGWAF operations."""
    from backend.core.vcl_generator import validate_destructive_token

    if authorization and authorization.lower().startswith("bearer "):
        resolved = authorization[len("bearer ") :].strip()
    else:
        resolved = (token or "").strip()
    if not resolved:
        raise HTTPException(
            status_code=401,
            detail={"error": "token_required", "message": missing_message},
        )
    validate_destructive_token(resolved, service_id=service_id)
    return resolved


@provision_router.get("/ngwaf-workspaces", response_model=NgwafWorkspacesResponse, response_model_exclude_unset=True)
def provision_ngwaf_workspaces(
    service_id: str | None = Query(default=None),
    token: str = Query(default=""),
    authorization: str | None = Header(default=None),
):
    """List NGWAF workspaces for a service (requires validated Fastly token)."""
    from backend.core.fastly.client import NgwafWorkspacesResponse
    from backend.provision import fastly
    from backend.utils.router_utils import format_debug_request

    service_id = require_service_id(service_id)
    token = _require_ngwaf_token(
        service_id,
        token,
        authorization,
        missing_message="A Fastly API token is required to list NGWAF workspaces.",
    )
    _ngwaf_url = "https://api.fastly.com/ngwaf/v1/workspaces"
    _req_headers = {"Fastly-Key": token, "Accept": "application/json"}

    try:
        data = fastly("GET", "/ngwaf/v1/workspaces", token=token, max_retries=0, timeout=10)
        raw_body = json.dumps(data)
        workspaces = NgwafWorkspacesResponse.model_validate(data).workspaces_list()
        result: dict[str, Any] = {"workspaces": workspaces}
        if not workspaces:
            try:
                token_data = fastly("GET", "/tokens/self", token=token)
                if not token_data.get("user_id"):
                    result["error_hint"] = (
                        "You are using an Automation Token (Service Account). "
                        "Fastly Automation Tokens do not have access to Next-Gen WAF (NGWAF) data. "
                        "To use NGWAF features, please use a Personal API Token from a human user with Security permissions."
                    )
            except Exception:
                logger.debug("provision_ngwaf_workspaces: step failed (ignored)", exc_info=True)
            req_debug = format_debug_request("GET", _ngwaf_url, _req_headers)
            result["_debug_raw"] = f"{req_debug}\n\n--- Response (200) ---\n{raw_body[:500]}"
        return result
    except RuntimeError as exc:
        http_match = _FASTLY_HTTP_ERROR_RE.match(str(exc))
        if http_match is None:
            raise_internal(logger, exc, code="ngwaf_workspaces_failed", status=400)
        status_code = int(http_match.group(1))
        logger.warning("[ngwaf-workspaces] HTTPError %s body=%s", status_code, str(exc)[:300])
        if status_code == 401:
            raise HTTPException(
                status_code=400,
                detail=make_error("ngwaf_token_invalid", "Invalid API token or missing Edge Security permissions."),
            )
        raise_internal(logger, exc, code="ngwaf_api_error", status=400)
    except Exception as e:
        raise_internal(logger, e, code="ngwaf_workspaces_failed", status=400)


@provision_router.patch(
    "/services/{service_id}/ngwaf-workspace",
    response_model=NgwafWorkspaceSetResponse,
    response_model_exclude_unset=True,
)
def provision_set_ngwaf_workspace(
    service_id: str,
    body: NgwafWorkspaceCreateRequest,
    token: str = Query(default=""),
    authorization: str | None = Header(default=None),
):
    """Persist the NGWAF workspace ID for a service and reload the scheduler."""
    from backend.utils.router_utils import load_service_config, mutate_service_config

    load_service_config(service_id)
    _require_ngwaf_token(service_id, token, authorization)

    workspace_id = (body.ngwaf_workspace_id or "").strip() or None
    mutate_service_config(service_id, lambda c: c.__setitem__("ngwaf_workspace_id", workspace_id))

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        logger.warning("provision_set_ngwaf_workspace: _sync_crontab failed (ignored)", exc_info=True)

    return {"ok": True, "ngwaf_workspace_id": workspace_id}
