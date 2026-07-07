"""Provisioning router — service list, validate, check-domain, teardown, execute."""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.provision import (
    CheckFosRequest,
    LakeInfoRequest,
    NgwafWorkspaceSetResponse,
    NgwafWorkspacesResponse,
    ProvisionCheckConfigResponse,
    ProvisionCheckDomainResponse,
    ProvisionCheckFosResponse,
    ProvisionConfigRequest,
    ProvisionExecuteRequest,
    ProvisionIngestResponse,
    ProvisionLakeInfoResponse,
    ProvisionTeardownRequest,
    ProvisionValidateRequest,
    ProvisionValidateResponse,
)
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, make_error, raise_internal

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/provision", tags=["provision"], responses=DEFAULT_ERROR_RESPONSES)


def _invalidate_service_credentials(service_id: str) -> None:
    """Drop the process-local caches that bake a service's FOS/CDN credentials so
    a same-process re-provision (which mints new keys) stops serving the old
    ones: the boto3 FOS client cache and pooled DuckDB connections' baked S3
    SECRET.

    Without this, a teardown→re-provision of the same service keeps the deleted
    access key alive in-process and every ingest GET/HEAD + parquet read 401s
    until the backend restarts. The iceberg table/view/catalog caches are
    cleared separately (the teardown path does so with the full runtime source).
    Keyed on ``service_id`` (== source ``name``). Best-effort — never raises into
    the provision flow; a no-op for a service with nothing cached yet.
    """
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


from backend.models.admin import ProvisionService


@router.get("/services", response_model=list[ProvisionService])
def provision_list_services(token: str = Query(...)):
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly
    from backend.provision.fos_setup import object_storage_enabled

    try:
        services = fastly("GET", "/service", token=token)
    except Exception as e:
        raise_internal(logger, e, code="list_services_failed", status=400)

    # Object Storage is REQUIRED for log storage and is an account-level product
    # that must be enabled. The token is valid here (services listed above), so a
    # failed product check means the account simply hasn't enabled Object Storage.
    # Surface a clear, actionable message now instead of letting provisioning die
    # with a cryptic 403 at the FOS access-key step (Step 2/8) and roll back.
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

    existing_ids = set(svcconfig.list_service_ids())
    return [
        {"id": s["id"], "name": s["name"], "provisioned": s["id"] in existing_ids}
        for s in services
        if s.get("type", "vcl") == "vcl"
    ]


@router.post("/validate", response_model=ProvisionValidateResponse, response_model_exclude_unset=True)
def provision_validate(body: ProvisionValidateRequest):
    from backend.core.fastly.client import fastly

    token = body.token
    service_id = body.service_id
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
        raise_internal(logger, e, code="provision_validate_failed", status=400)


@router.get(
    "/check-domain",
    response_model=ProvisionCheckDomainResponse,
    response_model_exclude_unset=True,
)
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


@router.post("/check-fos", response_model=ProvisionCheckFosResponse, response_model_exclude_unset=True)
def provision_check_fos(req: CheckFosRequest):
    """Validate FOS credentials by attempting to list objects."""
    bucket = req.bucket
    region = req.region
    access_key = req.access_key
    secret_key = req.secret_key
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


def _require_json_content_type(req: Request) -> None:
    """Reject any teardown request whose Content-Type isn't application/json.

    CSRF defense: an HTML form with ``enctype=text/plain`` can POST a body
    that LOOKS like JSON without triggering a CORS preflight. Requiring
    ``Content-Type: application/json`` forces the browser to preflight any
    cross-origin call (text/plain is "simple"; application/json is not),
    blocking the silent-invocation vector. Runs as a Depends() so it fires
    before FastAPI's body parser — otherwise a malformed text/plain body
    returns 422 from the parser and the explicit 415 never executes."""
    if not (req.headers.get("content-type") or "").startswith("application/json"):
        raise HTTPException(
            status_code=415,
            detail=make_error("unsupported_media_type", "Content-Type must be application/json"),
        )


# response_model intentionally omitted: SSE progress stream
# (EventSourceResponse), not a JSON body.
@router.post("/teardown", dependencies=[Depends(_require_json_content_type)])
def provision_teardown(req: Request, body: ProvisionTeardownRequest | None = None):
    """Destructive service teardown over SSE.

    Switched from ``GET`` to ``POST`` to defend against CSRF: a GET
    endpoint with side effects can be triggered by any cross-origin
    ``<img src="…">``, ``<link>``, or ``<form method=get>``. POST routes
    require the caller to send a request that browsers do not emit
    cross-origin without the user explicitly submitting a form, and
    ``Content-Type: application/json`` (sent by the dashboard's fetch
    client) puts the request in the CORS-preflighted bucket so the
    browser will block silent invocation entirely.
    """
    body = body or ProvisionTeardownRequest()
    token: str = body.token
    service_id: str | None = body.service_id
    remove_logging: bool = body.remove_logging
    remove_cdn: bool = body.remove_cdn
    remove_bucket: bool = body.remove_bucket
    remove_scoring: bool = body.remove_scoring
    remove_cache: bool = body.remove_cache
    remove_cron: bool = body.remove_cron
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.provision import _sync_crontab, perform_teardown
    from backend.utils.fastly_auth import validate_destructive_token

    state = None
    if service_id:
        svc_cfg = svcconfig.load_config(service_id)
        if svc_cfg:
            # Security: do NOT fall back to the server-stored
            # ``fastly_api_key``. Destructive operations require the caller to
            # supply a token that we then validate against Fastly's
            # /tokens/self endpoint. The stored key is only used for
            # scheduled, non-destructive background sync.
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
                # Carry the scoring block so perform_teardown can tear down the
                # Compute scorer service + its stores. Read while svc_cfg is
                # still in memory — the config file is removed below before
                # perform_teardown runs, so disable_scoring (which reloads
                # config) can't be used; the ids must travel via state.
                "scoring": svc_cfg.get("scoring") or {},
            }

    if not state:
        raise HTTPException(status_code=404, detail={"error": "No service config found."})

    # Security: teardown requires a caller-supplied Fastly token with the
    # ``global`` scope and access to this service.
    validate_destructive_token(token, service_id=service_id or "")

    opts = {
        "remove_logging": remove_logging,
        "remove_cdn": remove_cdn,
        "remove_bucket": remove_bucket,
        "remove_scoring": remove_scoring,
    }

    def stream():
        sid = state.get("logging_service_id") or service_id

        try:
            # Stop scheduler jobs for this service immediately so no background sync
            # can write into the cache dir or attempt to fetch from FOS while we're deleting.
            if sid:
                yield json.dumps({"type": "status", "message": f"Stopping background sync for service {sid}..."})

                cfg_path = svcconfig.config_path(sid)
                if os.path.exists(cfg_path):
                    os.remove(cfg_path)
                    # The service is gone the instant its config file is. Drop
                    # the 3s admin bootstrap cache now so /api/bootstrap stops
                    # listing it immediately (otherwise it lingers for up to
                    # the TTL — the provision-teardown e2e races exactly here).
                    # Via the registry, not a bootstrap-router import (routers
                    # must stay import-independent — import-linter R-9).
                    from backend.utils.cache_registry import CacheRegistry

                    CacheRegistry.clear("routers.bootstrap._bootstrap_cache")
                    yield json.dumps({"type": "status", "message": f"Removed configs/{sid}.json"})

                # Sync crontab and reload scheduler to remove jobs immediately
                try:
                    _sync_crontab()
                    from backend.cron.scheduler import get_scheduler

                    get_scheduler().reload()
                    if remove_cron:
                        yield json.dumps({"type": "status", "message": "Cron jobs updated"})
                except Exception as e:
                    if remove_cron:
                        yield json.dumps({"type": "status", "message": f"Warning: Failed to update cron jobs: {e}"})

                # Drop ALL in-memory iceberg caches for this service so a
                # same-process re-provision of the same bucket can't resurrect a
                # stale Table object (which makes init_iceberg_table skip
                # creation and commit_buffer append against deleted metadata).
                # Key off the runtime source, NOT the service_id — the caches
                # are keyed by source name + bucket, so the old
                # clear_source_caches(sid) call missed every entry. svc_cfg is
                # the in-memory config loaded before the file was removed above.
                try:
                    from backend.core import iceberg as db_iceberg

                    _cache_src = svcconfig.config_to_source(svc_cfg) if svc_cfg else {"name": sid}
                    db_iceberg.invalidate_service_caches(_cache_src)
                except Exception:
                    pass

                # Also drop the credential-bearing caches (boto3 FOS client +
                # pooled DuckDB S3 SECRET) so a same-process re-provision of this
                # service id can't keep serving the now-deleted access key.
                if sid:
                    _invalidate_service_credentials(sid)

            yield json.dumps({"type": "status", "message": "Starting teardown of Fastly resources..."})
            for event in perform_teardown(state, token, opts=opts):
                yield json.dumps(event)

            if remove_cache:
                import shutil
                import time

                # Remove the DuckDB file and WAL independently from the cache dir —
                # if one fails, the other still runs.
                db_path = svcconfig.duckdb_path(sid) if sid else _db.DUCKDB_PATH
                for f in [db_path, db_path + ".wal"]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                            _db.clear_initialization_state(f)
                        except Exception as e:
                            yield json.dumps(
                                {"type": "status", "message": f"Warning: could not remove {os.path.basename(f)}: {e}"}
                            )

                # Remove per-service cache dir (scoped by bucket name to avoid
                # wiping other services' caches).
                src_mock = {"bucket": state.get("fos_bucket_name", ""), "prefix": state.get("fos_prefix", "")}
                if src_mock["bucket"]:
                    svc_cache_dir = _db._cache_dir(src_mock)
                    if os.path.exists(svc_cache_dir):
                        # reload() above unscheduled this service's jobs, but
                        # APScheduler does NOT cancel a run that's already
                        # executing. An in-flight sync/compaction tick calls
                        # os.makedirs(cache_dir, exist_ok=True) and writes a
                        # parquet file back into the tree between rmtree's
                        # scandir and unlink — surfacing as ENOTEMPTY (errno 39)
                        # and leaving an orphaned partial cache dir. The job is
                        # gone now, so that last run drains within a second or
                        # two; retry briefly to let it finish, then sweep.
                        rmtree_err = None
                        for _attempt in range(6):
                            try:
                                shutil.rmtree(svc_cache_dir)
                                rmtree_err = None
                                break
                            except FileNotFoundError:
                                rmtree_err = None
                                break
                            except OSError as e:
                                rmtree_err = e
                                time.sleep(0.5)
                        if rmtree_err is not None:
                            yield json.dumps(
                                {"type": "status", "message": f"Warning: could not remove cache dir: {rmtree_err}"}
                            )

                # The per-service metadata SQLite (ingested_files, cron_runs,
                # rollups) lives in the system data dir, NOT the cache dir, so
                # the rmtree above never touched it. Without this a re-provision
                # inherits the dead service's ingested_files rollup and the
                # usage-log gap panel keeps showing stale "ours" counts.
                if sid:
                    try:
                        from backend.core import metadata as metadata_db

                        metadata_db.teardown(sid)
                    except Exception as e:
                        yield json.dumps({"type": "status", "message": f"Warning: could not clear metadata: {e}"})

                yield json.dumps({"type": "status", "message": "Removed local database and cache"})

            # Log teardown if the DB wasn't completely wiped
            if sid and not remove_cache:
                try:
                    from backend.core import metadata as metadata_db

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
                        yield json.dumps({"type": "status", "message": "Removed logs directory"})
                    except Exception as e:
                        yield json.dumps(
                            {"type": "status", "message": f"Warning: could not remove logs directory: {e}"}
                        )

            _db.reload_default_source()
            yield json.dumps({"type": "done", "message": "Teardown complete."})
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.post("/lake-info", response_model=ProvisionLakeInfoResponse, response_model_exclude_unset=True)
def provision_lake_info(req: LakeInfoRequest):
    """Return Iceberg table range and calendar for a given bucket/credentials without registering it."""
    bucket = req.bucket
    region = req.region
    access_key = req.access_key
    secret_key = req.secret_key
    prefix = req.prefix
    endpoint = req.endpoint
    iceberg_metadata_location = req.iceberg_metadata_location
    import hashlib

    from backend.core.iceberg.lake_info import fetch_lake_info

    # Use a deterministic name to isolate catalog caches from real services.
    # MD5 is fine here — this is a cache-key fingerprint, not a security
    # primitive. ``usedforsecurity=False`` flags intent so bandit / fips
    # / future readers don't second-guess the choice.
    h = hashlib.md5(f"{bucket}:{prefix}".encode(), usedforsecurity=False).hexdigest()[:12]
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


# response_model intentionally omitted: SSE progress stream
# (EventSourceResponse), not a JSON body.
@router.post("/execute")
def provision_execute(req: ProvisionExecuteRequest):
    token = req.token
    service_id = req.service_id
    service_name = req.service_name
    endpoint_name = req.endpoint_name
    fos_region = req.fos_region
    fos_bucket_name = req.fos_bucket_name
    fos_prefix = req.fos_prefix
    sample_rate = req.sample_rate
    edge_only = req.edge_only
    custom_condition = req.custom_condition
    log_period = req.log_period
    cdn_service_name = req.cdn_service_name
    cdn_url = req.cdn_url
    cdn_shield = req.cdn_shield
    enable_cron_sync = req.enable_cron_sync
    delete_after = req.delete_after
    commit_interval_mins = req.commit_interval_mins
    enable_cron_compact = req.enable_cron_compact
    log_retention_days = req.log_retention_days
    log_fields = req.log_fields
    import secrets

    from backend.core import duckdb as _db
    from backend.provision import parse_period
    from backend.utils.pop_utils import fetch_pop_locations

    fetch_pop_locations(token)

    if not service_name:
        from backend import config as svcconfig

        service_name = svcconfig.fetch_service_name(service_id, token) or service_id

    cfg: dict[str, Any] = {
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
        raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))

    bucket = cfg["fos_bucket_name"]
    if not re.match(r"^[A-Za-z0-9](?!.*--)[A-Za-z0-9-]{1,61}[A-Za-z0-9]$", bucket):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_bucket",
                f"Invalid bucket name: '{bucket}'. Use 3-63 alphanumeric characters or single hyphens.",
            ),
        )

    prefix = cfg.get("fos_prefix", "")
    if prefix and not re.match(r"^[A-Za-z0-9/_-]*$", prefix):
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

                    # New service's config is now on disk; drop the 3s admin
                    # bootstrap cache so /api/bootstrap surfaces it immediately
                    # rather than after the TTL lapses (symmetry with teardown).
                    # Via the registry, not a bootstrap-router import (R-9).
                    from backend.utils.cache_registry import CacheRegistry

                    CacheRegistry.clear("routers.bootstrap._bootstrap_cache")

                    # A re-provision of an existing service id mints fresh FOS
                    # keys; drop any in-process caches still holding the old key
                    # so the next sync/read uses the new creds (not a 401 until
                    # restart). No-op for a brand-new service.
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
                        pass

                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()

                        # Trigger an initial metadata sync
                        try:
                            from backend.cron.jobs.metadata import _run_metadata_sync

                            _run_metadata_sync(cfg["logging_service_id"])
                        except Exception as e:
                            logger.warning("[provision] Initial metadata sync after provision failed: %s", e)
                    except Exception:
                        pass
        except Exception as e:
            if not error_already_emitted:
                yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.post("/terraform/preview", response_model=dict[str, str])
def provision_terraform_preview(body: ProvisionConfigRequest):
    from backend.utils.terraform_gen import generate_terraform

    # ``extra="allow"`` on the model captures any future wizard fields
    # we haven't enumerated. ``model_dump`` includes them in the dict
    # passed to generate_terraform so the helper keeps reading the same
    # shape it always has.
    cfg = body.model_dump(exclude_none=True)
    access_key = cfg.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = cfg.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    return generate_terraform(cfg, access_key, secret_key)


# response_model intentionally omitted: streams a ZIP (StreamingResponse),
# not a JSON body.
@router.post("/terraform/export")
def provision_terraform_export(body: ProvisionConfigRequest):
    import io
    import zipfile

    from backend.utils.terraform_gen import generate_terraform

    cfg = body.model_dump(exclude_none=True)
    access_key = cfg.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = cfg.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    tf_files = generate_terraform(cfg, access_key, secret_key)

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


@router.post("/ingest", response_model=ProvisionIngestResponse, response_model_exclude_unset=True)
def provision_ingest(payload: ProvisionConfigRequest):
    import secrets

    from backend.provision import ensure_fos_access_key, find_fos_key, parse_period, write_service_config
    from backend.utils.fastly_auth import validate_destructive_token
    from backend.utils.pop_utils import fetch_pop_locations

    # The body is mutated in-place below (parse_period substitution,
    # cdn_secret default, log_fields decode). Pull a plain dict out of
    # the validated model so the existing logic keeps working — typed
    # known fields stay typed via ``payload.<field>``; the dict carries
    # the ``extra="allow"`` passthroughs (e.g. wizard-side fields we
    # haven't enumerated yet).
    body: dict[str, Any] = payload.model_dump(exclude_none=False)

    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail={"error": "Token is required"})

    # Provisioning writes a service config that the scheduler immediately
    # picks up and starts ingesting from. Without a token validation pass
    # here the route would mint configs for any service_id reachable by
    # the caller's network position, even though the caller may not
    # legitimately own that service. ``validate_destructive_token``
    # rejects when scope, bound-services, or tenant don't match.
    logging_service_id = body.get("service_id") or body.get("logging_service_id") or ""
    validate_destructive_token(token, service_id=logging_service_id)

    fetch_pop_locations(token)

    try:
        if body.get("log_period"):
            body["log_period"] = parse_period(body["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))

    # We skip bucket and service creation, and only ensure we have an access key
    # if one wasn't provided directly (though they really should be providing them or we generate one for the bucket)

    fos_access_key = body.get("fos_access_key")
    fos_secret_key = body.get("fos_secret_key")
    fos_key_id = ""

    if not fos_access_key or not fos_secret_key:
        # Try to find or create one
        desc = f"fos-log-analysis-{body.get('fos_bucket_name')}"
        existing = find_fos_key(desc, token)
        existing_secret = existing.get("secret_key") if existing else None
        if existing and existing_secret:
            fos_access_key = existing["access_key"]
            fos_secret_key = existing_secret
            fos_key_id = existing["access_key"]
        elif existing:
            # The access-key LIST response carries no secret_key (Fastly only
            # returns the secret at key-creation time). We can't reconstruct
            # it, so fail clearly rather than KeyError-ing into a 500 or
            # writing a broken credential. The caller must pass fos_secret_key.
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
                # Fastly API error bodies sometimes carry internal token /
                # account hints — log full server-side and return only a
                # correlation id, mirroring query.py's leak posture.
                raise_internal(logger, e, code="ensure_access_key_failed", status=400)

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

    log_fields_raw = body.get("log_fields")
    if log_fields_raw:
        try:
            state["log_fields"] = json.loads(log_fields_raw) if isinstance(log_fields_raw, str) else log_fields_raw
        except Exception:
            pass

    # NB: the token was already validated against this same service_id at the
    # top of the handler (validate_destructive_token above); the prior second
    # pass here re-imported and re-validated the identical token+service_id.
    write_service_config(state)

    # Re-ingest can carry refreshed FOS creds; drop the credential-bearing
    # caches so the next sync/read picks them up rather than 401ing on a stale
    # in-process key.
    _invalidate_service_credentials(state.get("logging_service_id") or "")

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        pass

    return {"ok": True, "service_id": state["logging_service_id"]}


@router.get(
    "/check-config",
    response_model=ProvisionCheckConfigResponse,
    response_model_exclude_unset=True,
)
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


@router.get(
    "/ngwaf-workspaces",
    response_model=NgwafWorkspacesResponse,
    response_model_exclude_unset=True,
)
def provision_ngwaf_workspaces(
    service_id: str = Query(...),
    token: str = Query(default=""),
    authorization: str | None = Header(default=None),
):
    """List NGWAF workspaces for a service.

    Security: previously the endpoint would silently fall back to
    the server-stored ``fastly_api_key`` if the caller didn't pass a
    token, letting any local-loopback caller enumerate NGWAF workspaces
    for any service using the stored credential. Now the caller MUST
    present a token, and we accept either:
      - the stored ``fastly_api_key`` for this service (constant-time
        match — preserves the existing admin UX where the frontend
        passes the stored key it just used to fetch workspaces), OR
      - a token whose /tokens/self response shows access to this service
        (the strict validation path used for the destructive op).
    Either way an unauthenticated caller can't enumerate workspaces
    even if they reach the loopback admin surface.
    """
    import urllib.error

    from backend.provision import fastly
    from backend.utils.fastly_auth import validate_destructive_token

    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[len("bearer ") :].strip()
    else:
        token = token.strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "token_required",
                "message": "A Fastly API token is required to list NGWAF workspaces.",
            },
        )
    # Secure token validation: we must always run validate_destructive_token
    # to verify that the token holds the necessary 'global' scope and is
    # authorized for this tenant's service. This prevents read-only token
    # bypasses, even if the token matches the server-stored fastly_api_key.
    validate_destructive_token(token, service_id=service_id)

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
                status_code=400,
                detail=make_error(
                    "ngwaf_token_invalid",
                    "Invalid API token or missing Edge Security permissions.",
                ),
            )
        # Upstream body may carry sensitive Fastly internals; collapse to
        # an error_id and log the full body for operator post-incident.
        raise_internal(logger, exc, code="ngwaf_api_error", status=400)
    except Exception as e:
        raise_internal(logger, e, code="ngwaf_workspaces_failed", status=400)


@router.patch(
    "/services/{service_id}/ngwaf-workspace",
    response_model=NgwafWorkspaceSetResponse,
    response_model_exclude_unset=True,
)
def provision_set_ngwaf_workspace(
    service_id: str,
    body: dict,
    token: str = Query(default=""),
    authorization: str | None = Header(default=None),
):
    """Persist the NGWAF workspace ID for a service and reload the scheduler.

    Security: require the caller to present a Fastly token bound to
    this service. Two paths are accepted:

      1. The caller passes a token that ``/tokens/self`` confirms has the
         ``global`` scope and access to ``service_id`` (preferred — admin
         can rotate without re-entering the stored key).
      2. The caller passes a token that constant-time-matches the
         service's stored ``fastly_api_key`` (the existing admin flow).

    Either way an unauthenticated attacker who can reach the endpoint can't
    rebind the workspace because they don't know the token. The middleware
    /api/provision/ block also gates this for analysts.
    """

    from backend import config as svcconfig
    from backend.utils.fastly_auth import validate_destructive_token
    from backend.utils.router_utils import load_service_config

    cfg = load_service_config(service_id)

    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[len("bearer ") :].strip()
    else:
        token = (token or "").strip()
    stored = (cfg.get("fastly_api_key") or "").strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail={"error": "token_required", "message": "A Fastly API token is required."},
        )

    # Secure token validation: we must always run validate_destructive_token
    # to verify that the token holds the necessary 'global' scope and is
    # authorized for this tenant's service. This prevents read-only token
    # bypasses, even if the token matches the server-stored fastly_api_key.
    validate_destructive_token(token, service_id=service_id)

    workspace_id = (body.get("ngwaf_workspace_id") or "").strip() or None
    cfg["ngwaf_workspace_id"] = workspace_id
    svcconfig.save_config(service_id, cfg)

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        pass

    return {"ok": True, "ngwaf_workspace_id": workspace_id}
