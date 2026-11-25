"""Provisioning router — validate, execute, teardown, check-fos, terraform, ngwaf."""

from __future__ import annotations

import datetime as _dt
import http.client
import io
import ipaddress
import json
import logging
import os
import re
import secrets
import shutil
import socket
import ssl
import time
import urllib.error
import urllib.request
import zipfile
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from backend.core.fastly.client import validate_rum_custom_condition
from backend.core.request_context import require_admin
from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS
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
    redact_error_text,
    require_json_content_type,
    require_service_id,
)
from backend.utils.sse import sse_payload as _sse
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/provision",
    tags=["provision"],
    responses=DEFAULT_ERROR_RESPONSES,
    dependencies=[Depends(require_admin)],
)
provision_router = router

_FASTLY_HTTP_ERROR_RE = re.compile(r"HTTP (\d+) ")


def _require_bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail={"error": "token_required", "message": "A Fastly API token is required."},
        )
    token = authorization[len("bearer ") :].strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail={"error": "token_required", "message": "A Fastly API token is required."},
        )
    return token


def _invalidate_service_credentials(service_id: str) -> None:
    """Drop in-process FOS client cache for ``service_id``."""
    try:
        from backend.core import fos_client as _db

        _db.clear_fos_client(service_id)
    except Exception:
        logger.warning("[provision] credential cache invalidation failed for %s", service_id, exc_info=True)


_PUBLIC_DOMAIN_RE = re.compile(
    r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{2,59})$"
)
_INTERNAL_DOMAIN_SUFFIXES = (
    ".local",
    ".localhost",
    ".localdomain",
    ".internal",
    ".cluster.local",
    ".svc",
    ".home.arpa",
    ".corp",
    ".lan",
)
_DOMAIN_PROBE_MAX_BYTES = 8192


def _is_public_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return _is_public_ip(ip.ipv4_mapped)
    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or not ip.is_global
    ):
        return False
    return True


def _validate_public_domain_syntax(raw_domain: str) -> tuple[str, str | None]:
    """Normalize ``raw_domain`` and reject IP literals, single-label hosts, and internal suffixes."""
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in raw_domain):
        return "", "Invalid custom domain. Must be a valid public hostname."
    clean = raw_domain.strip().rstrip(".").lower()
    if not clean:
        return (
            clean,
            "Invalid custom domain. Must be non-empty, contain at least one dot, and consist only of letters, digits, hyphens, and dots.",
        )
    unbracketed = clean[1:-1] if clean.startswith("[") and clean.endswith("]") else clean
    try:
        ipaddress.ip_address(unbracketed)
        return clean, "Domain must be a public hostname (IP address literals are not allowed)"
    except ValueError:
        pass
    if clean in {"localhost", "metadata"} or clean.endswith(_INTERNAL_DOMAIN_SUFFIXES):
        return clean, "Domain must be a public hostname (internal domains are not allowed)"
    if not _PUBLIC_DOMAIN_RE.fullmatch(clean):
        return (
            clean,
            "Invalid custom domain. Must be non-empty, contain at least one dot, and consist only of letters, digits, hyphens, and dots.",
        )
    return clean, None


def _resolve_pinned_public_ip(domain: str) -> tuple[str | None, str | None]:
    """Resolve ``domain`` and ensure EVERY A/AAAA record is globally routable."""
    try:
        records = socket.getaddrinfo(domain, 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return None, "DNS/Connection error (likely available): domain did not resolve"
    except OSError:
        return None, "DNS/Connection error (likely available): resolution failed"
    if not records:
        return None, "DNS/Connection error (likely available): domain did not resolve"

    first_ip: str | None = None
    for _family, _socktype, _proto, _canonname, sockaddr in records:
        ip_raw = str(sockaddr[0]).split("%", 1)[0]
        try:
            ip_obj = ipaddress.ip_address(ip_raw)
        except ValueError:
            return None, "Domain resolves to a non-public IP address"
        if not _is_public_ip(ip_obj):
            return None, "Domain resolves to a non-public IP address"
        if first_ip is None:
            first_ip = ip_raw
    return first_ip, None


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Connect TCP to ``pinned_ip`` while verifying TLS cert + SNI against ``server_hostname``."""

    def __init__(self, host: str, *, pinned_ip: str, server_hostname: str, **kwargs: Any) -> None:
        super().__init__(host=host, **kwargs)
        self._pinned_ip = pinned_ip
        self._server_hostname = server_hostname

    def connect(self) -> None:
        raw_sock = socket.create_connection((self._pinned_ip, self.port), self.timeout)
        try:
            ctx = getattr(self, "_context", None) or ssl.create_default_context()
            self.sock = ctx.wrap_socket(raw_sock, server_hostname=self._server_hostname)
        except BaseException:
            raw_sock.close()
            raise


class _PinnedHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, pinned_ip: str, server_hostname: str) -> None:
        ctx = ssl.create_default_context()
        super().__init__(context=ctx)
        self._pinned_ip = pinned_ip
        self._server_hostname = server_hostname
        self._pinned_ctx = ctx

    def https_open(self, req: urllib.request.Request):
        return self.do_open(
            lambda host, **kwargs: _PinnedHTTPSConnection(
                host,
                pinned_ip=self._pinned_ip,
                server_hostname=self._server_hostname,
                **kwargs,
            ),
            req,
            context=self._pinned_ctx,
        )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Never follow 3xx Location headers during domain availability probing."""

    def http_error_301(self, req, fp, code, msg, headers):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)

    http_error_302 = http_error_303 = http_error_307 = http_error_308 = http_error_301


def _check_domain_available(domain: str, timeout: int = 10) -> tuple[bool, str | None]:
    from backend.utils.telemetry import tracked_call

    clean, syntax_err = _validate_public_domain_syntax(domain)
    if syntax_err is not None:
        return False, syntax_err

    with tracked_call("GET", clean, service="Domain Check"):
        pinned_ip, dns_note_or_err = _resolve_pinned_public_ip(clean)
        if pinned_ip is None:
            if dns_note_or_err and dns_note_or_err.startswith("DNS/Connection"):
                return True, dns_note_or_err
            return False, dns_note_or_err or "Domain resolves to a non-public IP address"

        opener = urllib.request.build_opener(
            _PinnedHTTPSHandler(pinned_ip, clean),
            _NoRedirectHandler(),
            urllib.request.ProxyHandler({}),
        )
        req = urllib.request.Request(f"https://{clean}/", headers={"Host": clean}, method="GET")
        try:
            with opener.open(req, timeout=timeout):
                return False, "Domain already in use (returned 200)"
        except urllib.error.HTTPError as exc:
            try:
                if exc.code in (301, 302, 303, 307, 308):
                    return False, "Domain already registered or in use"
                body = exc.read(_DOMAIN_PROBE_MAX_BYTES).decode(errors="replace")
            finally:
                exc.close()
            if "Please check that this domain has been added to a service." in body:
                return True, None
            return False, "Domain already registered or in use"
        except (urllib.error.URLError, TimeoutError, ssl.SSLError, OSError):
            return True, "DNS/Connection error (likely available): connection failed"
        except Exception:
            logger.warning("[provision] Unexpected error probing domain %s", clean, exc_info=True)
            return False, "Unable to verify domain availability"


@provision_router.get("/services", response_model=list[ProvisionService])
def provision_list_services(authorization: str | None = Header(default=None)):
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly
    from backend.provision.fos_setup import object_storage_enabled

    token = _require_bearer_token(authorization)
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
        from backend.provision.opaque_names import generate_opaque_bucket_name, generate_opaque_cdn_prefix

        return debug_envelope(
            {
                "service_name": svc.get("name", service_id),
                "token_info": token_info,
                "defaults": {
                    "endpoint_name": "Fastly Object Storage Logs",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": generate_opaque_bucket_name(),
                    "fos_prefix": "",
                    "sample_rate": 100,
                    "edge_only": True,
                    "log_period": "1 minute",
                    "cdn_service_name": f"Log Analysis CDN Service for {service_id}",
                    "cdn_prefix": generate_opaque_cdn_prefix(),
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
        domain, syntax_err = _validate_public_domain_syntax(prefix)
        if syntax_err is not None:
            return {
                "available": False,
                "reason": syntax_err,
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
        raw_err = str(e)
        err_msg = redact_error_text(e)
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
        if "EndpointConnectionError" in raw_err:
            err_msg = "Connection failed. Please verify the FOS Region is correct."
        return debug_envelope({"ok": False, "error": err_msg}, gated=True, is_cached=None, inject_calls=True)


@provision_router.post("/teardown", dependencies=[Depends(require_json_content_type)])
def provision_teardown(req: Request, body: ProvisionTeardownRequest | None = None):
    """Destructive service teardown over SSE."""
    body = body or ProvisionTeardownRequest()
    token, service_id = body.token, body.service_id
    from backend import config as svcconfig
    from backend.core import fos_client as _db
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
                        yield _sse("status", f"Warning: Failed to update cron jobs: {redact_error_text(e)}")

                _invalidate_service_credentials(sid)

            yield _sse("status", "Starting teardown of Fastly resources...")
            for event in perform_teardown(state, token, opts=opts):
                if isinstance(event, dict) and isinstance(event.get("message"), str):
                    event = {**event, "message": redact_error_text(event["message"])}
                yield json.dumps(event)

            if body.remove_cache:
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
                            yield _sse(
                                "status", f"Warning: could not remove cache dir: {redact_error_text(rmtree_err)}"
                            )

                if sid:
                    try:
                        from backend.core import metadata as metadata_db

                        metadata_db.teardown(sid)
                    except Exception as e:
                        yield _sse("status", f"Warning: could not clear metadata: {redact_error_text(e)}")

                yield _sse("status", "Removed local metadata and cache")

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
                        yield _sse("status", f"Warning: could not remove logs directory: {redact_error_text(e)}")

            _db.reload_default_source()
            yield _sse("done", "Teardown complete.")
        except Exception as e:
            yield _sse("error", redact_error_text(e))

    return sse_response(stream())


def _require_int_field(val: Any, field_name: str, *, min_val: int | None = None, max_val: int | None = None) -> int:
    if isinstance(val, bool) or not isinstance(val, (int, str)):
        raise HTTPException(status_code=400, detail=make_error("invalid_config", f"{field_name} must be an integer."))
    try:
        parsed = int(val.strip() if isinstance(val, str) else val)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=make_error("invalid_config", f"{field_name} must be an integer.")
        ) from None
    if (min_val is not None and parsed < min_val) or (max_val is not None and parsed > max_val):
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_config", f"{field_name} is out of valid range."),
        )
    return parsed


def _validate_fos_bucket_name(bucket: Any) -> str:
    if not isinstance(bucket, str) or not re.fullmatch(r"^[A-Za-z0-9](?!.*--)[A-Za-z0-9-]{1,61}[A-Za-z0-9]$", bucket):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_bucket",
                f"Invalid bucket name: '{bucket}'. Use 3-63 alphanumeric characters or single hyphens.",
            ),
        )
    return bucket


def _validate_fos_prefix(prefix: Any) -> str:
    if not isinstance(prefix, str) or (prefix and not re.fullmatch(r"^[A-Za-z0-9/_-]*$", prefix)):
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_prefix", "Invalid prefix. Use alphanumerics, /, _, -."),
        )
    return prefix


def _validate_and_normalize_cdn_url(raw_val: Any) -> tuple[str, str]:
    from urllib.parse import urlsplit

    if not isinstance(raw_val, str) or not raw_val.isascii() or any(ord(ch) < 32 or ord(ch) == 127 for ch in raw_val):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_cdn_url",
                "cdn_url must be an https:// origin URL without port, credentials, path, query, or fragment.",
            ),
        )
    raw_cdn_url = raw_val.strip(" ")
    try:
        parts = urlsplit(raw_cdn_url)
        port = parts.port
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_cdn_url",
                "cdn_url must be an https:// origin URL without port, credentials, path, query, or fragment.",
            ),
        ) from None
    domain = (parts.hostname or "").rstrip(".").lower()
    validated_domain, domain_err = _validate_public_domain_syntax(domain)
    if (
        parts.scheme.lower() != "https"
        or parts.username is not None
        or parts.password is not None
        or port is not None
        or parts.path not in ("", "/")
        or "?" in raw_cdn_url
        or "#" in raw_cdn_url
        or bool(parts.query)
        or bool(parts.fragment)
        or not domain
        or parts.netloc.rstrip(".").lower() != domain
        or domain_err is not None
        or validated_domain != domain
    ):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_cdn_url",
                "cdn_url must be an https:// origin URL without port, credentials, path, query, or fragment.",
            ),
        )
    return f"https://{domain}", domain


def _validate_provision_vcl_inputs(cfg: dict[str, Any], *, strict_rum_path: bool = True) -> None:
    """Validate custom_condition, rum_custom_condition, scoring, shield/region, and custom_fields before VCL/Terraform emission."""
    from backend.core.catalog import validate_custom_field_entry, validate_vcl_custom_condition
    from backend.core.fastly.client import _assert_faro_version_safe
    from backend.core.vcl_generator import validate_quoted_literal, validate_url_exclusion_regex
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES
    from backend.provision.rum_orchestrator import _RUM_FIELD_NAMES
    from backend.provision.session_scoring.orchestrate import _SCORING_FIELD_NAMES

    for sid_key in ("service_id", "logging_service_id"):
        sid_val = cfg.get(sid_key)
        if sid_val is not None and sid_val != "":
            if not isinstance(sid_val, str) or "/" in sid_val or "\\" in sid_val or ".." in sid_val:
                raise HTTPException(status_code=400, detail=make_error("invalid_service_id", f"Invalid {sid_key}"))
            try:
                validate_quoted_literal(sid_val, field=sid_key)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_service_id", f"Invalid {sid_key}")
                ) from None

    for container_key in ("provisioning", "rum", "fos_proxy", "cmcd", "scoring"):
        raw_sub = cfg.get(container_key)
        if raw_sub is not None and not isinstance(raw_sub, dict):
            err_code = "invalid_scoring_config" if container_key == "scoring" else "invalid_config"
            raise HTTPException(
                status_code=400,
                detail=make_error(err_code, f"{container_key} must be a JSON object"),
            )

    prov_dict: dict[str, Any] = cfg.get("provisioning") or {}
    rum_dict: dict[str, Any] = cfg.get("rum") or {}
    fos_proxy_dict: dict[str, Any] = cfg.get("fos_proxy") or {}
    cmcd_dict: dict[str, Any] = cfg.get("cmcd") or {}
    scoring_dict: dict[str, Any] = cfg.get("scoring") or {}

    cond_msg = validate_vcl_custom_condition(cfg.get("custom_condition"))
    if cond_msg:
        raise HTTPException(status_code=400, detail=make_error("invalid_custom_condition", cond_msg))

    if "custom_condition" in prov_dict:
        p_msg = validate_vcl_custom_condition(prov_dict.get("custom_condition"))
        if p_msg:
            raise HTTPException(status_code=400, detail=make_error("invalid_custom_condition", p_msg))

    r_validator = validate_rum_custom_condition if strict_rum_path else validate_vcl_custom_condition
    for rum_cond_raw in (cfg.get("rum_custom_condition"), rum_dict.get("custom_condition")):
        if rum_cond_raw is not None:
            r_msg = r_validator(rum_cond_raw)
            if r_msg:
                raise HTTPException(status_code=400, detail=make_error("invalid_rum_condition", r_msg))

    for faro_val in (cfg.get("faro_version"), rum_dict.get("faro_version")):
        if faro_val is not None:
            if not isinstance(faro_val, str):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_faro_version", "faro_version must be a plain X.Y.Z version string."),
                )
            if faro_val != "":
                try:
                    _assert_faro_version_safe(faro_val)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=make_error("invalid_faro_version", "faro_version must be a plain X.Y.Z version string."),
                    ) from None

    for shield_val in (cfg.get("cdn_shield"), prov_dict.get("cdn_shield"), fos_proxy_dict.get("shield")):
        if shield_val is not None:
            if not isinstance(shield_val, str) or (
                shield_val and not re.fullmatch(r"^[A-Za-z0-9_-]{1,64}$", shield_val)
            ):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_cdn_shield", "cdn_shield must match ^[A-Za-z0-9_-]{1,64}$."),
                )

    for reg_val in (cfg.get("fos_region"),):
        if reg_val is not None:
            if not isinstance(reg_val, str) or (reg_val and not re.fullmatch(r"^[A-Za-z0-9_-]{1,64}$", reg_val)):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_fos_region", "fos_region must match ^[A-Za-z0-9_-]{1,64}$."),
                )

    for lp_key, lp_val in (
        ("log_period", cfg.get("log_period")),
        ("provisioning.log_period", prov_dict.get("log_period")),
        ("rum_log_period", cfg.get("rum_log_period")),
        ("rum.log_period", rum_dict.get("log_period")),
    ):
        if lp_val is not None and (isinstance(lp_val, bool) or not isinstance(lp_val, (int, str))):
            raise HTTPException(
                status_code=400,
                detail=make_error("invalid_log_period", f"{lp_key} must be an integer or period string."),
            )

    for sr_key, sr_val in (
        ("sample_rate", cfg.get("sample_rate")),
        ("provisioning.sample_rate", prov_dict.get("sample_rate")),
    ):
        if sr_val is not None:
            _require_int_field(sr_val, sr_key, min_val=1, max_val=100)

    for ret_key in ("log_retention_days", "rum_retention_days"):
        if cfg.get(ret_key) is not None:
            _require_int_field(cfg[ret_key], ret_key, min_val=1, max_val=36500)

    cmcd_enabled = bool(cfg["cmcd_enabled"] if cfg.get("cmcd_enabled") is not None else cmcd_dict.get("enabled", False))
    for cm_mode in (cfg.get("cmcd_mode"), cmcd_dict.get("mode")):
        if cm_mode is not None:
            if not isinstance(cm_mode, str) or (cmcd_enabled and cm_mode not in ("query_string", "headers")):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_config", "cmcd.mode must be 'query_string' or 'headers'."),
                )
    for cv_key, cv_val in (("cmcd_version", cfg.get("cmcd_version")), ("cmcd.version", cmcd_dict.get("version"))):
        if cv_val is not None:
            parsed_cv = _require_int_field(cv_val, cv_key)
            if cmcd_enabled and parsed_cv not in (1, 2):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_config", "cmcd.version must be 1 or 2."),
                )

    scoring_enabled = bool(
        cfg["scoring_enabled"] if cfg.get("scoring_enabled") is not None else scoring_dict.get("enabled", False)
    )

    for enf_key, enf_val in (
        ("scoring_enforce_status_code", cfg.get("scoring_enforce_status_code")),
        ("scoring.enforce_status_code", scoring_dict.get("enforce_status_code")),
    ):
        if enf_val is not None:
            if not isinstance(enf_val, int) or isinstance(enf_val, bool) or not (400 <= enf_val <= 599):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_scoring_config", f"{enf_key} must be an integer in [400, 599]."),
                )

    for excl_val in (cfg.get("scoring_exclude_url_regex"), scoring_dict.get("exclude_url_regex")):
        if excl_val is not None:
            try:
                validate_url_exclusion_regex(excl_val)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_exclude_url_regex", "Invalid URL exclusion regex.")
                ) from None

    for sec_val in (cfg.get("scoring_request_secret"), scoring_dict.get("request_secret")):
        if sec_val is not None:
            try:
                validate_quoted_literal(sec_val, field="scoring.request_secret")
            except ValueError:
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_scoring_config", "Invalid scoring.request_secret.")
                ) from None

    domain_candidates = (
        cfg.get("scoring_domain"),
        scoring_dict.get("domain"),
        scoring_dict.get("scoring_domain"),
    )
    resolved_scoring_domain = ""
    for dom_val in domain_candidates:
        if dom_val is not None:
            if not isinstance(dom_val, str):
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_scoring_domain", "scoring.domain must be a string")
                )
            if dom_val:
                try:
                    validate_quoted_literal(dom_val, field="scoring.domain")
                except ValueError:
                    raise HTTPException(
                        status_code=400, detail=make_error("invalid_scoring_domain", "Invalid scoring.domain.")
                    ) from None
                if ".." in dom_val or not re.fullmatch(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$", dom_val):
                    raise HTTPException(
                        status_code=400,
                        detail=make_error("invalid_scoring_domain", "scoring.domain must be a valid hostname."),
                    )
                if not resolved_scoring_domain:
                    resolved_scoring_domain = dom_val
    if scoring_enabled and not resolved_scoring_domain:
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_scoring_domain", "scoring.domain is required when scoring is enabled."),
        )

    lf = cfg.get("log_fields")
    if isinstance(lf, dict):
        from backend.core.catalog import FASTLY_LOG_LINE_DELIVER_MAX, GROUP_INFO

        valid_group_ids = {g for g in GROUP_INFO if isinstance(g, str)} | {"CORE", "core", "INTERNAL"}
        raw_groups = lf.get("groups")
        if raw_groups is not None:
            if not isinstance(raw_groups, list):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_log_fields", "log_fields.groups must be a JSON array"),
                )
            for grp in raw_groups:
                if not isinstance(grp, str) or grp not in valid_group_ids:
                    raise HTTPException(
                        status_code=400,
                        detail=make_error("invalid_log_fields", "log_fields.groups contains an invalid group ID"),
                    )

        raw_overrides = lf.get("field_overrides")
        if raw_overrides is not None:
            if not isinstance(raw_overrides, dict):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_log_fields", "log_fields.field_overrides must be a JSON object"),
                )
            for ov_k, ov_v in raw_overrides.items():
                if not isinstance(ov_k, str) or not ov_k or not isinstance(ov_v, bool):
                    raise HTTPException(
                        status_code=400,
                        detail=make_error(
                            "invalid_log_fields", "log_fields.field_overrides must map field names to booleans"
                        ),
                    )

        raw_limits = lf.get("field_limits")
        if raw_limits is not None:
            if not isinstance(raw_limits, dict):
                raise HTTPException(
                    status_code=400,
                    detail=make_error("invalid_log_fields", "log_fields.field_limits must be a JSON object"),
                )
            for lim_k, lim_v in raw_limits.items():
                if (
                    not isinstance(lim_k, str)
                    or not lim_k
                    or not isinstance(lim_v, int)
                    or isinstance(lim_v, bool)
                    or not (1 <= lim_v <= FASTLY_LOG_LINE_DELIVER_MAX)
                ):
                    raise HTTPException(
                        status_code=400,
                        detail=make_error(
                            "invalid_log_fields", "log_fields.field_limits values must be positive integers"
                        ),
                    )

        system_names = set(_CMCD_FIELD_NAMES) | set(_RUM_FIELD_NAMES) | set(_SCORING_FIELD_NAMES)
        seen_names: list[str] = []
        raw_cfs = lf.get("custom_fields")
        if raw_cfs is not None and not isinstance(raw_cfs, list):
            raise HTTPException(
                status_code=400, detail=make_error("invalid_log_fields", "custom_fields must be a JSON array")
            )
        for cf in raw_cfs or []:
            if not isinstance(cf, dict):
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_custom_field", "Each custom field must be an object")
                )
            hard_errs = validate_custom_field_entry(cf, seen_names, system_names=system_names)
            if hard_errs:
                raise HTTPException(status_code=400, detail=make_error("invalid_custom_field", hard_errs[0]))
            if "description" in cf and cf["description"] is None:
                cf["description"] = ""
            if isinstance(cf.get("name"), str):
                seen_names.append(cf["name"])
    elif lf is not None:
        raise HTTPException(
            status_code=400, detail=make_error("invalid_log_fields", "log_fields must be a JSON object")
        )


@provision_router.post("/execute")
def provision_execute(req: ProvisionExecuteRequest):
    from backend import config as svcconfig
    from backend.core import fos_client as _db
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
        "log_retention_days": req.log_retention_days,
        "rum_retention_days": req.rum_retention_days,
        "snapshot_interval_minutes": req.snapshot_interval_minutes,
        "logging_enabled": req.logging_enabled,
        "rum_enabled": req.rum_enabled,
    }

    if req.log_fields:
        try:
            parsed_lf = json.loads(req.log_fields)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400, detail=make_error("invalid_log_fields", "log_fields must be valid JSON")
            )
        if not isinstance(parsed_lf, dict):
            raise HTTPException(
                status_code=400, detail=make_error("invalid_log_fields", "log_fields must be a JSON object")
            )
        cfg["log_fields"] = parsed_lf

    if getattr(req, "rum_custom_condition", None) is not None and not getattr(req, "rum_enabled", False):
        raw_rum_cond = (getattr(req, "rum_custom_condition", None) or "").strip()
        if raw_rum_cond:
            early_rum_err = validate_rum_custom_condition(raw_rum_cond)
            if early_rum_err:
                raise HTTPException(status_code=400, detail=make_error("invalid_rum_condition", early_rum_err))

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

    _validate_provision_vcl_inputs(cfg, strict_rum_path=True)
    from backend.provision.opaque_names import generate_opaque_bucket_name, generate_opaque_cdn_prefix

    if not (cfg.get("fos_bucket_name") or "").strip():
        cfg["fos_bucket_name"] = generate_opaque_bucket_name()
        cfg["_system_generated_bucket"] = True
    elif req.bucket_generated:
        cfg["_system_generated_bucket"] = True
    _validate_fos_bucket_name(cfg["fos_bucket_name"])
    _validate_fos_prefix(cfg.get("fos_prefix", ""))

    if cfg.get("cdn_url"):
        cfg["cdn_url"], domain = _validate_and_normalize_cdn_url(cfg["cdn_url"])
        if req.cdn_domain_generated:
            cfg["_system_generated_cdn_domain"] = True
        available, reason = _check_domain_available(domain, timeout=5)
        if not available and reason and "DNS" not in reason:
            if req.cdn_domain_generated:
                cfg["cdn_url"] = f"https://{generate_opaque_cdn_prefix()}.global.ssl.fastly.net"
            else:
                raise HTTPException(
                    status_code=400,
                    detail=make_error("domain_unavailable", reason, domain=domain),
                )
    else:
        cfg["cdn_url"] = f"https://{generate_opaque_cdn_prefix()}.global.ssl.fastly.net"
        cfg["_system_generated_cdn_domain"] = True

    cfg["cdn_secret"] = secrets.token_urlsafe(24)

    def stream():
        error_already_emitted = False
        try:
            from backend.provision import provision

            for event in provision(cfg):
                if isinstance(event, dict) and isinstance(event.get("message"), str):
                    event = {**event, "message": redact_error_text(event["message"])}
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
                                "log_retention_days": cfg.get("log_retention_days"),
                                "snapshot_interval_minutes": cfg.get("snapshot_interval_minutes"),
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
                yield _sse("error", redact_error_text(e))

    return sse_response(stream())


@provision_router.post("/reconcile")
def provision_reconcile(body: ProvisionReconcileRequest):
    """Reconcile VCL state for a service and stream status via SSE."""
    if not body.service_id or not body.token:
        raise HTTPException(status_code=400, detail=make_error("service_id and token are required"))

    from backend import config as svcconfig
    from backend.provision.declarative.reconciler import reconcile_vcl_state
    from backend.utils.sse import stream_sse_from_worker

    _validate_provision_vcl_inputs({"service_id": body.service_id}, strict_rum_path=False)
    existing_cfg = svcconfig.load_config(body.service_id)
    if isinstance(existing_cfg, dict):
        _validate_provision_vcl_inputs(existing_cfg, strict_rum_path=False)

    def worker(events):
        try:
            result = reconcile_vcl_state(
                service_id=body.service_id,
                token=body.token,
                dry_run=False,
                status_cb=lambda msg: events.put({"type": "status", "message": redact_error_text(msg)}),
            )
            events.put(
                {
                    "type": "reconciliation_result",
                    "service_id": result.service_id,
                    "activated_version": result.activated_version,
                    "changes_applied": result.changes_applied,
                    "duration_ms": result.duration_ms,
                    "error": redact_error_text(result.error) if result.error else None,
                }
            )
            events.put({"type": "done", "success": result.error is None})
        except Exception as e:
            events.put({"type": "error", "message": redact_error_text(e)})

    return stream_sse_from_worker(worker)


def _render_terraform(body: ProvisionConfigRequest) -> dict[str, str]:
    from backend.utils.terraform_gen import generate_terraform

    cfg = body.model_dump(exclude_none=True)
    raw_lf = cfg.get("log_fields")
    if isinstance(raw_lf, str):
        if raw_lf.strip():
            try:
                parsed_lf = json.loads(raw_lf)
            except json.JSONDecodeError:
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_log_fields", "log_fields must be valid JSON")
                )
            if not isinstance(parsed_lf, dict):
                raise HTTPException(
                    status_code=400, detail=make_error("invalid_log_fields", "log_fields must be a JSON object")
                )
            cfg["log_fields"] = parsed_lf
        else:
            cfg.pop("log_fields", None)
    _validate_provision_vcl_inputs(cfg, strict_rum_path=False)
    access_key = cfg.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = cfg.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    try:
        return generate_terraform(cfg, access_key, secret_key)
    except (ValueError, KeyError):
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_config", "Invalid Terraform configuration parameters"),
        ) from None


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

    body: dict[str, Any] = payload.model_dump(exclude_none=True)
    token = body.get("token")
    if not isinstance(token, str) or not token:
        raise HTTPException(status_code=400, detail={"error": "Token is required"})

    logging_service_id = body.get("service_id") or body.get("logging_service_id") or ""
    if not isinstance(logging_service_id, str) or not logging_service_id:
        raise HTTPException(status_code=400, detail=make_error("invalid_service_id", "service_id is required"))
    _validate_provision_vcl_inputs(
        {"service_id": body.get("service_id"), "logging_service_id": body.get("logging_service_id")},
        strict_rum_path=False,
    )
    try:
        svcconfig._validate_service_id(logging_service_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=make_error("invalid_service_id", "Invalid service_id")) from None

    validate_destructive_token(token, service_id=logging_service_id)
    fetch_pop_locations(token)

    existing_cfg = svcconfig.load_config(logging_service_id) or {}
    existing_fos_proxy = existing_cfg.get("fos_proxy")
    if not isinstance(existing_fos_proxy, dict):
        existing_fos_proxy = {}

    if "log_period" in body and body["log_period"] != "":
        raw_lp = body["log_period"]
        if isinstance(raw_lp, bool) or not isinstance(raw_lp, (int, str)):
            raise HTTPException(
                status_code=400,
                detail=make_error("invalid_log_period", "log_period must be an integer or period string"),
            )
        try:
            parsed_lp = parse_period(str(raw_lp))
            if not (1 <= parsed_lp <= 3600):
                raise ValueError(f"log_period must be in range [1, 3600], got {parsed_lp}")
            body["log_period"] = parsed_lp
        except ValueError as e:
            raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))
    elif body.get("log_period") == "":
        body.pop("log_period", None)

    raw_scoring = body.get("scoring")
    scoring_sub = raw_scoring if isinstance(raw_scoring, dict) else {}
    scoring_enabled_flag = body["scoring_enabled"] if "scoring_enabled" in body else scoring_sub.get("enabled", False)
    precheck_cfg: dict[str, Any] = {
        "service_id": body.get("service_id"),
        "logging_service_id": body.get("logging_service_id"),
        "custom_condition": body.get("custom_condition"),
        "provisioning": body.get("provisioning"),
        "sample_rate": body.get("sample_rate"),
        "rum_custom_condition": body.get("rum_custom_condition"),
        "rum": body.get("rum"),
        "rum_log_period": body.get("rum_log_period"),
        "faro_version": body.get("faro_version"),
        "cdn_shield": body.get("cdn_shield"),
        "fos_region": body.get("fos_region"),
        "fos_proxy": body.get("fos_proxy"),
        "cmcd": body.get("cmcd"),
        "cmcd_enabled": body.get("cmcd_enabled"),
        "cmcd_mode": body.get("cmcd_mode"),
        "cmcd_version": body.get("cmcd_version"),
        "scoring": raw_scoring,
        "scoring_enabled": bool(scoring_enabled_flag),
        "scoring_domain": body.get("scoring_domain"),
        "scoring_request_secret": body.get("scoring_request_secret"),
        "scoring_exclude_url_regex": body.get("scoring_exclude_url_regex"),
        "scoring_enforce_status_code": body.get("scoring_enforce_status_code"),
        "log_retention_days": body.get("log_retention_days"),
        "rum_retention_days": body.get("rum_retention_days"),
    }
    parsed_ingest_lf: dict[str, Any] | None = None
    if "log_fields" in body:
        log_fields_raw = body.get("log_fields")
        if isinstance(log_fields_raw, str):
            if log_fields_raw.strip():
                try:
                    loaded_lf = json.loads(log_fields_raw)
                except json.JSONDecodeError:
                    raise HTTPException(
                        status_code=400, detail=make_error("invalid_log_fields", "log_fields must be valid JSON")
                    )
                if not isinstance(loaded_lf, dict):
                    raise HTTPException(
                        status_code=400, detail=make_error("invalid_log_fields", "log_fields must be a JSON object")
                    )
                parsed_ingest_lf = loaded_lf
        elif isinstance(log_fields_raw, dict):
            parsed_ingest_lf = log_fields_raw
        else:
            raise HTTPException(
                status_code=400, detail=make_error("invalid_log_fields", "log_fields must be a JSON object")
            )
        if parsed_ingest_lf is not None:
            precheck_cfg["log_fields"] = parsed_ingest_lf
    _validate_provision_vcl_inputs(precheck_cfg, strict_rum_path=True)

    if "fos_bucket_name" in body:
        _validate_fos_bucket_name(body["fos_bucket_name"])
    if "fos_prefix" in body:
        _validate_fos_prefix(body["fos_prefix"])
    if body.get("cdn_url"):
        body["cdn_url"], _ = _validate_and_normalize_cdn_url(body["cdn_url"])

    fos_access_key = body.get("fos_access_key")
    fos_secret_key = body.get("fos_secret_key")
    fos_key_id = ""

    if not fos_access_key or not fos_secret_key:
        from backend.provision.opaque_names import generate_opaque_label

        bucket_name = body.get("fos_bucket_name")
        legacy_descs = [
            *([] if not bucket_name else [f"fos-log-analysis-{bucket_name}"]),
            f"fos-log-analysis-{logging_service_id}",
        ]
        existing = None
        matched_desc = ""
        for candidate_desc in legacy_descs:
            existing = find_fos_key(candidate_desc, token)
            if existing:
                matched_desc = candidate_desc
                break
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
                    f"An access key '{matched_desc}' already exists but its secret is not retrievable; "
                    "provide fos_access_key + fos_secret_key explicitly.",
                ),
            )
        else:
            try:
                if not bucket_name:
                    raise HTTPException(status_code=400, detail={"error": "fos_bucket_name required"})
                opaque_desc = f"fos-log-analysis-{generate_opaque_label('key')}"
                new_key = ensure_fos_access_key(opaque_desc, body, token, buckets=[bucket_name])
                fos_access_key = new_key["access_key"]
                fos_secret_key = new_key["secret_key"]
                fos_key_id = new_key["id"]
            except HTTPException:
                raise
            except Exception as e:
                raise_internal(logger, e, code="ensure_access_key_failed", status=400)

    if not body.get("cdn_secret"):
        body["cdn_secret"] = (
            existing_cfg.get("cdn_secret") or existing_fos_proxy.get("secret") or secrets.token_urlsafe(24)
        )

    state: dict[str, Any] = {
        "admin_token": token,
        "logging_service_id": logging_service_id,
        "name": body.get("service_name") or logging_service_id,
        "endpoint_name": body.get("endpoint_name", "Fastly Object Storage Logs"),
        "fos_region": body.get("fos_region") or "us-east-1",
        "fos_bucket_name": body.get("fos_bucket_name") or existing_cfg.get("fos_bucket", ""),
        "fos_prefix": body.get("fos_prefix", ""),
        "fos_access_key_id": fos_access_key,
        "fos_secret_access_key": fos_secret_key,
        "sample_rate": body.get("sample_rate", "100"),
        "edge_only": body.get("edge_only", True),
        "custom_condition": body.get("custom_condition", ""),
        "log_period": body.get("log_period", DEFAULT_LOG_PERIOD_SECONDS),
        "cdn_service_name": body.get("cdn_service_name"),
        "cdn_url": body.get("cdn_url") or "",
        "cdn_shield": body.get("cdn_shield") or "none",
        "cdn_secret": body.get("cdn_secret"),
        "enable_cron_sync": body.get("enable_cron_sync", True),
        "delete_after": body.get("delete_after", True),
        "log_retention_days": int(body.get("log_retention_days", 30)),
        "rum_retention_days": int(body.get("rum_retention_days", 30)),
        "provisioning": {"fos_key_id": fos_key_id},
    }
    if "logging_enabled" in body:
        state["logging_enabled"] = bool(body["logging_enabled"])
    if "rum_enabled" in body:
        state["rum_enabled"] = bool(body["rum_enabled"])
    if body.get("snapshot_interval_minutes") is not None:
        state["snapshot_interval_minutes"] = body["snapshot_interval_minutes"]

    if parsed_ingest_lf is not None:
        state["log_fields"] = parsed_ingest_lf

    now_iso = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
    raw_cmcd = body.get("cmcd")
    if any(k in body for k in ("cmcd_enabled", "cmcd_mode", "cmcd_version")) or isinstance(raw_cmcd, dict):
        existing_cmcd: dict[str, Any] = raw_cmcd if isinstance(raw_cmcd, dict) else {}
        raw_cv = body["cmcd_version"] if "cmcd_version" in body else existing_cmcd.get("version")
        state["cmcd"] = {
            "enabled": bool(body.get("cmcd_enabled", existing_cmcd.get("enabled", False))),
            "mode": body.get("cmcd_mode") or existing_cmcd.get("mode") or "query_string",
            "version": int(raw_cv) if raw_cv is not None else 1,
            "enabled_at": existing_cmcd.get("enabled_at") or now_iso,
        }

    scoring_keys = (
        "scoring_enabled",
        "scoring_domain",
        "scoring_request_secret",
        "scoring_exclude_url_regex",
        "scoring_enforce_status_code",
    )
    raw_scoring = body.get("scoring")
    if any(k in body for k in scoring_keys) or isinstance(raw_scoring, dict):
        existing_scoring: dict[str, Any] = raw_scoring if isinstance(raw_scoring, dict) else {}
        raw_enf = (
            body["scoring_enforce_status_code"]
            if "scoring_enforce_status_code" in body
            else existing_scoring.get("enforce_status_code")
        )
        state["scoring"] = {
            "enabled": bool(body.get("scoring_enabled", existing_scoring.get("enabled", False))),
            "domain": body.get("scoring_domain")
            or existing_scoring.get("domain")
            or existing_scoring.get("scoring_domain")
            or "",
            "request_secret": (
                body["scoring_request_secret"]
                if "scoring_request_secret" in body
                else (existing_scoring.get("request_secret") or "")
            ),
            "exclude_url_regex": (
                body["scoring_exclude_url_regex"]
                if "scoring_exclude_url_regex" in body
                else (existing_scoring.get("exclude_url_regex") or "")
            ),
            "enforce_status_code": 429 if raw_enf is None else raw_enf,
            "enabled_at": existing_scoring.get("enabled_at") or now_iso,
        }

    raw_rum = body.get("rum")
    incoming_rum: dict[str, Any] = raw_rum if isinstance(raw_rum, dict) else {}
    rum_is_on = (
        bool(incoming_rum["enabled"])
        if incoming_rum.get("enabled") is not None
        else bool(body.get("rum_enabled", False))
    )
    if rum_is_on:
        state["rum_enabled"] = True
        raw_on_disk_rum = existing_cfg.get("rum")
        on_disk_rum: dict[str, Any] = raw_on_disk_rum if isinstance(raw_on_disk_rum, dict) else {}
        if "rum_custom_condition" in body:
            resolved_rum_cond = body["rum_custom_condition"].strip()
        elif incoming_rum.get("custom_condition") is not None:
            resolved_rum_cond = incoming_rum["custom_condition"].strip()
        elif isinstance(on_disk_rum.get("custom_condition"), str) and not validate_rum_custom_condition(
            on_disk_rum.get("custom_condition")
        ):
            resolved_rum_cond = on_disk_rum["custom_condition"].strip()
        else:
            resolved_rum_cond = ""
        state["rum"] = {
            "enabled": True,
            "enabled_at": incoming_rum.get("enabled_at") or on_disk_rum.get("enabled_at") or now_iso,
            "custom_condition": resolved_rum_cond,
        }
        if incoming_rum.get("faro_version"):
            faro_src: dict[str, Any] = incoming_rum
        elif body.get("faro_version"):
            faro_src = {"faro_version": body["faro_version"]}
        else:
            faro_src = on_disk_rum
        if faro_src.get("faro_version"):
            for k in ("faro_version", "faro_content_hash", "faro_fos_etag_md5", "faro_last_upstream_check"):
                if k in faro_src:
                    state["rum"][k] = faro_src[k]

    write_service_config(state)

    try:
        from backend.high_scale.ingest_controller import (
            HighScaleServiceRegistration,
            get_high_scale_service_registry,
        )

        svc_id = state.get("logging_service_id") or ""
        if svc_id:
            get_high_scale_service_registry().register(
                HighScaleServiceRegistration(
                    service_id=svc_id,
                    source_bucket=state.get("fos_bucket_name") or "",
                    source_prefix=state.get("fos_prefix") or "",
                    delete_raw_after_commit=bool(state.get("delete_after", True)),
                )
            )
    except Exception:
        logger.debug("[provision_ingest] high-scale registry registration skipped/failed", exc_info=True)

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
    authorization: str | None = Header(default=None),
    service_id: str | None = Query(default=None),
    cdn_service_id: str = Query(...),
    bucket: str = Query(...),
):
    """Verify that both services are correctly configured for log analysis."""
    token = _require_bearer_token(authorization)
    service_id = require_service_id(service_id)
    from backend.core.fastly.client import fastly

    results = {
        "logging_service": {"ok": False, "details": ""},
        "cdn_service": {"ok": False, "details": ""},
    }

    try:
        try:
            active_ver = fastly("GET", f"/service/{service_id}/version/active", token=token).get("number")
            endpoints = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/fos", token=token)
            endpoint = next((ep for ep in endpoints if ep.get("bucket_name") == bucket), None)

            if not endpoint:
                results["logging_service"]["details"] = f"No logging endpoint found pointing to bucket '{bucket}'"
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
            results["logging_service"]["details"] = f"Error checking logging service: {redact_error_text(e)}"

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
            results["cdn_service"]["details"] = f"Error checking CDN service: {redact_error_text(e)}"
    except Exception as e:
        logger.error("[provision-check-config] Global error: %s", e)

    return results


def _require_ngwaf_token(
    service_id: str,
    authorization: str | None,
    *,
    missing_message: str = "A Fastly API token is required.",
) -> str:
    """Resolve and validate a caller-supplied Fastly token for NGWAF operations."""
    from backend.core.vcl_generator import validate_destructive_token

    resolved = _require_bearer_token(authorization)
    validate_destructive_token(resolved, service_id=service_id)
    return resolved


@provision_router.get("/ngwaf-workspaces", response_model=NgwafWorkspacesResponse, response_model_exclude_unset=True)
def provision_ngwaf_workspaces(
    service_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
):
    """List NGWAF workspaces for a service (requires validated Fastly token)."""
    from backend.core.fastly.client import NgwafWorkspacesResponse
    from backend.provision import fastly
    from backend.utils.router_utils import format_debug_request

    service_id = require_service_id(service_id)
    token = _require_ngwaf_token(
        service_id,
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
    authorization: str | None = Header(default=None),
):
    """Persist the NGWAF workspace ID for a service and reload the scheduler."""
    from backend.utils.router_utils import load_service_config, mutate_service_config

    load_service_config(service_id)
    _require_ngwaf_token(service_id, authorization)

    workspace_id = (body.ngwaf_workspace_id or "").strip() or None
    mutate_service_config(service_id, lambda c: c.__setitem__("ngwaf_workspace_id", workspace_id))

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        logger.warning("provision_set_ngwaf_workspace: _sync_crontab failed (ignored)", exc_info=True)

    return {"ok": True, "ngwaf_workspace_id": workspace_id}
