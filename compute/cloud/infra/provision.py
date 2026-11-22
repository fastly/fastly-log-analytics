#!/usr/bin/env python3
"""CLI-only installation of the app shell; FOS provisioning belongs to Goal 2."""

from __future__ import annotations

import argparse
import base64
import datetime
import getpass
import hashlib
import ipaddress
import json
import logging
import os
import re
import secrets
import subprocess
import sys
import tarfile
import time
import urllib.parse
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

_FILE_RESOLVED = Path(__file__).resolve()
REPO_ROOT = (
    _FILE_RESOLVED.parents[3]
    if len(_FILE_RESOLVED.parents) >= 4 and _FILE_RESOLVED.parents[2].name == "compute"
    else _FILE_RESOLVED.parents[2]
)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.core.fastly.client import API_BASE, SHIELD_MAP, region_endpoint  # noqa: E402
from backend.core.retry import retry_call  # noqa: E402
from backend.provision import cli as _cli  # noqa: E402
from backend.provision import fos_setup as _fos_setup  # noqa: E402
from backend.provision import stores as _stores  # noqa: E402
from backend.provision import wasm_deploy as _wasm_deploy  # noqa: E402
from backend.utils import terraform_gen as _terraform_gen  # noqa: E402
from scripts.lib import environments as _environments  # noqa: E402
from scripts.lib import readiness as _readiness  # noqa: E402

FOS_REGIONS = tuple(SHIELD_MAP.keys())

VCL = _FILE_RESOLVED.parents[1] / "vcl" / "front"
LOG = logging.getLogger("flp.bootstrap")
LINKS = {"secret": "flp_secrets", "config": "flp_config", "kv": "flp_state"}
DICTIONARIES = {
    "front_auth": ("flp_front_auth", True),
    "buckets": ("flp_fos_buckets", False),
    "credentials": ("flp_fos_credentials", True),
}

HTTP_TRANSPORT: httpx.BaseTransport | None = None
# The ~25 MB package upload has taken over 90 s; Fastly ingests it slowly.
PACKAGE_UPLOAD_TIMEOUT_S = 300.0
PACKAGE_UPLOAD_ATTEMPTS = 3
API_READ_ATTEMPTS = 3
API_RETRY_BASE_DELAY_S = 1.0


def _http_client(*, timeout: float, transport: httpx.BaseTransport | None = None) -> httpx.Client:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    active_transport = transport if transport is not None else HTTP_TRANSPORT
    return httpx.Client(
        timeout=timeout,
        transport=active_transport,
        follow_redirects=False,
        trust_env=False,
    )


class APIError(RuntimeError):
    """An API failure with no server body or credential material."""


class FastlyAPI:
    def __init__(
        self,
        token: str,
        *,
        transport: httpx.BaseTransport | None = None,
        attempts: int = 1,
    ) -> None:
        if not token:
            raise ValueError("A Fastly API token is required")
        self._token = token
        self._transport = transport
        self._attempts = max(1, attempts)
        self._run_id = uuid.uuid4().hex[:12]

    def request(self, method: str, path: str, payload: Any = None, *, form: bool = False) -> Any:
        if not path.startswith("/") or "://" in path or "\n" in path:
            raise ValueError("API path must be relative to api.fastly.com")
        data = None
        headers = {"Fastly-Key": self._token, "Accept": "application/json"}
        if payload is not None:
            data = str(httpx.QueryParams(payload)).encode() if form else json.dumps(payload).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
        is_idempotent_read = method.upper() in {"GET", "HEAD"}
        return self._send(
            method,
            path,
            data,
            headers,
            attempts=API_READ_ATTEMPTS if is_idempotent_read else None,
            retry_base_delay=API_RETRY_BASE_DELAY_S if is_idempotent_read else 0.0,
        )

    def upload_package(self, service_id: str, version: int, package: Path) -> Any:
        boundary = "flp-" + uuid.uuid4().hex
        data = (
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="package"; filename="flp.tar.gz"\r\n'
                "Content-Type: application/gzip\r\n\r\n"
            ).encode()
            + package.read_bytes()
            + f"\r\n--{boundary}--\r\n".encode()
        )
        return self._send(
            "PUT",
            f"/service/{service_id}/version/{version}/package",
            data,
            {
                "Fastly-Key": self._token,
                "Accept": "application/json",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Expect": "100-continue",
            },
            timeout=PACKAGE_UPLOAD_TIMEOUT_S,
            attempts=PACKAGE_UPLOAD_ATTEMPTS,
            retry_base_delay=API_RETRY_BASE_DELAY_S,
        )

    def _send(
        self,
        method: str,
        path: str,
        data: bytes | None,
        headers: dict[str, str],
        *,
        timeout: float = 90.0,
        attempts: int | None = None,
        retry_base_delay: float = 0.0,
    ) -> Any:
        started = time.monotonic()
        status: int | None = None
        received = 0
        error_type: str | None = None
        url = API_BASE + path

        def _do_request() -> httpx.Response:
            with _http_client(timeout=timeout, transport=self._transport) as client:
                return client.request(method, url, content=data, headers=headers)

        retry_attempts = self._attempts if attempts is None else max(1, attempts)
        retry_count = 0

        def _before_retry(exc: BaseException) -> None:
            nonlocal retry_count
            retry_count += 1
            LOG.warning(
                json.dumps(
                    {
                        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                        "event": "fastly.api_retry",
                        "run_id": self._run_id,
                        "method": method,
                        "path": path.split("?")[0],
                        "attempt": retry_count + 1,
                        "max_attempts": retry_attempts,
                        "error_type": type(exc).__name__,
                    }
                )
            )

        try:
            response = retry_call(
                _do_request,
                retry_on=lambda exc: isinstance(exc, (httpx.TransportError, TimeoutError)),
                attempts=retry_attempts,
                base_delay=retry_base_delay,
                sleep=time.sleep,
                before_retry=_before_retry,
            )
            status = response.status_code
            body = response.content
            received = len(body)
            if status >= 400:
                raise APIError(f"Fastly {method} {path.split('?')[0]} returned {status}")
            return json.loads(body) if body else None
        except APIError as exc:
            error_type = type(exc).__name__
            raise
        except (httpx.HTTPError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            error_type = type(exc).__name__
            raise APIError(f"Fastly {method} {path.split('?')[0]} failed") from None
        finally:
            LOG.log(
                (logging.INFO if error_type is None and status is not None and status < 400 else logging.ERROR),
                json.dumps(
                    {
                        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                        "event": "fastly.api",
                        "run_id": self._run_id,
                        "method": method,
                        "path": path.split("?")[0],
                        "status": status,
                        "duration_ms": round((time.monotonic() - started) * 1000, 1),
                        "received_bytes": received,
                        "error_type": error_type,
                    }
                ),
            )


def domain(host: str) -> str:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError("Backend must use a domain, not an IP address")
    if (
        len(host) > 253
        or "." not in host
        or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host)
        or any(
            not label or len(label) > 63 or label.startswith("-") or label.endswith("-") for label in host.split(".")
        )
    ):
        raise ValueError("Expected a lowercase domain name without a scheme or path")
    return host


def backend(name: str, host: str, *, shield: bool = False) -> dict[str, str]:
    domain(host)
    fields = {
        "name": name,
        "address": host,
        "override_host": host,
        "ssl_sni_hostname": host,
        "ssl_cert_hostname": host,
        "use_ssl": "1",
        "ssl_check_cert": "1",
        "port": "443",
        "prefer_ipv6": "0",
        "connect_timeout": "5000",
        "first_byte_timeout": "60000",
        "between_bytes_timeout": "60000",
    }
    if shield:
        fields["shield"] = SHIELD_MAP["us-east-1"]
    return fields


# The front service's health checks are the clocks: `/_tick` drives ingest and maintenance,
# `/_flush` the operational-log flush in its own Compute execution. The VCL caches each at the
# IAD shield for 1 s, or for the sleep TTL flp returns in `x-flp-tick-ttl` (up to 60 s), so at
# most one probe a second reaches flp, whose dispatcher decides whether anything runs.
APP_CONDITION = 'req.url.path != "/_tick" && req.url.path != "/_flush" && req.url.path !~ "^/fos(/|$)"'
TICK_CONDITION = 'req.url.path == "/_tick" || req.url.path == "/_flush"'


def clock_healthcheck(name: str, host: str, path: str) -> dict[str, str]:
    return {
        "name": name,
        "host": domain(host),
        "path": path,
        "method": "GET",
        "check_interval": "1000",
        "timeout": "5000",
        "threshold": "1",
        "window": "2",
        "initial": "1",
        "expected_response": "200",
        "http_version": "1.1",
    }


def flush_clock_backend(host: str) -> dict[str, str]:
    return {
        **backend("flp_flush_clock", host),
        "request_condition": "flp_clock_never",
        "healthcheck": "flp_flush",
    }


def ensure_flush_clock(api: Any, base: str) -> None:
    """Adds the `/_flush` health check and its backend to a front draft that predates them."""
    checks = {item["name"]: item for item in api.request("GET", base + "/healthcheck")}
    host = checks["flp_clock"]["host"]
    if "flp_flush" not in checks:
        api.request(
            "POST",
            base + "/healthcheck",
            clock_healthcheck("flp_flush", host, "/_flush"),
            form=True,
        )
    backends = {item["name"] for item in api.request("GET", base + "/backend")}
    if "flp_flush_clock" not in backends:
        api.request("POST", base + "/backend", flush_clock_backend(host), form=True)


def resource_plan(
    name: str = "flp",
    *,
    public_host: str | None = None,
    compute_host: str | None = None,
    worker_host: str | None = None,
) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", name) or name.endswith("-"):
        raise ValueError("Install name must be 1–40 lowercase letters, digits or hyphens")
    hosts = {
        "front": domain(public_host or f"{name}.global.ssl.fastly.net"),
        "compute": domain(compute_host or f"{name}.edgecompute.app"),
        "worker": domain(worker_host or f"{name}-worker.edgecompute.app"),
    }
    raw_prefix = _cli.ANALYTICS_LOG_LAYOUT.split("year=")[0]
    return {
        "install_name": name,
        "services": {
            "compute": name,
            "worker": f"{name}-worker",
            "front": f"{name}-front",
        },
        "stores": {
            "secret": f"{name}_secrets",
            "config": f"{name}_config",
            "kv": f"{name}_state",
        },
        "links": LINKS.copy(),
        "domains": hosts,
        "public_base_url": "https://" + hosts["front"],
        "managed_prefix": f"{name}-",
        "backends": {
            "compute": [
                backend("api_fastly", "api.fastly.com"),
                backend("flp_worker", hosts["worker"]),
                backend("flp_front", hosts["front"]),
                *[
                    backend(
                        "fos_" + region.replace("-", "_"),
                        region_endpoint(region),
                    )
                    for region in FOS_REGIONS
                ],
            ],
            "worker": [
                backend("api_fastly", "api.fastly.com"),
                backend("flp_compute", hosts["compute"]),
                backend("flp_front", hosts["front"]),
                *[
                    backend(
                        "fos_" + region.replace("-", "_"),
                        region_endpoint(region),
                    )
                    for region in FOS_REGIONS
                ],
            ],
            "front": [
                {
                    **backend("flp", hosts["compute"], shield=True),
                    "request_condition": "flp_app",
                },
                {
                    **backend("flp_tick", hosts["compute"], shield=True),
                    "request_condition": "flp_tick",
                },
                {
                    **backend("flp_clock", hosts["front"]),
                    "request_condition": "flp_clock_never",
                    "healthcheck": "flp_clock",
                },
                flush_clock_backend(hosts["front"]),
            ],
        },
        "healthcheck": clock_healthcheck("flp_clock", hosts["front"], "/_tick"),
        "flush_healthcheck": clock_healthcheck("flp_flush", hosts["front"], "/_flush"),
        "dictionaries": {key: {"name": value[0], "write_only": value[1]} for key, value in DICTIONARIES.items()},
        "config_defaults": {
            "raw_prefix": raw_prefix,
            "out_prefix": "parquet/request/",
            "window_s": "120",
            "grace_s": "180",
            "clock_interval_s": "1",
            "run_interval_s": "120",
        },
        "secret_keys": [
            "session_key",
            "front_key",
            "cdn_key",
            "worker_secret",
            "admin_setup_hash",
            "fastly_token",
        ],
        "activation": "not performed; deploy frozen Compute packages separately",
        "fos": "one logged service; bucket, keys and front region routing provisioned in-app",
    }


def prepare_storage_backends(api: Any, base: str, front_host: str) -> None:
    desired = [
        backend("flp_front", front_host),
        *[
            backend(
                "fos_" + region.replace("-", "_"),
                region_endpoint(region),
            )
            for region in FOS_REGIONS
        ],
    ]
    existing = {item["name"]: item for item in api.request("GET", base + "/backend")}
    for fields in desired:
        current = existing.get(fields["name"])
        if current is None:
            api.request("POST", base + "/backend", fields, form=True)
        elif any(
            str(current.get(key)) != value
            for key, value in fields.items()
            if key
            in (
                "address",
                "override_host",
                "ssl_sni_hostname",
                "ssl_cert_hostname",
            )
        ):
            raise ValueError("Existing storage backend identity differs; refusing replacement")
        elif any(
            str(current.get(key)).lower() not in (value, "true" if value == "1" else "false")
            for key, value in fields.items()
            if key in ("use_ssl", "ssl_check_cert", "prefer_ipv6")
        ):
            api.request("PUT", base + "/backend/" + fields["name"], fields, form=True)


def setup_code() -> tuple[str, str]:
    code = secrets.token_urlsafe(24)
    return code, json.dumps(
        {
            "sha256": hashlib.sha256(code.encode()).hexdigest(),
            "expires_at": int(time.time()) + 900,
        }
    )


def initial_secrets(api_token: str) -> dict[str, str]:
    if not api_token:
        raise ValueError("A Fastly API token is required")
    # Expired placeholder: the first deploy issues the only initial setup code.
    placeholder = json.dumps(
        {
            "sha256": hashlib.sha256(secrets.token_bytes(32)).hexdigest(),
            "expires_at": 0,
        }
    )
    return {
        **{key: secrets.token_urlsafe(48) for key in ("session_key", "front_key", "cdn_key", "worker_secret")},
        "admin_setup_hash": placeholder,
        "fastly_token": api_token,
    }


EARLY_HINTS_MAX_BYTES = 4096
_EARLY_HINTS_BLOCK = re.compile(r"(?ms)^#EARLYHINTS_BEGIN\n.*?^#EARLYHINTS_END\n")
_BUILD_KEY_BLOCK = re.compile(r"(?ms)^#BUILDKEY_BEGIN\n.*?^#BUILDKEY_END\n")
_EARLY_HINT_ROUTE = re.compile(r"/(?:[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*)?")
_EARLY_HINT_LINK = re.compile(r"</(?!/)[A-Za-z0-9._~/%-]+>(?:; [a-z]+(?:=[A-Za-z0-9/._+-]+)?)+")
_EARLY_HINT_RESERVED = {"api", "auth", "fos", "_tick", "_flush", "_next", "duckdb", "geo"}


def early_hints_size(links: list[str]) -> int:
    """Bytes of the 103 header block: one `Link:` line (with CRLF) per hinted resource."""
    return sum(len("Link: ") + len(link) + 2 for link in links)


def _hint_dispatch(routes: list[tuple[str, str]]) -> list[str]:
    """Route chain on the normalized path: /x, /x/ and /x.html share /x; "" means /."""
    lines = [
        "  declare local var.flp_hint_route STRING;",
        '  set var.flp_hint_route = regsub(req.url.path, "(\\.html|/)$", "");',
        '  if (var.flp_hint_route == "") {',
        '    set var.flp_hint_route = "/";',
        "  }",
    ]
    for index, (route, body) in enumerate(routes):
        keyword = "if" if index == 0 else "} else if"
        lines.append(f'  {keyword} (var.flp_hint_route == "{route}") {{')
        lines.append(body)
    if routes:
        lines.append("  }")
    return lines


def early_hints_vcl(hints: dict[str, Any]) -> str:
    """Render the hint subs; every value is allowlisted so nothing can escape its VCL string.

    flp_early_hints (vcl_recv) passes each resource as its own early_hints() argument, one Link
    line each as in Fastly's example; falco's linter rejects a single-argument call, so routes
    need at least two links. flp_hint_links (vcl_deliver) repeats them as one Link header on the
    final HTML for clients that ignore 103 preloads (Safari, HTTP/1.1).
    """
    if not isinstance(hints, dict) or hints.get("version") != 1 or not isinstance(hints.get("routes"), dict):
        raise ValueError("Early hints must be a version 1 route map")
    early, final = [], []
    for route, links in sorted(hints["routes"].items()):
        if (
            not isinstance(route, str)
            or not _EARLY_HINT_ROUTE.fullmatch(route)
            or route.split("/")[1] in _EARLY_HINT_RESERVED
        ):
            raise ValueError(f"Unsafe early hints route: {route!r}")
        if not isinstance(links, list) or len(links) < 2:
            raise ValueError(f"Early hints for {route} need at least two links")
        if not all(isinstance(link, str) and _EARLY_HINT_LINK.fullmatch(link) for link in links):
            raise ValueError(f"Unsafe early hints link for {route}")
        if early_hints_size(links) > EARLY_HINTS_MAX_BYTES:
            raise ValueError(f"Early hints for {route} exceed {EARLY_HINTS_MAX_BYTES} bytes")
        args = ",\n      ".join(f'{{"Link: {link}"}}' for link in links)
        early.append((route, f"    early_hints(\n      {args}\n    );"))
        final.append((route, f'    set resp.http.Link = {{"{", ".join(links)}"}};'))
    return "\n".join(
        [
            "#EARLYHINTS_BEGIN",
            "sub flp_early_hints {",
            *_hint_dispatch(early),
            "}",
            "",
            "sub flp_hint_links {",
            *_hint_dispatch(final),
            "}",
            "#EARLYHINTS_END",
            "",
        ]
    )


def build_key_vcl(build_sha: str) -> str:
    if not re.fullmatch(r"[a-f0-9]{40}", build_sha):
        raise ValueError("Front build key needs a 40-character lowercase build SHA")
    return "\n".join(
        [
            "#BUILDKEY_BEGIN",
            "sub flp_build_hash {",
            f'  set req.hash += "build:{build_sha}";',
            "}",
            "#BUILDKEY_END",
            "",
        ]
    )


def load_vcl(
    rate_limiting: bool = True,
    *,
    source: str | None = None,
    early_hints: dict[str, Any] | None = None,
    build_sha: str | None = None,
) -> str:
    content = (VCL / "main.vcl").read_text() if source is None else source
    if build_sha:
        # Builds from before edge-cached app shells have no block and never cache them.
        rendered_key = build_key_vcl(build_sha)
        content = _BUILD_KEY_BLOCK.sub(lambda _: rendered_key, content, count=1)
    if early_hints is not None:
        rendered = early_hints_vcl(early_hints)
        content, count = _EARLY_HINTS_BLOCK.subn(lambda _: rendered, content)
        if count != 1:
            raise ValueError("Front VCL has no early hints block to fill")
    if not rate_limiting:
        content = re.sub(
            r"(?m)^[ \t]*#[ \t]*RATELIMIT_BEGIN\n.*?^[ \t]*#[ \t]*RATELIMIT_END\n",
            "",
            content,
            flags=re.DOTALL,
        )
        content = content.replace(
            'set req.http.x-flp-front-rl = "1";',
            'set req.http.x-flp-front-rl = "0";',
        )
    return content


def lint_front(
    rate_limiting: bool,
    *,
    source: str | None = None,
    early_hints: dict[str, Any] | None = None,
    build_sha: str | None = None,
) -> None:
    path = VCL / "tests" / f".lint-{uuid.uuid4()}.vcl"
    try:
        path.write_text(
            _cli.VclBuilder.join_lines(
                (
                    'table flp_front_auth {\n  "front_key": "test-not-live",\n}',
                    "table flp_fos_buckets {}",
                    "table flp_fos_credentials {}",
                    load_vcl(rate_limiting, source=source, early_hints=early_hints, build_sha=build_sha),
                )
            )
        )
        subprocess.run(["falco", "lint", str(path)], check=True, capture_output=True)
    finally:
        path.unlink(missing_ok=True)


def account_has_rate_limiting(api: Any, *service_ids: str) -> bool | None:
    """Port of pinned v3 detection; an inaccessible service is not proof of no ERL."""
    for service_id in service_ids:
        if not service_id:
            continue
        try:
            versions = api.request("GET", f"/service/{service_id}/version")
            active = next(
                (version for version in versions if version.get("active") in (True, 1, "1", "true")),
                None,
            )
            if active is None:
                continue
            result = api.request(
                "GET",
                f"/service/{service_id}/version/{active['number']}/generated_vcl",
            )
            content = result.get("content", "")
            if content:
                return bool(re.search(r"ratelimit_opt_in\s+true\b", content))
        except APIError:
            continue
    return None


def put_secret(api: Any, store_id: str, name: str, value: str) -> None:
    secret_endpoint = _stores.STORE_ENDPOINTS["secret"]
    api.request(
        "PUT",
        f"{secret_endpoint}/{store_id}/secrets",
        {"name": name, "secret": base64.b64encode(value.encode()).decode()},
    )


def ensure_fos_bucket(
    name: str,
    region: str,
    access_key: str,
    secret_key: str,
    status_cb: Any = None,
    *,
    service_id: str | None = None,
) -> bool:
    return _fos_setup.ensure_fos_bucket(
        name,
        region,
        access_key,
        secret_key,
        status_cb=status_cb,
        service_id=service_id,
    )


def render_terraform(
    cfg: dict[str, Any],
    fos_access_key: str = "replace-with-fos-access-key",
    fos_secret_key: str = "replace-with-fos-secret-key",
) -> dict[str, str]:
    return _terraform_gen.generate_cloud_terraform(cfg, fos_access_key, fos_secret_key)


def admin_reset(api: Any, store_id: str) -> str:
    code, setup = setup_code()
    put_secret(api, store_id, "admin_setup_hash", setup)
    return code


def prepare_front(
    api: Any,
    inventory_path: Path,
    *,
    rate_limiting: bool | None = None,
    frozen_source: str | None = None,
    early_hints: dict[str, Any] | None = None,
    build_sha: str | None = None,
) -> dict[str, Any]:
    """Prepare and validate a front draft; activation remains the release wrapper's job."""
    inventory = json.loads(inventory_path.read_text())
    if build_sha is None:
        build_sha = inventory.get("build_sha") or None
    lint_front(
        inventory["rate_limiting"] if rate_limiting is None else rate_limiting,
        source=frozen_source,
        early_hints=early_hints,
        build_sha=build_sha,
    )
    front_service = inventory["services"]["front"]
    service_id = front_service["id"]
    remote = api.request("GET", f"/service/{service_id}")
    if remote["name"] != front_service["name"] or remote["type"] != "vcl":
        raise ValueError("Front inventory identity does not match the remote service")
    if rate_limiting is None:
        rate_limiting = account_has_rate_limiting(api, service_id)
        if rate_limiting is None:
            rate_limiting = inventory["rate_limiting"]
        lint_front(rate_limiting, source=frozen_source, early_hints=early_hints, build_sha=build_sha)
    versions = api.request("GET", f"/service/{service_id}/version")
    latest = max(versions, key=lambda version: int(version["number"]))
    if latest.get("active") in (True, 1, "1", "true") or latest.get("locked") in (
        True,
        1,
        "1",
        "true",
    ):
        latest = api.request("PUT", f"/service/{service_id}/version/{latest['number']}/clone")
    version = int(latest["number"])
    front_service["version"] = version
    inventory["activated"] = False
    inventory["configured"] = False
    inventory["rate_limiting"] = rate_limiting
    _save(inventory_path, inventory)
    base = f"/service/{service_id}/version/{version}"
    content = load_vcl(rate_limiting, source=frozen_source, early_hints=early_hints, build_sha=build_sha)
    api.request(
        "PUT",
        base + "/vcl/main",
        {"content": content, "main": "true"},
        form=True,
    )
    if "req.http.x-flp-fos-service" in content:
        api.request(
            "PUT",
            base + "/condition/flp_app",
            {"statement": APP_CONDITION},
            form=True,
        )
    api.request("PUT", base + "/condition/flp_tick", {"statement": TICK_CONDITION}, form=True)
    ensure_flush_clock(api, base)
    validation = api.request("GET", base + "/validate")
    if validation.get("status") != "ok":
        raise APIError("Front draft validation failed; no version was activated")
    inventory["configured"] = True
    _save(inventory_path, inventory)
    return inventory


_git = _wasm_deploy._git


def verify_provenance(root: Path, package: Path) -> dict[str, str]:
    return _wasm_deploy.verify_package_provenance(root, package, git_fn=_git)


def wait_for_front(
    host: str,
    build_sha: str,
    *,
    timeout_s: float = 180,
    interval_s: float = 3,
    transport: httpx.BaseTransport | None = None,
) -> None:
    domain(host)
    if timeout_s <= 0 or interval_s <= 0:
        raise ValueError("Readiness timeout and interval must be positive")
    started = time.monotonic()
    deadline = started + timeout_s
    attempts = 0
    last_status = None
    expected_sha = _readiness.normalize_version(build_sha)
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempts += 1
        try:
            with _http_client(timeout=min(10.0, remaining), transport=transport) as client:
                response = client.get(
                    f"https://{host}/version",
                    headers={"Accept": "application/json", "Cache-Control": "no-cache"},
                )
            last_status = response.status_code
            if last_status >= 400:
                if last_status not in {404, 500, 502, 503, 504}:
                    raise APIError(f"Front readiness returned {last_status}; HTML remains unpurged")
            else:
                data = json.loads(response.content[:16384])
                if (
                    last_status == 200
                    and isinstance(data, dict)
                    and isinstance(data.get("build_sha"), str)
                    and _readiness.normalize_version(data["build_sha"]) == expected_sha
                ):
                    LOG.info(
                        json.dumps(
                            {
                                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                                "event": "front.ready",
                                "attempts": attempts,
                                "duration_ms": round((time.monotonic() - started) * 1000, 1),
                            }
                        )
                    )
                    return
        except APIError:
            raise
        except (
            httpx.HTTPError,
            TimeoutError,
            ConnectionError,
            OSError,
            UnicodeError,
            json.JSONDecodeError,
        ):
            last_status = None
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(interval_s, remaining))
    LOG.error(
        json.dumps(
            {
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                "event": "front.readiness_failed",
                "attempts": attempts,
                "status": last_status,
                "duration_ms": round((time.monotonic() - started) * 1000, 1),
            }
        )
    )
    raise APIError(
        f"Front readiness timed out after {timeout_s:g}s (last status {last_status}); "
        "versions are activated, HTML remains unpurged"
    )


def purge_html(api: Any, inventory_path: Path) -> dict[str, Any]:
    inventory = json.loads(inventory_path.read_text())
    front = inventory["services"]["front"]
    remote = api.request("GET", f"/service/{front['id']}")
    if remote["name"] != front["name"] or remote["type"] != "vcl":
        raise ValueError("Front inventory identity does not match the remote service")
    if not inventory.get("activated"):
        raise ValueError("Purge-only requires an already activated installation")
    build_sha = inventory.get("build_sha", "")
    if not re.fullmatch(r"[a-f0-9]{40}", build_sha):
        raise ValueError("Purge-only requires the recorded deployed build SHA")
    wait_for_front(front["domain"], build_sha)
    api.request("POST", f"/service/{front['id']}/purge/flp-html")
    inventory["html_purged"] = True
    _save(inventory_path, inventory)
    return inventory


def print_setup_code(code: str, *, color: bool = False) -> None:
    shown = f"\033[1;32m{code}\033[0m" if color else code
    print("One-time admin setup code (shown only once; only its hash is stored): " + shown)
    print("Expires 15 minutes after generation. Generating a replacement invalidates the previous code.")
    print(
        "New codes may take about a minute or longer to reach the app. "
        "API write success does not confirm edge readiness."
    )
    print("Wait before retrying; avoid repeated submissions or regenerating codes.")


def activate_version(api: Any, service_id: str, version: int, attempts: int = 3) -> None:
    # Fastly has returned 200 for an activation that never took effect.
    path = f"/service/{service_id}/version/{version}"
    for attempt in range(attempts):
        api.request("PUT", path + "/activate")
        for _ in range(5):
            if api.request("GET", path).get("active"):
                return
            time.sleep(1)
        LOG.warning("activation of %s version %s did not take", service_id, version)
    raise APIError(f"Service {service_id} version {version} is not active after activation")


def deploy(
    api: Any,
    inventory_path: Path,
    package: Path,
    name: str,
    *,
    announce: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    root = REPO_ROOT
    provenance = verify_provenance(root, package)
    expected_names = resource_plan(name)["services"]
    inventory = json.loads(inventory_path.read_text())
    if inventory["install_name"] != name:
        raise ValueError("Inventory install name does not match --name")
    for role, expected in expected_names.items():
        service = inventory["services"][role]
        remote = api.request("GET", f"/service/{service['id']}")
        if (
            service["name"] != expected
            or remote["name"] != expected
            or remote["type"] != ("vcl" if role == "front" else "wasm")
        ):
            raise ValueError("Deployment inventory identity does not match remote services")
    inventory["activated"] = False
    inventory["html_purged"] = False
    _save(inventory_path, inventory)
    package_hashes = []
    for role in ("compute", "worker"):
        service = inventory["services"][role]
        service_id = service["id"]
        versions = api.request("GET", f"/service/{service_id}/version")
        latest = max(versions, key=lambda version: int(version["number"]))
        if latest.get("active") in (True, 1, "1", "true") or latest.get("locked") in (
            True,
            1,
            "1",
            "true",
        ):
            latest = api.request("PUT", f"/service/{service_id}/version/{latest['number']}/clone")
        version = int(latest["number"])
        service["version"] = version
        service["activated"] = False
        _save(inventory_path, inventory)
        verify_provenance(root, package)
        api.upload_package(service_id, version, package)
        base = f"/service/{service_id}/version/{version}"
        prepare_storage_backends(api, base, inventory["services"]["front"]["domain"])
        details = api.request("GET", base + "/package")
        metadata = details.get("metadata", {})
        remote_hash = metadata.get("files_hash") or metadata.get("hashsum")
        if not isinstance(remote_hash, str) or not remote_hash:
            raise APIError("Uploaded package has no verifiable remote files hash")
        package_hashes.append(remote_hash)
        service["package_files_hash"] = remote_hash
        _save(inventory_path, inventory)
        if api.request("GET", base + "/validate").get("status") != "ok":
            raise APIError("Compute draft validation failed; no version was activated")
    if len(set(package_hashes)) != 1:
        raise APIError("Compute package hashes differ; no version was activated")
    subdir = "compute/cloud" if package.parent.parent.name == "cloud" else "app"
    frozen_front = _git(root, "show", f"{provenance['build_sha']}:{subdir}/vcl/front/main.vcl")
    early_hints = _wasm_deploy.load_early_hints(package, provenance)
    inventory = prepare_front(
        api,
        inventory_path,
        frozen_source=frozen_front,
        early_hints=early_hints,
        build_sha=provenance["build_sha"],
    )
    inventory.update(provenance)
    _save(inventory_path, inventory)
    for role in ("worker", "compute", "front"):
        verify_provenance(root, package)
        service = inventory["services"][role]
        activate_version(api, service["id"], int(service["version"]))
        service["activated"] = True
        _save(inventory_path, inventory)
    if not inventory.get("admin_code_issued"):
        code = admin_reset(api, inventory["stores"]["secret"]["id"])
        inventory["admin_code_issued"] = True
        _save(inventory_path, inventory)
        if announce is not None:
            announce(code)
    inventory["activated"] = True
    _save(inventory_path, inventory)
    return purge_html(api, inventory_path)


def _save(path: Path, inventory: dict[str, Any]) -> None:
    pending = path.with_name(path.name + f".{uuid.uuid4()}.new")
    try:
        descriptor = os.open(pending, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(inventory, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)


def _all_resources(api: Any, path: str) -> list[dict[str, Any]]:
    items = []
    cursor = None
    seen = set()
    while True:
        suffix = f"?{httpx.QueryParams({'cursor': cursor})}" if cursor else ""
        result = api.request("GET", path + suffix)
        if isinstance(result, list):
            items.extend(result)
            return items
        items.extend(result["data"])
        cursor = result.get("meta", {}).get("next_cursor")
        if not cursor:
            return items
        if cursor in seen:
            raise APIError("Store listing returned a repeated pagination cursor")
        seen.add(cursor)


def _list_services(api: Any) -> list[dict[str, Any]]:
    existing_services: list[dict[str, Any]] = []
    page = 1
    while True:
        found = api.request("GET", f"/service?page={page}&per_page=100")
        existing_services.extend(found)
        if len(found) < 100:
            break
        page += 1
    return existing_services


def _validate_store_id(kind: str, store_id: str | None) -> None:
    if store_id is not None and not re.fullmatch(r"[a-zA-Z0-9_-]+", store_id):
        raise ValueError(f"Invalid {kind} store ID")


def _resolve_store(
    api: Any,
    kind: str,
    expected_name: str,
    explicit_id: str | None,
) -> dict[str, str]:
    endpoint = _stores.STORE_ENDPOINTS[kind]
    if explicit_id is not None:
        _validate_store_id(kind, explicit_id)
        remote = api.request("GET", f"{endpoint}/{explicit_id}")
        if not isinstance(remote, dict):
            raise APIError(f"Unexpected {kind} store response")
        return {
            "id": str(remote.get("id") or explicit_id),
            "name": str(remote.get("name") or expected_name),
        }
    matching = [
        store
        for store in _all_resources(api, endpoint)
        if store.get("name") == expected_name and not store.get("deleted_at")
    ]
    if len(matching) != 1:
        raise ValueError(f"Expected exactly one {kind} store named {expected_name}; supply --{kind}-store-id")
    return {"id": str(matching[0]["id"]), "name": str(matching[0]["name"])}


def _latest_version(api: Any, service_id: str) -> tuple[int, bool]:
    versions = api.request("GET", f"/service/{service_id}/version")
    if not isinstance(versions, list) or not versions:
        raise APIError(f"Service {service_id} has no versions")
    latest = max(versions, key=lambda item: int(item["number"]))
    number = int(latest["number"])
    immutable = latest.get("active") in (True, 1, "1", "true") or latest.get("locked") in (
        True,
        1,
        "1",
        "true",
    )
    return number, immutable


def _ensure_draft_version(api: Any, service_id: str) -> int:
    number, immutable = _latest_version(api, service_id)
    if immutable:
        cloned = api.request("PUT", f"/service/{service_id}/version/{number}/clone")
        return int(cloned["number"])
    return number


def _normalize_origin(base_url: str) -> str:
    raw = base_url.strip()
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme != "https":
        raise ValueError("Base URL must use https://")
    if (
        parsed.username
        or parsed.password
        or "@" in parsed.netloc
        or parsed.query
        or "?" in raw
        or parsed.fragment
        or "#" in raw
    ):
        raise ValueError("Base URL must be an origin without credentials, query, or fragment")
    if parsed.path not in ("", "/"):
        raise ValueError("Base URL must be an origin without a path")
    host = domain(parsed.hostname or "")
    port = parsed.port
    if port is not None and port != 443:
        return f"https://{host}:{port}"
    return f"https://{host}"


def _normalize_session_cookie(raw: str) -> str:
    value = raw.strip()
    if value.startswith("flp_session="):
        value = value[len("flp_session=") :].strip()
    if not value or any(ch in value for ch in ("\r", "\n", ";")):
        raise ValueError("Invalid admin session cookie")
    return value


def _config_items_map(raw: Any) -> dict[str, str]:
    items = raw["data"] if isinstance(raw, dict) and isinstance(raw.get("data"), list) else raw
    if not isinstance(items, list):
        return {}
    out: dict[str, str] = {}
    for item in items:
        if isinstance(item, dict) and "item_key" in item and "item_value" in item:
            out[str(item["item_key"])] = str(item["item_value"])
    return out


def recover_relink(
    api: Any,
    plan: dict[str, Any],
    inventory_path: Path,
    *,
    api_token: str,
    kv_store_id: str | None = None,
    secret_store_id: str | None = None,
    config_store_id: str | None = None,
    rate_limiting: bool | None = None,
) -> dict[str, Any]:
    """Mode A recovery: recreate missing services and relink surviving KV, Secret, and Config stores."""
    if not api_token:
        raise ValueError("A Fastly API token is required before recovery")
    if not inventory_path.parent.is_dir():
        raise ValueError("Inventory parent directory must already exist")
    _validate_store_id("secret", secret_store_id)
    _validate_store_id("config", config_store_id)
    _validate_store_id("kv", kv_store_id)

    stores = {
        "secret": _resolve_store(api, "secret", plan["stores"]["secret"], secret_store_id),
        "config": _resolve_store(api, "config", plan["stores"]["config"], config_store_id),
        "kv": _resolve_store(api, "kv", plan["stores"]["kv"], kv_store_id),
    }

    existing_services = {svc["name"]: svc for svc in _list_services(api) if not svc.get("deleted_at")}

    # Pre-flight validate all surviving services and required front dictionaries before any writes.
    for role in ("compute", "worker"):
        expected_name = plan["services"][role]
        remote = existing_services.get(expected_name)
        if remote is not None and remote.get("type") != "wasm":
            raise ValueError(f"Surviving {role} service {expected_name} is not a Compute service")

    front_name = plan["services"]["front"]
    front_remote = existing_services.get(front_name)
    surviving_front_ver: int | None = None
    surviving_front_dicts: dict[str, dict[str, str]] = {}
    if front_remote is None:
        if rate_limiting is None:
            raise ValueError("Front service must be recreated; supply --erl-service-id or --no-erl")
        lint_front(rate_limiting)
    else:
        if front_remote.get("type") != "vcl":
            raise ValueError(f"Surviving front service {front_name} is not a VCL service")
        front_id = front_remote["id"]
        surviving_front_ver, _ = _latest_version(api, front_id)
        dicts = api.request("GET", f"/service/{front_id}/version/{surviving_front_ver}/dictionary")
        by_name = {
            entry["name"]: entry
            for entry in (dicts if isinstance(dicts, list) else [])
            if isinstance(entry, dict) and "name" in entry
        }
        for dict_role, (dict_name, _private) in DICTIONARIES.items():
            matched = by_name.get(dict_name)
            if not matched:
                raise ValueError(f"Surviving front service {front_name} is missing required dictionary {dict_name}")
            surviving_front_dicts[dict_role] = {"id": matched["id"], "name": dict_name}
        if rate_limiting is None:
            detected = account_has_rate_limiting(api, front_id)
            rate_limiting = bool(detected) if detected is not None else False

    inventory: dict[str, Any] = {
        "install_name": plan["install_name"],
        "services": {},
        "stores": stores,
        "dictionaries": {},
        "rate_limiting": bool(rate_limiting),
        "activated": False,
        "admin_code_issued": True,
    }
    _save(inventory_path, inventory)

    recreated_services: list[str] = []
    reused_services: list[str] = []

    for role in ("compute", "worker"):
        expected_name = plan["services"][role]
        remote = existing_services.get(expected_name)
        if remote is None:
            created = api.request(
                "POST",
                "/service",
                {"name": expected_name, "type": "wasm"},
                form=True,
            )
            sid = created["id"]
            inventory["services"][role] = {
                "id": sid,
                "name": expected_name,
                "version": 1,
                "domain": plan["domains"][role],
            }
            _save(inventory_path, inventory)
            api.request(
                "POST",
                f"/service/{sid}/version/1/domain",
                {"name": plan["domains"][role]},
                form=True,
            )
            for kind, link_name in LINKS.items():
                api.request(
                    "POST",
                    f"/service/{sid}/version/1/resource",
                    {"name": link_name, "resource_id": stores[kind]["id"]},
                    form=True,
                )
            for fields in plan["backends"][role]:
                api.request("POST", f"/service/{sid}/version/1/backend", fields, form=True)
            recreated_services.append(role)
        else:
            if remote.get("type") != "wasm":
                raise ValueError(f"Surviving {role} service {expected_name} is not a Compute service")
            sid = remote["id"]
            ver, _ = _latest_version(api, sid)
            resources = api.request("GET", f"/service/{sid}/version/{ver}/resource")
            by_name = {
                entry["name"]: entry
                for entry in (resources if isinstance(resources, list) else [])
                if isinstance(entry, dict) and "name" in entry
            }
            missing_or_wrong = [
                (kind, link_name)
                for kind, link_name in LINKS.items()
                if link_name not in by_name or by_name[link_name].get("resource_id") != stores[kind]["id"]
            ]
            if missing_or_wrong:
                ver = _ensure_draft_version(api, sid)
                draft_resources = api.request("GET", f"/service/{sid}/version/{ver}/resource")
                draft_by_name = {
                    entry["name"]: entry
                    for entry in (draft_resources if isinstance(draft_resources, list) else [])
                    if isinstance(entry, dict) and "name" in entry
                }
                for kind, link_name in missing_or_wrong:
                    existing_link = draft_by_name.get(link_name)
                    if existing_link and existing_link.get("id"):
                        api.request(
                            "DELETE",
                            f"/service/{sid}/version/{ver}/resource/{existing_link['id']}",
                        )
                    api.request(
                        "POST",
                        f"/service/{sid}/version/{ver}/resource",
                        {"name": link_name, "resource_id": stores[kind]["id"]},
                        form=True,
                    )
                prepare_storage_backends(
                    api,
                    f"/service/{sid}/version/{ver}",
                    plan["domains"]["front"],
                )
            inventory["services"][role] = {
                "id": sid,
                "name": expected_name,
                "version": ver,
                "domain": plan["domains"][role],
            }
            _save(inventory_path, inventory)
            reused_services.append(role)

    if front_remote is None:
        created = api.request(
            "POST",
            "/service",
            {"name": front_name, "type": "vcl"},
            form=True,
        )
        front_id = created["id"]
        inventory["services"]["front"] = {
            "id": front_id,
            "name": front_name,
            "version": 1,
            "domain": plan["domains"]["front"],
        }
        _save(inventory_path, inventory)
        front_prefix = f"/service/{front_id}/version/1"
        api.request(
            "POST",
            front_prefix + "/domain",
            {"name": plan["domains"]["front"]},
            form=True,
        )
        for dict_role, (dict_name, private) in DICTIONARIES.items():
            created_dict = api.request(
                "POST",
                front_prefix + "/dictionary",
                {"name": dict_name, "write_only": "true" if private else "false"},
                form=True,
            )
            inventory["dictionaries"][dict_role] = {"id": created_dict["id"], "name": dict_name}
            _save(inventory_path, inventory)
        for name, expression, priority in (
            ("flp_app", APP_CONDITION, "20"),
            ("flp_tick", TICK_CONDITION, "10"),
            ("flp_clock_never", "false", "1"),
        ):
            api.request(
                "POST",
                front_prefix + "/condition",
                {
                    "name": name,
                    "type": "REQUEST",
                    "statement": expression,
                    "priority": priority,
                },
                form=True,
            )
        for healthcheck in (plan["healthcheck"], plan["flush_healthcheck"]):
            api.request("POST", front_prefix + "/healthcheck", healthcheck, form=True)
        for fields in plan["backends"]["front"]:
            api.request("POST", front_prefix + "/backend", fields, form=True)

        # Secret Store is write-only: rotate only front_key & cdn_key so new front matches surviving secrets.
        front_key = secrets.token_urlsafe(48)
        cdn_key = secrets.token_urlsafe(48)
        put_secret(api, stores["secret"]["id"], "front_key", front_key)
        put_secret(api, stores["secret"]["id"], "cdn_key", cdn_key)
        front_auth_id = inventory["dictionaries"]["front_auth"]["id"]
        api.request(
            "PUT",
            f"/service/{front_id}/dictionary/{front_auth_id}/item/front_key",
            {"item_value": front_key},
            form=True,
        )
        api.request(
            "PUT",
            f"/service/{front_id}/dictionary/{front_auth_id}/item/cdn_key",
            {"item_value": cdn_key},
            form=True,
        )
        api.request(
            "PUT",
            f"/service/{front_id}/dictionary/{front_auth_id}/item/auth_epoch",
            {"item_value": "0"},
            form=True,
        )
        api.request(
            "POST",
            front_prefix + "/vcl",
            {"name": "main", "main": "true", "content": load_vcl(bool(rate_limiting))},
            form=True,
        )
        validation = api.request("GET", front_prefix + "/validate")
        if validation.get("status") != "ok":
            raise APIError("Front draft validation failed; no version was activated")
        recreated_services.append("front")
    else:
        front_id = front_remote["id"]
        assert surviving_front_ver is not None
        inventory["dictionaries"].update(surviving_front_dicts)
        inventory["services"]["front"] = {
            "id": front_id,
            "name": front_name,
            "version": surviving_front_ver,
            "domain": plan["domains"]["front"],
        }
        _save(inventory_path, inventory)
        reused_services.append("front")

    config_store_endpoint = f"{_stores.STORE_ENDPOINTS['config']}/{stores['config']['id']}"
    existing_config = _config_items_map(api.request("GET", config_store_endpoint + "/items"))
    config_updates: dict[str, str] = {
        "install_name": plan["install_name"],
        "public_base_url": plan["public_base_url"],
        "flp_service_id": inventory["services"]["compute"]["id"],
        "worker_service_id": inventory["services"]["worker"]["id"],
        "front_service_id": front_id,
        "worker_host": plan["domains"]["worker"],
        "compute_host": plan["domains"]["compute"],
        "secret_store_id": stores["secret"]["id"],
        "config_store_id": stores["config"]["id"],
        "kv_store_id": stores["kv"]["id"],
        "front_auth_dictionary_id": inventory["dictionaries"]["front_auth"]["id"],
        "buckets_dictionary_id": inventory["dictionaries"]["buckets"]["id"],
        "credentials_dictionary_id": inventory["dictionaries"]["credentials"]["id"],
    }
    for key, default_val in plan["config_defaults"].items():
        if key not in existing_config:
            config_updates[key] = default_val
    for key, value in config_updates.items():
        api.request(
            "PUT",
            f"{config_store_endpoint}/item/{key}",
            {"item_value": value},
            form=True,
        )

    inventory["configured"] = True
    _save(inventory_path, inventory)
    next_steps = [f"make cloud-deploy NAME={plan['install_name']}"]
    if "front" in recreated_services:
        next_steps.append(
            "Run Reconcile in Admin UI after deploy to repopulate per-service FOS dictionaries and region backend on the new Front VCL service"
        )
    return {
        "mode": "relink",
        "inventory": inventory,
        "recreated_services": recreated_services,
        "reused_services": reused_services,
        "front_keys_rotated": "front" in recreated_services,
        "next_steps": next_steps,
    }


def recover_fos(
    base_url: str,
    *,
    service_id: str,
    fos_bucket: str,
    fos_region: str,
    api_token: str,
    admin_cookie: str,
    fos_prefix: str = "",
    fos_access_key: str | None = None,
    fos_secret_key: str | None = None,
    transport: httpx.BaseTransport | None = None,
    timeout: float = 90.0,
) -> dict[str, Any]:
    """Mode B recovery: invoke POST /api/admin/provision/recover on a fresh Cloud install."""
    origin = _normalize_origin(base_url)
    cookie_value = _normalize_session_cookie(admin_cookie)
    if not service_id.strip():
        raise ValueError("--service-id is required")
    if not fos_bucket.strip():
        raise ValueError("--bucket is required")
    if fos_region not in FOS_REGIONS:
        raise ValueError(f"Unsupported FOS region: {fos_region}")
    if not api_token:
        raise ValueError("FASTLY_API_TOKEN is required")
    if bool(fos_access_key) != bool(fos_secret_key):
        raise ValueError("Both FOS_ACCESS_KEY and FOS_SECRET_KEY are required together")

    payload: dict[str, Any] = {
        "token": api_token,
        "service_id": service_id.strip(),
        "fos_bucket": fos_bucket.strip(),
        "fos_region": fos_region,
    }
    if fos_prefix:
        payload["fos_prefix"] = fos_prefix
    if fos_access_key and fos_secret_key:
        payload["fos_access_key"] = fos_access_key
        payload["fos_secret_key"] = fos_secret_key

    path = "/api/admin/provision/recover"
    started = time.monotonic()
    status: int | None = None
    error_type: str | None = None
    try:
        with _http_client(timeout=timeout, transport=transport) as client:
            response = client.post(
                origin + path,
                headers={
                    "Origin": origin,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Cookie": f"flp_session={cookie_value}",
                },
                json=payload,
            )
            status = response.status_code
            if status >= 400:
                error_type = f"http_{status}"
                raise APIError(f"Cloud POST {path} returned {status}")
            data = response.json()
            if not isinstance(data, dict):
                error_type = "invalid_json"
                raise APIError(f"Cloud POST {path} returned unexpected response")
            return data
    except APIError:
        raise
    except (httpx.HTTPError, TimeoutError, OSError, json.JSONDecodeError) as err:
        error_type = type(err).__name__
        raise APIError(f"Cloud POST {path} failed") from None
    finally:
        LOG.info(
            json.dumps(
                {
                    "event": "flp.recover_fos",
                    "method": "POST",
                    "path": path,
                    "status": status,
                    "duration_ms": round((time.monotonic() - started) * 1000, 1),
                    "error_type": error_type,
                }
            )
        )


def bootstrap(
    api: Any,
    plan: dict[str, Any],
    inventory_path: Path,
    *,
    rate_limiting: bool,
    api_token: str,
) -> dict[str, Any]:
    """Create drafts only. Each successful resource is checkpointed without secrets."""
    if not api_token:
        raise ValueError("A Fastly API token is required before bootstrap")
    lint_front(rate_limiting)
    if inventory_path.exists():
        raise ValueError("Inventory already exists; refuse to rotate a running install")
    if not inventory_path.parent.is_dir():
        raise ValueError("Inventory parent directory must already exist")
    existing_services = _list_services(api)
    for existing in existing_services:
        if existing["name"] in plan["services"].values():
            raise ValueError("A named install service already exists; refusing adoption")
    for kind, name in plan["stores"].items():
        if any(resource["name"] == name for resource in _all_resources(api, _stores.STORE_ENDPOINTS[kind])):
            raise ValueError("A named install store already exists; refusing adoption")

    inventory: dict[str, Any] = {
        "install_name": plan["install_name"],
        "services": {},
        "stores": {},
        "dictionaries": {},
        "rate_limiting": rate_limiting,
        "activated": False,
    }
    _save(inventory_path, inventory)
    for role, name in plan["services"].items():
        created = api.request(
            "POST",
            "/service",
            {"name": name, "type": "vcl" if role == "front" else "wasm"},
            form=True,
        )
        service = {
            "id": created["id"],
            "name": name,
            "version": 1,
            "domain": plan["domains"][role],
        }
        inventory["services"][role] = service
        _save(inventory_path, inventory)
        api.request(
            "POST",
            f"/service/{service['id']}/version/1/domain",
            {"name": service["domain"]},
            form=True,
        )

    for kind, name in plan["stores"].items():
        created = api.request("POST", _stores.STORE_ENDPOINTS[kind], {"name": name})
        inventory["stores"][kind] = {"id": created["id"], "name": name}
        _save(inventory_path, inventory)

    for role in ("compute", "worker"):
        sid = inventory["services"][role]["id"]
        for kind, link_name in LINKS.items():
            api.request(
                "POST",
                f"/service/{sid}/version/1/resource",
                {"name": link_name, "resource_id": inventory["stores"][kind]["id"]},
                form=True,
            )
        for fields in plan["backends"][role]:
            api.request("POST", f"/service/{sid}/version/1/backend", fields, form=True)

    front_id = inventory["services"]["front"]["id"]
    front = f"/service/{front_id}/version/1"
    for role, (name, private) in DICTIONARIES.items():
        created = api.request(
            "POST",
            front + "/dictionary",
            {"name": name, "write_only": "true" if private else "false"},
            form=True,
        )
        inventory["dictionaries"][role] = {"id": created["id"], "name": name}
        _save(inventory_path, inventory)

    for name, expression, priority in (
        ("flp_app", APP_CONDITION, "20"),
        ("flp_tick", TICK_CONDITION, "10"),
        ("flp_clock_never", "false", "1"),
    ):
        api.request(
            "POST",
            front + "/condition",
            {
                "name": name,
                "type": "REQUEST",
                "statement": expression,
                "priority": priority,
            },
            form=True,
        )
    for healthcheck in (plan["healthcheck"], plan["flush_healthcheck"]):
        api.request("POST", front + "/healthcheck", healthcheck, form=True)
    for fields in plan["backends"]["front"]:
        api.request("POST", front + "/backend", fields, form=True)

    values = initial_secrets(api_token)
    for name, value in values.items():
        put_secret(api, inventory["stores"]["secret"]["id"], name, value)
    dictionary_id = inventory["dictionaries"]["front_auth"]["id"]
    api.request(
        "PUT",
        f"/service/{front_id}/dictionary/{dictionary_id}/item/front_key",
        {"item_value": values["front_key"]},
        form=True,
    )
    api.request(
        "PUT",
        f"/service/{front_id}/dictionary/{dictionary_id}/item/cdn_key",
        {"item_value": values["cdn_key"]},
        form=True,
    )
    api.request(
        "PUT",
        f"/service/{front_id}/dictionary/{dictionary_id}/item/auth_epoch",
        {"item_value": "0"},
        form=True,
    )
    config = {
        **plan["config_defaults"],
        "install_name": plan["install_name"],
        "public_base_url": plan["public_base_url"],
        "flp_service_id": inventory["services"]["compute"]["id"],
        "worker_service_id": inventory["services"]["worker"]["id"],
        "front_service_id": front_id,
        "worker_host": plan["domains"]["worker"],
        "compute_host": plan["domains"]["compute"],
        "secret_store_id": inventory["stores"]["secret"]["id"],
        "config_store_id": inventory["stores"]["config"]["id"],
        "kv_store_id": inventory["stores"]["kv"]["id"],
        "front_auth_dictionary_id": dictionary_id,
        "buckets_dictionary_id": inventory["dictionaries"]["buckets"]["id"],
        "credentials_dictionary_id": inventory["dictionaries"]["credentials"]["id"],
    }
    if os.environ.get("FLP_DEV_TELEMETRY"):
        config["dev_telemetry"] = os.environ["FLP_DEV_TELEMETRY"]
    for key, value in config.items():
        api.request(
            "PUT",
            f"{_stores.STORE_ENDPOINTS['config']}/{inventory['stores']['config']['id']}/item/{key}",
            {"item_value": value},
            form=True,
        )
    api.request(
        "POST",
        front + "/vcl",
        {"name": "main", "main": "true", "content": load_vcl(rate_limiting)},
        form=True,
    )
    validation = api.request("GET", front + "/validate")
    if validation.get("status") != "ok":
        raise APIError("Front draft validation failed; no version was activated")
    inventory["configured"] = True
    _save(inventory_path, inventory)
    return inventory


# The rest of the product reads its Fastly token as FASTLY_API_KEY; accept it here too so an
# install does not need a second, separately rotated copy of the same credential.
CREDENTIAL_FALLBACKS = {"FASTLY_API_TOKEN": ("FASTLY_API_KEY",)}


def _credential(env_name: str, prompt: str) -> str:
    _environments.load_dotenv()
    value = os.environ.get(env_name)
    for fallback in CREDENTIAL_FALLBACKS.get(env_name, ()):
        value = value or os.environ.get(fallback)
    value = value or getpass.getpass(prompt)
    if not value:
        raise ValueError(f"{env_name} is required")
    return value


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    install = subcommands.add_parser("bootstrap", help="Plan, or create unactivated drafts")
    install.add_argument("--name", default="flp")
    install.add_argument("--public-host")
    install.add_argument("--compute-host")
    install.add_argument("--worker-host")
    install.add_argument("--dry-run", action="store_true")
    install.add_argument("--apply", action="store_true")
    install.add_argument("--inventory", type=Path)
    install.add_argument("--erl-service-id", action="append", default=[])
    install.add_argument("--no-erl", action="store_true")
    front = subcommands.add_parser("front", help="Prepare a front VCL draft without activation")
    front.add_argument("--inventory", type=Path, required=True)
    front.add_argument("--apply", action="store_true")
    front.add_argument("--no-erl", action="store_true")
    release = subcommands.add_parser("deploy", help="Deploy a frozen package; call through make")
    release.add_argument("--name", default="flp")
    release.add_argument("--package", type=Path, required=True)
    release.add_argument("--inventory", type=Path)
    release.add_argument("--dry-run", action="store_true")
    purge = subcommands.add_parser("purge-html", help="Finish only the deployed HTML purge")
    purge.add_argument("--inventory", type=Path, required=True)
    purge.add_argument("--dry-run", action="store_true")
    for command in ("admin-reset", "admin-setup-code"):
        reset = subcommands.add_parser(command, help="Rotate the one-time setup code")
        reset.add_argument("--secret-store-id")
        reset.add_argument("--name", default="flp")
        reset.add_argument("--apply", action="store_true")
        if command == "admin-reset":
            reset.add_argument(
                "--verbose",
                action="store_true",
                help="Show structured HTTP diagnostics",
            )
    recover = subcommands.add_parser(
        "recover",
        help="Recover from accidental service deletion (relink) or full KV loss (fos)",
    )
    recover.add_argument("--mode", choices=("relink", "fos"), required=True)
    recover.add_argument("--name", default="flp")
    recover.add_argument("--public-host")
    recover.add_argument("--compute-host")
    recover.add_argument("--worker-host")
    recover.add_argument("--inventory", type=Path)
    recover.add_argument("--dry-run", action="store_true")
    recover.add_argument("--apply", action="store_true")
    recover.add_argument("--kv-store-id")
    recover.add_argument("--secret-store-id")
    recover.add_argument("--config-store-id")
    recover.add_argument("--erl-service-id", action="append", default=[])
    recover.add_argument("--no-erl", action="store_true")
    recover.add_argument(
        "--base-url",
        help="Target Cloud install origin, e.g. https://flp.global.ssl.fastly.net",
    )
    recover.add_argument("--service-id", help="Monitored Fastly VCL service ID to recover")
    recover.add_argument(
        "--bucket",
        help="FOS bucket name containing meta/cloud_dr_state.json and processed/",
    )
    recover.add_argument("--region", help="FOS region (e.g. us-east-1)")
    recover.add_argument("--prefix", default="", help="Optional FOS key prefix")
    recover.add_argument(
        "--use-fos-keys",
        action="store_true",
        help="Read fallback FOS_ACCESS_KEY and FOS_SECRET_KEY from env/prompt instead of minting via Fastly API",
    )
    args = parser.parse_args(argv)
    log_level = LOG.level
    if args.command == "admin-reset":
        LOG.setLevel(logging.INFO if args.verbose else logging.WARNING)
    try:
        if args.command == "bootstrap":
            plan = resource_plan(
                args.name,
                public_host=args.public_host,
                compute_host=args.compute_host,
                worker_host=args.worker_host,
            )
            if args.dry_run and args.apply:
                raise ValueError("--dry-run and --apply are mutually exclusive")
            if not args.apply:
                print(json.dumps(plan, indent=2))
                return 0
            if args.inventory is None:
                raise ValueError("--apply requires --inventory for partial-resource recovery")
            deploy_token = _credential("FASTLY_API_TOKEN", "Local deploy token: ")
            api = FastlyAPI(deploy_token)
            erl = False if args.no_erl else account_has_rate_limiting(api, *args.erl_service_id)
            if erl is None:
                raise ValueError("ERL capability unknown: supply --erl-service-id or explicitly --no-erl")
            inventory = bootstrap(
                api,
                plan,
                args.inventory,
                rate_limiting=erl,
                api_token=deploy_token,
            )
            print(json.dumps(inventory, indent=2))
            return 0
        if args.command == "front":
            if not args.apply:
                print(json.dumps({"action": "prepare front draft", "writes": False}))
                return 0
            api = FastlyAPI(_credential("FASTLY_API_TOKEN", "Local deploy token: "))
            inventory = prepare_front(api, args.inventory, rate_limiting=False if args.no_erl else None)
            print(json.dumps(inventory, indent=2))
            return 0
        if args.command == "purge-html":
            if args.dry_run:
                print(
                    json.dumps(
                        {
                            "action": "wait for deployed front, purge flp-html",
                            "writes": False,
                        }
                    )
                )
                return 0
            api = FastlyAPI(_credential("FASTLY_API_TOKEN", "Local deploy token: "))
            print(json.dumps(purge_html(api, args.inventory), indent=2))
            return 0
        if args.command == "deploy":
            resource_plan(args.name)
            inventory_path = args.inventory or Path(
                os.environ.get(
                    "FLP_INVENTORY",
                    str(VCL.parents[1] / "pkg" / f"{args.name}-resources.local.json"),
                )
            )
            if args.dry_run:
                print(
                    json.dumps(
                        {
                            "action": "deploy frozen paired package and front, purge flp-html",
                            "package": str(args.package),
                            "inventory": str(inventory_path),
                            "writes": False,
                        }
                    )
                )
                return 0
            verify_provenance(REPO_ROOT, args.package)
            api = FastlyAPI(_credential("FASTLY_API_TOKEN", "Local deploy token: "))
            result = deploy(
                api,
                inventory_path,
                args.package,
                args.name,
                announce=print_setup_code,
            )
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "recover":
            if args.dry_run and args.apply:
                raise ValueError("--dry-run and --apply are mutually exclusive")
            if args.mode == "relink":
                _validate_store_id("kv", args.kv_store_id)
                _validate_store_id("secret", args.secret_store_id)
                _validate_store_id("config", args.config_store_id)
                plan = resource_plan(
                    args.name,
                    public_host=args.public_host,
                    compute_host=args.compute_host,
                    worker_host=args.worker_host,
                )
                if not args.apply:
                    print(
                        json.dumps(
                            {
                                "mode": "relink",
                                "action": "recreate missing services and relink surviving stores",
                                "plan": plan,
                                "kv_store_id": args.kv_store_id,
                                "secret_store_id": args.secret_store_id,
                                "config_store_id": args.config_store_id,
                                "writes": False,
                            },
                            indent=2,
                        )
                    )
                    return 0
                if args.inventory is None:
                    raise ValueError("--apply requires --inventory for partial-resource recovery")
                deploy_token = _credential("FASTLY_API_TOKEN", "Local deploy token: ")
                api = FastlyAPI(deploy_token)
                erl: bool | None
                if args.no_erl:
                    erl = False
                elif args.erl_service_id:
                    erl = account_has_rate_limiting(api, *args.erl_service_id)
                else:
                    erl = None
                result = recover_relink(
                    api,
                    plan,
                    args.inventory,
                    api_token=deploy_token,
                    kv_store_id=args.kv_store_id,
                    secret_store_id=args.secret_store_id,
                    config_store_id=args.config_store_id,
                    rate_limiting=erl,
                )
                print(json.dumps(result, indent=2))
                return 0
            # Mode B: --mode fos
            base_url = args.base_url
            if not base_url and args.inventory is not None:
                inv = json.loads(args.inventory.read_text())
                front_domain = inv.get("services", {}).get("front", {}).get("domain")
                if front_domain:
                    base_url = f"https://{front_domain}"
            if not base_url:
                raise ValueError("--mode fos requires --base-url or --inventory")
            origin = _normalize_origin(base_url)
            if not args.service_id or not args.bucket or not args.region:
                raise ValueError("--mode fos requires --service-id, --bucket, and --region")
            if args.region not in FOS_REGIONS:
                raise ValueError(f"Unsupported FOS region: {args.region}")
            if not args.apply:
                print(
                    json.dumps(
                        {
                            "mode": "fos",
                            "action": "POST /api/admin/provision/recover",
                            "base_url": origin,
                            "service_id": args.service_id,
                            "fos_bucket": args.bucket,
                            "fos_region": args.region,
                            "fos_prefix": args.prefix,
                            "use_fos_keys": bool(args.use_fos_keys),
                            "writes": False,
                        },
                        indent=2,
                    )
                )
                return 0
            api_token = _credential("FASTLY_API_TOKEN", "Fastly API token: ")
            admin_cookie = _credential("FLP_ADMIN_COOKIE", "Admin session cookie (flp_session): ")
            fos_ak = None
            fos_sk = None
            if args.use_fos_keys:
                fos_ak = _credential("FOS_ACCESS_KEY", "FOS access key: ")
                fos_sk = _credential("FOS_SECRET_KEY", "FOS secret key: ")
            result = recover_fos(
                origin,
                service_id=args.service_id,
                fos_bucket=args.bucket,
                fos_region=args.region,
                api_token=api_token,
                admin_cookie=admin_cookie,
                fos_prefix=args.prefix,
                fos_access_key=fos_ak,
                fos_secret_key=fos_sk,
            )
            print(json.dumps(result, indent=2))
            return 0
        if not args.apply:
            print(json.dumps({"action": "replace admin_setup_hash", "writes": False}))
            return 0
        api = FastlyAPI(_credential("FASTLY_API_TOKEN", "Local deploy token: "))
        store_id = args.secret_store_id
        if store_id is None:
            name = resource_plan(args.name)["stores"]["secret"]
            matching = [
                store
                for store in _all_resources(api, _stores.STORE_ENDPOINTS["secret"])
                if store["name"] == name and not store.get("deleted_at")
            ]
            if len(matching) != 1:
                raise ValueError("Expected exactly one named Secret Store; supply --secret-store-id")
            store_id = matching[0]["id"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", store_id):
            raise ValueError("Invalid Secret Store ID")
        code = admin_reset(api, store_id)
        print_setup_code(
            code,
            color=args.command == "admin-reset" and sys.stdout.isatty() and "NO_COLOR" not in os.environ,
        )
        return 0
    except (
        ValueError,
        APIError,
        OSError,
        tarfile.TarError,
        subprocess.CalledProcessError,
    ) as error:
        if isinstance(error, subprocess.CalledProcessError):
            print(
                "Error: a local validation command failed; operation stopped",
                file=sys.stderr,
            )
        else:
            print("Error: " + str(error), file=sys.stderr)
        return 1
    finally:
        LOG.setLevel(log_level)


if __name__ == "__main__":
    raise SystemExit(main())
