#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""CLI-only installation of the app shell; FOS provisioning belongs to Goal 2."""

from __future__ import annotations

import argparse
import base64
import datetime
import getpass
import hashlib
import io
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
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

_FILE_RESOLVED = Path(__file__).resolve()
REPO_ROOT = (
    _FILE_RESOLVED.parents[3]
    if len(_FILE_RESOLVED.parents) >= 4 and _FILE_RESOLVED.parents[2].name == "compute"
    else _FILE_RESOLVED.parents[2]
)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from backend.core.fastly.client import SHIELD_MAP, region_endpoint
    from backend.provision import fos_setup as _fos_setup
    from backend.provision import stores as _stores
    from backend.provision import wasm_deploy as _wasm_deploy
    from backend.utils import terraform_gen as _terraform_gen
    from scripts.lib import environments as _environments
    from scripts.lib import readiness as _readiness

    FOS_REGIONS = tuple(SHIELD_MAP.keys())
except Exception:
    SHIELD_MAP = {}
    region_endpoint = None  # type: ignore[assignment]
    _fos_setup = None  # type: ignore[assignment]
    _stores = None  # type: ignore[assignment]
    _wasm_deploy = None  # type: ignore[assignment]
    _terraform_gen = None  # type: ignore[assignment]
    _environments = None  # type: ignore[assignment]
    _readiness = None  # type: ignore[assignment]
    FOS_REGIONS = (
        "us-east-1",
        "us-west",
        "us-central-1",
        "eu-central-1",
        "eu-south-1",
        "eu-west-1",
        "uk-east-1",
        "jp-central-1",
        "au-east-1",
    )

VCL = _FILE_RESOLVED.parents[1] / "vcl" / "front"
LOG = logging.getLogger("flp.bootstrap")
LINKS = {"secret": "flp_secrets", "config": "flp_config", "kv": "flp_state"}
DICTIONARIES = {
    "front_auth": ("flp_front_auth", True),
    "buckets": ("flp_fos_buckets", False),
    "credentials": ("flp_fos_credentials", True),
}


class APIError(RuntimeError):
    """An API failure with no server body or credential material."""


class FastlyAPI:
    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("A Fastly API token is required")
        self._token = token
        self._run_id = uuid.uuid4().hex[:12]

    def request(self, method: str, path: str, payload: Any = None, *, form: bool = False) -> Any:
        if not path.startswith("/") or "://" in path or "\n" in path:
            raise ValueError("API path must be relative to api.fastly.com")
        data = None
        headers = {"Fastly-Key": self._token, "Accept": "application/json"}
        if payload is not None:
            data = urllib.parse.urlencode(payload).encode() if form else json.dumps(payload).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
        return self._send(method, path, data, headers)

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
        )

    def _send(self, method: str, path: str, data: bytes | None, headers: dict[str, str]) -> Any:
        started = time.monotonic()
        status = None
        received = 0
        request = urllib.request.Request("https://api.fastly.com" + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                status = getattr(response, "status", 200)
                body = response.read()
                received = len(body)
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            status = error.code
            raise APIError(f"Fastly {method} {path.split('?')[0]} returned {error.code}") from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            raise APIError(f"Fastly {method} {path.split('?')[0]} failed") from None
        finally:
            LOG.log(
                logging.INFO if status is not None and status < 400 else logging.ERROR,
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
        fields["shield"] = "iad-va-us"
    return fields


# The front service's health checks are the clocks: `/_tick` drives ingest and maintenance,
# `/_flush` the operational-log flush in its own Compute execution. The VCL caches each per
# one-second bucket at the IAD shield, so about one probe a second reaches flp, whose
# dispatcher decides whether anything runs.
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
                        f"{region}.object.fastlystorage.app",
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
                        f"{region}.object.fastlystorage.app",
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
            "raw_prefix": "raw/request/",
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
                f"{region}.object.fastlystorage.app",
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


def load_vcl(rate_limiting: bool = True, *, source: str | None = None) -> str:
    content = (VCL / "main.vcl").read_text() if source is None else source
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


def lint_front(rate_limiting: bool, *, source: str | None = None) -> None:
    path = VCL / "tests" / f".lint-{uuid.uuid4()}.vcl"
    try:
        path.write_text(
            'table flp_front_auth {\n  "front_key": "test-not-live",\n}\n'
            "table flp_fos_buckets {}\ntable flp_fos_credentials {}\n" + load_vcl(rate_limiting, source=source)
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
    api.request(
        "PUT",
        f"/resources/stores/secret/{store_id}/secrets",
        {"name": name, "secret": base64.b64encode(value.encode()).decode()},
    )


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
) -> dict[str, Any]:
    """Prepare and validate a front draft; activation remains the release wrapper's job."""
    inventory = json.loads(inventory_path.read_text())
    lint_front(
        inventory["rate_limiting"] if rate_limiting is None else rate_limiting,
        source=frozen_source,
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
        lint_front(rate_limiting, source=frozen_source)
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
    api.request(
        "PUT",
        base + "/vcl/main",
        {"content": load_vcl(rate_limiting, source=frozen_source), "main": "true"},
        form=True,
    )
    if "req.http.x-flp-fos-service" in load_vcl(rate_limiting, source=frozen_source):
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


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(root), *args], text=True, capture_output=True, check=True)
    return result.stdout.strip()


def verify_provenance(root: Path, package: Path) -> dict[str, str]:
    provenance = json.loads((package.parent / "provenance.json").read_text())
    sha = provenance.get("build_sha", "")
    digest = provenance.get("package_sha256", "")
    if not re.fullmatch(r"[a-f0-9]{40}", sha) or not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError("Invalid frozen build provenance")
    contents = package.read_bytes()
    if hashlib.sha256(contents).hexdigest() != digest:
        raise ValueError("Package changed after the recorded build")
    subdir = "compute/cloud" if package.parent.parent.name == "cloud" else "app"
    if _git(root, "status", "--porcelain", "--untracked-files=all", "--", subdir):
        raise ValueError(f"Runtime has uncommitted inputs; commit {subdir}/ before a release")
    if _git(root, "rev-parse", f"{sha}:{subdir}") != _git(root, "rev-parse", f"HEAD:{subdir}"):
        raise ValueError("Runtime source changed after the build")
    with tarfile.open(fileobj=io.BytesIO(contents), mode="r:gz") as archive:
        members = [member for member in archive.getmembers() if member.isfile() and member.name.endswith(".wasm")]
        if len(members) != 1:
            raise ValueError("Package must contain exactly one Wasm binary")
        stream = archive.extractfile(members[0])
        if stream is None or sha.encode() not in stream.read():
            raise ValueError("Package Wasm does not contain its frozen built SHA")
    return {"build_sha": sha, "package_sha256": digest}


def wait_for_front(host: str, build_sha: str, *, timeout_s: float = 180, interval_s: float = 3) -> None:
    domain(host)
    if timeout_s <= 0 or interval_s <= 0:
        raise ValueError("Readiness timeout and interval must be positive")
    started = time.monotonic()
    deadline = started + timeout_s
    attempts = 0
    last_status = None
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempts += 1
        request = urllib.request.Request(
            f"https://{host}/version",
            headers={"Accept": "application/json", "Cache-Control": "no-cache"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=min(10, remaining)) as response:
                last_status = response.status
                data = json.loads(response.read(16384))
                if last_status == 200 and isinstance(data, dict) and data.get("build_sha") == build_sha:
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
        except urllib.error.HTTPError as error:
            last_status = error.code
            if error.code not in {404, 500, 502, 503, 504}:
                raise APIError(f"Front readiness returned {error.code}; HTML remains unpurged") from None
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
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
    inventory = prepare_front(api, inventory_path, frozen_source=frozen_front)
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
        suffix = "?cursor=" + urllib.parse.quote(cursor, safe="") if cursor else ""
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
    existing_services = []
    page = 1
    while True:
        found = api.request("GET", f"/service?page={page}&per_page=100")
        existing_services.extend(found)
        if len(found) < 100:
            break
        page += 1
    for existing in existing_services:
        if existing["name"] in plan["services"].values():
            raise ValueError("A named install service already exists; refusing adoption")
    for kind, name in plan["stores"].items():
        if any(resource["name"] == name for resource in _all_resources(api, f"/resources/stores/{kind}")):
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
        created = api.request("POST", f"/resources/stores/{kind}", {"name": name})
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
            f"/resources/stores/config/{inventory['stores']['config']['id']}/item/{key}",
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


def _credential(env_name: str, prompt: str) -> str:
    value = os.environ.get(env_name) or getpass.getpass(prompt)
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
        if not args.apply:
            print(json.dumps({"action": "replace admin_setup_hash", "writes": False}))
            return 0
        api = FastlyAPI(_credential("FASTLY_API_TOKEN", "Local deploy token: "))
        store_id = args.secret_store_id
        if store_id is None:
            name = resource_plan(args.name)["stores"]["secret"]
            matching = [
                store
                for store in _all_resources(api, "/resources/stores/secret")
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
