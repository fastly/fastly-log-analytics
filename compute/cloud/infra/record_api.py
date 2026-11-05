# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Record allowlisted GET contracts only; never save tokens or full generated VCL."""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import shlex
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def _token() -> str:
    value = os.environ.get("FASTLY_API_TOKEN")
    if value:
        return value
    path = Path(".env")
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, raw = line.removeprefix("export ").split("=", 1)
                if key.strip() == "FASTLY_API_TOKEN":
                    parts = shlex.split(raw)
                    if parts:
                        return parts[0]
    raise ValueError("FASTLY_API_TOKEN unavailable; authenticated recording is blocked")


def _redact(value: Any, *, key: str = "", identifiers: dict[str, str] | None = None) -> Any:
    identifiers = identifiers or {}
    if key == "id" or key.endswith(("_id", "_by")):
        return identifiers.get(value, "REDACTED_ID") if isinstance(value, str) else value
    if key == "services" and isinstance(value, list):
        return ["REDACTED_ID" for _ in value]
    if key == "name" and value in {
        "flp",
        "flp-worker",
        "flp-front",
        "flp_config",
        "flp_secrets",
        "flp_state",
        "flp_front_auth",
        "flp_fos_buckets",
        "flp_fos_credentials",
        "api_fastly",
        "flp_compute",
        "flp_worker",
        "flp_tick",
        "flp_clock",
        "flp_flush",
        "flp_flush_clock",
        "fos_us_east_1",
    }:
        return value
    if (
        key
        in {
            "domain",
            "address",
            "override_host",
            "ssl_sni_hostname",
            "ssl_cert_hostname",
            "host",
        }
        and value
    ):
        return identifiers.get(value, "redacted.example")
    if key in {"name", "comment"} and value:
        return "REDACTED"
    if key == "digest" and value:
        return "REDACTED_DIGEST"
    if isinstance(value, dict):
        return {
            field: _redact(item, key=field, identifiers=identifiers)
            for field, item in value.items()
            if field != "_links" and not re.search(r"token|secret|password|authorization|key$", field, re.IGNORECASE)
        }
    if isinstance(value, list):
        return [_redact(item, identifiers=identifiers) for item in value]
    if isinstance(value, str):
        for original, replacement in identifiers.items():
            value = value.replace(original, replacement)
        if "https://" in value or "http://" in value:
            return re.sub(r"https?://[^ /]+", "https://redacted.example", value)
    return value


def fetch_contract(path: str, *, projection: str = "", identifiers: dict[str, str] | None = None) -> tuple[int, Any]:
    if not path.startswith("/") or "://" in path or "\n" in path:
        raise ValueError("Expected a relative Fastly API path")
    token = _token()
    headers = {"Fastly-Key": token, "Accept": "application/json"}
    request = urllib.request.Request("https://api.fastly.com" + path, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            body = json.loads(response.read())
    except urllib.error.HTTPError as error:
        status = error.code
        body = json.loads(error.read())
    if projection == "token":
        saved = {field: body[field] for field in ("scope", "expires_at", "services") if field in body}
        saved = _redact(saved, identifiers=identifiers)
    elif projection == "vcl":
        saved = {
            "content": "\n".join(
                line
                for line in body.get("content", "").splitlines()
                if re.search(r"ratelimit_opt_in\s+(true|false)\b", line)
            )
        }
    elif projection == "product":
        saved = _redact(body, identifiers=identifiers)
        if isinstance(body, dict) and isinstance(body.get("product"), dict):
            saved["product"] = {field: body["product"][field] for field in ("id", "object") if field in body["product"]}
    else:
        saved = _redact(body, identifiers=identifiers)
    if token in json.dumps(saved):
        raise ValueError("Credential unexpectedly appeared in sanitized response")
    return status, saved


def record(service_id: str, output: Path) -> list[str]:
    _token()
    recorded_at = datetime.datetime.now(datetime.UTC).isoformat()
    output.mkdir(parents=True, exist_ok=True)
    filenames = []

    def get(label: str, path: str, *, projection: str = "") -> Any:
        status, saved = fetch_contract(path, projection=projection)
        filename = label + ".json"
        envelope = {
            "provenance": "recorded authenticated GET; allowlisted/redacted response",
            "recorded_at": recorded_at,
            "request": {
                "method": "GET",
                "path": path.replace(service_id, "TEST_SERVICE"),
            },
            "status": status,
            "redaction": (
                "IDs, names, credentials and identifying hosts removed; token endpoint "
                "only scope/expiry/services retained; generated VCL only ERL pragma retained"
            ),
            "response": saved,
        }
        (output / filename).write_text(json.dumps(envelope, indent=2) + "\n")
        filenames.append(filename)
        print(f"{filename}: GET returned {status}")
        return saved if status == 200 else None

    get("token-self", "/tokens/self", projection="token")
    get("object-storage", "/enabled-products/v1/object_storage", projection="product")
    versions = get("test-service-versions", f"/service/{service_id}/version")
    active = next((version for version in versions or [] if version.get("active")), None)
    if active:
        base = f"/service/{service_id}/version/{active['number']}"
        get("test-service-generated-vcl", base + "/generated_vcl", projection="vcl")
        get("test-service-resource-links", base + "/resource")
        get("test-service-dictionaries", base + "/dictionary")
    get("missing-service", "/service/flp-contract-nonexistent-service/version")
    for kind in ("config", "secret", "kv"):
        get(f"{kind}-stores", f"/resources/stores/{kind}")
    return filenames


def install_identifiers(inventory: dict[str, Any]) -> dict[str, str]:
    result = {
        "api.fastly.com": "api.fastly.com",
        "us-east-1.object.fastlystorage.app": "us-east-1.object.fastlystorage.app",
    }
    for role, service in inventory["services"].items():
        result[service["id"]] = f"INSTALL_{role.upper()}_ID"
        result[service["domain"]] = f"{role}.example"
    for kind, store in inventory["stores"].items():
        result[store["id"]] = f"STORE_{kind.upper()}_ID"
    for role, dictionary in inventory["dictionaries"].items():
        result[dictionary["id"]] = f"DICTIONARY_{role.upper()}_ID"
    return result


def record_install(inventory_path: Path, output: Path) -> list[str]:
    inventory = json.loads(inventory_path.read_text())
    identifiers = install_identifiers(inventory)
    recorded_at = datetime.datetime.now(datetime.UTC).isoformat()
    paths = []
    for role, service in inventory["services"].items():
        base = f"/service/{service['id']}"
        version = f"{base}/version/{service['version']}"
        paths.extend(
            [
                (f"install-{role}-service", base),
                (f"install-{role}-versions", base + "/version"),
                (f"install-{role}-backends", version + "/backend"),
            ]
        )
        if role != "front":
            paths.append((f"install-{role}-resource-links", version + "/resource"))
        else:
            paths.extend(
                [
                    ("install-front-dictionaries", version + "/dictionary"),
                    ("install-front-healthchecks", version + "/healthcheck"),
                    ("install-front-validation", version + "/validate"),
                ]
            )
            for name, dictionary in inventory["dictionaries"].items():
                paths.append(
                    (
                        f"install-front-{name}-dictionary-info",
                        f"{version}/dictionary/{dictionary['id']}/info",
                    )
                )
    for kind, store in inventory["stores"].items():
        paths.append((f"install-{kind}-store", f"/resources/stores/{kind}/{store['id']}"))
    output.mkdir(parents=True, exist_ok=True)
    filenames = []
    for label, path in paths:
        status, response = fetch_contract(path, identifiers=identifiers)
        request_path = path
        for original, replacement in identifiers.items():
            request_path = request_path.replace(original, replacement)
        filename = label + ".json"
        (output / filename).write_text(
            json.dumps(
                {
                    "provenance": "recorded authenticated GET of CLI-created install; redacted readback",
                    "recording_group": "install",
                    "recorded_at": recorded_at,
                    "request": {"method": "GET", "path": request_path},
                    "status": status,
                    "redaction": (
                        "IDs consistently pseudonymized; fixed resource aliases preserved; "
                        "identifying hosts/names/credentials removed; dictionary values never fetched"
                    ),
                    "response": response,
                },
                indent=2,
            )
            + "\n"
        )
        filenames.append(filename)
        print(f"{filename}: GET returned {status}")
    return filenames


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--service-id")
    source.add_argument("--inventory", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("app/tests/fixtures/fastly-api"),
    )
    arguments = parser.parse_args()
    if arguments.inventory:
        record_install(arguments.inventory, arguments.output)
    else:
        record(arguments.service_id, arguments.output)
