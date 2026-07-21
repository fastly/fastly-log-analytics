"""R-3b: canned Fastly API responses for E2E (Playwright + contract).

The Playwright journeys (Phase 3 R-3c) run against a real backend
process. They must not call the production Fastly API — analyst
provisioning + per-test CDN service creation would either need real
tokens or fail on the wire. `FASTLY_MOCK_MODE=1` short-circuits
``backend.core.fastly.client.fastly()`` to return the right shape per
endpoint, mirroring the same mocks the wizard-E2E pytest already
uses (``tests/routers/test_provision_wizard_e2e.py``).

Fixtures are deliberately bland — minimum fields the orchestrator
reads. Test journeys that need richer payloads should override via
monkeypatch on the call site, not by editing this file.

The fixture file is in the public repo per the audit: it contains
NO real service IDs, NO real tokens, NO real domains. Scrub before
seeding any real response into it (per the ``infra-stays-local``
project memory).
"""

from __future__ import annotations

import os
import re
from typing import Any


def is_mock_mode() -> bool:
    """Single-source check the gated callers read on every request."""
    return os.environ.get("FASTLY_MOCK_MODE") == "1"


def _service_id_from_path(path: str) -> str:
    m = re.match(r"^/service/([^/]+)", path)
    return m.group(1) if m else "mock-svc-id"


def _version_from_path(path: str) -> int:
    m = re.search(r"/version/(\d+)", path)
    return int(m.group(1)) if m else 1


def mock_response(method: str, path: str, body: dict | None = None) -> Any:
    """Return a canned response for the given Fastly endpoint.

    Pattern-matches on (METHOD, path-prefix). Falls through to
    ``{"ok": True}`` for any uncovered endpoint so unknown calls don't
    blow up an E2E journey — they just return a no-op shape. If a
    journey breaks because it relies on a real response shape, that's
    the signal to add a more specific fixture here.
    """
    if path == "/tokens/self" and method == "GET":
        # `validate_destructive_token` reads `scope` (must include
        # "global") and `customer_id` (must match the target service's
        # customer_id from the next call) — the matching customer_id
        # is also pinned in the `/service/{id}` shape below so the
        # tenant check passes in mock mode.
        return {
            "id": "mock-token-id",
            "user_id": "mock-user",
            "scope": "global",
            "customer_id": "mock-customer",
            "services": [],
        }

    if path == "/service" and method == "POST":
        return {"id": "mock-svc-id", "name": (body or {}).get("name", "mock"), "version": 1}

    if path == "/service" and method == "GET":
        # find_service_by_name pages this; the wizard E2E expects an
        # empty list (the name is novel).
        return []

    if re.match(r"^/service/[^/]+$", path) and method in ("GET", "PUT", "DELETE"):
        return {
            "id": _service_id_from_path(path),
            "version": 1,
            # Must match `customer_id` returned by /tokens/self above —
            # `validate_destructive_token` rejects on mismatch.
            "customer_id": "mock-customer",
        }

    # GET-style list endpoints. These MUST match before the generic
    # POST handlers below — orchestrator helpers (get_active_version,
    # list_s3_endpoints, ensure_vcl_snippet, etc.) iterate the
    # response as a list and a dict fallback raises TypeError mid-
    # stream, which kills the SSE before all 8 banners emit.
    if method == "GET" and re.search(r"^/service/[^/]+/version$", path):
        # Single active version is enough — get_active_version returns
        # the first `v["active"]` it finds.
        return [{"number": 1, "active": True}]

    if method == "GET" and re.search(r"^/service/[^/]+/version/active$", path):
        return {"number": 1, "active": True}

    if method == "GET" and "/version/" in path:
        if path.endswith("/logging/s3"):
            return []
        if path.endswith("/snippet"):
            return []
        if path.endswith("/condition"):
            return []
        if path.endswith("/domain"):
            return []
        if path.endswith("/backend"):
            return []
        if path.endswith("/vcl"):
            return []
        # Fall through to the version-pinned dict if the path is a
        # specific resource (e.g. /version/1/domain/foo).

    if "/version/" in path and "/activate" in path:
        return {"number": _version_from_path(path), "active": True}

    if "/version/" in path and "/validate" in path:
        # ensure_logging_endpoint validates the draft before activating
        # it; the orchestrator checks `resp.get("status") == "ok"`.
        return {"status": "ok", "errors": []}

    if "/version/" in path and path.endswith("/clone"):
        # All orchestrator clone calls use PUT (Fastly's documented
        # method for version cloning) — accept POST too so this rule
        # isn't method-pinned.
        return {"number": _version_from_path(path) + 1}

    # Resource-specific rules MUST be checked before the bare
    # `/version` POST fallback below — the orchestrator hits paths
    # like `/service/{id}/version/{v}/dictionary` (POST) which
    # would otherwise match the generic version-create rule and
    # return `{"number": 1}` instead of `{"id": ...}`, crashing
    # the next `dict_resp["id"]` access mid-Step-6.
    if "/domain" in path:
        return {"name": (body or {}).get("name", "mock.example"), "service_id": _service_id_from_path(path)}

    if "/backend" in path:
        return {"name": (body or {}).get("name", "fos_origin"), "address": "mock.example"}

    if "/snippet" in path:
        return {"id": "mock-snippet-id", "name": (body or {}).get("name", "mock_snippet")}

    if "/logging/" in path:
        return {"name": (body or {}).get("name", "mock-logger"), "version": _version_from_path(path)}

    if "/dictionary" in path:
        return {"id": "mock-dict-id", "name": (body or {}).get("name", "mock_dict")}

    if "/acl" in path:
        return {"id": "mock-acl-id", "name": (body or {}).get("name", "mock_acl")}

    if "/condition" in path:
        return {"name": (body or {}).get("name", "mock_condition"), "statement": (body or {}).get("statement", "")}

    if "/vcl" in path:
        return {"name": (body or {}).get("name", "main"), "content": (body or {}).get("content", "")}

    # Generic version-create fallback — must come AFTER the
    # resource-specific rules above so it only catches the actual
    # `/service/{id}/version` POST (and the rare bare-`/version`
    # paths) instead of anything that happens to live under
    # `/version/{v}/`.
    if re.search(r"/version(?:/\d+)?$", path) and method == "POST":
        return {"number": _version_from_path(path)}

    if "/object-storage/access-keys" in path:
        if method == "POST":
            # `ensure_fos_access_key` reads `key["access_key"]` /
            # `key["secret_key"]` directly off the response (see
            # backend/provision/fos_setup.py:368-376). Mirror the flat
            # shape the helper expects — wrapping in JSON:API
            # (`data.attributes.*`) would KeyError mid-orchestrator and
            # the E2E SSE would die before all 8 banners emit.
            return {
                "access_key": "AKIA_MOCK",
                "secret_key": "SECRET_MOCK",  # gitleaks:allow — canned mock secret, not real
                "description": (body or {}).get("description", "fos-log-analysis-mock"),
            }
        if method == "DELETE":
            return {}
        if method == "GET":
            return {"data": []}

    if "/object-storage/buckets" in path:
        if method == "POST":
            return {"data": {"id": "mock-bucket-id", "attributes": {"name": (body or {}).get("name", "mock-bucket")}}}
        if method == "GET":
            return {"data": []}

    # Default: opaque success — orchestrator step paths the audit doesn't
    # explicitly enumerate. Returning {} keeps the response-shape contract
    # minimal (no fictional fields a future test could pin against).
    return {"ok": True}


# ── NGWAF ─────────────────────────────────────────────────────────────


def mock_ngwaf_verified_bots_page() -> dict:
    """Single empty page that satisfies the pagination loop's exit condition."""
    return {"data": [], "meta": {"next_cursor": ""}}
