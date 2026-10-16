"""Ratchet: JSON operations without a typed ``response_model`` may only shrink.

An untyped 2xx JSON body (``{}`` / bare ``additionalProperties: true``) reaches
the frontend as ``unknown``. ``UNTYPED_JSON`` is the seeded backlog (26 JSON
routes on release/v3.0.0-beta3; SSE/file routes excluded); remove an entry when
its route gets a model and never add one. ``NON_JSON_EXEMPT`` lists SSE, file,
redirect, beacon and always-501 stub routes whose body is not a JSON contract.
"""

from __future__ import annotations

import pytest

# Never grows. Each entry is a route whose JSON body still needs a response_model.
UNTYPED_JSON = frozenset(
    {
        "POST /api/admin/bot-sources/refresh",
        "POST /api/admin/bots/refresh",
        "POST /api/admin/duckdb/recycle",
        "POST /api/admin/ducklake/migrate",
        "POST /api/admin/metadata-cleanup/{service_id}",
        "POST /api/admin/ngwaf/sync/{service_id}",
        "POST /api/admin/rdns/enrich",
        "POST /api/admin/share/purge",
        "GET /api/admin/iceberg-calendar",
        "GET /api/admin/vcl-health",
        "GET /api/debug/page-telemetry",
        "GET /api/health",
        "POST /api/admin/backfill-window",
        "POST /api/admin/bot-sources/{source_id}/refresh",
        "POST /api/admin/commit-iceberg",
        "POST /api/admin/ingest-logs",
        "POST /api/admin/metadata-cleanup",
        "POST /api/admin/rebuild-local-view",
        "POST /api/admin/reset-logs",
        "POST /api/admin/reset-rum",
        "POST /api/services/{service_id}/cron-settings",
        "POST /api/services/{service_id}/logging-settings/update",
        "POST /api/services/{service_id}/ngwaf-sync",
        "POST /api/sharing/teardown-frontend",
        "POST /api/ux-events",
        "POST /api/web-vitals",
    }
)

NON_JSON_EXEMPT = frozenset(
    {
        "GET /api/admin/quarantine/download/{item_id}",
        "GET /api/services/{service_id}/control-room/error-stream",
        "GET /api/admin/events/stream",
        "GET /api/admin/usage-log/export",
        "GET /api/cron-runs/{run_id}/stream",
        "GET /api/download",
        "GET /api/download-all",
        "GET /api/download-folder",
        "GET /api/log-extents/stream",
        "GET /api/security/proxies/export",
        "GET /api/services/{service_id}/custom-fields/export",
        "GET /api/services/{service_id}/realtime-stream",
        "GET /api/share/oauth/authorize",
        "GET /api/share/oauth/callback",
        "GET /js/faro-sdk.js",
        "GET /js/rum.js",
        "POST /api/admin/share/backup/export",
        "POST /api/dashboard/raw/csv",
        "POST /api/provision/execute",
        "POST /api/provision/reconcile",
        "POST /api/provision/teardown",
        "POST /api/provision/terraform/export",
        "POST /api/services/{service_id}/cmcd/disable",
        "POST /api/services/{service_id}/cmcd/enable",
        "POST /api/services/{service_id}/control-room/allowlist",
        "POST /api/services/{service_id}/control-room/big-red-button",
        "POST /api/services/{service_id}/control-room/cost-governor",
        "POST /api/services/{service_id}/control-room/mitigations",
        "POST /api/services/{service_id}/control-room/rules",
        "POST /api/services/{service_id}/rum/disable",
        "POST /api/services/{service_id}/rum/enable",
        "POST /api/services/{service_id}/rum/upgrade",
        "POST /api/services/{service_id}/scoring/disable",
        "POST /api/services/{service_id}/scoring/enable",
    }
)

UNTYPED_JSON_CEILING = 26
_METHODS = ("get", "post", "put", "patch", "delete")


def _is_untyped(schema: dict | None) -> bool:
    if not schema:
        return True
    if "$ref" in schema or "anyOf" in schema or "oneOf" in schema or "allOf" in schema:
        return False
    if schema.get("type") == "array":
        return False
    if schema.get("properties"):
        return False
    if schema.get("type") in (None, "object"):
        return schema.get("additionalProperties") in (None, True, {})
    return False


@pytest.fixture(scope="module")
def untyped_ops() -> set[str]:
    from backend.main import app

    ops: set[str] = set()
    for path, item in app.openapi()["paths"].items():
        for method in _METHODS:
            op = item.get(method)
            if not op:
                continue
            for code, resp in op.get("responses", {}).items():
                json_body = (resp.get("content") or {}).get("application/json")
                if code.startswith("2") and json_body is not None and _is_untyped(json_body.get("schema")):
                    ops.add(f"{method.upper()} {path}")
    return ops


def test_no_new_untyped_json_operations(untyped_ops):
    new = untyped_ops - UNTYPED_JSON - NON_JSON_EXEMPT
    assert not new, f"Add a response_model (extra='allow' + response_model_exclude_unset=True) to: {sorted(new)}"


def test_allowlists_have_no_stale_entries(untyped_ops):
    stale = (UNTYPED_JSON | NON_JSON_EXEMPT) - untyped_ops
    assert not stale, f"Now typed or removed; delete from the allowlists: {sorted(stale)}"


def test_untyped_json_backlog_only_shrinks():
    assert len(UNTYPED_JSON) <= UNTYPED_JSON_CEILING
    assert not UNTYPED_JSON & NON_JSON_EXEMPT
