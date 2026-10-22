"""Ratchet: every JSON operation has a typed ``response_model``.

An untyped 2xx JSON body (``{}`` / bare ``additionalProperties: true``) reaches
the frontend as ``unknown``. ``UNTYPED_JSON`` (the former backlog) is now empty: a new JSON route
without a ``response_model`` fails here. ``NON_JSON_EXEMPT`` lists SSE, file, redirect and
always-501 stub routes whose body is not a JSON contract.
"""

from __future__ import annotations

import pytest

# Empty: every JSON route has a response_model. Never add an entry — type the route instead.
UNTYPED_JSON: frozenset[str] = frozenset()

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
        "GET /api/services/{service_id}/realtime-stream",
        "GET /api/share/oauth/authorize",
        "GET /api/share/oauth/callback",
        "GET /js/faro-sdk.js",
        "GET /js/rum.js",
        "POST /api/admin/metadata-cleanup",
        "POST /api/admin/metadata-cleanup/{service_id}",
        "POST /api/admin/reset-logs",
        "POST /api/admin/reset-rum",
        "POST /api/admin/share/backup/export",
        "POST /api/dashboard/raw/csv",
        "POST /api/provision/execute",
        "POST /api/provision/reconcile",
        "POST /api/provision/teardown",
        "POST /api/services/{service_id}/cmcd/disable",
        "POST /api/services/{service_id}/cmcd/enable",
        "POST /api/services/{service_id}/cron-settings",
        "POST /api/services/{service_id}/logging-settings/update",
        "POST /api/services/{service_id}/ngwaf-sync",
        "POST /api/services/{service_id}/rum/disable",
        "POST /api/services/{service_id}/rum/enable",
        "POST /api/services/{service_id}/rum/upgrade",
        "POST /api/services/{service_id}/scoring/disable",
        "POST /api/services/{service_id}/scoring/enable",
    }
)

UNTYPED_JSON_CEILING = 0
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


def test_export_downloads_declare_their_body_in_openapi():
    """File-download routes must say what they return so the client never sees ``unknown``."""
    from backend.main import app

    paths = app.openapi()["paths"]

    tf = paths["/api/provision/terraform/export"]["post"]["responses"]["200"]["content"]
    assert tf == {"application/zip": {"schema": {"type": "string", "format": "binary"}}}

    cf = paths["/api/services/{service_id}/custom-fields/export"]["get"]["responses"]["200"]["content"]
    assert cf == {"application/json": {"schema": {"$ref": "#/components/schemas/CustomFieldsExportFile"}}}


def test_custom_fields_export_file_matches_import_body():
    from backend.models.provision import CustomFieldsExportFile, CustomFieldsImportBody

    assert (
        CustomFieldsExportFile.model_json_schema()["properties"]
        == CustomFieldsImportBody.model_json_schema()["properties"]
    )
