"""Generalized Fastly Compute store helpers (Secret, Config, and KV stores + resource links).

Shared between session scoring (`compute/scorer`) and Cloud (`compute/cloud`).
Re-exports session-scoring store helpers for backwards compatibility.
"""

from __future__ import annotations

import base64
from typing import Any

from backend.core.fastly.client import fastly, fastly_raw
from backend.provision.cli import ok
from backend.provision.session_scoring.stores import (
    _MATRIX_PATH,
    _REPO_ROOT,
    _SCORER_PACKAGE,
    COMPUTE_MANAGE_URL,
    CONFIG_RESOURCE_LINK_NAME,
    CONFIG_STORE_MANAGE_URL,
    CONFIG_STORE_NAME_TEMPLATE,
    CURRENT_KEY_HEX,
    DEBUG_LOG_DEFAULT,
    DEBUG_LOG_KEY,
    KEYS_RESOURCE_LINK_NAME,
    KEYS_STORE_NAME_TEMPLATE,
    KV_STORE_MANAGE_URL,
    MATRIX_KEY_NAME,
    MATRIX_RESOURCE_LINK_NAME,
    MATRIX_STORE_NAME_TEMPLATE,
    PREVIOUS_KEY_HEX,
    REQUEST_SECRET_KEY,
    SCORING_ENABLED_AT_KEY,
    _delete_scoring_matrix_from_fos,
    _ensure_matrix_kv_store,
    _ensure_scoring_enabled_at,
    _find_config_store,
    _find_kv_store,
    _find_store_by_name,
    _publish_scoring_fos_side_effects,
    _resolve_tenant_matrix_for_deploy,
    _store_id,
    _tenant_matrix_path,
    _write_matrix_to_kv,
    rotate_aes_key,
)

STORE_ENDPOINTS: dict[str, str] = {
    "config": "/resources/stores/config",
    "kv": "/resources/stores/kv",
    "secret": "/resources/stores/secret",
}


def find_store(kind: str, store_name: str, token: str) -> dict[str, Any] | None:
    """Find a Fastly Compute store (`config`, `kv`, or `secret`) by name."""
    endpoint = STORE_ENDPOINTS.get(kind, f"/resources/stores/{kind}")
    return _find_store_by_name(endpoint, store_name, token)


def ensure_store(kind: str, store_name: str, token: str) -> dict[str, Any]:
    """Find or create a Fastly Compute store (`config`, `kv`, or `secret`)."""
    existing = find_store(kind, store_name, token)
    if existing:
        return existing
    endpoint = STORE_ENDPOINTS.get(kind, f"/resources/stores/{kind}")
    created = fastly("POST", endpoint, {"name": store_name}, token=token)
    ok(f"Created {kind} store {store_name}")
    if isinstance(created, dict) and "id" not in created and isinstance(created.get("data"), dict):
        return created["data"]
    return created


def link_store_resource(
    service_id: str,
    version: int,
    link_name: str,
    store_id: str,
    token: str,
) -> dict[str, Any]:
    """Link a Compute store to a service version as `link_name`."""
    return fastly(
        "POST",
        f"/service/{service_id}/version/{version}/resource",
        {"name": link_name, "resource_id": store_id},
        token=token,
    )


def put_secret_store_entry(store_id: str, name: str, value: str, token: str) -> dict[str, Any]:
    """Upsert a secret in a Fastly Secret Store using base64 JSON encoding."""
    encoded = base64.b64encode(value.encode()).decode()
    return fastly(
        "PUT",
        f"/resources/stores/secret/{store_id}/secrets",
        {"name": name, "secret": encoded},
        token=token,
    )


def put_config_store_item(store_id: str, key: str, value: str, token: str) -> dict[str, Any]:
    """Upsert an item in a Fastly Config Store."""
    return fastly(
        "PUT",
        f"/resources/stores/config/{store_id}/item/{key}",
        {"item_value": value},
        token=token,
    )


def put_kv_store_key(store_id: str, key: str, value: bytes, token: str) -> bytes:
    """Write a raw value into a Fastly KV Store."""
    return fastly_raw(
        "PUT",
        f"/resources/stores/kv/{store_id}/keys/{key}",
        value,
        content_type="application/octet-stream",
        token=token,
    )


__all__ = [
    "COMPUTE_MANAGE_URL",
    "CONFIG_RESOURCE_LINK_NAME",
    "CONFIG_STORE_MANAGE_URL",
    "CONFIG_STORE_NAME_TEMPLATE",
    "CURRENT_KEY_HEX",
    "DEBUG_LOG_DEFAULT",
    "DEBUG_LOG_KEY",
    "KEYS_RESOURCE_LINK_NAME",
    "KEYS_STORE_NAME_TEMPLATE",
    "KV_STORE_MANAGE_URL",
    "MATRIX_KEY_NAME",
    "MATRIX_RESOURCE_LINK_NAME",
    "MATRIX_STORE_NAME_TEMPLATE",
    "PREVIOUS_KEY_HEX",
    "REQUEST_SECRET_KEY",
    "SCORING_ENABLED_AT_KEY",
    "STORE_ENDPOINTS",
    "_MATRIX_PATH",
    "_REPO_ROOT",
    "_SCORER_PACKAGE",
    "_delete_scoring_matrix_from_fos",
    "_ensure_matrix_kv_store",
    "_ensure_scoring_enabled_at",
    "_find_config_store",
    "_find_kv_store",
    "_find_store_by_name",
    "_publish_scoring_fos_side_effects",
    "_resolve_tenant_matrix_for_deploy",
    "_store_id",
    "_tenant_matrix_path",
    "_write_matrix_to_kv",
    "ensure_store",
    "find_store",
    "link_store_resource",
    "put_config_store_item",
    "put_kv_store_key",
    "put_secret_store_entry",
    "rotate_aes_key",
]
