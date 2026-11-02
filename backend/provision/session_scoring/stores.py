"""KV store, Config store, secret store, and FOS matrix helpers for session scoring."""

from __future__ import annotations

import logging
from pathlib import Path

from backend.core.fastly.client import fastly, fastly_raw
from backend.provision.cli import info, ok, warn

logger = logging.getLogger(__name__)

COMPUTE_MANAGE_URL = "https://manage.fastly.com/products/compute"
KV_STORE_MANAGE_URL = "https://manage.fastly.com/resources/kv-stores"
CONFIG_STORE_MANAGE_URL = "https://manage.fastly.com/resources/config-stores"

KEYS_STORE_NAME_TEMPLATE = "scoring_keys_{sid}"
CONFIG_STORE_NAME_TEMPLATE = "scoring_config_{sid}"
MATRIX_STORE_NAME_TEMPLATE = "scoring_matrix_{sid}"
KEYS_RESOURCE_LINK_NAME = "scoring_keys"
CONFIG_RESOURCE_LINK_NAME = "scoring_config"
MATRIX_RESOURCE_LINK_NAME = "scoring_matrix"
MATRIX_KEY_NAME = "matrix"
DEBUG_LOG_KEY = "debug_logging_enabled"
DEBUG_LOG_DEFAULT = "0"
SCORING_ENABLED_AT_KEY = "scoring_enabled_at"
CURRENT_KEY_HEX = "current_key_hex"
PREVIOUS_KEY_HEX = "previous_key_hex"  # blank until first rotation
REQUEST_SECRET_KEY = "request_secret"

_REPO_ROOT: Path = Path(__file__).resolve().parent.parent.parent.parent
_MATRIX_PATH: Path = _REPO_ROOT / "compute" / "scorer" / "matrix.json"
_SCORER_PACKAGE: Path = _REPO_ROOT / "compute" / "scorer" / "pkg" / "session-scorer.tar.gz"


def _ensure_scoring_enabled_at(config_store_id: str, token: str) -> None:
    """Seed the ``scoring_enabled_at`` deployment-age anchor in the scoring_config
    store IFF absent (write-once). This anchor is now an advisory *readiness gauge*
    only — the backend reads it to surface "deployment age ≥ 7 days" in the admin
    UI as a hint that enough L2 data has been observed to opt into enforcement. It
    does NOT actuate anything (the Rust scorer no longer reads it; L2 enforcement
    is gated solely by the operator's explicit opt-in). Write-once semantics:

      * a brand-new / freshly-recreated store gets ``now`` → its readiness clock
        starts now;
      * an existing anchor is preserved, so a later re-enable / key rotation /
        exclude-regex change does NOT reset the readiness clock.

    Fail-soft: a missing item GETs as a RuntimeError (404) → fall through and
    seed; any write failure is warned, never fatal (the readiness gauge simply
    shows "unknown" when the anchor is absent)."""
    if not config_store_id:
        return
    try:
        existing = fastly(
            "GET", f"/resources/stores/config/{config_store_id}/item/{SCORING_ENABLED_AT_KEY}", token=token
        )
        if (existing or {}).get("item_value"):
            return  # already anchored — preserve the original warm-up start
    except RuntimeError:
        pass  # item absent (404) → seed it below
    import datetime as _dt

    now_epoch = str(int(_dt.datetime.now(_dt.UTC).timestamp()))
    try:
        fastly(
            "POST",
            f"/resources/stores/config/{config_store_id}/item",
            {"item_key": SCORING_ENABLED_AT_KEY, "item_value": now_epoch},
            token=token,
        )
        ok(f"Seeded {SCORING_ENABLED_AT_KEY} warm-up anchor ({now_epoch})")
    except RuntimeError as exc:
        warn(f"Could not seed {SCORING_ENABLED_AT_KEY} anchor: {exc}")


def _find_store_by_name(endpoint: str, store_name: str, token: str) -> dict | None:
    """Find a store named ``store_name`` under ``endpoint`` (idempotency lever)."""
    try:
        resp = fastly("GET", endpoint, token=token)
    except RuntimeError:
        return None
    items = resp if isinstance(resp, list) else resp.get("data", [])
    return next((i for i in items if i.get("name") == store_name), None)


def _find_config_store(store_name: str, token: str) -> dict | None:
    return _find_store_by_name("/resources/stores/config", store_name, token)


def _find_kv_store(store_name: str, token: str) -> dict | None:
    """KV-Store analogue of ``_find_config_store`` (idempotency lever)."""
    return _find_store_by_name("/resources/stores/kv", store_name, token)


def _store_id(resp: dict) -> str:
    """Extract a store id from a create/get response, tolerating both the
    bare ``{"id": ...}`` shape (ConfigStore) and the ``{"data": {"id": ...}}``
    envelope the KV API sometimes returns."""
    if not isinstance(resp, dict):
        return ""
    return resp.get("id") or (resp.get("data") or {}).get("id") or ""


def _ensure_matrix_kv_store(scoring_service_id: str, token: str, *, status_cb=None) -> dict:
    """Find-or-create the per-service matrix KV Store. Idempotent."""
    name = MATRIX_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)
    existing = _find_kv_store(name, token)
    if existing:
        return existing
    created = fastly("POST", "/resources/stores/kv", {"name": name}, token=token)
    ok(f"Created KV store {name}")
    if status_cb:
        status_cb("✅ Created matrix KV store.")
    if isinstance(created, dict) and "id" not in created and isinstance(created.get("data"), dict):
        return created["data"]
    return created


def _tenant_matrix_path(logging_service_id: str) -> Path:
    """Per-tenant matrix path used by retrain / boot / deploy."""
    return _MATRIX_PATH.with_name(f"{_MATRIX_PATH.stem}_{logging_service_id}{_MATRIX_PATH.suffix}")


def _resolve_tenant_matrix_for_deploy(logging_service_id: str) -> Path | None:
    """Return a local path containing this tenant's trained matrix, or None."""
    import json as _json

    tenant_path = _tenant_matrix_path(logging_service_id)
    if tenant_path.exists():
        return tenant_path
    try:
        from backend.core.metadata.base import fetch_matrix_from_fos

        matrix = fetch_matrix_from_fos(logging_service_id)
    except Exception:
        logger.debug("fetch_matrix_from_fos failed during deploy resolve", exc_info=True)
        matrix = None
    if not matrix or matrix.get("vocab_size", 0) <= 0:
        return None
    tenant_path.parent.mkdir(parents=True, exist_ok=True)
    with tenant_path.open("w") as f:
        _json.dump(matrix, f)
    return tenant_path


def _write_matrix_to_kv(matrix_store_id: str, logging_service_id: str, token: str, status_cb=None) -> None:
    """Write the tenant's trained matrix into the scoring_matrix KV Store via     the Fastly API. Resolves the matrix with the existing tenant-scoped chain     (local ``matrix_{sid}.json`` → FOS). No trained matrix → leave the key     absent so the scorer's L2 stays disabled (same as the old empty-default     deploy). This is also exactly what retrain calls to push a new matrix —     no rebuild, takes effect as Wasm instances recycle."""
    if not matrix_store_id:
        warn("no scoring_matrix store id — skipping matrix upload (L2 stays disabled)")
        return
    matrix_path = _resolve_tenant_matrix_for_deploy(logging_service_id)
    if matrix_path is None:
        info("no trained matrix for this tenant — L2 disabled until first retrain")
        return
    import json as _json

    from backend.scoring import serialize_kv

    with matrix_path.open() as f:
        data = serialize_kv(_json.load(f))
    if status_cb:
        status_cb("⏳ [Compute] Writing trained matrix to KV…")
    fastly_raw(
        "PUT",
        f"/resources/stores/kv/{matrix_store_id}/keys/{MATRIX_KEY_NAME}",
        data,
        content_type="application/octet-stream",
        token=token,
    )
    ok(f"Wrote matrix to KV ({len(data)} bytes FSM1) from {matrix_path.name}")


def _publish_scoring_fos_side_effects(logging_service_id: str) -> None:
    """Best-effort republish of the custom_fields list (admin_state.json) + the     trained matrix to FOS so read-only analyst hosts converge on the next     import tick. Never fatal — both legs swallow their own errors. Called on     both the deploy and the redeploy-no-op paths so a skipped redeploy still     keeps FOS consistent."""
    try:
        from backend.core.metadata.base import export_admin_state

        export_admin_state(logging_service_id)
        ok("Published custom_fields to FOS admin_state.json")
    except Exception as exc:
        warn(f"Could not export admin_state to FOS (non-fatal): {exc}")
    try:
        from backend.core.metadata.base import publish_matrix_to_fos

        tenant_matrix_path = _tenant_matrix_path(logging_service_id)
        if tenant_matrix_path.exists():
            import json as _json

            with tenant_matrix_path.open() as f:
                matrix = _json.load(f)
            publish_matrix_to_fos(logging_service_id, matrix)
            ok(f"Published scoring matrix to FOS (version={matrix.get('version', '?')})")
    except Exception as exc:
        warn(f"Could not publish scoring matrix to FOS (non-fatal): {exc}")


def _delete_scoring_matrix_from_fos(logging_service_id: str) -> None:
    """Inverse of publish_matrix_to_fos: remove the scoring-matrix artifacts a     disable would otherwise leave behind — the FOS-published current matrix +     its version history, and the local tenant matrix file. Best-effort; never     fatal. Without this a future re-enable silently re-adopts the stale matrix     via fetch_matrix_from_fos / _resolve_tenant_matrix_for_deploy."""
    try:
        _tenant_matrix_path(logging_service_id).unlink(missing_ok=True)
    except OSError as exc:
        warn(f"Could not remove local tenant matrix file (non-fatal): {exc}")
    try:
        from backend.core.duckdb import _get_fos_client, get_source_for_service
        from backend.core.ingest import _delete_objects_robust
        from backend.core.metadata.base import get_scoring_matrix_key

        source = get_source_for_service(logging_service_id)
        if not source:
            return
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        current_key = get_scoring_matrix_key(source)  # {prefix}/meta/scoring_matrix.json
        keys = [current_key]
        history_prefix = f"{current_key.rsplit('/', 1)[0]}/scoring_matrix_history/"
        try:
            paginator = s3.get_paginator("list_objects_v2", caller_hint="scoring_matrix_cleanup")
            for page in paginator.paginate(Bucket=bucket, Prefix=history_prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        except Exception as exc:  # noqa: BLE001 — history listing is best-effort
            warn(f"Could not list scoring matrix history for cleanup (non-fatal): {exc}")
        deleted = _delete_objects_robust(s3, bucket, keys)
        ok(f"Removed {deleted} scoring matrix object(s) from FOS")
    except Exception as exc:  # noqa: BLE001 — FOS cleanup must never break disable
        warn(f"Could not delete scoring matrix from FOS (non-fatal): {exc}")


def rotate_aes_key(scoring_keys_store_id: str, *, token: str) -> dict:
    """Rotate the AES-GCM cookie-state encryption key for a scoring service."""
    import datetime as _dt
    import secrets

    if not scoring_keys_store_id:
        raise ValueError("scoring_keys_store_id is required")
    try:
        cur_item = fastly(
            "GET", f"/resources/stores/config/{scoring_keys_store_id}/item/{CURRENT_KEY_HEX}", token=token
        )
        prev_value = (cur_item or {}).get("item_value", "") or ""
    except Exception:
        prev_value = ""
    new_key = secrets.token_hex(32)
    rotated_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")

    def _upsert_item(key: str, value: str) -> None:
        try:
            fastly(
                "PATCH",
                f"/resources/stores/config/{scoring_keys_store_id}/item/{key}",
                {"item_value": value},
                token=token,
            )
        except Exception:
            fastly(
                "POST",
                f"/resources/stores/config/{scoring_keys_store_id}/item",
                {"item_key": key, "item_value": value},
                token=token,
            )

    if prev_value:
        _upsert_item(PREVIOUS_KEY_HEX, prev_value)
    _upsert_item(CURRENT_KEY_HEX, new_key)
    ok(f"Rotated AES key at {rotated_at} (previous_key preserved for grace window)")
    return {"current_key_hex": new_key, "previous_key_hex": prev_value, "rotated_at": rotated_at}
