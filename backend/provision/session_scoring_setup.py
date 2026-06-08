"""Provision + tear down a per-customer session-scoring Compute service.

Pattern mirrors ``ensure_cdn_service`` / ``delete_cdn_service`` in
``backend.provision.fastly_api`` — primitive args in, status callback for
SSE progress, idempotent in both directions, no implicit state outside
the Fastly API + returned dict.

Naming convention (from the research doc):
  - service name:   ``Session Scoring Service for {logging_service_id}``
  - domain:         ``fos-{logging_service_id.lower()}-session-scorer.edgecompute.app``
  - keys store:     ``scoring_keys_{compute_service_id}``
  - config store:   ``scoring_config_{compute_service_id}``

The Wasm deploy itself (``fastly compute deploy``) is NOT done here —
that's the matrix-deploy concern owned by ``scripts/scoring/deploy_wasm.sh``
and gets invoked separately after a training run produces a matrix. This
keeps the provisioner small (~5s API calls) and the deploy slow (~30s
build + upload) as distinct lifecycle stages.
"""

from __future__ import annotations

import secrets
from typing import Any

from backend.core.fastly.client import fastly
from backend.provision.utils import BOLD, _c, info, ok, warn

SCORING_SERVICE_NAME_PREFIX = "Session Scoring Service for "
SCORING_DOMAIN_TEMPLATE = "fos-{sid_lower}-session-scorer.edgecompute.app"
KEYS_STORE_NAME_TEMPLATE = "scoring_keys_{sid}"
CONFIG_STORE_NAME_TEMPLATE = "scoring_config_{sid}"

# Resource-link names match the ConfigStore::open() arguments in
# compute/scorer/src/main.rs. Both must be edited in lockstep.
KEYS_RESOURCE_LINK_NAME = "scoring_keys"
CONFIG_RESOURCE_LINK_NAME = "scoring_config"

# Initial values for the config stores.
DEBUG_LOG_KEY = "debug_logging_enabled"
DEBUG_LOG_DEFAULT = "0"
CURRENT_KEY_HEX = "current_key_hex"
PREVIOUS_KEY_HEX = "previous_key_hex"  # blank until first rotation
# Shared secret VCL → Compute. The customer's VCL service embeds this
# secret in the X-Edge-Scorer-Auth request header before calling the
# scorer; the scorer rejects requests without a matching value. Stops
# the scorer's edgecompute.app domain from being scored on by anyone
# who happens to find the hostname.
REQUEST_SECRET_KEY = "request_secret"


def _scoring_service_name(logging_service_id: str) -> str:
    return f"{SCORING_SERVICE_NAME_PREFIX}{logging_service_id}"


def _scoring_domain(logging_service_id: str) -> str:
    return SCORING_DOMAIN_TEMPLATE.format(sid_lower=logging_service_id.lower())


def _find_scoring_service(logging_service_id: str, token: str) -> dict | None:
    """Return the existing scoring service for this logging service, if any.
    Idempotency lever — ``ensure_scoring_service`` reuses an existing
    service rather than failing on duplicate-name."""
    name = _scoring_service_name(logging_service_id)
    try:
        services = fastly("GET", "/service", token=token) or []
    except RuntimeError:
        return None
    for svc in services:
        if svc.get("name") == name:
            return svc
    return None


def _find_config_store(store_name: str, token: str) -> dict | None:
    try:
        resp = fastly("GET", "/resources/stores/config", token=token)
    except RuntimeError:
        return None
    # Fastly's list endpoint returns either a list or {"data": [...]} depending
    # on the version; tolerate both.
    items = resp if isinstance(resp, list) else resp.get("data", [])
    for item in items:
        if item.get("name") == store_name:
            return item
    return None


def ensure_scoring_service(
    logging_service_id: str,
    token: str,
    *,
    status_cb=None,
) -> dict[str, Any]:
    """Create (or reuse) the per-customer session-scoring Compute service,
    its two ConfigStores, the AES-256 key, and resource links from v1 of
    the service to the stores.

    Returns a dict suitable for stashing into the customer's config:

        {
          "scoring_service_id":  "...",
          "scoring_service_name": "Session Scoring Service for ...",
          "scoring_domain":      "fos-...-session-scorer.edgecompute.app",
          "scoring_keys_store_id":   "...",
          "scoring_config_store_id": "...",
          "aes_key_hex":  "..."   # only populated on first creation
        }

    Idempotent: re-running against an existing scoring service no-ops the
    create steps. The returned ``aes_key_hex`` is empty when reusing an
    existing service (we don't have a way to read back the key once it's
    in the store)."""
    name = _scoring_service_name(logging_service_id)
    domain = _scoring_domain(logging_service_id)

    info(f"Ensuring scoring service {_c(BOLD, name)}")
    if status_cb:
        status_cb(f"⏳ Ensuring scoring service '{name}'...")

    existing = _find_scoring_service(logging_service_id, token)
    if existing:
        ok(f"Scoring service already exists ({existing['id']})")
        if status_cb:
            status_cb(f"✅ Scoring service '{name}' already exists.")
        scoring_service_id = existing["id"]
        keys_store = _find_config_store(KEYS_STORE_NAME_TEMPLATE.format(sid=scoring_service_id), token)
        cfg_store = _find_config_store(CONFIG_STORE_NAME_TEMPLATE.format(sid=scoring_service_id), token)
        return {
            "scoring_service_id": scoring_service_id,
            "scoring_service_name": name,
            "scoring_domain": domain,
            "scoring_keys_store_id": (keys_store or {}).get("id", ""),
            "scoring_config_store_id": (cfg_store or {}).get("id", ""),
            "aes_key_hex": "",
            # On reuse, neither secret is readable back from the store.
            # The orchestrator falls back to whatever it stashed in
            # cfg["scoring"]["request_secret"] on a prior provision.
            "request_secret": "",
        }

    # 1. Create the wasm Compute service.
    svc = fastly("POST", "/service", {"name": name, "type": "wasm"}, token=token)
    scoring_service_id = svc["id"]
    ok(f"Created scoring service {scoring_service_id}")
    if status_cb:
        status_cb(f"✅ Created scoring service '{name}'.")

    # 2. Add the domain to version 1 (auto-created with the service).
    fastly(
        "POST",
        f"/service/{scoring_service_id}/version/1/domain",
        {"name": domain},
        token=token,
    )
    ok(f"Added domain {domain}")
    if status_cb:
        status_cb(f"✅ Added domain '{domain}'.")

    # 3. Add a placeholder backend (Compute services require at least one).
    #    The scorer never calls it; it's just to make the service version
    #    valid.
    fastly(
        "POST",
        f"/service/{scoring_service_id}/version/1/backend",
        {
            "name": "placeholder_origin",
            "address": "127.0.0.1",
            "port": 80,
            "override_host": "example.com",
        },
        token=token,
    )
    ok("Added placeholder backend")

    # 4. Create the two ConfigStores, namespaced by the scoring service id.
    keys_store_name = KEYS_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)
    cfg_store_name = CONFIG_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)

    keys_store = fastly("POST", "/resources/stores/config", {"name": keys_store_name}, token=token)
    cfg_store = fastly("POST", "/resources/stores/config", {"name": cfg_store_name}, token=token)
    ok(f"Created config stores {keys_store_name}, {cfg_store_name}")
    if status_cb:
        status_cb("✅ Created config stores.")

    # 5. Generate the AES-256 key + request secret and write both to
    #    scoring_keys. The request secret is the shared-secret header
    #    value that VCL embeds in X-Edge-Scorer-Auth so the Compute
    #    service can reject requests not coming from "our" VCL.
    aes_key_hex = secrets.token_hex(32)
    request_secret = secrets.token_hex(32)
    fastly(
        "POST",
        f"/resources/stores/config/{keys_store['id']}/item",
        {"item_key": CURRENT_KEY_HEX, "item_value": aes_key_hex},
        token=token,
    )
    fastly(
        "POST",
        f"/resources/stores/config/{keys_store['id']}/item",
        {"item_key": REQUEST_SECRET_KEY, "item_value": request_secret},
        token=token,
    )
    fastly(
        "POST",
        f"/resources/stores/config/{cfg_store['id']}/item",
        {"item_key": DEBUG_LOG_KEY, "item_value": DEBUG_LOG_DEFAULT},
        token=token,
    )
    ok("Populated config stores")

    # 6. Link both stores to the service version so the Wasm can open them
    #    by the short ResourceLink names (scoring_keys / scoring_config).
    fastly(
        "POST",
        f"/service/{scoring_service_id}/version/1/resource",
        {"name": KEYS_RESOURCE_LINK_NAME, "resource_id": keys_store["id"]},
        token=token,
    )
    fastly(
        "POST",
        f"/service/{scoring_service_id}/version/1/resource",
        {"name": CONFIG_RESOURCE_LINK_NAME, "resource_id": cfg_store["id"]},
        token=token,
    )
    ok("Linked stores to service v1")
    if status_cb:
        status_cb("✅ Linked config stores to service v1.")

    return {
        "scoring_service_id": scoring_service_id,
        "scoring_service_name": name,
        "scoring_domain": domain,
        "scoring_keys_store_id": keys_store["id"],
        "scoring_config_store_id": cfg_store["id"],
        "aes_key_hex": aes_key_hex,
        "request_secret": request_secret,
    }


def rotate_aes_key(
    scoring_keys_store_id: str,
    *,
    token: str,
) -> dict:
    """Rotate the AES-GCM cookie-state encryption key for a scoring service.

    Pulls the current ``current_key_hex`` from the scoring_keys
    ConfigStore, moves it to ``previous_key_hex``, generates a fresh
    32-byte key, writes it as the new ``current_key_hex``. The Rust
    scorer's cookie codec tries current first then previous, so cookies
    issued under the old key keep decoding through one full rotation
    cycle (typically the cookie idle-expire window, ~hours).

    Idempotent — calling twice rotates twice, and the previous-previous
    key is dropped (only one rotation grace level by design). Fastly
    ConfigStore items use PUT for replace; ``item_value`` is the new
    hex string.

    Returns ``{"current_key_hex": "<new>", "previous_key_hex": "<was>",
    "rotated_at": "<iso>"}`` so the caller can audit.
    """
    import datetime as _dt
    import secrets

    if not scoring_keys_store_id:
        raise ValueError("scoring_keys_store_id is required")

    # Fetch current to move it into previous_key_hex slot.
    try:
        cur_item = fastly(
            "GET",
            f"/resources/stores/config/{scoring_keys_store_id}/item/{CURRENT_KEY_HEX}",
            token=token,
        )
        prev_value = (cur_item or {}).get("item_value", "") or ""
    except Exception:
        prev_value = ""

    new_key = secrets.token_hex(32)
    rotated_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")

    # PATCH updates an existing item. If previous_key_hex doesn't exist
    # yet (first rotation ever), PATCH 404s — fall back to POST.
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

    return {
        "current_key_hex": new_key,
        "previous_key_hex": prev_value,
        "rotated_at": rotated_at,
    }


def delete_scoring_service(
    scoring_service_id: str,
    *,
    scoring_keys_store_id: str = "",
    scoring_config_store_id: str = "",
    token: str,
    status_cb=None,
) -> None:
    """Tear down the Compute service AND both ConfigStores. Idempotent —
    deleting an already-deleted resource is a no-op.

    Order: service first (deactivate → delete), then stores. Service must
    go first because the resource-link tying the stores to the service
    will block store-deletion otherwise."""
    if not scoring_service_id:
        warn("delete_scoring_service called with empty service id — nothing to do")
        return

    info(f"Tearing down scoring service {_c(BOLD, scoring_service_id)}")
    if status_cb:
        status_cb(f"⏳ Tearing down scoring service '{scoring_service_id}'...")

    # 1. Deactivate any active versions so we can delete the service.
    try:
        versions = fastly("GET", f"/service/{scoring_service_id}/version", token=token) or []
        for v in versions:
            if v.get("active"):
                if status_cb:
                    status_cb(f"⏳ Deactivating version {v['number']}...")
                fastly(
                    "PUT",
                    f"/service/{scoring_service_id}/version/{v['number']}/deactivate",
                    token=token,
                )
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("Scoring service already deleted")
            return
        # fall through; delete still might work
        warn(f"Failed to deactivate versions (will try delete anyway): {exc}")

    # 2. Delete the service.
    try:
        fastly("DELETE", f"/service/{scoring_service_id}", token=token, expect_empty=True)
        ok("Scoring service deleted")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("Scoring service already deleted")
        else:
            raise

    # 3. Delete the config stores. Each lookup-then-delete is tolerant of
    #    "already deleted" so this is safe to re-run.
    for label, store_id in (
        ("scoring_keys", scoring_keys_store_id),
        ("scoring_config", scoring_config_store_id),
    ):
        if not store_id:
            continue
        try:
            fastly(
                "DELETE",
                f"/resources/stores/config/{store_id}",
                token=token,
                expect_empty=True,
            )
            ok(f"Deleted {label} store ({store_id})")
        except RuntimeError as exc:
            if "404" in str(exc):
                ok(f"{label} store already deleted")
            else:
                warn(f"Could not delete {label} store {store_id}: {exc}")

    if status_cb:
        status_cb("✅ Scoring service torn down.")
