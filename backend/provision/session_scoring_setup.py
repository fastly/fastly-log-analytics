"""Provision + tear down a per-customer session-scoring Compute service.

Pattern mirrors ``ensure_cdn_service`` / ``delete_cdn_service`` in
``backend.provision.fastly_api`` — primitive args in, status callback for
SSE progress, idempotent in both directions, no implicit state outside
the Fastly API + returned dict.

Naming convention (from the research doc):
  - service name:   ``Session Scoring Service for {logging_service_id}``
  - domain:         ``fos-{logging_service_id.lower()}-session-scorer.edgecompute.app``
  - keys store:     ``scoring_keys_{compute_service_id}``    (ConfigStore)
  - config store:   ``scoring_config_{compute_service_id}``  (ConfigStore)
  - matrix store:   ``scoring_matrix_{compute_service_id}``  (KV Store)

The Wasm deploy itself is NOT done here. The published Wasm is matrix-less
and built once (committed ``compute/scorer/pkg/session-scorer.tar.gz``);
``enable_scoring`` uploads that prebuilt package via the Fastly API and
writes the trained matrix into the ``scoring_matrix`` KV Store — no Rust
toolchain on the backend, and retrain becomes a KV write. This provisioner
just creates + links the stores (~5s of API calls).
"""

from __future__ import annotations

import secrets
from typing import Any

from backend.core.fastly.client import fastly
from backend.provision.fos_setup import product_enabled
from backend.provision.utils import BOLD, _c, info, ok, warn

# Fastly control-panel deep links surfaced to the operator when a required
# product isn't enabled on their account. The scorer needs all three on a
# Compute account (Compute itself, plus Config Store + KV Store — both are
# implicit for VCL services but must be enabled for Compute).
COMPUTE_MANAGE_URL = "https://manage.fastly.com/products/compute"
KV_STORE_MANAGE_URL = "https://manage.fastly.com/resources/kv-stores"
CONFIG_STORE_MANAGE_URL = "https://manage.fastly.com/resources/config-stores"


class EntitlementError(Exception):
    """A Fastly product required by session scoring isn't enabled on the
    account. Carries a machine-readable ``code`` and an actionable
    ``link`` (a manage.fastly.com deep link) so the SSE layer can render a
    clickable "enable it" message instead of a raw ``HTTP 4xx`` string.

    Not a RuntimeError subclass on purpose: the create path maps ``fastly``'s
    ``RuntimeError("HTTP 4xx …")`` into this richer type, and keeping it off the
    RuntimeError hierarchy ensures a stray ``except RuntimeError`` upstream
    can't silently swallow an entitlement failure."""

    def __init__(self, code: str, message: str, link: str = ""):
        super().__init__(message)
        self.code = code
        self.link = link


SCORING_SERVICE_NAME_PREFIX = "Session Scoring Service for "
SCORING_DOMAIN_TEMPLATE = "fos-{sid_lower}-session-scorer.edgecompute.app"
KEYS_STORE_NAME_TEMPLATE = "scoring_keys_{sid}"
CONFIG_STORE_NAME_TEMPLATE = "scoring_config_{sid}"
MATRIX_STORE_NAME_TEMPLATE = "scoring_matrix_{sid}"

# Resource-link names match the store-open() arguments in the Rust scorer:
# KEYS/CONFIG are ConfigStore::open() in main.rs; MATRIX is
# KVStore::open(MATRIX_STORE) in matrix.rs. All must be edited in lockstep.
KEYS_RESOURCE_LINK_NAME = "scoring_keys"
CONFIG_RESOURCE_LINK_NAME = "scoring_config"
MATRIX_RESOURCE_LINK_NAME = "scoring_matrix"
# KV key the matrix is stored under — matches MATRIX_KEY in
# compute/scorer/src/matrix.rs. Edit in lockstep.
MATRIX_KEY_NAME = "matrix"

# Initial values for the config stores.
DEBUG_LOG_KEY = "debug_logging_enabled"
DEBUG_LOG_DEFAULT = "0"
# UNIX-epoch (seconds) at which scoring was deployed on this service. Written
# write-once-if-absent so a re-enable / key rotation / regex change never resets
# it. NO LONGER an actuator: the Rust scorer does not read this key. It is now
# purely an advisory *readiness gauge* — the backend surfaces "deployment age ≥ 7
# days" in the admin UI as a hint that there's enough observed L2 data to opt
# into enforcement, but it never turns L2 on automatically. The actual L2
# enforcement gate is the operator's explicit opt-in (see L2_ENFORCE_ENABLED_KEY).
SCORING_ENABLED_AT_KEY = "scoring_enabled_at"
# Layer-2 enforcement opt-in (scoring_config). ``l2_enforce_enabled`` is the
# explicit operator switch the Rust scorer reads to decide whether L2 joins the
# *enforced* combined score ("1" = on); ``l2_enabled_at`` is the UNIX-epoch
# anchor the backend stamps on the off→on transition so the scorer can fade L2
# in over a few days from the moment of consent (NOT from deployment age). Both
# are written by the ``/scoring/l2-enforce`` admin endpoint, not at provision
# time — a freshly-provisioned service has neither key, so L2 stays observe-only
# until an operator explicitly opts in. The 7-day ``scoring_enabled_at`` deploy
# age above is now only an advisory readiness gauge surfaced in the admin UI.
L2_ENFORCE_ENABLED_KEY = "l2_enforce_enabled"
L2_ENABLED_AT_KEY = "l2_enabled_at"
CURRENT_KEY_HEX = "current_key_hex"
PREVIOUS_KEY_HEX = "previous_key_hex"  # blank until first rotation
# Shared secret VCL → Compute. The customer's VCL service embeds this
# secret in the X-Edge-Scorer-Auth request header before calling the
# scorer; the scorer rejects requests without a matching value. Stops
# the scorer's edgecompute.app domain from being scored on by anyone
# who happens to find the hostname.
REQUEST_SECRET_KEY = "request_secret"


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
            "GET",
            f"/resources/stores/config/{config_store_id}/item/{SCORING_ENABLED_AT_KEY}",
            token=token,
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


def _find_store_by_name(endpoint: str, store_name: str, token: str) -> dict | None:
    """Find a store named ``store_name`` under ``endpoint`` (idempotency lever).

    Shared body for ``_find_config_store`` / ``_find_kv_store`` — the only
    difference between the two was the resource path. Fastly's list endpoint
    returns either a bare list or a ``{"data": [...]}`` envelope depending on
    the version; tolerate both. Returns the store object ({"id", "name", ...})
    or None (also None when the list call RuntimeErrors)."""
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
    """Find-or-create the per-service matrix KV Store. Idempotent.

    Does NOT link the store to a service version — resource links are
    versioned config, so linking happens where the scoring service version is
    cloned + activated (``_deploy_wasm_package`` in the orchestrator), which
    keeps the link on the version that actually ships the package. Returns the
    store object (normalized to carry ``id``)."""
    name = MATRIX_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)
    existing = _find_kv_store(name, token)
    if existing:
        return existing
    created = fastly("POST", "/resources/stores/kv", {"name": name}, token=token)
    ok(f"Created KV store {name}")
    if status_cb:
        status_cb("✅ Created matrix KV store.")
    # Normalize the {"data": {...}} envelope to a bare store dict.
    if isinstance(created, dict) and "id" not in created and isinstance(created.get("data"), dict):
        return created["data"]
    return created


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
        matrix_store = _find_kv_store(MATRIX_STORE_NAME_TEMPLATE.format(sid=scoring_service_id), token)
        # Self-heal: a service provisioned before the KV-matrix change won't
        # have a matrix store yet. Create + link it on reuse so re-enabling
        # an old service backfills the store rather than failing the KV write.
        if not matrix_store:
            matrix_store = _ensure_matrix_kv_store(scoring_service_id, token, status_cb=status_cb)
        # Self-heal the deployment-age readiness anchor. A service provisioned
        # before this anchor existed won't have scoring_enabled_at yet; seed it
        # now (write-once) so the admin readiness gauge has a clock to count
        # from. It does NOT turn L2 on — enforcement is opt-in only.
        _ensure_scoring_enabled_at((cfg_store or {}).get("id", ""), token)
        # The request_secret IS a readable ConfigStore item (unlike the AES
        # cookie key, which we deliberately never read back). Read it so the
        # caller embeds the STORE's actual secret into the VCL — the store is
        # the source of truth. Without this, the orchestrator fell back to
        # cfg["scoring"]["request_secret"], and a cfg-vs-store drift (e.g.
        # running enable from a different host) desynced the VCL's
        # X-Edge-Scorer-Auth from the store, making the scorer 401 every
        # request → 100% fail-open.
        store_request_secret = ""
        if keys_store:
            try:
                item = fastly(
                    "GET",
                    f"/resources/stores/config/{keys_store['id']}/item/{REQUEST_SECRET_KEY}",
                    token=token,
                )
                store_request_secret = (item or {}).get("item_value", "") or ""
            except RuntimeError:
                store_request_secret = ""  # missing → caller heals
        return {
            "scoring_service_id": scoring_service_id,
            "scoring_service_name": name,
            "scoring_domain": domain,
            "scoring_keys_store_id": (keys_store or {}).get("id", ""),
            "scoring_config_store_id": (cfg_store or {}).get("id", ""),
            "scoring_matrix_store_id": _store_id(matrix_store or {}),
            "aes_key_hex": "",  # AES key stays write-only by policy.
            "request_secret": store_request_secret,
        }

    # ── Fresh create. The scorer runs on Compute, which needs THREE Fastly
    # product types enabled on the account: Compute itself, Config Store, and
    # KV Store. (Config + KV stores are implicit for VCL services but must be
    # separately enabled for Compute.) Only KV Store exposes a status endpoint,
    # so we check it up front and let the Compute + Config Store *creates* be
    # their own entitlement checks — a 4xx there means the product is off.
    # Everything created below is rolled back if any later step fails, so a
    # botched enable never leaves an orphaned half-built service behind. ───────
    if not product_enabled(token, "kv_store"):
        raise EntitlementError(
            "kv_store_not_enabled",
            "KV Store isn't enabled on this Fastly account, and the scorer needs it to "
            "hold the L2 transition matrix. Enable the KV Store product for your account, "
            "then click Enable again.",
            KV_STORE_MANAGE_URL,
        )

    # Track ids as we create resources so the rollback can tear down exactly
    # what exists (delete_scoring_service tolerates empty/missing ids).
    scoring_service_id = ""
    keys_store_id = ""
    cfg_store_id = ""
    matrix_store_id = ""
    try:
        # 1. Create the wasm Compute service. First durable resource → a 4xx
        #    here means Compute isn't enabled on the account; nothing exists
        #    yet to clean up.
        try:
            svc = fastly("POST", "/service", {"name": name, "type": "wasm"}, token=token)
        except RuntimeError as exc:
            if "HTTP 4" in str(exc):
                raise EntitlementError(
                    "compute_not_enabled",
                    "Compute isn't enabled on this Fastly account, and session scoring runs as a "
                    "Compute (Wasm) service. Enable Compute for your account, then click Enable again.",
                    COMPUTE_MANAGE_URL,
                ) from exc
            raise
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
        #    The create IS the Config Store entitlement check (no status
        #    endpoint exists) — a 4xx means it isn't enabled for Compute.
        keys_store_name = KEYS_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)
        cfg_store_name = CONFIG_STORE_NAME_TEMPLATE.format(sid=scoring_service_id)

        try:
            keys_store = fastly("POST", "/resources/stores/config", {"name": keys_store_name}, token=token)
            keys_store_id = keys_store["id"]
            cfg_store = fastly("POST", "/resources/stores/config", {"name": cfg_store_name}, token=token)
            cfg_store_id = cfg_store["id"]
        except RuntimeError as exc:
            if "HTTP 4" in str(exc):
                raise EntitlementError(
                    "config_store_not_enabled",
                    "Config Store isn't enabled for Compute on this Fastly account. The scorer keeps "
                    "its keys and toggles in two Config Stores. Enable the Config Store product for your "
                    "account, then click Enable again.",
                    CONFIG_STORE_MANAGE_URL,
                ) from exc
            raise
        ok(f"Created config stores {keys_store_name}, {cfg_store_name}")
        if status_cb:
            status_cb("✅ Created config stores.")

        # 4b. Create the matrix KV Store (1.8MB trained matrix won't fit a
        #     ConfigStore's ~8KB item limit). Seeded with the trained matrix by
        #     enable_scoring via the Fastly API; read at runtime by the scorer.
        matrix_store = _ensure_matrix_kv_store(scoring_service_id, token, status_cb=status_cb)
        matrix_store_id = _store_id(matrix_store)

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
        # Stamp the deployment-age readiness anchor (now) for the admin readiness
        # gauge. L2 stays observe-only until an operator explicitly opts into
        # enforcement (the /scoring/l2-enforce endpoint writes the opt-in keys) —
        # the deploy age never turns L2 on by itself. See _ensure_scoring_enabled_at.
        _ensure_scoring_enabled_at(cfg_store["id"], token)
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
        fastly(
            "POST",
            f"/service/{scoring_service_id}/version/1/resource",
            {"name": MATRIX_RESOURCE_LINK_NAME, "resource_id": _store_id(matrix_store)},
            token=token,
        )
        ok("Linked stores to service v1")
        if status_cb:
            status_cb("✅ Linked config + matrix stores to service v1.")

        return {
            "scoring_service_id": scoring_service_id,
            "scoring_service_name": name,
            "scoring_domain": domain,
            "scoring_keys_store_id": keys_store["id"],
            "scoring_config_store_id": cfg_store["id"],
            "scoring_matrix_store_id": _store_id(matrix_store),
            "aes_key_hex": aes_key_hex,
            "request_secret": request_secret,
        }
    except Exception:
        # Roll back EVERYTHING we created so a retry starts from a clean slate
        # (the reuse path above does NOT backfill missing stores, so a partial
        # service would otherwise wedge every future enable). Best-effort and
        # never masks the original error.
        if scoring_service_id:
            warn(f"Enable failed — rolling back partial scoring service {scoring_service_id}")
            if status_cb:
                status_cb("⚠️ Enable failed — rolling back the partially-created scoring service…")
            try:
                delete_scoring_service(
                    scoring_service_id,
                    scoring_keys_store_id=keys_store_id,
                    scoring_config_store_id=cfg_store_id,
                    scoring_matrix_store_id=matrix_store_id,
                    token=token,
                )
            except Exception:
                warn("Rollback of partial scoring service failed — manual cleanup may be needed")
        raise


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
    scoring_matrix_store_id: str = "",
    token: str,
    status_cb=None,
) -> list[tuple[str, str]]:
    """Tear down the Compute service AND its stores (2 ConfigStores + the
    matrix KV Store). Idempotent — deleting an already-deleted resource is a
    no-op.

    Order: service first (deactivate → delete), then stores. Service must
    go first because the resource-link tying the stores to the service
    will block store-deletion otherwise.

    Returns the list of ``(label, store_id)`` pairs that genuinely failed to
    delete (a non-404 error) so the caller can surface them for manual cleanup —
    empty list on full success. A non-404 store/KV delete is downgraded to a
    warning (+ status_cb) rather than raised so one stuck store can't block the
    rest of the teardown; the returned list is how that gets reported instead of
    silently swallowed."""
    failed: list[tuple[str, str]] = []
    if not scoring_service_id:
        warn("delete_scoring_service called with empty service id — nothing to do")
        return failed

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
            return failed
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
                failed.append((label, store_id))
                if status_cb:
                    status_cb(f"⚠️ Could not delete {label} store {store_id} — remove it manually: {exc}")

    # 4. Delete the matrix KV Store. KV stores must be empty before deletion,
    #    so drop the matrix key first, then the store. Both tolerant of 404.
    if scoring_matrix_store_id:
        for sub in (f"/keys/{MATRIX_KEY_NAME}", ""):
            try:
                fastly(
                    "DELETE",
                    f"/resources/stores/kv/{scoring_matrix_store_id}{sub}",
                    token=token,
                    expect_empty=True,
                )
            except RuntimeError as exc:
                if "404" not in str(exc):
                    warn(f"Could not delete matrix KV {sub or 'store'} {scoring_matrix_store_id}: {exc}")
                    # Only the store-level delete (sub == "") is a real orphan;
                    # a stuck key-delete just means the store delete below/next
                    # run will report it.
                    if sub == "":
                        failed.append(("scoring_matrix", scoring_matrix_store_id))
                        if status_cb:
                            status_cb(
                                f"⚠️ Could not delete matrix KV store {scoring_matrix_store_id} "
                                f"— remove it manually: {exc}"
                            )
        ok(f"Deleted matrix KV store ({scoring_matrix_store_id})")

    if status_cb:
        status_cb("✅ Scoring service torn down.")
    return failed
