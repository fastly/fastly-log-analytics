"""Tests for backend.provision.session_scoring_setup.

The provisioner is a thin orchestrator over Fastly's HTTP API — every test
here pins one assertion about WHAT the Fastly API gets called with (URL,
method, JSON body), and a few about the idempotency contract. Real Fastly
roundtrips live in the manual deploy script."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from backend.provision import session_scoring_setup as sss
from backend.provision.session_scoring_setup import (
    COMPUTE_MANAGE_URL,
    CONFIG_RESOURCE_LINK_NAME,
    CONFIG_STORE_MANAGE_URL,
    CURRENT_KEY_HEX,
    DEBUG_LOG_DEFAULT,
    DEBUG_LOG_KEY,
    KEYS_RESOURCE_LINK_NAME,
    KV_STORE_MANAGE_URL,
    MATRIX_RESOURCE_LINK_NAME,
    SCORING_ENABLED_AT_KEY,
    EntitlementError,
    delete_scoring_service,
    ensure_scoring_service,
)

LOG_SVC = "TestLogSvcABC123"
TOKEN = "FAKE_TOKEN"


# ── Name + domain templates ──────────────────────────────────────────────────


def test_service_name_pattern():
    assert sss._scoring_service_name("ABC123") == "Session Scoring Service for ABC123"


def test_domain_lowercases_service_id():
    # Customer-facing domain must be RFC-1035 lowercase even if the
    # logging service id is mixed-case.
    d = sss._scoring_domain("MixedCaseSvcId001")
    assert d == "fos-mixedcasesvcid001-session-scorer.edgecompute.app"
    assert d == d.lower()


# ── ensure_scoring_service: create-from-scratch path ─────────────────────────


def _fastly_mock_for_create():
    """A MagicMock that returns sensible defaults for each API call the
    create-path makes. The side_effect is keyed off the (method, path)
    tuple so test assertions can verify call order without coupling to
    side-effect ordering."""
    responses = {
        ("GET", "/service"): [],  # no pre-existing services
        ("POST", "/service"): {"id": "SCORING_SVC_ID", "name": "x", "type": "wasm"},
        ("POST", "/service/SCORING_SVC_ID/version/1/domain"): {},
        ("POST", "/service/SCORING_SVC_ID/version/1/backend"): {},
        ("POST", "/resources/stores/config"): None,  # set below per call
        ("POST", "/resources/stores/config/KEYS_STORE_ID/item"): {},
        ("POST", "/resources/stores/config/CFG_STORE_ID/item"): {},
        ("POST", "/service/SCORING_SVC_ID/version/1/resource"): {},
        # Matrix KV store: not found on first list, then created.
        ("GET", "/resources/stores/kv"): [],
        ("POST", "/resources/stores/kv"): {"id": "MATRIX_STORE_ID", "name": "scoring_matrix_SCORING_SVC_ID"},
    }

    config_store_responses = iter(
        [
            {"id": "KEYS_STORE_ID", "name": "scoring_keys_SCORING_SVC_ID"},
            {"id": "CFG_STORE_ID", "name": "scoring_config_SCORING_SVC_ID"},
        ]
    )

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("POST", "/resources/stores/config"):
            return next(config_store_responses)
        return responses.get((method, path), {})

    return MagicMock(side_effect=side_effect)


def test_ensure_creates_full_stack_when_nothing_exists():
    fastly_mock = _fastly_mock_for_create()
    # KV-Store entitlement probe uses fos_setup.product_enabled (a separate
    # `fastly` binding), so patch it True or it leaks a real api.fastly.com call.
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    # Returned dict contains all the IDs the caller needs to stash.
    assert result["scoring_service_id"] == "SCORING_SVC_ID"
    assert result["scoring_service_name"] == f"Session Scoring Service for {LOG_SVC}"
    assert result["scoring_domain"] == f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app"
    assert result["scoring_keys_store_id"] == "KEYS_STORE_ID"
    assert result["scoring_config_store_id"] == "CFG_STORE_ID"
    assert result["scoring_matrix_store_id"] == "MATRIX_STORE_ID"
    # Real AES key — 64 hex chars (32 bytes).
    assert len(result["aes_key_hex"]) == 64
    int(result["aes_key_hex"], 16)  # valid hex

    # Verify the API was called with the right shapes (subset of all calls).
    calls = fastly_mock.call_args_list
    # 1. List services (idempotency probe).
    assert call("GET", "/service", token=TOKEN) in calls
    # 2. Create scoring service as wasm type.
    create_svc_call = next(c for c in calls if c.args[:2] == ("POST", "/service"))
    assert create_svc_call.args[2] == {"name": f"Session Scoring Service for {LOG_SVC}", "type": "wasm"}
    # 3. Add domain.
    domain_call = next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/domain")
    assert domain_call.args[2]["name"].endswith("session-scorer.edgecompute.app")
    # 4. Add placeholder backend.
    backend_call = next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/backend")
    assert backend_call.args[2]["name"] == "placeholder_origin"
    # 5. Two stores created.
    store_creates = [c for c in calls if c.args[:2] == ("POST", "/resources/stores/config")]
    assert len(store_creates) == 2
    assert any("scoring_keys_" in c.args[2]["name"] for c in store_creates)
    assert any("scoring_config_" in c.args[2]["name"] for c in store_creates)
    # 6. AES key uploaded to keys store.
    key_upload = next(c for c in calls if c.args[1].endswith("/KEYS_STORE_ID/item"))
    assert key_upload.args[2]["item_key"] == CURRENT_KEY_HEX
    assert len(key_upload.args[2]["item_value"]) == 64
    # 7. Debug toggle uploaded to config store, default "0".
    debug_upload = next(c for c in calls if c.args[1].endswith("/CFG_STORE_ID/item"))
    assert debug_upload.args[2] == {"item_key": DEBUG_LOG_KEY, "item_value": DEBUG_LOG_DEFAULT}
    # 7b. Layer-2 warm-up anchor seeded into the config store with an epoch value
    #     (EC-01). It rides the same POST .../CFG_STORE_ID/item path as the debug
    #     toggle, so select it by item_key.
    cfg_item_posts = [c for c in calls if c.args[:2] == ("POST", "/resources/stores/config/CFG_STORE_ID/item")]
    anchor_upload = next(c for c in cfg_item_posts if c.args[2]["item_key"] == SCORING_ENABLED_AT_KEY)
    assert anchor_upload.args[2]["item_value"].isdigit(), "anchor must be epoch seconds"
    assert int(anchor_upload.args[2]["item_value"]) > 0
    # 8. Resource links created with the short names the Wasm expects
    #    (keys + config ConfigStores + the matrix KV store).
    link_calls = [c for c in calls if c.args[1].endswith("/version/1/resource")]
    assert len(link_calls) == 3
    link_names = {c.args[2]["name"] for c in link_calls}
    assert link_names == {KEYS_RESOURCE_LINK_NAME, CONFIG_RESOURCE_LINK_NAME, MATRIX_RESOURCE_LINK_NAME}


# ── ensure_scoring_service: entitlement gating + rollback ────────────────────


def _create_side_effect(*, fail_on=None, fail_exc=None):
    """`fastly` mock for the create path that ALSO answers the rollback
    (delete_scoring_service) calls, so a mid-build failure can be asserted to
    tear everything down. Raises ``fail_exc`` whenever it sees the
    ``(method, path)`` tuple ``fail_on``."""
    config_store_responses = iter(
        [
            {"id": "KEYS_STORE_ID", "name": "scoring_keys_SCORING_SVC_ID"},
            {"id": "CFG_STORE_ID", "name": "scoring_config_SCORING_SVC_ID"},
        ]
    )
    base = {
        ("GET", "/service"): [],  # idempotency probe: nothing exists
        ("POST", "/service"): {"id": "SCORING_SVC_ID", "name": "x", "type": "wasm"},
        ("GET", "/resources/stores/kv"): [],
        ("POST", "/resources/stores/kv"): {"id": "MATRIX_STORE_ID", "name": "scoring_matrix_SCORING_SVC_ID"},
        # rollback (delete_scoring_service): no active versions → straight to deletes.
        ("GET", "/service/SCORING_SVC_ID/version"): [],
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if fail_on and (method, path) == fail_on:
            raise fail_exc
        if (method, path) == ("POST", "/resources/stores/config"):
            return next(config_store_responses)
        return base.get((method, path), {})

    return MagicMock(side_effect=side_effect)


def test_ensure_raises_when_kv_store_not_enabled():
    """KV Store is the only required product with a status endpoint, so it's a
    pre-flight check BEFORE any resource is created — a missing entitlement
    must cost zero cleanup (no service ever gets made)."""
    fastly_mock = _create_side_effect()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=False),
    ):
        with pytest.raises(EntitlementError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    assert ei.value.code == "kv_store_not_enabled"
    assert ei.value.link == KV_STORE_MANAGE_URL
    # Nothing was created — the wasm service POST is never reached.
    assert not any(c.args[:2] == ("POST", "/service") for c in fastly_mock.call_args_list)


def test_ensure_maps_compute_create_4xx_to_compute_not_enabled():
    """A 4xx on the wasm `POST /service` means Compute isn't enabled. It's the
    first durable resource, so there's nothing to roll back."""
    fastly_mock = _create_side_effect(
        fail_on=("POST", "/service"),
        fail_exc=RuntimeError("HTTP 403 Forbidden"),
    )
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        with pytest.raises(EntitlementError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    assert ei.value.code == "compute_not_enabled"
    assert ei.value.link == COMPUTE_MANAGE_URL
    # Service create failed → no rollback delete attempted.
    assert not any(c.args[:2] == ("DELETE", "/service/SCORING_SVC_ID") for c in fastly_mock.call_args_list)


def test_ensure_maps_config_store_4xx_to_config_store_not_enabled_and_rolls_back():
    """A 4xx on the Config Store create means it isn't enabled for Compute. By
    then the wasm service exists, so it must be rolled back."""
    fastly_mock = _create_side_effect(
        fail_on=("POST", "/resources/stores/config"),
        fail_exc=RuntimeError("HTTP 400 Bad Request"),
    )
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        with pytest.raises(EntitlementError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    assert ei.value.code == "config_store_not_enabled"
    assert ei.value.link == CONFIG_STORE_MANAGE_URL
    # Rollback tore down the just-created wasm service.
    assert any(c.args[:2] == ("DELETE", "/service/SCORING_SVC_ID") for c in fastly_mock.call_args_list)


def test_ensure_rolls_back_everything_on_late_non_entitlement_failure():
    """A failure AFTER the stores are created (here, the first resource-link
    POST) must roll back the service AND all three stores, and re-raise the
    original error unchanged (not mis-mapped to an entitlement error)."""
    fastly_mock = _create_side_effect(
        fail_on=("POST", "/service/SCORING_SVC_ID/version/1/resource"),
        fail_exc=RuntimeError("HTTP 500 Internal Server Error"),
    )
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        with pytest.raises(RuntimeError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    # Original error preserved — NOT converted to an EntitlementError.
    assert not isinstance(ei.value, EntitlementError)
    assert "HTTP 5" in str(ei.value)
    deletes = {c.args[:2] for c in fastly_mock.call_args_list if c.args[0] == "DELETE"}
    assert ("DELETE", "/service/SCORING_SVC_ID") in deletes
    assert ("DELETE", "/resources/stores/config/KEYS_STORE_ID") in deletes
    assert ("DELETE", "/resources/stores/config/CFG_STORE_ID") in deletes
    assert ("DELETE", "/resources/stores/kv/MATRIX_STORE_ID") in deletes


# ── ensure_scoring_service: idempotent reuse path ────────────────────────────


def test_ensure_reuses_existing_service():
    """If the scoring service already exists, ensure_ returns its IDs
    without creating new stores or rotating the AES key."""
    existing = {"id": "EXISTING_SVC_ID", "name": f"Session Scoring Service for {LOG_SVC}"}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service"):
            return [existing]
        if (method, path) == ("GET", "/resources/stores/config"):
            return [
                {"id": "EXISTING_KEYS", "name": "scoring_keys_EXISTING_SVC_ID"},
                {"id": "EXISTING_CFG", "name": "scoring_config_EXISTING_SVC_ID"},
            ]
        # Matrix KV store already exists for this service → no self-heal create.
        if (method, path) == ("GET", "/resources/stores/kv"):
            return [{"id": "EXISTING_MATRIX", "name": "scoring_matrix_EXISTING_SVC_ID"}]
        # request_secret is read back from the store (source of truth) on reuse.
        if (method, path) == ("GET", "/resources/stores/config/EXISTING_KEYS/item/request_secret"):
            return {"item_key": "request_secret", "item_value": "stored_secret_" + "z" * 50}
        # Warm-up anchor already present → preserved, no re-seed (write-once).
        if (method, path) == ("GET", f"/resources/stores/config/EXISTING_CFG/item/{SCORING_ENABLED_AT_KEY}"):
            return {"item_key": SCORING_ENABLED_AT_KEY, "item_value": "1700000000"}
        # We should NOT see any POSTs.
        raise AssertionError(f"unexpected API call: {method} {path}")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == "EXISTING_SVC_ID"
    assert result["scoring_keys_store_id"] == "EXISTING_KEYS"
    assert result["scoring_config_store_id"] == "EXISTING_CFG"
    assert result["scoring_matrix_store_id"] == "EXISTING_MATRIX"
    # request_secret is read back from the store (source of truth) so the
    # caller embeds the store's actual secret into the VCL — no cfg/store drift.
    assert result["request_secret"] == "stored_secret_" + "z" * 50
    # Critical: we did NOT rotate the AES key on a reuse.
    assert result["aes_key_hex"] == ""


def test_ensure_reuse_heals_missing_warmup_anchor():
    """A service provisioned before EC-01 has no scoring_enabled_at anchor.
    Re-enabling it must SEED the anchor (write-once) so the service's advisory
    readiness clock starts from now — not turn L2 on with a step change. Pins that
    the reuse path POSTs the anchor exactly when the GET 404s."""
    existing = {"id": "EXISTING_SVC_ID", "name": f"Session Scoring Service for {LOG_SVC}"}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service"):
            return [existing]
        if (method, path) == ("GET", "/resources/stores/config"):
            return [
                {"id": "EXISTING_KEYS", "name": "scoring_keys_EXISTING_SVC_ID"},
                {"id": "EXISTING_CFG", "name": "scoring_config_EXISTING_SVC_ID"},
            ]
        if (method, path) == ("GET", "/resources/stores/kv"):
            return [{"id": "EXISTING_MATRIX", "name": "scoring_matrix_EXISTING_SVC_ID"}]
        if (method, path) == ("GET", "/resources/stores/config/EXISTING_KEYS/item/request_secret"):
            return {"item_key": "request_secret", "item_value": "s" * 64}
        # Anchor item MISSING on this legacy service → GET 404s, heal POSTs it.
        if (method, path) == ("GET", f"/resources/stores/config/EXISTING_CFG/item/{SCORING_ENABLED_AT_KEY}"):
            raise RuntimeError("404 not found")
        if (method, path) == ("POST", "/resources/stores/config/EXISTING_CFG/item"):
            return {}
        raise AssertionError(f"unexpected API call: {method} {path}")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == "EXISTING_SVC_ID"
    anchor_post = next(
        c for c in fastly_mock.call_args_list if c.args[:2] == ("POST", "/resources/stores/config/EXISTING_CFG/item")
    )
    assert anchor_post.args[2]["item_key"] == SCORING_ENABLED_AT_KEY
    assert anchor_post.args[2]["item_value"].isdigit()


# ── ensure_scoring_service: status callback wiring ───────────────────────────


def test_ensure_emits_status_callbacks_during_creation():
    fastly_mock = _fastly_mock_for_create()
    status_cb = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        ensure_scoring_service(LOG_SVC, TOKEN, status_cb=status_cb)
    # At least the "ensuring", "created service", "domain", "stores", "linked"
    # phases each emit a callback.
    assert status_cb.call_count >= 4
    msgs = " ".join(str(c.args[0]) for c in status_cb.call_args_list)
    assert "scoring service" in msgs.lower()
    assert "domain" in msgs.lower()
    assert "config stores" in msgs.lower()


# ── delete_scoring_service: teardown path ────────────────────────────────────


def test_delete_runs_full_teardown_in_correct_order():
    """Order is: deactivate active versions → delete service → delete each
    config store. Versions must be deactivated first or service-delete 409s;
    service must die before stores or the resource-link blocks store-delete."""
    versions = [{"number": 1, "active": True}, {"number": 2, "active": False}]
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path))
        if (method, path) == ("GET", "/service/SVC_X/version"):
            return versions
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        delete_scoring_service(
            "SVC_X",
            scoring_keys_store_id="KEYS_X",
            scoring_config_store_id="CFG_X",
            scoring_matrix_store_id="MATRIX_X",
            token=TOKEN,
        )

    # Ordering invariant: deactivate happens before DELETE /service/SVC_X,
    # which happens before either store delete.
    deactivate_idx = calls_in_order.index(("PUT", "/service/SVC_X/version/1/deactivate"))
    svc_delete_idx = calls_in_order.index(("DELETE", "/service/SVC_X"))
    keys_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/KEYS_X"))
    cfg_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/CFG_X"))
    assert deactivate_idx < svc_delete_idx < keys_delete_idx
    assert svc_delete_idx < cfg_delete_idx
    # Inactive version (#2) does NOT get a deactivate call.
    assert ("PUT", "/service/SVC_X/version/2/deactivate") not in calls_in_order
    # Matrix KV store torn down too: empty the key first, then the store,
    # both after the service is gone (resource link would otherwise block it).
    kv_key_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X/keys/matrix"))
    kv_store_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X"))
    assert svc_delete_idx < kv_key_idx < kv_store_idx


def test_delete_skips_already_deleted_service_silently():
    """404s on the version-list call short-circuit; the function returns
    normally (idempotent teardown). Stores still get tried."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/MISSING/version"):
            raise RuntimeError("404 not found")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        # Should not raise.
        delete_scoring_service("MISSING", token=TOKEN)


def test_delete_with_empty_service_id_is_noop():
    """Common case: called from teardown when the logging service was never
    scoring-enabled. Must not call Fastly at all."""
    fastly_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        delete_scoring_service("", token=TOKEN)
    fastly_mock.assert_not_called()


def test_delete_tolerates_404_on_store_delete():
    """A store that was already manually deleted shouldn't fail the teardown
    (idempotency contract)."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if path.startswith("/resources/stores/config/MISSING_STORE"):
            raise RuntimeError("404 not found")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        # Should not raise even though one of the store deletes 404s.
        delete_scoring_service(
            "SVC",
            scoring_keys_store_id="MISSING_STORE",
            scoring_config_store_id="ALSO_MISSING",
            token=TOKEN,
        )


def test_delete_surfaces_non_404_store_failure():
    """A non-404 store-delete failure must NOT raise (one stuck store can't block
    the rest of teardown) but MUST be returned + surfaced via status_cb so the
    operator can hand-clean it — otherwise the id is silently dropped from cfg
    and becomes an un-retryable orphan."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if method == "DELETE" and path.startswith("/resources/stores/config/STUCK"):
            raise RuntimeError("500 internal error")  # non-404 → genuine failure
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    msgs = []
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        failed = delete_scoring_service(
            "SVC",
            scoring_keys_store_id="STUCK",
            scoring_config_store_id="FINE",
            token=TOKEN,
            status_cb=msgs.append,
        )

    # The stuck store is reported (label, id); the healthy one is not.
    assert ("scoring_keys", "STUCK") in failed
    assert all(store_id != "FINE" for _, store_id in failed)
    # And a manual-cleanup message reached the stream.
    assert any("STUCK" in m and "manual" in m.lower() for m in msgs)


def test_delete_returns_empty_list_on_clean_teardown():
    """Full-success teardown returns an empty failed-id list."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        failed = delete_scoring_service(
            "SVC",
            scoring_keys_store_id="K",
            scoring_config_store_id="C",
            scoring_matrix_store_id="M",
            token=TOKEN,
        )
    assert failed == []
