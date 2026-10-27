from __future__ import annotations

import json
from functools import partial
from unittest.mock import patch

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from backend import config
from backend.main import app
from tests._helpers.sources import make_source

_make_source = partial(make_source, "svc-admin-test", kind="cloud")

# ── Merged from tests/routers/test_provision_lifecycle_and_teardown.py ──

import io
import os
import urllib.error
import zipfile
from contextlib import ExitStack, contextmanager
from io import BytesIO
from unittest.mock import MagicMock

import botocore.exceptions
from backend.core.faro_versions import DEFAULT_FARO_VERSION

from backend import config as svcconfig
from backend.provision import fos_setup
from backend.provision.fastly_api import _CDN_NO_CACHE_404_SNIPPET_NAME, _CDN_SNIPPETS
from backend.provision.fos_setup import object_storage_enabled, product_enabled
from backend.routers.provision import _check_domain_available
from backend.utils.router_utils import require_json_content_type as _require_json_content_type
from tests._helpers.wire import parse_sse_events as _parse_sse_events

_FAKE_CFG = {
    "service_id": "svc123",
    "name": "Test Service",
    "access_level": "read_write",
    "fos_bucket": "bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
}
_VALID_TOKEN = "test-admin-token"


def _client() -> TestClient:
    return TestClient(app)


def _seed_cfg(sid: str, **overrides) -> dict:
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "name": f"Test Svc {sid}",
        "fos_bucket": f"test-bucket-{sid}",
        "fos_region": "us-east-1",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "SECRET",
        "fastly_api_key": "server-stored-key-MUST-NOT-BE-USED",
        "cdn_secret": "x",
        "provisioning": {
            "fos_key_id": "fk_1",
            "endpoint_name": "EP",
            "cdn_service_id": "cdn_svc_1",
            "cdn_url": "https://cdn.example/",
        },
        **overrides,
    }
    svcconfig.save_config(sid, cfg)
    return cfg


def _fake_perform_teardown(state, token, opts=None):
    yield {"type": "status", "message": "removing logging endpoint (mock)"}
    yield {"type": "status", "message": "removing FOS bucket (mock)"}
    yield {"type": "done", "message": "teardown complete (mock)"}


def _auth_fastly_ok(
    service_id: str = "svc-1",
    customer_id: str = "cust-T",
    *,
    scope="global",
    services=None,
    service_customer_id: str | None = None,
):
    svcs = [] if services is None else services
    svc_cid = service_customer_id or customer_id
    return lambda method, path, *, token, **kw: (
        {"id": "tok", "user_id": "u", "scope": scope, "services": svcs, "customer_id": customer_id}
        if path == "/tokens/self"
        else {"id": path.rsplit("/", 1)[-1] if "/service/" in path else service_id, "customer_id": svc_cid}
    )


@contextmanager
def _teardown_patches(sid: str = "svc-1", *, teardown_fn=_fake_perform_teardown, fastly_fn=None):
    with (
        patch("backend.provision.perform_teardown", side_effect=teardown_fn),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn or _auth_fastly_ok(sid)),
    ):
        yield


def _destructive_payload(sid: str, **overrides) -> dict:
    return {
        "service_id": sid,
        "remove_logging": True,
        "remove_cdn": True,
        "remove_bucket": True,
        "remove_cache": False,
        **overrides,
    }


# ── Merged from tests/routers/test_provision_lifecycle.py ──


def test_teardown_no_service_id_returns_404(isolated_configs_dir):
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json={"service_id": "nope-not-a-real-svc"})
    assert r.status_code == 404


def test_teardown_emits_done_and_removes_config(isolated_configs_dir, tmp_path, monkeypatch):
    sid = "svc-teardown-1"
    _seed_cfg(sid, fos_bucket="td-test-bucket", fastly_api_key="fastly-test-token")
    assert os.path.exists(svcconfig.config_path(sid)), "precondition: config seeded"

    with _teardown_patches(sid), TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json=_destructive_payload(sid, token="test-tok", remove_cron=False),
        )

    assert r.status_code == 200
    events = _parse_sse_events(r.text)
    assert events, f"expected SSE events, got: {r.text[:300]!r}"
    assert "done" in [e.get("type") for e in events]
    assert not os.path.exists(svcconfig.config_path(sid)), "config should have been removed"


def test_teardown_remove_cache_clears_service_metadata(isolated_configs_dir):
    sid = "svc-teardown-meta"
    _seed_cfg(sid, fos_bucket="td-meta-bucket", fastly_api_key="fastly-test-token")
    meta_calls: list[str] = []

    with (
        _teardown_patches(sid),
        patch("backend.core.metadata.teardown", side_effect=lambda s: meta_calls.append(s)),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/teardown",
            json=_destructive_payload(
                sid,
                token="test-tok",
                remove_logging=False,
                remove_cdn=False,
                remove_bucket=False,
                remove_cache=True,
                remove_cron=False,
            ),
        )

    assert r.status_code == 200
    assert meta_calls == [sid], f"metadata.teardown({sid!r}) must be called on remove_cache; got {meta_calls}"


def test_teardown_skips_perform_teardown_when_no_logging_service(isolated_configs_dir):
    sid = "svc-teardown-min"
    svcconfig.save_config(sid, {"service_id": sid, "name": "minimal"})

    with _teardown_patches(sid), TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": "test-tok", "remove_cache": False},
        )
    assert r.status_code == 200
    assert not os.path.exists(svcconfig.config_path(sid))


def test_execute_delegates_to_provision_with_query_args(isolated_configs_dir):
    sid = "svc-create-1"
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "provision complete"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="My Service"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json={
                "token": "test-token",
                "service_id": sid,
                "fos_bucket_name": "create-test-bucket",
                "fos_region": "us-east-1",
                "endpoint_name": "Test Logger",
                "edge_only": True,
                "log_period": "60",
                "enable_cron_sync": True,
                "enable_cron_compact": True,
            },
        )

    assert r.status_code == 200, r.text[:500]
    cfg = captured.get("cfg")
    assert cfg is not None, "provision() was never called"
    assert cfg["logging_service_id"] == sid
    assert cfg["fos_bucket_name"] == "create-test-bucket"
    assert cfg["endpoint_name"] == "Test Logger"
    assert cfg["fos_region"] == "us-east-1"
    assert "done" in [e.get("type") for e in _parse_sse_events(r.text)]


def test_execute_propagates_orchestrator_error_event(isolated_configs_dir):
    def fake_provision(cfg, _resume_from_state=False):
        yield {"type": "status", "message": "starting"}
        yield {"type": "error", "message": "Fastly API: 503 Service Unavailable"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json={"token": "tok", "service_id": "svc-create-err", "fos_bucket_name": "create-err-bucket"},
        )

    events = _parse_sse_events(r.text)
    err = next((e for e in events if e.get("type") == "error"), None)
    assert err is not None and "503" in err["message"]


# ── Merged from tests/routers/test_provision_teardown_auth.py ──


@pytest.mark.security_regression
def test_destructive_teardown_without_token_rejected_401(isolated_configs_dir):
    """No token + destructive flags = 401, no fallback to server creds."""
    sid = "svc-auth-1"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "token_required"


@pytest.mark.security_regression
def test_destructive_teardown_with_read_only_scope_rejected(isolated_configs_dir):
    """``global:read`` does not grant destructive ops — must be rejected."""
    sid = "svc-auth-2"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        assert path == "/tokens/self"
        return {"id": "tok_ro", "user_id": "u", "scope": "global:read", "services": []}

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="ro-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "insufficient_scope"


@pytest.mark.security_regression
def test_destructive_teardown_with_purge_scope_rejected(isolated_configs_dir):
    """purge_select / purge_all also reject — only ``global`` qualifies."""
    sid = "svc-auth-3"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, scope="purge_select purge_all")
    with patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="purge-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "insufficient_scope"


@pytest.mark.security_regression
def test_destructive_teardown_token_bound_to_wrong_service_rejected(isolated_configs_dir):
    """Token has 'global' but services=[other-id] — must reject (service mismatch)."""
    sid = "svc-auth-4"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, services=["different-svc"])
    with patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="bound-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_destructive_teardown_with_global_token_proceeds(isolated_configs_dir):
    """Valid 'global' token + matching/empty services → proceeds (200 SSE)."""
    sid = "svc-auth-5"
    _seed_cfg(sid)
    with _teardown_patches(sid, fastly_fn=_auth_fastly_ok(sid, "cust-A")), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="valid-global-token"))
    assert r.status_code == 200, r.text[:500]


@pytest.mark.security_regression
def test_destructive_teardown_service_in_bound_list_proceeds(isolated_configs_dir):
    """Token bound to [sid, other] — matches target → proceeds."""
    sid = "svc-auth-6"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, "cust-B", services=[sid, "other-svc"])
    with _teardown_patches(sid, fastly_fn=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="bound-valid"))
    assert r.status_code == 200


@pytest.mark.security_regression
def test_cache_only_teardown_requires_auth_gate(isolated_configs_dir):
    """Cache-only cleanup still requires a token to prevent unauthenticated local state destruction."""
    sid = "svc-auth-7"
    _seed_cfg(sid)
    payload = _destructive_payload(
        sid, remove_logging=False, remove_cdn=False, remove_bucket=False, remove_cache=True, remove_cron=False
    )
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=payload)
    assert r.status_code == 401
    assert "token_required" in r.json()["detail"]["error"]


@pytest.mark.security_regression
def test_destructive_teardown_fastly_unreachable_rejects(isolated_configs_dir):
    """If Fastly's /tokens/self errors out, fail closed rather than falling back to server creds."""
    sid = "svc-auth-8"
    _seed_cfg(sid)
    with (
        patch("backend.utils.fastly_auth.fastly", side_effect=RuntimeError("Network error on GET /tokens/self")),
        TestClient(app) as client,
    ):
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="any"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "token_validation_failed"


@pytest.mark.security_regression
def test_destructive_teardown_scope_as_list_accepted(isolated_configs_dir):
    """Some Fastly token responses return scope as a JSON list; validator must normalize both forms."""
    sid = "svc-auth-9"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, "cust-L", scope=["global", "purge_all"])
    with _teardown_patches(sid, fastly_fn=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="list-scope"))
    assert r.status_code == 200


@pytest.mark.security_regression
def test_destructive_teardown_tenant_mismatch_rejected(isolated_configs_dir):
    """Token with 'global' scope under a different customer_id from target service is rejected."""
    sid = "svc-auth-tenant-mismatch"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, "cust-ATTACKER", service_customer_id="cust-VICTIM")
    with patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="cross-tenant-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "tenant_mismatch"


@pytest.mark.security_regression
def test_destructive_teardown_get_method_rejected(isolated_configs_dir):
    """CSRF defense: teardown must not be triggerable via GET."""
    sid = "svc-csrf-rejected"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.get(
            "/api/provision/teardown",
            params={"service_id": sid, "token": "anything", "remove_logging": "true"},
        )
    assert r.status_code in (404, 405), f"GET must be rejected; got {r.status_code}: {r.text[:300]}"


@pytest.mark.security_regression
def test_destructive_teardown_text_plain_content_type_rejected(isolated_configs_dir):
    """Regression for audit finding 012: text/plain POST must be rejected with 415."""
    sid = "svc-csrf-text-plain"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            content='{"service_id":"' + sid + '","remove_logging":true}',
            headers={"Content-Type": "text/plain"},
        )
    assert r.status_code == 415


# ── Merged from tests/routers/test_provision_teardown_idempotency.py ──


def test_teardown_is_idempotent_second_call_is_404(isolated_configs_dir):
    """Two back-to-back teardown calls: first 200 + cleans up, second 404."""
    sid = "svc-idem-1"
    _seed_cfg(sid)

    with _teardown_patches(sid, fastly_fn=_auth_fastly_ok(sid, "cust-IDEM")), TestClient(app) as client:
        r1 = client.post(
            "/api/provision/teardown",
            json=_destructive_payload(sid, token=_VALID_TOKEN, remove_cron=False),
        )
        assert r1.status_code == 200
        assert any(e.get("type") == "done" for e in _parse_sse_events(r1.text))
        assert not os.path.exists(svcconfig.config_path(sid))

        r2 = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )

    assert r2.status_code == 404
    assert "detail" in r2.json()


def test_teardown_unknown_service_id_never_provisioned_is_404(isolated_configs_dir):
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json={"service_id": "never-existed-svc", "remove_cache": False})
    assert r.status_code == 404


def test_teardown_preserves_duckdb_when_remove_cache_false(isolated_configs_dir, tmp_path):
    """``remove_cache=False`` MUST leave the per-service DuckDB file intact."""
    sid = "svc-idem-keep-cache"
    _seed_cfg(sid)

    db_path = svcconfig.duckdb_path(sid)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, "w") as f:
        f.write("placeholder-duckdb-bytes")
    assert os.path.exists(db_path)

    with _teardown_patches(sid, fastly_fn=_auth_fastly_ok(sid, "cust-IDEM")), TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False, "remove_cron": False},
        )

    assert r.status_code == 200
    assert not os.path.exists(svcconfig.config_path(sid))
    assert os.path.exists(db_path)


def test_teardown_partial_failure_in_perform_teardown_still_succeeds(isolated_configs_dir):
    """If ``perform_teardown`` raises mid-stream, SSE emits error, config is removed, and re-run is 404."""
    sid = "svc-idem-partial-fail"
    _seed_cfg(sid)

    def _boom_teardown(state, token, opts=None):
        yield {"type": "status", "message": "removing logging endpoint (mock)"}
        raise RuntimeError("Fastly API: 503 Service Unavailable (mock)")

    with (
        _teardown_patches(sid, teardown_fn=_boom_teardown, fastly_fn=_auth_fastly_ok(sid, "cust-IDEM")),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )

    assert r.status_code == 200
    assert "error" in [e.get("type") for e in _parse_sse_events(r.text)]
    assert not os.path.exists(svcconfig.config_path(sid))

    with TestClient(app) as client:
        r2 = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )
    assert r2.status_code == 404


# ── Merged from tests/routers/test_provision_wizard_e2e.py ──


def _fake_ensure_fos_access_key(
    description, state, token, *, permission="read-write-objects", buckets=None, status_cb=None
):
    if status_cb:
        status_cb(f"⏳ Creating {permission} access key (mock)…")
    suffix = "temp" if "temp-admin" in description else "perm"
    return {"id": f"AKIA_TEST_{suffix}", "access_key": f"AKIA_TEST_{suffix}", "secret_key": f"SECRET_TEST_{suffix}"}


def _fake_ensure_fos_bucket(bucket, region, access_key, secret_key, *, service_id=None, status_cb=None):
    if status_cb:
        status_cb(f"🪣 Bucket {bucket} in {region} (mock)…")
    return True


def _fake_ensure_cdn_service(cfg, fos_access_key, fos_secret_key, token, status_cb=None, on_created=None):
    if status_cb:
        status_cb("🌐 CDN service (mock)…")
    if on_created:
        on_created("cdn-svc-mock-123")
    return {"id": "cdn-svc-mock-123", "name": cfg.get("cdn_service_name") or "Mock CDN Service", "version": 1}


def _fake_ensure_logging_via_reconciler(state, token, status_cb=None):
    if status_cb:
        status_cb("📡 Logging endpoint (mock)…")
    return 42


def _fake_delete_fos_access_key(key_id, token, status_cb=None):
    if status_cb:
        status_cb(f"🧹 Deleted {key_id} (mock)…")


@contextmanager
def _wizard_env(tmp_path, monkeypatch, *, service_name="Wizard E2E Service", extra_patches=()):
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    base_patches = [
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=_fake_ensure_fos_access_key),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=_fake_ensure_fos_bucket),
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_fake_ensure_cdn_service),
        patch(
            "backend.provision.orchestrator.ensure_logging_via_reconciler",
            side_effect=_fake_ensure_logging_via_reconciler,
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_fake_delete_fos_access_key),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.config.fetch_service_name", return_value=service_name),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ]
    with ExitStack() as stack:
        for p in [*base_patches, *extra_patches]:
            stack.enter_context(p)
        yield


def _wizard_payload(sid: str, bucket: str, **overrides) -> dict:
    return {
        "token": "fake-fastly-token",
        "service_id": sid,
        "service_name": "Wizard E2E Service",
        "endpoint_name": "Wizard E2E Logger",
        "fos_region": "us-east-1",
        "fos_bucket_name": bucket,
        "fos_prefix": "logs/",
        "sample_rate": "100",
        "edge_only": True,
        "log_period": "60",
        "enable_cron_sync": True,
        "delete_after": True,
        "commit_interval_mins": 5,
        "enable_cron_compact": True,
        "log_retention_days": 30,
        **overrides,
    }


def test_wizard_execute_runs_orchestrator_and_bootstrap_sees_service(isolated_configs_dir, tmp_path, monkeypatch):
    """End-to-end: execute → orchestrator yields done → config persists → bootstrap lists svc."""
    sid = "svc-wizard-e2e-1"
    extras = [
        patch("backend.core.storage.init_storage_table", side_effect=RuntimeError("storage init skipped (test)")),
    ]
    with _wizard_env(tmp_path, monkeypatch, extra_patches=extras), TestClient(app) as client:
        r = client.post("/api/provision/execute", json=_wizard_payload(sid, "wizard-e2e-bucket"))

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types and "error" not in types

    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    for n in range(1, 9):
        assert f"Step {n}/8" in status_msgs

    cfg_path = svcconfig.config_path(sid)
    assert os.path.exists(cfg_path)
    written = json.loads(cfg_path.read_text())
    assert written["service_id"] == sid
    assert written["fos_bucket"] == "wizard-e2e-bucket"
    assert written["fos_region"] == "us-east-1"
    assert written["fos_access_key_id"] == "AKIA_TEST_perm"
    assert written["fos_secret_access_key"] == "SECRET_TEST_perm"
    assert written["fastly_api_key"] == "fake-fastly-token"
    assert written["cdn_service_id"] == "cdn-svc-mock-123"
    assert isinstance(written["provisioning"]["endpoint_name"], str) and written["provisioning"]["endpoint_name"]
    assert isinstance(written["cdn_secret"], str) and written["cdn_secret"]

    with TestClient(app) as client:
        boot = client.get("/api/bootstrap")
    assert boot.status_code == 200
    assert sid in [s.get("service_id") for s in boot.json()["services"]]


def test_wizard_execute_e2e_storage_init_success(isolated_configs_dir, tmp_path, monkeypatch, s3_mock):
    """R-15: positive-path variant exercising real `init_storage_table` against moto S3."""
    sid = "svc-wizard-storage-success"
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", tmp_path / "data" / "cache")
    extras = []

    with (
        _wizard_env(tmp_path, monkeypatch, service_name="Wizard Storage Success", extra_patches=extras),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json=_wizard_payload(
                sid,
                "test-bucket",
                service_name="Wizard Storage Success",
                endpoint_name="Wizard Storage Logger",
            ),
        )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types and "error" not in types

    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    for n in range(1, 9):
        assert f"Step {n}/8" in status_msgs
    assert svcconfig.config_path(sid).exists()

    with TestClient(app) as client:
        boot = client.get("/api/bootstrap")
    assert boot.status_code == 200
    assert sid in [s.get("service_id") for s in boot.json()["services"]]


def test_wizard_execute_rolls_back_on_helper_failure(isolated_configs_dir, tmp_path, monkeypatch):
    """Mid-pipeline helper failure emits error event, runs rollback, and removes config."""
    sid = "svc-wizard-e2e-fail"

    def _boom_cdn(cfg, fos_access_key, fos_secret_key, token, status_cb=None):
        raise RuntimeError("Fastly API: 500 Internal Server Error (mock)")

    extras = [
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_boom_cdn),
        patch("backend.provision.orchestrator.remove_logging_endpoint"),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch("backend.provision.orchestrator.delete_fos_bucket"),
        patch("backend.core.fastly.client.fastly", return_value={"data": []}),
    ]
    with (
        _wizard_env(tmp_path, monkeypatch, service_name="Wizard E2E Service Fail", extra_patches=extras),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json=_wizard_payload(
                sid,
                "wizard-e2e-fail-bucket",
                service_name="Wizard E2E Service Fail",
                endpoint_name="Wizard E2E Logger Fail",
            ),
        )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    assert "error" in [e.get("type") for e in events]

    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    assert "Step 3/8" in status_msgs and "Step 8/8" not in status_msgs
    assert not os.path.exists(svcconfig.config_path(sid))


def test_wizard_execute_rollback_actually_invokes_resource_deletion(isolated_configs_dir, tmp_path, monkeypatch):
    """Mid-pipeline failure at step 7 must invoke resource deletion helpers for steps 1-6."""
    sid = "svc-wizard-teardown-trace"
    teardown_calls: list[tuple[str, dict]] = []

    def _track(name):
        def _capture(*args, **kwargs):
            teardown_calls.append((name, {"args": args, "kwargs": kwargs}))
            return None

        return _capture

    def _boom_logging_endpoint(state, token, status_cb=None):
        raise RuntimeError("Fastly API: 502 Bad Gateway on logging endpoint (mock)")

    extras = [
        patch("backend.provision.orchestrator.ensure_logging_via_reconciler", side_effect=_boom_logging_endpoint),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_track("delete_fos_access_key")),
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=_track("delete_fos_bucket")),
        patch("backend.provision.orchestrator.delete_cdn_service", side_effect=_track("delete_cdn_service")),
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=_track("remove_logging_endpoint")),
        patch("backend.core.fastly.client.fastly", return_value={"data": []}),
    ]
    with (
        _wizard_env(tmp_path, monkeypatch, service_name="Wizard Teardown Trace", extra_patches=extras),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json=_wizard_payload(
                sid,
                "wizard-teardown-bucket",
                service_name="Wizard Teardown Trace",
                endpoint_name="Wizard Teardown Logger",
                cdn_service_name="Wizard Teardown CDN",
            ),
        )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    assert "error" in [e.get("type") for e in events]
    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    assert "Step 6/8" in status_msgs

    called_names = [name for name, _ in teardown_calls]
    for expected in ("delete_cdn_service", "remove_logging_endpoint", "delete_fos_bucket", "delete_fos_access_key"):
        assert expected in called_names, f"expected {expected} in {called_names}"
    assert not os.path.exists(svcconfig.config_path(sid))


# ── Merged from tests/routers/test_provision.py ──


# ─── /api/provision/services/{id}/ngwaf-workspace & /ngwaf-workspaces ────────


def test_set_ngwaf_workspace_saves_body_and_ignores_query_param():
    saved: dict = {}
    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="test-stored-key")

    def fake_mutate(sid, fn, **kwargs):
        cfg = dict(cfg_with_key)
        fn(cfg)
        saved.update(cfg)
        return cfg

    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.mutate_config", side_effect=fake_mutate),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = _client().patch(
            "/api/provision/services/svc123/ngwaf-workspace?token=test-stored-key",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
        assert resp.status_code == 200
        mock_validate.assert_called_once_with("test-stored-key", service_id="svc123")
        assert resp.json()["ngwaf_workspace_id"] == "workspace-abc"
        assert saved.get("ngwaf_workspace_id") == "workspace-abc"

        saved.clear()
        resp2 = _client().patch(
            "/api/provision/services/svc123/ngwaf-workspace?workspace_id=workspace-abc&token=test-stored-key",
            json={},
        )
        assert resp2.status_code == 200
        assert saved.get("ngwaf_workspace_id") is None


@pytest.mark.parametrize(
    ("query", "cfg_key", "fastly_effect", "expected_error"),
    [
        ("", None, None, "token_required"),
        ("?token=attacker-supplied-wrong-token", "legit-stored-key", RuntimeError("HTTP 401"), None),
        (
            "?token=stored-read-only-token",
            "stored-read-only-token",
            {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"},
            "insufficient_scope",
        ),
    ],
)
def test_set_ngwaf_workspace_auth_rejections(query, cfg_key, fastly_effect, expected_error):
    cfg = dict(_FAKE_CFG, **({"fastly_api_key": cfg_key} if cfg_key else {}))
    fastly_kw = (
        {"side_effect": fastly_effect} if isinstance(fastly_effect, Exception) else {"return_value": fastly_effect}
    )
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", **fastly_kw),
    ):
        resp = _client().patch(
            f"/api/provision/services/svc123/ngwaf-workspace{query}",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
    assert resp.status_code == 401
    if expected_error:
        assert resp.json()["detail"]["error"] == expected_error


def test_ngwaf_workspaces_listing_variants():
    cfg_ro = dict(_FAKE_CFG, fastly_api_key="stored-read-only-token")
    ro_data = {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"}
    with (
        patch("backend.config.load_config", return_value=cfg_ro),
        patch("backend.utils.fastly_auth.fastly", return_value=ro_data),
    ):
        r_ro = _client().get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc123", "token": "stored-read-only-token"},
        )
        assert r_ro.status_code == 401
        assert r_ro.json()["detail"]["error"] == "insufficient_scope"

    with patch("backend.config.get_fastly_api_key", return_value=""):
        r_no_tok = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc"})
        assert r_no_tok.status_code == 401
        assert r_no_tok.json()["detail"]["error"] == "token_required"

    def _mock_resp(payload: bytes):
        m = MagicMock()
        m.read.return_value = payload
        m.status = 200
        m.__enter__ = lambda s: s
        m.__exit__ = MagicMock(return_value=False)
        return m

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch(
            "urllib.request.urlopen",
            return_value=_mock_resp(b'{"data": [{"id": "ws-1", "name": "Prod"}, {"id": "ws-2", "name": "Staging"}]}'),
        ),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_val,
    ):
        r_data = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_data.status_code == 200
        assert r_data.json()["workspaces"][0] == {"id": "ws-1", "name": "Prod"}
        mock_val.assert_called_once_with("tok", service_id="svc")

        r_hdr = _client().get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc"},
            headers={"Authorization": "Bearer tok"},
        )
        assert r_hdr.status_code == 200
        assert r_hdr.json()["workspaces"][0] == {"id": "ws-1", "name": "Prod"}

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch(
            "urllib.request.urlopen",
            return_value=_mock_resp(b'{"workspaces": [{"id": "ws-old", "attributes": {"name": "OldShape"}}]}'),
        ),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_old = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_old.json()["workspaces"][0]["name"] == "OldShape"

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=_mock_resp(b'{"data": []}')),
        patch("backend.provision.fastly", return_value={"id": "tok-id", "name": "automation"}),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_empty = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_empty.json()["workspaces"] == []
        assert "automation" in r_empty.json()["error_hint"].lower()

    err_401 = urllib.error.HTTPError(
        url="https://api.fastly.com/ngwaf/v1/workspaces",
        code=401,
        msg="unauthorized",
        hdrs={},  # type: ignore[arg-type]
        fp=BytesIO(b"unauthorized"),
    )
    with (
        patch("backend.config.get_fastly_api_key", return_value="bad-tok"),
        patch("urllib.request.urlopen", side_effect=err_401),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_401 = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "bad-tok"})
        assert r_401.status_code == 400
        assert r_401.json()["detail"]["error"] == "ngwaf_token_invalid"


# ─── /api/provision/services & /validate ─────────────────────────────────────


def test_provision_list_services_flows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config("svc-existing", {"service_id": "svc-existing"})

    fake_services = [
        {"id": "svc-existing", "name": "Already Done", "type": "vcl"},
        {"id": "svc-new", "name": "Fresh", "type": "vcl"},
        {"id": "svc-wasm", "name": "Compute", "type": "wasm"},
    ]
    with (
        patch("backend.core.fastly.client.fastly", return_value=fake_services),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=True),
    ):
        resp = _client().get("/api/provision/services?token=tok")
        assert resp.status_code == 200
        assert {s["id"]: s["provisioned"] for s in resp.json()} == {"svc-existing": True, "svc-new": False}

    assert _client().get("/api/provision/services").status_code == 422

    with (
        patch("backend.core.fastly.client.fastly", return_value=fake_services[:1]),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=False),
        patch("backend.config.list_service_ids", return_value=[]),
    ):
        r_no_os = _client().get("/api/provision/services", params={"token": "tok"})
        assert r_no_os.status_code == 400
        assert r_no_os.json()["detail"]["error"] == "object_storage_not_enabled"

    probe_called: list[bool] = []
    with (
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 401 GET /service")),
        patch(
            "backend.provision.fos_setup.object_storage_enabled",
            side_effect=lambda *a, **k: probe_called.append(True) or False,
        ),
    ):
        r_bad = _client().get("/api/provision/services", params={"token": "bad"})
        assert r_bad.status_code == 400
        assert r_bad.json()["detail"]["error"] == "list_services_failed"
        assert probe_called == []


def test_provision_validate_flows():
    for bad_body in (
        {"service_id": "svc"},
        {"token": "tok"},
        {"token": "", "service_id": "svc"},
        {"token": "tok", "service_id": ""},
    ):
        assert _client().post("/api/provision/validate", json=bad_body).status_code == 400

    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda m, p, **kw: (
            {"id": "tok-1", "name": "Test Token", "user_id": "u1"}
            if "/tokens/self" in p
            else {"name": "My Production CDN"}
        ),
    ):
        resp = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc.with/specials!"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["service_name"] == "My Production CDN"
        assert not any(ch in body["defaults"]["fos_bucket_name"] for ch in ("/", ".", "!"))
        assert body["token_info"]["type"] == "user"

    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda m, p, **kw: {"id": "tok-bot", "name": "Bot Token"} if "/tokens/self" in p else {"name": "X"},
    ):
        resp_bot = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})
        assert resp_bot.json()["token_info"]["type"] == "automation"

    def _fail_tok(m, p, *a, **kw):
        if p == "/tokens/self":
            raise RuntimeError("token endpoint down")
        return {"name": "My Service"}

    with patch("backend.core.fastly.client.fastly", side_effect=_fail_tok):
        resp_swallow = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc-abc"})
        assert resp_swallow.status_code == 200
        assert resp_swallow.json()["token_info"] == {}


# ─── /api/provision/check-domain & _check_domain_available ───────────────────


@pytest.mark.parametrize(
    ("query", "mock_ret", "expected_avail", "expected_call_arg"),
    [
        ("prefix=-bad", None, False, None),
        ("prefix=foo_bar", None, False, None),
        ("prefix=valid-prefix-1", (True, None), True, "valid-prefix-1.global.ssl.fastly.net"),
        ("prefix=my.custom.domain.com&is_custom=true", (True, None), True, "my.custom.domain.com"),
        (
            "prefix=my.custom.domain.com&is_custom=true",
            (False, "Domain already registered or in use"),
            False,
            "my.custom.domain.com",
        ),
    ],
)
def test_check_domain_routes(query, mock_ret, expected_avail, expected_call_arg):
    assert _client().get("/api/provision/check-domain?prefix=").status_code in (400, 422)
    with patch("backend.routers.provision._check_domain_available", return_value=mock_ret) as mock_check:
        resp = _client().get(f"/api/provision/check-domain?{query}")
    assert resp.status_code == 200
    assert resp.json()["available"] is expected_avail
    if expected_call_arg:
        mock_check.assert_called_once_with(expected_call_arg)


def test_check_domain_available_helper_branches():
    fake_resp = MagicMock()
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=fake_resp), patch("backend.utils.telemetry.tracked_call"):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
        assert ok is False and "in use" in (reason or "").lower()

    def _http_404(body: bytes):
        return urllib.error.HTTPError(url="https://x", code=404, msg="not found", hdrs={}, fp=BytesIO(body))  # type: ignore[arg-type]

    with (
        patch(
            "urllib.request.urlopen",
            side_effect=_http_404(b"... Please check that this domain has been added to a service. ..."),
        ),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        assert _check_domain_available("test.global.ssl.fastly.net") == (True, None)

    with (
        patch("urllib.request.urlopen", side_effect=_http_404(b"some other body")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_o, reason_o = _check_domain_available("test.global.ssl.fastly.net")
        assert ok_o is False and "already registered" in (reason_o or "")

    with (
        patch("urllib.request.urlopen", side_effect=urllib.error.URLError("dns failed")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_dns, reason_dns = _check_domain_available("test.global.ssl.fastly.net")
        assert ok_dns is True and "DNS/Connection" in (reason_dns or "")

    with (
        patch("urllib.request.urlopen", side_effect=RuntimeError("unexpected")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_ex, reason_ex = _check_domain_available("foo.example")
        assert ok_ex is False and "unexpected" in (reason_ex or "")


# ─── /api/provision/check-fos & /terraform/* ─────────────────────────────────


def test_check_fos_success_and_sample_get():
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = [{"Contents": []}, {"Contents": []}]
    fos_req = {"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client) as get_client:
        resp = _client().post("/api/provision/check-fos", json=fos_req)
    assert resp.status_code == 200 and resp.json()["ok"] is True
    get_client.assert_called_once_with("ak", "sk", "us-east-1", bucket_name="b", context="provision:check-fos")
    assert [c.kwargs["Prefix"] for c in fake_client.list_objects_v2.call_args_list] == ["raw/request/", "raw/rum/"]

    fake_client2 = MagicMock()
    sample_key = "raw/request/year=2026/month=09/day=22/hour=13/minute=00/object.gz"
    fake_client2.list_objects_v2.return_value = {"Contents": [{"Key": sample_key}]}
    fake_client2.get_object.return_value = {"Body": MagicMock()}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client2):
        resp2 = _client().post("/api/provision/check-fos", json=fos_req)
    assert resp2.status_code == 200 and resp2.json()["ok"] is True
    fake_client2.get_object.assert_called_once_with(Bucket="b", Key=sample_key, Range="bytes=0-0")


def _fos_client_err(code: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError({"Error": {"Code": code, "Message": "raw"}}, "ListObjectsV2")


@pytest.mark.parametrize(
    ("exc", "expected_in_msg"),
    [
        (_fos_client_err("AccessDenied"), "Access Denied"),
        (_fos_client_err("InvalidAccessKeyId"), "Access Denied"),
        (_fos_client_err("SignatureDoesNotMatch"), "Access Denied"),
        (_fos_client_err("NoSuchBucket"), "Bucket not found"),
        (_fos_client_err("IllegalLocationConstraintException"), "Region mismatch"),
        (RuntimeError("EndpointConnectionError: Could not connect to the endpoint URL"), "verify the FOS Region"),
        (RuntimeError("weird unknown error xyz"), "weird unknown error xyz"),
    ],
)
def test_check_fos_error_mappings(exc, expected_in_msg):
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = exc
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "k", "secret_key": "s"},
        )
    body = resp.json()
    assert body["ok"] is False and expected_in_msg in body["error"]


def test_terraform_preview_and_export():
    fake_files = {"main.tf": 'resource "fastly_service_vcl" "x" {}\n', "instructions": "# Setup steps\n"}
    with patch("backend.utils.terraform_gen.generate_terraform", return_value=fake_files):
        prev = _client().post(
            "/api/provision/terraform/preview",
            json={
                "service_id": "svc-1",
                "fos_bucket": "b",
                "cdn_service_name": "CDN",
                "cdn_url": "https://x.global.ssl.fastly.net",
                "fos_region": "us-east-1",
            },
        )
        assert prev.status_code == 200 and "main.tf" in prev.json()

        exp = _client().post("/api/provision/terraform/export", json={"service_id": "svc"})
        assert exp.status_code == 200
        assert exp.headers["content-type"] == "application/zip"
        assert "fastly-log-analysis-terraform.zip" in exp.headers["content-disposition"]
        names = zipfile.ZipFile(io.BytesIO(exp.content)).namelist()
        assert "README.md" in names and "main.tf" in names and "instructions" not in names


# ─── /api/provision/ingest ───────────────────────────────────────────────────


def test_ingest_validation_and_key_flows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    assert _client().post("/api/provision/ingest", json={"service_id": "svc"}).status_code == 400

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=ValueError("unknown period: fortnight")),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc")),
    ):
        r_period = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc", "log_period": "1 fortnight"},
        )
        assert r_period.status_code == 400
        assert r_period.json()["detail"]["error"] == "invalid_log_period"

    new_key = {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "kid"}
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", return_value=new_key) as mock_ensure,
        patch("backend.provision.write_service_config") as mock_write,
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_new = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b", "fos_region": "us-east-1"},
        )
        assert r_new.status_code == 200
        mock_ensure.assert_called_once()
        persisted = mock_write.call_args[0][0]
        assert persisted["fos_access_key_id"] == "AKNEW" and persisted["fos_secret_access_key"] == "SKNEW"

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", side_effect=RuntimeError("rate limited")),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_err = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b"},
        )
        assert r_err.status_code == 400
        assert r_err.json()["detail"]["error"] == "ensure_access_key_failed"

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key") as mock_find,
        patch("backend.provision.ensure_fos_access_key") as mock_ensure2,
        patch("backend.provision.write_service_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_prov = _client().post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "fos_access_key": "EXISTING_AK",
                "fos_secret_key": "EXISTING_SK",
            },
        )
        assert r_prov.status_code == 200
        mock_find.assert_not_called()
        mock_ensure2.assert_not_called()


def test_ingest_rerun_preserves_faro_pin_and_cdn_secret(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        "svc-1",
        {
            "service_id": "svc-1",
            "cdn_secret": "persisted-cdn-secret",
            "rum_enabled": True,
            "rum": {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00Z",
                "faro_version": "9.9.9",
                "faro_content_hash": "existing-hash",
                "faro_fos_etag_md5": "existing-etag",
            },
        },
    )
    captured: dict = {}
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch(
            "backend.provision.ensure_fos_access_key",
            return_value={"access_key": "AK", "secret_key": "SK", "id": "kid"},
        ),
        patch("backend.provision.write_service_config", side_effect=lambda st: captured.update(state=st)),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        base_req = {"token": "t", "service_id": "svc-1", "fos_bucket_name": "b", "rum_enabled": True}
        resp = _client().post("/api/provision/ingest", json=base_req)
        assert resp.status_code == 200
        assert captured["state"]["cdn_secret"] == "persisted-cdn-secret"
        assert captured["state"]["rum"]["faro_version"] == "9.9.9"
        assert captured["state"]["rum"]["faro_content_hash"] == "existing-hash"

        resp_override = _client().post("/api/provision/ingest", json={**base_req, "rum": {"faro_version": "1.0.0"}})
        assert resp_override.status_code == 200
        assert captured["state"]["rum"]["faro_version"] == "1.0.0"


# ─── /api/provision/execute ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("payload_extra", "parse_period_effect", "domain_avail", "expected_err_sub"),
    [
        ({"fos_bucket_name": "invalid_underscore_name"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "bad--bucket"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "-leading-hyphen"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "valid-bucket", "fos_prefix": "x'; DROP TABLE--"}, 60, (True, None), "prefix"),
        (
            {"fos_bucket_name": "valid-bucket", "log_period": "fortnight"},
            ValueError("unknown: fortnight"),
            (True, None),
            "invalid_log_period",
        ),
        (
            {"fos_bucket_name": "valid-bucket", "cdn_url": "https://taken.global.ssl.fastly.net"},
            60,
            (False, "Domain already registered or in use"),
            "unavailable",
        ),
    ],
)
def test_provision_execute_validation_rejections(payload_extra, parse_period_effect, domain_avail, expected_err_sub):
    pp_kw = (
        {"side_effect": parse_period_effect}
        if isinstance(parse_period_effect, Exception)
        else {"return_value": parse_period_effect}
    )
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", **pp_kw),
        patch("backend.routers.provision._check_domain_available", return_value=domain_avail),
    ):
        r = _client().post(
            "/api/provision/execute",
            json={"token": "tok", "service_id": "svc-1", **payload_extra},
        )
    assert r.status_code == 400
    assert expected_err_sub in r.json()["detail"]["error"].lower()


def test_provision_execute_dns_fallback_and_faro_version_threading():
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "done", "message": "ok"}

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(False, "DNS resolution failed transiently"),
        ),
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.core.metadata.record_audit"),
    ):
        base_req = {"token": "tok", "service_id": "svc-1", "fos_bucket_name": "valid-bucket", "rum_enabled": True}
        r1 = _client().post(
            "/api/provision/execute",
            json={**base_req, "cdn_url": "https://x.global.ssl.fastly.net", "faro_version": "1.2.3"},
        )
        assert r1.status_code == 200
        assert captured["cfg"]["rum"]["faro_version"] == "1.2.3"

        r2 = _client().post("/api/provision/execute", json=base_req)
        assert r2.status_code == 200
        assert captured["cfg"]["rum"]["faro_version"] == DEFAULT_FARO_VERSION


# ─── /api/provision/check-config ─────────────────────────────────────────────

_CHECK_PARAMS = {"token": "t", "service_id": "log-svc", "cdn_service_id": "cdn-svc", "bucket": "b"}
_REQUIRED_SNIPPETS = [
    "Fastly Log Analysis Capture",
    "Fastly Log Analysis Miss",
    "Fastly Log Analysis Pass",
]
_OPTIONAL_SNIPPETS = [
    "Fastly Log Analysis Origin Fetch",
    "Fastly Log Analysis Origin Error",
    "Fastly Log Analysis Origin Deliver",
]
_REQUIRED_CDN_SNIPPETS = [
    "cdn-swr-shield-disable",
    "cdn-race-condition-generation",
    "cdn-no-cache-404",
]


def _check_config_fastly_router(*, endpoints, snippets, dicts, backends, cdn_snippets):
    def fake_fastly(method, path, token=None, **_):
        if path == "/service/log-svc/version/active":
            return {"number": 7}
        if "/version/7/logging/s3" in path:
            return endpoints
        if "/version/7/snippet" in path:
            return snippets
        if "/version/active/dictionary" in path:
            return dicts
        if "/version/active/backend" in path:
            return backends
        if path == "/service/cdn-svc/version/active":
            return {"number": 3}
        if "/version/3/snippet" in path:
            return cdn_snippets
        raise AssertionError(f"unexpected fastly path: {path}")

    return fake_fastly


def test_check_config_logging_and_cdn_permutations():
    by_name = {entry[0]: entry for entry in _CDN_SNIPPETS}
    assert _CDN_NO_CACHE_404_SNIPPET_NAME in by_name
    _name, snippet_type, content, _priority = by_name[_CDN_NO_CACHE_404_SNIPPET_NAME]
    assert snippet_type == "fetch"
    assert 'req.method == "GET"' in content and 'req.method == "HEAD"' in content
    assert "beresp.status == 404" in content and "set beresp.cacheable = false;" in content

    full_dicts = [{"name": "fos_credentials"}, {"name": "cdn_auth"}]
    full_backends = [{"address": "x.fastlystorage.app"}]
    full_cdn_snips = [{"name": s} for s in _REQUIRED_CDN_SNIPPETS]

    # 1. No matching logging endpoint
    side_no_ep = _check_config_fastly_router(
        endpoints=[{"name": "other", "bucket_name": "different-bucket"}],
        snippets=[],
        dicts=full_dicts,
        backends=full_backends,
        cdn_snippets=full_cdn_snips,
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_no_ep):
        body = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert body["logging_service"]["ok"] is False
        assert "No S3 logging endpoint" in body["logging_service"]["details"]

    # 2. Missing core logging snippets + missing CDN perf snippets
    side_missing_snip = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": "Fastly Log Analysis Capture"}],
        dicts=full_dicts,
        backends=full_backends,
        cdn_snippets=[{"name": "cdn-swr-shield-disable"}],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_missing_snip):
        res = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert "missing CORE snippets" in res["logging_service"]["details"]
        assert res["cdn_service"]["ok"] is False
        assert "cdn-no-cache-404" in res["cdn_service"]["details"]

    # 3. Full logging + optional origin snippets + full CDN
    side_full = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS + _OPTIONAL_SNIPPETS],
        dicts=full_dicts,
        backends=full_backends,
        cdn_snippets=full_cdn_snips,
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_full):
        res_full = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert res_full["logging_service"]["ok"] is True
        assert "3 origin metric snippets" in res_full["logging_service"]["details"]
        assert res_full["cdn_service"]["ok"] is True
        assert "fully configured" in res_full["cdn_service"]["details"]

    # 4. No response condition warns + CDN missing dicts/backend
    side_warn = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": None}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[],
        backends=[],
        cdn_snippets=[],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_warn):
        res_warn = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert res_warn["logging_service"]["ok"] is True
        assert "no response condition" in res_warn["logging_service"]["details"].lower()
        assert res_warn["cdn_service"]["ok"] is False
        assert "fos_credentials" in res_warn["cdn_service"]["details"]

    # 5. Logging & CDN exceptions captured in details
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly 503")):
        res_err = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert "Fastly 503" in res_err["logging_service"]["details"]
        assert "Fastly 503" in res_err["cdn_service"]["details"]


# ─── /api/provision/teardown, product helpers, /reconcile, JSON content-type ─


def test_teardown_endpoint_flows():
    with patch("backend.config.load_config", return_value=None):
        resp_404 = _client().post("/api/provision/teardown", json={})
        assert resp_404.status_code == 404
        assert "No service config" in resp_404.json()["detail"]["error"]

    fake_cfg = {
        "service_id": "svc",
        "fastly_api_key": "key",
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "name": "Test Svc",
        "provisioning": {},
    }
    sync_calls: list[bool] = []
    fake_scheduler = MagicMock()
    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent-config.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab", side_effect=lambda: sync_calls.append(True)),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.core.storage.clear_source_caches"),
        patch(
            "backend.provision.perform_teardown",
            return_value=iter([{"type": "status", "message": "removed s3 bucket"}]),
        ),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc", "cust-X")),
    ):
        resp = _client().post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": True},
        )
        assert resp.status_code == 200
        assert "removed s3 bucket" in resp.text and '"type": "done"' in resp.text and "Cron jobs updated" in resp.text
        assert sync_calls == [True]
        fake_scheduler.reload.assert_called_once()

    def _boom(state, token, opts=None):
        raise RuntimeError("Fastly API rate-limited")
        yield

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler", return_value=MagicMock()),
        patch("backend.core.storage.clear_source_caches"),
        patch("backend.provision.perform_teardown", side_effect=_boom),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc", "cust-X")),
    ):
        resp_err = _client().post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": False},
        )
        assert resp_err.status_code == 200
        assert '"type": "error"' in resp_err.text and "Fastly API rate-limited" in resp_err.text


def test_product_and_object_storage_helpers():
    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "object_storage"}}):
        assert object_storage_enabled("tok") is True
    with patch(
        "backend.provision.fos_setup.fastly",
        side_effect=RuntimeError("HTTP 404 GET /enabled-products/v1/object_storage"),
    ):
        assert object_storage_enabled("tok") is False
    for err_msg in ("HTTP 503 GET x", "Network error on GET x"):
        with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError(err_msg)):
            assert object_storage_enabled("tok") is True

    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "kv_store"}}) as m:
        assert product_enabled("tok", "kv_store") is True
        assert m.call_args.args[:2] == ("GET", "/enabled-products/v1/kv_store")
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 404 not found")):
        assert product_enabled("tok", "kv_store") is False
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 502 bad gateway")):
        assert product_enabled("tok", "kv_store") is True

    with patch("backend.provision.fos_setup.product_enabled", return_value=True) as m_del:
        assert fos_setup.object_storage_enabled("tok") is True
        assert m_del.call_args.args == ("tok", "object_storage")


def test_reconcile_route_and_json_content_type():
    post_op = app.openapi()["paths"]["/api/provision/reconcile"]["post"]
    assert [p["name"] for p in post_op.get("parameters", []) if p.get("in") == "query"] == []
    assert "requestBody" in post_op

    with patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile:
        resp = _client().post("/api/provision/reconcile", json={})
        assert resp.status_code == 400
        mock_reconcile.assert_not_called()

    for ct in (b"application/json", b"application/json; charset=utf-8"):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"content-type", ct)]}, lambda: None)
        _require_json_content_type(req)

    for bad_hdrs in ([(b"content-type", b"text/plain")], []):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": bad_hdrs}, lambda: None)
        with pytest.raises(HTTPException) as exc:
            _require_json_content_type(req)
        assert exc.value.status_code == 415
