"""Teardown idempotency + partial-failure recovery (TESTING_PLAN_3 item 15).

Existing ``test_provision_lifecycle.py`` covers the happy path
(teardown removes config + emits ``done``) and the 404 path (no config).
This file pins two operational contracts the lifecycle suite doesn't:

  1. **Idempotency.** A user re-fires teardown after a partial failure
     (network flaked mid-stream) — the second call must NOT crash the
     server. Expected behavior: first call 200, second call 404 with
     the same clean SSE-free body. No 500s, no stack traces.
  2. **Partial-failure recovery via the orphaned-key reaper.** If
     provisioning died after step 6 (CDN service created) but before
     write_service_config ran, the user has a Fastly CDN service and a
     temp FOS key but no config file. The teardown route returns 404
     in that case (correct: there's nothing for it to clean up). The
     reaper path is covered separately by the orchestrator's except
     branch, which already calls ``perform_teardown`` on the partial
     state — verified by ``test_provision_wizard_e2e.py``'s rollback
     test. This file pins the *route-level* idempotency contract.
  3. **Cache preservation.** ``remove_cache=False`` must leave the
     DuckDB file alone — otherwise users can lose their analytical data
     to a "just clean up Fastly" intent.
"""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.main import app


def _parse_sse_events(text: str) -> list[dict]:
    out: list[dict] = []
    for chunk in text.split("\n\n"):
        chunk = chunk.strip()
        if chunk.startswith("data:"):
            try:
                out.append(json.loads(chunk[len("data:") :].strip()))
            except json.JSONDecodeError:
                pass
    return out


@pytest.fixture
def isolated_configs_dir(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    return cfg_dir


def _seed_minimal_cfg(sid: str) -> None:
    svcconfig.save_config(
        sid,
        {
            "service_id": sid,
            "logging_service_id": sid,
            "name": f"Idempotency Svc {sid}",
            "fos_bucket": f"idem-bucket-{sid}",
            "fos_region": "us-east-1",
            "fos_access_key_id": "AKIA",
            "fos_secret_access_key": "SECRET",
            "fastly_api_key": "fastly-test-token",
            "cdn_secret": "x",
            "provisioning": {
                "fos_key_id": "fk_1",
                "endpoint_name": "EP",
                "cdn_service_id": "cdn_svc_1",
                "cdn_url": "https://cdn.example/",
            },
        },
    )


def _fake_perform_teardown(state, token, opts=None):
    yield {"type": "status", "message": "removing logging endpoint (mock)"}
    yield {"type": "status", "message": "removing FOS bucket (mock)"}
    yield {"type": "done", "message": "teardown complete (mock)"}


# Security: destructive teardown requires a caller-supplied Fastly token
# validated via /tokens/self. These operational tests bypass the real Fastly
# call by stubbing the validator — the auth gate itself is exercised in
# tests/routers/test_provision_teardown_auth.py.
_VALID_TOKEN = "test-admin-token"
_VALID_TOKEN_SHAPE = {
    "id": "tok_test",
    "user_id": "user_test",
    "scope": "global",
    "services": [],
    "customer_id": "cust-IDEM",
}


def _fake_fastly_dual(method, path, *, token, **kw):
    """Dispatcher that returns ``/tokens/self`` or ``/service/{id}`` payloads
    for the two-shot pattern introduced by the 035 tenant cross-check."""
    if path == "/tokens/self":
        return _VALID_TOKEN_SHAPE
    # /service/{sid} — must match the token's customer_id
    return {"id": path.rsplit("/", 1)[-1], "customer_id": _VALID_TOKEN_SHAPE["customer_id"]}


def test_teardown_is_idempotent_second_call_is_404(isolated_configs_dir):
    """Two back-to-back teardown calls: first 200 + cleans up, second 404."""
    sid = "svc-idem-1"
    _seed_minimal_cfg(sid)

    with (
        patch("backend.provision.perform_teardown", side_effect=_fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly_dual),
    ):
        with TestClient(app) as client:
            r1 = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": _VALID_TOKEN,
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                    "remove_cron": False,
                },
            )
            assert r1.status_code == 200
            events1 = _parse_sse_events(r1.text)
            assert any(e.get("type") == "done" for e in events1), (
                f"first teardown should emit done, got: {[e.get('type') for e in events1]}"
            )
            assert not os.path.exists(svcconfig.config_path(sid)), "first teardown should remove the config file"

            # Second call: config is gone, route should reject with 404 cleanly.
            # No 500, no stack trace, no partial SSE stream.
            r2 = client.post(
                "/api/provision/teardown",
                json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
            )

    assert r2.status_code == 404, (
        f"second teardown after config removed should be 404, got {r2.status_code}: {r2.text[:300]}"
    )
    body2 = r2.json()
    assert "detail" in body2, f"404 should have FastAPI 'detail' shape: {body2}"


def test_teardown_unknown_service_id_never_provisioned_is_404(isolated_configs_dir):
    """Teardown on a service that never existed (typo / wrong env) — 404, no crash.

    No token needed because we return 404 before reaching the auth gate (state
    is None → 404 first). The auth gate only fires once state is loaded.
    """
    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": "never-existed-svc", "remove_cache": False},
        )
    assert r.status_code == 404


def test_teardown_preserves_duckdb_when_remove_cache_false(isolated_configs_dir, tmp_path):
    """``remove_cache=False`` MUST leave the per-service DuckDB file intact.

    A teardown intent is sometimes "decommission the Fastly resources but
    keep the local analytics data". This contract pin prevents a future
    refactor from accidentally wiping the DuckDB regardless of the flag.
    """
    sid = "svc-idem-keep-cache"
    _seed_minimal_cfg(sid)

    # Simulate an existing DuckDB file on disk (the cron pipeline would
    # have created this — for the test we just touch a placeholder).
    db_path = svcconfig.duckdb_path(sid)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, "w") as f:
        f.write("placeholder-duckdb-bytes")
    assert os.path.exists(db_path), "precondition: db placeholder exists"

    with (
        patch("backend.provision.perform_teardown", side_effect=_fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly_dual),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": _VALID_TOKEN,
                    "remove_cache": False,
                    "remove_cron": False,
                },
            )

    assert r.status_code == 200
    # Config removed (always, regardless of remove_cache).
    assert not os.path.exists(svcconfig.config_path(sid))
    # DuckDB preserved (because remove_cache=False).
    assert os.path.exists(db_path), f"DuckDB at {db_path} should NOT be removed when remove_cache=false"


def test_teardown_partial_failure_in_perform_teardown_still_succeeds(isolated_configs_dir):
    """If ``perform_teardown`` raises mid-stream (e.g. Fastly 503 while deleting
    the logging endpoint), the route MUST still:
      - emit an error event in the SSE stream
      - leave the config file removed (it was deleted at the *start* of stream())
      - return 200 (SSE streams don't change status mid-flight)

    This is the recovery contract: a flaky Fastly API call shouldn't strand
    the user with a half-torn-down service and a config they can't get rid of.
    """
    sid = "svc-idem-partial-fail"
    _seed_minimal_cfg(sid)

    def _boom_teardown(state, token, opts=None):
        yield {"type": "status", "message": "removing logging endpoint (mock)"}
        raise RuntimeError("Fastly API: 503 Service Unavailable (mock)")

    with (
        patch("backend.provision.perform_teardown", side_effect=_boom_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly_dual),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
            )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "error" in types, f"expected error event after teardown raised, got: {types}"

    # Config was removed at the top of stream() (line ~242 in provision.py),
    # so it should be gone even though the Fastly call failed. Re-firing
    # teardown should now return 404 — verifying the idempotency contract
    # composes with the partial-failure path.
    assert not os.path.exists(svcconfig.config_path(sid)), (
        "config should have been removed before perform_teardown raised"
    )
    with TestClient(app) as client:
        r2 = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )
    assert r2.status_code == 404, f"second teardown should be 404, got {r2.status_code}"
