"""The call-trace envelope on admin routes follows ``DEBUG_RESPONSES`` like ``BaseResponse`` does.

``provision``, ``services/core``, ``services/audit`` and ``admin/iceberg`` used to attach
``"_debug_calls": get_tracked_calls()`` by hand, unconditionally, so the envelope (Fastly call
paths and timings) went out on every admin response whatever the flag said. They now go through
``telemetry.debug_envelope`` / ``BaseResponse.with_telemetry``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.utils import telemetry
from tests.conftest import MOCK_SERVICE_ID

pytestmark = pytest.mark.security_regression

_CALLS = [{"service": "Fastly API", "method": "GET", "path": "/probe", "time_ms": 1.0}]


@pytest.fixture
def recorded_calls():
    with patch.object(telemetry, "get_tracked_calls", return_value=_CALLS):
        yield


def _debug_keys(body: dict) -> list[str]:
    return [k for k in body if k.startswith(("_debug", "debug_"))]


# ── debug_envelope(gated=True, inject_calls=True) ────────────────────────────


def test_gated_envelope_adds_calls_only_when_debug_responses_is_on(monkeypatch, recorded_calls):
    monkeypatch.setenv("DEBUG_RESPONSES", "true")

    out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)

    assert out == {"ok": True, "_debug_calls": _CALLS}


def test_gated_envelope_adds_nothing_when_debug_responses_is_off(monkeypatch, recorded_calls):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

    out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)

    assert out == {"ok": True}


def test_gated_envelope_drops_a_stale_envelope_when_debug_responses_is_off(monkeypatch, recorded_calls):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

    out = telemetry.debug_envelope(
        {"ok": True, "_debug_calls": ["stale"], "_debug_queries": ["stale"]},
        gated=True,
        is_cached=None,
        inject_calls=True,
    )

    assert out == {"ok": True}


def test_gated_envelope_without_inject_calls_still_never_injects(monkeypatch, recorded_calls):
    monkeypatch.setenv("DEBUG_RESPONSES", "true")

    out = telemetry.debug_envelope({"ok": True}, gated=True, is_cached=True)

    assert out == {"ok": True, "_is_cached": True}


# ── the migrated admin routes ────────────────────────────────────────────────


def _validate(client):
    def _fastly_spy(method, path, **kwargs):
        return {"id": "tok-1", "name": "T", "user_id": "u"} if "/tokens/self" in path else {"name": "Svc"}

    with patch("backend.core.fastly.client.fastly", side_effect=_fastly_spy):
        return client.post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})


def _check_domain(client):
    with patch("backend.routers.provision._check_domain_available", return_value=(True, None)):
        return client.get("/api/provision/check-domain?prefix=valid-prefix-1")


def _check_fos_ok(client):
    fake = MagicMock()
    fake.list_objects_v2.return_value = {"Contents": []}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _check_fos_failed(client):
    fake = MagicMock()
    fake.list_objects_v2.side_effect = RuntimeError("boom")
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _audit_logs(client):
    return client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _iceberg_calendar(client):
    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {"service_id": "service123", "name": "service123"}
    with patch(
        "backend.core.iceberg.get_snapshot_calendar", return_value={"2026-08-19": {"data_files": 5, "size_bytes": 1}}
    ):
        return client.get("/api/admin/iceberg-calendar?table_name=logs")


def _log_fields_get(client):
    svcconfig.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "log_fields": {"groups": ["A"]}})
    return client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _generate_viewer_key(client):
    invite = {
        "service_id": "svc-1",
        "name": "MyService",
        "access_key_id": "AK",
        "secret_key": "SK",
        "fos_bucket": "b",
        "fos_region": "us-east-1",
        "fos_endpoint": "https://s3.us-east-1.amazonaws.com",
        "fos_prefix": "",
    }
    with patch("backend.provision.generate_analyst_invite", return_value=invite):
        return client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")


_SITES = [
    pytest.param(_validate, id="provision-validate"),
    pytest.param(_check_domain, id="provision-check-domain"),
    pytest.param(_check_fos_ok, id="provision-check-fos-ok"),
    pytest.param(_check_fos_failed, id="provision-check-fos-failed"),
    pytest.param(_audit_logs, id="audit-logs"),
    pytest.param(_iceberg_calendar, id="iceberg-calendar"),
    pytest.param(_log_fields_get, id="service-log-fields"),
    pytest.param(_generate_viewer_key, id="generate-viewer-key"),
]


@pytest.mark.parametrize("call", _SITES)
def test_route_emits_no_debug_envelope_when_debug_responses_is_off(client, monkeypatch, call):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)

    r = call(client)

    assert r.status_code == 200
    assert _debug_keys(r.json()) == []


@pytest.mark.parametrize("call", _SITES)
def test_route_emits_the_call_trace_when_debug_responses_is_on(client, monkeypatch, recorded_calls, call):
    monkeypatch.setenv("DEBUG_RESPONSES", "true")

    r = call(client)

    assert r.status_code == 200
    assert [c["path"] for c in r.json()["_debug_calls"]] == ["/probe"]
