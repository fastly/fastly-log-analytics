"""Tests for ``backend.routers.services.core`` — GET-side endpoints.

The mutating endpoints (PATCH/POST/DELETE) are covered by
[test_service_mutations.py](tests/routers/test_service_mutations.py).
This file pins the **GET** + read-side endpoints:

  - GET /services (list)
  - GET /services/{id}/lake-info
  - GET /services/{id}/logging-settings (Fastly API GET + regex parsing)
  - GET /services/{id}/log-fields (with audit history)
  - GET /cron-schedule (scheduler enumeration)

These power the Settings page's read paths — a regression silently
breaks the dashboard's preflight rendering.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import MOCK_SERVICE_ID


@pytest.fixture(autouse=True)
def _clear_cron_schedule_ttl_cache():
    """``api_cron_schedule`` memoises by service_id with a 5 s TTL via a
    module-level dict. Tests in this file all hit MOCK_SERVICE_ID within
    that window so the second test would receive the first test's
    payload — masking real route behaviour. Clear on enter and exit."""
    from backend.routers.services import core as _core

    _core._cron_schedule_cache.clear()
    yield
    _core._cron_schedule_cache.clear()


# ── GET /services ───────────────────────────────────────────────────────────


def test_services_list_returns_enriched_services(client):
    """Wraps ``get_enriched_services``; the response shape is
    ``{services: [...]}``. Pinned because the FE's service-switcher
    keys on this exact shape."""
    fake_services = [
        {"service_id": "svc-1", "name": "Svc 1", "fos_bucket": "b1", "fos_region": "us-east-1"},
        {"service_id": "svc-2", "name": "Svc 2", "fos_bucket": "b2", "fos_region": "us-west-2"},
    ]
    with patch("backend.services.service_manager.get_enriched_services", return_value=fake_services):
        resp = client.get("/api/services")
    assert resp.status_code == 200
    services = resp.json()["services"]
    assert len(services) == 2
    assert services[0]["service_id"] == "svc-1"
    assert services[1]["service_id"] == "svc-2"


def test_services_list_returns_empty_when_no_services(client):
    """No services configured → ``{services: []}``. Pinned because
    the FE distinguishes empty (show "create your first service" CTA)
    from missing (500)."""
    with patch("backend.services.service_manager.get_enriched_services", return_value=[]):
        resp = client.get("/api/services")
    assert resp.status_code == 200
    assert resp.json()["services"] == []


# ── GET /services/{id}/lake-info ────────────────────────────────────────────


def test_lake_info_returns_fetched_payload(client):
    """Lake-info wraps ``fetch_lake_info`` with ``use_temp_cache=False``.
    Pinned because the FE keys on the returned earliest/latest fields
    to render the time-range picker."""
    fake_info = {
        "earliest": "2026-01-01T00:00:00Z",
        "latest": "2026-05-18T00:00:00Z",
        "calendar": {},
    }
    with patch("backend.core.iceberg.lake_info.fetch_lake_info", return_value=fake_info) as mock_fetch:
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/lake-info",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["earliest"] == "2026-01-01T00:00:00Z"
    # Verify use_temp_cache=False (the deployment flag — temp_cache=True
    # is only for the provisioning catalog leak workaround)
    _, kwargs = mock_fetch.call_args
    assert kwargs.get("use_temp_cache") is False


# ── GET /services/{id}/logging-settings ────────────────────────────────────


def test_logging_settings_404s_when_service_not_found(client, tmp_path, monkeypatch):
    """Missing service config → 404. Pinned because the FE
    distinguishes 404 (deleted) from 500 (transient API failure)."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    # No service saved

    resp = client.get(
        f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 404


def test_logging_settings_400s_when_no_active_version(client, tmp_path, monkeypatch):
    """If the Fastly service has no active version (brand-new service),
    the endpoint can't read the logging endpoint config — 400 with
    a clear message. Pinned because this is the recovery-path signal
    the FE shows ("Activate your Fastly service first")."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    with patch("backend.core.fastly.service.get_active_version", return_value=None):
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
    assert resp.status_code == 400


def test_logging_settings_parses_sample_rate_from_vcl_condition(client, tmp_path, monkeypatch):
    """When the logging endpoint has a ``response_condition`` pointing
    at a "Log Sampling" condition, the route parses the ``randombool(N, …)``
    out of the VCL statement and exposes N as ``sample_rate``. Pinned
    because the FE renders the slider position from this value."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    fake_endpoint = {
        "format": "x",
        "period": 60,
        "path": "/raw/logs/",
        "response_condition": "Log Sampling",
    }
    # Statement encodes a 25% sample rate + edge-only filter
    fake_cond = {"statement": "randombool(25, 100) && req.restarts == 0"}

    with (
        patch("backend.core.fastly.service.get_active_version", return_value=42),
        patch("backend.core.fastly.client.fastly", return_value=fake_endpoint),
        patch("backend.core.fastly.service.find_condition", return_value=fake_cond),
    ):
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sample_rate"] == 25
    assert body["edge_only"] is True
    assert body["version"] == 42
    assert body["period"] == 60


def test_logging_settings_extracts_custom_condition_from_vcl(client, tmp_path, monkeypatch):
    """When the VCL statement has an ``&& (custom_predicate)`` tail
    (after the sampling/edge-only prefix), the route extracts the
    predicate as ``custom_condition``. Pinned because the FE renders
    it as the "Custom condition" textarea — losing it would silently
    drop the customer's filter."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    fake_endpoint = {"format": "x", "period": 60, "path": "/raw/", "response_condition": "Log Sampling"}
    fake_cond = {"statement": 'randombool(100, 100) && (req.url ~ "^/api/")'}

    with (
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch("backend.core.fastly.client.fastly", return_value=fake_endpoint),
        patch("backend.core.fastly.service.find_condition", return_value=fake_cond),
    ):
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    assert resp.json()["custom_condition"] == 'req.url ~ "^/api/"'


def test_logging_settings_defaults_sample_rate_to_100_when_no_condition(client, tmp_path, monkeypatch):
    """No ``response_condition`` on the endpoint → defaults
    (sample_rate=100, edge_only=False). Pinned because losing the
    100 default would render the slider at 0 on a logging endpoint
    that captures every request."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    fake_endpoint = {"format": "x", "period": 60, "path": "/raw/", "response_condition": None}

    with (
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch("backend.core.fastly.client.fastly", return_value=fake_endpoint),
    ):
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sample_rate"] == 100
    assert body["edge_only"] is False


def test_logging_settings_500s_on_generic_exception(client, tmp_path, monkeypatch):
    """Generic exceptions in the Fastly API flow → 500 with the
    error text. Pinned because the FE renders the error in the
    settings panel."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    with patch("backend.core.fastly.service.get_active_version", side_effect=RuntimeError("network")):
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
    assert resp.status_code == 500


def test_logging_settings_falls_back_to_provisioning_custom_condition(client, tmp_path, monkeypatch):
    """When no condition VCL matches the regex (e.g. unconditional
    logging endpoint), the route falls back to the saved
    ``provisioning.custom_condition``. Pinned because the wizard
    saves it there; losing the fallback would forget the user's
    setting."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "fastly_api_key": "tok",
            "provisioning": {"custom_condition": "saved_predicate"},
        },
    )

    fake_endpoint = {"format": "x", "period": 60, "path": "/raw/", "response_condition": None}
    with (
        patch("backend.core.fastly.service.get_active_version", return_value=5),
        patch("backend.core.fastly.client.fastly", return_value=fake_endpoint),
    ):
        resp = client.get(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    assert resp.json()["custom_condition"] == "saved_predicate"


# ── GET /services/{id}/log-fields ───────────────────────────────────────────


def test_log_fields_get_404s_when_service_not_found(client, tmp_path, monkeypatch):
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-fields",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 404


def test_log_fields_get_returns_saved_config_with_estimate(client, tmp_path, monkeypatch):
    """Happy path: returns ``log_fields``, ``estimate``, ``history``.
    Pinned because the FE renders the byte-estimate panel from the
    estimate field."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {"groups": ["A", "B"], "field_overrides": {}},
        },
    )

    resp = client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-fields",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert "log_fields" in body
    assert "estimate" in body
    assert isinstance(body["history"], list)
    assert "waf_warning" in body


def test_log_fields_get_defaults_to_standard_preset_when_unset(client, tmp_path, monkeypatch):
    """A service config without ``log_fields.groups`` (older install
    pre-presets) → defaults to the standard preset. Pinned because
    losing this fallback would render an empty groups list in the FE
    picker."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID},  # no log_fields at all
    )

    resp = client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-fields",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert resp.status_code == 200
    body = resp.json()
    # The standard preset is non-empty
    assert len(body["log_fields"]["groups"]) > 0


# ── GET /cron-schedule ──────────────────────────────────────────────────────


def test_cron_schedule_returns_schedules_list(client):
    """When the scheduler is running, return per-task schedules with
    next_run_time + last_run_status. Pinned because the Settings
    panel renders the per-task row from this exact shape."""
    fake_per_task = {
        "log_discovery": {
            "started_at": "2026-05-18T00:00:00Z",
            "status": "ok",
            "duration_s": 1.2,
            "summary": "120 files",
        }
    }

    with (
        patch("backend.core.metadata.latest_cron_per_task", return_value=fake_per_task),
        patch("backend.cron.scheduler.get_scheduler") as mock_get_sched,
    ):
        # Empty scheduler — no jobs registered. The route should still
        # surface the last-run history for tasks in _TASK_MAP.
        fake_sched = type("S", (), {"_sched": type("X", (), {"get_jobs": lambda self: []})()})()
        mock_get_sched.return_value = fake_sched

        resp = client.get("/api/cron-schedule", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    schedules = resp.json()["schedules"]
    # The log_discovery task has no scheduled job but has a last_run — should appear
    sync_entries = [s for s in schedules if s["task"] == "log_discovery"]
    assert len(sync_entries) == 1
    assert sync_entries[0]["last_run_status"] == "ok"
    assert sync_entries[0]["next_run_time"] is None


def test_cron_schedule_swallows_metadata_db_exception(client):
    """If reading last_runs from metadata_db raises, return [] (not
    500). Pinned because metadata_db errors during a deploy should
    not surface as broken Settings panels. count_alerts is also
    patched to raise so the no-alerts-placeholder synthesis path
    doesn't mask a real metadata_db outage."""
    with (
        patch("backend.core.metadata.latest_cron_per_task", side_effect=RuntimeError("locked")),
        patch("backend.core.metadata.count_alerts", side_effect=RuntimeError("locked")),
        patch("backend.cron.scheduler.get_scheduler") as mock_get_sched,
    ):
        fake_sched = type("S", (), {"_sched": type("X", (), {"get_jobs": lambda self: []})()})()
        mock_get_sched.return_value = fake_sched

        resp = client.get("/api/cron-schedule", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert resp.json()["schedules"] == []


def test_cron_schedule_synthesizes_alerts_placeholder_when_zero_alerts(client):
    """When no alerts exist and the alerts cron is therefore not
    registered, surface a placeholder entry with
    disabled_reason='no_alerts_configured' so the UI shows "No alerts
    configured" instead of silently omitting the tile."""
    with (
        patch("backend.core.metadata.latest_cron_per_task", return_value={}),
        patch("backend.core.metadata.count_alerts", return_value=0),
        patch("backend.cron.scheduler.get_scheduler") as mock_get_sched,
    ):
        fake_sched = type("S", (), {"_sched": type("X", (), {"get_jobs": lambda self: []})()})()
        mock_get_sched.return_value = fake_sched

        resp = client.get("/api/cron-schedule", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    schedules = resp.json()["schedules"]
    alerts_entries = [s for s in schedules if s["task"] == "alerts"]
    assert len(alerts_entries) == 1
    assert alerts_entries[0]["next_run_time"] is None
    assert alerts_entries[0]["disabled_reason"] == "no_alerts_configured"


def test_cron_schedule_tags_historical_alerts_entry_with_disabled_reason(client):
    """Regression: when the user deletes their last alert AFTER the
    alerts cron has previously run, the historical-runs loop adds an
    alerts entry with next_run_time=None. Without explicit tagging the
    UI rendered "Next: Disabled" — ambiguous, looks like an outage.
    Verify the disabled_reason is applied to the existing entry."""
    with (
        patch(
            "backend.core.metadata.latest_cron_per_task",
            return_value={
                "alerts": {
                    "started_at": "2026-05-22T10:00:00Z",
                    "status": "success",
                    "duration_s": 0.5,
                    "summary": "No alerts to evaluate",
                    "error_message": None,
                }
            },
        ),
        patch("backend.core.metadata.count_alerts", return_value=0),
        patch("backend.cron.scheduler.get_scheduler") as mock_get_sched,
    ):
        fake_sched = type("S", (), {"_sched": type("X", (), {"get_jobs": lambda self: []})()})()
        mock_get_sched.return_value = fake_sched

        resp = client.get("/api/cron-schedule", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    schedules = resp.json()["schedules"]
    alerts_entries = [s for s in schedules if s["task"] == "alerts"]
    assert len(alerts_entries) == 1
    # Historical fields preserved
    assert alerts_entries[0]["last_run_status"] == "success"
    # New tag applied
    assert alerts_entries[0]["disabled_reason"] == "no_alerts_configured"


# ── GET /services/{id}/logging-settings/update (SSE) ─────────────────────


def test_update_logging_settings_404s_when_service_missing(client, tmp_path, monkeypatch):
    """Missing config → 404. Pinned because this endpoint mutates
    Fastly via SSE; the 404 must short-circuit before any Fastly
    API call."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update")
    assert resp.status_code == 404


def test_update_logging_settings_400s_on_out_of_range_period(client, tmp_path, monkeypatch):
    """``period`` must be in [1, 86400] seconds. Fastly's S3 logging API
    accepts ≥1s; the UI's "1 second" tier maps the sync cron to a 5s
    cadence (see backend/provision/orchestrator.py). period=0 is the
    nearest invalid value worth pinning."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    resp = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update",
        params={"period": 0},  # below 1s floor
    )
    assert resp.status_code == 400
    assert "1" in resp.json()["detail"]["error"]


def test_update_logging_settings_400s_on_out_of_range_sample_rate(client, tmp_path, monkeypatch):
    """``sample_rate`` must be in [1, 100]. Pinned for the same
    reason as period — catching client-side avoids the deploy round-
    trip."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    resp = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update",
        params={"sample_rate": 0},
    )
    assert resp.status_code == 400


def test_update_logging_settings_streams_done_event_and_persists_config(client, tmp_path, monkeypatch):
    """Happy path: SSE stream emits the orchestrator's ``done`` event
    and the route persists the new sample_rate/edge_only/period into
    the local config. Pinned because admins re-read the config from
    the Settings page after the SSE finishes — losing the persist
    step would silently revert their settings on reload."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "fastly_api_key": "tok",
            "log_period": 60,
            "fos_prefix": "old-prefix",
            "provisioning": {"sample_rate": 100, "edge_only": False, "endpoint_name": "ep"},
        },
    )

    fake_events = [{"type": "status", "message": "ok"}, {"type": "done", "changed": True, "version": 99}]

    with (
        patch("backend.provision.update_logging_endpoint", return_value=iter(fake_events)),
        patch("backend.provision._sync_crontab"),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update",
            params={"period": 120, "sample_rate": 50, "prefix": "new-prefix", "edge_only": True},
        )

    assert resp.status_code == 200
    # SSE stream → text body contains the events
    assert "done" in resp.text

    # Config was persisted with the new values
    fresh = config.load_config(MOCK_SERVICE_ID)
    assert fresh["log_period"] == 120
    assert fresh["fos_prefix"] == "new-prefix"
    assert fresh["provisioning"]["sample_rate"] == 50
    assert fresh["provisioning"]["edge_only"] is True


def test_update_logging_settings_sub_minute_period_uses_interval_seconds(client, tmp_path, monkeypatch):
    """For periods <60s, cron_sync stores ``interval_seconds`` rather
    than ``interval_mins`` (which would round down to 0 and break the
    cron). Pinned because losing this branch produces a silently-
    disabled sync."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "fastly_api_key": "tok",
            "log_period": 60,
            "provisioning": {"cron_sync": {"interval_mins": 1}, "endpoint_name": "ep"},
        },
    )

    fake_events = [{"type": "done", "changed": True}]
    with (
        patch("backend.provision.update_logging_endpoint", return_value=iter(fake_events)),
        patch("backend.provision._sync_crontab"),
    ):
        client.post(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update",
            params={"period": 30},  # below 60s
        )

    fresh = config.load_config(MOCK_SERVICE_ID)
    cron_sync = fresh["provisioning"]["cron_sync"]
    assert cron_sync.get("interval_seconds") == 30
    assert "interval_mins" not in cron_sync


def test_update_logging_settings_emits_error_event_on_orchestrator_exception(client, tmp_path, monkeypatch):
    """An exception from update_logging_endpoint surfaces as a typed
    ``error`` SSE event (not a 500 — the SSE protocol is unidirectional
    once headers are sent). Pinned because the FE's SSE consumer
    keys on the ``error`` type to render the failure toast."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    with patch("backend.provision.update_logging_endpoint", side_effect=RuntimeError("API down")):
        resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update")

    assert resp.status_code == 200
    assert "API down" in resp.text
    assert "error" in resp.text


# ── POST /services/{id}/generate-viewer-key (api_invite_analyst) ─────────


def test_invite_analyst_returns_payload_on_success(client):
    """Wraps ``generate_analyst_invite`` + appends `_debug_calls`.
    Pinned because the FE renders the returned secret_key in the
    invite dialog."""
    fake_invite = {
        "service_id": "svc-1",
        "name": "MyService",
        "access_key_id": "AK",
        "secret_key": "SK",
        "fos_bucket": "b",
        "fos_region": "us-east-1",
        "fos_endpoint": "https://s3.us-east-1.amazonaws.com",
        "fos_prefix": "",
    }
    with patch("backend.provision.generate_analyst_invite", return_value=fake_invite):
        resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")
    assert resp.status_code == 200
    body = resp.json()
    assert body["access_key_id"] == "AK"
    assert body["secret_key"] == "SK"
    # _debug_calls always appended for telemetry overlay
    assert "_debug_calls" in body


def test_invite_analyst_maps_not_found_runtime_error_to_404(client):
    """``RuntimeError("Service X not found")`` from the orchestrator
    → 404. Pinned because the FE distinguishes 404 (typo'd id) from
    403 (wrong access level) when re-rendering."""
    with patch("backend.provision.generate_analyst_invite", side_effect=RuntimeError("Service ghost not found")):
        resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")
    assert resp.status_code == 404


def test_invite_analyst_maps_read_write_runtime_error_to_403(client):
    """``RuntimeError("requires a read_write …")`` → 403. Pinned
    because the FE renders a different "wrong-permissions" CTA on
    403 vs the 404 "service-not-found" path."""
    with patch(
        "backend.provision.generate_analyst_invite",
        side_effect=RuntimeError("Invite generation requires a read_write service configuration"),
    ):
        resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")
    assert resp.status_code == 403


def test_invite_analyst_maps_other_runtime_error_to_400(client):
    """Other RuntimeErrors (Fastly API failure, DB issue) → 400 with
    the underlying message. Pinned because the FE distinguishes 400
    (transient — retry) from 403/404 (config — change input)."""
    with patch("backend.provision.generate_analyst_invite", side_effect=RuntimeError("Fastly upstream timeout")):
        resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")
    assert resp.status_code == 400
    assert "timeout" in resp.json()["detail"]["error"]


# ── POST /services/{id}/ngwaf-sync (SSE) ─────────────────────────────────


def test_ngwaf_sync_emits_error_when_service_missing(client, tmp_path, monkeypatch):
    """SSE error event when the service config is missing. Pinned
    because the FE distinguishes "service deleted" (the SSE error)
    from "network down" (HTTP-layer failure) — both result in 200
    from FastAPI but only one has the error event body."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/ngwaf-sync")
    assert resp.status_code == 200
    assert "Service not found" in resp.text


def test_ngwaf_sync_emits_error_when_workspace_not_configured(client, tmp_path, monkeypatch):
    """Service exists but no NGWAF workspace_id configured → SSE
    error. Pinned because triggering an NGWAF sync without a
    workspace would 400 the Fastly API and surface confusingly."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    # Save config WITHOUT ngwaf_workspace_id
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/ngwaf-sync")
    assert resp.status_code == 200
    assert "NGWAF workspace" in resp.text


def test_ngwaf_sync_emits_error_when_no_api_key(client, tmp_path, monkeypatch):
    """No ``fastly_api_key`` → SSE error. Pinned because the cron
    can't authenticate to Fastly without one."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "ngwaf_workspace_id": "ws-1"},  # no api key
    )

    resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/ngwaf-sync")
    assert resp.status_code == 200
    assert "Fastly API key" in resp.text


# ── _get_dir_stats helper (pure) ─────────────────────────────────────────


def test_get_dir_stats_returns_zero_for_missing_path(tmp_path):
    """Non-existent path → ``(0, 0)`` (not crash). Pinned because
    `_get_dir_stats` powers the "cache size" hint on the Settings
    panel — a missing cache directory shouldn't 500 the page."""
    from backend.services.service_manager import _get_dir_stats

    size, count = _get_dir_stats(str(tmp_path / "does_not_exist"))
    assert (size, count) == (0, 0)


def _poll_dir_stats(path: str, expected: tuple[int, int], timeout: float = 2.0) -> tuple[int, int]:
    """Call `_get_dir_stats` until it settles on `expected` or `timeout` elapses.

    `_get_dir_stats` never blocks the caller, even on the very first call
    for a path — a cold/expired entry returns a best-effort value
    immediately (stale, or `(0, 0)`) and populates the real value on a
    background thread. Tests must poll for that thread to land rather
    than asserting on the single synchronous return.
    """
    import time

    from backend.services.service_manager import _get_dir_stats

    deadline = time.monotonic() + timeout
    result = _get_dir_stats(path)
    while result != expected and time.monotonic() < deadline:
        time.sleep(0.02)
        result = _get_dir_stats(path)
    return result


def test_get_dir_stats_walks_recursively_and_sums_file_sizes(tmp_path):
    """Recursive walk summing file sizes + count. Pinned because
    the Settings panel shows the per-service cache footprint and a
    refactor that broke the recursion would under-report."""
    (tmp_path / "a.txt").write_bytes(b"x" * 100)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.txt").write_bytes(b"y" * 200)

    size, count = _poll_dir_stats(str(tmp_path), (300, 2))
    assert size == 300
    assert count == 2


def test_get_dir_stats_skips_symlinks(tmp_path):
    """Symlinks are NOT followed (would double-count or loop forever).
    Pinned because the cache dir can contain symlinks to shared
    parquet files; following them would inflate the size estimate."""
    import os

    real = tmp_path / "real.txt"
    real.write_bytes(b"x" * 50)
    link = tmp_path / "link.txt"
    os.symlink(real, link)

    # Only the real file counts (50 bytes); the symlink is skipped
    size, count = _poll_dir_stats(str(tmp_path), (50, 1))
    assert size == 50
    assert count == 1


def test_get_dir_stats_never_blocks_on_a_cold_path(tmp_path, monkeypatch):
    """The very first call for a never-seen path must return immediately
    (the `(0, 0)` placeholder) rather than walking synchronously — a slow
    walk (thousands of files) previously blocked /api/bootstrap +
    /api/services for as long as the walk took, taking /admin down after
    every restart for services with a large local cache."""
    import time

    from backend.services import service_manager
    from backend.services.service_manager import _get_dir_stats

    (tmp_path / "a.txt").write_bytes(b"x" * 100)

    # Make the walk itself artificially slow so a blocking implementation
    # would fail this test's timing assertion; a non-blocking one won't
    # notice since the walk runs on a background thread.
    real_walk = service_manager._walk_dir_stats

    def slow_walk(path: str) -> tuple[int, int]:
        time.sleep(0.5)
        return real_walk(path)

    monkeypatch.setattr(service_manager, "_walk_dir_stats", slow_walk)

    started = time.monotonic()
    size, count = _get_dir_stats(str(tmp_path))
    elapsed = time.monotonic() - started

    assert (size, count) == (0, 0)
    assert elapsed < 0.4, f"_get_dir_stats blocked for {elapsed:.2f}s on a cold path"


# ── GET /services/{id}/custom-fields/export ───────────────────────────────


def test_export_custom_fields_404s_when_service_missing(client, tmp_path, monkeypatch):
    """Missing service → 404. Pinned because the FE distinguishes
    404 (deleted) from 500 (transient) when re-rendering."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.get(
        f"/api/services/{MOCK_SERVICE_ID}/custom-fields/export",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 404


def test_export_custom_fields_returns_json_attachment_with_filename(client, tmp_path, monkeypatch):
    """Export streams JSON with a `Content-Disposition` filename
    keyed on the service_id. Pinned because the FE's download
    helper keys on this exact filename pattern."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "groups": ["A"],
                "field_overrides": {},
                "custom_fields": [
                    {"name": "my_field", "label": "My Field", "enabled": True},
                ],
            },
        },
    )

    resp = client.get(
        f"/api/services/{MOCK_SERVICE_ID}/custom-fields/export",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    assert f"custom_fields_{MOCK_SERVICE_ID}.json" in resp.headers["content-disposition"]

    import json as _json

    body = _json.loads(resp.text)
    assert body["custom_fields"][0]["name"] == "my_field"


# ── POST /services/{id}/custom-fields/import ──────────────────────────────


def test_import_custom_fields_404s_when_service_missing(client, tmp_path, monkeypatch):
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"custom_fields": []},
    )
    assert resp.status_code == 404


def test_import_custom_fields_422s_when_payload_not_a_list(client, tmp_path, monkeypatch):
    """``custom_fields`` must be a list. The FE's drag-and-drop import
    sends a JSON file directly; a dict-shaped file now produces a 422
    from the Pydantic body validator (matches the body/query
    classification from commit 3c036cf — see also Phase O's flips)."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    resp = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"custom_fields": {"not": "a list"}},
    )
    assert resp.status_code == 422
    errors = resp.json()["detail"]
    assert any(e.get("loc", [])[-1] == "custom_fields" for e in errors)


def test_import_custom_fields_merges_new_into_existing(client, tmp_path, monkeypatch):
    """Imported fields merge with existing (upsert by name). Pinned
    because admins import a partial set expecting to keep their other
    custom fields — full-replace would silently delete the rest.

    019: imported fields now run through ``validate_custom_field``, so
    the JSON body must include every required key. The fixture below is
    a fully-populated valid field — testing the merge contract, not
    validator leniency.
    """
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "groups": ["A"],
                "field_overrides": {},
                "custom_fields": [
                    {
                        "name": "keep_me",
                        "label": "Existing",
                        "enabled": True,
                        "vcl_log_expression": "req.http.x-keep-me",
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "bytes_estimate": 20,
                    },
                ],
            },
        },
    )

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "custom_fields": [
                    {
                        "name": "new_one",
                        "label": "New",
                        "enabled": True,
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "vcl_log_expression": "req.http.x-new-one",
                        "bytes_estimate": 20,
                    }
                ]
            },
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["imported_count"] == 1

    fresh = config.load_config(MOCK_SERVICE_ID)
    names = {cf["name"] for cf in fresh["log_fields"]["custom_fields"]}
    # Both kept
    assert names == {"keep_me", "new_one"}


def test_import_custom_fields_422s_on_locked_type_change(client, tmp_path, monkeypatch):
    """If an imported field already exists in the Iceberg table AND
    changes `duckdb_type`/`value_type`, refuse with 422. Pinned because
    Iceberg's schema-evolution rules don't allow incompatible type
    swaps — Letting through would error at commit time with an
    opaque message."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "groups": ["A"],
                "field_overrides": {},
                "custom_fields": [
                    {"name": "locked_field", "label": "x", "enabled": True, "duckdb_type": "VARCHAR"},
                ],
            },
        },
    )

    fake_table = MagicMock()
    fake_field = MagicMock()
    fake_field.name = "locked_field"
    fake_table.schema.return_value.fields = [fake_field]
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc"}),
        patch("backend.core.iceberg._get_catalog", return_value=fake_catalog),
        patch("backend.core.iceberg._table_identifier", return_value=("default", "logs")),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "custom_fields": [
                    {"name": "locked_field", "label": "x", "duckdb_type": "INTEGER"},  # Type change
                ]
            },
        )

    assert resp.status_code == 422
    errors = resp.json()["detail"]["errors"]
    assert any("locked_field" in e for e in errors)


def test_import_custom_fields_422s_when_log_format_would_exceed_limit(client, tmp_path, monkeypatch):
    """If the merged custom fields would push the log format over
    8000 chars (Fastly's limit), refuse with 422. Pinned because
    importing oversize is the most common failure mode for admins
    migrating from another tool.

    019: the imported field must be valid in isolation (vcl expr,
    types, etc.) so we hit the format-too-long branch instead of
    the per-field validator's 422.
    """
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch(
            "backend.provision.validate_log_format",
            return_value=["LOG_FORMAT_TOO_LONG: would be 12000 chars"],
        ),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "custom_fields": [
                    {
                        "name": "x",
                        "label": "x",
                        "vcl_log_expression": "req.http.x",
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "bytes_estimate": 20,
                    }
                ]
            },
        )

    assert resp.status_code == 422
    errors = resp.json()["detail"]["errors"]
    assert any("LOG_FORMAT_TOO_LONG" in e for e in errors)


# ── POST /services/{id}/custom-fields/validate-vcl ────────────────────────


def test_validate_custom_vcl_404s_when_service_missing(client, tmp_path, monkeypatch):
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"vcl_log_expression": "req.url", "collection_stage": "edge"},
    )
    assert resp.status_code == 404


def test_validate_custom_vcl_returns_valid_true_for_clean_expression(client, tmp_path, monkeypatch):
    """A simple, valid VCL expression → `valid: true` with empty
    errors. Pinned because the FE keys on the `valid` boolean to
    enable the "Save" button in the custom-field drawer."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch("backend.core.field_registry.validate_custom_field", return_value=[]),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.provision.load_log_format", return_value="format string"),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"vcl_log_expression": "req.url", "collection_stage": "edge"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["errors"] == []
    assert body["format_length"] == len("format string")


def test_validate_custom_vcl_routes_warn_prefix_to_warnings_not_errors(client, tmp_path, monkeypatch):
    """Lines prefixed with `WARN:` go to `warnings` (FE renders as
    yellow), unprefixed lines go to `errors` (red blocking). Pinned
    because misrouting would prevent saves on harmless warnings or
    allow saves on hard errors."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch(
            "backend.core.field_registry.validate_custom_field",
            return_value=["WARN: deprecated VCL function used", "Real syntax error"],
        ),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"vcl_log_expression": "bad VCL", "collection_stage": "edge"},
        )

    body = resp.json()
    assert body["valid"] is False  # Has a real error
    assert any("Real syntax error" in e for e in body["errors"])
    assert any("deprecated" in w for w in body["warnings"])
    # No "WARN:" prefix on the warning (stripped)
    assert not any(w.startswith("WARN:") for w in body["warnings"])


def test_validate_custom_vcl_omits_format_length_when_invalid(client, tmp_path, monkeypatch):
    """When validation errors are present, `format_length` is None
    (no point computing format length for invalid VCL). Pinned because
    the FE renders "—" when format_length is None vs a number when
    valid."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch("backend.core.field_registry.validate_custom_field", return_value=["Hard error"]),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.provision.load_log_format") as mock_load,
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"vcl_log_expression": "x", "collection_stage": "edge"},
        )

    assert resp.json()["format_length"] is None
    # We didn't call load_log_format because errors short-circuited
    mock_load.assert_not_called()


# ── POST /services/{id}/cron-settings (SSE) ────────────────────────────


def test_cron_settings_sse_emits_error_when_service_missing(client, tmp_path, monkeypatch):
    """Missing service → SSE error event with "Service not found".
    Pinned because the FE distinguishes "service was deleted" from
    "network error" — both produce 200 status, only one has an
    error message in the body."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/cron-settings",
        json={"cron_sync": {"enabled": False}},
    )
    assert resp.status_code == 200
    assert "Service not found" in resp.text


def test_cron_settings_sse_persists_allowed_keys_only(client, tmp_path, monkeypatch):
    """Body fields outside the allow-list are silently dropped.
    Pinned because losing the allow-list would let the FE inject
    arbitrary keys into the persisted config — a security concern
    if those keys are later read by other code paths."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/cron-settings",
            json={
                "cron_sync": {
                    "enabled": True,
                    "interval_mins": 5,
                    "evil_arbitrary_key": "should-be-dropped",
                }
            },
        )

    assert resp.status_code == 200
    assert "done" in resp.text

    fresh = config.load_config(MOCK_SERVICE_ID)
    cs = fresh["provisioning"]["cron_sync"]
    assert cs["enabled"] is True
    assert cs["interval_mins"] == 5
    # Arbitrary key dropped
    assert "evil_arbitrary_key" not in cs


def test_cron_settings_sse_warns_but_proceeds_when_crontab_sync_fails(client, tmp_path, monkeypatch):
    """If _sync_crontab raises after the config save, emit a warning
    status but still report "done". Pinned because the persistence
    succeeded — losing this would make the SSE error out and
    confuse the user about whether their settings stuck."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch("backend.provision._sync_crontab", side_effect=RuntimeError("scheduler busy")),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/cron-settings",
            json={"cron_sync": {"enabled": False}},
        )

    assert resp.status_code == 200
    text = resp.text
    assert "Cron sync failed" in text or "scheduler busy" in text
    # And the done event still fires (the config save succeeded)
    assert "done" in text


def test_cron_settings_sse_supports_all_three_cron_namespaces(client, tmp_path, monkeypatch):
    """Body can include cron_sync / cron_compact / cron_ngwaf — all
    three are merged. Pinned because losing any namespace would
    silently ignore the FE's per-namespace settings."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    with (
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        client.post(
            f"/api/services/{MOCK_SERVICE_ID}/cron-settings",
            json={
                "cron_sync": {"enabled": True, "interval_mins": 5},
                "cron_compact": {"enabled": True, "log_retention_days": 30},
                "cron_ngwaf": {"enabled": False, "interval_mins": 10},
            },
        )

    fresh = config.load_config(MOCK_SERVICE_ID)
    prov = fresh["provisioning"]
    assert prov["cron_sync"]["enabled"] is True
    assert prov["cron_compact"]["log_retention_days"] == 30
    assert prov["cron_ngwaf"]["enabled"] is False


# ── DELETE /services/{id}/time-range ──────────────────────────────────


def test_clear_time_range_404s_when_service_missing(client, tmp_path, monkeypatch):
    """Missing service → 404 (not 500). Pinned because the FE
    distinguishes "service deleted" from "transient API failure"."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    resp = client.delete(f"/api/services/{MOCK_SERVICE_ID}/time-range")
    assert resp.status_code == 404


def test_clear_time_range_returns_noop_message_when_not_set(client, tmp_path, monkeypatch):
    """If no time_range is set, return 200 with "No time_range was
    set." (not an error). Pinned because losing this would make
    re-clicking the "Clear time range" button surface a confusing
    error toast on the second click."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    resp = client.delete(f"/api/services/{MOCK_SERVICE_ID}/time-range")
    assert resp.status_code == 200
    assert "No time_range" in resp.json()["message"]


def test_clear_time_range_removes_from_provisioning_config(client, tmp_path, monkeypatch):
    """When time_range is set, DELETE removes it from
    `cfg.provisioning.time_range`. Pinned because the DuckDB view
    bound uses this saved range — losing the delete would silently
    keep the old range filter."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "provisioning": {"time_range": {"start": "2026-01-01", "end": "2026-01-31"}},
        },
    )

    with patch("backend.core.metadata.record_audit"):
        resp = client.delete(f"/api/services/{MOCK_SERVICE_ID}/time-range")

    assert resp.status_code == 200
    fresh = config.load_config(MOCK_SERVICE_ID)
    assert "time_range" not in fresh["provisioning"]


def test_update_custom_field_422s_on_type_change_when_field_in_iceberg_schema(client, tmp_path, monkeypatch):
    """Changing `duckdb_type` or `value_type` of a field that already
    exists in the Iceberg schema → 422 with a friendly "create a new
    field instead" message. Pinned because Iceberg's schema-evolution
    rules forbid type changes and a silent override would corrupt
    Parquet writes."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "groups": ["A"],
                "field_overrides": {},
                "custom_fields": [
                    {"name": "locked_field", "label": "x", "enabled": True, "duckdb_type": "VARCHAR"},
                ],
            },
        },
    )

    fake_table = MagicMock()
    fake_field = MagicMock()
    fake_field.name = "locked_field"
    fake_table.schema.return_value.fields = [fake_field]
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc"}),
        patch("backend.core.iceberg._get_catalog", return_value=fake_catalog),
        patch("backend.core.iceberg._table_identifier", return_value=("default", "logs")),
    ):
        resp = client.patch(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/locked_field",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"duckdb_type": "INTEGER"},  # Type change
        )

    assert resp.status_code == 422
    errors = resp.json()["detail"]["errors"]
    assert any("Cannot change" in e for e in errors)


# ── GET /cron-schedule cache busting ──────────────────────────────────────


def test_clear_cron_schedule_cache_service_specific():
    """clear_cron_schedule_cache with a service_id pops only that key."""
    from backend.routers.services.core import _cron_schedule_cache, clear_cron_schedule_cache

    _cron_schedule_cache["svc-1"] = (123.45, {"data": "foo"})
    _cron_schedule_cache["svc-2"] = (678.90, {"data": "bar"})

    clear_cron_schedule_cache("svc-1")

    assert "svc-1" not in _cron_schedule_cache
    assert "svc-2" in _cron_schedule_cache


def test_clear_cron_schedule_cache_all():
    """clear_cron_schedule_cache without a service_id clears all keys."""
    from backend.routers.services.core import _cron_schedule_cache, clear_cron_schedule_cache

    _cron_schedule_cache["svc-1"] = (123.45, {"data": "foo"})
    _cron_schedule_cache["svc-2"] = (678.90, {"data": "bar"})

    clear_cron_schedule_cache()

    assert len(_cron_schedule_cache) == 0


def test_cron_log_hooks_bust_cache(monkeypatch):
    """start_cron_run, log_cron_run, and finalize_cron_run_if_running bust the cache."""
    from backend.routers.services.core import _cron_schedule_cache

    _cron_schedule_cache["svc-1"] = (123.45, {"data": "foo"})

    # We mock out database and publisher side-effects so we can test the cron_log hooks
    # in complete isolation.
    with (
        patch("backend.core.metadata.cron_log.get_con"),
        patch("backend.core.metadata.cron_log._retry_on_locked", return_value=123),
        patch("backend.cron_runs_publisher.publisher.publish"),
    ):
        from backend.core.metadata.cron_log import finalize_cron_run_if_running, log_cron_run, start_cron_run

        # start_cron_run
        start_cron_run("svc-1", "sync")
        assert "svc-1" not in _cron_schedule_cache

        # Re-populate
        _cron_schedule_cache["svc-1"] = (123.45, {"data": "foo"})

        # log_cron_run
        log_cron_run("svc-1", "sync", 10.0, "success")
        assert "svc-1" not in _cron_schedule_cache

        # Re-populate
        _cron_schedule_cache["svc-1"] = (123.45, {"data": "foo"})

        # finalize_cron_run_if_running
        finalize_cron_run_if_running("svc-1", "sync", 123)
        assert "svc-1" not in _cron_schedule_cache
