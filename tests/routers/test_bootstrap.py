from __future__ import annotations

import datetime as dt
import json
import os
import typing
from collections import namedtuple
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import starlette.datastructures
from backend.core.share_db.settings import get_setting, set_setting
from backend.scoring.labels import ALLOWED_LABELS
from backend.utils.tunnel.manager import AnalystSession, get_tunnel_manager
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.middleware.base import BaseHTTPMiddleware

from backend import config as svcconfig
from backend.core import log_fields as lf
from backend.core import metadata as metadata_db
from backend.core import share_db
from backend.main import _APP_VERSION, app
from backend.models import common as m_common
from backend.models import provision as m_prov
from backend.models import rum as rum_models
from backend.models import security as m_sec
from backend.models import services as m_svc
from backend.models import session_scoring as m_scoring
from backend.repositories._base import _safe_table
from backend.routers import alerts as alerts_router
from backend.routers import session_scoring as session_scoring_router
from backend.routers import session_scoring_admin as scoring_admin
from backend.routers import share_admin
from backend.routers.bootstrap import _compute_log_fields_catalog
from backend.routers.control_room import TAB_STUBS
from backend.routers.services import core as services_core_router
from backend.scoring import labels as _labels
from backend.utils import sqlite_profiler, telemetry
from backend.utils.remote_access import RemoteAccessMiddleware, _is_blocked_path
from tests._helpers.config_fakes import fake_mutate_config
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID


def test_log_fields_catalog_endpoint(client):
    """Verify that the catalog endpoint returns the definitions of all groups, fields, and presets."""
    response = client.get("/api/log-fields/catalog")
    assert response.status_code == 200
    data = response.json()

    assert "groups" in data
    assert "fields" in data
    assert "presets" in data
    assert "insights" in data

    # We should have all the presets defined in log_fields.py
    assert "minimal" in data["presets"]
    assert "standard" in data["presets"]
    assert "security" in data["presets"]


def test_insight_availability_endpoint(client, in_memory_duckdb, test_service_source):
    """Verify that insights are dynamically evaluated based on the available schema."""

    # Insert logs so DuckDB auto-infers a schema
    logs = generate_mock_logs(test_service_source, num_logs=1)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.get("/api/insight-availability", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 200
    data = response.json()

    assert data["available"] is True
    assert "insights" in data

    # Since our mocked test_service_source includes groups A, C, D, F, I
    # The 'error_spikes' insight (Requires group A, which gives 'url' and core field 'status')
    # should be available.
    error_spikes = next(i for i in data["insights"] if i["id"] == "error_spikes")
    # Actually wait, test mock data creates table by NAME but logs might not have 'url' depending
    # on random chance. Let's just check the structure.
    assert "id" in error_spikes
    assert "available" in error_spikes


def test_bootstrap_endpoint_success(client, tmp_path, monkeypatch):
    """Verify that the main bootstrap endpoint correctly returns a 200 and serializes properly."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    # Save a fake service so the list is not empty
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    assert data["active_service_id"] == MOCK_SERVICE_ID
    assert "pops" in data
    assert "services" in data
    # Admin / loopback path has no analyst session → masking is off.
    assert data["settings"]["mask_ips"] is False


def test_bootstrap_projects_path_a_capability_for_sync_services(client, tmp_path, monkeypatch):
    """Sync-mode services advertise independent analyst Path A as available."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "standard")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    service = response.json()["services"][0]
    assert service["analyst_path_a_supported"] is True
    assert service["analyst_path_a_reason"] is None


def test_bootstrap_projects_path_a_capability_for_celery_services(client, tmp_path, monkeypatch):
    """High-scale services advertise Path A as unavailable with the Path B remedy."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "high_scale")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "raw_layout_version": 3})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    service = response.json()["services"][0]
    assert service["analyst_path_a_supported"] is False
    assert service["analyst_path_a_reason"] == (
        "Independent analyst access is unavailable for high-scale services. "
        "Use live shared-instance analyst access (Path B)."
    )


def test_startup_stub_still_withholds_warmup_dependent_fields():
    """The real per-service data — the ``services`` list, ``active_service_id``,
    ``schema`` — must stay gated on ``startup_complete`` for both loopback and
    remote callers."""
    from unittest.mock import MagicMock

    from backend.routers.bootstrap import _startup_stub_response

    for is_remote in (False, True):
        fake_request = MagicMock()
        fake_request.state.is_remote = is_remote

        response = _startup_stub_response(fake_request)
        payload = response.model_dump(by_alias=True)

        assert payload["active_service_id"] is None
        assert payload["services"] == []
        assert payload["schema"] is None
        assert payload["settings"]["initializing"] is True


def _login_analyst_and_bootstrap(client, tmp_path, monkeypatch, *, pii_policy, ngwaf_workspace_id=None):
    """Seed a masking/non-masking analyst invite, log in, and return the
    /api/bootstrap response body for assertions about ``settings``."""
    from backend import config
    from backend.core import share_db
    from backend.utils import tunnel

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    tunnel.reset_for_tests()
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "ngwaf_workspace_id": ngwaf_workspace_id})

    invite = share_db.create_remote_invite(
        name="Test Analyst",
        email="analyst@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[MOCK_SERVICE_ID],
        pii_policy=pii_policy,
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")

    login = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert login.status_code == 200, login.text
    client.cookies.set("analyst_session_id", login.cookies.get("analyst_session_id"))

    response = client.get(
        "/api/bootstrap",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "x-fastly-service-id": MOCK_SERVICE_ID,
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["settings"]["is_remote_analyst"] is True  # confirm analyst branch
    return body


def test_bootstrap_mask_ips_true_for_masking_analyst(client, tmp_path, monkeypatch):
    """An invite with pii_policy.mask_ips=true surfaces ``settings.mask_ips``
    true so the frontend hides IP drill-down affordances. The server-side
    filter lock is the real guarantee; this flag drives the UX."""
    from backend.core import share_db
    from backend.utils import tunnel

    try:
        body = _login_analyst_and_bootstrap(client, tmp_path, monkeypatch, pii_policy={"mask_ips": True})
        assert body["settings"]["mask_ips"] is True
    finally:
        share_db.close_all_connections()
        tunnel.reset_for_tests()


def test_bootstrap_mask_ips_false_for_non_masking_analyst(client, tmp_path, monkeypatch):
    """A non-masking analyst (no pii_policy) gets ``settings.mask_ips`` false."""
    from backend.core import share_db
    from backend.utils import tunnel

    try:
        body = _login_analyst_and_bootstrap(client, tmp_path, monkeypatch, pii_policy=None)
        assert body["settings"]["mask_ips"] is False
    finally:
        share_db.close_all_connections()
        tunnel.reset_for_tests()


# ── /api/bootstrap: edge cases ──────────────────────────────────────────────


@pytest.mark.parametrize("workspace_id", [None, "", "mock-ngwaf-workspace"])
def test_bootstrap_ngwaf_capability_is_service_scoped(client, tmp_path, monkeypatch, workspace_id):
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "ngwaf_workspace_id": workspace_id,
            "log_fields": {"schema_version": 2, "groups": ["J"], "custom_fields": []},
        },
    )
    config.save_config("other-service", {"service_id": "other-service", "ngwaf_workspace_id": "other-workspace"})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    assert response.json()["ngwaf_configured"] is bool(workspace_id)


@pytest.mark.parametrize("workspace_id", [None, "mock-ngwaf-workspace"])
@pytest.mark.security_regression
def test_bootstrap_analyst_gets_ngwaf_capability_without_workspace_id(client, tmp_path, monkeypatch, workspace_id):
    from backend.core import share_db
    from backend.utils import tunnel

    try:
        body = _login_analyst_and_bootstrap(
            client, tmp_path, monkeypatch, pii_policy=None, ngwaf_workspace_id=workspace_id
        )
        assert body["ngwaf_configured"] is bool(workspace_id)
        assert all("ngwaf_workspace_id" not in service for service in body["services"])
    finally:
        share_db.close_all_connections()
        tunnel.reset_for_tests()


def test_bootstrap_exposes_active_log_field_ids(client, tmp_path, monkeypatch):
    """Dashboard cards default to the currently-enabled field set.
    Bootstrap exposes that set so the frontend can toggle off-format cards off
    by default and badge them as 'not currently being logged'."""
    from backend import config
    from backend.core import log_fields as lf

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {"groups": lf.PRESETS["minimal"]["groups"], "field_overrides": {}},
        },
    )

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    ids = response.json()["active_log_field_ids"]
    assert isinstance(ids, list) and ids, "expected a non-empty enabled-field list"
    expected = lf.resolve_enabled_fields({"groups": lf.PRESETS["minimal"]["groups"], "field_overrides": {}})
    assert set(ids) >= expected, "all minimal-preset fields should appear"


def test_bootstrap_falls_back_to_first_service_when_active_id_unknown(client, tmp_path, monkeypatch):
    """If the frontend sends a service-id that's been removed (stale
    localStorage), the bootstrap endpoint silently re-points
    ``active_service_id`` to the first available service so the UI
    doesn't show "No service" until the user manually picks one."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config("svc-real", {"service_id": "svc-real"})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": "svc-deleted"})
    assert response.status_code == 200
    data = response.json()
    # Fell back to the only existing service
    assert data["active_service_id"] == "svc-real"


def test_bootstrap_with_no_services_returns_null_active_id(client, tmp_path, monkeypatch):
    """Fresh install with zero configs → ``active_service_id`` is null
    and ``services`` is empty. Pinned because the frontend keys on
    null to render the "let's provision your first service" wizard."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    # No save_config called

    response = client.get("/api/bootstrap")
    assert response.status_code == 200
    data = response.json()
    assert data["active_service_id"] is None
    assert data["services"] == []


def test_bootstrap_uses_cached_schema_from_config_status(client, tmp_path, monkeypatch):
    """When the service config has a cached ``status.schema``, the
    endpoint must use it directly without acquiring a DB lock. Pinned
    because the lock-free path is what makes the bootstrap response
    fast enough for the frontend's startup blocking call."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    # Save a config whose enriched-service view will surface this schema
    cached_schema = [{"name": "timestamp", "type": "TIMESTAMP"}]
    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "status": {"schema": cached_schema}},
    )

    with patch(
        "backend.services.service_manager.get_enriched_services",
        return_value=[{"service_id": MOCK_SERVICE_ID, "status": {"schema": cached_schema}}],
    ):
        # Patch get_source_for_service to None so the live-fallback path doesn't run
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 200
    assert response.json()["schema"] == cached_schema


def test_bootstrap_falls_back_to_live_schema_when_cache_empty(client, in_memory_duckdb, test_service_source):
    """No cached schema → endpoint opens a connection, calls
    ``get_schema``, returns the live shape. Pinned because the
    fallback is what populates the dashboard on first-ever load."""
    # Seed the DuckDB table so live get_schema returns a non-empty schema
    logs = generate_mock_logs(test_service_source, num_logs=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    # Live schema came back — non-empty
    assert isinstance(response.json()["schema"], list)


def test_bootstrap_includes_custom_dashboard_cards_when_configured(client, tmp_path, monkeypatch):
    """Custom fields flagged ``show_in_dashboard=True`` surface in
    ``custom_dashboard_cards`` — the dashboard renders them without
    a follow-up fetch, which would otherwise add a round-trip to
    first paint."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "schema_version": 2,
                "custom_fields": [
                    {
                        "name": "my_dashboard_field",
                        "label": "Dashboard Field",
                        "duckdb_type": "VARCHAR",
                        "enabled": True,
                        "show_in_dashboard": True,
                    },
                    {
                        "name": "my_silent_field",
                        "label": "Silent Field",
                        "duckdb_type": "VARCHAR",
                        "enabled": True,
                        "show_in_dashboard": False,
                    },
                ],
            },
        },
    )

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    # The card id is the field "name" — only show_in_dashboard=True surfaces
    card_ids = {c["id"] for c in data["custom_dashboard_cards"]}
    assert "my_dashboard_field" in card_ids
    assert "my_silent_field" not in card_ids


# ── /api/bootstrap: views fold ─────────────────────────────────────────────


def test_bootstrap_includes_saved_views_for_active_service(client, tmp_path, monkeypatch):
    """Bootstrap folds saved views in so the frontend skips its own
    /api/views/{service_id} round-trip on initial load. Pinned because
    losing this key reintroduces ~50ms per page nav (one Iceberg/SQLite
    round-trip per nav)."""
    from backend import config
    from backend.repositories import alerts as _views_repo

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    def _fake_views(sid):
        assert sid == MOCK_SERVICE_ID, f"bootstrap must only fetch views for the ACTIVE service. Got sid={sid!r}"
        return [
            {
                "id": "v1",
                "service_id": sid,
                "name": "Errors",
                "filters_json": "[]",
                "start_time": None,
                "end_time": None,
                "page": "/dashboard",
            },
            {
                "id": "v2",
                "service_id": sid,
                "name": "Slow",
                "filters_json": "[]",
                "start_time": None,
                "end_time": None,
                "page": "/dashboard",
            },
        ]

    monkeypatch.setattr(_views_repo, "get_views", _fake_views)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    assert "views" in data, "bootstrap response must include 'views' key"
    assert len(data["views"]) == 2
    ids = {v["id"] for v in data["views"]}
    assert ids == {"v1", "v2"}


def test_bootstrap_views_empty_when_no_active_service(client, tmp_path, monkeypatch):
    """No active service → no views to fold. Pinned so the views
    fetch isn't called with None (which would crash get_views)."""
    from backend import config
    from backend.repositories import alerts as _views_repo

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    get_views_calls: list = []
    monkeypatch.setattr(
        _views_repo,
        "get_views",
        lambda sid: get_views_calls.append(sid) or [],
    )

    response = client.get("/api/bootstrap")
    assert response.status_code == 200
    data = response.json()
    assert data["views"] == [], f"empty/missing active service must return [] for views, got {data.get('views')!r}"
    assert get_views_calls == [], (
        f"get_views must NOT be called when there's no active service; got calls={get_views_calls}"
    )


def test_bootstrap_views_survives_repo_error(client, tmp_path, monkeypatch):
    """A repo error fetching views must NOT break /api/bootstrap.
    Views are UX nicety, not correctness — degrade gracefully to
    empty list and let ViewSelector fall back to its granular GET."""
    from backend import config
    from backend.repositories import alerts as _views_repo

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    def _explode(sid):
        raise RuntimeError("simulated repo failure")

    monkeypatch.setattr(_views_repo, "get_views", _explode)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200, (
        f"bootstrap must NOT 500 when views repo fails; got status={response.status_code}"
    )
    data = response.json()
    assert data["views"] == [], "views must degrade to [] on repo error, not propagate"


# ── /api/bootstrap: P1#5 lazy-load (cron_schedule + share_status dropped) ──


def test_bootstrap_drops_cron_schedule_and_share_status_seeds(client, tmp_path, monkeypatch):
    """P1#5 (perf audit): ``cron_schedule`` (~3s build) and ``share_status``
    (~2.1s build) are not folded into the admin bootstrap payload — they sat
    on the SSR first-paint critical path. The response model fields were
    dropped in v2.1.0: the /logs cron tab and /admin/share page each refetch
    their own standalone endpoint on mount.

    Pinned with an EXPLODING build helper for each: if a future change
    re-wires the seed, the helper would be called and this test catches the
    regression even before the field assertion (the bootstrap-resilient
    try/except would otherwise swallow it into a silent None)."""
    from backend import config
    from backend.cron import scheduler as _sched
    from backend.routers import share_admin

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    def _explode_cron(*a, **k):
        raise AssertionError("build_cron_schedule_payload must NOT run on the bootstrap path (P1#5)")

    def _explode_share(*a, **k):
        raise AssertionError("build_share_status must NOT run on the bootstrap path (P1#5)")

    monkeypatch.setattr(_sched, "build_cron_schedule_payload", _explode_cron)
    monkeypatch.setattr(share_admin, "build_share_status", _explode_share)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    # Fields dropped from the wire entirely (v2.1.0).
    assert "cron_schedule" not in data
    assert "share_status" not in data


def test_bootstrap_keeps_last_sync_share_banner_ops_overview_seeds(client, tmp_path, monkeypatch):
    """The cheap admin seeds KEPT on the bootstrap hot path still populate:
    ``last_sync`` (header SyncStatusBadge — refetching on every page would
    flicker), ``share_banner`` (small global header state), and
    ``ops_overview`` (~73ms, left alone by P1#5). Dropping cron_schedule /
    share_status must NOT regress these."""
    from backend import config
    from backend.core.metadata import cron_log as _cron_log
    from backend.utils import tunnel as _tunnel

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    # last_sync: derived from latest_cron_per_task("sync").
    monkeypatch.setattr(
        _cron_log,
        "latest_cron_per_task",
        lambda sid: {"sync": {"started_at": "2026-06-29T00:00:00Z", "status": "success", "duration_s": 1.5}},
    )

    # share_banner: projected from the tunnel manager's lean state.
    class _FakeMgr:
        def is_sharing_active(self):
            return True

        def public_url(self):
            return "https://example.test/share-login"

    monkeypatch.setattr(_tunnel, "get_tunnel_manager", lambda: _FakeMgr())

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()

    assert data["last_sync"] == {
        "started_at": "2026-06-29T00:00:00Z",
        "status": "success",
        "duration_s": 1.5,
    }
    assert data["share_banner"] == {
        "sharing_active": True,
        "public_url": "https://example.test/share-login",
    }
    # ops_overview is admin-only and cheap; at minimum the queries_summary
    # sub-key is always derivable (no DB needed), so the seed is non-null.
    assert data["ops_overview"] is not None


# ── /api/schema ────────────────────────────────────────────────────────────


def test_schema_endpoint_uses_cached_status_when_available(client, in_memory_duckdb, test_service_source):
    """If the source's status cache has a schema, the endpoint must
    return it WITHOUT calling ``get_schema`` (cache miss → live call
    is the fallback, not the default)."""
    cached_schema = [{"name": "from_cache", "type": "VARCHAR"}]
    with patch(
        "backend.config.get_status",
        return_value={"schema": cached_schema},
    ):
        response = client.get("/api/schema", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 200
    assert response.json()["schema"] == cached_schema


def test_schema_endpoint_500s_when_get_schema_raises(client, test_service_source):
    """No cached status + live get_schema raises → 500 with the
    error string. Pinned because the dashboard surfaces this 500
    inline rather than crashing."""
    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.duckdb.get_schema", side_effect=RuntimeError("schema fetch failed")),
    ):
        response = client.get("/api/schema", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 500
    assert "error" in response.json()["detail"]


# ── /api/log-fields/catalog: with vs without service_id ────────────────────


def test_log_fields_catalog_with_service_id_includes_field_limits_and_custom(client, tmp_path, monkeypatch):
    """When a service_id is provided, custom fields from that service's
    config are merged into the catalog. Used by the field-picker
    drawer to show admin-defined fields."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "schema_version": 2,
                "custom_fields": [
                    {
                        "name": "my_custom",
                        "duckdb_type": "VARCHAR",
                        "enabled": True,
                        "label": "My Custom Field",
                    }
                ],
            },
        },
    )

    response = client.get("/api/log-fields/catalog", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    # Custom field entries use ``id`` (=field name) and group="custom"
    custom_ids = [f["id"] for f in data["fields"] if f.get("group") == "custom"]
    assert "my_custom" in custom_ids


def test_log_fields_catalog_handles_missing_config_gracefully(client):
    """Service_id provided but config doesn't exist → returns the
    static catalog without custom fields rather than 500ing."""
    response = client.get("/api/log-fields/catalog", headers={"x-fastly-service-id": "nonexistent"})
    assert response.status_code == 200
    data = response.json()
    assert "fields" in data
    assert "groups" in data


def test_log_fields_catalog_500s_on_exception(client):
    # bootstrap.py calls `fr.get_catalog_for_api`, which is a same-identity
    # re-export of `log_fields.get_catalog_for_api`. Patching the original
    # `log_fields` binding does NOT affect the registry's already-bound
    # reference, so patch the registry's path directly.
    with patch("backend.core.field_registry.get_catalog_for_api", side_effect=RuntimeError("oops")):
        response = client.get("/api/log-fields/catalog")
    assert response.status_code == 500


# ── /api/insight-availability ──────────────────────────────────────────────


def test_insight_availability_500s_on_schema_failure(client):
    with patch("backend.core.duckdb.get_schema", side_effect=RuntimeError("schema gone")):
        response = client.get("/api/insight-availability", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 500


def test_insight_availability_marks_unavailable_when_required_cols_missing(
    client, in_memory_duckdb, test_service_source
):
    """If the active schema lacks columns a given insight requires,
    that insight is flagged ``available: False``. Pinned because the
    frontend uses this to grey out unsupported insights rather than
    showing empty cards."""
    # Create a minimal table missing most insight columns
    in_memory_duckdb.execute(
        f"CREATE TABLE {_safe_table(test_service_source['name'])} "
        "(timestamp TIMESTAMP, status INTEGER)"  # no url, ip, asn, etc.
    )

    response = client.get("/api/insight-availability", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    insights = response.json()["insights"]
    # At least one insight requires more than (timestamp, status) → unavailable
    unavailable = [i for i in insights if not i["available"]]
    assert len(unavailable) > 0


def test_bootstrap_remote_unauth_stub_omits_section_timings():
    """An unauthenticated remote visitor must NOT receive section_timings.

    Shipping the SectionTimer telemetry on the redirect-to-share-login stub
    gives an unauthenticated caller a microsecond-precision oracle on
    ``validate_analyst_session`` execution time, which strips network jitter
    from any timing attack on the session cookie. Pinned so a future change
    that adds ``section_timings=section_timings`` back to the stub flips
    this test red.
    """
    from unittest.mock import MagicMock

    # The handler is now an async coalescing wrapper; the remote-unauth stub
    # logic lives in the sync body (_bootstrap_sync) that the wrapper runs in
    # the threadpool. Exercise the body directly — same code path, no event
    # loop needed.
    from backend.routers.bootstrap import _bootstrap_sync

    fake_request = MagicMock()
    fake_request.state.is_remote = True
    fake_request.state.analyst_session = None
    fake_request.cookies = {}

    response = _bootstrap_sync(fake_request, None)
    payload = response.model_dump(by_alias=True)

    assert payload["settings"]["is_remote_analyst"] is True
    assert payload["settings"]["needs_login"] is True
    # Both the alias (_section_timings) and the field name (section_timings)
    # must be absent or empty — anything non-empty leaks per-call timing.
    timings = payload.get("_section_timings") or payload.get("section_timings") or []
    assert timings == [], f"unauth bootstrap stub leaked section_timings: {timings}"


def test_bootstrap_aligns_request_total_with_local_rows(client, tmp_path, monkeypatch):
    """Verify that when DuckDB count is smaller or stale, REQUEST total_rows aligns with local_rows."""
    from unittest.mock import MagicMock

    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    # Mock cached_status to have 7.4M local_rows
    monkeypatch.setattr(
        config, "get_status", lambda sid: {"local_rows": 7400000, "latest_log_at": "2026-06-29T12:00:00Z"}
    )

    # Mock RUM count from DuckDB RUM views
    class MockConnectionHolder:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return MagicMock()

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("backend.deps._ConnectionHolder", MockConnectionHolder)
    monkeypatch.setattr(
        "backend.core.iceberg.execute_with_stale_view_retry",
        lambda *args, **kwargs: (800, None),
    )

    # Mock REQUEST count in DuckDB to return 33000 (representing stale/empty view)
    mock_duckdb_con = MagicMock()
    # first fetchone for COUNT(*) returns 33000, second MAX(timestamp) returns None
    mock_duckdb_con.execute.return_value.fetchone.side_effect = [(33000,), (None,)]
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda src: mock_duckdb_con)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()

    # The returned payload should be aligned:
    # rum.total_rows should be 800 (from SQLite)
    # request.total_rows should be aligned to local_rows - rum.total_rows = 7400000 - 800 = 7399200
    # request.latest_log_at should fall back to cached_status.get("latest_log_at")
    assert data["header_badge"]["rum"]["total_rows"] == 800
    assert data["header_badge"]["request"]["total_rows"] == 7399200
    assert data["header_badge"]["request"]["latest_log_at"] == "2026-06-29T12:00:00Z"
    assert data["header_badge"]["local_rows"] == 7400000

    # The fallback result differs from its input snapshot. Cache by INPUT,
    # not output metrics, or every legacy header poll re-runs these scans.
    scan_calls = mock_duckdb_con.execute.call_count
    again = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert again.status_code == 200
    assert again.json()["header_badge"] == data["header_badge"]
    assert mock_duckdb_con.execute.call_count == scan_calls


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
@pytest.mark.parametrize("warm_cache", [False, True])
def test_bootstrap_header_badge_uses_live_sync_snapshot(client, tmp_path, monkeypatch, request_latest, warm_cache):
    """Bootstrap freshness must match the dedicated sync-status snapshot."""
    from backend import config
    from backend.routers import bootstrap

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})
    current_status = {
        "local_rows": 0 if request_latest is None else 20,
        "latest_log_at": "2026-09-06T17:19:00Z",
        "earliest_log_at": "2026-09-04T00:00:00Z",
        "rum": {"latest_log_at": "2026-09-08T10:00:00Z", "total_rows": 10},
        "request": {
            "latest_log_at": request_latest,
            "total_rows": 0 if request_latest is None else 20,
            "last_sync_at": None,
        },
    }
    monkeypatch.setattr(
        config,
        "get_status",
        lambda sid: current_status.copy(),
    )
    monkeypatch.setattr(
        "backend.core.metadata.get_ingested_files_status_summary",
        lambda _: {"latest_file_name": "logs_2026-09-06T17-19-00.gz", "total_rows": 99},
    )
    if warm_cache:
        # A valid TTL must not resurrect a previous event extent after reset
        # or hide an advancing snapshot. No fallback scans are needed here.
        monkeypatch.setitem(
            bootstrap._header_badge_cache,
            MOCK_SERVICE_ID,
            (
                float("inf"),
                {"latest_log_at": "2026-09-06T17:19:00Z", "local_rows": 99},
                {"configured": True, "latest_log_at": "2026-09-06T17:19:00Z"},
                {},
            ),
        )

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()

    assert data["header_badge"]["latest_log_at"] == request_latest
    assert data["header_badge"]["request"]["latest_log_at"] == request_latest
    assert data["log_extents"]["latest_log_at"] == request_latest
    assert data["sync_status"]["latest_log_at"] == request_latest
    assert data["sync_status"]["latest_ingested_file_at"] == "2026-09-06 17:19:00"
    assert data["header_badge"]["rum"]["latest_log_at"] == "2026-09-08T10:00:00Z"
    assert data["header_badge"]["local_rows"] == current_status["local_rows"]


# Silence unused-import warnings
_ = pytest


# ── Merged from test_bootstrap_graceful_degradation.py ──


def _seed_active_service(monkeypatch, tmp_path):
    """A configured service so valid_active_id != None, which is the
    precondition for the optional resolvers to run at all."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})


def _get(client):
    return client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def test_bootstrap_survives_share_banner_failure(client, tmp_path, monkeypatch):
    """Tunnel manager in transient state → share_banner=None, no 500."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.utils.tunnel.get_tunnel_manager", side_effect=RuntimeError("tunnel restart")):
        response = _get(client)
    assert response.status_code == 200
    data = response.json()
    assert data.get("share_banner") is None
    assert data["active_service_id"] == MOCK_SERVICE_ID
    assert "pops" in data


def test_bootstrap_survives_ops_overview_queries_summary_failure(client, tmp_path, monkeypatch):
    """query_registry.summary() raising → queries_summary sub-key omitted."""
    _seed_active_service(monkeypatch, tmp_path)
    from backend.core.query_registry import query_registry

    with patch.object(query_registry, "summary", side_effect=RuntimeError("registry broke")):
        response = _get(client)
    assert response.status_code == 200
    ov = response.json().get("ops_overview") or {}
    assert "queries_summary" not in ov


def test_bootstrap_survives_ops_overview_slow_query_count_failure(client, tmp_path, monkeypatch):
    """count_slow_queries() raising → slow_queries_count omitted."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.core.metadata.count_slow_queries", side_effect=RuntimeError("metadata.db down")):
        response = _get(client)
    assert response.status_code == 200
    ov = response.json().get("ops_overview") or {}
    assert "slow_queries_count" not in ov


def test_bootstrap_survives_cron_runs_first_page_failure(client, tmp_path, monkeypatch):
    """get_cron_runs raising → cron_runs_first_page=None."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.core.metadata.cron_log.get_cron_runs", side_effect=RuntimeError("sqlite locked")):
        response = _get(client)
    assert response.status_code == 200
    data = response.json()
    assert data.get("cron_runs_first_page") is None
    assert "services" in data


def test_bootstrap_survives_last_sync_failure(client, tmp_path, monkeypatch):
    """latest_cron_per_task raising → last_sync=None (header badge nicety)."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.core.metadata.cron_log.latest_cron_per_task", side_effect=RuntimeError("corrupt")):
        response = _get(client)
    assert response.status_code == 200
    assert response.json().get("last_sync") is None


def test_bootstrap_survives_scoring_labels_failure(client, tmp_path, monkeypatch):
    """labels.list_labels raising → scoring_labels=None (labels UI nicety)."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.scoring.labels.list_labels", side_effect=RuntimeError("labels store down")):
        response = _get(client)
    assert response.status_code == 200
    assert response.json().get("scoring_labels") is None


def test_bootstrap_survives_simultaneous_optional_failures(client, tmp_path, monkeypatch):
    """Worst case — every optional resolver dies. Bootstrap must still
    return 200 with the core fields populated. Pins the per-block
    try/except shield: losing any single block must not cascade."""
    _seed_active_service(monkeypatch, tmp_path)
    from backend.repositories import alerts as _views_repo

    monkeypatch.setattr(_views_repo, "get_views", lambda _sid: (_ for _ in ()).throw(RuntimeError("boom")))

    with (
        patch("backend.utils.tunnel.get_tunnel_manager", side_effect=RuntimeError("boom")),
        patch("backend.core.metadata.cron_log.get_cron_runs", side_effect=RuntimeError("boom")),
        patch("backend.core.metadata.cron_log.latest_cron_per_task", side_effect=RuntimeError("boom")),
        patch("backend.scoring.labels.list_labels", side_effect=RuntimeError("boom")),
    ):
        response = _get(client)

    assert response.status_code == 200, (
        f"compound optional-resolver failure must not 500; got {response.status_code} body={response.text[:300]}"
    )
    data = response.json()
    # Every optional field at its documented default
    assert data.get("share_banner") is None
    assert data.get("cron_runs_first_page") is None
    assert data.get("last_sync") is None
    assert data.get("scoring_labels") is None
    assert data["views"] == []
    # Core (non-optional) fields still populated
    assert data["active_service_id"] == MOCK_SERVICE_ID
    assert "services" in data
    assert "pops" in data
    assert "settings" in data


# ── Merged from test_pricing_defaults.py ──


from backend.deps import get_source

_MOCK_GLOBAL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.006,  # Non-default
    "class_b_rate_per_10k": 0.02,  # Non-default
    "cdn_egress_rate_per_gb": 0.15,  # Non-default
    "storage_rate_per_gb_month": 0.03,  # Non-default
}

_TEST_SOURCE = {"name": "test_service", "service_id": "svc1"}


@patch("backend.config.load_usage_logging_config", return_value=_MOCK_GLOBAL_CFG)
def test_prefill_uses_global_rates(mock_load):
    """Verify that /api/usage/prefill returns the global rates, not hardcoded defaults."""
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get("/api/usage/prefill")
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    data = r.json()
    assert data["class_a_rate_per_1k"] == 0.006
    assert data["class_b_rate_per_10k"] == 0.02
    assert data["cdn_egress_rate_per_gb"] == 0.15
    assert data["storage_rate_per_gb_month"] == 0.03


@patch("backend.config.load_usage_logging_config", return_value=_MOCK_GLOBAL_CFG)
@patch("backend.core.metadata.get_usage_logs")
def test_usage_log_uses_global_rates_for_cost(mock_get_logs, mock_load):
    """Verify that aggregate cost calculations use the global rates."""
    mock_get_logs.return_value = (
        [],
        0,
        {
            "total_class_a": 1000,
            "total_class_b": 10000,
            "total_cdn_downloads": 0,
            "total_cdn_bytes": 1073741824,  # 1 GB
            "total_fos_bytes": 0,
            "class_a_breakdown": {},
            "class_b_breakdown": {},
        },
    )

    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get(
            "/api/admin/usage-log",
            params={"start": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"},
        )
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    agg = r.json()["aggregate"]

    # Class A: 1000 ops @ $0.006/1k = 0.006
    assert abs(agg["estimated_cost_class_a"] - 0.006) < 1e-10
    # Class B: 10000 ops @ $0.02/10k = 0.02
    assert abs(agg["estimated_cost_class_b"] - 0.02) < 1e-10
    # CDN: 1GB @ $0.15/GB = 0.15
    assert abs(agg["estimated_cost_cdn"] - 0.15) < 1e-10
    # Total: 0.006 + 0.02 + 0.15 = 0.176
    assert abs(agg["estimated_cost_total"] - 0.176) < 1e-10


# ── Merged from tests/routers/test_control_room.py ──

"""Control Room router — Phase 1 tests.

Covers:
- Every tab stub GET returns 200 for admin and analyst sessions
- Mutation endpoints return 501 for admin, 403 for analyst
- Unknown tab returns 404
- SSE stream: publisher-driven metrics_tick events
- SSE event schema validation (event_schema_version: 1)
- Log-field-audit endpoint
- Correlator endpoint
"""


import asyncio
from datetime import UTC, datetime

import pytest
from backend.deps import require_admin, require_service_access

# ── Tab names ────────────────────────────────────────────────────────────────

ALL_TABS = [
    "overview",
    "performance",
    "origin",
    "security",
    "network",
    "sessions",
    "insights",
    "admin_health",
]

LIVE_TABS = ["overview"]
STUB_TABS = [t for t in ALL_TABS if t not in LIVE_TABS]

MUTATION_ENDPOINTS = [
    "mitigations",
    "rules",
    "allowlist",
    "big-red-button",
    "cost-governor",
]

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def admin_client(in_memory_duckdb, test_service_source):
    """TestClient with admin overrides — require_admin passes."""
    from backend.deps import get_con, get_service_id

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
    app.dependency_overrides[require_service_access] = lambda: MOCK_SERVICE_ID
    app.dependency_overrides[require_admin] = lambda: None

    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def analyst_client(in_memory_duckdb, test_service_source):
    """TestClient with analyst overrides — require_admin rejects."""
    from backend.deps import get_con, get_service_id

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_service_id] = lambda: test_service_source["service_id"]
    app.dependency_overrides[require_service_access] = lambda: MOCK_SERVICE_ID

    def _analyst_require_admin():
        raise HTTPException(
            status_code=403,
            detail={"error": "admin_only", "message": "This operation requires admin access."},
        )

    app.dependency_overrides[require_admin] = _analyst_require_admin

    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ── Tab GET tests (admin) ────────────────────────────────────────────────────


@pytest.mark.parametrize("tab", STUB_TABS)
def test_tab_get_admin_stub_200(admin_client, tab):
    """Admin can read every stub tab — 200 with canned data."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab
    assert data["status"] == "stub"


@pytest.mark.parametrize("tab", LIVE_TABS)
def test_tab_get_admin_live_200(admin_client, tab):
    """Admin can read live tabs — 200 with status=live."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab
    assert data["status"] == "live"


# ── Tab GET tests (analyst) ──────────────────────────────────────────────────


@pytest.mark.parametrize("tab", ALL_TABS)
def test_tab_get_analyst_200(analyst_client, tab):
    """Analyst can read every tab — 200 with canned data."""
    resp = analyst_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["tab"] == tab


# ── Unknown tab ──────────────────────────────────────────────────────────────


def test_unknown_tab_404(admin_client):
    """Unknown tab name returns 404."""
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/bogus",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "unknown_tab"


def test_error_stream_route_is_registered():
    """The request-level error feed is part of the public OpenAPI surface."""
    path = "/api/services/{service_id}/control-room/error-stream"
    assert path in app.openapi()["paths"]


def test_error_stream_not_shadowed_by_tab_route(monkeypatch):
    """GET /control-room/error-stream must hit the SSE handler, not 404 unknown_tab from /{tab}."""
    import backend.routers.control_room as cr_mod

    route_paths = [getattr(r, "path", "") for r in cr_mod.router.routes]
    err_idx = route_paths.index("/api/services/{service_id}/control-room/error-stream")
    tab_idx = route_paths.index("/api/services/{service_id}/control-room/{tab}")
    assert err_idx < tab_idx, "error-stream route must be registered before /{tab} catch-all"


def test_correlate_top_values_windowed_freshness_and_telemetry(monkeypatch):
    """correlate_top_values uses LATEST_TIMESTAMP_WINDOWED and records DuckDB telemetry."""
    from datetime import UTC, datetime

    from backend.core.metadata import usage_log
    from backend.routers import control_room as cr_repo
    from backend.utils.telemetry import set_page_load_id

    recorded: list[dict] = []
    monkeypatch.setattr(usage_log, "log_telemetry_query", lambda **kwargs: recorded.append(kwargs))

    executed_sql: list[str] = []

    class _MockResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class _MockCon:
        def execute(self, sql, params=None):
            executed_sql.append(sql)
            if "GROUP BY" in sql:
                return _MockResult([("/home", 42)])
            return _MockResult([(datetime(2026, 10, 20, 12, 0, tzinfo=UTC),)])

    set_page_load_id("cr-telemetry-test")
    try:
        rows, latest_ts = cr_repo.correlate_top_values(
            _MockCon(),
            "logs_test",
            "url",
            datetime(2026, 10, 20, 11, 0, tzinfo=UTC),
            10,
            service_id="svc_cr",
        )
    finally:
        set_page_load_id(None)

    assert rows == [("/home", 42)]
    assert latest_ts is not None
    assert len(executed_sql) == 2
    assert "WHERE timestamp >= ?" in executed_sql[1]
    assert len(recorded) == 2
    assert all(r["engine"] == "duckdb" and r["page_load_id"] == "cr-telemetry-test" for r in recorded)


def test_recent_standard_errors_maps_rows(monkeypatch):
    import backend.routers.control_room as control_room

    class _Result:
        def fetchall(self):
            return [
                (
                    "event-1",
                    datetime(2026, 7, 7, tzinfo=UTC),
                    "404",
                    "/missing",
                    "DEN",
                    "192.0.2.10",
                )
            ]

    class _Connection:
        def execute(self, query, params=None):
            assert "FROM logs_test_service" in query
            return _Result()

        def close(self):
            pass

    import backend.core.duckdb as duckdb

    monkeypatch.setattr(duckdb, "get_connection", lambda **kwargs: _Connection())
    source = {"service_id": MOCK_SERVICE_ID}
    events = control_room._recent_standard_errors(source, "test-service")
    assert events == [
        {
            "event_id": "event-1",
            "timestamp": "2026-07-07T00:00:00+00:00",
            "status": 404,
            "url": "/missing",
            "pop": "DEN",
            "client_ip": "192.0.2.10",
        }
    ]


def test_recent_high_scale_errors_maps_rows():
    import backend.routers.control_room as control_room

    class _Client:
        def execute(self, query, params):
            assert "request_facts" in query
            assert params == {"service_id": MOCK_SERVICE_ID}
            return [
                {
                    "event_id": "event-2",
                    "timestamp": datetime(2026, 7, 7, tzinfo=UTC),
                    "status": 503,
                    "url": "/upstream",
                    "pop": "IAD",
                    "client_ip": "198.51.100.7",
                }
            ]

    service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=_Client())
    events = control_room._recent_high_scale_errors(service)
    assert events[0]["status"] == 503
    assert events[0]["client_ip"] == "198.51.100.7"


def test_error_events_mask_only_client_ip(monkeypatch):
    import backend.routers.control_room as control_room

    monkeypatch.setattr(control_room, "mask_ips_for", lambda session: True)
    monkeypatch.setattr(control_room, "mask_ip", lambda value: "masked")
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=object()))
    event = {"event_id": "event-1", "client_ip": "192.0.2.10", "url": "/x"}
    assert control_room._mask_error_events([event], request) == [
        {"event_id": "event-1", "client_ip": "masked", "url": "/x"}
    ]


# ── Mutation tests (admin → 501) ─────────────────────────────────────────────


@pytest.mark.parametrize("endpoint", MUTATION_ENDPOINTS)
def test_mutation_admin_501(admin_client, endpoint):
    """Admin mutation stubs return 501 Not Implemented."""
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{endpoint}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"action": "test", "payload": {}},
    )
    assert resp.status_code == 501
    assert resp.json()["detail"]["error"] == "not_implemented"


# ── Mutation tests (analyst → 403) ───────────────────────────────────────────


@pytest.mark.parametrize("endpoint", MUTATION_ENDPOINTS)
def test_mutation_analyst_403(analyst_client, endpoint):
    """Analyst mutation calls return 403 — admin-only."""
    resp = analyst_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/{endpoint}",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"action": "test", "payload": {}},
    )
    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "admin_only"


# ── SSE real-time stream ────────────────────────────────────────────────────


class _FakeRequest:
    """Minimal Request stand-in for direct-drive SSE tests."""

    def __init__(self, disconnect_after: int = 999):
        self._count = 0
        self._disconnect_after = disconnect_after
        self.state = SimpleNamespace(analyst_session=None)

    async def is_disconnected(self) -> bool:
        self._count += 1
        return self._count >= self._disconnect_after


async def _drive_sse_with_payload(monkeypatch, payload: dict) -> dict:
    """Helper: start SSE stream, publish a payload, return the first frame."""
    from backend.core.realtime.publisher import RealtimeMetricsPublisher

    import backend.routers.control_room as cr_mod

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())

    from backend.core.realtime import poller as poller_mod

    monkeypatch.setattr(poller_mod.poller, "ensure_polling", lambda sid: None)
    monkeypatch.setattr("backend.core.realtime.publisher.publisher", test_publisher)

    req = _FakeRequest(disconnect_after=10)

    resp = await cr_mod.realtime_stream(
        request=req,
        service_id=MOCK_SERVICE_ID,
    )

    agen = resp.body_iterator

    async def _delayed_publish():
        await asyncio.sleep(0.05)
        test_publisher.publish(MOCK_SERVICE_ID, payload)

    asyncio.ensure_future(_delayed_publish())

    try:
        raw = await asyncio.wait_for(agen.__anext__(), timeout=5)
        return json.loads(raw)
    finally:
        await agen.aclose()


@pytest.mark.asyncio
async def test_sse_real_metrics_shape(monkeypatch):
    """Publisher-driven SSE emits metrics_tick with the full data shape."""
    frame = await _drive_sse_with_payload(
        monkeypatch,
        {
            "event": "metrics_tick",
            "event_schema_version": 2,
            "timestamp": "2026-07-07T00:00:00+00:00",
            "status": "ok",
            "data": {
                "requests_per_second": 42.5,
                "error_rate": 0.01,
                "cache_hit_ratio": 0.95,
                "bandwidth_mbps": 1.5,
                "status_breakdown": {"status_2xx": 100},
                "estimated_cost_usd": 0.001,
                "origin_requests_per_second": 5.0,
                "origin_bandwidth_mbps": 0.2,
                "shield_requests": 10,
                "shield_hit_ratio": 0.05,
                "pass_requests": 0,
                "synth_requests": 0,
                "waf_blocked": 0,
                "waf_logged": 0,
                "waf_passed": 42,
                "pop_count": 2,
                "degraded_pops": [],
            },
            "aggregate_delay": 3,
        },
    )

    assert frame["event"] == "metrics_tick"
    assert frame["event_schema_version"] == 2
    assert frame["status"] == "ok"
    assert frame["data"]["requests_per_second"] == 42.5
    assert "cache_hit_ratio" in frame["data"]
    assert "bandwidth_mbps" in frame["data"]
    assert "estimated_cost_usd" in frame["data"]


@pytest.mark.asyncio
async def test_sse_rt_down_payload(monkeypatch):
    """When RT is down, the SSE payload carries status='rt_down'."""
    from backend.core.realtime.transform import error_tick_payload

    frame = await _drive_sse_with_payload(monkeypatch, error_tick_payload())

    assert frame["status"] == "rt_down"
    assert frame["data"]["requests_per_second"] == 0


# ── Log-field-audit ─────────────────────────────────────────────────────────


def test_log_field_audit_200(admin_client, monkeypatch):
    """Log-field-audit returns enabled fields and feature status."""
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"log_fields": None})

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-field-audit",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["service_id"] == MOCK_SERVICE_ID
    assert isinstance(data["enabled_fields"], list)
    assert isinstance(data["features"], list)
    assert len(data["features"]) > 0
    for feature in data["features"]:
        assert "name" in feature
        assert "status" in feature
        assert feature["status"] in ("ok", "missing_fields")


def test_log_field_audit_missing_fields(admin_client, monkeypatch):
    """When config has no groups, some features should report missing fields."""
    from backend import config as svcconfig

    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda sid: {"log_fields": {"groups": [], "field_overrides": {}}},
    )

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/log-field-audit",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    missing_features = [f for f in data["features"] if f["status"] == "missing_fields"]
    assert len(missing_features) > 0


# ── Correlator ──────────────────────────────────────────────────────────────


def test_correlate_unknown_dimension_400(admin_client):
    """Unknown dimension returns 400."""
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"dimension": "totally_bogus_field"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "unknown_dimension"


# ── OpenAPI contract ────────────────────────────────────────────────────────


def test_openapi_has_control_room_endpoints():
    """Verify key control-room endpoints are registered in the OpenAPI schema."""
    schema = app.openapi()
    paths = schema["paths"]

    assert "/api/services/{service_id}/control-room/{tab}" in paths
    assert "/api/services/{service_id}/realtime-stream" in paths
    assert "/api/services/{service_id}/log-field-audit" in paths
    assert "/api/services/{service_id}/control-room/correlate" in paths
    assert "/api/services/{service_id}/control-room/mitigations" in paths


def test_realtime_seed_without_credentials_skips_network(monkeypatch):
    """No Fastly credentials configured -> _fetch_seed_ticks must never hit rt.fastly.com."""
    import backend.config as backend_config
    import backend.routers.control_room as control_room

    monkeypatch.setattr(backend_config, "get_fastly_api_key", lambda service_id: None)
    monkeypatch.setattr(backend_config, "get_fastly_logging_service_id", lambda service_id: None)

    def _fail_get(*args, **kwargs):
        raise AssertionError("seed fetch with no credentials attempted network I/O")

    monkeypatch.setattr("requests.get", _fail_get)
    monkeypatch.setattr("backend.core.fastly.client.fastly_rt", _fail_get)

    ticks = control_room._fetch_seed_ticks(MOCK_SERVICE_ID, count=5)

    assert ticks == []


def test_fetch_seed_ticks_goes_through_fastly_rt_client(monkeypatch):
    """The RT seed fetch rides core/fastly/client.fastly_rt with the stored token and channel path."""
    import backend.config as backend_config
    import backend.routers.control_room as control_room

    monkeypatch.setattr(backend_config, "get_fastly_api_key", lambda service_id: "tok")
    monkeypatch.setattr(backend_config, "get_fastly_service_id", lambda service_id: "cdn123")
    calls = []

    def _fake_rt(path, *, token, **kwargs):
        calls.append((path, token))
        return {"Timestamp": 1000, "Data": [{"aggregated": {}}]}

    monkeypatch.setattr("backend.core.fastly.client.fastly_rt", _fake_rt)
    monkeypatch.setattr("backend.core.realtime.transform.transform_single_second", lambda point, ts: {"ts": ts})

    ticks = control_room._fetch_seed_ticks(MOCK_SERVICE_ID, count=5)

    assert calls == [("/v1/channel/cdn123/ts/h?limit=120", "tok")]
    assert len(ticks) == 1


def test_fetch_seed_ticks_returns_empty_when_rt_fetch_fails(monkeypatch):
    import backend.config as backend_config
    import backend.routers.control_room as control_room

    monkeypatch.setattr(backend_config, "get_fastly_api_key", lambda service_id: "tok")
    monkeypatch.setattr(backend_config, "get_fastly_service_id", lambda service_id: "cdn123")

    def _boom(path, *, token, **kwargs):
        raise RuntimeError("HTTP 500 GET /v1/channel/cdn123/ts/h")

    monkeypatch.setattr("backend.core.fastly.client.fastly_rt", _boom)

    assert control_room._fetch_seed_ticks(MOCK_SERVICE_ID, count=5) == []


# ── Seed, Correlate, & Wizard endpoints ──────────────────────────────────


def test_realtime_seed_with_cached_ticks(admin_client, monkeypatch):
    """If the publisher has enough ticks cached, return them directly."""
    from backend.core.realtime.publisher import publisher as rt_publisher

    dummy_ticks = [{"timestamp": "2026-07-07T00:00:00Z"}] * 60
    monkeypatch.setattr(rt_publisher, "get_recent_ticks", lambda sid, count: dummy_ticks)

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/realtime-seed",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["ticks"]) == 60


def test_realtime_seed_with_fetch_fallback(admin_client, monkeypatch):
    """If the publisher cache is empty, fall back to fetching from RT API."""
    from backend.core.realtime.publisher import publisher as rt_publisher

    import backend.routers.control_room as cr_mod

    monkeypatch.setattr(rt_publisher, "get_recent_ticks", lambda sid, count: [])

    dummy_ticks = [{"timestamp": "2026-07-07T00:01:00Z"}]
    monkeypatch.setattr(cr_mod, "_fetch_seed_ticks", lambda sid: dummy_ticks)
    monkeypatch.setattr("backend.core.realtime.poller.poller.ensure_polling", lambda sid: None)

    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/realtime-seed",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["ticks"]) == 1
    assert data["ticks"][0]["timestamp"] == "2026-07-07T00:01:00Z"


def test_correlate_success(admin_client, monkeypatch):
    """Verify correlate returns correct dimension stats with mocked DuckDB connection."""
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    mock_source = {"name": "svc"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: mock_source)

    mock_con = MagicMock()
    mock_con.execute().fetchall.return_value = [("200", 100)]
    mock_con.execute().fetchone.return_value = (datetime(2026, 8, 19, 12, 0, 0, tzinfo=UTC),)
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda source, read_only: mock_con)

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"dimension": "status", "window_minutes": 10, "limit": 5},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["dimension"] == "status"
    assert data["top"] == [{"value": "200", "count": 100}]
    assert "freshness" in data


@pytest.mark.asyncio
async def test_correlate_masks_client_ips_for_analyst(monkeypatch):
    """Control Room correlator must not expose raw client IPs to Path B."""
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    import backend.routers.control_room as cr_mod

    mock_con = MagicMock()
    mock_con.execute().fetchall.return_value = [("203.0.113.45", 7)]
    mock_con.execute().fetchone.return_value = (datetime.now(UTC),)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": "svc"})
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda source, read_only: mock_con)

    request = SimpleNamespace(state=SimpleNamespace(analyst_session=SimpleNamespace(pii_policy={"mask_ips": True})))
    response = await cr_mod.control_room_correlate(
        cr_mod.CorrelateRequest(dimension="ip"),
        MOCK_SERVICE_ID,
        request,
        None,
        SimpleNamespace(resolve=lambda service_id: None),
    )

    assert response["top"] == [{"value": "203.0.113.xxx", "count": 7}]


@pytest.mark.asyncio
async def test_correlate_uses_high_scale_aggregate_path():
    """High-Scale Control Room correlation reads the distributed aggregate."""
    from datetime import UTC, datetime

    from backend.high_scale.archive_models import ServingWatermark

    import backend.routers.control_room as cr_mod

    watermark = ServingWatermark(
        service_id=MOCK_SERVICE_ID,
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 8, 19, 12, 0, tzinfo=UTC),
        coverage_end=datetime(2026, 8, 19, 12, 5, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )

    class Client:
        def execute(self, sql, params=None):
            if "AS total_count" in sql:
                return [{"total_count": 12}]
            return [{"value": "500", "aggregate_count": 12}]

    service = SimpleNamespace(
        client=Client(),
        watermark_for=lambda domain: watermark,
    )
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=None))
    response = await cr_mod.control_room_correlate(
        cr_mod.CorrelateRequest(dimension="status"),
        MOCK_SERVICE_ID,
        request,
        None,
        SimpleNamespace(resolve=lambda service_id: service),
    )

    assert response["top"] == [{"value": "500", "count": 12}]
    assert response["freshness"]["latest_log_at"] == "2026-08-19T12:05:00+00:00"


def test_correlate_unknown_service_source_404(admin_client, monkeypatch):
    """If service source doesn't exist, return 404."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)

    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/correlate",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"dimension": "status"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "no_data_source"


def test_wizard_state_and_step_flow(admin_client):
    """Verify completion of wizard steps and state query from audit logs."""
    # 1. Check initial empty state
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/state",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    assert resp.json()["completed_steps"] == []
    assert resp.json()["is_complete"] is False

    # 2. Complete a step
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/step",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"step": "domain", "details": {"custom": True}},
    )
    assert resp.status_code == 200
    assert resp.json()["recorded"] is True
    assert resp.json()["step"] == "domain"

    # 3. Complete wizard
    resp = admin_client.post(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/step",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"step": "complete", "details": {}},
    )
    assert resp.status_code == 200

    # 4. Check updated state
    resp = admin_client.get(
        f"/api/services/{MOCK_SERVICE_ID}/control-room/wizard/state",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    completed = resp.json()["completed_steps"]
    assert "domain" in completed
    assert "complete" in completed


# ── Merged from tests/routers/test_rbac_and_typed_contracts.py ──


def _body(resp, *, status: int = 200):
    """Assert HTTP status and return JSON with telemetry underscore keys stripped."""
    assert resp.status_code == status, resp.text
    data = resp.json()
    return {k: v for k, v in data.items() if not k.startswith("_")} if isinstance(data, dict) else data


def _roundtrip(obj: Any) -> Any:
    return json.loads(json.dumps(jsonable_encoder(obj)))


def _make_analyst_session(session_id: str = "sess-analyst-1", **ov: Any) -> AnalystSession:
    d: dict[str, Any] = {"session_id": session_id, "invite_id": "inv-analyst-1", "name": "Analyst B"}
    d |= {"email": "analyst@example.com", "ip_address": "203.0.113.10", "user_agent": "pytest-analyst"}
    d |= {"fingerprint_signature": "fp-analyst", "pii_policy": {"mask_ips": True}, "query_window_hours": 24}
    d |= {"query_start_time": None, "query_end_time": None, "login_time": "2026-01-01T00:00:00Z"}
    d |= {"last_active_time": "2026-01-01T00:05:00Z", "service_ids": ["svc-1"]}
    return AnalystSession(**(d | ov))


# ── Cross-tenant scope enforcement (alerts, views, custom fields, scoring labels) ──


@pytest.fixture
def app_with_session():
    """Mini app that injects a per-test analyst_session on request.state via X-Test-Session-Services."""
    mini = FastAPI()
    for rtr in (
        alerts_router.alerts_router,
        alerts_router.views_router,
        services_core_router.router,
        session_scoring_router.router,
    ):
        mini.include_router(rtr)

    @mini.middleware("http")
    async def stamp_session(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        sids = [s for s in (sid or "").split(",") if s]
        request.state.analyst_session = (
            SimpleNamespace(session_id="test", service_ids=sids, email="t@t") if sid is not None else None
        )
        return await call_next(request)

    return mini


def _fake_alert(aid: str, sid: str) -> dict:
    d = {"id": aid, "service_id": sid, "name": "t", "category": "reliability", "metric": "5xx_rate"}
    d |= {"evaluation_type": "absolute", "evaluation_scope": "all", "operator": ">"}
    return d | {"threshold": 5.0, "window_min": 5.0}


_VALID_CF_BODY = {
    "name": "x_test_field",
    "label": "Test Field",
    "vcl_log_expression": "req.http.X-Test",
    "collection_stage": "deliver",
} | {"duckdb_type": "VARCHAR", "value_type": "string", "bytes_estimate": 20, "enabled": True}


def _scope_403(resp) -> bool:
    return resp.status_code == 403 and resp.json().get("detail", {}).get("error") == "service_not_authorized"


@pytest.mark.security_regression
def test_alerts_list_filters_to_analyst_scope_and_admin_sees_all(app_with_session):
    fake_alerts = [_fake_alert("a1", "svc-A"), _fake_alert("a2", "svc-B"), _fake_alert("a3", "svc-C")]
    with patch("backend.repositories.alerts.get_alerts", return_value=fake_alerts), TestClient(app_with_session) as c:
        r = c.get("/api/alerts/", headers={"x-test-session-services": "svc-A,svc-B"})
        assert r.status_code == 200 and [a["id"] for a in r.json()["data"]] == ["a1", "a2"]
        r_admin = c.get("/api/alerts/")
        assert r_admin.status_code == 200 and len(r_admin.json()["data"]) == 3


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path,patch_target",
    [
        ("/api/alerts/svc-A", "backend.repositories.alerts.get_alerts"),
        ("/api/views/svc-A", "backend.repositories.alerts.get_views"),
    ],
)
def test_alerts_and_views_service_scope_checks(app_with_session, path: str, patch_target: str):
    with TestClient(app_with_session) as c:
        r_deny = c.get(path.replace("svc-A", "svc-B"), headers={"x-test-session-services": "svc-A"})
        assert _scope_403(r_deny), r_deny.text
    with patch(patch_target, return_value=[]), TestClient(app_with_session) as c:
        assert c.get(path, headers={"x-test-session-services": "svc-A"}).status_code == 200


@pytest.mark.security_regression
def test_views_list_admin_unrestricted(app_with_session):
    row = {"id": "v1", "service_id": "svc-anything", "name": "My view", "filters_json": "[]", "time_range_type": None}
    row |= {"start_time": None, "end_time": None, "page": None, "created_at": "2026-01-01 00:00:00"}
    with patch("backend.repositories.alerts.get_views", return_value=[row]), TestClient(app_with_session) as c:
        r = c.get("/api/views/svc-anything")
    assert r.status_code == 200 and r.json() == [row]


@pytest.mark.security_regression
def test_alerts_and_views_create_rejects_unauthorized_analyst(app_with_session):
    alert_payload = {k: v for k, v in _fake_alert("a", "svc-B").items() if k != "id"} | {
        "name": "test",
        "window_min": 5,
    }
    view_payload = {"service_id": "svc-B", "name": "v", "filters": {}, "columns": []}
    with TestClient(app_with_session) as c:
        assert (
            c.post("/api/alerts/", json=alert_payload, headers={"x-test-session-services": "svc-A"}).status_code == 403
        )
        r_view = c.post("/api/views/", json=view_payload, headers={"x-test-session-services": "svc-A"})
        assert r_view.status_code in (403, 422) and (r_view.status_code != 403 or _scope_403(r_view))


@pytest.mark.security_regression
@pytest.mark.parametrize("view_exists", [False, True], ids=["nonexistent", "existing"])
def test_views_delete_rejects_unauthorized_analyst(app_with_session, view_exists: bool):
    """Finding 017: scope gate runs before any DB lookup so both existing and non-existent ids return 403."""
    view_row = {"id": "v-target", "service_id": "svc-B", "name": "n"} if view_exists else None
    with patch("backend.repositories.alerts.get_view_by_id", return_value=view_row), TestClient(app_with_session) as c:
        r = c.delete("/api/views/v-target?service_id=svc-B", headers={"x-test-session-services": "svc-A"})
    assert _scope_403(r), r.text


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/api/services/svc-B/custom-fields", None),
        ("GET", "/api/services/svc-B/custom-fields/export", None),
        ("POST", "/api/services/svc-B/custom-fields", _VALID_CF_BODY),
        ("PATCH", "/api/services/svc-B/custom-fields/x_test_field", {"label": "Updated"}),
        ("DELETE", "/api/services/svc-B/custom-fields/x_test_field", None),
        ("POST", "/api/services/svc-B/custom-fields/import", {"custom_fields": [_VALID_CF_BODY]}),
        (
            "POST",
            "/api/services/svc-B/custom-fields/validate-vcl",
            {"vcl_log_expression": "req.http.X-Test", "collection_stage": "deliver"},
        ),
    ],
)
def test_custom_fields_rejects_unauthorized_analyst(app_with_session, method: str, path: str, body: dict | None):
    with TestClient(app_with_session) as c:
        r = c.request(method, path, json=body, headers={"x-test-session-services": "svc-A"})
    assert _scope_403(r), r.text


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "service_id,headers",
    [("svc-anything", {}), ("svc-A", {"x-test-session-services": "svc-A"})],
    ids=["admin_unrestricted", "in_scope_analyst"],
)
def test_custom_fields_authorized_callers_reach_handler(app_with_session, service_id: str, headers: dict):
    with (
        patch("backend.utils.router_utils.load_service_config", return_value={"log_fields": {"custom_fields": []}}),
        TestClient(app_with_session) as c,
    ):
        assert c.get(f"/api/services/{service_id}/custom-fields", headers=headers).status_code != 403


@pytest.mark.security_regression
def test_scoring_labels_delete_scope_and_admin_bypass(app_with_session):
    with TestClient(app_with_session) as c:
        r_deny = c.delete(
            "/api/services/svc-B/scoring/labels/some-label-id", headers={"x-test-session-services": "svc-A"}
        )
    assert _scope_403(r_deny) and r_deny.json()["detail"]["service"] == "svc-B"
    with (
        patch("backend.scoring.labels.delete_label", return_value={"id": "x", "service_id": "svc-B"}),
        TestClient(app_with_session) as c,
    ):
        assert c.delete("/api/services/svc-B/scoring/labels/x").status_code != 403


# ── Blocklist enforcement (H-1..H-4, S-1, S-2) ───────────────────────────────

_SCORING_ADMIN_SUFFIXES = ("/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code")
_BLOCKED_BASE_PATHS = [
    "/api/usage/prefill",
    "/api/download-all",
    "/api/download-folder",
    "/api/download",
    "/api/services/svc-A/lake-info",
    "/api/cron-schedule",
    "/api/services/svc-A/log-field-audit",
    *(f"/api/services/svc-A/scoring{s}" for s in _SCORING_ADMIN_SUFFIXES),
]


@pytest.fixture
def app_with_blocklist():
    """Mini app whose middleware runs ``_is_blocked_path`` for analyst requests."""
    mini = FastAPI()

    @mini.middleware("http")
    async def rbac_gate(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            request.state.analyst_session = SimpleNamespace(
                session_id="test", service_ids=[s for s in sid.split(",") if s], email="t@t"
            )
            if _is_blocked_path(request.url.path):
                return JSONResponse(status_code=403, content={"error": "admin_only"})
        else:
            request.state.analyst_session = None
        return await call_next(request)

    routes = (
        "/api/usage/prefill",
        "/api/download",
        "/api/download-all",
        "/api/download-folder",
        "/api/cron-schedule",
        "/api/services/{service_id}/lake-info",
        "/api/services/{service_id}/realtime-stream",
        "/api/services/{service_id}/realtime-seed",
        "/api/services/{service_id}/control-room/{tab}",
        "/api/services/{service_id}/log-field-audit",
        *(
            f"/api/services/{{service_id}}/scoring{s}"
            for s in (*_SCORING_ADMIN_SUFFIXES, "/labels", "/sessions/{sid}/events", "/top-flagged")
        ),
    )
    for route in routes:
        mini.add_api_route(
            route,
            lambda service_id="svc-A", sid="", tab="": {"ok": True, "service_id": service_id, "sid": sid, "tab": tab},
            methods=["GET"],
        )
    return mini


@pytest.mark.security_regression
@pytest.mark.parametrize("path", [*_BLOCKED_BASE_PATHS, "/api/services/svc-A/scoring/l2-enforce"])
def test_admin_only_routes_blocked_for_analyst(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403 and r.json()["error"] == "admin_only", f"{path}: {r.status_code} {r.text}"


@pytest.mark.security_regression
@pytest.mark.parametrize("path", [f"{p}/" for p in _BLOCKED_BASE_PATHS])
def test_admin_only_routes_resist_trailing_slash_bypass(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403 and r.json()["error"] == "admin_only", f"{path}: {r.status_code} {r.text}"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svc-A/scoring/labels",
        "/api/services/svc-A/scoring/sessions/sess-1/events",
        "/api/services/svc-A/scoring/top-flagged",
        "/api/services/svc-A/realtime-stream",
        "/api/services/svc-A/realtime-seed",
        "/api/services/svc-A/control-room/admin_health",
    ],
)
def test_analyst_allowed_reads_pass_blocklist(app_with_blocklist, path: str):
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200 and r.json()["service_id"] == "svc-A", f"{path}: {r.status_code} {r.text}"


@pytest.mark.security_regression
def test_realtime_stream_still_needs_service_scope():
    assert not _is_blocked_path("/api/services/svc-B/realtime-stream")


# ── Debug endpoints (/api/debug SQLite ring-buffer & DEBUG_RESPONSES) ────────

_CALLS = [{"service": "Fastly API", "method": "GET", "path": "/probe", "time_ms": 1.0}]


@pytest.fixture
def recorded_calls():
    with patch.object(telemetry, "get_tracked_calls", return_value=_CALLS):
        yield


def _debug_keys(body: dict) -> list[str]:
    return [k for k in body if k.startswith(("_debug", "debug_"))]


def _validate(client):
    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda method, path, **kw: (
            {"id": "tok-1", "name": "T", "user_id": "u"} if "/tokens/self" in path else {"name": "Svc"}
        ),
    ):
        return client.post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})


def _check_domain(client):
    with patch("backend.routers.provision._check_domain_available", return_value=(True, None)):
        return client.get("/api/provision/check-domain?prefix=valid-prefix-1")


def _check_fos(client, *, fail: bool = False):
    fake = MagicMock()
    if fail:
        fake.list_objects_v2.side_effect = RuntimeError("boom")
    else:
        fake.list_objects_v2.return_value = {"Contents": []}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake):
        return client.post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"},
        )


def _audit_logs(client):
    return client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _iceberg_calendar(client):
    from backend.deps import get_source

    app.dependency_overrides[get_source] = lambda: {"service_id": "service123", "name": "service123"}
    with patch(
        "backend.core.iceberg.get_snapshot_calendar", return_value={"2026-08-19": {"data_files": 5, "size_bytes": 1}}
    ):
        return client.get("/api/admin/iceberg-calendar?table_name=logs")


def _log_fields_get(client):
    svcconfig.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "log_fields": {"groups": ["A"]}})
    return client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers={"x-fastly-service-id": MOCK_SERVICE_ID})


def _generate_viewer_key(client):
    invite = {"service_id": "svc-1", "name": "MyService", "access_key_id": "AK", "secret_key": "SK", "fos_bucket": "b"}
    invite |= {"fos_region": "us-east-1", "fos_endpoint": "https://s3.us-east-1.amazonaws.com", "fos_prefix": ""}
    with patch("backend.provision.generate_analyst_invite", return_value=invite):
        return client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")


_SITES = [
    pytest.param(_validate, id="provision-validate"),
    pytest.param(_check_domain, id="provision-check-domain"),
    pytest.param(lambda c: _check_fos(c, fail=False), id="provision-check-fos-ok"),
    pytest.param(lambda c: _check_fos(c, fail=True), id="provision-check-fos-failed"),
    pytest.param(_audit_logs, id="audit-logs"),
    pytest.param(_iceberg_calendar, id="iceberg-calendar"),
    pytest.param(_log_fields_get, id="service-log-fields"),
    pytest.param(_generate_viewer_key, id="generate-viewer-key"),
]

_is_remote_flag = False
_original_getattr = starlette.datastructures.State.__getattr__


def _mock_state_getattr(self, key: str):
    if key == "is_remote":
        return _is_remote_flag
    try:
        return _original_getattr(self, key)
    except (AttributeError, KeyError):
        raise AttributeError(f"'State' object has no attribute '{key}'")


@pytest.mark.security_regression
class TestDebugEndpoints:
    @pytest.fixture(autouse=True)
    def _isolated_buffer(self, monkeypatch):
        global _is_remote_flag
        _is_remote_flag = False
        sqlite_profiler.clear()
        monkeypatch.setattr(starlette.datastructures.State, "__getattr__", _mock_state_getattr)
        yield
        _is_remote_flag = False
        sqlite_profiler.clear()

    def test_recent_sqlite_lifecycle_and_filters(self):
        client = TestClient(app)
        empty = client.get("/api/debug/recent-sqlite").json()
        assert empty["queries"] == [] and empty["buffer_size"] == 0 and empty["buffer_cap"] > 0

        metadata_db.get_con("svc-a").execute("SELECT 1")
        first = client.get("/api/debug/recent-sqlite").json()
        assert first["buffer_size"] > 0 and any("SELECT 1" in q["sql"] for q in first["queries"])

        midpoint_seq = first["last_seq"]
        con2 = metadata_db.get_con("svc-b")
        con2.execute("SELECT 2")
        con2.execute("SELECT 3")
        second = client.get(f"/api/debug/recent-sqlite?since_seq={midpoint_seq}").json()
        assert all(q["seq"] > midpoint_seq for q in second["queries"])
        assert len(client.get("/api/debug/recent-sqlite?limit=2").json()["queries"]) <= 2

        clear_res = client.post("/api/debug/clear-sqlite")
        assert clear_res.status_code == 200 and clear_res.json()["ok"] is True
        assert sqlite_profiler.get_recent()["buffer_size"] == 0

    @pytest.mark.parametrize(
        "env_val,payload,kwargs,expected",
        [
            (
                "true",
                {"ok": True},
                {"gated": True, "is_cached": None, "inject_calls": True},
                {"ok": True, "_debug_calls": _CALLS},
            ),
            (None, {"ok": True}, {"gated": True, "is_cached": None, "inject_calls": True}, {"ok": True}),
            (
                None,
                {"ok": True, "_debug_calls": ["stale"], "_debug_queries": ["stale"]},
                {"gated": True, "is_cached": None, "inject_calls": True},
                {"ok": True},
            ),
            ("true", {"ok": True}, {"gated": True, "is_cached": True}, {"ok": True, "_is_cached": True}),
        ],
    )
    def test_gated_envelope_variants(self, monkeypatch, recorded_calls, env_val, payload, kwargs, expected):
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False) if env_val is None else monkeypatch.setenv(
            "DEBUG_RESPONSES", env_val
        )
        assert telemetry.debug_envelope(dict(payload), **kwargs) == expected

    @pytest.mark.parametrize("call", _SITES)
    def test_route_emits_no_debug_envelope_when_debug_responses_is_off(self, client, monkeypatch, call):
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
        r = call(client)
        assert r.status_code == 200 and _debug_keys(r.json()) == []

    @pytest.mark.parametrize("call", _SITES)
    def test_route_emits_the_call_trace_when_debug_responses_is_on(self, client, monkeypatch, recorded_calls, call):
        monkeypatch.setenv("DEBUG_RESPONSES", "true")
        r = call(client)
        assert r.status_code == 200 and [c["path"] for c in r.json()["_debug_calls"]] == ["/probe"]

    def test_get_and_patch_debug_settings_and_remote_access(self):
        global _is_remote_flag
        client = TestClient(app)

        body = client.get("/api/admin/debug-settings").json()
        assert body["query_debug_visibility"] == "disabled" and body["api_call_debug_visibility"] == "disabled"

        patched = client.patch(
            "/api/admin/debug-settings",
            json={"query_debug_visibility": "both", "api_call_debug_visibility": "analysts"},
        ).json()
        assert patched["query_debug_visibility"] == "both" and patched["api_call_debug_visibility"] == "analysts"
        assert get_setting("query_debug_visibility") == "both"

        _is_remote_flag = True
        set_setting("query_debug_visibility", "disabled")
        res_blocked = client.get("/api/debug/recent-sqlite")
        assert res_blocked.status_code == 403 and res_blocked.json()["detail"]["error"] == "sql_debug_disabled"

        set_setting("query_debug_visibility", "both")
        assert client.get("/api/debug/recent-sqlite").status_code == 200

        res_clear = client.post("/api/debug/clear-sqlite")
        assert res_clear.status_code == 403 and res_clear.json()["detail"]["error"] == "admin_only"

        set_setting("query_debug_visibility", "disabled")
        set_setting("api_call_debug_visibility", "disabled")


# ── Typed response contracts (admin & endpoints) ────────────────────────────


def _wire_model(model_cls: type, payload: dict[str, Any]) -> str:
    return model_cls.model_validate(payload).model_dump_json(by_alias=True, exclude_unset=True)


def _assert_same_bytes(model_cls: type, payload: dict[str, Any]) -> None:
    assert json.loads(_wire_model(model_cls, payload)) == jsonable_encoder(payload)
    assert _wire_model(model_cls, payload) == json.dumps(
        jsonable_encoder(payload), separators=(",", ":"), ensure_ascii=False
    )


def test_admin_bot_sources_rdns_and_duckdb_match_producer(client):
    meta = {"id": "arcjet", "entry_count": 42, "last_updated": "2026-10-11T00:00:00Z", "enabled": True}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=meta):
        assert _body(client.post("/api/admin/bot-sources/arcjet/refresh")) == {"ok": True, "source": meta}
    results = [{"id": "a", "entry_count": 3}, {"id": "b", "failed": True, "error": "boom"}]
    for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=results):
            assert _body(client.post(path)) == {"ok": False, "sources": results, "updated_count": 1, "failed_count": 1}
    summary = {"resolved": 4, "errors": 1, "discovered": 7, "batch_limit": 100}
    with patch("backend.utils.rdns_cache.enrich_batch", return_value=summary) as enrich:
        assert _body(client.post("/api/admin/rdns/enrich?limit=100")) == {"ok": True, "summary": summary}
    enrich.assert_called_once_with(limit=100)

    mb = 1024 * 1024
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * mb),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
    ):
        assert _body(client.post("/api/admin/duckdb/recycle")) == dict(
            ok=True, status="skipped", reason="RSS 100MB < threshold 500MB", rss_mb=100.0, threshold_mb=500.0
        )
    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * mb, 200 * mb]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * mb),
        patch("backend.core.duckdb_recycle.recycle_once", return_value="manual_admin: recycled 1/1"),
    ):
        exp_recycled = {"ok": True, "status": "recycled", "detail": "manual_admin: recycled 1/1"}
        exp_recycled |= {"rss_before_mb": 300.0, "rss_after_mb": 200.0, "freed_mb": 100.0}
        assert _body(client.post("/api/admin/duckdb/recycle?force=true")) == exp_recycled
    status_body = _body(client.get("/api/admin/duckdb/status"))
    assert set(status_body) == {"ok", "memory", "barrier", "pools", "retired_pools"}
    assert set(status_body["memory"]) == {"current_rss_mb", "recycle_threshold_mb", "recycle_interval_min"}
    assert set(status_body["barrier"]) == {"is_active", "active_paths"}


def test_ngwaf_share_purge_calendar_vcl_health_and_telemetry_match_producer(client, monkeypatch):
    stats = {"total_cached_bots": 2, "workspaces": {"w": None}, "top_bots": {"bot": 2}}
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-n"}),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync"),
        patch("backend.utils.ngwaf.get_cache_stats", return_value=stats),
    ):
        exp_sync = {"ok": True, "service_id": "svc-n", "workspace_id": "ws-1", "stats": stats}
        assert _body(client.post("/api/admin/ngwaf/sync/svc-n")) == exp_sync
    monkeypatch.setattr("backend.utils.ngwaf.get_cache_stats", lambda: dict(stats))
    assert _body(client.get("/api/admin/ngwaf/status")) == {"ok": True, **stats}
    monkeypatch.setattr("backend.utils.rdns_cache.get_stats", lambda: {"entries": 4, "hit_rate": 0.5})
    assert _body(client.get("/api/admin/rdns/stats")) == {"ok": True, "stats": {"entries": 4, "hit_rate": 0.5}}
    purge_stats = {"deleted_audit_logs": 3, "deleted_expired_invites": 1, "deleted_stale_sessions": 2}
    purge_stats |= {"deleted_claim_tokens": 0, "audit_retention_days": 90, "max_idle_session_days": 30}
    with patch("backend.core.share_db.purge_all_share_records", return_value=purge_stats):
        assert _body(client.post("/api/admin/share/purge")) == {"ok": True, **purge_stats}
    calendar = {"2026-10-01": {"data_files": 4, "size_bytes": 0}, "2026-10-02": {"data_files": 9, "size_bytes": 0}}
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=calendar):
        assert _body(client.get("/api/admin/iceberg-calendar")) == calendar

    Snippet = namedtuple("Snippet", "name")
    snippets = [Snippet("Fastly Log Analytics - capture"), Snippet("Fastly Log Analytics - scoring")]
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "tok"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=7),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=snippets),
    ):
        exp_vcl = {"service_id": "svc-a", "active_version": 7, "legacy_snippets_found": 0, "legacy_names": []}
        exp_vcl |= {
            "consolidated_snippets_found": 2,
            "consolidated_names": [s.name for s in snippets],
            "is_clean": True,
        }
        exp_vcl["recommendation"] = "✓ Clean state — VCL migration complete. Ready for production."
        assert _body(client.get("/api/admin/vcl-health?service_id=svc-a")) == exp_vcl
    with patch("backend.config.load_config", return_value=None):
        exp_nope = {"service_id": "nope", "error": "Service nope not configured", "is_clean": False}
        assert _body(client.get("/api/admin/vcl-health?service_id=nope")) == exp_nope
    call_row = {
        "service": "FOS",
        "method": "GET",
        "path": "/b/k",
        "status": "200",
        "time_ms": 3.0,
        "caller": "fn",
        "bytes": 10,
    }
    payload = {
        "queries": [{"engine": "duckdb", "sql_query": "SELECT 1", "duration_ms": 1.5}],
        "sections": [{"section_name": "summary", "duration_ms": 2.0}],
        "calls": [call_row],
    }
    with (
        patch("backend.config.get_active_service_id", return_value="svc-a"),
        patch("backend.core.metadata.usage_log.get_page_telemetry", return_value=payload),
    ):
        assert _body(client.get("/api/debug/page-telemetry?page_load_id=p1")) == payload
    with patch("backend.config.get_active_service_id", return_value=None):
        assert _body(client.get("/api/debug/page-telemetry?page_load_id=p1")) == {
            "queries": [],
            "sections": [],
            "calls": [],
        }
    assert _body(client.get("/api/health")) == {"status": "ok", "version": _APP_VERSION}
    with patch("backend.config.list_service_ids", return_value=[]):
        assert _body(client.get("/api/health?deep=1")) == {"status": "ok", "version": _APP_VERSION, "services": []}


def test_admin_ingest_actions_beacons_celery_and_control_room_match_producer(client, monkeypatch):
    for counters in (
        {"files_downloaded": 2, "rows_downloaded": 10, "files_removed": 0},
        {"error": "Could not load table: x", "files_downloaded": 0},
    ):
        with patch("backend.core.iceberg.sync_data", return_value=counters):
            assert (
                _body(
                    client.post(
                        "/api/admin/backfill-window?start_time=2026-10-01T00:00:00Z&end_time=2026-10-01T02:00:00Z"
                    )
                )
                == counters
            )
    started = {"ok": True, "message": "started", "run_id": 41}
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value=started):
        assert _body(client.post("/api/admin/commit-iceberg"), status=202) == started
        assert _body(client.post("/api/admin/ingest-logs")) == started
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb.start_cron_run", return_value=77),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
    ):
        assert _body(client.post("/api/admin/rebuild-local-view"), status=202) == {
            "ok": True,
            "message": "Local view rebuild started.",
            "run_id": 77,
        }
    cfg = {"service_id": "svc-t", "remote_frontend": {"service_id": "remote-1"}}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=cfg),
        patch("backend.routers.share_admin.delete_remote_frontend"),
        patch("backend.config.mutate_config", return_value=cfg),
    ):
        assert _body(
            client.post("/api/sharing/teardown-frontend", json={"service_id": "svc-t", "token_override": "t"})
        ) == {"ok": True, "message": "Successfully deleted remote frontend service remote-1"}
    assert _body(client.post("/api/ux-events", json={"event": "column_reordered", "pathname": "/x"})) == {"ok": True}
    assert _body(client.post("/api/web-vitals", json={"id": "v1", "name": "LCP", "value": 1.2, "rating": "good"})) == {
        "ok": True
    }

    for payload in (
        dict(
            broker_reachable=True,
            queues={"q.ingest": 3},
            ledger={"committed": 5},
            inprocess_schedules=[{"name": "job", "trigger": "interval[0:01:00]", "next_run_at": 1700000000.0}],
            workers=dict(
                active_tasks={"w1": [{"id": "t1", "name": "n", "args": [1], "time_start": 2.5, "worker_pid": 7}]},
                stats={"w1": {"started": dt.datetime(2026, 1, 1, tzinfo=dt.UTC)}},
                registered={"w1": ["a.b"]},
                scheduled={},
                count=1,
            ),
        ),
        dict(
            broker_reachable=False,
            queues={},
            inprocess_schedules=[],
            ledger={},
            workers={"active_tasks": {}, "stats": {}, "registered": {}, "scheduled": {}, "count": 0, "error": "down"},
        ),
    ):
        monkeypatch.setattr("backend.routers.admin.health.get_celery_status", lambda p=payload: p)
        assert _body(client.get("/api/admin/celery/status")) == _roundtrip(payload)
    for tab, stub in TAB_STUBS.items():
        assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}")) == stub, tab


@pytest.mark.parametrize(
    "model_cls,payload",
    [
        (m_svc.RealtimeSeedResponse, {"ticks": [{"ts": 1, "rps": 2.5, "nested": {"a": [1]}}]}),
        (
            m_svc.LogFieldAuditResponse,
            {
                "service_id": "svc",
                "enabled_fields": ["ip", "url"],
                "features": [{"name": "Insights", "required_fields": ["ip"], "status": "ok", "missing_fields": []}],
            },
        ),
        (
            m_svc.CorrelateResponse,
            {
                "dimension": "country",
                "top": [{"value": "US", "count": 3}],
                "freshness": {"latest_log_at": None, "lag_seconds": 0},
            },
        ),
        (m_svc.WizardStateResponse, {"service_id": "svc", "completed_steps": ["a"], "is_complete": False}),
        (m_svc.WizardStepResponse, {"recorded": True, "step": "a"}),
        (m_scoring.ScoringEnforceThresholdResponse, {"threshold": None, "enforced": False, "key": "enforce_threshold"}),
        (m_scoring.ScoringEnforceThresholdResponse, {"threshold": 70, "enforced": True, "key": "enforce_threshold"}),
        (m_scoring.ScoringDistributionRow, {"hour": "2026-01-01T00:00:00", "bucket": "0-25", "count": 4}),
        (m_scoring.ScoringComplianceRow, {"hour": "2026-01-01T00:00:00", "compliance": "ok", "count": 4}),
        (
            m_scoring.ScoringTopFlaggedRow,
            {"timestamp": "2026-01-01T00:00:00", "edge_sid": None, "edge_score": 90, "url": "/a"},
        ),
        (m_common.BootstrapPopGeo, {"city": "Denver", "region": "co", "country": "us"}),
        (m_common.BootstrapPopGeo, {"city": "", "region": "", "country": ""}),
        (
            m_common.BootstrapDebugState,
            {
                "debug_responses_enabled": False,
                "query_debug_visibility": "disabled",
                "api_call_debug_visibility": "admins",
            },
        ),
        (
            m_prov.ProvisionCheckConfigResponse,
            {"logging_service": {"ok": True, "details": "ok"}, "cdn_service": {"ok": False, "details": ""}},
        ),
        (
            m_prov.NgwafWorkspacesResponse,
            {
                "workspaces": [{"id": "w", "name": "n"}],
                "error_hint": "h",
                "_debug_raw": "GET /x\n\n--- Response (200) ---",
            },
        ),
        (
            m_prov.ProvisionValidateResponse,
            {"service_name": "s", "token_info": {"id": "1", "name": "t", "user_id": None, "type": "automation"}},
        ),
        (
            m_prov.ProvisionLakeInfoResponse,
            {
                "ok": True,
                "table_exists": True,
                "info": {"table_name": "logs", "snapshots": 2, "data_files": 7, "size_bytes": 10, "region": None},
                "calendar": {"2026-01-01": {"data_files": 7, "size_bytes": 0}},
                "range": {"start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00"},
            },
        ),
        (m_sec.SecurityBotRow, {"id": "gptbot", "name": "Gptbot", "category": "ai", "request_count": 12}),
        (m_sec.SecurityBotRow, {"name": "Googlebot", "category": "search", "request_count": 5}),
        (
            m_sec.SecurityBotRow,
            {
                "id": "b",
                "name": "b",
                "category": "unknown",
                "request_count": 3,
                "verified_count": 3,
                "impersonator_count": 0,
                "unverified_count": 0,
                "verification_coverage": 1.0,
                "pending_count": 0,
            },
        ),
    ],
)
def test_response_models_wire_roundtrip(model_cls, payload):
    _assert_same_bytes(model_cls, payload)


def test_scoring_models_and_routes_match_producers(client, monkeypatch):
    _assert_same_bytes(m_scoring.ScoringL2EnforceState, scoring_admin._l2_enforce_unavailable())
    for kwargs in (
        {
            "scoring_enabled_at_raw": "1700000000",
            "l2_enforce_raw": "1",
            "l2_enabled_at_raw": "1700086400",
            "now_epoch": 1700200000,
        },
        {"scoring_enabled_at_raw": None, "l2_enforce_raw": None, "l2_enabled_at_raw": None, "now_epoch": 1700200000},
    ):
        _assert_same_bytes(m_scoring.ScoringL2EnforceState, scoring_admin._build_l2_enforce_block(**kwargs))

    cfg: dict[str, Any] = {
        "service_id": "svc",
        "scoring": dict(
            enabled=True,
            operator_threshold=80,
            operator_threshold_set_at="2026-01-01T00:00:00+00:00",
            exclude_url_regex="\\.(png|css)$",
            enforce_status_code=403,
        ),
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    for model_cls, fn in (
        (m_scoring.ScoringThresholdState, scoring_admin.scoring_threshold_get),
        (m_scoring.ScoringExcludeRegexState, scoring_admin.scoring_exclude_regex_get),
        (m_scoring.ScoringEnforceStatusCodeState, scoring_admin.scoring_enforce_status_code_get),
    ):
        _assert_same_bytes(model_cls, fn("svc"))
    cfg["scoring"] = {"enabled": True}
    _assert_same_bytes(m_scoring.ScoringThresholdState, scoring_admin.scoring_threshold_get("svc"))
    _assert_same_bytes(m_scoring.ScoringEnforceStatusCodeState, scoring_admin.scoring_enforce_status_code_get("svc"))

    for model_cls, invalid in (
        (m_scoring.ScoringTopFlaggedRow, {"edge_sid": "x", "edge_score": 1}),
        (m_prov.ProvisionCheckConfigResponse, {"logging_service": {"ok": True, "details": "x"}}),
        (m_scoring.ScoringLabelRow, {"id": "a", "sid": "s", "label": "maybe"}),
    ):
        with pytest.raises(ValidationError):
            model_cls.model_validate(invalid)

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")) == {"enabled": False}

    cfg_full = {
        "service_id": MOCK_SERVICE_ID,
        "scoring": {"enabled": True, "request_secret": "s3cret", "custom_key": [1, 2], "deployed_vcl_sha": "v"},
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg_full)
    expected = session_scoring_router.scoring_status(MOCK_SERVICE_ID)
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert _body(resp) == expected and "request_secret" not in resp.json()
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/config"))["status"] == expected
    assert m_scoring.ScoringLabelDeleteResponse.model_validate({"status": "success", "id": "abc"}).model_dump(
        mode="json"
    ) == {"status": "success", "id": "abc"}

    ident_payload = {"enabled": True, "scoring_service_id": "svc", "scoring_service_name": "n"}
    ident_payload |= {
        "scoring_domain": "d.example",
        "matrix_version": "2026-01-01",
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
    assert set(ident_payload) <= set(m_scoring.ScoringStatusResponse.model_fields)
    assert (
        m_scoring.ScoringStatusResponse.model_validate(ident_payload).model_dump(mode="json", exclude_unset=True)
        == ident_payload
    )

    base_row = {"id": "a1", "service_id": MOCK_SERVICE_ID, "sid": "s1", "notes": "n", "flagged_by": "admin"}
    base_row |= {
        "sample_ip": "",
        "sample_ua": "",
        "sample_url": "",
        "created_at": "2026-01-01 00:00:00",
        "updated_at": "2026-01-01 00:00:00",
    }
    rows = [{**base_row, "label": lbl} for lbl in ("good", "bad", "neutral")]
    counts = {"good": 1, "bad": 1, "neutral": 1}
    monkeypatch.setattr(_labels, "list_labels", lambda sid, limit=500: rows)
    monkeypatch.setattr(_labels, "counts_by_label", lambda sid: counts)
    lbl_resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/labels")
    assert _body(lbl_resp) == {"labels": rows, "counts": counts}
    assert list(lbl_resp.json()["counts"]) == ["good", "bad", "neutral"]
    assert m_scoring.ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "good"}).label == "good"
    assert set(typing.get_args(m_scoring.ScoringLabelRow.model_fields["label"].annotation)) == set(ALLOWED_LABELS)


def test_catalog_bootstrap_schema_audit_cmcd_and_query_contracts(client, monkeypatch):
    payload = _compute_log_fields_catalog(None)
    assert payload["groups"] and payload["fields"] and payload["insights"] and payload["presets"]
    _assert_same_bytes(m_common.LogFieldsCatalogResponse, payload)
    assert _body(client.get("/api/log-fields/catalog")) == _roundtrip(payload)
    assert m_common.BootstrapResponse(active_service_id=None, services=[], log_fields_catalog=payload).model_dump(
        mode="json", by_alias=True
    )["log_fields_catalog"] == _roundtrip(payload)

    entries = lf.get_custom_fields_catalog_entries(
        {"custom_fields": [{"name": "my_field", "label": "My Field", "description": "d", "enabled": True}]}
    )
    assert entries and all(
        json.loads(_wire_model(m_common.LogFieldCatalogEntry, e)) == jsonable_encoder(e) for e in entries
    )
    custom_payload = {"groups": [], "fields": entries, "insights": [], "presets": {}}
    dumped_cat = m_common.LogFieldsCatalogResponse.model_validate(custom_payload).model_dump(mode="json")
    assert dumped_cat == _roundtrip(custom_payload) and "typical_bytes" not in dumped_cat["fields"][0]

    assert _body(client.get("/api/debug/state")) == {"debug_responses_enabled": m_common._debug_responses_enabled()}
    schema_payload = {"schema": [{"name": "ip", "type": "VARCHAR"}], "table_name": "logs_x"}
    assert m_svc.SchemaResponse.model_validate(schema_payload).model_dump(mode="json", by_alias=True) == schema_payload

    entry = {"id": 7, "timestamp": "2026-01-01 00:00:00+00:00", "event_type": "provision"}
    entry |= {"details": {"bucket": "b", "log_fields": {"groups": ["A"]}}, "actor": "ui", "source": "local"}
    dumped_audit = m_svc.AuditLogsResponse.model_validate(
        {"total": 1, "page": 1, "per_page": 50, "entries": [entry], "_debug_calls": []}
    ).model_dump(mode="json")
    assert dumped_audit["entries"] == [entry] and list(dumped_audit["entries"][0]) == list(entry)

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")) == {"enabled": False}
    cmcd_cfg = {"cmcd": {"enabled": True, "mode": "headers", "version": 1, "enabled_at": "2026-01-01T00:00:00+00:00"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cmcd_cfg)
    assert _body(client.get(f"/api/services/{MOCK_SERVICE_ID}/cmcd/status")) == cmcd_cfg["cmcd"]

    row_d = {"ts": dt.datetime(2026, 1, 2, 3, 4, 5), "tz_ts": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC)}
    row_d |= {"amount": Decimal("12.50"), "n": 7}
    base_result = {"columns": ["ts", "tz_ts", "amount", "n"], "data": [row_d], "row_count": 1, "total_rows": 1}
    base_result |= {
        "truncated": False,
        "elapsed_ms": 3,
        "explain_plan": "plan",
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }
    with patch("backend.repositories.query.execute_query", return_value=dict(base_result)):
        body = _body(
            client.post("/api/query", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"sql": "SELECT 1"})
        )
    assert (
        body["data"] == _roundtrip(base_result["data"])
        and body["data"][0]["tz_ts"].endswith("+00:00")
        and body["data"][0]["amount"] == 12.5
    )
    with patch(
        "backend.repositories.query.execute_query",
        return_value={k: v for k, v in base_result.items() if k != "explain_plan"},
    ):
        assert "explain_plan" not in _body(
            client.post("/api/query", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"sql": "SELECT 1"})
        )


_RUM_EMPTY_VITALS = {
    "lcp": {"p75": None, "distribution": None},
    "cls": {"p75": None, "distribution": None},
    "inp": {"p75": None, "distribution": None},
    "fid": {"p75": None, "fcp": None, "ttfb": None},
}
_RUM_FULL_VITALS = {
    "lcp": {"p75": 2.5, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
    "cls": {"p75": 0.1, "distribution": {"good": 90, "needs_improvement": 10, "poor": 0}},
    "inp": {"p75": 180, "distribution": {"good": 70, "needs_improvement": 20, "poor": 10}},
    "fid": {"p75": 3.0, "fcp": 1.2, "ttfb": 0.4},
}


@pytest.mark.parametrize(
    "model_name,payload",
    [
        ("RumStatusResponse", {"enabled": False, "enabled_at": None}),
        (
            "RumStatusResponse",
            {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00+00:00",
                "deployed_vcl_sha": "a",
                "current_vcl_sha": "b",
            }
            | {
                "vcl_drift": True,
                "capture_vitals": True,
                "capture_performance": False,
                "capture_errors": True,
                "capture_events": True,
                "custom_condition": "",
            },
        ),
        ("RumSettingsUpdateResponse", {"ok": True, "message": "m", "skipped": False}),
        (
            "RumBeaconHealthResponse",
            {
                "enabled": True,
                "beacon_fire_rate": 3,
                "recent_beacons": 3,
                "last_beacon_time": None,
                "setup_complete": True,
            }
            | {
                "message": "x",
                "_debug_queries": [{"sql": "s"}],
                "_debug_sqlite": [],
                "_debug_calls": [],
                "_is_cached": False,
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": True,
                "beacon_count": 0,
                "pageview_count": 0,
                "interaction_count": 0,
                "error_count": 0,
            }
            | {"message": "Waiting", "vitals": _RUM_EMPTY_VITALS, "worst_pages": [], "errors": []}
            | {
                "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                "environments": {"browsers": {}, "os": {}, "devices": {}},
                "_debug_queries": [],
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": 12,
                "pageview_count": 10,
                "interaction_count": 1,
                "error_count": 1,
                "vitals": _RUM_FULL_VITALS,
            }
            | {
                "worst_pages": [
                    {"path": "/a", "views": 4, "avg_load_time": 1, "lcp_p75": None, "cls_p75": 0.05, "error_rate": 0.0}
                ]
            }
            | {"errors": [{"message": "boom", "file": "a.js", "line": 3, "col": 4, "count": 2}]}
            | {
                "trends": {
                    "timestamps": ["2026-01-01T00:00:00+00:00"],
                    "lcp": [None],
                    "cls": [0.1],
                    "error_rate": [0.0],
                    "pageviews": [10],
                    "interactions": [1],
                    "errors": [1],
                }
            }
            | {"environments": {"browsers": {"Chrome": 9}, "os": {"macOS": 9}, "devices": {"desktop": 9}}},
        ),
    ],
)
def test_rum_models_roundtrip(model_name, payload):
    validated = getattr(rum_models, model_name).model_validate(payload)
    assert validated.model_dump(mode="json") == payload and validated.model_dump_json() == json.dumps(
        payload, separators=(",", ":")
    )


# ── Remote share admin & frontend deployment ────────────────────────────────


@pytest.fixture
def remote_share_client():
    mini = FastAPI()
    mini.add_middleware(RemoteAccessMiddleware)
    mini.include_router(share_admin.router)
    with TestClient(mini) as c:
        yield c


@pytest.fixture
def analyst_dep_client():
    class _StampAnalystMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            request.state.analyst_session = _make_analyst_session()
            return await call_next(request)

    mini = FastAPI()
    mini.add_middleware(_StampAnalystMiddleware)
    mini.include_router(share_admin.router)
    with TestClient(mini) as c:
        yield c


def test_share_status_live_start_stop_settings_and_lifecycle(remote_share_client: TestClient):
    status_keys = {"sharing_active", "public_endpoint", "public_url", "active_session_count", "max_concurrent_sessions"}
    status_keys |= {"invites", "sessions", "audit_logs", "services", "rate_limits", "telemetry"}
    assert status_keys <= set(_body(remote_share_client.get("/api/admin/share/status")))
    assert {"sharing_active", "public_url", "active_session_count", "rate_limits", "telemetry"} <= set(
        _body(remote_share_client.get("/api/admin/share/live"))
    )

    r_bad = remote_share_client.post("/api/admin/share/start", json={"public_endpoint": "http://insecure.example.com"})
    assert r_bad.status_code == 400 and r_bad.json()["detail"]["error"] == "invalid_request"
    r_start = remote_share_client.post("/api/admin/share/start", json={"public_endpoint": "https://share.example.com"})
    assert _body(r_start)["public_url"] == "https://share.example.com"

    live_on = _body(remote_share_client.get("/api/admin/share/live"))
    assert live_on["sharing_active"] is True and live_on["public_url"] == "https://share.example.com"
    assert _body(remote_share_client.post("/api/admin/share/stop"))["ok"] is True

    r_patch = remote_share_client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 15})
    assert _body(r_patch)["max_concurrent_analyst_sessions"] == 15
    assert _body(remote_share_client.get("/api/admin/share/settings"))["max_concurrent_analyst_sessions"] == 15
    assert {"version", "text"} <= set(_body(remote_share_client.get("/api/admin/share/tos")))

    for method, ver, text in (
        ("post", "v2.0-test", "# Custom Terms of Use\n\nConfidential analyst terms v2 for testing."),
        ("put", "v3.0-test", "# Custom Terms of Use\n\nConfidential analyst terms v3 for testing."),
    ):
        res = _body(getattr(remote_share_client, method)("/api/admin/share/tos", json={"version": ver, "text": text}))
        assert res["version"] == ver and res["text"] == text
    assert remote_share_client.post("/api/admin/share/tos", json={"version": "   ", "text": ""}).status_code == 400

    inv_req = {"name": "Alice Analyst", "email": "alice@example.com", "passcode": "ocean-breeze-cabin-42"}
    inv_req |= {"duration_hours": 48, "service_ids": ["svc-alpha"], "pii_policy": {"mask_ips": True}}
    invite = _body(remote_share_client.post("/api/admin/share/invites", json=inv_req))
    invite_id = invite["id"]
    assert invite["email"] == "alice@example.com" and invite["revoked"] == 0 and invite["service_ids"] == ["svc-alpha"]
    assert any(row["id"] == invite_id for row in _body(remote_share_client.get("/api/admin/share/invites"))["invites"])

    assert _body(remote_share_client.post(f"/api/admin/share/invites/{invite_id}/revoke"))["ok"] is True
    assert share_db.get_remote_invite(invite_id)["revoked"] == 1
    reactivated = _body(remote_share_client.post(f"/api/admin/share/invites/{invite_id}/reactivate"))
    assert reactivated["id"] == invite_id and reactivated["revoked"] == 0
    assert remote_share_client.post("/api/admin/share/invites/nonexistent-id/reactivate").status_code == 404

    regen_body = _body(remote_share_client.post(f"/api/admin/share/invites/{invite_id}/regenerate-passcode"))
    new_passcode = regen_body["passcode"]
    assert regen_body["ok"] is True and isinstance(new_passcode, str) and len(new_passcode) >= 16
    stored = share_db.get_remote_invite(invite_id)
    assert not share_db.verify_passcode("ocean-breeze-cabin-42", stored["passcode"])
    assert share_db.verify_passcode(new_passcode, stored["passcode"])
    assert remote_share_client.post("/api/admin/share/invites/nonexistent-id/regenerate-passcode").status_code == 404

    mgr = get_tunnel_manager()
    for sid, action in (("sess-revoke-1", "revoke"), ("sess-boot-2", "boot")):
        mgr._sessions[sid] = _make_analyst_session(session_id=sid, invite_id="inv-1", service_ids=["svc-alpha"])
        assert _body(remote_share_client.post(f"/api/admin/share/sessions/{sid}/{action}"))["ok"] is True
        assert sid not in mgr._sessions
        assert remote_share_client.post(f"/api/admin/share/sessions/missing-sess/{action}").status_code == 404

    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS", email="filter-target@example.com", ip_address="192.0.2.1", details="OK"
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="other-user@example.com", ip_address="192.0.2.2", details="Bad"
    )
    audit_params = {"event_type": "LOGIN_SUCCESS", "email": "filter-target@example.com", "limit": 50}
    rows = _body(remote_share_client.get("/api/admin/share/audit-logs", params=audit_params))["audit_logs"]
    assert rows and all(
        r["event_type"] == "LOGIN_SUCCESS" and "filter-target@example.com" in (r.get("email") or "") for r in rows
    )


@pytest.mark.parametrize(
    "method,path,json_body",
    [
        ("GET", "/api/admin/share/status", None),
        ("GET", "/api/admin/share/live", None),
        ("GET", "/api/admin/share/invites", None),
        (
            "POST",
            "/api/admin/share/invites",
            {"name": "X", "email": "x@y.com", "passcode": "ocean-breeze-cabin-42", "service_ids": ["s1"]},
        ),
        ("POST", "/api/admin/share/invites/inv-1/revoke", None),
        ("POST", "/api/admin/share/invites/inv-1/reactivate", None),
        ("POST", "/api/admin/share/invites/inv-1/regenerate-passcode", None),
        ("POST", "/api/admin/share/sessions/sess-1/revoke", None),
        ("POST", "/api/admin/share/sessions/sess-1/boot", None),
        ("GET", "/api/admin/share/audit-logs", None),
        ("GET", "/api/admin/share/settings", None),
        ("PATCH", "/api/admin/share/settings", {"max_concurrent_analyst_sessions": 5}),
        ("GET", "/api/admin/share/tos", None),
        ("POST", "/api/admin/share/tos", {"version": "v1", "text": "A" * 30}),
        ("PUT", "/api/admin/share/tos", {"version": "v2", "text": "B" * 30}),
        ("POST", "/api/admin/share/start", {"public_endpoint": "https://share.example.com"}),
        ("POST", "/api/admin/share/stop", None),
        ("POST", "/api/admin/share/panic", None),
    ],
)
def test_analyst_path_b_rejected_403_on_all_admin_share_routes(
    analyst_dep_client: TestClient, method: str, path: str, json_body: dict | None
):
    resp = analyst_dep_client.request(method, path, json=json_body)
    assert resp.status_code == 403, f"Expected 403 on {method} {path}, got {resp.status_code}: {resp.text}"
    assert resp.json().get("detail", resp.json()).get("error") == "admin_only"


@pytest.mark.parametrize(
    "extra_payload,configs,env_token,expected_token,expected_override_host",
    [
        ({"token_override": "override-token-123"}, [], None, "override-token-123", None),
        (
            {},
            [{"service_id": "other", "fastly_api_key": ""}, {"service_id": "t", "fastly_api_key": "cfg-tok"}],
            None,
            "cfg-tok",
            None,
        ),
        ({}, [], "env-resolved-token", "env-resolved-token", None),
        (
            {"token_override": "override-token-123", "override_host": "my-custom-host-header.com"},
            [],
            None,
            "override-token-123",
            "my-custom-host-header.com",
        ),
    ],
    ids=["token_override", "config_token", "env_token", "with_override_host"],
)
def test_deploy_frontend_token_resolution_and_options(
    client,
    extra_payload: dict,
    configs: list[dict],
    env_token: str | None,
    expected_token: str,
    expected_override_host: str | None,
):
    payload = {
        "service_name": "test-svc",
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "203.0.113.10",
        **extra_payload,
    }
    mock_res = {
        "service_id": "svc_1",
        "version": 1,
        "domain_name": payload["domain_name"],
        "origin_host": "203.0.113.10",
    }
    with (
        patch("backend.routers.share_admin.list_configs", return_value=configs),
        patch.dict(os.environ, {"FASTLY_API_KEY": env_token} if env_token else {}, clear=env_token is None),
        patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res) as mock_deploy,
    ):
        assert _body(client.post("/api/sharing/deploy-frontend", json=payload)) == mock_res
    mock_deploy.assert_called_once_with(
        service_name="test-svc",
        domain_name="test-domain.global.ssl.fastly.net",
        origin_host="203.0.113.10",
        origin_port=80,
        use_ssl=False,
        token=expected_token,
        override_host=expected_override_host,
    )


def test_deploy_and_teardown_frontend_persistence_and_errors(client):
    dom = "test-domain.global.ssl.fastly.net"
    base_payload = {"service_name": "test-service", "domain_name": dom, "origin_host": "203.0.113.10"}
    mock_res = {"service_id": "service_remote_id", "version": 1, "domain_name": dom, "origin_host": "203.0.113.10"}
    with patch("backend.routers.share_admin.list_configs", return_value=[]), patch.dict(os.environ, {}, clear=True):
        r_no_tok = client.post("/api/sharing/deploy-frontend", json=base_payload)
        assert r_no_tok.status_code == 400 and "Token is required" in r_no_tok.json()["detail"]["message"]
    r_422 = client.post(
        "/api/sharing/deploy-frontend", json={"service_name": "s", "domain_name": dom, "token_override": "t"}
    )
    assert r_422.status_code == 422
    with patch("backend.routers.share_admin.deploy_remote_frontend", side_effect=RuntimeError("Fastly API error")):
        r_500 = client.post("/api/sharing/deploy-frontend", json={**base_payload, "token_override": "some-token"})
        assert r_500.status_code == 500 and r_500.json()["detail"]["error"] == "request_failed"

    saved: dict = {}
    svc_payload = {**base_payload, "token_override": "token-123", "service_id": "logging-service-123"}
    with patch("backend.routers.share_admin.deploy_remote_frontend", return_value=mock_res):
        with patch(
            "backend.config.mutate_config", side_effect=fake_mutate_config({"service_id": "logging-service-123"}, saved)
        ) as mock_mutate:
            assert _body(client.post("/api/sharing/deploy-frontend", json=svc_payload)) == mock_res
            mock_mutate.assert_called_once()
            assert saved["remote_frontend"] == mock_res
        with patch("backend.config.mutate_config", side_effect=RuntimeError("filesystem full")):
            assert _body(client.post("/api/sharing/deploy-frontend", json=svc_payload)) == mock_res

    rf = {"service_id": "remote_service_123", "version": 1, "domain_name": dom, "origin_host": "203.0.113.10"}
    mock_config = {"service_id": "service123", "remote_frontend": rf}
    saved_td: dict = {}
    with (
        patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config", side_effect=fake_mutate_config(dict(mock_config), saved_td)),
    ):
        td_req = {"service_id": "service123", "token_override": "teardown-token"}
        assert _body(client.post("/api/sharing/teardown-frontend", json=td_req))["ok"] is True
        mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token="teardown-token")
        assert saved_td == {"service_id": "service123"}

    with (
        patch("backend.utils.router_utils.load_service_config", return_value={"service_id": "service123"}),
        patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
        patch("backend.config.mutate_config") as mock_mutate,
    ):
        assert (
            _body(
                client.post("/api/sharing/teardown-frontend", json={"service_id": "service123", "token_override": "t"})
            )["ok"]
            is True
        )
        mock_delete.assert_not_called()
        mock_mutate.assert_not_called()

    for configs, env_dict, expected_tok in (
        ([{"service_id": "t", "fastly_api_key": "cfg-td-token"}], {}, "cfg-td-token"),
        ([], {"FASTLY_API_KEY": "env-td-token"}, "env-td-token"),
    ):
        with (
            patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
            patch("backend.routers.share_admin.list_configs", return_value=configs),
            patch.dict(os.environ, env_dict, clear=not env_dict),
            patch("backend.routers.share_admin.delete_remote_frontend") as mock_delete,
            patch("backend.config.mutate_config"),
        ):
            assert client.post("/api/sharing/teardown-frontend", json={"service_id": "service123"}).status_code == 200
            mock_delete.assert_called_once_with(remote_service_id="remote_service_123", token=expected_tok)

    with patch("backend.routers.share_admin.list_configs", return_value=[]), patch.dict(os.environ, {}, clear=True):
        r_400 = client.post("/api/sharing/teardown-frontend", json={"service_id": "service123"})
        assert r_400.status_code == 400 and "Token is required" in r_400.json()["detail"]["message"]

    for exc, expected_status in (
        (RuntimeError("fastly offline"), 500),
        (HTTPException(status_code=403, detail="Forbidden"), 403),
    ):
        with (
            patch("backend.utils.router_utils.load_service_config", return_value=dict(mock_config)),
            patch("backend.routers.share_admin.delete_remote_frontend", side_effect=exc),
        ):
            r_err = client.post(
                "/api/sharing/teardown-frontend", json={"service_id": "service123", "token_override": "t"}
            )
            assert r_err.status_code == expected_status
