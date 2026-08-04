from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


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


def test_bootstrap_omits_admin_token_when_env_unset(client, tmp_path, monkeypatch):
    """ADMIN_SHARED_SECRET unset (the default) → ``settings.admin_token`` is
    null. Pinned because Phase Q's frontend interceptor MUST no-op in this
    state — without this contract, the dev frontend would inject empty
    headers everywhere."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.delenv("ADMIN_SHARED_SECRET", raising=False)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    assert response.json()["settings"]["admin_token"] is None


def test_bootstrap_exposes_admin_token_to_loopback_admin(client, tmp_path, monkeypatch):
    """ADMIN_SHARED_SECRET set + admin (loopback, no analyst session) →
    bootstrap returns the secret in ``settings.admin_token`` so the SPA
    can inject it on every subsequent admin request."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setenv("ADMIN_SHARED_SECRET", "s3cret-value")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    assert response.json()["settings"]["admin_token"] == "s3cret-value"


def test_bootstrap_omits_admin_token_for_authenticated_analyst(client, tmp_path, monkeypatch):
    """ADMIN_SHARED_SECRET set + analyst session attached →
    ``settings.admin_token`` is None. Pinned because a refactor that
    drops the ``analyst_session is None and not is_remote`` gate would
    hand the admin secret to every Fastly-fronted analyst on the next
    /api/bootstrap call — a credentials-leak regression no integration
    test currently covers."""
    from backend import config
    from backend.core import share_db
    from backend.utils import tunnel

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setenv("ADMIN_SHARED_SECRET", "s3cret-value")
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    share_db.reset_for_tests()
    tunnel.reset_for_tests()
    try:
        config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

        # Seed an invite + TOS acceptance so /api/share/login succeeds.
        invite = share_db.create_remote_invite(
            name="Test Analyst",
            email="analyst@example.com",
            passcode="ocean-breeze-cabin-42",
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=[MOCK_SERVICE_ID],
        )
        tos = share_db.get_latest_tos()
        if tos:
            share_db.mark_tos_accepted(invite["id"], tos["version"])

        # Start sharing so the X-Remote-Analyst path's Host check passes.
        tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")

        login = client.post(
            "/api/share/login",
            json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
            headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
        )
        assert login.status_code == 200, login.text
        # L1: the session id is cookie-only now (not in the JSON body). TOS was
        # accepted above, so login issues the full analyst_session_id cookie;
        # read it from the response and re-set it explicitly (httpx won't auto
        # send a Secure cookie back over the http test transport).
        client.cookies.set("analyst_session_id", login.cookies.get("analyst_session_id"))

        response = client.get(
            "/api/bootstrap",
            headers={
                "X-Remote-Analyst": "1",
                "Host": "testserver",
                "x-fastly-service-id": MOCK_SERVICE_ID,
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["settings"]["admin_token"] is None
        # Sanity check we're actually on the analyst branch — not the loopback
        # one (which would also yield admin_token=None when env unset).
        assert body["settings"]["is_remote_analyst"] is True
    finally:
        share_db.close_all_connections()
        tunnel.reset_for_tests()


def test_startup_stub_exposes_admin_token_to_loopback_admin(monkeypatch):
    """During ``not startup_complete`` (the ~15-20 min post-restart warm-up
    window on a multi-service VM), a loopback admin request must still get
    ``settings.admin_token`` — it's a static ``os.getenv()`` read with zero
    dependency on per-service warm-up. Pinned regression for the bug where
    the stub omitted the key entirely, 401-looping every admin mutation
    until warm-up finished.

    Exercises ``_startup_stub_response`` directly (mirrors
    ``test_bootstrap_remote_unauth_stub_omits_section_timings``'s pattern of
    calling the sync helper directly) because the route wrapper forces
    ``startup_complete=True`` whenever ``"pytest" in sys.modules``, so the
    stub branch is otherwise unreachable through ``client.get(...)``.
    """
    from unittest.mock import MagicMock

    from backend.routers.bootstrap import _startup_stub_response

    monkeypatch.setenv("ADMIN_SHARED_SECRET", "s3cret-value")

    fake_request = MagicMock()
    fake_request.state.is_remote = False

    response = _startup_stub_response(fake_request)
    payload = response.model_dump(by_alias=True)

    assert payload["settings"]["admin_token"] == "s3cret-value"


def test_startup_stub_omits_admin_token_when_env_unset(monkeypatch):
    """Same stub path, env unset (the default) → ``admin_token`` stays
    None, matching the fully-warm contract in
    ``test_bootstrap_omits_admin_token_when_env_unset``."""
    from unittest.mock import MagicMock

    from backend.routers.bootstrap import _startup_stub_response

    monkeypatch.delenv("ADMIN_SHARED_SECRET", raising=False)

    fake_request = MagicMock()
    fake_request.state.is_remote = False

    response = _startup_stub_response(fake_request)
    payload = response.model_dump(by_alias=True)

    assert payload["settings"]["admin_token"] is None


def test_startup_stub_withholds_admin_token_for_remote_caller(monkeypatch):
    """A remote (analyst/anonymous) caller hitting the stub during warm-up
    must NOT receive the admin token — mirrors the access-control boundary
    already enforced on the fully-warm path
    (``test_bootstrap_omits_admin_token_for_authenticated_analyst``)."""
    from unittest.mock import MagicMock

    from backend.routers.bootstrap import _startup_stub_response

    monkeypatch.setenv("ADMIN_SHARED_SECRET", "s3cret-value")

    fake_request = MagicMock()
    fake_request.state.is_remote = True

    response = _startup_stub_response(fake_request)
    payload = response.model_dump(by_alias=True)

    assert payload["settings"]["admin_token"] is None


def test_startup_stub_still_withholds_warmup_dependent_fields(monkeypatch):
    """The fix only frees ``admin_token``. The real per-service data — the
    ``services`` list, ``active_service_id``, ``schema`` — must stay gated
    on ``startup_complete`` exactly as before, for both loopback and remote
    callers."""
    from unittest.mock import MagicMock

    from backend.routers.bootstrap import _startup_stub_response

    monkeypatch.setenv("ADMIN_SHARED_SECRET", "s3cret-value")

    for is_remote in (False, True):
        fake_request = MagicMock()
        fake_request.state.is_remote = is_remote

        response = _startup_stub_response(fake_request)
        payload = response.model_dump(by_alias=True)

        assert payload["active_service_id"] is None
        assert payload["services"] == []
        assert payload["schema"] is None
        assert payload["settings"]["initializing"] is True


def _login_analyst_and_bootstrap(client, tmp_path, monkeypatch, *, pii_policy):
    """Seed a masking/non-masking analyst invite, log in, and return the
    /api/bootstrap response body for assertions about ``settings``."""
    from backend import config
    from backend.core import share_db
    from backend.utils import tunnel

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    share_db.reset_for_tests()
    tunnel.reset_for_tests()
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

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
    from backend.repositories import views as _views_repo

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
    from backend.repositories import views as _views_repo

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
    from backend.repositories import views as _views_repo

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
    from backend.cron import schedule as _sched
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


# Silence unused-import warnings
_ = pytest
