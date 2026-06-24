"""Graceful degradation guarantees for /api/bootstrap.

Audit finding: ``backend/routers/bootstrap.py`` resolves ~10 optional
sub-payloads (share_banner, ops_overview, cron_schedule, cron_runs_first_page,
last_sync, scoring_labels, share_status, views, ...) inside dedicated
``try/except Exception`` blocks. Sub-call failures are non-essential UX and
MUST NOT propagate into a 500 on the bootstrap endpoint — the frontend's
blocking startup call. If any block regresses (e.g. a refactor lifts a
raise out of the ``try``), the admin UI white-screens on startup.

These tests pin the contract: a sub-call exploding leaves its field at the
documented default (None for payload dicts, [] for views) while other
fields stay populated and the endpoint returns 200. Happy-path coverage
lives in ``test_bootstrap.py``; this file only exercises failure modes.
``test_bootstrap_views_survives_repo_error`` there established the pattern.
"""

from unittest.mock import patch

from tests.conftest import MOCK_SERVICE_ID


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


def test_bootstrap_survives_cron_schedule_failure(client, tmp_path, monkeypatch):
    """build_cron_schedule_payload raising → cron_schedule=None."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.cron.schedule.build_cron_schedule_payload", side_effect=RuntimeError("cron hiccup")):
        response = _get(client)
    assert response.status_code == 200
    data = response.json()
    assert data.get("cron_schedule") is None
    assert data["active_service_id"] == MOCK_SERVICE_ID


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


def test_bootstrap_survives_share_status_failure(client, tmp_path, monkeypatch):
    """build_share_status raising → share_status=None (share dashboard nicety)."""
    _seed_active_service(monkeypatch, tmp_path)
    with patch("backend.routers.share_admin.build_share_status", side_effect=RuntimeError("share admin broken")):
        response = _get(client)
    assert response.status_code == 200
    data = response.json()
    assert data.get("share_status") is None
    assert "services" in data


def test_bootstrap_survives_simultaneous_optional_failures(client, tmp_path, monkeypatch):
    """Worst case — every optional resolver dies. Bootstrap must still
    return 200 with the core fields populated. Pins the per-block
    try/except shield: losing any single block must not cascade."""
    _seed_active_service(monkeypatch, tmp_path)
    from backend.repositories import views as _views_repo

    monkeypatch.setattr(_views_repo, "get_views", lambda _sid: (_ for _ in ()).throw(RuntimeError("boom")))

    with (
        patch("backend.utils.tunnel.get_tunnel_manager", side_effect=RuntimeError("boom")),
        patch("backend.cron.schedule.build_cron_schedule_payload", side_effect=RuntimeError("boom")),
        patch("backend.core.metadata.cron_log.get_cron_runs", side_effect=RuntimeError("boom")),
        patch("backend.core.metadata.cron_log.latest_cron_per_task", side_effect=RuntimeError("boom")),
        patch("backend.scoring.labels.list_labels", side_effect=RuntimeError("boom")),
        patch("backend.routers.share_admin.build_share_status", side_effect=RuntimeError("boom")),
    ):
        response = _get(client)

    assert response.status_code == 200, (
        f"compound optional-resolver failure must not 500; got {response.status_code} body={response.text[:300]}"
    )
    data = response.json()
    # Every optional field at its documented default
    assert data.get("share_banner") is None
    assert data.get("cron_schedule") is None
    assert data.get("cron_runs_first_page") is None
    assert data.get("last_sync") is None
    assert data.get("scoring_labels") is None
    assert data.get("share_status") is None
    assert data["views"] == []
    # Core (non-optional) fields still populated
    assert data["active_service_id"] == MOCK_SERVICE_ID
    assert "services" in data
    assert "pops" in data
    assert "settings" in data
