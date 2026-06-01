"""Router-level contract tests for ``/api/query`` and ``/api/presets``.

The happy path for ``/api/query`` is already pinned in
[tests/routers/test_pages.py](tests/routers/test_pages.py); this file
exercises the *error branches* and the ``/api/presets`` endpoint that
``test_pages.py`` doesn't touch.

The router translates a few specific exceptions from the repository
layer into HTTP status codes — that mapping is what the frontend's
query editor depends on to distinguish "you don't have access to this
table" (403) from "syntax error in your SQL" (400), so the mapping
itself is the contract.
"""

from __future__ import annotations

from unittest.mock import patch

from tests.conftest import MOCK_SERVICE_ID

# ── /api/query: input validation + exception → HTTP mapping ─────────────────


def test_query_empty_sql_returns_400(client):
    """Empty SQL → 400 with structured error (not a generic 422)."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": ""},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "No SQL provided"


def test_query_whitespace_only_sql_returns_400(client):
    """Whitespace counts as empty — the router strips before checking
    so the frontend can't accidentally submit a single newline and
    blow up downstream."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "   \n\t"},
    )
    assert resp.status_code == 400


def test_query_permission_error_maps_to_403(client):
    """``PermissionError`` from the repo → 403 (not 400). The frontend
    distinguishes these for the "access denied" UI affordance."""
    with patch(
        "backend.repositories.query.execute_query",
        side_effect=PermissionError("not authorized for system catalog"),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )

    assert resp.status_code == 403
    assert "not authorized" in resp.json()["detail"]["error"]


def test_query_unexpected_exception_maps_to_400(client):
    """Any other exception (syntax error, missing table, etc.) → 400
    with the exception message in ``detail.error``."""
    with patch(
        "backend.repositories.query.execute_query",
        side_effect=RuntimeError("table 'nope' does not exist"),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT * FROM nope"},
        )

    assert resp.status_code == 400
    assert "does not exist" in resp.json()["detail"]["error"]


# ── /api/presets: source lookup + connection fallback ───────────────────────


def test_presets_no_service_id_returns_empty_list(client):
    """No header AND no configured active service → ``[]`` (don't 500).
    The frontend pre-fetches presets before a service is selected; the
    only way to hit this branch is on a freshly-provisioned install."""
    # get_service_id falls back to svcconfig.get_active_service_id() when
    # no header/query is set, so we have to pin both to None.
    with patch("backend.deps.svcconfig.get_active_service_id", return_value=None):
        resp = client.get("/api/presets")
    assert resp.status_code == 200
    assert resp.json() == []


def test_presets_unknown_service_returns_empty_list(client):
    """Service id present but ``get_source_for_service`` returns None
    → ``[]``. Prevents a 500 when an admin yanks a service while the
    frontend still has its id in the URL."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": "ghost-service"})

    assert resp.status_code == 200
    assert resp.json() == []


def test_presets_returns_repo_output_when_source_resolves(client):
    """Source resolves → router calls ``repo.get_presets`` and returns
    whatever it produces. The router itself doesn't shape the payload
    — that's the repo's job."""
    fake_presets = [{"id": "p1", "name": "Top 5xx", "sql": "SELECT 1"}]
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("connection unavailable")),
        patch("backend.repositories.query.get_presets", return_value=fake_presets),
    ):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert resp.json() == fake_presets


def test_presets_connection_failure_falls_back_to_no_connection(client):
    """If ``get_connection`` raises (lock contention, missing DB file),
    the endpoint falls back to ``get_presets(src=..., con=None)``.
    Pinned because the fallback is the difference between a working
    sidebar and a broken one when the analytical DB is busy."""
    captured: dict = {}

    def _capture_get_presets(src, con):
        captured["con"] = con
        return []

    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("locked")),
        patch("backend.repositories.query.get_presets", side_effect=_capture_get_presets),
    ):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert captured["con"] is None
