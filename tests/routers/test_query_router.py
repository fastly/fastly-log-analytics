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

import pytest

from tests.conftest import MOCK_SERVICE_ID, override_request_context


def _stub_result() -> dict:
    """Minimal execute_query return shape so the router can serialize it."""
    return {
        "columns": [],
        "data": [],
        "row_count": 0,
        "total_rows": 0,
        "truncated": False,
        "elapsed_ms": 0,
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }


# ── /api/query: input validation + exception → HTTP mapping ─────────────────


def test_query_empty_sql_returns_400(client):
    """Empty SQL → 400 with structured error (not a generic 422)."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": ""},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "empty_sql"
    assert resp.json()["detail"]["message"] == "No SQL provided"


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
    distinguishes these for the "access denied" UI affordance.

    Unified envelope: ``detail.error`` is the machine code
    (``sql_not_permitted``) and the human-readable message lives in
    ``detail.message``."""
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
    body = resp.json()["detail"]
    assert body["error"] == "sql_not_permitted"
    assert "not authorized" in body["message"]


def test_query_unexpected_exception_maps_to_400(client):
    """Any other exception (syntax error, missing table, etc.) → 400 with
    the exception text in ``detail.message`` (after path redaction) and
    a stable machine code in ``detail.error``."""
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
    body = resp.json()["detail"]
    assert body["error"] == "query_failed"
    assert "does not exist" in body["message"]


def test_query_io_error_redacts_filesystem_paths(client):
    """DuckDB IO Errors interpolate absolute paths (``Cannot open file
    '/srv/data/buffers/...'``). The router must strip them before echoing
    so the wire payload doesn't leak server-side directory layout, while
    still keeping the SQL diagnostic context."""
    with patch(
        "backend.repositories.query.execute_query",
        # Two attempts: first 'Cannot open file' triggers the inline
        # retry; second non-match raises through.
        side_effect=[
            RuntimeError("Cannot open file '/srv/data/buffers/x.parquet'"),
            RuntimeError("IO Error: file '/srv/data/iceberg/snap.json' is corrupt"),
        ],
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )

    assert resp.status_code == 400
    body = resp.json()["detail"]
    assert body["error"] == "query_failed"
    assert "/srv/" not in body["message"], "absolute paths must be redacted"
    assert "<path>" in body["message"]


# ── _redact_paths: regex shape ──────────────────────────────────────────────


def test_redact_paths_strips_deploy_root_prefixes():
    """The redaction regex anchors on a conservative list of Unix root
    prefixes (srv|var|tmp|data|home|opt|usr|mnt|app). Each is a documented
    location DuckDB / Iceberg / our code emits in exception strings. ``app``
    is the prod container WORKDIR (/app/data/services/<sid>/...)."""
    from backend.routers.query import _redact_paths

    for prefix in ("/srv", "/var", "/tmp", "/data", "/home", "/opt", "/usr", "/mnt", "/app"):
        msg = f"Cannot open file '{prefix}/foo/bar.parquet'"
        out = _redact_paths(msg)
        assert prefix not in out, f"{prefix} should have been redacted; got: {out!r}"
        assert "<path>" in out


def test_redact_paths_strips_prod_data_root():
    """Regression: the real prod data root is /app/data/services/<sid>/...
    (backend/Dockerfile WORKDIR=/app). Before ``app`` was added to the
    allowlist these absolute paths reached the analyst un-redacted because
    the ``/data`` token in ``/app/data`` is preceded by an alnum and so
    failed the leading-boundary lookbehind."""
    from backend.routers.query import _redact_paths

    msg = "IO Error: No files found that match '/app/data/services/abc123/raw/2026-06-17/x.parquet'"
    out = _redact_paths(msg)
    assert "/app/data" not in out, f"prod data root must be redacted; got: {out!r}"
    assert "abc123" not in out, "service id must not leak"
    assert "<path>" in out


def test_redact_paths_preserves_url_path_segments_that_look_like_server_paths():
    """https://example.com/var/foo is NOT a leak — the /var here is part
    of a URL path. The previous over-broad ``/[A-Za-z0-9_./-]+`` pattern
    stripped it (lost URL context inside SQL error messages); the
    anchored variant requires the leading slash to NOT be preceded by an
    alphanumeric, so URL paths survive."""
    from backend.routers.query import _redact_paths

    msg = "lookup failed for https://example.com/var/foo on row 12"
    out = _redact_paths(msg)
    assert out == msg, "URL path should NOT have been redacted"


def test_redact_paths_leaves_non_root_paths_alone():
    """/api/foo (column ref) and /etc/hosts (non-allowlisted root) both
    stay intact. If a real /etc leak shows up in prod we'll add it; the
    conservative default is to err on the side of preserving diagnostic
    context."""
    from backend.routers.query import _redact_paths

    for msg in ("column /api/foo missing", "ENOENT: /etc/hosts not found"):
        assert _redact_paths(msg) == msg, f"unexpected redaction in: {msg!r}"


def test_redact_paths_strips_quoted_and_whitespace_prefixed_paths():
    """The lookbehind allows whitespace, quotes, and line-start — all the
    contexts DuckDB / Python tracebacks put a leaked path in."""
    from backend.routers.query import _redact_paths

    cases = [
        ("Cannot open '/var/lib/duckdb/foo.db'", "/var/lib/duckdb/foo.db"),
        ('No such file: "/srv/cache/x.parquet"', "/srv/cache/x.parquet"),
        ("Path was /tmp/buffer.json on disk", "/tmp/buffer.json"),
        ("/data/services/svc.metadata.db corrupted", "/data/services/svc.metadata.db"),
    ]
    for msg, leaked in cases:
        out = _redact_paths(msg)
        assert leaked not in out, f"path {leaked!r} survived in: {out!r}"


# ── M1: max_rows model bound ────────────────────────────────────────────────


def test_query_request_rejects_oversized_max_rows():
    """M1: the model caps max_rows at 10_000 (was unbounded → OOM lever)."""
    from pydantic import ValidationError

    from backend.models.dashboard import QueryRequest

    with pytest.raises(ValidationError):
        QueryRequest(sql="SELECT 1", max_rows=10_001)
    with pytest.raises(ValidationError):
        QueryRequest(sql="SELECT 1", max_rows=0)
    assert QueryRequest(sql="SELECT 1", max_rows=10_000).max_rows == 10_000


# ── H1/H2 wiring: router forwards window + mask flag to execute_query ────────


def test_admin_request_passes_no_window_and_no_mask(client):
    """An admin request (no analyst session on the context) must reach the
    repo with ``time_filter=None`` (full range) and ``mask_ips=False``."""
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_result()

    with patch("backend.repositories.query.execute_query", side_effect=_fake):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )

    assert resp.status_code == 200, resp.text
    assert captured["time_filter"] is None
    assert captured["mask_ips"] is False


def test_analyst_request_forwards_clamped_window_and_mask(client, in_memory_duckdb, test_service_source):
    """An analyst session → the router clamps to the session window and
    forwards the concrete bounds + the mask_ips flag to execute_query."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds, get_analyst_time_bounds

    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-1",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_result()

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, con=in_memory_duckdb, session=session, path="/api/query"
    )
    app.dependency_overrides[get_analyst_time_bounds] = lambda: TimeBounds(start=start, end=end)
    try:
        with patch("backend.repositories.query.execute_query", side_effect=_fake):
            resp = client.post(
                "/api/query",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"sql": "SELECT ip FROM logs"},
            )
    finally:
        # The client fixture clears overrides on teardown; pop the extra one
        # we added so it can't bleed into another test in the same worker.
        app.dependency_overrides.pop(get_analyst_time_bounds, None)

    assert resp.status_code == 200, resp.text
    assert captured["time_filter"] == (start.isoformat(), end.isoformat())
    assert captured["mask_ips"] is True


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
        patch("backend.repositories.query.get_presets", return_value=fake_presets),
    ):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert resp.json() == fake_presets


def test_presets_does_not_acquire_a_duckdb_connection(client):
    """Presets are pure SQL templates derived from the service name
    — the endpoint must NOT call ``get_connection``, which would pay
    extension-install / pool-wait cost on cache miss for no benefit."""
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.core.duckdb.get_connection") as mock_get_connection,
        patch("backend.repositories.query.get_presets", return_value=[]),
    ):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert mock_get_connection.call_count == 0
