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

    from backend.models.query import QueryRequest

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
    from backend.utils.remote_access import TimeBounds

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

    # The handler clamps via ctx.clamp, which reads ctx.time_bounds (what
    # build_request_context resolves from get_analyst_time_bounds in prod).
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )
    with patch("backend.repositories.query.execute_query", side_effect=_fake):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT ip FROM logs"},
        )

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
    fake_presets = [{"id": "p1", "name": "Top 5xx", "description": "Top 5xx URLs", "sql": "SELECT 1"}]
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


# ── End-to-end router + repository SELECT, AST safety, schema & masking ─────


def test_query_select_executes_against_duckdb(client, in_memory_duckdb, monkeypatch):
    """Valid SELECT statement executes against the service table and returns
    tabular columns, rows, and telemetry."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    in_memory_duckdb.execute(
        "CREATE TABLE IF NOT EXISTS logs_test_service (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR, status INTEGER)"
    )
    in_memory_duckdb.execute(
        "INSERT INTO logs_test_service (timestamp, ip, url, status) "
        "VALUES (TIMESTAMPTZ '2026-06-17T10:00:00Z', '203.0.113.42', '/index.html', 200)"
    )
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "SELECT url, status FROM logs"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["columns"] == ["url", "status"]
    assert body["row_count"] == 1
    assert body["data"][0] == {"url": "/index.html", "status": 200}
    assert isinstance(body["_debug_queries"], list) and len(body["_debug_queries"]) >= 1


@pytest.mark.parametrize(
    "unsafe_sql",
    [
        "INSERT INTO logs (url) VALUES ('/x')",
        "UPDATE logs SET status = 500",
        "DELETE FROM logs WHERE status = 200",
        "DROP TABLE logs",
        "ALTER TABLE logs ADD COLUMN extra VARCHAR",
        "ATTACH ':memory:' AS ext_db",
        "COPY logs TO '/tmp/out.parquet'",
    ],
)
def test_query_ast_safety_rejects_non_select_statements(client, unsafe_sql):
    """AST validator strictly rejects mutating and administrative SQL statements
    with HTTP 403 sql_not_permitted."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": unsafe_sql},
    )
    assert resp.status_code == 403, f"Expected 403 for {unsafe_sql!r}, got {resp.status_code}: {resp.text}"
    detail = resp.json()["detail"]
    assert detail["error"] == "sql_not_permitted"


def test_query_syntax_error_rejected_cleanly(client):
    """Malformed SQL syntax is rejected at parse time without leaking server paths."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "SELECT FROM WHERE ("},
    )
    assert resp.status_code in (400, 403)
    detail = resp.json()["detail"]
    assert detail["error"] in ("sql_not_permitted", "query_failed")
    assert "parse error" in detail["message"].lower() or "syntax error" in detail["message"].lower()


def test_query_wraps_execution_with_stale_view_retry(client):
    """Verify execute_with_stale_view_retry wraps /api/query execution."""
    with patch(
        "backend.core.iceberg.execute_with_stale_view_retry",
        return_value=_stub_result(),
    ) as mock_retry:
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    assert mock_retry.call_count == 1


def test_query_remote_analyst_masks_ips_end_to_end(client, in_memory_duckdb, test_service_source):
    """Analyst session with mask_ips=True redacts raw IP columns and masks IP-shaped values."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    in_memory_duckdb.execute(
        "CREATE TABLE IF NOT EXISTS logs_test_service (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR, status INTEGER)"
    )
    in_memory_duckdb.execute(
        "INSERT INTO logs_test_service (timestamp, ip, url, status) "
        "VALUES (TIMESTAMPTZ '2026-06-17T09:30:00Z', '198.51.100.77', '198.51.100.88', 200)"
    )
    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-mask",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )

    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "SELECT ip, url AS leaked_ip FROM logs"},
    )
    assert resp.status_code == 200, resp.text
    row = resp.json()["data"][0]
    assert row["ip"] == "[redacted]"
    assert row["leaked_ip"] == "198.51.100.xxx"


def test_query_high_scale_validates_ast_and_masks_ips(client, monkeypatch):
    """High-Scale /api/query enforces AST validation and IP masking."""
    from types import SimpleNamespace

    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    fake_hs_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}]  # the console's fail-closed check that additional_table_filters is enforced
            if "system.one" in sql
            else [{"client_ip": "203.0.113.9", "custom_fields['url']": "203.0.113.10"}]
        )
    )
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)

    with patch(
        "backend.high_scale.registry.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        # 1. Mutating query must 403
        drop_resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "DROP TABLE logs"},
        )
        assert drop_resp.status_code == 403
        assert drop_resp.json()["detail"]["error"] == "sql_not_permitted"

        # 2. Valid SELECT query succeeds and returns QueryResponse shape
        ok_resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) FROM logs"},
        )
        assert ok_resp.status_code == 200
        body = ok_resp.json()
        assert body["row_count"] == 1
        assert len(body["_debug_queries"]) == 1
        assert body["_debug_queries"][0]["engine"] == "ClickHouse"


def test_query_schema_endpoint_returns_columns(client):
    """GET /api/query/schema/{service_id} returns reflected schema columns."""
    with patch(
        "backend.config.get_status",
        return_value={"schema": [{"column": "timestamp", "type": "TIMESTAMPTZ"}, {"column": "url", "type": "VARCHAR"}]},
    ):
        resp = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "test_service"
    assert len(body["schema"]) == 2
    assert body["schema"][0]["column"] == "timestamp"


def test_query_schema_endpoint_falls_back_when_empty(client):
    """GET /api/query/schema/{service_id} returns fallback columns when live schema is empty."""
    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.duckdb.get_schema", return_value=[]),
    ):
        resp = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    col_names = {c["column"] for c in body["schema"]}
    assert {"timestamp", "url", "status"}.issubset(col_names)


@pytest.mark.security_regression
def test_query_high_scale_is_closed_to_analysts_until_clickhouse_enforces_the_boundary(
    client, in_memory_duckdb, test_service_source
):
    """Analysts are an adversary: the console's name-based guards cannot be the only thing between them and ClickHouse."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    calls: list[str] = []
    fake_hs_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)
    session = SimpleNamespace(
        session_id="sess-hs",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime(2026, 6, 17, 9, 0, tzinfo=UTC), end=datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
        ),
    )

    with patch(
        "backend.high_scale.registry.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) FROM logs"},
        )

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert calls == [], "nothing may reach ClickHouse for an analyst"


def _analyst_on_high_scale(client, in_memory_duckdb, test_service_source, main_calls, console, mask_ips=True):
    """Post a console query as an analyst against a fake high-scale service; ``console`` is the stand-in manager."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    main_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: main_calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=main_client)
    session = SimpleNamespace(
        session_id="sess-hs2", pii_policy={"mask_ips": mask_ips}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime(2026, 6, 17, 9, 0, tzinfo=UTC), end=datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
        ),
    )
    with (
        patch(
            "backend.high_scale.registry.get_high_scale_service_registry",
            return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
        ),
        patch("backend.high_scale.console_access.get_console_clients", return_value=console),
    ):
        return client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) AS n FROM logs"},
        )


@pytest.mark.security_regression
def test_analyst_console_runs_on_the_services_own_least_privilege_identity_through_its_views(
    client, in_memory_duckdb, test_service_source
):
    from types import SimpleNamespace

    seen: list[tuple[str, dict]] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}] if "system.one" in sql else seen.append((sql, dict(settings or {}))) or [{"n": 7}]
        )
    )
    asked: list[bool] = []

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    main_calls: list[str] = []

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=client_for)
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [{"n": 7}]
    sql, settings = seen[0]
    assert "fla_cv_view_req" in sql and "request_facts" not in sql, "analysts read through their view only"
    scope = settings["additional_table_filters"]
    assert "event_timestamp >=" in scope and "service_id" not in scope, "the view fixes the service; only the window"
    assert main_calls == [], "and never on the application's privileged ClickHouse user"
    assert asked == [True], "a mask_ips invite must get the masked identity"


@pytest.mark.security_regression
def test_an_invite_without_mask_ips_gets_the_raw_identity(client, in_memory_duckdb, test_service_source):
    from types import SimpleNamespace

    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, [], SimpleNamespace(client_for=client_for), mask_ips=False
    )

    assert resp.status_code == 200, resp.text
    assert asked == [False]


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_no_console_identity_is_available(
    client, in_memory_duckdb, test_service_source
):
    from types import SimpleNamespace

    main_calls: list[str] = []
    console = SimpleNamespace(client_for=lambda sid: None)

    resp = _analyst_on_high_scale(client, in_memory_duckdb, test_service_source, main_calls, console)

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_the_identity_fails_its_isolation_check(
    client, in_memory_duckdb, test_service_source
):
    from types import SimpleNamespace

    def refuse(sid):
        raise RuntimeError("console identity failed the row isolation check")

    main_calls: list[str] = []

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=refuse)
    )

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("policy", "expected_masked"),
    [
        (None, True),
        ({}, True),
        ({"mask_ips": None}, True),
        ({"mask_ips": "false"}, True),
        ({"mask_ips": True}, True),
        ({"mask_ips": False}, False),
    ],
)
def test_the_console_picks_the_masked_identity_unless_the_invite_explicitly_disables_masking(
    client, in_memory_duckdb, test_service_source, policy, expected_masked
):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    session = SimpleNamespace(session_id="s", pii_policy=policy, service_ids=[test_service_source["service_id"]])
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime(2026, 6, 17, 9, 0, tzinfo=UTC), end=datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
        ),
    )
    hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=lambda *a, **k: []))
    with (
        patch(
            "backend.high_scale.registry.get_high_scale_service_registry",
            return_value=SimpleNamespace(resolve=lambda sid: hs_service),
        ),
        patch(
            "backend.high_scale.console_access.get_console_clients",
            return_value=SimpleNamespace(client_for=client_for),
        ),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) AS n FROM logs"},
        )

    assert resp.status_code == 200, resp.text
    assert asked == [expected_masked]
