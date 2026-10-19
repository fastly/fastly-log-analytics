"""Tests for ``backend.routers.admin_queries`` — the Live Query Monitor's
admin API surface.

Targets the endpoints' behavior at the HTTP layer (FastAPI dependency
resolution, response shapes, feature flag, rate limiting) — the registry
unit-tests in ``tests/core/test_query_registry.py`` cover the underlying
data model. Together they reach ≥ 80% coverage on the router file per the
cleanup plan's coverage commitment.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from backend.core.query_attribution import Attribution, current_attribution
from backend.core.query_registry import query_registry


@pytest.fixture(autouse=True)
def _reset_registry():
    """Snapshot + restore the singleton's internal state so tests don't
    bleed into each other. Mirrors the conftest in test_query_registry.py."""
    queries = dict(query_registry._queries)
    history = list(query_registry._history)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(queries)
    query_registry._history.clear()
    query_registry._history.extend(history)


# ── Feature flag (QUERY_MONITOR_ENABLED) ────────────────────────────────────


class TestFeatureFlag:
    def test_app_config_endpoint_returns_enabled_when_unset(self, client):
        # No env override — defaults to True (see settings.py).
        resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200
        body = resp.json()
        assert body["enabled"] is True

    def test_app_config_endpoint_returns_false_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/app-config/query-monitor")
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    def test_queries_endpoint_returns_404_when_disabled(self, client):
        # Endpoints flip to 404 (not 503) so the frontend treats the
        # feature as absent rather than broken — matches the comment in
        # admin_queries._ensure_enabled.
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/queries")
        assert resp.status_code == 404
        assert resp.json().get("detail") == {"error": "query_monitor_disabled"}

    def test_summary_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.get("/api/admin/queries/summary")
        assert resp.status_code == 404

    def test_cancel_endpoint_returns_404_when_disabled(self, client):
        with patch.dict(os.environ, {"QUERY_MONITOR_ENABLED": "0"}):
            resp = client.post("/api/admin/queries/1/cancel")
        assert resp.status_code == 404


# ── Snapshot endpoint shape ─────────────────────────────────────────────────


class TestSnapshotEndpoint:
    def test_empty_state_returns_zero_active(self, client):
        # Clear registry for deterministic empty state.
        query_registry._queries.clear()
        query_registry._history.clear()
        resp = client.get("/api/admin/queries")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active"] == []
        assert body["completed"] == []
        assert body["last_seq"] == 0

    def test_active_query_appears_in_snapshot(self, client):
        query_registry._queries.clear()
        prev = current_attribution.get()
        current_attribution.set(Attribution.admin(admin_id="t", request_path="/api/x", request_id=None))
        try:
            qid = query_registry.register("DuckDB", "SELECT 1", con=None)
        finally:
            current_attribution.set(prev)

        resp = client.get("/api/admin/queries")
        assert resp.status_code == 200
        body = resp.json()
        ids = [r["query_id"] for r in body["active"]]
        assert qid in ids
        row = next(r for r in body["active"] if r["query_id"] == qid)
        assert row["db_type"] == "DuckDB"
        assert row["attribution"]["kind"] == "admin"
        # SQL preview, not full SQL on this endpoint
        assert row["sql"] is None
        assert "SELECT 1" in row["sql_preview"]
        query_registry.deregister(qid)

    def test_include_completed_returns_history(self, client):
        query_registry._queries.clear()
        query_registry._history.clear()
        qid = query_registry.register("SQLite", "DROP TABLE x", con=None)
        query_registry.deregister(qid)

        resp = client.get("/api/admin/queries?include_completed=true")
        body = resp.json()
        assert any(r["query_id"] == qid for r in body["completed"])

    def test_since_seq_keeps_active_visible_across_polls(self, client):
        """The /api/admin/queries endpoint must keep every currently-active
        query visible regardless of since_seq — the delta-poll UI tracks
        only the highest seq it's seen, and a long-running query with id
        below that watermark must NOT disappear from subsequent polls.

        (since_seq still filters the completed list — that's where the
        wire-byte savings come from.)
        """
        query_registry._queries.clear()
        a = query_registry.register("SQLite", "A", con=None)
        b = query_registry.register("SQLite", "B", con=None)

        resp = client.get(f"/api/admin/queries?since_seq={a}")
        body = resp.json()
        ids = [r["query_id"] for r in body["active"]]
        assert b in ids
        assert a in ids, "active query 'a' must remain visible even though its id is below since_seq"
        query_registry.deregister(a)
        query_registry.deregister(b)


# ── Summary endpoint ────────────────────────────────────────────────────────


class TestSummaryEndpoint:
    def test_summary_shape(self, client):
        query_registry._queries.clear()
        resp = client.get("/api/admin/queries/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active_total"] == 0
        assert body["by_db_type"] == {}
        assert body["longest_ms"] == 0.0

    def test_summary_counts_active_queries(self, client):
        query_registry._queries.clear()
        a = query_registry.register("DuckDB", "SELECT 1", con=None)
        b = query_registry.register("SQLite", "SELECT 2", con=None)
        resp = client.get("/api/admin/queries/summary")
        body = resp.json()
        assert body["active_total"] == 2
        assert body["by_db_type"]["DuckDB"] == 1
        assert body["by_db_type"]["SQLite"] == 1
        assert body["longest_ms"] >= 0
        query_registry.deregister(a)
        query_registry.deregister(b)


# ── Per-query detail endpoint ───────────────────────────────────────────────


class TestPerQueryEndpoint:
    def test_unknown_qid_returns_404(self, client):
        resp = client.get("/api/admin/queries/9999999")
        assert resp.status_code == 404
        assert resp.json().get("detail") == {"error": "query_not_found"}

    def test_known_qid_returns_full_row_with_sql(self, client):
        query_registry._queries.clear()
        qid = query_registry.register("DuckDB", "SELECT something_specific_xyz", con=None)
        resp = client.get(f"/api/admin/queries/{qid}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["query_id"] == qid
        # /queries returns sql=None; /queries/{qid} returns the full SQL
        assert body["sql"] is not None
        assert "something_specific_xyz" in body["sql"]
        query_registry.deregister(qid)


# ── Cancel endpoint ─────────────────────────────────────────────────────────


class TestCancelEndpoint:
    def test_cancel_unknown_qid_returns_state_not_found(self, client):
        resp = client.post("/api/admin/queries/9999999/cancel")
        # Always 200 with structured state — endpoint is idempotent.
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "not_found"
        assert body["query_id"] == 9999999

    def test_cancel_query_with_no_connection_returns_already_finished(self, client):
        qid = query_registry.register("SQLite", "SELECT 1", con=None)
        resp = client.post(f"/api/admin/queries/{qid}/cancel")
        assert resp.status_code == 200
        body = resp.json()
        # Registered with con=None → no cancellable handle.
        assert body["state"] == "already_finished"
        query_registry.deregister(qid)

    def test_cancel_rate_limit_kicks_in_after_10_per_second(self, client):
        # Reset rate-limit history before AND after — the bucket is
        # process-global keyed on admin id, and the test client uses the
        # default testserver client.host, so without the post-test reset
        # the next test in the same xdist worker would inherit a primed
        # bucket and see a spurious 429.
        from unittest.mock import patch

        from backend.routers import admin_queries as mod

        mod._cancel_history.clear()
        try:
            # Fire 11 requests in rapid succession to one qid (which won't
            # exist — irrelevant, the rate-limiter runs before the registry).
            last_status = None
            with patch("backend.routers.admin_queries.time.monotonic", return_value=1000.0):
                for _ in range(11):
                    resp = client.post("/api/admin/queries/9999999/cancel")
                    last_status = resp.status_code
            # The 11th should trip the limiter (10 per second per admin id).
            assert last_status == 429
        finally:
            mod._cancel_history.clear()


# ── Admin-id derivation ────────────────────────────────────────────────────


class TestAdminIdHelper:
    def test_admin_id_falls_back_to_client_host(self):
        from backend.routers.admin_queries import _admin_id_from_request

        class FakeClient:
            host = "127.0.0.1"

        class FakeReq:
            headers = {}
            client = FakeClient()

        assert _admin_id_from_request(FakeReq()) == "127.0.0.1"

    def test_admin_id_fallback_when_no_client(self):
        from backend.routers.admin_queries import _admin_id_from_request

        class FakeReq:
            headers = {}
            client = None

        # The `or` chain returns "unknown" (truthy) so the further `or
        # "admin"` fallback is unreachable in practice — this test pins
        # the actual behavior so a future refactor of the helper doesn't
        # silently lose the no-client guard.
        assert _admin_id_from_request(FakeReq()) == "unknown"


# ── Cancellation with active handle + audit logging ────────────────────────


class TestCancelWithHandleAndAudit:
    def test_cancel_cancellable_duckdb_query_interrupts_and_records_audit(self, client):
        import time as _time

        from backend.core import metadata as _meta

        svc_id = "test_svc_cancel_audit"
        _meta.clear_slow_queries(svc_id)

        interrupted = []

        class FakeDuckDBCon:
            def interrupt(self):
                interrupted.append(True)

        fake_con = FakeDuckDBCon()
        prev = current_attribution.get()
        current_attribution.set(
            Attribution.admin(
                admin_id="127.0.0.1",
                request_path="/api/query",
                request_id="req-cancel-1",
            )
        )
        try:
            qid = query_registry.register("DuckDB", "SELECT * FROM logs", con=fake_con, service_id=svc_id)
        finally:
            current_attribution.set(prev)

        resp = client.post(f"/api/admin/queries/{qid}/cancel")
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "cancelled"
        assert body["query_id"] == qid
        assert interrupted == [True]

        query_registry.deregister(qid, error=RuntimeError("Query interrupted"))
        # Verify audit log entry was written for the service
        audits = _meta.list_audit(svc_id, limit=20)
        assert any(a["event_type"] == "query_cancelled" for a in audits)
        _ = _time.time()


# ── Persisted slow-queries endpoints (PostgreSQL METADATA_DSN) ─────────────


class TestSlowQueriesEndpoints:
    def test_list_count_and_clear_slow_queries(self, client):
        import time as _time

        from backend.core import metadata as _meta

        svc_id = "test-service-id"
        _meta.clear_slow_queries(svc_id)
        now = _time.time()

        _meta.insert_slow_query(
            svc_id,
            {
                "query_id": 7001,
                "db_type": "DuckDB",
                "service_id": svc_id,
                "started_at_utc": now - 120.0,
                "ended_at_utc": now - 118.2,
                "duration_ms": 1800.0,
                "outcome": "ok",
                "sql_preview": "SELECT count(*) FROM logs",
                "sql_full": "SELECT count(*) FROM logs WHERE status >= 500",
                "sql_len": 46,
                "attr_kind": "admin",
                "attr_label": "admin @ 127.0.0.1",
                "attr_principal_id": "127.0.0.1",
                "attr_caller_qualname": "dashboard.aggregates",
                "attr_caller_file": "backend/routers/dashboard.py:42",
                "attr_request_path": "/api/dashboard/aggregates",
                "attr_request_id": "req-slow-1",
                "attr_cron_job": None,
                "attr_cron_run_id": None,
                "attr_pool_slot": "pool#1",
                "error_type": None,
                "error_message": None,
                "peak_memory_mb": 64.5,
            },
        )
        _meta.insert_slow_query(
            svc_id,
            {
                "query_id": 7002,
                "db_type": "Postgres",
                "service_id": svc_id,
                "started_at_utc": now - 60.0,
                "ended_at_utc": now - 59.5,
                "duration_ms": 500.0,
                "outcome": "ok",
                "sql_preview": "SELECT * FROM ingested_files",
                "sql_full": "SELECT * FROM ingested_files WHERE service_id = $1",
                "sql_len": 50,
                "attr_kind": "cron",
                "attr_label": "cron:full_sync",
                "attr_principal_id": None,
                "attr_caller_qualname": "ingest_log.list_ingested_files",
                "attr_caller_file": "backend/core/metadata/ingest_log.py:100",
                "attr_request_path": None,
                "attr_request_id": None,
                "attr_cron_job": "full_sync",
                "attr_cron_run_id": 99,
                "attr_pool_slot": None,
                "error_type": None,
                "error_message": None,
                "peak_memory_mb": None,
            },
        )

        # Count with default 1000ms threshold -> only query 7001 qualifies
        cnt_resp = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=1000")
        assert cnt_resp.status_code == 200
        cnt_body = cnt_resp.json()
        assert cnt_body["count"] == 1
        assert cnt_body["since_hours"] == 24
        assert cnt_body["threshold_ms"] == 1000.0

        # List with 100ms threshold -> both rows returned
        list_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&sort=duration"
        )
        assert list_resp.status_code == 200
        list_body = list_resp.json()
        assert len(list_body["rows"]) == 2
        assert list_body["rows"][0]["query_id"] == 7001
        assert list_body["rows"][0]["sql"] == "SELECT count(*) FROM logs WHERE status >= 500"
        assert list_body["rows"][0]["attribution"]["kind"] == "admin"
        assert list_body["rows"][0]["attribution"]["pool_slot"] == "pool#1"
        assert list_body["rows"][1]["attribution"]["cron_run_id"] == "99"

        # Filter by db_type=Postgres
        pg_resp = client.get(
            f"/api/admin/slow-queries?service_id={svc_id}&since_hours=24&threshold_ms=100&db_type=Postgres"
        )
        assert pg_resp.status_code == 200
        pg_rows = pg_resp.json()["rows"]
        assert len(pg_rows) == 1
        assert pg_rows[0]["query_id"] == 7002
        assert pg_rows[0]["db_type"] == "Postgres"

        # Clear persisted slow queries via DELETE /api/admin/slow-queries
        del_resp = client.delete(f"/api/admin/slow-queries?service_id={svc_id}")
        assert del_resp.status_code == 200
        del_body = del_resp.json()
        assert del_body["deleted"] == 2
        assert del_body["service_id"] == svc_id

        # Verify count is now 0
        after_cnt = client.get(f"/api/admin/slow-queries/count?service_id={svc_id}&since_hours=24&threshold_ms=100")
        assert after_cnt.status_code == 200
        assert after_cnt.json()["count"] == 0


# ── Analyst Path B RBAC rejection (403 Forbidden) ──────────────────────────


class TestAnalystRbacRejection:
    @pytest.mark.parametrize(
        "method,path",
        [
            ("GET", "/api/admin/queries"),
            ("GET", "/api/admin/queries/summary"),
            ("GET", "/api/admin/queries/1"),
            ("POST", "/api/admin/queries/1/cancel"),
            ("GET", "/api/admin/slow-queries?service_id=svc1"),
            ("GET", "/api/admin/slow-queries/count?service_id=svc1"),
            ("DELETE", "/api/admin/slow-queries?service_id=svc1"),
            ("GET", "/api/admin/app-config/query-monitor"),
        ],
    )
    def test_analyst_session_receives_403_on_all_query_monitor_routes(self, client, method, path):
        from types import SimpleNamespace

        from fastapi import HTTPException

        from backend.deps import require_admin
        from backend.utils.remote_access import _is_blocked_path

        raw_path = path.split("?", 1)[0]
        assert _is_blocked_path(raw_path) is True, f"Expected {raw_path} to be blocked for Analyst Path B"

        fake_req = SimpleNamespace(
            state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst_qmon")),
            headers={},
            client=SimpleNamespace(host="203.0.113.55"),
        )
        with pytest.raises(HTTPException) as exc:
            require_admin(fake_req)  # type: ignore[arg-type]
        assert exc.value.status_code == 403

        # Also verify HTTP-layer rejection when require_admin evaluates an Analyst Path B session
        def _analyst_override():
            require_admin(fake_req)  # type: ignore[arg-type]

        client.app.dependency_overrides[require_admin] = _analyst_override
        try:
            resp = client.request(method, path)
            assert resp.status_code == 403
        finally:
            client.app.dependency_overrides.pop(require_admin, None)
