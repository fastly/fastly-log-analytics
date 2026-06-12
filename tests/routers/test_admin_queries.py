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
        assert resp.json().get("detail") == "query_monitor_disabled"

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

    def test_since_seq_filters_older_rows(self, client):
        query_registry._queries.clear()
        a = query_registry.register("SQLite", "A", con=None)
        b = query_registry.register("SQLite", "B", con=None)

        resp = client.get(f"/api/admin/queries?since_seq={a}")
        body = resp.json()
        ids = [r["query_id"] for r in body["active"]]
        assert b in ids
        assert a not in ids
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
        assert resp.json().get("detail") == "query_not_found"

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
        from backend.routers import admin_queries as mod

        mod._cancel_history.clear()
        try:
            # Fire 11 requests in rapid succession to one qid (which won't
            # exist — irrelevant, the rate-limiter runs before the registry).
            last_status = None
            for _ in range(11):
                resp = client.post("/api/admin/queries/9999999/cancel")
                last_status = resp.status_code
            # The 11th should trip the limiter (10 per second per admin id).
            assert last_status == 429
        finally:
            mod._cancel_history.clear()


# ── Admin-id derivation ────────────────────────────────────────────────────


class TestAdminIdHelper:
    def test_admin_id_prefers_x_forwarded_for(self):
        from backend.routers.admin_queries import _admin_id_from_request

        class FakeReq:
            headers = {"x-forwarded-for": "10.1.2.3, 192.168.0.1"}
            client = None

        assert _admin_id_from_request(FakeReq()) == "10.1.2.3"

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
