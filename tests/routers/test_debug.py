"""Tests for the /api/debug router (SQLite ring-buffer surface)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.utils import sqlite_profiler


@pytest.fixture(autouse=True)
def _isolated_buffer():
    sqlite_profiler.clear()
    yield
    sqlite_profiler.clear()


def test_recent_sqlite_returns_empty_shape_when_buffer_empty():
    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 200
    body = res.json()
    assert body["queries"] == []
    assert body["buffer_size"] == 0
    assert body["buffer_cap"] > 0
    assert body["dropped"] == 0
    assert body["last_seq"] == 0


def test_recent_sqlite_surfaces_real_metadata_db_traffic():
    """Hitting any endpoint that touches metadata_db must populate the
    captured-statements ring buffer. This is the end-to-end contract the
    Debug Panel relies on."""
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("debug-test-svc")
    con.execute("SELECT 1")
    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 200
    body = res.json()
    assert body["buffer_size"] > 0
    sqls = [q["sql"] for q in body["queries"]]
    assert any("SELECT 1" in s for s in sqls)


def test_recent_sqlite_since_seq_filters():
    from backend.core import metadata as metadata_db

    con1 = metadata_db.get_con("svc-a")
    con1.execute("SELECT 1")
    client = TestClient(app)
    first = client.get("/api/debug/recent-sqlite").json()
    midpoint_seq = first["last_seq"]
    con2 = metadata_db.get_con("svc-b")
    con2.execute("SELECT 2")
    second = client.get(f"/api/debug/recent-sqlite?since_seq={midpoint_seq}").json()
    # Every returned entry must have seq > midpoint.
    assert all(q["seq"] > midpoint_seq for q in second["queries"])


def test_clear_sqlite_drains_buffer():
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("svc-clear-test")
    con.execute("SELECT 1")
    client = TestClient(app)
    assert client.get("/api/debug/recent-sqlite").json()["buffer_size"] > 0
    res = client.post("/api/debug/clear-sqlite")
    assert res.status_code == 200
    assert res.json()["ok"] is True
    # Buffer must be empty immediately after the POST returns.
    assert sqlite_profiler.get_recent()["buffer_size"] == 0


def test_recent_sqlite_respects_limit():
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("svc-limit")
    con.execute("SELECT 1")
    con.execute("SELECT 2")
    con.execute("SELECT 3")
    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite?limit=2")
    body = res.json()
    assert len(body["queries"]) <= 2
