"""Tests for the /api/admin/debug-settings router and related access-control checks."""

from __future__ import annotations

import pytest
import starlette.datastructures
from fastapi.testclient import TestClient

from backend.core.share_db.settings import get_setting, set_setting
from backend.main import app

# Global flag to control the mocked is_remote state of Starlette requests during tests
_is_remote_flag = False

original_getattr = starlette.datastructures.State.__getattr__


def mock_state_getattr(self, key: str):
    if key == "is_remote":
        return _is_remote_flag
    try:
        return original_getattr(self, key)
    except (AttributeError, KeyError):
        raise AttributeError(f"'State' object has no attribute '{key}'")


@pytest.fixture(autouse=True)
def patch_state(monkeypatch):
    monkeypatch.setattr(starlette.datastructures.State, "__getattr__", mock_state_getattr)


def test_get_and_patch_debug_settings():
    global _is_remote_flag
    _is_remote_flag = False
    client = TestClient(app)

    # 1. Fetch initial settings (should default to disabled)
    res = client.get("/api/admin/debug-settings")
    assert res.status_code == 200
    body = res.json()
    assert body["query_debug_visibility"] == "disabled"
    assert body["api_call_debug_visibility"] == "disabled"

    # 2. Update settings via PATCH
    patch_res = client.patch(
        "/api/admin/debug-settings",
        json={
            "query_debug_visibility": "both",
            "api_call_debug_visibility": "analysts",
        },
    )
    assert patch_res.status_code == 200
    patched_body = patch_res.json()
    assert patched_body["query_debug_visibility"] == "both"
    assert patched_body["api_call_debug_visibility"] == "analysts"

    # Verify database settings were updated
    assert get_setting("query_debug_visibility") == "both"
    assert get_setting("api_call_debug_visibility") == "analysts"

    # Clean up settings
    set_setting("query_debug_visibility", "disabled")
    set_setting("api_call_debug_visibility", "disabled")


def test_recent_sqlite_is_remote_blocked():
    global _is_remote_flag
    _is_remote_flag = True
    set_setting("query_debug_visibility", "disabled")

    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 403
    detail = res.json()["detail"]
    assert detail["error"] == "sql_debug_disabled"
    assert "SQL debug is disabled for analysts" in detail["message"]


def test_recent_sqlite_is_remote_allowed_when_visibility_both():
    global _is_remote_flag
    _is_remote_flag = True
    set_setting("query_debug_visibility", "both")

    client = TestClient(app)
    res = client.get("/api/debug/recent-sqlite")
    assert res.status_code == 200

    set_setting("query_debug_visibility", "disabled")


def test_clear_sqlite_is_remote_always_blocked():
    global _is_remote_flag
    _is_remote_flag = True

    client = TestClient(app)
    res = client.post("/api/debug/clear-sqlite")
    assert res.status_code == 403
    detail = res.json()["detail"]
    assert detail["error"] == "admin_only"
    assert "Only administrators can clear the SQLite debug buffer" in detail["message"]
