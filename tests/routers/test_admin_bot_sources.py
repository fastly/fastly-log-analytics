"""Tests for bot sources admin endpoints (backend.routers.admin.bot_sources).

Covers:
  - GET /api/admin/bot-sources
  - POST /api/admin/bot-sources/{source_id}/refresh
  - POST /api/admin/bot-sources/refresh
  - POST /api/admin/bots/refresh
  - POST /api/admin/rdns/enrich
  - GET /api/admin/rdns/stats
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


# ── GET /api/admin/bot-sources ────────────────────────────────────────────────


def test_get_bot_sources_returns_sources_and_rdns_stats():
    """Returns source metadata list and rDNS cache statistics."""
    fake_sources = [
        {
            "id": "well-known-bots",
            "name": "Well-Known Bots (arcjet)",
            "url": "https://example.com/bots.json",
            "enabled": True,
            "last_updated": "2026-10-11T00:00:00Z",
            "entry_count": 42,
        }
    ]
    fake_rdns = {"total": 120, "pending": 10, "last_enrichment_at": "2026-10-11T00:05:00Z"}

    with (
        patch("backend.utils.bot_sources.get_all_sources_meta", return_value=fake_sources),
        patch("backend.utils.rdns_cache.get_stats", return_value=fake_rdns),
    ):
        resp = client.get("/api/admin/bot-sources")

    assert resp.status_code == 200
    data = resp.json()
    assert "sources" in data
    assert len(data["sources"]) == 1
    assert data["sources"][0]["id"] == "well-known-bots"
    assert data["sources"][0]["entry_count"] == 42
    assert "rdns" in data
    assert data["rdns"]["total"] == 120


# ── POST /api/admin/bot-sources/{source_id}/refresh ──────────────────────────


def test_refresh_single_bot_source_success():
    """POST /api/admin/bot-sources/{source_id}/refresh re-caches the specified source."""
    fake_meta = {
        "id": "well-known-bots",
        "name": "Well-Known Bots (arcjet)",
        "last_updated": "2026-10-11T02:00:00Z",
        "entry_count": 100,
    }

    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=fake_meta) as mock_fetch:
        resp = client.post("/api/admin/bot-sources/well-known-bots/refresh")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["source"]["id"] == "well-known-bots"
    assert data["source"]["entry_count"] == 100
    mock_fetch.assert_called_once_with("well-known-bots")


def test_refresh_single_bot_source_not_found():
    """Unknown source_id returns 404."""
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=ValueError("Unknown bot source")):
        resp = client.post("/api/admin/bot-sources/invalid-source/refresh")

    assert resp.status_code == 404
    data = resp.json()
    assert "detail" in data


def test_refresh_single_bot_source_fetch_failed():
    """Network/feed failure in single source refresh returns 502."""
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=RuntimeError("Connection timeout")):
        resp = client.post("/api/admin/bot-sources/well-known-bots/refresh")

    assert resp.status_code == 502
    data = resp.json()
    assert "detail" in data


# ── POST /api/admin/bot-sources/refresh & /api/admin/bots/refresh ────────────


def test_refresh_all_bot_sources_all_succeed():
    """POST /api/admin/bot-sources/refresh triggers full refresh across all sources."""
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "entry_count": 50},
    ]

    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results):
        resp = client.post("/api/admin/bot-sources/refresh")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["updated_count"] == 2
    assert data["failed_count"] == 0
    assert len(data["sources"]) == 2


def test_refresh_all_bot_sources_partial_failure():
    """POST /api/admin/bot-sources/refresh reports ok=False when any source fails."""
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "failed": True, "error": "503 Unavailable", "entry_count": 0},
    ]

    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results):
        resp = client.post("/api/admin/bot-sources/refresh")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is False
    assert data["updated_count"] == 1
    assert data["failed_count"] == 1


def test_refresh_all_bot_sources_exception_returns_502():
    """Unhandled exception in refresh_all_sources returns 502."""
    with patch("backend.utils.bot_sources.refresh_all_sources", side_effect=RuntimeError("Fatal subsystem error")):
        resp = client.post("/api/admin/bot-sources/refresh")

    assert resp.status_code == 502


def test_refresh_bots_alias_endpoint():
    """POST /api/admin/bots/refresh behaves identically to /api/admin/bot-sources/refresh."""
    fake_results = [{"id": "well-known-bots", "entry_count": 100}]

    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results):
        resp = client.post("/api/admin/bots/refresh")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["updated_count"] == 1


# ── POST /api/admin/rdns/enrich & GET /api/admin/rdns/stats ──────────────────


def test_rdns_enrich_and_stats_endpoints():
    """POST /api/admin/rdns/enrich triggers batch enrichment and GET /api/admin/rdns/stats returns stats."""
    fake_summary = {"resolved": 25, "errors": 0, "discovered": 5}
    fake_stats = {"total": 500, "resolved": 450, "pending": 50}

    with (
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich,
        patch("backend.utils.rdns_cache.get_stats", return_value=fake_stats),
    ):
        enrich_resp = client.post("/api/admin/rdns/enrich?limit=50")
        assert enrich_resp.status_code == 200
        enrich_data = enrich_resp.json()
        assert enrich_data["ok"] is True
        assert enrich_data["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)

        stats_resp = client.get("/api/admin/rdns/stats")
        assert stats_resp.status_code == 200
        stats_data = stats_resp.json()
        assert stats_data["ok"] is True
        assert stats_data["stats"] == fake_stats
