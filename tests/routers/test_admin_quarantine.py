"""Tests for the item-based quarantine admin API."""

from __future__ import annotations

from io import BytesIO
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.deps import get_source, require_admin
from backend.main import app


@pytest.fixture
def override_source():
    source = {
        "service_id": "service123",
        "name": "service123",
        "bucket": "test-bucket",
        "access_level": "read_write",
    }
    app.dependency_overrides[get_source] = lambda: source
    yield source
    app.dependency_overrides.clear()


def clean_res(data: dict) -> dict:
    return {key: value for key, value in data.items() if not key.startswith("_")}


def test_quarantine_evidence_routes_are_registered():
    paths = app.openapi()["paths"]
    assert "/api/admin/quarantine" in paths
    assert "/api/admin/quarantine/summary" in paths
    assert "/api/admin/quarantine/download/{item_id}" in paths
    assert "/api/admin/quarantine/purge" in paths


def test_list_quarantine_evidence_filters_and_paginates(client, override_source):
    item = {
        "id": 7,
        "source_type": "request",
        "original_key": "raw/request/batch.log.gz",
        "line_ordinal": 4,
        "byte_offset": 92,
        "byte_length": 11,
        "error_category": "invalid_json",
        "error_text": "malformed record",
        "sha256": "a" * 64,
        "quarantined_at": "2026-09-29T12:00:00Z",
    }
    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", return_value=[item]) as list_items,
        patch(
            "backend.routers.admin.quarantine.get_quarantine_evidence_summary",
            return_value={
                "total_items": 1,
                "total_bytes": 11,
                "request_items": 1,
                "rum_items": 0,
                "oldest_at": item["quarantined_at"],
                "newest_at": item["quarantined_at"],
                "category_counts": {"invalid_json": 1},
            },
        ),
    ):
        response = client.get("/api/admin/quarantine?limit=20&offset=5&error_category=invalid_json")

    assert response.status_code == 200
    payload = clean_res(response.json())
    assert payload["total"] == 1
    assert payload["items"][0] == item
    list_items.assert_called_once_with("service123", limit=20, offset=5, error_category="invalid_json")


def test_quarantine_summary_uses_item_evidence(client, override_source):
    summary = {
        "total_items": 3,
        "total_bytes": 18,
        "request_items": 2,
        "rum_items": 1,
        "oldest_at": "2026-09-29T11:00:00Z",
        "newest_at": "2026-09-29T12:00:00Z",
        "category_counts": {"invalid_json": 2, "corrupt_container": 1},
    }
    with patch("backend.routers.admin.quarantine.get_quarantine_evidence_summary", return_value=summary) as get_summary:
        response = client.get("/api/admin/quarantine/summary")

    assert response.status_code == 200
    assert clean_res(response.json()) == summary
    get_summary.assert_called_once_with("service123")


def test_analyst_path_a_read_only_source_is_denied(client, override_source):
    override_source["access_level"] = "read_only"
    with patch("backend.routers.admin.quarantine.list_quarantine_evidence") as list_items:
        response = client.get("/api/admin/quarantine")

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "admin_only"
    list_items.assert_not_called()


def test_quarantine_routes_apply_endpoint_admin_dependency(client, override_source):
    def reject_admin():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_admin
    try:
        response = client.get("/api/admin/quarantine/summary")
    finally:
        app.dependency_overrides.pop(require_admin, None)

    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "admin_only"


def test_download_quarantine_evidence_streams_exact_bytes(client, override_source):
    raw_bytes = b'{"invalid":"\x00\xff"}\r\n'
    item = {
        "id": 7,
        "file_path": "7.dat",
        "byte_length": len(raw_bytes),
        "original_key": "raw/request/batch.log.gz",
    }
    with patch(
        "backend.core.quarantine.read_evidence",
        return_value=(item, BytesIO(raw_bytes)),
    ):
        response = client.get("/api/admin/quarantine/download/7")

    assert response.status_code == 200
    assert response.content == raw_bytes
    assert response.headers["content-disposition"] == 'attachment; filename="quarantine-item-7.dat"'


def test_download_quarantine_evidence_returns_not_found(client, override_source):
    with patch("backend.core.quarantine.read_evidence", return_value=None):
        response = client.get("/api/admin/quarantine/download/999")

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "quarantine_not_found"


def test_download_quarantine_evidence_enforces_size_limit(client, override_source):
    item = {"id": 7, "file_path": "7.dat", "byte_length": 50 * 1024 * 1024 + 1}
    with patch(
        "backend.core.quarantine.read_evidence",
        return_value=(item, BytesIO(b"unused")),
    ):
        response = client.get("/api/admin/quarantine/download/7")

    assert response.status_code == 413


@pytest.mark.parametrize(
    ("item_id", "expected"),
    [(7, {"purged": 1}), (None, {"purged": 3})],
)
def test_purge_quarantine_evidence(client, override_source, item_id, expected):
    with (
        patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value={"id": item_id}),
        patch("backend.core.quarantine.purge_evidence", return_value=expected) as purge,
    ):
        query = "" if item_id is None else f"?item_id={item_id}"
        response = client.post(f"/api/admin/quarantine/purge{query}")

    assert response.status_code == 200
    assert clean_res(response.json()) == expected
    purge.assert_called_once_with("service123", item_id)


def test_purge_quarantine_evidence_unknown_item_returns_404(client, override_source):
    with patch("backend.routers.admin.quarantine.get_quarantine_evidence", return_value=None):
        response = client.post("/api/admin/quarantine/purge?item_id=999")

    assert response.status_code == 404


def test_quarantine_admin_dependency_denies_analyst_path_b(override_source):
    from tests.remote_access.test_middleware import _login_analyst, _seed_invite, _start_share

    _start_share()
    invite = _seed_invite(service_ids=["service123"])
    with TestClient(app) as client:
        _login_analyst(client, invite)
        response = client.get(
            "/api/admin/quarantine/summary",
            headers={"X-Remote-Analyst": "1", "Host": "testserver"},
        )

    assert response.status_code == 403
