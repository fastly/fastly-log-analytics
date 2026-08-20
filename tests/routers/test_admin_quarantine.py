"""Tests for the quarantine admin router."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from backend.deps import get_source
from backend.main import app


@pytest.fixture
def override_source():
    src = {
        "service_id": "service123",
        "name": "service123",
        "bucket": "test-bucket",
    }
    app.dependency_overrides[get_source] = lambda: src
    yield src
    app.dependency_overrides.clear()


def clean_res(d: dict) -> dict:
    """Helper to strip telemetry/debug fields added by middleware."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


def test_list_quarantine(client, override_source):
    """Verify list_quarantine returns expected schema structures."""
    fake_files = [
        {
            "id": 1,
            "file_name": "batch_1.parquet",
            "error_key": "raw/error/batch_1.bad.jsonl",
            "valid_rows": 100,
            "corrupt_rows": 5,
            "file_size_bytes": 1024,
            "corrupt_samples": ["sample1", "sample2"],
            "reason_counts": {"missing_ip": 5},
            "quarantined_at": "2026-08-19T00:00:00Z",
        }
    ]
    fake_summary = {
        "total_files": 1,
        "total_valid_rows": 100,
        "total_corrupt_rows": 5,
        "total_bytes": 1024,
    }

    with (
        patch("backend.core.metadata.list_quarantined_files", return_value=fake_files) as mock_list,
        patch("backend.core.metadata.get_quarantine_summary", return_value=fake_summary) as mock_sum,
    ):
        response = client.get("/api/admin/quarantine?limit=50&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["files"]) == 1
        assert data["files"][0]["id"] == 1
        assert data["summary"]["total_files"] == 1
        mock_list.assert_called_once_with("service123", limit=50, offset=0)
        mock_sum.assert_called_once_with("service123")


def test_quarantine_summary(client, override_source):
    """Verify quarantine_summary return properties."""
    fake_summary = {
        "total_files": 10,
        "total_valid_rows": 1000,
        "total_corrupt_rows": 50,
        "total_bytes": 51200,
    }

    with patch("backend.core.metadata.get_quarantine_summary", return_value=fake_summary) as mock_sum:
        response = client.get("/api/admin/quarantine/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["total_files"] == 10
        mock_sum.assert_called_once_with("service123")


def test_export_quarantine(client, override_source):
    """Verify export_quarantine streams NDJSON of quarantined file entries."""
    fake_files = [
        {"id": 1, "file_name": "b1.parquet", "error_key": "err/b1"},
        {"id": 2, "file_name": "b2.parquet", "error_key": "err/b2"},
    ]

    with patch("backend.core.metadata.list_quarantined_files", return_value=fake_files):
        response = client.get("/api/admin/quarantine/export")
        assert response.status_code == 200
        assert response.headers["content-disposition"] == "attachment; filename=quarantine-export.jsonl"

        lines = response.text.strip().split("\n")
        assert len(lines) == 2
        row1 = json.loads(lines[0])
        assert row1["id"] == 1
        assert row1["file_name"] == "b1.parquet"


def test_download_quarantined_file_success(client, override_source):
    """Verify download_quarantined_file streams object body from FOS."""
    fake_record = {
        "id": 1,
        "file_name": "b1.parquet",
        "error_key": "err/b1.bad.jsonl",
    }

    class FakeBody:
        def iter_chunks(self, chunk_size):
            yield b'{"error": "corrupt"}\n'

    fake_fos = MagicMock()
    fake_fos.get_object.return_value = {"Body": FakeBody()}

    with (
        patch("backend.core.metadata.get_quarantined_file_by_id", return_value=fake_record) as mock_get,
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
    ):
        response = client.get("/api/admin/quarantine/1/download")
        assert response.status_code == 200
        assert response.headers["content-disposition"] == 'attachment; filename="b1.parquet.bad.jsonl"'
        assert response.text == '{"error": "corrupt"}\n'
        mock_get.assert_called_once_with("service123", 1)


def test_download_quarantined_file_success_no_iter_chunks(client, override_source):
    """Verify download_quarantined_file fallback stream when Body lacks iter_chunks."""
    fake_record = {
        "id": 1,
        "file_name": "b1.parquet",
        "error_key": "err/b1.bad.jsonl",
    }

    class FakeBodyNoIter:
        def read(self):
            return b'{"error": "legacy"}'

    fake_fos = MagicMock()
    fake_fos.get_object.return_value = {"Body": FakeBodyNoIter()}

    with (
        patch("backend.core.metadata.get_quarantined_file_by_id", return_value=fake_record),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
    ):
        response = client.get("/api/admin/quarantine/1/download")
        assert response.status_code == 200
        assert response.text == '{"error": "legacy"}'


def test_download_quarantined_file_not_found(client, override_source):
    """Verify download_quarantined_file raises 404 error if record is missing."""
    with patch("backend.core.metadata.get_quarantined_file_by_id", return_value=None):
        response = client.get("/api/admin/quarantine/999/download")
        assert response.status_code == 404
        assert response.json()["detail"]["error"] == "quarantine_not_found"


def test_purge_quarantine_no_expired(client, override_source):
    """Verify purge_quarantine returns zeroed result if no expired items are found."""
    with patch("backend.core.metadata.get_expired_quarantined_files", return_value=[]):
        response = client.post("/api/admin/quarantine/purge?retention_days=7")
        assert response.status_code == 200
        assert clean_res(response.json()) == {"purged_fos": 0, "purged_metadata": 0}


def test_purge_quarantine_success(client, override_source):
    """Verify purge_quarantine purges files from FOS and sqlite successfully."""
    fake_expired = [
        {"id": 10, "error_key": "err/k1", "meta_key": "meta/m1"},
        {"id": 11, "error_key": "err/k2", "meta_key": "meta/m2"},
    ]

    fake_fos = MagicMock()

    with (
        patch("backend.core.metadata.get_expired_quarantined_files", return_value=fake_expired) as mock_exp,
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.core.ingest._delete_objects_robust", return_value=4) as mock_del_fos,
        patch("backend.core.metadata.delete_quarantined_rows", return_value=2) as mock_del_meta,
    ):
        response = client.post("/api/admin/quarantine/purge?retention_days=5")
        assert response.status_code == 200
        assert clean_res(response.json()) == {"purged_fos": 4, "purged_metadata": 2}
        mock_exp.assert_called_once_with("service123", retention_days=5)
        mock_del_fos.assert_called_once_with(fake_fos, "test-bucket", ["err/k1", "meta/m1", "err/k2", "meta/m2"])
        mock_del_meta.assert_called_once_with("service123", [10, 11])


def test_purge_quarantine_fos_failure_silenced(client, override_source):
    """Verify purge_quarantine handles FOS deletion exception gracefully while purging SQLite metadata."""
    fake_expired = [
        {"id": 10, "error_key": "err/k1", "meta_key": "meta/m1"},
    ]

    fake_fos = MagicMock()

    with (
        patch("backend.core.metadata.get_expired_quarantined_files", return_value=fake_expired),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.core.ingest._delete_objects_robust", side_effect=Exception("S3 disconnect")),
        patch("backend.core.metadata.delete_quarantined_rows", return_value=1) as mock_del_meta,
    ):
        response = client.post("/api/admin/quarantine/purge?retention_days=5")
        assert response.status_code == 200
        assert clean_res(response.json()) == {"purged_fos": 0, "purged_metadata": 1}
        mock_del_meta.assert_called_once_with("service123", [10])
