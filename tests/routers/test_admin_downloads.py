"""Tests for the download endpoints in ``backend.routers.admin.downloads``."""

from __future__ import annotations

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
        "prefix": "raw_prefix",
        "access_level": "read_write",
        "cdn_url": "",
    }
    app.dependency_overrides[get_source] = lambda: src
    yield src
    app.dependency_overrides.clear()


def test_download_file_missing_key(client, override_source):
    # key is empty
    response = client.get("/api/download?key=")
    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "Missing key parameter"


def test_download_file_invalid_key_prefix(client, override_source):
    # key doesn't start with raw_prefix
    response = client.get("/api/download?key=other_prefix/file.log")
    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "invalid_key"


def test_download_file_invalid_path_traversal(client, override_source):
    # path traversal
    response = client.get("/api/download?key=raw_prefix/../../../etc/passwd")
    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "invalid_key"


def test_download_file_from_local_cache(client, override_source, tmp_path):
    # Create local cached file
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    local_file = cache_dir / "raw_prefix" / "file.log"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("local-cached-data")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200
        assert response.text == "local-cached-data"


def test_download_file_from_fos_success(client, override_source, tmp_path):
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)

    fake_body = MagicMock()
    fake_body.iter_chunks.return_value = [b"fos-data"]
    fake_obj = {
        "Body": fake_body,
        "ContentType": "text/plain",
        "ContentLength": 8,
    }
    fake_fos = MagicMock()
    fake_fos.get_object.return_value = fake_obj

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.utils.telemetry.record_call") as mock_record,
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200
        # Consume stream
        content = b"".join(response.iter_bytes())
        assert content == b"fos-data"
        mock_record.assert_called_once()
        fake_body.close.assert_called_once()


def test_download_file_from_cdn_success(client, override_source, tmp_path):
    override_source["cdn_url"] = "http://mycdn.com"
    override_source["cdn_secret"] = "secret123"
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)

    fake_resp = MagicMock()
    fake_resp.headers = {"Content-Type": "text/plain", "Content-Length": "8"}
    fake_resp.read.side_effect = [b"cdn-data", b""]

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("urllib.request.urlopen", return_value=fake_resp) as mock_urlopen,
        patch("backend.utils.telemetry.record_cdn_call") as mock_record,
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 200
        # Consume stream
        content = b"".join(response.iter_bytes())
        assert content == b"cdn-data"
        mock_urlopen.assert_called_once()
        mock_record.assert_called_once()
        fake_resp.close.assert_called_once()


def test_download_file_from_cdn_fails_raises_internal(client, override_source, tmp_path):
    override_source["cdn_url"] = "http://mycdn.com"
    override_source["cdn_secret"] = "secret123"
    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("urllib.request.urlopen", side_effect=ValueError("connection refused")),
    ):
        response = client.get("/api/download?key=raw_prefix/file.log")
        assert response.status_code == 502  # raise_internal maps to 502 as specified in download_file


def test_download_folder(client, override_source):
    fake_fos = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw_prefix/raw/foo/file1.log"},
                {"Key": "raw_prefix/raw/foo/sub/file2.log"},
                {"Key": "raw_prefix/raw/foo/dir_marker/"},  # skipped
            ]
        }
    ]
    fake_fos.get_paginator.return_value = fake_paginator

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.routers.admin.downloads._fetch_file_to_zip") as mock_fetch,
    ):
        response = client.get("/api/download-folder?prefix=foo&root=raw")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        # Consume StreamingResponse to trigger populate generator
        content = b"".join(response.iter_bytes())
        assert len(content) > 0

        # Verify paginator was called with correct Prefix
        fake_fos.get_paginator.assert_called_with("list_objects_v2", caller_hint="download_zip")
        fake_paginator.paginate.assert_called_with(Bucket="test-bucket", Prefix="raw_prefix/raw/foo/")

        # Verify _fetch_file_to_zip was called for files (excluding folder marker)
        assert mock_fetch.call_count == 2


def test_download_all_local_include(client, override_source, tmp_path):
    db_file = tmp_path / "test.duckdb"
    db_file.write_text("duckdb-binary-data")

    override_source["duckdb_path"] = str(db_file)
    override_source["prefix"] = "raw_prefix"

    cache_dir = tmp_path / "cache" / "service123"
    cache_dir.mkdir(parents=True)
    local_file = cache_dir / "raw_prefix" / "cached_file.parquet"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("parquet-bytes")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.get_source_for_service", return_value=override_source),
        patch("backend.config.load_config", return_value={"name": "service123", "service_id": "service123"}),
    ):
        response = client.get("/api/download-all", params={"service_id": "service123", "include": "local"})
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        content = b"".join(response.iter_bytes())
        assert len(content) > 0


def test_download_all_cloud_include(client, override_source):
    fake_fos = MagicMock()
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw_prefix/raw/file1.log"},
                {"Key": "raw_prefix/raw/file2.log"},
            ]
        }
    ]
    fake_fos.get_paginator.return_value = fake_paginator

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_fos),
        patch("backend.routers.admin.downloads._fetch_file_to_zip") as mock_fetch,
        patch("backend.core.duckdb.get_source_for_service", return_value=override_source),
        patch("backend.config.load_config", return_value={"name": "service123", "service_id": "service123"}),
    ):
        response = client.get("/api/download-all", params={"service_id": "service123", "include": "cloud"})
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        content = b"".join(response.iter_bytes())
        assert len(content) > 0
        assert mock_fetch.call_count == 2
