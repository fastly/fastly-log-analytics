"""Branch coverage tests for backend/core/iceberg/sync.py.

Targets the residual ~93 uncovered statements after the existing
test_iceberg.py suite (~65% baseline). Each test pins a specific
branch in ``sync_data``:

- Phase 1 catalog load + ``_try_register_from_fos`` fallback (lines 53-63)
- ``get_locally_compacted_basenames`` exception swallow (94-95)
- Fast-path s3:// URI rewriting from cache (124-129)
- Fast-path exception fallback to slow path (150-153)
- Slow-path scan exception (198, 204-205)
- ``cdn_url`` validation (scheme/hostname/IP/DNS — 224-243)
- CDN download path: secret-in-URL, HTTPError 401/403 no-retry, generic
  retries with backoff, success after retry (262-316)
- ``record_cdn_call`` exception swallow (326-338)
- ``_download_file`` tmp-file cleanup on raise (347-355)
- Slow-path resolved-files: bare-URI rel_path fallback (line 458)

Approach: every test mocks the PyIceberg catalog + table (real PyIceberg
state setup is too brittle for branch-coverage scope). Downloads either
mock the boto3 S3 client (``_get_fos_client``) or use ``urllib.request``
patches for the CDN path. No moto needed — the existing E2E coverage
under ``test_iceberg.py`` already exercises the real S3 happy path.
"""

from __future__ import annotations

import os
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from backend.core import iceberg as _ice

# ── Phase 1: catalog load failure → _try_register_from_fos ───────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
def test_sync_data_returns_error_when_register_from_fos_fails(mock_get_catalog, _mock_refresh, fos_source):
    """Phase 1: when ``_load_table_cached`` raises AND
    ``_try_register_from_fos`` returns ``None``, sync_data must surface
    the "Iceberg table not found in FOS" error and bail out with
    ``files_downloaded=0``. Pins lines 55-61."""
    source = {**fos_source, "name": "no-table-svc", "prefix": "logs"}
    fake_catalog = MagicMock()
    mock_get_catalog.return_value = fake_catalog

    with (
        patch("backend.core.iceberg._load_table_cached", side_effect=RuntimeError("not found")),
        patch("backend.core.iceberg._try_register_from_fos", return_value=None),
    ):
        res = _ice.sync_data(source)

    assert res["files_downloaded"] == 0
    assert "not found in FOS" in res["error"]


@patch("backend.core.iceberg._get_catalog", side_effect=RuntimeError("boom catalog"))
def test_sync_data_returns_error_when_catalog_init_raises(_mock_get_catalog, fos_source):
    """Phase 1: outer ``except`` wrap. ``_get_catalog`` itself blowing up
    should surface as the ``Could not load table`` error envelope.
    Pins line 62-63."""
    source = {**fos_source, "name": "catalog-boom-svc", "prefix": "logs"}
    res = _ice.sync_data(source)
    assert res["files_downloaded"] == 0
    assert "Could not load table" in res["error"]
    assert "boom catalog" in res["error"]


# ── get_locally_compacted_basenames exception swallow ────────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_swallows_compacted_basenames_lookup_failure(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """When ``metadata_db.get_locally_compacted_basenames`` raises, the
    function must continue with an empty compacted set rather than
    aborting the sync. Pins lines 94-95."""
    source = {**fos_source, "name": "compacted-err-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m.json"
    mock_table.location.return_value = "s3://b"
    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    with patch(
        "backend.core.metadata.get_locally_compacted_basenames",
        side_effect=RuntimeError("sqlite down"),
    ):
        res = _ice.sync_data(source)

    # Sync still completed cleanly despite the registry-lookup failure
    assert res["files_downloaded"] == 0
    assert "error" not in res


# ── Fast path: s3:// uri entries in cache get downloaded ─────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_fast_path_downloads_s3_uri_cache_entries(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """Fast path: a cache entry that begins with ``s3://`` (i.e. has not
    been downloaded yet) must be added to ``cloud_files`` and queued for
    download even when every local-path entry on disk is present.
    Pins lines 122-129."""
    source = {**fos_source, "name": "fast-s3-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    # The cache lists one s3:// URI that has never been downloaded yet.
    metadata_loc = "s3://test-bucket/logs/iceberg/m.json"
    iceberg_loc = "s3://test-bucket/logs/iceberg"
    uri = f"{iceberg_loc}/data/timestamp_hour=2026-06-01-00/00000-0-new.parquet"
    _ice._snapshot_files_cache[source["name"]] = (metadata_loc, 1, iceberg_loc, [uri])

    mock_table = MagicMock()
    mock_table.metadata_location = metadata_loc
    mock_table.location.return_value = iceberg_loc
    mock_table.scan.return_value = MagicMock()  # not consulted on fast path
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    # The boto3 client just writes a placeholder body to the requested tmp path.
    def _fake_download(bucket, key, dest):
        with open(dest, "wb") as fp:
            fp.write(b"parquet")

    s3 = MagicMock()
    s3.download_file.side_effect = _fake_download
    mock_get_fos.return_value = s3

    try:
        res = _ice.sync_data(source)
    finally:
        _ice._snapshot_files_cache.pop(source["name"], None)

    assert res["files_downloaded"] == 1
    assert s3.download_file.called
    # File now exists locally at the relpath under cache/data/
    expected = tmp_path / "data" / "timestamp_hour=2026-06-01-00" / "00000-0-new.parquet"
    assert expected.exists()


# ── Fast path: cache-handler exception falls back to slow path ───────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_fast_path_exception_falls_back_to_slow_path(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path, caplog
):
    """If the fast-path block raises mid-iteration (e.g. corrupt cache
    tuple), it must log a warning and fall through to the slow plan_files
    scan rather than crashing the sync. Pins lines 150-153."""
    import logging

    source = {**fos_source, "name": "fastpath-boom-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    metadata_loc = "s3://test-bucket/logs/iceberg/m.json"
    iceberg_loc = "s3://test-bucket/logs/iceberg"

    # Seed cache with a non-string entry to trigger AttributeError on `.startswith`
    _ice._snapshot_files_cache[source["name"]] = (metadata_loc, 1, iceberg_loc, [12345])

    plan_calls = {"n": 0}

    def _plan_files():
        plan_calls["n"] += 1
        return []

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.side_effect = _plan_files
    mock_table = MagicMock()
    mock_table.metadata_location = metadata_loc
    mock_table.location.return_value = iceberg_loc
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    try:
        with caplog.at_level(logging.WARNING, logger="backend.core.iceberg._core"):
            res = _ice.sync_data(source)
    finally:
        _ice._snapshot_files_cache.pop(source["name"], None)

    assert plan_calls["n"] >= 1, "slow path must run after fast-path raises"
    assert res["files_downloaded"] == 0
    assert any("cache fast-path failed" in r.message for r in caplog.records)


# ── Slow path: scan exception bubbles up as error ────────────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
def test_sync_data_slow_path_scan_failure_returns_error(
    mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """When ``table.scan().plan_files()`` raises on the slow path, the
    function returns the ``Metadata scan failed`` error envelope rather
    than propagating. Pins lines 204-205."""
    source = {**fos_source, "name": "scan-boom-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.side_effect = RuntimeError("manifest GET failed")
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    res = _ice.sync_data(source)
    assert res["files_downloaded"] == 0
    assert "Metadata scan failed" in res["error"]
    assert "manifest GET failed" in res["error"]


# ── cdn_url validation: scheme, hostname, internal IP, DNS failure ──────────


@pytest.mark.parametrize(
    "cdn_url,expected_msg",
    [
        ("http://example.com", "scheme must be https"),
        ("https:///nopath", "must include a hostname"),
    ],
)
@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_rejects_invalid_cdn_url(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path, cdn_url, expected_msg
):
    """Pins the cdn_url scheme + hostname validators (lines 229-234)."""
    source = {**fos_source, "name": f"cdn-{expected_msg[:8]}", "prefix": "logs", "cdn_url": cdn_url}
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    # Empty plan_files keeps the slow path cheap
    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    res = _ice.sync_data(source)
    assert res["files_downloaded"] == 0
    assert expected_msg in res["error"]


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_rejects_cdn_url_resolving_to_internal_ip(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """SSRF guard: ``socket.getaddrinfo`` returning a non-global IP must
    short-circuit with the ``internal IP`` error. Pins lines 236-241."""
    source = {**fos_source, "name": "cdn-internal-ip", "prefix": "logs", "cdn_url": "https://internal.example.com"}
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    # getaddrinfo returns a private 10.x address
    fake_addr = [(2, 1, 6, "", ("10.0.0.1", 0))]
    with patch("socket.getaddrinfo", return_value=fake_addr):
        res = _ice.sync_data(source)

    assert res["files_downloaded"] == 0
    assert "internal IP" in res["error"]


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_rejects_cdn_url_with_dns_failure(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """``socket.getaddrinfo`` raising (NXDOMAIN, network down) surfaces
    as the ``hostname resolution failed`` error. Pins lines 242-243."""
    source = {**fos_source, "name": "cdn-dns-fail", "prefix": "logs", "cdn_url": "https://nope.invalid"}
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    with patch("socket.getaddrinfo", side_effect=OSError("nxdomain")):
        res = _ice.sync_data(source)

    assert res["files_downloaded"] == 0
    assert "hostname resolution failed" in res["error"]


# ── CDN download: secret in URL + header, success ───────────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_cdn_download_with_secret_appends_query_and_header(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """When ``cdn_url`` + ``cdn_secret`` are set, the request URL must
    contain ``?key=<secret>`` and the x-fastly-key header must be added.
    Pins lines 261-302 (success branch, no retries)."""
    source = {
        **fos_source,
        "name": "cdn-secret-svc",
        "prefix": "logs",
        "cdn_url": "https://cdn.example.com",
        "cdn_secret": "secret-token-abc",
    }
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    uri = "s3://test-bucket/logs/iceberg/data/timestamp_hour=2026-06-01-00/00000-0-cdn.parquet"
    mock_file = MagicMock()
    mock_file.file.file_path = uri
    mock_file.file.record_count = 7

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = [mock_file]
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    # Mock urlopen → fake response with body
    captured = {}

    class _FakeResponse:
        headers = {"x-served-by": "fake"}

        def __init__(self):
            self._b = b"cdn-body-bytes"
            self._read = False

        def read(self, n=-1):
            if self._read:
                return b""
            self._read = True
            return self._b

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def _fake_cdn_open(opener, req, timeout):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.headers)
        return _FakeResponse()

    # Skip DNS validation and intercept the CDN-download network call at
    # the ``_cdn_open`` indirection so we don't need a real socket.
    with (
        patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("8.8.8.8", 0))]),
        patch("backend.core.iceberg.sync._cdn_open", side_effect=_fake_cdn_open),
    ):
        res = _ice.sync_data(source)

    assert res["files_downloaded"] == 1
    assert "key=secret-token-abc" in captured["url"]
    # urllib normalises header names with title-case
    assert any(k.lower() == "x-fastly-key" for k in captured["headers"])


# ── CDN download: HTTPError 401 short-circuits retries ──────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_cdn_auth_error_does_not_retry(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """HTTP 401/403 must break the retry loop immediately — re-trying
    auth errors wastes bandwidth and rate-limit budget. Pins lines
    303-307 + the ``not success`` raise on 315-318."""
    source = {
        **fos_source,
        "name": "cdn-auth-fail",
        "prefix": "logs",
        "cdn_url": "https://cdn.example.com",
        "cdn_secret": "wrong",
    }
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    uri = "s3://test-bucket/logs/iceberg/data/timestamp_hour=2026-06-01-00/00000-0-401.parquet"
    mock_file = MagicMock()
    mock_file.file.file_path = uri
    mock_file.file.record_count = 1

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = [mock_file]
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    call_count = {"n": 0}

    def _raise_401(opener, req, timeout):
        call_count["n"] += 1
        raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, None)

    # The sync_data orchestrator runs CDN downloads in a thread pool and
    # bubbles exceptions via ``future.result()``. The whole sync call
    # therefore raises a ``RuntimeError("CDN download failed for ...")``.
    with (
        patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("8.8.8.8", 0))]),
        patch("backend.core.iceberg.sync._cdn_open", side_effect=_raise_401),
        patch("time.sleep") as mock_sleep,
        pytest.raises(RuntimeError, match="CDN download failed"),
    ):
        _ice.sync_data(source)

    # Auth error: exactly one attempt, no retry sleep
    assert call_count["n"] == 1, f"401 must not retry, got {call_count['n']} attempts"
    assert not mock_sleep.called, "401 must not trigger the inter-attempt sleep"


# ── CDN download: generic error retries then succeeds ───────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_cdn_transient_error_retries_then_succeeds(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """Generic (non-auth) urlopen failures must retry up to 3 times with
    a 1-second backoff between attempts. Pins lines 308-313 + the
    success-after-retry exit."""
    source = {
        **fos_source,
        "name": "cdn-retry-svc",
        "prefix": "logs",
        "cdn_url": "https://cdn.example.com",
        # No cdn_secret → exercises the no-secret URL branch (line 281-282)
    }
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    uri = "s3://test-bucket/logs/iceberg/data/timestamp_hour=2026-06-01-00/00000-0-retry.parquet"
    mock_file = MagicMock()
    mock_file.file.file_path = uri
    mock_file.file.record_count = 3

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = [mock_file]
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    class _OkResponse:
        headers = {}

        def read(self, n=-1):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    attempts = {"n": 0}

    def _flaky_open(opener, req, timeout):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TimeoutError("connection reset")
        return _OkResponse()

    with (
        patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("8.8.8.8", 0))]),
        patch("backend.core.iceberg.sync._cdn_open", side_effect=_flaky_open),
        patch("time.sleep"),
    ):
        res = _ice.sync_data(source)

    assert attempts["n"] == 3, f"expected 3 attempts (2 fail + 1 succeed), got {attempts['n']}"
    assert res["files_downloaded"] == 1


# ── CDN download: telemetry record swallows errors ──────────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_cdn_telemetry_failure_does_not_break_download(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """``record_cdn_call`` raising must NOT fail the download — telemetry
    is best-effort. Pins lines 325-338 (the bare ``except: pass``)."""
    source = {
        **fos_source,
        "name": "cdn-telemetry-fail",
        "prefix": "logs",
        "cdn_url": "https://cdn.example.com",
    }
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    uri = "s3://test-bucket/logs/iceberg/data/timestamp_hour=2026-06-01-00/00000-0-telem.parquet"
    mock_file = MagicMock()
    mock_file.file.file_path = uri
    mock_file.file.record_count = 1

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = [mock_file]
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    class _OkResponse:
        headers = {}

        def read(self, n=-1):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    with (
        patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("8.8.8.8", 0))]),
        patch("backend.core.iceberg.sync._cdn_open", return_value=_OkResponse()),
        patch("backend.utils.telemetry.record_cdn_call", side_effect=RuntimeError("telemetry sink down")),
    ):
        res = _ice.sync_data(source)

    # Download still counted as successful despite telemetry failure
    assert res["files_downloaded"] == 1


# ── _download_file: tmp file removed on raise ───────────────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_tmp_file_cleaned_up_on_download_error(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """When ``s3.download_file`` raises mid-download, the ``.tmp.<tid>``
    file the writer thread created must be deleted before the exception
    bubbles up. Pins lines 349-355."""
    source = {**fos_source, "name": "tmp-cleanup-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    uri = "s3://test-bucket/logs/iceberg/data/timestamp_hour=2026-06-01-00/00000-0-fail.parquet"
    mock_file = MagicMock()
    mock_file.file.file_path = uri
    mock_file.file.record_count = 1

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = [mock_file]
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    def _failing_download(bucket, key, dest):
        # Create the tmp file (simulating a partial write) then raise
        with open(dest, "wb") as fp:
            fp.write(b"partial")
        raise OSError("disk full mid-stream")

    s3 = MagicMock()
    s3.download_file.side_effect = _failing_download
    mock_get_fos.return_value = s3

    target_dir = tmp_path / "data" / "timestamp_hour=2026-06-01-00"

    with pytest.raises(IOError, match="disk full"):
        _ice.sync_data(source)

    # The .tmp.<tid> file must have been removed by the cleanup branch
    if target_dir.exists():
        leftover = [p for p in target_dir.iterdir() if ".tmp." in p.name]
        assert leftover == [], f"tmp file leak after failed download: {leftover}"


# ── Slow-path resolved-files: rel_path basename fallback (line 458) ─────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_slow_path_handles_uri_without_data_segment(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path
):
    """When a plan_files URI has no ``/data/`` segment, the post-sync
    cache-rebuild branch must fall back to ``uri.split('/')[-1]`` as
    the relative path. Pins line 458 (the ``else`` branch of the slow
    path's rel_path resolution).

    The first plan_files() call (Phase 2) returns a URI with /data/ so
    the download succeeds; the second call (post-sync slow-path cache
    rebuild) returns a URI WITHOUT /data/ so we hit the basename
    fallback. The branch must not crash and must register the URI in
    ``_snapshot_files_cache``.
    """
    source = {**fos_source, "name": "no-data-segment-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)

    # Ensure no stale cache so the slow-path post-sync rebuild branch runs.
    _ice._snapshot_files_cache.pop(source["name"], None)

    # Two plan_files passes. First: has /data/ so download works.
    # Second (post-sync): no /data/ so we exercise the basename branch.
    has_data_uri = "s3://test-bucket/logs/iceberg/data/timestamp_hour=2026-06-01-00/00000-0-a.parquet"
    no_data_uri = "s3://test-bucket/somewhere/else/file-b.parquet"

    f_has = MagicMock()
    f_has.file.file_path = has_data_uri
    f_has.file.record_count = 1
    f_no = MagicMock()
    f_no.file.file_path = no_data_uri
    f_no.file.record_count = 0

    plan_pass = {"n": 0}

    def _plan_files_seq():
        plan_pass["n"] += 1
        if plan_pass["n"] == 1:
            return [f_has]  # Phase 2 — download
        return [f_no]  # Post-sync rebuild — exercises line 458

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.side_effect = _plan_files_seq
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    # current_snapshot() may be called on the post-sync slow-path branch
    snap = MagicMock()
    snap.snapshot_id = 999
    mock_table.current_snapshot.return_value = snap
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    def _fake_download(bucket, key, dest):
        with open(dest, "wb") as fp:
            fp.write(b"x")

    s3 = MagicMock()
    s3.download_file.side_effect = _fake_download
    mock_get_fos.return_value = s3

    try:
        res = _ice.sync_data(source)
    finally:
        _ice._snapshot_files_cache.pop(source["name"], None)

    # Both phases ran cleanly
    assert res["files_downloaded"] == 1
    assert plan_pass["n"] >= 2, "post-sync slow-path rebuild must invoke plan_files() a second time"


# ── Orphan-cleanup exception swallowed with warning ─────────────────────────


@patch("backend.core.iceberg._refresh_local_catalog_metadata", return_value=True)
@patch("backend.core.iceberg._get_catalog")
@patch("backend.core.duckdb._cache_dir")
@patch("backend.core.duckdb._get_fos_client")
def test_sync_data_orphan_cleanup_failure_logs_and_continues(
    mock_get_fos, mock_cache_dir, mock_get_catalog, _mock_refresh, fos_source, tmp_path, caplog
):
    """When the orphan-cleanup walk raises (e.g. permission denied on a
    subdir), the function must log a warning and continue to the
    post-sync cache update rather than aborting. Pins lines 408-409."""
    import logging

    source = {**fos_source, "name": "orphan-fail-svc", "prefix": "logs"}
    mock_cache_dir.return_value = str(tmp_path)
    mock_get_fos.return_value = MagicMock()

    # Create the cache dir so isdir() returns True, triggering the walk
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "timestamp_hour=2026-06-01-00").mkdir(parents=True, exist_ok=True)

    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.plan_files.return_value = []
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://b/m"
    mock_table.location.return_value = "s3://b"
    mock_table.scan.return_value = mock_scan
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = mock_table
    mock_get_catalog.return_value = fake_catalog

    # listdir raises after the data root isdir check passes
    real_listdir = os.listdir

    def _flaky_listdir(p):
        if p.endswith("/data"):
            raise PermissionError("denied")
        return real_listdir(p)

    with (
        caplog.at_level(logging.WARNING, logger="backend.core.iceberg._core"),
        patch("os.listdir", side_effect=_flaky_listdir),
    ):
        res = _ice.sync_data(source)

    # Sync completed; warning surfaced
    assert "error" not in res
    assert any("Failed to cleanup orphaned files" in r.message for r in caplog.records)
