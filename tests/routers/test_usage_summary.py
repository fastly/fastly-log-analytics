from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.deps import get_source
from backend.main import app
from backend.routers import usage

# ── From test_usage_endpoints.py ──


@pytest.fixture
def test_client():
    return TestClient(app)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_success(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """The endpoint returns 200 when storage stats and FOS listing both
    succeed. Migrated from inline boto3-MagicMock paginator stubs to the
    shared moto-backed s3_mock fixture — the LIST fallback now flows
    through real S3 semantics rather than a hand-rolled paginator stub.

    ``get_connection`` is still mocked: the route opens a DuckDB handle
    only to pass to the already-mocked ``get_storage_stats``, so the real
    DB connection contributes nothing to what we're verifying — and under
    pytest-xdist the on-disk per-service DuckDB file can be held by a
    peer worker, surfacing as a DBBusyError 500 we'd otherwise chase.
    """
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    mock_get_storage_stats.return_value = {"total_files": 10, "total_bytes": 1024, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    # Seed one iceberg object so the FOS LIST fallback finds something.
    # get_table_info (tried first) has no real iceberg metadata to read
    # and will swallow its own exception, falling through to the LIST.
    source = {**fos_source, "prefix": "test-prefix"}
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="test-prefix/iceberg/data/x.parquet",
        Body=b"x" * 1024,
    )

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_splits_out_rum_bytes(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """RUM beacon objects live under ``raw/rum/`` inside the same bucket as
    regular logs. The endpoint must report their size separately
    (``rum_bytes``) and subtract it from the regular-log total
    (``regular_log_bytes``) rather than double-counting it into both."""
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    total_bytes = 5000
    mock_get_storage_stats.return_value = {"total_files": 3, "total_bytes": total_bytes, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    source = {**fos_source, "prefix": "test-prefix"}
    rum_object_size = 777
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="test-prefix/raw/rum/2026-08-05T00-00-00.log.gz",
        Body=b"r" * rum_object_size,
    )

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
        body = response.json()
        assert body["rum_bytes"] == rum_object_size
        assert body["regular_log_bytes"] == total_bytes - rum_object_size
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_rum_scan_failure_defaults_to_zero(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """A broken FOS listing for the RUM prefix must not 500 the whole
    cost panel — it should fail open to rum_bytes=0 (regular_log_bytes
    falls back to the full total, same as before this field existed)."""
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    total_bytes = 2048
    mock_get_storage_stats.return_value = {"total_files": 1, "total_bytes": total_bytes, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    source = {**fos_source, "bucket": "bucket-that-does-not-exist"}

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
        body = response.json()
        assert body["rum_bytes"] == 0
        assert body["regular_log_bytes"] == total_bytes
    finally:
        app.dependency_overrides.pop(get_source, None)


# ── From test_usage_mock_mode.py ──


def test_api_key_falls_back_to_placeholder_in_mock_mode(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "")
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert usage.api_key_or_mock(usage.get_fastly_api_key("svc")) == "mock-api-key"


def test_api_key_stays_empty_outside_mock_mode(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "")
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    assert usage.api_key_or_mock(usage.get_fastly_api_key("svc")) == ""


def test_api_key_prefers_the_configured_key(monkeypatch):
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda _sid: "real-key")
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert usage.api_key_or_mock(usage.get_fastly_api_key("svc")) == "real-key"


# ── From test_usage_rum_breakdown.py ──


def _clear(service_id: str) -> None:
    db = get_con(service_id)
    db.execute(
        "DELETE FROM ingested_files WHERE source_name = ? AND table_name IN ('client_vitals', 'client_errors')",
        (service_id,),
    )
    db.commit()


def _seed(service_id: str, received_at: str, count: int) -> None:
    db = get_con(service_id)
    date_str = received_at.split("T")[0]
    db.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name, file_date) VALUES (?, ?, ?, ?, ?, ?)",
        (f"s3://bucket/{received_at}.parquet", service_id, count, 100, "client_vitals", date_str),
    )
    db.commit()


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_computes_beacon_counts_and_cost(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_basic"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.01}

    _seed(service_id, "2026-08-05T10:00:00Z", 3)
    _seed(service_id, "2026-08-06T10:00:00Z", 7)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
        )
        assert r.status_code == 200, r.text
        body = r.json()

        assert body["total_beacons"] == 10
        assert body["total_estimated_class_a"] == 10
        assert body["class_a_rate_per_1k"] == 0.01
        # 10 ops / 1000 * 0.01 = 0.0001
        assert body["total_estimated_cost_usd"] == pytest.approx(0.0001, abs=1e-8)

        by_date = {p["date"]: p for p in body["data"]}
        assert by_date["2026-08-05"]["beacon_count"] == 3
        assert by_date["2026-08-06"]["beacon_count"] == 7
        assert by_date["2026-08-06"]["estimated_cost_usd"] == pytest.approx((7 / 1000.0) * 0.01, abs=1e-8)
    finally:
        app.dependency_overrides.pop(get_source, None)
        _clear(service_id)


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_no_beacons_returns_zeroed_response(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_empty"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.005}

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
        assert body["total_estimated_cost_usd"] == 0.0
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_rum_breakdown_no_resolvable_service_returns_not_found_note(test_client):
    source = {"name": None, "service_id": None}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get("/api/usage/rum-breakdown")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
        assert body["note"] == "Service not found."
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_clamps_sub_hourly_granularity_to_daily(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_clamp"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.005}
    _seed(service_id, "2026-08-05T10:00:00Z", 1)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-06T00:00:00Z", "by": "minute"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "adjusted to daily granularity" in body["note"]
    finally:
        app.dependency_overrides.pop(get_source, None)
        _clear(service_id)


def test_rum_breakdown_db_query_failure_falls_back_to_zero_counts(test_client, monkeypatch):
    service_id = "test_usage_rum_breakdown_db_failure"

    def _raise(_sid):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.core.metadata.get_con", _raise)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get("/api/usage/rum-breakdown")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
    finally:
        app.dependency_overrides.pop(get_source, None)
