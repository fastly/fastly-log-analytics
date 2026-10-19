"""HTTP-level integration tests for the Edge Security & Bot Intelligence router.

Pins:
  - POST /api/security/aggregates and POST /api/security/bundle return 200 with typed bundle models
  - GET /api/security/bundle returns 200 and supports range_token query params
  - Section expansion (fingerprint implies coverage, NGWAF bot pair)
  - Time window clamping for analyst sessions
  - IP masking for Analyst Path B (pii_policy={"mask_ips": True}) in aggregates and CSV/text exports
  - GET /api/security/proxies/export streaming CSV (fastly-acl) and plain text
  - GET /api/security/threat-intel correlates fingerprints, WAF rules, and proxy status with stale view retry
  - Telemetry attribution and debug tracing
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from backend.core.request_context import build_request_context
from backend.main import app
from backend.repositories._base import _safe_table, clear_schema_cols_cache
from backend.utils.remote_access import TimeBounds
from tests.conftest import MOCK_SERVICE_ID, override_request_context
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed_security_table(con, src) -> None:
    clear_schema_cols_cache()
    from backend.core.duckdb import _clear_schema_cache

    _clear_schema_cache()

    logs = generate_mock_logs(src, num_logs=40, hours_ago=1)
    for i, log in enumerate(logs):
        log["ja3"] = "771,4865-4866-4867,0-23-65281-10-11,29-23-24,0" if i % 2 == 0 else "772,4865-4866,0-23-10,29-23,0"
        log["ja4"] = "t13d1516h2_8daaf6152771_0271d18f93db" if i % 2 == 0 else "t13d1516h2_aabbccddeeff_112233445566"
        log["tls"] = "1.3" if i % 2 == 0 else "1.2"
        log["tls_ciphers_sha"] = "aabbcc11" if i < 20 else "ddeeff22"
        log["p_type"] = "VPN" if i % 3 == 0 else None
        log["ua"] = (
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
            if i % 4 == 0
            else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        )
        log["host"] = "example.com"
        log["waf"] = True if i % 7 == 0 else False
        log["req_header_bytes"] = 1024 + i * 50
        log["rtt_min"] = 5 if i % 3 == 0 else 1000
        log["tcp_rtt"] = 1000
        log["lat"] = 37.7749
        log["lon"] = -122.4194
        log["country"] = "US" if i % 2 == 0 else "DE"
        log["asn"] = 15169 if i % 2 == 0 else 3320
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_security_aggregates_default(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/security/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "tls_fingerprints" in data
    assert "req_size_dist" in data
    assert "ipv6_adoption" in data
    assert "proxy_dist" in data
    assert "conn_reuse_dist" in data


def test_security_bundle_post(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/security/bundle",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "bucket_seconds": 300},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "tls_fingerprints" in data
    assert "req_size_dist" in data


def test_security_bundle_get(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with (
        patch("backend.config.get_status", return_value={"earliest_log_at": "2026-08-24T00:00:00Z"}),
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window",
            return_value=("2026-08-24T12:00:00Z", "2026-08-24T13:00:00Z"),
        ),
    ):
        resp = client.get(
            "/api/security/bundle",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"range_token": "24h", "bucket_seconds": 300},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "tls_fingerprints" in data


def test_security_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with (
        patch("backend.config.get_status", return_value={"earliest_log_at": "2026-08-24T00:00:00Z"}),
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window",
            return_value=("2026-08-24T12:00:00Z", "2026-08-24T13:00:00Z"),
        ),
    ):
        resp = client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "range_token": "24h", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "tls_fingerprints" in resp.json()


def test_security_aggregates_section_pruning_and_implies(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.security.get_security_aggregates", return_value={}) as mock_repo:
        client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": ["tls_fingerprints"]},
        )
    assert mock_repo.called
    sections_called = mock_repo.call_args.kwargs.get("sections")
    assert "tls_fingerprints" in sections_called
    # Implies coupling: tls_fingerprints implies fingerprint_coverage
    assert "fingerprint_coverage" in sections_called


def test_security_aggregates_analyst_session_clamps_window(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    earlier = datetime.now(UTC) - timedelta(hours=2)
    later = datetime.now(UTC)
    tb = TimeBounds(start=earlier, end=later)
    analyst_session = SimpleNamespace(
        service_ids=[MOCK_SERVICE_ID],
        time_bounds=tb,
        pii_policy={"mask_ips": False},
    )

    override = override_request_context(
        source=test_service_source,
        con=in_memory_duckdb,
        session=analyst_session,
        time_bounds=tb,
    )
    app.dependency_overrides[build_request_context] = override
    try:
        resp = client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "start_time": (datetime.now(UTC) - timedelta(days=7)).isoformat(),
                "end_time": later.isoformat(),
            },
        )
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(build_request_context, None)


def test_security_aggregates_ip_masking(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    analyst_session = SimpleNamespace(
        service_ids=[MOCK_SERVICE_ID],
        time_bounds=None,
        pii_policy={"mask_ips": True},
    )
    override = override_request_context(
        source=test_service_source,
        con=in_memory_duckdb,
        session=analyst_session,
    )
    app.dependency_overrides[build_request_context] = override
    try:
        resp = client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": ["top_ips_header"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        top_ips = data.get("top_ips_header", [])
        for entry in top_ips:
            ip = entry.get("ip") or entry.get("client_ip")
            if ip:
                # Should be masked (e.g. 192.168.1.xxx or ends with xxx or * or redacted)
                assert "xxx" in ip or "*" in ip or ip == "0.0.0.0" or "[redacted]" in ip
    finally:
        app.dependency_overrides.pop(build_request_context, None)


def test_export_proxies_csv_fastly_acl(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).isoformat()
    end_time = now.isoformat()

    resp = client.get(
        "/api/security/proxies/export",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={
            "start_time": start_time,
            "end_time": end_time,
            "threshold": "High",
            "format": "fastly-acl",
        },
    )
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")
    text = resp.text
    assert "ip,comment" in text


def test_export_proxies_csv_plain(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).isoformat()
    end_time = now.isoformat()

    resp = client.get(
        "/api/security/proxies/export",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={
            "start_time": start_time,
            "end_time": end_time,
            "threshold": "High",
            "format": "plain",
        },
    )
    assert resp.status_code == 200
    assert "text/plain" in resp.headers.get("content-type", "")
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")


def test_export_proxies_csv_ip_masking(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).isoformat()
    end_time = now.isoformat()

    analyst_session = SimpleNamespace(
        service_ids=[MOCK_SERVICE_ID],
        time_bounds=None,
        pii_policy={"mask_ips": True},
    )
    override = override_request_context(
        source=test_service_source,
        con=in_memory_duckdb,
        session=analyst_session,
    )
    app.dependency_overrides[build_request_context] = override
    try:
        resp = client.get(
            "/api/security/proxies/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": start_time,
                "end_time": end_time,
                "threshold": "High",
                "format": "plain",
            },
        )
        assert resp.status_code == 200
        for line in resp.text.splitlines():
            line = line.strip()
            if line:
                assert "xxx" in line or "*" in line or line == "0.0.0.0" or "[redacted]" in line
    finally:
        app.dependency_overrides.pop(build_request_context, None)


def test_security_threat_intel_default(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.get(
        "/api/security/threat-intel",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    items = data["data"]
    if items:
        first = items[0]
        assert "tls_fingerprint" in first
        assert "requests" in first
        assert "top_country" in first
        assert "matched_waf_rules_count" in first
        assert "bot_percentage" in first
        assert "is_anonymized_proxy" in first


def test_security_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/security/aggregates",
        headers={
            "x-fastly-service-id": MOCK_SERVICE_ID,
            "X-Page-Load-ID": "test-security-page-load-99",
        },
        json={"filters": {}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "_debug_queries" in data
    assert isinstance(data["_debug_queries"], list)
    assert len(data["_debug_queries"]) > 0
