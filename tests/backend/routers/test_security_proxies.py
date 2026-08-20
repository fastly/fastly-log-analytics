from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app
from tests.conftest import MOCK_SERVICE_ID

client = TestClient(app)


def test_get_proxies_endpoint_not_found_without_auth():
    # Enforces tenancy/auth
    resp = client.post(
        "/api/security/proxies", json={"start_time": "2026-08-01T00:00:00Z", "end_time": "2026-08-02T00:00:00Z"}
    )
    assert resp.status_code in (400, 401, 403, 404)


def test_get_proxies_endpoint_success(client):
    # Mocking get_security_proxies to return dummy data matching SecurityProxiesResponse
    mock_res = {
        "active_proxies_count": 5,
        "tunnel_requests_count": 100,
        "distance_mismatches_count": 2,
        "traffic_quality": [{"label": "High", "value": 10}],
        "suspicious_isps": [{"isp": "Suspicious ISP", "count": 12}],
        "active_clients": [
            {
                "ip": "1.1.1.1",
                "risk_level": "High",
                "asn_name": "Cloudflare",
                "impossible_distance": True,
                "distance_km": 12000.5,
                "rtt_min_ms": 1.2,
                "tcp_rtt_ms": 3.4,
            },
            {
                "ip": "2.2.2.2",
                "risk_level": "Medium",
                "asn_name": "Google",
                "impossible_distance": False,
                "distance_km": 200.0,
                "rtt_min_ms": 10.0,
                "tcp_rtt_ms": 12.0,
            },
        ],
    }

    with patch("backend.repositories.security.get_security_proxies", return_value=mock_res) as mock_get:
        resp = client.post(
            "/api/security/proxies",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"start_time": "2026-08-01T00:00:00Z", "end_time": "2026-08-02T00:00:00Z"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["active_proxies_count"] == 5
        assert data["tunnel_requests_count"] == 100
        assert data["distance_mismatches_count"] == 2
        assert len(data["active_clients"]) == 2
        assert data["active_clients"][0]["ip"] == "1.1.1.1"
        assert data["active_clients"][0]["impossible_distance"] is True


def test_export_proxies_csv_endpoint_high_threshold(client):
    mock_res = {
        "active_proxies_count": 5,
        "tunnel_requests_count": 100,
        "distance_mismatches_count": 2,
        "traffic_quality": [],
        "suspicious_isps": [],
        "active_clients": [
            {
                "ip": "1.1.1.1",
                "risk_level": "High",
                "asn_name": "Cloudflare",
                "impossible_distance": True,
            },
            {
                "ip": "2.2.2.2",
                "risk_level": "Medium",
                "asn_name": "Google",
                "impossible_distance": False,
            },
        ],
    }

    with patch("backend.repositories.security.get_security_proxies", return_value=mock_res):
        resp = client.get(
            "/api/security/proxies/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-02T00:00:00Z",
                "threshold": "High",
                "format": "fastly-acl",
            },
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "attachment; filename=" in resp.headers["content-disposition"]
        content = resp.content.decode("utf-8")
        assert "ip,comment" in content
        assert "1.1.1.1" in content
        assert "Impossible distance mismatch" in content
        assert "2.2.2.2" not in content  # Medium should be excluded with High threshold


def test_export_proxies_csv_endpoint_medium_threshold(client):
    mock_res = {
        "active_proxies_count": 5,
        "tunnel_requests_count": 100,
        "distance_mismatches_count": 2,
        "traffic_quality": [],
        "suspicious_isps": [],
        "active_clients": [
            {
                "ip": "1.1.1.1",
                "risk_level": "High",
                "asn_name": "Cloudflare",
                "impossible_distance": False,
            },
            {
                "ip": "2.2.2.2",
                "risk_level": "Medium",
                "asn_name": "Google",
                "impossible_distance": False,
            },
        ],
    }

    with patch("backend.repositories.security.get_security_proxies", return_value=mock_res):
        resp = client.get(
            "/api/security/proxies/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-02T00:00:00Z",
                "threshold": "Medium",
                "format": "fastly-acl",
            },
        )
        assert resp.status_code == 200
        content = resp.content.decode("utf-8")
        assert "1.1.1.1" in content
        assert "Behavioral VPN/Proxy tunnel" in content
        assert "2.2.2.2" in content  # Medium should be included


def test_export_proxies_csv_endpoint_plain_format(client):
    mock_res = {
        "active_proxies_count": 5,
        "tunnel_requests_count": 100,
        "distance_mismatches_count": 2,
        "traffic_quality": [],
        "suspicious_isps": [],
        "active_clients": [
            {
                "ip": "1.1.1.1",
                "risk_level": "High",
                "asn_name": "Cloudflare",
                "impossible_distance": True,
            }
        ],
    }

    with patch("backend.repositories.security.get_security_proxies", return_value=mock_res):
        resp = client.get(
            "/api/security/proxies/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-02T00:00:00Z",
                "threshold": "High",
                "format": "plain",
            },
        )
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        content = resp.content.decode("utf-8")
        assert content.strip() == "1.1.1.1"


def test_export_proxies_csv_endpoint_deduplication(client):
    mock_res = {
        "active_proxies_count": 5,
        "tunnel_requests_count": 100,
        "distance_mismatches_count": 2,
        "traffic_quality": [],
        "suspicious_isps": [],
        "active_clients": [
            {
                "ip": "1.1.1.1",
                "risk_level": "High",
                "asn_name": "Cloudflare",
                "impossible_distance": True,
            },
            {
                "ip": "1.1.1.1",
                "risk_level": "High",
                "asn_name": "Cloudflare",
                "impossible_distance": True,
            },
            {
                "ip": "2.2.2.2",
                "risk_level": "High",
                "asn_name": "Google",
                "impossible_distance": False,
            },
        ],
    }

    with patch("backend.repositories.security.get_security_proxies", return_value=mock_res):
        resp = client.get(
            "/api/security/proxies/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "start_time": "2026-08-01T00:00:00Z",
                "end_time": "2026-08-02T00:00:00Z",
                "threshold": "High",
                "format": "plain",
            },
        )
        assert resp.status_code == 200
        content = resp.content.decode("utf-8").strip().split("\n")
        assert content == ["1.1.1.1", "2.2.2.2"]
