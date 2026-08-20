from backend.models.security import SecurityProxiesRequest, SecurityProxiesResponse


def test_security_proxies_response_validation():
    data = {
        "active_proxies_count": 42,
        "tunnel_requests_count": 9800,
        "distance_mismatches_count": 5,
        "traffic_quality": [
            {"type": "Direct Connection", "count": 8000},
            {"type": "WiFi / Mobile", "count": 1500},
            {"type": "Active Tunnel / Proxy", "count": 300},
        ],
        "suspicious_isps": [
            {"asn_name": "DigitalOcean, LLC", "count": 120},
            {"asn_name": "Linode, LLC", "count": 80},
        ],
        "active_clients": [
            {
                "ip": "192.0.2.1",
                "risk_level": "High",
                "asn_name": "DigitalOcean, LLC",
                "impossible_distance": True,
                "rtt_min_ms": 1.2,
                "tcp_rtt_ms": 120.5,
                "distance_km": 8400.2,
            }
        ],
    }
    resp = SecurityProxiesResponse(**data)
    assert resp.active_proxies_count == 42
    assert len(resp.active_clients) == 1
    assert resp.active_clients[0].impossible_distance is True


def test_security_proxies_request_validation():
    req = SecurityProxiesRequest(start_time="2026-08-17T00:00:00Z", end_time="2026-08-17T23:59:59Z")
    assert req.start_time == "2026-08-17T00:00:00Z"
    assert req.end_time == "2026-08-17T23:59:59Z"
