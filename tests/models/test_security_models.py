from backend.models.security import SecurityAggregatesRequest, SecurityProxiesResponse


def test_security_proxies_response_validation():
    data = {
        "active_proxies_count": 42,
        "tunnel_requests_count": 9800,
        "distance_mismatches_count": 5,
        "traffic_quality": [
            {"label": "Direct Connection", "value": 81.6},
            {"label": "WiFi / Mobile", "value": 15.3},
            {"label": "Active Tunnel / Proxy", "value": 3.1},
        ],
        "suspicious_isps": [
            {"isp": "DigitalOcean, LLC", "asn": 14061, "count": 120},
            {"isp": "Linode, LLC", "asn": 63949, "count": 80},
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


def test_security_aggregates_request_validation():
    req = SecurityAggregatesRequest(start_time="2026-08-17T00:00:00Z", end_time="2026-08-17T23:59:59Z")
    assert req.start_time == "2026-08-17T00:00:00Z"
    assert req.end_time == "2026-08-17T23:59:59Z"
