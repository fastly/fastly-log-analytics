"""Unit tests for Round 30 High-Scale Security 3d-30d sampling."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale import security as hs_security
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.security import _security_sample_stride

pytestmark = pytest.mark.no_postgres

_RANGE_RE = re.compile(
    r"event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)"
)


class _FakeRealCHClient:
    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params or {})))
        if callable(self._handler):
            return self._handler(sql, params or {})
        return list(self._handler or [])


def _service(client: Any, sid: str = "svc-opt30-sec") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt30-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


def test_security_sample_stride_tiers() -> None:
    assert _security_sample_stride(3600) == 1
    assert _security_sample_stride(43_200) == 1
    assert _security_sample_stride(86_400) == 4
    assert _security_sample_stride(259_200) == 10
    assert _security_sample_stride(604_800) == 40
    assert _security_sample_stride(1_209_600) == 100
    assert _security_sample_stride(2_592_000) == 200


def test_security_aggregates_samples_30d_and_leaves_24h_unsampled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: "")
    monkeypatch.setattr("backend.core.fos_client.get_asn_names", lambda *_a, **_kw: {})
    hs_security._security_memo.cache.clear()
    hs_security._security_memo._telemetry_cache.clear()

    t0 = 1_790_002_800

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* hs_security:overview_5in1 */" in sql:
            return [
                {
                    "bucket_ts": t0,
                    "total_rows": 100,
                    "tls_populated": 80,
                    "ipv6_rows": 25,
                    "h_map": {"0-256B": 100},
                    "c_map": {"1 (New)": 100},
                    "p_map": {"hosting": 10},
                    "vb_map": {},
                }
            ]
        if "/* hs_security:tls_and_ips */" in sql:
            return [
                {"kind": "fp", "k": "sha-1", "v1": 4, "v2": 50},
                {"kind": "ip", "k": "203.0.113.5", "v1": 8192, "v2": 0},
            ]
        if "/* hs_security:proxies_combined */" in sql:
            return [
                {
                    "active_proxies_count": 3,
                    "total_requests_count": 15,
                    "distance_mismatches_count": 2,
                    "q_tunnel": 10,
                    "q_wifi": 20,
                    "q_total": 100,
                    "isp_map": {13335: 7},
                    "top_clients": [],
                }
            ]
        return []

    client = _FakeRealCHClient(_handler)
    req = SimpleNamespace(filters={}, bucket_seconds=3600)
    end = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    res_30d = hs_security.security_aggregates(
        _service(client, "svc-opt30-30d"),
        req,
        start_30d.isoformat(),
        end.isoformat(),
        sections={
            "tls_fingerprints",
            "top_ips_header",
            "fingerprint_coverage",
            "req_size_dist",
            "ipv6_adoption",
            "proxy_dist",
            "conn_reuse_dist",
            "proxies",
        },
    ).model_dump()

    overview_sql = next(q for q, _ in client.calls if "/* hs_security:overview_5in1 */" in q)
    assert "AND line_ordinal % 200 = 0" in overview_sql

    tls_sql = next(q for q, _ in client.calls if "/* hs_security:tls_and_ips */" in q)
    assert tls_sql.count("toDateTime(") >= 4
    fps_part, ips_part = tls_sql.split("UNION ALL", 1)
    assert 1 <= len(_RANGE_RE.findall(fps_part)) <= 16
    assert 1 <= len(_RANGE_RE.findall(ips_part)) <= 16

    proxies_sql = next(q for q, _ in client.calls if "/* hs_security:proxies_combined */" in q)
    q_total_part, outer_part = proxies_sql.split("AS q_total", 1)
    assert 1 <= len(_RANGE_RE.findall(q_total_part)) <= 16
    assert 1 <= len(_RANGE_RE.findall(outer_part)) <= 16

    # Scaled by 200 while percentage and max_header stay unscaled.
    assert res_30d["ipv6_adoption"] == [{"time": "2026-09-21T15:00:00Z", "pct": 25.0}]
    assert res_30d["req_size_dist"] == [{"bucket": "0-256B", "count": 20000}]
    assert res_30d["tls_fingerprints"] == [{"fingerprint": "sha-1", "ip_count": 4, "request_count": 10000}]
    assert res_30d["top_ips_header"] == [{"ip": "203.0.113.5", "max_header": 8192}]
    assert res_30d["tunnel_requests_count"] == 3000
    assert res_30d["active_proxies_count"] == 3
    assert res_30d["suspicious_isps"] == [{"isp": "AS13335", "asn": 13335, "count": 1400}]
    assert res_30d["traffic_quality"] == [
        {"label": "Direct Connection", "value": 70.0},
        {"label": "WiFi / Mobile", "value": 20.0},
        {"label": "Active Tunnel / Proxy", "value": 10.0},
    ]

    # Sub-24h (6h) window remains completely unsampled.
    client.calls.clear()
    start_6h = end - timedelta(hours=6)
    res_6h = hs_security.security_aggregates(
        _service(client, "svc-opt30-6h"),
        req,
        start_6h.isoformat(),
        end.isoformat(),
        sections={
            "tls_fingerprints",
            "top_ips_header",
            "fingerprint_coverage",
            "req_size_dist",
            "ipv6_adoption",
            "proxy_dist",
            "conn_reuse_dist",
            "proxies",
        },
    ).model_dump()
    for q, _ in client.calls:
        assert "line_ordinal %" not in q
        assert "toDateTime(" not in q
    assert res_6h["req_size_dist"] == [{"bucket": "0-256B", "count": 100}]
    assert res_6h["tls_fingerprints"] == [{"fingerprint": "sha-1", "ip_count": 4, "request_count": 50}]
