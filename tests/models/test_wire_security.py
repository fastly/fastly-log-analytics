"""Security response row models keep every producer's wire bytes (repository + high-scale)."""

from __future__ import annotations

from unittest.mock import patch

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService
from backend.high_scale.security import security_aggregates as hs_security_aggregates
from backend.models.security import (
    SecurityAggregatesResponse,
    SecurityProxiesResponse,
    SecurityTopBotsResponse,
)
from backend.repositories._base import _safe_table
from backend.repositories.security import get_security_aggregates, get_security_proxies, get_top_bots
from tests._helpers.wire import assert_sections_wire_identical

START = "2026-08-01T00:00:00Z"
END = "2026-08-02T00:00:00Z"


def _seed(con, src, n=200):
    table = _safe_table(src["name"])
    con.execute(
        f"""CREATE TABLE {table} (
            timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER,
            lat DOUBLE, lon DOUBLE, asn INTEGER, country VARCHAR, city VARCHAR, tls_ciphers_sha VARCHAR,
            req_header_bytes INTEGER, conn_requests INTEGER, is_ipv6 BOOLEAN, p_type VARCHAR,
            waf_sig VARCHAR, waf_req_id VARCHAR)"""
    )
    rows = []
    for i in range(n):
        rows.append(
            (
                f"2026-08-01 {12 + i % 3}:{i % 60:02d}:00Z",
                f"10.0.{i % 7}.{i % 11}",
                "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" if i % 2 else "curl/8",
                "SJC",
                [5, 10, 500, 1000][i % 4],
                1000,
                37.0,
                -121.0,
                [1111, 2222, 3333][i % 3],
                "US",
                "San Jose",
                f"fp{i % 5}",
                100 + (i * 97) % 40000,
                1 + i % 120,
                i % 3 == 0,
                ["vpn", "tor", ""][i % 3],
                "VERIFIED-BOT.search-engine,OTHER.tag" if i % 4 == 0 else None,
                None,
            )
        )
    con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * 18)})", rows)


def test_repository_aggregates_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source)
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = get_security_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time=START,
            end_time=END,
            filters={},
            bucket_seconds=3600,
        )
    assert_sections_wire_identical(
        SecurityAggregatesResponse,
        res,
        non_empty=("tls_fingerprints", "req_size_dist", "conn_reuse_dist", "ipv6_adoption", "proxy_dist"),
    )


def test_repository_masked_top_ips_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source)
    res = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=START,
        end_time=END,
        filters={},
        sections={"top_ips_header"},
        mask_ips=True,
    )
    assert_sections_wire_identical(SecurityAggregatesResponse, res, non_empty=("top_ips_header",))


def test_repository_proxies_and_top_bots_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source, n=300)
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        proxies = get_security_proxies(
            con=in_memory_duckdb, src=test_service_source, start_time=START, end_time=END, filters={}
        )
    assert_sections_wire_identical(
        SecurityProxiesResponse, proxies, non_empty=("traffic_quality", "suspicious_isps", "active_clients")
    )
    assert_sections_wire_identical(SecurityAggregatesResponse, proxies)

    bots = get_top_bots(con=in_memory_duckdb, src=test_service_source, start_time=START, end_time=END, filters={})
    assert_sections_wire_identical(SecurityTopBotsResponse, bots)


def test_repository_wellknown_and_verified_bots_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source, n=60)
    entry = {"id": "googlebot", "categories": ["search-engine"], "verification": {"domains": ["googlebot.com"]}}
    with (
        patch(
            "backend.utils.bot_sources.build_matcher",
            return_value=lambda ua: (entry,) if "Googlebot" in (ua or "") else (),
        ),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=None),
        patch("backend.utils.rdns_cache.get_hostnames", return_value={}),
        patch("backend.utils.rdns_cache.enqueue"),
    ):
        res = _wellknown_and_verified(in_memory_duckdb, test_service_source)
    assert_sections_wire_identical(SecurityAggregatesResponse, res, non_empty=("wellknown_bots", "verified_bots_ts"))


def _wellknown_and_verified(con, src):
    res = get_security_aggregates(
        con=con,
        src=src,
        start_time=START,
        end_time=END,
        filters={},
        sections={"wellknown_bots", "verified_bots_ts"},
        bucket_seconds=3600,
    )
    return res


class _FakeClickHouse:
    def execute(self, sql, params=None):
        if "p_desc" in sql:
            return [{"type": "vpn", "count": 7}]
        if "tls_ciphers_sha" in sql and "ngwaf_bot" not in sql:
            return [{"fingerprint": "abc", "count": 5, "ips": 2}]
        if "req_bytes" in sql:
            return [{"label": "<1KB", "count": 9}]
        if "is_ipv6" in sql:
            return [{"version": "IPv4", "count": 9}]
        if "conn_requests" in sql:
            return [{"label": "1", "count": 4}]
        if "ngwaf_bot" in sql:
            return [{"bot_name": "bing", "request_count": 11}]
        return []


def test_high_scale_aggregates_wire_identical(monkeypatch):
    from backend.high_scale import security as hs_security

    svc = HighScaleService(
        service_id="svc",
        client=_FakeClickHouse(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=None,
            coverage_end=None,
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    captured: dict = {}

    def _capture(cls, **kwargs):
        captured.update(kwargs)
        return cls(**kwargs)

    monkeypatch.setattr(hs_security.SecurityAggregatesResponse, "with_telemetry", classmethod(_capture))
    hs_security_aggregates(svc, None, None, None)
    captured["bots"] = []
    captured["ngwaf_bots"] = [r.model_dump() for r in captured["ngwaf_bots"]]
    assert captured["tls_fingerprints"][0]["ips"] == 2
    assert captured["req_size_dist"][0]["label"] == "<1KB"
    assert captured["ipv6_adoption"][0]["version"] == "IPv4"
    assert_sections_wire_identical(
        SecurityAggregatesResponse,
        captured,
        non_empty=("tls_fingerprints", "req_size_dist", "ipv6_adoption", "ngwaf_verified_bots", "ngwaf_bots"),
    )
