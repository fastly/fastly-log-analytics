"""High-scale Security reads return real data under the standard response contract."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend.high_scale import dashboard as hs_dashboard
from backend.high_scale import security as hs
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService

START = "2026-10-18T00:00:00Z"
END = "2026-10-19T00:00:00Z"
_EPOCH = int(datetime(2026, 10, 18, 3, 0, tzinfo=UTC).timestamp())
_TAG = re.compile(r"hs_security:(\w+)")


class _Client:
    """Routes each query to canned rows by its ``hs_security:<tag>`` marker and records every call."""

    def __init__(self, rows: dict[str, list[dict[str, Any]]] | None = None, error: Exception | None = None):
        self.rows = rows or {}
        self.error = error
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        match = _TAG.search(sql)
        assert match, f"query is missing its hs_security tag: {sql[:120]}"
        tag = match.group(1)
        self.calls.append((tag, sql, params or {}))
        return self.rows.get(tag, [])

    def tags(self) -> list[str]:
        return [tag for tag, _, _ in self.calls]

    def call(self, tag: str) -> tuple[str, dict[str, Any]]:
        return next((sql, params) for t, sql, params in self.calls if t == tag)


def _service(client: _Client) -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=client,
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


def _req(filters: dict[str, Any] | None = None, bucket_seconds: int = 3600) -> SimpleNamespace:
    return SimpleNamespace(filters=filters or {}, bucket_seconds=bucket_seconds)


_COUNTRY_US = {"country": {"mode": "include", "values": ["US"]}}
_BOT_ENTRY = {"id": "googlebot", "categories": ["search-engine"], "verification": {"domains": ["googlebot.com"]}}


def _match_googlebot(ua: str | None) -> tuple[dict[str, Any], ...]:
    return (_BOT_ENTRY,) if "Googlebot" in (ua or "") else ()


def test_every_section_uses_the_standard_contract_shapes():
    client = _Client(
        {
            "tls_fingerprints": [{"fingerprint": "abc", "ip_count": "2", "request_count": "5"}],
            "fingerprint_coverage": [{"total_rows": "10", "populated": "4"}],
            "req_size_dist": [{"bucket": "0-256B", "count": "9", "min_val": "0"}],
            "conn_reuse_dist": [{"bucket": "2-5", "count": "4", "min_val": "2"}],
            "ipv6_adoption": [{"bucket_ts": _EPOCH, "pct": 40.0}],
            "proxy_dist": [{"type": "vpn", "count": "7"}],
            "verified_bots_ts": [{"bucket_ts": _EPOCH, "bot_type": "search-engine", "count": "3"}],
            "top_ips_header": [{"ip": "10.0.1.5", "max_header": "9000"}],
        }
    )
    res = hs.security_aggregates(
        _service(client),
        _req(),
        START,
        END,
        sections={
            "tls_fingerprints",
            "fingerprint_coverage",
            "req_size_dist",
            "conn_reuse_dist",
            "ipv6_adoption",
            "proxy_dist",
            "verified_bots_ts",
            "top_ips_header",
        },
    ).model_dump(exclude_unset=True)

    assert res["tls_fingerprints"] == [{"fingerprint": "abc", "ip_count": 2, "request_count": 5}]
    assert res["fingerprint_coverage"] == {"tls_ciphers_sha": 0.4}
    assert res["req_size_dist"] == [{"bucket": "0-256B", "count": 9}]
    assert res["conn_reuse_dist"] == [{"bucket": "2-5", "count": 4}]
    assert res["ipv6_adoption"] == [{"time": "2026-10-18T03:00:00Z", "pct": 40.0}]
    assert res["proxy_dist"] == [{"type": "vpn", "count": 7}]
    assert res["verified_bots_ts"] == [{"time": "2026-10-18T03:00:00Z", "bot_type": "search-engine", "count": 3}]
    assert res["top_ips_header"] == [{"ip": "10.0.1.5", "max_header": 9000}]


def test_fingerprint_coverage_is_computed_not_hardcoded():
    client = _Client({"fingerprint_coverage": [{"total_rows": "0", "populated": "0"}]})
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"fingerprint_coverage"})
    assert res.fingerprint_coverage == {"tls_ciphers_sha": 0.0}


def test_proxy_and_header_sections_read_the_standard_columns():
    client = _Client()
    hs.security_aggregates(
        _service(client), _req(), START, END, sections={"proxy_dist", "req_size_dist", "ipv6_adoption"}
    )
    assert "custom_fields['p_type']" in client.call("proxy_dist")[0]
    assert "req_header_bytes" in client.call("req_size_dist")[0]
    assert "toStartOfHour" in client.call("ipv6_adoption")[0]


def test_sections_selector_only_runs_requested_queries():
    client = _Client()
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"proxy_dist"})
    assert client.tags() == ["proxy_dist"]
    assert res.tls_fingerprints == []


def test_no_sections_runs_every_section_including_bots_and_proxies():
    client = _Client()
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.core.duckdb.get_asn_names", return_value={}),
    ):
        hs.security_aggregates(_service(client), _req(), START, END)
    assert set(client.tags()) >= {
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "verified_bots_ts",
        "ngwaf_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "top_bots_ua",
        "proxy_stats",
        "traffic_quality",
        "suspicious_isps",
        "active_clients",
    }


def test_request_filters_are_bound_into_every_request_facts_query():
    client = _Client()
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.core.duckdb.get_asn_names", return_value={}),
    ):
        hs.security_aggregates(_service(client), _req(_COUNTRY_US), START, END)
    assert client.calls
    for tag, sql, params in client.calls:
        assert params["filter_0_0"] == "US", tag
        assert "country IN ({filter_0_0:String})" in sql, tag


def test_unfiltered_ngwaf_sections_read_the_security_projection():
    client = _Client(
        {
            "ngwaf_bots": [
                {"bot_name": "bingbot", "wk_name": "bing", "bot_cat": "search", "request_count": "11"},
                {"bot_name": "other", "wk_name": "", "bot_cat": "", "request_count": "2"},
            ],
            "ngwaf_verified_bots_ts": [{"bucket_ts": _EPOCH, "bot_name": "bingbot", "count": "4"}],
        }
    )
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"):
        res = hs.security_aggregates(
            _service(client), _req(), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        )
    summary_sql = client.call("ngwaf_bots")[0]
    assert "FROM security_minute_dimensions" in summary_sql
    assert "dimension='ngwaf_bot'" in summary_sql
    assert "request_facts" not in summary_sql
    assert "FROM security_minute_dimensions" in client.call("ngwaf_verified_bots_ts")[0]
    assert res.ngwaf_configured is True
    assert [r.model_dump() for r in res.ngwaf_verified_bots] == [
        {"bot_name": "bingbot", "wellknown_bot_name": "bing", "category": "search", "request_count": 11},
        {"bot_name": "other", "wellknown_bot_name": None, "category": None, "request_count": 2},
    ]
    assert [r.model_dump() for r in res.ngwaf_verified_bots_ts] == [
        {"time": "2026-10-18T03:00:00Z", "bot_name": "bingbot", "count": 4}
    ]


def test_filtered_ngwaf_sections_read_the_persisted_fact_keys():
    client = _Client()
    hs.security_aggregates(
        _service(client),
        _req(_COUNTRY_US),
        START,
        END,
        sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
    )
    for tag in ("ngwaf_bots", "ngwaf_verified_bots_ts"):
        sql, params = client.call(tag)
        assert "FROM request_facts" in sql
        assert "custom_fields['ngwaf_bot']" in sql
        assert "security_minute_dimensions" not in sql
        assert params["filter_0_0"] == "US"


def test_ngwaf_not_configured_is_reported_from_service_config():
    with patch("backend.config.get_ngwaf_workspace_id", return_value=None):
        res = hs.security_aggregates(
            _service(_Client()), _req(), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        )
    assert res.ngwaf_configured is False


def test_wellknown_bots_carry_real_verification_counters():
    client = _Client(
        {
            "wellknown_bots": [
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "ip": "66.249.66.1", "cnt": "8"},
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "ip": "203.0.113.9", "cnt": "2"},
            ]
        }
    )
    hostnames = {
        "66.249.66.1": ("crawl-66-249-66-1.googlebot.com", "resolved", True),
        "203.0.113.9": ("evil.example.net", "resolved", False),
    }
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="(?i)googlebot"),
        patch("backend.utils.rdns_cache.get_hostnames", return_value=hostnames),
        patch("backend.utils.rdns_cache.enqueue"),
    ):
        res = hs.security_aggregates(_service(client), _req(), START, END, sections={"wellknown_bots"})
    sql, params = client.call("wellknown_bots")
    assert "match(custom_fields['ua'], {bot_pattern:String})" in sql
    assert params["bot_pattern"] == "(?i)googlebot"
    assert len(res.wellknown_bots) == 1
    bot = res.wellknown_bots[0]
    assert (bot.id, bot.request_count) == ("googlebot", 10)
    assert bot.verified_count == 8
    assert bot.impersonator_count == 2
    assert bot.verification_coverage == 1.0


def test_top_bots_classifies_user_agents_and_lists_ngwaf_bots():
    client = _Client(
        {
            "top_bots_ua": [
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "cnt": "30"},
                {"ua": "curl/8", "cnt": "50"},
            ],
            "ngwaf_bots": [{"bot_name": "bingbot", "wk_name": "bing", "bot_cat": "search", "request_count": "11"}],
        }
    )
    with patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot):
        res = hs.top_bots(_service(client), _req(), START, END)
    assert [b.model_dump(exclude_unset=True) for b in res.bots] == [
        {"id": "googlebot", "name": "Googlebot", "category": "search-engine", "request_count": 30}
    ]
    assert [b.model_dump(exclude_unset=True) for b in res.ngwaf_bots] == [
        {"name": "bingbot", "category": "search", "request_count": 11}
    ]
    assert "LIMIT 10" in client.call("ngwaf_bots")[0]


def test_top_bots_honours_request_filters():
    client = _Client()
    hs.top_bots(_service(client), _req(_COUNTRY_US), START, END)
    assert all(params["filter_0_0"] == "US" for _, _, params in client.calls)


_CLIENT_ROW = {
    "ip": "198.51.100.77",
    "asn": "15169",
    "rtt_min_ms": 5.0,
    "tcp_rtt_ms": 100.0,
    "distance_km": 8800.4,
    "pop": "SJC",
    "client_lat": 51.5074,
    "client_lon": -0.1278,
    "pop_lat": 37.3382,
    "pop_lon": -121.8863,
    "impossible_distance": 1,
    "risk_level": "High",
    "country": "GB",
    "city": "",
}


def _proxy_client() -> _Client:
    return _Client(
        {
            "proxy_stats": [
                {"active_proxies_count": "3", "total_requests_count": "40", "distance_mismatches_count": "2"}
            ],
            "traffic_quality": [
                {"type": "Direct Connection", "count": "75"},
                {"type": "Active Tunnel / Proxy", "count": "25"},
            ],
            "suspicious_isps": [{"asn": "15169", "count": "25"}, {"asn": None, "count": "1"}],
            "active_clients": [_CLIENT_ROW],
        }
    )


def test_proxies_report_real_counts_quality_isps_and_clients():
    with patch("backend.core.duckdb.get_asn_names", return_value={15169: "Example Net"}):
        res = hs.get_proxies_data(_service(_proxy_client()), _req(), START, END)
    assert (res["active_proxies_count"], res["tunnel_requests_count"], res["distance_mismatches_count"]) == (3, 40, 2)
    assert [(q["label"], q["value"]) for q in res["traffic_quality"]] == [
        ("Direct Connection", 75.0),
        ("Active Tunnel / Proxy", 25.0),
    ]
    assert [(i["isp"], i["asn"], i["count"]) for i in res["suspicious_isps"]] == [
        ("Example Net", 15169, 25),
        ("Unknown ISP", None, 1),
    ]
    client = res["active_clients"][0]
    assert client["ip"] == "198.51.100.77"
    assert client["asn_name"] == "Example Net"
    assert client["impossible_distance"] is True
    assert client["risk_level"] == "High"
    assert client["city"] is None
    assert client["country"] == "GB"
    assert client["distance_km"] == 8800.4


def test_proxies_mask_client_ips_for_analyst_sessions():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        masked = hs.get_proxies_data(_service(_proxy_client()), _req(), START, END, mask_ips=True)
        clear = hs.get_proxies_data(_service(_proxy_client()), _req(), START, END, mask_ips=False)
    assert masked["active_clients"][0]["ip"] == "198.51.100.xxx"
    assert clear["active_clients"][0]["ip"] == "198.51.100.77"


def test_top_ips_header_masks_ips_for_analyst_sessions():
    client = _Client({"top_ips_header": [{"ip": "10.0.1.5", "max_header": "9000"}]})
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"top_ips_header"}, mask_ips=True)
    assert res.top_ips_header[0].ip == "10.0.1.xxx"


def test_proxies_section_is_served_inside_aggregates_with_masking():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = hs.security_aggregates(_service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=True)
    assert res.active_proxies_count == 3
    assert res.active_clients[0].ip == "198.51.100.xxx"


def test_proxies_with_no_rows_report_zero_without_dividing_by_nothing():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = hs.get_proxies_data(_service(_Client()), _req(), START, END)
    assert res["active_proxies_count"] == 0
    assert res["traffic_quality"] == []
    assert res["active_clients"] == []


def test_active_clients_list_high_risk_first():
    client = _proxy_client()
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        hs.get_proxies_data(_service(client), _req(), START, END)
    assert "ORDER BY multiIf(risk_level = 'High', 0, risk_level = 'Medium', 1, 2)" in client.call("active_clients")[0]


def test_clickhouse_failures_propagate_instead_of_rendering_empty_cards():
    client = _Client(error=RuntimeError("clickhouse unavailable"))
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.security_aggregates(_service(client), _req(), START, END, sections={"proxy_dist"})
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.top_bots(_service(client), _req(), START, END)
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.get_security_threat_intel(_service(client), START, END)


def test_threat_intel_returns_ranked_fingerprints():
    client = _Client(
        {
            "threat_intel": [
                {
                    "tls_fingerprint": "ja3-a",
                    "requests": "30",
                    "top_country": "US",
                    "matched_waf_rules_count": "2",
                    "bot_percentage": 12.5,
                    "is_anonymized_proxy": 1,
                },
                {
                    "tls_fingerprint": "ja3-b",
                    "requests": "5",
                    "top_country": "",
                    "matched_waf_rules_count": "0",
                    "bot_percentage": 0.0,
                    "is_anonymized_proxy": 0,
                },
            ]
        }
    )
    res = hs.get_security_threat_intel(_service(client), START, END)
    assert res == {
        "data": [
            {
                "tls_fingerprint": "ja3-a",
                "requests": 30,
                "top_country": "US",
                "matched_waf_rules_count": 2,
                "bot_percentage": 12.5,
                "is_anonymized_proxy": True,
            },
            {
                "tls_fingerprint": "ja3-b",
                "requests": 5,
                "top_country": "Unknown",
                "matched_waf_rules_count": 0,
                "bot_percentage": 0.0,
                "is_anonymized_proxy": False,
            },
        ]
    }


def test_dashboard_bundle_serves_real_top_bots():
    client = _Client(
        {
            "top_bots_ua": [{"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "cnt": "30"}],
            "ngwaf_bots": [{"bot_name": "bingbot", "wk_name": "", "bot_cat": "search", "request_count": "11"}],
        }
    )
    request = SimpleNamespace(filters={}, bucket_seconds=3600, sections=None)
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch.object(hs_dashboard, "aggregates", return_value="aggregates"),
    ):
        out = hs_dashboard.bundle(_service(client), request, START, END)
    assert out["aggregates"] == "aggregates"
    assert [b.name for b in out["top_bots"].bots] == ["Googlebot"]
    assert [b.name for b in out["top_bots"].ngwaf_bots] == ["bingbot"]
