"""Golden characterization tests for all 49 registered InsightDefinitions.

Built BEFORE any performance optimization to guarantee zero behavior change
across all insight detections, coalesced pre-aggregations, and row processors.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.repositories._base import _safe_table
from backend.repositories.insights import get_cache_collapse_detail, get_insights, registry
from tests._helpers.duckdb_fixtures import generate_mock_records

GOLDEN_SNAPSHOT_PATH = Path(__file__).parent / "fixtures" / "insights_golden_snapshot.json"

FIXED_NOW = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
WINDOW_HOURS = 1.0
BASELINE_HOURS = 24.0

_MOCK_POP_MAP = {
    "JFK": (40.6413, -73.7781),
    "LAX": (33.9416, -118.4085),
    "DEN": (39.8561, -104.6737),
    "IAD": (38.9531, -77.4565),
}
_MOCK_ASN_NAMES = {
    "64501": "MonoNet ASN",
    "64502": "MetroISP Corp",
    "64503": "LossyTransit Telecom",
    "64504": "BrandNew Cloud",
    "64505": "HostingShift Networks",
}
_MOCK_DMA_MAP = {"501": "New York, NY", "803": "Los Angeles, CA"}


def _create_full_schema_table(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    """Create a log table with all raw catalog columns plus any required_fields."""
    col_types = {
        f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"])
        for f in LOG_FIELD_CATALOG
        if f.get("vcl") is not None
    }
    for d in registry.get_all():
        for req_col in d.required_fields:
            col_types.setdefault(req_col, "VARCHAR")
    for extra_col in ("ja3", "ja4", "region"):
        col_types.setdefault(extra_col, "VARCHAR")
    cols_sql = ", ".join(f'"{c}" {t}' for c, t in col_types.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql})")
    return list(col_types.keys())


_BASE_ROW_DEFAULTS: dict[str, Any] = {
    "ip": "192.0.2.1",
    "status": 200,
    "elapsed": 25000,
    "cache": "HIT",
    "resp_bytes": 2048,
    "host": "www.example.com",
    "url": "/normal/page",
    "method": "GET",
    "proto": "HTTP/2",
    "ua": "Mozilla/5.0 StandardBrowser/1.0",
    "pop": "IAD",
    "country": "US",
    "region": "VA",
    "city": "Ashburn",
    "asn": "64500",
    "metro": None,
    "lat": 39.0438,
    "lon": -77.4874,
    "tcp_rtt": 20000,
    "rtt_var": 2000,
    "ploss": 0.0,
    "retrans": 0,
    "delivery_rate": 0,
    "ja3": "ja3_default",
    "ja4": "ja4_default",
    "tls_ciphers_sha": "cipher_default",
    "req_header_bytes": 400,
    "conn_requests": 2,
    "server_region": "",
    "ottfb": None,
    "ttl": 0,
    "hits": 0,
    "age": 0,
    "ost": None,
    "oretries": 0,
    "oip": "",
    "rid": "",
    "prid": "",
    "edge": True,
    "waf_sig": "",
    "p_type": "",
    "c_type": "",
    "c_speed": "",
    "transport": "",
    "referer": "",
    "resp_header_content_encoding": "br",
    "cookie_session": "",
    "oconnect_ms": None,
}


def _build_comprehensive_anomaly_rows() -> list[dict[str, Any]]:
    """Construct a deterministic synthetic dataset using log_factory + anomaly rows
    that triggers all 49 registered InsightDefinitions simultaneously.
    """
    rng = random.Random(42)
    base_records = generate_mock_records(
        {"log_fields": {"groups": ["A", "B", "C", "D", "E", "F", "G"]}}, num_logs=40, hours_ago=12, rng=rng
    )
    w_ts_base = FIXED_NOW - timedelta(minutes=30)
    b_ts_base = FIXED_NOW - timedelta(hours=12)
    earliest_ts = FIXED_NOW - timedelta(hours=24, minutes=55)

    def _row(ts: datetime, **overrides: Any) -> dict[str, Any]:
        return {"timestamp": ts.isoformat(), **_BASE_ROW_DEFAULTS, **overrides}

    rows: list[dict[str, Any]] = [_row(earliest_ts, ip="192.0.2.250", cache="HIT")]

    def _add(ts: datetime, n: int, **kw: Any) -> None:
        for i in range(n):
            rows.append(_row(ts + timedelta(milliseconds=i), **kw))

    for idx, rec in enumerate(base_records):
        rows.append(
            _row(
                b_ts_base - timedelta(minutes=idx * 5),
                ip=rec.get("ip", "192.0.2.2"),
                status=200,
                elapsed=20000,
                cache="HIT",
                resp_bytes=rec.get("resp_bytes", 2048),
                url="/factory/baseline",
                ua="Mozilla/5.0 FactoryBaseline/1.0",
                pop="IAD",
                country="US",
                asn="64500",
            )
        )

    # 1. error_spikes: /api/err-spike
    _add(b_ts_base, 12, url="/api/err-spike", status=200)
    _add(w_ts_base, 5, url="/api/err-spike", status=502)

    # 2. botnet_grouping: ja4='ja4_botnet_1'
    rows.append(_row(b_ts_base, ja4="ja4_botnet_1", ip="10.1.0.1"))
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ja4="ja4_botnet_1", ip=f"10.1.0.{i + 1}"))

    # 4. new_country_traffic: country='IS'
    _add(w_ts_base, 4, country="IS", city="Reykjavik", region="1")

    # 5. city_surges: SurgeVille
    _add(b_ts_base, 5, city="SurgeVille", region="TX", country="US")
    _add(w_ts_base, 25, city="SurgeVille", region="TX", country="US")

    # 6. city_error_spikes: ErrTown
    _add(b_ts_base, 20, city="ErrTown", region="CA", country="US", status=200)
    _add(w_ts_base, 12, city="ErrTown", region="CA", country="US", status=500)

    # 7. city_latency_regressions: SlowBerg
    _add(b_ts_base, 55, city="SlowBerg", region="NY", country="US", url="/city/slowberg", elapsed=100000)
    _add(w_ts_base, 12, city="SlowBerg", region="NY", country="US", url="/city/slowberg", elapsed=1500000)

    # 8. new_city_traffic: FreshPolis
    _add(w_ts_base, 6, city="FreshPolis", region="WA", country="US")

    # 10. new_probe_urls: /wp-admin/setup.php
    _add(w_ts_base, 4, url="/wp-admin/setup.php", status=404)

    # 11. waf_signal_spikes: SQLI-PROBE
    rows.append(_row(b_ts_base, waf_sig="SQLI-PROBE"))
    _add(w_ts_base, 5, waf_sig="SQLI-PROBE,BOT-ANALYSIS")

    # 12. proxy_surge + 37b. asn_hosting_shift
    rows.append(_row(b_ts_base, asn="64500", p_type="hosting"))
    rows.append(_row(b_ts_base, asn="64505", p_type=""))
    _add(w_ts_base, 120, asn="64505", p_type="hosting")

    # 14. asn_metro_performance
    _add(b_ts_base, 55, asn="64502", metro="501", country="US", tcp_rtt=20000)
    _add(w_ts_base, 22, asn="64502", metro="501", country="US", tcp_rtt=120000)

    # 15. cache_collapse & 15b. cacheability_regression & 16. latency_regression
    _add(b_ts_base, 25, url="/static/bundle.js", cache="HIT")
    _add(w_ts_base, 10, url="/static/bundle.js", cache="MISS")
    _add(b_ts_base, 55, url="/account/overview", cache="HIT")
    _add(w_ts_base, 12, url="/account/overview", cache="PASS")
    _add(b_ts_base, 25, url="/api/slow-endpoint", elapsed=100000)
    _add(w_ts_base, 8, url="/api/slow-endpoint", elapsed=900000)

    # 17. impossible_distance
    _add(w_ts_base, 3, pop="JFK", lat=0.0, lon=0.0, tcp_rtt=1000, ja4="ja4_spoof_fp", ip="198.51.100.77", edge=True)

    # 18. tail_latency
    _add(w_ts_base, 22, url="/api/tail-heavy", elapsed=10000)
    _add(w_ts_base + timedelta(milliseconds=30), 2, url="/api/tail-heavy", elapsed=200000)

    # 19. cipher_spread
    rows.append(_row(b_ts_base, tls_ciphers_sha="cipher_sha_bot_1", ip="10.2.0.1"))
    for i in range(6):
        rows.append(
            _row(w_ts_base + timedelta(milliseconds=i), tls_ciphers_sha="cipher_sha_bot_1", ip=f"10.2.0.{i + 1}")
        )

    # 20. request_size_anomaly & 21. connection_abuse
    _add(b_ts_base, 10, ip="10.3.0.1", req_header_bytes=500)
    _add(w_ts_base, 4, ip="10.3.0.1", req_header_bytes=12000)
    _add(b_ts_base, 10, ip="10.4.0.1", conn_requests=5)
    _add(w_ts_base, 6, ip="10.4.0.1", conn_requests=120)

    # 22. region_latency + 25. origin_latency_spike + 45. timeout_split
    _add(b_ts_base, 105, server_region="US-East", url="/api/origin-timed", elapsed=100000, ottfb=20000, oconnect_ms=10)
    _add(w_ts_base, 55, server_region="US-East", url="/api/origin-timed", elapsed=600000, ottfb=450000, oconnect_ms=150)

    # 23. cache_ttl_mismatch & 24. image_optimization_opportunities
    _add(w_ts_base, 12, url="/Promo/Banner", ttl=3600, hits=0, age=10)
    _add(
        w_ts_base,
        10,
        url="/media/hero-banner.png",
        status=200,
        resp_bytes=102400,
        ua="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile",
    )

    # 26. origin_error_rate + 28. origin_ip_failure + 27. origin_retries
    _add(b_ts_base, 40, oip="192.0.2.10", ost=200)
    _add(w_ts_base, 12, oip="192.0.2.10", ost=200)
    _add(w_ts_base, 12, oip="192.0.2.11", ost=200)
    _add(w_ts_base, 12, oip="192.0.2.99", ost=503)
    _add(w_ts_base, 6, url="/api/flaky-upstream", oretries=3)

    # 29. shield_path_degradation
    for i in range(6):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), rid=f"rid_b_{i}", pop="LAX", edge=True, ottfb=60000))
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), prid=f"rid_b_{i}", pop="IAD", edge=False, ottfb=20000))
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), rid=f"rid_w_{i}", pop="LAX", edge=True, ottfb=260000))
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), prid=f"rid_w_{i}", pop="IAD", edge=False, ottfb=20000))

    # 30. repeated_patterns + 30b. repeated_patterns_fp
    for i in range(15):
        rows.append(
            _row(
                w_ts_base + timedelta(seconds=i * 10),
                ip="10.5.0.1",
                ja4="ja4_beacon_1",
                ua="Mozilla/5.0 CustomPoller/1.0",
            )
        )

    # 31. low_and_slow
    for i, probe_url in enumerate(["/.env", "/.env", "/.env", "/.git/config", "/.git/config", "/wp-login.php"]):
        rows.append(_row(w_ts_base + timedelta(seconds=i * 150), ip="10.6.0.1", url=probe_url, status=404))

    # 32. credential_enumeration & 33. network_asn_health
    _add(w_ts_base, 25, ip="10.7.0.1", url="/api/auth/login", status=401)
    _add(b_ts_base, 110, asn="64503", ploss=0.001, rtt_var=5000, retrans=0)
    _add(w_ts_base, 55, asn="64503", ploss=0.08, rtt_var=60000, retrans=2)

    # 34. content_discovery
    for i in range(22):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ip="10.8.0.1",
                url="/scan/missing-0" if i >= 18 else f"/scan/missing-{i}",
                status=404,
            )
        )

    # 37. new_asn_traffic, 38. metro_delivery_degradation, 39. connection_type_mix, 40. pop_latency_regression
    _add(w_ts_base, 22, asn="64504")
    _add(b_ts_base, 55, metro="803", delivery_rate=500000)
    _add(w_ts_base, 22, metro="803", delivery_rate=100000)
    _add(b_ts_base, 60, c_type="broadband", c_speed="fiber")
    _add(w_ts_base, 25, c_type="cellular", c_speed="5g")
    _add(b_ts_base, 55, pop="DEN", url="/pop/den", elapsed=50000)
    _add(w_ts_base, 22, pop="DEN", url="/pop/den", elapsed=500000)

    # 41. http3_fallback
    _add(b_ts_base, 60, transport="quic")
    _add(b_ts_base + timedelta(milliseconds=60), 60, transport="tcp")
    _add(w_ts_base, 12, transport="quic")
    _add(w_ts_base + timedelta(milliseconds=12), 108, transport="tcp")

    # 43. payload_compression_regression
    _add(b_ts_base, 55, url="/static/main.js", status=200, resp_bytes=4096, resp_header_content_encoding="br")
    _add(w_ts_base, 12, url="/static/main.js", status=200, resp_bytes=4096, resp_header_content_encoding="")

    # 44. session_harvesting & 46. crawler_disparity
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ip="10.9.0.1", cookie_session=f"sess_harvest_{i}"))
    _add(b_ts_base, 10, ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0)")
    _add(w_ts_base, 105, ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0)")

    # 47. stale_browser_version + referer_monoculture, asn_concentration, method_drift, cache_hit_cliff
    for i in range(505):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ip=f"10.12.0.{(i % 10) + 1}",
                url="/window/miss-only",
                cache="MISS",
                ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
                asn="64501",
                referer="https://hotlink-scraper.example/page",
                method="POST" if i < 220 else "GET",
            )
        )

    # 48. orphaned_deep_crawl & 49. residential_fingerprint_dispersion & 9. ua_monoculture
    for i in range(55):
        rows.append(
            _row(w_ts_base + timedelta(milliseconds=i), ip="10.10.0.1", url=f"/catalog/dept/item-{i % 15}", referer="")
        )
    for i in range(2):
        rows.append(
            _row(
                b_ts_base + timedelta(milliseconds=i),
                ja4="ja4_res_disp_1",
                ip=f"10.11.0.{i + 1}",
                asn="64600",
                p_type="",
            )
        )
    for i in range(32):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ja4="ja4_res_disp_1",
                ip=f"10.11.0.{i + 1}",
                asn=str(64600 + (i % 6)),
                p_type="",
            )
        )
    _add(w_ts_base, 520, ua="MonoScraper/9.9", asn="64501", referer="https://hotlink-scraper.example/page")
    return rows


def _insert_rows(con: duckdb.DuckDBPyConnection, table_name: str, rows: list[dict[str, Any]]) -> None:
    cols = _create_full_schema_table(con, table_name)
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(f'"{c}"' for c in cols)
    values = [[r.get(c) for c in cols] for r in rows]
    con.executemany(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})", values)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FIXED_NOW


def _extract_snapshot(res: dict[str, Any]) -> dict[str, Any]:
    return {
        ins["id"]: {
            "category": ins["category"],
            "title": ins["title"],
            "severity": ins["severity"],
            "summary": ins["summary"],
            "items": ins["items"],
        }
        for ins in res["insights"]
    }


def test_all_registered_insights_golden_characterization(in_memory_duckdb):
    """Every single registered insight in registry.get_all() must execute,
    produce at least 1 anomaly item on the deterministic golden dataset, and
    match the exact golden snapshot across admin, analyst-clamped/masked, and
    cache-collapse detail queries.
    """
    in_memory_duckdb.execute("SET TimeZone = 'UTC'")
    src = {"name": "golden-svc", "service_id": "svc-golden"}
    table_name = _safe_table(src["name"])
    rows = _build_comprehensive_anomaly_rows()
    _insert_rows(in_memory_duckdb, table_name, rows)

    expected = json.loads(GOLDEN_SNAPSHOT_PATH.read_text())

    with (
        patch("backend.repositories.insights.repository.datetime", _FrozenDatetime),
        patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_MOCK_POP_MAP),
        patch(
            "backend.core.duckdb.get_asn_names",
            side_effect=lambda _s, asns: {str(a): _MOCK_ASN_NAMES.get(str(a), f"AS{a}") for a in asns},
        ),
        patch("backend.core.duckdb._get_dma_map", return_value=_MOCK_DMA_MAP),
    ):
        res_admin = get_insights(
            in_memory_duckdb,
            src,
            window_hours=WINDOW_HOURS,
            baseline_hours=BASELINE_HOURS,
            service_id="svc-golden",
            force_refresh=True,
        )
        res_analyst = get_insights(
            in_memory_duckdb,
            src,
            window_hours=WINDOW_HOURS,
            baseline_hours=BASELINE_HOURS,
            service_id="svc-golden",
            force_refresh=True,
            mask_ips=True,
            clamp_start=(FIXED_NOW - timedelta(hours=25)).isoformat(),
            clamp_end=FIXED_NOW.isoformat(),
        )
        cc_detail = get_cache_collapse_detail(
            in_memory_duckdb,
            src,
            url="/static/bundle.js",
            window_hours=WINDOW_HOURS,
            baseline_hours=BASELINE_HOURS,
        )

    registered_ids = [d.id for d in registry.get_all()]
    assert len(registered_ids) >= 45

    actual_admin = _extract_snapshot(res_admin)
    missing = [iid for iid in registered_ids if iid not in actual_admin]
    assert not missing, f"Missing registered insights from response: {missing}"

    non_firing = [
        (iid, actual_admin[iid]["severity"], actual_admin[iid]["summary"])
        for iid in registered_ids
        if not actual_admin[iid].get("items")
    ]
    assert not non_firing, f"Registered insights did not fire any items: {non_firing}"

    for iid in registered_ids:
        assert actual_admin[iid] == expected["admin"][iid], f"Admin golden mismatch for {iid}"

    actual_analyst = _extract_snapshot(res_analyst)
    for iid in registered_ids:
        assert actual_analyst[iid] == expected["analyst_clamped_masked"][iid], (
            f"Analyst clamped/masked golden mismatch for {iid}"
        )

    cc_actual = {k: v for k, v in cc_detail.items() if k != "debug_queries"}
    cc_expected = {k: v for k, v in expected["cache_collapse_detail"].items() if k != "debug_queries"}
    assert cc_actual == cc_expected


# ── Advanced bot insights tests (merged from test_advanced_bot_insights.py) ──

import pytest  # noqa: E402

from backend.repositories._sql import insights as SQL  # noqa: E402
from backend.repositories.insights import _insights_cache  # noqa: E402
from backend.repositories.insights import definitions as defs  # noqa: E402


@pytest.mark.parametrize(
    ("ua", "referer", "referral_ua", "raw_tok", "display_name"),
    [
        (
            "Claude-SearchBot GPTBot",
            "https://CLAUDE.AI/chatgpt.com",
            "Browser",
            "claude-searchbot",
            "Anthropic Crawler Search",
        ),
        ("AMZN-SEARCHBOT", "https://AMAZON.CO.uk", "Browser", "amzn-searchbot", "Amazon Searchbot"),
        ("gPtBoT ChatGPT-User", "https://CHATGPT.COM", "Browser", "gptbot", "Openai Crawler"),
        ("PerplexityBot/1.0", "https://perplexity.ai/search", "Browser", "perplexitybot", "Perplexity Crawler"),
        (
            "Mozilla/5.0 (compatible; Googlebot/2.1)",
            "https://www.google.com/search",
            "Browser",
            "googlebot",
            "Google Crawler",
        ),
        (
            "Mozilla/5.0 (compatible; Bytespider; +https://bytedance.com)",
            "https://www.bytedance.com/",
            "Browser",
            "bytespider",
            "Bytedance Crawler",
        ),
        ("SemrushBOT", "https://SEMRUSH.COM", "Browser", "semrushbot", "Semrush Crawler"),
        ("AHREFSBOT", "https://AHREFS.COM", "Browser", "ahrefsbot", "Ahrefs Crawler"),
        ("Claude-SearchBot", None, "CLAUDE-USER", "claude-searchbot", "Anthropic Crawler Search"),
        (
            "\u0130 Claude-SearchBot",
            "https://claude.ai/\u00df",
            "Browser",
            "claude-searchbot",
            "Anthropic Crawler Search",
        ),
    ],
)
def test_crawler_matching_preserves_referrals(
    in_memory_duckdb: duckdb.DuckDBPyConnection,
    ua: str,
    referer: str | None,
    referral_ua: str,
    raw_tok: str,
    display_name: str,
) -> None:
    from backend.utils.bot_sources import resolve_bot_display_name

    con = in_memory_duckdb
    boundary = datetime(2026, 1, 2, tzinfo=UTC)
    before = boundary - timedelta(seconds=1)
    con.execute(
        "CREATE TABLE ua_logs (timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR, referer VARCHAR, resp_bytes BIGINT)"
    )
    con.execute("INSERT INTO ua_logs SELECT ?, '192.0.2.1', ?, NULL, 2048 FROM range(1000)", [boundary, ua])
    con.execute("INSERT INTO ua_logs SELECT ?, '192.0.2.1', ?, NULL, 1024 FROM range(20)", [before, ua])
    con.executemany(
        "INSERT INTO ua_logs VALUES (?, ?, ?, ?, 512)",
        [
            (boundary, "192.0.2.2", referral_ua, referer),
            (boundary, "192.0.2.2", referral_ua, referer),
            (before, "192.0.2.3", referral_ua, referer),
            (before, "192.0.2.4", referral_ua, referer),
            (boundary, None, referral_ua, referer),
            (boundary, "192.0.2.5", None, referer),
            (boundary, "192.0.2.6", "Crawler", referer),
            (boundary, "192.0.2.7", "Spider", referer),
            (boundary, "192.0.2.8", "Browser BOT", referer),
        ],
    )
    rows = con.execute(SQL.CRAWLER_DISPARITY.format(table_name="ua_logs"), [boundary] * 4).fetchall()
    assert rows == [(raw_tok, 1000, 1, 2048000, 1000.0, 10.0, 2048000.0, 10240.0, 99.83)]
    assert resolve_bot_display_name(rows[0][0]) == display_name


def test_crawler_disparity_includes_non_referring_bots_and_bandwidth_share(
    in_memory_duckdb: duckdb.DuckDBPyConnection,
) -> None:
    from backend.utils.bot_sources import resolve_bot_display_name

    con = in_memory_duckdb
    boundary = datetime(2026, 1, 2, tzinfo=UTC)
    con.execute(
        "CREATE TABLE ua_logs (timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR, referer VARCHAR, resp_bytes BIGINT)"
    )
    con.execute(
        "INSERT INTO ua_logs SELECT ?, '198.51.100.10', 'CCBot/2.0 (https://commoncrawl.org/faq/)', '-', 50000 FROM range(100)",
        [boundary],
    )
    con.execute(
        "INSERT INTO ua_logs SELECT ?, '203.0.113.10', 'Mozilla/5.0 Safari/537.36', 'https://example.com', 10000 FROM range(500)",
        [boundary],
    )
    rows = con.execute(SQL.CRAWLER_DISPARITY.format(table_name="ua_logs"), [boundary] * 4).fetchall()
    assert len(rows) == 1
    (
        bot_tok,
        w_crawls,
        w_referrals,
        w_bytes,
        crawls_per_ref,
        _base_crawls_per_ref,
        bytes_per_ref,
        _base_bytes_per_ref,
        bw_share_pct,
    ) = rows[0]
    assert bot_tok == "ccbot"
    assert resolve_bot_display_name(bot_tok) == "Commoncrawl Crawler"
    assert w_crawls == 100
    assert w_bytes == 5_000_000
    assert round(bw_share_pct, 1) == 50.0
    assert w_referrals == 0
    assert crawls_per_ref == 100.0
    assert bytes_per_ref == 5_000_000.0


def test_stale_browser_matching_preserves_exclusions_and_window(
    in_memory_duckdb: duckdb.DuckDBPyConnection,
) -> None:
    con = in_memory_duckdb
    boundary = datetime(2026, 1, 2, tzinfo=UTC)
    before = boundary - timedelta(seconds=1)
    con.execute("CREATE TABLE ua_logs (timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR)")
    con.execute(
        "INSERT INTO ua_logs SELECT ?, CASE WHEN i % 15 = 0 THEN NULL ELSE '192.0.2.' || (i % 15) END, "
        "'Mozilla \u0130 Chrome/100.0' FROM range(600) t(i)",
        [boundary],
    )
    con.execute("INSERT INTO ua_logs SELECT ?, '192.0.2.1', 'Chrome/100.0' FROM range(24)", [before])
    for ua in [
        "Chrome/100.0 BoT",
        "Chrome/100.0 cRaWlEr",
        "Chrome/100.0 SPIDER",
        "chrome/100.0",
        "Chrome/131.0",
        "Chrome/invalid",
        None,
    ]:
        con.execute("INSERT INTO ua_logs SELECT ?, '192.0.2.2', ? FROM range(600)", [boundary, ua])
    con.execute("INSERT INTO ua_logs SELECT ?, '192.0.2.3', 'Chrome/110.0' FROM range(499)", [boundary])
    query = SQL.STALE_BROWSER_VERSION.format(table_name="ua_logs", baseline_hours=24, window_hours=1)
    assert con.execute(query, [boundary] * 2).fetchall() == [("Chrome 100", 600, 14, 600.0)]


def test_advanced_bot_insights_trigger(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])

    in_memory_duckdb.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "timestamp" TIMESTAMPTZ,
            "ip" VARCHAR,
            "ua" VARCHAR,
            "referer" VARCHAR,
            "url" VARCHAR,
            "ja4" VARCHAR,
            "asn" INTEGER,
            "p_type" VARCHAR,
            "resp_bytes" BIGINT
        )
    """)

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=10)
    baseline_ts = now - timedelta(hours=3)
    history_ts = now - timedelta(hours=24, minutes=30)

    def _insert_log(ts, ip, ua, referer, url, ja4=None, asn=None, p_type=None, resp_bytes=1024):
        in_memory_duckdb.execute(
            f"""INSERT INTO {table_name}
                (timestamp, ip, ua, referer, url, ja4, asn, p_type, resp_bytes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [ts.isoformat(), ip, ua, referer, url, ja4 or "", asn or 0, p_type or "", resp_bytes],
        )

    _insert_log(
        ts=history_ts,
        ip="1.1.1.1",
        ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36",
        referer="https://google.com",
        url="/index.html",
    )

    for i in range(10):
        _insert_log(
            ts=baseline_ts,
            ip=f"192.0.2.{i % 10}",
            ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +http://www.anthropic.com/claudebot)",
            referer="-",
            url=f"/item/{i}",
            resp_bytes=20000,
        )
    for i in range(150):
        _insert_log(
            ts=window_ts,
            ip=f"192.0.2.{i % 10}",
            ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +http://www.anthropic.com/claudebot)",
            referer="-",
            url=f"/item/{i}",
            resp_bytes=20000,
        )

    for i in range(60):
        _insert_log(
            ts=window_ts,
            ip=f"198.18.0.{i % 5}",
            ua="CCBot/2.0 (https://commoncrawl.org/faq/)",
            referer="-",
            url=f"/archive/{i}",
            resp_bytes=50000,
        )

    for i in range(10):
        _insert_log(
            ts=baseline_ts,
            ip=f"198.51.100.{i % 15}",
            ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
            referer="https://google.com",
            url="/index.html",
        )
    for i in range(600):
        _insert_log(
            ts=window_ts,
            ip=f"198.51.100.{i % 15}",
            ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
            referer="https://google.com",
            url="/index.html",
        )

    for i in range(10):
        _insert_log(
            ts=baseline_ts,
            ip="203.0.113.50",
            ua="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            referer="-",
            url=f"/catalog/category/item-{i % 12}",
        )
    for i in range(60):
        _insert_log(
            ts=window_ts,
            ip="203.0.113.50",
            ua="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            referer="-",
            url=f"/catalog/category/item-{i % 12}",
        )

    for i in range(5):
        _insert_log(
            ts=baseline_ts,
            ip=f"203.0.113.{i}",
            ua="Mozilla/5.0 (Windows NT 10.0; x64)",
            referer="https://google.com",
            url="/search",
            ja4="t13d1516h2_8e6120e8544e_3a4b5c6d7e8f",
            asn=1000 + (i % 8),
            p_type="residential",
        )
    for i in range(40):
        _insert_log(
            ts=window_ts,
            ip=f"203.0.113.{i}",
            ua="Mozilla/5.0 (Windows NT 10.0; x64)",
            referer="https://google.com",
            url="/search",
            ja4="t13d1516h2_8e6120e8544e_3a4b5c6d7e8f",
            asn=1000 + (i % 8),
            p_type="residential",
        )

    _insights_cache.clear()

    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    assert "bot_bandwidth_disparity" not in by_id
    assert "crawler_disparity" in by_id
    cd = by_id["crawler_disparity"]
    assert cd["title"] == "Bot & Crawler Referral & Bandwidth Disparity"
    assert cd["category"] == "security"
    assert len(cd["items"]) >= 2
    cd_labels = {item["label"]: item for item in cd["items"]}
    assert "Anthropic Crawler Search" in cd_labels
    assert cd_labels["Anthropic Crawler Search"]["meta"]["crawls"] == 150
    assert cd_labels["Anthropic Crawler Search"]["meta"]["w_bytes"] == 3_000_000
    assert cd_labels["Anthropic Crawler Search"]["meta"]["bytes_per_referral"] == 3_000_000.0
    assert cd_labels["Anthropic Crawler Search"]["meta"]["bandwidth_share_pct"] > 0
    assert "Commoncrawl Crawler" in cd_labels
    assert cd_labels["Commoncrawl Crawler"]["meta"]["crawls"] == 60
    assert cd_labels["Commoncrawl Crawler"]["meta"]["w_bytes"] == 3_000_000
    assert cd_labels["Commoncrawl Crawler"]["meta"]["referrals"] == 0

    assert "stale_browser_version" in by_id
    sb = by_id["stale_browser_version"]
    assert sb["category"] == "security"
    assert len(sb["items"]) > 0
    assert sb["items"][0]["label"] == "Chrome 100"

    assert "orphaned_deep_crawl" in by_id
    odc = by_id["orphaned_deep_crawl"]
    assert odc["category"] == "security"
    assert len(odc["items"]) > 0
    assert odc["items"][0]["label"] == "203.0.113.50"
    assert odc["items"][0]["meta"]["requests"] == 60

    assert "residential_fingerprint_dispersion" in by_id
    rfd = by_id["residential_fingerprint_dispersion"]
    assert rfd["category"] == "security"
    assert len(rfd["items"]) > 0
    assert rfd["items"][0]["label"] == "t13d1516h2_8e6120e8544e_3a4b5c6d7e8f"
    assert rfd["items"][0]["meta"]["asns"] == 8


def test_resolve_bot_display_name_and_efficiency_without_runtime_cache(tmp_path, monkeypatch) -> None:
    """When `data/cache/bot_sources/well-known-bots.json` is absent (fresh checkout or CI),
    `resolve_bot_display_name` and `summarize_bot_efficiency_rows` fall back to the
    bundled `well_known_bots_seed.json` catalog."""
    from backend.repositories.security import summarize_bot_efficiency_rows
    from backend.utils import bot_sources
    from backend.utils.bot_sources import load_source, resolve_bot_display_name

    monkeypatch.setattr(bot_sources, "_CACHE_DIR", tmp_path)
    with bot_sources._matcher_lock:
        bot_sources._matcher_cache.clear()
    with bot_sources._source_cache_lock:
        bot_sources._source_cache.clear()

    assert load_source("well-known-bots") == []
    assert resolve_bot_display_name("claude-searchbot") == "Anthropic Crawler Search"
    assert resolve_bot_display_name("googlebot") == "Google Crawler"
    assert resolve_bot_display_name("ccbot") == "Commoncrawl Crawler"

    rows = [
        ("site", "", 100, 500_000),
        ("ua", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", 10, 100_000),
        ("ref", "google", 2, 0),
        ("ua", "CCBot/2.0 (https://commoncrawl.org/faq/)", 5, 100_000),
    ]
    by_name = {item["bot_name"]: item for item in summarize_bot_efficiency_rows(rows)}
    assert by_name["Google Crawler"]["referrals"] == 2
    assert by_name["Commoncrawl Crawler"]["referrals"] == 0


# ── Repeated patterns insight tests (merged from test_insights_repeated_patterns.py) ──

_PERFECT_BEACON_ROW = ("1.1.1.1", 34, 35, 60.0, 0.0, 0.0, 1.0, 1, 2040, 60)


def _create_repeated_patterns_table(con, table: str) -> None:
    con.execute(f'CREATE TABLE IF NOT EXISTS {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "ua" VARCHAR)')


def _seed_repeated_patterns(con, table: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ua: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        con.execute(f'INSERT INTO {table} ("timestamp", "ip", "ua") VALUES (?, ?, ?)', [ts, ip, ua])

    ins(70 * 60, "9.9.9.9", "anchor")

    for i in range(35):
        ins(60 + i * 60, "1.1.1.1", "Mozilla/5.0 SteadyClient")

    for i in range(30):
        ins(60 + i * 30, "1.1.1.3", f"Mozilla/5.0 Browser-{i}")

    bursty_gaps = [3, 47, 5, 2, 88, 14, 6, 120, 9, 4, 60, 33, 7, 200, 11, 5, 75, 19, 2]
    cum = 60
    for _ in range(3):
        ins(cum, "2.2.2.2", "Mozilla/5.0 Human")
    for gp in bursty_gaps:
        cum += gp
        ins(cum, "2.2.2.2", "Mozilla/5.0 Human")

    for i in range(8):
        ins(60 + i * 60, "3.3.3.3", "Mozilla/5.0 Small")

    for i in range(30):
        ins(60 + i * 30, "4.4.4.4", "Googlebot/2.1 (+http://www.google.com/bot.html)")


def _anchor(con, table: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    ts = (now - timedelta(minutes=70)).isoformat()
    con.execute(f'INSERT INTO {table} ("timestamp", "ip", "ua") VALUES (?, ?, ?)', [ts, "9.9.9.9", "anchor"])


def _seed_gaps(
    con, table: str, ip: str, ua: str, first_offset_s: int, gaps: list[int], *, dups_per_second: int = 1
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    offsets = [first_offset_s]
    for g in gaps:
        offsets.append(offsets[-1] + g)
    for off in offsets:
        ts = (now - timedelta(seconds=off)).isoformat()
        for _ in range(dups_per_second):
            con.execute(f'INSERT INTO {table} ("timestamp", "ip", "ua") VALUES (?, ?, ?)', [ts, ip, ua])


def _repeated_card(con, src, *, mask_ips: bool = False) -> dict:
    _insights_cache.clear()
    res = get_insights(con, src, window_hours=1, baseline_hours=1, mask_ips=mask_ips)
    card = next((i for i in res["insights"] if i["id"] == "repeated_patterns"), None)
    assert card is not None, "repeated_patterns card missing from insights payload"
    assert card.get("severity") != "error", f"repeated_patterns errored: {card.get('summary')}"
    return card


def test_detection_matrix(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    items = {item["label"]: item for item in card["items"]}

    assert "1.1.1.1" in items, "steady beacon should be detected"
    assert "1.1.1.3" in items, "UA-rotating beacon should be detected (no UA gate)"

    assert "2.2.2.2" not in items, "bursty human traffic must not be flagged (high CV)"
    assert "3.3.3.3" not in items, "small sample (n_gaps < 12) must not be flagged"
    assert "4.4.4.4" not in items, "Googlebot UA must be suppressed by the allowlist"
    assert "9.9.9.9" not in items, "single-event anchor must not be flagged"


def test_steady_beacon_is_critical_and_regular(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next(i for i in card["items"] if i["label"] == "1.1.1.1")

    assert item["severity"] == "critical"
    assert item["current_val"] == 60.0
    assert item["meta"]["n_events"] == 35
    assert item["meta"]["cv"] < 0.05
    assert item["meta"]["modal_frac"] >= 0.95
    assert item["meta"]["score"] == 100
    assert card["severity"] == "critical"


def test_ua_rotating_beacon_detected_with_high_distinct_ua(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next(i for i in card["items"] if i["label"] == "1.1.1.3")

    assert item["meta"]["distinct_ua"] == 30
    assert item["meta"]["n_events"] == 30
    assert item["severity"] == "critical"


def test_jittered_beacon_detected_and_sheppard_correction_applied(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    gaps = [6, 6, 5, 6, 7, 6, 6, 5, 6, 7, 6, 6, 6, 6, 6]
    _seed_gaps(in_memory_duckdb, table, "1.1.1.2", "Mozilla/5.0 Jitter", 60, gaps)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next((i for i in card["items"] if i["label"] == "1.1.1.2"), None)
    assert item is not None, "jittered slow-path beacon should be detected"
    m = item["meta"]
    naive_cv = m["stddev_s"] / m["mean_interval_s"]
    assert m["cv"] < naive_cv, f"Sheppard correction not applied: cv={m['cv']} naive={naive_cv}"
    assert 0 < m["cv"] < 0.3


def test_fast_path_subsecond_cadence_detected(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, "1.1.1.5", "Mozilla/5.0 Fast", 60, [2] * 14)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next((i for i in card["items"] if i["label"] == "1.1.1.5"), None)
    assert item is not None, "fast-path (2 s cadence) poller should be detected"
    assert 1 <= item["meta"]["mean_interval_s"] < 5
    assert item["meta"]["modal_frac"] >= 0.85


def test_fast_path_below_modal_threshold_ignored(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, "1.1.1.6", "Mozilla/5.0 IrregularFast", 60, [2, 4] * 7)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    assert "1.1.1.6" not in {i["label"] for i in card["items"]}, "fast-path modal < 0.85 must not be flagged"


def test_rps_flood_ignored_despite_perfect_cadence(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, "1.1.1.7", "Mozilla/5.0 Flood", 60, [1] * 13, dups_per_second=3)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    assert "1.1.1.7" not in {i["label"] for i in card["items"]}, "high-rps flood must be excluded by the rps gate"


def test_mask_ips_masks_label_and_filter(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    unmasked = _repeated_card(in_memory_duckdb, test_service_source, mask_ips=False)
    raw_labels = {i["label"] for i in unmasked["items"]}
    assert "1.1.1.1" in raw_labels

    masked = _repeated_card(in_memory_duckdb, test_service_source, mask_ips=True)
    masked_item = next(i for i in masked["items"] if i["label"] == "1.1.1.xxx")
    assert masked_item["meta"]["filters"]["ip"] == "1.1.1.xxx"
    assert "1.1.1.1" not in {i["label"] for i in masked["items"]}


def test_processor_critical_requires_dense_and_regular():
    p = defs.repeated_patterns_processor
    crit = p(("1.1.1.1", 29, 30, 30.0, 0.0, 0.0, 1.0, 30, 870, 30), None, {})
    assert crit["severity"] == "critical"
    warn = p(("1.1.1.1", 19, 20, 60.0, 0.0, 0.0, 1.0, 1, 1140, 60), None, {})
    assert warn["severity"] == "warning"


def test_processor_low_score_is_info():
    p = defs.repeated_patterns_processor
    out = p(("5.5.5.5", 40, 50, 3.0, 2.0, 1.0, 0.5, 5, 150, 3), None, {})
    assert out["meta"]["score"] < 70
    assert out["severity"] == "info"


def test_processor_score_boundary_at_90():
    p = defs.repeated_patterns_processor
    above = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.1, 1.0, 1, 2400, 60), None, {})
    below = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.2, 1.0, 1, 2400, 60), None, {})
    assert above["meta"]["score"] == 94 and above["severity"] == "critical"
    assert below["meta"]["score"] == 88 and below["severity"] == "warning"


def test_processor_masks_ip_when_mask_ips():
    p = defs.repeated_patterns_processor
    masked = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx"
    assert masked["meta"]["filters"]["ip"] == "203.0.113.xxx"


def test_processor_keeps_ip_for_admin():
    p = defs.repeated_patterns_processor
    raw = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {})
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


def test_processor_computes_rps_and_handles_none_mode():
    p = defs.repeated_patterns_processor
    out = p(("1.1.1.1", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, None), None, {})
    assert out["meta"]["rps"] == round(35 / 2040, 4)
    assert out["meta"]["mode_gap_s"] is None


def test_severity_logic_clean_when_no_items():
    assert defs.repeated_patterns_severity([]) == "clean"
    assert defs.repeated_patterns_severity([{"severity": "warning"}]) == "warning"
    assert defs.repeated_patterns_severity([{"severity": "info"}, {"severity": "critical"}]) == "critical"


def test_bot_ua_regex_contains_no_question_mark():
    assert "?" not in SQL.REPEATED_BOT_UA_REGEX


def test_template_has_exactly_one_bind_after_hydration():
    hydrated = SQL.REPEATED_PATTERNS.format(
        table_name="t",
        ua_col='"ua"',
        bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX,
    )
    assert hydrated.count("?") == 1


def test_mode_is_aliased_not_bare():
    assert "MODE(gap) AS mode_gap" in SQL.REPEATED_PATTERNS


def test_repeated_patterns_registered():
    d = registry.get("repeated_patterns")
    assert d is not None
    assert d.title == "Scripted Traffic Patterns"
    assert d.required_fields == ["ip", "timestamp"]
    assert d.row_processor is not None
    assert d.severity_logic is not None


def test_repeated_patterns_in_availability_metadata():
    from backend.core.field_registry import INSIGHT_DEFINITIONS

    entry = next((d for d in INSIGHT_DEFINITIONS if d["id"] == "repeated_patterns"), None)
    assert entry is not None, "repeated_patterns missing from INSIGHT_DEFINITIONS"
    assert entry["required_fields"] == ["ip"]
    assert entry["required_groups"] == []


def _create_fp_table(con, table: str) -> None:
    con.execute(
        f'CREATE TABLE IF NOT EXISTS {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "ua" VARCHAR, "ja4" VARCHAR)'
    )


def _fp_card(con, src, *, mask_ips: bool = False) -> dict:
    _insights_cache.clear()
    res = get_insights(con, src, window_hours=1, baseline_hours=1, mask_ips=mask_ips)
    card = next((i for i in res["insights"] if i["id"] == "repeated_patterns_fp"), None)
    assert card is not None, "repeated_patterns_fp card missing from insights payload"
    assert card.get("severity") != "error", f"repeated_patterns_fp errored: {card.get('summary')}"
    return card


def _seed_fp_beacon(con, table: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ja4: str, ua: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        con.execute(
            f'INSERT INTO {table} ("timestamp", "ip", "ua", "ja4") VALUES (?, ?, ?, ?)',
            [ts, ip, ua, ja4],
        )

    ins(70 * 60, "9.9.9.9", "anchor_fp", "anchor")

    for i in range(35):
        ins(60 + i * 60, f"10.0.0.{i + 1}", "aaaa", "Mozilla/5.0 Scraper")


def _seed_fp_bursty(con, table: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ja4: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        con.execute(
            f'INSERT INTO {table} ("timestamp", "ip", "ua", "ja4") VALUES (?, ?, ?, ?)',
            [ts, ip, "Mozilla/5.0 Human", ja4],
        )

    ins(70 * 60, "9.9.9.9", "anchor_fp")

    bursty_gaps = [3, 47, 5, 2, 88, 14, 6, 120, 9, 4, 60, 33, 7, 200, 11, 5, 75, 19, 2]
    cum = 60
    for gp in bursty_gaps:
        cum += gp
        ins(cum, f"10.1.0.{gp}", "bbbb")


def _seed_fp_small(con, table: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ja4: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        con.execute(
            f'INSERT INTO {table} ("timestamp", "ip", "ua", "ja4") VALUES (?, ?, ?, ?)',
            [ts, ip, "Mozilla/5.0 Small", ja4],
        )

    ins(70 * 60, "9.9.9.9", "anchor_fp")
    for i in range(8):
        ins(60 + i * 60, f"10.2.0.{i}", "cccc")


def _seed_fp_bot(con, table: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ja4: str, ua: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        con.execute(
            f'INSERT INTO {table} ("timestamp", "ip", "ua", "ja4") VALUES (?, ?, ?, ?)',
            [ts, ip, ua, ja4],
        )

    ins(70 * 60, "9.9.9.9", "anchor_fp", "anchor")
    for i in range(30):
        ins(60 + i * 30, f"10.3.0.{i}", "dddd", "Googlebot/2.1 (+http://www.google.com/bot.html)")


def test_fp_detection(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_fp_table(in_memory_duckdb, table)
    _seed_fp_beacon(in_memory_duckdb, table)

    card = _fp_card(in_memory_duckdb, test_service_source)
    items = {item["label"]: item for item in card["items"]}
    assert "aaaa" in items, "steady beacon fingerprint should be detected"
    item = items["aaaa"]
    assert item["severity"] == "critical"
    assert item["meta"]["distinct_ip"] >= 30
    assert item["meta"]["score"] == 100


def test_fp_bursty_ignored(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_fp_table(in_memory_duckdb, table)
    _seed_fp_bursty(in_memory_duckdb, table)

    card = _fp_card(in_memory_duckdb, test_service_source)
    assert "bbbb" not in {i["label"] for i in card["items"]}, "bursty fingerprint must not be flagged"


def test_fp_small_sample_ignored(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_fp_table(in_memory_duckdb, table)
    _seed_fp_small(in_memory_duckdb, table)

    card = _fp_card(in_memory_duckdb, test_service_source)
    assert "cccc" not in {i["label"] for i in card["items"]}, "small-sample fingerprint must not be flagged"


def test_fp_bot_ua_suppressed(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_fp_table(in_memory_duckdb, table)
    _seed_fp_bot(in_memory_duckdb, table)

    card = _fp_card(in_memory_duckdb, test_service_source)
    assert "dddd" not in {i["label"] for i in card["items"]}, "Googlebot fingerprint must be suppressed"


def test_fp_diversity_field_in_meta(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_fp_table(in_memory_duckdb, table)
    _seed_fp_beacon(in_memory_duckdb, table)

    card = _fp_card(in_memory_duckdb, test_service_source)
    item = next(i for i in card["items"] if i["label"] == "aaaa")
    assert "distinct_ip" in item["meta"]
    assert item["meta"]["distinct_ip"] >= 30


def test_fp_filter_key_uses_fp_col(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_fp_table(in_memory_duckdb, table)
    _seed_fp_beacon(in_memory_duckdb, table)

    card = _fp_card(in_memory_duckdb, test_service_source)
    item = next(i for i in card["items"] if i["label"] == "aaaa")
    assert "ja4" in item["meta"]["filters"], f"expected ja4 filter key, got {item['meta']['filters']}"
    assert item["meta"]["filters"]["ja4"] == "aaaa"


def test_fp_template_has_exactly_one_bind_after_hydration():
    hydrated = SQL.REPEATED_PATTERNS_FP.format(
        table_name="t",
        fp_col="ja4",
        ua_col='"ua"',
        bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX,
    )
    assert hydrated.count("?") == 1


def test_fp_bot_ua_regex_contains_no_question_mark():
    assert "?" not in SQL.REPEATED_BOT_UA_REGEX


def test_repeated_patterns_fp_registered():
    d = registry.get("repeated_patterns_fp")
    assert d is not None
    assert d.title == "Scripted Traffic Patterns (by TLS Fingerprint)"
    assert d.required_fields == ["ip", "timestamp"]
    assert d.row_processor is not None
    assert d.severity_logic is not None


def test_repeated_patterns_fp_in_availability_metadata():
    from backend.core.field_registry import INSIGHT_DEFINITIONS

    entry = next((d for d in INSIGHT_DEFINITIONS if d["id"] == "repeated_patterns_fp"), None)
    assert entry is not None, "repeated_patterns_fp missing from INSIGHT_DEFINITIONS"
    assert entry["required_fields"] == ["ip"]
    assert entry["required_groups"] == []


def test_fp_processor_score_and_severity():
    p = defs.repeated_patterns_fp_processor
    crit = p(("aaaa", 29, 30, 60.0, 0.0, 0.0, 1.0, 10, 1740, 60), None, {})
    assert crit["severity"] == "critical"
    assert crit["meta"]["score"] == 100
    assert crit["meta"]["distinct_ip"] == 10
    warn = p(("aaaa", 19, 20, 60.0, 0.0, 0.0, 1.0, 5, 1140, 60), None, {})
    assert warn["severity"] == "warning"


def test_fp_processor_uses_context_fp_col():
    p = defs.repeated_patterns_fp_processor
    out = p(("xyz", 29, 30, 60.0, 0.0, 0.0, 1.0, 10, 1740, 60), None, {"fp_col": "ja3"})
    assert "ja3" in out["meta"]["filters"]
    assert out["meta"]["filters"]["ja3"] == "xyz"


# ── Merged from tests/repositories/test_insights.py ──

from unittest.mock import MagicMock

import pytest
from backend.core.share_db.validation import mask_ip

from backend.core.field_registry import INSIGHT_DEFINITIONS
from backend.models.insights import InsightsRequest, InsightsResponse
from backend.repositories.insights import (
    InsightCategory,
    InsightDefinition,
)
from backend.repositories.insights import (
    repository as insights_repo,
)
from backend.repositories.insights.repository import _coalesced_url_aggregates
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs


def _create_impossible_distance_table(con, table_name: str, n: int = 3):
    now = datetime.now(UTC).isoformat()
    con.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "pop" VARCHAR, '
        '"lat" FLOAT, "lon" FLOAT, "tcp_rtt" INTEGER, "ja3" VARCHAR, "status" INTEGER, "country" VARCHAR, "city" VARCHAR)'
    )
    for _ in range(n):
        con.execute(
            f"INSERT INTO {table_name} (timestamp, ip, pop, lat, lon, tcp_rtt, ja3, status, country, city) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [now, "1.2.3.4", "JFK", 0.0, 0.0, 1000, "aabbcc112233", 200, "US", "SomeCity"],
        )


def _seed_network_edge_data(con, table_name: str) -> None:
    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    baseline_ts = now - timedelta(hours=12)
    old_filler = now - timedelta(hours=24, minutes=30)

    def _ins(ts, metro, drate, c_type, c_speed, pop, elapsed, transport, cache):
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "metro", "delivery_rate", "c_type", '
            '"c_speed", "pop", "elapsed", "transport", "cache") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [ts.isoformat(), metro, drate, c_type, c_speed, pop, elapsed, transport, cache],
        )

    _ins(old_filler, 501, 10_000_000, "residential", "broadband", "JFK", 50_000, "quic", "HIT")
    for i in range(300):
        _ins(
            baseline_ts,
            501,
            10_000_000,
            "residential",
            "broadband",
            "JFK",
            50_000,
            "quic" if i % 5 < 4 else "tcp",
            "HIT" if i % 10 < 7 else "MISS",
        )
    for _ in range(150):
        _ins(window_ts, 501, 1_000_000, "cellular", "mobile", "JFK", 3_000_000, "tcp", "MISS")


def _seed_track_c_data(con, table_name: str) -> None:
    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    baseline_ts = now - timedelta(hours=12)
    old_filler = now - timedelta(hours=24, minutes=30)

    def _ins(ts, url, status, rb, ce, ip, cs, oc, ottfb):
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "url", "status", "resp_bytes", '
            '"resp_header_content_encoding", "ip", "cookie_session", "oconnect_ms", "ottfb") '
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [ts.isoformat(), url, status, rb, ce, ip, cs, oc, ottfb],
        )

    _ins(old_filler, "/app.js", 200, 50_000, "gzip", "10.0.0.9", "s_old", 5, 50_000)
    for _ in range(60):
        _ins(baseline_ts, "/app.js", 200, 50_000, "gzip", "10.0.0.9", "s_b", 5, 50_000)
    for _ in range(20):
        _ins(window_ts, "/app.js", 200, 50_000, "", "10.0.0.9", "s_w", 5, 50_000)
    for i in range(30):
        _ins(window_ts, "/login", 200, 500, "gzip", "10.0.0.2", f"sess_{i}", 5, 50_000)
    for _ in range(120):
        _ins(baseline_ts, "/api", 200, 500, "gzip", "10.0.0.3", "s", 5, 60_000)
    for _ in range(60):
        _ins(window_ts, "/api", 200, 500, "gzip", "10.0.0.3", "s", 500, 560_000)


def _create_full_catalog_table(con, table_name: str) -> None:
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")


def _norm_items(items: list[dict]) -> list[tuple]:
    return [
        (
            i["label"],
            round(float(i.get("current_val") or 0), 4),
            round(float(i.get("baseline_val") or 0), 4),
        )
        for i in items
    ]


def test_get_insights_basic_and_cache(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    _create_full_catalog_table(in_memory_duckdb, table_name)

    logs = generate_mock_logs(test_service_source, num_logs=50, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    assert "insights" in res and "window_start" in res
    assert isinstance(res["insights"], list)

    res2 = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    assert res2["is_cached"] is True and res2["insights"] == res["insights"]

    resp_model = InsightsResponse(**res)
    assert isinstance(resp_model.insights, list)


def test_insights_request_model_validation():
    req = InsightsRequest(window_size_hrs=2.0, baseline_hours=48.0)
    assert req.window_size_hrs == 2.0 and req.baseline_hours == 48.0


def test_get_insights_custom_definition_and_error_handling(in_memory_duckdb, test_service_source, monkeypatch):
    table_name = _safe_table(test_service_source["name"])
    _create_full_catalog_table(in_memory_duckdb, table_name)

    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    def _proc(row, definition, context):
        return {"label": str(row[0]), "current_val": float(row[1]), "severity": "warning"}

    good_def = InsightDefinition(
        id="test_custom_ok",
        title="Custom OK",
        description="desc",
        category=InsightCategory.traffic,
        required_fields=["url"],
        sql_template="SELECT '/custom', 42 FROM {table_name} LIMIT 1",
        row_processor=_proc,
        check_baseline=False,
    )
    bad_def = InsightDefinition(
        id="test_custom_bad",
        title="Custom Bad",
        description="desc",
        category=InsightCategory.traffic,
        required_fields=["url"],
        sql_template="SELECT INVALID_COLUMN FROM {table_name}",
        row_processor=_proc,
        check_baseline=False,
    )
    monkeypatch.setattr(registry, "get_all", lambda: [good_def, bad_def])
    _insights_cache.clear()

    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}
    assert by_id["test_custom_ok"]["items"][0]["label"] == "/custom"
    assert by_id["test_custom_bad"]["items"] == []


def test_get_insights_ip_masking_and_baseline_clamp(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    _create_full_catalog_table(in_memory_duckdb, table_name)

    now = datetime.now(UTC)
    in_memory_duckdb.execute(
        f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status") VALUES (?, ?, ?, ?)',
        [(now - timedelta(minutes=10)).isoformat(), "203.0.113.99", "/login", 401],
    )
    _insights_cache.clear()
    res = get_insights(
        in_memory_duckdb,
        test_service_source,
        window_hours=1,
        baseline_hours=24,
        mask_ips=True,
        clamp_start=(now - timedelta(hours=2)).isoformat(),
        clamp_end=now.isoformat(),
        clamp_cache_key="clamp-test",
    )
    assert "insights" in res


def _seed_city_data(con, table_name: str) -> None:
    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    baseline_ts = now - timedelta(hours=24, minutes=30)

    def _ins(ts: datetime, city: str, region: str, status: int, elapsed: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "city", "region", "country", "status", "elapsed") '
            "VALUES (?, ?, ?, 'US', ?, ?)",
            [ts.isoformat(), city, region, status, elapsed],
        )

    for _ in range(25):
        _ins(window_ts, "SurgeCity", "RegionS", 200, 50_000)

    for i in range(20):
        _ins(window_ts, "ErrorCity", "RegionE", 500 if i < 15 else 200, 50_000)
    for i in range(50):
        _ins(baseline_ts, "ErrorCity", "RegionE", 500 if i < 1 else 200, 50_000)

    for _ in range(12):
        _ins(window_ts, "SlowCity", "RegionL", 200, 2_000_000)
    for _ in range(60):
        _ins(baseline_ts, "SlowCity", "RegionL", 200, 100_000)

    for _ in range(8):
        _ins(window_ts, "FreshCity", "RegionF", 200, 50_000)


def test_coalesced_city_path_matches_per_insight_scan_output(in_memory_duckdb, test_service_source, monkeypatch):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "city" VARCHAR, '
        '"region" VARCHAR, "country" VARCHAR, "status" INTEGER, "elapsed" INTEGER)'
    )
    _seed_city_data(in_memory_duckdb, table_name)

    expected_ids = {"city_surges", "city_error_spikes", "city_latency_regressions", "new_city_traffic"}

    _insights_cache.clear()
    fast = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    fast_city = {i["id"]: i for i in fast["insights"] if i["id"] in expected_ids}

    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, "_coalesced_city_aggregates", lambda *a, **k: {})
    slow = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    slow_city = {i["id"]: i for i in slow["insights"] if i["id"] in expected_ids}

    assert set(fast_city.keys()) == expected_ids and set(slow_city.keys()) == expected_ids
    for iid in expected_ids:
        assert _norm_items(fast_city[iid]["items"]) == _norm_items(slow_city[iid]["items"])


def _seed_url_data(con, table_name: str) -> None:
    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    baseline_ts = now - timedelta(hours=24, minutes=30)

    def _ins(ts: datetime, url: str, status: int, cache: str, elapsed: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "url", "status", "cache", "elapsed") VALUES (?, ?, ?, ?, ?)',
            [ts.isoformat(), url, status, cache, elapsed],
        )

    for i in range(20):
        _ins(window_ts, "/ErrUrl", 500 if i < 14 else 200, "MISS", 50_000)
    for i in range(50):
        _ins(baseline_ts, "/ErrUrl", 500 if i < 1 else 200, "MISS", 50_000)

    for i in range(30):
        _ins(window_ts, "/CollUrl", 200, "HIT" if i < 5 else "MISS", 50_000)
    for i in range(200):
        _ins(baseline_ts, "/CollUrl", 200, "HIT" if i < 160 else "MISS", 50_000)

    for _ in range(20):
        _ins(window_ts, "/PassUrl", 200, "PASS", 50_000)
    for i in range(60):
        _ins(baseline_ts, "/PassUrl", 200, "HIT" if i < 50 else "MISS", 50_000)

    for _ in range(12):
        _ins(window_ts, "/RegUrl", 200, "MISS", 4_000_000)
    for _ in range(60):
        _ins(baseline_ts, "/RegUrl", 200, "MISS", 200_000)

    for _ in range(23):
        _ins(window_ts, "/TailUrl", 200, "MISS", 10_000)
    for _ in range(2):
        _ins(window_ts, "/TailUrl", 200, "MISS", 10_000_000)


def test_coalesced_url_path_matches_per_insight_scan_output(in_memory_duckdb, test_service_source, monkeypatch):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "url" VARCHAR, '
        '"status" INTEGER, "cache" VARCHAR, "elapsed" INTEGER)'
    )
    _seed_url_data(in_memory_duckdb, table_name)

    url_ids = {"error_spikes", "cache_collapse", "cacheability_regression", "latency_regression", "tail_latency"}

    _insights_cache.clear()
    fast = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    fast_url = {i["id"]: i for i in fast["insights"] if i["id"] in url_ids}

    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, "_coalesced_url_aggregates", lambda *a, **k: {})
    slow = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    slow_url = {i["id"]: i for i in slow["insights"] if i["id"] in url_ids}

    assert set(fast_url.keys()) == url_ids and set(slow_url.keys()) == url_ids
    for iid in url_ids:
        assert _norm_items(fast_url[iid]["items"]) == _norm_items(slow_url[iid]["items"])


def test_coalesced_url_aggregates_caps_per_insight_at_top_15():
    rows = [
        (
            f"/url-{i}",
            1000,
            100,
            500 + i,
            1,
            10,
            90 - i % 20,
            90,
            10,
            60,
            1,
            100,
            100,
            500.0 + i,
            100.0,
            3000.0 + i * 10,
            100.0,
        )
        for i in range(100)
    ]
    fake_cursor = MagicMock()
    fake_cursor.fetchmany.side_effect = [rows, []]
    fake_runner = MagicMock()
    fake_runner.execute.return_value = fake_cursor

    result = _coalesced_url_aggregates(fake_runner, "t", window_start_s="2024-01-01T00:00:00Z")
    assert set(result.keys()) == {
        "error_spikes",
        "cache_collapse",
        "cacheability_regression",
        "latency_regression",
        "tail_latency",
    }
    for items in result.values():
        assert len(items) <= 15
    assert result["error_spikes"][0][0] == "/url-99"


def _seed_ip_security_data(con, table_name: str) -> None:
    now = datetime.now(UTC)
    for idx, p in enumerate(["/admin", "/.env", "/.git", "/wp-login.php", "/phpmyadmin", "/config.php"]):
        ts = now - timedelta(minutes=25 - idx * 4)
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status") VALUES (?, ?, ?, ?)',
            [ts.isoformat(), "10.0.0.1", p, 200],
        )
    window_ts = now - timedelta(minutes=15)
    for i in range(25):
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status") VALUES (?, ?, ?, ?)',
            [(window_ts + timedelta(seconds=i)).isoformat(), "10.0.0.2", "/api/v1/login", 401 if i < 22 else 200],
        )
    for i in range(35):
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status") VALUES (?, ?, ?, ?)',
            [
                (window_ts + timedelta(seconds=i)).isoformat(),
                "10.0.0.3",
                f"/nonexistent/path_{i}" if i < 32 else "/valid/home",
                404 if i < 32 else 200,
            ],
        )
    con.execute(
        f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status") VALUES (?, ?, ?, ?)',
        [(now - timedelta(hours=24, minutes=30)).isoformat(), "10.0.0.99", "/valid/home", 200],
    )


def test_coalesced_ip_security_parity_and_caps(in_memory_duckdb, test_service_source, monkeypatch):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, '
        '"url" VARCHAR, "status" INTEGER)'
    )
    _seed_ip_security_data(in_memory_duckdb, table_name)

    ip_ids = {"low_and_slow", "credential_enumeration", "content_discovery"}

    _insights_cache.clear()
    fast = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    fast_ip = {i["id"]: i for i in fast["insights"] if i["id"] in ip_ids}

    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, "_coalesced_ip_security_aggregates", lambda *a, **k: {})
    slow = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    slow_ip = {i["id"]: i for i in slow["insights"] if i["id"] in ip_ids}

    assert set(fast_ip.keys()) == ip_ids and set(slow_ip.keys()) == ip_ids
    for iid in ip_ids:
        assert _norm_items(fast_ip[iid]["items"]) == _norm_items(slow_ip[iid]["items"])


@pytest.mark.security_regression
def test_coalesced_ip_security_masks_ip_end_to_end_for_analyst(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, '
        '"url" VARCHAR, "status" INTEGER)'
    )
    _seed_ip_security_data(in_memory_duckdb, table_name)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24, mask_ips=True)
    ip_ids = {"low_and_slow", "credential_enumeration", "content_discovery"}
    raw_ips = {"10.0.0.1", "10.0.0.2", "10.0.0.3"}

    found_cards = [i for i in res["insights"] if i["id"] in ip_ids]
    assert len(found_cards) == 3
    for card in found_cards:
        assert card["items"], f"expected items for {card['id']}"
        for item in card["items"]:
            assert item["label"] not in raw_ips
            assert item["label"] in {mask_ip(ip) for ip in raw_ips}
            for raw_ip in raw_ips:
                assert raw_ip not in item.get("investigate_url", "")


def test_every_insight_has_valid_category_and_matches_availability_catalog():
    valid = {c.value for c in InsightCategory}
    for d in registry.get_all():
        assert str(d.category) in valid, f"{d.id} has bad category {d.category!r}"

    reg = {d.id: str(d.category) for d in registry.get_all()}
    legacy = {d["id"]: d.get("category") for d in INSIGHT_DEFINITIONS}

    missing = sorted(set(reg) - set(legacy))
    assert not missing, f"computed insights missing from availability catalog: {missing}"
    no_cat = sorted(i for i, c in legacy.items() if not c)
    assert not no_cat, f"availability catalog entries without a category: {no_cat}"
    mismatch = {i: (reg[i], legacy[i]) for i in reg if i in legacy and reg[i] != legacy[i]}
    assert not mismatch, f"category mismatch: {mismatch}"


def test_registry_definitions_and_error_spikes_processor():
    all_defs = registry.get_all()
    ids = [d.id for d in all_defs]
    assert "error_spikes" in ids and "botnet_grouping" in ids

    definition = registry.get("error_spikes")
    assert definition is not None and definition.title == "Error Spikes"
    assert "url" in definition.required_fields and "status" in definition.required_fields
    assert "{table_name}" in definition.sql_template and definition.row_processor is not None

    item = definition.row_processor(("/test", 0.1, 0.02, 10, 100, 500), definition, {})
    assert item["label"] == "/test" and item["current_val"] == 10.0 and item["severity"] == "warning"

    item_crit = definition.row_processor(("/crit", 0.6, 0.02, 60, 100, 500), definition, {})
    assert item_crit["severity"] == "critical"
