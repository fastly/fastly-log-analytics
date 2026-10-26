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
import pytest
from backend.core.share_db.validation import mask_ip

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.repositories._base import _safe_table
from backend.repositories.insights import definitions as defs
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


from backend.repositories._sql import insights as SQL  # noqa: E402
from backend.repositories.insights import _insights_cache  # noqa: E402


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


# ── Merged from tests/repositories/test_insights_processors.py ──

# ── Registry contract: every documented insight is registered ───────────────


_EXPECTED_INSIGHT_IDS = {
    "error_spikes",
    "botnet_grouping",
    "new_country_traffic",
    "city_surges",
    "city_error_spikes",
    "city_latency_regressions",
    "new_city_traffic",
    "ua_monoculture",
    "new_probe_urls",
    "waf_signal_spikes",
    "proxy_surge",
    "asn_concentration",
    "asn_metro_performance",
    "cache_collapse",
    "latency_regression",
    "impossible_distance",
    "tail_latency",
    "cipher_spread",
    "request_size_anomaly",
    "connection_abuse",
    "region_latency",
    "cache_ttl_mismatch",
    "image_optimization_opportunities",
    "origin_latency_spike",
    "origin_error_rate",
    "origin_retries",
    "origin_ip_failure",
    "shield_path_degradation",
    "repeated_patterns",
    "low_and_slow",
    "credential_enumeration",
    "network_asn_health",
    "content_discovery",
    "referer_monoculture",
    "method_drift",
    "new_asn_traffic",
    "asn_hosting_shift",
    "metro_delivery_degradation",
    "connection_type_mix",
    "pop_latency_regression",
    "http3_fallback",
    "cache_hit_cliff",
    "payload_compression_regression",
    "session_harvesting",
    "timeout_split",
}


def test_every_expected_insight_is_registered():
    """The frontend keys on these exact IDs; dropping or renaming one
    here would silently make the matching card disappear from the UI."""
    registered = {d.id for d in registry.get_all()}
    missing = _EXPECTED_INSIGHT_IDS - registered
    assert not missing, f"missing from registry: {missing}"


def test_no_duplicate_required_fields_in_any_definition():
    """Trivial sanity — a typo'd required_fields list could cause the
    field-existence check to filter the insight out for valid services."""
    for d in registry.get_all():
        assert len(d.required_fields) == len(set(d.required_fields)), (
            f"{d.id} has duplicate required_fields: {d.required_fields}"
        )


# ── error_spikes ────────────────────────────────────────────────────────────


def test_error_spikes_critical_threshold_at_50_percent():
    """At ``w_rate=0.5`` the processor flips warning → critical."""
    p = defs.error_spikes_processor
    just_under = p(("/x", 0.49, 0.02, 49, 100, 500), None, {})
    at_boundary = p(("/x", 0.5, 0.02, 50, 100, 500), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_error_spikes_empty_label_falls_back_to_placeholder():
    """An empty/None URL → ``(empty)`` so the card still renders.
    Pinned because a None label would crash the frontend list."""
    out = defs.error_spikes_processor((None, 0.1, 0.0, 5, 50, 100), None, {})
    assert out["label"] == "(empty)"


# ── botnet_grouping ─────────────────────────────────────────────────────────


def test_botnet_grouping_severity_threshold_at_50_ips():
    p = defs.botnet_grouping_processor
    just_under = p(("ja3fp", 49, 200, 5, 9.8), None, {})
    at_boundary = p(("ja3fp", 50, 200, 5, 10.0), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_botnet_grouping_filters_use_actual_fp_col():
    """The filter must use the column that was actually queried (from
    context['fp_col']), not both ja3 and ja4 — setting both creates an
    AND filter on the dashboard that matches nothing."""
    out = defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja4"})
    assert out["meta"]["filters"] == {"ja4": "fp123"}

    out = defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja3"})
    assert out["meta"]["filters"] == {"ja3": "fp123"}


# ── new_country_traffic ─────────────────────────────────────────────────────


def test_new_country_traffic_severity_threshold_at_10():
    p = defs.new_country_traffic_processor
    assert p(("ZW", 9, 0), None, {})["severity"] == "info"
    assert p(("ZW", 10, 0), None, {})["severity"] == "warning"


def test_new_country_traffic_baseline_is_always_zero():
    """By definition this insight only fires when baseline_val=0; that
    invariant is encoded both in the SQL HAVING clause AND the processor."""
    out = defs.new_country_traffic_processor(("XX", 50, 0), None, {})
    assert out["baseline_val"] == 0


# ── city_surges ─────────────────────────────────────────────────────────────


def test_city_surges_normalises_baseline_to_window_size():
    """``baseline_val`` is rescaled from baseline_hours → window_hours
    so the card shows comparable numbers (not "this hour vs the last
    week's total")."""
    context = {"baseline_hours": 24, "window_hours": 1}
    out = defs.city_surges_processor(("LA, US", "LA", "CA", "US", 100, 240, 4.2), None, context)
    assert out["baseline_val"] == 10.0  # 240 / 24 * 1


def test_city_surges_severity_threshold_at_ratio_10():
    p = defs.city_surges_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    assert p(("x", "x", "", "X", 100, 10, 9.5), None, ctx)["severity"] == "info"
    assert p(("x", "x", "", "X", 100, 10, 10.0), None, ctx)["severity"] == "warning"


# ── city_error_spikes ──────────────────────────────────────────────────────


def test_city_error_spikes_critical_above_50_percent_error_rate():
    p = defs.city_error_spikes_processor
    just_under = p(("L", "L", "", "X", 0.49, 0.02, 49, 100, 500), None, {})
    at_boundary = p(("L", "L", "", "X", 0.5, 0.02, 50, 100, 500), None, {})
    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_city_error_spikes_region_filter_null_when_empty():
    """Empty region string must surface as ``None`` in the filter dict
    — the frontend's filter pill renderer drops None keys, but would
    include an empty-string filter literally (producing zero results)."""
    out = defs.city_error_spikes_processor(("L", "L", "", "US", 0.3, 0.1, 30, 100, 500), None, {})
    assert out["meta"]["filters"]["region"] is None


# ── city_latency_regressions ───────────────────────────────────────────────


def test_city_latency_critical_at_5000ms():
    p = defs.city_latency_processor
    assert p(("L", "L", "", "X", 4999.0, 1000.0, 100, 500), None, {})["severity"] == "warning"
    assert p(("L", "L", "", "X", 5000.0, 1000.0, 100, 500), None, {})["severity"] == "critical"


def test_city_latency_regression_ratio_zero_when_baseline_zero():
    """``b_p95=0`` would divide-by-zero; the processor guards with
    ``if row[5] else 0``."""
    out = defs.city_latency_processor(("L", "L", "", "X", 100.0, 0.0, 100, 500), None, {})
    assert out["meta"]["regression_ratio"] == 0


# ── new_city_traffic ────────────────────────────────────────────────────────


def test_new_city_severity_threshold_at_50():
    p = defs.new_city_traffic_processor
    assert p(("L", "L", "", "X", 49, 0), None, {})["severity"] == "info"
    assert p(("L", "L", "", "X", 50, 0), None, {})["severity"] == "warning"


# ── ua_monoculture ──────────────────────────────────────────────────────────


def test_ua_monoculture_critical_at_50_percent():
    p = defs.ua_monoculture_processor
    just_under = p(("bot/1.0", 49, 0, 100, 100), None, {})
    at_boundary = p(("bot/1.0", 50, 0, 100, 100), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_ua_monoculture_empty_label_falls_back():
    out = defs.ua_monoculture_processor((None, 10, 0, 100, 100), None, {})
    assert out["label"] == "(empty)"


# ── new_probe_urls ──────────────────────────────────────────────────────────


def test_new_probe_urls_critical_at_5_hits():
    p = defs.new_probe_urls_processor
    just_under = p(("/.env", 4, 0, 100.0), None, {})
    at_boundary = p(("/.env", 5, 0, 100.0), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_new_probe_urls_error_pct_defaults_to_zero_when_none():
    """SQL may return None for error_pct on a single-row group; the
    processor must coerce to 0 so the meta dict is JSON-serialisable."""
    out = defs.new_probe_urls_processor(("/x", 1, 0, None), None, {})
    assert out["meta"]["error_pct"] == 0


def test_new_probes_list_is_regex_escaped():
    """Every probe in NEW_PROBES is passed through ``re.escape`` before
    joining — pinned because a probe like ``.env`` contains regex
    metacharacters that would otherwise match almost any URL."""
    import re

    from backend.repositories._sql import insights as SQL

    # ``.env`` → ``\.env`` in the compiled regex
    assert r"\.env" in SQL.NEW_PROBE_REGEX
    assert r"\.git" in SQL.NEW_PROBE_REGEX
    # Sanity: the compiled regex compiles
    re.compile(SQL.NEW_PROBE_REGEX)


def test_new_probes_regex_is_case_insensitive_via_duckdb_flag():
    """Case-insensitivity is supplied by the ``'i'`` flag arg passed to
    ``regexp_matches`` in the SQL template — NOT by an inline ``(?i)``
    prefix in NEW_PROBE_REGEX itself.

    The inline ``(?i)`` was removed because its literal ``?`` was
    miscounted by the repository's ``sql.count("?")`` placeholder
    heuristic, producing one excess bound parameter and a
    ``Parameter argument/count mismatch`` error at execution. Pinned
    here so future refactors don't regress to ``(?i)``."""
    from backend.repositories._sql import insights as SQL

    # Regex itself must NOT contain inline flags (would break param counting).
    assert "(?i)" not in SQL.NEW_PROBE_REGEX
    # SQL template must pass the 'i' flag as the third arg to regexp_matches.
    sql = next(d.sql_template for d in defs.registry.get_all() if d.id == "new_probe_urls")
    assert "regexp_matches" in sql
    assert "'i'" in sql, "Case-insensitive flag missing from regexp_matches call"


# ── waf_signal_spikes ──────────────────────────────────────────────────────


def test_waf_signal_spikes_normalises_baseline_to_window():
    ctx = {"baseline_hours": 24, "window_hours": 1}
    out = defs.waf_signal_spikes_processor(("SQLI", 50, 240, 12.0), None, ctx)
    assert out["baseline_val"] == 10.0


def test_waf_signal_spikes_critical_at_ratio_10():
    p = defs.waf_signal_spikes_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    assert p(("S", 5, 1, 9.9), None, ctx)["severity"] == "warning"
    assert p(("S", 5, 1, 10.0), None, ctx)["severity"] == "critical"


# ── proxy_surge ─────────────────────────────────────────────────────────────


def test_proxy_surge_processor_computes_percent_of_total():
    """Severity is fixed to warning for the row processor; the
    *insight-level* severity is what ``proxy_surge_severity`` returns."""
    out = defs.proxy_surge_processor(("VPN", 250, 50, 1000, 800), None, {})
    assert out["current_val"] == 25.0
    assert out["severity"] == "warning"


def test_proxy_surge_processor_handles_zero_total_safely():
    """``max(w_total_all, 1)`` guards against division by zero."""
    out = defs.proxy_surge_processor(("VPN", 5, 0, 0, 0), None, {})
    assert out["current_val"] == 500.0  # 5 * 100 / max(0, 1) = 500


def test_proxy_surge_severity_clean_when_no_items():
    """Empty result → 'clean' (rendered as a green badge by the UI).
    Non-empty → 'warning' (the items themselves are above threshold)."""
    assert defs.proxy_surge_severity([]) == "clean"
    assert defs.proxy_surge_severity([{"label": "VPN"}]) == "warning"


# ── asn_concentration ───────────────────────────────────────────────────────


def test_asn_concentration_attaches_name_when_available():
    """``context['asn_names']`` is an optional lookup the repo may
    populate from the asn_names cache. When present, the label gets
    ``(name)`` appended — without it, just ``AS{n}``."""
    p = defs.asn_concentration_processor
    out_with_name = p(
        (16509, 50, 5, 100, 100),
        None,
        {"asn_names": {16509: "AMAZON-02"}},
    )
    out_without = p((16509, 50, 5, 100, 100), None, {})

    assert out_with_name["label"] == "AS16509 (AMAZON-02)"
    assert out_without["label"] == "AS16509"


def test_asn_concentration_critical_above_50_percent():
    p = defs.asn_concentration_processor
    assert p((1, 49, 5, 100, 100), None, {})["severity"] == "warning"
    assert p((1, 50, 5, 100, 100), None, {})["severity"] == "critical"


# ── asn_metro_performance ──────────────────────────────────────────────────


def test_asn_metro_performance_label_combines_asn_and_metro():
    """The label uses ``dma_map`` to translate DMA codes to names; when
    a DMA isn't in the map it falls back to ``DMA {code}`` so the card
    is still readable."""
    ctx = {"dma_map": {"803": "Los Angeles, CA"}, "asn_names": {7922: "COMCAST"}}
    out = defs.asn_metro_performance_processor((7922, "803", 150.0, 30.0, 100, 500), None, ctx)
    assert out["label"] == "AS7922 (COMCAST) in Los Angeles, CA"


def test_asn_metro_performance_unknown_dma_falls_back_to_code():
    out = defs.asn_metro_performance_processor((7922, "999", 150.0, 30.0, 100, 500), None, {})
    assert "DMA 999" in out["label"]
    assert "AS7922" in out["label"]


def test_asn_metro_performance_severity_at_baseline_plus_100():
    """Critical when ``w_med >= b_med + 100``."""
    p = defs.asn_metro_performance_processor
    just_under = p((1, "M", 99.0, 0.0, 100, 500), None, {})
    at_boundary = p((1, "M", 100.0, 0.0, 100, 500), None, {})
    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


# ── cache_collapse ──────────────────────────────────────────────────────────


def test_cache_collapse_critical_when_dropped_from_above_60_to_below_10():
    p = defs.cache_collapse_processor
    # Critical: w<=0.10 AND b>=0.60
    crit = p(("/img", 0.05, 0.65, 100, 500), None, {})
    # Warning: above threshold OR below baseline
    warn = p(("/img", 0.11, 0.65, 100, 500), None, {})

    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


# ── cacheability_regression ────────────────────────────────────────────────


def test_cacheability_regression_processor_shape_and_severity():
    p = defs.cacheability_regression_processor
    # row schema: [url, w_pass_rate, b_pass_rate, w_total, b_total]
    out = p(("/api", 0.95, 0.02, 200, 800), None, {})
    assert out["label"] == "/api"
    assert out["unit"] == "% PASS"
    assert out["current_val"] == 95.0
    assert out["baseline_val"] == 2.0
    assert out["meta"]["filters"] == {"url": "/api"}
    # Critical: w>=0.80 AND b<=0.10
    assert out["severity"] == "critical"
    # Warning: surge present but not extreme enough for critical.
    assert p(("/api", 0.60, 0.15, 200, 800), None, {})["severity"] == "warning"


# ── latency_regression ─────────────────────────────────────────────────────


def test_latency_regression_critical_at_5000ms():
    p = defs.latency_regression_processor
    assert p(("/x", 4999.0, 1000.0, 100, 500), None, {})["severity"] == "warning"
    assert p(("/x", 5000.0, 1000.0, 100, 500), None, {})["severity"] == "critical"


def test_latency_regression_ratio_zero_when_baseline_zero():
    out = defs.latency_regression_processor(("/x", 100.0, 0.0, 100, 500), None, {})
    assert out["meta"]["regression_ratio"] == 0


# ── impossible_distance ────────────────────────────────────────────────────


def test_impossible_distance_critical_when_excess_over_5000km():
    p = defs.impossible_distance_processor
    row = ("ja3", 5, 5001.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC")
    crit = p(row, None, {})
    warn = p(
        ("ja3", 5, 5000.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC"), None, {}
    )
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


def test_impossible_distance_fp_col_filter_uses_context_default():
    """``fp_col`` defaults to 'ja3' when context doesn't specify — used
    by the click-through to apply the right column filter."""
    p = defs.impossible_distance_processor
    out_default = p(("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"), None, {})
    out_ja4 = p(
        ("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"),
        None,
        {"fp_col": "ja4"},
    )
    assert "ja3" in out_default["meta"]["filters"]
    assert "ja4" in out_ja4["meta"]["filters"]


# ── tail_latency ────────────────────────────────────────────────────────────


def test_tail_latency_critical_above_ratio_10():
    p = defs.tail_latency_processor
    assert p(("/x", 5000, 500, 10.0, 100), None, {})["severity"] == "warning"
    assert p(("/x", 5000, 500, 10.1, 100), None, {})["severity"] == "critical"


# ── cipher_spread ──────────────────────────────────────────────────────────


def test_cipher_spread_label_truncates_long_sha():
    """SHAs are long; the label is truncated to 12 chars + ellipsis so
    the card title doesn't wrap."""
    p = defs.cipher_spread_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    long_sha = "abcdef1234567890abcdef"
    short_sha = "shorthash"

    out_long = p((long_sha, 10, 100, 5), None, ctx)
    out_short = p((short_sha, 10, 100, 5), None, ctx)

    assert out_long["label"] == "abcdef123456…"
    assert out_short["label"] == "shorthash"  # short stays unchanged


def test_cipher_spread_unknown_label_when_sha_empty():
    p = defs.cipher_spread_processor
    out = p(("", 10, 100, 5), None, {"baseline_hours": 1, "window_hours": 1})
    # Empty string → fallback to "(unknown)"
    assert out["label"] == "(unknown)"


def test_cipher_spread_severity_critical_at_50_ips():
    p = defs.cipher_spread_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    assert p(("abc", 49, 100, 5), None, ctx)["severity"] == "warning"
    assert p(("abc", 50, 100, 5), None, ctx)["severity"] == "critical"


# ── request_size_anomaly ───────────────────────────────────────────────────


def test_request_size_anomaly_critical_above_64kb():
    p = defs.request_size_anomaly_processor
    assert p(("1.2.3.4", 64000, 30000, 5, 1000), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 64001, 30000, 5, 1000), None, {})["severity"] == "critical"


def test_request_size_anomaly_handles_none_bytes():
    """``int(None or 0) == 0`` — no AttributeError."""
    out = defs.request_size_anomaly_processor((None, None, None, 3, None), None, {})
    assert out["current_val"] == 0
    assert out["baseline_val"] == 0


# ── connection_abuse ───────────────────────────────────────────────────────


def test_connection_abuse_critical_above_500_reqs_per_conn():
    p = defs.connection_abuse_processor
    assert p(("1.2.3.4", 500, 100, 50, 100), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 501, 100, 50, 100), None, {})["severity"] == "critical"


# ── region_latency ─────────────────────────────────────────────────────────


def test_region_latency_includes_ottfb_p95_when_provided():
    """The ottfb_p95 column is OPTIONAL (LEFT JOIN in SQL); when None
    the meta dict must NOT have an ``ottfb_p95_ms`` key. Pinned because
    the frontend distinguishes "no data" (don't render the badge) from
    "0 ms" (render but in red — that's literally instant)."""
    with_ottfb = defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, 30.0), None, {})
    without_ottfb = defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, None), None, {})

    assert with_ottfb["meta"]["ottfb_p95_ms"] == 30.0
    assert "ottfb_p95_ms" not in without_ottfb["meta"]


def test_region_latency_critical_at_5000ms():
    p = defs.region_latency_processor
    assert p(("r", 4999.0, 1000.0, 100, 500, None), None, {})["severity"] == "warning"
    assert p(("r", 5000.0, 1000.0, 100, 500, None), None, {})["severity"] == "critical"


# ── cache_ttl_mismatch ─────────────────────────────────────────────────────


def test_cache_ttl_mismatch_label_coerces_to_str():
    """``q_col`` may be any DuckDB-castable type; the processor wraps
    in ``str()`` so a numeric label doesn't crash the JSON serialiser."""
    out = defs.cache_ttl_mismatch_processor((123, 3600, 1.5, 60, 100), None, {})
    assert out["label"] == "123"  # not int(123)


def test_cache_ttl_mismatch_severity_is_always_warning():
    """This insight doesn't have a critical tier — pinned so a future
    refactor that adds one doesn't silently change the threshold for
    existing dashboards."""
    out = defs.cache_ttl_mismatch_processor(("/x", 3600, 0.5, 60, 100), None, {})
    assert out["severity"] == "warning"


# ── image_optimization ─────────────────────────────────────────────────────


def test_image_optimization_critical_when_total_over_10MB():
    p = defs.image_optimization_processor
    crit_total = p(("/x.jpg", 100, 10 * 1024 * 1024 + 1, 500.0, 0.3), None, {})
    assert crit_total["severity"] == "critical"


def test_image_optimization_critical_when_avg_over_1MB():
    p = defs.image_optimization_processor
    crit_avg = p(("/x.jpg", 5, 1024 * 1024 * 5, 1025.0, 0.3), None, {})
    assert crit_avg["severity"] == "critical"


def test_image_optimization_warning_when_mobile_ratio_above_50_percent():
    """Mobile-heavy traffic should escalate to warning even if total
    size is moderate — mobile users feel image bloat more acutely."""
    p = defs.image_optimization_processor
    out = p(("/x.jpg", 50, 3 * 1024 * 1024, 60.0, 0.6), None, {})
    assert out["severity"] == "warning"


def test_image_optimization_info_severity_for_small_total():
    p = defs.image_optimization_processor
    out = p(("/x.jpg", 5, 600 * 1024, 100.0, 0.1), None, {})
    assert out["severity"] == "info"


def test_image_optimization_mobile_ratio_defaults_to_zero_when_none():
    out = defs.image_optimization_processor(("/x", 1, 600 * 1024, 100.0, None), None, {})
    assert out["meta"]["mobile_ratio"] == 0


# ── origin_latency_spike ───────────────────────────────────────────────────


def test_origin_latency_spike_critical_when_5x_baseline():
    p = defs.origin_latency_spike_processor
    crit = p(("/x", 100.0, 90.0, 10.0, 100), None, {})  # 100 > 10*5
    warn = p(("/x", 50.0, 90.0, 10.0, 100), None, {})  # 50 == 10*5 → not >
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


# ── origin_error_rate ──────────────────────────────────────────────────────


def test_origin_error_rate_critical_above_5_percent():
    p = defs.origin_error_rate_processor
    # row: status, w_cnt, w_total, b_total, w_5xx, b_5xx
    crit = p((503, 10, 100, 100, 6, 1), None, {})  # 6%
    warn = p((503, 5, 100, 100, 5, 1), None, {})  # 5% → not > 5
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


def test_origin_error_rate_label_includes_http_status():
    out = defs.origin_error_rate_processor((503, 10, 100, 100, 6, 1), None, {})
    assert out["label"] == "HTTP 503"


# ── origin_retries ─────────────────────────────────────────────────────────


def test_origin_retries_severity_always_warning():
    """Retries are noteworthy but not critical — pinned so adding a
    critical tier requires updating this test (and presumably the UI)."""
    out = defs.origin_retries_processor(("/x", 100, 2.5, 5), None, {})
    assert out["severity"] == "warning"


# ── origin_ip_failure ──────────────────────────────────────────────────────


def test_origin_ip_failure_critical_above_20_percent_errors():
    p = defs.origin_ip_failure_processor
    assert p(("10.0.0.1", 100, 20.0, 5.0), None, {})["severity"] == "warning"
    assert p(("10.0.0.1", 100, 21.0, 5.0), None, {})["severity"] == "critical"


# ── shield_path_degradation ────────────────────────────────────────────────


def test_shield_path_degradation_critical_at_3x_ratio():
    p = defs.shield_path_degradation_processor
    crit = p(("LAX", "SFO", 300.0, 100.0, 10), None, {})  # ratio=3.0
    warn = p(("LAX", "SFO", 299.0, 100.0, 10), None, {})
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


def test_shield_path_degradation_handles_zero_baseline():
    """``b_p50=0`` would div-by-zero; guard with truthiness check."""
    out = defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 0.0, 10), None, {})
    assert out["meta"]["ratio"] == 0


def test_shield_path_degradation_label_uses_arrow_format():
    out = defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 50.0, 10), None, {})
    assert out["label"] == "LAX → SFO"


# ── M3: IP-keyed processors mask the client IP when the analyst sets mask_ips ──


def test_request_size_anomaly_masks_ip_when_mask_ips():
    # row schema: [ip, max_bytes, avg_bytes, w_total, b_p95]
    row = ("203.0.113.42", 70000, 100, 50, 1000)
    masked = defs.request_size_anomaly_processor(row, None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx"
    assert masked["meta"]["filters"]["ip"] == "203.0.113.xxx"  # also feeds investigate_url


def test_request_size_anomaly_keeps_ip_for_admin():
    row = ("203.0.113.42", 70000, 100, 50, 1000)
    raw = defs.request_size_anomaly_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


def test_connection_abuse_masks_ip_when_mask_ips():
    # row schema: [ip, max_reqs, avg_reqs, w_total, b_p95]
    row = ("203.0.113.42", 800, 10, 50, 100)
    masked = defs.connection_abuse_processor(row, None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx"
    assert masked["meta"]["filters"]["ip"] == "203.0.113.xxx"


def test_connection_abuse_keeps_ip_for_admin():
    row = ("203.0.113.42", 800, 10, 50, 100)
    raw = defs.connection_abuse_processor(row, None, {})
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


# ── Phase-3: low_and_slow ────────────────────────────────────────────────────


def test_low_and_slow_happy_path():
    """current_val is the distinct sensitive-path count; hits/span/rps land
    in meta and the IP keys both the label and the investigate filter."""
    # row schema: [ip, hits, distinct_paths, span_s, rps]
    out = defs.low_and_slow_processor(("198.51.100.7", 42, 6, 7200, 0.0058), None, {})
    assert out["label"] == "198.51.100.7"
    assert out["current_val"] == 6
    assert out["baseline_val"] is None
    assert out["unit"] == "sensitive paths"
    assert out["meta"]["hits"] == 42
    assert out["meta"]["distinct_paths"] == 6
    assert out["meta"]["span_s"] == 7200
    assert out["meta"]["rps"] == 0.0058
    assert out["meta"]["filters"] == {"ip": "198.51.100.7"}


def test_low_and_slow_critical_at_10_distinct_paths():
    """distinct_paths flips warning → critical at 10 sensitive paths."""
    p = defs.low_and_slow_processor
    assert p(("1.2.3.4", 20, 9, 7200, 0.001), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 20, 10, 7200, 0.001), None, {})["severity"] == "critical"


def test_low_and_slow_handles_none_span_and_rps():
    """A single-timestamp group can yield NULL span/rps; the processor
    coerces to 0 so the meta dict stays JSON-serialisable."""
    out = defs.low_and_slow_processor(("1.2.3.4", 5, 3, None, None), None, {})
    assert out["meta"]["span_s"] == 0
    assert out["meta"]["rps"] == 0.0


# ── Phase-3: credential_enumeration ──────────────────────────────────────────


def test_credential_enumeration_happy_path():
    """current_val is the window denied count; baseline_val is the baseline
    denied count; fail_rate_pct = denied/attempts."""
    # row schema: [ip, w_denied, w_attempts, w_paths, b_denied]
    out = defs.credential_enumeration_processor(("203.0.113.9", 80, 100, 4, 2), None, {})
    assert out["label"] == "203.0.113.9"
    assert out["current_val"] == 80
    assert out["baseline_val"] == 2
    assert out["unit"] == "denied (401/403)"
    assert out["meta"]["attempts"] == 100
    assert out["meta"]["denied"] == 80
    assert out["meta"]["fail_rate_pct"] == 80.0  # 80 / 100 * 100
    assert out["meta"]["distinct_paths"] == 4
    assert out["meta"]["filters"] == {"ip": "203.0.113.9"}


def test_credential_enumeration_critical_at_100_denied():
    """w_denied flips warning → critical at 100 denied auth responses."""
    p = defs.credential_enumeration_processor
    assert p(("1.2.3.4", 99, 120, 3, 0), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 100, 120, 3, 0), None, {})["severity"] == "critical"


def test_credential_enumeration_fail_rate_zero_when_no_attempts():
    """``w_attempts=0`` would divide-by-zero; the processor guards it."""
    out = defs.credential_enumeration_processor(("1.2.3.4", 0, 0, 0, 0), None, {})
    assert out["meta"]["fail_rate_pct"] == 0.0


# ── Phase-4 (Track A): content_discovery ─────────────────────────────────────


def test_content_discovery_happy_path():
    """current_val is the window 404 count; baseline_val the baseline 404
    count; not_found_rate_pct = 404s/total; the IP keys label + filter."""
    # row schema: [ip, w_404, w_total, distinct_404, b_404]
    out = defs.content_discovery_processor(("198.51.100.7", 60, 75, 40, 3), None, {})
    assert out["label"] == "198.51.100.7"
    assert out["current_val"] == 60
    assert out["baseline_val"] == 3
    assert out["unit"] == "404s"
    assert out["meta"]["requests"] == 75
    assert out["meta"]["not_found"] == 60
    assert out["meta"]["not_found_rate_pct"] == 80.0  # 60 / 75 * 100
    assert out["meta"]["distinct_404_urls"] == 40
    assert out["meta"]["filters"] == {"ip": "198.51.100.7"}


def test_content_discovery_critical_at_100_not_found():
    """w_404 flips warning → critical at 100 window 404s."""
    p = defs.content_discovery_processor
    assert p(("1.2.3.4", 99, 120, 40, 0), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 100, 120, 40, 0), None, {})["severity"] == "critical"


def test_content_discovery_rate_zero_when_no_total():
    """``w_total=0`` would divide-by-zero; the processor guards it."""
    out = defs.content_discovery_processor(("1.2.3.4", 0, 0, 0, 0), None, {})
    assert out["meta"]["not_found_rate_pct"] == 0.0


# ── Phase-4 (Track B): referer_monoculture ───────────────────────────────────


def test_referer_monoculture_happy_path():
    # row schema: [referer, w_cnt, b_cnt, b_total, w_total]
    out = defs.referer_monoculture_processor(("http://spam.example", 40, 5, 500, 100), None, {})
    assert out["label"] == "http://spam.example"
    assert out["current_val"] == 40.0  # 40/100 * 100
    assert out["baseline_val"] == 1.0  # 5/500 * 100
    assert out["unit"] == "% of traffic"
    assert out["meta"]["requests"] == 40
    assert out["meta"]["filters"] == {"referer": "http://spam.example"}


def test_referer_monoculture_critical_at_50_percent():
    p = defs.referer_monoculture_processor
    assert p(("r", 49, 0, 100, 100), None, {})["severity"] == "warning"
    assert p(("r", 50, 0, 100, 100), None, {})["severity"] == "critical"


def test_referer_monoculture_empty_label_and_zero_total_guard():
    out = defs.referer_monoculture_processor((None, 0, 0, 0, 0), None, {})
    assert out["label"] == "(empty)"
    assert out["current_val"] == 0.0  # w_total=0 guarded


# ── Phase-4 (Track B): method_drift ──────────────────────────────────────────


def test_method_drift_happy_path():
    # row schema: [method, w_cnt, b_cnt, w_total, b_total]
    out = defs.method_drift_processor(("POST", 30, 2, 100, 200), None, {})
    assert out["label"] == "POST"
    assert out["current_val"] == 30.0  # 30/100 * 100
    assert out["baseline_val"] == 1.0  # 2/200 * 100
    assert out["unit"] == "% of traffic"
    assert out["meta"]["filters"] == {"method": "POST"}


def test_method_drift_critical_at_25_percent():
    p = defs.method_drift_processor
    assert p(("POST", 24, 0, 100, 100), None, {})["severity"] == "warning"
    assert p(("POST", 25, 0, 100, 100), None, {})["severity"] == "critical"


# ── Phase-4 (Track B): new_asn_traffic ───────────────────────────────────────


def test_new_asn_traffic_happy_path():
    # row schema: [asn, w_cnt, b_cnt]
    out = defs.new_asn_traffic_processor((16509, 60, 0), None, {})
    assert out["label"] == "AS16509"
    assert out["current_val"] == 60
    assert out["baseline_val"] == 0
    assert out["unit"] == "requests"
    assert out["meta"]["asn"] == 16509
    assert out["meta"]["filters"] == {"asn": 16509}


def test_new_asn_traffic_attaches_name_and_severity_floor():
    p = defs.new_asn_traffic_processor
    with_name = p((7922, 150, 0), None, {"asn_names": {7922: "Example ISP"}})
    assert with_name["label"] == "AS7922 (Example ISP)"
    assert with_name["severity"] == "warning"  # >= 100
    assert p((7922, 99, 0), None, {})["severity"] == "info"  # < 100


# ── Phase-4 (Track B): asn_hosting_shift ──────────────────────────────────────


def test_asn_hosting_shift_happy_path():
    # row schema: [asn, w_hosting, w_total, b_hosting, b_total]
    out = defs.asn_hosting_shift_processor((16509, 45, 100, 5, 200), None, {})
    assert out["label"] == "AS16509"
    assert out["current_val"] == 45.0  # 45/100 = 45%
    assert out["baseline_val"] == 2.5  # 5/200 = 2.5%
    assert out["unit"] == "% hosting"
    assert out["meta"]["asn"] == 16509
    assert out["meta"]["filters"] == {"asn": 16509}
    assert out["meta"]["window_hosting"] == 45
    assert out["meta"]["window_total"] == 100


def test_asn_hosting_shift_attaches_name():
    out = defs.asn_hosting_shift_processor((7922, 30, 60, 2, 100), None, {"asn_names": {7922: "Comcast"}})
    assert out["label"] == "AS7922 (Comcast)"


def test_asn_hosting_shift_severity_boundary():
    p = defs.asn_hosting_shift_processor
    # 60% → critical
    assert p((1, 60, 100, 0, 100), None, {})["severity"] == "critical"
    # 59.9% → warning
    assert p((1, 599, 1000, 0, 100), None, {})["severity"] == "warning"


def test_asn_hosting_shift_zero_total_safety():
    out = defs.asn_hosting_shift_processor((1, 0, 0, 0, 0), None, {})
    assert out["current_val"] == 0.0
    assert out["baseline_val"] == 0.0


# ── Phase-4 (Track B2): metro_delivery_degradation ───────────────────────────


def test_metro_delivery_degradation_happy_path():
    # row schema: [metro, w_med(bytes/s), b_med(bytes/s), w_total, b_total]
    # 1_000_000 B/s = 8.0 Mbps; 10_000_000 B/s = 80.0 Mbps.
    out = defs.metro_delivery_degradation_processor((501, 1_000_000, 10_000_000, 60, 200), None, {})
    assert out["current_val"] == 8.0
    assert out["baseline_val"] == 80.0
    assert out["unit"] == "Mbps (median)"
    assert out["meta"]["filters"] == {"metro": 501}
    assert out["label"] == "DMA 501"  # no dma_map in context


def test_metro_delivery_degradation_uses_dma_map_and_severity():
    p = defs.metro_delivery_degradation_processor
    named = p((501, 1_000_000, 10_000_000, 60, 200), None, {"dma_map": {"501": "New York City"}})
    assert named["label"] == "New York City"
    assert named["severity"] == "critical"  # 8 <= 80 * 0.25
    mild = p((501, 4_000_000, 10_000_000, 60, 200), None, {})  # 40 vs 80 → not <= 25%
    assert mild["severity"] == "warning"


# ── Phase-4 (Track B2): connection_type_mix ──────────────────────────────────


def test_connection_type_mix_happy_path():
    # row schema: [c_type, c_speed, w_cnt, b_cnt, w_total, b_total]
    out = defs.connection_type_mix_processor(("cellular", "mobile", 60, 5, 100, 500), None, {})
    assert out["label"] == "cellular / mobile"
    assert out["current_val"] == 60.0  # 60/100
    assert out["baseline_val"] == 1.0  # 5/500
    assert out["meta"]["filters"] == {"c_type": "cellular", "c_speed": "mobile"}


# ── Phase-4 (Track B2): pop_latency_regression ───────────────────────────────


def test_pop_latency_regression_happy_path_and_severity():
    # row schema: [pop, w_p95(ms), b_p95(ms), w_total, b_total]
    p = defs.pop_latency_regression_processor
    out = p(("JFK", 3000.0, 50.0, 60, 200), None, {})
    assert out["label"] == "JFK"
    assert out["current_val"] == 3000.0
    assert out["baseline_val"] == 50.0
    assert out["unit"] == "ms (P95)"
    assert out["meta"]["filters"] == {"pop": "JFK"}
    assert out["severity"] == "warning"  # < 5000
    assert p(("JFK", 5000.0, 50.0, 60, 200), None, {})["severity"] == "critical"


# ── Phase-4 (Track B2): http3_fallback ───────────────────────────────────────


def test_http3_fallback_happy_path_and_severity():
    # row schema: [w_quic, w_total, b_quic, b_total]
    p = defs.http3_fallback_processor
    out = p((0, 1000, 800, 1000), None, {})
    assert out["current_val"] == 0.0  # 0% QUIC in window
    assert out["baseline_val"] == 80.0  # 80% QUIC baseline
    assert out["unit"] == "% QUIC"
    assert out["severity"] == "critical"  # drop >= 40 pts
    mild = p((550, 1000, 700, 1000), None, {})  # 55% vs 70% → 15pt drop
    assert mild["severity"] == "warning"


# ── Phase-4 (Track B2): cache_hit_cliff ──────────────────────────────────────


def test_cache_hit_cliff_happy_path_and_severity():
    # row schema: [w_hits, w_cacheable, b_hits, b_cacheable]
    p = defs.cache_hit_cliff_processor
    out = p((10, 1000, 700, 1000), None, {})
    assert out["current_val"] == 1.0  # 10/1000
    assert out["baseline_val"] == 70.0  # 700/1000
    assert out["unit"] == "% HIT"
    assert out["severity"] == "critical"  # drop >= 30 pts
    mild = p((550, 1000, 700, 1000), None, {})  # 55 vs 70 → 15pt drop
    assert mild["severity"] == "warning"


# ── Phase-4 (Track C): payload_compression_regression ────────────────────────


def test_payload_compression_regression_happy_path_and_severity():
    # row schema: [url, w_rate, b_rate, w_total, b_total]  (rates = uncompressed share)
    p = defs.payload_compression_regression_processor
    out = p(("/app.js", 0.8, 0.02, 40, 200), None, {})
    assert out["label"] == "/app.js"
    assert out["current_val"] == 80.0
    assert out["baseline_val"] == 2.0
    assert out["unit"] == "% uncompressed"
    assert out["meta"]["filters"] == {"url": "/app.js"}
    assert out["severity"] == "warning"  # < 0.90
    assert p(("/app.js", 0.90, 0.0, 40, 200), None, {})["severity"] == "critical"


# ── Phase-4 (Track C): session_harvesting ────────────────────────────────────


def test_session_harvesting_happy_path_and_severity():
    # row schema: [ip, w_sessions, w_reqs, b_sessions]
    p = defs.session_harvesting_processor
    out = p(("203.0.113.5", 60, 200, 3), None, {})
    assert out["label"] == "203.0.113.5"
    assert out["current_val"] == 60
    assert out["baseline_val"] == 3
    assert out["unit"] == "distinct sessions"
    assert out["meta"]["filters"] == {"ip": "203.0.113.5"}
    assert out["severity"] == "warning"  # < 100
    assert p(("203.0.113.5", 100, 300, 3), None, {})["severity"] == "critical"


def test_session_harvesting_never_surfaces_a_session_id():
    """The cookie_session hash is only counted upstream; the card must expose a
    COUNT, never a session id, so no session token can leak even unmasked."""
    out = defs.session_harvesting_processor(("203.0.113.5", 60, 200, 3), None, {})
    blob = repr(out)
    assert "cookie_session" not in blob and "sess" not in out["label"]
    assert isinstance(out["current_val"], int)


@pytest.mark.security_regression
def test_session_harvesting_masks_ip_when_mask_ips():
    # row schema: [ip, w_sessions, w_reqs, b_sessions]
    row = ("203.0.113.42", 60, 200, 3)
    masked = defs.session_harvesting_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected


def test_session_harvesting_keeps_ip_for_admin():
    raw = defs.session_harvesting_processor(("203.0.113.42", 60, 200, 3), None, {})
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


# ── Phase-4 (Track C): timeout_split ─────────────────────────────────────────


def test_timeout_split_connect_phase_dominant():
    # row schema: [w_conn, b_conn, w_read, b_read, w_total, b_total]  (ms P95)
    out = defs.timeout_split_processor((500.0, 5.0, 60.0, 55.0, 100, 300), None, {})
    assert out["label"] == "Origin connect P95"
    assert out["current_val"] == 500.0
    assert out["baseline_val"] == 5.0
    assert out["meta"]["phase"] == "connect"
    assert out["severity"] == "critical"  # 500 >= 5*3


def test_timeout_split_read_phase_dominant():
    # read regressed, connect steady → read is the headline
    out = defs.timeout_split_processor((10.0, 8.0, 900.0, 100.0, 100, 300), None, {})
    assert out["label"] == "Origin read P95"
    assert out["meta"]["phase"] == "read"
    assert out["current_val"] == 900.0


# ── Phase-3: network_asn_health ──────────────────────────────────────────────


def test_network_asn_health_happy_path():
    """current_val / baseline_val are packet-loss percentages (ploss * 100),
    rounded to 2 dp; the ASN keys both the label and the filter."""
    # row schema: [asn, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans, w_total, b_total]
    row = (16509, 0.08, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    out = defs.network_asn_health_processor(row, None, {})
    assert out["label"] == "AS16509"
    assert out["current_val"] == 8.0  # round(0.08 * 100, 2)
    assert out["baseline_val"] == 0.1  # round(0.001 * 100, 2)
    assert out["unit"] == "% packet loss"
    assert out["meta"]["window_packet_loss_pct"] == 8.0
    assert out["meta"]["requests"] == 60
    assert out["meta"]["asn"] == 16509
    assert out["meta"]["filters"] == {"asn": 16509}


def test_network_asn_health_attaches_name_when_available():
    """``context['asn_names']`` appends the resolved ISP name to the label;
    without it, the label is just ``AS{n}``. NOT IP-keyed, so no masking."""
    p = defs.network_asn_health_processor
    row = (7922, 0.06, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    with_name = p(row, None, {"asn_names": {7922: "Example ISP"}})
    without = p(row, None, {})
    assert with_name["label"] == "AS7922 (Example ISP)"
    assert without["label"] == "AS7922"


def test_network_asn_health_critical_at_5_percent_packet_loss():
    """w_ploss flips warning → critical at 0.05 (5% packet loss)."""
    p = defs.network_asn_health_processor
    warn = (1, 0.049, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    crit = (1, 0.05, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    assert p(warn, None, {})["severity"] == "warning"
    assert p(crit, None, {})["severity"] == "critical"


# ── Phase-3: IP-keyed processors mask the client IP when the analyst sets ─────
# mask_ips. ANALYST = adversary: the raw IP must not survive in the label OR in
# meta.filters.ip (which seeds investigate_url). Mirrors the request_size_anomaly
# / connection_abuse mask contract above.


@pytest.mark.security_regression
def test_low_and_slow_masks_ip_when_mask_ips():
    # row schema: [ip, hits, distinct_paths, span_s, rps]
    row = ("203.0.113.42", 42, 6, 7200, 0.0058)
    masked = defs.low_and_slow_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected  # also feeds investigate_url


def test_low_and_slow_keeps_ip_for_admin():
    row = ("203.0.113.42", 42, 6, 7200, 0.0058)
    raw = defs.low_and_slow_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


@pytest.mark.security_regression
def test_credential_enumeration_masks_ip_when_mask_ips():
    # row schema: [ip, w_denied, w_attempts, w_paths, b_denied]
    row = ("203.0.113.42", 80, 100, 4, 2)
    masked = defs.credential_enumeration_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected  # also feeds investigate_url


def test_credential_enumeration_keeps_ip_for_admin():
    row = ("203.0.113.42", 80, 100, 4, 2)
    raw = defs.credential_enumeration_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


@pytest.mark.security_regression
def test_content_discovery_masks_ip_when_mask_ips():
    # row schema: [ip, w_404, w_total, distinct_404, b_404]
    row = ("203.0.113.42", 60, 75, 40, 3)
    masked = defs.content_discovery_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected  # also feeds investigate_url


def test_content_discovery_keeps_ip_for_admin():
    row = ("203.0.113.42", 60, 75, 40, 3)
    raw = defs.content_discovery_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"
