"""Golden characterization tests for all 49 registered InsightDefinitions.

Built BEFORE any performance optimization to guarantee zero behavior change
across all insight detections, coalesced pre-aggregations, and row processors.
"""

from __future__ import annotations

import json
import random
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

import backend.repositories.insights.definitions as SQL
from backend.core.log_fields import INSIGHT_DEFINITIONS, LOG_FIELD_CATALOG
from backend.core.share_db import mask_ip
from backend.models.analytics import InsightsRequest, InsightsResponse
from backend.repositories._base import _safe_table
from backend.repositories.insights import (
    InsightCategory,
    InsightDefinition,
    _insights_cache,
    get_cache_collapse_detail,
    get_insights,
    registry,
)
from backend.repositories.insights import (
    definitions as defs,
)
from backend.repositories.insights import (
    repository as insights_repo,
)
from backend.repositories.insights.repository import _coalesced_url_aggregates
from backend.repositories.security import summarize_bot_efficiency_rows
from backend.utils import bot_sources
from backend.utils.bot_sources import load_source, resolve_bot_display_name
from tests._helpers.duckdb_fixtures import generate_mock_logs, generate_mock_records, insert_mock_logs

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

    _add(b_ts_base, 12, url="/api/err-spike", status=200)
    _add(w_ts_base, 5, url="/api/err-spike", status=502)
    rows.append(_row(b_ts_base, ja4="ja4_botnet_1", ip="10.1.0.1"))
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ja4="ja4_botnet_1", ip=f"10.1.0.{i + 1}"))
    _add(w_ts_base, 4, country="IS", city="Reykjavik", region="1")
    _add(b_ts_base, 5, city="SurgeVille", region="TX", country="US")
    _add(w_ts_base, 25, city="SurgeVille", region="TX", country="US")
    _add(b_ts_base, 20, city="ErrTown", region="CA", country="US", status=200)
    _add(w_ts_base, 12, city="ErrTown", region="CA", country="US", status=500)
    _add(b_ts_base, 55, city="SlowBerg", region="NY", country="US", url="/city/slowberg", elapsed=100000)
    _add(w_ts_base, 12, city="SlowBerg", region="NY", country="US", url="/city/slowberg", elapsed=1500000)
    _add(w_ts_base, 6, city="FreshPolis", region="WA", country="US")
    _add(w_ts_base, 4, url="/wp-admin/setup.php", status=404)
    rows.append(_row(b_ts_base, waf_sig="SQLI-PROBE"))
    _add(w_ts_base, 5, waf_sig="SQLI-PROBE,BOT-ANALYSIS")
    rows.append(_row(b_ts_base, asn="64500", p_type="hosting"))
    rows.append(_row(b_ts_base, asn="64505", p_type=""))
    _add(w_ts_base, 120, asn="64505", p_type="hosting")
    _add(b_ts_base, 55, asn="64502", metro="501", country="US", tcp_rtt=20000)
    _add(w_ts_base, 22, asn="64502", metro="501", country="US", tcp_rtt=120000)
    _add(b_ts_base, 25, url="/static/bundle.js", cache="HIT")
    _add(w_ts_base, 10, url="/static/bundle.js", cache="MISS")
    _add(b_ts_base, 55, url="/account/overview", cache="HIT")
    _add(w_ts_base, 12, url="/account/overview", cache="PASS")
    _add(b_ts_base, 25, url="/api/slow-endpoint", elapsed=100000)
    _add(w_ts_base, 8, url="/api/slow-endpoint", elapsed=900000)
    _add(w_ts_base, 3, pop="JFK", lat=0.0, lon=0.0, tcp_rtt=1000, ja4="ja4_spoof_fp", ip="198.51.100.77", edge=True)
    _add(w_ts_base, 22, url="/api/tail-heavy", elapsed=10000)
    _add(w_ts_base + timedelta(milliseconds=30), 2, url="/api/tail-heavy", elapsed=200000)
    rows.append(_row(b_ts_base, tls_ciphers_sha="cipher_sha_bot_1", ip="10.2.0.1"))
    for i in range(6):
        rows.append(
            _row(w_ts_base + timedelta(milliseconds=i), tls_ciphers_sha="cipher_sha_bot_1", ip=f"10.2.0.{i + 1}")
        )
    _add(b_ts_base, 10, ip="10.3.0.1", req_header_bytes=500)
    _add(w_ts_base, 4, ip="10.3.0.1", req_header_bytes=12000)
    _add(b_ts_base, 10, ip="10.4.0.1", conn_requests=5)
    _add(w_ts_base, 6, ip="10.4.0.1", conn_requests=120)
    _add(b_ts_base, 105, server_region="US-East", url="/api/origin-timed", elapsed=100000, ottfb=20000, oconnect_ms=10)
    _add(w_ts_base, 55, server_region="US-East", url="/api/origin-timed", elapsed=600000, ottfb=450000, oconnect_ms=150)
    _add(w_ts_base, 12, url="/Promo/Banner", ttl=3600, hits=0, age=10)
    _add(
        w_ts_base,
        10,
        url="/media/hero-banner.png",
        status=200,
        resp_bytes=102400,
        ua="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile",
    )
    _add(b_ts_base, 40, oip="192.0.2.10", ost=200)
    _add(w_ts_base, 12, oip="192.0.2.10", ost=200)
    _add(w_ts_base, 12, oip="192.0.2.11", ost=200)
    _add(w_ts_base, 12, oip="192.0.2.99", ost=503)
    _add(w_ts_base, 6, url="/api/flaky-upstream", oretries=3)
    for i in range(6):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), rid=f"rid_b_{i}", pop="LAX", edge=True, ottfb=60000))
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), prid=f"rid_b_{i}", pop="IAD", edge=False, ottfb=20000))
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), rid=f"rid_w_{i}", pop="LAX", edge=True, ottfb=260000))
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), prid=f"rid_w_{i}", pop="IAD", edge=False, ottfb=20000))
    for i in range(15):
        rows.append(
            _row(
                w_ts_base + timedelta(seconds=i * 10),
                ip="10.5.0.1",
                ja4="ja4_beacon_1",
                ua="Mozilla/5.0 CustomPoller/1.0",
            )
        )
    for i, probe_url in enumerate(["/.env", "/.env", "/.env", "/.git/config", "/.git/config", "/wp-login.php"]):
        rows.append(_row(w_ts_base + timedelta(seconds=i * 150), ip="10.6.0.1", url=probe_url, status=404))
    _add(w_ts_base, 25, ip="10.7.0.1", url="/api/auth/login", status=401)
    _add(b_ts_base, 110, asn="64503", ploss=0.001, rtt_var=5000, retrans=0)
    _add(w_ts_base, 55, asn="64503", ploss=0.08, rtt_var=60000, retrans=2)
    for i in range(22):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ip="10.8.0.1",
                url="/scan/missing-0" if i >= 18 else f"/scan/missing-{i}",
                status=404,
            )
        )
    _add(w_ts_base, 22, asn="64504")
    _add(b_ts_base, 55, metro="803", delivery_rate=500000)
    _add(w_ts_base, 22, metro="803", delivery_rate=100000)
    _add(b_ts_base, 60, c_type="broadband", c_speed="fiber")
    _add(w_ts_base, 25, c_type="cellular", c_speed="5g")
    _add(b_ts_base, 55, pop="DEN", url="/pop/den", elapsed=50000)
    _add(w_ts_base, 22, pop="DEN", url="/pop/den", elapsed=500000)
    _add(b_ts_base, 60, transport="quic")
    _add(b_ts_base + timedelta(milliseconds=60), 60, transport="tcp")
    _add(w_ts_base, 12, transport="quic")
    _add(w_ts_base + timedelta(milliseconds=12), 108, transport="tcp")
    _add(b_ts_base, 55, url="/static/main.js", status=200, resp_bytes=4096, resp_header_content_encoding="br")
    _add(w_ts_base, 12, url="/static/main.js", status=200, resp_bytes=4096, resp_header_content_encoding="")
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ip="10.9.0.1", cookie_session=f"sess_harvest_{i}"))
    _add(b_ts_base, 10, ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0)")
    _add(w_ts_base, 105, ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0)")
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
        ins["id"]: {k: ins[k] for k in ("category", "title", "severity", "summary", "items")} for ins in res["insights"]
    }


def test_all_registered_insights_golden_characterization(in_memory_duckdb):
    in_memory_duckdb.execute("SET TimeZone = 'UTC'")
    src = {"name": "golden-svc", "service_id": "svc-golden"}
    table_name = _safe_table(src["name"])
    _insert_rows(in_memory_duckdb, table_name, _build_comprehensive_anomaly_rows())
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
            in_memory_duckdb, src, url="/static/bundle.js", window_hours=WINDOW_HOURS, baseline_hours=BASELINE_HOURS
        )

    registered_ids = [d.id for d in registry.get_all()]
    assert len(registered_ids) >= 45
    actual_admin = _extract_snapshot(res_admin)
    assert not [iid for iid in registered_ids if iid not in actual_admin]
    assert not [iid for iid in registered_ids if not actual_admin[iid].get("items")]
    for iid in registered_ids:
        assert actual_admin[iid] == expected["admin"][iid], f"Admin golden mismatch for {iid}"
    actual_analyst = _extract_snapshot(res_analyst)
    for iid in registered_ids:
        assert actual_analyst[iid] == expected["analyst_clamped_masked"][iid], f"Analyst golden mismatch for {iid}"
    assert {k: v for k, v in cc_detail.items() if k != "debug_queries"} == {
        k: v for k, v in expected["cache_collapse_detail"].items() if k != "debug_queries"
    }


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
    bot_tok, w_crawls, w_referrals, w_bytes, crawls_per_ref, _, bytes_per_ref, _, bw_share_pct = rows[0]
    assert bot_tok == "ccbot" and resolve_bot_display_name(bot_tok) == "Commoncrawl Crawler"
    assert (w_crawls, w_bytes, round(bw_share_pct, 1), w_referrals, crawls_per_ref, bytes_per_ref) == (
        100,
        5_000_000,
        50.0,
        0,
        100.0,
        5_000_000.0,
    )


def test_stale_browser_matching_preserves_exclusions_and_window(in_memory_duckdb: duckdb.DuckDBPyConnection) -> None:
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
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "ua" VARCHAR, '
        '"referer" VARCHAR, "url" VARCHAR, "ja4" VARCHAR, "asn" INTEGER, "p_type" VARCHAR, "resp_bytes" BIGINT)'
    )
    now = datetime.now(UTC)
    window_ts, baseline_ts, history_ts = (
        now - timedelta(minutes=10),
        now - timedelta(hours=3),
        now - timedelta(hours=24, minutes=30),
    )

    def _ins(ts, ip, ua, referer, url, ja4="", asn=0, p_type="", resp_bytes=1024):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} (timestamp, ip, ua, referer, url, ja4, asn, p_type, resp_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [ts.isoformat(), ip, ua, referer, url, ja4, asn, p_type, resp_bytes],
        )

    _ins(
        history_ts,
        "1.1.1.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/100.0.0.0 Safari/537.36",
        "https://google.com",
        "/index.html",
    )
    claude_ua = "Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +http://www.anthropic.com/claudebot)"
    for ts, n in [(baseline_ts, 10), (window_ts, 150)]:
        for i in range(n):
            _ins(ts, f"192.0.2.{i % 10}", claude_ua, "-", f"/item/{i}", resp_bytes=20000)
    for i in range(60):
        _ins(
            window_ts,
            f"198.18.0.{i % 5}",
            "CCBot/2.0 (https://commoncrawl.org/faq/)",
            "-",
            f"/archive/{i}",
            resp_bytes=50000,
        )
    chrome_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
    for ts, n in [(baseline_ts, 10), (window_ts, 600)]:
        for i in range(n):
            _ins(ts, f"198.51.100.{i % 15}", chrome_ua, "https://google.com", "/index.html")
    for ts, n in [(baseline_ts, 10), (window_ts, 60)]:
        for i in range(n):
            _ins(
                ts,
                "203.0.113.50",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "-",
                f"/catalog/category/item-{i % 12}",
            )
    for ts, n in [(baseline_ts, 5), (window_ts, 40)]:
        for i in range(n):
            _ins(
                ts,
                f"203.0.113.{i}",
                "Mozilla/5.0 (Windows NT 10.0; x64)",
                "https://google.com",
                "/search",
                "t13d1516h2_8e6120e8544e_3a4b5c6d7e8f",
                1000 + (i % 8),
                "residential",
            )

    _insights_cache.clear()
    by_id = {
        i["id"]: i
        for i in get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)["insights"]
    }
    assert "bot_bandwidth_disparity" not in by_id and "crawler_disparity" in by_id
    cd = by_id["crawler_disparity"]
    assert cd["title"] == "Bot & Crawler Referral & Bandwidth Disparity" and cd["category"] == "security"
    cd_labels = {item["label"]: item for item in cd["items"]}
    assert cd_labels["Anthropic Crawler Search"]["meta"]["crawls"] == 150
    assert cd_labels["Anthropic Crawler Search"]["meta"]["w_bytes"] == 3_000_000
    assert cd_labels["Anthropic Crawler Search"]["meta"]["bytes_per_referral"] == 3_000_000.0
    assert cd_labels["Anthropic Crawler Search"]["meta"]["bandwidth_share_pct"] > 0
    assert cd_labels["Commoncrawl Crawler"]["meta"]["crawls"] == 60
    assert cd_labels["Commoncrawl Crawler"]["meta"]["w_bytes"] == 3_000_000
    assert cd_labels["Commoncrawl Crawler"]["meta"]["referrals"] == 0
    assert by_id["stale_browser_version"]["items"][0]["label"] == "Chrome 100"
    assert by_id["orphaned_deep_crawl"]["items"][0]["label"] == "203.0.113.50"
    assert by_id["orphaned_deep_crawl"]["items"][0]["meta"]["requests"] == 60
    assert by_id["residential_fingerprint_dispersion"]["items"][0]["label"] == "t13d1516h2_8e6120e8544e_3a4b5c6d7e8f"
    assert by_id["residential_fingerprint_dispersion"]["items"][0]["meta"]["asns"] == 8


def test_resolve_bot_display_name_and_efficiency_without_runtime_cache(tmp_path, monkeypatch) -> None:
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
    assert by_name["Google Crawler"]["referrals"] == 2 and by_name["Commoncrawl Crawler"]["referrals"] == 0


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
    ts = (datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=70)).isoformat()
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


def _repeated_card(con, src, *, mask_ips: bool = False, card_id: str = "repeated_patterns") -> dict:
    _insights_cache.clear()
    res = get_insights(con, src, window_hours=1, baseline_hours=1, mask_ips=mask_ips)
    card = next((i for i in res["insights"] if i["id"] == card_id), None)
    assert card is not None and card.get("severity") != "error"
    return card


def test_detection_matrix_and_beacon_attributes(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _seed_repeated_patterns(in_memory_duckdb, table)

    card = _repeated_card(in_memory_duckdb, test_service_source)
    items = {item["label"]: item for item in card["items"]}
    assert "1.1.1.1" in items and "1.1.1.3" in items
    for excluded in ("2.2.2.2", "3.3.3.3", "4.4.4.4", "9.9.9.9"):
        assert excluded not in items

    steady = items["1.1.1.1"]
    assert steady["severity"] == "critical" and steady["current_val"] == 60.0 and card["severity"] == "critical"
    assert (
        steady["meta"]["n_events"] == 35
        and steady["meta"]["cv"] < 0.05
        and steady["meta"]["modal_frac"] >= 0.95
        and steady["meta"]["score"] == 100
    )

    rotating = items["1.1.1.3"]
    assert (
        rotating["meta"]["distinct_ua"] == 30
        and rotating["meta"]["n_events"] == 30
        and rotating["severity"] == "critical"
    )

    masked = _repeated_card(in_memory_duckdb, test_service_source, mask_ips=True)
    masked_item = next(i for i in masked["items"] if i["label"] == "1.1.1.xxx")
    assert masked_item["meta"]["filters"]["ip"] == "1.1.1.xxx"
    assert "1.1.1.1" not in {i["label"] for i in masked["items"]}


@pytest.mark.parametrize(
    ("ip", "ua", "gaps", "dups", "expect_found"),
    [
        ("1.1.1.2", "Mozilla/5.0 Jitter", [6, 6, 5, 6, 7, 6, 6, 5, 6, 7, 6, 6, 6, 6, 6], 1, "jitter"),
        ("1.1.1.5", "Mozilla/5.0 Fast", [2] * 14, 1, "fast"),
        ("1.1.1.6", "Mozilla/5.0 IrregularFast", [2, 4] * 7, 1, None),
        ("1.1.1.7", "Mozilla/5.0 Flood", [1] * 13, 3, None),
    ],
)
def test_repeated_patterns_gap_variants(in_memory_duckdb, test_service_source, ip, ua, gaps, dups, expect_found):
    table = _safe_table(test_service_source["name"])
    _create_repeated_patterns_table(in_memory_duckdb, table)
    _anchor(in_memory_duckdb, table)
    _seed_gaps(in_memory_duckdb, table, ip, ua, 60, gaps, dups_per_second=dups)
    card = _repeated_card(in_memory_duckdb, test_service_source)
    item = next((i for i in card["items"] if i["label"] == ip), None)
    if expect_found is None:
        assert item is None
    elif expect_found == "jitter":
        assert item is not None
        m = item["meta"]
        assert 0 < m["cv"] < (m["stddev_s"] / m["mean_interval_s"]) and m["cv"] < 0.3
    elif expect_found == "fast":
        assert item is not None and 1 <= item["meta"]["mean_interval_s"] < 5 and item["meta"]["modal_frac"] >= 0.85


def test_repeated_patterns_processors_and_metadata():
    p = defs.repeated_patterns_processor
    assert p(("1.1.1.1", 29, 30, 30.0, 0.0, 0.0, 1.0, 30, 870, 30), None, {})["severity"] == "critical"
    assert p(("1.1.1.1", 19, 20, 60.0, 0.0, 0.0, 1.0, 1, 1140, 60), None, {})["severity"] == "warning"
    info_out = p(("5.5.5.5", 40, 50, 3.0, 2.0, 1.0, 0.5, 5, 150, 3), None, {})
    assert info_out["meta"]["score"] < 70 and info_out["severity"] == "info"
    above = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.1, 1.0, 1, 2400, 60), None, {})
    below = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.2, 1.0, 1, 2400, 60), None, {})
    assert above["meta"]["score"] == 94 and above["severity"] == "critical"
    assert below["meta"]["score"] == 88 and below["severity"] == "warning"

    masked = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx" and masked["meta"]["filters"]["ip"] == "203.0.113.xxx"
    raw = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {})
    assert raw["label"] == "203.0.113.42" and raw["meta"]["filters"]["ip"] == "203.0.113.42"
    none_mode = p(("1.1.1.1", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, None), None, {})
    assert none_mode["meta"]["rps"] == round(35 / 2040, 4) and none_mode["meta"]["mode_gap_s"] is None

    assert defs.repeated_patterns_severity([]) == "clean"
    assert defs.repeated_patterns_severity([{"severity": "warning"}]) == "warning"
    assert defs.repeated_patterns_severity([{"severity": "info"}, {"severity": "critical"}]) == "critical"
    assert "?" not in SQL.REPEATED_BOT_UA_REGEX
    assert (
        SQL.REPEATED_PATTERNS.format(table_name="t", ua_col='"ua"', bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX).count("?")
        == 1
    )
    assert (
        SQL.REPEATED_PATTERNS_FP.format(
            table_name="t", fp_col="ja4", ua_col='"ua"', bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX
        ).count("?")
        == 1
    )
    assert "MODE(gap) AS mode_gap" in SQL.REPEATED_PATTERNS

    for iid, title in [
        ("repeated_patterns", "Scripted Traffic Patterns"),
        ("repeated_patterns_fp", "Scripted Traffic Patterns (by TLS Fingerprint)"),
    ]:
        d = registry.get(iid)
        assert d is not None and d.title == title and d.required_fields == ["ip", "timestamp"]
        assert d.row_processor is not None and d.severity_logic is not None
        entry = next((e for e in INSIGHT_DEFINITIONS if e["id"] == iid), None)
        assert entry is not None and entry["required_fields"] == ["ip"] and entry["required_groups"] == []

    p_fp = defs.repeated_patterns_fp_processor
    crit_fp = p_fp(("aaaa", 29, 30, 60.0, 0.0, 0.0, 1.0, 10, 1740, 60), None, {})
    assert (
        crit_fp["severity"] == "critical" and crit_fp["meta"]["score"] == 100 and crit_fp["meta"]["distinct_ip"] == 10
    )
    assert p_fp(("aaaa", 19, 20, 60.0, 0.0, 0.0, 1.0, 5, 1140, 60), None, {})["severity"] == "warning"
    assert p_fp(("xyz", 29, 30, 60.0, 0.0, 0.0, 1.0, 10, 1740, 60), None, {"fp_col": "ja3"})["meta"]["filters"] == {
        "ja3": "xyz"
    }


def test_fp_detection_matrix(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "ua" VARCHAR, "ja4" VARCHAR)'
    )
    now = datetime.now(UTC).replace(microsecond=0)

    def ins(offset_s: int, ip: str, ja4: str, ua: str) -> None:
        ts = (now - timedelta(seconds=offset_s)).isoformat()
        in_memory_duckdb.execute(
            f'INSERT INTO {table} ("timestamp", "ip", "ua", "ja4") VALUES (?, ?, ?, ?)', [ts, ip, ua, ja4]
        )

    ins(70 * 60, "9.9.9.9", "anchor_fp", "anchor")
    for i in range(35):
        ins(60 + i * 60, f"10.0.0.{i + 1}", "aaaa", "Mozilla/5.0 Scraper")
    cum = 60
    for gp in [3, 47, 5, 2, 88, 14, 6, 120, 9, 4, 60, 33, 7, 200, 11, 5, 75, 19, 2]:
        cum += gp
        ins(cum, f"10.1.0.{gp}", "bbbb", "Mozilla/5.0 Human")
    for i in range(8):
        ins(60 + i * 60, f"10.2.0.{i}", "cccc", "Mozilla/5.0 Small")
    for i in range(30):
        ins(60 + i * 30, f"10.3.0.{i}", "dddd", "Googlebot/2.1 (+http://www.google.com/bot.html)")

    card = _repeated_card(in_memory_duckdb, test_service_source, card_id="repeated_patterns_fp")
    items = {item["label"]: item for item in card["items"]}
    assert "aaaa" in items and all(fp not in items for fp in ("bbbb", "cccc", "dddd"))
    item = items["aaaa"]
    assert item["severity"] == "critical" and item["meta"]["distinct_ip"] >= 30 and item["meta"]["score"] == 100
    assert item["meta"]["filters"] == {"ja4": "aaaa"}


# ── Merged from tests/repositories/test_insights.py ──


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
    window_ts, baseline_ts, old_filler = (
        now - timedelta(minutes=30),
        now - timedelta(hours=12),
        now - timedelta(hours=24, minutes=30),
    )

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
    window_ts, baseline_ts, old_filler = (
        now - timedelta(minutes=30),
        now - timedelta(hours=12),
        now - timedelta(hours=24, minutes=30),
    )

    def _ins(ts, url, status, rb, ce, ip, cs, oc, ottfb):
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "url", "status", "resp_bytes", '
            '"resp_header_content_encoding", "ip", "cookie_session", "oconnect_ms", "ottfb") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
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
        (i["label"], round(float(i.get("current_val") or 0), 4), round(float(i.get("baseline_val") or 0), 4))
        for i in items
    ]


def test_get_insights_basic_cache_and_models(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    _create_full_catalog_table(in_memory_duckdb, table_name)
    insert_mock_logs(in_memory_duckdb, table_name, generate_mock_logs(test_service_source, num_logs=50, hours_ago=1))
    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    assert "insights" in res and "window_start" in res and isinstance(res["insights"], list)
    res2 = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    assert res2["is_cached"] is True and res2["insights"] == res["insights"]
    assert isinstance(InsightsResponse(**res).insights, list)
    req = InsightsRequest(window_size_hrs=2.0, baseline_hours=48.0)
    assert req.window_size_hrs == 2.0 and req.baseline_hours == 48.0


def test_get_insights_custom_definition_and_error_handling(in_memory_duckdb, test_service_source, monkeypatch):
    table_name = _safe_table(test_service_source["name"])
    _create_full_catalog_table(in_memory_duckdb, table_name)
    now = datetime.now(UTC)
    in_memory_duckdb.executemany(
        f'INSERT INTO {table_name} ("timestamp", "url") VALUES (?, ?)',
        [
            ((now - timedelta(hours=24, minutes=30)).isoformat(), "/baseline"),
            ((now - timedelta(minutes=10)).isoformat(), "/custom"),
        ],
    )

    def _proc(row: Any, definition: Any, context: Any) -> dict[str, Any]:
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
    by_id = {
        i["id"]: i
        for i in get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)["insights"]
    }
    assert by_id["test_custom_ok"]["items"][0]["label"] == "/custom" and by_id["test_custom_bad"]["items"] == []


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
    window_ts, baseline_ts = now - timedelta(minutes=30), now - timedelta(hours=24, minutes=30)

    def _ins(ts: datetime, city: str, region: str, status: int, elapsed: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "city", "region", "country", "status", "elapsed") VALUES (?, ?, ?, \'US\', ?, ?)',
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


def _seed_url_data(con, table_name: str) -> None:
    now = datetime.now(UTC)
    window_ts, baseline_ts = now - timedelta(minutes=30), now - timedelta(hours=24, minutes=30)

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


@pytest.mark.parametrize(
    ("cols_sql", "seeder", "patch_attr", "expected_ids"),
    [
        (
            '"timestamp" TIMESTAMPTZ, "city" VARCHAR, "region" VARCHAR, "country" VARCHAR, "status" INTEGER, "elapsed" INTEGER',
            _seed_city_data,
            "_coalesced_city_aggregates",
            {"city_surges", "city_error_spikes", "city_latency_regressions", "new_city_traffic"},
        ),
        (
            '"timestamp" TIMESTAMPTZ, "url" VARCHAR, "status" INTEGER, "cache" VARCHAR, "elapsed" INTEGER',
            _seed_url_data,
            "_coalesced_url_aggregates",
            {"error_spikes", "cache_collapse", "cacheability_regression", "latency_regression", "tail_latency"},
        ),
        (
            '"timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER',
            _seed_ip_security_data,
            "_coalesced_ip_security_aggregates",
            {"low_and_slow", "credential_enumeration", "content_discovery"},
        ),
    ],
)
def test_coalesced_paths_match_per_insight_scan_output(
    in_memory_duckdb, test_service_source, monkeypatch, cols_sql, seeder, patch_attr, expected_ids
):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql})")
    seeder(in_memory_duckdb, table_name)

    _insights_cache.clear()
    fast = {
        i["id"]: i
        for i in get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)["insights"]
        if i["id"] in expected_ids
    }

    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, patch_attr, lambda *a, **k: {})
    slow = {
        i["id"]: i
        for i in get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)["insights"]
        if i["id"] in expected_ids
    }

    assert set(fast.keys()) == expected_ids and set(slow.keys()) == expected_ids
    for iid in expected_ids:
        assert _norm_items(fast[iid]["items"]) == _norm_items(slow[iid]["items"])


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


@pytest.mark.security_regression
def test_coalesced_ip_security_masks_ip_end_to_end_for_analyst(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER)'
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


def test_registry_and_catalog_contracts():
    valid = {c.value for c in InsightCategory}
    all_defs = registry.get_all()
    for d in all_defs:
        assert str(d.category) in valid, f"{d.id} has bad category {d.category!r}"
        assert len(d.required_fields) == len(set(d.required_fields)), f"{d.id} has duplicate required_fields"

    reg = {d.id: str(d.category) for d in all_defs}
    legacy = {d["id"]: d.get("category") for d in INSIGHT_DEFINITIONS}
    assert not sorted(set(reg) - set(legacy))
    assert not sorted(i for i, c in legacy.items() if not c)
    assert not {i: (reg[i], legacy[i]) for i in reg if i in legacy and reg[i] != legacy[i]}
    assert not (_EXPECTED_INSIGHT_IDS - set(reg))

    definition = registry.get("error_spikes")
    assert definition is not None and definition.title == "Error Spikes"
    assert "url" in definition.required_fields and "status" in definition.required_fields
    assert "{table_name}" in definition.sql_template and definition.row_processor is not None
    item = definition.row_processor(("/test", 0.1, 0.02, 10, 100, 500), definition, {})
    assert item["label"] == "/test" and item["current_val"] == 10.0 and item["severity"] == "warning"
    assert definition.row_processor(("/crit", 0.6, 0.02, 60, 100, 500), definition, {})["severity"] == "critical"


# ── Row processor severity boundary & contract matrix ───────────────────────


@pytest.mark.parametrize(
    ("proc", "low_row", "high_row", "ctx", "low_sev", "high_sev"),
    [
        (
            defs.error_spikes_processor,
            ("/x", 0.49, 0.02, 49, 100, 500),
            ("/x", 0.5, 0.02, 50, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.botnet_grouping_processor,
            ("ja3fp", 49, 200, 5, 9.8),
            ("ja3fp", 50, 200, 5, 10.0),
            {},
            "warning",
            "critical",
        ),
        (defs.new_country_traffic_processor, ("ZW", 9, 0), ("ZW", 10, 0), {}, "info", "warning"),
        (
            defs.city_surges_processor,
            ("x", "x", "", "X", 100, 10, 9.5),
            ("x", "x", "", "X", 100, 10, 10.0),
            {"baseline_hours": 1, "window_hours": 1},
            "info",
            "warning",
        ),
        (
            defs.city_error_spikes_processor,
            ("L", "L", "", "X", 0.49, 0.02, 49, 100, 500),
            ("L", "L", "", "X", 0.5, 0.02, 50, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.city_latency_processor,
            ("L", "L", "", "X", 4999.0, 1000.0, 100, 500),
            ("L", "L", "", "X", 5000.0, 1000.0, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.new_city_traffic_processor,
            ("L", "L", "", "X", 49, 0),
            ("L", "L", "", "X", 50, 0),
            {},
            "info",
            "warning",
        ),
        (
            defs.ua_monoculture_processor,
            ("bot/1.0", 49, 0, 100, 100),
            ("bot/1.0", 50, 0, 100, 100),
            {},
            "warning",
            "critical",
        ),
        (defs.new_probe_urls_processor, ("/.env", 4, 0, 100.0), ("/.env", 5, 0, 100.0), {}, "warning", "critical"),
        (
            defs.waf_signal_spikes_processor,
            ("S", 5, 1, 9.9),
            ("S", 5, 1, 10.0),
            {"baseline_hours": 1, "window_hours": 1},
            "warning",
            "critical",
        ),
        (defs.asn_concentration_processor, (1, 49, 5, 100, 100), (1, 50, 5, 100, 100), {}, "warning", "critical"),
        (
            defs.asn_metro_performance_processor,
            (1, "M", 99.0, 0.0, 100, 500),
            (1, "M", 100.0, 0.0, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.cache_collapse_processor,
            ("/img", 0.11, 0.65, 100, 500),
            ("/img", 0.05, 0.65, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.cacheability_regression_processor,
            ("/api", 0.60, 0.15, 200, 800),
            ("/api", 0.95, 0.02, 200, 800),
            {},
            "warning",
            "critical",
        ),
        (
            defs.latency_regression_processor,
            ("/x", 4999.0, 1000.0, 100, 500),
            ("/x", 5000.0, 1000.0, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.impossible_distance_processor,
            ("ja3", 5, 5000.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC"),
            ("ja3", 5, 5001.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC"),
            {},
            "warning",
            "critical",
        ),
        (
            defs.tail_latency_processor,
            ("/x", 5000, 500, 10.0, 100),
            ("/x", 5000, 500, 10.1, 100),
            {},
            "warning",
            "critical",
        ),
        (
            defs.cipher_spread_processor,
            ("abc", 49, 100, 5),
            ("abc", 50, 100, 5),
            {"baseline_hours": 1, "window_hours": 1},
            "warning",
            "critical",
        ),
        (
            defs.request_size_anomaly_processor,
            ("1.2.3.4", 64000, 30000, 5, 1000),
            ("1.2.3.4", 64001, 30000, 5, 1000),
            {},
            "warning",
            "critical",
        ),
        (
            defs.connection_abuse_processor,
            ("1.2.3.4", 500, 100, 50, 100),
            ("1.2.3.4", 501, 100, 50, 100),
            {},
            "warning",
            "critical",
        ),
        (
            defs.region_latency_processor,
            ("r", 4999.0, 1000.0, 100, 500, None),
            ("r", 5000.0, 1000.0, 100, 500, None),
            {},
            "warning",
            "critical",
        ),
        (
            defs.origin_latency_spike_processor,
            ("/x", 50.0, 90.0, 10.0, 100),
            ("/x", 100.0, 90.0, 10.0, 100),
            {},
            "warning",
            "critical",
        ),
        (
            defs.origin_error_rate_processor,
            (503, 5, 100, 100, 5, 1),
            (503, 10, 100, 100, 6, 1),
            {},
            "warning",
            "critical",
        ),
        (
            defs.origin_ip_failure_processor,
            ("10.0.0.1", 100, 20.0, 5.0),
            ("10.0.0.1", 100, 21.0, 5.0),
            {},
            "warning",
            "critical",
        ),
        (
            defs.shield_path_degradation_processor,
            ("LAX", "SFO", 299.0, 100.0, 10),
            ("LAX", "SFO", 300.0, 100.0, 10),
            {},
            "warning",
            "critical",
        ),
        (
            defs.low_and_slow_processor,
            ("1.2.3.4", 20, 9, 7200, 0.001),
            ("1.2.3.4", 20, 10, 7200, 0.001),
            {},
            "warning",
            "critical",
        ),
        (
            defs.credential_enumeration_processor,
            ("1.2.3.4", 99, 120, 3, 0),
            ("1.2.3.4", 100, 120, 3, 0),
            {},
            "warning",
            "critical",
        ),
        (
            defs.content_discovery_processor,
            ("1.2.3.4", 99, 120, 40, 0),
            ("1.2.3.4", 100, 120, 40, 0),
            {},
            "warning",
            "critical",
        ),
        (defs.referer_monoculture_processor, ("r", 49, 0, 100, 100), ("r", 50, 0, 100, 100), {}, "warning", "critical"),
        (defs.method_drift_processor, ("POST", 24, 0, 100, 100), ("POST", 25, 0, 100, 100), {}, "warning", "critical"),
        (defs.new_asn_traffic_processor, (7922, 99, 0), (7922, 150, 0), {}, "info", "warning"),
        (defs.asn_hosting_shift_processor, (1, 599, 1000, 0, 100), (1, 60, 100, 0, 100), {}, "warning", "critical"),
        (
            defs.metro_delivery_degradation_processor,
            (501, 4_000_000, 10_000_000, 60, 200),
            (501, 1_000_000, 10_000_000, 60, 200),
            {},
            "warning",
            "critical",
        ),
        (
            defs.pop_latency_regression_processor,
            ("JFK", 3000.0, 50.0, 60, 200),
            ("JFK", 5000.0, 50.0, 60, 200),
            {},
            "warning",
            "critical",
        ),
        (defs.http3_fallback_processor, (550, 1000, 700, 1000), (0, 1000, 800, 1000), {}, "warning", "critical"),
        (defs.cache_hit_cliff_processor, (550, 1000, 700, 1000), (10, 1000, 700, 1000), {}, "warning", "critical"),
        (
            defs.payload_compression_regression_processor,
            ("/app.js", 0.8, 0.02, 40, 200),
            ("/app.js", 0.90, 0.0, 40, 200),
            {},
            "warning",
            "critical",
        ),
        (
            defs.session_harvesting_processor,
            ("203.0.113.5", 60, 200, 3),
            ("203.0.113.5", 100, 300, 3),
            {},
            "warning",
            "critical",
        ),
        (
            defs.network_asn_health_processor,
            (1, 0.049, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120),
            (1, 0.05, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120),
            {},
            "warning",
            "critical",
        ),
    ],
)
def test_processor_severity_thresholds(proc, low_row, high_row, ctx, low_sev, high_sev):
    assert proc(low_row, None, ctx)["severity"] == low_sev
    assert proc(high_row, None, ctx)["severity"] == high_sev


def test_processor_labels_fallbacks_and_edge_cases():
    assert defs.error_spikes_processor((None, 0.1, 0.0, 5, 50, 100), None, {})["label"] == "(empty)"
    assert defs.ua_monoculture_processor((None, 10, 0, 100, 100), None, {})["label"] == "(empty)"
    assert defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja4"})["meta"]["filters"] == {
        "ja4": "fp123"
    }
    assert defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja3"})["meta"]["filters"] == {
        "ja3": "fp123"
    }
    assert defs.new_country_traffic_processor(("XX", 50, 0), None, {})["baseline_val"] == 0
    assert (
        defs.city_surges_processor(
            ("LA, US", "LA", "CA", "US", 100, 240, 4.2), None, {"baseline_hours": 24, "window_hours": 1}
        )["baseline_val"]
        == 10.0
    )
    assert (
        defs.city_error_spikes_processor(("L", "L", "", "US", 0.3, 0.1, 30, 100, 500), None, {})["meta"]["filters"][
            "region"
        ]
        is None
    )
    assert (
        defs.city_latency_processor(("L", "L", "", "X", 100.0, 0.0, 100, 500), None, {})["meta"]["regression_ratio"]
        == 0
    )
    assert defs.new_probe_urls_processor(("/x", 1, 0, None), None, {})["meta"]["error_pct"] == 0
    assert r"\.env" in SQL.NEW_PROBE_REGEX and r"\.git" in SQL.NEW_PROBE_REGEX and "(?i)" not in SQL.NEW_PROBE_REGEX
    re.compile(SQL.NEW_PROBE_REGEX)
    probe_sql = next(d.sql_template for d in defs.registry.get_all() if d.id == "new_probe_urls")
    assert "regexp_matches" in probe_sql and "'i'" in probe_sql

    assert (
        defs.waf_signal_spikes_processor(("SQLI", 50, 240, 12.0), None, {"baseline_hours": 24, "window_hours": 1})[
            "baseline_val"
        ]
        == 10.0
    )
    ps = defs.proxy_surge_processor(("VPN", 250, 50, 1000, 800), None, {})
    assert ps["current_val"] == 25.0 and ps["severity"] == "warning"
    assert defs.proxy_surge_processor(("VPN", 5, 0, 0, 0), None, {})["current_val"] == 500.0
    assert defs.proxy_surge_severity([]) == "clean" and defs.proxy_surge_severity([{"label": "VPN"}]) == "warning"

    assert (
        defs.asn_concentration_processor((16509, 50, 5, 100, 100), None, {"asn_names": {16509: "AMAZON-02"}})["label"]
        == "AS16509 (AMAZON-02)"
    )
    assert defs.asn_concentration_processor((16509, 50, 5, 100, 100), None, {})["label"] == "AS16509"
    assert (
        defs.asn_metro_performance_processor(
            (7922, "803", 150.0, 30.0, 100, 500),
            None,
            {"dma_map": {"803": "Los Angeles, CA"}, "asn_names": {7922: "COMCAST"}},
        )["label"]
        == "AS7922 (COMCAST) in Los Angeles, CA"
    )
    unk_dma = defs.asn_metro_performance_processor((7922, "999", 150.0, 30.0, 100, 500), None, {})["label"]
    assert "DMA 999" in unk_dma and "AS7922" in unk_dma

    cr = defs.cacheability_regression_processor(("/api", 0.95, 0.02, 200, 800), None, {})
    assert (cr["label"], cr["unit"], cr["current_val"], cr["baseline_val"], cr["meta"]["filters"]) == (
        "/api",
        "% PASS",
        95.0,
        2.0,
        {"url": "/api"},
    )
    assert defs.latency_regression_processor(("/x", 100.0, 0.0, 100, 500), None, {})["meta"]["regression_ratio"] == 0
    assert (
        "ja3"
        in defs.impossible_distance_processor(
            ("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"), None, {}
        )["meta"]["filters"]
    )
    assert (
        "ja4"
        in defs.impossible_distance_processor(
            ("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"), None, {"fp_col": "ja4"}
        )["meta"]["filters"]
    )

    cs_ctx = {"baseline_hours": 1, "window_hours": 1}
    assert (
        defs.cipher_spread_processor(("abcdef1234567890abcdef", 10, 100, 5), None, cs_ctx)["label"] == "abcdef123456…"
    )
    assert defs.cipher_spread_processor(("shorthash", 10, 100, 5), None, cs_ctx)["label"] == "shorthash"
    assert defs.cipher_spread_processor(("", 10, 100, 5), None, cs_ctx)["label"] == "(unknown)"
    rsa_none = defs.request_size_anomaly_processor((None, None, None, 3, None), None, {})
    assert rsa_none["current_val"] == 0 and rsa_none["baseline_val"] == 0

    assert (
        defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, 30.0), None, {})["meta"]["ottfb_p95_ms"]
        == 30.0
    )
    assert (
        "ottfb_p95_ms"
        not in defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, None), None, {})["meta"]
    )
    ctm = defs.cache_ttl_mismatch_processor((123, 3600, 1.5, 60, 100), None, {})
    assert ctm["label"] == "123" and ctm["severity"] == "warning"

    assert (
        defs.image_optimization_processor(("/x.jpg", 100, 10 * 1024 * 1024 + 1, 500.0, 0.3), None, {})["severity"]
        == "critical"
    )
    assert (
        defs.image_optimization_processor(("/x.jpg", 5, 1024 * 1024 * 5, 1025.0, 0.3), None, {})["severity"]
        == "critical"
    )
    assert (
        defs.image_optimization_processor(("/x.jpg", 50, 3 * 1024 * 1024, 60.0, 0.6), None, {})["severity"] == "warning"
    )
    assert defs.image_optimization_processor(("/x.jpg", 5, 600 * 1024, 100.0, 0.1), None, {})["severity"] == "info"
    assert defs.image_optimization_processor(("/x", 1, 600 * 1024, 100.0, None), None, {})["meta"]["mobile_ratio"] == 0

    assert defs.origin_error_rate_processor((503, 10, 100, 100, 6, 1), None, {})["label"] == "HTTP 503"
    assert defs.origin_retries_processor(("/x", 100, 2.5, 5), None, {})["severity"] == "warning"
    assert defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 0.0, 10), None, {})["meta"]["ratio"] == 0
    assert defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 50.0, 10), None, {})["label"] == "LAX → SFO"


def test_phase3_and_phase4_processors_happy_paths():
    ls = defs.low_and_slow_processor(("198.51.100.7", 42, 6, 7200, 0.0058), None, {})
    assert (ls["label"], ls["current_val"], ls["baseline_val"], ls["unit"]) == (
        "198.51.100.7",
        6,
        None,
        "sensitive paths",
    )
    assert ls["meta"] == {
        "hits": 42,
        "distinct_paths": 6,
        "span_s": 7200,
        "rps": 0.0058,
        "filters": {"ip": "198.51.100.7"},
    }
    ls_none = defs.low_and_slow_processor(("1.2.3.4", 5, 3, None, None), None, {})
    assert ls_none["meta"]["span_s"] == 0 and ls_none["meta"]["rps"] == 0.0

    ce = defs.credential_enumeration_processor(("203.0.113.9", 80, 100, 4, 2), None, {})
    assert (ce["label"], ce["current_val"], ce["baseline_val"], ce["unit"]) == (
        "203.0.113.9",
        80,
        2,
        "denied (401/403)",
    )
    assert ce["meta"] == {
        "attempts": 100,
        "denied": 80,
        "fail_rate_pct": 80.0,
        "distinct_paths": 4,
        "filters": {"ip": "203.0.113.9"},
    }
    assert defs.credential_enumeration_processor(("1.2.3.4", 0, 0, 0, 0), None, {})["meta"]["fail_rate_pct"] == 0.0

    cd = defs.content_discovery_processor(("198.51.100.7", 60, 75, 40, 3), None, {})
    assert (cd["label"], cd["current_val"], cd["baseline_val"], cd["unit"]) == ("198.51.100.7", 60, 3, "404s")
    assert cd["meta"] == {
        "requests": 75,
        "not_found": 60,
        "not_found_rate_pct": 80.0,
        "distinct_404_urls": 40,
        "filters": {"ip": "198.51.100.7"},
    }
    assert defs.content_discovery_processor(("1.2.3.4", 0, 0, 0, 0), None, {})["meta"]["not_found_rate_pct"] == 0.0

    rm = defs.referer_monoculture_processor(("http://spam.example", 40, 5, 500, 100), None, {})
    assert (rm["label"], rm["current_val"], rm["baseline_val"], rm["unit"]) == (
        "http://spam.example",
        40.0,
        1.0,
        "% of traffic",
    )
    assert rm["meta"]["requests"] == 40 and rm["meta"]["filters"] == {"referer": "http://spam.example"}
    rm_empty = defs.referer_monoculture_processor((None, 0, 0, 0, 0), None, {})
    assert rm_empty["label"] == "(empty)" and rm_empty["current_val"] == 0.0

    md = defs.method_drift_processor(("POST", 30, 2, 100, 200), None, {})
    assert (md["label"], md["current_val"], md["baseline_val"], md["unit"], md["meta"]["filters"]) == (
        "POST",
        30.0,
        1.0,
        "% of traffic",
        {"method": "POST"},
    )

    na = defs.new_asn_traffic_processor((16509, 60, 0), None, {})
    assert (
        na["label"],
        na["current_val"],
        na["baseline_val"],
        na["unit"],
        na["meta"]["asn"],
        na["meta"]["filters"],
    ) == ("AS16509", 60, 0, "requests", 16509, {"asn": 16509})
    assert (
        defs.new_asn_traffic_processor((7922, 150, 0), None, {"asn_names": {7922: "Example ISP"}})["label"]
        == "AS7922 (Example ISP)"
    )

    ahs = defs.asn_hosting_shift_processor((16509, 45, 100, 5, 200), None, {})
    assert (ahs["label"], ahs["current_val"], ahs["baseline_val"], ahs["unit"]) == ("AS16509", 45.0, 2.5, "% hosting")
    assert (ahs["meta"]["asn"], ahs["meta"]["filters"], ahs["meta"]["window_hosting"], ahs["meta"]["window_total"]) == (
        16509,
        {"asn": 16509},
        45,
        100,
    )
    assert (
        defs.asn_hosting_shift_processor((7922, 30, 60, 2, 100), None, {"asn_names": {7922: "Comcast"}})["label"]
        == "AS7922 (Comcast)"
    )
    ahs_zero = defs.asn_hosting_shift_processor((1, 0, 0, 0, 0), None, {})
    assert ahs_zero["current_val"] == 0.0 and ahs_zero["baseline_val"] == 0.0

    mdd = defs.metro_delivery_degradation_processor((501, 1_000_000, 10_000_000, 60, 200), None, {})
    assert (mdd["label"], mdd["current_val"], mdd["baseline_val"], mdd["unit"], mdd["meta"]["filters"]) == (
        "DMA 501",
        8.0,
        80.0,
        "Mbps (median)",
        {"metro": 501},
    )
    assert (
        defs.metro_delivery_degradation_processor(
            (501, 1_000_000, 10_000_000, 60, 200), None, {"dma_map": {"501": "New York City"}}
        )["label"]
        == "New York City"
    )

    ct_mix = defs.connection_type_mix_processor(("cellular", "mobile", 60, 5, 100, 500), None, {})
    assert (ct_mix["label"], ct_mix["current_val"], ct_mix["baseline_val"], ct_mix["meta"]["filters"]) == (
        "cellular / mobile",
        60.0,
        1.0,
        {"c_type": "cellular", "c_speed": "mobile"},
    )

    plr = defs.pop_latency_regression_processor(("JFK", 3000.0, 50.0, 60, 200), None, {})
    assert (plr["label"], plr["current_val"], plr["baseline_val"], plr["unit"], plr["meta"]["filters"]) == (
        "JFK",
        3000.0,
        50.0,
        "ms (P95)",
        {"pop": "JFK"},
    )

    h3 = defs.http3_fallback_processor((0, 1000, 800, 1000), None, {})
    assert (h3["current_val"], h3["baseline_val"], h3["unit"]) == (0.0, 80.0, "% QUIC")

    chc = defs.cache_hit_cliff_processor((10, 1000, 700, 1000), None, {})
    assert (chc["current_val"], chc["baseline_val"], chc["unit"]) == (1.0, 70.0, "% HIT")

    pcr = defs.payload_compression_regression_processor(("/app.js", 0.8, 0.02, 40, 200), None, {})
    assert (pcr["label"], pcr["current_val"], pcr["baseline_val"], pcr["unit"], pcr["meta"]["filters"]) == (
        "/app.js",
        80.0,
        2.0,
        "% uncompressed",
        {"url": "/app.js"},
    )

    sh = defs.session_harvesting_processor(("203.0.113.5", 60, 200, 3), None, {})
    assert (sh["label"], sh["current_val"], sh["baseline_val"], sh["unit"], sh["meta"]["filters"]) == (
        "203.0.113.5",
        60,
        3,
        "distinct sessions",
        {"ip": "203.0.113.5"},
    )
    assert "cookie_session" not in repr(sh) and "sess" not in sh["label"] and isinstance(sh["current_val"], int)

    ts_conn = defs.timeout_split_processor((500.0, 5.0, 60.0, 55.0, 100, 300), None, {})
    assert (
        ts_conn["label"],
        ts_conn["current_val"],
        ts_conn["baseline_val"],
        ts_conn["meta"]["phase"],
        ts_conn["severity"],
    ) == ("Origin connect P95", 500.0, 5.0, "connect", "critical")
    ts_read = defs.timeout_split_processor((10.0, 8.0, 900.0, 100.0, 100, 300), None, {})
    assert (ts_read["label"], ts_read["meta"]["phase"], ts_read["current_val"]) == ("Origin read P95", "read", 900.0)

    nah = defs.network_asn_health_processor((16509, 0.08, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120), None, {})
    assert (nah["label"], nah["current_val"], nah["baseline_val"], nah["unit"]) == (
        "AS16509",
        8.0,
        0.1,
        "% packet loss",
    )
    assert (
        nah["meta"]["window_packet_loss_pct"],
        nah["meta"]["requests"],
        nah["meta"]["asn"],
        nah["meta"]["filters"],
    ) == (8.0, 60, 16509, {"asn": 16509})
    assert (
        defs.network_asn_health_processor(
            (7922, 0.06, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120), None, {"asn_names": {7922: "Example ISP"}}
        )["label"]
        == "AS7922 (Example ISP)"
    )


@pytest.mark.parametrize(
    ("proc", "row"),
    [
        (defs.request_size_anomaly_processor, ("203.0.113.42", 70000, 100, 50, 1000)),
        (defs.connection_abuse_processor, ("203.0.113.42", 800, 10, 50, 100)),
        (defs.session_harvesting_processor, ("203.0.113.42", 60, 200, 3)),
        (defs.low_and_slow_processor, ("203.0.113.42", 42, 6, 7200, 0.0058)),
        (defs.credential_enumeration_processor, ("203.0.113.42", 80, 100, 4, 2)),
        (defs.content_discovery_processor, ("203.0.113.42", 60, 75, 40, 3)),
    ],
)
def test_ip_keyed_processors_admin_and_masked_contract(proc, row):
    raw = proc(row, None, {})
    assert raw["label"] == "203.0.113.42" and raw["meta"]["filters"]["ip"] == "203.0.113.42"
    masked = proc(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_session_harvesting_masks_ip_when_mask_ips():
    masked = defs.session_harvesting_processor(("203.0.113.42", 60, 200, 3), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_low_and_slow_masks_ip_when_mask_ips():
    masked = defs.low_and_slow_processor(("203.0.113.42", 42, 6, 7200, 0.0058), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_credential_enumeration_masks_ip_when_mask_ips():
    masked = defs.credential_enumeration_processor(("203.0.113.42", 80, 100, 4, 2), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_content_discovery_masks_ip_when_mask_ips():
    masked = defs.content_discovery_processor(("203.0.113.42", 60, 75, 40, 3), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected
