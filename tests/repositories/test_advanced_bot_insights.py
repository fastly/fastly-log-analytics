"""Unit and integration tests for advanced bot-detection insights."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.repositories._base import _safe_table
from backend.repositories.insights import _insights_cache, get_insights


def test_advanced_bot_insights_trigger(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])

    # Create a unified table schema containing all necessary fields for the 4 insights
    in_memory_duckdb.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "timestamp" TIMESTAMPTZ,
            "ip" VARCHAR,
            "ua" VARCHAR,
            "referer" VARCHAR,
            "url" VARCHAR,
            "ja4" VARCHAR,
            "asn" INTEGER,
            "p_type" VARCHAR
        )
    """)

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=10)
    baseline_ts = now - timedelta(hours=3)
    history_ts = now - timedelta(
        hours=24, minutes=30
    )  # Safe: >= 25h ago baseline_start, but < 24h ago to satisfy baseline hours count

    def _insert_log(ts, ip, ua, referer, url, ja4=None, asn=None, p_type=None):
        in_memory_duckdb.execute(
            f"""INSERT INTO {table_name}
                (timestamp, ip, ua, referer, url, ja4, asn, p_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [ts.isoformat(), ip, ua, referer, url, ja4 or "", asn or 0, p_type or ""],
        )

    # Insert a historical log to satisfy the check_baseline (available_history_hours >= baseline_hours) check
    _insert_log(
        ts=history_ts,
        ip="1.1.1.1",
        ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36",
        referer="https://google.com",
        url="/index.html",
    )

    # 1. Trigger crawler_disparity: Anthropic (Claude) has 150 crawls in window, 0 referrals
    # Insert some crawls in the baseline too so there is baseline data
    for i in range(10):
        _insert_log(
            ts=baseline_ts,
            ip=f"192.0.2.{i % 10}",
            ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +http://www.anthropic.com/claudebot)",
            referer="-",
            url=f"/item/{i}",
        )
    for i in range(150):
        _insert_log(
            ts=window_ts,
            ip=f"192.0.2.{i % 10}",
            ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +http://www.anthropic.com/claudebot)",
            referer="-",
            url=f"/item/{i}",
        )

    # 2. Trigger stale_browser_version: Chrome 100 has 600 requests in window across 15 distinct IPs
    # Insert some in baseline to compare
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

    # 3. Trigger orphaned_deep_crawl: IP 203.0.113.50 hits 60 nested catalog URLs with no referer
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

    # 4. Trigger residential_fingerprint_dispersion: identical JA4 fingerprint from 40 residential IPs and 8 ASNs
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

    # Clear insights cache to force fresh scan
    _insights_cache.clear()

    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    # Verify crawler_disparity
    assert "crawler_disparity" in by_id
    cd = by_id["crawler_disparity"]
    assert cd["category"] == "security"
    assert len(cd["items"]) > 0
    assert cd["items"][0]["label"] == "Anthropic (Claude)"
    assert cd["items"][0]["meta"]["crawls"] == 150

    # Verify stale_browser_version
    assert "stale_browser_version" in by_id
    sb = by_id["stale_browser_version"]
    assert sb["category"] == "security"
    assert len(sb["items"]) > 0
    assert sb["items"][0]["label"] == "Chrome 100"

    # Verify orphaned_deep_crawl
    assert "orphaned_deep_crawl" in by_id
    odc = by_id["orphaned_deep_crawl"]
    assert odc["category"] == "security"
    assert len(odc["items"]) > 0
    assert odc["items"][0]["label"] == "203.0.113.50"
    assert odc["items"][0]["meta"]["requests"] == 60

    # Verify residential_fingerprint_dispersion
    assert "residential_fingerprint_dispersion" in by_id
    rfd = by_id["residential_fingerprint_dispersion"]
    assert rfd["category"] == "security"
    assert len(rfd["items"]) > 0
    assert rfd["items"][0]["label"] == "t13d1516h2_8e6120e8544e_3a4b5c6d7e8f"
    assert rfd["items"][0]["meta"]["asns"] == 8
