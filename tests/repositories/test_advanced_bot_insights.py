"""Unit and integration tests for advanced bot-detection insights."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from backend.repositories._base import _safe_table
from backend.repositories._sql import insights as SQL
from backend.repositories.insights import _insights_cache, get_insights


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
    assert rows == [(raw_tok, 1000, 1, 2048000, 1000.0, 10.0, 2048000.0, 10240.0)]
    assert resolve_bot_display_name(rows[0][0]) == display_name


def test_bot_bandwidth_disparity_sql(
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
    rows = con.execute(SQL.BOT_BANDWIDTH_DISPARITY.format(table_name="ua_logs"), [boundary] * 3).fetchall()
    assert len(rows) == 1
    bot_tok, w_requests, w_bytes, bw_share_pct, w_referrals, req_per_ref, bytes_per_ref = rows[0]
    assert bot_tok == "ccbot"
    assert resolve_bot_display_name(bot_tok) == "Commoncrawl Crawler"
    assert w_requests == 100
    assert w_bytes == 5_000_000
    assert round(bw_share_pct, 1) == 50.0
    assert w_referrals == 0
    assert req_per_ref == 100.0
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

    # Create a unified table schema containing all necessary fields for the 5 insights
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
    history_ts = now - timedelta(
        hours=24, minutes=30
    )  # Safe: >= 25h ago baseline_start, but < 24h ago to satisfy baseline hours count

    def _insert_log(ts, ip, ua, referer, url, ja4=None, asn=None, p_type=None, resp_bytes=1024):
        in_memory_duckdb.execute(
            f"""INSERT INTO {table_name}
                (timestamp, ip, ua, referer, url, ja4, asn, p_type, resp_bytes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [ts.isoformat(), ip, ua, referer, url, ja4 or "", asn or 0, p_type or "", resp_bytes],
        )

    # Insert a historical log to satisfy the check_baseline (available_history_hours >= baseline_hours) check
    _insert_log(
        ts=history_ts,
        ip="1.1.1.1",
        ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36",
        referer="https://google.com",
        url="/index.html",
    )

    # 1. Trigger crawler_disparity & bot_bandwidth_disparity: Anthropic (Claude) has 150 crawls in window, 0 referrals
    # Insert some crawls in the baseline too so there is baseline data
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

    # Also insert a non-referring scraper (CCBot) with high bandwidth for bot_bandwidth_disparity
    for i in range(60):
        _insert_log(
            ts=window_ts,
            ip=f"198.18.0.{i % 5}",
            ua="CCBot/2.0 (https://commoncrawl.org/faq/)",
            referer="-",
            url=f"/archive/{i}",
            resp_bytes=50000,
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
    assert cd["items"][0]["label"] == "Anthropic Crawler Search"
    assert cd["items"][0]["meta"]["crawls"] == 150
    assert cd["items"][0]["meta"]["w_bytes"] == 3_000_000
    assert cd["items"][0]["meta"]["bytes_per_referral"] == 3_000_000.0

    # Verify bot_bandwidth_disparity
    assert "bot_bandwidth_disparity" in by_id
    bbd = by_id["bot_bandwidth_disparity"]
    assert bbd["category"] == "security"
    assert len(bbd["items"]) >= 2
    bbd_labels = {item["label"]: item for item in bbd["items"]}
    assert "Commoncrawl Crawler" in bbd_labels
    assert bbd_labels["Commoncrawl Crawler"]["meta"]["requests"] == 60
    assert bbd_labels["Commoncrawl Crawler"]["meta"]["w_bytes"] == 3_000_000
    assert bbd_labels["Commoncrawl Crawler"]["meta"]["referrals"] == 0

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
