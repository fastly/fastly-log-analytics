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
from backend.repositories.insights import get_cache_collapse_detail, get_insights
from backend.repositories.insights.registry import registry
from tests.utils.mock_data import generate_mock_records

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

_MOCK_DMA_MAP = {
    "501": "New York, NY",
    "803": "Los Angeles, CA",
}


def _create_full_schema_table(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    """Create a log table with all raw catalog columns plus any required_fields."""
    col_types: dict[str, str] = {}
    for f in LOG_FIELD_CATALOG:
        if f.get("vcl") is not None:
            col_types[f["id"]] = "TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]

    for d in registry.get_all():
        for req_col in d.required_fields:
            if req_col not in col_types:
                col_types[req_col] = "VARCHAR"

    for extra_col in ("ja3", "ja4", "region"):
        col_types.setdefault(extra_col, "VARCHAR")

    cols_sql = ", ".join(f'"{c}" {t}' for c, t in col_types.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql})")
    return list(col_types.keys())


def _build_comprehensive_anomaly_rows() -> list[dict[str, Any]]:
    """Construct a deterministic synthetic dataset using log_factory + anomaly rows
    that triggers all 49 registered InsightDefinitions simultaneously.
    """
    rng = random.Random(42)
    base_records = generate_mock_records(
        {"log_fields": {"groups": ["A", "B", "C", "D", "E", "F", "G"]}},
        num_logs=40,
        hours_ago=12,
        rng=rng,
    )

    w_ts_base = FIXED_NOW - timedelta(minutes=30)
    b_ts_base = FIXED_NOW - timedelta(hours=12)
    earliest_ts = FIXED_NOW - timedelta(hours=24, minutes=55)

    def _row(ts: datetime, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "timestamp": ts.isoformat(),
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
        base.update(overrides)
        return base

    rows: list[dict[str, Any]] = []

    # Anchor row at -24h55m so available_history_hours > 24.0
    rows.append(_row(earliest_ts, ip="192.0.2.250", cache="HIT"))

    # Fold in deterministic log_factory records (pinned to baseline timestamps)
    for idx, rec in enumerate(base_records):
        pinned_ts = b_ts_base - timedelta(minutes=idx * 5)
        rows.append(
            _row(
                pinned_ts,
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
    for i in range(12):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), url="/api/err-spike", status=200))
    for i in range(5):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/api/err-spike", status=502))

    # 2. botnet_grouping: ja4='ja4_botnet_1'
    rows.append(_row(b_ts_base, ja4="ja4_botnet_1", ip="10.1.0.1"))
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ja4="ja4_botnet_1", ip=f"10.1.0.{i + 1}"))

    # 4. new_country_traffic: country='IS'
    for i in range(4):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), country="IS", city="Reykjavik", region="1"))

    # 5. city_surges: SurgeVille
    for i in range(5):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), city="SurgeVille", region="TX", country="US"))
    for i in range(25):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), city="SurgeVille", region="TX", country="US"))

    # 6. city_error_spikes: ErrTown
    for i in range(20):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), city="ErrTown", region="CA", country="US", status=200))
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), city="ErrTown", region="CA", country="US", status=500))

    # 7. city_latency_regressions: SlowBerg
    for i in range(55):
        rows.append(
            _row(
                b_ts_base + timedelta(milliseconds=i),
                city="SlowBerg",
                region="NY",
                country="US",
                url="/city/slowberg",
                elapsed=100000,
            )
        )
    for i in range(12):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                city="SlowBerg",
                region="NY",
                country="US",
                url="/city/slowberg",
                elapsed=1500000,
            )
        )

    # 8. new_city_traffic: FreshPolis
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), city="FreshPolis", region="WA", country="US"))

    # 10. new_probe_urls: /wp-admin/setup.php
    for i in range(4):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/wp-admin/setup.php", status=404))

    # 11. waf_signal_spikes: SQLI-PROBE
    rows.append(_row(b_ts_base, waf_sig="SQLI-PROBE"))
    for i in range(5):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), waf_sig="SQLI-PROBE,BOT-ANALYSIS"))

    # 12. proxy_surge + 37b. asn_hosting_shift
    rows.append(_row(b_ts_base, asn="64500", p_type="hosting"))
    rows.append(_row(b_ts_base, asn="64505", p_type=""))
    for i in range(120):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), asn="64505", p_type="hosting"))

    # 14. asn_metro_performance
    for i in range(55):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), asn="64502", metro="501", country="US", tcp_rtt=20000))
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), asn="64502", metro="501", country="US", tcp_rtt=120000))

    # 15. cache_collapse
    for i in range(25):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), url="/static/bundle.js", cache="HIT"))
    for i in range(10):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/static/bundle.js", cache="MISS"))

    # 15b. cacheability_regression
    for i in range(55):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), url="/account/overview", cache="HIT"))
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/account/overview", cache="PASS"))

    # 16. latency_regression
    for i in range(25):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), url="/api/slow-endpoint", elapsed=100000))
    for i in range(8):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/api/slow-endpoint", elapsed=900000))

    # 17. impossible_distance
    for i in range(3):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                pop="JFK",
                lat=0.0,
                lon=0.0,
                tcp_rtt=1000,
                ja4="ja4_spoof_fp",
                ip="198.51.100.77",
                edge=True,
            )
        )

    # 18. tail_latency
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/api/tail-heavy", elapsed=10000))
    for i in range(2):
        rows.append(_row(w_ts_base + timedelta(milliseconds=30 + i), url="/api/tail-heavy", elapsed=200000))

    # 19. cipher_spread
    rows.append(_row(b_ts_base, tls_ciphers_sha="cipher_sha_bot_1", ip="10.2.0.1"))
    for i in range(6):
        rows.append(
            _row(w_ts_base + timedelta(milliseconds=i), tls_ciphers_sha="cipher_sha_bot_1", ip=f"10.2.0.{i + 1}")
        )

    # 20. request_size_anomaly
    for i in range(10):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), ip="10.3.0.1", req_header_bytes=500))
    for i in range(4):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ip="10.3.0.1", req_header_bytes=12000))

    # 21. connection_abuse
    for i in range(10):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), ip="10.4.0.1", conn_requests=5))
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ip="10.4.0.1", conn_requests=120))

    # 22. region_latency + 25. origin_latency_spike + 45. timeout_split
    for i in range(105):
        rows.append(
            _row(
                b_ts_base + timedelta(milliseconds=i),
                server_region="US-East",
                url="/api/origin-timed",
                elapsed=100000,
                ottfb=20000,
                oconnect_ms=10,
            )
        )
    for i in range(55):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                server_region="US-East",
                url="/api/origin-timed",
                elapsed=600000,
                ottfb=450000,
                oconnect_ms=150,
            )
        )

    # 23. cache_ttl_mismatch
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/Promo/Banner", ttl=3600, hits=0, age=10))

    # 24. image_optimization_opportunities
    for i in range(10):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                url="/media/hero-banner.png",
                status=200,
                resp_bytes=102400,
                ua="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile",
            )
        )

    # 26. origin_error_rate + 28. origin_ip_failure
    for i in range(40):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), oip="192.0.2.10", ost=200))
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), oip="192.0.2.10", ost=200))
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), oip="192.0.2.11", ost=200))
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), oip="192.0.2.99", ost=503))

    # 27. origin_retries
    for i in range(6):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), url="/api/flaky-upstream", oretries=3))

    # 29. shield_path_degradation
    for i in range(6):
        rid_b = f"rid_b_{i}"
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), rid=rid_b, pop="LAX", edge=True, ottfb=60000))
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), prid=rid_b, pop="IAD", edge=False, ottfb=20000))
    for i in range(6):
        rid_w = f"rid_w_{i}"
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), rid=rid_w, pop="LAX", edge=True, ottfb=260000))
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), prid=rid_w, pop="IAD", edge=False, ottfb=20000))

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
    probe_paths = ["/.env", "/.env", "/.env", "/.git/config", "/.git/config", "/wp-login.php"]
    for i, probe_url in enumerate(probe_paths):
        rows.append(
            _row(
                w_ts_base + timedelta(seconds=i * 150),
                ip="10.6.0.1",
                url=probe_url,
                status=404,
            )
        )

    # 32. credential_enumeration
    for i in range(25):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ip="10.7.0.1", url="/api/auth/login", status=401))

    # 33. network_asn_health
    for i in range(110):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), asn="64503", ploss=0.001, rtt_var=5000, retrans=0))
    for i in range(55):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), asn="64503", ploss=0.08, rtt_var=60000, retrans=2))

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

    # 37. new_asn_traffic
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), asn="64504"))

    # 38. metro_delivery_degradation
    for i in range(55):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), metro="803", delivery_rate=500000))
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), metro="803", delivery_rate=100000))

    # 39. connection_type_mix
    for i in range(60):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), c_type="broadband", c_speed="fiber"))
    for i in range(25):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), c_type="cellular", c_speed="5g"))

    # 40. pop_latency_regression
    for i in range(55):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), pop="DEN", url="/pop/den", elapsed=50000))
    for i in range(22):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), pop="DEN", url="/pop/den", elapsed=500000))

    # 41. http3_fallback
    for i in range(60):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), transport="quic"))
    for i in range(60):
        rows.append(_row(b_ts_base + timedelta(milliseconds=60 + i), transport="tcp"))
    for i in range(12):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), transport="quic"))
    for i in range(108):
        rows.append(_row(w_ts_base + timedelta(milliseconds=12 + i), transport="tcp"))

    # 43. payload_compression_regression
    for i in range(55):
        rows.append(
            _row(
                b_ts_base + timedelta(milliseconds=i),
                url="/static/main.js",
                status=200,
                resp_bytes=4096,
                resp_header_content_encoding="br",
            )
        )
    for i in range(12):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                url="/static/main.js",
                status=200,
                resp_bytes=4096,
                resp_header_content_encoding="",
            )
        )

    # 44. session_harvesting
    for i in range(22):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ip="10.9.0.1",
                cookie_session=f"sess_harvest_{i}",
            )
        )

    # 46. crawler_disparity
    for i in range(10):
        rows.append(_row(b_ts_base + timedelta(milliseconds=i), ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0)"))
    for i in range(105):
        rows.append(_row(w_ts_base + timedelta(milliseconds=i), ua="Mozilla/5.0 (compatible; Claude-SearchBot/1.0)"))

    # 47. stale_browser_version (needs >= 500 window rows with Chrome < 131)
    # Also piggyback referer_monoculture, asn_concentration, method_drift, and cache_hit_cliff!
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

    # 48. orphaned_deep_crawl
    for i in range(55):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ip="10.10.0.1",
                url=f"/catalog/dept/item-{i % 15}",
                referer="",
            )
        )

    # 49. residential_fingerprint_dispersion
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

    # 9. ua_monoculture (needs >= 25% of window total)
    # Add 520 window rows with MonoScraper/9.9 so 520 / ~1920 >= 27%
    # (505 / ~1920 = 26.3% for Chrome 120, so both exceed 25%!)
    for i in range(520):
        rows.append(
            _row(
                w_ts_base + timedelta(milliseconds=i),
                ua="MonoScraper/9.9",
                asn="64501",
                referer="https://hotlink-scraper.example/page",
            )
        )

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
