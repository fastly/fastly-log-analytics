"""Consolidated tests for the Insights repository, registry, coalesced scans, and models."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from backend.core.field_registry import INSIGHT_DEFINITIONS
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.share_db.validation import mask_ip
from backend.models.insights import InsightsRequest, InsightsResponse
from backend.repositories._base import _safe_table
from backend.repositories.insights import (
    InsightCategory,
    InsightDefinition,
    _insights_cache,
    get_insights,
    registry,
)
from backend.repositories.insights import (
    repository as insights_repo,
)
from backend.repositories.insights.repository import (
    _coalesced_url_aggregates,
)
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs

# ── Helpers ───────────────────────────────────────────────────────────────────

_JFK_COORDS = (40.6413, -73.7781)
_MOCK_POP_MAP = {"JFK": _JFK_COORDS}


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


# ── Core Repository & Model Tests ─────────────────────────────────────────────


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


# ── Coalesced Scans & Parity Tests ────────────────────────────────────────────


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
    """Analyst/share view (`mask_ips=True`) must never leak raw attacker IPs through the coalesced IP-security path."""
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


# ── Registry Invariants & Structure Tests ─────────────────────────────────────


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
