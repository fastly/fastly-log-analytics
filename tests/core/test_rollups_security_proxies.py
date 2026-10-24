"""Tests for the security_proxies (proxy_quality / proxy_flagged / bot_eff) per-hour
bundle writers, the day-compaction merge, and the two /api/security/aggregates
readers (try_security_proxies_from_rollup / try_security_bot_efficiency_from_rollup).

The proxies rollup is EXACT. The bot_efficiency rollup is exact for site totals and
per-UA counts/bytes; its per-stem referral count is a SUM of per-hour distinct IPs,
so the parity test pins that documented overcount instead of equality.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
import pytest

from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.locks import noop_lock as _noop_lock
from tests._helpers.rollups import build_writer_patches as _build_patches

pytestmark = pytest.mark.usefixtures("frozen_clock")

# (ip, asn, pop, rtt_min µs, tcp_rtt µs, lat, lon, country, city, ua, referer, resp_bytes)
_ROWS = [
    # direct, near its POP: never flagged
    ("10.0.0.1", 100, "IAD", 20000, 21000, 38.9, -77.0, "US", "Ashburn", "Mozilla/5.0", "https://www.site.com/a", 1000),
    # rtt_min/tcp_rtt = 0.02: tunnel
    ("10.0.0.2", 200, "IAD", 1000, 50000, 39.0, -77.1, "US", "Reston", "Mozilla/5.0", None, 500),
    # ratio 0.5: wifi/mobile, not flagged
    ("10.0.0.3", 300, "IAD", 1000, 2000, 39.1, -77.2, "US", "Herndon", "Mozilla/5.0", None, 300),
    # ratio 0.143: flagged (medium) but not a tunnel
    ("10.0.0.4", 400, "IAD", 10000, 70000, 39.2, -77.3, "US", "Sterling", "Mozilla/5.0", None, 200),
    # client in Tokyo claiming a 5 ms RTT to IAD: distance mismatch
    ("10.0.0.5", 500, "IAD", 5000, 6000, 35.0, 139.0, "JP", "Tokyo", "Googlebot/2.1", "https://www.news.com/x", 4000),
    # no RTT data: direct, never flagged
    ("10.0.0.6", 600, "IAD", None, None, 40.0, -75.0, "US", "Philadelphia", "GPTBot-User/1.0", None, 100),
]


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple], base: datetime) -> None:
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, ip VARCHAR, asn BIGINT, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, "
        f"  lat DOUBLE, lon DOUBLE, country VARCHAR, city VARCHAR, ua VARCHAR, referer VARCHAR, resp_bytes BIGINT)"
    )
    for j, r in enumerate(rows):
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [base + timedelta(seconds=j), *r]
        )


def _build_hour(cache_root, src, table: str, hour_dt: datetime, rows: list[tuple]) -> int:
    from backend.core.rollups import security_proxies

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs(c, table, rows, hour_dt)
        return c

    p = _build_patches(cache_root, table, _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        return security_proxies.build_security_proxies_bundles(src["name"], src, [hour_dt.strftime("%Y-%m-%d-%H")])


def test_writer_emits_three_bundles_with_only_flagged_rows(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sp-1"}
    hour_token, hour_dt = _past_hour(2)

    assert _build_hour(cache_root, src, "logs_sp1", hour_dt, _ROWS) == 3

    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
    quality = {
        r["type"]: r["count"] for r in pq.read_table(str(hour_dir / "security_proxy_quality.parquet")).to_pylist()
    }
    assert quality == {"Direct Connection": 3, "Active Tunnel / Proxy": 1, "WiFi / Mobile": 2}

    flagged = pq.read_table(str(hour_dir / "security_proxy_flagged.parquet")).to_pylist()
    assert sorted(r["ip"] for r in flagged) == ["10.0.0.2", "10.0.0.4", "10.0.0.5"]
    assert all(r["cnt"] == 1 for r in flagged)

    bot_eff = pq.read_table(str(hour_dir / "security_bot_eff.parquet")).to_pylist()
    site = [(r["cnt"], r["resp_bytes"]) for r in bot_eff if r["kind"] == "site"]
    assert site == [(6, 6100)]
    assert {r["key"] for r in bot_eff if r["kind"] == "ua"} == {"Mozilla/5.0", "Googlebot/2.1", "GPTBot-User/1.0"}


def test_writer_skips_services_without_the_required_columns(tmp_path):
    from backend.core.rollups import security_proxies

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sp-2"}
    hour_token, hour_dt = _past_hour(2)

    def _fresh_con():
        c = duckdb.connect(":memory:")
        c.execute("CREATE TABLE logs_sp2 (timestamp TIMESTAMPTZ, ua VARCHAR, ip VARCHAR)")
        c.execute("INSERT INTO logs_sp2 VALUES (?, 'x', '10.0.0.1')", [hour_dt])
        return c

    p = _build_patches(cache_root, "logs_sp2", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = security_proxies.build_security_proxies_bundles("svc-sp-2", src, [hour_token])

    # Only bot_eff needs just ua + ip; the proxy bundles need the RTT/geo columns.
    assert n == 1
    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
    assert (hour_dir / "security_bot_eff.parquet").exists()
    assert not (hour_dir / "security_proxy_flagged.parquet").exists()
    assert not (hour_dir / "security_proxy_quality.parquet").exists()


def test_readers_eligibility_gates(tmp_path):
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-sp-3"})
    readers = [
        lambda s, e, hf: runner.try_security_proxies_from_rollup(s, e, has_filters=hf),
        lambda s, e, hf: runner.try_security_bot_efficiency_from_rollup(s, e, has_filters=hf),
    ]
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        for call in readers:
            assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True) is None  # filters
            assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False) is None  # < 48 h
            assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False) is None  # nothing built
            assert call("not-a-date", "2026-06-15T00:00:00Z", False) is None


def _build_window(tmp_path, name: str):
    """Build 50 closed hours of identical rows; return (cache_root, src, hours, live con, runner)."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": name}
    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        _build_hour(cache_root, src, "logs_par", dt, _ROWS)

    con = duckdb.connect(":memory:")
    all_rows_table = "t_par"
    con.execute(
        f"CREATE TABLE {all_rows_table} (timestamp TIMESTAMPTZ, ip VARCHAR, asn BIGINT, pop VARCHAR, rtt_min BIGINT, "
        f"tcp_rtt BIGINT, lat DOUBLE, lon DOUBLE, country VARCHAR, city VARCHAR, ua VARCHAR, referer VARCHAR, "
        f"resp_bytes BIGINT)"
    )
    for dt in hours:
        for j, r in enumerate(_ROWS):
            con.execute(
                f"INSERT INTO {all_rows_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [dt + timedelta(seconds=j), *r],
            )
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    return cache_root, QueryRunner(con, src), con, st_iso, et_iso


def test_proxies_reader_matches_live_sql(tmp_path):
    from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED

    cache_root, runner, con, st_iso, et_iso = _build_window(tmp_path, "svc-sp-4")

    live = con.execute(
        GET_SECURITY_PROXIES_COMBINED.format(
            temp_table="t_par",
            select_country_city_inner=",\n            country,\n            city",
            select_country_city_outer=",\n                country,\n                city",
        )
    ).fetchall()
    live_stats = next(r for r in live if r[0] == "stats")
    live_quality = sorted((r[1], r[2]) for r in live if r[0] == "quality")
    live_isps = sorted((r[2], r[3]) for r in live if r[0] == "isp")
    # The live scan repeats a client once per request; the rollup groups identical tuples.
    live_clients = {tuple(r[1:3]) + tuple(r[5:]) for r in live if r[0] == "client"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)

    assert rolled is not None
    assert rolled["stats"] == {
        "active_proxies_count": live_stats[2],
        "total_requests_count": live_stats[3],
        "distance_mismatches_count": live_stats[4],
    }
    assert sorted(rolled["quality_rows"]) == live_quality
    assert sorted((r["asn"], r["count"]) for r in rolled["isps"]) == live_isps
    rolled_clients = {
        (
            c["ip"],
            c["asn"],
            c["rtt_min_ms"],
            c["tcp_rtt_ms"],
            c["distance_km"],
            c["pop"],
            c["client_lat"],
            c["client_lon"],
            c["pop_lat"],
            c["pop_lon"],
            c["impossible_distance"],
            c["risk_level"],
            c["country"],
            c["city"],
        )
        for c in rolled["clients"]
    }
    # The live scan returns one row per request, so its 100-row cap fills with the 100 High-risk
    # repeats (50 hours x 2 IPs) and drops the Medium-risk client; the grouped rollup lists it once.
    assert live_clients <= rolled_clients
    assert {c[0] for c in rolled_clients} == {"10.0.0.2", "10.0.0.4", "10.0.0.5"}
    assert len(rolled["clients"]) == len(rolled_clients) == 3
    # Sanity on the seeded scenario: tunnel + tokyo mismatch counted, the 0.143 ratio row is not a tunnel.
    assert rolled["stats"]["active_proxies_count"] == 2
    assert rolled["stats"]["distance_mismatches_count"] == 1
    assert rolled["stats"]["total_requests_count"] == 2 * 50


def test_bot_efficiency_reader_matches_live_sql_with_documented_ref_overcount(tmp_path):
    from backend.repositories._sql.security import BOT_EFFICIENCY

    cache_root, runner, con, st_iso, et_iso = _build_window(tmp_path, "svc-sp-5")

    live = con.execute(
        BOT_EFFICIENCY.format(temp_table="t_par", resp_bytes_expr="COALESCE(t.resp_bytes, 0)", referer_expr="t.referer")
    ).fetchall()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rolled = runner.try_security_bot_efficiency_from_rollup(st_iso, et_iso, has_filters=False)

    assert rolled is not None
    live_by_kind: dict[str, dict[str, tuple[int, int]]] = {}
    for kind, key, cnt, rb in live:
        live_by_kind.setdefault(kind, {})[key] = (int(cnt), int(rb))
    rolled_by_kind: dict[str, dict[str, tuple[int, int]]] = {}
    for kind, key, cnt, rb in rolled:
        rolled_by_kind.setdefault(kind, {})[key] = (cnt, rb)

    assert rolled_by_kind["site"] == live_by_kind["site"]
    assert rolled_by_kind["ua"] == live_by_kind["ua"]
    # The same IPs recur every hour, so SUM(per-hour distinct) is exactly hours x the true distinct count.
    assert set(rolled_by_kind["ref"]) == set(live_by_kind["ref"])
    for stem, (cnt, _) in rolled_by_kind["ref"].items():
        assert cnt == live_by_kind["ref"][stem][0] * 50


def test_day_compaction_merges_hours_exactly(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sp-6"}
    day = (datetime.now(UTC) - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    for h in range(24):
        _build_hour(cache_root, src, "logs_day", day + timedelta(hours=h), _ROWS)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            n = day_bundles.compact_security_proxies_closed_days_to_daily("svc-sp-6", src)
    assert n == 3

    day_root = cache_root / "rollups" / "day_bundled" / f"day={day.strftime('%Y-%m-%d')}"
    quality = {
        r["type"]: r["count"] for r in pq.read_table(str(day_root / "security_proxy_quality.parquet")).to_pylist()
    }
    assert quality == {"Direct Connection": 3 * 24, "Active Tunnel / Proxy": 24, "WiFi / Mobile": 2 * 24}
    flagged = pq.read_table(str(day_root / "security_proxy_flagged.parquet")).to_pylist()
    assert sorted((r["ip"], r["cnt"]) for r in flagged) == [("10.0.0.2", 24), ("10.0.0.4", 24), ("10.0.0.5", 24)]
    bot_eff = pq.read_table(str(day_root / "security_bot_eff.parquet")).to_pylist()
    assert [(r["cnt"], r["resp_bytes"]) for r in bot_eff if r["kind"] == "site"] == [(6 * 24, 6100 * 24)]


def test_proxies_reader_handles_sparse_and_empty_flagged_hours(tmp_path):
    """Hours with 0 flagged proxy rows write a .done marker instead of
    security_proxy_flagged.parquet; the reader must still serve from rollups."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sp-7"}
    base_dt = (datetime.now(UTC) - timedelta(hours=30)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(24)]
    direct_only = [_ROWS[0]]  # Direct connection, never flagged
    for dt in hours:
        _build_hour(cache_root, src, "logs_sparse", dt, direct_only)

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)

    assert rolled is not None
    assert rolled["stats"] == {
        "active_proxies_count": 0,
        "total_requests_count": 0,
        "distance_mismatches_count": 0,
    }
    assert rolled["quality_rows"] == [("Direct Connection", 24)]
    assert rolled["isps"] == []
    assert rolled["clients"] == []


def test_blank_ip_requests_never_appear_as_suspicious_clients(tmp_path):
    """Requests logged before the capture snippet ran (redirects) carry no IP, UA or country but
    still have edge RTT/geo, so they used to pass the proxy flags and render as blank-IP rows."""
    from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED

    blank = ("", 700, "IAD", 1000, 50000, 39.0, -77.1, "", "", "", None, 0)
    rows = [*_ROWS, blank]
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        _build_hour(cache_root, {"name": "svc-sp-blank"}, "logs_par", dt, rows)

    live_con = duckdb.connect(":memory:")
    _seed_logs(live_con, "t_blank", rows, hours[0])
    live = live_con.execute(
        GET_SECURITY_PROXIES_COMBINED.format(
            temp_table="t_blank",
            select_country_city_inner=",\n            country,\n            city",
            select_country_city_outer=",\n                country,\n                city",
        )
    ).fetchall()
    assert [r[1] for r in live if r[0] == "client" and not r[1]] == []
    assert next(r for r in live if r[0] == "stats")[2] == 2  # active proxies: the tunnel + the Tokyo mismatch

    from backend.repositories._base import QueryRunner

    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-sp-blank"})
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rolled = runner.try_security_proxies_from_rollup(st_iso, et_iso, has_filters=False)
    assert rolled is not None
    assert all(c["ip"] for c in rolled["clients"])
    assert rolled["stats"]["active_proxies_count"] == 2
