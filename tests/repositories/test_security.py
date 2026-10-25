from __future__ import annotations

import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.models.security import SecurityTopBotsResponse

from backend.models.common import FilterSpec
from backend.repositories import security
from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories._sql import security as SQL
from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED
from backend.repositories.security import get_security_aggregates, get_top_bots
from backend.utils.hll import HyperLogLog
from scripts.lib.section_measurements import describe_response
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs

_ALL_SECTION_KEYS = {
    "verified_bots_ts",
    "ngwaf_verified_bots",
    "ngwaf_verified_bots_ts",
    "wellknown_bots",
    "tls_fingerprints",
    "fingerprint_coverage",
    "req_size_dist",
    "top_ips_header",
    "ipv6_adoption",
    "proxy_dist",
    "conn_reuse_dist",
}


def _seed_logs(con, src: dict, num_logs: int = 10, **overrides: Any) -> str:
    table_name = _safe_table(src["name"])
    logs = generate_mock_logs(src, num_logs=num_logs)
    for i, log in enumerate(logs):
        for k, v in overrides.items():
            log[k] = v(i) if callable(v) else v
    insert_mock_logs(con, table_name, logs)
    return table_name


def _use_cache_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cache_root = tmp_path / "cache"
    cache_root.mkdir(exist_ok=True)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
    return cache_root


def _capture_runner_sqls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    captured: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str):
            captured.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "execute", _spy)
    return captured


def _seed_count_rollup(cache_root: Path, field: str, hours_back: int, value_counts: dict[str, int]) -> str:
    closed_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)
    closed_hour = closed_hour_dt.strftime("%Y-%m-%d-%H")
    count_dir = cache_root / "rollups" / "hour" / f"field={field}" / f"hour={closed_hour}"
    count_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "value": pa.array(list(value_counts.keys())),
                "count": pa.array(list(value_counts.values()), type=pa.int64()),
            }
        ),
        str(count_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"),
    )
    return closed_hour


def _seed_bundled_hour_rollup(cache_root: Path, hours_back: int, field_value_counts: dict[str, dict[str, int]]) -> str:
    closed_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)
    closed_hour = closed_hour_dt.strftime("%Y-%m-%d-%H")
    bundle_dir = cache_root / "rollups" / "hour_bundled" / f"hour={closed_hour}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rows = [(f, v, c) for f, vc in field_value_counts.items() for v, c in vc.items()]
    pq.write_table(
        pa.table(
            {
                "field": pa.array([r[0] for r in rows]),
                "value": pa.array([r[1] for r in rows]),
                "count": pa.array([r[2] for r in rows], type=pa.int64()),
            }
        ),
        str(bundle_dir / "all_fields.parquet"),
    )
    return closed_hour


def _wide_window(closed_hour_str: str) -> tuple[str, str]:
    dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    return (
        (dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        (dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


# ── Core get_security_aggregates & get_top_bots ──────────────────────────────


def test_security_aggregates_empty_table(in_memory_duckdb):
    src = {"name": "nonexistent_security_svc", "service_id": "nonexistent_security_svc"}
    result = get_security_aggregates(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
    assert "debug_queries" in result and "debug_calls" in result


def test_security_aggregates_returns_required_keys(in_memory_duckdb, test_service_source):
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=40)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    expected_keys = {
        "tls_fingerprints",
        "req_size_dist",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "verified_bots_ts",
        "debug_queries",
        "debug_calls",
    }
    assert expected_keys.issubset(result.keys())
    for key in expected_keys - {"debug_queries", "debug_calls"}:
        assert isinstance(result[key], list), f"Expected list for '{key}'"


def test_get_top_bots_empty_table(in_memory_duckdb):
    src = {"name": "nonexistent_bots_svc", "service_id": "nonexistent_bots_svc"}
    result = get_top_bots(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
    assert result["bots"] == [] and result["ngwaf_bots"] == []
    assert "debug_queries" in result and "debug_calls" in result


def test_get_top_bots_with_bot_uas(in_memory_duckdb, test_service_source):
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=30)
    result = get_top_bots(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}, n=10
    )
    assert isinstance(result, dict) and "bots" in result and "ngwaf_bots" in result
    if result["bots"]:
        bot = result["bots"][0]
        assert {"id", "category", "request_count"}.issubset(bot.keys())
        assert isinstance(bot["request_count"], int) and bot["request_count"] > 0


def test_get_top_bots_respects_n_limit(in_memory_duckdb, test_service_source):
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=50)
    result = get_top_bots(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}, n=1
    )
    assert len(result["bots"]) <= 1


def test_security_aggregates_includes_tls_fingerprints_when_both_cols_present(in_memory_duckdb, test_service_source):
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        num_logs=20,
        tls_ciphers_sha=lambda i: "aabbcc11" if i < 10 else "ddeeff22",
    )
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert "tls_fingerprints" in result
    for fp in result["tls_fingerprints"]:
        assert {"fingerprint", "ip_count", "request_count"}.issubset(fp.keys())


def test_security_aggregates_tls_fingerprints_empty_when_col_absent(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE "{table_name}" (timestamp TIMESTAMP, ip VARCHAR, status INTEGER)')
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), '1.1.1.1', 200)")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert result["tls_fingerprints"] == []


def test_security_aggregates_ngwaf_keys_empty_when_no_waf_req_id_col(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE "{table_name}" (timestamp TIMESTAMP, status INTEGER)')
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), 200)")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert result["ngwaf_verified_bots"] == [] and result["ngwaf_verified_bots_ts"] == []


@pytest.mark.parametrize(
    "pattern,ua,ip,matched",
    [
        (
            "(?i)bot|crawler",
            "Mozilla/5.0 (compatible; Googlebot/2.1)",
            "66.249.66.1",
            [{"id": "googlebot", "categories": ["search"]}],
        ),
        ("", "MyBot/1.0", "1.2.3.4", []),
    ],
    ids=["regex_prefilter", "fallback_no_pattern"],
)
def test_security_aggregates_wellknown_bots_regex_and_fallback(
    in_memory_duckdb, test_service_source, pattern, ua, ip, matched
):
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, ua=ua, ip=ip)
    with (
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=pattern),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda _ua: matched),
        patch("backend.utils.rdns_cache.get_hostnames", return_value={}),
        patch("backend.utils.rdns_cache.classify", return_value="unverified_pending"),
        patch("backend.utils.rdns_cache.enqueue"),
    ):
        result = get_security_aggregates(
            con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
        )
    assert "wellknown_bots" in result


def test_security_aggregates_fingerprints_use_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        num_logs=20,
        tls_ciphers_sha=lambda i: "rollup-served-fp" if i < 10 else "live-fp-fallback",
    )
    closed_hour = _seed_count_rollup(cache_root, "tls_ciphers_sha", hours_back=2, value_counts={"rollup-served-fp": 42})
    closed_hour_dt = datetime.strptime(closed_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    hll = HyperLogLog()
    hll.update([f"10.0.0.{i}" for i in range(7)])
    ip_dir = cache_root / "rollups" / "hour_ip_spread" / "field=tls_ciphers_sha" / f"hour={closed_hour}"
    ip_dir.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "value": pa.array(["rollup-served-fp"]),
                "ip_sketch": pa.array([hll.to_bytes()], type=pa.binary()),
                "ip_count_observed": pa.array([7], type=pa.int64()),
                "sample_capped": pa.array([False], type=pa.bool_()),
            }
        ),
        str(ip_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"),
    )

    sqls = _capture_runner_sqls(monkeypatch)
    start = (closed_hour_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )

    by_value = {fp["fingerprint"]: fp for fp in result["tls_fingerprints"]}
    assert "rollup-served-fp" in by_value
    assert by_value["rollup-served-fp"]["request_count"] == 42
    assert by_value["rollup-served-fp"]["ip_count"] > 0
    assert not any("count(DISTINCT ip)" in s and '"tls_ciphers_sha"' in s for s in sqls)


def test_security_aggregates_fingerprints_fall_back_to_live_when_ip_spread_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, tls_ciphers_sha="live-served-fp")
    closed_hour = _seed_count_rollup(cache_root, "tls_ciphers_sha", hours_back=2, value_counts={"live-served-fp": 42})
    closed_hour_dt = datetime.strptime(closed_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    sqls = _capture_runner_sqls(monkeypatch)
    start = (closed_hour_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )

    assert any("count(DISTINCT ip)" in s and '"tls_ciphers_sha"' in s for s in sqls)
    for fp in result["tls_fingerprints"]:
        if fp.get("fingerprint") == "live-served-fp":
            assert fp["ip_count"] > 0


def test_security_aggregates_fingerprints_skip_rollup_when_filters_present(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, tls_ciphers_sha="fp-A")
    rollup_calls = {"top_n": 0, "ip_spread": 0}
    orig_top_n = QueryRunner.execute_top_n_rollups
    orig_ip = QueryRunner.execute_ip_spread_rollups
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, *a, **kw: (
            rollup_calls.__setitem__("top_n", rollup_calls["top_n"] + 1) or orig_top_n(self, *a, **kw)
        ),
    )
    monkeypatch.setattr(
        QueryRunner,
        "execute_ip_spread_rollups",
        lambda self, *a, **kw: (
            rollup_calls.__setitem__("ip_spread", rollup_calls["ip_spread"] + 1) or orig_ip(self, *a, **kw)
        ),
    )

    get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200"])},
    )
    assert rollup_calls == {"top_n": 0, "ip_spread": 0}


def test_security_aggregates_ipv6_uses_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=False)
    closed_hour_str = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 23, "false": 77})
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed_hour_str)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    by_time = {row["time"]: row for row in result["ipv6_adoption"]}
    expected_time = f"{closed_hour_str[:10]}T{closed_hour_str[11:13]}:00:00+00:00"
    assert expected_time in by_time and abs(by_time[expected_time]["pct"] - 23.0) < 0.001
    assert not any("CASE WHEN is_ipv6" in s and "FROM t_" in s for s in sqls)


def test_security_aggregates_proxy_uses_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, p_type="hosting")
    closed_hour_str = _seed_count_rollup(
        cache_root, "p_type", hours_back=2, value_counts={"hosting": 1496, "edu": 7, "": 1382}
    )
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed_hour_str)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    by_type = {row["type"]: row["count"] for row in result["proxy_dist"]}
    assert "" not in by_type and by_type.get("hosting", 0) >= 1496 and by_type.get("edu", 0) >= 7
    assert list(by_type) == sorted(by_type, key=by_type.get, reverse=True)
    assert not any("SELECT p_type, count(*)" in s and "FROM t_" in s for s in sqls)


def test_security_aggregates_ipv6_proxy_skip_rollup_when_filters_present(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, is_ipv6=True, p_type="hosting")
    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 10, "false": 0})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 10})
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200"])},
    )
    assert "ipv6_adoption" in result and "proxy_dist" in result


def test_security_aggregates_ipv6_proxy_fall_back_to_live_when_rollup_cold(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=True, p_type="hosting")
    sqls = _capture_runner_sqls(monkeypatch)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert any("CASE WHEN is_ipv6" in s and "FROM t_" in s for s in sqls)
    assert any("SELECT p_type, count(*)" in s and "FROM t_" in s for s in sqls)
    assert result["ipv6_adoption"] and any(row["type"] == "hosting" for row in result["proxy_dist"])


def test_security_aggregates_drops_ipv6_ptype_from_temp_when_rollups_will_serve(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, is_ipv6=False, p_type="hosting")
    closed_hour_str = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"false": 50})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed_hour_str)
    get_security_aggregates(con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={})
    catalog_creates = [s for s in sqls if "CREATE TEMP TABLE" in s and "tls_ciphers_sha" in s]
    assert catalog_creates
    catalog_sql = max(catalog_creates, key=lambda s: s.count('"'))
    assert "is_ipv6" not in catalog_sql and "p_type" not in catalog_sql


def test_security_aggregates_ipv6_serves_from_bundled_hour_when_per_field_swept(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=False)
    closed_hour_str = _seed_bundled_hour_rollup(
        cache_root, hours_back=3, field_value_counts={"is_ipv6": {"true": 17, "false": 83}}
    )
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed_hour_str)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    by_time = {row["time"]: row for row in result["ipv6_adoption"]}
    expected_time = f"{closed_hour_str[:10]}T{closed_hour_str[11:13]}:00:00+00:00"
    assert expected_time in by_time and abs(by_time[expected_time]["pct"] - 17.0) < 0.001
    assert not any("CASE WHEN is_ipv6" in s and "FROM t_" in s for s in sqls)


def test_security_aggregates_ipv6_mixes_bundled_and_per_field_hours(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, is_ipv6=False)
    bundled_hour = _seed_bundled_hour_rollup(
        cache_root, hours_back=4, field_value_counts={"is_ipv6": {"true": 30, "false": 70}}
    )
    per_field_hour = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 50, "false": 50})
    bundled_dt = datetime.strptime(bundled_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    per_field_dt = datetime.strptime(per_field_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)

    start = (bundled_dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (per_field_dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    by_time = {row["time"]: row for row in result["ipv6_adoption"]}
    bundled_iso = f"{bundled_hour[:10]}T{bundled_hour[11:13]}:00:00+00:00"
    per_field_iso = f"{per_field_hour[:10]}T{per_field_hour[11:13]}:00:00+00:00"
    assert abs(by_time[bundled_iso]["pct"] - 30.0) < 0.001
    assert abs(by_time[per_field_iso]["pct"] - 50.0) < 0.001


def test_security_aggregates_ipv6_proxy_skip_rollup_on_small_window(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, is_ipv6=True, p_type="hosting")
    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 100})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 100})
    end = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (datetime.now(UTC) - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
    sqls = _capture_runner_sqls(monkeypatch)

    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    assert any("CASE WHEN is_ipv6" in s and "FROM t_" in s for s in sqls)
    assert any("SELECT p_type, count(*)" in s and "FROM t_" in s for s in sqls)
    assert "ipv6_adoption" in result and "proxy_dist" in result


def test_security_aggregates_window_gate_keeps_ipv6_ptype_in_temp_on_small_window(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, is_ipv6=False, p_type="hosting")
    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"false": 50})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    end = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (datetime.now(UTC) - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
    sqls = _capture_runner_sqls(monkeypatch)

    get_security_aggregates(con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={})
    catalog_creates = [s for s in sqls if "CREATE TEMP TABLE" in s and "tls_ciphers_sha" in s]
    assert catalog_creates
    catalog_sql = max(catalog_creates, key=lambda s: s.count('"'))
    assert "is_ipv6" in catalog_sql and "p_type" in catalog_sql


@pytest.mark.parametrize(
    "sections,expected_present",
    [
        (None, _ALL_SECTION_KEYS),
        ({"ipv6_adoption"}, {"ipv6_adoption"}),
        ({"tls_fingerprints", "fingerprint_coverage"}, {"tls_fingerprints", "fingerprint_coverage"}),
    ],
    ids=["none_full", "single_ipv6", "coupled_fingerprints"],
)
def test_security_aggregates_section_selector(in_memory_duckdb, test_service_source, sections, expected_present):
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        num_logs=20,
        tls_ciphers_sha=lambda i: "abc123" if i < 10 else "def456",
    )
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sections=sections,
    )
    assert (_ALL_SECTION_KEYS & result.keys()) == expected_present
    assert "section_timings" in result and "debug_queries" in result


def test_security_aggregates_skips_temp_entirely_when_every_section_rollup_served(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    table_name = _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=False)
    closed_hour_str = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 23, "false": 77})
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed_hour_str)
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
        sections={"ipv6_adoption"},
    )
    markers = {e["section"] for e in result["section_timings"]}
    assert "temp_table_create" not in markers and "security:temp_skipped" in markers
    assert not [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s]
    assert result["ipv6_adoption"]


def test_security_aggregates_drops_wide_string_cols_from_default_temp(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    table_name = _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=False, p_type="hosting")
    closed = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"false": 50})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    monkeypatch.setattr(
        "backend.core.rollups.read_wellknown_bots_rollup",
        lambda *a, **k: [("Googlebot/2.1", "66.249.66.1", 5)],
    )
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed)
    get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
        sections=_ALL_SECTION_KEYS - {"verified_bots_ts"},
    )
    create_sqls = [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s]
    assert create_sqls
    catalog_sql = max(create_sqls, key=lambda s: s.count('"'))
    for col in ('"ua"', '"waf_sig"', '"is_ipv6"', '"p_type"'):
        assert col not in catalog_sql


def test_security_aggregates_histogram_rollups_skip_temp(in_memory_duckdb, test_service_source, monkeypatch):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table_name}" (timestamp TIMESTAMPTZ, ip VARCHAR, req_header_bytes BIGINT, conn_requests BIGINT)'
    )
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), '1.2.3.4', 300, 2)")

    monkeypatch.setattr(
        QueryRunner,
        "try_security_req_size_from_rollup",
        lambda self, s, e, has_filters: [{"bucket": "0-256B", "count": 5}],
    )
    monkeypatch.setattr(
        QueryRunner,
        "try_security_conn_reuse_from_rollup",
        lambda self, s, e, has_filters: [{"bucket": "1 (None)", "count": 3}],
    )
    monkeypatch.setattr(
        QueryRunner,
        "try_security_top_ips_from_rollup",
        lambda self, s, e, has_filters: [{"ip": "9.9.9.9", "max_header": 900}],
    )
    sqls = _capture_runner_sqls(monkeypatch)
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-08T00:00:00Z",
        filters={},
        sections={"req_size_dist", "conn_reuse_dist", "top_ips_header"},
    )
    markers = {e["section"] for e in result["section_timings"]}
    assert "temp_table_create" not in markers and "security:temp_skipped" in markers
    assert not [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s]
    assert result["req_size_dist"] == [{"bucket": "0-256B", "count": 5}]
    assert result["conn_reuse_dist"] == [{"bucket": "1 (None)", "count": 3}]
    assert result["top_ips_header"] == [{"ip": "9.9.9.9", "max_header": 900}]
    reads = [row for row in result["section_timings"] if row.get("outcome") == "served"]
    assert len(reads) == 3
    assert all(row["source"] == "rollup" and row["current_execution"] for row in reads)
    assert (
        next(row for row in result["section_timings"] if row["section"] == "security:temp_skipped")["activity"]
        == "skipped"
    )


def test_security_aggregates_coverage_rollup_drops_tls_and_skips_temp(
    in_memory_duckdb, test_service_source, monkeypatch
):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table_name}" (timestamp TIMESTAMPTZ, ip VARCHAR, tls_ciphers_sha VARCHAR)'
    )
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (now(), '1.2.3.4', 'abc123')")
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("tls_ciphers_sha", "abc123", 10)], []),
    )
    monkeypatch.setattr(
        QueryRunner,
        "execute_ip_spread_rollups",
        lambda self, fields, s, e, **kw: ({("tls_ciphers_sha", "abc123"): 4}, {}),
    )
    monkeypatch.setattr(QueryRunner, "try_security_coverage_from_rollup", lambda self, s, e, has_filters: (1000, 600))
    sqls = _capture_runner_sqls(monkeypatch)

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-08T00:00:00Z",
        filters={},
        sections={"tls_fingerprints", "fingerprint_coverage"},
    )
    markers = {e["section"] for e in result["section_timings"]}
    assert "temp_table_create" not in markers and "security:temp_skipped" in markers
    assert not [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s]
    assert result["tls_fingerprints"] == [{"fingerprint": "abc123", "ip_count": 4, "request_count": 10}]
    assert result["fingerprint_coverage"] == {"tls_ciphers_sha": 0.6}


def test_security_aggregates_bot_efficiency(in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table_name}" (timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR, referer VARCHAR, resp_bytes BIGINT)'
    )
    for i in range(10):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES ('2026-01-02T01:00:00Z', '66.249.66.{i}', "
            "'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)', '', 10000)"
        )
    for i in range(2):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES ('2026-01-02T01:05:00Z', '198.51.100.{i + 1}', "
            "'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15', 'https://www.google.com/search?q=x', 5000)"
        )
    for i in range(5):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES ('2026-01-02T01:10:00Z', '203.0.113.{i + 1}', "
            "'CCBot/2.0 (https://commoncrawl.org/faq/)', '', 20000)"
        )

    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-03T00:00:00Z",
        filters={},
        sections={"bot_efficiency"},
    )
    by_name = {item["bot_name"]: item for item in result["bot_efficiency"]}
    google = by_name["Google Crawler"]
    assert (google["request_count"], google["resp_bytes"], google["referrals"]) == (10, 100000, 2)
    assert (google["requests_per_referral"], google["bytes_per_referral"]) == (5.0, 50000.0)
    ccbot = by_name["Commoncrawl Crawler"]
    assert (ccbot["request_count"], ccbot["resp_bytes"], ccbot["referrals"]) == (5, 100000, 0)
    assert (ccbot["requests_per_referral"], ccbot["bytes_per_referral"]) == (5.0, 100000.0)


def test_get_security_proxies_mapping():
    from backend.core.duckdb import _clear_schema_cache
    from backend.repositories._base import _schema_cols_cache

    _schema_cols_cache.clear()
    _clear_schema_cache()

    con = duckdb.connect(":memory:")
    src = {"name": "test-service"}
    table_name = _safe_table(src["name"])
    con.execute(
        f"CREATE TABLE {table_name} (ip VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER, "
        "lat DOUBLE, lon DOUBLE, asn INTEGER, timestamp TIMESTAMPTZ)"
    )
    con.execute(f"""
        INSERT INTO {table_name} VALUES
        ('1.1.1.1', 'SJC', 1000, 1000, 37.0, -121.0, 1111, '2026-08-01 12:00:00Z'),
        ('2.2.2.2', 'SJC', 5, 1000, 37.0, -121.0, 2222, '2026-08-01 12:00:00Z'),
        ('3.3.3.3', 'SJC', 10, 1000, 37.0, -121.0, 2222, '2026-08-01 12:00:00Z'),
        ('4.4.4.4', 'SJC', 500, 1000, 37.0, -121.0, 3333, '2026-08-01 12:00:00Z')
    """)
    with patch(
        "backend.core.duckdb.get_asn_names", return_value={1111: "Good ISP", 2222: "Bad ISP 1", 3333: "Mobile ISP"}
    ):
        res = security.get_security_proxies(
            con=con, src=src, start_time="2026-08-01T00:00:00Z", end_time="2026-08-02T00:00:00Z", filters={}
        )
    tq = {item["label"]: item["value"] for item in res["traffic_quality"]}
    assert tq == {"Active Tunnel / Proxy": 50.0, "Direct Connection": 25.0, "WiFi / Mobile": 25.0}
    isps = {item["isp"]: item["count"] for item in res["suspicious_isps"]}
    assert isps.get("Bad ISP 1") == 2 and "Good ISP" not in isps


def _rows(medium: int, high: int) -> list[tuple]:
    return [
        (f"203.0.113.{i}", "SJC", 15000.0, 100000.0, 37.3382, -121.8863, 7922, "US", "San Jose") for i in range(medium)
    ] + [(f"198.51.100.{i}", "SJC", 5000.0, 100000.0, 37.3382, -121.8863, 7922, "US", "San Jose") for i in range(high)]


def _active_clients(rows: list[tuple]) -> list[tuple]:
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min DOUBLE, tcp_rtt DOUBLE, lat DOUBLE, lon DOUBLE, "
        "asn INTEGER, country VARCHAR, city VARCHAR)"
    )
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    sql = SQL.GET_SECURITY_PROXIES_COMBINED.format(
        temp_table="t",
        select_country_city_inner=",\n            country,\n            city",
        select_country_city_outer=",\n        country,\n        city",
    )
    return [row for row in con.execute(sql).fetchall() if row[0] == "client"]


def test_high_risk_clients_are_listed_first_and_survive_the_100_row_cap() -> None:
    result = _active_clients(_rows(medium=150, high=3))
    risks = [row[14] for row in result]
    assert len(result) == 100 and risks[:3] == ["High", "High", "High"] and set(risks[3:]) == {"Medium"}


# ── Defensive Rollup Gate Helpers ────────────────────────────────────────────


@pytest.mark.parametrize(
    "start,end,expected",
    [
        (None, "2026-01-04T00:00:00Z", False),
        ("2026-01-01T00:00:00Z", None, False),
        (None, None, False),
        ("", "2026-01-04T00:00:00Z", False),
        ("2026-01-10T00:00:00Z", "2026-01-01T00:00:00Z", False),
        ("2026-01-01T00:00:00Z", "2026-01-01T23:00:00Z", False),
        ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", True),
        ("2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z", True),
    ],
)
def test_window_eligible_for_rollup_bounds_and_thresholds(start, end, expected):
    assert security._window_eligible_for_rollup(start, end) is expected


@pytest.mark.parametrize("side_effect,return_val", [(ValueError("bad iso"), None), (None, None)])
def test_window_eligible_for_rollup_parse_failures(side_effect, return_val):
    with patch("backend.utils.date_utils.parse_iso_utc", side_effect=side_effect, return_value=return_val):
        assert security._window_eligible_for_rollup("bad", "alsobad") is False


@pytest.mark.parametrize(
    "field,start,end",
    [
        ("bad field", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z"),
        ("is_ipv6", None, "2026-01-04T00:00:00Z"),
        ("is_ipv6", "2026-01-01T00:00:00Z", None),
    ],
)
def test_has_rollup_coverage_rejects_invalid_inputs(tmp_path, field, start, end):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    assert security._has_rollup_coverage(src, field, start, end) is False


def test_has_rollup_coverage_returns_false_when_parse_returns_none(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with patch("backend.utils.date_utils.parse_iso_utc", return_value=None):
        assert security._has_rollup_coverage(src, "is_ipv6", "x", "y") is False


@pytest.mark.parametrize("rel_dir", ["rollups/hour_bundled", "rollups/hour/field=is_ipv6/hour=2026-01-02-12"])
def test_has_rollup_coverage_swallows_oserror_on_listdir(tmp_path, rel_dir):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    target_dir = tmp_path / rel_dir
    target_dir.mkdir(parents=True)
    real_listdir = os.listdir

    def _listdir(p):
        if str(p) == str(target_dir):
            raise OSError("simulated")
        return real_listdir(p)

    with patch("os.listdir", side_effect=_listdir):
        assert security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is False


@contextmanager
def _runner_ctx():
    con = duckdb.connect(":memory:")
    yield QueryRunner(con, {"name": "svc"})
    con.close()


def test_ipv6_per_hour_from_rollups_defensive_none_branches(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with _runner_ctx() as runner:
        assert security._ipv6_per_hour_from_rollups(runner, src, None, "2026-01-04T00:00:00Z") is None
        assert security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", None) is None
        with patch("backend.utils.date_utils.parse_iso_utc", side_effect=ValueError("bad iso")):
            assert security._ipv6_per_hour_from_rollups(runner, src, "bad", "alsobad") is None
        assert security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None

        bundled = tmp_path / "rollups" / "hour_bundled" / "hour=2026-01-02-12"
        bundled.mkdir(parents=True)
        (bundled / "all_fields.parquet").write_bytes(b"not a parquet")
        assert security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None


def test_proxy_dist_from_rollups_defensive_and_sort_branches():
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.side_effect = RuntimeError("rollup pool unavailable")
    assert security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None

    runner.execute_top_n_rollups.side_effect = None
    for empty_rows in ([], [("p_type", "__other__", 100), ("p_type", "", 50), ("p_type", None, 25)]):
        runner.execute_top_n_rollups.return_value = (empty_rows, None)
        assert security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None

    runner.execute_top_n_rollups.return_value = (
        [
            ("p_type", "datacenter", 100),
            ("p_type", "residential", 250),
            ("p_type", "datacenter", 50),
            ("p_type", "__other__", 999),
            ("p_type", "vpn", 75),
        ],
        None,
    )
    assert security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") == [
        {"type": "residential", "count": 250},
        {"type": "datacenter", "count": 150},
        {"type": "vpn", "count": 75},
    ]


# ── NGWAF Verified Bots & Cache Integration ──────────────────────────────────

_WAF_REQ_ID = "05daf2b7aedc405da50c000000000001"
_BOT_NAME = "OpenAI SearchBot"
_CATEGORY = "AI-FETCHER"
_WK_NAME = "OpenAI SearchBot crawler"


def _create_log_table_with_waf(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        "(timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR, waf_req_id VARCHAR, waf_sig VARCHAR)"
    )
    now = datetime.now(UTC)
    con.execute(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
        [now, "1.2.3.4", "GPTBot/1.0", _WAF_REQ_ID, "VERIFIED-BOT,VERIFIED-BOT.AI-FETCHER"],
    )
    con.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)", [now, "5.6.7.8", "curl/7.0", "deadbeef" * 4, None])


def _create_cache_db(db_path: str, *, empty: bool = False) -> None:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ngwaf_bots ("
        "waf_req_id TEXT PRIMARY KEY, bot_name TEXT, category TEXT, "
        "wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    if not empty:
        con.execute(
            "INSERT INTO ngwaf_bots VALUES (?, ?, ?, ?, ?, ?)",
            (
                _WAF_REQ_ID,
                _BOT_NAME,
                _CATEGORY,
                "openai-searchbot",
                _WK_NAME,
                datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        )
    con.commit()
    con.close()


@pytest.fixture
def con_with_waf_logs():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    yield con
    con.close()


def test_ngwaf_verified_bots_and_ts_populated_from_cache(con_with_waf_logs, tmp_path):
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)
    table_name = _safe_table("ngwaf_test_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_test_svc", "service_id": "ngwaf-test"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(con=con_with_waf_logs, src=src, start_time=None, end_time=None, filters={})

    bots = result.get("ngwaf_verified_bots", [])
    assert len(bots) == 1
    assert (bots[0]["bot_name"], bots[0]["wellknown_bot_name"], bots[0]["category"], bots[0]["request_count"]) == (
        _BOT_NAME,
        _WK_NAME,
        _CATEGORY,
        1,
    )
    ts = result.get("ngwaf_verified_bots_ts", [])
    assert len(ts) >= 1 and ts[0]["bot_name"] == _BOT_NAME and ts[0]["count"] == 1


@pytest.mark.parametrize("mode", ["missing_file", "empty_cache", "missing_col"])
def test_ngwaf_fields_empty_on_missing_or_unmatched_state(con_with_waf_logs, tmp_path, mode):
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    if mode == "empty_cache":
        _create_cache_db(db_path, empty=True)
    elif mode == "missing_col":
        _create_cache_db(db_path)

    svc_name = f"ngwaf_{mode}_svc"
    table_name = _safe_table(svc_name)
    if mode == "missing_col":
        con_with_waf_logs.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR)")
        con_with_waf_logs.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)", [datetime.now(UTC), "1.2.3.4", "curl/7.0"]
        )
    else:
        _create_log_table_with_waf(con_with_waf_logs, table_name)

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src={"name": svc_name, "service_id": svc_name},
            start_time=None,
            end_time=None,
            filters={},
        )
    assert result["ngwaf_verified_bots"] == [] and result["ngwaf_verified_bots_ts"] == []


def test_ngwaf_only_request_narrows_temp_to_waf_req_id(con_with_waf_logs, tmp_path, monkeypatch):
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)
    table_name = _safe_table("ngwaf_narrow_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_narrow_svc", "service_id": "ngwaf-narrow"}
    sqls = _capture_runner_sqls(monkeypatch)
    now = datetime.now(UTC)

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_time=(now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            filters={},
            sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
        )

    markers = {e["section"] for e in result["section_timings"]}
    assert "security:temp_ngwaf_narrowed" in markers
    create_sqls = [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s]
    assert create_sqls
    catalog_sql = create_sqls[-1]
    assert "waf_req_id IS NOT NULL" in catalog_sql
    assert '"waf_req_id"' in catalog_sql and '"timestamp"' in catalog_sql
    for absent in ('"ua"', '"ip"', '"tls_ciphers_sha"', '"req_header_bytes"', '"conn_requests"', '"waf_sig"'):
        assert absent not in catalog_sql
    assert len(result["ngwaf_verified_bots"]) == 1 and result["ngwaf_verified_bots"][0]["bot_name"] == _BOT_NAME


def test_top_bots_ngwaf_only_skips_temp_and_joins_direct(con_with_waf_logs, tmp_path, monkeypatch):
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)
    table_name = _safe_table("ngwaf_direct_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_direct_svc", "service_id": "ngwaf-direct"}
    sqls = _capture_runner_sqls(monkeypatch)
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("ua", "GPTBot/1.0", 5)], ["ua"]),
    )

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_top_bots(con=con_with_waf_logs, src=src, start_time=None, end_time=None, filters={})

    assert not [s for s in sqls if "CREATE TEMP TABLE" in s]
    markers = {e["section"] for e in result["section_timings"]}
    assert "top_bots:ngwaf_join_direct" in markers and "top_bots:temp_table_create" not in markers
    assert result["ngwaf_bots"] == [{"name": _BOT_NAME, "category": _CATEGORY, "request_count": 1}]


def test_top_bots_ngwaf_rollup_served_and_miss_fallback(con_with_waf_logs, tmp_path, monkeypatch):
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)
    table_name = _safe_table("ngwaf_rollup_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_rollup_svc", "service_id": "ngwaf-rollup"}
    sentinel = [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 42}]
    sqls = _capture_runner_sqls(monkeypatch)
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("ua", "GPTBot/1.0", 5)], ["ua"]),
    )

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        monkeypatch.setattr(
            QueryRunner, "try_ngwaf_top_bots_from_rollup", lambda self, s, e, *, has_filters, n: list(sentinel)
        )
        hit_res = get_top_bots(con=con_with_waf_logs, src=src, start_time=None, end_time=None, filters={})
        assert hit_res["ngwaf_bots"] == sentinel
        assert not [s for s in sqls if "ngwaf_top.ngwaf_bots" in s]
        hit_markers = {e["section"] for e in hit_res["section_timings"]}
        assert "top_bots:ngwaf_rollup" in hit_markers and "top_bots:ngwaf_join_direct" not in hit_markers

        monkeypatch.setattr(QueryRunner, "try_ngwaf_top_bots_from_rollup", lambda self, s, e, *, has_filters, n: None)
        miss_res = get_top_bots(
            con=con_with_waf_logs,
            src={"name": "ngwaf_rollup_svc", "service_id": "ngwaf-rollup-miss"},
            start_time=None,
            end_time=None,
            filters={},
        )
        assert miss_res["ngwaf_bots"] == [{"name": _BOT_NAME, "category": _CATEGORY, "request_count": 1}]
        assert "top_bots:ngwaf_join_direct" in {e["section"] for e in miss_res["section_timings"]}


def test_blank_ip_rows_are_not_suspicious_clients_or_counted_as_proxies():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, lat DOUBLE, lon DOUBLE, "
        "asn BIGINT, country VARCHAR, city VARCHAR)"
    )
    con.execute("INSERT INTO t VALUES ('10.0.0.2', 'IAD', 1000, 50000, 39.0, -77.1, 200, 'US', 'Reston')")
    con.execute("INSERT INTO t VALUES ('', 'IAD', 1000, 50000, 39.0, -77.1, 700, '', '')")
    rows = con.execute(
        GET_SECURITY_PROXIES_COMBINED.format(
            temp_table="t",
            select_country_city_inner=",\n            country,\n            city",
            select_country_city_outer=",\n                country,\n                city",
        )
    ).fetchall()

    assert [r[1] for r in rows if r[0] == "client"] == ["10.0.0.2"]
    stats = next(r for r in rows if r[0] == "stats")
    assert (stats[2], stats[3]) == (1, 1)
    assert {r[1]: r[2] for r in rows if r[0] == "quality"} == {"Active Tunnel / Proxy": 2}


def test_rollup_reader_drops_blank_ip_tuples(tmp_path):
    base = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base + timedelta(hours=i) for i in range(50)]
    flagged = pa.table(
        {
            "ip": ["10.0.0.2", ""],
            "asn": [200, 700],
            "pop": ["IAD", "IAD"],
            "rtt_min": [1000.0, 1000.0],
            "tcp_rtt": [50000.0, 50000.0],
            "lat": [39.0, 39.0],
            "lon": [-77.1, -77.1],
            "country": ["US", ""],
            "city": ["Reston", ""],
            "cnt": pa.array([1, 1], type=pa.int64()),
        }
    )
    quality = pa.table({"type": ["Active Tunnel / Proxy"], "count": pa.array([2], type=pa.int64())})
    for h in hours:
        d = tmp_path / "rollups" / "hour_bundled" / f"hour={h.strftime('%Y-%m-%d-%H')}"
        d.mkdir(parents=True)
        pq.write_table(flagged, str(d / "security_proxy_flagged.parquet"))
        pq.write_table(quality, str(d / "security_proxy_quality.parquet"))

    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-blank-rollup"})
    st = hours[0].isoformat().replace("+00:00", "Z")
    et = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        rolled = runner.try_security_proxies_from_rollup(st, et, has_filters=False)

    assert rolled is not None
    assert [c["ip"] for c in rolled["clients"]] == ["10.0.0.2"]
    assert rolled["stats"]["active_proxies_count"] == 1


# ── Folded from test_bot_reader_source_observations.py ───────────────────────


@pytest.fixture
def bot_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    con = duckdb.connect(":memory:")
    source = {"name": "bot-observation-test", "service_id": "bot-observation-test"}
    table = _safe_table(source["name"])
    con.execute(f'CREATE TABLE "{table}" (timestamp TIMESTAMPTZ, ua VARCHAR, waf_req_id VARCHAR)')
    con.execute("CREATE TEMP TABLE ngwaf_top_ngwaf_bots (waf_req_id VARCHAR, bot_name VARCHAR, category VARCHAR)")
    con.execute("INSERT INTO ngwaf_top_ngwaf_bots VALUES ('matched', 'ExampleBot', 'Test')")
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(cache))
    monkeypatch.setattr(security, "ensure_ngwaf_bots_materialized", lambda *_: True)
    monkeypatch.setattr(
        security, "classify_ua_counts", lambda _rows, _limit: [{"name": "ExampleBot", "request_count": 1}]
    )
    runner = QueryRunner(con, source)
    monkeypatch.setattr(runner, "execute_top_n_rollups", lambda *_args, **_kwargs: ([("ua", "ExampleBot", 1)], ["ua"]))
    executed: list[str] = []

    def execute(sql: str, params: list | None = None):
        executed.append(sql)
        return con.execute(sql, params) if params is not None else con.execute(sql)

    monkeypatch.setattr(runner, "execute", execute)
    yield con, source, runner, cache, executed
    con.close()


def _bot_bundle(path: Path, count: int = 4) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"bot_name": ["ExampleBot"], "category": ["Test"], "count": pa.array([count], type=pa.int64())}),
        path,
    )


def _bot_get(env: tuple, start: datetime, end: datetime, **extra: Any) -> dict:
    con, source, runner, _, _ = env
    return security.get_top_bots(con, source, start.isoformat(), end.isoformat(), {}, parent_runner=runner, **extra)


def _ngwaf_observation(result: dict) -> dict:
    return next(row for row in result["reader_coverage"] if row["reader"] == "dashboard.ngwaf_bots")


def _bot_phase(result: dict, name: str) -> dict:
    return next(row for row in result["section_timings"] if row["section"] == name)


@pytest.mark.parametrize("tier", ["day", "hour"])
def test_ngwaf_rollup_source_preserves_actual_tier_and_outside_window(bot_reader, tier: str) -> None:
    _, _, _, cache, executed = bot_reader
    start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    if tier == "day":
        _bot_bundle(cache / "rollups" / "day_bundled" / f"day={start:%Y-%m-%d}" / "ngwaf_bots.parquet", 96)
    else:
        for i in range(24):
            _bot_bundle(
                cache
                / "rollups"
                / "hour_bundled"
                / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}"
                / "ngwaf_bots.parquet"
            )
    result = _bot_get(bot_reader, start, start + timedelta(days=1))
    observed = _ngwaf_observation(result)
    assert (
        observed["eligible"] is True and observed["outcome"] == "served" and observed["live_top_up"] == "outside_window"
    )
    attempt = observed["attempts"][0]
    assert attempt["day_files"] == (1 if tier == "day" else 0)
    assert attempt["hour_files"] == (24 if tier == "hour" else 0)
    phase = _bot_phase(result, "top_bots:ngwaf_rollup")
    assert (phase["source"], phase["current_execution"], phase["outcome"]) == ("rollup", True, "success")
    assert result["ngwaf_bots"] == [{"name": "ExampleBot", "category": "Test", "request_count": 96}]
    assert sum("read_parquet" in sql for sql in executed) == 1
    assert not any("INNER JOIN" in sql for sql in executed)


@pytest.mark.parametrize("topup_fails", [False, True])
def test_ngwaf_active_hour_source_is_mixed_and_topup_not_hidden(
    bot_reader, monkeypatch: pytest.MonkeyPatch, topup_fails: bool
) -> None:
    con, _, runner, cache, executed = bot_reader
    active = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = active - timedelta(hours=2)
    for i in range(2):
        _bot_bundle(
            cache / "rollups" / "hour_bundled" / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}" / "ngwaf_bots.parquet"
        )
    event = active + timedelta(seconds=1)
    con.execute(
        f'INSERT INTO "{_safe_table("bot-observation-test")}" VALUES (?, ?, ?)', [event, "ExampleBot", "matched"]
    )
    buffer = cache / "buffer"
    buffer.mkdir()
    pq.write_table(
        pa.table({"timestamp": pa.array([event], type=pa.timestamp("us", tz="UTC")), "waf_req_id": ["matched"]}),
        buffer / "live.parquet",
    )
    if topup_fails:
        execute = runner.execute

        def fail_live(sql: str, params: list | None = None):
            if "INNER JOIN temp.ngwaf_top_ngwaf_bots" in sql:
                executed.append(sql)
                raise duckdb.BinderException("Local top-up fixture failure")
            return execute(sql, params)

        monkeypatch.setattr(runner, "execute", fail_live)
    result = _bot_get(bot_reader, start, active + timedelta(minutes=30))
    observed = _ngwaf_observation(result)
    assert observed["outcome"] == "served"
    assert observed["live_top_up"] == ("failed" if topup_fails else "used")
    assert observed["live_start"] == active.isoformat()
    phase = _bot_phase(result, "top_bots:ngwaf_rollup")
    assert phase["source"] == "mixed" and phase["outcome"] == ("error" if topup_fails else "success")
    assert result["ngwaf_bots"][0]["request_count"] == (8 if topup_fails else 9)
    assert sum("INNER JOIN temp.ngwaf_top_ngwaf_bots" in sql for sql in executed) == 1


@pytest.mark.parametrize("reason", ["window_too_short", "no_coverage", "read_failed"])
def test_ngwaf_live_fallback_keeps_refusal_or_failure_and_actual_source(bot_reader, reason: str) -> None:
    con, _, _, cache, executed = bot_reader
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    end = start + timedelta(hours=1 if reason == "window_too_short" else 3)
    con.execute(
        f'INSERT INTO "{_safe_table("bot-observation-test")}" VALUES (?, ?, ?)',
        [start + timedelta(minutes=1), "ExampleBot", "matched"],
    )
    if reason == "read_failed":
        for i in range(3):
            path = (
                cache
                / "rollups"
                / "hour_bundled"
                / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}"
                / "ngwaf_bots.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"not_bot_name": ["ExampleBot"]}), path)
    result = _bot_get(bot_reader, start, end)
    observed = _ngwaf_observation(result)
    assert observed["outcome"] == ("ineligible" if reason == "window_too_short" else reason)
    assert observed["live_top_up"] == "not_attempted"
    attempt = _bot_phase(result, "top_bots:ngwaf_rollup")
    assert attempt["source"] == ("rollup" if reason == "read_failed" else "unknown")
    assert attempt["outcome"] == ("error" if reason == "read_failed" else "success")
    phase = _bot_phase(result, "top_bots:ngwaf_join_direct")
    assert (phase["source"], phase["outcome"], phase["current_execution"]) == ("live", "success", True)
    assert result["ngwaf_bots"] == [{"name": "ExampleBot", "category": "Test", "request_count": 1}]
    assert sum("INNER JOIN temp.ngwaf_top_ngwaf_bots" in sql for sql in executed) == 1


def test_cached_reader_dispatch_is_historical_after_existing_wire_serialization(bot_reader) -> None:
    _, _, _, cache, _ = bot_reader
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    for i in range(3):
        _bot_bundle(
            cache / "rollups" / "hour_bundled" / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}" / "ngwaf_bots.parquet"
        )
    fresh = _bot_get(bot_reader, start, start + timedelta(hours=3))
    fresh_report = describe_response(SecurityTopBotsResponse(**fresh).model_dump(by_alias=True))
    fresh_phase = next(
        row for row in fresh_report["components"][0]["sections"] if row["section"] == "top_bots:ngwaf_rollup"
    )
    assert (fresh_phase["source_evidence"], fresh_phase["current_execution"]) == ("explicit", True)
    assert fresh_report["components"][0]["reader_coverage"][0]["current_observation"] is True
    cached = _bot_get(bot_reader, start, start + timedelta(hours=3))
    assert cached["_response_cache"]["state"] == "hit"
    report = describe_response(SecurityTopBotsResponse(**cached).model_dump(by_alias=True))
    phase = next(row for row in report["components"][0]["sections"] if row["section"] == "top_bots:ngwaf_rollup")
    assert (phase["source"], phase["source_evidence"], phase["current_execution"]) == ("rollup", "explicit", False)
    assert report["components"][0]["reader_coverage"][0]["current_observation"] is False


def test_filtered_ngwaf_temp_join_reports_live_without_claiming_rollup_eligibility(bot_reader) -> None:
    con, source, runner, _, _ = bot_reader
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    con.execute(
        f'INSERT INTO "{_safe_table(source["name"])}" VALUES (?, ?, ?)',
        [start + timedelta(minutes=1), "ExampleBot", "matched"],
    )
    result = security.get_top_bots(
        con,
        source,
        start.isoformat(),
        (start + timedelta(hours=3)).isoformat(),
        {"ua": {"values": ["ExampleBot"]}, "waf_req_id": {"values": ["matched"]}},
        parent_runner=runner,
    )
    observed = _ngwaf_observation(result)
    assert (observed["eligible"], observed["eligibility_reason"], observed["outcome"]) == (
        False,
        "filtered",
        "ineligible",
    )
    assert observed["attempts"] == [] and observed["live_top_up"] == "not_attempted"
    phase = _bot_phase(result, "top_bots:ngwaf_join")
    assert (phase["source"], phase["outcome"]) == ("live", "success")
    assert result["ngwaf_bots"][0]["request_count"] == 1


def test_failed_live_fallback_is_observed_without_claiming_success(bot_reader) -> None:
    con, _, _, _, _ = bot_reader
    con.execute("DROP TABLE ngwaf_top_ngwaf_bots")
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    result = _bot_get(bot_reader, start, start + timedelta(hours=1))
    observed = _ngwaf_observation(result)
    assert (observed["outcome"], observed["eligibility_reason"]) == ("ineligible", "window_too_short")
    phase = _bot_phase(result, "top_bots:ngwaf_join_direct")
    assert (phase["source"], phase["outcome"], phase["current_execution"]) == ("live", "error", True)
    assert result["ngwaf_bots"] == []
