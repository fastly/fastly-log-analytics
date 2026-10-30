from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import time_machine

import backend.repositories.security as SQL
from backend.core.rollups.security_bots import upsert_bots
from backend.models.analytics import SecurityTopBotsResponse
from backend.models.common import FilterSpec
from backend.repositories import _base, security
from backend.repositories import dashboard as dashboard_repo
from backend.repositories._base import (
    _EMPTY_ROLLUP_WARN_TS,
    _LIVE_TOPN_SKIP_FIELDS,
    QueryRunner,
    _is_stale_view_error,
    _safe_table,
    optional_col,
    safe_iso,
)
from backend.repositories.security import GET_SECURITY_PROXIES_COMBINED, get_security_aggregates, get_top_bots
from backend.utils.bounded_cache import BoundedTTLCache
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


def test_top_bots_cache_peek_matches_real_reader_and_preserves_scope(
    in_memory_duckdb, test_service_source, monkeypatch
):
    now = datetime.now(UTC)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, timestamp=now.isoformat())
    classify = MagicMock(return_value=[{"id": "test-bot", "request_count": 5}])
    monkeypatch.setattr(security, "classify_ua_counts", classify)
    monkeypatch.setattr(dashboard_repo, "DASHBOARD_CACHE_TTL", 30)
    kwargs = {
        "src": test_service_source,
        "start_time": (now - timedelta(minutes=5)).isoformat(),
        "end_time": (now + timedelta(minutes=1)).isoformat(),
        "filters": {},
        "n": 3,
    }
    original = get_top_bots(con=in_memory_duckdb, **kwargs)
    classify.assert_called_once()
    monkeypatch.setattr(security, "QueryRunner", MagicMock(side_effect=AssertionError("cache opened a reader")))
    cached = security.peek_top_bots(**kwargs)
    assert cached is not None
    assert cached["bots"] == original["bots"]
    assert cached["_response_cache"]["state"] == "hit"
    assert get_top_bots(con=None, **kwargs)["bots"] == original["bots"]
    assert security.peek_top_bots(**{**kwargs, "n": 4}) is None
    assert security.peek_top_bots(**{**kwargs, "src": {**test_service_source, "name": "other_bots"}}) is None
    assert (
        security.peek_top_bots(**{**kwargs, "filters": {"country": FilterSpec(mode="include", values=["US"])}}) is None
    )
    assert security.peek_top_bots(**{**kwargs, "start_time": (now - timedelta(minutes=4)).isoformat()}) is None
    assert security.peek_top_bots(**{**kwargs, "start_time": None}) is None
    with pytest.raises(AssertionError, match="cache opened a reader"):
        get_top_bots(con=None, shared_temp_table="external_temp", **kwargs)
    monkeypatch.setattr(dashboard_repo, "DASHBOARD_CACHE_TTL", 0)
    assert security.peek_top_bots(**kwargs) is None


def test_top_bots_caches_valid_empty_window(in_memory_duckdb, test_service_source, monkeypatch):
    now = datetime.now(UTC)
    monkeypatch.setattr(dashboard_repo, "DASHBOARD_CACHE_TTL", 30)
    monkeypatch.setattr(security, "classify_ua_counts", MagicMock(return_value=[]))
    empty_src = {**test_service_source, "name": "empty_bots_svc"}
    _seed_logs(in_memory_duckdb, empty_src, num_logs=2, timestamp=now.isoformat())
    empty_kwargs = {
        "src": empty_src,
        "start_time": (now - timedelta(minutes=5)).isoformat(),
        "end_time": (now + timedelta(minutes=1)).isoformat(),
        "filters": {},
        "n": 3,
    }
    empty_res = get_top_bots(con=in_memory_duckdb, **empty_kwargs)
    assert empty_res["bots"] == [] and empty_res["_response_cache"]["state"] == "miss"
    cached_empty = security.peek_top_bots(**empty_kwargs)
    assert cached_empty is not None and cached_empty["bots"] == [] and cached_empty["_response_cache"]["state"] == "hit"


@pytest.mark.parametrize(
    ("bound", "different_time"),
    [
        ("start_time", "2026-10-27T20:46:06.100000Z"),
        ("start_time", "2026-10-27T20:46:05.100001Z"),
        ("end_time", "2026-10-27T21:46:11.100000Z"),
        ("end_time", "2026-10-27T21:46:10.100001Z"),
    ],
)
def test_top_bots_cache_preserves_exact_bounds(
    monkeypatch: pytest.MonkeyPatch, bound: str, different_time: str
) -> None:
    # Local replacement, not a clear/mutation of the shared module cache.
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=30)
    monkeypatch.setattr(dashboard_repo, "_dashboard_cache", cache)
    monkeypatch.setattr(dashboard_repo, "DASHBOARD_CACHE_TTL", 30)
    reader = MagicMock(side_effect=AssertionError("cache miss opened a reader"))
    monkeypatch.setattr(security, "QueryRunner", reader)
    kwargs: dict[str, Any] = {
        "src": {"name": "exact_bot_window"},
        "start_time": "2026-10-27T20:46:05.100000Z",
        "end_time": "2026-10-27T21:46:10.100000Z",
        "filters": {},
        "n": 3,
    }
    key = security._top_bots_cache_key(**kwargs)
    assert key is not None
    original = {"bots": [{"id": "original-window", "request_count": 5}], "ngwaf_bots": []}
    cache[key] = (0.0, original)
    assert get_top_bots(con=None, **kwargs)["bots"] == original["bots"]
    reader.assert_not_called()

    different = {**kwargs, bound: different_time}
    different_key = security._top_bots_cache_key(**different)
    assert different_key is not None and different_key != key
    assert security.peek_top_bots(**different) is None
    with pytest.raises(AssertionError, match="cache miss opened a reader"):
        get_top_bots(con=None, **different)
    other = {"bots": [{"id": "different-window", "request_count": 2}], "ngwaf_bots": []}
    cache[different_key] = (0.0, other)
    assert get_top_bots(con=None, **different)["bots"] == other["bots"]
    assert get_top_bots(con=None, **kwargs)["bots"] == original["bots"]
    cached = security.peek_top_bots(**kwargs)
    assert cached is not None and cached["_response_cache"]["state"] == "hit"

    for missing in ({"start_time": None}, {"end_time": None}, {"start_time": None, "end_time": None}):
        assert security._top_bots_cache_key(**{**kwargs, **missing}) is None
        assert security.peek_top_bots(**{**kwargs, **missing}) is None
    assert security._top_bots_cache_key(**kwargs, shared_temp_table="external_temp") is None
    monkeypatch.setattr(dashboard_repo, "DASHBOARD_CACHE_TTL", 0)
    assert security._top_bots_cache_key(**kwargs) is None
    assert security.peek_top_bots(**kwargs) is None


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


def _closed_hour_str(hours_back: int) -> str:
    return (datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)).strftime(
        "%Y-%m-%d-%H"
    )


def _seed_count_rollup(cache_root: Path, field: str, hours_back: int, value_counts: dict[str, int]) -> str:
    closed_hour = _closed_hour_str(hours_back)
    count_dir = cache_root / "rollups" / "hour" / f"field={field}" / f"hour={closed_hour}"
    count_dir.mkdir(parents=True, exist_ok=True)
    tbl = pa.table(
        {"value": pa.array(list(value_counts.keys())), "count": pa.array(list(value_counts.values()), type=pa.int64())}
    )
    pq.write_table(tbl, str(count_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"))
    return closed_hour


def _seed_bundled_hour_rollup(cache_root: Path, hours_back: int, field_value_counts: dict[str, dict[str, int]]) -> str:
    closed_hour = _closed_hour_str(hours_back)
    bundle_dir = cache_root / "rollups" / "hour_bundled" / f"hour={closed_hour}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rows = [(f, v, c) for f, vc in field_value_counts.items() for v, c in vc.items()]
    tbl = pa.table(
        {
            "field": [r[0] for r in rows],
            "value": [r[1] for r in rows],
            "count": pa.array([r[2] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(tbl, str(bundle_dir / "all_fields.parquet"))
    return closed_hour


def _wide_window(closed_hour_str: str) -> tuple[str, str]:
    dt = datetime.strptime(closed_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    return (dt - timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%SZ"), (dt + timedelta(hours=2)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# ── Core get_security_aggregates & get_top_bots ──────────────────────────────


def test_security_aggregates_and_top_bots_empty_table(in_memory_duckdb):
    src_sec = {"name": "nonexistent_security_svc", "service_id": "nonexistent_security_svc"}
    res_sec = get_security_aggregates(con=in_memory_duckdb, src=src_sec, start_time=None, end_time=None, filters={})
    assert "debug_queries" in res_sec and "debug_calls" in res_sec
    src_bot = {"name": "nonexistent_bots_svc", "service_id": "nonexistent_bots_svc"}
    res_bot = get_top_bots(con=in_memory_duckdb, src=src_bot, start_time=None, end_time=None, filters={})
    assert (
        res_bot["bots"] == []
        and res_bot["ngwaf_bots"] == []
        and "debug_queries" in res_bot
        and "debug_calls" in res_bot
    )


def test_security_aggregates_returns_required_keys_and_tls_fingerprints(in_memory_duckdb, test_service_source):
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        num_logs=40,
        tls_ciphers_sha=lambda i: "aabbcc11" if i < 20 else "ddeeff22",
    )
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    list_keys = {
        "tls_fingerprints",
        "req_size_dist",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "verified_bots_ts",
    }
    assert (list_keys | {"debug_queries", "debug_calls"}).issubset(result.keys())
    for key in list_keys:
        assert isinstance(result[key], list), f"Expected list for '{key}'"
    for fp in result["tls_fingerprints"]:
        assert {"fingerprint", "ip_count", "request_count"}.issubset(fp.keys())


@pytest.mark.parametrize("num_logs,n", [(30, 10), (50, 1)])
def test_get_top_bots_with_bot_uas_and_n_limit(in_memory_duckdb, test_service_source, num_logs, n):
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=num_logs)
    result = get_top_bots(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}, n=n
    )
    assert isinstance(result, dict) and "bots" in result and "ngwaf_bots" in result and len(result["bots"]) <= n
    if result["bots"]:
        bot = result["bots"][0]
        assert (
            {"id", "category", "request_count"}.issubset(bot.keys())
            and isinstance(bot["request_count"], int)
            and bot["request_count"] > 0
        )


@pytest.mark.parametrize(
    "schema_sql,row_sql,empty_keys",
    [
        ("(timestamp TIMESTAMP, ip VARCHAR, status INTEGER)", "(now(), '1.1.1.1', 200)", ["tls_fingerprints"]),
        ("(timestamp TIMESTAMP, status INTEGER)", "(now(), 200)", ["ngwaf_verified_bots", "ngwaf_verified_bots_ts"]),
    ],
)
def test_security_aggregates_keys_empty_when_cols_absent(
    in_memory_duckdb, test_service_source, schema_sql, row_sql, empty_keys
):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE "{table_name}" {schema_sql}')
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES {row_sql}")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    for k in empty_keys:
        assert result[k] == []


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
        patch("backend.utils.bot_sources.get_hostnames", return_value={}),
        patch("backend.utils.bot_sources.classify", return_value="unverified_pending"),
        patch("backend.utils.bot_sources.enqueue"),
    ):
        result = get_security_aggregates(
            con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
        )
    assert "wellknown_bots" in result


@pytest.mark.parametrize("with_ip_spread,fp_val", [(True, "rollup-served-fp"), (False, "live-served-fp")])
@time_machine.travel("2026-10-28T00:59:59Z", tick=False)
def test_security_aggregates_fingerprints_rollup_and_live_fallback(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch, with_ip_spread, fp_val
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    closed_hour = _seed_count_rollup(cache_root, "tls_ciphers_sha", hours_back=2, value_counts={fp_val: 42})
    closed_hour_dt = datetime.strptime(closed_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        num_logs=20,
        tls_ciphers_sha=fp_val,
        timestamp=(closed_hour_dt + timedelta(minutes=30)).isoformat(),
    )
    if with_ip_spread:
        hll = HyperLogLog()
        hll.update([f"10.0.0.{i}" for i in range(7)])
        ip_dir = cache_root / "rollups" / "hour_ip_spread" / "field=tls_ciphers_sha" / f"hour={closed_hour}"
        ip_dir.mkdir(parents=True)
        tbl = pa.table(
            {
                "value": pa.array([fp_val]),
                "ip_sketch": pa.array([hll.to_bytes()], type=pa.binary()),
                "ip_count_observed": pa.array([7], type=pa.int64()),
                "sample_capped": pa.array([False], type=pa.bool_()),
            }
        )
        pq.write_table(tbl, str(ip_dir / f"compacted_{uuid.uuid4().hex[:12]}.parquet"))
    sqls = _capture_runner_sqls(monkeypatch)
    start = (closed_hour_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (closed_hour_dt + timedelta(hours=1 if with_ip_spread else 2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    by_value = {fp["fingerprint"]: fp for fp in result["tls_fingerprints"]}
    assert by_value[fp_val]["ip_count"] > 0
    assert by_value[fp_val]["request_count"] == (42 if with_ip_spread else 20)
    assert any("count(DISTINCT ip)" in s and '"tls_ciphers_sha"' in s for s in sqls) is (not with_ip_spread)


def test_security_aggregates_fingerprints_skip_rollup_when_filters_present(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, tls_ciphers_sha="fp-A")
    rollup_calls = {"top_n": 0, "ip_spread": 0}
    orig_top_n, orig_ip = QueryRunner.execute_top_n_rollups, QueryRunner.execute_ip_spread_rollups
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


def test_security_aggregates_ipv6_proxy_skip_or_cold_rollup_fallback(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=True, p_type="hosting")
    sqls = _capture_runner_sqls(monkeypatch)
    cold = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert any("CASE WHEN is_ipv6" in s and "FROM t_" in s for s in sqls)
    assert any("SELECT p_type, count(*)" in s and "FROM t_" in s for s in sqls)
    assert cold["ipv6_adoption"] and any(row["type"] == "hosting" for row in cold["proxy_dist"])

    _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 10, "false": 0})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 10})
    filtered = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200"])},
    )
    assert "ipv6_adoption" in filtered and "proxy_dist" in filtered


@pytest.mark.parametrize("small_window", [False, True], ids=["wide_drops_from_temp", "small_keeps_in_temp_and_live"])
def test_security_aggregates_temp_projection_and_window_gate_for_ipv6_ptype(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch, small_window
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=5, is_ipv6=small_window, p_type="hosting")
    closed_hour_str = _seed_count_rollup(
        cache_root, "is_ipv6", hours_back=2, value_counts={"true" if small_window else "false": 50}
    )
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    if small_window:
        start = (datetime.now(UTC) - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        start, end = _wide_window(closed_hour_str)
    sqls = _capture_runner_sqls(monkeypatch)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    catalog_creates = [s for s in sqls if "CREATE TEMP TABLE" in s and "tls_ciphers_sha" in s]
    assert catalog_creates
    catalog_sql = max(catalog_creates, key=lambda s: s.count('"'))
    assert ("is_ipv6" in catalog_sql) is small_window and ("p_type" in catalog_sql) is small_window
    if small_window:
        assert any("CASE WHEN is_ipv6" in s and "FROM t_" in s for s in sqls)
        assert any("SELECT p_type, count(*)" in s and "FROM t_" in s for s in sqls)
        assert "ipv6_adoption" in result and "proxy_dist" in result


@pytest.mark.parametrize(
    "bundled,hours_back,counts,expected_pct",
    [(False, 2, {"true": 23, "false": 77}, 23.0), (True, 3, {"true": 17, "false": 83}, 17.0)],
    ids=["per_field", "bundled_hour"],
)
def test_security_aggregates_ipv6_uses_rollup_when_filters_empty(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch, bundled, hours_back, counts, expected_pct
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=False)
    closed_hour_str = (
        _seed_bundled_hour_rollup(cache_root, hours_back=hours_back, field_value_counts={"is_ipv6": counts})
        if bundled
        else _seed_count_rollup(cache_root, "is_ipv6", hours_back=hours_back, value_counts=counts)
    )
    sqls = _capture_runner_sqls(monkeypatch)
    start, end = _wide_window(closed_hour_str)
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=start, end_time=end, filters={}
    )
    by_time = {row["time"]: row for row in result["ipv6_adoption"]}
    expected_time = f"{closed_hour_str[:10]}T{closed_hour_str[11:13]}:00:00+00:00"
    assert expected_time in by_time and abs(by_time[expected_time]["pct"] - expected_pct) < 0.001
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
    assert abs(by_time[f"{bundled_hour[:10]}T{bundled_hour[11:13]}:00:00+00:00"]["pct"] - 30.0) < 0.001
    assert abs(by_time[f"{per_field_hour[:10]}T{per_field_hour[11:13]}:00:00+00:00"]["pct"] - 50.0) < 0.001


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
        in_memory_duckdb, test_service_source, num_logs=20, tls_ciphers_sha=lambda i: "abc123" if i < 10 else "def456"
    )
    result = get_security_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}, sections=sections
    )
    assert (
        (_ALL_SECTION_KEYS & result.keys()) == expected_present
        and "section_timings" in result
        and "debug_queries" in result
    )


def test_security_aggregates_skips_temp_or_drops_wide_cols_when_rollup_served(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch
):
    cache_root = _use_cache_root(tmp_path, monkeypatch)
    table_name = _seed_logs(in_memory_duckdb, test_service_source, num_logs=10, is_ipv6=False, p_type="hosting")
    closed = _seed_count_rollup(cache_root, "is_ipv6", hours_back=2, value_counts={"true": 23, "false": 77})
    _seed_count_rollup(cache_root, "p_type", hours_back=2, value_counts={"hosting": 50})
    start, end = _wide_window(closed)
    sqls = _capture_runner_sqls(monkeypatch)
    res_skip = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
        sections={"ipv6_adoption"},
    )
    markers = {e["section"] for e in res_skip["section_timings"]}
    assert "temp_table_create" not in markers and "security:temp_skipped" in markers
    assert not [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s] and res_skip["ipv6_adoption"]

    monkeypatch.setattr(
        "backend.core.rollups.read_wellknown_bots_rollup", lambda *a, **k: [("Googlebot/2.1", "66.249.66.1", 5)]
    )
    sqls.clear()
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
    assert (result["req_size_dist"], result["conn_reuse_dist"], result["top_ips_header"]) == (
        [{"bucket": "0-256B", "count": 5}],
        [{"bucket": "1 (None)", "count": 3}],
        [{"ip": "9.9.9.9", "max_header": 900}],
    )
    reads = [row for row in result["section_timings"] if row.get("outcome") == "served"]
    assert len(reads) == 3 and all(row["source"] == "rollup" and row["current_execution"] for row in reads)
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
    rows = (
        [
            (
                "2026-01-02T01:00:00Z",
                f"66.249.66.{i}",
                "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                "",
                10000,
            )
            for i in range(10)
        ]
        + [
            (
                "2026-01-02T01:05:00Z",
                f"198.51.100.{i + 1}",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
                "https://www.google.com/search?q=x",
                5000,
            )
            for i in range(2)
        ]
        + [
            ("2026-01-02T01:10:00Z", f"203.0.113.{i + 1}", "CCBot/2.0 (https://commoncrawl.org/faq/)", "", 20000)
            for i in range(5)
        ]
    )
    in_memory_duckdb.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)", rows)
    result = get_security_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-03T00:00:00Z",
        filters={},
        sections={"bot_efficiency"},
    )
    by_name = {item["bot_name"]: item for item in result["bot_efficiency"]}
    google, ccbot = by_name["Google Crawler"], by_name["Commoncrawl Crawler"]
    assert (
        google["request_count"],
        google["resp_bytes"],
        google["referrals"],
        google["requests_per_referral"],
        google["bytes_per_referral"],
    ) == (10, 100000, 2, 5.0, 50000.0)
    assert (
        ccbot["request_count"],
        ccbot["resp_bytes"],
        ccbot["referrals"],
        ccbot["requests_per_referral"],
        ccbot["bytes_per_referral"],
    ) == (5, 100000, 0, 5.0, 100000.0)


def test_get_security_proxies_mapping_and_100_row_cap():
    from backend.core.duckdb import _clear_schema_cache
    from backend.repositories._base import _schema_cols_cache

    _schema_cols_cache.clear()
    _clear_schema_cache()
    con = duckdb.connect(":memory:")
    src = {"name": "test-service"}
    table_name = _safe_table(src["name"])
    con.execute(
        f"CREATE TABLE {table_name} (ip VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER, lat DOUBLE, lon DOUBLE, asn INTEGER, timestamp TIMESTAMPTZ)"
    )
    con.execute(f"""INSERT INTO {table_name} VALUES
        ('1.1.1.1', 'SJC', 1000, 1000, 37.0, -121.0, 1111, '2026-08-01 12:00:00Z'),
        ('2.2.2.2', 'SJC', 5, 1000, 37.0, -121.0, 2222, '2026-08-01 12:00:00Z'),
        ('3.3.3.3', 'SJC', 10, 1000, 37.0, -121.0, 2222, '2026-08-01 12:00:00Z'),
        ('4.4.4.4', 'SJC', 500, 1000, 37.0, -121.0, 3333, '2026-08-01 12:00:00Z')""")
    with patch(
        "backend.core.duckdb.get_asn_names", return_value={1111: "Good ISP", 2222: "Bad ISP 1", 3333: "Mobile ISP"}
    ):
        res = security.get_security_proxies(
            con=con, src=src, start_time="2026-08-01T00:00:00Z", end_time="2026-08-02T00:00:00Z", filters={}
        )
    assert {item["label"]: item["value"] for item in res["traffic_quality"]} == {
        "Active Tunnel / Proxy": 50.0,
        "Direct Connection": 25.0,
        "WiFi / Mobile": 25.0,
    }
    isps = {item["isp"]: item["count"] for item in res["suspicious_isps"]}
    assert isps.get("Bad ISP 1") == 2 and "Good ISP" not in isps

    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min DOUBLE, tcp_rtt DOUBLE, lat DOUBLE, lon DOUBLE, asn INTEGER, country VARCHAR, city VARCHAR)"
    )
    cap_rows = [
        (f"203.0.113.{i}", "SJC", 15000.0, 100000.0, 37.3382, -121.8863, 7922, "US", "San Jose") for i in range(150)
    ] + [(f"198.51.100.{i}", "SJC", 5000.0, 100000.0, 37.3382, -121.8863, 7922, "US", "San Jose") for i in range(3)]
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", cap_rows)
    sql = SQL.GET_SECURITY_PROXIES_COMBINED.format(
        temp_table="t",
        select_country_city_inner=",\n            country,\n            city",
        select_country_city_outer=",\n        country,\n        city",
    )
    clients = [row for row in con.execute(sql).fetchall() if row[0] == "client"]
    risks = [row[14] for row in clients]
    assert len(clients) == 100 and risks[:3] == ["High", "High", "High"] and set(risks[3:]) == {"Medium"}


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
def test_window_eligible_and_coverage_parse_failures(tmp_path, side_effect, return_val):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with patch("backend.utils.date_utils.parse_iso_utc", side_effect=side_effect, return_value=return_val):
        assert security._window_eligible_for_rollup("bad", "alsobad") is False
        if side_effect is None:
            assert security._has_rollup_coverage(src, "is_ipv6", "x", "y") is False


@pytest.mark.parametrize(
    "field,start,end",
    [
        ("bad field", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z"),
        ("is_ipv6", None, "2026-01-04T00:00:00Z"),
        ("is_ipv6", "2026-01-01T00:00:00Z", None),
    ],
)
def test_has_rollup_coverage_rejects_invalid_inputs(tmp_path, field, start, end):
    assert (
        security._has_rollup_coverage({"name": "svc", "_cache_dir_override": str(tmp_path)}, field, start, end) is False
    )


@pytest.mark.parametrize("rel_dir", ["rollups/hour_bundled", "rollups/hour/field=is_ipv6/hour=2026-01-02-12"])
def test_has_rollup_coverage_swallows_oserror_on_listdir(tmp_path, rel_dir):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    target_dir = tmp_path / rel_dir
    target_dir.mkdir(parents=True)
    real_listdir = os.listdir
    with patch(
        "os.listdir",
        side_effect=lambda p: (
            (_ for _ in ()).throw(OSError("simulated")) if str(p) == str(target_dir) else real_listdir(p)
        ),
    ):
        assert security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is False


def test_ipv6_and_proxy_dist_from_rollups_defensive_branches(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, {"name": "svc"})
        assert security._ipv6_per_hour_from_rollups(runner, src, None, "2026-01-04T00:00:00Z") is None
        assert security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", None) is None
        with patch("backend.utils.date_utils.parse_iso_utc", side_effect=ValueError("bad iso")):
            assert security._ipv6_per_hour_from_rollups(runner, src, "bad", "alsobad") is None
        assert security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None
        bundled = tmp_path / "rollups" / "hour_bundled" / "hour=2026-01-02-12"
        bundled.mkdir(parents=True)
        (bundled / "all_fields.parquet").write_bytes(b"not a parquet")
        assert security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None
    finally:
        con.close()

    mock_runner = MagicMock(spec=QueryRunner)
    mock_runner.execute_top_n_rollups.side_effect = RuntimeError("rollup pool unavailable")
    assert security._proxy_dist_from_rollups(mock_runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None
    mock_runner.execute_top_n_rollups.side_effect = None
    for empty_rows in ([], [("p_type", "__other__", 100), ("p_type", "", 50), ("p_type", None, 25)]):
        mock_runner.execute_top_n_rollups.return_value = (empty_rows, None)
        assert security._proxy_dist_from_rollups(mock_runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is None
    mock_runner.execute_top_n_rollups.return_value = (
        [
            ("p_type", "datacenter", 100),
            ("p_type", "residential", 250),
            ("p_type", "datacenter", 50),
            ("p_type", "__other__", 999),
            ("p_type", "vpn", 75),
        ],
        None,
    )
    assert security._proxy_dist_from_rollups(mock_runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") == [
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
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
        [
            now,
            "1.2.3.4",
            "GPTBot/1.0",
            _WAF_REQ_ID,
            "VERIFIED-BOT,VERIFIED-BOT.AI-FETCHER",
            now,
            "5.6.7.8",
            "curl/7.0",
            "deadbeef" * 4,
            None,
        ],
    )


def _create_cache_db(*, empty: bool = False) -> None:
    """Seed the PostgreSQL ``ngwaf_bots`` cache through the real writer."""
    if empty:
        return
    upsert_bots(
        [
            {
                "waf_req_id": _WAF_REQ_ID,
                "bot_name": _BOT_NAME,
                "category": _CATEGORY,
                "wellknown_bot_id": "openai-searchbot",
                "wellknown_bot_name": _WK_NAME,
            }
        ],
        "ws-test",
        None,
    )


@pytest.fixture
def con_with_waf_logs():
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def test_ngwaf_verified_bots_and_ts_populated_from_cache(con_with_waf_logs):
    _create_cache_db()
    _create_log_table_with_waf(con_with_waf_logs, _safe_table("ngwaf_test_svc"))
    result = get_security_aggregates(
        con=con_with_waf_logs,
        src={"name": "ngwaf_test_svc", "service_id": "ngwaf-test"},
        start_time=None,
        end_time=None,
        filters={},
    )
    bots = result.get("ngwaf_verified_bots", [])
    assert len(bots) == 1 and (
        bots[0]["bot_name"],
        bots[0]["wellknown_bot_name"],
        bots[0]["category"],
        bots[0]["request_count"],
    ) == (
        _BOT_NAME,
        _WK_NAME,
        _CATEGORY,
        1,
    )
    ts = result.get("ngwaf_verified_bots_ts", [])
    assert len(ts) >= 1 and ts[0]["bot_name"] == _BOT_NAME and ts[0]["count"] == 1


@pytest.mark.parametrize("mode", ["empty_cache", "missing_col"])
def test_ngwaf_fields_empty_on_missing_or_unmatched_state(con_with_waf_logs, mode):
    _create_cache_db(empty=(mode == "empty_cache"))
    svc_name = f"ngwaf_{mode}_svc"
    table_name = _safe_table(svc_name)
    if mode == "missing_col":
        con_with_waf_logs.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR)")
        con_with_waf_logs.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)", [datetime.now(UTC), "1.2.3.4", "curl/7.0"]
        )
    else:
        _create_log_table_with_waf(con_with_waf_logs, table_name)
    result = get_security_aggregates(
        con=con_with_waf_logs,
        src={"name": svc_name, "service_id": svc_name},
        start_time=None,
        end_time=None,
        filters={},
    )
    assert result["ngwaf_verified_bots"] == [] and result["ngwaf_verified_bots_ts"] == []


def test_ngwaf_only_request_narrows_temp_to_waf_req_id(con_with_waf_logs, monkeypatch):
    _create_cache_db()
    table_name = _safe_table("ngwaf_narrow_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    sqls = _capture_runner_sqls(monkeypatch)
    now = datetime.now(UTC)
    result = get_security_aggregates(
        con=con_with_waf_logs,
        src={"name": "ngwaf_narrow_svc", "service_id": "ngwaf-narrow"},
        start_time=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_time=(now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        filters={},
        sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
    )
    assert "security:temp_ngwaf_narrowed" in {e["section"] for e in result["section_timings"]}
    create_sqls = [s for s in sqls if "CREATE TEMP TABLE" in s and table_name in s]
    assert create_sqls
    catalog_sql = create_sqls[-1]
    assert "waf_req_id IS NOT NULL" in catalog_sql and '"waf_req_id"' in catalog_sql and '"timestamp"' in catalog_sql
    assert all(
        a not in catalog_sql
        for a in ('"ua"', '"ip"', '"tls_ciphers_sha"', '"req_header_bytes"', '"conn_requests"', '"waf_sig"')
    )
    assert len(result["ngwaf_verified_bots"]) == 1 and result["ngwaf_verified_bots"][0]["bot_name"] == _BOT_NAME


def test_top_bots_ngwaf_rollup_served_miss_fallback_and_direct_join(con_with_waf_logs, monkeypatch):
    _create_cache_db()
    _create_log_table_with_waf(con_with_waf_logs, _safe_table("ngwaf_rollup_svc"))
    sentinel = [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 42}]
    sqls = _capture_runner_sqls(monkeypatch)
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("ua", "GPTBot/1.0", 5)], ["ua"]),
    )
    monkeypatch.setattr(
        QueryRunner, "try_ngwaf_top_bots_from_rollup", lambda self, s, e, *, has_filters, n: list(sentinel)
    )
    hit_res = get_top_bots(
        con=con_with_waf_logs,
        src={"name": "ngwaf_rollup_svc", "service_id": "ngwaf-rollup"},
        start_time=None,
        end_time=None,
        filters={},
    )
    assert hit_res["ngwaf_bots"] == sentinel and not [s for s in sqls if "ngwaf_top.ngwaf_bots" in s]
    hit_markers = {e["section"] for e in hit_res["section_timings"]}
    assert "top_bots:ngwaf_rollup" in hit_markers and "top_bots:ngwaf_join_direct" not in hit_markers

    sqls.clear()
    monkeypatch.setattr(QueryRunner, "try_ngwaf_top_bots_from_rollup", lambda self, s, e, *, has_filters, n: None)
    miss_res = get_top_bots(
        con=con_with_waf_logs,
        src={"name": "ngwaf_rollup_svc", "service_id": "ngwaf-rollup-miss"},
        start_time=None,
        end_time=None,
        filters={},
    )
    assert not [s for s in sqls if "CREATE TEMP TABLE" in s]
    miss_markers = {e["section"] for e in miss_res["section_timings"]}
    assert "top_bots:ngwaf_join_direct" in miss_markers and "top_bots:temp_table_create" not in miss_markers
    assert miss_res["ngwaf_bots"] == [{"name": _BOT_NAME, "category": _CATEGORY, "request_count": 1}]


def test_blank_ip_rows_are_not_suspicious_clients_or_counted_as_proxies():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, lat DOUBLE, lon DOUBLE, asn BIGINT, country VARCHAR, city VARCHAR)"
    )
    con.execute(
        "INSERT INTO t VALUES ('10.0.0.2', 'IAD', 1000, 50000, 39.0, -77.1, 200, 'US', 'Reston'), ('', 'IAD', 1000, 50000, 39.0, -77.1, 700, '', '')"
    )
    rows = con.execute(
        GET_SECURITY_PROXIES_COMBINED.format(
            temp_table="t",
            select_country_city_inner=",\n            country,\n            city",
            select_country_city_outer=",\n                country,\n                city",
        )
    ).fetchall()
    assert [r[1] for r in rows if r[0] == "client"] == ["10.0.0.2"]
    assert next(r for r in rows if r[0] == "stats")[2:4] == (1, 1)
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
    st, et = (
        hours[0].isoformat().replace("+00:00", "Z"),
        (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        rolled = runner.try_security_proxies_from_rollup(st, et, has_filters=False)
    assert (
        rolled is not None
        and [c["ip"] for c in rolled["clients"]] == ["10.0.0.2"]
        and rolled["stats"]["active_proxies_count"] == 1
    )


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


# ── Merged from tests/repositories/test_base.py ──


class TestSafeIso:
    @pytest.mark.parametrize(
        "val,check",
        [
            (None, lambda r: r is None),
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), lambda r: isinstance(r, str) and "2024-06-15" in r),
            (datetime(2024, 6, 15, 12, 0, 0), lambda r: isinstance(r, str) and r.endswith("Z")),
            ("2024-06-15T12:00:00Z", lambda r: r == "2024-06-15T12:00:00Z"),
            (42, lambda r: r == "42"),
        ],
    )
    def test_safe_iso_variants(self, val, check):
        assert check(safe_iso(val))


class TestIsStaleViewError:
    @pytest.mark.parametrize(
        "msg,exc_cls",
        [
            ("No files found in the given path", "IOException"),
            ("No such file or directory: /tmp/buf.parquet", "IOException"),
            ("Catalog Error: Table with name foo does not exist", "CatalogException"),
            ("does not exist in this context", "CatalogException"),
        ],
    )
    def test_stale_messages_return_true(self, msg, exc_cls):
        assert _is_stale_view_error(getattr(duckdb, exc_cls)(msg)) is True

    @pytest.mark.parametrize(
        "msg",
        ["Syntax error near SELECT", "Column 'foo' not found", "Out of memory", ""],
    )
    def test_non_stale_messages_return_false(self, msg):
        assert _is_stale_view_error(duckdb.IOException(msg)) is False
        assert _is_stale_view_error(Exception(msg)) is False

    def test_substring_in_non_duckdb_exception_does_not_trigger_rebuild(self):
        spoofed = duckdb.ConversionException("Conversion failed: value 'No files found' is not a valid INTEGER")
        assert _is_stale_view_error(spoofed) is False
        binder = duckdb.BinderException("Binder Error: Table 'fake' does not exist")
        assert _is_stale_view_error(binder) is False


class TestQueryRunner:
    def test_conn_requests_hist_uses_all_fields_rollup_when_specialized_bundle_is_missing(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        path = tmp_path / "all_fields.parquet"
        pq.write_table(
            pa.table(
                {
                    "field": ["conn_requests", "conn_requests", "conn_requests"],
                    "value": ["1", "7", "22"],
                    "count": [3, 2, 4],
                }
            ),
            path,
        )
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        paths = iter([None, [str(path)]])
        monkeypatch.setattr(runner, "_collect_rollup_paths", lambda *args, **kwargs: next(paths))

        result = runner.try_conn_requests_hist_from_rollup(
            "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", has_filters=False, actual_cols=["conn_requests"]
        )
        assert result == {
            "top": [{"value": "1", "count": 3}, {"value": "6–20", "count": 2}, {"value": "21+", "count": 4}],
            "total": 9,
        }

    def test_execute_tracks_query_and_returns_cursor(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        initial_count = len(runner.debug_queries)
        res = runner.execute("SELECT 42 AS n")
        assert res.fetchone()[0] == 42
        assert len(runner.debug_queries) == initial_count + 1
        last_q = runner.debug_queries[-1]
        assert "SELECT 42" in last_q["sql"] and isinstance(last_q["time_ms"], float)

    def test_execute_self_heals_on_stale_view(self, test_service_source):
        attempts = {"n": 0}

        def flaky_execute(sql, params=None):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise duckdb.IOException("IO Error: No files found that match the pattern .../buffer/batch_x.parquet")
            cursor = MagicMock()
            cursor.fetchone.return_value = (1,)
            return cursor

        fake_con = MagicMock()
        fake_con.execute.side_effect = flaky_execute
        runner = QueryRunner(fake_con, test_service_source)
        refresh_calls = {"n": 0, "force_seen": None}

        def fake_refresh(con, src, force=False, lock_timeout=5.0):
            refresh_calls["n"] += 1
            refresh_calls["force_seen"] = force

        with patch("backend.core.storage.update_storage_view", side_effect=fake_refresh):
            res = runner.execute("SELECT 1")
        assert attempts["n"] == 2 and refresh_calls["n"] == 1 and refresh_calls["force_seen"] is True
        assert res.fetchone()[0] == 1

    def test_execute_reraises_non_stale_and_refresh_failure_errors(self, test_service_source):
        attempts = {"n": 0}

        def always_fail(sql, params=None):
            attempts["n"] += 1
            raise Exception("Parser Error: syntax error at or near 'BLOOP'")

        fake_con = MagicMock()
        fake_con.execute.side_effect = always_fail
        runner = QueryRunner(fake_con, test_service_source)
        with pytest.raises(Exception, match="Parser Error"):
            runner.execute("BLOOP")
        assert attempts["n"] == 1

        fake_con.execute.side_effect = duckdb.IOException("IO Error: No files found at path")
        with patch("backend.core.storage.update_storage_view", side_effect=RuntimeError("rebind failed")):
            with pytest.raises(Exception, match="No files found"):
                runner.execute("SELECT 1")

    def test_execute_with_retry_behaviors(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        assert runner.execute_with_retry("SELECT 99 AS n").fetchone()[0] == 99

        stale_error = duckdb.IOException("No files found in the given path")
        call_count = {"n": 0}
        original_execute = runner.execute

        def flaky_execute(sql, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise stale_error
            return original_execute(sql, params)

        with (
            patch.object(runner, "execute", side_effect=flaky_execute),
            patch("backend.core.storage.update_storage_view"),
        ):
            runner.execute_with_retry("SELECT 1")
        assert call_count["n"] == 2

        with patch.object(runner, "execute", side_effect=Exception("Syntax error near SELECT")):
            with pytest.raises(Exception, match="Syntax error"):
                runner.execute_with_retry("SELECT bad syntax !!!")

    def test_create_temp_table_behaviors(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        ok = runner.create_temp_table("CREATE TEMP TABLE _test_runner_tmp AS SELECT 1 AS x")
        in_memory_duckdb.execute("DROP TABLE IF EXISTS _test_runner_tmp")
        assert ok is True

        stale_error = duckdb.IOException("No files found in the given path")
        with (
            patch.object(runner, "execute", side_effect=stale_error),
            patch("backend.core.storage.update_storage_view"),
        ):
            assert runner.create_temp_table("CREATE TEMP TABLE _unreachable AS SELECT 1") is False

    def test_get_schema_cols_and_self_heal_cache_bust(self, in_memory_duckdb, test_service_source):
        assert isinstance(QueryRunner(in_memory_duckdb, test_service_source).get_schema_cols(), list)

        runner = QueryRunner(MagicMock(), test_service_source)
        call_order: list[str] = []
        get_schema_calls = {"n": 0}

        def fake_get_schema(con, src):
            get_schema_calls["n"] += 1
            return [] if get_schema_calls["n"] == 1 else [{"name": "timestamp"}, {"name": "ip"}, {"name": "status"}]

        with (
            patch("backend.repositories._base._get_schema", side_effect=fake_get_schema),
            patch(
                "backend.core.storage.clear_source_caches",
                side_effect=lambda _k, keep_snapshot_cache=False: call_order.append(
                    f"clear_source_caches(keep_snapshot_cache={keep_snapshot_cache})"
                ),
            ),
            patch(
                "backend.core.storage.update_storage_view",
                side_effect=lambda con, src, force=False, lock_timeout=5.0: call_order.append(
                    f"update_storage_view(force={force})"
                ),
            ),
        ):
            cols = runner.get_schema_cols()

        assert call_order == ["clear_source_caches(keep_snapshot_cache=True)", "update_storage_view(force=True)"]
        assert cols == ["timestamp", "ip", "status"]


class TestOptionalColAndFilteredTempTable:
    @pytest.mark.parametrize(
        "col,actual,default,expected",
        [
            ("ottlb", {"ottlb", "ottfb"}, "NULL", '"ottlb"'),
            ("obytes", {"ottlb"}, "NULL", "NULL"),
            ("elapsed", set(), "0", "0"),
            ("ottfb", ["ottfb", "ottlb"], "NULL", '"ottfb"'),
            ("obytes", ["ottlb"], "NULL", "NULL"),
        ],
    )
    def test_optional_col(self, col, actual, default, expected):
        assert optional_col(col, actual, default=default) == expected

    def test_create_filtered_temp_table_behaviors(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        assert (
            runner.create_filtered_temp_table(
                cols=["nonexistent"], actual_cols=["ts", "status"], source_table="any_table", where_clause="1=1"
            )
            is None
        )

        in_memory_duckdb.execute("CREATE TABLE src_test (ts VARCHAR, status INT)")
        in_memory_duckdb.execute("INSERT INTO src_test VALUES ('2024-01-01', 200)")
        try:
            name = runner.create_filtered_temp_table(
                cols=["ts", "status", "missing_col"],
                actual_cols=["ts", "status"],
                source_table="src_test",
                where_clause="1=1",
            )
            assert name is not None and name.startswith("t_")
            assert in_memory_duckdb.execute(f"SELECT status FROM {name}").fetchone()[0] == 200
            cols = [d[0] for d in in_memory_duckdb.execute(f"DESCRIBE {name}").fetchall()]
            assert "ts" in cols and "status" in cols and "missing_col" not in cols
        finally:
            in_memory_duckdb.execute("DROP TABLE src_test")


class TestExecuteTopNBatchTypesAndDrift:
    @pytest.mark.parametrize(
        "col,col_type,schema_type,values_sql,expected_buckets",
        [
            (
                "ttl",
                "FLOAT",
                "FLOAT",
                "(3600.027), (3600.028), (3600.029), (3600.030), (3601.001)",
                {"3600": 4, "3601": 1},
            ),
            ("age", "FLOAT", "FLOAT", "(0.0), (0.0), (1.0), (1.0), (1.0), (2.0)", {"0": 2, "1": 3, "2": 1}),
            ("status", "INTEGER", "VARCHAR", "(200), (200), (404), (500), (NULL)", {"200": 2, "404": 1, "500": 1}),
            ("country", "VARCHAR", "VARCHAR", "('US'), ('US'), (''), (NULL), ('CA')", {"US": 2, "CA": 1}),
        ],
    )
    def test_top_n_batch_rounding_and_schema_drift(
        self, in_memory_duckdb, test_service_source, col, col_type, schema_type, values_sql, expected_buckets
    ):
        tbl = f"logs_{col}_test"
        in_memory_duckdb.execute(f"CREATE TABLE {tbl} ({col} {col_type})")
        in_memory_duckdb.execute(f"INSERT INTO {tbl} VALUES {values_sql}")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, order = runner.execute_top_n_batch(
                fields=[col], table_name=tbl, actual_cols=[col], schema_types={col: schema_type}
            )
            assert order == [col]
            buckets = {value: count for (_field, value, count) in rows}
            assert buckets == expected_buckets
        finally:
            in_memory_duckdb.execute(f"DROP TABLE {tbl}")

    def test_non_int_aggregate_float_field_is_not_rounded(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_rtt (tcp_rtt DOUBLE)")
        in_memory_duckdb.execute("INSERT INTO logs_rtt VALUES (0.012), (0.013), (0.013)")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_batch(
                fields=["tcp_rtt"], table_name="logs_rtt", actual_cols=["tcp_rtt"], schema_types={"tcp_rtt": "DOUBLE"}
            )
            buckets = {value: count for (_field, value, count) in rows}
            assert "." in next(iter(buckets)) and buckets.get("0.013") == 2
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_rtt")


class TestExecuteTopNBatchPerFieldLimits:
    @staticmethod
    def _patch_rollup_env(
        monkeypatch,
        cache_root,
        cols: list | tuple = ("country",),
        *,
        safe_table: str = "dummy",
        disable_direct: bool = False,
        ensure_hour_dir: bool = True,
    ) -> None:
        schema_defs: list[dict[str, str]] = [{"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"}]
        col_names: list[str] = ["timestamp"]
        for item in cols:
            if isinstance(item, tuple):
                col_names.append(item[0])
                schema_defs.append({"name": item[0], "type": item[1]})
            elif item != "timestamp":
                col_names.append(item)
                schema_defs.append({"name": item, "type": "VARCHAR"})
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: safe_table)
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: list(col_names))
        monkeypatch.setattr("backend.repositories._base._get_schema", lambda _con, _src: list(schema_defs))
        if disable_direct:
            monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
        if ensure_hour_dir:
            (cache_root / "rollups" / "hour").mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _seed_partial_hour_country_fixture(tmp_path, active_dt, active_hour: str) -> None:
        hourly_dir = tmp_path / "data" / f"timestamp_hour={active_hour}"
        hourly_dir.mkdir(parents=True)
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone='UTC';")
            con.execute("CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR)")
            con.executemany(
                "INSERT INTO t VALUES (?, ?)",
                [(active_dt + timedelta(minutes=1), "DE"), (active_dt + timedelta(minutes=1), "DE")],
            )
            con.execute(f"COPY t TO '{hourly_dir / 'batch1.parquet'}' (FORMAT PARQUET)")
        finally:
            con.close()
        watermark_time = (active_dt + timedelta(minutes=15)).timestamp()
        os.utime(hourly_dir / "batch1.parquet", (watermark_time, watermark_time))

    @staticmethod
    def _write_per_hour(cache_root: Path, field: str, hour: str, rows: list[tuple]) -> None:
        d = cache_root / "rollups" / "hour" / f"field={field}" / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "value": pa.array([v for v, _ in rows]),
                    "count": pa.array([c for _, c in rows], type=pa.int64()),
                }
            ),
            str(d / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
        )

    @staticmethod
    def _write_per_day(cache_root: Path, field: str, day: str, rows: list[tuple]) -> None:
        d = cache_root / "rollups" / "day" / f"field={field}" / f"day={day}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "field": pa.array([field for _ in rows]),
                    "value": pa.array([v for v, _ in rows]),
                    "count": pa.array([c for _, c in rows], type=pa.int64()),
                }
            ),
            str(d / "compacted.parquet"),
        )

    @staticmethod
    def _write_bundled_hour(cache_root: Path, hour: str, rows: list[tuple[str, str, int]]) -> None:
        bd = cache_root / "rollups" / "hour_bundled" / f"hour={hour}"
        bd.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "field": [f for f, _, _ in rows],
                    "value": [v for _, v, _ in rows],
                    "count": pa.array([c for _, _, c in rows], type=pa.int64()),
                }
            ),
            str(bd / "all_fields.parquet"),
        )

    @pytest.mark.parametrize(
        "limit,per_field_limits,expected_country,expected_ua",
        [
            (2, {"country": 5}, 5, 2),
            (3, None, 3, 3),
        ],
    )
    def test_per_field_limits_behavior(
        self, in_memory_duckdb, test_service_source, limit, per_field_limits, expected_country, expected_ua
    ):
        in_memory_duckdb.execute("CREATE TABLE logs_pfl (country VARCHAR, ua VARCHAR)")
        rows_sql = ", ".join(f"('c{i}', 'u{i}')" for i in range(10))
        in_memory_duckdb.execute(f"INSERT INTO logs_pfl VALUES {rows_sql}")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, order = runner.execute_top_n_batch(
                fields=["country", "ua"],
                table_name="logs_pfl",
                actual_cols=["country", "ua"],
                schema_types={"country": "VARCHAR", "ua": "VARCHAR"},
                limit=limit,
                per_field_limits=per_field_limits,
            )
            assert order == ["country", "ua"]
            assert len([r for r in rows if r[0] == "country"]) == expected_country
            assert len([r for r in rows if r[0] == "ua"]) == expected_ua
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_pfl")

    @pytest.mark.parametrize(
        "cols",
        [
            ["country"],
            ["country", "timestamp_hour", "dt"],
        ],
    )
    def test_execute_top_n_rollups_uses_direct_active_hour_fast_path(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, cols
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        cache_root = tmp_path / "cache"
        (cache_root / "buffer").mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([active_dt + timedelta(minutes=5)], type=pa.timestamp("us", tz="UTC")),
                    "country": pa.array(["US"]),
                }
            ),
            str(cache_root / "buffer" / "batch_test.parquet"),
        )
        self._patch_rollup_env(monkeypatch, cache_root, cols)

        direct_calls = {"n": 0}
        orig_direct = QueryRunner._create_active_hour_temp_direct
        monkeypatch.setattr(
            QueryRunner,
            "_create_active_hour_temp_direct",
            lambda self, *a, **kw: (direct_calls.__setitem__("n", direct_calls["n"] + 1), orig_direct(self, *a, **kw))[
                1
            ],
        )
        view_fallback_calls = {"n": 0}
        orig_view = QueryRunner.create_filtered_temp_table
        monkeypatch.setattr(
            QueryRunner,
            "create_filtered_temp_table",
            lambda self, *a, **kw: (
                view_fallback_calls.__setitem__("n", view_fallback_calls["n"] + 1),
                orig_view(self, *a, **kw),
            )[1],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        rows, _ = runner.execute_top_n_rollups(
            ["country"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
        )
        assert direct_calls["n"] == 1 and view_fallback_calls["n"] == 0
        assert ("country", "US", 1) in [r for r in rows if r[0] == "country"]

    def test_shared_active_hour_temps_reused_between_count_and_top_n(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        cache_root = tmp_path / "cache"
        (cache_root / "buffer").mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([active_dt + timedelta(minutes=5)], type=pa.timestamp("us", tz="UTC")),
                    "country": pa.array(["US"]),
                    "status": pa.array([200]),
                }
            ),
            str(cache_root / "buffer" / "batch_test.parquet"),
        )
        self._patch_rollup_env(monkeypatch, cache_root, ["country", ("status", "INTEGER"), "timestamp_hour", "dt"])
        (cache_root / "rollups" / "hour_bundled").mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        active_end = active_dt + timedelta(hours=1)
        runner.begin_shared_active_hour_temps()
        try:
            cnt = runner.try_count_from_rollup(
                active_dt.isoformat(),
                active_end.isoformat(),
                "dummy",
                "1=1",
                [],
                unfiltered_window=True,
                projected_fields=["country", "status"],
                actual_cols=["timestamp", "country", "status", "timestamp_hour", "dt"],
            )
            assert cnt == 1 and runner._shared_active_temps is not None and len(runner._shared_active_temps) == 1
            shared_table_name = runner._shared_active_temps[0][3]

            view_fallback_calls = {"n": 0}
            orig_view = QueryRunner.create_filtered_temp_table
            monkeypatch.setattr(
                QueryRunner,
                "create_filtered_temp_table",
                lambda self, *a, **kw: (
                    view_fallback_calls.__setitem__("n", view_fallback_calls["n"] + 1),
                    orig_view(self, *a, **kw),
                )[1],
            )

            rows, _ = runner.execute_top_n_rollups(
                ["country"],
                active_dt.isoformat(),
                active_end.isoformat(),
                limit=10,
                actual_cols=["timestamp", "country", "status", "timestamp_hour", "dt"],
            )
            assert len(runner._shared_active_temps) == 1 and runner._shared_active_temps[0][3] == shared_table_name
            assert runner._last_active_direct_n_files == 0 and view_fallback_calls["n"] == 0
            assert ("country", "US", 1) in [r for r in rows if r[0] == "country"]
        finally:
            runner.end_shared_active_hour_temps()

    def test_execute_top_n_rollups_falls_back_to_view_when_direct_finds_nothing(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        self._patch_rollup_env(monkeypatch, cache_root, ["country"])
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        rows, _ = runner.execute_top_n_rollups(
            ["country"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
        )
        assert [r for r in rows if r[0] == "country"] == []

    def test_execute_top_n_rollups_live_branch_and_clamp_and_skip_fields(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        assert "rid" in _LIVE_TOPN_SKIP_FIELDS and "country" not in _LIVE_TOPN_SKIP_FIELDS
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute("CREATE TABLE logs_live (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_live VALUES (?, 'US', 'r1'), (?, 'US', 'r2'), (?, 'JP', 'r3')",
            [active_dt + timedelta(minutes=10), active_dt + timedelta(minutes=30), active_dt + timedelta(minutes=45)],
        )
        try:
            self._patch_rollup_env(
                monkeypatch, tmp_path, ["country", "rid"], safe_table="logs_live", disable_direct=True
            )
            runner = QueryRunner(in_memory_duckdb, test_service_source)

            # Full active hour: live branch runs, rid skipped
            rows_full, _ = runner.execute_top_n_rollups(
                ["country", "rid"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            country_full = {v: c for f, v, c in rows_full if f == "country"}
            assert country_full == {"US": 2, "JP": 1}
            assert [v for f, v, _ in rows_full if f == "rid"] == []

            # Clamped sub-window [+5m, +35m]: excludes +45m JP row
            rows_clamped, _ = runner.execute_top_n_rollups(
                ["country"],
                (active_dt + timedelta(minutes=5)).isoformat(),
                (active_dt + timedelta(minutes=35)).isoformat(),
                limit=10,
            )
            country_clamped = {v: c for f, v, c in rows_clamped if f == "country"}
            assert country_clamped == {"US": 2} and "JP" not in country_clamped
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_live")

    @pytest.mark.parametrize("seed_pre_watermark_de", [False, True])
    def test_execute_top_n_rollups_partial_hour_merge_and_no_double_count(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, seed_pre_watermark_de
    ):
        import backend.core.rollups.day_bundles as ph

        frozen_now = datetime(2026, 6, 20, 10, 20, 0, tzinfo=UTC)
        active_dt = datetime(2026, 6, 20, 10, 0, 0, tzinfo=UTC)
        active_hour = "2026-06-20-10"
        tbl = "logs_pht_test"

        self._patch_rollup_env(monkeypatch, tmp_path, ["country"], safe_table=tbl, disable_direct=True)
        self._seed_partial_hour_country_fixture(tmp_path, active_dt, active_hour)

        with time_machine.travel(frozen_now, tick=False):
            stats = ph.merge_partial_hour("svc-a", test_service_source, ["country"])
            assert stats["new_files"] == 1

            in_memory_duckdb.execute(f"CREATE TABLE {tbl} (timestamp TIMESTAMPTZ, country VARCHAR)")
            try:
                now = datetime.now(UTC)
                if seed_pre_watermark_de:
                    in_memory_duckdb.execute(
                        f"INSERT INTO {tbl} VALUES (?, 'DE'), (?, 'DE'), (?, 'US'), (?, 'US')",
                        [
                            active_dt + timedelta(seconds=1),
                            active_dt + timedelta(seconds=1),
                            now + timedelta(minutes=1),
                            now + timedelta(minutes=1),
                        ],
                    )
                else:
                    in_memory_duckdb.execute(
                        f"INSERT INTO {tbl} VALUES (?, 'US'), (?, 'US')",
                        [now + timedelta(minutes=1), now + timedelta(minutes=1)],
                    )

                runner = QueryRunner(in_memory_duckdb, test_service_source)
                rows, _ = runner.execute_top_n_rollups(
                    ["country"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
                )
            finally:
                in_memory_duckdb.execute(f"DROP TABLE {tbl}")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert country_counts.get("US") == 2 and country_counts.get("DE") == 2

    def test_execute_top_n_rollups_rollup_path_still_returns_skip_field(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_hour_dt = active_dt - timedelta(hours=1)
        closed_hour_str = closed_hour_dt.strftime("%Y-%m-%d-%H")

        for fld, val, cnt in [("rid", "r_rolled", 7), ("country", "US", 5)]:
            self._write_per_hour(tmp_path, fld, closed_hour_str, [(val, cnt)])

        in_memory_duckdb.execute("CREATE TABLE logs_rollupkeep (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_rollupkeep VALUES (?, 'US', 'r_live'), (?, 'US', 'r_live')",
            [active_dt + timedelta(minutes=5), active_dt + timedelta(minutes=15)],
        )
        try:
            self._patch_rollup_env(
                monkeypatch, tmp_path, ["country", "rid"], safe_table="logs_rollupkeep", disable_direct=True
            )
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_rollups(
                ["country", "rid"], closed_hour_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
            )
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_rollupkeep")

        by_field: dict[str, dict] = {}
        for fld, val, cnt in rows:
            by_field.setdefault(fld, {})[val] = cnt
        assert by_field.get("country", {}).get("US") == 7
        assert by_field.get("rid", {}).get("r_rolled") == 7
        assert "r_live" not in by_field.get("rid", {})

    def test_execute_top_n_rollups_skip_field_with_no_data_does_not_warn(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        _EMPTY_ROLLUP_WARN_TS.clear()
        monkeypatch.setattr(_base, "_EMPTY_ROLLUP_WARN_INTERVAL_S", 0.0)
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute(
            "CREATE TABLE logs_skipwarn (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR, status VARCHAR)"
        )
        in_memory_duckdb.execute(
            "INSERT INTO logs_skipwarn VALUES (?, 'US', NULL, NULL), (?, 'JP', NULL, NULL)",
            [active_dt + timedelta(minutes=5), active_dt + timedelta(minutes=15)],
        )
        try:
            self._patch_rollup_env(
                monkeypatch, tmp_path, ["country", "rid", "status"], safe_table="logs_skipwarn", disable_direct=True
            )
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            captured: list[str] = []
            monkeypatch.setattr(
                _base._logger, "debug", lambda msg, *args, **_kw: captured.append(msg % args if args else str(msg))
            )
            runner.execute_top_n_rollups(
                ["country", "rid", "status"],
                active_dt.isoformat(),
                (active_dt + timedelta(hours=1)).isoformat(),
                limit=10,
            )
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_skipwarn")

        warnings = [m for m in captured if "empty result for field=" in m]
        assert not any("field='rid'" in m for m in warnings)
        assert any("field='status'" in m for m in warnings)

    def test_live_topn_skip_fields_are_not_rendered_dashboard_panels(self):
        repo_root = Path(__file__).resolve().parents[2]
        target_file = repo_root / "frontend" / "hooks" / "useDashboardCardDefinitions.ts"
        assert target_file.is_file()
        text = target_file.read_text()
        rendered: set[str] = set()
        for block in re.findall(r"cardIds:\s*\[(.*?)\]", text, re.DOTALL):
            rendered.update(re.findall(r"'([^']+)'", block))
        assert "country" in rendered and len(rendered) > 30
        assert not (_LIVE_TOPN_SKIP_FIELDS & rendered)

    def test_execute_top_n_rollups_skips_day_file_on_partial_window(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_plus_1 = day_d + timedelta(days=1)
        day_d_str, day_d_plus_1_str = day_d.isoformat(), day_d_plus_1.isoformat()

        self._write_per_day(cache_root, "edge_score", day_d_str, [("in_window_val", 10), ("out_of_window_val", 99)])
        for h in range(17, 24):
            self._write_per_hour(cache_root, "edge_score", f"{day_d_str}-{h:02d}", [("in_window_val", 1)])
        for h in range(0, 18):
            self._write_per_hour(cache_root, "edge_score", f"{day_d_plus_1_str}-{h:02d}", [("in_window_val", 1)])

        self._patch_rollup_env(monkeypatch, cache_root, ["edge_score"], ensure_hour_dir=False)
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = (datetime.combine(day_d, datetime.min.time(), tzinfo=UTC) + timedelta(hours=17, minutes=36)).isoformat()
        et = (
            datetime.combine(day_d_plus_1, datetime.min.time(), tzinfo=UTC) + timedelta(hours=17, minutes=36)
        ).isoformat()
        rows, _ = runner.execute_top_n_rollups(["edge_score"], st, et, limit=10)
        values = {value: count for (field, value, count) in rows if field == "edge_score"}
        assert "out_of_window_val" not in values and values.get("in_window_val", 0) > 0

    def test_execute_top_n_rollups_day_vs_bundled_and_per_field_resolution(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_str = day_d.isoformat()
        day_d_plus_1_str = (day_d + timedelta(days=1)).isoformat()

        # Field A has day file (50); Field B has no day file, only per-hour (24 * 3 = 72); both have bundled-hour (2, 3)
        self._write_per_day(cache_root, "field_a", day_d_str, [("a1", 50)])
        for h_idx in range(24):
            self._write_per_hour(cache_root, "field_b", f"{day_d_str}-{h_idx:02d}", [("b1", 3)])
            self._write_per_hour(cache_root, "field_a", f"{day_d_str}-{h_idx:02d}", [("a1", 2)])
            self._write_bundled_hour(
                cache_root, f"{day_d_str}-{h_idx:02d}", [("field_a", "a1", 2), ("field_b", "b1", 3)]
            )

        self._patch_rollup_env(monkeypatch, cache_root, ["field_a", "field_b"], ensure_hour_dir=False)
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st, et = f"{day_d_str}T00:00:00+00:00", f"{day_d_plus_1_str}T00:00:00+00:00"

        # Single-field day-contained read uses day file without double-counting bundled or per-hour
        rows_a, _ = runner.execute_top_n_rollups(["field_a"], st, et, limit=10)
        assert {v: c for f, v, c in rows_a if f == "field_a"} == {"a1": 50}

        # Multi-field read where field_a has day file and field_b falls back to per-field per-hour
        rows_both, _ = runner.execute_top_n_rollups(["field_a", "field_b"], st, et, limit=10)
        by_field: dict[str, dict] = {}
        for f, v, c in rows_both:
            by_field.setdefault(f, {})[v] = c
        assert by_field.get("field_a", {}).get("a1") == 50
        assert by_field.get("field_b", {}).get("b1") == 72

    def test_execute_top_n_rollups_bundled_branch_scoping_and_per_field_qualify(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d_str = (active_dt - timedelta(days=2)).date().isoformat()

        (cache_root / "rollups" / "hour").mkdir(parents=True, exist_ok=True)
        self._write_bundled_hour(
            cache_root, f"{day_d_str}-05", [("ua", "bot-a", 7), ("ua", "bot-b", 3), ("country", "US", 99)]
        )
        self._patch_rollup_env(monkeypatch, cache_root, ["ua", "country"], ensure_hour_dir=False)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st, et = f"{day_d_str}T05:00:00+00:00", f"{day_d_str}T06:00:00+00:00"

        # Single-field bundled read scopes to requested field
        rows_ua, _ = runner.execute_top_n_rollups(["ua"], st, et, limit=50000, per_field_limits={"ua": 50000})
        assert {(f, v, c) for f, v, c in rows_ua} == {("ua", "bot-a", 7), ("ua", "bot-b", 3)}

        # Multi-field with per_field_limits uses CASE field QUALIFY
        captured_queries: list[str] = []
        orig_execute = runner.execute
        monkeypatch.setattr(
            runner, "execute", lambda q, *a, **kw: (captured_queries.append(q), orig_execute(q, *a, **kw))[1]
        )
        runner.execute_top_n_rollups(["country", "ua"], st, et, limit=10, per_field_limits={"country": 500})
        rolled_queries = [q for q in captured_queries if "GROUP BY field, value QUALIFY" in q]
        assert rolled_queries
        q = rolled_queries[0]
        assert "CASE field" in q and "THEN 500" in q and "ELSE 100 END" in q and q.count("THEN") == 1

    # ── execute_ip_spread_rollups ────────────────────────────────────────────

    @staticmethod
    def _write_per_field_ip_spread(cache_root, field, hour, rows):
        d = os.path.join(str(cache_root), "rollups", "hour_ip_spread", f"field={field}", f"hour={hour}")
        os.makedirs(d, exist_ok=True)
        table = pa.table(
            {
                "value": pa.array([r["value"] for r in rows]),
                "ip_sketch": pa.array([r["ip_sketch"] for r in rows], type=pa.binary()),
                "ip_count_observed": pa.array([r["ip_count_observed"] for r in rows], type=pa.int64()),
                "sample_capped": pa.array([r["sample_capped"] for r in rows], type=pa.bool_()),
            }
        )
        p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
        pq.write_table(table, p)
        return p

    def test_execute_ip_spread_rollups_empty_and_unsafe_fields(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        assert runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha", "ja3"], "2026-05-15T00:00:00Z", "2026-05-15T01:00:00Z"
        ) == ({}, {})
        assert runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha'; DROP TABLE foo; --"], "2026-05-15T10:00:00Z", "2026-05-15T11:00:00Z"
        ) == ({}, {})

    def test_execute_ip_spread_rollups_merge_and_capped_and_active_skip(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

        hll_a, hll_b = HyperLogLog(), HyperLogLog()
        hll_a.update([f"1.1.1.{i}" for i in range(1, 101)])
        hll_b.update([f"1.1.1.{i}" for i in range(51, 151)])
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-10",
            [
                {"value": "abc", "ip_sketch": hll_a.to_bytes(), "ip_count_observed": 100, "sample_capped": False},
                {"value": "capped-fp", "ip_sketch": hll_a.to_bytes(), "ip_count_observed": 5000, "sample_capped": True},
            ],
        )
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-11",
            [{"value": "abc", "ip_sketch": hll_b.to_bytes(), "ip_count_observed": 100, "sample_capped": False}],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        counts, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha"], "2026-05-15T10:00:00Z", "2026-05-15T12:00:00Z"
        )
        merged_estimate = counts[("tls_ciphers_sha", "abc")]
        assert abs(merged_estimate - 150) < abs(merged_estimate - 200) and abs(merged_estimate - 150) <= 25
        assert meta["tls_ciphers_sha"]["coverage_hours"] == 2
        assert meta["tls_ciphers_sha"]["capped_values"] == 1

        # Active hour skip check
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        hll_active, hll_closed = HyperLogLog(), HyperLogLog()
        hll_active.update([f"9.9.9.{i}" for i in range(1, 51)])
        hll_closed.update([f"1.1.1.{i}" for i in range(1, 11)])
        self._write_per_field_ip_spread(
            cache_root,
            "ja3",
            active_dt.strftime("%Y-%m-%d-%H"),
            [{"value": "x", "ip_sketch": hll_active.to_bytes(), "ip_count_observed": 50, "sample_capped": False}],
        )
        self._write_per_field_ip_spread(
            cache_root,
            "ja3",
            (active_dt - timedelta(hours=1)).strftime("%Y-%m-%d-%H"),
            [{"value": "x", "ip_sketch": hll_closed.to_bytes(), "ip_count_observed": 10, "sample_capped": False}],
        )
        counts_act, _ = runner.execute_ip_spread_rollups(
            ["ja3"], (active_dt - timedelta(hours=2)).isoformat(), (active_dt + timedelta(hours=1)).isoformat()
        )
        assert counts_act.get(("ja3", "x"), 0) < 25

    def test_execute_top_n_batch_prevents_sql_injection(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_safe (status VARCHAR)")
        in_memory_duckdb.execute("INSERT INTO logs_safe VALUES ('200'), ('200'), ('500')")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            malicious_field = "status' UNION ALL SELECT 'evil' as field, 'payload' as value, 100 as c --"
            rows, order = runner.execute_top_n_batch(
                fields=[malicious_field, "status"],
                table_name="logs_safe",
                actual_cols=["status"],
                schema_types={"status": "VARCHAR"},
            )
            assert order == ["status"] and len(rows) == 2 and all(row[0] == "status" for row in rows)
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_safe")

    # ── Missing-hour live heal ───────────────────────────────────────────────

    def _heal_test_setup(self, in_memory_duckdb, tmp_path, monkeypatch, table):
        in_memory_duckdb.execute("SET TimeZone='UTC'")
        self._patch_rollup_env(monkeypatch, tmp_path, ["country"], safe_table=table, disable_direct=True)
        return QueryRunner

    def test_execute_top_n_rollups_missing_hour_heal_matrix(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)

        # 1. Unrolled closed hour is healed; mid-hour start/end clamps exclude out-of-window rows
        in_memory_duckdb.execute("CREATE TABLE logs_heal (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_heal VALUES (?, 'US'), (?, 'US'), (?, 'JP')",
            [closed_dt + timedelta(minutes=5), closed_dt + timedelta(minutes=15), closed_dt + timedelta(minutes=45)],
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_heal")
            runner = QueryRunner(in_memory_duckdb, test_service_source)

            # Full closed window
            rows, _ = runner.execute_top_n_rollups(
                ["country"],
                (active_dt - timedelta(hours=3)).isoformat(),
                (active_dt - timedelta(hours=1)).isoformat(),
                limit=10,
            )
            assert {v: c for f, v, c in rows if f == "country"} == {"US": 2, "JP": 1}

            # Mid-hour start (+30m to +60m): only +45m JP inside
            rows_st, _ = runner.execute_top_n_rollups(
                ["country"],
                (closed_dt + timedelta(minutes=30)).isoformat(),
                (closed_dt + timedelta(hours=1)).isoformat(),
                limit=10,
            )
            assert {v: c for f, v, c in rows_st if f == "country"} == {"JP": 1}

            # Mid-hour end (+0m to +30m): only +5m and +15m US inside
            rows_et, _ = runner.execute_top_n_rollups(
                ["country"],
                closed_dt.isoformat(),
                (closed_dt + timedelta(minutes=30)).isoformat(),
                limit=10,
            )
            assert {v: c for f, v, c in rows_et if f == "country"} == {"US": 2}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_heal")

    @pytest.mark.parametrize("coverage_mode", ["bundled", "per_field"])
    def test_execute_top_n_rollups_heal_does_not_double_count_covered_hours(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, coverage_mode
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        hour_token = closed_dt.strftime("%Y-%m-%d-%H")
        in_memory_duckdb.execute("CREATE TABLE logs_healcov (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healcov VALUES (?, 'US'), (?, 'US'), (?, 'US')",
            [closed_dt + timedelta(minutes=5), closed_dt + timedelta(minutes=15), closed_dt + timedelta(minutes=25)],
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healcov")
            if coverage_mode == "bundled":
                self._write_bundled_hour(tmp_path, hour_token, [("country", "US", 5)])
            else:
                self._write_per_hour(tmp_path, "country", hour_token, [("US", 5)])

            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_rollups(
                ["country"], closed_dt.isoformat(), (closed_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            assert {v: c for f, v, c in rows if f == "country"} == {"US": 5}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_healcov")

    @pytest.mark.parametrize("has_day_parquet", [True, False])
    @pytest.mark.parametrize("whole_day", [False, True])
    def test_execute_top_n_rollups_heal_day_compacted_vs_crashed_empty_dir(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, has_day_parquet, whole_day
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        compacted_hour_dt = active_dt - timedelta(hours=26)
        day_token = compacted_hour_dt.strftime("%Y-%m-%d")
        in_memory_duckdb.execute("CREATE TABLE logs_healday (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healday VALUES (?, 'US')", [compacted_hour_dt + timedelta(minutes=10)]
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healday")
            day_dir = tmp_path / "rollups" / "day" / "field=country" / f"day={day_token}"
            day_dir.mkdir(parents=True)
            if has_day_parquet:
                self._write_per_day(tmp_path, "country", day_token, [("US", 99)])
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            start = compacted_hour_dt.replace(hour=0) if whole_day else compacted_hour_dt - timedelta(hours=1)
            end = start + timedelta(days=1) if whole_day else compacted_hour_dt + timedelta(hours=2)
            rows, _ = runner.execute_top_n_rollups(
                ["country"],
                start.isoformat(),
                end.isoformat(),
                limit=10,
            )
            counts = {v: c for f, v, c in rows if f == "country"}
            assert counts == {"US": 99 if whole_day and has_day_parquet else 1}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_healday")

    def test_execute_top_n_rollups_heal_cap_and_failure_and_sentinel(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        from backend.core.rollups.day_bundles import stamp_empty_hour_sentinels

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        beyond_cap_dt = active_dt - timedelta(hours=_base._MISSING_HOUR_HEAL_CAP + 2)
        closed_dt = active_dt - timedelta(hours=2)
        bundled_dt = active_dt - timedelta(hours=3)

        in_memory_duckdb.execute("CREATE TABLE logs_healedge (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healedge VALUES (?, 'US'), (?, 'US')",
            [beyond_cap_dt + timedelta(minutes=10), closed_dt + timedelta(minutes=10)],
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healedge")
            runner = QueryRunner(in_memory_duckdb, test_service_source)

            # 1. Beyond _MISSING_HOUR_HEAL_CAP is not healed
            rows_cap, _ = runner.execute_top_n_rollups(
                ["country"],
                (beyond_cap_dt - timedelta(hours=1)).isoformat(),
                (beyond_cap_dt + timedelta(hours=2)).isoformat(),
                limit=10,
            )
            assert "US" not in {v: c for f, v, c in rows_cap if f == "country"}

            # 2. Sentinel bundle suppresses heal temp creation
            from backend.utils.bounded_cache import CacheRegistry

            empty_dt = active_dt - timedelta(hours=4)
            assert (
                stamp_empty_hour_sentinels("test_service", {"name": "test_service"}, [empty_dt.strftime("%Y-%m-%d-%H")])
                == 1
            )
            CacheRegistry.clear_all()
            calls: list = []
            orig_temp = QueryRunner.create_filtered_temp_table
            monkeypatch.setattr(
                QueryRunner,
                "create_filtered_temp_table",
                lambda self, *a, **k: (calls.append(a), orig_temp(self, *a, **k))[1],
            )
            rows_sent, _ = runner.execute_top_n_rollups(
                ["country"], empty_dt.isoformat(), (empty_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            assert calls == [] and not [r for r in rows_sent if r[0] == "country"]

            # 3. Heal failure never raises and preserves bundled rollup results
            self._write_bundled_hour(tmp_path, bundled_dt.strftime("%Y-%m-%d-%H"), [("country", "JP", 7)])
            CacheRegistry.clear_all()
            monkeypatch.setattr(
                QueryRunner,
                "create_filtered_temp_table",
                lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("temp build exploded")),
            )
            rows_fail, _ = runner.execute_top_n_rollups(
                ["country"], bundled_dt.isoformat(), (closed_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            counts_fail = {v: c for f, v, c in rows_fail if f == "country"}
            assert counts_fail == {"JP": 7}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_healedge")


class TestActiveHourDirectTempSchemaDriftAndTombstones:
    def test_mixed_buffer_schemas_fall_back_to_union_by_name(self, in_memory_duckdb, tmp_path, monkeypatch):
        cache_root = tmp_path / "cache"
        buffer_dir = cache_root / "buffer"
        buffer_dir.mkdir(parents=True)
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        ts = active_start + timedelta(minutes=5)

        pq.write_table(
            pa.table({"timestamp": pa.array([ts], type=pa.timestamp("us", tz="UTC"))}),
            str(buffer_dir / "old_schema.parquet"),
        )
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([ts, ts], type=pa.timestamp("us", tz="UTC")),
                    "brand_new_col": pa.array(["x", "y"], type=pa.string()),
                }
            ),
            str(buffer_dir / "new_schema.parquet"),
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        runner = QueryRunner(in_memory_duckdb, {"name": "drift_svc"})
        tmp = runner._create_active_hour_temp_direct([], ["timestamp"], active_start, active_start + timedelta(hours=1))

        assert tmp is not None
        assert in_memory_duckdb.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0] == 3
        in_memory_duckdb.execute(f'DROP TABLE IF EXISTS "{tmp}"')

    def test_tombstoned_buffer_files_are_not_double_counted(self, in_memory_duckdb, tmp_path, monkeypatch):
        cache_root = tmp_path / "cache"
        buffer_dir = cache_root / "buffer"
        buffer_dir.mkdir(parents=True)
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        hourly_dir = cache_root / "data" / f"timestamp_hour={active_start.strftime('%Y-%m-%d-%H')}"
        hourly_dir.mkdir(parents=True)
        ts = active_start + timedelta(minutes=5)

        def _write(path, n):
            pq.write_table(
                pa.table({"timestamp": pa.array([ts] * n, type=pa.timestamp("us", tz="UTC"))}),
                str(path),
            )

        _write(buffer_dir / "batch_live.parquet", 2)
        _write(buffer_dir / "batch_committed.parquet", 3)
        (buffer_dir / "batch_committed.parquet.consumed-1234567890").touch()
        _write(hourly_dir / "00000-0-committed.parquet", 3)

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        runner = QueryRunner(in_memory_duckdb, {"name": "tombstone_svc"})
        tmp = runner._create_active_hour_temp_direct([], ["timestamp"], active_start, active_start + timedelta(hours=1))

        assert tmp is not None
        assert in_memory_duckdb.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0] == 5
        assert runner._last_active_direct_n_files == 2
        in_memory_duckdb.execute(f'DROP TABLE IF EXISTS "{tmp}"')
