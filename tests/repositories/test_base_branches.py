"""Tests for narrow defensive branches in backend.repositories._base.

The bulk of QueryRunner / rollup-routing coverage lives in test_base.py and
test_security.py. This file targets the cache helpers, error path fallbacks,
and pure-function branches that the existing happy-path tests don't exercise.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.repositories import _base
from backend.repositories._base import (
    QueryRunner,
    _attach_sqlite,
    _cached_listdir,
    clear_listdir_cache,
    clear_schema_cols_cache,
    empty_schema_response,
    error_rate_expr,
    get_source_extent,
    origin_latency_us_expr,
)

# ── clear_schema_cols_cache: per-service filtering ──────────────────────────


def test_clear_schema_cols_cache_per_service_drops_only_matching_entries():
    """A specific service_id only drops cache entries for that service —
    other services' cached schemas remain. Pinned because the
    test-suite uses this to invalidate one service's cache without
    forcing every other service to re-summarize."""
    # Seed the cache with two different services.
    _base._schema_cols_cache[("svc-a", "hash1")] = ["col1", "col2"]
    _base._schema_cols_cache[("svc-b", "hash2")] = ["col3"]
    _base._schema_cols_cache[("svc-a", "hash3")] = ["col4"]  # different format hash same svc

    clear_schema_cols_cache("svc-a")

    # All svc-a entries gone regardless of format_hash; svc-b survives.
    assert ("svc-a", "hash1") not in _base._schema_cols_cache
    assert ("svc-a", "hash3") not in _base._schema_cols_cache
    assert _base._schema_cols_cache.get(("svc-b", "hash2")) == ["col3"]

    # Cleanup so other tests start fresh.
    clear_schema_cols_cache(None)


def test_clear_schema_cols_cache_with_none_clears_everything():
    _base._schema_cols_cache[("svc-x", "h")] = ["c1"]
    _base._schema_cols_cache[("svc-y", "h")] = ["c2"]
    clear_schema_cols_cache(None)
    assert _base._schema_cols_cache == {}


# ── _cached_listdir: OSError + cache full ──────────────────────────────────


def test_cached_listdir_returns_empty_on_oserror(tmp_path):
    """A missing or unreadable path returns [] instead of raising —
    callers treat empty and missing identically when walking rollups."""
    clear_listdir_cache()
    nonexistent = str(tmp_path / "does_not_exist")
    out = _cached_listdir(nonexistent)
    assert out == []


def test_cached_listdir_flushes_cache_when_full(tmp_path):
    """When the cache reaches the max-entries cap, it's flat-cleared
    before the new entry is added. Pinned to prevent unbounded growth
    on a service with high (hour, field) churn."""
    clear_listdir_cache()
    # Fill the cache to the cap.
    cap = _base._LISTDIR_CACHE_MAX_ENTRIES
    for i in range(cap):
        _base._listdir_cache[f"/fake/path/{i}"] = (0.0, [])
    assert len(_base._listdir_cache) == cap

    # Adding one more triggers the flat-clear branch.
    p = str(tmp_path)
    out = _cached_listdir(p)
    assert isinstance(out, list)
    # Cache was cleared then re-populated with just the new entry.
    assert len(_base._listdir_cache) == 1
    assert p in _base._listdir_cache
    clear_listdir_cache()


# ── origin_latency_us_expr: column-presence branches ────────────────────────


def test_origin_latency_us_expr_both_columns_coalesces():
    """The COALESCE form is preferred when both columns exist — ottfb's
    microsecond value wins; ttfb is the scaled fallback."""
    expr = origin_latency_us_expr({"ottfb", "ttfb"})
    assert expr == 'COALESCE("ottfb", "ttfb" * 1000000.0)'


def test_origin_latency_us_expr_ottfb_only_returns_bare_column():
    expr = origin_latency_us_expr({"ottfb"})
    assert expr == '"ottfb"'


def test_origin_latency_us_expr_ttfb_only_scales_to_microseconds():
    """ttfb is stored in seconds; the * 1e6 scale aligns it with ottfb's
    microsecond unit so downstream histograms compare apples-to-apples."""
    expr = origin_latency_us_expr({"ttfb"})
    assert expr == '"ttfb" * 1000000.0'


def test_origin_latency_us_expr_no_columns_returns_null_literal():
    """A schema with neither latency column → 'NULL' literal so the
    aggregate doesn't BinderException; the caller renders an empty
    chart."""
    assert origin_latency_us_expr(set()) == "NULL"


# ── error_rate_expr: filter clause ──────────────────────────────────────────


def test_error_rate_expr_includes_filter_clause_when_provided():
    expr = error_rate_expr(status_col="status", threshold=500, filter_expr="cache = 'MISS'")
    assert "FILTER (cache = 'MISS')" in expr


def test_error_rate_expr_omits_filter_clause_when_blank():
    expr = error_rate_expr()
    assert "FILTER" not in expr


# ── empty_schema_response: dict shape ───────────────────────────────────────


def test_empty_schema_response_merges_extra_fields():
    out = empty_schema_response(rows=[], series=[])
    assert out["has_data"] is False
    assert out["total"] == 0
    assert out["rows"] == []
    assert out["series"] == []


# ── _attach_sqlite: missing file + DETACH failure swallowed ─────────────────


def test_attach_sqlite_yields_false_when_path_missing(tmp_path):
    """A nonexistent SQLite path yields False (the bridge isn't
    available) — caller falls back to a Python-side query."""
    con = duckdb.connect(":memory:")
    try:
        with _attach_sqlite(con, str(tmp_path / "missing.db"), "alias") as attached:
            assert attached is False
    finally:
        con.close()


def test_attach_sqlite_swallows_detach_exception(tmp_path):
    """On exit, if DETACH raises (e.g. extension state corruption), the
    exception is swallowed so the context exit doesn't mask the user's
    code result."""
    # Make a real SQLite file so ATTACH succeeds.
    sqlite_path = tmp_path / "real.db"
    raw = sqlite3.connect(str(sqlite_path))
    raw.execute("CREATE TABLE t (id INTEGER)")
    raw.commit()
    raw.close()

    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite_scanner; LOAD sqlite_scanner")
    real_execute = con.execute
    detach_called = {"n": 0}

    def _patched(sql, *a, **kw):
        if sql.startswith("DETACH"):
            detach_called["n"] += 1
            raise RuntimeError("simulated detach failure")
        return real_execute(sql, *a, **kw)

    try:
        # Wrap con.execute via a wrapper class because duckdb conn.execute is read-only.
        class _ConWrap:
            def execute(self, sql, *a, **kw):
                return _patched(sql, *a, **kw)

            def __getattr__(self, name):
                return getattr(con, name)

        wrap = _ConWrap()
        with _attach_sqlite(wrap, str(sqlite_path), "alias") as attached:
            assert attached is True
        # No exception bubbled — the detach error was swallowed.
        assert detach_called["n"] == 1
    finally:
        con.close()


# ── get_source_extent: cached / live / both-fail paths ─────────────────────


def test_get_source_extent_returns_cached_status_when_available():
    """When svcconfig.get_status returns a cached entry, we use it
    verbatim and skip the live COUNT query."""
    runner = MagicMock(spec=QueryRunner)
    cached = {
        "local_rows": 12345,
        "earliest_log_at": "2026-05-01T00:00:00Z",
        "latest_log_at": "2026-05-02T00:00:00Z",
    }
    with patch("backend.config.get_status", return_value=cached):
        out = get_source_extent(runner, {"name": "svc"}, "log_svc")
    assert out == (12345, "2026-05-01T00:00:00Z", "2026-05-02T00:00:00Z")
    runner.execute.assert_not_called()


def test_get_source_extent_returns_zeros_when_both_queries_fail():
    """When the cache misses AND the COUNT-with-min/max raises AND the
    fallback COUNT-only also raises, return the safe (0, None, None)
    triple rather than 500ing."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute.side_effect = RuntimeError("table missing")
    with patch("backend.config.get_status", return_value=None):
        out = get_source_extent(runner, {"name": "svc"}, "log_svc")
    assert out == (0, None, None)


# ── QueryRunner.execute_with_retry / create_temp_table failure paths ────────


def _stale_view_error() -> Exception:
    """Build an exception that _is_stale_view_error recognises."""
    return duckdb.IOException("No files found: batch_x.parquet")


def test_execute_with_retry_returns_none_when_retry_also_fails():
    """When the original query AND the post-refresh retry both fail with
    stale-view-shaped errors, execute_with_retry returns None so the
    caller can fall back. Non-stale errors re-raise on the first try."""
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc"})
    runner.con = MagicMock()
    runner.con.execute.side_effect = _stale_view_error()

    with patch("backend.core.iceberg.update_iceberg_view"):
        out = runner.execute_with_retry("SELECT 1")
    assert out is None


def test_create_temp_table_returns_false_when_retry_also_fails():
    """A stale-view error on the original CREATE TEMP TABLE → refresh
    → retry → still fails → return False so the caller branches on the
    bool (vs catching an exception)."""
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc"})
    runner.con = MagicMock()
    runner.con.execute.side_effect = _stale_view_error()

    with patch("backend.core.iceberg.update_iceberg_view"):
        out = runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1")
    assert out is False


def test_create_temp_table_reraises_on_non_stale_error():
    """A non-stale exception escapes the wrapper unchanged — the caller
    decides what to do with a real SQL error."""
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc"})
    runner.con = MagicMock()
    runner.con.execute.side_effect = RuntimeError("syntax error")

    with pytest.raises(RuntimeError, match="syntax error"):
        runner.create_temp_table("BAD SQL")


# ── QueryRunner.get_schema_cols: cache hit ─────────────────────────────────


def test_get_schema_cols_cache_hit_skips_summarize():
    """When the format_hash-keyed cache has an entry, get_schema_cols
    returns it verbatim and does NOT call _get_schema (the expensive
    SUMMARIZE-over-view path)."""
    src = {"name": "svc-cache", "service_id": "svc-cache", "log_fields": {"format_hash": "hash-abc"}}
    runner = QueryRunner(duckdb.connect(":memory:"), src)
    _base._schema_cols_cache[("svc-cache", "hash-abc")] = ["timestamp", "status", "url"]

    with patch("backend.repositories._base._get_schema") as mock_get_schema:
        out = runner.get_schema_cols()

    assert out == ["timestamp", "status", "url"]
    mock_get_schema.assert_not_called()
    # Side effect: actual_cols is also populated for subsequent code paths.
    assert runner.actual_cols == {"timestamp", "status", "url"}
    clear_schema_cols_cache(None)
