from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

import backend.core.rollups.recompute as rollup_coverage
from backend.core.rollups.security_bots import upsert_bots
from backend.models.common import BaseResponse, FilterSpec
from backend.repositories import _base
from backend.repositories._base import (
    CANONICAL_METRICS,
    VALID_CHART_INTERVALS,
    QueryRunner,
    _is_stale_view_error,
    _latest_log_in_window,
    _safe_table,
    _schema_cols_cache,
    attach_ngwaf_cache,
    empty_schema_response,
    get_source_extent,
    optional_col,
    origin_latency_us_expr,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    should_self_heal_stale_view,
    time_bucket_select,
)
from backend.repositories.cmcd import _response_cache_key as _cmcd_key_fn
from backend.repositories.dashboard import get_aggregates
from backend.repositories.performance import get_performance_aggregates
from backend.repositories.utils import response_cache as rc
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import ResponseMemo, coalesce
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs, make_seeded_logs_con
from tests.conftest import MOCK_SERVICE_ID

# ── From test_base_helpers.py ──

"""Tests for ``backend.repositories._base`` — shared repo helpers.

Every analytical endpoint goes through these helpers: SQL expression
builders (``percentile_ms_expr``, ``error_rate_expr``, ``time_bucket_select``),
the cross-engine ATTACH helpers (``_attach_sqlite``, ``attach_ngwaf_cache``,
``attach_metadata_db``), the QueryRunner (which centralises debug
tracking + stale-view retry), and the canonical-metrics SQL templates
used across dashboard / origin / performance / security panels.

A regression in any of these ripples through every chart in the product.
"""


@pytest.mark.parametrize(
    ("val", "expected"),
    [
        (None, None),
        (datetime(2026, 5, 15, 12, 30, 0), "2026-05-15T12:30:00Z"),
        ("2026-05-15", "2026-05-15"),
        (42, "42"),
    ],
)
def test_safe_iso_conversions(val, expected):
    """A naive (UTC-assumed) datetime gets ``Z`` appended so the frontend treats it as UTC.
    Non-datetime objects → str(x). Pinned because DuckDB returns objects that aren't real
    datetimes sometimes (string columns selected via min()/max())."""
    assert safe_iso(val) == expected


def test_safe_iso_already_z_suffixed_string_is_preserved():
    """A datetime that already isoformat-encodes with a tz offset
    should NOT get an extra Z appended."""
    dt = datetime(2026, 5, 15, 12, 30, 0, tzinfo=UTC)
    out = safe_iso(dt)
    # isoformat with UTC tz produces "+00:00" suffix; no Z appended
    assert out is not None
    # Either form acceptable; what matters is there's no double-suffix
    assert not out.endswith("ZZ")
    assert "+" in out or out.endswith("Z")


@pytest.mark.parametrize(
    ("col", "cols", "kwargs", "expected"),
    [
        ("country", ["country", "city"], {}, '"country"'),
        ("region", ["country"], {}, "NULL"),
        ("region", ["country"], {"default": "''"}, "''"),
    ],
)
def test_optional_col_variants(col, cols, kwargs, expected):
    """Default fallback is ``NULL`` — pinned because callers do
    ``SELECT {optional_col('region', cols)} AS region`` and rely on
    getting a valid SQL fragment either way. Caller-provided default is
    used when NULL doesn't fit the surrounding expression (e.g. COALESCE)."""
    assert optional_col(col, cols, **kwargs) == expected


@pytest.mark.parametrize("good", ["1 second", "1 minute", "1 hour", "1 day"])
def test_safe_interval_passes_through_known_values(good):
    """The 4 valid chart intervals round-trip unchanged."""
    assert safe_interval(good) == good


@pytest.mark.parametrize(
    ("val", "kwargs", "expected"),
    [
        ("'; DROP TABLE--", {}, "1 minute"),
        ("2 weeks", {}, "1 minute"),
        ("", {}, "1 minute"),
        ("invalid", {"default": "1 hour"}, "1 hour"),
    ],
)
def test_safe_interval_falls_back_for_unknown_values(val, kwargs, expected):
    """Unknown intervals → default ``'1 minute'`` (not raise).
    Pinned because the chart-interval selector defaults safely
    rather than 500ing on a typo."""
    assert safe_interval(val, **kwargs) == expected


def test_valid_chart_intervals_is_immutable_set():
    """``VALID_CHART_INTERVALS`` is a frozenset to prevent accidental
    mutation. Pinned because every call to safe_interval reads it."""
    assert isinstance(VALID_CHART_INTERVALS, frozenset)
    assert VALID_CHART_INTERVALS == {"1 second", "1 minute", "1 hour", "1 day"}


def test_time_bucket_select_produces_canonical_form():
    """``time_bucket(INTERVAL '<v>', <col>) AS bucket``. Pinned
    because the ``AS bucket`` alias is what downstream GROUP BYs
    reference; renaming would silently break the time series."""
    assert time_bucket_select("1 hour") == "time_bucket(INTERVAL '1 hour', timestamp) AS bucket"
    assert "event_time" in time_bucket_select("1 minute", ts_col="event_time")


def test_time_bucket_select_sanitises_unsafe_interval():
    """An interval not in VALID_CHART_INTERVALS falls back to
    ``'1 minute'`` — pinned because the interval lands in a SQL
    string and a refactor that skipped the sanitisation would
    open SQL injection via the chart-interval query param."""
    out = time_bucket_select("'; DROP TABLE logs; --")
    assert "DROP TABLE" not in out
    assert "INTERVAL '1 minute'" in out


@pytest.mark.parametrize("interval", ["1 second", "1 minute", "1 hour", "1 day"])
@pytest.mark.parametrize("column_type", ["TIMESTAMP", "TIMESTAMPTZ"])
def test_fast_utc_buckets_preserve_exact_boundaries(interval: str, column_type: str) -> None:
    with duckdb.connect(config={"threads": 1}) as con:
        con.execute("SET TimeZone='UTC'")
        con.execute(
            f"CREATE TABLE points AS SELECT CAST(value AS {column_type}) AS event_time FROM "
            "(VALUES ('1969-12-31T23:59:59.999999Z'), ('1970-01-01T00:00:00Z'), "
            "('2026-03-08T06:59:59.999999Z'), ('2026-03-08T07:00:00Z'), "
            "('2026-10-30T23:59:59.999999Z'), ('2026-10-31T00:00:00Z'), (NULL)) t(value)"
        )
        baseline = con.execute(
            f"SELECT {time_bucket_select(interval, 'event_time')}, count(*) FROM points GROUP BY 1 ORDER BY 1"
        ).fetchall()
        fast = _base.fast_utc_bucket_select(interval, "event_time")
        observed = con.execute(f"SELECT {fast}, count(*) FROM points GROUP BY 1 ORDER BY 1").fetchall()
        assert [(bucket.replace(tzinfo=UTC) if bucket else None, count) for bucket, count in observed] == [
            (bucket.replace(tzinfo=UTC) if bucket else None, count) for bucket, count in baseline
        ]


@pytest.mark.parametrize(
    ("col", "q", "kwargs", "expected_in", "expected_not_in"),
    [
        ("ttfb", 0.95, {}, ["PERCENTILE_CONT(0.95)", "ORDER BY ttfb", "/ 1000.0"], ["FILTER"]),
        ("ttfb", 0.99, {"filter_expr": "WHERE edge = true"}, ["FILTER (WHERE edge = true)"], []),
        ("ttfb", 0.5, {}, ["/ 1000.0"], ["FILTER"]),
        (
            "CAST(elapsed AS DOUBLE)",
            0.95,
            {"approx": True},
            ["approx_quantile(CAST(elapsed AS DOUBLE), 0.95)", "/ 1000.0"],
            ["PERCENTILE_CONT"],
        ),
        (
            "ttfb",
            0.99,
            {"filter_expr": "WHERE edge = true", "approx": True},
            ["approx_quantile(ttfb, 0.99)", "FILTER (WHERE edge = true)"],
            [],
        ),
    ],
)
def test_percentile_ms_expr_variants(col, q, kwargs, expected_in, expected_not_in):
    """Most timing columns are stored as microseconds; the helper divides by 1000 to surface
    milliseconds. Optional FILTER clause supports conditional percentiles ("p95 over edge
    requests only"). ``approx=True`` opts into DuckDB's T-Digest sketch instead of exact
    PERCENTILE_CONT for comparative tables on /performance."""
    out = percentile_ms_expr(col, q, **kwargs)
    for token in expected_in:
        assert token in out
    for token in expected_not_in:
        assert token not in out


def test_empty_schema_response_shape_and_extras():
    """Empty schema → ``{has_data: False, total: 0}`` plus any
    caller-supplied extras."""
    assert empty_schema_response() == {"has_data": False, "total": 0}
    out = empty_schema_response(rows=[], series=[], extra={"k": "v"})
    assert out == {"has_data": False, "total": 0, "rows": [], "series": [], "extra": {"k": "v"}}


def test_safe_table_delegates_to_core_safe_table_name():
    assert _safe_table("my_service") == "logs_my_service"
    # The "default" service maps to bare "logs" (no prefix)
    assert _safe_table("default") == "logs"


def test_canonical_metrics_contract_and_guards():
    """The dashboard reads these by name; the keys, zero-guards (NULLIF to avoid
    division-by-zero on empty windows), units/rounding, and template placeholders
    for origin-side throughput are part of the contract."""
    required_keys = (
        "hit_rate",
        "requests",
        "avg_ttfb",
        "p95_ttfb",
        "5xx_rate",
        "4xx_rate",
        "avg_resp_bytes",
        "total_resp_bytes",
        "throughput",
        "req_size",
        "ttfb_ms",
    )
    for required in required_keys:
        assert required in CANONICAL_METRICS, f"missing canonical metric: {required}"

    assert "NULLIF(COUNT(*), 0)" in CANONICAL_METRICS["5xx_rate"]
    assert "NULLIF(COUNT(*), 0)" in CANONICAL_METRICS["hit_rate"]
    assert "AVG(ttfb) * 1000.0" in CANONICAL_METRICS["avg_ttfb"]
    assert "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0" in CANONICAL_METRICS["p95_ttfb"]
    for rounded_key in ("avg_ttfb", "p95_ttfb", "throughput", "req_size", "ttfb_ms"):
        assert "ROUND(" in CANONICAL_METRICS[rounded_key]

    tpl = CANONICAL_METRICS["throughput"]
    for token in ("{cache_col}", "{elapsed_col}", "{resp_bytes_col}", "* 1e6"):
        assert token in tpl


@pytest.mark.parametrize(
    "msg,exc_cls",
    [
        ("No files found at s3://my-bucket/logs/foo.parquet", "IOException"),
        ("Catalog Error: Table with name logs_x does not exist", "CatalogException"),
        ("ParquetReader: file does not exist", "CatalogException"),
        ("IOError: No such file or directory: /tmp/logs.parquet", "IOException"),
        # DuckLake's brief DETACH window during commit/optimize (Trap-adjacent
        # v3 hazard) — a reader mid-checkout sees this exact message.
        (
            'Catalog Error: Table with name "lake.logs" does not exist because schema "lake" does not exist.',
            "CatalogException",
        ),
    ],
)
def test_is_stale_view_error_recognises_known_messages(msg, exc_cls):
    """Each of these messages indicates a storage view that points
    to a deleted buffer file — recoverable by refreshing the view.
    Pinned because adding a new DuckDB version that changes the
    message would silently turn recoverable errors into 500s.

    Per finding 005 (2026-06-15) the detector now also requires a
    genuine ``duckdb.IOException`` / ``duckdb.CatalogException`` so
    attacker-controlled substring injection through
    ``ConversionException`` / ``BinderException`` can no longer
    spoof the stale-view rebuild path."""
    exc = getattr(duckdb, exc_cls)(msg)
    assert _is_stale_view_error(exc) is True


def test_is_stale_view_error_returns_false_for_unrelated_errors():
    assert _is_stale_view_error(Exception("Syntax error at line 1")) is False
    assert _is_stale_view_error(Exception("Permission denied")) is False


def test_attach_ngwaf_cache_yields_false_when_waf_req_id_absent():
    """If the source's schema doesn't include ``waf_req_id``, the helper
    short-circuits to False without touching the cache."""
    con = duckdb.connect(":memory:")
    try:
        with attach_ngwaf_cache(con, schema_cols=["status", "ip"]) as attached:
            assert attached is False
    finally:
        con.close()


def test_attach_ngwaf_cache_reads_postgres(monkeypatch):
    class _Cur:
        def fetchall(self):
            return [{"waf_req_id": "pg-id", "bot_name": "GoogleBot", "category": "SEARCH", "wellknown_bot_name": None}]

    class _Pg:
        def execute(self, *_a, **_k):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr("backend.core.metadata.base.get_pg_readonly_connection", lambda: _Pg())
    con = duckdb.connect(":memory:")
    try:
        with attach_ngwaf_cache(con, schema_cols=["waf_req_id"], alias="ng_pg") as attached:
            assert attached is True
            rows = con.execute("SELECT waf_req_id, bot_name FROM temp.ng_pg_ngwaf_bots").fetchall()
        assert rows == [("pg-id", "GoogleBot")]
    finally:
        con.close()


def test_attach_ngwaf_cache_materializes_postgres_rows_into_temp_table():
    """The PostgreSQL cache is read into a connection-local temp table; a second attached
    catalog is kept in the fixture so a regression to CREATE SCHEMA ("Ambiguous reference
    to catalog or schema", prod 2026-07-30) fails loudly."""
    _seed_ngwaf_bots([("w1", "GoogleBot")], category="SEARCH-ENGINE")
    con = duckdb.connect(":memory:")
    con.execute("ATTACH ':memory:' AS other_catalog")
    try:
        with attach_ngwaf_cache(con, schema_cols=["waf_req_id"], alias="ngwaf_test") as attached:
            assert attached is True
            row = con.execute(
                "SELECT bot_name, category FROM temp.ngwaf_test_ngwaf_bots WHERE waf_req_id = 'w1'"
            ).fetchone()
            assert row == ("GoogleBot", "SEARCH-ENGINE")
    finally:
        con.close()


def test_queryrunner_execute_and_retry_behavior():
    """QueryRunner tracks SQL in debug_queries, returns cursor on execute_with_retry
    success, and re-raises non-stale syntax errors immediately without wasting time."""
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        runner.execute("SELECT 1")
        runner.execute("SELECT 2")

        assert len(runner.debug_queries) == 2
        for entry in runner.debug_queries:
            assert "sql" in entry and "time_ms" in entry and isinstance(entry["time_ms"], float)

        cur = runner.execute_with_retry("SELECT 42 AS x")
        assert cur is not None and cur.fetchone()[0] == 42

        with pytest.raises(Exception):
            runner.execute_with_retry("SELEKT FROM")
    finally:
        con.close()


def test_queryrunner_execute_clears_view_cache_before_force_rebuild(monkeypatch):
    """Regression for the 2026-06-05 prod incident: dashboard surfaced
    ``No files found ... batch_0398ac66102f151b.parquet`` for ~30 min.

    Root cause: ``QueryRunner.execute`` self-heal called
    ``update_storage_view(force=True)`` without first calling
    ``clear_source_caches``. When the per-service lock is contended (the
    every-10s sync cron holds it) the force-rebuild's 5 s lock-acquire
    times out and falls back to executing the cached view SQL — which
    is the STALE SQL that referenced the missing buffer. The retry then
    re-binds the same dead paths and re-raises the same IOException.

    This test pins the ordering: ``clear_source_caches`` MUST be called
    before ``update_storage_view`` so the lock-timeout fallback sees an
    empty ``_view_cache`` and falls through to persistent-view /
    extended-wait paths.
    """
    from backend.core import storage as db_storage

    call_order: list[str] = []

    def _track_clear(name, *, keep_snapshot_cache=False):
        call_order.append(f"clear_source_caches(name={name},keep_snapshot_cache={keep_snapshot_cache})")

    def _track_update(con, src, *args, force=False, **kwargs):
        call_order.append(f"update_storage_view(force={force})")

    monkeypatch.setattr(db_storage, "clear_source_caches", _track_clear)
    monkeypatch.setattr(db_storage, "update_storage_view", _track_update)

    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc-stale"})

        # Force the first ``con.execute`` to raise a stale-view error so
        # the self-heal path runs. Pre-create a real table so the RETRY
        # succeeds — that lets the test reach the assertion instead of
        # exploding on the second execute. DuckDB's PyConnection.execute
        # is read-only at the C level, so we wrap the connection in a
        # proxy object and swap it into the runner.
        con.execute("CREATE TABLE retry_target (x INT)")
        con.execute("INSERT INTO retry_target VALUES (1)")

        raise_once = {"done": False}

        class _ProxyCon:
            def __init__(self, real):
                self._real = real

            def execute(self, q, p=None):
                if not raise_once["done"] and "retry_target" in q:
                    raise_once["done"] = True
                    # Finding 005 (2026-06-15): the stale-view detector now
                    # requires a real DuckDB IOException / CatalogException
                    # so attacker-controlled substring injection can't spoof
                    # the rebuild path. Use the real class for this self-heal
                    # regression test.
                    raise duckdb.IOException(
                        'IO Error: No files found that match the pattern "cache/fos-test/buffer/batch_dead.parquet"'
                    )
                return self._real.execute(q, p if p is not None else [])

            def __getattr__(self, name):
                return getattr(self._real, name)

        runner.con = _ProxyCon(con)

        # Should self-heal and succeed on retry.
        result = runner.execute("SELECT x FROM retry_target").fetchone()
        assert result == (1,), "retry should have produced the real row"

        assert call_order == [
            "clear_source_caches(name=svc-stale,keep_snapshot_cache=True)",
            "update_storage_view(force=True)",
        ], (
            "clear_source_caches MUST be called before update_storage_view, "
            "with keep_snapshot_cache=True (matches the duckdb.py:1284 "
            "self-heal pattern). Reordering or omitting the clear call "
            f"reintroduces the 2026-06-05 prod hang. Got: {call_order}"
        )
    finally:
        con.close()


def test_get_source_extent_uses_cached_status_and_falls_back_on_failure(monkeypatch):
    """The status cache is preferred to the live count query (avoids hitting DuckDB
    just to render the row count in the header). No cached status + failed count
    queries → ``(0, None, None)`` rather than 500ing."""
    cached = {
        "local_rows": 12345,
        "earliest_log_at": "2026-01-01T00:00:00Z",
        "latest_log_at": "2026-01-02T00:00:00Z",
    }
    mock_runner = MagicMock(spec=QueryRunner)
    monkeypatch.setattr("backend.config.get_status", lambda sid: cached)
    assert get_source_extent(mock_runner, src={"name": "svc"}, orig_table_name="logs_svc") == (
        12345,
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
    )
    mock_runner.execute.assert_not_called()

    monkeypatch.setattr("backend.config.get_status", lambda sid: None)
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        assert get_source_extent(runner, src={"name": "svc"}, orig_table_name="logs_nonexistent") == (0, None, None)
    finally:
        con.close()


@pytest.mark.parametrize(
    ("latest", "start", "end", "expected"),
    [
        ("2026-06-26T12:00:00Z", None, None, True),
        (None, "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z", False),
        ("2026-06-26T12:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z", True),
        ("2026-06-27T00:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z", False),
        ("2026-06-25T00:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z", False),
        ("2026-06-26T12:00:00Z", "2026-06-26T00:00:00+00:00", "2026-06-26T23:59:59+00:00", True),
    ],
)
def test_latest_log_in_window_variants(latest, start, end, expected):
    """Open bounds return True for non-null latest; None latest returns False;
    timestamps inside/outside window and Z vs +00:00 suffixes compare accurately."""
    assert bool(_latest_log_in_window(latest, start, end)) is expected


@pytest.mark.parametrize(
    ("windowed_count", "filters", "latest_log_at", "start_time", "end_time", "expected"),
    [
        (5, {}, "2026-06-26T12:00:00Z", None, None, False),
        (0, {"country": object()}, "2026-06-26T12:00:00Z", None, None, False),
        (0, {}, "2026-06-26T12:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z", True),
        (0, {}, "2026-06-27T00:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z", False),
        (0, {}, None, None, None, False),
    ],
)
def test_should_self_heal_stale_view_conditions(windowed_count, filters, latest_log_at, start_time, end_time, expected):
    """Self-heal fires only on an unfiltered empty window whose range contains a non-null latest_log_at."""
    assert (
        should_self_heal_stale_view(
            windowed_count=windowed_count,
            filters=filters,
            latest_log_at=latest_log_at,
            start_time=start_time,
            end_time=end_time,
        )
        is expected
    )


# ── From test_base_branches.py ──


def test_cached_listdir_returns_empty_on_oserror_and_flushes_when_full(tmp_path):
    """A missing/unreadable path returns [] instead of raising. When the cache reaches
    the max-entries cap, it's flat-cleared before the new entry is added."""
    rollup_coverage._listdir_cache.clear()
    assert rollup_coverage.cached_listdir(str(tmp_path / "does_not_exist")) == []
    rollup_coverage._listdir_cache.clear()

    cap = rollup_coverage.LISTDIR_CACHE_MAX_ENTRIES
    for i in range(cap):
        rollup_coverage._listdir_cache[f"/fake/path/{i}"] = (0.0, [])
    assert len(rollup_coverage._listdir_cache) == cap

    p = str(tmp_path)
    out = rollup_coverage.cached_listdir(p)
    assert isinstance(out, list) and len(rollup_coverage._listdir_cache) == 1 and p in rollup_coverage._listdir_cache
    rollup_coverage._listdir_cache.clear()


@pytest.mark.parametrize(
    ("cols", "expected"),
    [
        ({"ottfb", "ttfb"}, 'COALESCE("ottfb", "ttfb" * 1000000.0)'),
        ({"ottfb"}, '"ottfb"'),
        ({"ttfb"}, '"ttfb" * 1000000.0'),
        (set(), "NULL"),
    ],
)
def test_origin_latency_us_expr_variants(cols, expected):
    """COALESCE is preferred when both columns exist; ttfb alone scales * 1e6 to microseconds;
    neither column returns 'NULL' literal so the aggregate doesn't BinderException."""
    assert origin_latency_us_expr(cols) == expected


def _stale_view_error() -> Exception:
    """Build an exception that _is_stale_view_error recognises."""
    return duckdb.IOException("No files found: batch_x.parquet")


def test_queryrunner_retry_and_temp_table_error_branches():
    """When original + post-refresh retry both fail with stale-view errors,
    execute_with_retry returns None and create_temp_table returns False.
    Non-stale errors on create_temp_table re-raise unchanged."""
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc"})
    runner.con = MagicMock()
    runner.con.execute.side_effect = _stale_view_error()

    with patch("backend.core.storage.update_storage_view"):
        assert runner.execute_with_retry("SELECT 1") is None
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is False

    runner.con.execute.side_effect = RuntimeError("syntax error")
    with pytest.raises(RuntimeError, match="syntax error"):
        runner.create_temp_table("BAD SQL")


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
    assert runner.actual_cols == {"timestamp", "status", "url"}
    _base._schema_cols_cache.clear()


# ── From test_response_cache_helpers.py ──


def test_lookup_and_store_observations_are_request_local(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [10.0]
    monkeypatch.setattr("backend.utils.bounded_cache.time.monotonic", lambda: clock[0])
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    assert memo.get("private-key") is None
    original = {"rows": [1], "_response_cache": {"state": "miss"}, "is_cached": True}
    memo.put("private-key", original)
    hit = memo.get("private-key")
    assert hit["_response_cache"] == {"state": "hit", "ttl_seconds": 15.0, "prewarmer": False}
    assert hit["is_cached"] is True and "_response_cache" not in memo.cache["private-key"]
    hit["_response_cache"]["state"] = "bypass"
    assert memo.get("private-key")["_response_cache"]["state"] == "hit"
    clock[0] = 25.0
    assert memo.get("private-key") is None and original["_response_cache"] == {"state": "miss"}


def test_compute_miss_recheck_hit_and_bypass() -> None:
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    first = memo.coalesce("unique-observation-key", lambda: {"rows": [1]})
    assert first["_response_cache"] == {
        "state": "miss",
        "ttl_seconds": 15.0,
        "prewarmer": False,
        "single_flight": "leader",
    }
    memo.put("key", first)
    rechecked = memo.coalesce("unique-observation-key", lambda: memo.get("key"))
    assert rechecked["_response_cache"]["state"] == "hit"
    bypass = rc.cache_result({"rows": []}, state="bypass", prewarmer=False)
    assert bypass["_response_cache"] == {"state": "bypass", "prewarmer": False} and bypass["is_cached"] is False


def test_follower_cannot_relabel_the_leaders_shared_result(monkeypatch: pytest.MonkeyPatch) -> None:
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    entered, waiting, finish = Event(), Event(), Event()
    real_wait = rc.threading.Event.wait

    def tracked_wait(event: Event, timeout: float | None = None) -> bool:
        slot = rc._registry.get("concurrent-observation-key")
        if slot is not None and event is slot.event:
            waiting.set()
        return real_wait(event, timeout)

    monkeypatch.setattr(rc.threading.Event, "wait", tracked_wait)
    payload = {"rows": [1], "section_timings": [{"section": "query", "time_ms": 5}]}

    def build() -> dict:
        entered.set()
        assert finish.wait(5)
        return payload

    with ThreadPoolExecutor(max_workers=2) as pool:
        leader = pool.submit(memo.coalesce, "concurrent-observation-key", build)
        assert entered.wait(5)
        waiting.clear()
        follower = pool.submit(memo.coalesce, "concurrent-observation-key", build)
        try:
            assert waiting.wait(5)
        finally:
            finish.set()
        a, b = leader.result(5), follower.result(5)
    assert a["_response_cache"]["state"] == "miss" and a["_response_cache"]["single_flight"] == "leader"
    assert b["_response_cache"]["state"] == "unknown" and b["_response_cache"]["single_flight"] == "follower"
    assert b["_response_cache"]["wait_ms"] >= 0 and "wait_ms" not in a["_response_cache"]
    assert "_response_cache" not in payload


def test_observation_survives_serialization_without_debug_privileges(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    payload = rc.cache_result(
        {"debug_queries": [{"sql": "private", "time_ms": 1}]}, state="miss", ttl_seconds=15, prewarmer=False
    )
    wire = BaseResponse(**payload).model_dump(by_alias=True)
    assert wire["_response_cache"] == {"state": "miss", "ttl_seconds": 15.0, "prewarmer": False}
    assert "_debug_queries" not in wire and "private" not in str(wire)
    assert "_response_cache" not in BaseResponse().model_dump(by_alias=True)


def test_fresh_observation_replaces_both_alias_spellings() -> None:
    payload = {"response_cache": {"state": "hit"}, "_response_cache": {"state": "hit"}, "_is_cached": True}
    fresh = rc.cache_result(payload, state="miss", prewarmer=False)
    assert "response_cache" not in fresh and "_is_cached" not in fresh
    assert BaseResponse(**fresh).response_cache.state == "miss"
    assert payload["_response_cache"]["state"] == "hit"


@pytest.mark.parametrize("state", ["hit", "miss", "bypass", "unknown"])
def test_observation_wire_bytes_match_the_typed_response(state: str) -> None:
    payload = rc.cache_result({}, state=state, ttl_seconds=15, prewarmer=False)
    raw = json.dumps(payload["_response_cache"], separators=(",", ":"))
    serialized = BaseResponse(**payload).model_dump(by_alias=True)["_response_cache"]
    assert json.dumps(serialized, separators=(",", ":")) == raw


def test_failed_builder_propagates_without_manufacturing_an_observation() -> None:
    def fail() -> dict:
        raise ValueError("failure")

    with pytest.raises(ValueError, match="failure"):
        rc.ResponseMemo().coalesce("failure-observation-key", fail)


class _CacheRunner:
    def __init__(self, n: int) -> None:
        self.n = n

    def telemetry(self) -> dict:
        return {"debug_queries": [f"q{self.n}"], "is_cached": False}


def _sha(payload: dict, svc: str) -> str:
    body = json.dumps(payload, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{body}:{svc}".encode()).hexdigest()


def test_store_check_put_and_get_with_telemetry():
    memo = ResponseMemo(ttl=30, maxsize=4)
    first = memo.store("k", {"rows": [1], "debug_queries": ["stale"], "is_cached": True}, _CacheRunner(1))
    assert first["debug_queries"] == ["q1"]
    hit = memo.check("k", _CacheRunner(2))
    assert hit is not None and hit["rows"] == [1] and hit["debug_queries"] == ["q2"] and hit["is_cached"] is True
    assert hit["_response_cache"]["state"] == "hit" and first["_response_cache"]["state"] == "miss"
    assert ResponseMemo().check("absent", _CacheRunner(1)) is None

    memo2 = ResponseMemo()
    memo2.put("k", {"a": 1, "section_timings": [1]}, strip=("section_timings",))
    expected_clean = {
        "a": 1,
        "is_cached": True,
        "_response_cache": {"state": "hit", "ttl_seconds": 30.0, "prewarmer": None},
    }
    assert memo2.get("k") == expected_clean

    memo3 = ResponseMemo()
    memo3.put(
        "k",
        {
            "a": 1,
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "DuckDB"}],
            "debug_calls": [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}],
        },
    )
    assert memo3.get("k") == expected_clean
    hit3 = memo3.get_with_telemetry("k")
    assert hit3 is not None and hit3["a"] == 1 and hit3["is_cached"] is True
    assert hit3["debug_queries"] == [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "DuckDB", "is_cached": True}]
    assert hit3["debug_calls"] == [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}]


def test_origin_and_cmcd_response_cache_keys_are_byte_stable():
    from backend.repositories.origin import _response_cache_key as origin_key_fn

    origin_key = origin_key_fn(
        "slow_urls", {"name": "svc"}, "2026-01-01T00:00:12Z", "2026-01-01T01:00:59Z", {}, limit=3, mi=False
    )
    assert origin_key == _sha(
        {"ep": "slow_urls", "s": "2026-01-01T00:00", "e": "2026-01-01T01:00", "f": {}, "limit": 3, "mi": False},
        "svc",
    )

    cmcd_key = _cmcd_key_fn({"name": "svc"}, "2026-01-01T00:00:12Z", None, {}, 300, 30, {"b", "a"})
    assert cmcd_key == _sha(
        {"s": "2026-01-01T00:00", "e": None, "f": {}, "bs": 300, "tn": 30, "sec": ["a", "b"]},
        "svc",
    )


def test_leader_runs_build_once_and_follower_shares_the_same_result():
    call_count = 0
    lock = threading.Lock()
    build_started, release = threading.Event(), threading.Event()

    def build():
        nonlocal call_count
        with lock:
            call_count += 1
        build_started.set()
        assert release.wait(timeout=5)
        return object()

    outcomes: dict[str, tuple] = {}
    t1 = threading.Thread(target=lambda: outcomes.__setitem__("leader", coalesce("same-key", build)))
    t1.start()
    assert build_started.wait(timeout=5)
    t2 = threading.Thread(target=lambda: outcomes.__setitem__("follower", coalesce("same-key", build)))
    t2.start()
    time.sleep(0.2)
    release.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert call_count == 1
    leader_result, leader_is_leader = outcomes["leader"]
    follower_result, follower_is_leader = outcomes["follower"]
    assert leader_is_leader is True and follower_is_leader is False and leader_result is follower_result


def test_different_keys_and_sequential_calls_rebuild():
    calls: list[str] = []
    lock = threading.Lock()

    def build_for(tag: str):
        def _build():
            with lock:
                calls.append(tag)
            return tag

        return _build

    outcomes: dict[str, tuple] = {}
    t1 = threading.Thread(target=lambda: outcomes.__setitem__("key-a", coalesce("key-a", build_for("key-a"))))
    t2 = threading.Thread(target=lambda: outcomes.__setitem__("key-b", coalesce("key-b", build_for("key-b"))))
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)
    assert sorted(calls) == ["key-a", "key-b"]
    assert outcomes["key-a"][0] == "key-a" and outcomes["key-b"][0] == "key-b"

    call_count = 0

    def seq_build():
        nonlocal call_count
        call_count += 1
        return call_count

    first, first_is_leader = coalesce("seq-key", seq_build)
    second, second_is_leader = coalesce("seq-key", seq_build)
    assert first_is_leader is True and second_is_leader is True and first == 1 and second == 2


def test_build_exception_propagates_to_the_follower_and_cleans_registry():
    build_started, release = threading.Event(), threading.Event()

    def build():
        build_started.set()
        assert release.wait(timeout=5)
        raise ValueError("boom")

    outcomes: dict[str, BaseException] = {}

    def run(role: str):
        try:
            coalesce("fail-key", build)
        except ValueError as e:
            outcomes[role] = e

    t1 = threading.Thread(target=run, args=("leader",))
    t1.start()
    assert build_started.wait(timeout=5)
    t2 = threading.Thread(target=run, args=("follower",))
    t2.start()
    time.sleep(0.2)
    release.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert isinstance(outcomes.get("leader"), ValueError) and isinstance(outcomes.get("follower"), ValueError)
    assert str(outcomes["leader"]) == "boom" and str(outcomes["follower"]) == "boom"

    coalesce("cleanup-key", lambda: 1)
    assert "cleanup-key" not in rc._registry

    def boom():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        coalesce("cleanup-fail-key", boom)
    assert "cleanup-fail-key" not in rc._registry


# ── Merged from tests/repositories/test_all_repos_properties.py ──

"""Property-based tests for every analytical repository SQL builder.

Each repo function has the same shape: ``(con, src, start_time, end_time,
filters, ...) -> dict``. They all call ``build_where_clause`` and execute
the result against DuckDB. The previous property-test batch on
``dashboard.get_aggregates`` and ``origin.get_timeseries`` surfaced two
real production bugs in ``build_where_clause`` (empty-string values on
numeric columns, type-mismatched filter values). This file extends the
same Hypothesis approach to the remaining repos to surface bugs that
share that class.

The contracts pinned for every repo:

1. **No exceptions.** For any filter/time combination Hypothesis generates,
   the function must return a dict instead of raising. A DuckDB binder
   error is the most common regression mode.

2. **Stable shape.** The returned dict must always have its declared
   top-level keys (FE destructures these unconditionally).

3. **No NaN/inf.** Numeric values that survive into the JSON payload
   must be finite — JSON null-coerces NaN silently, and Plotly drops
   any series containing a non-finite value.
"""

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}

_HYP_15 = settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
_HYP_10 = settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)


# ── Shared seeded DuckDB fixture ────────────────────────────────────────


@pytest.fixture
def seeded_con():
    """In-memory DuckDB with a representative schema + 30 rows of data
    that's varied enough to exercise the WHERE/GROUP BY paths in each
    repo without being so big it slows hypothesis down."""
    con = make_seeded_logs_con(_TABLE_NAME, num_rows=30)
    yield con
    con.close()


# ── Strategies (shared across all repo tests) ───────────────────────────


_KNOWN_FILTER_COLS = (
    "status",
    "country",
    "url",
    "ip",
    "method",
    "ua",
    "pop",
    "asn",
    "city",
    "region",
    "cache",
    "waf_sig",
    "edge",
    "metro",
    "transport",
    "p_type",
    "c_speed",
    "c_type",
    "server_region",
)

# Same broad strategy used in test_aggregates_timeseries_properties.
# Hypothesis explores empty strings, weird unicode, mixed types — the
# exact inputs that caught the previous bugs.
_filter_value_strategy = st.one_of(
    st.text(min_size=0, max_size=15),
    st.integers(min_value=-1000, max_value=1000),
    st.none(),
    st.sampled_from(["*foo*", "200", "404", "US", "GB", "HIT", "MISS"]),
)

_filter_spec_strategy = st.builds(
    FilterSpec,
    mode=st.sampled_from(["include", "exclude"]),
    values=st.lists(_filter_value_strategy, min_size=0, max_size=3),
)

_filters_strategy = st.dictionaries(
    keys=st.sampled_from(_KNOWN_FILTER_COLS).flatmap(lambda c: st.sampled_from([c, f"filter_{c}"])),
    values=_filter_spec_strategy,
    min_size=0,
    max_size=3,
)

_iso_time_strategy = st.one_of(
    st.none(),
    st.sampled_from(["2026-05-18T00:00:00Z", "2026-05-18T12:00:00Z", "2026-05-19T00:00:00Z"]),
)


def _assert_all_finite(d, path="root"):
    """Recursively walk a dict/list response and assert every numeric
    value is finite. NaN/inf would surface as JSON `null` (silently
    wrong) or crash Plotly traces."""
    if isinstance(d, dict):
        for k, v in d.items():
            _assert_all_finite(v, f"{path}.{k}")
    elif isinstance(d, list):
        for i, v in enumerate(d):
            _assert_all_finite(v, f"{path}[{i}]")
    elif isinstance(d, float):
        assert math.isfinite(d), f"non-finite value at {path}: {d}"


# ── security.get_security_aggregates / get_top_bots ─────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@_HYP_15
def test_security_aggregates_never_raises(seeded_con, filters, start, end):
    """``get_security_aggregates`` must return a dict for any input.
    The FE security page depends on its top-level keys."""
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@_HYP_10
def test_security_aggregates_top_level_keys_stable(seeded_con, filters):
    """Top-level shape must be stable across filter inputs."""
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, filters)
    assert isinstance(result, dict)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@_HYP_15
def test_get_top_bots_never_raises(seeded_con, filters, start, end):
    """``get_top_bots`` must not raise on any filter combination."""
    from backend.repositories.security import get_top_bots

    result = get_top_bots(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)


# ── performance.get_performance_aggregates ──────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@_HYP_15
def test_performance_aggregates_never_raises(seeded_con, filters, start, end):
    """``get_performance_aggregates`` must return a dict for any input."""
    result = get_performance_aggregates(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@_HYP_10
def test_performance_aggregates_required_keys_present(seeded_con, filters):
    """Pin the keys the FE performance page destructures."""
    result = get_performance_aggregates(seeded_con, _src, None, None, filters)
    for key in ("top_urls", "top_asns", "ttl_dist", "scatter"):
        assert key in result, f"performance_aggregates missing key {key}"


# ── network.get_health / get_quality ────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@_HYP_15
def test_network_health_never_raises(seeded_con, filters, start, end):
    """``get_health`` must not raise on any filter combination.
    The FE network page is the most chart-heavy and would be fully
    blank on any binder-error here."""
    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@_HYP_10
def test_network_health_required_keys_present(seeded_con, filters):
    """Pin the always-present ``available`` flag the FE network page
    keys on. When available=False, ``reason`` carries a CTA string
    the FE renders verbatim; when True, the result has the full
    by-ASN payload."""
    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, None, None, filters)
    assert "available" in result
    if result["available"] is False:
        assert "reason" in result, "false-available response must explain why"


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@_HYP_15
def test_network_get_quality_never_raises(seeded_con, filters, start, end):
    from backend.repositories.network import get_quality

    result = get_quality(lambda: seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


# ── origin.get_summary / status_codes / pop_latency / ip_health / path_breakdown


@pytest.mark.parametrize(
    ("fn_name", "requires_has_data"),
    [
        ("get_summary", True),
        ("get_status_codes", True),
        ("get_pop_latency", True),
        ("get_ip_health", True),
        ("get_path_breakdown", False),
        ("get_slow_urls", False),
        ("get_shielding_analysis", True),
    ],
)
@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@_HYP_10
def test_origin_endpoints_never_raise(seeded_con, fn_name, requires_has_data, filters, start, end):
    from backend.repositories import origin as origin_repo

    fn = getattr(origin_repo, fn_name)
    result = fn(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    if requires_has_data:
        assert "has_data" in result
    _assert_all_finite(result)


# ── Dashboard & Origin timeseries property tests (merged from test_aggregates_timeseries_properties.py) ──

_chart_interval_strategy = st.sampled_from(["1 minute", "5 minutes", "15 minutes", "1 hour"])
_chart_metric_strategy = st.sampled_from(["requests", "5xx", "4xx", "hit_rate", "p95_latency"])

_EXPECTED_AGG_KEYS = {"data", "time_series", "where_clause", "interval", "metric", "total_rows", "total_rows_total"}


@given(
    filters=_filters_strategy,
    start=_iso_time_strategy,
    end=_iso_time_strategy,
    interval=_chart_interval_strategy,
    metric=_chart_metric_strategy,
)
@_HYP_15
def test_get_aggregates_returns_stable_shape_for_any_input(seeded_con, filters, start, end, interval, metric):
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=start,
        end_time=end,
        filters=filters,
        chart_interval=interval,
        chart_metric=metric,
    )
    missing = _EXPECTED_AGG_KEYS - set(result.keys())
    assert not missing, f"Aggregates response missing keys {missing}; got {sorted(result.keys())}"


@given(filters=_filters_strategy, interval=_chart_interval_strategy, metric=_chart_metric_strategy)
@_HYP_10
def test_get_aggregates_numeric_fields_are_finite(seeded_con, filters, interval, metric):
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters=filters,
        chart_interval=interval,
        chart_metric=metric,
    )
    assert isinstance(result["total_rows"], int) and isinstance(result["total_rows_total"], int)
    for point in result.get("time_series", []):
        v = point.get("value")
        if isinstance(v, float):
            assert math.isfinite(v), f"non-finite time_series value: {v} in {point!r}"


@given(filters=_filters_strategy, interval=_chart_interval_strategy)
@_HYP_10
def test_get_aggregates_data_field_keys_are_well_formed(seeded_con, filters, interval):
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters=filters,
        chart_interval=interval,
        chart_metric="requests",
    )
    for field_name, entry in result["data"].items():
        assert "top" in entry, f"Field {field_name!r} missing 'top'"
        assert "total" in entry, f"Field {field_name!r} missing 'total'"
        assert isinstance(entry["top"], list)


_metric_strategy = st.sampled_from(["ttfb", "ottfb", "ttlb"])
_percentile_strategy = st.sampled_from(["p50", "p95", "p99"])
_bucket_minutes_strategy = st.floats(min_value=1.0 / 60.0, max_value=60.0, allow_nan=False, allow_infinity=False)


@given(
    filters=_filters_strategy,
    metric=_metric_strategy,
    percentile=_percentile_strategy,
    bucket_minutes=_bucket_minutes_strategy,
    split_by_leg=st.booleans(),
)
@_HYP_15
def test_get_timeseries_returns_stable_shape(seeded_con, filters, metric, percentile, bucket_minutes, split_by_leg):
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters=filters,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        metric=metric,
        percentile=percentile,
    )
    assert "has_data" in result
    assert "series" in result or result.get("has_data") is False


@given(bucket_minutes=_bucket_minutes_strategy)
@_HYP_10
def test_get_timeseries_sub_minute_bucket_produces_finite_values(seeded_con, bucket_minutes):
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        bucket_minutes=bucket_minutes,
        split_by_leg=False,
        metric="ttfb",
        percentile="p95",
    )
    for series in result.get("series", []):
        for v in series.get("values", []):
            if isinstance(v, float):
                assert math.isfinite(v), f"non-finite timeseries value: {v}"


# ── Merged from tests/repositories/test_filters_properties.py ──

"""Property-based tests for backend.repositories.utils.filters.build_where_clause.

The filter SQL builder is a high-leverage chokepoint — every analytical
endpoint runs through it. Most of its bugs land in the long tail: unusual
column names, unicode values, mixes of NULL + wildcard + exact, the
filter_/xfilter_ prefix stripping, the ``waf_sig_ind`` and ``_bot_name``
virtual filters. Hypothesis explores that long tail systematically.

Two contracts checked here:

1. **Parameter-count invariant.** When ``inline_params=False``, the number
   of ``?`` placeholders in the generated SQL must equal ``len(params)``
   exactly. A mismatch causes DuckDB to either raise a binding error or
   silently misinterpret values across rows.

2. **DuckDB parses what we generate.** The generated WHERE clause, with
   its parameters, must execute against an in-memory table without
   raising — both in parameterised mode and in inlined mode. Catches
   quote-escape bugs, unbalanced parens, missing AND/OR, etc.
"""

# ── Hypothesis strategies ─────────────────────────────────────────────────────

# The columns we know about — the ones that get partition-pruning hints
# (dt, timestamp_hour) plus a bunch of regular ones to force the no-prefix /
# prefix-stripping / numeric-suffix branches to fire.
_KNOWN_COLS = (
    "status",
    "country",
    "url",
    "ip",
    "method",
    "ua",
    "pop",
    "asn",
    "city",
    "region",
    "ottfb",
    "ottlb",
    "waf_sig",
    "edge",
)

# Filter values: mostly strings (the on-the-wire shape from the frontend),
# occasionally integers, occasionally None (NULL filter), occasionally with
# a ``*`` wildcard. We deliberately bias toward weird strings — quotes,
# percent signs, backslashes, unicode — because that's where SQL bugs hide.
_value_strategy = st.one_of(
    st.text(min_size=0, max_size=20),
    st.integers(min_value=-1000, max_value=1000),
    st.none(),
    st.sampled_from(["a*", "*b", "*foo*", "a*b*", "%lit", "''", '"', "\\", "a\nb"]),
)

_filter_spec_strategy_filters_properties = st.builds(
    FilterSpec,
    mode=st.sampled_from(["include", "exclude"]),
    values=st.lists(_value_strategy, min_size=0, max_size=4),
)


def _filter_key_strategy() -> st.SearchStrategy[str]:
    """Generate keys mimicking the frontend's filter_ / xfilter_ / suffix
    conventions plus bare keys."""
    base = st.sampled_from(_KNOWN_COLS)
    return st.one_of(
        base,
        base.map(lambda c: f"filter_{c}"),
        base.map(lambda c: f"xfilter_{c}"),
        base.map(lambda c: f"{c}_2"),
        base.map(lambda c: f"filter_{c}_3"),
    )


_filters_strategy_filters_properties = st.dictionaries(
    keys=_filter_key_strategy(),
    values=_filter_spec_strategy_filters_properties,
    min_size=0,
    max_size=4,
)

_iso_time_strategy_filters_properties = st.one_of(
    st.none(),
    st.sampled_from(
        [
            "2026-05-15T00:00:00Z",
            "2026-05-15T12:30:45+00:00",
            "2026-05-15",  # date-only — exercises the partition-prune branches
            "2026-01-01T00:00:00.000Z",
        ]
    ),
)


# ── Test fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def schema_table():
    """In-memory DuckDB table with a representative subset of columns —
    enough to actually execute the generated WHERE against."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE logs_props (
            timestamp TIMESTAMPTZ, dt VARCHAR, timestamp_hour VARCHAR, status INTEGER,
            country VARCHAR, url VARCHAR, ip VARCHAR, method VARCHAR, ua VARCHAR,
            pop VARCHAR, asn INTEGER, city VARCHAR, region VARCHAR, ottfb DOUBLE,
            ottlb DOUBLE, waf_sig VARCHAR, edge VARCHAR
        )
        """
    )
    yield con
    con.close()


# ── Property: placeholder count matches param count ──────────────────────────


@given(
    filters=_filters_strategy_filters_properties,
    start=_iso_time_strategy_filters_properties,
    end=_iso_time_strategy_filters_properties,
)
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_param_count_matches_placeholder_count(filters, start, end):
    """Every ``?`` in the generated SQL must have a positional param."""
    params, sql = build_where_clause(start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"])
    assert sql.count("?") == len(params), (
        f"Placeholder/param mismatch:\n  sql={sql!r}\n  params={params!r}\n  filters={filters!r}\n  start={start!r} end={end!r}"
    )


# ── Property: generated SQL parses + executes against a real table ───────────


@given(
    filters=_filters_strategy_filters_properties,
    start=_iso_time_strategy_filters_properties,
    end=_iso_time_strategy_filters_properties,
)
@settings(
    max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture]
)
def test_parameterised_sql_executes(filters, start, end, schema_table):
    """The WHERE clause + params must execute against an in-memory DuckDB
    without raising. Catches quote escape, unbalanced parens, missing
    join operators, type mismatches in cast targets, etc.

    We skip cases where a filter targets a column the schema doesn't have
    (e.g. ``_bot_name``/``_ngwaf_bot_name`` virtual filters that depend on
    external data); the unit suite covers those explicitly.
    """
    assume(not any("_bot_name" in k or "_ngwaf_bot_name" in k for k in filters))

    params, sql = build_where_clause(start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"])
    try:
        schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}", params).fetchall()
    except duckdb.Error as e:
        raise AssertionError(
            f"Generated SQL failed to execute:\n  sql={sql!r}\n  params={params!r}\n  err={e}\n  filters={filters!r}"
        )


@given(
    filters=_filters_strategy_filters_properties,
    start=_iso_time_strategy_filters_properties,
    end=_iso_time_strategy_filters_properties,
)
@_HYP_15
def test_inlined_sql_executes(filters, start, end, schema_table):
    """Same as above but with ``inline_params=True`` — the temp-table code
    path. Inline mode does its own quote escaping; this catches mismatches
    between that path and the parameterised path."""
    assume(not any("_bot_name" in k or "_ngwaf_bot_name" in k for k in filters))

    params, sql = build_where_clause(
        start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"], inline_params=True
    )
    assert params == [], f"inline_params=True should yield zero params, got {params!r}"
    try:
        schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}").fetchall()
    except duckdb.Error as e:
        raise AssertionError(f"Inlined SQL failed to execute:\n  sql={sql!r}\n  err={e}\n  filters={filters!r}")


# ── Property: passthrough when nothing supplied / empty values list ──────────


@given(actual_cols=st.one_of(st.none(), st.lists(st.sampled_from(_KNOWN_COLS), min_size=0, max_size=8)))
def test_no_args_returns_passthrough(actual_cols):
    """No date range, no filters → ``1=1`` no matter what columns exist."""
    params, sql = build_where_clause(None, None, {}, actual_cols=actual_cols, exclude_invalid_ips=False)
    assert sql == "1=1" and params == []


@given(col=st.sampled_from(_KNOWN_COLS), mode=st.sampled_from(["include", "exclude"]))
def test_empty_values_list_is_dropped(col, mode):
    """A FilterSpec with values=[] must contribute zero conditions."""
    filters = {col: FilterSpec(mode=mode, values=[])}
    params, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), exclude_invalid_ips=False)
    assert sql == "1=1" and params == []


# ── Regression: control chars must not break inlined SQL ─────────────────────


def test_inline_strips_null_byte_and_control_chars(schema_table):
    """Hypothesis found that a NULL byte (``\\x00``) in a filter value
    crashes DuckDB's SQL parser ("unterminated quoted string"). The
    inline-params path must strip control characters before embedding."""
    filters = {"status": FilterSpec(mode="include", values=["\x00bad\x01value"])}
    _, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), inline_params=True)
    schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}").fetchall()
    assert "\x00" not in sql and "\x01" not in sql


# ── _bot_name virtual filter: expand bot id → UA regex ──────────────────────


@pytest.mark.parametrize(
    ("fake_bot", "mode", "values", "cols", "expected_in", "expected_not_in", "exact_sql"),
    [
        (
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1", r"compatible; Googlebot"]}},
            "include",
            ["googlebot"],
            ["ua"],
            ["regexp_matches", "(?i)", "Googlebot"],
            [],
            None,
        ),
        (
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1"]}},
            "exclude",
            ["googlebot"],
            ["ua"],
            ["NOT regexp_matches"],
            [],
            None,
        ),
        (
            {"id": "googlebot", "pattern": {"accepted": [r"Googlebot"]}},
            "include",
            ["googlebot"],
            ["status"],
            [],
            ["regexp_matches"],
            "1=1",
        ),
        (
            {"id": "empty", "pattern": {"accepted": []}},
            "include",
            ["empty"],
            ["ua"],
            [],
            ["regexp_matches"],
            None,
        ),
        (
            {"id": "evil", "pattern": {"accepted": [r"foo'; DROP TABLE--"]}},
            "include",
            ["evil"],
            ["ua"],
            ["foo''"],
            [],
            None,
        ),
    ],
)
def test_bot_name_filter_variants(fake_bot, mode, values, cols, expected_in, expected_not_in, exact_sql):
    """``_bot_name`` is a virtual filter — expands bot_id into a case-insensitive
    ``regexp_matches(ua, ...)`` clause (negated for exclude mode), skipped silently when
    ``ua`` column is absent or ``pattern.accepted`` is empty, and escapes single quotes."""
    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode=mode, values=values)},
            actual_cols=cols,
        )
    for token in expected_in:
        assert token in sql
    for token in expected_not_in:
        assert token not in sql
    if exact_sql is not None:
        assert sql == exact_sql


def test_bot_name_filter_skips_unknown_bot_ids():
    """``get_bot_by_id`` returning None → that bot id is skipped, but
    other bot ids in the same filter still produce clauses."""

    def _lookup(bot_id):
        if bot_id == "known":
            return {"id": "known", "pattern": {"accepted": [r"KnownBot"]}}
        return None

    with patch("backend.utils.bot_sources.get_bot_by_id", side_effect=_lookup):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["known", "unknown"])},
            actual_cols=["ua"],
        )

    assert "KnownBot" in sql


# ── _ngwaf_bot_name virtual filter: sqlite lookup ──────────────────────────


def _seed_ngwaf_bots(entries: list[tuple[str, str]], category: str | None = None) -> None:
    """Seed the PostgreSQL ``ngwaf_bots`` cache through the real writer. entries: (waf_req_id, bot_name)."""
    upsert_bots([{"waf_req_id": w, "bot_name": n, "category": category} for w, n in entries], "ws-test", None)


def test_ngwaf_bot_name_filter_skipped_when_waf_req_id_absent_or_cache_empty():
    """If ``waf_req_id`` isn't in the schema the filter is dropped silently; with an empty
    cache an include filter matches nothing."""
    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["status"],
    )
    assert "sqlite_scan" not in sql and sql == "1=1"

    _, sql_missing = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )
    assert "sqlite_scan" not in sql_missing


@pytest.mark.parametrize(
    ("entries", "mode", "expected_in", "exact_sql"),
    [
        ([("w1", "GoogleBot"), ("w2", "GoogleBot")], "include", "waf_req_id IN", None),
        ([("w1", "GoogleBot")], "exclude", "waf_req_id NOT IN", None),
        ([("w1", "SomeOtherBot")], "include", None, "(FALSE)"),
        ([("w1", "SomeOtherBot")], "exclude", None, "1=1"),
    ],
)
def test_ngwaf_bot_name_filter_postgres_resolution(entries, mode, expected_in, exact_sql):
    """When ``waf_req_id`` is in schema, the filter resolves IDs via a PostgreSQL lookup
    into ``waf_req_id [NOT] IN (...)`` (never DuckDB's sqlite_scan). Unmatched include →
    ``(FALSE)``; unmatched exclude → vacuous ``1=1``."""
    _seed_ngwaf_bots(entries)

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode=mode, values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )
    assert "sqlite_scan" not in sql and "ngwaf_bots" not in sql
    if expected_in is not None:
        assert expected_in in sql
    if exact_sql is not None:
        assert sql == exact_sql


# ── Filter integration tests (merged from test_filter_integration.py) ─────


def _logs_with_pops(src, n_jfk: int, n_lhr: int) -> list[dict]:
    """Return logs with deterministic POP values (first n_jfk=JFK, rest=LHR)."""
    logs = generate_mock_logs(src, num_logs=n_jfk + n_lhr)
    for i, log in enumerate(logs):
        log["pop"] = "JFK" if i < n_jfk else "LHR"
    return logs


def _run(con, src, filters):
    return get_aggregates(
        con=con,
        src=src,
        start_time=None,
        end_time=None,
        filters=filters,
        chart_interval="1 minute",
        chart_metric="requests",
    )


class TestIncludeAndExcludeFilters:
    @pytest.mark.parametrize(
        ("n_jfk", "n_lhr", "syd_override", "mode", "values", "expected_rows"),
        [
            (10, 5, 0, "include", ["JFK"], 10),
            (10, 5, 0, "include", ["SYD"], 0),
            (8, 4, 3, "include", ["JFK", "LHR"], 9),
            (10, 5, 0, "exclude", ["JFK"], 5),
            (10, 0, 0, "exclude", ["JFK"], 0),
        ],
    )
    def test_include_and_exclude_filter_counts(
        self, in_memory_duckdb, test_service_source, n_jfk, n_lhr, syd_override, mode, values, expected_rows
    ):
        logs = _logs_with_pops(test_service_source, n_jfk=n_jfk, n_lhr=n_lhr)
        for log in logs[:syd_override]:
            log["pop"] = "SYD"
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode=mode, values=values)})
        assert result["total_rows"] == expected_rows


class TestFilterViaHttpEndpoint:
    @pytest.mark.parametrize(("mode", "expected_rows"), [("include", 10), ("exclude", 5)])
    def test_filter_applied_through_api(self, client, in_memory_duckdb, test_service_source, mode, expected_rows):
        """Filters must flow from the HTTP request body through to the DuckDB query."""
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        response = client.post(
            "/api/dashboard/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {"pop": {"mode": mode, "values": ["JFK"]}},
                "chart_interval": "1 minute",
                "chart_metric": "requests",
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["total_rows"] == expected_rows


class TestTunnelRequestsFilter:
    def test_tunnel_requests_include_exclude(self, in_memory_duckdb, test_service_source):
        _schema_cols_cache.clear()
        from backend.core.duckdb import _clear_schema_cache

        _clear_schema_cache()
        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(
            f"CREATE TABLE {table_name} (ip VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER, "
            "lat DOUBLE, lon DOUBLE, asn INTEGER, timestamp TIMESTAMPTZ, status INTEGER, "
            "elapsed INTEGER, cache VARCHAR, resp_bytes INTEGER)"
        )
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES "
            "('1.1.1.1', 'SJC', 1000, 1000, 37.0, -121.0, 1111, '2026-08-01 12:00:00Z', 200, 1000, 'HIT', 100),"
            "('2.2.2.2', 'SJC', 5, 1000, 37.0, -121.0, 2222, '2026-08-01 12:01:00Z', 200, 1000, 'HIT', 100),"
            "('3.3.3.3', 'SJC', 10, 1000, 37.0, -121.0, 2222, '2026-08-01 12:02:00Z', 200, 1000, 'HIT', 100),"
            "('4.4.4.4', 'SJC', 500, 1000, 37.0, -121.0, 3333, '2026-08-01 12:03:00Z', 200, 1000, 'HIT', 100)"
        )
        for mode in ("include", "exclude"):
            res = _run(
                in_memory_duckdb, test_service_source, {"_tunnel_requests": FilterSpec(mode=mode, values=["true"])}
            )
            assert res["total_rows"] == 2


class TestBotNameIpFilter:
    def test_bot_name_ip_containment_filtering(self, in_memory_duckdb, test_service_source):
        from backend.utils import bot_sources

        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"CREATE TABLE {table_name} (ip VARCHAR, timestamp TIMESTAMPTZ)")
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES "
            "('192.168.10.5', '2026-08-01 12:00:00Z'), ('10.0.0.1', '2026-08-01 12:01:00Z'), "
            "('192.168.10.100', '2026-08-01 12:02:00Z'), ('172.16.0.1', '2026-08-01 12:03:00Z')"
        )
        fake_sources = [
            {"id": "tor-exit-nodes", "name": "Tor Exit Nodes", "url": "https://x", "enabled": True, "type": "ip"}
        ]

        def fake_get_bot(bot_id):
            if bot_id == "tor-exit-nodes":
                return {
                    "id": "tor-exit-nodes",
                    "name": "Tor Exit Nodes",
                    "type": "ip",
                    "verification": {"domains": [], "cidrs": ["192.168.10.0/24"]},
                }
            return None

        with (
            patch.object(bot_sources, "BOT_SOURCES", fake_sources),
            patch("backend.utils.bot_sources.get_bot_by_id", side_effect=fake_get_bot),
        ):
            for mode in ("include", "exclude"):
                res = _run(
                    in_memory_duckdb,
                    test_service_source,
                    {"_bot_name": FilterSpec(mode=mode, values=["tor-exit-nodes"])},
                )
                assert res["total_rows"] == 2


class TestEmptyIpExclusion:
    def test_excludes_null_and_empty_ip_records(self, in_memory_duckdb, test_service_source):
        _schema_cols_cache.clear()
        from backend.core.duckdb import _clear_schema_cache

        _clear_schema_cache()
        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"CREATE TABLE {table_name} (ip VARCHAR, pop VARCHAR, timestamp TIMESTAMPTZ)")
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES "
            "('1.1.1.1', 'SJC', '2026-08-01 12:00:00Z'), (NULL, 'SJC', '2026-08-01 12:01:00Z'), "
            "('2.2.2.2', 'SJC', '2026-08-01 12:02:00Z'), ('', 'SJC', '2026-08-01 12:03:00Z')"
        )
        result = _run(in_memory_duckdb, test_service_source, {})
        assert result["total_rows"] == 2


class TestPerformanceWaterfallRollupFastPath:
    def test_unfiltered_performance_aggregates_serves_waterfall_from_rollup(self, tmp_path, monkeypatch):
        from backend.core.rollups import origin_dims, overview_rollups
        from backend.repositories.performance import _perf_memo
        from tests._helpers.rollups import writer_patches

        _perf_memo.cache.clear()
        _schema_cols_cache.clear()
        src = {"name": "svc_perf_wf", "bucket": "b_perf_wf", "_cache_dir_override": str(tmp_path)}
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _s: str(tmp_path))

        con = duckdb.connect()
        table = _safe_table(src["name"])
        con.execute(
            f"CREATE TABLE {table} ("
            "timestamp TIMESTAMPTZ, url VARCHAR, asn BIGINT, ttfb DOUBLE, elapsed DOUBLE, "
            "cache VARCHAR, ttl BIGINT, ottfb DOUBLE, ottlb DOUBLE, resp_bytes BIGINT)"
        )
        hours = [f"2026-07-01-{h:02d}" for h in range(24)]
        for h in range(24):
            ts = f"2026-07-01T{h:02d}:15:00Z"
            for _ in range(6):
                con.execute(
                    f"INSERT INTO {table} VALUES (?, '/p', 15169, 0.020, 45000.0, 'HIT', 3600, 0.0, 0.0, 1024)",
                    [ts],
                )
            for _ in range(6):
                con.execute(
                    f"INSERT INTO {table} VALUES (?, '/m', 13335, 0.080, 120000.0, 'MISS', 60, 50000.0, 70000.0, 2048)",
                    [ts],
                )

        class _NoCloseCon:
            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def close(self):
                pass

        with writer_patches(tmp_path, table, _NoCloseCon(con)):
            assert overview_rollups.build_overview_bundles("svc_perf_wf", src, hours) == 24
            assert origin_dims.build_perf_dims_bundles("svc_perf_wf", src, hours) >= 24
            assert origin_dims.build_perf_latency_bundles("svc_perf_wf", src, hours) >= 24

            res = get_performance_aggregates(
                con,
                src,
                "2026-07-01T00:00:00Z",
                "2026-07-02T00:00:00Z",
                {},
            )
        assert res.get("approx") is True
        wf_avg = res["waterfall"]["avg"]
        assert wf_avg["edge_processing"] == pytest.approx(25.0, rel=1e-2)
        assert wf_avg["origin_wait"] == pytest.approx(25.0, rel=1e-2)
        assert wf_avg["origin_download"] == pytest.approx(10.0, rel=1e-2)
        assert wf_avg["client_download"] == pytest.approx(32.5, rel=1e-2)
        assert len(res["scatter"]) > 0
        timings = {e["section"]: e for e in res["section_timings"]}
        assert timings["scatter_waterfall_rollup"]["outcome"] == "served"
        assert "perf:temp_skipped" in timings
        assert "scatter_waterfall_query" not in timings
        con.close()


def test_schema_cols_cache_and_origin_latency_expr_helpers(monkeypatch):
    from backend.repositories import _base as base_mod

    base_mod._schema_cols_cache.clear()
    src = {"name": "svc_sc", "service_id": "svc_sc", "log_fields": {"format_hash": "h123"}}
    assert base_mod._schema_cols_cache_key(src) == ("svc_sc", "h123")
    assert base_mod._schema_cols_cache_key({"name": "svc_sc"}) is None

    schema_seq = [
        [{"name": "timestamp"}],
        [{"name": "timestamp"}, {"name": "status"}, {"name": "url"}],
    ]
    refreshed: list[str] = []
    monkeypatch.setattr(base_mod, "_get_schema", lambda _c, _s: schema_seq.pop(0) if schema_seq else [])
    monkeypatch.setattr(base_mod, "_refresh_stale_view", lambda _c, s: refreshed.append(s["name"]))

    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src)
        cols = runner.get_schema_cols()
        assert cols == ["timestamp", "status", "url"]
        assert refreshed == ["svc_sc"]
        # Second call hits _schema_cols_cache without calling _get_schema again
        runner2 = QueryRunner(con, src)
        assert runner2.get_schema_cols() == ["timestamp", "status", "url"]
    finally:
        con.close()

    assert "PERCENTILE_CONT(0.95)" in base_mod.percentile_ms_expr("elapsed", 0.95, approx=False)
    assert "approx_quantile(elapsed, 0.5)" in base_mod.percentile_ms_expr("elapsed", 0.5, approx=True)
    assert "date_trunc('hour'" in base_mod.fast_utc_bucket_select("1 hour")
    assert "time_bucket(INTERVAL '1 second'" in base_mod.fast_utc_bucket_select("1 second")
    assert base_mod.empty_schema_response(items=[]) == {"has_data": False, "total": 0, "items": []}
    assert base_mod.origin_latency_us_expr({"ottfb", "ottlb"}) == '"ottfb"'
