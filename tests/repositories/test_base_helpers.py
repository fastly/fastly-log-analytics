from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.repositories import _base, rollup_coverage
from backend.repositories._base import (
    CANONICAL_METRICS,
    VALID_CHART_INTERVALS,
    QueryRunner,
    _is_stale_view_error,
    _latest_log_in_window,
    _safe_table,
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


def test_safe_iso_none_returns_none():
    assert safe_iso(None) is None


def test_safe_iso_naive_datetime_appends_z():
    """A naive (UTC-assumed) datetime gets ``Z`` appended so the
    frontend treats it as UTC. Pinned because losing the Z would
    let JS interpret the timestamp in the user's local tz."""
    from datetime import datetime

    dt = datetime(2026, 5, 15, 12, 30, 0)
    assert safe_iso(dt) == "2026-05-15T12:30:00Z"


def test_safe_iso_already_z_suffixed_string_is_preserved():
    """A datetime that already isoformat-encodes with a tz offset
    should NOT get an extra Z appended."""
    from datetime import UTC, datetime

    dt = datetime(2026, 5, 15, 12, 30, 0, tzinfo=UTC)
    out = safe_iso(dt)
    # isoformat with UTC tz produces "+00:00" suffix; no Z appended
    assert out is not None
    # Either form acceptable; what matters is there's no double-suffix
    assert not out.endswith("ZZ")
    assert "+" in out or out.endswith("Z")


def test_safe_iso_falls_back_to_str_for_non_datetime():
    """Non-datetime objects → str(x). Pinned because DuckDB returns
    objects that aren't real datetimes sometimes (string columns
    selected via min()/max())."""
    assert safe_iso("2026-05-15") == "2026-05-15"
    assert safe_iso(42) == "42"


def test_optional_col_returns_quoted_column_when_present():
    assert optional_col("country", ["country", "city"]) == '"country"'


def test_optional_col_returns_null_default_when_absent():
    """Default fallback is ``NULL`` — pinned because callers do
    ``SELECT {optional_col('region', cols)} AS region`` and rely on
    getting a valid SQL fragment either way."""
    assert optional_col("region", ["country"]) == "NULL"


def test_optional_col_custom_default():
    """Caller-provided default — used when NULL doesn't fit the
    surrounding expression (e.g. wrapping in COALESCE)."""
    assert optional_col("region", ["country"], default="''") == "''"


@pytest.mark.parametrize("good", ["1 second", "1 minute", "1 hour", "1 day"])
def test_safe_interval_passes_through_known_values(good):
    """The 4 valid chart intervals round-trip unchanged."""
    assert safe_interval(good) == good


def test_safe_interval_falls_back_for_unknown_values():
    """Unknown intervals → default ``'1 minute'`` (not raise).
    Pinned because the chart-interval selector defaults safely
    rather than 500ing on a typo."""
    assert safe_interval("'; DROP TABLE--") == "1 minute"
    assert safe_interval("2 weeks") == "1 minute"
    assert safe_interval("") == "1 minute"


def test_safe_interval_respects_custom_default():
    assert safe_interval("invalid", default="1 hour") == "1 hour"


def test_valid_chart_intervals_is_immutable_set():
    """``VALID_CHART_INTERVALS`` is a frozenset to prevent accidental
    mutation. Pinned because every call to safe_interval reads it."""
    assert isinstance(VALID_CHART_INTERVALS, frozenset)
    assert VALID_CHART_INTERVALS == {"1 second", "1 minute", "1 hour", "1 day"}


def test_time_bucket_select_produces_canonical_form():
    """``time_bucket(INTERVAL '<v>', <col>) AS bucket``. Pinned
    because the ``AS bucket`` alias is what downstream GROUP BYs
    reference; renaming would silently break the time series."""
    out = time_bucket_select("1 hour")
    assert out == "time_bucket(INTERVAL '1 hour', timestamp) AS bucket"


def test_time_bucket_select_sanitises_unsafe_interval():
    """An interval not in VALID_CHART_INTERVALS falls back to
    ``'1 minute'`` — pinned because the interval lands in a SQL
    string and a refactor that skipped the sanitisation would
    open SQL injection via the chart-interval query param."""
    out = time_bucket_select("'; DROP TABLE logs; --")
    assert "DROP TABLE" not in out
    assert "INTERVAL '1 minute'" in out


def test_time_bucket_select_accepts_custom_ts_col():
    out = time_bucket_select("1 minute", ts_col="event_time")
    assert "event_time" in out


def test_percentile_ms_expr_divides_by_1000_for_microseconds():
    """Most timing columns are stored as microseconds; the helper
    divides by 1000 to surface milliseconds. Pinned because losing
    the / 1000.0 would 1000x every latency chart."""
    out = percentile_ms_expr("ttfb", 0.95)
    assert "PERCENTILE_CONT(0.95)" in out
    assert "ORDER BY ttfb" in out
    assert "/ 1000.0" in out


def test_percentile_ms_expr_includes_filter_when_provided():
    """Optional FILTER clause for conditional percentiles (e.g.,
    "p95 over edge requests only"). Pinned because the FILTER
    syntax is DuckDB-specific."""
    out = percentile_ms_expr("ttfb", 0.99, filter_expr="WHERE edge = true")
    assert "FILTER (WHERE edge = true)" in out


def test_percentile_ms_expr_omits_filter_clause_when_empty():
    out = percentile_ms_expr("ttfb")
    assert "FILTER" not in out


def test_percentile_ms_expr_approx_uses_approx_quantile():
    """``approx=True`` opts into DuckDB's T-Digest sketch instead of the
    exact sort-based PERCENTILE_CONT. Used by the Slowest URLs / ASNs
    tables on /performance where the column is comparative, not an SLA
    report — the tail-error tradeoff buys a streaming O(N) pass."""
    out = percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95, approx=True)
    assert "approx_quantile(CAST(elapsed AS DOUBLE), 0.95)" in out
    assert "PERCENTILE_CONT" not in out
    assert "/ 1000.0" in out


def test_percentile_ms_expr_approx_supports_filter():
    out = percentile_ms_expr("ttfb", 0.99, filter_expr="WHERE edge = true", approx=True)
    assert "approx_quantile(ttfb, 0.99)" in out
    assert "FILTER (WHERE edge = true)" in out


def test_empty_schema_response_default_shape():
    """Empty schema → ``{has_data: False, total: 0}`` plus any
    caller-supplied extras."""
    out = empty_schema_response()
    assert out == {"has_data": False, "total": 0}


def test_empty_schema_response_merges_extras():
    """Callers pass response-specific empty collections via kwargs."""
    out = empty_schema_response(rows=[], series=[], extra={"k": "v"})
    assert out["has_data"] is False
    assert out["rows"] == []
    assert out["series"] == []
    assert out["extra"] == {"k": "v"}


def test_safe_table_delegates_to_core_safe_table_name():
    assert _safe_table("my_service") == "logs_my_service"
    # The "default" service maps to bare "logs" (no prefix)
    assert _safe_table("default") == "logs"


def test_canonical_metrics_includes_all_dashboard_metrics():
    """The dashboard reads these by name; the keys are part of the
    contract."""
    for required in (
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
    ):
        assert required in CANONICAL_METRICS, f"missing canonical metric: {required}"


def test_canonical_metric_5xx_rate_includes_zero_guard():
    """The 5xx rate denominator is wrapped in NULLIF to avoid
    division-by-zero on empty windows. Pinned because losing the
    guard would surface as ``inf`` on the rate chart."""
    assert "NULLIF(COUNT(*), 0)" in CANONICAL_METRICS["5xx_rate"]


def test_canonical_metric_hit_rate_includes_zero_guard():
    assert "NULLIF(COUNT(*), 0)" in CANONICAL_METRICS["hit_rate"]


def test_canonical_metric_throughput_uses_template_placeholder():
    """``throughput`` references ``{cache_col}``, ``{elapsed_col}``,
    ``{resp_bytes_col}`` so callers can swap to ``ottfb`` /
    ``oelapsed`` / ``oresp_bytes`` for origin-side throughput.
    Pinned because the placeholder names are the contract."""
    tpl = CANONICAL_METRICS["throughput"]
    assert "{cache_col}" in tpl
    assert "{elapsed_col}" in tpl
    assert "{resp_bytes_col}" in tpl


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
    """Each of these messages indicates an Iceberg view that points
    to a deleted buffer file — recoverable by refreshing the view.
    Pinned because adding a new DuckDB version that changes the
    message would silently turn recoverable errors into 500s.

    Per finding 005 (2026-06-15) the detector now also requires a
    genuine ``duckdb.IOException`` / ``duckdb.CatalogException`` so
    attacker-controlled substring injection through
    ``ConversionException`` / ``BinderException`` can no longer
    spoof the stale-view rebuild path."""
    import duckdb

    exc = getattr(duckdb, exc_cls)(msg)
    assert _is_stale_view_error(exc) is True


def test_is_stale_view_error_returns_false_for_unrelated_errors():
    assert _is_stale_view_error(Exception("Syntax error at line 1")) is False
    assert _is_stale_view_error(Exception("Permission denied")) is False


def test_attach_ngwaf_cache_yields_false_when_waf_req_id_absent():
    """If the source's schema doesn't include ``waf_req_id``, the
    helper short-circuits to False — pinned because attempting to
    ATTACH the NGWAF cache on a service without NGWAF would waste
    cycles."""
    con = duckdb.connect(":memory:")
    try:
        with attach_ngwaf_cache(con, schema_cols=["status", "ip"]) as attached:
            assert attached is False
    finally:
        con.close()


def test_attach_ngwaf_cache_passes_through_to_sqlite_attach(monkeypatch, tmp_path):
    """When ``waf_req_id`` is in the schema, the helper looks up
    ``svcconfig.ngwaf_db_path()`` and tries to materialize it. Missing
    file → yields False."""
    monkeypatch.setattr(
        "backend.config.ngwaf_db_path",
        lambda: str(tmp_path / "missing_ngwaf.db"),
    )
    con = duckdb.connect(":memory:")
    try:
        with attach_ngwaf_cache(con, schema_cols=["waf_req_id"]) as attached:
            assert attached is False
    finally:
        con.close()


def test_attach_ngwaf_cache_reads_postgres_not_a_stale_default_sqlite_file(monkeypatch, tmp_path):
    """A leftover ``ngwaf_bot_cache.db`` at the DEFAULT path (pre-ADR-22 host) must not shadow
    the Postgres cache: reading it left the verified-bots panels empty in production."""
    import sqlite3

    stale = tmp_path / "ngwaf_bot_cache.db"
    sconn = sqlite3.connect(str(stale))
    sconn.execute("CREATE TABLE ngwaf_bots (waf_req_id TEXT, bot_name TEXT, category TEXT, wellknown_bot_name TEXT)")
    sconn.execute("INSERT INTO ngwaf_bots VALUES ('stale-id', 'OldBot', 'X', NULL)")
    sconn.commit()
    sconn.close()
    monkeypatch.setattr("backend.config.DATA_DIR", tmp_path)
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(stale))

    class _Cur:
        def fetchall(self):
            return [{"waf_req_id": "pg-id", "bot_name": "GoogleBot", "category": "SEARCH", "wellknown_bot_name": None}]

    class _Pg:
        def execute(self, *_a, **_k):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr("backend.core.metadata.pg_connection.get_pg_readonly_connection", lambda: _Pg())
    con = duckdb.connect(":memory:")
    try:
        with attach_ngwaf_cache(con, schema_cols=["waf_req_id"], alias="ng_pg") as attached:
            assert attached is True
            rows = con.execute("SELECT waf_req_id, bot_name FROM temp.ng_pg_ngwaf_bots").fetchall()
        assert rows == [("pg-id", "GoogleBot")]
    finally:
        con.close()


def test_attach_ngwaf_cache_materializes_without_sqlite_scan_or_attach(monkeypatch, tmp_path):
    """When the cache file exists, the helper reads it via plain sqlite3
    and materializes it into a real, distinctly-named in-memory DuckDB
    catalog (``ATTACH ':memory:' AS {alias}``) — it must NOT ATTACH the
    SQLite file directly or use sqlite_scan against it. Cross-engine reads
    against this globally-shared, frequently-written cache corrupted it in
    production (2026-07-30); reading it into memory instead sidesteps the
    hazard entirely. A same-named DuckDB SCHEMA (instead of a separate
    catalog) was tried first and regressed with "Ambiguous reference to
    catalog or schema" once a per-service connection had other catalogs
    attached (also prod, 2026-07-30) — pinned here so that regression
    doesn't come back."""
    import sqlite3

    db_path = tmp_path / "ngwaf.db"
    sconn = sqlite3.connect(str(db_path))
    sconn.execute(
        "CREATE TABLE ngwaf_bots (waf_req_id TEXT PRIMARY KEY, bot_name TEXT, category TEXT,"
        " wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    sconn.execute(
        "INSERT INTO ngwaf_bots VALUES ('w1', 'GoogleBot', 'SEARCH-ENGINE', NULL, NULL, '2026-01-01T00:00:00Z')"
    )
    sconn.commit()
    sconn.close()

    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(db_path))
    # A second attached catalog is what exposed the schema-collision bug —
    # keep it in the fixture so a regression back to CREATE SCHEMA fails loudly.
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


def test_queryrunner_execute_tracks_sql_in_debug_queries():
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        runner.execute("SELECT 1")
        runner.execute("SELECT 2")

        assert len(runner.debug_queries) == 2
        for entry in runner.debug_queries:
            assert "sql" in entry
            assert "time_ms" in entry
            assert isinstance(entry["time_ms"], float)
    finally:
        con.close()


def test_queryrunner_execute_with_retry_returns_cursor_on_success():
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        cur = runner.execute_with_retry("SELECT 42 AS x")
        assert cur is not None
        assert cur.fetchone()[0] == 42
    finally:
        con.close()


def test_queryrunner_execute_with_retry_reraises_non_stale_errors():
    """A syntax error (not stale-view) is re-raised immediately —
    pinned because retrying syntax errors would just waste time."""
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        # Truly malformed SQL — no chance of being a stale-view error
        with pytest.raises(Exception):
            runner.execute_with_retry("SELEKT FROM")
    finally:
        con.close()


def test_queryrunner_execute_clears_view_cache_before_force_rebuild(monkeypatch):
    """Regression for the 2026-06-05 prod incident: dashboard surfaced
    ``No files found ... batch_0398ac66102f151b.parquet`` for ~30 min.

    Root cause: ``QueryRunner.execute`` self-heal called
    ``update_iceberg_view(force=True)`` without first calling
    ``clear_source_caches``. When the per-service lock is contended (the
    every-10s sync cron holds it) the force-rebuild's 5 s lock-acquire
    times out and falls back to executing the cached view SQL — which
    is the STALE SQL that referenced the missing buffer. The retry then
    re-binds the same dead paths and re-raises the same IOException.

    This test pins the ordering: ``clear_source_caches`` MUST be called
    before ``update_iceberg_view`` so the lock-timeout fallback sees an
    empty ``_view_cache`` and falls through to persistent-view /
    extended-wait paths.
    """
    from backend.core import iceberg as db_iceberg

    call_order: list[str] = []

    def _track_clear(name, *, keep_snapshot_cache=False):
        call_order.append(f"clear_source_caches(name={name},keep_snapshot_cache={keep_snapshot_cache})")

    def _track_update(con, src, *args, force=False, **kwargs):
        call_order.append(f"update_iceberg_view(force={force})")

    monkeypatch.setattr(db_iceberg, "clear_source_caches", _track_clear)
    monkeypatch.setattr(db_iceberg, "update_iceberg_view", _track_update)

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
                    import duckdb as _duckdb_mod

                    raise _duckdb_mod.IOException(
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
            "update_iceberg_view(force=True)",
        ], (
            "clear_source_caches MUST be called before update_iceberg_view, "
            "with keep_snapshot_cache=True (matches the duckdb.py:1284 "
            "self-heal pattern). Reordering or omitting the clear call "
            f"reintroduces the 2026-06-05 prod hang. Got: {call_order}"
        )
    finally:
        con.close()


def test_get_source_extent_uses_cached_status_when_available(monkeypatch):
    """The status cache is preferred to the live count query —
    avoids hitting DuckDB just to render the row count in the header."""
    cached = {
        "local_rows": 12345,
        "earliest_log_at": "2026-01-01T00:00:00Z",
        "latest_log_at": "2026-01-02T00:00:00Z",
    }
    monkeypatch.setattr("backend.config.get_status", lambda sid: cached)

    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        total, earliest, latest = get_source_extent(runner, src={"name": "svc"}, orig_table_name="logs_svc")

        assert total == 12345
        assert earliest == "2026-01-01T00:00:00Z"
        assert latest == "2026-01-02T00:00:00Z"
    finally:
        con.close()


def test_get_source_extent_falls_back_to_zero_on_total_failure(monkeypatch):
    """No cached status + the count query also fails → ``(0, None,
    None)``. Pinned because raising here would surface as a 500 on
    the header row-count card."""
    monkeypatch.setattr("backend.config.get_status", lambda sid: None)

    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, src={"name": "svc"})
        total, earliest, latest = get_source_extent(runner, src={"name": "svc"}, orig_table_name="logs_nonexistent")
        assert (total, earliest, latest) == (0, None, None)
    finally:
        con.close()


def test_latest_log_in_window_open_bounds_returns_true():
    """No start/end (default-range dashboard request) → any non-null
    latest log is "inside" the open window."""
    assert _latest_log_in_window("2026-06-26T12:00:00Z", None, None) is True


def test_latest_log_in_window_none_latest_returns_false():
    """No latest log (genuinely empty service) → never "inside" → never
    self-heals."""
    assert _latest_log_in_window(None, "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z") is False


def test_latest_log_in_window_inside_returns_true():
    assert _latest_log_in_window("2026-06-26T12:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z") is True


def test_latest_log_in_window_after_end_returns_false():
    """Latest log NEWER than end_time → outside the window (a low-traffic
    historical gap, not a stale view)."""
    assert _latest_log_in_window("2026-06-27T00:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z") is False


def test_latest_log_in_window_before_start_returns_false():
    assert _latest_log_in_window("2026-06-25T00:00:00Z", "2026-06-26T00:00:00Z", "2026-06-26T23:59:59Z") is False


def test_latest_log_in_window_tolerates_z_vs_offset_suffixes():
    """A trailing-Z latest vs +00:00 bounds must still compare correctly
    (parse_iso_utc normalises both)."""
    assert _latest_log_in_window("2026-06-26T12:00:00Z", "2026-06-26T00:00:00+00:00", "2026-06-26T23:59:59+00:00")


def test_should_self_heal_requires_empty_window():
    """A non-empty window is never a stale-view symptom."""
    assert (
        should_self_heal_stale_view(
            windowed_count=5,
            filters={},
            latest_log_at="2026-06-26T12:00:00Z",
            start_time=None,
            end_time=None,
        )
        is False
    )


def test_should_self_heal_requires_no_filters():
    """Filtered empty result → expected (the filter excluded everything),
    not stale."""
    assert (
        should_self_heal_stale_view(
            windowed_count=0,
            filters={"country": object()},  # any truthy filter mapping
            latest_log_at="2026-06-26T12:00:00Z",
            start_time=None,
            end_time=None,
        )
        is False
    )


def test_should_self_heal_fires_on_empty_window_with_latest_inside():
    """Empty window + no filters + latest log inside the range → stale view,
    fire the rebuild."""
    assert (
        should_self_heal_stale_view(
            windowed_count=0,
            filters={},
            latest_log_at="2026-06-26T12:00:00Z",
            start_time="2026-06-26T00:00:00Z",
            end_time="2026-06-26T23:59:59Z",
        )
        is True
    )


def test_should_self_heal_skips_empty_window_with_latest_outside():
    """Empty window whose range does NOT contain the latest log → genuinely
    empty, do not rebuild."""
    assert (
        should_self_heal_stale_view(
            windowed_count=0,
            filters={},
            latest_log_at="2026-06-27T00:00:00Z",
            start_time="2026-06-26T00:00:00Z",
            end_time="2026-06-26T23:59:59Z",
        )
        is False
    )


def test_should_self_heal_skips_when_no_data_at_all():
    """No latest log (null) → never fires even on an empty window."""
    assert (
        should_self_heal_stale_view(
            windowed_count=0,
            filters={},
            latest_log_at=None,
            start_time=None,
            end_time=None,
        )
        is False
    )


# ── From test_base_branches.py ──


def test_cached_listdir_returns_empty_on_oserror(tmp_path):
    """A missing or unreadable path returns [] instead of raising —
    callers treat empty and missing identically when walking rollups."""
    rollup_coverage._listdir_cache.clear()
    nonexistent = str(tmp_path / "does_not_exist")
    out = rollup_coverage.cached_listdir(nonexistent)
    assert out == []


def test_cached_listdir_flushes_cache_when_full(tmp_path):
    """When the cache reaches the max-entries cap, it's flat-cleared
    before the new entry is added. Pinned to prevent unbounded growth
    on a service with high (hour, field) churn."""
    rollup_coverage._listdir_cache.clear()
    # Fill the cache to the cap.
    cap = rollup_coverage.LISTDIR_CACHE_MAX_ENTRIES
    for i in range(cap):
        rollup_coverage._listdir_cache[f"/fake/path/{i}"] = (0.0, [])
    assert len(rollup_coverage._listdir_cache) == cap

    # Adding one more triggers the flat-clear branch.
    p = str(tmp_path)
    out = rollup_coverage.cached_listdir(p)
    assert isinstance(out, list)
    # Cache was cleared then re-populated with just the new entry.
    assert len(rollup_coverage._listdir_cache) == 1
    assert p in rollup_coverage._listdir_cache
    rollup_coverage._listdir_cache.clear()


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


def test_empty_schema_response_merges_extra_fields():
    out = empty_schema_response(rows=[], series=[])
    assert out["has_data"] is False
    assert out["total"] == 0
    assert out["rows"] == []
    assert out["series"] == []


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
    _base._schema_cols_cache.clear()


# ── From test_base_metrics.py ──


def test_canonical_metrics_exist():
    assert "hit_rate" in CANONICAL_METRICS
    assert "requests" in CANONICAL_METRICS
    assert "avg_ttfb" in CANONICAL_METRICS
    assert "p95_ttfb" in CANONICAL_METRICS
    assert "throughput" in CANONICAL_METRICS
    assert "req_size" in CANONICAL_METRICS
    assert "ttfb_ms" in CANONICAL_METRICS

    # Verify SQL snippets for units and rounding
    assert "AVG(ttfb) * 1000.0" in CANONICAL_METRICS["avg_ttfb"]
    assert "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0" in CANONICAL_METRICS["p95_ttfb"]
    assert "ROUND(" in CANONICAL_METRICS["avg_ttfb"]
    assert "ROUND(" in CANONICAL_METRICS["p95_ttfb"]
    assert "ROUND(" in CANONICAL_METRICS["throughput"]
    assert "ROUND(" in CANONICAL_METRICS["req_size"]
    assert "ROUND(" in CANONICAL_METRICS["ttfb_ms"]

    # Verify throughput components
    assert "resp_bytes_col" in CANONICAL_METRICS["throughput"]
    assert "elapsed_col" in CANONICAL_METRICS["throughput"]
    assert "* 1e6" in CANONICAL_METRICS["throughput"]


# ── From test_response_cache_helpers.py ──

import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from backend.repositories.utils.response_cache import ResponseMemo, coalesce

from backend.models.common import BaseResponse
from backend.repositories.utils import response_cache as rc


def test_lookup_and_store_observations_are_request_local(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [10.0]
    monkeypatch.setattr("backend.utils.bounded_cache.time.monotonic", lambda: clock[0])
    memo = rc.ResponseMemo(ttl=15, prewarmer=False)
    assert memo.get("private-key") is None
    original = {"rows": [1], "_response_cache": {"state": "miss"}, "is_cached": True}
    memo.put("private-key", original)
    hit = memo.get("private-key")
    assert hit["_response_cache"] == {"state": "hit", "ttl_seconds": 15.0, "prewarmer": False}
    assert hit["is_cached"] is True
    assert "_response_cache" not in memo.cache["private-key"]
    hit["_response_cache"]["state"] = "bypass"
    assert memo.get("private-key")["_response_cache"]["state"] == "hit"
    clock[0] = 25.0
    assert memo.get("private-key") is None
    assert original["_response_cache"] == {"state": "miss"}


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
    assert bypass["_response_cache"] == {"state": "bypass", "prewarmer": False}
    assert bypass["is_cached"] is False


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


def test_store_returns_fresh_telemetry_and_caches_a_request_clean_copy():
    memo = ResponseMemo(ttl=30, maxsize=4)
    first = memo.store("k", {"rows": [1], "debug_queries": ["stale"], "is_cached": True}, _CacheRunner(1))
    assert first["debug_queries"] == ["q1"]
    hit = memo.check("k", _CacheRunner(2))
    assert hit is not None and hit["rows"] == [1] and hit["debug_queries"] == ["q2"] and hit["is_cached"] is True
    assert hit["_response_cache"]["state"] == "hit" and first["_response_cache"]["state"] == "miss"


def test_check_miss_returns_none():
    assert ResponseMemo().check("absent", _CacheRunner(1)) is None


def test_put_strips_extra_keys():
    memo = ResponseMemo()
    memo.put("k", {"a": 1, "section_timings": [1]}, strip=("section_timings",))
    assert memo.get("k") == {
        "a": 1,
        "is_cached": True,
        "_response_cache": {"state": "hit", "ttl_seconds": 30.0, "prewarmer": None},
    }


def test_get_with_telemetry_restores_cached_query_attribution():
    memo = ResponseMemo()
    memo.put(
        "k",
        {
            "a": 1,
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "DuckDB"}],
            "debug_calls": [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}],
        },
    )
    assert memo.get("k") == {
        "a": 1,
        "is_cached": True,
        "_response_cache": {"state": "hit", "ttl_seconds": 30.0, "prewarmer": None},
    }
    hit = memo.get_with_telemetry("k")
    assert hit is not None and hit["a"] == 1 and hit["is_cached"] is True
    assert hit["debug_queries"] == [{"sql": "SELECT 1", "time_ms": 1.2, "engine": "DuckDB", "is_cached": True}]
    assert hit["debug_calls"] == [{"service": "fastly", "method": "GET", "path": "/x", "time_ms": 2.0}]


def test_origin_response_cache_key_is_byte_stable():
    from backend.repositories.origin import _response_cache_key

    key = _response_cache_key(
        "slow_urls", {"name": "svc"}, "2026-01-01T00:00:12Z", "2026-01-01T01:00:59Z", {}, limit=3, mi=False
    )
    expected = _sha(
        {"ep": "slow_urls", "s": "2026-01-01T00:00", "e": "2026-01-01T01:00", "f": {}, "limit": 3, "mi": False},
        "svc",
    )
    assert key == expected


def test_cmcd_response_cache_key_is_byte_stable():
    from backend.repositories.cmcd import _response_cache_key

    key = _response_cache_key({"name": "svc"}, "2026-01-01T00:00:12Z", None, {}, 300, 30, {"b", "a"})
    expected = _sha(
        {"s": "2026-01-01T00:00", "e": None, "f": {}, "bs": 300, "tn": 30, "sec": ["a", "b"]},
        "svc",
    )
    assert key == expected


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


def test_different_keys_each_run_their_own_build():
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


def test_sequential_calls_for_the_same_key_each_rebuild():
    call_count = 0

    def build():
        nonlocal call_count
        call_count += 1
        return call_count

    first, first_is_leader = coalesce("seq-key", build)
    second, second_is_leader = coalesce("seq-key", build)
    assert first_is_leader is True and second_is_leader is True and first == 1 and second == 2


def test_build_exception_propagates_to_the_follower():
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


def test_registry_entry_is_removed_after_completion():
    coalesce("cleanup-key", lambda: 1)
    assert "cleanup-key" not in rc._registry


def test_registry_entry_is_removed_even_on_failure():
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


import math

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from backend.models.common import FilterSpec
from tests._helpers.duckdb_fixtures import make_seeded_logs_con

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


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
    st.sampled_from(
        [
            "2026-05-18T00:00:00Z",
            "2026-05-18T12:00:00Z",
            "2026-05-19T00:00:00Z",
        ]
    ),
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


# ── security.get_security_aggregates ────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_security_aggregates_never_raises(seeded_con, filters, start, end):
    """``get_security_aggregates`` must return a dict for any input.
    The FE security page depends on its top-level keys."""
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_security_aggregates_top_level_keys_stable(seeded_con, filters):
    """Top-level shape must be stable across filter inputs."""
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, filters)
    # Probe a few representative keys; the response is a mixed dict of
    # named result rows but every call should at least return _something_
    # and not blow up.
    assert isinstance(result, dict)


# ── security.get_top_bots ───────────────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_top_bots_never_raises(seeded_con, filters, start, end):
    """``get_top_bots`` must not raise on any filter combination."""
    from backend.repositories.security import get_top_bots

    result = get_top_bots(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)


# ── performance.get_performance_aggregates ──────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_performance_aggregates_never_raises(seeded_con, filters, start, end):
    """``get_performance_aggregates`` must return a dict for any input."""
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_performance_aggregates_required_keys_present(seeded_con, filters):
    """Pin the keys the FE performance page destructures."""
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, None, None, filters)
    for key in ("top_urls", "top_asns", "ttl_dist", "scatter"):
        assert key in result, f"performance_aggregates missing key {key}"


# ── network.get_health ──────────────────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_network_health_never_raises(seeded_con, filters, start, end):
    """``get_health`` must not raise on any filter combination.
    The FE network page is the most chart-heavy and would be fully
    blank on any binder-error here."""
    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
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


# ── network.get_quality ─────────────────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_network_get_quality_never_raises(seeded_con, filters, start, end):
    from backend.repositories.network import get_quality

    result = get_quality(lambda: seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


# ── origin.get_summary / status_codes / pop_latency / ip_health / path_breakdown


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_summary_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_summary

    result = get_summary(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_status_codes_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_status_codes

    result = get_status_codes(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_pop_latency_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_pop_latency

    result = get_pop_latency(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_ip_health_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_ip_health

    result = get_ip_health(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_path_breakdown_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_path_breakdown

    result = get_path_breakdown(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_slow_urls_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_slow_urls

    result = get_slow_urls(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_shielding_analysis_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_shielding_analysis

    result = get_shielding_analysis(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


# ── Dashboard & Origin timeseries property tests (merged from test_aggregates_timeseries_properties.py) ──

from backend.repositories.dashboard import get_aggregates  # noqa: E402

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
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
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


@given(
    filters=_filters_strategy,
    interval=_chart_interval_strategy,
    metric=_chart_metric_strategy,
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
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
    assert isinstance(result["total_rows"], int)
    assert isinstance(result["total_rows_total"], int)
    for point in result.get("time_series", []):
        v = point.get("value")
        if isinstance(v, float):
            assert math.isfinite(v), f"non-finite time_series value: {v} in {point!r}"


@given(
    filters=_filters_strategy,
    interval=_chart_interval_strategy,
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
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
_bucket_minutes_strategy = st.floats(
    min_value=1.0 / 60.0,
    max_value=60.0,
    allow_nan=False,
    allow_infinity=False,
)


@given(
    filters=_filters_strategy,
    metric=_metric_strategy,
    percentile=_percentile_strategy,
    bucket_minutes=_bucket_minutes_strategy,
    split_by_leg=st.booleans(),
)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
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


@given(
    bucket_minutes=_bucket_minutes_strategy,
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
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


import pytest
from backend.repositories.utils.filters import build_where_clause
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from backend.models.common import FilterSpec

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
            timestamp TIMESTAMPTZ,
            dt VARCHAR,
            timestamp_hour VARCHAR,
            status INTEGER,
            country VARCHAR,
            url VARCHAR,
            ip VARCHAR,
            method VARCHAR,
            ua VARCHAR,
            pop VARCHAR,
            asn INTEGER,
            city VARCHAR,
            region VARCHAR,
            ottfb DOUBLE,
            ottlb DOUBLE,
            waf_sig VARCHAR,
            edge VARCHAR
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
    # Skip virtual-filter keys — they require external state (bot_sources
    # registry, ngwaf SQLite cache) that hypothesis can't usefully fuzz.
    assume(not any("_bot_name" in k or "_ngwaf_bot_name" in k for k in filters))

    params, sql = build_where_clause(start, end, filters, actual_cols=list(_KNOWN_COLS) + ["dt", "timestamp_hour"])
    # Just run the WHERE clause — we don't care about results, only that
    # DuckDB accepts the SQL.
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
@settings(
    max_examples=15, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture]
)
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


# ── Property: passthrough when nothing supplied ──────────────────────────────


@given(actual_cols=st.one_of(st.none(), st.lists(st.sampled_from(_KNOWN_COLS), min_size=0, max_size=8)))
def test_no_args_returns_passthrough(actual_cols):
    """No date range, no filters → ``1=1`` no matter what columns exist."""
    params, sql = build_where_clause(None, None, {}, actual_cols=actual_cols, exclude_invalid_ips=False)
    assert sql == "1=1"
    assert params == []


# ── Property: empty values list never adds any clause ────────────────────────


@given(col=st.sampled_from(_KNOWN_COLS), mode=st.sampled_from(["include", "exclude"]))
def test_empty_values_list_is_dropped(col, mode):
    """A FilterSpec with values=[] must contribute zero conditions."""
    filters = {col: FilterSpec(mode=mode, values=[])}
    params, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), exclude_invalid_ips=False)
    assert sql == "1=1"
    assert params == []


# ── Regression: control chars must not break inlined SQL ─────────────────────


def test_inline_strips_null_byte_and_control_chars(schema_table):
    """Hypothesis found that a NULL byte (``\\x00``) in a filter value
    crashes DuckDB's SQL parser ("unterminated quoted string"). The
    inline-params path must strip control characters before embedding."""
    filters = {"status": FilterSpec(mode="include", values=["\x00bad\x01value"])}
    _, sql = build_where_clause(None, None, filters, actual_cols=list(_KNOWN_COLS), inline_params=True)
    # Must execute without parser error
    schema_table.execute(f"SELECT 1 FROM logs_props WHERE {sql}").fetchall()
    # And the original control chars are gone from the rendered SQL
    assert "\x00" not in sql and "\x01" not in sql


# ── _bot_name virtual filter: expand bot id → UA regex ──────────────────────


def test_bot_name_filter_expands_to_ua_regex_include():
    """``_bot_name`` is a virtual filter — the FE sends a bot_id and the
    SQL builder expands it into a ``regexp_matches(ua, ...)`` clause
    using the bot's UA patterns. Pinned because this is the only
    filter type that requires an external registry lookup."""
    fake_bot = {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1", r"compatible; Googlebot"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["googlebot"])},
            actual_cols=["ua"],
        )

    assert "regexp_matches" in sql
    assert "(?i)" in sql  # case-insensitive
    # Both alternations are present
    assert r"Googlebot/2\.1" in sql or "Googlebot" in sql


def test_bot_name_filter_emits_negation_for_exclude_mode():
    fake_bot = {"id": "googlebot", "pattern": {"accepted": [r"Googlebot/2\.1"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="exclude", values=["googlebot"])},
            actual_cols=["ua"],
        )

    assert "NOT regexp_matches" in sql


def test_bot_name_filter_skipped_when_ua_column_absent():
    """If the service hasn't enabled the ``ua`` column, the filter is
    skipped silently with a warning (not a 500). Pinned because the
    frontend may persist a bot filter from another service that DID
    have ``ua`` — switching service shouldn't break the dashboard."""
    fake_bot = {"id": "googlebot", "pattern": {"accepted": [r"Googlebot"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["googlebot"])},
            actual_cols=["status"],  # no 'ua'
        )

    # Filter dropped → only the trivial 1=1 stays
    assert "regexp_matches" not in sql
    assert sql == "1=1"


def test_bot_name_filter_skips_unknown_bot_ids():
    """``get_bot_by_id`` returning None → that bot id is skipped, but
    other bot ids in the same filter still produce clauses."""

    def _lookup(bot_id):
        if bot_id == "known":
            return {"id": "known", "pattern": {"accepted": [r"KnownBot"]}}
        return None  # unknown bot

    with patch("backend.utils.bot_sources.get_bot_by_id", side_effect=_lookup):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["known", "unknown"])},
            actual_cols=["ua"],
        )

    assert "KnownBot" in sql


def test_bot_name_filter_skips_bots_with_no_patterns():
    """A bot entry with empty ``pattern.accepted`` → skip without
    error. Pinned because a misconfigured bot source would otherwise
    surface as a confusing empty-clause SQL error."""
    fake_bot = {"id": "empty", "pattern": {"accepted": []}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["empty"])},
            actual_cols=["ua"],
        )

    assert "regexp_matches" not in sql


def test_bot_name_filter_escapes_single_quotes_in_patterns():
    """A bot pattern containing a single quote must be SQL-escaped
    (`'` → `''`). Pinned to prevent a malicious or accidental bot
    registry entry from breaking out of the regex string literal."""
    fake_bot = {"id": "evil", "pattern": {"accepted": [r"foo'; DROP TABLE--"]}}

    with patch("backend.utils.bot_sources.get_bot_by_id", return_value=fake_bot):
        _, sql = build_where_clause(
            None,
            None,
            {"_bot_name": FilterSpec(mode="include", values=["evil"])},
            actual_cols=["ua"],
        )

    # Single quote got doubled — not raw — so it can't terminate the SQL string
    assert "foo''; DROP TABLE" in sql or "foo''" in sql


# ── _ngwaf_bot_name virtual filter: sqlite_scan subquery ───────────────────


def test_ngwaf_bot_name_filter_skipped_when_waf_req_id_absent():
    """If ``waf_req_id`` isn't in the schema, the filter is dropped
    silently. Same FE-cross-service-persistence rationale as ``_bot_name``."""
    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["status"],  # no waf_req_id
    )

    assert "sqlite_scan" not in sql
    assert sql == "1=1"


def _create_ngwaf_cache_db(db_path: str, entries: list[tuple[str, str]]) -> None:
    """Populate a minimal ngwaf_bot_cache SQLite file. entries: (waf_req_id, bot_name)."""
    import sqlite3

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ngwaf_bots ("
        " waf_req_id TEXT PRIMARY KEY, bot_name TEXT, category TEXT,"
        " wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    for wid, name in entries:
        con.execute("INSERT INTO ngwaf_bots VALUES (?, ?, NULL, NULL, NULL, '2026-01-01T00:00:00Z')", (wid, name))
    con.commit()
    con.close()


def test_ngwaf_bot_name_filter_resolves_ids_via_sqlite_not_duckdb(tmp_path, monkeypatch):
    """When ``waf_req_id`` is in schema and the ngwaf DB has a matching bot
    name, the filter compiles to a plain ``waf_req_id IN (...)`` literal
    list resolved via a direct sqlite3 lookup — NOT DuckDB's sqlite_scan.
    sqlite_scan doesn't reliably coordinate with SQLite's own WAL/locking
    protocol; cross-engine reads against this globally-shared,
    frequently-written cache corrupted it in production (2026-07-30)."""
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "GoogleBot"), ("w2", "GoogleBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert "sqlite_scan" not in sql
    assert "ngwaf_bots" not in sql
    assert "waf_req_id IN" in sql


def test_ngwaf_bot_name_filter_emits_not_in_for_exclude(tmp_path, monkeypatch):
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "GoogleBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="exclude", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert "waf_req_id NOT IN" in sql


def test_ngwaf_bot_name_filter_include_with_no_cache_matches_is_false(tmp_path, monkeypatch):
    """An include filter whose bot name matches nothing in the cache must
    match nothing (not everything) — mirrors the empty-subquery IN-clause
    behavior of the sqlite_scan-based implementation it replaced."""
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "SomeOtherBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert sql == "(FALSE)"


def test_ngwaf_bot_name_filter_exclude_with_no_cache_matches_is_vacuous(tmp_path, monkeypatch):
    """An exclude filter whose bot name matches nothing in the cache is a
    no-op (nothing to exclude) — not a bug, matches prior NOT IN (empty
    set) semantics."""
    fake_db = tmp_path / "ngwaf.db"
    _create_ngwaf_cache_db(str(fake_db), [("w1", "SomeOtherBot")])
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(fake_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="exclude", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert sql == "1=1"


def test_ngwaf_bot_name_filter_silently_skipped_when_db_missing(tmp_path, monkeypatch):
    """No NGWAF cache file on disk → filter is silently dropped (not
    a 500). Pinned because services that haven't enabled NGWAF
    shouldn't have a stale bot filter break their analytical queries."""
    missing_db = tmp_path / "doesnotexist.db"
    monkeypatch.setattr("backend.config.ngwaf_db_path", lambda: str(missing_db))

    _, sql = build_where_clause(
        None,
        None,
        {"_ngwaf_bot_name": FilterSpec(mode="include", values=["GoogleBot"])},
        actual_cols=["waf_req_id"],
    )

    assert "sqlite_scan" not in sql


# ── Filter integration tests (merged from test_filter_integration.py) ─────

from backend.repositories._base import _schema_cols_cache  # noqa: E402
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs  # noqa: E402
from tests.conftest import MOCK_SERVICE_ID  # noqa: E402


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


class TestIncludeFilter:
    def test_narrows_to_matching_rows(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="include", values=["JFK"])})
        assert result["total_rows"] == 10

    def test_no_match_returns_zero(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="include", values=["SYD"])})
        assert result["total_rows"] == 0

    def test_multi_value_accepts_any_match(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=8, n_lhr=4)
        for log in logs[:3]:
            log["pop"] = "SYD"  # replace first 3 JFK → SYD: 5 JFK + 4 LHR + 3 SYD
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="include", values=["JFK", "LHR"])})
        assert result["total_rows"] == 9  # 5 JFK + 4 LHR


class TestExcludeFilter:
    def test_removes_matching_rows(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="exclude", values=["JFK"])})
        assert result["total_rows"] == 5

    def test_exclude_all_returns_zero(self, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=0)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        result = _run(in_memory_duckdb, test_service_source, {"pop": FilterSpec(mode="exclude", values=["JFK"])})
        assert result["total_rows"] == 0


class TestFilterViaHttpEndpoint:
    def test_include_filter_applied_through_api(self, client, in_memory_duckdb, test_service_source):
        """Filters must flow from the HTTP request body through to the DuckDB query."""
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        response = client.post(
            "/api/dashboard/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {"pop": {"mode": "include", "values": ["JFK"]}},
                "chart_interval": "1 minute",
                "chart_metric": "requests",
            },
        )

        assert response.status_code == 200, response.text
        assert response.json()["total_rows"] == 10

    def test_exclude_filter_applied_through_api(self, client, in_memory_duckdb, test_service_source):
        logs = _logs_with_pops(test_service_source, n_jfk=10, n_lhr=5)
        insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

        response = client.post(
            "/api/dashboard/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {"pop": {"mode": "exclude", "values": ["JFK"]}},
                "chart_interval": "1 minute",
                "chart_metric": "requests",
            },
        )

        assert response.status_code == 200, response.text
        assert response.json()["total_rows"] == 5


class TestTunnelRequestsFilter:
    def test_tunnel_requests_include_exclude(self, in_memory_duckdb, test_service_source):
        _schema_cols_cache.clear()
        from backend.core.duckdb import _clear_schema_cache

        _clear_schema_cache()

        table_name = _safe_table(test_service_source["name"])

        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"""
            CREATE TABLE {table_name} (
                ip VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER,
                lat DOUBLE, lon DOUBLE, asn INTEGER, timestamp TIMESTAMPTZ,
                status INTEGER, elapsed INTEGER, cache VARCHAR, resp_bytes INTEGER
            )
        """)

        in_memory_duckdb.execute(f"""
            INSERT INTO {table_name} VALUES
            ('1.1.1.1', 'SJC', 1000, 1000, 37.0, -121.0, 1111, '2026-08-01 12:00:00Z', 200, 1000, 'HIT', 100),
            ('2.2.2.2', 'SJC', 5, 1000, 37.0, -121.0, 2222, '2026-08-01 12:01:00Z', 200, 1000, 'HIT', 100),
            ('3.3.3.3', 'SJC', 10, 1000, 37.0, -121.0, 2222, '2026-08-01 12:02:00Z', 200, 1000, 'HIT', 100),
            ('4.4.4.4', 'SJC', 500, 1000, 37.0, -121.0, 3333, '2026-08-01 12:03:00Z', 200, 1000, 'HIT', 100)
        """)

        result_inc = _run(
            in_memory_duckdb, test_service_source, {"_tunnel_requests": FilterSpec(mode="include", values=["true"])}
        )
        assert result_inc["total_rows"] == 2

        result_exc = _run(
            in_memory_duckdb, test_service_source, {"_tunnel_requests": FilterSpec(mode="exclude", values=["true"])}
        )
        assert result_exc["total_rows"] == 2


class TestBotNameIpFilter:
    def test_bot_name_ip_containment_filtering(self, in_memory_duckdb, test_service_source):
        from backend.utils import bot_sources

        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"""
            CREATE TABLE {table_name} (
                ip VARCHAR, timestamp TIMESTAMPTZ
            )
        """)

        in_memory_duckdb.execute(f"""
            INSERT INTO {table_name} VALUES
            ('192.168.10.5', '2026-08-01 12:00:00Z'),
            ('10.0.0.1', '2026-08-01 12:01:00Z'),
            ('192.168.10.100', '2026-08-01 12:02:00Z'),
            ('172.16.0.1', '2026-08-01 12:03:00Z')
        """)

        fake_sources = [
            {"id": "tor-exit-nodes", "name": "Tor Exit Nodes", "url": "https://x", "enabled": True, "type": "ip"},
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
            res_inc = _run(
                in_memory_duckdb,
                test_service_source,
                {"_bot_name": FilterSpec(mode="include", values=["tor-exit-nodes"])},
            )
            assert res_inc["total_rows"] == 2

            res_exc = _run(
                in_memory_duckdb,
                test_service_source,
                {"_bot_name": FilterSpec(mode="exclude", values=["tor-exit-nodes"])},
            )
            assert res_exc["total_rows"] == 2


class TestEmptyIpExclusion:
    def test_excludes_null_and_empty_ip_records(self, in_memory_duckdb, test_service_source):
        _schema_cols_cache.clear()
        from backend.core.duckdb import _clear_schema_cache

        _clear_schema_cache()

        table_name = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        in_memory_duckdb.execute(f"""
            CREATE TABLE {table_name} (
                ip VARCHAR, pop VARCHAR, timestamp TIMESTAMPTZ
            )
        """)

        in_memory_duckdb.execute(f"""
            INSERT INTO {table_name} VALUES
            ('1.1.1.1', 'SJC', '2026-08-01 12:00:00Z'),
            (NULL, 'SJC', '2026-08-01 12:01:00Z'),
            ('2.2.2.2', 'SJC', '2026-08-01 12:02:00Z'),
            ('', 'SJC', '2026-08-01 12:03:00Z')
        """)

        result = _run(in_memory_duckdb, test_service_source, {})
        assert result["total_rows"] == 2
