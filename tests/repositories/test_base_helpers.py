"""Tests for ``backend.repositories._base`` — shared repo helpers.

Every analytical endpoint goes through these helpers: SQL expression
builders (``percentile_ms_expr``, ``error_rate_expr``, ``time_bucket_select``),
the cross-engine ATTACH helpers (``_attach_sqlite``, ``attach_ngwaf_cache``,
``attach_metadata_db``), the QueryRunner (which centralises debug
tracking + stale-view retry), and the canonical-metrics SQL templates
used across dashboard / origin / performance / security panels.

A regression in any of these ripples through every chart in the product.
"""

from __future__ import annotations

import duckdb
import pytest

from backend.repositories._base import (
    CANONICAL_METRICS,
    VALID_CHART_INTERVALS,
    QueryRunner,
    _attach_sqlite,
    _is_stale_view_error,
    _safe_table,
    attach_metadata_db,
    attach_ngwaf_cache,
    empty_schema_response,
    error_rate_expr,
    get_source_extent,
    optional_col,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    time_bucket_select,
)

# ── safe_iso: datetime / string / None handling ────────────────────────────


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


# ── optional_col: SQL column reference with fallback ──────────────────────


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


# ── safe_interval: validate chart interval against allow-list ──────────────


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


# ── time_bucket_select: SQL fragment ───────────────────────────────────────


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


# ── percentile_ms_expr ────────────────────────────────────────────────────


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


# ── error_rate_expr ──────────────────────────────────────────────────────


def test_error_rate_expr_default_5xx_threshold():
    """Default threshold is 500 (5xx errors). Pinned because the
    dashboard's "error rate" card keys on this default."""
    out = error_rate_expr()
    assert ">= 500" in out
    assert "100.0" in out  # percentage
    assert "NULLIF(count(*)" in out  # zero-guard


def test_error_rate_expr_custom_threshold():
    """``threshold=400`` → 4xx+ error rate (used by the URL latency
    panel)."""
    out = error_rate_expr(threshold=400)
    assert ">= 400" in out


def test_error_rate_expr_custom_status_col():
    """``status_col='ost'`` for origin-status rate calculations."""
    out = error_rate_expr(status_col="ost")
    assert "ost >= 500" in out


def test_error_rate_expr_includes_filter_for_both_sum_and_count():
    """The FILTER clause must apply to BOTH the sum AND the count —
    otherwise the resulting rate is denominator-mismatched."""
    out = error_rate_expr(filter_expr="WHERE edge = true")
    # The filter appears twice
    assert out.count("FILTER (WHERE edge = true)") == 2


# ── empty_schema_response ────────────────────────────────────────────────


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


# ── _safe_table: re-exports core.duckdb._safe_table_name ───────────────────


def test_safe_table_delegates_to_core_safe_table_name():
    assert _safe_table("my_service") == "logs_my_service"
    # The "default" service maps to bare "logs" (no prefix)
    assert _safe_table("default") == "logs"


# ── CANONICAL_METRICS template registry ──────────────────────────────────


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


# ── _is_stale_view_error: error-message heuristic ─────────────────────────


@pytest.mark.parametrize(
    "msg,exc_cls",
    [
        ("No files found at s3://my-bucket/logs/foo.parquet", "IOException"),
        ("Catalog Error: Table with name logs_x does not exist", "CatalogException"),
        ("ParquetReader: file does not exist", "CatalogException"),
        ("IOError: No such file or directory: /tmp/logs.parquet", "IOException"),
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


# ── _attach_sqlite: context-manager ATTACH semantics ──────────────────────


def test_attach_sqlite_yields_false_when_path_missing(tmp_path):
    """Missing SQLite file → yield False without attempting ATTACH.
    Pinned because attempting ATTACH on a missing file is what
    causes the SQLite extension to load (slow) and then fail."""
    con = duckdb.connect(":memory:")
    try:
        missing = str(tmp_path / "does-not-exist.db")
        with _attach_sqlite(con, missing, "alias_x") as attached:
            assert attached is False
    finally:
        con.close()


def test_attach_sqlite_attaches_and_detaches_on_exit(tmp_path):
    """Happy path: file exists → ATTACH on enter, DETACH on exit.
    Pinned because forgetting the DETACH leaks an attachment slot
    per request and DuckDB caps the total."""
    import sqlite3

    db_path = tmp_path / "real.db"
    sqlite3.connect(str(db_path)).close()  # touch the file

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL sqlite; LOAD sqlite;")
        with _attach_sqlite(con, str(db_path), "myalias") as attached:
            assert attached is True
            # The attachment is queryable
            con.execute("SELECT * FROM duckdb_databases() WHERE database_name = 'myalias'").fetchone()
        # After exit, the attachment is gone
        attached_after = con.execute(
            "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'myalias'"
        ).fetchone()[0]
        assert attached_after == 0
    finally:
        con.close()


def test_attach_sqlite_swallows_attach_failure(tmp_path):
    """An ATTACH that raises (sqlite extension unavailable, lock
    contention, permission denied) is caught and yields ``False`` —
    pinned because raising here would cascade into a 500 for routes
    that only need the attachment optionally.

    DuckDB lazily validates SQLite files, so writing garbage bytes
    doesn't fail at ATTACH time. Force the failure by stubbing
    ``con.execute`` to raise for the ATTACH statement specifically;
    that exercises the same except-and-yield-False path real callers
    would hit when an attach raises."""
    real_path = tmp_path / "exists.db"
    real_path.write_text("placeholder")  # passes the os.path.exists() guard

    class _FailingConn:
        def execute(self, sql: str):
            if sql.startswith("ATTACH "):
                raise RuntimeError("simulated ATTACH failure")
            raise AssertionError(f"unexpected SQL: {sql!r}")

    con = _FailingConn()
    with _attach_sqlite(con, str(real_path), "broken") as attached:
        assert attached is False
    # If attached had leaked through as True, the context manager would
    # have attempted a DETACH on exit, hitting the AssertionError above.


# ── attach_ngwaf_cache: schema-gated cross-engine ATTACH ──────────────────


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
    ``svcconfig.ngwaf_db_path()`` and ATTACHes it. Missing file →
    yields False."""
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


# ── attach_metadata_db ────────────────────────────────────────────────────


def test_attach_metadata_db_yields_false_when_file_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "backend.core.metadata.db_path",
        lambda sid: str(tmp_path / f"{sid}.metadata.db"),
    )
    con = duckdb.connect(":memory:")
    try:
        with attach_metadata_db(con, "svc-missing", alias="m") as attached:
            assert attached is False
    finally:
        con.close()


# ── QueryRunner: debug tracking + stale-view retry ────────────────────────


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


# ── get_source_extent: status-cache fallback ──────────────────────────────


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
