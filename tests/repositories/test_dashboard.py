from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from backend.repositories.dashboard import (
    DASHBOARD_CACHE_TTL,
    FIELDS,
    get_aggregates,
    get_field_values,
    get_raw_df,
)
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _src_without_table():
    """Returns a src dict whose table does not exist in the in-memory DB."""
    return {"name": "nonexistent_service", "service_id": "nonexistent_service"}


def test_get_aggregates_empty_table(in_memory_duckdb):
    """Returns correct empty structure when the table does not exist."""
    src = _src_without_table()
    result = get_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert "data" in result
    assert "time_series" in result
    assert "map_data" in result
    assert result["total_rows"] == 0
    assert result["time_series"] == []
    assert result["map_data"] == []
    # All FIELDS should be present with empty tops
    for field in FIELDS:
        assert field in result["data"]
        assert result["data"][field]["top"] == []
        assert result["data"][field]["total"] == 0


def test_get_aggregates_with_data(in_memory_duckdb, test_service_source):
    """Returns populated aggregates after data is inserted."""
    logs = generate_mock_logs(test_service_source, num_logs=60, hours_ago=1)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert result["total_rows"] > 0
    assert result["metric"] == "requests"
    assert isinstance(result["time_series"], list)
    assert isinstance(result["map_data"], list)

    # Fields present in the mock data (Group A, C, D, F) should have non-zero totals
    assert result["data"]["url"]["total"] > 0
    assert result["data"]["ip"]["total"] > 0
    assert result["data"]["status"]["total"] > 0
    assert result["data"]["country"]["total"] > 0

    # Top entries for url should be valid strings
    if result["data"]["url"]["top"]:
        top_url = result["data"]["url"]["top"][0]
        assert "value" in top_url
        assert "count" in top_url
        assert isinstance(top_url["count"], int)

    # Country map data should be present (group D generates country field)
    if result["map_data"]:
        entry = result["map_data"][0]
        assert "country" in entry
        assert "count" in entry


# ── View-lag self-heal ──────────────────────────────────────────────────────
#
# When a fresh log is buffered, the per-service status cache (latest_log_at /
# local_rows, from a direct parquet+buffer read at ingest) updates immediately,
# but the live Iceberg VIEW the aggregate queries read from is built from CACHED
# view SQL that lags until a sync/commit cron tick rebuilds it. With a freshly-
# buffered single log the status cache shows data while the view returns 0 rows
# for every window — the frontend then loops on "Preparing your data" (and on
# dev, which runs no crons, it never resolves). The self-heal forces ONE view
# rebuild + re-query under a tight, false-positive-resistant trigger.


def _empty_logs_table(con, src):
    """Create the logs table with the full mock schema but no rows.

    Mirrors what ``insert_mock_logs`` builds (so ``get_schema_cols`` returns a
    real column set) without seeding any data — the starting state for the
    stale-view simulation where the cache reports data but the view is empty.
    """
    from backend.core.log_fields import LOG_FIELD_CATALOG

    # Production connections run in UTC (backend/core/duckdb.py SET
    # TimeZone='UTC'); match it so naive-TIMESTAMP vs TIMESTAMPTZ window
    # comparisons behave as they do in prod.
    con.execute("SET TimeZone='UTC';")
    table_name = _safe_table(src["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    con.execute(f"CREATE TABLE {table_name} ({schema_def})")
    return table_name


def test_self_heal_fires_once_and_returns_data_when_view_is_stale(in_memory_duckdb, test_service_source, monkeypatch):
    """Trigger case (a): empty windowed result + no filters + the all-time
    latest_log_at falls inside [start, end] → force exactly ONE view rebuild
    and return the now-visible data.

    Simulated by starting with an empty table (schema only) and having the
    patched ``force_rebuild_view`` spy insert the rows — i.e. the rebuild is
    what makes the previously-stale view return data. The status cache is
    patched to report ``latest_log_at`` inside the queried window so the
    tight trigger fires."""
    from datetime import UTC, datetime, timedelta

    from backend.repositories import dashboard as dash

    table_name = _empty_logs_table(in_memory_duckdb, test_service_source)
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)

    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")  # inside [start, end]

    # Status cache reports data exists (the fresh-ingest snapshot).
    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _name: {"local_rows": 30, "earliest_log_at": start_time, "latest_log_at": latest},
    )

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        # The rebuild is what makes the stale view return data: seed the rows.
        rebuild_calls["n"] += 1
        insert_mock_logs(con, table_name, logs)

    monkeypatch.setattr(dash, "force_rebuild_view", fake_rebuild)

    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start_time,
        end_time=end_time,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert rebuild_calls["n"] == 1, f"self-heal must rebuild EXACTLY once; got {rebuild_calls['n']}"
    # After the rebuild + re-query, the data is visible.
    assert result["total_rows"] == 30
    assert result["data"]["url"]["total"] > 0


def test_self_heal_does_not_fire_when_latest_log_outside_window(in_memory_duckdb, test_service_source, monkeypatch):
    """Trigger case (b): a legitimately-empty window whose range does NOT
    contain the all-time latest log → NO rebuild, returns empty.

    This is the false-positive guard: a low-traffic gap window returns 0 rows
    but the all-time latest log is NEWER than end_time, so the window simply
    has no data — rebuilding the view wouldn't help and must not fire."""
    from datetime import UTC, datetime, timedelta

    from backend.repositories import dashboard as dash

    table_name = _empty_logs_table(in_memory_duckdb, test_service_source)
    # Seed rows that are NOW (so the table genuinely has data), but query a
    # historical window that contains none of them.
    insert_mock_logs(in_memory_duckdb, table_name, generate_mock_logs(test_service_source, num_logs=10, hours_ago=0))

    now = datetime.now(UTC)
    # Window is 2 days ago; latest log is ~now → latest is AFTER end_time.
    start_time = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = now.strftime("%Y-%m-%dT%H:%M:%SZ")  # OUTSIDE [start, end]

    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _name: {"local_rows": 10, "earliest_log_at": start_time, "latest_log_at": latest},
    )

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        rebuild_calls["n"] += 1

    monkeypatch.setattr(dash, "force_rebuild_view", fake_rebuild)

    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start_time,
        end_time=end_time,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert rebuild_calls["n"] == 0, "self-heal must NOT fire when latest_log_at is outside the window"
    assert result["total_rows"] == 0


def test_self_heal_does_not_fire_when_filters_applied(in_memory_duckdb, test_service_source, monkeypatch):
    """Trigger case (c): a filtered empty result → NO rebuild.

    With user filters active, an empty window is expected (the filter just
    excludes everything) — not a stale-view symptom. The trigger requires
    ``not filters``, so the rebuild must not fire even when latest_log_at is
    nominally inside the window."""
    from datetime import UTC, datetime, timedelta

    from backend.models.common import FilterSpec
    from backend.repositories import dashboard as dash

    table_name = _empty_logs_table(in_memory_duckdb, test_service_source)
    # Real rows present, but the filter below matches none of them.
    insert_mock_logs(in_memory_duckdb, table_name, generate_mock_logs(test_service_source, num_logs=10, hours_ago=1))

    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")  # inside window

    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _name: {"local_rows": 10, "earliest_log_at": start_time, "latest_log_at": latest},
    )

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        rebuild_calls["n"] += 1

    monkeypatch.setattr(dash, "force_rebuild_view", fake_rebuild)

    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start_time,
        end_time=end_time,
        # A country that doesn't exist in the seeded data → 0 matching rows.
        filters={"country": FilterSpec(mode="include", values=["ZZ"])},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert rebuild_calls["n"] == 0, "self-heal must NOT fire when user filters are applied"
    assert result["total_rows"] == 0


def test_get_aggregates_rollup_path_map_data_uses_per_field_limits(in_memory_duckdb, test_service_source, monkeypatch):
    """Rollup fast-path: map_data must come from the ALREADY-RUNNING batch
    execute_top_n_rollups call via per_field_limits={"country": 500},
    NOT from a second execute_top_n_rollups invocation.

    History: the original choropleth-cap bug (commit 3cec3b0) was fixed
    by adding a second call for ["country"] with limit=500. Profiling
    revealed that second call cost ~200-250ms per request (full duplicate
    active-hour temp + rollup parquet scan for one low-cardinality field).
    This commit collapses to ONE call with per_field_limits — same
    correctness, ~200ms cheaper.

    Pinned to catch a regression that re-introduces the second call OR
    drops per_field_limits and falls back to limit=10 for country (which
    would silently re-cap the choropleth at 10 entries)."""
    import os

    from backend.repositories import dashboard as dash
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    real_isdir = os.path.isdir

    def fake_isdir(path: str) -> bool:
        if path.endswith(os.path.join("rollups", "hour")):
            return True
        return real_isdir(path)

    monkeypatch.setattr(dash.os.path, "isdir", fake_isdir)

    # Track every execute_top_n_rollups call: (fields, limit, per_field_limits).
    calls: list[tuple] = []

    def spy_top_n(self, fields, start_time, end_time, limit=10, per_field_limits=None, _phase_log=None, **_kwargs):
        # **_kwargs absorbs new schema-seed kwargs (actual_cols, schema_types)
        # added by perf commit 6e6a5f9 so this spy stays compatible with future
        # signature growth without re-pinning the test on each plumbing change.
        calls.append((tuple(fields), limit, dict(per_field_limits or {})))
        # Return 12 country entries to confirm the panel caps at 10 but
        # map_data sees all 12.
        country_entries = [("country", f"C{i:02d}", 100 - i) for i in range(12)]
        url_entries = [("url", "/page1", 50), ("url", "/page2", 30)]
        return country_entries + url_entries, list(fields)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", spy_top_n)

    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    # Exactly ONE call to execute_top_n_rollups (not two — that's the perf fix).
    assert len(calls) == 1, f"expected exactly 1 execute_top_n_rollups call (was 2 pre-fix); got {len(calls)}: {calls}"
    fields_called, limit_called, pfl_called = calls[0]
    assert "country" in fields_called, (
        f"country must be included in the batch call so its results can populate map_data; got fields={fields_called}"
    )
    assert pfl_called.get("country") == 500, (
        f"country must use per_field_limits=500 so the choropleth gets the full distribution. "
        f"per_field_limits passed: {pfl_called}"
    )
    assert limit_called == 10, f"default limit for other fields stays at 10; got limit={limit_called}"

    # The panel must cap country at 10 (not show all 12 returned by the spy).
    assert len(result["data"]["country"]["top"]) == 10, (
        f"country PANEL must be capped at 10 entries even when more are available for the map; "
        f"got {len(result['data']['country']['top'])} entries"
    )

    # map_data sees ALL country entries (12 in this fixture, would be up to 500 in prod).
    assert len(result["map_data"]) == 12, (
        f"map_data must include ALL country entries from all_top_res (not the panel-cap slice); "
        f"got {len(result['map_data'])} entries"
    )
    countries = {entry["country"] for entry in result["map_data"]}
    assert countries == {f"C{i:02d}" for i in range(12)}


def test_get_aggregates_topten_only_keeps_rollup_batch_whole(in_memory_duckdb, test_service_source, monkeypatch):
    """Slice 3 invariant: requesting just the topten section MUST keep
    execute_top_n_rollups as ONE merged scan across the full batch_fields
    list. Splitting top-N across HTTP requests would re-trigger N
    active-hour live-temp builds + N rollup-directory enumerations — the
    exact regression the 4-section plan exists to prevent.

    Drive the rollup fast-path via the same fake-isdir monkeypatch as
    test_get_aggregates_rollup_path_map_data_uses_per_field_limits, then
    assert exactly one call AND that include_top_n=True keeps the panel
    population path on (results[field]['top'] populated)."""
    import os

    from backend.repositories import dashboard as dash
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    real_isdir = os.path.isdir

    def fake_isdir(path: str) -> bool:
        if path.endswith(os.path.join("rollups", "hour")):
            return True
        return real_isdir(path)

    monkeypatch.setattr(dash.os.path, "isdir", fake_isdir)

    calls: list[tuple] = []

    def spy_top_n(self, fields, start_time, end_time, limit=10, per_field_limits=None, _phase_log=None, **_kwargs):
        calls.append((tuple(fields), limit, dict(per_field_limits or {})))
        # One row per field so panel population has something to fill.
        return [(f, f"{f}_val", 5) for f in fields], list(fields)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", spy_top_n)

    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
        include_time_series=False,
        include_conn_requests=False,
        include_map_data=False,
        include_top_n=True,
    )

    assert len(calls) == 1, f"topten section must keep the rollup batch whole; got {len(calls)} calls: {calls}"
    # Many fields were batched, not split into separate calls.
    fields_called, _, _ = calls[0]
    assert len(fields_called) > 5, (
        f"the batch call should cover most of FIELDS in one scan, not a per-card slice; got {fields_called}"
    )
    # Per-field totals + tops were populated (proves include_top_n=True flowed through).
    assert result["data"]["url"]["total"] > 0
    assert result["data"]["url"]["top"], "top-N panel must populate when include_top_n=True"
    # Page-shape blocks stayed empty (selector turned them off).
    assert result["time_series"] == []
    assert result["map_data"] == []
    assert result["data"]["conn_requests"]["top"] == []


def test_get_aggregates_core_only_skips_top_n_scan(in_memory_duckdb, test_service_source, monkeypatch):
    """sections=['core'] expands to include_top_n=False — the rollup
    batch call must NOT fire (there's nothing to populate). Map data on
    the rollup path falls back to deriving from all_top_res only when
    map_data is also requested with country in the field list, but with
    no top-N scan and no map gate, both are empty."""
    import os

    from backend.repositories import dashboard as dash
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    real_isdir = os.path.isdir

    def fake_isdir(path: str) -> bool:
        if path.endswith(os.path.join("rollups", "hour")):
            return True
        return real_isdir(path)

    monkeypatch.setattr(dash.os.path, "isdir", fake_isdir)

    calls: list[tuple] = []

    def spy_top_n(self, fields, start_time, end_time, limit=10, per_field_limits=None, _phase_log=None, **_kwargs):
        calls.append((tuple(fields), limit))
        return [], list(fields)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", spy_top_n)

    # ``include_top_n=False`` simulates the router's expansion of
    # sections=['core']. include_map_data stays True so map_data can use
    # the rollup-derived path (which still fires execute_top_n_rollups for
    # country) — proves the partial-skip coupling rule from the plan.
    dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
        include_time_series=True,
        include_conn_requests=True,
        include_map_data=True,
        include_top_n=False,
    )

    # ONE call expected — the map_data rollup-derive path needs country
    # entries from the same scan, so the batch fires once even though
    # the per-field cards are off.
    assert len(calls) == 1, (
        f"core section with map enabled must still trigger one rollup scan for country; got {len(calls)}: {calls}"
    )

    # Now run again with map ALSO off — the batch must skip entirely.
    calls.clear()
    dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
        include_time_series=True,
        include_conn_requests=True,
        include_map_data=False,
        include_top_n=False,
    )
    assert len(calls) == 0, (
        f"with top_n + map both off, the rollup batch must not fire at all; got {len(calls)}: {calls}"
    )


@pytest.mark.skipif(
    DASHBOARD_CACHE_TTL == 0,
    reason=(
        "Dashboard cache disabled in commit 0f0887e after a 2026-06-09 "
        "incident where stale cache entries served 'No data available' "
        "across tabs. Re-enable this assertion when caching is restored."
    ),
)
def test_get_aggregates_result_is_cached(in_memory_duckdb, test_service_source):
    """Second call with identical params returns a cached result."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result1 = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    result2 = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    # The cache-hit path writes the unaliased ``is_cached`` field
    # (matches origin.py's pattern); the ``_is_cached`` Pydantic alias
    # only appears on serialized responses, not raw repository dicts.
    assert result2.get("is_cached") is True
    assert result1["total_rows"] == result2["total_rows"]


def test_get_aggregates_5xx_metric(in_memory_duckdb, test_service_source):
    """chart_metric='5xx' returns time series grouped by status code."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    # Inject some 500s to guarantee 5xx data
    for log in logs[:10]:
        log["status"] = 500
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="5xx",
    )

    assert result["metric"] == "5xx"
    assert isinstance(result["time_series"], list)
    if result["time_series"]:
        pt = result["time_series"][0]
        assert "time" in pt
        assert "value" in pt


def test_get_aggregates_hit_rate_metric(in_memory_duckdb, test_service_source):
    """chart_metric='hit_rate' returns time series with float values."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="hit_rate",
    )

    assert result["metric"] == "hit_rate"
    assert isinstance(result["time_series"], list)
    if result["time_series"]:
        assert "value" in result["time_series"][0]
        # hit_rate is a percentage so should be 0–100
        assert 0.0 <= result["time_series"][0]["value"] <= 100.0


def test_get_aggregates_debug_queries_populated(in_memory_duckdb, test_service_source):
    """_debug_queries list is populated with executed SQL."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert isinstance(result["debug_queries"], list)
    assert len(result["debug_queries"]) > 0
    first_q = result["debug_queries"][0]
    assert "sql" in first_q
    assert "time_ms" in first_q


# ── get_raw_df: DataFrame variant for direct manipulation ──────────────────


def test_get_raw_df_returns_empty_df_when_no_schema(in_memory_duckdb):
    """Missing table → empty pandas DataFrame (not None). Pinned
    because callers do ``df.empty`` checks."""
    out = get_raw_df(
        con=in_memory_duckdb,
        src=_src_without_table(),
        start_time=None,
        end_time=None,
        filters={},
        limit=100,
        columns=[],
    )
    assert out.empty


def test_get_raw_df_returns_pandas_dataframe(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_raw_df(in_memory_duckdb, test_service_source, None, None, {}, limit=5, columns=["timestamp", "status"])

    import pandas as pd

    assert isinstance(out, pd.DataFrame)
    assert len(out) <= 5
    # Only the requested columns are kept
    assert set(out.columns) <= {"timestamp", "status"}


# ── get_field_values: picker dropdown ───────────────────────────────────────


def test_get_field_values_rejects_empty_or_all_unsafe_field_name(in_memory_duckdb, test_service_source):
    """Field name that strips to empty (all unsafe chars) → ValueError.
    The cleaner allows ``[A-Za-z0-9_]`` only; anything else is dropped,
    and if nothing's left we raise rather than execute a bare SQL."""
    with pytest.raises(ValueError, match="Invalid field name"):
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="\";'''",  # all stripped → empty
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )


def test_get_field_values_strips_unsafe_chars_from_field_name(in_memory_duckdb, test_service_source):
    """A field name with mixed safe + unsafe chars gets the unsafe
    ones stripped. The cleaned name is what reaches the table-existence
    check. Pinned because this is the primary defence against SQL
    injection in the field-name slot."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # ``"; DROP TABLE foo`` strips to ``DROPTABLEfoo`` (no spaces/quotes/semis).
    # That field doesn't exist → LookupError, not a SQL execution.
    with pytest.raises(LookupError):
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field='"; DROP TABLE foo',
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )


def test_get_field_values_returns_empty_when_table_missing(in_memory_duckdb):
    """Missing table → empty values (not 500)."""
    out = get_field_values(
        in_memory_duckdb,
        _src_without_table(),
        field="country",
        search="",
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    assert out["values"] == []
    assert out["field"] == "country"


def test_get_field_values_returns_top_values_grouped_by_count(in_memory_duckdb, test_service_source, tmp_path):
    """Happy path: returns ``{values: [{value, count}, ...]}`` sorted
    by count desc.

    Note: patches ``_cache_dir`` to a tmp_path so the test never short-
    circuits on a stale ``cache/default/top_values.json`` left behind
    by other tests that exercise the refresh-status caching path."""
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 20 else "GB"  # 20 US, 10 GB
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"]: v["count"] for v in out["values"]}
    assert "US" in values
    assert values["US"] > values.get("GB", 0)


def test_get_field_values_raises_lookup_error_for_unknown_field(in_memory_duckdb, test_service_source):
    """Field that's valid syntax but not in the table → LookupError
    (which the router maps to 404). Pinned because the picker UI
    distinguishes "no values yet" from "field doesn't exist"."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with pytest.raises(LookupError, match="not found"):
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="completely_unknown_field",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )


def test_get_field_values_filters_by_search_substring(in_memory_duckdb, test_service_source):
    """Non-empty ``search`` → ILIKE filter. Pinned because the
    picker's typeahead box sends this; losing it would surface as
    "search returns nothing"."""
    logs = generate_mock_logs(test_service_source, num_logs=15, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = ["US", "GB", "FR"][i % 3]
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_field_values(
        in_memory_duckdb,
        test_service_source,
        field="country",
        search="U",  # matches "US"
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    # Country search also maps the search term against COUNTRY_MAP
    values = {v["value"] for v in out["values"]}
    assert "US" in values
    # GB and FR don't match "U" as a substring on the code, and
    # neither "United Kingdom" nor "France" matches "U" exactly
    # (though United Kingdom contains "U" — pin permissively)


def test_get_field_values_country_search_resolves_full_names(in_memory_duckdb, test_service_source):
    """Searching ``"united"`` should surface US / GB / etc by looking
    up against ``COUNTRY_MAP``. Pinned because this is the only way
    the picker knows the country names — without it, users would
    have to type ISO codes."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_field_values(
        in_memory_duckdb,
        test_service_source,
        field="country",
        search="united",
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    values = {v["value"] for v in out["values"]}
    assert "US" in values  # United States matched via the name lookup


def test_get_field_values_excludes_own_filter_from_query(in_memory_duckdb, test_service_source, tmp_path):
    """When the user filters on country=US and then opens the country
    picker, the picker should still show ALL countries (not just US)
    so they can switch. Pinned because the FE keys on the picker
    showing all options to be functional.

    Patches ``_cache_dir`` for the same reason as
    ``test_get_field_values_returns_top_values_grouped_by_count``."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 10 else "GB"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    from backend.models.common import FilterSpec

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={"country": FilterSpec(mode="include", values=["US"])},
        )
    # Both US and GB appear because the country filter was excluded from the picker query
    values = {v["value"] for v in out["values"]}
    assert "US" in values and "GB" in values


def test_get_field_values_uses_top_values_cache_when_present(in_memory_duckdb, test_service_source, tmp_path):
    """If a ``top_values.json`` cache exists for the source, the helper
    short-circuits and reads from it. Pinned because losing this would
    re-run the expensive DISTINCT query on every picker open."""
    fake_cache = {"country": [{"value": "ZZ", "count": 999}, {"value": "YY", "count": 1}]}
    cache_path = tmp_path / "top_values.json"
    import json as _json

    cache_path.write_text(_json.dumps(fake_cache))

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",  # no search → cache path eligible
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    # The cached values surface without hitting the table at all
    assert out["values"][0]["value"] == "ZZ"


# ── get_field_values: _bot_name virtual field ─────────────────────────────


def test_get_field_values_bot_name_returns_empty_when_no_ua_column(in_memory_duckdb, test_service_source, tmp_path):
    """The ``_bot_name`` virtual field requires the ``ua`` column —
    without it, return empty (not 500). Pinned because services
    without UA tracking (Group D disabled) should still render the
    bot filter picker as "no options"."""
    # Create the table WITHOUT a ua column
    in_memory_duckdb.execute(
        f'CREATE TABLE "{_safe_table(test_service_source["name"])}" (status INTEGER, timestamp TIMESTAMP)'
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    assert out["values"] == []


def test_get_field_values_bot_name_aggregates_matched_bots(in_memory_duckdb, test_service_source, tmp_path):
    """When ``ua`` is present, query unique UAs and match each to a
    bot via the matcher. Pinned because losing this would silently
    drop the bot filter picker."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["ua"] = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # Mock build_matcher to return a fake matcher mapping Googlebot UA → bot entry
    def fake_matcher_factory():
        def _match(ua):
            if "Googlebot" in ua:
                return [{"id": "googlebot", "name": "Google Bot"}]
            return []

        return _match

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch("backend.utils.bot_sources.build_matcher", side_effect=fake_matcher_factory),
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"]: v for v in out["values"]}
    assert "googlebot" in values
    assert values["googlebot"]["label"] == "Google Bot"
    assert values["googlebot"]["count"] == 10


def test_get_field_values_bot_name_filters_by_search_term(in_memory_duckdb, test_service_source, tmp_path):
    """Search filter applies to BOTH id and name (not just one).
    Pinned because admins use both shorthand IDs and full names."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["ua"] = "Bingbot/2.0"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    def fake_matcher_factory():
        def _match(_ua):
            return [{"id": "bingbot", "name": "Microsoft Bingbot"}]

        return _match

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch("backend.utils.bot_sources.build_matcher", side_effect=fake_matcher_factory),
    ):
        # Search by name fragment → matches
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="microsoft",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert any(v["value"] == "bingbot" for v in out["values"])

        # Search by mismatching term → excluded
        out_miss = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="google",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert out_miss["values"] == []


# ── get_field_values: search variations ────────────────────────────────────


def test_get_field_values_country_search_matches_by_country_name(in_memory_duckdb, test_service_source, tmp_path):
    """Search for "united" matches the COUNTRY_MAP entries for US,
    GB, etc. and includes those rows by IN list. Pinned because losing
    the name-resolution would force users to know ISO-3166 codes."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 5 else "GB"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="united",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    values = {v["value"] for v in out["values"]}
    # Both US (United States) and GB (United Kingdom) match "united"
    assert "US" in values
    assert "GB" in values


def test_get_field_values_asn_search_resolves_via_metadata_db(in_memory_duckdb, test_service_source, tmp_path):
    """When searching ASN by name fragment, the route resolves matching
    ASN integers via metadata_db and includes them in the IN list.
    Pinned because losing the name-resolution would force users to
    know AS numbers."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["asn"] = 13335 if i < 5 else 15169  # Cloudflare / Google
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", return_value=[13335]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="cloudflare",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    values = {str(v["value"]) for v in out["values"]}
    assert "13335" in values


def test_get_field_values_asn_search_swallows_metadata_db_exception(in_memory_duckdb, test_service_source, tmp_path):
    """If asn_ints_for_search raises (DB locked, missing column), fall
    back to the plain ILIKE search instead of 500ing. Pinned because
    metadata_db failures during a deploy shouldn't break the picker."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["asn"] = 13335
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", side_effect=RuntimeError("db locked")),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        # Should not raise; returns whatever the ILIKE matches (likely [])
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="something",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    assert "values" in out


# ── get_aggregates: additional chart_metric branches ──────────────────


def test_get_aggregates_4xx_metric_returns_time_series(in_memory_duckdb, test_service_source):
    """`chart_metric='4xx'` → time series of 4xx error rates. Pinned
    because the FE's 4xx-rate chart keys on this metric name and
    losing it would render an empty chart."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    # Inject some 404s
    for log in logs[:10]:
        log["status"] = 404
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="4xx",
    )

    assert result["metric"] == "4xx"
    assert isinstance(result["time_series"], list)


def test_get_aggregates_p95_latency_metric_uses_percentile_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='p95_latency'` → time series of p95 latency in
    ms. Pinned because the latency chart keys on this exact metric
    name and any rename would blank the panel."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=50)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="p95_latency",
    )

    assert result["metric"] == "p95_latency"


def test_get_aggregates_p50_latency_metric_uses_median_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='p50_latency'` → time series of median latency.
    Pinned because admin perf-debug workflow uses p50 to identify
    sustained vs spiky regressions."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="p50_latency",
    )

    assert result["metric"] == "p50_latency"


def test_get_aggregates_p99_latency_metric_uses_percentile_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='p99_latency'` → time series of p99 latency.
    Pinned because the SRE-watching tail-latency panel keys on this
    exact metric name."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="p99_latency",
    )

    assert result["metric"] == "p99_latency"


def test_get_aggregates_throughput_metric_uses_throughput_formula(in_memory_duckdb, test_service_source):
    """`chart_metric='throughput'` → time series of bandwidth.
    Pinned because the throughput chart on the home dashboard keys
    on this metric — losing it would blank that panel."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="throughput",
    )

    assert result["metric"] == "throughput"


def test_get_aggregates_ttfb_metric_uses_ttfb_ms_expression(in_memory_duckdb, test_service_source):
    """`chart_metric='ttfb'` → time series of TTFB in ms. Pinned
    because the TTFB chart on the performance panel keys on this
    metric name."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="ttfb",
    )

    assert result["metric"] == "ttfb"


def test_get_aggregates_unknown_metric_falls_back_to_requests(in_memory_duckdb, test_service_source):
    """`chart_metric='gibberish'` falls back to requests count time
    series rather than 500ing. Pinned because admins sometimes
    type/paste typo'd metric names — fail-soft is friendlier."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="gibberish_not_a_metric",
    )

    # Falls back to requests count (silent default)
    assert result["metric"] == "requests"


def test_get_field_values_country_search_no_country_map_match_uses_plain_ilike(
    in_memory_duckdb, test_service_source, tmp_path
):
    """When the search string doesn't match any COUNTRY_MAP entry,
    fall back to a plain ILIKE on the country column (so admins can
    still search by code fragment)."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["country"] = "ZZZ"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="zz",  # No COUNTRY_MAP name contains "zz" but the code does
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
    values = {v["value"] for v in out["values"]}
    assert "ZZZ" in values


# ── _add_bot_columns (helper) ───────────────────────────────────────────


def test_add_bot_columns_no_virtual_fields_requested_returns_false_false():
    """Neither ``_bot_name`` nor ``_ngwaf_bot_name`` in columns →
    helper reports (False, False) and leaves select_cols untouched.
    Pinned because losing this would add ua/ip/waf_req_id to every
    query that didn't need them, ballooning the SELECT list."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"status"']
    wants_bot, wants_ngwaf = _add_bot_columns({"ua", "ip", "waf_req_id"}, ["status"], select_cols)
    assert (wants_bot, wants_ngwaf) == (False, False)
    assert select_cols == ['"status"']


def test_add_bot_columns_bot_name_adds_ua_and_ip_when_available():
    """``_bot_name`` requested + ua/ip both in actual_cols → both get
    appended exactly once. Pinned because the bot enricher
    (`enrich_bot_metadata`) reads both ua AND ip for the Arcjet rule
    set — missing either silently produces "unknown bot"."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"timestamp"']
    wants_bot, _ = _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols)
    assert wants_bot is True
    assert '"ua"' in select_cols
    assert '"ip"' in select_cols


def test_add_bot_columns_bot_name_skips_columns_already_in_select_list():
    """If ``"ua"`` is already in select_cols, the helper must NOT add
    a duplicate. Pinned because DuckDB errors on duplicate column
    references in the same SELECT list."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"ua"']  # already present
    _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols)
    # No duplicate
    assert select_cols.count('"ua"') == 1


def test_add_bot_columns_bot_name_omits_missing_columns_silently():
    """``_bot_name`` requested but ua/ip not in actual_cols → no
    column is added (returns True for wants but doesn't break the
    SELECT). Pinned because services that don't log ua should still
    be able to query for _bot_name without 500'ing."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"status"']
    wants_bot, _ = _add_bot_columns(set(), ["_bot_name"], select_cols)
    assert wants_bot is True
    assert select_cols == ['"status"']  # nothing added


def test_add_bot_columns_ngwaf_bot_name_adds_waf_req_id_only_when_present():
    """``_ngwaf_bot_name`` requires only ``waf_req_id`` (NOT ua/ip).
    Pinned because the NGWAF enricher keys solely on waf_req_id —
    adding ua/ip here would over-project columns for NGWAF-only
    services."""
    from backend.repositories.dashboard import _add_bot_columns

    select_cols = ['"timestamp"']
    _, wants_ngwaf = _add_bot_columns({"waf_req_id"}, ["_ngwaf_bot_name"], select_cols)
    assert wants_ngwaf is True
    assert '"waf_req_id"' in select_cols
    # Importantly, ua/ip are NOT added when only _ngwaf_bot_name was requested
    assert '"ua"' not in select_cols
    assert '"ip"' not in select_cols


# ── get_field_values _bot_name virtual field ────────────────────────────


def test_get_field_values_bot_name_returns_empty_when_table_missing(in_memory_duckdb):
    """`_bot_name` field but no table → empty values, no raise.
    Pinned because the FE's bot-filter picker calls this endpoint
    on first render; a 500 would block the whole filter panel."""
    src = _src_without_table()
    out = get_field_values(
        in_memory_duckdb,
        src,
        field="_bot_name",
        search="",
        limit=10,
        start_time=None,
        end_time=None,
        filters={},
    )
    assert out["values"] == []


# ── get_field_values waf_sig_ind (signal unnesting) ─────────────────────


def test_get_field_values_waf_sig_ind_splits_comma_separated_signals(in_memory_duckdb, test_service_source, tmp_path):
    """``waf_sig_ind`` (virtual field) → SQL unnests the comma-
    separated waf_sig column into individual signals. Pinned because
    the security page's "Top WAF signals" picker expects per-signal
    counts, not the raw concatenated string."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["waf_sig"] = "SQLI,XSS" if i % 2 == 0 else "SQLI"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="waf_sig_ind",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"]: v["count"] for v in out["values"]}
    # SQLI present on all 10 rows; XSS only on the 5 even-indexed rows
    assert values.get("SQLI") == 10
    assert values.get("XSS") == 5


def test_get_field_values_waf_sig_ind_applies_search_filter(in_memory_duckdb, test_service_source, tmp_path):
    """Search filter on `waf_sig_ind` runs ILIKE on the unnested
    signal. Pinned because the picker's typeahead won't surface
    matches otherwise."""
    logs = generate_mock_logs(test_service_source, num_logs=4, hours_ago=1)
    for i, log in enumerate(logs):
        log["waf_sig"] = "SQLI" if i % 2 == 0 else "XSS"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="waf_sig_ind",
            search="SQL",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"] for v in out["values"]}
    assert values == {"SQLI"}


# ── get_field_values asn search via metadata_db ──────────────────────────


def test_get_field_values_asn_search_includes_matching_asn_ints(in_memory_duckdb, test_service_source, tmp_path):
    """ASN-name search → `metadata_db.asn_ints_for_search` returns
    matching ints, which get inlined into the WHERE clause. Pinned
    because losing the metadata-DB lookup would force admins to
    type the raw ASN number (defeating the search-by-org-name UX)."""
    logs = generate_mock_logs(test_service_source, num_logs=6, hours_ago=1)
    for i, log in enumerate(logs):
        log["asn"] = 15169 if i < 4 else 32934
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", return_value=[15169]),
        patch("backend.core.duckdb.enrich_asn_labels"),  # avoid SQLite hit
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="Google",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"] for v in out["values"]}
    # 15169 matches via the inlined IN clause; 32934 is excluded
    assert 15169 in values
    assert 32934 not in values


def test_get_field_values_asn_search_falls_back_to_ilike_when_no_metadata_match(
    in_memory_duckdb, test_service_source, tmp_path
):
    """If `asn_ints_for_search` returns an empty list, fall back to
    plain ILIKE on the ASN column. Pinned because losing the
    fallback would silently drop ASN searches for services whose
    per-service metadata SQLite is empty (e.g. brand-new
    services)."""
    logs = generate_mock_logs(test_service_source, num_logs=4, hours_ago=1)
    for log in logs:
        log["asn"] = 15169
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", return_value=[]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="151",  # raw-digit substring
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    values = {v["value"] for v in out["values"]}
    assert 15169 in values


def test_get_aggregates_rollup_path_builds_no_temp_and_serves_conn_requests_rollup(
    in_memory_duckdb, test_service_source, monkeypatch
):
    """Rollup fast-path materializes NO per-request temp (the eager narrow
    live-temp — 391ms on the 2026-07-06 trace — is gone) and the
    conn_requests histogram serves from try_conn_requests_hist_from_rollup."""
    import os

    from backend.repositories import dashboard as dash
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    real_isdir = os.path.isdir

    def fake_isdir(path: str) -> bool:
        if path.endswith(os.path.join("rollups", "hour")):
            return True
        return real_isdir(path)

    monkeypatch.setattr(dash.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("url", "/p", 5)], ["url"]),
    )

    hist_calls: list[dict] = []
    sentinel = {"top": [{"value": "1", "count": 5}, {"value": "21+", "count": 2}], "total": 7}

    def _stub_hist(self, start_time, end_time, *, has_filters, actual_cols=None):
        hist_calls.append({"has_filters": has_filters})
        return dict(sentinel)

    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", _stub_hist)

    temp_creates: list[str] = []
    orig_ctt = QueryRunner.create_temp_table

    def _spy_ctt(self, sql, params=None):
        temp_creates.append(sql)
        return orig_ctt(self, sql, params)

    monkeypatch.setattr(QueryRunner, "create_temp_table", _spy_ctt)

    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert temp_creates == [], f"rollup path must not materialize any temp; got: {temp_creates}"
    markers = {e["section"] for e in result["section_timings"]}
    assert "wide_temp_create" not in markers and "live_temp_create" not in markers, markers
    assert hist_calls == [{"has_filters": False}]
    assert result["data"]["conn_requests"] == sentinel


def test_get_aggregates_conn_requests_rollup_miss_falls_back_to_base_scan(
    in_memory_duckdb, test_service_source, monkeypatch
):
    """When the histogram reader returns None on the rollup path, the live
    CONN_REQUESTS_BUCKET scan runs against the BASE table with the real
    where_clause/params (there is no temp to rewrite them to '1=1')."""
    import os
    from datetime import UTC, datetime, timedelta

    from backend.repositories import dashboard as dash
    from backend.repositories._base import QueryRunner

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=4)
    insert_mock_logs(in_memory_duckdb, table_name, logs)
    # 3 rows in the '1' bucket, 1 row in '6–20'.
    in_memory_duckdb.execute(f"UPDATE {table_name} SET conn_requests = 1")
    in_memory_duckdb.execute(
        f"UPDATE {table_name} SET conn_requests = 7 WHERE rowid IN (SELECT rowid FROM {table_name} LIMIT 1)"
    )

    real_isdir = os.path.isdir

    def fake_isdir(path: str) -> bool:
        if path.endswith(os.path.join("rollups", "hour")):
            return True
        return real_isdir(path)

    monkeypatch.setattr(dash.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([], []),
    )
    monkeypatch.setattr(
        QueryRunner,
        "try_conn_requests_hist_from_rollup",
        lambda self, s, e, *, has_filters, actual_cols=None: None,
    )

    # Wide window: mock-log timestamps are NAIVE local times, so a tight
    # UTC window can miss them by the tz offset. ±26h keeps the rows in
    # regardless of the machine's timezone while still exercising the
    # real where_clause/params on the base-table fallback.
    st = (datetime.now(UTC) - timedelta(hours=26)).isoformat()
    et = (datetime.now(UTC) + timedelta(hours=26)).isoformat()
    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=st,
        end_time=et,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )

    assert result["data"]["conn_requests"] == {
        "top": [{"value": "1", "count": 3}, {"value": "6–20", "count": 1}],
        "total": 4,
    }
