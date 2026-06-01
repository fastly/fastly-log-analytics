"""Regression tests for backend.repositories.insights — validates return shape."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.repositories._base import _safe_table
from backend.repositories.insights import _insights_cache, get_insights
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs

# JFK coords: (40.6413, -73.7781). Client at (0, 0) is ~8,880 km away.
# tcp_rtt=1000 µs → max_km ≈ 200 km — impossible.
_JFK_COORDS = (40.6413, -73.7781)
_MOCK_POP_MAP = {"JFK": _JFK_COORDS}


def _create_impossible_distance_table(con, table_name: str, n: int = 3):
    """Create a log table and insert rows that trigger the impossible-distance insight.

    Client at (0, 0) is ~8,880 km from JFK (40.6413, -73.7781).
    tcp_rtt=1000 µs → max_km ≈ 200 km — physically impossible.
    """
    now = datetime.now(UTC).isoformat()
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "timestamp" TIMESTAMPTZ,
            "ip" VARCHAR,
            "pop" VARCHAR,
            "lat" FLOAT,
            "lon" FLOAT,
            "tcp_rtt" INTEGER,
            "ja3" VARCHAR,
            "status" INTEGER,
            "country" VARCHAR,
            "city" VARCHAR
        )
    """)
    for _ in range(n):
        con.execute(
            f"""INSERT INTO {table_name}
                    (timestamp, ip, pop, lat, lon, tcp_rtt, ja3, status, country, city)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [now, "1.2.3.4", "JFK", 0.0, 0.0, 1000, "aabbcc112233", 200, "US", "SomeCity"],
        )


@pytest.fixture(autouse=True)
def clear_caches():
    _insights_cache.clear()
    _clear_schema_cache()
    yield
    _insights_cache.clear()
    _clear_schema_cache()


_EXPECTED_TOP_LEVEL_KEYS = {
    "insights",
    "window_start",
    "window_end",
    "baseline_start",
    "baseline_end",
    "computed_at",
    "window_hours",
    "baseline_hours",
    "window_total_requests",
}


def test_get_insights_empty_table_returns_expected_keys(in_memory_duckdb, test_service_source):
    """Empty table returns all expected top-level keys with zero requests."""
    from backend.core.log_fields import LOG_FIELD_CATALOG

    table_name = _safe_table(test_service_source["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    for key in _EXPECTED_TOP_LEVEL_KEYS:
        assert key in result, f"Missing top-level key: {key}"
    assert isinstance(result["insights"], list)
    assert result["window_hours"] == 1
    assert result["baseline_hours"] == 24
    assert result["window_total_requests"] == 0


def test_get_insights_with_data_returns_expected_keys(in_memory_duckdb, test_service_source):
    """With data present, all expected keys are returned."""
    logs = generate_mock_logs(test_service_source, num_logs=50, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    for key in _EXPECTED_TOP_LEVEL_KEYS:
        assert key in result, f"Missing top-level key: {key}"
    assert isinstance(result["insights"], list)
    assert isinstance(result["window_total_requests"], int)


def test_get_insights_insight_items_have_required_fields(in_memory_duckdb, test_service_source):
    """Each insight item (if any) contains id, label, items, and severity."""
    logs = generate_mock_logs(test_service_source, num_logs=100, hours_ago=1)
    # Skew data to trigger anomaly detection: make some IPs very high volume
    for i, log in enumerate(logs[:30]):
        log["ip"] = "10.99.99.99"
        log["status"] = 404
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    for insight in result["insights"]:
        assert "id" in insight, f"Insight missing 'id': {list(insight.keys())}"
        assert "title" in insight, f"Insight missing 'title': {list(insight.keys())}"
        assert "items" in insight, f"Insight missing 'items': {list(insight.keys())}"
        assert isinstance(insight["items"], list)


def test_get_insights_no_table_returns_empty(in_memory_duckdb, test_service_source):
    """When no log data exists, all keys are present and window_total_requests is 0."""
    # Don't insert any data — table won't exist (or iceberg view will be empty)
    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    for key in _EXPECTED_TOP_LEVEL_KEYS:
        assert key in result, f"Missing key: {key}"
    assert result["window_total_requests"] == 0
    # Any returned insight stubs must have empty items (no anomalies detected)
    for insight in result["insights"]:
        assert insight.get("items") == [], f"Expected empty items on no-data insight: {insight['id']}"


def test_impossible_distance_excluded_when_no_pop_data(in_memory_duckdb, test_service_source):
    """impossible_distance insight is not included when the POP location cache is empty."""
    table_name = _safe_table(test_service_source["name"])
    _create_impossible_distance_table(in_memory_duckdb, table_name)

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={}):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)

    ids = [i["id"] for i in result["insights"]]
    assert "impossible_distance" not in ids


# ── Cache hit path ─────────────────────────────────────────────────────────


def test_get_insights_cached_result_is_returned_on_second_call(in_memory_duckdb, test_service_source):
    """A second call with identical params returns the cached dict
    with `_is_cached=True`. Pinned because the dashboard re-renders
    on every tab switch — losing the cache would re-run the whole
    insight pipeline (heavy SQL) on every render."""
    from backend.core.log_fields import LOG_FIELD_CATALOG

    table_name = _safe_table(test_service_source["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    out1 = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    out2 = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)

    assert "_is_cached" not in out1
    assert out2.get("_is_cached") is True


# ── Empty-schema short-circuit ────────────────────────────────────────────


def test_get_insights_returns_zero_total_when_table_empty(in_memory_duckdb):
    """Service with no rows → `window_total_requests=0` with the
    standard top-level shape. Pinned because the dashboard renders
    "0 requests in window" rather than 500 when the table is empty.
    Note: insights array may still contain `info`-severity placeholders
    (one per registered insight) explaining the data gap — the
    important contract is that the top-level shape stays consistent."""
    src = {"name": "no-table-svc", "service_id": "x"}
    out = get_insights(in_memory_duckdb, src, window_hours=1, baseline_hours=24)
    # All top-level keys present
    for key in ("insights", "window_start", "window_end", "computed_at", "window_total_requests"):
        assert key in out
    # Zero requests recorded
    assert out["window_total_requests"] == 0
    # Insights list is well-formed (may be empty OR contain info placeholders)
    assert isinstance(out["insights"], list)
    # Every insight has the required keys
    for ins in out["insights"]:
        assert "id" in ins
        assert "severity" in ins
        assert "items" in ins


# ── Insufficient history check_baseline branch ───────────────────────────


def test_get_insights_check_baseline_creates_info_insight_when_history_short(
    in_memory_duckdb,
    test_service_source,
):
    """When available history < baseline_hours, insights that rely on
    historical comparison return an `info`-severity placeholder
    explaining "Requires Xh of historical data (only Yh available)".
    Pinned because the FE renders this exact text to tell admins
    when they're undersampling."""
    table_name = _safe_table(test_service_source["name"])
    _create_impossible_distance_table(in_memory_duckdb, table_name, n=1)

    # Request baseline of 24h — but only the recent row exists
    result = get_insights(
        in_memory_duckdb,
        test_service_source,
        window_hours=1,
        baseline_hours=24,
    )

    # At least one insight should have the "requires Xh of history" placeholder
    info_insights = [i for i in result["insights"] if i.get("severity") == "info"]
    assert any("Requires" in i.get("summary", "") for i in info_insights)


def test_temp_table_projects_every_registered_required_field(in_memory_duckdb, test_service_source):
    """Pin that the insights temp-table projection includes every
    ``required_field`` declared by every registered insight definition.

    Previously a hard-coded ``needed_cols`` list in ``get_insights``
    silently dropped columns like ``metro`` / ``tls_ciphers_sha`` /
    ``oretries``. Insights whose SQL referenced those columns then
    raised ``Binder Error: Referenced column "X" not found in FROM
    clause`` and returned as ``severity="error"`` entries to the FE."""
    from backend.repositories.insights.registry import registry

    all_required: set[str] = {"timestamp"}
    for d in registry.get_all():
        all_required.update(d.required_fields)

    table_name = _safe_table(test_service_source["name"])
    # Build the schema with sensible types so SQL like `tcp_rtt > 0` works.
    type_map = {
        "timestamp": "TIMESTAMPTZ",
        "tcp_rtt": "INTEGER",
        "elapsed": "INTEGER",
        "status": "INTEGER",
        "lat": "FLOAT",
        "lon": "FLOAT",
        "edge": "BOOLEAN",
    }
    cols_sql = ", ".join(f'"{c}" {type_map.get(c, "VARCHAR")}' for c in sorted(all_required))
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql})")

    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)

    binder_failures = [
        i for i in result["insights"] if i.get("severity") == "error" and "not found in FROM" in i.get("summary", "")
    ]
    assert not binder_failures, (
        f"Insights failed with binder errors: {[(i['id'], i['summary']) for i in binder_failures]}"
    )


def test_error_path_emits_distinct_insight_ids(in_memory_duckdb, test_service_source):
    """Pin that when multiple insights raise during execution, the
    resulting error-path entries carry each failing insight's actual
    ``id`` (and not a duplicate placeholder).

    Previously every per-insight closure was named ``compute_insight``,
    so the error handler — which read ``fn.__name__`` — emitted
    ``id="insight"`` for every failure. The FE keys insight cards on
    ``insight.id``; duplicate ids triggered React's "two children with
    the same key" warning and risked omitted renders."""
    from datetime import timedelta

    from backend.repositories.insights.registry import InsightDefinition, registry

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "status" INTEGER)')
    # Insert a row inside the [baseline_start, now] window so the temp-table
    # filter keeps it, and far enough back so ``check_baseline`` lets the
    # insight SQL actually run (otherwise we get an "info" placeholder).
    old_ts = (datetime.now(UTC) - timedelta(hours=23, minutes=30)).isoformat()
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES (?, ?)",
        [old_ts, 200],
    )

    stub_a = InsightDefinition(
        id="stub_alpha",
        title="Stub Alpha",
        sql_template="SELECT nonexistent_col_alpha FROM {table_name}",
        required_fields=["timestamp", "status"],
    )
    stub_b = InsightDefinition(
        id="stub_beta",
        title="Stub Beta",
        sql_template="SELECT nonexistent_col_beta FROM {table_name}",
        required_fields=["timestamp", "status"],
    )

    with patch.object(registry, "get_all", return_value=[stub_a, stub_b]):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=23)

    error_entries = [i for i in result["insights"] if i.get("severity") == "error"]
    ids = sorted(i["id"] for i in error_entries)
    assert ids == ["stub_alpha", "stub_beta"], f"Expected distinct ids for each failing insight, got {ids}"
    titles = {i["id"]: i["title"] for i in error_entries}
    assert titles["stub_alpha"] == "Stub Alpha"
    assert titles["stub_beta"] == "Stub Beta"


def test_all_insight_sql_templates_execute_without_error(in_memory_duckdb, test_service_source):
    """Pin that every registered insight's SQL template hydrates and
    executes without raising on a fully-populated schema.

    This is the catch-all guard for SQL template bugs that the
    "missing-column" check (``test_temp_table_projects_every_registered_required_field``)
    can't detect. Past failures included:
    - ``image_optimization_opportunities``: stray comma after the
      ``avg_kb`` column when ``{ua_mobile_sel}`` expanded with its
      own leading comma → ``syntax error at or near ","``.
    - ``new_probe_urls``: the ``(?i)`` flag literal in NEW_PROBE_REGEX
      contained a ``?`` that ``sql.count("?")`` mis-counted as a
      placeholder → ``Parameter argument/count mismatch``.
    - ``proxy_surge``: SELECTed bare ``timestamp`` in a CTE that
      ``GROUP BY "p_type"`` → ``"timestamp" must appear in the GROUP
      BY clause``.
    - ``city_surges`` / ``new_city_traffic``: used ``{region_sel}``
      (which expanded to ``NULL AS region``) in a GROUP BY clause →
      ``Referenced column "region" not found``.
    - ``city_error_spikes`` / ``city_latency_regressions``: used
      ``{region_sel} AS region`` where ``{region_sel}`` already
      contained ``AS region`` → ``cannot be referenced before defined``.

    If a future insight is added with a broken SQL template, this
    test surfaces the failure immediately rather than waiting for a
    user to hit it in production."""
    from datetime import timedelta

    from backend.core.log_fields import LOG_FIELD_CATALOG

    table_name = _safe_table(test_service_source["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    # Insert one row inside the [baseline_start, now] window so that
    # ``check_baseline`` lets every insight's SQL actually run.
    old_ts = (datetime.now(UTC) - timedelta(minutes=90)).isoformat()
    cols = [f'"{f["id"]}"' for f in raw_fields]
    placeholders = ", ".join(["?"] * len(cols))
    numeric_types = {
        "INTEGER",
        "BIGINT",
        "DOUBLE",
        "FLOAT",
        "TINYINT",
        "SMALLINT",
        "UINT8",
        "UINT16",
        "UINT32",
        "UINT64",
    }
    values: list = []
    for f in raw_fields:
        if f["id"] == "timestamp":
            values.append(old_ts)
        elif f["duckdb_type"] == "BOOLEAN":
            values.append(True)
        elif any(t in f["duckdb_type"].upper() for t in numeric_types):
            values.append(1)
        else:
            values.append("x")
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})",
        values,
    )

    # impossible_distance needs a non-empty POP map or it returns None
    # (skipped, not errored). Patch with a tiny map.
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"SJC": (37.3626, -121.929)}):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)

    errors = [i for i in result["insights"] if i.get("severity") == "error"]
    assert not errors, "These insights raised at execution time — likely SQL template bugs:\n" + "\n".join(
        f"  {i['id']}: {i.get('summary', '')[:200]}" for i in errors
    )


def test_get_insights_empty_actual_cols_returns_empty_response_with_telemetry(in_memory_duckdb):
    """No schema (table missing entirely) → returns the empty-response
    dict with telemetry already merged. Pinned because the FE keys
    on the standard top-level shape — losing it would render a
    confusing "loading forever" state for brand-new services that
    haven't ingested yet."""
    src = {"name": "no_table_svc_999", "service_id": "x"}
    out = get_insights(in_memory_duckdb, src, window_hours=1, baseline_hours=24)
    assert out["window_total_requests"] == 0
    assert out["insights"] == [] or all(i.get("items") == [] for i in out["insights"])
    # debug_queries should be merged from runner.telemetry()
    assert "debug_queries" in out or "debug_calls" in out


def test_get_insights_falls_back_to_source_table_when_temp_table_creation_fails(
    in_memory_duckdb,
    test_service_source,
):
    """If ``runner.create_temp_table`` returns False (e.g. read-only
    connection), fall back to querying the source table directly
    instead of crashing. Pinned because analyst replicas open
    read-only connections — losing the fallback would make insights
    unavailable for the entire read-only path."""
    table_name = _safe_table(test_service_source["name"])
    _create_impossible_distance_table(in_memory_duckdb, table_name)

    with patch(
        "backend.repositories._base.QueryRunner.create_temp_table",
        return_value=False,
    ):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)

    # Top-level shape is intact even on fallback
    assert "insights" in result
    assert isinstance(result["insights"], list)


def test_get_insights_handles_earliest_timestamp_as_string(in_memory_duckdb, test_service_source):
    """When DuckDB returns the earliest timestamp as a string instead
    of a datetime (rare driver path), `parse_iso_utc` converts it.
    Pinned because losing this branch would silently zero
    available_history_hours and force every insight into the
    "Requires Xh of historical data" placeholder even when data is
    present."""
    from backend.repositories import _base as _base_mod

    table_name = _safe_table(test_service_source["name"])
    _create_impossible_distance_table(in_memory_duckdb, table_name)

    real_execute = _base_mod.QueryRunner.execute
    call_count = [0]

    class _StringTimestampResult:
        def fetchone(self):
            return ("2026-05-18T00:00:00Z",)

        def fetchall(self):
            return []

    def faux_execute(self, query, params=None):
        if isinstance(query, str) and "min(timestamp)" in query.lower():
            call_count[0] += 1
            return _StringTimestampResult()
        return real_execute(self, query, params) if params is not None else real_execute(self, query)

    with patch.object(_base_mod.QueryRunner, "execute", faux_execute):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)

    assert call_count[0] >= 1
    assert "insights" in result


def test_get_insights_swallows_window_total_count_exception(in_memory_duckdb, test_service_source):
    """If the per-window count(*) query fails (locked table mid-tick),
    fall back to ``w_total = 0`` rather than 500'ing the whole
    endpoint. Pinned because a single flaky count must not take
    down the insights page."""
    from backend.repositories import _base as _base_mod

    table_name = _safe_table(test_service_source["name"])
    _create_impossible_distance_table(in_memory_duckdb, table_name)

    real_execute = _base_mod.QueryRunner.execute

    def faux_execute(self, query, params=None):
        if isinstance(query, str) and "count(*)" in query.lower() and "WHERE timestamp >= CAST(?" in query:
            raise RuntimeError("transient lock")
        return real_execute(self, query, params) if params is not None else real_execute(self, query)

    with patch.object(_base_mod.QueryRunner, "execute", faux_execute):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)

    assert result["window_total_requests"] == 0


def test_get_insights_skips_insight_with_keyerror_during_template_hydration(
    in_memory_duckdb,
    test_service_source,
):
    """If an insight's SQL template references a `{placeholder}` that
    isn't in the hydration kwargs, the per-insight `KeyError` is
    swallowed → return None for that insight. Pinned because losing
    this would crash the entire insights pipeline when a single
    template has a typo'd placeholder."""
    from backend.repositories.insights.registry import InsightDefinition, registry

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "status" INTEGER)')
    from datetime import timedelta

    old_ts = (datetime.now(UTC) - timedelta(hours=23, minutes=30)).isoformat()
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES (?, ?)",
        [old_ts, 200],
    )

    bad_stub = InsightDefinition(
        id="stub_typo",
        title="Stub Typo",
        sql_template="SELECT 1 FROM {table_name} WHERE {my_typo}",
        required_fields=["timestamp", "status"],
    )

    with patch.object(registry, "get_all", return_value=[bad_stub]):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=23)

    assert "insights" in result
    ids = [i["id"] for i in result["insights"]]
    assert "stub_typo" not in ids


def test_get_insights_swallows_row_processor_exceptions_per_row(
    in_memory_duckdb,
    test_service_source,
):
    """A row_processor that raises must NOT kill the whole insight;
    losing this would let a single malformed row blank the entire
    "Error Spikes" card."""
    from backend.repositories.insights.registry import InsightDefinition, registry

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "status" INTEGER)')
    from datetime import timedelta

    base = datetime.now(UTC) - timedelta(hours=23)
    for i in range(3):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES (?, ?)",
            [(base + timedelta(minutes=i)).isoformat(), 200 + i],
        )

    call_count = [0]

    def raising_processor(row, definition, context):
        call_count[0] += 1
        raise RuntimeError("synthetic processor failure")

    stub = InsightDefinition(
        id="stub_proc_raises",
        title="Stub Proc Raises",
        sql_template='SELECT "status", count(*) FROM {table_name} GROUP BY "status"',
        required_fields=["timestamp", "status"],
        row_processor=raising_processor,
    )

    with patch.object(registry, "get_all", return_value=[stub]):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=23)

    stub_card = next((i for i in result["insights"] if i["id"] == "stub_proc_raises"), None)
    assert stub_card is not None
    assert stub_card["items"] == []
    assert call_count[0] >= 1


def test_get_insights_error_spikes_summary_renders_url_count(in_memory_duckdb, test_service_source):
    """When `error_spikes` has items, the summary text is
    "N URLs with elevated server error rates". Pinned because the
    dashboard's anomaly headline keys on that wording."""
    from backend.repositories.insights.registry import InsightDefinition, registry

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "status" INTEGER)')
    from datetime import timedelta

    base = datetime.now(UTC) - timedelta(hours=23)
    for i in range(3):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES (?, ?)",
            [(base + timedelta(minutes=i)).isoformat(), 500],
        )

    fake_row_processor = lambda row, d, ctx: {"label": str(row[0]), "current_val": row[1]}  # noqa: E731

    stub = InsightDefinition(
        id="error_spikes",
        title="Error Spikes",
        sql_template='SELECT "status", count(*) FROM {table_name} GROUP BY "status"',
        required_fields=["timestamp", "status"],
        row_processor=fake_row_processor,
    )

    with patch.object(registry, "get_all", return_value=[stub]):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=23)

    card = next((i for i in result["insights"] if i["id"] == "error_spikes"), None)
    assert card is not None and card["items"]
    assert "URLs with elevated server error rates" in card["summary"]


def test_get_insights_botnet_grouping_summary_renders_fingerprint_count(
    in_memory_duckdb,
    test_service_source,
):
    """When `botnet_grouping` has items, the summary text is
    "N fingerprints with suspicious IP spread". Same pinning
    rationale as the `error_spikes` summary test above."""
    from backend.repositories.insights.registry import InsightDefinition, registry

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ja3" VARCHAR, "ja4" VARCHAR)'
    )
    from datetime import timedelta

    base = datetime.now(UTC) - timedelta(hours=23)
    for i in range(3):
        in_memory_duckdb.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)",
            [(base + timedelta(minutes=i)).isoformat(), "ja3_fp", "ja4_fp"],
        )

    fake_row_processor = lambda row, d, ctx: {"label": str(row[0])}  # noqa: E731

    stub = InsightDefinition(
        id="botnet_grouping",
        title="Botnet Grouping",
        sql_template='SELECT DISTINCT "ja3" FROM {table_name}',
        required_fields=["timestamp", "ja3"],
        row_processor=fake_row_processor,
    )

    with patch.object(registry, "get_all", return_value=[stub]):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=23)

    card = next((i for i in result["insights"] if i["id"] == "botnet_grouping"), None)
    assert card is not None and card["items"]
    assert "fingerprints with suspicious IP spread" in card["summary"]


def test_get_insights_severity_logic_callable_overrides_default(in_memory_duckdb, test_service_source):
    """When an insight definition supplies a `severity_logic` callable,
    its return value wins over the default `_sev()`. Pinned because
    custom insights (e.g. proxy_surge → always "warning") rely on
    this hook."""
    from backend.repositories.insights.registry import InsightDefinition, registry

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "status" INTEGER)')
    from datetime import timedelta

    # Insert 90min ago: comfortably inside the temp-table window
    # ([now-2h, now] given baseline=1 + window=1) AND old enough that
    # available_history_hours (~1.5) clears baseline_hours=1 with margin.
    # This test pins severity_logic, not the baseline-check shortcut.
    ts = (datetime.now(UTC) - timedelta(minutes=90)).isoformat()
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES (?, ?)",
        [ts, 500],
    )

    fake_row_processor = lambda row, d, ctx: {"label": "x", "severity": "critical"}  # noqa: E731

    stub = InsightDefinition(
        id="stub_custom_sev",
        title="Stub Custom Sev",
        sql_template='SELECT "status" FROM {table_name}',
        required_fields=["timestamp", "status"],
        row_processor=fake_row_processor,
        severity_logic=lambda items: "warning",
    )

    with patch.object(registry, "get_all", return_value=[stub]):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)

    card = next(i for i in result["insights"] if i["id"] == "stub_custom_sev")
    assert card["severity"] == "warning"


def test_impossible_distance_items_include_pop_coords(in_memory_duckdb, test_service_source):
    """When POP data is cached, impossible-distance items include finite pop_lat and pop_lon."""
    table_name = _safe_table(test_service_source["name"])
    _create_impossible_distance_table(in_memory_duckdb, table_name)

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_MOCK_POP_MAP):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)

    insight = next((i for i in result["insights"] if i["id"] == "impossible_distance"), None)
    assert insight is not None, "impossible_distance insight should be present when POP data available"
    assert len(insight["items"]) > 0, "Expected at least one impossible-distance item"
    for item in insight["items"]:
        meta = item["meta"]
        assert meta["pop_lat"] is not None, "pop_lat must not be None"
        assert meta["pop_lon"] is not None, "pop_lon must not be None"
        assert isinstance(meta["pop_lat"], float), f"pop_lat should be float, got {type(meta['pop_lat'])}"
        assert isinstance(meta["pop_lon"], float), f"pop_lon should be float, got {type(meta['pop_lon'])}"
        assert meta["client_lat"] is not None, "client_lat must not be None"
        assert meta["client_lon"] is not None, "client_lon must not be None"
