"""Regression tests for backend.repositories.insights — validates return shape."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from backend.repositories.insights import _insights_cache, get_insights
from backend.utils.date_utils import parse_iso_utc
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
    """A second call with identical params returns the cached dict, and the
    cache-hit marker survives the response-model round-trip as
    ``_is_cached=True``. Pinned because the dashboard re-renders on every
    tab switch — losing the cache would re-run the whole insight pipeline
    (heavy SQL) on every render.

    Regression guard: the repo stamps the model FIELD name
    (``is_cached``), not its serialization alias. Stamping the alias was
    dropped on validation, so every cache hit serialized as
    ``"_is_cached": false`` — invisible to clients. The raw-dict-only
    assertion this test used to make never exercised serialization, which
    is why the bug slipped through; we now round-trip through the model.
    """
    from backend.core.log_fields import LOG_FIELD_CATALOG
    from backend.models.dashboard import InsightsResponse

    table_name = _safe_table(test_service_source["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    out1 = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    out2 = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)

    # Freshly-computed result carries no cache marker; the cache-hit path
    # stamps the unaliased field name.
    assert out1.get("is_cached") is not True
    assert out2.get("is_cached") is True

    # And it must survive serialization as the wire-facing alias.
    dumped = InsightsResponse(**out1).model_dump(by_alias=True)
    assert dumped["_is_cached"] is False
    dumped_cached = InsightsResponse(**out2).model_dump(by_alias=True)
    assert dumped_cached["_is_cached"] is True


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

    from backend.repositories.insights.registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.traffic,
        title="Stub Alpha",
        sql_template="SELECT nonexistent_col_alpha FROM {table_name}",
        required_fields=["timestamp", "status"],
    )
    stub_b = InsightDefinition(
        id="stub_beta",
        category=InsightCategory.traffic,
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
    from backend.repositories.insights.registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.traffic,
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
    from backend.repositories.insights.registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.traffic,
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
    from backend.repositories.insights.registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.traffic,
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
    from backend.repositories.insights.registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.traffic,
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
    from backend.repositories.insights.registry import InsightCategory, InsightDefinition, registry

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
        category=InsightCategory.traffic,
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


def _seed_city_data_for_all_four_insights(con, table_name: str) -> None:
    """Insert rows engineered to trigger each of the 4 city-based insights.

    Layout:
      - city_surges: "SurgeCity" has 25 window reqs vs 0 baseline reqs
        (HAVING w_cnt >= 20 AND w_cnt > b_normalized * 3).
      - city_error_spikes: "ErrorCity" has 20 window reqs, 15 status>=400
        (75% rate, well above the 10% floor and the 3× baseline ratchet).
      - city_latency_regressions: "SlowCity" needs >= 10 window reqs and
        >= 50 baseline reqs with w_p95 / b_p95 ratio >= 3 and absolute
        delta >= 500. Window has 12 rows at elapsed=2_000_000 (p95=2000 ms);
        baseline has 60 rows at elapsed=100_000 (p95=100 ms).
      - new_city_traffic: "FreshCity" has 8 window reqs and 0 baseline.
    """
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)  # within last hour (window)
    baseline_ts = now - timedelta(hours=12)  # < window_start (baseline)

    def _ins(ts: datetime, city: str, region: str, country: str, status: int, elapsed: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "city", "region", "country", "status", "elapsed") '
            "VALUES (?, ?, ?, ?, ?, ?)",
            [ts.isoformat(), city, region, country, status, elapsed],
        )

    # city_surges: 25 window rows in SurgeCity (>= 20 trigger floor), no baseline
    for _ in range(25):
        _ins(window_ts, "SurgeCity", "RegionS", "US", 200, 50_000)

    # city_error_spikes: 20 window rows in ErrorCity, 15 of which are 5xx (75% rate)
    for i in range(20):
        _ins(window_ts, "ErrorCity", "RegionE", "US", 500 if i < 15 else 200, 50_000)
    # 50 baseline rows at 1% error rate (so b_rate ≈ 0.02) — keeps b_total < 50? no,
    # b_total = 50 which is NOT < 50, so the ratchet path applies: w_rate (0.75)
    # >= b_rate (0.02) * 3 + 0.05 = 0.11 → True.
    for i in range(50):
        _ins(baseline_ts, "ErrorCity", "RegionE", "US", 500 if i < 1 else 200, 50_000)

    # city_latency_regressions: 12 window rows at elapsed=2_000_000us (p95=2000ms),
    # 60 baseline rows at elapsed=100_000us (p95=100ms). w_p95/b_p95 = 20 >= 3, and
    # delta 1900 >= 500.
    for _ in range(12):
        _ins(window_ts, "SlowCity", "RegionL", "US", 200, 2_000_000)
    for _ in range(60):
        _ins(baseline_ts, "SlowCity", "RegionL", "US", 200, 100_000)

    # new_city_traffic: 8 window rows in FreshCity, 0 baseline. b_cnt = 0
    for _ in range(8):
        _ins(window_ts, "FreshCity", "RegionF", "US", 200, 50_000)


def test_coalesced_city_path_matches_per_insight_scan_output(in_memory_duckdb, test_service_source, monkeypatch):
    """Regression for O2: the coalesced city-aggregate path
    (`_coalesced_city_aggregates`) must produce per-insight items that
    are *equivalent* to the legacy per-insight scans.

    Compares the 4 city-based insights (city_surges, city_error_spikes,
    city_latency_regressions, new_city_traffic) item-by-item between
    the coalesced path (fast) and the legacy path (fallback when
    coalescing is monkeypatched out).
    """
    from backend.repositories.insights import repository as insights_repo

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, '
        '"city" VARCHAR, '
        '"region" VARCHAR, '
        '"country" VARCHAR, '
        '"status" INTEGER, '
        '"elapsed" INTEGER'
        ")"
    )
    _seed_city_data_for_all_four_insights(in_memory_duckdb, table_name)

    # Pass 1 — coalesced path (default).
    _insights_cache.clear()
    fast = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    fast_city = {i["id"]: i for i in fast["insights"] if i["id"].startswith(("city_", "new_city"))}

    # Pass 2 — disable coalescing, force per-insight scans.
    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, "_coalesced_city_aggregates", lambda *a, **k: {})
    slow = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    slow_city = {i["id"]: i for i in slow["insights"] if i["id"].startswith(("city_", "new_city"))}

    # Verify both paths produced all four city insights at all.
    expected_ids = {"city_surges", "city_error_spikes", "city_latency_regressions", "new_city_traffic"}
    assert set(fast_city.keys()) == expected_ids, f"fast missing: {expected_ids - set(fast_city.keys())}"
    assert set(slow_city.keys()) == expected_ids, f"slow missing: {expected_ids - set(slow_city.keys())}"

    for insight_id in expected_ids:
        fast_items = fast_city[insight_id]["items"]
        slow_items = slow_city[insight_id]["items"]

        assert len(fast_items) == len(slow_items), (
            f"{insight_id}: fast had {len(fast_items)} items, slow had {len(slow_items)}"
        )

        # Compare ordered tuples of (label, current_val, baseline_val) — order
        # matters because each insight has an ORDER BY clause that the bypass
        # has to replicate. Use rough float-equality on values because the
        # coalesced PERCENTILE_CONT and the legacy one can differ in the
        # last ULP across SQL execution paths.
        def _norm(items: list[dict]) -> list[tuple]:
            return [
                (
                    i["label"],
                    round(float(i.get("current_val") or 0), 4),
                    round(float(i.get("baseline_val") or 0), 4),
                )
                for i in items
            ]

        assert _norm(fast_items) == _norm(slow_items), (
            f"{insight_id} item lists differ between fast and slow paths:\n"
            f"  fast: {_norm(fast_items)}\n  slow: {_norm(slow_items)}"
        )


def _seed_url_data_for_all_url_insights(con, table_name: str) -> None:
    """Insert rows engineered to trigger each of the URL-keyed insights
    folded into the coalesced URL aggregate (Step 2 / Option C, 2026-06-06).

    Layout:
      - error_spikes: "ErrUrl" has 20 window reqs, 14 5xx (70% rate, well
        above 5% floor + 2× baseline ratchet).
      - cache_collapse: "CollUrl" has 30 window reqs (5 HITs / 25 MISS →
        17% cacheable hit ratio) vs 200 baseline reqs (160 HITs / 40 MISS →
        80%). Drop is 63 points (>= 20), and 17% <= 80% * 0.6 = 48%.
      - cacheability_regression: "PassUrl" has 20 window reqs all PASS
        (100% PASS) vs 60 baseline reqs all cacheable (0% PASS). The window
        has 0 cacheable reqs, so it does NOT trip cache_collapse.
      - latency_regression: "RegUrl" has 12 window reqs at elapsed=4_000_000
        (p95=4000ms) vs 60 baseline at elapsed=200_000 (p95=200ms).
        w_p95/b_p95 = 20 >= 2.0; delta 3800 >= 200.
      - tail_latency: "TailUrl" has 25 window reqs with elapsed distribution
        producing p99 >> 5*p50. 23 fast (elapsed=10_000) + 2 slow
        (elapsed=10_000_000) → p99 ≈ 10000ms, p50 ≈ 10ms, ratio ≈ 1000.
    """
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    # Baseline rows must be old enough that available_history >= baseline_hours
    # (24h) or get_insights' check_baseline short-circuits every insight with an
    # empty "needs more history" card. 24.5h sits inside [now-25h, now-1h).
    baseline_ts = now - timedelta(hours=24, minutes=30)

    def _ins(ts: datetime, url: str, status: int, cache: str, elapsed: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "url", "status", "cache", "elapsed") VALUES (?, ?, ?, ?, ?)',
            [ts.isoformat(), url, status, cache, elapsed],
        )

    # error_spikes: "ErrUrl" — 20 window, 14 of which are 5xx
    for i in range(20):
        _ins(window_ts, "/ErrUrl", 500 if i < 14 else 200, "MISS", 50_000)
    # Baseline: 50 reqs, only 1 5xx (b_rate ~ 0.02), so w_rate 0.70 >= 0.02*2+0.05=0.09 ✓
    for i in range(50):
        _ins(baseline_ts, "/ErrUrl", 500 if i < 1 else 200, "MISS", 50_000)

    # cache_collapse: "CollUrl" — 30 window (5 HIT = 17%) vs 200 baseline (160 HIT = 80%)
    for i in range(30):
        _ins(window_ts, "/CollUrl", 200, "HIT" if i < 5 else "MISS", 50_000)
    for i in range(200):
        _ins(baseline_ts, "/CollUrl", 200, "HIT" if i < 160 else "MISS", 50_000)

    # cacheability_regression: "PassUrl" — 20 window all PASS, 60 baseline all cacheable
    for _ in range(20):
        _ins(window_ts, "/PassUrl", 200, "PASS", 50_000)
    for i in range(60):
        _ins(baseline_ts, "/PassUrl", 200, "HIT" if i < 50 else "MISS", 50_000)

    # latency_regression: "RegUrl" — 12 window at 4000ms, 60 baseline at 200ms
    for _ in range(12):
        _ins(window_ts, "/RegUrl", 200, "MISS", 4_000_000)
    for _ in range(60):
        _ins(baseline_ts, "/RegUrl", 200, "MISS", 200_000)

    # tail_latency: "TailUrl" — window only, 23 fast + 2 slow → ratio ≈ 1000
    for _ in range(23):
        _ins(window_ts, "/TailUrl", 200, "MISS", 10_000)
    for _ in range(2):
        _ins(window_ts, "/TailUrl", 200, "MISS", 10_000_000)


def test_coalesced_url_path_matches_per_insight_scan_output(in_memory_duckdb, test_service_source, monkeypatch):
    """Regression for Step 2 / Option C: the coalesced URL-aggregate path
    (`_coalesced_url_aggregates`) must produce per-insight items
    *equivalent* to the legacy per-insight scans, item-by-item.

    Compares the 4 URL-keyed insights coalesced into the new CTE
    (error_spikes, cache_collapse, latency_regression, tail_latency)
    between the coalesced path (fast) and the legacy per-insight SQL
    templates (slow, forced by monkeypatching the coalesce to return {}).

    Modeled directly on test_coalesced_city_path_matches_per_insight_scan_output
    so the two regression tests pin the same equivalence contract for
    both O2 (city) and Step 2 (URL).
    """
    from backend.repositories.insights import repository as insights_repo

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, '
        '"url" VARCHAR, '
        '"status" INTEGER, '
        '"cache" VARCHAR, '
        '"elapsed" INTEGER'
        ")"
    )
    _seed_url_data_for_all_url_insights(in_memory_duckdb, table_name)

    url_ids = ("error_spikes", "cache_collapse", "cacheability_regression", "latency_regression", "tail_latency")

    # Pass 1 — coalesced path (default).
    _insights_cache.clear()
    fast = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    fast_url = {i["id"]: i for i in fast["insights"] if i["id"] in url_ids}

    # Pass 2 — disable URL coalescing, force per-insight scans.
    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, "_coalesced_url_aggregates", lambda *a, **k: {})
    slow = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    slow_url = {i["id"]: i for i in slow["insights"] if i["id"] in url_ids}

    expected_ids = set(url_ids)
    assert set(fast_url.keys()) == expected_ids, f"fast missing: {expected_ids - set(fast_url.keys())}"
    assert set(slow_url.keys()) == expected_ids, f"slow missing: {expected_ids - set(slow_url.keys())}"

    for insight_id in expected_ids:
        fast_items = fast_url[insight_id]["items"]
        slow_items = slow_url[insight_id]["items"]

        assert len(fast_items) == len(slow_items), (
            f"{insight_id}: fast had {len(fast_items)} items, slow had {len(slow_items)}"
        )

        # Same _norm comparison shape as the city equivalence test: (label,
        # current_val, baseline_val) rounded to 4 decimals to absorb the
        # last-ULP differences between Python aggregation and DuckDB's
        # native PERCENTILE_CONT.
        def _norm(items: list[dict]) -> list[tuple]:
            return [
                (
                    i["label"],
                    round(float(i.get("current_val") or 0), 4),
                    round(float(i.get("baseline_val") or 0), 4),
                )
                for i in items
            ]

        assert _norm(fast_items) == _norm(slow_items), (
            f"{insight_id} item lists differ between fast and slow paths:\n"
            f"  fast: {_norm(fast_items)}\n  slow: {_norm(slow_items)}"
        )


def _seed_ip_security_data(con, table_name: str) -> None:
    """Seed rows that trigger each of the 3 IP-keyed security insights that
    fold into COALESCED_IP_SECURITY_AGGREGATES, with clean separation so no
    single IP trips more than one card:

      - low_and_slow: 10.0.0.1 probes 6 distinct sensitive paths (NEW_PROBE
        set, status 200) spread over ~50 min (span >= 600, rps << 0.2).
      - credential_enumeration: 10.0.0.2 hits 2 auth paths 15× each in the
        window, all 401 (w_denied=30, ratio=1.0 >= 0.5, > baseline 5-floor).
      - content_discovery: 10.0.0.3 hits 30 distinct non-probe/non-auth URLs
        all 404 in the window (w_404=30, distinct>=15, ratio=1.0 >= 0.7).

    24.5h of history is seeded (one filler baseline row) so available_history
    >= baseline_hours (24h) and check_baseline doesn't short-circuit.
    """
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)  # within last hour (window)
    baseline_ts = now - timedelta(hours=24, minutes=30)  # >= baseline_hours old

    def _ins(ts: datetime, ip: str, url: str, status: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status") VALUES (?, ?, ?, ?)',
            [ts.isoformat(), ip, url, status],
        )

    # Filler baseline row so available_history >= 24h (not an anomaly signal).
    _ins(baseline_ts, "10.0.0.9", "/healthz", 200)

    # low_and_slow: 6 distinct probe paths, status 200, spanning ~50 min.
    probe_paths = ["/admin", "/.env", "/.git/config", "/phpmyadmin/", "/backup.sql", "/actuator/health"]
    for i, path in enumerate(probe_paths):
        _ins(now - timedelta(minutes=90 - i * 10), "10.0.0.1", path, 200)

    # credential_enumeration: 2 auth paths, 15× each, all 401, in the window.
    for _ in range(15):
        _ins(window_ts, "10.0.0.2", "/login", 401)
    for _ in range(15):
        _ins(window_ts, "10.0.0.2", "/account/signin", 401)

    # content_discovery: 30 distinct non-probe/non-auth URLs, all 404, in window.
    for i in range(30):
        _ins(window_ts, "10.0.0.3", f"/page{i}", 404)


def test_coalesced_ip_security_path_matches_per_insight_scan_output(in_memory_duckdb, test_service_source, monkeypatch):
    """Track A parity: the coalesced IP-security path
    (`_coalesced_ip_security_aggregates`) must produce per-insight items
    *equivalent* to the legacy per-insight scans, item-by-item.

    Mirrors test_coalesced_url_path_matches_per_insight_scan_output for the 3
    IP-keyed security insights (low_and_slow, credential_enumeration,
    content_discovery): compares the coalesced path (fast, default) against the
    standalone templates (slow, forced by monkeypatching the coalesce to {}).
    """
    from backend.repositories.insights import repository as insights_repo

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER)'
    )
    _seed_ip_security_data(in_memory_duckdb, table_name)

    ip_ids = ("low_and_slow", "credential_enumeration", "content_discovery")

    # Pass 1 — coalesced path (default).
    _insights_cache.clear()
    fast = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    fast_ip = {i["id"]: i for i in fast["insights"] if i["id"] in ip_ids}

    # Pass 2 — disable IP-security coalescing, force per-insight scans.
    _insights_cache.clear()
    monkeypatch.setattr(insights_repo, "_coalesced_ip_security_aggregates", lambda *a, **k: {})
    slow = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    slow_ip = {i["id"]: i for i in slow["insights"] if i["id"] in ip_ids}

    expected_ids = set(ip_ids)
    assert set(fast_ip.keys()) == expected_ids, f"fast missing: {expected_ids - set(fast_ip.keys())}"
    assert set(slow_ip.keys()) == expected_ids, f"slow missing: {expected_ids - set(slow_ip.keys())}"

    for insight_id in expected_ids:
        fast_items = fast_ip[insight_id]["items"]
        slow_items = slow_ip[insight_id]["items"]
        # Each seeded IP trips exactly one card → at least one item both ways.
        assert fast_items, f"{insight_id}: coalesced path produced no items (seed didn't fire)"
        assert len(fast_items) == len(slow_items), (
            f"{insight_id}: fast had {len(fast_items)} items, slow had {len(slow_items)}"
        )

        def _norm(items: list[dict]) -> list[tuple]:
            return [
                (
                    i["label"],
                    round(float(i.get("current_val") or 0), 4),
                    round(float(i.get("baseline_val") or 0), 4),
                )
                for i in items
            ]

        assert _norm(fast_items) == _norm(slow_items), (
            f"{insight_id} item lists differ between fast and slow paths:\n"
            f"  fast: {_norm(fast_items)}\n  slow: {_norm(slow_items)}"
        )


def test_coalesced_ip_security_aggregates_demux_shapes(in_memory_duckdb, test_service_source):
    """Directly exercise the coalesced helper: it returns the 3 IP-security
    insight keys, each row matching its processor's row-schema, so the parity
    test above can't pass merely because both paths silently fell back."""
    from backend.repositories._base import QueryRunner
    from backend.repositories.insights.repository import _coalesced_ip_security_aggregates

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER)'
    )
    _seed_ip_security_data(in_memory_duckdb, table_name)

    from datetime import UTC, datetime, timedelta

    window_start_s = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    runner = QueryRunner(in_memory_duckdb, test_service_source)
    out = _coalesced_ip_security_aggregates(runner, table_name, window_start_s, 1.0, 24.0)

    assert set(out.keys()) == {"low_and_slow", "credential_enumeration", "content_discovery"}
    # low_and_slow: [ip, hits, distinct_paths, span_s, rps]
    ls = out["low_and_slow"]
    assert len(ls) == 1 and ls[0][0] == "10.0.0.1" and ls[0][2] == 6
    # credential_enumeration: [ip, w_denied, w_attempts, w_paths, b_denied]
    ce = out["credential_enumeration"]
    assert len(ce) == 1 and ce[0][0] == "10.0.0.2" and ce[0][1] == 30 and ce[0][3] == 2
    # content_discovery: [ip, w_404, w_total, distinct_404, b_404]
    cd = out["content_discovery"]
    assert len(cd) == 1 and cd[0][0] == "10.0.0.3" and cd[0][1] == 30 and cd[0][3] == 30


def test_content_discovery_detects_404_enumeration(in_memory_duckdb, test_service_source):
    """End-to-end: a per-IP 404 burst across many distinct URLs surfaces as a
    content_discovery card (security section) with the expected counts."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER)'
    )
    _seed_ip_security_data(in_memory_duckdb, table_name)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    assert "content_discovery" in by_id
    cd = by_id["content_discovery"]
    assert cd["category"] == "security"
    labels = {item["label"]: item for item in cd["items"]}
    assert "10.0.0.3" in labels
    item = labels["10.0.0.3"]
    assert item["current_val"] == 30  # w_404
    assert item["meta"]["distinct_404_urls"] == 30
    assert item["meta"]["not_found_rate_pct"] == 100.0


@pytest.mark.security_regression
def test_coalesced_ip_security_masks_ip_end_to_end_for_analyst(in_memory_duckdb, test_service_source):
    """End-to-end masking through the ACTIVE coalesced path: with mask_ips=True
    (analyst policy) the 3 IP-keyed security cards produced via
    COALESCED_IP_SECURITY_AGGREGATES must render the masked IP in the label AND
    meta.filters.ip — never the raw IPv4. The processor-level mask tests pin the
    pure function; this pins the connective tissue so a future pre-agg refactor
    that bypasses the shared processor loop can't silently leak the raw IP."""
    from backend.core.share_db.validation import mask_ip

    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER)'
    )
    _seed_ip_security_data(in_memory_duckdb, table_name)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24, mask_ips=True)
    by_id = {i["id"]: i for i in res["insights"]}

    # id -> the raw seeded IP that trips that card (see _seed_ip_security_data).
    seeded_raw_ip = {
        "low_and_slow": "10.0.0.1",
        "credential_enumeration": "10.0.0.2",
        "content_discovery": "10.0.0.3",
    }
    for insight_id, raw_ip in seeded_raw_ip.items():
        card = by_id[insight_id]
        assert card["items"], f"{insight_id}: coalesced path produced no items under mask_ips"
        item = card["items"][0]
        expected = mask_ip(raw_ip)
        assert item["label"] == expected, f"{insight_id}: label {item['label']!r} not masked"
        assert item["meta"]["filters"]["ip"] == expected, f"{insight_id}: filters.ip not masked"
        # Hard fail-closed: the raw IPv4 must not survive anywhere in label/filter.
        assert item["label"] != raw_ip
        assert item["meta"]["filters"]["ip"] != raw_ip


def _seed_traffic_data(con, table_name: str) -> None:
    """Seed rows that trigger each of the 3 coalesced traffic/network insights,
    one clean item each:

      - referer_monoculture: 'https://ref-a.example' drives 100% of window
        traffic vs a google.com-dominated baseline.
      - method_drift: POST is 100% of window traffic vs a GET-only baseline.
      - new_asn_traffic: AS70001 sends 60 window requests with zero baseline.

    24.5h of history is seeded so available_history >= baseline_hours (24h) and
    check_baseline doesn't short-circuit.
    """
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    baseline_ts = now - timedelta(hours=12)
    old_filler = now - timedelta(hours=24, minutes=30)

    def _ins(ts: datetime, ref: str, method: str, asn: int) -> None:
        con.execute(
            f'INSERT INTO {table_name} ("timestamp", "referer", "method", "asn") VALUES (?, ?, ?, ?)',
            [ts.isoformat(), ref, method, asn],
        )

    _ins(old_filler, "https://google.com", "GET", 15169)  # history >= 24h
    for _ in range(100):
        _ins(baseline_ts, "https://google.com", "GET", 15169)
    for _ in range(60):
        _ins(window_ts, "https://ref-a.example", "POST", 70001)


def test_coalesced_traffic_detects_all_three_dims(in_memory_duckdb, test_service_source):
    """End-to-end: the coalesced traffic pass surfaces referer_monoculture,
    method_drift (traffic) and new_asn_traffic (network) with expected values."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "referer" VARCHAR, "method" VARCHAR, "asn" UINTEGER)'
    )
    _seed_traffic_data(in_memory_duckdb, table_name)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    ref = by_id["referer_monoculture"]
    assert ref["category"] == "traffic"
    assert {it["label"] for it in ref["items"]} == {"https://ref-a.example"}
    assert ref["items"][0]["current_val"] == 100.0  # 60/60 window share

    meth = by_id["method_drift"]
    assert meth["category"] == "traffic"
    assert {it["label"] for it in meth["items"]} == {"POST"}

    asn_card = by_id["new_asn_traffic"]
    assert asn_card["category"] == "network"
    assert {it["meta"]["asn"] for it in asn_card["items"]} == {70001}


def _seed_network_edge_data(con, table_name: str) -> None:
    """Seed rows that trip all 5 Track-B2 standalone cards at once:
    metro delivery halves, connection type flips to cellular, PoP P95 blows up,
    QUIC collapses to TCP, and the cache HIT ratio cliffs. 150 window rows keep
    the two service-wide headline cards (http3_fallback, cache_hit_cliff) above
    their ≥100-window-sample floors. 24.5h history avoids check_baseline."""
    from datetime import UTC, datetime, timedelta

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
            "quic" if i % 5 < 4 else "tcp",  # 80% QUIC
            "HIT" if i % 10 < 7 else "MISS",  # 70% HIT of cacheable (no PASS)
        )
    for _ in range(150):
        _ins(window_ts, 501, 1_000_000, "cellular", "mobile", "JFK", 3_000_000, "tcp", "MISS")


def test_track_b2_standalone_network_edge_cards_detect(in_memory_duckdb, test_service_source):
    """End-to-end: the 5 Track-B2 standalone cards each fire with expected
    values and land in the right section."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "metro" USMALLINT, "delivery_rate" UBIGINT, '
        '"c_type" VARCHAR, "c_speed" VARCHAR, "pop" VARCHAR, "elapsed" UBIGINT, '
        '"transport" VARCHAR, "cache" VARCHAR)'
    )
    _seed_network_edge_data(in_memory_duckdb, table_name)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    metro = by_id["metro_delivery_degradation"]
    assert metro["category"] == "network"
    assert metro["items"] and metro["items"][0]["current_val"] == 8.0  # 1MB/s → 8 Mbps
    assert metro["items"][0]["baseline_val"] == 80.0  # 10MB/s → 80 Mbps

    ctype = by_id["connection_type_mix"]
    assert ctype["category"] == "network"
    assert {it["label"] for it in ctype["items"]} == {"cellular / mobile"}

    pop = by_id["pop_latency_regression"]
    assert pop["category"] == "edge"
    assert pop["items"] and pop["items"][0]["label"] == "JFK"
    assert pop["items"][0]["current_val"] == 3000.0  # 3_000_000 µs → 3000 ms

    h3 = by_id["http3_fallback"]
    assert h3["category"] == "network"
    assert h3["severity"] == "critical" and h3["items"]
    assert h3["items"][0]["current_val"] == 0.0  # 0% QUIC in window

    cliff = by_id["cache_hit_cliff"]
    assert cliff["category"] == "edge"
    assert cliff["severity"] == "critical" and cliff["items"]
    assert cliff["items"][0]["current_val"] == 0.0  # 0% HIT in window


def _seed_track_c_data(con, table_name: str) -> None:
    """Seed rows that trip all 3 Track-C field-gated insights: a compressible
    URL flips gzip→uncompressed, one IP presents 30 distinct session cookies,
    and origin connect-P95 blows up 5ms→500ms. 24.5h history avoids
    check_baseline."""
    from datetime import UTC, datetime, timedelta

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
    # payload_compression_regression: baseline gzip, window uncompressed
    for _ in range(60):
        _ins(baseline_ts, "/app.js", 200, 50_000, "gzip", "10.0.0.9", "s_b", 5, 50_000)
    for _ in range(20):
        _ins(window_ts, "/app.js", 200, 50_000, "", "10.0.0.9", "s_w", 5, 50_000)
    # session_harvesting: 30 distinct sessions from one IP in the window
    for i in range(30):
        _ins(window_ts, "/login", 200, 500, "gzip", "10.0.0.2", f"sess_{i}", 5, 50_000)
    # timeout_split: origin connect P95 5ms → 500ms (dominant connect phase)
    for _ in range(120):
        _ins(baseline_ts, "/api", 200, 500, "gzip", "10.0.0.3", "s", 5, 60_000)
    for _ in range(60):
        _ins(window_ts, "/api", 200, 500, "gzip", "10.0.0.3", "s", 500, 560_000)


def test_track_c_field_gated_insights_detect(in_memory_duckdb, test_service_source):
    """End-to-end: the 3 Track-C insights each fire once their (simulated) edge
    fields are present, land in the right section, and session_harvesting masks
    the IP for an analyst while never surfacing a session id."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "url" VARCHAR, "status" USMALLINT, "resp_bytes" UBIGINT, '
        '"resp_header_content_encoding" VARCHAR, "ip" VARCHAR, "cookie_session" VARCHAR, '
        '"oconnect_ms" UINTEGER, "ottfb" UBIGINT)'
    )
    _seed_track_c_data(in_memory_duckdb, table_name)

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    pcr = by_id["payload_compression_regression"]
    assert pcr["category"] == "edge"
    assert {it["label"] for it in pcr["items"]} == {"/app.js"}
    assert pcr["items"][0]["current_val"] == 100.0  # 100% uncompressed in window

    sh = by_id["session_harvesting"]
    assert sh["category"] == "security"
    assert sh["items"] and sh["items"][0]["current_val"] == 30

    ts_card = by_id["timeout_split"]
    assert ts_card["category"] == "origin"
    assert ts_card["items"] and ts_card["items"][0]["meta"]["phase"] == "connect"

    # Analyst masking end-to-end: IP masked, no raw session id anywhere.
    _insights_cache.clear()
    res_m = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24, mask_ips=True)
    sh_m = {i["id"]: i for i in res_m["insights"]}["session_harvesting"]
    assert sh_m["items"], "session_harvesting produced no items under mask_ips"
    label = sh_m["items"][0]["label"]
    assert label.endswith(".xxx") and "10.0.0.2" != label
    assert "sess_" not in repr(sh_m["items"][0])  # no session id ever surfaced


def _create_url_insights_table(con, table_name: str) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        '"timestamp" TIMESTAMPTZ, "url" VARCHAR, "status" INTEGER, "cache" VARCHAR, "elapsed" INTEGER)'
    )


def test_pass_surge_is_cacheability_regression_not_cache_collapse(in_memory_duckdb, test_service_source):
    """Regression for the reported bug: a surge of uncacheable PASS traffic
    must NOT trip cache_collapse — PASS is excluded from the HIT/(HIT+MISS)
    cacheable hit ratio — and must instead surface under cacheability_regression.
    """
    from datetime import UTC, datetime, timedelta

    table_name = _safe_table(test_service_source["name"])
    _create_url_insights_table(in_memory_duckdb, table_name)

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    # 24.5h old so available_history >= baseline_hours (24h); see check_baseline.
    baseline_ts = now - timedelta(hours=24, minutes=30)

    def _ins(ts, cache):
        in_memory_duckdb.execute(
            f'INSERT INTO {table_name} ("timestamp", "url", "status", "cache", "elapsed") VALUES (?, ?, ?, ?, ?)',
            [ts.isoformat(), "/PassSurge", 200, cache, 50_000],
        )

    # Baseline: 100 cacheable reqs, mostly HIT (80 HIT / 20 MISS, 0 PASS).
    for i in range(100):
        _ins(baseline_ts, "HIT" if i < 80 else "MISS")
    # Recent window: 40 reqs, ALL PASS (content became uncacheable).
    for _ in range(40):
        _ins(window_ts, "PASS")

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}

    cc_labels = {item["label"] for item in by_id["cache_collapse"]["items"]}
    cr_items = {item["label"]: item for item in by_id["cacheability_regression"]["items"]}

    assert "/PassSurge" not in cc_labels, "a PASS surge must not be flagged as a cache-efficiency collapse"
    assert "/PassSurge" in cr_items, "a PASS surge should surface as a cacheability regression"
    assert cr_items["/PassSurge"]["current_val"] == 100.0  # 100% PASS in window
    assert cr_items["/PassSurge"]["baseline_val"] == 0.0  # 0% PASS in baseline


def test_cache_collapse_hit_ratio_ignores_pass(in_memory_duckdb, test_service_source):
    """cache_collapse hit ratio is HIT/(HIT+MISS): adding PASS traffic to both
    windows leaves the reported ratios unchanged, and a genuine HIT→MISS drop
    on the cacheable traffic still fires."""
    from datetime import UTC, datetime, timedelta

    table_name = _safe_table(test_service_source["name"])
    _create_url_insights_table(in_memory_duckdb, table_name)

    now = datetime.now(UTC)
    window_ts = now - timedelta(minutes=30)
    # 24.5h old so available_history >= baseline_hours (24h); see check_baseline.
    baseline_ts = now - timedelta(hours=24, minutes=30)

    def _ins(ts, cache):
        in_memory_duckdb.execute(
            f'INSERT INTO {table_name} ("timestamp", "url", "status", "cache", "elapsed") VALUES (?, ?, ?, ?, ?)',
            [ts.isoformat(), "/Coll", 200, cache, 50_000],
        )

    # Baseline: 90 HIT + 10 MISS (90% cacheable hit ratio) + 100 PASS noise.
    for i in range(100):
        _ins(baseline_ts, "HIT" if i < 90 else "MISS")
    for _ in range(100):
        _ins(baseline_ts, "PASS")
    # Window: 3 HIT + 27 MISS (10% cacheable hit ratio) + 100 PASS noise.
    for i in range(30):
        _ins(window_ts, "HIT" if i < 3 else "MISS")
    for _ in range(100):
        _ins(window_ts, "PASS")

    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    by_id = {i["id"]: i for i in res["insights"]}
    coll = {item["label"]: item for item in by_id["cache_collapse"]["items"]}

    assert "/Coll" in coll, "a real cacheable hit-ratio drop should fire cache_collapse"
    # Ratios reflect HIT/(HIT+MISS), not HIT/total — PASS is excluded.
    assert coll["/Coll"]["baseline_val"] == 90.0
    assert coll["/Coll"]["current_val"] == 10.0


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


def test_get_insights_drops_temp_tables_after_each_call(in_memory_duckdb, test_service_source):
    """Finding 009: ``get_insights`` previously created two TEMP tables
    (``insights_temp_*`` and possibly ``insights_waf_*``) but never dropped
    them, so on a pooled DuckDB connection they accumulated across requests —
    an attacker who could force cache misses (e.g. via a varying cache-bypass
    parameter) would gradually exhaust temp storage / memory. The fix wraps
    the post-create work in try/finally that DROPs both tables before
    returning. This test seeds a logs table, calls ``get_insights`` a few
    times, and verifies no ``insights_temp_*`` or ``insights_waf_*`` tables
    remain on the connection."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    def _leftover_temps() -> list[str]:
        rows = in_memory_duckdb.execute("SELECT table_name FROM duckdb_tables() WHERE temporary = true").fetchall()
        return [r[0] for r in rows if r[0].startswith(("insights_temp_", "insights_waf_"))]

    # Bypass the in-process result cache between calls so each invocation
    # actually re-creates the temp pair (otherwise the second call would
    # short-circuit through ``_insights_cache.get(cache_key)`` and never
    # touch the cleanup branch we are validating).
    for n in range(3):
        _insights_cache.clear()
        get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1.0 + n * 0.01,
            baseline_hours=24,
        )

    leftover = _leftover_temps()
    assert leftover == [], f"insights temp tables must be dropped after every call; leftover: {leftover}"


class TestInsightsAnalystClamp:
    """M2: get_insights clamps the scanned [baseline_start, now] range to the
    analyst's window and keys the cache on the clamp so a scoped result can't
    read the unclamped admin/prewarmer entry."""

    def _empty_schema(self, con, src) -> None:
        from backend.core.log_fields import LOG_FIELD_CATALOG

        table = _safe_table(src["name"])
        raw = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
        schema = ", ".join(f'"{f["id"]}" {f["duckdb_type"]}' for f in raw)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")

    def test_clamp_start_floors_baseline_start(self, in_memory_duckdb, test_service_source):
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        clamp_start = (now - timedelta(hours=2)).isoformat()
        out = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=168,
            clamp_start=clamp_start,
        )
        bstart = parse_iso_utc(out["baseline_start"])
        # Unclamped this would be ~now-169h; the clamp floors it to ~now-2h.
        assert bstart >= parse_iso_utc(clamp_start) - timedelta(seconds=2)
        assert bstart > now - timedelta(hours=3)

    def test_clamp_end_ceilings_window_end(self, in_memory_duckdb, test_service_source):
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        clamp_end = (now - timedelta(hours=5)).isoformat()
        out = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_end=clamp_end,
        )
        assert parse_iso_utc(out["window_end"]) <= parse_iso_utc(clamp_end) + timedelta(seconds=2)

    def test_admin_no_clamp_keeps_full_lookback(self, in_memory_duckdb, test_service_source):
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        out = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=168)
        # Full baseline+window lookback (~169h) — the clamp didn't fire.
        assert parse_iso_utc(out["baseline_start"]) < now - timedelta(hours=160)

    def test_clamped_call_does_not_read_unclamped_cache(self, in_memory_duckdb, test_service_source):
        """The prewarmer caches an UNCLAMPED result under source:window:baseline.
        A scoped analyst requesting the same window/baseline must not read it."""
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        # Prime the admin/prewarmer entry (no clamp).
        admin = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=168)
        assert "_is_cached" not in admin
        assert parse_iso_utc(admin["baseline_start"]) < now - timedelta(hours=160)
        # Analyst call, same window/baseline, but clamped: must compute fresh
        # (different cache key) and reflect the clamp — not the cached admin range.
        clamp_start = (now - timedelta(hours=2)).isoformat()
        analyst = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=168,
            clamp_start=clamp_start,
        )
        assert analyst.get("_is_cached") is not True
        assert parse_iso_utc(analyst["baseline_start"]) > now - timedelta(hours=3)

    def test_mask_ips_isolates_cache_entry(self, in_memory_duckdb, test_service_source):
        """M3: mask_ips is part of the cache key — a masked analyst result and an
        unmasked one (same source/window/baseline/clamp) must not share an entry,
        else one analyst could read the other's IP-keyed insight labels."""
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        cs, ce = (now - timedelta(hours=2)).isoformat(), now.isoformat()
        unmasked = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_start=cs,
            clamp_end=ce,
            mask_ips=False,
        )
        assert "_is_cached" not in unmasked
        masked = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_start=cs,
            clamp_end=ce,
            mask_ips=True,
        )
        # Different mask_ips → different cache key → fresh compute, not a cache hit.
        assert masked.get("_is_cached") is not True

    def test_stable_clamp_cache_key_hits_across_rolling_bounds(self, in_memory_duckdb, test_service_source):
        """THE core fix: an analyst's clamp_start/clamp_end roll with ``now`` on
        every request, but ``clamp_cache_key`` is keyed on the invite's stable
        window params. Two calls with the SAME cache key but DIFFERENT resolved
        bounds must share one entry — so analysts stop recomputing every call."""
        _insights_cache.clear()
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        ck = "||24"  # stable invite shape (qs='', qe='', qwh=24)
        first = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_start=(now - timedelta(hours=24)).isoformat(),
            clamp_end=now.isoformat(),
            clamp_cache_key=ck,
        )
        assert first.get("is_cached") is not True
        # A moment later the bounds rolled forward, but the stable key is unchanged.
        later = now + timedelta(seconds=30)
        second = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_start=(later - timedelta(hours=24)).isoformat(),
            clamp_end=later.isoformat(),
            clamp_cache_key=ck,
        )
        assert second.get("is_cached") is True

    def test_distinct_clamp_cache_keys_do_not_share(self, in_memory_duckdb, test_service_source):
        """Different invite shapes (different cache keys) never collide; neither
        reads the admin (no-key) entry."""
        _insights_cache.clear()
        self._empty_schema(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        cs, ce = (now - timedelta(hours=24)).isoformat(), now.isoformat()
        a = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_start=cs,
            clamp_end=ce,
            clamp_cache_key="||24",
        )
        b = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_start=cs,
            clamp_end=ce,
            clamp_cache_key="||72",
        )
        assert a.get("is_cached") is not True
        assert b.get("is_cached") is not True  # distinct key → fresh, not a's entry

    def test_force_refresh_recomputes_warm_entry(self, in_memory_duckdb, test_service_source):
        """The prewarmer passes force_refresh=True so every tick rewrites the
        entry (resets TTL). A warm entry must NOT be served back on that path."""
        _insights_cache.clear()
        self._empty_schema(in_memory_duckdb, test_service_source)
        ck = "||24"
        warm = get_insights(
            in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24, clamp_cache_key=ck
        )
        assert warm.get("is_cached") is not True
        hit = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24, clamp_cache_key=ck)
        assert hit.get("is_cached") is True  # confirms the entry WAS warm
        forced = get_insights(
            in_memory_duckdb,
            test_service_source,
            window_hours=1,
            baseline_hours=24,
            clamp_cache_key=ck,
            force_refresh=True,
        )
        assert forced.get("is_cached") is not True  # recomputed despite the warm entry


def test_insights_request_bounds_window_fields():
    """M2: InsightsRequest caps both windows (were unbounded floats → a
    baseline_hours like 8_760_000 scanned ~1000 years of data)."""
    from pydantic import ValidationError

    from backend.models.dashboard import InsightsRequest

    with pytest.raises(ValidationError):
        InsightsRequest(window_size_hrs=169)
    with pytest.raises(ValidationError):
        InsightsRequest(baseline_hours=2161)
    with pytest.raises(ValidationError):
        InsightsRequest(window_size_hrs=0)
    with pytest.raises(ValidationError):
        InsightsRequest(baseline_hours=0)
    ok = InsightsRequest(window_size_hrs=168, baseline_hours=2160)
    assert ok.window_size_hrs == 168
    assert ok.baseline_hours == 2160


def test_coalesced_url_aggregates_caps_per_insight_at_top_15():
    """``_coalesced_url_aggregates`` must hold at most 15 entries per
    insight in memory, regardless of how many input rows meet the gates.

    Regression for F007 (audit run 7ba15352): previously the function
    appended every qualifying row to four unbounded lists and only
    sliced [:15] after the full result set was materialised. An attacker
    generating millions of unique URLs that hit any of the per-insight
    gates (e.g. random query strings producing 5xx) could OOM the worker
    before the trailing sort+slice ran. The bounded-heap rewrite caps
    each insight at K=15 in flight, so total memory stays O(1) in the
    number of unique URLs.

    We can't easily simulate a million-row DuckDB cursor; instead we
    drive the helper with a fake cursor returning 100 synthetic rows
    and assert (a) the result is sorted-descending by the documented
    score and (b) the four output lists are each ≤ 15 entries.
    """
    from unittest.mock import MagicMock

    from backend.repositories.insights.repository import _coalesced_url_aggregates

    # Each row has 17 columns matching the SQL projection:
    # (url, w_total, b_total, w_5xx, b_5xx, w_hits, b_hits, w_miss, b_miss,
    #  w_pass, b_pass, w_lat_total, b_lat_total, w_p95, b_p95, w_p99, w_p50)
    # Construct 100 rows that satisfy every insight's gate but with
    # increasing scores so the top 15 are the highest-i rows.
    rows = []
    for i in range(100):
        w_5xx = 50 + i  # ascending w_rate (w_5xx / w_total = (50+i)/100)
        rows.append(
            (
                f"/url-{i}",
                100,  # w_total >= 5 and >= 3
                100,  # b_total >= 20
                w_5xx,  # w_5xx
                1,  # b_5xx (baseline rate ~0.01)
                10,  # w_hits (cacheable w_rate = 10/(10+90) ~0.1)
                90 - i % 20,  # b_hits varying so cache_collapse selects
                90,  # w_miss (w_cacheable = 100)
                10,  # b_miss (b_cacheable ~ 100, b_rate ~0.9)
                60,  # w_pass (cacheability w_pass_rate = 0.6)
                1,  # b_pass (baseline pass rate ~0.01)
                100,  # w_lat_total >= 20
                100,  # b_lat_total
                500.0 + i,  # w_p95 ascending
                100.0,  # b_p95 (w_p95 / b_p95 ascending)
                3000.0 + i * 10,  # w_p99
                100.0,  # w_p50 (ratio = p99 / p50)
            )
        )

    # Single fetchmany returns all 100 rows then signals exhaustion.
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
    for key, items in result.items():
        assert len(items) <= 15, f"{key} returned {len(items)} entries — must be capped at 15"

    # error_spikes is the cleanest to verify: score = w_rate - b_rate ≈
    # (50+i)/100 - 0.01. Top 15 must be the highest-i URLs in DESC order.
    es = result["error_spikes"]
    if es:
        urls = [row[0] for row in es]
        # Top entry is the highest score → highest i.
        assert urls[0] == "/url-99", f"expected top error_spike to be /url-99, got {urls[0]}"
        # Sorted descending by score (which is monotone in i): urls should
        # follow /url-99, /url-98, ...
        expected = [f"/url-{99 - n}" for n in range(len(urls))]
        assert urls == expected, f"error_spikes not sorted DESC by score: {urls} vs {expected}"


# ── Phase-3 insights: end-to-end detection through get_insights ───────────────
# These drive the full get_insights() pipeline (template hydration + param
# binding + row_processor) rather than executing the raw templates, matching
# the harness the rest of this file uses. Each new insight runs with a SHORT
# ``baseline_hours=1`` so the ``check_baseline`` gate passes with ~1.5-2h of
# synthetic history (the templates themselves are baseline-hour agnostic in
# their firing logic). Timestamps are relative to ``datetime.now(UTC)`` — the
# same time seam every other test in this module uses; time-machine ticks the
# clock monotonically forward so no assertion here is wall-clock sensitive.
#
# NOTE ON THE ≤15 CAP: the existing cap test
# ``test_coalesced_url_aggregates_caps_per_insight_at_top_15`` enumerates the
# keys of ``_coalesced_url_aggregates`` ONLY — the three Phase-3 insights are
# per-insight scans (not coalesced), so extending that test would assert keys
# it never produces. Instead each detection test below asserts ``len(items) <=
# 15`` directly (the templates carry ``LIMIT 15``).


def test_low_and_slow_detects_slow_probe_scanner(in_memory_duckdb, test_service_source):
    """An IP touching 6 distinct sensitive/vuln paths at a deliberately low
    rate over a ~1.7h span fires low_and_slow (warning; <10 distinct paths)."""
    table_name = _safe_table(test_service_source["name"])
    # ja3 present so the ip+timestamp-eligible botnet_grouping insight doesn't
    # error on a missing fingerprint column — keeps the card set clean.
    in_memory_duckdb.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "ja3" VARCHAR)'
    )

    now = datetime.now(UTC)
    scanner_ip = "192.0.2.55"
    # 6 distinct paths, each matching NEW_PROBE_REGEX, spread over ~104 min so
    # span_s >= 600 and rps (6/6240 ≈ 0.001) < 0.2. All rows sit inside the
    # temp window [now-2h, now] (baseline_hours=1 + window_hours=1).
    probes = ["/admin", "/.env", "/.git", "/wp-login.php", "/phpmyadmin/index.php", "/config.json"]
    minutes_ago = [110, 88, 66, 44, 22, 6]
    for path, mins in zip(probes, minutes_ago, strict=True):
        in_memory_duckdb.execute(
            f'INSERT INTO {table_name} ("timestamp", "ip", "url", "ja3") VALUES (?, ?, ?, ?)',
            [(now - timedelta(minutes=mins)).isoformat(), scanner_ip, path, None],
        )

    _insights_cache.clear()
    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)
    card = next((i for i in result["insights"] if i["id"] == "low_and_slow"), None)

    assert card is not None, "low_and_slow card should be present when ip+url are in schema"
    assert card["severity"] == "warning"  # 6 distinct paths < 10 critical floor
    assert len(card["items"]) <= 15, "low_and_slow must cap at 15 items"
    assert len(card["items"]) == 1
    item = card["items"][0]
    assert item["label"] == scanner_ip
    assert item["current_val"] == 6  # distinct_paths
    assert item["meta"]["distinct_paths"] == 6
    assert item["meta"]["filters"]["ip"] == scanner_ip


def test_credential_enumeration_detects_login_brute_force(in_memory_duckdb, test_service_source):
    """An IP generating 30 in-window 401s on /api/login fires
    credential_enumeration (warning; <100 denied)."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        '("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "url" VARCHAR, "status" INTEGER, "ja3" VARCHAR)'
    )

    now = datetime.now(UTC)
    attacker_ip = "198.51.100.23"
    window_ts = now - timedelta(minutes=30)  # is_w (>= window_start = now-1h)
    baseline_ts = now - timedelta(minutes=90)  # is_b + deepens history past baseline_hours

    def _ins(ts, status):
        in_memory_duckdb.execute(
            f'INSERT INTO {table_name} ("timestamp", "ip", "url", "status", "ja3") VALUES (?, ?, ?, ?, ?)',
            [ts.isoformat(), attacker_ip, "/api/login", status, None],
        )

    # 30 window 401s → w_denied=30, w_attempts=30 (100% fail rate).
    for _ in range(30):
        _ins(window_ts, 401)
    # A handful of successful baseline attempts (b_denied=0) so available_history
    # >= baseline_hours=1 and check_baseline lets the insight run.
    for _ in range(5):
        _ins(baseline_ts, 200)

    _insights_cache.clear()
    result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)
    card = next((i for i in result["insights"] if i["id"] == "credential_enumeration"), None)

    assert card is not None, "credential_enumeration card should be present when ip+url+status are in schema"
    assert card["severity"] == "warning"  # 30 denied < 100 critical floor
    assert len(card["items"]) <= 15, "credential_enumeration must cap at 15 items"
    assert len(card["items"]) == 1
    item = card["items"][0]
    assert item["label"] == attacker_ip
    assert item["current_val"] == 30  # w_denied
    assert item["meta"]["denied"] == 30
    assert item["meta"]["attempts"] == 30
    assert item["meta"]["filters"]["ip"] == attacker_ip


def test_network_asn_health_detects_packet_loss_degradation(in_memory_duckdb, test_service_source):
    """An ASN whose window packet loss (8%) far exceeds its baseline (0.1%),
    with enough samples on both sides, fires network_asn_health (critical)."""
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        '("timestamp" TIMESTAMPTZ, "asn" INTEGER, "ploss" DOUBLE, "rtt_var" INTEGER, "retrans" DOUBLE)'
    )

    now = datetime.now(UTC)
    degraded_asn = 64500
    window_ts = now - timedelta(minutes=30)  # is_w
    baseline_ts = now - timedelta(minutes=90)  # is_b + history depth

    def _ins(ts, ploss):
        in_memory_duckdb.execute(
            f'INSERT INTO {table_name} ("timestamp", "asn", "ploss", "rtt_var", "retrans") VALUES (?, ?, ?, ?, ?)',
            [ts.isoformat(), degraded_asn, ploss, 1000, 0.0],
        )

    # 60 window rows @ 8% loss, 120 baseline rows @ 0.1% loss (>= 50 / >= 100 gate).
    for _ in range(60):
        _ins(window_ts, 0.08)
    for _ in range(120):
        _ins(baseline_ts, 0.001)

    # Keep the test hermetic: don't depend on the per-service asn_names SQLite —
    # the ASN-name label enrichment is covered by the processor test.
    _insights_cache.clear()
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        result = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)
    card = next((i for i in result["insights"] if i["id"] == "network_asn_health"), None)

    assert card is not None, "network_asn_health card should be present when asn+ploss+rtt_var+retrans are in schema"
    assert card["severity"] == "critical"  # 8% loss >= 5% critical floor
    assert len(card["items"]) <= 15, "network_asn_health must cap at 15 items"
    assert len(card["items"]) == 1
    item = card["items"][0]
    assert item["label"] == f"AS{degraded_asn}"
    assert item["current_val"] == 8.0  # round(0.08 * 100, 2)
    assert item["meta"]["filters"]["asn"] == degraded_asn
