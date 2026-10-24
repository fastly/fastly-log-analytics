"""Regression tests for backend.repositories.sessions — validates return keys."""

import os
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.repositories._base import _safe_table
from backend.repositories.sessions import get_session_detail, get_sessions
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs

# ── get_sessions ──────────────────────────────────────────────────────────────


def test_get_sessions_returns_expected_keys(in_memory_duckdb, test_service_source):
    """Result always contains sessions/total/page/limit/has_rtt/has_ja4/has_edge/has_edge_sid."""
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=20,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    for key in ("sessions", "total", "page", "limit", "has_rtt", "has_ja4", "has_edge", "has_edge_sid"):
        assert key in result, f"Missing key: {key}"
    assert isinstance(result["sessions"], list)
    assert result["page"] == 1
    assert result["limit"] == 20


def test_get_sessions_flagged_only(in_memory_duckdb, test_service_source):
    """Result correctly applies flagging rules and filters to flagged_only=True."""
    from datetime import UTC, datetime, timedelta

    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    base_time = datetime.now(UTC) - timedelta(minutes=5)

    # Force 6 requests to have one IP (flagged) and 4 requests to have another IP (not flagged)
    # Ensure they have sequential timestamps close to each other to guarantee they belong to the same session
    for i, log in enumerate(logs):
        log_time = base_time + timedelta(seconds=i)
        log["timestamp"] = log_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        if log["timestamp"].endswith("0000"):
            log["timestamp"] = log["timestamp"][:-4] + "00:00"

        if i < 6:
            log["ip"] = "1.1.1.1"
        else:
            log["ip"] = "2.2.2.2"
            log["status"] = 200  # Prevent status-based flagging
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # First, run with flagged_only=False
    result_all = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=5,
        min_4xx_pct_flag=None,
    )
    ips_all = [s["ip"] for s in result_all["sessions"]]
    assert "1.1.1.1" in ips_all
    assert "2.2.2.2" in ips_all

    # Next, run with flagged_only=True
    result_flagged = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=True,
        min_reqs_flag=5,
        min_4xx_pct_flag=None,
    )
    ips_flagged = [s["ip"] for s in result_flagged["sessions"]]
    assert "1.1.1.1" in ips_flagged
    assert "2.2.2.2" not in ips_flagged


def test_get_sessions_groups_requests_by_ip(in_memory_duckdb, test_service_source):
    """Multiple requests from the same IP within 30 minutes form a single session."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    # Force all logs to the same IP
    for log in logs:
        log["ip"] = "10.0.0.1"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    # All 20 requests from one IP within 1 hour → should collapse to 1 session
    assert result["total"] <= 2  # ≤2 because generate spreads randomly within 1hr window
    for session in result["sessions"]:
        assert session["ip"] == "10.0.0.1"
        assert "session_start" in session
        assert "session_end" in session
        assert "req_count" in session
        assert session["req_count"] >= 1


def test_get_sessions_session_start_end_are_strings(in_memory_duckdb, test_service_source):
    """session_start and session_end are serialized as strings, not datetime objects."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    for session in result["sessions"]:
        if session.get("session_start") is not None:
            assert isinstance(session["session_start"], str), "session_start must be str for JSON serialization"
        if session.get("session_end") is not None:
            assert isinstance(session["session_end"], str), "session_end must be str for JSON serialization"


def test_get_sessions_empty_table(in_memory_duckdb, test_service_source):
    """Returns correct empty structure when table has no rows."""
    # Create empty table
    from backend.core.log_fields import LOG_FIELD_CATALOG

    table_name = _safe_table(test_service_source["name"])
    schema_def = ", ".join(
        [
            f'"{f["id"]}" {f["duckdb_type"]}'
            for f in LOG_FIELD_CATALOG
            if f.get("group") not in ("METRICS", "VIRTUAL") and f.get("vcl") is not None
        ]
    )
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=20,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    assert result["sessions"] == []
    assert result["total"] == 0


# ── get_sessions: edge_sid aggregation ────────────────────────────────────────


def test_get_sessions_has_edge_sid_false_when_column_absent(in_memory_duckdb, test_service_source):
    """When the log table has no ``edge_sid`` column (the default
    LOG_FIELD_CATALOG shape — edge_sid is only added when the
    session_scoring orchestrator provisions it), the response reports
    ``has_edge_sid: False`` and individual sessions do not carry an
    ``edge_sid`` field. Pinned because the frontend gates the flag column
    on this flag — without it, the column would render for every service
    even when the data isn't there to power it."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=20,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    assert result["has_edge_sid"] is False
    for session in result["sessions"]:
        assert "edge_sid" not in session, f"edge_sid should not appear in session dict when column is absent: {session}"


def test_get_sessions_has_edge_sid_true_and_per_session_value_when_column_present(
    in_memory_duckdb, test_service_source
):
    """When ``edge_sid`` is in the schema, the response reports
    ``has_edge_sid: True`` and each session row carries an ``edge_sid``
    aggregated via MAX() across the session's requests. Pinned because
    the frontend's per-row Flag popover keys label lookups on this
    string — a regression where MAX is dropped or aliased differently
    would silently break flagging from the sessions table."""
    # Add edge_sid to the mock-data schema. LOG_FIELD_CATALOG only
    # contains edge_sid when session_scoring is provisioned, so this
    # test injects the column directly into the in-memory table after
    # insert_mock_logs creates it.
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=8, hours_ago=1)
    for log in logs:
        log["ip"] = "10.0.0.50"
    insert_mock_logs(in_memory_duckdb, table_name, logs)
    in_memory_duckdb.execute(f'ALTER TABLE {table_name} ADD COLUMN "edge_sid" VARCHAR')
    # Tag every row in this session with the same edge_sid so MAX is
    # deterministic. Production sessions usually carry a single cookie
    # value end-to-end; intra-session rotation would still resolve to
    # one MAX value.
    in_memory_duckdb.execute(
        f'UPDATE {table_name} SET "edge_sid" = ? WHERE "ip" = ?', ["sid_abc123def456", "10.0.0.50"]
    )

    # Bust the schema cache so get_schema_cols picks up the new column.
    _clear_schema_cache()

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=20,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    assert result["has_edge_sid"] is True
    assert len(result["sessions"]) >= 1
    for session in result["sessions"]:
        # Every session that has rows from the seeded IP should carry
        # the aggregated cookie id. Sessions from other random IPs (if
        # any leaked through) would also have the column key present
        # (even if NULL) because the SELECT projects it unconditionally.
        assert "edge_sid" in session
        if session.get("ip") == "10.0.0.50":
            assert session["edge_sid"] == "sid_abc123def456"


# ── get_session_detail ────────────────────────────────────────────────────────


def test_get_session_detail_returns_data_and_columns(in_memory_duckdb, test_service_source):
    """Returns 'data' (list of records) and 'columns' keys."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["ip"] = "10.0.0.1"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_session_detail(
        con=in_memory_duckdb,
        src=test_service_source,
        ip="10.0.0.1",
        session_start="2000-01-01T00:00:00+00:00",
        session_end="2099-12-31T23:59:59+00:00",
    )
    assert "data" in result, f"Expected 'data' key, got: {list(result.keys())}"
    assert "columns" in result
    assert isinstance(result["data"], list)
    assert isinstance(result["columns"], list)
    assert len(result["data"]) > 0


def test_get_session_detail_timestamps_are_strings(in_memory_duckdb, test_service_source):
    """Timestamp fields in detail records are serialized as strings."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["ip"] = "10.0.0.2"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    result = get_session_detail(
        con=in_memory_duckdb,
        src=test_service_source,
        ip="10.0.0.2",
        session_start=(now - timedelta(hours=2)).isoformat(),
        session_end=now.isoformat(),
    )
    for record in result["data"]:
        for val in record.values():
            assert not hasattr(val, "isoformat"), "datetime objects must be serialized to str before returning"


# ── get_sessions: 7-day range guard ───────────────────────────────────────


def _sessions_kwargs(**overrides):
    """Default kwargs for `get_sessions` — all 7 required positional args."""
    base = {
        "start_time": None,
        "end_time": None,
        "filters": {},
        "page": 1,
        "limit": 50,
        "sort_by": "total_reqs",
        "sort_dir": "DESC",
        "flagged_only": False,
        "min_reqs_flag": None,
        "min_4xx_pct_flag": None,
    }
    base.update(overrides)
    return base


def test_get_sessions_raises_value_error_on_8_day_range(in_memory_duckdb, test_service_source):
    """Sessions view is limited to 7 days. Pinned because longer
    ranges produce huge result sets — the cap prevents accidental
    full-bucket sessions queries from OOM'ing the worker."""
    from backend.repositories.sessions import get_sessions

    with pytest.raises(ValueError, match="7 days"):
        get_sessions(
            con=in_memory_duckdb,
            src=test_service_source,
            **_sessions_kwargs(
                start_time="2026-01-01T00:00:00",
                end_time="2026-01-09T00:00:00",  # 8 days
            ),
        )


def test_get_sessions_allows_exactly_7_day_range(in_memory_duckdb, test_service_source):
    """Exactly 7 days is allowed (the cap is `> 7`). Pinned because
    customers often select "this week" which is 7 days."""
    from backend.repositories.sessions import get_sessions

    # No table to query — just verify the range check doesn't raise.
    out = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        **_sessions_kwargs(
            start_time="2026-01-01T00:00:00",
            end_time="2026-01-08T00:00:00",  # 7 days
        ),
    )
    assert "sessions" in out


# ── get_session_detail: UA + JA4 filter additions ────────────────────────


def test_get_session_detail_filters_by_ua_when_provided(in_memory_duckdb, test_service_source):
    """When `ua=` is provided, the WHERE clause adds `ua IS NOT
    DISTINCT FROM ?`. Pinned because the session-drill-down view
    needs to scope to the exact UA — without this the panel
    would show ALL sessions for the IP, mixing browsers + bots."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["ip"] = "10.0.0.3"
        log["ua"] = "Browser-A" if i < 5 else "Bot-B"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # Use a very wide window — we're testing the UA filter, not time bounds
    out = get_session_detail(
        con=in_memory_duckdb,
        src=test_service_source,
        ip="10.0.0.3",
        session_start="2000-01-01T00:00:00",
        session_end="2099-12-31T23:59:59",
        ua="Browser-A",
    )
    # Only the 5 Browser-A rows
    assert len(out["data"]) == 5
    for r in out["data"]:
        assert r["ua"] == "Browser-A"


def test_get_session_detail_filters_by_ja4_when_provided(in_memory_duckdb, test_service_source):
    """JA4 fingerprint filter narrows further. Pinned because two
    bots with the same IP+UA can still differ by TLS fingerprint."""
    logs = generate_mock_logs(test_service_source, num_logs=6, hours_ago=1)
    for i, log in enumerate(logs):
        log["ip"] = "10.0.0.4"
        log["ja4"] = "t13d-A" if i < 3 else "t13d-B"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_session_detail(
        con=in_memory_duckdb,
        src=test_service_source,
        ip="10.0.0.4",
        session_start="2000-01-01T00:00:00",
        session_end="2099-12-31T23:59:59",
        ja4="t13d-A",
    )
    assert len(out["data"]) == 3


# ── _get_sessions_from_rollup + helpers ─────────────────────────────────────
#
# These tests target the rollup fast-path in
# ``backend.repositories.sessions`` — the branch that serves
# /api/sessions from per-hour sessions.parquet bundles instead of
# scanning the raw log table. Coverage was 60% pre-test; the gaps are
# in the helpers (_collect_sessions_rollup_paths, _build_rollup_filter_sql,
# _build_active_hour_session_sql) and the stitching path through
# _get_sessions_from_rollup.


_ROLLUP_PARQUET_COLUMNS = [
    # Matches backend.core.rollups.sessions.build_session_bundles writer.
    # Order MUST match — DuckDB's read_parquet positional SELECT in the
    # union depends on the column order being identical to the writer's.
    "bucket",
    "ip",
    "ja4",
    "first_ts",
    "last_ts",
    "req_count",
    "country",
    "asn",
    "reqs_4xx",
    "reqs_5xx",
    "total_bytes",
    "rtt_sum",
    "rtt_count",
    "edge_count",
    "shield_count",
    "ua_min",
    "edge_sid_max",
    "cmcd_count",
]


def _write_rollup_parquet(path: str, rows: list[dict]) -> None:
    """Write a sessions.parquet bundle at ``path`` whose schema matches the
    17-col writer in ``build_session_bundles``.

    Rather than hand-build a pyarrow schema (brittle if the writer
    drifts), the function builds the parquet by running a DuckDB
    SELECT that emits the exact CAST/typing the writer uses, then
    uses COPY ... TO ... (FORMAT PARQUET). This ties the test
    fixture to the same SQL types the production writer emits.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        # Build SELECTs out of literal rows. Each row produces a
        # ``SELECT ...`` chained with UNION ALL. This avoids parameter
        # binding for the literal-cast forms (TIMESTAMPTZ).
        selects: list[str] = []
        for row in rows:
            select_parts = [
                f"TIMESTAMPTZ '{row['bucket'].isoformat()}' AS bucket",
                f"CAST('{row['ip']}' AS VARCHAR) AS ip",
                (
                    f"CAST('{row['ja4']}' AS VARCHAR) AS ja4"
                    if row.get("ja4") is not None
                    else "CAST(NULL AS VARCHAR) AS ja4"
                ),
                f"TIMESTAMPTZ '{row['first_ts'].isoformat()}' AS first_ts",
                f"TIMESTAMPTZ '{row['last_ts'].isoformat()}' AS last_ts",
                f"CAST({row['req_count']} AS BIGINT) AS req_count",
                (
                    f"CAST('{row['country']}' AS VARCHAR) AS country"
                    if row.get("country") is not None
                    else "CAST(NULL AS VARCHAR) AS country"
                ),
                (
                    f"CAST({row['asn']} AS INTEGER) AS asn"
                    if row.get("asn") is not None
                    else "CAST(NULL AS INTEGER) AS asn"
                ),
                f"CAST({row.get('reqs_4xx', 0)} AS BIGINT) AS reqs_4xx",
                f"CAST({row.get('reqs_5xx', 0)} AS BIGINT) AS reqs_5xx",
                f"CAST({row.get('total_bytes', 0)} AS BIGINT) AS total_bytes",
                f"CAST({row.get('rtt_sum', 0.0)} AS DOUBLE) AS rtt_sum",
                f"CAST({row.get('rtt_count', 0)} AS BIGINT) AS rtt_count",
                f"CAST({row.get('edge_count', 0)} AS BIGINT) AS edge_count",
                f"CAST({row.get('shield_count', 0)} AS BIGINT) AS shield_count",
                (
                    f"CAST('{row['ua_min']}' AS VARCHAR) AS ua_min"
                    if row.get("ua_min") is not None
                    else "CAST(NULL AS VARCHAR) AS ua_min"
                ),
                (
                    f"CAST('{row['edge_sid_max']}' AS VARCHAR) AS edge_sid_max"
                    if row.get("edge_sid_max") is not None
                    else "CAST(NULL AS VARCHAR) AS edge_sid_max"
                ),
                f"CAST({row.get('cmcd_count', 0)} AS BIGINT) AS cmcd_count",
            ]
            selects.append("SELECT " + ", ".join(select_parts))
        query = " UNION ALL ".join(selects)
        # Escape single quotes in path (none in tmp paths in practice).
        safe_path = path.replace("'", "''")
        con.execute(f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _seed_per_field_hour(cache_root: str, hour_token: str) -> None:
    """Create an empty per-field rollup hour directory so the
    'hour had data' check in collect_hourly_bundle_paths fires.

    ``_hour_had_any_data`` looks for ``rollups/hour/field=*/hour=<H>``
    entries — any one of them is enough to mark the hour as having
    rollup data."""
    field_hour_dir = os.path.join(cache_root, "rollups", "hour", "field=ip", f"hour={hour_token}")
    os.makedirs(field_hour_dir, exist_ok=True)


@pytest.fixture
def rollup_source(tmp_path, test_service_source):
    """A source dict that points cache I/O at a per-test tmp tree.

    Uses the ``_cache_dir_override`` escape hatch so
    ``_hour_bundled_root`` and ``_rollups_root`` resolve under
    ``tmp_path/cache/<bucket>``. The autouse conftest sandbox
    already redirects the *default* ``_cache_dir``, but the rollup
    code also goes through the same ``_cache_dir`` indirectly via
    ``_hour_bundled_root`` — using the override avoids any
    accidental coupling to that fixture's path.
    """
    cache_root = tmp_path / "cache" / "test-bucket"
    cache_root.mkdir(parents=True, exist_ok=True)
    src = dict(test_service_source)
    src["bucket"] = "test-bucket"
    src["_cache_dir_override"] = str(cache_root)
    return src


def test_rollup_path_returns_sessions_for_multi_hour_window(in_memory_duckdb, rollup_source):
    """3-hour window with 2 stitched sessions (one IP active in all 3
    hours, another active in only 1) — rollup path returns sessions
    with str timestamps and ``_rollup_served=True``."""
    cache_root = rollup_source["_cache_dir_override"]
    # Pick 3 consecutive closed hours well before "now" so the
    # crosses-active check stays False and the window doesn't slip
    # into the live hour mid-test.
    base = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    hours = [base + timedelta(hours=i) for i in range(3)]
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]

    # Build rollup rows: IP A has one row per hour (stitches to one
    # session); IP B has just one row in the middle hour.
    for h, token in zip(hours, hour_tokens):
        rows = [
            {
                "bucket": h,
                "ip": "10.0.0.10",
                "ja4": "ja4-A",
                "first_ts": h + timedelta(minutes=1),
                "last_ts": h + timedelta(minutes=50),
                "req_count": 100,
                "country": "US",
                "asn": 7922,
                "reqs_4xx": 5,
                "reqs_5xx": 0,
                "total_bytes": 100000,
                "rtt_sum": 50000.0,
                "rtt_count": 80,
                "edge_count": 90,
                "shield_count": 10,
                "ua_min": "curl/8.0",
                "edge_sid_max": "sid_A",
            }
        ]
        if token == hour_tokens[1]:
            rows.append(
                {
                    "bucket": h,
                    "ip": "10.0.0.20",
                    "ja4": "ja4-B",
                    "first_ts": h + timedelta(minutes=15),
                    "last_ts": h + timedelta(minutes=20),
                    "req_count": 5,
                    "country": "GB",
                    "asn": 3320,
                    "reqs_4xx": 0,
                    "reqs_5xx": 0,
                    "total_bytes": 5000,
                    "rtt_sum": 1000.0,
                    "rtt_count": 5,
                    "edge_count": 5,
                    "shield_count": 0,
                    "ua_min": "Mozilla",
                    "edge_sid_max": "sid_B",
                }
            )
        bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")
        _write_rollup_parquet(bundle_path, rows)
        # Seed the per-field tree so collect_hourly_bundle_paths
        # doesn't trip the writer-behind guard on this hour.
        _seed_per_field_hour(cache_root, token)

    # Window spans the whole 3-hour stretch (open interval at the end).
    start_iso = hours[0].isoformat()
    end_iso = (hours[2] + timedelta(hours=1)).isoformat()

    # Create the raw table so get_schema_cols returns something — the
    # rollup path still asks for actual_cols to build the active-hour
    # SQL. Empty rows OK; we're not in the active hour.
    from backend.core.log_fields import LOG_FIELD_CATALOG

    table = _safe_table(rollup_source["name"])
    schema_def = ", ".join(
        f'"{f["id"]}" {f["duckdb_type"]}'
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in ("METRICS", "VIRTUAL") and f.get("vcl") is not None
    )
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema_def})")

    out = get_sessions(
        con=in_memory_duckdb,
        src=rollup_source,
        start_time=start_iso,
        end_time=end_iso,
        filters={},
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
    )
    assert out.get("_rollup_served") is True, f"Expected rollup path to serve this query, got keys: {list(out.keys())}"
    assert out["total"] == 2, f"Expected 2 stitched sessions, got: {out['sessions']}"
    for sess in out["sessions"]:
        assert isinstance(sess["session_start"], str)
        assert isinstance(sess["session_end"], str)
    ips = {s["ip"] for s in out["sessions"]}
    assert ips == {"10.0.0.10", "10.0.0.20"}


def test_rollup_returns_none_when_window_le_1h(rollup_source):
    """A ≤1h window should bail out of the rollup path immediately
    so the caller falls back to the raw scan (which is fast there)."""
    from backend.repositories._base import QueryRunner, SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, rollup_source)
        st = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        et = st + timedelta(minutes=45)
        timer = SectionTimer()
        out = _get_sessions_from_rollup(
            runner=runner,
            con=con,
            src=rollup_source,
            table_name=_safe_table(rollup_source["name"]),
            actual_cols=set(),
            start_dt=st,
            end_dt=et,
            page=1,
            limit=50,
            sort_by="session_start",
            sort_dir="desc",
            flagged_only=False,
            min_reqs_flag=1000,
            min_4xx_pct_flag=20.0,
            has_ja4=False,
            has_rtt=False,
            has_edge=False,
            has_edge_sid=False,
            has_cmcd=False,
            section_timings=timer.entries,
        )
        assert out is None
    finally:
        con.close()


def test_rollup_returns_none_when_writer_behind(rollup_source):
    """Per-field rollup tree has the hour but no sessions.parquet
    bundle exists → return None so the caller falls back to raw
    rather than undercount."""
    from backend.repositories._base import QueryRunner, SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    cache_root = rollup_source["_cache_dir_override"]
    # Pick a closed hour and seed per-field but NOT the bundled
    # sessions.parquet — exactly the writer-behind shape.
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=12)
    token = closed.strftime("%Y-%m-%d-%H")
    _seed_per_field_hour(cache_root, token)
    # Also create the hour_bundled root so the no-bundled-root short
    # circuit doesn't trigger first.
    os.makedirs(os.path.join(cache_root, "rollups", "hour_bundled"), exist_ok=True)

    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, rollup_source)
        timer = SectionTimer()
        out = _get_sessions_from_rollup(
            runner=runner,
            con=con,
            src=rollup_source,
            table_name=_safe_table(rollup_source["name"]),
            actual_cols=set(),
            start_dt=closed,
            end_dt=closed + timedelta(hours=2),
            page=1,
            limit=50,
            sort_by="session_start",
            sort_dir="desc",
            flagged_only=False,
            min_reqs_flag=1000,
            min_4xx_pct_flag=20.0,
            has_ja4=False,
            has_rtt=False,
            has_edge=False,
            has_edge_sid=False,
            has_cmcd=False,
            section_timings=timer.entries,
        )
        assert out is None, "writer-behind hour must fall back to raw (return None)"
    finally:
        con.close()


def test_rollup_returns_none_when_no_bundled_root(rollup_source):
    """If the hour_bundled root directory doesn't exist at all, the
    rollup path bails immediately."""
    from backend.repositories._base import QueryRunner, SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    # rollup_source fixture creates cache_root but not the
    # rollups/hour_bundled subdir — perfect for this case.
    con = duckdb.connect(":memory:")
    try:
        runner = QueryRunner(con, rollup_source)
        st = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=12)
        et = st + timedelta(hours=2)
        timer = SectionTimer()
        out = _get_sessions_from_rollup(
            runner=runner,
            con=con,
            src=rollup_source,
            table_name=_safe_table(rollup_source["name"]),
            actual_cols=set(),
            start_dt=st,
            end_dt=et,
            page=1,
            limit=50,
            sort_by="session_start",
            sort_dir="desc",
            flagged_only=False,
            min_reqs_flag=1000,
            min_4xx_pct_flag=20.0,
            has_ja4=False,
            has_rtt=False,
            has_edge=False,
            has_edge_sid=False,
            has_cmcd=False,
            section_timings=timer.entries,
        )
        assert out is None
    finally:
        con.close()


def test_rollup_filter_sql_country_include_and_exclude():
    """``_build_rollup_filter_sql`` must produce IN/NOT IN fragments
    for country (VARCHAR) and asn (INTEGER, cast), and defensively
    return '' for any column outside the rollup-eligible set."""
    from backend.models.common import FilterSpec
    from backend.repositories.sessions import _build_rollup_filter_sql

    # Country include
    out = _build_rollup_filter_sql({"country": FilterSpec(mode="include", values=["US", "CA"])})
    assert ' AND "country" IN (' in out
    assert "'US'" in out and "'CA'" in out

    # Country exclude
    out = _build_rollup_filter_sql({"country": FilterSpec(mode="exclude", values=["RU"])})
    assert ' AND "country" NOT IN (' in out
    assert "'RU'" in out

    # ASN include — integer cast, no quotes
    out = _build_rollup_filter_sql({"asn": FilterSpec(mode="include", values=[7922, "3320"])})
    assert ' AND "asn" IN (' in out
    assert "7922" in out and "3320" in out
    assert "'7922'" not in out  # NOT quoted

    # ASN with all-invalid values → that pill drops, returns '' (no
    # other filters)
    out = _build_rollup_filter_sql({"asn": FilterSpec(mode="include", values=["abc", None])})
    assert out == ""

    # Invalid column → defensive empty return
    out = _build_rollup_filter_sql({"url": FilterSpec(mode="include", values=["/foo"])})
    assert out == ""

    # Empty / None
    assert _build_rollup_filter_sql(None) == ""
    assert _build_rollup_filter_sql({}) == ""

    # Single-quote escaping in country values
    out = _build_rollup_filter_sql({"country": FilterSpec(mode="include", values=["O'Reilly"])})
    assert "'O''Reilly'" in out

    # Multiple compatible filters compose with AND
    out = _build_rollup_filter_sql(
        {
            "country": FilterSpec(mode="include", values=["US"]),
            "asn": FilterSpec(mode="exclude", values=[666]),
        }
    )
    assert ' AND "country" IN (' in out
    assert '"asn" NOT IN (666)' in out

    # Plain-dict spec shape — historically supported via the isinstance
    # branch. The original hasattr(spec, "values") check broke because
    # dict.values is a METHOD; the isinstance(spec, dict) check now
    # routes dict specs through spec.get(...).
    out = _build_rollup_filter_sql({"country": {"mode": "include", "values": ["DE", "FR"]}})
    assert ' AND "country" IN (' in out
    assert "'DE'" in out and "'FR'" in out

    out = _build_rollup_filter_sql({"asn": {"mode": "exclude", "values": [13335]}})
    assert ' AND "asn" NOT IN (13335)' in out

    # Mixed dict + FilterSpec specs compose
    out = _build_rollup_filter_sql(
        {
            "country": {"mode": "include", "values": ["US"]},
            "asn": FilterSpec(mode="include", values=[7922]),
        }
    )
    assert ' AND "country" IN (' in out
    assert '"asn" IN (7922)' in out

    # Dict missing mode defaults to include
    out = _build_rollup_filter_sql({"country": {"values": ["JP"]}})
    assert ' AND "country" IN (' in out
    assert "'JP'" in out
    assert "NOT IN" not in out


def test_rollup_query_failure_returns_none_for_raw_fallback(in_memory_duckdb, rollup_source, monkeypatch):
    """When the stitch SQL throws (duckdb.Error), the function
    records a ``sessions_rollup_failed`` entry in ``section_timings``
    and returns None so the caller falls back to raw."""
    from backend.repositories import _base
    from backend.repositories._base import SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    cache_root = rollup_source["_cache_dir_override"]
    # Seed a valid bundle so we get past collect_hourly_bundle_paths.
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    token = closed.strftime("%Y-%m-%d-%H")
    _seed_per_field_hour(cache_root, token)
    bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")
    _write_rollup_parquet(
        bundle_path,
        [
            {
                "bucket": closed,
                "ip": "10.0.0.30",
                "ja4": "ja4-x",
                "first_ts": closed + timedelta(minutes=1),
                "last_ts": closed + timedelta(minutes=5),
                "req_count": 10,
                "country": "US",
                "asn": 7922,
                "reqs_4xx": 0,
                "reqs_5xx": 0,
                "total_bytes": 1000,
                "rtt_sum": 100.0,
                "rtt_count": 10,
                "edge_count": 10,
                "shield_count": 0,
                "ua_min": "ua",
                "edge_sid_max": "sid",
            }
        ],
    )

    # Patch QueryRunner.execute to raise duckdb.Error.
    original_execute = _base.QueryRunner.execute

    def _boom(self, q, p=None):
        raise duckdb.Error("simulated stitch failure")

    monkeypatch.setattr(_base.QueryRunner, "execute", _boom)
    try:
        runner = _base.QueryRunner(in_memory_duckdb, rollup_source)
        timer = SectionTimer()
        out = _get_sessions_from_rollup(
            runner=runner,
            con=in_memory_duckdb,
            src=rollup_source,
            table_name=_safe_table(rollup_source["name"]),
            actual_cols=set(),
            start_dt=closed,
            end_dt=closed + timedelta(hours=2),
            page=1,
            limit=50,
            sort_by="session_start",
            sort_dir="desc",
            flagged_only=False,
            min_reqs_flag=1000,
            min_4xx_pct_flag=20.0,
            has_ja4=True,
            has_rtt=False,
            has_edge=False,
            has_edge_sid=False,
            has_cmcd=False,
            section_timings=timer.entries,
        )
        assert out is None
        # The failure must have logged a section_timings entry so the
        # perf harness can see why the rollup didn't serve.
        labels = [e["section"] for e in timer.entries]
        assert "sessions_rollup_failed" in labels, (
            f"Expected 'sessions_rollup_failed' in section_timings, got: {labels}"
        )
    finally:
        monkeypatch.setattr(_base.QueryRunner, "execute", original_execute)


def test_active_hour_union_emits_live_sql_when_window_crosses_now(in_memory_duckdb, rollup_source, monkeypatch):
    """When the user window extends into the active (live) hour,
    the union has two parts: the rollup file(s) AND the
    active-hour live SELECT (so the response is current to the
    second).

    Uses *real* ``datetime.now(UTC)`` to align with what
    ``collect_hourly_bundle_paths`` will see (it imports datetime
    inside its body which makes monkeypatching it racy). We pick
    ``active_hour`` from real now, seed the previous hour as the
    closed-bundle half, and pass a window that explicitly extends
    into the active hour."""
    from backend.repositories import _base
    from backend.repositories._base import SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    cache_root = rollup_source["_cache_dir_override"]
    active_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    prev_hour = active_hour - timedelta(hours=1)
    token_prev = prev_hour.strftime("%Y-%m-%d-%H")

    # Seed one closed hour bundle (the rollup half of the union).
    _seed_per_field_hour(cache_root, token_prev)
    _write_rollup_parquet(
        os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token_prev}", "sessions.parquet"),
        [
            {
                "bucket": prev_hour,
                "ip": "10.0.0.40",
                "ja4": None,
                "first_ts": prev_hour + timedelta(minutes=1),
                "last_ts": prev_hour + timedelta(minutes=5),
                "req_count": 3,
                "country": None,
                "asn": None,
                "reqs_4xx": 0,
                "reqs_5xx": 0,
                "total_bytes": 0,
                "rtt_sum": 0.0,
                "rtt_count": 0,
                "edge_count": 0,
                "shield_count": 0,
                "ua_min": None,
                "edge_sid_max": None,
            }
        ],
    )

    # Capture the stitch SQL without actually running it (real
    # execution would fail because the raw table has no data and
    # describe-via-column-names is not what we're asserting).
    captured_sql: list[str] = []

    def _capture_execute(self, q, p=None):
        captured_sql.append(q)
        return self.con.execute("SELECT 1 WHERE 1=0")

    monkeypatch.setattr(_base.QueryRunner, "execute", _capture_execute)

    # Need the raw table to exist for the live SELECT to reference it.
    from backend.core.log_fields import LOG_FIELD_CATALOG

    table = _safe_table(rollup_source["name"])
    schema_def = ", ".join(
        f'"{f["id"]}" {f["duckdb_type"]}'
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in ("METRICS", "VIRTUAL") and f.get("vcl") is not None
    )
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema_def})")

    # Window spans previous closed hour + into the active hour.
    st = prev_hour
    et = active_hour + timedelta(minutes=30)

    runner = _base.QueryRunner(in_memory_duckdb, rollup_source)
    timer = SectionTimer()
    _get_sessions_from_rollup(
        runner=runner,
        con=in_memory_duckdb,
        src=rollup_source,
        table_name=table,
        actual_cols={"ip", "status", "country", "asn"},
        start_dt=st,
        end_dt=et,
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=1000,
        min_4xx_pct_flag=20.0,
        has_ja4=False,
        has_rtt=False,
        has_edge=False,
        has_edge_sid=False,
        has_cmcd=False,
        section_timings=timer.entries,
    )
    assert captured_sql, "Expected QueryRunner.execute to have been called"
    stitch = captured_sql[-1]
    # Two-part union: one from read_parquet (rollup) + one from the
    # raw table (active-hour live).
    assert "read_parquet" in stitch, "Expected rollup half of union to be present"
    assert table in stitch, f"Expected active-hour half of union to reference the raw table {table}; got SQL:\n{stitch}"
    # The UNION ALL keyword must be present (the two halves are joined).
    assert "UNION ALL" in stitch.upper()


def test_rollup_sessions_correctly_identifies_streaming(in_memory_duckdb, rollup_source):
    """When a parquet file contains cmcd_count and has_cmcd is True,
    the session is returned with is_streaming=True."""
    from backend.repositories._base import QueryRunner, SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    cache_root = rollup_source["_cache_dir_override"]
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    token = closed.strftime("%Y-%m-%d-%H")
    _seed_per_field_hour(cache_root, token)
    bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")

    _write_rollup_parquet(
        bundle_path,
        [
            {
                "bucket": closed,
                "ip": "123.45.67.89",
                "ja4": None,
                "first_ts": closed + timedelta(minutes=1),
                "last_ts": closed + timedelta(minutes=5),
                "req_count": 10,
                "country": "US",
                "asn": 7922,
                "reqs_4xx": 0,
                "reqs_5xx": 0,
                "total_bytes": 1000,
                "rtt_sum": 100.0,
                "rtt_count": 10,
                "edge_count": 10,
                "shield_count": 0,
                "ua_min": "ua",
                "edge_sid_max": "sid",
                "cmcd_count": 5,
            }
        ],
    )

    runner = QueryRunner(in_memory_duckdb, rollup_source)
    timer = SectionTimer()
    out = _get_sessions_from_rollup(
        runner=runner,
        con=in_memory_duckdb,
        src=rollup_source,
        table_name=_safe_table(rollup_source["name"]),
        actual_cols={"ip", "cmcd_sid"},
        start_dt=closed,
        end_dt=closed + timedelta(hours=2),
        page=1,
        limit=50,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=1000,
        min_4xx_pct_flag=20.0,
        has_ja4=False,
        has_rtt=False,
        has_edge=False,
        has_edge_sid=False,
        has_cmcd=True,
        section_timings=timer.entries,
    )
    assert out is not None
    assert len(out["sessions"]) == 1
    session = out["sessions"][0]
    assert session["ip"] == "123.45.67.89"
    assert session["is_streaming"] is True


def test_rollup_sessions_suffix_pruning_and_direct_buffer_active_hour(in_memory_duckdb, rollup_source):
    """Suffix pruning on session_start DESC reads only the latest 3 hourly rollup
    bundles when they satisfy `limit`, and `_build_direct_active_hour_session_sql`
    reads active-hour buffer parquets directly without requiring the view."""
    from backend.repositories._base import QueryRunner, SectionTimer
    from backend.repositories.sessions import _get_sessions_from_rollup

    cache_root = rollup_source["_cache_dir_override"]
    active_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hours = [active_hour - timedelta(hours=h) for h in (5, 4, 3, 2, 1)]

    for idx, h in enumerate(hours):
        token = h.strftime("%Y-%m-%d-%H")
        _seed_per_field_hour(cache_root, token)
        bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")
        _write_rollup_parquet(
            bundle_path,
            [
                {
                    "bucket": h,
                    "ip": f"10.99.0.{idx + 1}",
                    "ja4": f"ja4-{idx + 1}",
                    "first_ts": h + timedelta(minutes=10),
                    "last_ts": h + timedelta(minutes=20),
                    "req_count": 10 + idx,
                    "country": "US",
                    "asn": 7922,
                    "reqs_4xx": 0,
                    "reqs_5xx": 0,
                    "total_bytes": 1000,
                    "rtt_sum": 100.0,
                    "rtt_count": 10,
                    "edge_count": 10,
                    "shield_count": 0,
                    "ua_min": "Mozilla/5.0",
                    "edge_sid_max": f"sid-{idx + 1}",
                    "cmcd_count": 0,
                }
            ],
        )

    buf_dir = os.path.join(cache_root, "buffer")
    os.makedirs(buf_dir, exist_ok=True)
    buf_file = os.path.join(buf_dir, "live_batch.parquet")
    live_ts = (active_hour + timedelta(minutes=1)).isoformat()
    in_memory_duckdb.execute(
        f"""
        COPY (
            SELECT
                TIMESTAMPTZ '{live_ts}' AS timestamp,
                '10.99.0.99'::VARCHAR AS ip,
                'ja4-live'::VARCHAR AS ja4,
                'US'::VARCHAR AS country,
                7922::INTEGER AS asn,
                200::INTEGER AS status,
                5000::BIGINT AS resp_bytes,
                1200.0::DOUBLE AS tcp_rtt,
                1::INTEGER AS edge,
                'LiveAgent/1.0'::VARCHAR AS ua,
                'sid-live'::VARCHAR AS edge_sid,
                NULL::VARCHAR AS cmcd_sid
        ) TO '{buf_file}' (FORMAT PARQUET)
        """
    )

    runner = QueryRunner(in_memory_duckdb, rollup_source)
    timer = SectionTimer()
    out = _get_sessions_from_rollup(
        runner=runner,
        con=in_memory_duckdb,
        src=rollup_source,
        table_name="nonexistent_view_table",
        actual_cols={
            "timestamp",
            "ip",
            "ja4",
            "country",
            "asn",
            "status",
            "resp_bytes",
            "tcp_rtt",
            "edge",
            "ua",
            "edge_sid",
            "cmcd_sid",
        },
        start_dt=hours[0],
        end_dt=active_hour + timedelta(minutes=30),
        page=1,
        limit=2,
        sort_by="session_start",
        sort_dir="DESC",
        flagged_only=False,
        min_reqs_flag=1000,
        min_4xx_pct_flag=20.0,
        has_ja4=True,
        has_rtt=True,
        has_edge=True,
        has_edge_sid=True,
        has_cmcd=True,
        section_timings=timer.entries,
    )
    assert out is not None
    assert [s["ip"] for s in out["sessions"]] == ["10.99.0.99", "10.99.0.5"]
    assert len(out["debug_queries"]) == 1
    assert hours[0].strftime("%Y-%m-%d-%H") not in out["debug_queries"][0]["sql"]


def test_get_sessions_streaming_only_count_query_on_full_page(in_memory_duckdb, test_service_source):
    """A full page triggers the count query, which must still see streaming_reqs."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=6, hours_ago=1)
    now = datetime.now(UTC) - timedelta(minutes=5)
    for i, log in enumerate(logs):
        log["ip"] = f"10.0.0.{i}"
        log["timestamp"] = (now + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    insert_mock_logs(in_memory_duckdb, table, logs)
    in_memory_duckdb.execute(f"ALTER TABLE {table} ADD COLUMN cmcd_sid VARCHAR")
    in_memory_duckdb.execute(f"UPDATE {table} SET cmcd_sid = 'sid-1' WHERE ip IN ('10.0.0.0', '10.0.0.1', '10.0.0.2')")
    _clear_schema_cache()

    result = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=2,
        sort_by="session_start",
        sort_dir="desc",
        flagged_only=False,
        min_reqs_flag=None,
        min_4xx_pct_flag=None,
        streaming_only=True,
    )
    assert result["total"] == 3
    assert all(s["is_streaming"] for s in result["sessions"])


# ── Session scoring repo tests (merged from test_session_scoring_repo.py) ──

from unittest.mock import MagicMock  # noqa: E402

from fastapi import HTTPException  # noqa: E402

from backend.repositories import session_scoring as repo  # noqa: E402


def _stub_get_source(monkeypatch, src: dict | None) -> None:
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)


def _stub_connection(monkeypatch, rows: list, columns: list[str] | None = None) -> MagicMock:
    """Make get_connection return a context-manager-ish mock whose execute
    yields ``rows`` with ``columns`` schema. Returns the connection mock so
    tests can assert on .execute calls."""
    mock_con = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_con.execute.return_value = mock_cursor
    if columns is not None:
        mock_con.description = [(c, None) for c in columns]
    else:
        mock_con.description = None
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)
    return mock_con


def test_query_logs_404s_when_service_missing(monkeypatch):
    _stub_get_source(monkeypatch, None)
    with pytest.raises(HTTPException) as ei:
        repo.query_logs("missing-svc", "SELECT 1")
    assert ei.value.status_code == 404
    assert "No service" in ei.value.detail["error"]


def test_query_logs_returns_rows_as_dicts(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    con = _stub_connection(monkeypatch, rows=[(1, "a"), (2, "b")], columns=["id", "name"])

    result = repo.query_logs("svc-1", "SELECT id, name FROM logs")

    assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    con.close.assert_called_once()


def test_query_logs_passes_params_when_provided(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    con = _stub_connection(monkeypatch, rows=[(1,)], columns=["v"])

    repo.query_logs("svc-1", "SELECT 1 WHERE x IN (?, ?)", params=("a", "b"))

    con.execute.assert_called_with("SELECT 1 WHERE x IN (?, ?)", ("a", "b"))


def test_query_logs_400s_on_duckdb_error(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    mock_con = MagicMock()
    mock_con.execute.side_effect = RuntimeError("table 'logs' does not exist")
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)

    with pytest.raises(HTTPException) as ei:
        repo.query_logs("svc-1", "SELECT * FROM logs")
    assert ei.value.status_code == 400
    assert "table 'logs'" in ei.value.detail["error"]


def test_query_logs_handles_empty_description(monkeypatch):
    """Some DDL/no-result statements yield ``description=None``; the
    repository must still return an empty list rather than blowing up."""
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(monkeypatch, rows=[], columns=None)

    result = repo.query_logs("svc-1", "CREATE TABLE t (i INT)")

    assert result == []


def test_query_logs_closes_connection_on_error(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    mock_con = MagicMock()
    mock_con.execute.side_effect = RuntimeError("boom")
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)

    with pytest.raises(HTTPException):
        repo.query_logs("svc-1", "SELECT 1")

    mock_con.close.assert_called_once()


def test_query_logs_appends_to_telemetry_queries(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(monkeypatch, rows=[(1,)], columns=["v"])

    from backend.utils.telemetry import get_queries, start_call_tracking

    start_call_tracking()
    repo.query_logs("svc-1", "SELECT 1")
    queries = get_queries()

    assert len(queries) >= 1
    last = queries[-1]
    assert "SELECT 1" in last["sql"]
    assert last["rows"] == 1
    assert "time_ms" in last


def test_fetch_session_events_returns_empty_when_no_sids():
    assert repo.fetch_session_events("svc-1", []) == {}


def test_fetch_session_events_groups_by_edge_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0, 0), "/", 200, "1.1.1.1", "ua-x", 0.5, "ok", None),
        ("sid-a", datetime(2026, 6, 12, 10, 1, 0), "/p", 200, "1.1.1.1", "ua-x", 0.6, "ok", None),
        ("sid-b", datetime(2026, 6, 12, 10, 2, 0), "/", 404, "2.2.2.2", "ua-y", None, "skipped", "abc"),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a", "sid-b"])

    assert set(result.keys()) == {"sid-a", "sid-b"}
    assert len(result["sid-a"]) == 2
    assert len(result["sid-b"]) == 1
    assert result["sid-a"][0]["ts"].startswith("2026-06-12T10:00:00")
    assert result["sid-b"][0]["url"] == "/"


def test_fetch_session_events_drops_rows_with_no_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("", datetime(2026, 6, 12), "/a", 200, None, None, None, None, None),
        ("sid-real", datetime(2026, 6, 12), "/b", 200, None, None, None, None, None),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-real"])

    assert list(result.keys()) == ["sid-real"]


def test_fetch_session_events_caps_per_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [("sid-a", datetime(2026, 6, 12, 10, i, 0), f"/r{i}", 200, None, None, None, None, None) for i in range(6)]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a"], limit_per_sid=3)

    assert len(result["sid-a"]) == 3


def test_fetch_session_events_stringifies_non_iso_ts(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [("sid-a", "2026-06-12 10:00:00", "/", 200, None, None, None, None, None)]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a"])

    assert result["sid-a"][0]["ts"] == "2026-06-12 10:00:00"


def test_fetch_session_events_handles_none_ts(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [("sid-a", None, "/", 200, None, None, None, None, None)]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a"])

    assert result["sid-a"][0]["ts"] is None


def test_reconstruct_returns_empty_when_no_labels():
    assert repo.reconstruct_labeled_sessions("svc-1", []) == []


def test_reconstruct_returns_empty_when_labels_missing_sid():
    out = repo.reconstruct_labeled_sessions("svc-1", [{"label": "good"}])
    assert out == []


def test_reconstruct_pairs_sessions_with_labels(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0), "/", 200, None, None, 0.4, None, None),
        ("sid-a", datetime(2026, 6, 12, 10, 1), "/p", 200, None, None, 0.7, None, None),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    out = repo.reconstruct_labeled_sessions("svc-1", [{"sid": "sid-a", "label": "bot"}])

    assert len(out) == 1
    session, label = out[0]
    assert label == "bot"
    assert session["session_id"] == "sid-a"
    assert session["max_edge_score"] == 0.7
    assert len(session["events"]) == 2


def test_reconstruct_drops_sids_with_no_events(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(
        monkeypatch,
        rows=[],
        columns=[
            "edge_sid",
            "ts",
            "url",
            "status",
            "ip",
            "ua",
            "edge_score",
            "edge_cookie_compliance",
            "edge_score_reason",
        ],
    )

    out = repo.reconstruct_labeled_sessions(
        "svc-1",
        [
            {"sid": "sid-a", "label": "bot"},
            {"sid": "sid-b", "label": "human"},
        ],
    )

    assert out == []


def test_reconstruct_max_edge_score_none_when_all_scores_null(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0), "/", 200, None, None, None, None, None),
        ("sid-a", datetime(2026, 6, 12, 10, 1), "/p", 200, None, None, None, None, None),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    out = repo.reconstruct_labeled_sessions("svc-1", [{"sid": "sid-a", "label": "human"}])

    session, _ = out[0]
    assert session["max_edge_score"] is None


def test_scoring_source_passthrough_when_all_present():
    cols = set(repo._SCORING_COLUMN_TYPES) | {"edge", "timestamp"}
    assert repo.scoring_source("logs_x", cols) == "logs_x"


def test_scoring_source_passthrough_on_empty_cols():
    assert repo.scoring_source("logs_x", set()) == "logs_x"


def test_scoring_source_synthesizes_missing_columns():
    src = repo.scoring_source("logs_x", {"edge", "timestamp"})
    assert src.startswith("(SELECT *, ") and src.endswith(" FROM logs_x)")
    for col, typ in repo._SCORING_COLUMN_TYPES.items():
        assert f"CAST(NULL AS {typ}) AS {col}" in src


def test_fetch_session_events_degrades_when_scoring_columns_absent(monkeypatch):
    """REGRESSION: the /scoring/sessions/{sid}/events route is reachable before
    scoring is enabled (no label gate). On a never-provisioned service the log
    table lacks edge_sid/edge_score/etc.; fetch_session_events must wrap the
    source so it returns {} (→ HTTP 200, empty timeline) instead of binder-
    erroring into a 400."""
    captured: dict = {}

    def fake_query_logs(service_id, sql, params=()):
        if sql.strip().upper().startswith("DESCRIBE"):
            return [{"column_name": c} for c in ("timestamp", "edge", "ip", "ua", "url", "status")]
        captured["sql"] = sql
        return []

    monkeypatch.setattr(repo, "query_logs", fake_query_logs)

    out = repo.fetch_session_events("noscore-svc", ["abc123"], since_days=30)

    assert out == {}
    assert "CAST(NULL AS VARCHAR) AS edge_sid" in captured["sql"]
    assert "CAST(NULL AS DOUBLE) AS edge_score" in captured["sql"]
