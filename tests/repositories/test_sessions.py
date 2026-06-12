"""Regression tests for backend.repositories.sessions — validates return keys."""

import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.repositories._base import _safe_table
from backend.repositories.sessions import get_session_detail, get_sessions
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


@pytest.fixture(autouse=True)
def clear_caches():
    _clear_schema_cache()
    yield
    _clear_schema_cache()


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
