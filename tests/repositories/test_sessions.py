"""Regression tests for backend.repositories.sessions — validates return keys."""

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import duckdb
import pytest
from fastapi import HTTPException

from backend.core.duckdb import _clear_schema_cache
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.models.common import FilterSpec
from backend.repositories import _base
from backend.repositories import session_scoring as repo
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories.sessions import (
    _build_rollup_filter_sql,
    _get_sessions_from_rollup,
    get_session_detail,
    get_sessions,
)
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs


def _sessions_kwargs(**overrides):
    """Default kwargs for `get_sessions` — all required keyword/positional args."""
    base = {
        "start_time": None,
        "end_time": None,
        "filters": {},
        "page": 1,
        "limit": 50,
        "sort_by": "session_start",
        "sort_dir": "desc",
        "flagged_only": False,
        "min_reqs_flag": None,
        "min_4xx_pct_flag": None,
    }
    base.update(overrides)
    return base


def _create_empty_raw_table(con, table_name: str) -> None:
    schema_def = ", ".join(
        f'"{f["id"]}" {f["duckdb_type"]}'
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in ("METRICS", "VIRTUAL") and f.get("vcl") is not None
    )
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")


# ── get_sessions ──────────────────────────────────────────────────────────────


def test_get_sessions_returns_expected_keys(in_memory_duckdb, test_service_source):
    """Result always contains sessions/total/page/limit/has_rtt/has_ja4/has_edge/has_edge_sid."""
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs(limit=20))
    for key in ("sessions", "total", "page", "limit", "has_rtt", "has_ja4", "has_edge", "has_edge_sid"):
        assert key in result, f"Missing key: {key}"
    assert isinstance(result["sessions"], list)
    assert result["page"] == 1
    assert result["limit"] == 20


def test_get_sessions_flagged_only(in_memory_duckdb, test_service_source):
    """Result correctly applies flagging rules and filters to flagged_only=True."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    base_time = datetime.now(UTC) - timedelta(minutes=5)

    for i, log in enumerate(logs):
        log_time = base_time + timedelta(seconds=i)
        log["timestamp"] = log_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        if log["timestamp"].endswith("0000"):
            log["timestamp"] = log["timestamp"][:-4] + "00:00"
        if i < 6:
            log["ip"] = "1.1.1.1"
        else:
            log["ip"] = "2.2.2.2"
            log["status"] = 200
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result_all = get_sessions(
        con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs(flagged_only=False, min_reqs_flag=5)
    )
    ips_all = [s["ip"] for s in result_all["sessions"]]
    assert "1.1.1.1" in ips_all
    assert "2.2.2.2" in ips_all

    result_flagged = get_sessions(
        con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs(flagged_only=True, min_reqs_flag=5)
    )
    ips_flagged = [s["ip"] for s in result_flagged["sessions"]]
    assert "1.1.1.1" in ips_flagged
    assert "2.2.2.2" not in ips_flagged


def test_get_sessions_groups_requests_by_ip(in_memory_duckdb, test_service_source):
    """Multiple requests from the same IP within 30 minutes form a single session."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for log in logs:
        log["ip"] = "10.0.0.1"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs())
    assert result["total"] <= 2
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

    result = get_sessions(con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs())
    for session in result["sessions"]:
        if session.get("session_start") is not None:
            assert isinstance(session["session_start"], str), "session_start must be str for JSON serialization"
        if session.get("session_end") is not None:
            assert isinstance(session["session_end"], str), "session_end must be str for JSON serialization"


def test_get_sessions_empty_table(in_memory_duckdb, test_service_source):
    """Returns correct empty structure when table has no rows."""
    _create_empty_raw_table(in_memory_duckdb, _safe_table(test_service_source["name"]))

    result = get_sessions(con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs(limit=20))
    assert result["sessions"] == []
    assert result["total"] == 0


# ── get_sessions: edge_sid aggregation ────────────────────────────────────────


def test_get_sessions_has_edge_sid_false_when_column_absent(in_memory_duckdb, test_service_source):
    """When the log table has no ``edge_sid`` column, the response reports ``has_edge_sid: False``."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_sessions(con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs(limit=20))
    assert result["has_edge_sid"] is False
    for session in result["sessions"]:
        assert "edge_sid" not in session, f"edge_sid should not appear in session dict when column is absent: {session}"


def test_get_sessions_has_edge_sid_true_and_per_session_value_when_column_present(
    in_memory_duckdb, test_service_source
):
    """When ``edge_sid`` is in the schema, the response reports ``has_edge_sid: True`` and MAX(edge_sid)."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=8, hours_ago=1)
    for log in logs:
        log["ip"] = "10.0.0.50"
    insert_mock_logs(in_memory_duckdb, table_name, logs)
    in_memory_duckdb.execute(f'ALTER TABLE {table_name} ADD COLUMN "edge_sid" VARCHAR')
    in_memory_duckdb.execute(
        f'UPDATE {table_name} SET "edge_sid" = ? WHERE "ip" = ?', ["sid_abc123def456", "10.0.0.50"]
    )
    _clear_schema_cache()

    result = get_sessions(con=in_memory_duckdb, src=test_service_source, **_sessions_kwargs(limit=20))
    assert result["has_edge_sid"] is True
    assert len(result["sessions"]) >= 1
    for session in result["sessions"]:
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


def test_get_sessions_raises_value_error_on_8_day_range(in_memory_duckdb, test_service_source):
    """Sessions view is limited to 7 days."""
    with pytest.raises(ValueError, match="7 days"):
        get_sessions(
            con=in_memory_duckdb,
            src=test_service_source,
            **_sessions_kwargs(
                start_time="2026-01-01T00:00:00",
                end_time="2026-01-09T00:00:00",
                sort_by="total_reqs",
                sort_dir="DESC",
            ),
        )


def test_get_sessions_allows_exactly_7_day_range(in_memory_duckdb, test_service_source):
    """Exactly 7 days is allowed (the cap is `> 7`)."""
    out = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        **_sessions_kwargs(
            start_time="2026-01-01T00:00:00",
            end_time="2026-01-08T00:00:00",
            sort_by="total_reqs",
            sort_dir="DESC",
        ),
    )
    assert "sessions" in out


# ── get_session_detail: UA + JA4 filter additions ────────────────────────


@pytest.mark.parametrize(
    "ip, col, target_val, other_val, num_logs, match_count",
    [
        ("10.0.0.3", "ua", "Browser-A", "Bot-B", 10, 5),
        ("10.0.0.4", "ja4", "t13d-A", "t13d-B", 6, 3),
    ],
    ids=["ua", "ja4"],
)
def test_get_session_detail_filters_by_ua_or_ja4_when_provided(
    in_memory_duckdb, test_service_source, ip, col, target_val, other_val, num_logs, match_count
):
    """When `ua=` or `ja4=` is provided, the WHERE clause adds `<col> IS NOT DISTINCT FROM ?`."""
    logs = generate_mock_logs(test_service_source, num_logs=num_logs, hours_ago=1)
    for i, log in enumerate(logs):
        log["ip"] = ip
        log[col] = target_val if i < match_count else other_val
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_session_detail(
        con=in_memory_duckdb,
        src=test_service_source,
        ip=ip,
        session_start="2000-01-01T00:00:00",
        session_end="2099-12-31T23:59:59",
        **{col: target_val},
    )
    assert len(out["data"]) == match_count
    for r in out["data"]:
        assert r[col] == target_val


# ── _get_sessions_from_rollup + helpers ─────────────────────────────────────


_ROLLUP_PARQUET_COLUMNS = [
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


def _rollup_row(bucket: datetime, ip: str, **overrides) -> dict:
    base = {
        "bucket": bucket,
        "ip": ip,
        "ja4": None,
        "first_ts": bucket + timedelta(minutes=1),
        "last_ts": bucket + timedelta(minutes=5),
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
        "cmcd_count": 0,
    }
    base.update(overrides)
    return base


def _write_rollup_parquet(path: str, rows: list[dict]) -> None:
    """Write a sessions.parquet bundle at ``path`` matching ``build_session_bundles``."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:

        def _v(val):
            return f"CAST('{val}' AS VARCHAR)" if val is not None else "CAST(NULL AS VARCHAR)"

        selects: list[str] = []
        for row in rows:
            asn_sql = f"CAST({row['asn']} AS INTEGER)" if row.get("asn") is not None else "CAST(NULL AS INTEGER)"
            select_parts = [
                f"TIMESTAMPTZ '{row['bucket'].isoformat()}' AS bucket",
                f"CAST('{row['ip']}' AS VARCHAR) AS ip",
                f"{_v(row.get('ja4'))} AS ja4",
                f"TIMESTAMPTZ '{row['first_ts'].isoformat()}' AS first_ts",
                f"TIMESTAMPTZ '{row['last_ts'].isoformat()}' AS last_ts",
                f"CAST({row['req_count']} AS BIGINT) AS req_count",
                f"{_v(row.get('country'))} AS country",
                f"{asn_sql} AS asn",
                f"CAST({row.get('reqs_4xx', 0)} AS BIGINT) AS reqs_4xx",
                f"CAST({row.get('reqs_5xx', 0)} AS BIGINT) AS reqs_5xx",
                f"CAST({row.get('total_bytes', 0)} AS BIGINT) AS total_bytes",
                f"CAST({row.get('rtt_sum', 0.0)} AS DOUBLE) AS rtt_sum",
                f"CAST({row.get('rtt_count', 0)} AS BIGINT) AS rtt_count",
                f"CAST({row.get('edge_count', 0)} AS BIGINT) AS edge_count",
                f"CAST({row.get('shield_count', 0)} AS BIGINT) AS shield_count",
                f"{_v(row.get('ua_min'))} AS ua_min",
                f"{_v(row.get('edge_sid_max'))} AS edge_sid_max",
                f"CAST({row.get('cmcd_count', 0)} AS BIGINT) AS cmcd_count",
            ]
            selects.append("SELECT " + ", ".join(select_parts))
        query = " UNION ALL ".join(selects)
        safe_path = path.replace("'", "''")
        con.execute(f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _seed_per_field_hour(cache_root: str, hour_token: str) -> None:
    """Create an empty per-field rollup hour directory so ``collect_hourly_bundle_paths`` fires."""
    field_hour_dir = os.path.join(cache_root, "rollups", "hour", "field=ip", f"hour={hour_token}")
    os.makedirs(field_hour_dir, exist_ok=True)


def _run_rollup(con, src, start_dt, end_dt, *, timer: SectionTimer | None = None, **overrides):
    runner = QueryRunner(con, src)
    timer = timer or SectionTimer()
    kwargs = {
        "runner": runner,
        "con": con,
        "src": src,
        "table_name": _safe_table(src["name"]),
        "actual_cols": set(),
        "start_dt": start_dt,
        "end_dt": end_dt,
        "page": 1,
        "limit": 50,
        "sort_by": "session_start",
        "sort_dir": "desc",
        "flagged_only": False,
        "min_reqs_flag": 1000,
        "min_4xx_pct_flag": 20.0,
        "has_ja4": False,
        "has_rtt": False,
        "has_edge": False,
        "has_edge_sid": False,
        "has_cmcd": False,
        "section_timings": timer.entries,
    }
    kwargs.update(overrides)
    return _get_sessions_from_rollup(**kwargs)


@pytest.fixture
def rollup_source(tmp_path, test_service_source):
    """A source dict that points cache I/O at a per-test tmp tree."""
    cache_root = tmp_path / "cache" / "test-bucket"
    cache_root.mkdir(parents=True, exist_ok=True)
    src = dict(test_service_source)
    src["bucket"] = "test-bucket"
    src["_cache_dir_override"] = str(cache_root)
    return src


def test_rollup_path_returns_sessions_for_multi_hour_window(in_memory_duckdb, rollup_source):
    """3-hour window with 2 stitched sessions — rollup path returns sessions with ``_rollup_served=True``."""
    cache_root = rollup_source["_cache_dir_override"]
    base = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    hours = [base + timedelta(hours=i) for i in range(3)]
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]

    for h, token in zip(hours, hour_tokens):
        rows = [
            _rollup_row(
                h,
                "10.0.0.10",
                ja4="ja4-A",
                first_ts=h + timedelta(minutes=1),
                last_ts=h + timedelta(minutes=50),
                req_count=100,
                reqs_4xx=5,
                total_bytes=100000,
                rtt_sum=50000.0,
                rtt_count=80,
                edge_count=90,
                shield_count=10,
                ua_min="curl/8.0",
                edge_sid_max="sid_A",
            )
        ]
        if token == hour_tokens[1]:
            rows.append(
                _rollup_row(
                    h,
                    "10.0.0.20",
                    ja4="ja4-B",
                    first_ts=h + timedelta(minutes=15),
                    last_ts=h + timedelta(minutes=20),
                    req_count=5,
                    country="GB",
                    asn=3320,
                    total_bytes=5000,
                    rtt_sum=1000.0,
                    rtt_count=5,
                    edge_count=5,
                    ua_min="Mozilla",
                    edge_sid_max="sid_B",
                )
            )
        bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")
        _write_rollup_parquet(bundle_path, rows)
        _seed_per_field_hour(cache_root, token)

    start_iso = hours[0].isoformat()
    end_iso = (hours[2] + timedelta(hours=1)).isoformat()
    _create_empty_raw_table(in_memory_duckdb, _safe_table(rollup_source["name"]))

    out = get_sessions(
        con=in_memory_duckdb,
        src=rollup_source,
        **_sessions_kwargs(start_time=start_iso, end_time=end_iso),
    )
    assert out.get("_rollup_served") is True, f"Expected rollup path to serve this query, got keys: {list(out.keys())}"
    assert out["total"] == 2, f"Expected 2 stitched sessions, got: {out['sessions']}"
    for sess in out["sessions"]:
        assert isinstance(sess["session_start"], str)
        assert isinstance(sess["session_end"], str)
    ips = {s["ip"] for s in out["sessions"]}
    assert ips == {"10.0.0.10", "10.0.0.20"}


def test_rollup_returns_none_when_window_le_1h(rollup_source):
    """A ≤1h window should bail out of the rollup path immediately."""
    con = duckdb.connect(":memory:")
    try:
        st = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        assert _run_rollup(con, rollup_source, st, st + timedelta(minutes=45)) is None
    finally:
        con.close()


def test_rollup_returns_none_when_writer_behind(rollup_source):
    """Per-field rollup tree has the hour but no sessions.parquet bundle exists → return None."""
    cache_root = rollup_source["_cache_dir_override"]
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=12)
    _seed_per_field_hour(cache_root, closed.strftime("%Y-%m-%d-%H"))
    os.makedirs(os.path.join(cache_root, "rollups", "hour_bundled"), exist_ok=True)

    con = duckdb.connect(":memory:")
    try:
        out = _run_rollup(con, rollup_source, closed, closed + timedelta(hours=2))
        assert out is None, "writer-behind hour must fall back to raw (return None)"
    finally:
        con.close()


def test_rollup_returns_none_when_no_bundled_root(rollup_source):
    """If the hour_bundled root directory doesn't exist at all, the rollup path bails immediately."""
    con = duckdb.connect(":memory:")
    try:
        st = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=12)
        assert _run_rollup(con, rollup_source, st, st + timedelta(hours=2)) is None
    finally:
        con.close()


def test_rollup_filter_sql_country_include_and_exclude():
    """``_build_rollup_filter_sql`` produces IN/NOT IN fragments for country and asn."""
    out = _build_rollup_filter_sql({"country": FilterSpec(mode="include", values=["US", "CA"])})
    assert ' AND "country" IN (' in out and "'US'" in out and "'CA'" in out

    out = _build_rollup_filter_sql({"country": FilterSpec(mode="exclude", values=["RU"])})
    assert ' AND "country" NOT IN (' in out and "'RU'" in out

    out = _build_rollup_filter_sql({"asn": FilterSpec(mode="include", values=[7922, "3320"])})
    assert ' AND "asn" IN (' in out and "7922" in out and "3320" in out and "'7922'" not in out

    assert _build_rollup_filter_sql({"asn": FilterSpec(mode="include", values=["abc", None])}) == ""
    assert _build_rollup_filter_sql({"url": FilterSpec(mode="include", values=["/foo"])}) == ""
    assert _build_rollup_filter_sql(None) == ""
    assert _build_rollup_filter_sql({}) == ""

    assert "'O''Reilly'" in _build_rollup_filter_sql({"country": FilterSpec(mode="include", values=["O'Reilly"])})

    out = _build_rollup_filter_sql(
        {"country": FilterSpec(mode="include", values=["US"]), "asn": FilterSpec(mode="exclude", values=[666])}
    )
    assert ' AND "country" IN (' in out and '"asn" NOT IN (666)' in out

    out = _build_rollup_filter_sql({"country": {"mode": "include", "values": ["DE", "FR"]}})
    assert ' AND "country" IN (' in out and "'DE'" in out and "'FR'" in out
    assert ' AND "asn" NOT IN (13335)' in _build_rollup_filter_sql({"asn": {"mode": "exclude", "values": [13335]}})

    out = _build_rollup_filter_sql(
        {"country": {"mode": "include", "values": ["US"]}, "asn": FilterSpec(mode="include", values=[7922])}
    )
    assert ' AND "country" IN (' in out and '"asn" IN (7922)' in out

    out = _build_rollup_filter_sql({"country": {"values": ["JP"]}})
    assert ' AND "country" IN (' in out and "'JP'" in out and "NOT IN" not in out


def test_rollup_query_failure_returns_none_for_raw_fallback(in_memory_duckdb, rollup_source, monkeypatch):
    """When the stitch SQL throws (duckdb.Error), records ``sessions_rollup_failed`` and returns None."""
    cache_root = rollup_source["_cache_dir_override"]
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    token = closed.strftime("%Y-%m-%d-%H")
    _seed_per_field_hour(cache_root, token)
    bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")
    _write_rollup_parquet(bundle_path, [_rollup_row(closed, "10.0.0.30", ja4="ja4-x")])

    def _boom(self, q, p=None):
        raise duckdb.Error("simulated stitch failure")

    monkeypatch.setattr(_base.QueryRunner, "execute", _boom)
    timer = SectionTimer()
    out = _run_rollup(in_memory_duckdb, rollup_source, closed, closed + timedelta(hours=2), timer=timer, has_ja4=True)
    assert out is None
    labels = [e["section"] for e in timer.entries]
    assert "sessions_rollup_failed" in labels, f"Expected 'sessions_rollup_failed' in section_timings, got: {labels}"


def test_active_hour_union_emits_live_sql_when_window_crosses_now(in_memory_duckdb, rollup_source, monkeypatch):
    """When the user window extends into the active hour, union has rollup file(s) AND active-hour live SELECT."""
    cache_root = rollup_source["_cache_dir_override"]
    active_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    prev_hour = active_hour - timedelta(hours=1)
    token_prev = prev_hour.strftime("%Y-%m-%d-%H")

    _seed_per_field_hour(cache_root, token_prev)
    _write_rollup_parquet(
        os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token_prev}", "sessions.parquet"),
        [
            _rollup_row(
                prev_hour,
                "10.0.0.40",
                req_count=3,
                country=None,
                asn=None,
                total_bytes=0,
                rtt_sum=0.0,
                rtt_count=0,
                edge_count=0,
                ua_min=None,
                edge_sid_max=None,
            )
        ],
    )

    captured_sql: list[str] = []

    def _capture_execute(self, q, p=None):
        captured_sql.append(q)
        return self.con.execute("SELECT 1 WHERE 1=0")

    monkeypatch.setattr(_base.QueryRunner, "execute", _capture_execute)

    table = _safe_table(rollup_source["name"])
    _create_empty_raw_table(in_memory_duckdb, table)

    _run_rollup(
        in_memory_duckdb,
        rollup_source,
        prev_hour,
        active_hour + timedelta(minutes=30),
        table_name=table,
        actual_cols={"ip", "status", "country", "asn"},
    )
    assert captured_sql, "Expected QueryRunner.execute to have been called"
    stitch = captured_sql[-1]
    assert "read_parquet" in stitch, "Expected rollup half of union to be present"
    assert table in stitch, f"Expected active-hour half of union to reference the raw table {table}; got SQL:\n{stitch}"
    assert "UNION ALL" in stitch.upper()


def test_rollup_sessions_correctly_identifies_streaming(in_memory_duckdb, rollup_source):
    """When a parquet file contains cmcd_count and has_cmcd is True, session is returned with is_streaming=True."""
    cache_root = rollup_source["_cache_dir_override"]
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    token = closed.strftime("%Y-%m-%d-%H")
    _seed_per_field_hour(cache_root, token)
    bundle_path = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={token}", "sessions.parquet")
    _write_rollup_parquet(bundle_path, [_rollup_row(closed, "123.45.67.89", cmcd_count=5)])

    out = _run_rollup(
        in_memory_duckdb,
        rollup_source,
        closed,
        closed + timedelta(hours=2),
        actual_cols={"ip", "cmcd_sid"},
        has_cmcd=True,
    )
    assert out is not None
    assert len(out["sessions"]) == 1
    session = out["sessions"][0]
    assert session["ip"] == "123.45.67.89"
    assert session["is_streaming"] is True


def test_rollup_sessions_suffix_pruning_and_direct_buffer_active_hour(in_memory_duckdb, rollup_source):
    """Suffix pruning on session_start DESC reads only the latest 3 hourly rollup bundles."""
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
                _rollup_row(
                    h,
                    f"10.99.0.{idx + 1}",
                    ja4=f"ja4-{idx + 1}",
                    first_ts=h + timedelta(minutes=10),
                    last_ts=h + timedelta(minutes=20),
                    req_count=10 + idx,
                    ua_min="Mozilla/5.0",
                    edge_sid_max=f"sid-{idx + 1}",
                )
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

    out = _run_rollup(
        in_memory_duckdb,
        rollup_source,
        hours[0],
        active_hour + timedelta(minutes=30),
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
        limit=2,
        sort_dir="DESC",
        has_ja4=True,
        has_rtt=True,
        has_edge=True,
        has_edge_sid=True,
        has_cmcd=True,
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
        **_sessions_kwargs(limit=2, streaming_only=True),
    )
    assert result["total"] == 3
    assert all(s["is_streaming"] for s in result["sessions"])


# ── Session scoring repo tests (merged from test_session_scoring_repo.py) ──

_EVENT_COLUMNS = [
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


def _stub_get_source(monkeypatch, src: dict | None) -> None:
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)


def _stub_connection(monkeypatch, rows: list, columns: list[str] | None = None) -> MagicMock:
    """Make get_connection return a context-manager-ish mock yielding ``rows`` with ``columns``."""
    mock_con = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_con.execute.return_value = mock_cursor
    mock_con.description = [(c, None) for c in columns] if columns is not None else None
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


def test_query_logs_400s_on_duckdb_error_and_closes_connection(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    mock_con = MagicMock()
    mock_con.execute.side_effect = RuntimeError("table 'logs' does not exist")
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)
    with pytest.raises(HTTPException) as ei:
        repo.query_logs("svc-1", "SELECT * FROM logs")
    assert ei.value.status_code == 400
    assert "table 'logs'" in ei.value.detail["error"]
    mock_con.close.assert_called_once()


def test_query_logs_handles_empty_description(monkeypatch):
    """Some DDL/no-result statements yield ``description=None``; must return an empty list."""
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(monkeypatch, rows=[], columns=None)
    assert repo.query_logs("svc-1", "CREATE TABLE t (i INT)") == []


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
    _stub_connection(monkeypatch, rows=rows, columns=_EVENT_COLUMNS)
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
    _stub_connection(monkeypatch, rows=rows, columns=_EVENT_COLUMNS)
    assert list(repo.fetch_session_events("svc-1", ["sid-real"]).keys()) == ["sid-real"]


def test_fetch_session_events_caps_per_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [("sid-a", datetime(2026, 6, 12, 10, i, 0), f"/r{i}", 200, None, None, None, None, None) for i in range(6)]
    _stub_connection(monkeypatch, rows=rows, columns=_EVENT_COLUMNS)
    result = repo.fetch_session_events("svc-1", ["sid-a"], limit_per_sid=3)
    assert len(result["sid-a"]) == 3


@pytest.mark.parametrize(
    "raw_ts, expected_ts",
    [
        ("2026-06-12 10:00:00", "2026-06-12 10:00:00"),
        (None, None),
    ],
)
def test_fetch_session_events_ts_edge_cases(monkeypatch, raw_ts, expected_ts):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [("sid-a", raw_ts, "/", 200, None, None, None, None, None)]
    _stub_connection(monkeypatch, rows=rows, columns=_EVENT_COLUMNS)
    result = repo.fetch_session_events("svc-1", ["sid-a"])
    assert result["sid-a"][0]["ts"] == expected_ts


@pytest.mark.parametrize("labels", [[], [{"label": "good"}]])
def test_reconstruct_returns_empty_when_no_valid_labels(labels):
    assert repo.reconstruct_labeled_sessions("svc-1", labels) == []


def test_reconstruct_pairs_sessions_with_labels(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0), "/", 200, None, None, 0.4, None, None),
        ("sid-a", datetime(2026, 6, 12, 10, 1), "/p", 200, None, None, 0.7, None, None),
    ]
    _stub_connection(monkeypatch, rows=rows, columns=_EVENT_COLUMNS)
    out = repo.reconstruct_labeled_sessions("svc-1", [{"sid": "sid-a", "label": "bot"}])
    assert len(out) == 1
    session, label = out[0]
    assert label == "bot"
    assert session["session_id"] == "sid-a"
    assert session["max_edge_score"] == 0.7
    assert len(session["events"]) == 2


def test_reconstruct_drops_sids_with_no_events(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(monkeypatch, rows=[], columns=_EVENT_COLUMNS)
    out = repo.reconstruct_labeled_sessions(
        "svc-1",
        [{"sid": "sid-a", "label": "bot"}, {"sid": "sid-b", "label": "human"}],
    )
    assert out == []


def test_reconstruct_max_edge_score_none_when_all_scores_null(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0), "/", 200, None, None, None, None, None),
        ("sid-a", datetime(2026, 6, 12, 10, 1), "/p", 200, None, None, None, None, None),
    ]
    _stub_connection(monkeypatch, rows=rows, columns=_EVENT_COLUMNS)
    out = repo.reconstruct_labeled_sessions("svc-1", [{"sid": "sid-a", "label": "human"}])
    session, _ = out[0]
    assert session["max_edge_score"] is None


@pytest.mark.parametrize("cols", [set(repo._SCORING_COLUMN_TYPES) | {"edge", "timestamp"}, set()])
def test_scoring_source_passthrough(cols):
    assert repo.scoring_source("logs_x", cols) == "logs_x"


def test_scoring_source_synthesizes_missing_columns():
    src = repo.scoring_source("logs_x", {"edge", "timestamp"})
    assert src.startswith("(SELECT *, ") and src.endswith(" FROM logs_x)")
    for col, typ in repo._SCORING_COLUMN_TYPES.items():
        assert f"CAST(NULL AS {typ}) AS {col}" in src


def test_fetch_session_events_degrades_when_scoring_columns_absent(monkeypatch):
    """REGRESSION: fetch_session_events wraps the source when scoring columns are absent."""
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
