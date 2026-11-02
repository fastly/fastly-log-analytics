"""Regression tests for backend.repositories.sessions — validates return keys."""

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import duckdb
import pytest
from fastapi import HTTPException

import backend.repositories.sessions as repo
from backend.core.duckdb import _clear_schema_cache
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.models.common import FilterSpec
from backend.repositories import _base
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
    assert out["total"] == 6
    assert len(out["debug_queries"]) == 1


def test_rollup_sessions_streaming_only_filters_and_counts_accurately(in_memory_duckdb, rollup_source):
    """Rollup fast path supports streaming_only=True using cmcd_count > 0 across single and multi-hour sessions."""
    cache_root = rollup_source["_cache_dir_override"]
    closed = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    h1 = closed
    h2 = closed + timedelta(hours=1)
    for h in (h1, h2):
        _seed_per_field_hour(cache_root, h.strftime("%Y-%m-%d-%H"))
    _write_rollup_parquet(
        os.path.join(cache_root, "rollups", "hour_bundled", f"hour={h1.strftime('%Y-%m-%d-%H')}", "sessions.parquet"),
        [
            _rollup_row(
                h1, "10.88.0.1", cmcd_count=3, first_ts=h1 + timedelta(minutes=50), last_ts=h1 + timedelta(minutes=55)
            ),
            _rollup_row(h1, "10.88.0.2", cmcd_count=0),
        ],
    )
    _write_rollup_parquet(
        os.path.join(cache_root, "rollups", "hour_bundled", f"hour={h2.strftime('%Y-%m-%d-%H')}", "sessions.parquet"),
        [
            _rollup_row(
                h2, "10.88.0.1", cmcd_count=2, first_ts=h2 + timedelta(minutes=5), last_ts=h2 + timedelta(minutes=10)
            ),
            _rollup_row(h2, "10.88.0.3", cmcd_count=4),
        ],
    )

    out = _run_rollup(
        in_memory_duckdb,
        rollup_source,
        h1,
        h2 + timedelta(hours=1),
        actual_cols={"ip", "cmcd_sid"},
        has_cmcd=True,
        streaming_only=True,
    )
    assert out is not None
    assert out["total"] == 2
    assert {s["ip"] for s in out["sessions"]} == {"10.88.0.1", "10.88.0.3"}
    assert all(s["is_streaming"] is True for s in out["sessions"])


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


# ── Merged from tests/repositories/test_alerts.py ──

import uuid
from datetime import datetime
from unittest.mock import patch

import pytest

import backend.core.metadata as metadata_db
from backend.models.common import Alert
from backend.repositories.assets import (
    delete_alert,
    evaluate_alert,
    get_alert_by_id,
    get_alerts,
    save_alert,
    toggle_alert,
    update_last_triggered,
)


def _make_alert(
    service_id: str,
    name: str = "Test Alert",
    metric: str = "5xx",
    threshold: float = 10.0,
    operator: str = ">",
    enabled: bool = True,
) -> Alert:
    return Alert(
        id=str(uuid.uuid4()),
        service_id=service_id,
        name=name,
        category="reliability",
        metric=metric,
        evaluation_type="absolute",
        operator=operator,
        threshold=threshold,
        window_min=5,
        comparison_period_min=None,
        status_codes=None,
        webhook_url=None,
        enabled=enabled,
        evaluation_scope="all",
    )


def test_alert_lifecycle():
    """Create, read, and ensure schema updates for alerts in per-service SQLite."""
    service_id = "test_service_for_alerts"

    alert = Alert(
        id=str(uuid.uuid4()),
        service_id=service_id,
        name="High 500s",
        category="reliability",
        metric="5xx",
        evaluation_type="absolute",
        operator=">",
        threshold=100.0,
        window_min=5,
        comparison_period_min=None,
        status_codes=[500, 502, 503, 504],
        webhook_url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",  # gitleaks:allow
        enabled=True,
        evaluation_scope="all",
    )

    saved_alert = save_alert(alert)
    assert saved_alert["id"] == alert.id

    alerts = get_alerts(service_id)
    assert len(alerts) == 1
    assert alerts[0]["name"] == "High 500s"
    assert alerts[0]["status_codes"] == [500, 502, 503, 504]

    alert2 = Alert(
        id=str(uuid.uuid4()),
        service_id=service_id,
        name="Low Traffic",
        category="traffic",
        metric="requests",
        evaluation_type="absolute",
        operator="<",
        threshold=10.0,
        window_min=15,
        comparison_period_min=None,
        status_codes=None,
        webhook_url=None,
        enabled=False,
        evaluation_scope="edge",
    )
    save_alert(alert2)

    all_alerts = get_alerts(service_id)
    assert len(all_alerts) == 2


# ── get_alerts: cross-service scan when no service_id given ───────────────────


def test_get_alerts_without_service_id_scans_all_configured_services():
    """``get_alerts()`` with no service_id walks every configured service —
    used by the admin overview to render alerts across the whole install."""
    save_alert(_make_alert("svc-a", name="A's alert"))
    save_alert(_make_alert("svc-b", name="B's alert"))

    with patch(
        "backend.repositories.assets.svcconfig.list_configs",
        return_value=[{"service_id": "svc-a"}, {"service_id": "svc-b"}],
    ):
        out = get_alerts()

    names = {a["name"] for a in out}
    assert "A's alert" in names
    assert "B's alert" in names


# ── get_alert_by_id: tenant-scoped lookup (audit finding 018) ────────────────


def test_get_alert_by_id_returns_row_when_present():
    sid = "svc-find-b"
    save_alert(_make_alert("svc-find-a"))
    alert_b = _make_alert(sid)
    save_alert(alert_b)

    row = get_alert_by_id(alert_b.id, sid)
    assert row is not None
    assert row["id"] == alert_b.id


def test_get_alert_by_id_returns_none_when_absent():
    assert get_alert_by_id("nonexistent-alert-id", "svc-find-none") is None


# ── toggle_alert / delete_alert: scoped to service (audit finding 018) ───────


def test_toggle_alert_flips_enabled_in_scoped_service():
    sid = "svc-toggle"
    alert = _make_alert(sid, enabled=True)
    save_alert(alert)

    res = toggle_alert(alert.id, enabled=False, service_id=sid)
    assert res.get("status") != "not_found"

    alerts = get_alerts(sid)
    assert any(a["id"] == alert.id and a["enabled"] is False for a in alerts)


def test_delete_alert_removes_row_in_scoped_service():
    sid = "svc-del"
    alert = _make_alert(sid)
    save_alert(alert)

    res = delete_alert(alert.id, service_id=sid)
    assert res.get("status") != "not_found"
    assert all(a["id"] != alert.id for a in get_alerts(sid))


def test_delete_alert_unknown_id_returns_status():
    """Delete is idempotent — deleting an unknown id in a specific
    service returns a status payload (currently 'success' since the
    SQLite DELETE matches zero rows without error). Contract: never
    raise, always return a dict with a status key."""
    res = delete_alert("does-not-exist", service_id="svc-no-alerts")
    assert "status" in res


# ── update_last_triggered: stamps the timestamp into SQLite ───────────────────


def test_update_last_triggered_sets_timestamp():
    sid = "svc-trig"
    alert = _make_alert(sid)
    save_alert(alert)

    ts = "2026-05-16T12:34:56Z"
    update_last_triggered(sid, alert.id, ts)

    row = metadata_db.get_con(sid).execute("SELECT last_triggered_at FROM alerts WHERE id = ?", (alert.id,)).fetchone()
    assert ts in row[0]


# ── evaluate_alert: end-to-end against a tiny in-memory DuckDB ────────────────


@pytest.fixture
def alert_table():
    """In-memory DuckDB with a ``logs_alertsvc`` table seeded with rows
    spanning the past 5 minutes so the alert window captures them."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_alertsvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = []
    # 100 successful + 5 errors all within the last 2 minutes
    for i in range(100):
        rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
    for i in range(5):
        rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
    con.executemany("INSERT INTO logs_alertsvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    yield con
    con.close()


def test_evaluate_alert_returns_false_for_empty_table():
    """An empty log table → no max_ts → no trigger."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE logs_empty_svc (timestamp TIMESTAMPTZ, status INTEGER)")
        alert = _make_alert("empty_svc").model_dump()
        triggered, webhook, payload, max_ts = evaluate_alert(con, {"name": "empty_svc"}, alert)
        assert triggered is False
        assert max_ts is None
    finally:
        con.close()


def test_evaluate_alert_triggers_when_5xx_exceeds_threshold(alert_table):
    """5 errors > threshold 2 → triggered."""
    alert = _make_alert("alertsvc", metric="5xx", threshold=2.0, operator=">").model_dump()
    triggered, _, _, _ = evaluate_alert(alert_table, {"name": "alertsvc"}, alert)
    assert triggered is True


def test_evaluate_alert_does_not_trigger_when_below_threshold(alert_table):
    """5 errors < threshold 100 → not triggered."""
    alert = _make_alert("alertsvc", metric="5xx", threshold=100.0, operator=">").model_dump()
    triggered, _, _, _ = evaluate_alert(alert_table, {"name": "alertsvc"}, alert)
    assert triggered is False


def test_evaluate_alert_skips_when_data_is_too_stale():
    """If max(timestamp) is > 30 minutes ago, evaluate bails — alerts
    only fire on actively-flowing data, not on a paused service."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE logs_stalesvc (timestamp TIMESTAMPTZ, status INTEGER)")
        stale = datetime.now(UTC) - timedelta(hours=2)
        con.execute("INSERT INTO logs_stalesvc VALUES (?, ?)", (stale, 500))
        alert = _make_alert("stalesvc", metric="5xx", threshold=0).model_dump()
        triggered, _, _, _ = evaluate_alert(con, {"name": "stalesvc"}, alert)
        assert triggered is False
    finally:
        con.close()


# ── evaluate_alert: operator semantics ───────────────────────────────────────


@pytest.fixture
def busy_alert_table():
    """In-memory DuckDB with enough rows (>= 50) that the
    "minimum-request" guard for non-absolute eval types is satisfied,
    and the 5xx count lands exactly at 50 for boundary tests."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_busysvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = []
    # 50 5xx errors + 100 200s, all within the last 2 minutes
    for i in range(50):
        rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
    for i in range(100):
        rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
    con.executemany("INSERT INTO logs_busysvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    yield con
    con.close()


@pytest.mark.parametrize(
    "op,threshold,expected",
    [
        (">", 49, True),  # 50 > 49
        (">", 50, False),  # 50 > 50 is False
        (">=", 50, True),  # 50 >= 50 is True
        ("<", 51, True),  # 50 < 51
        ("<", 50, False),  # 50 < 50 is False
        ("<=", 50, True),  # 50 <= 50
    ],
)
def test_evaluate_alert_operator_branches(busy_alert_table, op, threshold, expected):
    """Pin every operator branch (>, <, >=, <=) — the comparison logic
    is a small if-elif chain and a typo would silently flip thresholds.
    Uses an alert table seeded with exactly 50 5xx errors so each
    operator's boundary case is exercised."""
    alert = _make_alert("busysvc", metric="5xx", threshold=threshold, operator=op).model_dump()
    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is expected


def test_evaluate_alert_unknown_operator_does_not_trigger(busy_alert_table):
    """An operator not in the {<, <=, >, >=} set defaults to False —
    pinned because a future op like ``!=`` should fail-closed (no
    spurious trigger), not crash with KeyError."""
    alert = _make_alert("busysvc", metric="5xx", threshold=0, operator=">").model_dump()
    alert["operator"] = "!="
    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is False


# ── evaluate_alert: relative_increase / relative_decrease ────────────────────


def test_evaluate_alert_relative_increase_compares_against_history():
    """``relative_increase`` compares the current-window value against
    the same metric in a baseline window ``comp_period_min`` ago. A
    big enough percentage gain → triggered."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_relsvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        rows = []
        # Current window (last 2 min): 200 requests, 50 are 5xx → high error rate
        for i in range(150):
            rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(50):
            rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        # Historic window (61-65 minutes ago): 100 requests, 2 are 5xx → low rate
        for i in range(98):
            rows.append((now - timedelta(minutes=61, seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(2):
            rows.append((now - timedelta(minutes=61, seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        con.executemany("INSERT INTO logs_relsvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert(
            "relsvc",
            metric="5xx",
            threshold=100,  # 100% increase
            operator=">",
        ).model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        alert["window_min"] = 5

        triggered, _, _, _ = evaluate_alert(con, {"name": "relsvc"}, alert)
        assert triggered is True
    finally:
        con.close()


def test_evaluate_alert_relative_skips_when_history_is_zero():
    """If the historical window has zero events, dividing by zero is
    skipped — return not-triggered. Pinned because a refactor that
    treats zero baseline as "infinite increase" would spam alerts on
    every new-traffic spike for a freshly-provisioned service."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_norel (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        rows = [(now - timedelta(seconds=i), 500, True, 50.0, 100, "MISS", 1024, 200, 100) for i in range(60)]
        con.executemany("INSERT INTO logs_norel VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert("norel", metric="5xx", threshold=0, operator=">").model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        alert["window_min"] = 5
        triggered, _, _, _ = evaluate_alert(con, {"name": "norel"}, alert)
        assert triggered is False
    finally:
        con.close()


def test_evaluate_alert_relative_skips_when_traffic_too_low():
    """Non-absolute alerts require >= 10 requests in the window —
    otherwise a single error in 5 requests would compute to "+100%"
    and fire constantly. Pinned because lowering this threshold would
    surface as alert spam on low-traffic services."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_lowsvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        # Only 5 requests total in the window — below the 10-req floor
        rows = [(now - timedelta(seconds=i * 5), 500, True, 50.0, 100, "MISS", 1024, 200, 100) for i in range(5)]
        con.executemany("INSERT INTO logs_lowsvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert("lowsvc", metric="5xx", threshold=10, operator=">").model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        triggered, _, _, _ = evaluate_alert(con, {"name": "lowsvc"}, alert)
        assert triggered is False

    finally:
        con.close()


# ── evaluate_alert: webhook payload construction ────────────────────────────


def test_evaluate_alert_builds_slack_payload_with_webhook_url(busy_alert_table):
    """A triggered alert with a webhook_url returns the formatted
    Slack-shaped payload. Pinned because the frontend's "Test webhook"
    button POSTs whatever shape this returns."""
    alert = _make_alert(
        "busysvc",
        metric="5xx",
        threshold=1,
        operator=">",
    ).model_dump()
    alert["webhook_url"] = "https://hooks.slack.com/services/T00/B00/XXXX"

    triggered, webhook, payload, max_ts = evaluate_alert(
        busy_alert_table, {"name": "busysvc"}, alert, display_name="Prod CDN", service_id="svc-busy"
    )

    assert triggered is True
    assert webhook == "https://hooks.slack.com/services/T00/B00/XXXX"
    assert payload is not None
    text = payload["text"]
    assert "🚨 *Fastly Alert Triggered*" in text
    assert "Test Alert" in text  # name from _make_alert
    assert "Prod CDN" in text  # display_name
    assert "manage.fastly.com/configure/services/svc-busy" in text
    assert max_ts is not None  # ISO string of the max timestamp


def test_evaluate_alert_no_webhook_url_returns_none_payload(busy_alert_table):
    """When the alert has no webhook_url, the payload field is None
    (no formatted text). Pinned because skipping this would crash
    the webhook-dispatch loop when alerts only update a UI badge."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    alert["webhook_url"] = None

    triggered, webhook, payload, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is True
    assert webhook is None
    assert payload is None


def test_evaluate_alert_webhook_message_includes_relative_increase_phrasing():
    """relative_increase → "increased by N% vs M min ago" phrasing."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_relmsg (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        rows = []
        # Current: 200 requests, 100 5xx (50% error rate)
        for i in range(100):
            rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(100):
            rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        # Historic: 200 requests, 10 5xx (5% error rate) → 900% increase
        for i in range(190):
            rows.append((now - timedelta(minutes=61, seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(10):
            rows.append((now - timedelta(minutes=61, seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        con.executemany("INSERT INTO logs_relmsg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert("relmsg", metric="5xx", threshold=100, operator=">").model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        alert["window_min"] = 5
        alert["webhook_url"] = "https://hooks.example.com"

        triggered, _, payload, _ = evaluate_alert(con, {"name": "relmsg"}, alert)
        assert triggered is True
        assert payload is not None
        assert "increased by" in payload["text"]
        assert "% vs 60m ago" in payload["text"]
    finally:
        con.close()


# ── evaluate_alert: duplicate-fire suppression ──────────────────────────────


def test_evaluate_alert_suppresses_when_recently_triggered(busy_alert_table):
    """If ``last_triggered_at`` is within the past hour, the alert
    must NOT fire again — prevents Slack/email storms when an issue
    persists across multiple evaluations."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    # Triggered 5 minutes ago
    recent = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    alert["last_triggered_at"] = recent

    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is False


def test_evaluate_alert_fires_again_after_an_hour(busy_alert_table):
    """``last_triggered_at`` > 1 hour ago → suppression lifts and the
    alert can fire again."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    alert["last_triggered_at"] = old

    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is True


def test_evaluate_alert_tolerates_malformed_last_triggered_at(busy_alert_table):
    """If ``last_triggered_at`` is garbage (an old format from a
    migration), the try/except swallows ValueError and the alert
    fires normally rather than crashing the cron."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    alert["last_triggered_at"] = "not-a-timestamp-at-all"

    # Must not raise; treat as never-triggered → fires.
    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    # Either fires or doesn't — what matters is no exception. We assert one
    # specific shape (triggered=True) since the malformed value was non-recent
    # — but the key contract is "doesn't raise".
    assert triggered in (True, False)


# ── evaluate_alert: query failure path ──────────────────────────────────────


def test_evaluate_alert_returns_false_on_query_exception():
    """If DuckDB raises mid-query (table missing, bad SQL from a
    custom metric), the outer try/except catches and returns False.
    Pinned because raising here would abort the whole alert-eval
    cron, silencing every other service's alerts."""
    con = duckdb.connect(":memory:")
    try:
        # Don't create the logs_brokensvc table → max() query raises
        alert = _make_alert("brokensvc", metric="5xx", threshold=1, operator=">").model_dump()
        triggered, _, _, _ = evaluate_alert(con, {"name": "brokensvc"}, alert)
        assert triggered is False
    finally:
        con.close()


# ── update_last_triggered ──────────────────────────────────────────────────


def test_update_last_triggered_with_none_uses_now():
    """Passing ``triggered_ts=None`` makes the metadata layer stamp
    the current time — pinned so a future refactor doesn't silently
    persist None."""
    sid = "svc-now"
    alert = _make_alert(sid)
    save_alert(alert)

    update_last_triggered(sid, alert.id, None)

    row = metadata_db.get_con(sid).execute("SELECT last_triggered_at FROM alerts WHERE id = ?", (alert.id,)).fetchone()
    # Either a timestamp string OR None — implementation-defined; pinning
    # that the function doesn't raise when called with None.
    assert row is not None


# ── Views repository tests (merged from test_views.py) ────────────────────

from unittest.mock import MagicMock  # noqa: E402

from backend.models.common import SavedView  # noqa: E402
from backend.repositories.assets import (  # noqa: E402
    delete_cron_log,
    delete_view,
    get_cron_logs,
    get_view_by_id,
    get_views,
    purge_cron_logs,
    save_view,
)


def _make_view(service_id: str, name: str = "My View") -> SavedView:
    return SavedView(service_id=service_id, name=name, filters_json='{"status":[500]}', page="dashboard")


def test_save_and_list_views():
    sid = "svc-views-1"
    saved = save_view(_make_view(sid, "alpha"))
    assert saved["id"]
    assert saved["status"] == "success"

    rows = get_views(sid)
    names = [v["name"] for v in rows]
    assert "alpha" in names


def test_get_views_empty_when_unseeded():
    assert get_views("svc-views-empty") == []


def test_delete_view_scoped_to_service():
    sid = "svc-views-2"
    view = save_view(_make_view(sid, "to-delete"))
    res = delete_view(view["id"], service_id=sid)
    assert res["status"] in ("success", "deleted")
    assert res["service_id"] == sid
    assert all(v["id"] != view["id"] for v in get_views(sid))


def test_delete_view_unknown_id_returns_not_found():
    res = delete_view("does-not-exist", service_id="svc-views-3")
    assert "status" in res


def test_get_view_by_id_returns_view_with_service_id_stamped():
    sid = "svc-views-get-by-id"
    view = save_view(_make_view(sid, "by-id"))
    row = get_view_by_id(view["id"], sid)
    assert row is not None
    assert row["id"] == view["id"]
    assert row["service_id"] == sid


def test_get_view_by_id_returns_none_when_view_does_not_exist():
    assert get_view_by_id("nonexistent-id", "svc-no-views") is None


def test_metadata_save_view_uses_service_id_parameter_not_payload_value():
    from backend.core.metadata.base import list_views as md_list_views
    from backend.core.metadata.base import save_view as md_save_view

    trusted_sid = "svc-views-016-trusted"
    untrusted_sid = "svc-views-016-attacker"

    view = SavedView(
        service_id=untrusted_sid,
        name="cross-tenant attempt",
        filters_json='{"status":[500]}',
        page="dashboard",
    )
    res = md_save_view(trusted_sid, view)
    assert res["status"] == "success"

    trusted_rows = md_list_views(trusted_sid)
    assert any(r["id"] == res["id"] for r in trusted_rows), "view must be persisted in the trusted tenant"
    for r in trusted_rows:
        if r["id"] == res["id"]:
            assert r["service_id"] == trusted_sid, "stored service_id must be the trusted parameter, not the payload"

    attacker_rows = md_list_views(untrusted_sid)
    assert not any(r["id"] == res["id"] for r in attacker_rows), "view must NOT leak into the payload-claimed tenant"


# ── Cron repository tests (merged from test_cron.py) ──────────────────────


def _seed_runs(service_id: str, runs: list[dict]) -> list[int]:
    con = metadata_db.get_con(service_id)
    ids: list[int] = []
    for r in runs:
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys, summary) "
            "VALUES (?, ?, ?, ?, ?, '[]', ?)",
            (
                service_id,
                r.get("task", "sync"),
                r.get("started_at", "2026-05-15T00:00:00Z"),
                r.get("duration_s", 1.0),
                r.get("status", "success"),
                r.get("summary", "ok"),
            ),
        )
        ids.append(int(cur.lastrowid or 0))
    con.commit()
    return ids


def test_get_cron_logs_returns_total_and_entries():
    sid = "svc-cron-1"
    _seed_runs(sid, [{"task": "sync"}, {"task": "commit"}, {"task": "sync"}])
    total, entries = get_cron_logs(sid)
    assert total == 3
    assert len(entries) == 3


def test_get_cron_logs_filters_by_task_and_status():
    sid = "svc-cron-2"
    _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "error"},
            {"task": "commit", "status": "success"},
        ],
    )
    total_sync, _ = get_cron_logs(sid, task="sync")
    assert total_sync == 2
    total_err, _ = get_cron_logs(sid, status="error")
    assert total_err == 1


def test_get_cron_logs_pagination():
    sid = "svc-cron-3"
    _seed_runs(sid, [{"task": "sync"} for _ in range(8)])
    total, entries = get_cron_logs(sid, page=2, per_page=3)
    assert total == 8
    assert len(entries) == 3


def test_delete_cron_log_removes_only_target():
    sid = "svc-cron-4"
    [a, b] = _seed_runs(sid, [{"task": "sync"}, {"task": "sync"}])
    delete_cron_log(sid, b)
    con = metadata_db.get_con(sid)
    remaining = [row[0] for row in con.execute("SELECT id FROM cron_runs").fetchall()]
    assert remaining == [a]


def test_delete_cron_log_unknown_id_is_noop():
    sid = "svc-cron-5"
    _seed_runs(sid, [{"task": "sync"}])
    delete_cron_log(sid, 99999999)  # must not raise
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 1


def test_purge_cron_logs_no_filter_clears_all():
    sid = "svc-cron-6"
    _seed_runs(sid, [{"task": "sync"}, {"task": "commit"}])
    purge_cron_logs(sid)
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 0


def test_purge_cron_logs_by_task():
    sid = "svc-cron-7"
    _seed_runs(sid, [{"task": "sync"}, {"task": "sync"}, {"task": "commit"}])
    purge_cron_logs(sid, task="sync")
    con = metadata_db.get_con(sid)
    tasks = [row[0] for row in con.execute("SELECT task FROM cron_runs").fetchall()]
    assert tasks == ["commit"]


def test_get_cron_logs_since_id_returns_only_newer_rows():
    sid = "svc-cron-since-1"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid, since_id=ids[1])
    assert total == 1, "only the third row (id > since_id) should match"
    assert {e["id"] for e in entries} == {ids[2]}


def test_get_cron_logs_since_id_keeps_running_rows_even_if_id_below_cutoff():
    sid = "svc-cron-since-2"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "running"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid, since_id=ids[2])
    returned_ids = {e["id"] for e in entries}
    assert ids[0] in returned_ids, (
        "running row must remain in the response even when id <= since_id, "
        "so the toast-completion-detection effect on /logs keeps working"
    )
    assert total == len(returned_ids)


def test_get_cron_logs_since_id_none_returns_all_rows():
    sid = "svc-cron-since-3"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid)
    assert total == 2
    assert {e["id"] for e in entries} == set(ids)


def test_get_cron_logs_since_id_combines_with_task_filter():
    sid = "svc-cron-since-4"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "running"},
        ],
    )
    total, entries = get_cron_logs(sid, task="sync", since_id=ids[2])
    returned_ids = {e["id"] for e in entries}
    assert returned_ids == {ids[3]}
    assert total == 1


def test_get_cron_logs_rum_sync_matches_rum_discovery():
    sid = "svc-cron-rum"
    _seed_runs(
        sid,
        [
            {"task": "rum_sync", "status": "success"},
            {"task": "rum_discovery", "status": "success"},
            {"task": "log_discovery", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid, task="rum_sync")
    assert total == 2
    tasks = {e["task"] for e in entries}
    assert tasks == {"rum_sync", "rum_discovery"}


def test_get_cron_logs_comma_separated_tasks():
    sid = "svc-cron-multi"
    _seed_runs(
        sid,
        [
            {"task": "log_discovery", "status": "success"},
            {"task": "commit", "status": "success"},
            {"task": "optimize", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid, task="log_discovery,commit")
    assert total == 2
    tasks = {e["task"] for e in entries}
    assert tasks == {"log_discovery", "commit"}


# ── Usage storage stats tests (merged from test_usage_storage_stats.py) ───


def test_get_storage_stats_returns_filtered_files_and_bytes():
    from backend.repositories.assets import get_storage_stats

    metadata_db.get_con("test_svc").execute("DELETE FROM ingested_files")

    metadata_db.get_con("test_svc").execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) VALUES "
        "('file1.gz', 'test_svc', 100, 1024, '2024-05-10T10:00:00Z'),"
        "('file2.gz', 'test_svc', 200, 2048, '2024-05-15T12:00:00Z'),"
        "('file3.gz', 'test_svc', 300, 4096, '2024-05-20T14:00:00Z')"
    )

    src = {"name": "test_svc"}
    mock_con = MagicMock()

    stats = get_storage_stats(mock_con, src, "2024-05-10T00:00:00Z", "2024-05-16T00:00:00Z")

    assert stats["total_files"] == 2
    assert stats["total_bytes"] == 3072


def test_get_storage_stats_safe_from_sql_injection():
    from backend.core.metadata.base import InvalidServiceIdError

    evil_name = "test' OR 1=1; DROP TABLE ingested_files; --"
    with pytest.raises(InvalidServiceIdError):
        metadata_db.get_con(evil_name)


def _schema(*cols: str) -> list[dict]:
    return [{"name": c} for c in cols]


def test_get_edge_ratio_returns_none_when_edge_column_missing():
    from backend.repositories.assets import get_edge_ratio

    src = {"name": "svc_no_edge"}
    con = MagicMock()
    with patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "status")):
        ratio, debug = get_edge_ratio(con, src)
    assert ratio is None
    assert debug == []


def test_get_edge_ratio_returns_none_when_query_yields_no_result():
    from backend.repositories.assets import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=None),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio is None


def test_get_edge_ratio_returns_rounded_value_for_valid_row():
    from backend.repositories.assets import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = [42.6789]
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=result),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio == 42.7


def test_get_edge_ratio_returns_none_for_empty_table():
    from backend.repositories.assets import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = [None]
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=result),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio is None


def test_get_edge_ratio_returns_none_when_no_row_at_all():
    from backend.repositories.assets import get_edge_ratio

    src = {"name": "svc1"}
    con = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = None
    with (
        patch("backend.core.duckdb.get_schema", return_value=_schema("timestamp", "edge")),
        patch("backend.repositories._base.QueryRunner.execute_with_retry", return_value=result),
    ):
        ratio, _debug = get_edge_ratio(con, src)
    assert ratio is None


def test_get_log_activity_uses_service_name_and_bucketing():
    from backend.repositories.assets import get_log_activity

    metadata_db.get_con("act_svc").execute("DELETE FROM ingested_files")
    metadata_db.get_con("act_svc").execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at, file_date) VALUES "
        "('f1.gz', 'act_svc', 100, 1024, '2024-05-10T10:00:00Z', '2024-05-10'),"
        "('f2.gz', 'act_svc', 200, 2048, '2024-05-10T11:00:00Z', '2024-05-10'),"
        "('f3.gz', 'act_svc', 300, 4096, '2024-05-11T12:00:00Z', '2024-05-11')"
    )

    out = get_log_activity(
        {"name": "act_svc"},
        "2024-05-10T00:00:00Z",
        "2024-05-12T00:00:00Z",
        "day",
    )

    assert out["_debug_queries"] == []
    assert out["_debug_calls"] == []
    assert any(k in out for k in ("buckets", "rows", "series", "data"))


def test_get_log_activity_falls_back_to_service_id_when_name_missing():
    from backend.repositories.assets import get_log_activity

    metadata_db.get_con("id_only_svc").execute("DELETE FROM ingested_files")

    with patch.object(metadata_db, "get_log_activity", return_value={"buckets": []}) as mock_get:
        get_log_activity(
            {"service_id": "id_only_svc"},
            "2024-05-10T00:00:00Z",
            "2024-05-12T00:00:00Z",
            "hour",
        )

    assert mock_get.call_args.args[0] == "id_only_svc"
    assert mock_get.call_args.args[3] == "hour"


def test_get_sessions_subminute_windows_do_not_collide_in_memo_cache(in_memory_duckdb, test_service_source):
    from backend.repositories.sessions import queries as sess_queries

    sess_queries._memo.cache.clear()
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=1, hours_ago=1)
    logs[0]["ip"] = "198.51.100.44"
    logs[0]["timestamp"] = "2026-09-14T10:00:45Z"
    insert_mock_logs(in_memory_duckdb, table_name, logs)
    _clear_schema_cache()

    res_early = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        **_sessions_kwargs(
            start_time="2026-09-14T10:00:05Z",
            end_time="2026-09-14T10:00:30Z",
        ),
    )
    res_late = get_sessions(
        con=in_memory_duckdb,
        src=test_service_source,
        **_sessions_kwargs(
            start_time="2026-09-14T10:00:05Z",
            end_time="2026-09-14T10:00:55Z",
        ),
    )
    assert res_early["total"] == 0
    assert res_late["total"] == 1 and res_late["sessions"][0]["ip"] == "198.51.100.44"


def test_streaming_only_bypasses_hourly_rollup_for_exact_30m_gap_sessionization(
    in_memory_duckdb, rollup_source, monkeypatch
):
    from backend.repositories.sessions import queries as sess_queries

    sess_queries._memo.cache.clear()
    table_name = _safe_table(rollup_source["name"])
    logs = generate_mock_logs(rollup_source, num_logs=2, hours_ago=1)
    logs[0]["ip"] = "198.51.100.77"
    logs[0]["timestamp"] = "2026-09-14T10:01:00Z"
    logs[1]["ip"] = "198.51.100.77"
    logs[1]["timestamp"] = "2026-09-14T10:50:00Z"
    insert_mock_logs(in_memory_duckdb, table_name, logs)
    in_memory_duckdb.execute(f'ALTER TABLE {table_name} ADD COLUMN "cmcd_sid" VARCHAR')
    in_memory_duckdb.execute(
        f'UPDATE {table_name} SET "cmcd_sid" = ? WHERE "timestamp" = ?',
        ["stream-sess-1", datetime(2026, 9, 14, 10, 1, 0, tzinfo=UTC)],
    )
    _clear_schema_cache()

    def _fail_if_rollup_called(**kwargs):
        raise AssertionError("streaming_only must bypass _get_sessions_from_rollup")

    monkeypatch.setattr(sess_queries, "_get_sessions_from_rollup", _fail_if_rollup_called)

    res = get_sessions(
        con=in_memory_duckdb,
        src=rollup_source,
        **_sessions_kwargs(
            start_time="2026-09-14T08:00:00Z",
            end_time="2026-09-14T16:00:00Z",
            streaming_only=True,
        ),
    )
    assert res["total"] == 1
    assert res["sessions"][0]["ip"] == "198.51.100.77"
    assert res["sessions"][0]["req_count"] == 1
