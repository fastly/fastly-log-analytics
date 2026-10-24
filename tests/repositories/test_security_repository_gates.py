from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.repositories import security
from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED
from backend.repositories.security import get_security_aggregates

# ── From test_security_branches.py ──

"""Tests for narrow defensive branches in backend.repositories.security.

The rest of the repository's test coverage lives in test_security.py and
heavily exercises the rollup-routing decisions on the happy paths. This
file targets the parse-failure / OSError / empty-input branches that
guard those rollup paths from blowing up at runtime."""


@pytest.mark.parametrize(
    "start,end",
    [
        (None, "2026-01-04T00:00:00Z"),
        ("2026-01-01T00:00:00Z", None),
        (None, None),
        ("", "2026-01-04T00:00:00Z"),
    ],
)
def test_window_eligible_for_rollup_returns_false_when_bounds_missing(start, end):
    """Open-ended requests can't be rollup-served — they fall through to
    live SQL via the False return."""
    assert security._window_eligible_for_rollup(start, end) is False


def test_window_eligible_for_rollup_returns_false_when_parse_raises():
    """parse_iso_utc may raise on garbage input; the wrapper must catch
    rather than let the exception bubble into the request handler."""
    with patch("backend.utils.date_utils.parse_iso_utc", side_effect=ValueError("bad iso")):
        assert security._window_eligible_for_rollup("bad", "alsobad") is False


def test_window_eligible_for_rollup_returns_false_when_parse_yields_none():
    """parse_iso_utc returns None (not raises) on some malformed inputs.
    The None-guard must trip so the function returns False instead of
    crashing on the subtraction below."""
    with patch("backend.utils.date_utils.parse_iso_utc", return_value=None):
        assert security._window_eligible_for_rollup("x", "y") is False


def test_window_eligible_for_rollup_returns_false_when_end_before_start():
    """A reversed window is rejected — defends against a buggy caller
    that would otherwise produce a negative timedelta."""
    assert security._window_eligible_for_rollup("2026-01-10T00:00:00Z", "2026-01-01T00:00:00Z") is False


def test_window_eligible_for_rollup_returns_true_for_window_at_3d_threshold():
    """24h and wider windows qualify (>= _ROLLUP_MIN_WINDOW_SECONDS = 86400)."""
    assert security._window_eligible_for_rollup("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z") is True
    assert security._window_eligible_for_rollup("2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is True


def test_window_eligible_for_rollup_returns_false_below_3d_threshold():
    """Just under 24h disqualifies — the live path is cheaper there."""
    assert security._window_eligible_for_rollup("2026-01-01T00:00:00Z", "2026-01-01T23:00:00Z") is False


def test_has_rollup_coverage_returns_false_for_unsafe_ident_field(tmp_path):
    """The field name flows into a SQL identifier; an unsafe value
    short-circuits to False before any filesystem work."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    # A space breaks _is_safe_ident.
    assert security._has_rollup_coverage(src, "bad field", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is False


def test_has_rollup_coverage_returns_false_when_bounds_missing(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    assert security._has_rollup_coverage(src, "is_ipv6", None, "2026-01-04T00:00:00Z") is False
    assert security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", None) is False


def test_has_rollup_coverage_returns_false_when_parse_returns_none(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with patch("backend.utils.date_utils.parse_iso_utc", return_value=None):
        assert security._has_rollup_coverage(src, "is_ipv6", "x", "y") is False


def test_has_rollup_coverage_swallows_oserror_on_bundled_listdir(tmp_path):
    """OSError on the bundled directory's listdir is caught — the
    function falls through to the per-field tree (which is also empty
    here) and returns False rather than propagating."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    bundled = tmp_path / "rollups" / "hour_bundled"
    bundled.mkdir(parents=True)

    real_listdir = os.listdir

    def _listdir(p):
        if str(p) == str(bundled):
            raise OSError("simulated")
        return real_listdir(p)

    with patch("os.listdir", side_effect=_listdir):
        out = security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is False


def test_has_rollup_coverage_swallows_oserror_on_hour_dir_listdir(tmp_path):
    """OSError on an INNER (per-field hour-dir) listdir is caught with
    ``continue`` — the outer loop keeps walking."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    per_field = tmp_path / "rollups" / "hour" / "field=is_ipv6"
    hour_dir = per_field / "hour=2026-01-02-12"
    hour_dir.mkdir(parents=True)

    real_listdir = os.listdir

    def _listdir(p):
        if str(p) == str(hour_dir):
            raise OSError("simulated inner")
        return real_listdir(p)

    with patch("os.listdir", side_effect=_listdir):
        out = security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is False


@contextmanager
def _runner_ctx():
    con = duckdb.connect(":memory:")
    yield QueryRunner(con, {"name": "svc"})
    con.close()


def test_ipv6_per_hour_from_rollups_returns_none_when_bounds_missing(tmp_path):
    with _runner_ctx() as runner:
        assert (
            security._ipv6_per_hour_from_rollups(
                runner, {"name": "svc", "_cache_dir_override": str(tmp_path)}, None, "2026-01-04T00:00:00Z"
            )
            is None
        )
        assert (
            security._ipv6_per_hour_from_rollups(
                runner, {"name": "svc", "_cache_dir_override": str(tmp_path)}, "2026-01-01T00:00:00Z", None
            )
            is None
        )


def test_ipv6_per_hour_from_rollups_returns_none_when_parse_raises(tmp_path):
    with _runner_ctx() as runner:
        src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
        with patch("backend.utils.date_utils.parse_iso_utc", side_effect=ValueError("bad iso")):
            assert security._ipv6_per_hour_from_rollups(runner, src, "bad", "alsobad") is None


def test_ipv6_per_hour_from_rollups_returns_none_when_no_in_window_paths(tmp_path):
    """No bundled or per-field parquets exist on disk → no closed-hour
    data, return None (caller falls back to live SQL)."""
    with _runner_ctx() as runner:
        src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
        out = security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_ipv6_per_hour_from_rollups_returns_none_when_query_raises(tmp_path):
    """A real read_parquet error is caught; the function returns None
    so the caller falls back instead of failing the whole card."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    bundled = tmp_path / "rollups" / "hour_bundled" / "hour=2026-01-02-12"
    bundled.mkdir(parents=True)
    # Write a SENTINEL file (not a real parquet) so listdir finds it,
    # but read_parquet will throw a duckdb.IOException trying to parse it.
    (bundled / "all_fields.parquet").write_bytes(b"not a parquet")

    with _runner_ctx() as runner:
        out = security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_returns_none_when_execute_raises():
    """When execute_top_n_rollups raises (cold pool, permission error,
    etc.), the function returns None so _build_security_response falls
    back to live SQL."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.side_effect = RuntimeError("rollup pool unavailable")
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_returns_none_for_empty_rolled():
    """Reader returned no rows (cold service or no p_type values) → None."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.return_value = ([], None)
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_filters_other_and_empty_and_nones():
    """The reader may return synthetic ``__other__`` rows, empty strings,
    and DuckDB NULLs. All three must be filtered out. If after filtering
    every row was excluded, return None."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.return_value = (
        [
            ("p_type", "__other__", 100),
            ("p_type", "", 50),
            ("p_type", None, 25),
        ],
        None,
    )
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_aggregates_and_sorts_by_count_desc():
    """Valid p_type rows are summed by value and sorted by count
    descending. The aggregate is exact across hours (no approximation)."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.return_value = (
        [
            ("p_type", "datacenter", 100),
            ("p_type", "residential", 250),
            ("p_type", "datacenter", 50),  # additional hour for datacenter
            ("p_type", "__other__", 999),  # filtered
            ("p_type", "vpn", 75),
        ],
        None,
    )
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out == [
        {"type": "residential", "count": 250},
        {"type": "datacenter", "count": 150},
        {"type": "vpn", "count": 75},
    ]


# ── From test_security_ngwaf.py ──

_WAF_REQ_ID = "05daf2b7aedc405da50c000000000001"


_BOT_NAME = "OpenAI SearchBot"


_CATEGORY = "AI-FETCHER"


_WK_NAME = "OpenAI SearchBot crawler"


def _create_log_table_with_waf(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Create a minimal log table that includes waf_req_id."""
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp   TIMESTAMPTZ,
            ip          VARCHAR,
            ua          VARCHAR,
            waf_req_id  VARCHAR,
            waf_sig     VARCHAR
        )
    """)
    now = datetime.now(UTC)
    con.execute(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
        [now, "1.2.3.4", "GPTBot/1.0", _WAF_REQ_ID, "VERIFIED-BOT,VERIFIED-BOT.AI-FETCHER"],
    )
    # A second row without a matching waf_req_id (no bot cache entry)
    con.execute(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
        [now, "5.6.7.8", "curl/7.0", "deadbeef" * 4, None],
    )


def _create_cache_db(db_path: str) -> None:
    """Populate a minimal ngwaf_bot_cache SQLite file."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ngwaf_bots (
            waf_req_id         TEXT PRIMARY KEY,
            bot_name           TEXT,
            category           TEXT,
            wellknown_bot_id   TEXT,
            wellknown_bot_name TEXT,
            synced_at          TEXT
        )
    """)
    con.execute(
        "INSERT INTO ngwaf_bots VALUES (?, ?, ?, ?, ?, ?)",
        (
            _WAF_REQ_ID,
            _BOT_NAME,
            _CATEGORY,
            "openai-searchbot",
            _WK_NAME,
            datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
    )
    con.commit()
    con.close()


@pytest.fixture
def con_with_waf_logs():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    yield con
    con.close()


def test_ngwaf_verified_bots_populated_from_cache(con_with_waf_logs, tmp_path):
    """ngwaf_verified_bots is non-empty when the cache has a matching waf_req_id."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    table_name = _safe_table("ngwaf_test_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_test_svc", "service_id": "ngwaf-test"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    bots = result.get("ngwaf_verified_bots", [])
    assert len(bots) == 1
    assert bots[0]["bot_name"] == _BOT_NAME
    assert bots[0]["wellknown_bot_name"] == _WK_NAME
    assert bots[0]["category"] == _CATEGORY
    assert bots[0]["request_count"] == 1


def test_ngwaf_verified_bots_ts_populated_from_cache(con_with_waf_logs, tmp_path):
    """ngwaf_verified_bots_ts contains time-bucketed counts for each bot_name."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    table_name = _safe_table("ngwaf_ts_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_ts_svc", "service_id": "ngwaf-ts"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    ts = result.get("ngwaf_verified_bots_ts", [])
    assert len(ts) >= 1
    assert ts[0]["bot_name"] == _BOT_NAME
    assert ts[0]["count"] == 1


def test_ngwaf_fields_empty_when_cache_file_missing(con_with_waf_logs, tmp_path):
    """Both NGWAF fields must be empty lists when ngwaf_bot_cache.db doesn't exist."""
    missing_path = str(tmp_path / "does_not_exist.db")

    table_name = _safe_table("ngwaf_nocache_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_nocache_svc", "service_id": "ngwaf-nocache"}

    with patch("backend.config.ngwaf_db_path", return_value=missing_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []


def test_ngwaf_fields_empty_when_waf_req_id_column_absent(tmp_path):
    """Both NGWAF fields must be empty lists when waf_req_id is not in the log schema."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL sqlite; LOAD sqlite;")
        table_name = _safe_table("ngwaf_nowaf_svc")
        # Table without waf_req_id column
        con.execute(f"""
            CREATE TABLE {table_name} (
                timestamp TIMESTAMPTZ,
                ip        VARCHAR,
                ua        VARCHAR
            )
        """)
        now = datetime.now(UTC)
        con.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?)", [now, "1.2.3.4", "curl/7.0"])

        src = {"name": "ngwaf_nowaf_svc", "service_id": "ngwaf-nowaf"}

        with patch("backend.config.ngwaf_db_path", return_value=db_path):
            result = get_security_aggregates(
                con=con,
                src=src,
                start_time=None,
                end_time=None,
                filters={},
            )
    finally:
        con.close()

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []


def test_ngwaf_only_request_narrows_temp_to_waf_req_id(con_with_waf_logs, tmp_path):
    """When the ONLY live sections are the NGWAF bot pair, the catalog temp
    must collapse to {timestamp, waf_req_id} filtered to ``waf_req_id IS NOT
    NULL`` — the tiny NGWAF-flagged subset rather than a full-window scan.
    Both bot templates INNER JOIN on a non-null waf_req_id, so this is
    semantically identical and is the residual-temp half of the P0 win.
    """
    from datetime import timedelta

    from backend.repositories._base import QueryRunner

    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    table_name = _safe_table("ngwaf_narrow_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_narrow_svc", "service_id": "ngwaf-narrow"}

    create_sqls: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql and table_name in sql:
            create_sqls.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    now = datetime.now(UTC)
    start = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    with patch("backend.config.ngwaf_db_path", return_value=db_path), patch.object(QueryRunner, "execute", _spy):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=start,
            end_time=end,
            filters={},
            sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
        )

    # The narrowing marker fired and the temp carries ONLY the two needed
    # columns + the NGWAF row filter.
    markers = {e["section"] for e in result["section_timings"]}
    assert "security:temp_ngwaf_narrowed" in markers, f"narrowing marker missing; timings={markers}"
    assert create_sqls, "no catalog temp captured"
    catalog_sql = create_sqls[-1]
    assert "waf_req_id IS NOT NULL" in catalog_sql, f"NGWAF row filter not applied: {catalog_sql}"
    assert '"waf_req_id"' in catalog_sql and '"timestamp"' in catalog_sql
    for absent in ('"ua"', '"ip"', '"tls_ciphers_sha"', '"req_header_bytes"', '"conn_requests"', '"waf_sig"'):
        assert absent not in catalog_sql, f"{absent} leaked into NGWAF-only temp: {catalog_sql}"

    # Result still correct: the one cache-matched waf_req_id resolves.
    assert len(result["ngwaf_verified_bots"]) == 1
    assert result["ngwaf_verified_bots"][0]["bot_name"] == _BOT_NAME


def test_ngwaf_unmatched_waf_req_ids_not_in_result(con_with_waf_logs, tmp_path):
    """Rows whose waf_req_id is not in the cache produce no bot entries (INNER JOIN)."""
    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    # Create an empty cache (no rows)
    _con = sqlite3.connect(db_path)
    _con.execute(
        "CREATE TABLE ngwaf_bots (waf_req_id TEXT PRIMARY KEY, bot_name TEXT, "
        "category TEXT, wellknown_bot_id TEXT, wellknown_bot_name TEXT, synced_at TEXT)"
    )
    _con.commit()
    _con.close()

    table_name = _safe_table("ngwaf_nomatch_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_nomatch_svc", "service_id": "ngwaf-nomatch"}

    with patch("backend.config.ngwaf_db_path", return_value=db_path):
        result = get_security_aggregates(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    assert result["ngwaf_verified_bots"] == []
    assert result["ngwaf_verified_bots_ts"] == []


def test_top_bots_ngwaf_only_skips_temp_and_joins_direct(con_with_waf_logs, tmp_path):
    """When the UA branch is rollup-served and NGWAF is attached, the join is
    the temp's only would-be consumer — get_top_bots must materialize NO temp
    and run NGWAF_TOP_BOTS_JOIN_DIRECT against the base table, returning the
    same bot rows the temp path produced."""
    from unittest.mock import patch as _patch

    from backend.repositories._base import QueryRunner
    from backend.repositories.security import get_top_bots

    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)

    table_name = _safe_table("ngwaf_direct_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)

    src = {"name": "ngwaf_direct_svc", "service_id": "ngwaf-direct"}

    temp_creates: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "CREATE TEMP TABLE" in sql:
            temp_creates.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    with (
        _patch("backend.config.ngwaf_db_path", return_value=db_path),
        # UA branch rollup-served → needs_filtered_ua_scan is False.
        _patch.object(
            QueryRunner,
            "execute_top_n_rollups",
            lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("ua", "GPTBot/1.0", 5)], ["ua"]),
        ),
        _patch.object(QueryRunner, "execute", _spy),
    ):
        result = get_top_bots(
            con=con_with_waf_logs,
            src=src,
            start_time=None,
            end_time=None,
            filters={},
        )

    assert temp_creates == [], f"ngwaf-only top_bots must not materialize a temp: {temp_creates}"
    markers = {e["section"] for e in result["section_timings"]}
    assert "top_bots:ngwaf_join_direct" in markers, f"direct-join marker missing; timings={markers}"
    assert "top_bots:temp_table_create" not in markers, f"temp path fired; timings={markers}"
    assert result["ngwaf_bots"] == [{"name": _BOT_NAME, "category": _CATEGORY, "request_count": 1}]


def test_top_bots_ngwaf_rollup_served_skips_direct_join(con_with_waf_logs, tmp_path):
    """When try_ngwaf_top_bots_from_rollup returns rows, the direct join must
    not run at all — the panel serves fully from the rollup."""
    from unittest.mock import patch as _patch

    from backend.repositories._base import QueryRunner
    from backend.repositories.security import get_top_bots

    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)
    table_name = _safe_table("ngwaf_rollup_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_rollup_svc", "service_id": "ngwaf-rollup"}

    sentinel = [{"name": "GPTBot", "category": "AI-CRAWLER", "request_count": 42}]
    join_queries: list[str] = []
    orig_execute = QueryRunner.execute

    def _spy(self, sql, *args, **kwargs):
        if isinstance(sql, str) and "ngwaf_top.ngwaf_bots" in sql:
            join_queries.append(sql)
        return orig_execute(self, sql, *args, **kwargs)

    with (
        _patch("backend.config.ngwaf_db_path", return_value=db_path),
        _patch.object(
            QueryRunner,
            "execute_top_n_rollups",
            lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("ua", "GPTBot/1.0", 5)], ["ua"]),
        ),
        _patch.object(
            QueryRunner,
            "try_ngwaf_top_bots_from_rollup",
            lambda self, s, e, *, has_filters, n: list(sentinel),
        ),
        _patch.object(QueryRunner, "execute", _spy),
    ):
        result = get_top_bots(con=con_with_waf_logs, src=src, start_time=None, end_time=None, filters={})

    assert result["ngwaf_bots"] == sentinel
    assert join_queries == [], f"direct join ran despite rollup hit: {join_queries}"
    markers = {e["section"] for e in result["section_timings"]}
    assert "top_bots:ngwaf_rollup" in markers
    assert "top_bots:ngwaf_join_direct" not in markers


def test_top_bots_ngwaf_rollup_miss_falls_back_to_direct_join(con_with_waf_logs, tmp_path):
    """Reader returns None (no coverage / short window) → the direct join
    still serves the panel exactly as before."""
    from unittest.mock import patch as _patch

    from backend.repositories._base import QueryRunner
    from backend.repositories.security import get_top_bots

    db_path = str(tmp_path / "ngwaf_bot_cache.db")
    _create_cache_db(db_path)
    table_name = _safe_table("ngwaf_rollup_miss_svc")
    _create_log_table_with_waf(con_with_waf_logs, table_name)
    src = {"name": "ngwaf_rollup_miss_svc", "service_id": "ngwaf-rollup-miss"}

    with (
        _patch("backend.config.ngwaf_db_path", return_value=db_path),
        _patch.object(
            QueryRunner,
            "execute_top_n_rollups",
            lambda self, fields, s, e, limit=10, per_field_limits=None, **kw: ([("ua", "GPTBot/1.0", 5)], ["ua"]),
        ),
        _patch.object(
            QueryRunner,
            "try_ngwaf_top_bots_from_rollup",
            lambda self, s, e, *, has_filters, n: None,
        ),
    ):
        result = get_top_bots(con=con_with_waf_logs, src=src, start_time=None, end_time=None, filters={})

    assert result["ngwaf_bots"] == [{"name": _BOT_NAME, "category": _CATEGORY, "request_count": 1}]
    markers = {e["section"] for e in result["section_timings"]}
    assert "top_bots:ngwaf_join_direct" in markers


# ── From test_security_proxies_blank_ip.py ──


def test_blank_ip_rows_are_not_suspicious_clients_or_counted_as_proxies():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, lat DOUBLE, lon DOUBLE, "
        "asn BIGINT, country VARCHAR, city VARCHAR)"
    )
    # A real tunnel client and a blank-IP redirect with identical RTT shape.
    con.execute("INSERT INTO t VALUES ('10.0.0.2', 'IAD', 1000, 50000, 39.0, -77.1, 200, 'US', 'Reston')")
    con.execute("INSERT INTO t VALUES ('', 'IAD', 1000, 50000, 39.0, -77.1, 700, '', '')")
    rows = con.execute(
        GET_SECURITY_PROXIES_COMBINED.format(
            temp_table="t",
            select_country_city_inner=",\n            country,\n            city",
            select_country_city_outer=",\n                country,\n                city",
        )
    ).fetchall()

    clients = [r[1] for r in rows if r[0] == "client"]
    assert clients == ["10.0.0.2"]
    stats = next(r for r in rows if r[0] == "stats")
    assert stats[2] == 1  # active proxies: the real client only
    assert stats[3] == 1  # flagged requests
    quality = {r[1]: r[2] for r in rows if r[0] == "quality"}
    assert quality == {"Active Tunnel / Proxy": 2}  # traffic quality still counts every request


def test_rollup_reader_drops_blank_ip_tuples(tmp_path):
    """The rollup path serves the default 24h-48h+ windows, so it needs the same filter."""
    from datetime import UTC, datetime, timedelta
    from unittest.mock import patch

    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.repositories._base import QueryRunner

    base = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base + timedelta(hours=i) for i in range(50)]
    flagged = pa.table(
        {
            "ip": ["10.0.0.2", ""],
            "asn": [200, 700],
            "pop": ["IAD", "IAD"],
            "rtt_min": [1000.0, 1000.0],
            "tcp_rtt": [50000.0, 50000.0],
            "lat": [39.0, 39.0],
            "lon": [-77.1, -77.1],
            "country": ["US", ""],
            "city": ["Reston", ""],
            "cnt": pa.array([1, 1], type=pa.int64()),
        }
    )
    quality = pa.table({"type": ["Active Tunnel / Proxy"], "count": pa.array([2], type=pa.int64())})
    for h in hours:
        d = tmp_path / "rollups" / "hour_bundled" / f"hour={h.strftime('%Y-%m-%d-%H')}"
        d.mkdir(parents=True)
        pq.write_table(flagged, str(d / "security_proxy_flagged.parquet"))
        pq.write_table(quality, str(d / "security_proxy_quality.parquet"))

    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-blank-rollup"})
    st = hours[0].isoformat().replace("+00:00", "Z")
    et = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        rolled = runner.try_security_proxies_from_rollup(st, et, has_filters=False)

    assert rolled is not None
    assert [c["ip"] for c in rolled["clients"]] == ["10.0.0.2"]
    assert rolled["stats"]["active_proxies_count"] == 1
