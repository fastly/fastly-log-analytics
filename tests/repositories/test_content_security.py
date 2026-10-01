"""Unit tests for backend.repositories.content_security."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table
from backend.repositories.content_security import _response_cache, get_content_security

_NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
_START = (_NOW - timedelta(hours=1)).isoformat()
_END = _NOW.isoformat()

_ALL_COLS = {
    "timestamp": "TIMESTAMPTZ",
    "ip": "VARCHAR",
    "country": "VARCHAR",
    "referer": "VARCHAR",
    "host": "VARCHAR",
    "resp_bytes": "UBIGINT",
    "edge": "BOOLEAN",
    "cmcd_cid": "VARCHAR",
}


@pytest.fixture(autouse=True)
def _clear_cache():
    _response_cache.clear()
    yield
    _response_cache.clear()


def _make_table(con, src, rows, cols=None):
    cols = cols or list(_ALL_COLS)
    table = _safe_table(src["name"])
    con.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {_ALL_COLS[c]}' for c in cols)})")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
            [r.get(c) for c in cols],
        )


def _row(minutes_ago, *, edge=True, country="US", referer="", host="", cid="movie-1", nbytes=1000):
    return {
        "timestamp": _NOW - timedelta(minutes=minutes_ago),
        "ip": "203.0.113.5",
        "country": country,
        "referer": referer,
        "host": host,
        "resp_bytes": nbytes,
        "edge": edge,
        "cmcd_cid": cid,
    }


def _fixture_rows():
    return [
        # movie-1: three edge requests from US via pirate.example, one shield fetch.
        _row(10, host="pirate.example", referer="https://pirate.example/watch", nbytes=1000),
        _row(10, host="pirate.example", referer="https://pirate.example/watch", nbytes=1000),
        _row(40, host="pirate.example", referer="https://pirate.example/watch", nbytes=1000),
        _row(40, edge=False, host="pirate.example", referer="https://pirate.example/watch", nbytes=500),
        # movie-2: two edge requests from DE via the legit player.
        _row(20, country="DE", host="player.example", cid="movie-2", nbytes=2000),
        _row(20, country="DE", host="player.example", cid="movie-2", nbytes=2000),
    ]


def _call(con, src, **kw):
    args = {"start_time": _START, "end_time": _END, "filters": {}, "bucket_seconds": 1800, "top_n": 10}
    args.update(kw)
    return get_content_security(con=con, src=src, **args)


def test_leaderboards_count_edge_lines_only(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source)

    assert res["available"] is True
    # The shield line (edge=False) is the same client request seen twice — not counted.
    assert res["top_countries"] == [
        {"value": "US", "requests": 3, "bytes": 3000},
        {"value": "DE", "requests": 2, "bytes": 4000},
    ]
    assert res["top_hosts"] == [
        {"value": "pirate.example", "requests": 3, "bytes": 3000},
        {"value": "player.example", "requests": 2, "bytes": 4000},
    ]
    # Empty referers are excluded rather than reported as a blank row.
    assert res["top_referers"] == [{"value": "https://pirate.example/watch", "requests": 3, "bytes": 3000}]


def test_bandwidth_splits_edge_and_shield(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source)

    assert res["has_shield_split"] is True
    ts = res["bandwidth_ts"]
    assert sum(p["edge_bytes"] for p in ts) == 7000
    assert sum(p["shield_bytes"] for p in ts) == 500
    # Padded across the whole window: 11:00, 11:30 and 12:00 buckets.
    assert [p["bucket"] for p in ts] == ["2026-09-01 11:00:00", "2026-09-01 11:30:00", "2026-09-01 12:00:00"]


def test_content_id_narrows_metrics_but_not_picker(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source, content_id="movie-2")

    assert res["content_id"] == "movie-2"
    assert res["top_countries"] == [{"value": "DE", "requests": 2, "bytes": 4000}]
    assert res["top_referers"] == []
    assert sum(p["edge_bytes"] for p in res["bandwidth_ts"]) == 4000
    # The picker always lists every content id so the user can switch.
    assert res["content_ids"] == [
        {"content_id": "movie-1", "requests": 3},
        {"content_id": "movie-2", "requests": 2},
    ]


def test_content_id_is_escaped(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source, content_id="x' OR '1'='1")

    assert res["available"] is True
    assert res["top_countries"] == []


def test_global_filters_still_apply(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(
        in_memory_duckdb,
        test_service_source,
        filters={"country": FilterSpec(mode="include", values=["DE"])},
    )

    assert [r["value"] for r in res["top_countries"]] == ["DE"]
    assert [c["content_id"] for c in res["content_ids"]] == ["movie-2"]


def test_missing_optional_columns_degrade(in_memory_duckdb, test_service_source):
    """A service without host or CMCD logging still gets the rest."""
    cols = ["timestamp", "ip", "country", "referer", "resp_bytes"]
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows(), cols=cols)
    res = _call(in_memory_duckdb, test_service_source)

    assert res["available"] is True
    assert res["fields"]["host"] is False
    assert res["fields"]["cmcd_cid"] is False
    assert res["top_hosts"] == []
    assert res["content_ids"] == []
    # Without the edge flag every line counts, shield included, and there is no split.
    assert res["top_countries"][0] == {"value": "US", "requests": 4, "bytes": 3500}
    assert res["has_shield_split"] is False
    assert all(p["shield_bytes"] is None for p in res["bandwidth_ts"])


def test_response_cache_hit(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    cold = _call(in_memory_duckdb, test_service_source, content_id="movie-1")
    warm = _call(in_memory_duckdb, test_service_source, content_id="movie-1")
    other = _call(in_memory_duckdb, test_service_source, content_id="movie-2")

    assert cold.get("is_cached") is not True
    assert warm.get("is_cached") is True
    assert warm["top_hosts"] == cold["top_hosts"]
    assert other.get("is_cached") is not True
