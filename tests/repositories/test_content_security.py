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
    "cs_content_id": "VARCHAR",
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
        "cs_content_id": cid,
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
        {"value": "US", "users": 1, "requests": 3, "bytes": 3000},
        {"value": "DE", "users": 1, "requests": 2, "bytes": 4000},
    ]
    assert res["top_hosts"] == [
        {"value": "pirate.example", "users": 1, "requests": 3, "bytes": 3000},
        {"value": "player.example", "users": 1, "requests": 2, "bytes": 4000},
    ]
    # Empty referers are excluded rather than reported as a blank row.
    assert res["top_referers"] == [{"value": "https://pirate.example/watch", "users": 1, "requests": 3, "bytes": 3000}]


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
    assert res["top_countries"] == [{"value": "DE", "users": 1, "requests": 2, "bytes": 4000}]
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
    assert res["fields"]["cs_content_id"] is False
    assert res["top_hosts"] == []
    assert res["content_ids"] == []
    # Without the edge flag every line counts, shield included, and there is no split.
    assert res["top_countries"][0] == {"value": "US", "users": 1, "requests": 4, "bytes": 3500}
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


def _sub_row(sub, ip, *, minutes_ago=10, edge=True, country="US"):
    return {
        "timestamp": _NOW - timedelta(minutes=minutes_ago),
        "ip": ip,
        "country": country,
        "referer": "",
        "host": "",
        "resp_bytes": 100,
        "edge": edge,
        "cs_content_id": "movie-1",
        "cs_sub_id": sub,
    }


def test_top_subscribers_ranked_by_distinct_ips(in_memory_duckdb, test_service_source):
    _ALL_COLS["cs_sub_id"] = "VARCHAR"
    try:
        rows = [
            # sub-shared: 3 distinct IPs (one repeated) — likely credential sharing.
            _sub_row("sub-shared", "203.0.113.1"),
            _sub_row("sub-shared", "203.0.113.2"),
            _sub_row("sub-shared", "203.0.113.3", country="DE"),
            _sub_row("sub-shared", "203.0.113.3", country="DE", minutes_ago=5),
            # sub-busy: more requests but one IP — must rank below sub-shared.
            *[_sub_row("sub-busy", "198.51.100.7") for _ in range(6)],
            # Shield line from a new IP is the same request seen twice — not counted.
            _sub_row("sub-busy", "198.51.100.8", edge=False),
            # Blank ids are excluded.
            _sub_row("", "192.0.2.1"),
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call(in_memory_duckdb, test_service_source)
    finally:
        _ALL_COLS.pop("cs_sub_id")

    assert res["fields"]["cs_sub_id"] is True
    assert res["top_subscribers"] == [
        {
            "subscriber_id": "sub-shared",
            "distinct_ips": 3,
            "distinct_countries": 2,
            "requests": 4,
            "bytes": 400,
            "last_seen": (_NOW - timedelta(minutes=5)).isoformat(),
        },
        {
            "subscriber_id": "sub-busy",
            "distinct_ips": 1,
            "distinct_countries": 1,
            "requests": 6,
            "bytes": 600,
            "last_seen": (_NOW - timedelta(minutes=10)).isoformat(),
        },
    ]


def test_top_subscribers_empty_without_token_column(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source)

    assert res["fields"]["cs_sub_id"] is False
    assert res["top_subscribers"] == []


def test_leaderboard_users_are_subscriber_ip_pairs_or_ip_alone(in_memory_duckdb, test_service_source):
    """Users = distinct (subscriber id, IP) pairs; a request without a
    subscriber id counts by its IP alone."""
    _ALL_COLS["cs_sub_id"] = "VARCHAR"
    try:
        rows = [
            # US: acct-a on two IPs = 2 users; acct-b on the first IP = 1 more.
            _sub_row("acct-a", "203.0.113.1"),
            _sub_row("acct-a", "203.0.113.1"),
            _sub_row("acct-a", "203.0.113.2"),
            _sub_row("acct-b", "203.0.113.1"),
            # No subscriber id: one user per IP (NULL and '' are the same "none").
            _sub_row("", "198.51.100.9"),
            _sub_row(None, "198.51.100.9"),
            _sub_row("", "198.51.100.10"),
            # Shield line is the same request seen twice — not a user.
            _sub_row("acct-c", "192.0.2.50", edge=False),
            # DE: one subscriber, one IP.
            _sub_row("acct-d", "192.0.2.7", country="DE"),
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call(in_memory_duckdb, test_service_source)
    finally:
        _ALL_COLS.pop("cs_sub_id")

    assert [(r["value"], r["users"], r["requests"]) for r in res["top_countries"]] == [("US", 5, 7), ("DE", 1, 1)]
    # The same definition applies to every leaderboard.
    assert res["top_hosts"] == []  # host is '' on these rows


def _proxy_row(p_desc, ip, *, edge=True, nbytes=1000):
    return {**_row(10, edge=edge, nbytes=nbytes), "ip": ip, "p_desc": p_desc}


def test_proxy_descriptions_list_every_value_uncapped(in_memory_duckdb, test_service_source):
    """Every p_desc value comes back regardless of top_n; non-proxy ('') and
    shield lines are excluded; users/requests/bytes match the leaderboards."""
    _ALL_COLS["p_desc"] = "VARCHAR"
    try:
        rows = [
            _proxy_row("vpn", "203.0.113.1"),
            _proxy_row("vpn", "203.0.113.1"),
            _proxy_row("vpn", "203.0.113.2"),
            _proxy_row("vpn", "203.0.113.3", edge=False, nbytes=500),  # shield: not counted
            _proxy_row("tor-exit", "198.51.100.1", nbytes=4000),
            _proxy_row("apple", "192.0.2.1", nbytes=250),
            _proxy_row("", "192.0.2.99"),  # not a proxy
            _proxy_row(None, "192.0.2.98"),
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call(in_memory_duckdb, test_service_source, top_n=1)
    finally:
        _ALL_COLS.pop("p_desc")

    assert res["fields"]["p_desc"] is True
    assert res["proxy_descriptions"] == [
        {"value": "vpn", "users": 2, "requests": 3, "bytes": 3000},
        {"value": "apple", "users": 1, "requests": 1, "bytes": 250},
        {"value": "tor-exit", "users": 1, "requests": 1, "bytes": 4000},
    ]
    # top_n still caps the leaderboards.
    assert len(res["top_countries"]) == 1


def test_proxy_sections_empty_without_group_i_columns(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source)

    assert res["fields"]["p_type"] is False
    assert res["proxy_types"] == []
    assert res["fields"]["p_desc"] is False
    assert res["proxy_descriptions"] == []


def test_proxy_types_list_every_value_uncapped(in_memory_duckdb, test_service_source):
    """Every p_type value comes back regardless of top_n, independently of
    p_desc; non-proxy ('') and shield lines are excluded."""
    _ALL_COLS["p_type"] = "VARCHAR"
    try:
        rows = [
            {**_row(10), "ip": "203.0.113.1", "p_type": "hosting"},
            {**_row(10), "ip": "203.0.113.2", "p_type": "hosting"},
            {**_row(10, edge=False, nbytes=500), "ip": "203.0.113.3", "p_type": "hosting"},  # shield
            {**_row(10, nbytes=3000), "ip": "198.51.100.1", "p_type": "anonymous"},
            {**_row(10, nbytes=250), "ip": "192.0.2.1", "p_type": "corporate"},
            {**_row(10), "ip": "192.0.2.99", "p_type": ""},  # not a proxy
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call(in_memory_duckdb, test_service_source, top_n=1)
    finally:
        _ALL_COLS.pop("p_type")

    assert res["fields"]["p_type"] is True
    assert res["proxy_types"] == [
        {"value": "hosting", "users": 2, "requests": 2, "bytes": 2000},
        {"value": "anonymous", "users": 1, "requests": 1, "bytes": 3000},
        {"value": "corporate", "users": 1, "requests": 1, "bytes": 250},
    ]
    # No p_desc column on this table: its section degrades on its own.
    assert res["fields"]["p_desc"] is False
    assert res["proxy_descriptions"] == []
