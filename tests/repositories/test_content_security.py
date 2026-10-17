"""Unit tests for backend.repositories.content_security."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table
from backend.repositories.content_security import _response_cache, get_content_security, get_subscriber_ips

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
# Opt-in columns (the tests above add/remove their own on _ALL_COLS).
_ENF_TYPES = {"cs_status": "VARCHAR", "cs_mode": "VARCHAR", "cs_rl_mode": "VARCHAR", "asn": "UINTEGER"}
_POLICY_TYPES = {"cs_policy_id": "VARCHAR", "cs_policy_reason": "VARCHAR"}


@pytest.fixture(autouse=True)
def _clear_cache():
    _response_cache.clear()
    yield
    _response_cache.clear()


def _make_table(con, src, rows, cols=None):
    cols = cols or list(_ALL_COLS)
    table = _safe_table(src["name"])
    con.execute(
        f"CREATE TABLE {table} ({', '.join(f'{c} {({**_ALL_COLS, **_ENF_TYPES, **_POLICY_TYPES})[c]}' for c in cols)})"
    )
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
        _row(40, host="pirate.example", referer="https://Pirate.example:8443/other?x=1", nbytes=1000),
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
    assert res["top_referers"] == [{"value": "pirate.example", "users": 1, "requests": 3, "bytes": 3000}]


def test_top_asns_are_labelled_and_skip_unknown(in_memory_duckdb, test_service_source):
    cols = [*_ALL_COLS, "asn"]
    rows = [
        {**_row(10), "asn": 7922},
        {**_row(10), "asn": 7922},
        {**_row(10), "asn": 64496},
        {**_row(10, edge=False), "asn": 64496},  # shield line, not counted
        {**_row(10), "asn": 0},  # unknown ASN
        {**_row(10), "asn": None},
    ]
    _make_table(in_memory_duckdb, test_service_source, rows, cols)
    with patch("backend.core.duckdb.get_asn_names", return_value={7922: "Comcast Cable"}):
        res = _call(in_memory_duckdb, test_service_source)

    assert res["fields"]["asn"] is True
    assert [(r["value"], r["requests"]) for r in res["top_asns"]] == [("7922", 2), ("64496", 1)]
    # A resolved owner gets "Name (number)"; an unresolved one falls back to the bare AS number.
    assert [r["label"] for r in res["top_asns"]] == ["Comcast Cable (7922)", "AS64496"]


def test_top_asns_survive_name_lookup_failure(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, [{**_row(10), "asn": 7922}], [*_ALL_COLS, "asn"])
    with patch("backend.core.duckdb.get_asn_names", side_effect=RuntimeError("dns down")):
        res = _call(in_memory_duckdb, test_service_source)

    assert [(r["value"], r["requests"]) for r in res["top_asns"]] == [("7922", 1)]
    assert "label" not in res["top_asns"][0]


def test_top_asns_empty_without_asn_column(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source)

    assert res["fields"]["asn"] is False
    assert res["top_asns"] == []


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


def _call_subscriber_ips(con, src, subscriber_id, **kw):
    args = {"start_time": _START, "end_time": _END, "filters": {}}
    args.update(kw)
    return get_subscriber_ips(con=con, src=src, subscriber_id=subscriber_id, **args)


def test_subscriber_ips_one_row_per_ip_country_host(in_memory_duckdb, test_service_source):
    _ALL_COLS["cs_sub_id"] = "VARCHAR"
    try:
        rows = [
            {**_sub_row("sub-shared", "203.0.113.1", minutes_ago=30), "host": "a.example"},
            # .3 from DE twice on b.example, once from FR on b.example: two rows, not one.
            {**_sub_row("sub-shared", "203.0.113.3", minutes_ago=8, country="DE"), "host": "b.example"},
            {**_sub_row("sub-shared", "203.0.113.3", minutes_ago=2, country="DE"), "host": "b.example"},
            {**_sub_row("sub-shared", "203.0.113.3", minutes_ago=4, country="FR"), "host": "b.example"},
            # Same IP + country on another host is its own row too.
            {**_sub_row("sub-shared", "203.0.113.3", minutes_ago=6, country="DE"), "host": "c.example"},
            # Shield line is the same request seen twice — not counted.
            {**_sub_row("sub-shared", "203.0.113.9", minutes_ago=1, edge=False), "host": "a.example"},
            # Another subscriber's IP is not listed.
            _sub_row("sub-other", "198.51.100.7", minutes_ago=1),
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call_subscriber_ips(in_memory_duckdb, test_service_source, "sub-shared")
    finally:
        _ALL_COLS.pop("cs_sub_id")

    def _ip(ip, country, host, requests, minutes_ago):
        return {
            "ip": ip,
            "country": country,
            "host": host,
            "requests": requests,
            "bytes": requests * 100,
            "last_seen": (_NOW - timedelta(minutes=minutes_ago)).isoformat(),
        }

    assert res["available"] is True
    assert res["truncated"] is False
    assert res["ips"] == [
        _ip("203.0.113.3", "DE", "b.example", 2, 2),
        _ip("203.0.113.3", "FR", "b.example", 1, 4),
        _ip("203.0.113.3", "DE", "c.example", 1, 6),
        _ip("203.0.113.1", "US", "a.example", 1, 30),
    ]


def test_subscriber_ips_blank_country_and_host_are_null(in_memory_duckdb, test_service_source):
    _ALL_COLS["cs_sub_id"] = "VARCHAR"
    try:
        rows = [
            _sub_row("sub-shared", "203.0.113.1", country=""),
            _sub_row("sub-shared", "203.0.113.1", country=None),
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call_subscriber_ips(in_memory_duckdb, test_service_source, "sub-shared")
    finally:
        _ALL_COLS.pop("cs_sub_id")

    assert [(r["ip"], r["country"], r["host"], r["requests"]) for r in res["ips"]] == [("203.0.113.1", None, None, 2)]


def test_subscriber_ips_narrowed_by_content_id(in_memory_duckdb, test_service_source):
    _ALL_COLS["cs_sub_id"] = "VARCHAR"
    try:
        rows = [
            _sub_row("sub-shared", "203.0.113.1"),
            {**_sub_row("sub-shared", "203.0.113.2"), "cs_content_id": "movie-2"},
        ]
        _make_table(in_memory_duckdb, test_service_source, rows)
        res = _call_subscriber_ips(in_memory_duckdb, test_service_source, "sub-shared", content_id="movie-2")
    finally:
        _ALL_COLS.pop("cs_sub_id")

    assert [r["ip"] for r in res["ips"]] == ["203.0.113.2"]


def test_subscriber_ips_unavailable_without_token_column(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call_subscriber_ips(in_memory_duckdb, test_service_source, "sub-shared")

    assert res["available"] is False
    assert res["ips"] == []


# ── enforcement outcomes ─────────────────────────────────────────────────────

_ENF_COLS = [*_ALL_COLS, *_ENF_TYPES]


def _enf(minutes_ago, status, mode=None, rl_mode=None, **kw):
    return {**_row(minutes_ago, **kw), "cs_status": status, "cs_mode": mode, "cs_rl_mode": rl_mode}


def test_enforcement_separates_enforced_from_warn_only(in_memory_duckdb, test_service_source):
    rows = [
        _enf(10, "blocked", mode="ENFORCE"),
        _enf(10, "blocked", mode="WARN"),
        _enf(10, "throttled", mode="ENFORCE"),
        _enf(40, "rate_limited", rl_mode="ENFORCE"),
        _enf(40, "rate_limited", rl_mode="WARN"),
        # A block-mode of ENFORCE says nothing about a rate-limit outcome.
        _enf(40, "rate_limited", mode="ENFORCE", rl_mode="WARN"),
        _enf(20, "counted", rl_mode="ENFORCE"),
        _enf(20, "allowed", mode="ENFORCE"),
        _enf(20, "", mode="ENFORCE"),  # no enforcement ran
        _enf(20, "blocked", mode="ENFORCE", edge=False),  # shield line: not counted
    ]
    _make_table(in_memory_duckdb, test_service_source, rows, cols=_ENF_COLS)
    res = _call(in_memory_duckdb, test_service_source)

    by_status = {r["status"]: r for r in res["enforcement"]}
    assert set(by_status) == {"blocked", "throttled", "rate_limited", "counted", "allowed"}
    assert (by_status["blocked"]["requests"], by_status["blocked"]["enforced"]) == (2, 1)
    assert (by_status["throttled"]["requests"], by_status["throttled"]["enforced"]) == (1, 1)
    assert (by_status["rate_limited"]["requests"], by_status["rate_limited"]["enforced"]) == (3, 1)
    assert by_status["counted"]["enforced"] == 0 and by_status["allowed"]["enforced"] == 0
    assert sum(p["requests"] for p in res["enforcement_ts"]) == 8


def test_enforcement_is_empty_without_the_status_column(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, _fixture_rows())
    res = _call(in_memory_duckdb, test_service_source)
    assert res["fields"]["cs_status"] is False
    assert res["enforcement"] == [] and res["enforcement_ts"] == []


def test_enforcement_reasons_group_by_policy_rule_and_status(in_memory_duckdb, test_service_source):
    def row(status, mode, policy, reason, **kw):
        return {**_enf(10, status, mode=mode, **kw), "cs_policy_id": policy, "cs_policy_reason": reason}

    rows = [
        row("blocked", "ENFORCE", "p1", "country_regex"),
        row("blocked", "ENFORCE", "p1", "country_regex"),
        row("blocked", "WARN", "p1", "country_regex"),
        row("throttled", "ENFORCE", "p1", "asn"),
        row("allowed", "ENFORCE", "p2", "exempt:country"),
        row("allowed", "ENFORCE", None, None),  # no policy applied
        row("blocked", "ENFORCE", "p1", "ip", edge=False),  # shield line: not counted
    ]
    _make_table(in_memory_duckdb, test_service_source, rows, cols=[*_ENF_COLS, *_POLICY_TYPES])
    res = _call(in_memory_duckdb, test_service_source)

    got = {
        (r["policy_id"], r["reason"], r["status"]): (r["requests"], r["enforced"]) for r in res["enforcement_reasons"]
    }
    assert got == {
        ("p1", "country_regex", "blocked"): (3, 2),
        ("p1", "asn", "throttled"): (1, 1),
        ("p2", "exempt:country", "allowed"): (1, 0),
    }


def test_enforcement_reasons_empty_without_policy_columns(in_memory_duckdb, test_service_source):
    _make_table(in_memory_duckdb, test_service_source, [_enf(10, "blocked", mode="ENFORCE")], cols=_ENF_COLS)
    res = _call(in_memory_duckdb, test_service_source)
    assert res["enforcement_reasons"] == [] and res["enforcement"]
