"""Tests for backend.scoring.fixtures — log rows → sessionized JSONL traces."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta

import pytest

from backend.scoring.fixtures import (
    SESSION_GAP_SECONDS,
    Event,
    extract_traces,
    rows_to_events,
    sessionize,
    write_jsonl,
)

UTC = UTC


def _row(
    ts: datetime,
    ip: str = "1.1.1.1",
    ua: str = "Mozilla/5.0",
    url: str = "/",
    method: str = "GET",
    status: int = 200,
    referer: str = "",
    ttfb: float = 0.05,
    country: str = "US",
    asn: int | None = 7922,
) -> tuple:
    """Build a row tuple in the order rows_to_events expects."""
    return (ts, ip, ua, url, method, status, referer, ttfb, country, asn)


# ── rows_to_events ────────────────────────────────────────────────────────────


def test_rows_to_events_basic_field_mapping():
    rows = [_row(datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC), url="/foo", status=404)]
    items = list(rows_to_events(rows))
    assert len(items) == 1
    ip, ua, ev = items[0]
    assert ip == "1.1.1.1"
    assert ua == "Mozilla/5.0"
    assert ev.url == "/foo"
    assert ev.status == 404
    assert ev.method == "GET"
    assert ev.ttfb_ms == 50.0  # 0.05s → 50ms


def test_rows_to_events_method_uppercased():
    rows = [_row(datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC), method="get")]
    ip, ua, ev = next(rows_to_events(rows))
    assert ev.method == "GET"


def test_rows_to_events_null_safe():
    """Production logs can have NULL ASN, missing referer, etc. Don't crash."""
    rows = [
        (
            datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC),
            None,  # ip
            None,  # ua
            None,  # url
            None,  # method
            None,  # status
            None,  # referer
            None,  # ttfb
            None,  # country
            None,  # asn
        )
    ]
    ip, ua, ev = next(rows_to_events(rows))
    assert ip == ""
    assert ua == ""
    assert ev.url == ""
    assert ev.method == ""
    assert ev.status == 0
    assert ev.ttfb_ms == 0.0
    assert ev.asn is None


# ── sessionize ────────────────────────────────────────────────────────────────


def _events_for(
    ip: str, ua: str, timestamps: list[datetime], urls: list[str] | None = None
) -> list[tuple[str, str, Event]]:
    """Build a stream pre-sorted by (ip, ua, ts) as the sessionizer requires."""
    if urls is None:
        urls = [f"/page{i}" for i in range(len(timestamps))]
    assert len(urls) == len(timestamps)
    return [
        (
            ip,
            ua,
            Event(
                ts=ts.isoformat(timespec="seconds"),
                url=urls[i],
                method="GET",
                status=200,
                referer="",
                ttfb_ms=50.0,
                country="US",
                asn=7922,
            ),
        )
        for i, ts in enumerate(timestamps)
    ]


def test_sessionize_single_session_all_close():
    """Six requests within the gap window → one session."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for(
        "1.1.1.1",
        "Mozilla",
        [base + timedelta(seconds=10 * i) for i in range(6)],
    )
    sessions = list(sessionize(events))
    assert len(sessions) == 1
    assert sessions[0].event_count == 6


def test_sessionize_gap_splits_into_two_sessions():
    """One request, then a 31-min gap, then more → two sessions."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for(
        "1.1.1.1",
        "Mozilla",
        [
            base,
            base + timedelta(seconds=10),
            base + timedelta(seconds=SESSION_GAP_SECONDS + 60),  # past threshold
            base + timedelta(seconds=SESSION_GAP_SECONDS + 70),
        ],
    )
    sessions = list(sessionize(events))
    assert len(sessions) == 2
    assert sessions[0].event_count == 2
    assert sessions[1].event_count == 2


def test_sessionize_different_ips_are_separate_sessions():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = [
        *_events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)]),
        *_events_for("2.2.2.2", "Mozilla", [base + timedelta(seconds=20)]),
    ]
    sessions = list(sessionize(events))
    assert len(sessions) == 2
    assert {s.client_ip for s in sessions} == {"1.1.1.1", "2.2.2.2"}


def test_sessionize_different_uas_same_ip_are_separate_sessions():
    """NAT'd users sharing an IP get distinguished by UA."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = [
        *_events_for("1.1.1.1", "Mozilla/Firefox", [base]),
        *_events_for("1.1.1.1", "Mozilla/Chrome", [base + timedelta(seconds=5)]),
    ]
    sessions = list(sessionize(events))
    assert len(sessions) == 2
    assert {s.user_agent for s in sessions} == {"Mozilla/Firefox", "Mozilla/Chrome"}


def test_sessionize_empty_input_yields_nothing():
    assert list(sessionize(iter([]))) == []


def test_sessionize_custom_gap_seconds():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for(
        "1.1.1.1",
        "Mozilla",
        [base, base + timedelta(seconds=120)],
    )
    sessions_tight = list(sessionize(events, gap_seconds=60))
    sessions_loose = list(sessionize(events, gap_seconds=300))
    assert len(sessions_tight) == 2
    assert len(sessions_loose) == 1


def test_sessionize_stable_session_id_for_same_input():
    """Re-running extraction on the same data must produce identical session
    ids — required for reproducible test fixtures."""
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = _events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)])
    s1 = list(sessionize(events))
    s2 = list(sessionize(_events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)])))
    assert s1[0].session_id == s2[0].session_id


# ── write_jsonl ───────────────────────────────────────────────────────────────


def test_write_jsonl_one_line_per_session_round_trip():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    events = [
        *_events_for("1.1.1.1", "Mozilla", [base, base + timedelta(seconds=10)]),
        *_events_for("2.2.2.2", "Mozilla", [base + timedelta(seconds=20)]),
    ]
    sessions = list(sessionize(events))
    buf = io.StringIO()
    count = write_jsonl(sessions, buf)
    assert count == 2

    lines = buf.getvalue().splitlines()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["client_ip"] == "1.1.1.1"
    assert parsed[0]["event_count"] == 2
    assert parsed[1]["client_ip"] == "2.2.2.2"
    # Schema sanity — each event has the right keys.
    assert set(parsed[0]["events"][0].keys()) == {
        "ts",
        "url",
        "method",
        "status",
        "referer",
        "ttfb_ms",
        "country",
        "asn",
    }


# ── extract_traces (with mock DuckDB connection) ──────────────────────────────


class _FakeCursor:
    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeCon:
    def __init__(self, rows: list[tuple]):
        self._rows = rows
        self.last_sql: str | None = None

    def execute(self, sql: str) -> _FakeCursor:
        self.last_sql = sql
        return _FakeCursor(self._rows)


def test_extract_traces_builds_correct_sql():
    con = _FakeCon([])
    start = datetime(2026, 5, 15, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 16, 0, 0, 0, tzinfo=UTC)
    list(extract_traces(con, service_id="MySvc123", start=start, end=end, limit=100))
    assert con.last_sql is not None
    # View name lowercased per existing project convention.
    assert "logs_mysvc123" in con.last_sql
    assert "ORDER BY ip, ua, timestamp" in con.last_sql
    assert "LIMIT 100" in con.last_sql
    assert "timestamp >= TIMESTAMP '2026-05-15T00:00:00+00:00'" in con.last_sql
    assert "timestamp < TIMESTAMP '2026-05-16T00:00:00+00:00'" in con.last_sql


def test_extract_traces_end_to_end_yields_sessions():
    base = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
    rows = [
        _row(base, ip="1.1.1.1", url="/home"),
        _row(base + timedelta(seconds=5), ip="1.1.1.1", url="/products"),
        _row(base + timedelta(seconds=10), ip="1.1.1.1", url="/checkout"),
        # Different IP → separate session
        _row(base + timedelta(seconds=15), ip="2.2.2.2", url="/home"),
    ]
    con = _FakeCon(rows)
    sessions = list(extract_traces(con, service_id="svc"))
    assert len(sessions) == 2
    assert sessions[0].event_count == 3
    assert sessions[1].event_count == 1
    # Order should match the sort: 1.1.1.1 then 2.2.2.2
    assert sessions[0].client_ip == "1.1.1.1"
    assert sessions[1].client_ip == "2.2.2.2"
    # Events within a session preserve URL order
    assert [e.url for e in sessions[0].events] == ["/home", "/products", "/checkout"]


def test_extract_traces_no_filters_omits_where_clause():
    con = _FakeCon([])
    list(extract_traces(con, service_id="svc"))
    assert "WHERE" not in con.last_sql
    assert "LIMIT" not in con.last_sql


@pytest.mark.parametrize(
    "start,end,expected_clauses",
    [
        (datetime(2026, 5, 15, 0, 0, 0, tzinfo=UTC), None, ["timestamp >="]),
        (None, datetime(2026, 5, 16, 0, 0, 0, tzinfo=UTC), ["timestamp <"]),
    ],
)
def test_extract_traces_partial_time_filters(start, end, expected_clauses):
    con = _FakeCon([])
    list(extract_traces(con, service_id="svc", start=start, end=end))
    for clause in expected_clauses:
        assert clause in con.last_sql
