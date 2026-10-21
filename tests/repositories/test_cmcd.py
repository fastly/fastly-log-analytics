"""Behavior tests for the CMCD aggregates repository against a real DuckDB table."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest

from backend.repositories import _base
from backend.repositories._base import _safe_table
from backend.repositories.cmcd import get_cmcd_aggregates

_T0 = datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _make_table(con, name: str, rows: list[tuple]) -> dict:
    table = _safe_table(name)
    con.execute(f"""
        CREATE TABLE {table} (
            timestamp  TIMESTAMPTZ,
            cmcd_sid   VARCHAR,
            cmcd_bs    BOOLEAN,
            cmcd_mtp   INTEGER,
            cmcd_ot    VARCHAR,
            country    VARCHAR,
            asn        INTEGER
        )
    """)
    con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    return {"name": name, "service_id": f"svc-{name}"}


def _country_rows(result: dict) -> dict[str, dict]:
    return {r["country"]: r for r in result["rebuffer_by_country"]}


def test_rebuffer_by_country_uses_per_session_video_medians(con):
    rows = [
        # US / s1: video throughputs 100, 300, 500 (median 300), one starvation, plus an audio row that must be ignored
        (_T0, "s1", False, 100, "v", "US", 1),
        (_T0, "s1", True, 300, "v", "US", 1),
        (_T0, "s1", False, 500, "v", "US", 1),
        (_T0, "s1", False, 9999, "a", "US", 1),
        # US / s2: single video request, no starvation
        (_T0, "s2", False, 1000, "v", "US", 2),
        # GB / s3: audio only, so there is no video median
        (_T0, "s3", False, 7777, "a", "GB", 3),
    ]
    src = _make_table(con, "cmcd_country", rows)

    result = get_cmcd_aggregates(
        con=con,
        src=src,
        start_time="2026-10-21T00:00:00Z",
        end_time="2026-10-22T00:00:00Z",
        filters={},
        sections={"rebuffer_by_country"},
    )

    by_country = _country_rows(result)
    assert by_country["US"]["session_count"] == 2
    assert by_country["US"]["rebuffer_rate"] == 50.0
    # mean of the per-session video medians: (300 + 1000) / 2
    assert by_country["US"]["median_throughput"] == 650
    assert by_country["GB"]["session_count"] == 1
    assert by_country["GB"]["median_throughput"] is None


def test_rebuffer_by_country_avoids_per_group_approx_quantile(con, monkeypatch):
    """APPROX_QUANTILE builds a t-digest per (country, session) group; with many
    countries and sessions that was ~9x slower than MEDIAN (about 1.1 s vs 0.12 s
    on 3M rows / 72k groups). Pin the cheaper form."""
    src = _make_table(con, "cmcd_sql", [(_T0, "s1", False, 100, "v", "US", 1)])

    seen: list[str] = []
    original = _base.QueryRunner.execute

    def spy(self, sql, *args, **kwargs):
        seen.append(sql)
        return original(self, sql, *args, **kwargs)

    monkeypatch.setattr(_base.QueryRunner, "execute", spy)
    get_cmcd_aggregates(
        con=con,
        src=src,
        start_time="2026-10-21T00:00:00Z",
        end_time="2026-10-22T00:00:00Z",
        filters={},
        sections={"rebuffer_by_country"},
    )

    per_session = [s for s in seen if "per_session" in s and "country" in s]
    assert per_session, "expected the rebuffer_by_country statement to be executed"
    assert all("APPROX_QUANTILE" not in s.upper() for s in per_session)


def test_unavailable_when_the_service_has_no_cmcd_columns(con):
    table = _safe_table("cmcd_none")
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, country VARCHAR)")
    result = get_cmcd_aggregates(
        con=con,
        src={"name": "cmcd_none", "service_id": "svc-none"},
        start_time=None,
        end_time=None,
        filters={},
    )
    assert result["available"] is False
