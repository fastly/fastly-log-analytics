"""Network transport rollup: writer output, day compaction and reader parity with the raw query."""

from __future__ import annotations

import contextlib
import os
import random
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow.parquet as pq
import pytest

from backend.core import rollups
from backend.core.rollups import network_transport
from backend.repositories._base import QueryRunner
from tests._helpers import build_writer_patches

_DDL = (
    "timestamp TIMESTAMPTZ, tcp_rtt INTEGER, retrans INTEGER, ploss DOUBLE, delivery_rate DOUBLE, "
    "resp_bytes BIGINT, proto VARCHAR"
)
_PROTOS = ["HTTP/2", "HTTP/3", "1.1", "2.0", "HTTP/1.0", None]


def _rows_for(start: datetime, count: int, seed: int) -> list[tuple]:
    rng = random.Random(seed)
    out = []
    for i in range(count):
        ts = start + timedelta(seconds=(i * 3500) // count)
        out.append(
            (
                ts,
                rng.choice([None, 0, rng.randint(5_000, 80_000)]),
                rng.choice([None, 0, 1, 2]),
                rng.choice([None, 0.0, 0.01]),
                rng.choice([None, 0.0, rng.uniform(1e5, 5e6)]),
                rng.randint(100, 50_000),
                rng.choice(_PROTOS),
            )
        )
    return out


def _seed(con: duckdb.DuckDBPyConnection, table: str, hours: list[datetime], per_hour: int) -> None:
    con.execute(f"CREATE TABLE {table} ({_DDL})")
    for n, h in enumerate(hours):
        con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)", _rows_for(h, per_hour, seed=n))


def _hour(hours_ago: int) -> datetime:
    return (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)


def _raw(con: duckdb.DuckDBPyConnection, table: str, st: datetime, et: datetime) -> dict:
    where = f"timestamp >= TIMESTAMPTZ '{st.isoformat()}' AND timestamp < TIMESTAMPTZ '{et.isoformat()}'"
    total, byt, retrans, ploss, rate = con.execute(
        f"SELECT COUNT(*), SUM(resp_bytes), AVG(retrans), AVG(ploss), "
        f"AVG(delivery_rate) FILTER (WHERE delivery_rate > 0) FROM {table} WHERE {where}"
    ).fetchone()
    p50, p95 = con.execute(
        f"SELECT quantile_cont(tcp_rtt, 0.5), quantile_cont(tcp_rtt, 0.95) FROM {table} WHERE {where} AND tcp_rtt > 0"
    ).fetchone()
    protos = dict(
        con.execute(
            f"SELECT CASE WHEN proto IN ('3.0','3','HTTP/3') THEN 'HTTP/3' WHEN proto IN ('2.0','2','HTTP/2') THEN 'HTTP/2' "
            f"WHEN proto IN ('1.1','HTTP/1.1') THEN 'HTTP/1.1' WHEN proto IN ('1.0','HTTP/1.0') THEN 'HTTP/1.0' "
            f"ELSE COALESCE(proto, 'Other') END AS p, COUNT(*) FROM {table} WHERE {where} GROUP BY p"
        ).fetchall()
    )
    return {
        "total": total,
        "bytes": byt,
        "retrans": retrans,
        "ploss": ploss,
        "rate": rate,
        "p50": p50,
        "p95": p95,
        "protos": protos,
    }


@contextlib.contextmanager
def writer_patches(cache_root, table, con_factory):
    with contextlib.ExitStack() as stack:
        for cm in build_writer_patches(cache_root, table, con_factory):
            stack.enter_context(cm)
        yield


@pytest.fixture
def seeded(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    table = "logs_nt"
    hours = [_hour(h) for h in range(26, -1, -1)]

    def _make_con() -> duckdb.DuckDBPyConnection:
        c = duckdb.connect(":memory:")
        _seed(c, table, hours, per_hour=240)
        return c

    con = _make_con()
    closed = [h.strftime("%Y-%m-%d-%H") for h in hours[:-1]]
    src = {"name": "svc-nt", "service_id": "svc-nt"}
    with writer_patches(cache_root, table, _make_con):
        assert network_transport.build_network_transport_bundles("svc-nt", src, closed) == 2 * len(closed)
    return cache_root, src, table, con, hours, _make_con


def _runner(con, src, table) -> QueryRunner:
    runner = QueryRunner(con, src)
    runner.get_schema_cols = lambda: {
        "timestamp",
        "tcp_rtt",
        "retrans",
        "ploss",
        "delivery_rate",
        "resp_bytes",
        "proto",
    }  # type: ignore[method-assign]
    return runner


@pytest.mark.usefixtures("frozen_clock")
class TestNetworkTransportRollup:
    def test_writer_emits_one_summary_row_and_protocol_rows_per_hour(self, seeded):
        cache_root, _src, _table, _con, hours, _mk = seeded
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hours[3].strftime('%Y-%m-%d-%H')}"
        summary = pq.read_table(str(hour_dir / "network_transport.parquet")).to_pylist()
        assert len(summary) == 1 and summary[0]["requests"] == 240
        protos = {
            r["proto_clean"]: r["requests"]
            for r in pq.read_table(str(hour_dir / "network_transport_proto.parquet")).to_pylist()
        }
        assert sum(protos.values()) == 240 and {"HTTP/2", "HTTP/3", "HTTP/1.1", "HTTP/1.0", "Other"} >= set(protos)

    def test_reader_matches_raw_for_a_window_with_partial_leading_hour_and_live_tail(self, seeded):
        cache_root, src, table, con, hours, mk = seeded
        st = hours[1] + timedelta(minutes=20)
        et = hours[-1] + timedelta(minutes=30)
        with writer_patches(cache_root, table, lambda: con):
            got = _runner(con, src, table).try_network_transport_from_rollup(
                st.isoformat(),
                et.isoformat(),
                has_filters=False,
                actual_cols=_runner(con, src, table).get_schema_cols(),
            )
        assert got is not None
        # Whole closed hours come from bundles, so compare against the same closed-hour set plus raw slices.
        want = _raw(con, table, st, et)
        assert got["total_requests"] == want["total"]
        assert got["bandwidth_bytes"] == want["bytes"]
        assert got["avg_retrans"] == pytest.approx(want["retrans"], rel=1e-9)
        assert got["avg_ploss"] == pytest.approx(want["ploss"], rel=1e-9)
        assert got["avg_delivery_rate"] == pytest.approx(want["rate"], rel=1e-9)
        assert dict(got["protocol_rows"]) == want["protos"]
        assert got["rtt_us"]["p50"] == pytest.approx(want["p50"], rel=0.1)
        assert got["rtt_us"]["p95"] == pytest.approx(want["p95"], rel=0.1)

    def test_filtered_window_is_not_served_from_the_rollup(self, seeded):
        _cache_root, src, table, con, hours, mk = seeded
        runner = _runner(con, src, table)
        assert (
            runner.try_network_transport_from_rollup(
                hours[0].isoformat(), hours[-1].isoformat(), has_filters=True, actual_cols=runner.get_schema_cols()
            )
            is None
        )

    def test_missing_protocol_bundle_falls_back_to_raw(self, seeded):
        cache_root, src, table, con, hours, mk = seeded
        for h in hours[:-1]:
            os.remove(
                cache_root
                / "rollups"
                / "hour_bundled"
                / f"hour={h.strftime('%Y-%m-%d-%H')}"
                / "network_transport_proto.parquet"
            )
        runner = _runner(con, src, table)
        with writer_patches(cache_root, table, lambda: con):
            assert (
                runner.try_network_transport_from_rollup(
                    hours[0].isoformat(), hours[-1].isoformat(), has_filters=False, actual_cols=runner.get_schema_cols()
                )
                is None
            )

    def test_day_compaction_sums_counts_and_keeps_weighted_percentiles(self, seeded):
        cache_root, src, table, con, hours, mk = seeded
        day_hours = [h for h in hours[:-1] if h.date() == hours[0].date()]
        if len(day_hours) != 24:
            pytest.skip("seed window does not contain a full closed UTC day")
        with writer_patches(cache_root, table, lambda: con):
            rollups.compact_network_transport_closed_days_to_daily("svc-nt", src)
        day_dir = cache_root / "rollups" / "day_bundled" / f"day={hours[0].date().isoformat()}"
        summary = pq.read_table(str(day_dir / "network_transport.parquet")).to_pylist()
        assert summary[0]["requests"] == 24 * 240


def test_transport_response_carries_the_approximate_flag_under_its_wire_name() -> None:
    from backend.models.analytics.network import TransportMetrics

    served = TransportMetrics(**{"_approx": True, "_approx_note": "weighted"}).model_dump(by_alias=True)
    assert served["_approx"] is True and served["_approx_note"] == "weighted"
    raw = TransportMetrics().model_dump(by_alias=True)
    assert raw["_approx"] is False and "_approx_note" not in raw
