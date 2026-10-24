"""Shared DuckDB analytical table fixtures for contract and property tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb

from backend.repositories._base import _safe_table

DEFAULT_TABLE_NAME = "test_service"


def make_seeded_logs_con(
    table_name: str = DEFAULT_TABLE_NAME,
    num_rows: int = 20,
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with the full 43-column analytical schema
    and ``num_rows`` deterministic rows covering status codes, POPs, countries, and latencies.
    """
    con = duckdb.connect(":memory:")
    table = _safe_table(table_name)
    con.execute(
        f"""
        CREATE TABLE {table} (
            "timestamp" TIMESTAMPTZ,
            "dt" VARCHAR,
            "timestamp_hour" VARCHAR,
            "status" INTEGER,
            "country" VARCHAR,
            "url" VARCHAR,
            "ip" VARCHAR,
            "method" VARCHAR,
            "ua" VARCHAR,
            "pop" VARCHAR,
            "asn" INTEGER,
            "city" VARCHAR,
            "region" VARCHAR,
            "elapsed" INTEGER,
            "cache" VARCHAR,
            "ottfb" DOUBLE,
            "ttfb" DOUBLE,
            "ottlb" DOUBLE,
            "waf_sig" VARCHAR,
            "edge" BOOLEAN,
            "lat" DOUBLE,
            "lon" DOUBLE,
            "resp_bytes" INTEGER,
            "oip" VARCHAR,
            "ost" INTEGER,
            "tcp_rtt" INTEGER,
            "ploss" DOUBLE,
            "rtt_var" INTEGER,
            "rtt_min" INTEGER,
            "metro" VARCHAR,
            "ja3" VARCHAR,
            "ja4" VARCHAR,
            "ttl" INTEGER,
            "p_type" VARCHAR,
            "transport" VARCHAR,
            "c_speed" VARCHAR,
            "c_type" VARCHAR,
            "server_region" VARCHAR,
            "retrans" INTEGER,
            "bw" INTEGER,
            "delivery_rate" INTEGER,
            "rid" VARCHAR,
            "prid" VARCHAR
        )
        """
    )
    base = datetime.now(UTC) - timedelta(hours=1)
    statuses = [200, 200, 200, 200, 404, 500, 200, 304, 301, 200]
    pops = ["LAX", "JFK", "LHR"]
    step_minutes = max(1, 60 // max(num_rows, 1))
    for i in range(num_rows):
        ts = base + timedelta(minutes=i * step_minutes)
        status = statuses[i % len(statuses)]
        con.execute(
            f"INSERT INTO {table} VALUES " + "(" + ", ".join(["?"] * 43) + ")",
            [
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                status,
                "US" if i % 3 == 0 else "GB",
                f"/path-{i % 10}",
                f"10.0.0.{i}",
                "GET",
                "Mozilla/5.0",
                pops[i % len(pops)],
                15169 + (i % 5),
                "San Francisco",
                "CA",
                50 + i * 10,
                "HIT" if i % 2 == 0 else "MISS",
                50000.0 + i,
                0.05 + i * 0.01,
                0.1 + i * 0.01,
                "",
                True,
                37.7749,
                -122.4194,
                500 + i * 100,
                "origin-1.example.com",
                status,
                10000 + i * 100,
                0.0,
                100,
                50,
                "807",
                "ja3_fp",
                "ja4_fp",
                300,
                "U",
                "h2",
                "C",
                "broadband",
                "us-west",
                0,
                100000,
                100000,
                f"rid_{i}",
                f"prid_{i % 5}",
            ],
        )
    return con
