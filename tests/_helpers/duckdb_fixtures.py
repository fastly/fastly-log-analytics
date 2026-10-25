"""Shared DuckDB analytical table fixtures for contract and property tests."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.core.log_fields import LOG_FIELD_CATALOG, resolve_enabled_fields
from backend.repositories._base import _safe_table

DEFAULT_TABLE_NAME = "test_service"


def generate_mock_records(
    config: dict,
    num_logs: int = 100,
    hours_ago: int = 1,
    rng: random.Random | Any = random,
) -> list[dict]:
    """Generate synthetic per-record log entries based on the log_fields schema.

    ``rng`` defaults to the stdlib ``random`` module so output is byte-identical
    to the historical ``mock_data.generate_mock_logs`` behaviour; pass a seeded
    ``random.Random`` for deterministic fixtures.
    """
    enabled_fields = resolve_enabled_fields(config.get("log_fields"))
    logs = []

    now = datetime.now(UTC)
    start_time = now - timedelta(hours=hours_ago)

    ips = ["192.168.1.1", "10.0.0.1", "203.0.113.5", "198.51.100.10"]
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "curl/7.68.0",
        "python-requests/2.25.1",
    ]
    urls = ["/", "/api/data", "/images/logo.png", "/about", "/login"]

    for _ in range(num_logs):
        log_entry = {}
        log_time = start_time + timedelta(seconds=rng.randint(0, hours_ago * 3600))

        if "timestamp" in enabled_fields:
            log_entry["timestamp"] = log_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            if log_entry["timestamp"].endswith("0000"):
                log_entry["timestamp"] = log_entry["timestamp"][:-4] + "00:00"

        if "ip" in enabled_fields:
            log_entry["ip"] = rng.choice(ips)

        if "status" in enabled_fields:
            log_entry["status"] = rng.choices([200, 404, 500], weights=[0.8, 0.1, 0.1])[0]

        if "elapsed" in enabled_fields:
            log_entry["elapsed"] = rng.randint(1000, 500000)

        if "cache" in enabled_fields:
            log_entry["cache"] = rng.choices(["HIT", "MISS", "PASS"], weights=[0.6, 0.3, 0.1])[0]

        if "resp_bytes" in enabled_fields:
            log_entry["resp_bytes"] = rng.randint(500, 50000)

        if "url" in enabled_fields:
            log_entry["url"] = rng.choice(urls)
        if "ua" in enabled_fields:
            log_entry["ua"] = rng.choice(uas)
        if "method" in enabled_fields:
            log_entry["method"] = "GET"

        if "pop" in enabled_fields:
            log_entry["pop"] = rng.choice(["JFK", "LHR", "SYD"])
        if "ttfb" in enabled_fields:
            ttfb_ms = log_entry.get("elapsed", 20000) * 0.8
            log_entry["ttfb"] = f"{ttfb_ms / 1000000:.6f}"

        if "country" in enabled_fields:
            log_entry["country"] = rng.choice(["US", "GB", "AU", "DE"])

        if "asn" in enabled_fields:
            log_entry["asn"] = rng.choice([7922, 3320, 15169])

        logs.append(log_entry)

    return logs


def generate_mock_logs(config: dict, num_logs: int = 100, hours_ago: int = 1) -> list[dict]:
    """Generate synthetic log entries based on the log_fields schema."""
    return generate_mock_records(config, num_logs=num_logs, hours_ago=hours_ago)


def insert_mock_logs(con, table_name: str, logs: list[dict]):
    """
    Inserts a list of mock JSON logs directly into the DuckDB connection.
    This bypasses the S3 download step for testing repositories.
    """
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
        temp_path = f.name

    try:
        # We need to map JSON fields to DuckDB schema types, similar to ingest.py
        # Filter to only fields that have VCL (raw log fields), excluding computed metrics
        raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
        columns_map = {f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]) for f in raw_fields}
        for log in logs:
            for k, v in log.items():
                if k not in columns_map:
                    if isinstance(v, bool):
                        columns_map[k] = "BOOLEAN"
                    elif isinstance(v, int):
                        columns_map[k] = "BIGINT"
                    elif isinstance(v, float):
                        columns_map[k] = "DOUBLE"
                    else:
                        columns_map[k] = "VARCHAR"

        schema_def = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_map.items()])

        # Create table if it doesn't exist
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")
        for col, dtype in columns_map.items():
            try:
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {dtype}')
            except Exception:
                pass

        cols_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns_map.items()) + "}"

        # Load the JSON data
        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM read_json_auto('{temp_path}', columns={cols_sql})")
    finally:
        os.unlink(temp_path)


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
