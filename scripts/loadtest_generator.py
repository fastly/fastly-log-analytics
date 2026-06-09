#!/usr/bin/env python3
"""Synthetic Fastly-log generator for local load testing.

Writes Parquet directly to ``cache/{bucket}/buffer/`` so the dashboard's
active-hour read path (``backend/repositories/_base.py:480`` —
``read_parquet(buffer_glob, union_by_name=true)``) picks it up. Then optionally
runs ``backend.core.iceberg.commit_buffer`` so the data also lives in the
permanent Iceberg table for cross-hour / windowed queries.

Designed for the dummy services configured with ``fos_endpoint="http://localhost:0"``
(see ``docs/performance_load_test_plan.md`` and ``configs/dummy-*-rps.json``);
those use the local ``file://`` warehouse path added to ``_get_catalog``.

Streams 500K-row Arrow batches through ``pq.ParquetWriter`` so heap stays
bounded regardless of total row count.

Usage::

  python scripts/loadtest_generator.py \
    --service dummy-10k-rps \
    --hour-start "2026-06-09T04:00:00Z" \
    --rows 1_000_000 \
    [--batch-size 500_000] [--file-rows 500_000] [--seed 42] [--commit]

Cardinality knobs (``--cardinality {low,med,high}``) control the size of the
URL / IP / UA / JA3 / ASN pools so different hash-table-load regimes can be
exercised without changing row count.
"""

from __future__ import annotations

import argparse
import os
import resource
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Cardinality profiles. See docs/performance_load_test_plan.md §3.
CARDINALITY_PROFILES = {
    "low": dict(urls=100, ips=1_000, uas=50, ja3=20, asns=10),
    "med": dict(urls=50_000, ips=100_000, uas=5_000, ja3=500, asns=100),
    "high": dict(urls=5_000_000, ips=10_000_000, uas=500_000, ja3=50_000, asns=1_000),
}

ZIPF_S = 1.1

COUNTRIES = ["US", "DE", "GB", "JP", "BR", "FR", "CA", "AU", "IN", "NL"]
_CW = [0.35, 0.08, 0.07, 0.06, 0.05, 0.03, 0.03, 0.03, 0.03, 0.03]
COUNTRY_WEIGHTS = [w / sum(_CW) for w in _CW]

STATUSES = [200, 204, 304, 301, 302, 400, 401, 403, 404, 500, 502, 503, 504, 406, 429]
_SW = [0.70, 0.10, 0.10, 0.02, 0.01, 0.005, 0.005, 0.005, 0.025, 0.005, 0.005, 0.01, 0.005, 0.005, 0.005]
STATUS_WEIGHTS = [w / sum(_SW) for w in _SW]

CACHE_VALS = ["HIT", "MISS", "PASS", "ERROR", "HIT-CLUSTER"]
CACHE_WEIGHTS = [0.60, 0.25, 0.10, 0.03, 0.02]

METHODS = ["GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE"]
_MW = [0.88, 0.08, 0.02, 0.005, 0.005, 0.01]
METHOD_WEIGHTS = [w / sum(_MW) for w in _MW]

PROTOCOLS = ["HTTP/2", "HTTP/1.1", "HTTP/3"]
PROTO_WEIGHTS = [0.70, 0.20, 0.10]

POPS = [
    "JFK", "LHR", "SYD", "NRT", "FRA", "AMS", "SIN", "GRU", "LAX", "ORD",
    "DFW", "MIA", "SEA", "DEN", "ATL", "BOS", "IAD", "PHX", "MSP", "DTW",
    "YYZ", "YVR", "MAD", "MIL", "MUC", "BER", "STO", "OSL", "CPH", "DUB",
    "ZRH", "VIE", "PRG", "WAW", "ATH", "IST", "DXB", "BOM", "HKG", "ICN",
    "BKK", "MEL", "PER", "AKL", "JNB", "CAI", "SFO", "PDX", "HOU", "PHL",
]

HOSTS = ["www.example.com", "api.example.com", "static.example.com"]
HOST_WEIGHTS = [0.80, 0.15, 0.05]

UAS = [
    "Mozilla/5.0 Chrome/120",
    "Mozilla/5.0 Safari/17",
    "Mozilla/5.0 Firefox/120",
    "Googlebot/2.1",
    "Bingbot/2.0",
]


def _zipf_indices(n: int, pool_size: int, rng: np.random.Generator) -> np.ndarray:
    z = rng.zipf(ZIPF_S, n)
    return (z - 1) % pool_size


def _gen_batch(n: int, hour_start_ms: int, hour_end_ms: int, card: dict, rng: np.random.Generator) -> dict:
    ts_ms = rng.integers(hour_start_ms, hour_end_ms, size=n, dtype=np.int64)
    ts_us = ts_ms * 1000

    status = rng.choice(STATUSES, size=n, p=STATUS_WEIGHTS).astype(np.int32)
    cache = rng.choice(CACHE_VALS, size=n, p=CACHE_WEIGHTS)
    method = rng.choice(METHODS, size=n, p=METHOD_WEIGHTS)
    proto = rng.choice(PROTOCOLS, size=n, p=PROTO_WEIGHTS)
    host = rng.choice(HOSTS, size=n, p=HOST_WEIGHTS)
    country = rng.choice(COUNTRIES, size=n, p=COUNTRY_WEIGHTS)
    pop = rng.choice(POPS, size=n)

    url_idx = _zipf_indices(n, card["urls"], rng)
    ip_idx = _zipf_indices(n, card["ips"], rng)
    ua_idx = _zipf_indices(n, card["uas"], rng)
    ja3_idx = _zipf_indices(n, card["ja3"], rng)
    asn_idx = _zipf_indices(n, card["asns"], rng)

    url = np.array([f"/page-{i}.html" for i in url_idx], dtype=object)
    ip = np.array(
        [f"10.{(i // 65536) & 0xFF}.{(i // 256) & 0xFF}.{i & 0xFF}" for i in ip_idx],
        dtype=object,
    )
    ua = np.array(
        [UAS[i % len(UAS)] if i < len(UAS) else f"ua-{i}" for i in ua_idx],
        dtype=object,
    )
    ja3 = np.array([f"ja3-{i:04x}" for i in ja3_idx], dtype=object)
    ja4 = np.array([f"ja4-{i:04x}" for i in ja3_idx], dtype=object)
    asn = (asn_idx.astype(np.int32) + 1000)

    elapsed_ms = rng.lognormal(mean=np.log(25), sigma=1.2, size=n).astype(np.int32)
    elapsed = np.clip(elapsed_ms, 1, 30_000)
    ttfb = (elapsed * rng.uniform(0.3, 0.9, size=n)).astype(np.int32)
    resp_bytes = np.clip(
        rng.lognormal(mean=np.log(8_000), sigma=1.5, size=n).astype(np.int64),
        100,
        50_000_000,
    )
    req_bytes = np.clip(
        rng.lognormal(mean=np.log(1_200), sigma=1.0, size=n).astype(np.int64),
        50,
        1_000_000,
    )

    return {
        "timestamp": ts_us,
        "ip": ip,
        "status": status,
        "elapsed": elapsed,
        "cache": cache,
        "resp_bytes": resp_bytes,
        "host": host,
        "url": url,
        "method": method,
        "proto": proto,
        "ua": ua,
        "req_bytes": req_bytes,
        "pop": pop,
        "ttfb": ttfb,
        "country": country,
        "asn": asn,
        "ja3": ja3,
        "ja4": ja4,
        "_source_file": np.array([f"synthetic://gen/{int(time.time())}"] * n, dtype=object),
    }


def _cols_to_arrow_table(cols: dict, schema: pa.Schema) -> pa.Table:
    """Build a pa.Table matching ``schema``, filling NULL for any missing columns."""
    n_rows = len(next(iter(cols.values())))
    arrays = []
    for field in schema:
        name = field.name
        if name in cols:
            arr = pa.array(cols[name], type=None)
            if arr.type != field.type:
                arr = arr.cast(field.type, safe=False)
            arrays.append(arr)
        else:
            arrays.append(pa.nulls(n_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _rss_mb() -> float:
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is bytes on macOS, kilobytes on Linux.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return raw / divisor


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--service", required=True, help="service_id, e.g. dummy-10k-rps")
    ap.add_argument("--rows", type=int, required=True, help="total rows to generate")
    ap.add_argument(
        "--hour-start",
        required=True,
        help='ISO 8601 UTC start of the target hour partition, e.g. "2026-06-09T04:00:00Z"',
    )
    ap.add_argument("--cardinality", choices=list(CARDINALITY_PROFILES), default="med")
    ap.add_argument("--batch-size", type=int, default=500_000, help="rows per Arrow batch")
    ap.add_argument("--file-rows", type=int, default=500_000, help="rows per output Parquet file")
    ap.add_argument("--seed", type=int, default=42, help="numpy RNG seed for reproducibility")
    ap.add_argument(
        "--commit",
        action="store_true",
        help="After writing to buffer/, run commit_buffer to materialize as Iceberg snapshot.",
    )
    args = ap.parse_args()

    # Lazy import: pulling in backend.config + backend.core.iceberg up front
    # would inflate the baseline heap before any allocation work begins.
    from backend.config import load_config
    from backend.core.iceberg import _buffer_dir, commit_buffer, get_arrow_schema

    src = load_config(args.service)
    if not src:
        print(f"ERROR: service {args.service!r} not found in configs/", file=sys.stderr)
        return 2

    schema = get_arrow_schema(src.get("log_fields", {}))
    buf_dir = _buffer_dir(src)
    os.makedirs(buf_dir, exist_ok=True)

    hour_start_dt = datetime.fromisoformat(args.hour_start.replace("Z", "+00:00"))
    if hour_start_dt.tzinfo is None:
        hour_start_dt = hour_start_dt.replace(tzinfo=timezone.utc)
    hour_start_ms = int(hour_start_dt.timestamp() * 1000)
    hour_end_ms = hour_start_ms + 3600 * 1000

    rng = np.random.default_rng(args.seed)
    card = CARDINALITY_PROFILES[args.cardinality]
    t0 = time.monotonic()
    rows_remaining = args.rows
    file_idx = 0
    total_rows = 0

    while rows_remaining > 0:
        rows_this_file = min(args.file_rows, rows_remaining)
        fname = f"loadtest_batch_{int(time.time())}_{file_idx:04d}.parquet"
        fpath = os.path.join(buf_dir, fname)
        writer = pq.ParquetWriter(fpath, schema, compression="zstd", compression_level=1)

        rows_in_this_file = 0
        while rows_in_this_file < rows_this_file:
            n = min(args.batch_size, rows_this_file - rows_in_this_file)
            cols = _gen_batch(n, hour_start_ms, hour_end_ms, card, rng)
            tbl = _cols_to_arrow_table(cols, schema)
            # Match write_to_buffer's sort keys so DuckDB's row-group min/max
            # statistics work the same on synthetic vs real buffer files.
            tbl = tbl.sort_by([("timestamp", "ascending"), ("ip", "ascending")])
            writer.write_table(tbl)
            rows_in_this_file += n
            del cols, tbl

        writer.close()
        rows_remaining -= rows_this_file
        total_rows += rows_this_file
        file_idx += 1
        elapsed = time.monotonic() - t0
        rate = total_rows / max(elapsed, 0.001)
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        print(
            f"  wrote {fname}: {rows_this_file:,} rows, {size_mb:.1f} MB | "
            f"total {total_rows:,}/{args.rows:,} ({100*total_rows/args.rows:.1f}%) | "
            f"{rate:,.0f} rows/sec | RSS {_rss_mb():.0f} MB | elapsed {elapsed:.1f}s",
            flush=True,
        )

    total_elapsed = time.monotonic() - t0
    print(
        f"\nGENERATED: {total_rows:,} rows in {total_elapsed:.1f}s "
        f"({total_rows/total_elapsed:,.0f} rows/sec). Peak RSS {_rss_mb():.0f} MB."
    )
    print(f"Buffer dir: {buf_dir}")

    if args.commit:
        print("\nRunning commit_buffer...", flush=True)
        t_commit = time.monotonic()
        result = commit_buffer(src)
        print(
            f"COMMITTED: {result.get('rows_committed', 0):,} rows in "
            f"{result.get('files_committed', 0)} files "
            f"(snapshot={result.get('snapshot_id')}) in "
            f"{time.monotonic() - t_commit:.1f}s"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
