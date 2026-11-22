#!/usr/bin/env python3
"""Emit ``tests/perf/latest.json`` for the perf-regression gate.

Run at two scales:

  * **Fast smoke** (default, runs on every PR — see ci.yml): 100K
    synthetic rows, ~2 s wall. Catches gross regressions: extra
    GROUP BY columns, accidental cross-joins, a SELECT * inside a
    hot lookup. Misses superlinear-only regressions because the
    constant-factor overhead dominates at 100K.
  * **Nightly large** (1–5M rows, on-demand or scheduled — see
    perf-nightly.yml): a 36×–360× scale gap from production lets
    a superlinear regression (extra n-ary join, missing index,
    O(N log N) instead of O(N)) cross the threshold even though
    the smoke run cleared it. This is the "passes CI, fails in
    prod" generator the audit flagged.

Both scales write to ``tests/perf/latest.json`` and the gate
(``scripts/perf_gate.sh``) reads the corresponding section of
``tests/perf/baseline.json``. Each scale has its OWN per-scenario
ceiling — the absolute constants don't scale linearly with rows.

Override scale via ``--rows N`` or ``PERF_NUM_ROWS=N`` env var.

The absolute thresholds in baseline.json are NOT production targets;
they are the headroom-padded CI/nightly numbers (separately measured).
Production targets live in baseline.json's ``production_targets_comment``
and are validated by scripts/load_test/generate_synthetic_traffic.py against a real
dataset — that path is documented but not enforced by THIS gate.

Run ``uv run python scripts/emit_perf_latest.py`` for the smoke variant
or ``uv run python scripts/emit_perf_latest.py --rows 2000000`` for the
nightly variant.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "tests" / "perf" / "latest.json"

# Default row count for the fast PR-blocking smoke gate. Bigger numbers
# tighten the signal but inflate CI wall time; 100K rows × 7 query runs
# takes ~2 s on a 2024 macbook-class runner and produces stable timings.
# Override via ``--rows N`` or PERF_NUM_ROWS for the nightly variant.
DEFAULT_NUM_ROWS = 100_000
NUM_RUNS_COLD = 5
NUM_RUNS_WARM = 7


def _resolve_num_rows() -> int:
    """``--rows N`` > ``PERF_NUM_ROWS`` env var > DEFAULT_NUM_ROWS."""
    parser = argparse.ArgumentParser(description="Emit perf samples for the CI gate")
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help=f"row count for the synthetic dataset (default {DEFAULT_NUM_ROWS:,})",
    )
    args, _ = parser.parse_known_args()
    if args.rows is not None:
        return args.rows
    env = os.environ.get("PERF_NUM_ROWS")
    if env:
        return int(env)
    return DEFAULT_NUM_ROWS


def _scale_key_for(num_rows: int) -> str:
    """Map a row count to the baseline.json section key.

    Keep this thin — the gate doesn't interpolate between sizes; a row
    count without a matching baseline section fails fast. Add new keys
    here (and a matching baseline section) when introducing a new
    scheduled scale.

    Tiers:
      - smoke_100k:  ≤  250K — PR-blocking smoke
      - mid_500k:    ≤  750K — mid-tier gate (catches scan-bound
                              regressions hidden at 100K constant
                              factors but visible before 1M; audit R-9d)
      - nightly_1m:  ≤ 1.5M  — nightly cron
      - nightly_5m:  >  1.5M — explicit large-scale opt-in
    """
    if num_rows <= 250_000:
        return "smoke_100k"
    if num_rows <= 750_000:
        return "mid_500k"
    if num_rows <= 1_500_000:
        return "nightly_1m"
    return "nightly_5m"


def _generate_seed_table(num_rows: int) -> tuple[pa.Table, pa.Buffer]:
    """Build a synthetic ``num_rows`` Arrow table and serialize to canonical Archive Parquet."""
    base_statuses = [200, 200, 200, 200, 204, 301, 302, 400, 403, 404, 500, 502, 503]
    base_methods = ["GET", "GET", "GET", "POST", "HEAD"]
    base_countries = ["US", "DE", "GB", "JP", "BR", "FR", "CA", "AU", "IN", "NL"]
    chunk = 1000
    chunk_status = [base_statuses[(i * 17) % len(base_statuses)] for i in range(chunk)]
    chunk_method = [base_methods[(i * 31) % len(base_methods)] for i in range(chunk)]
    chunk_country = [base_countries[(i * 53) % len(base_countries)] for i in range(chunk)]
    chunk_url = [f"/path/{i % 500}" for i in range(chunk)]
    chunk_ottfb = [10 + ((i * 71) % 4990) for i in range(chunk)]
    chunk_bytes = [100 + ((i * 89) % 49900) for i in range(chunk)]

    repeats = (num_rows + chunk - 1) // chunk
    table = pa.table(
        {
            "status": pa.array((chunk_status * repeats)[:num_rows], type=pa.int32()),
            "method": pa.array((chunk_method * repeats)[:num_rows], type=pa.string()),
            "country": pa.array((chunk_country * repeats)[:num_rows], type=pa.string()),
            "url": pa.array((chunk_url * repeats)[:num_rows], type=pa.string()),
            "ottfb_ms": pa.array((chunk_ottfb * repeats)[:num_rows], type=pa.int64()),
            "response_size_bytes": pa.array((chunk_bytes * repeats)[:num_rows], type=pa.int64()),
        }
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return table, sink.getvalue()


def _run_aggregate_query(table: pa.Table) -> pa.Table:
    """Execute the representative dashboard aggregate query over an Arrow table."""
    mask = pc.and_(
        pc.greater_equal(table["status"], 200),
        pc.less(table["ottfb_ms"], 10000),
    )
    filtered = table.filter(mask)
    is_5xx = pc.if_else(pc.greater_equal(filtered["status"], 500), 1, 0)
    enriched = filtered.append_column("is_5xx", is_5xx)
    grouped = enriched.group_by("country").aggregate(
        [
            ("status", "count"),
            ("is_5xx", "sum"),
            ("ottfb_ms", "mean"),
            ("ottfb_ms", "max"),
            ("response_size_bytes", "sum"),
        ]
    )
    having = grouped.filter(pc.greater(grouped["status_count"], 10))
    return having.sort_by([("status_count", "descending")])


def _time_cold_ms(parquet_buf: pa.Buffer) -> int:
    t0 = time.perf_counter()
    table = pq.read_table(pa.BufferReader(parquet_buf))
    _run_aggregate_query(table)
    return int((time.perf_counter() - t0) * 1000)


def _time_warm_ms(table: pa.Table) -> int:
    t0 = time.perf_counter()
    _run_aggregate_query(table)
    return int((time.perf_counter() - t0) * 1000)


def main() -> int:
    num_rows = _resolve_num_rows()
    scale_key = _scale_key_for(num_rows)
    print(f"[perf-emit] generating {num_rows:,}-row synthetic dataset (scale={scale_key})...", flush=True)

    table, parquet_buf = _generate_seed_table(num_rows)
    _run_aggregate_query(table.slice(0, 1))

    # Cold-path proxy: deserialize canonical Archive Parquet + aggregate NUM_RUNS_COLD times, take p95.
    cold_samples: list[int] = []
    for i in range(NUM_RUNS_COLD):
        ms = _time_cold_ms(parquet_buf)
        cold_samples.append(ms)
        print(f"  cold run {i + 1}/{NUM_RUNS_COLD}: {ms} ms", flush=True)

    # p95 across the cold samples — with N=5 that's max() since 5 * 0.95 = 4.75.
    cold_samples.sort()
    cold_p95 = cold_samples[-1]

    # Warm-path proxy: repeat against the resident Arrow table. Take p50.
    warm_samples = [_time_warm_ms(table) for _ in range(NUM_RUNS_WARM)]
    print(f"  warm samples: {warm_samples}", flush=True)
    warm_p50 = int(statistics.median(warm_samples))

    payload = {
        "schema_version": 2,
        "scale_note": (
            f"Emitted at {num_rows:,} rows (scale_key={scale_key}). The gate "
            "reads the matching baseline.scenarios_by_scale[<scale_key>] "
            "section; thresholds at different scales are not interchangeable."
        ),
        "ci_dataset_rows": num_rows,
        # ``scale_key`` is what the gate keys baseline.scenarios_by_scale on.
        # Stays stable across scales — a future 5M scale is just an extra
        # baseline section, no schema bump.
        "scale_key": scale_key,
        "scenarios": {
            "cold_path_36M_1h_committed_p95_ms": cold_p95,
            "warm_path_36M_1h_p50_ms": warm_p50,
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    print(
        f"[perf-emit] wrote {OUT_PATH}: scale_key={scale_key} cold_p95={cold_p95}ms, warm_p50={warm_p50}ms",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
