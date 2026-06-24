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
and are validated by scripts/loadtest_generator.py against a real
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

import duckdb

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


def _generate_seed_data(con: duckdb.DuckDBPyConnection, num_rows: int) -> None:
    """Seed a single ``logs`` table with synthetic rows that resemble the
    real Fastly log shape closely enough for the dashboard's aggregate
    query to exercise the same code paths.

    Generates in-DuckDB via ``range()`` + deterministic ``hash(i, seed)``
    for the categorical / int columns. The previous Python
    ``random.choice`` + ``executemany`` path was O(N) Python-side and
    dominated wall time at 1M+ rows; pure-SQL generation does 1M rows
    in well under a second.
    """
    statuses = [200, 200, 200, 200, 204, 301, 302, 400, 403, 404, 500, 502, 503]
    methods = ["GET", "GET", "GET", "POST", "HEAD"]
    countries = ["US", "DE", "GB", "JP", "BR", "FR", "CA", "AU", "IN", "NL"]
    statuses_sql = "[" + ", ".join(str(s) for s in statuses) + "]"
    methods_sql = "[" + ", ".join(f"'{m}'" for m in methods) + "]"
    countries_sql = "[" + ", ".join(f"'{c}'" for c in countries) + "]"
    con.execute(
        f"""
        CREATE TABLE logs AS
        SELECT
            format('2026-06-09T04:{{:02d}}:{{:02d}}Z',
                   CAST((i // 1000) % 60 AS INTEGER),
                   CAST(i % 60 AS INTEGER)) AS timestamp,
            {statuses_sql}[1 + CAST(hash(i, 17) % {len(statuses)} AS INTEGER)] AS status,
            {methods_sql}[1 + CAST(hash(i, 31) % {len(methods)} AS INTEGER)] AS method,
            {countries_sql}[1 + CAST(hash(i, 53) % {len(countries)} AS INTEGER)] AS country,
            '/path/' || CAST(i % 500 AS VARCHAR) AS url,
            10 + CAST(hash(i, 71) % 4990 AS INTEGER) AS ottfb_ms,
            100 + CAST(hash(i, 89) % 49900 AS INTEGER) AS response_size_bytes
        FROM range({num_rows}) AS t(i)
        """
    )


# Representative dashboard-aggregate-style query.
_AGG_QUERY = """
    SELECT
        country,
        COUNT(*) AS requests,
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors_5xx,
        AVG(ottfb_ms) AS avg_ottfb,
        APPROX_QUANTILE(ottfb_ms, 0.95) AS p95_ottfb,
        SUM(response_size_bytes) / 1024 / 1024 AS total_mb
    FROM logs
    WHERE status >= 200 AND ottfb_ms < 10000
    GROUP BY country
    HAVING requests > 10
    ORDER BY requests DESC
"""


def _time_query_ms(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return int((time.perf_counter() - t0) * 1000)


def main() -> int:
    num_rows = _resolve_num_rows()
    scale_key = _scale_key_for(num_rows)
    print(f"[perf-emit] generating {num_rows:,}-row synthetic dataset (scale={scale_key})...", flush=True)
    con = duckdb.connect(":memory:")
    _generate_seed_data(con, num_rows)

    # Cold-path proxy: run NUM_RUNS_COLD times, take p95.
    cold_samples: list[int] = []
    for i in range(NUM_RUNS_COLD):
        # New connection per run to defeat statement / catalog caching.
        run_con = duckdb.connect(":memory:")
        _generate_seed_data(run_con, num_rows)
        ms = _time_query_ms(run_con, _AGG_QUERY)
        cold_samples.append(ms)
        print(f"  cold run {i + 1}/{NUM_RUNS_COLD}: {ms} ms", flush=True)
        run_con.close()

    # p95 across the cold samples — with N=5 that's max() since 5 * 0.95 = 4.75.
    cold_samples.sort()
    cold_p95 = cold_samples[-1]

    # Warm-path proxy: repeat against the SAME connection so DuckDB's
    # statement cache / metadata cache stays warm. Take p50.
    warm_samples = [_time_query_ms(con, _AGG_QUERY) for _ in range(NUM_RUNS_WARM)]
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
            "cold_path_36M_1h_iceberg_committed_p95_ms": cold_p95,
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
