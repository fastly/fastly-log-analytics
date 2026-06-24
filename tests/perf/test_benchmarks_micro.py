"""Hot-path micro-benchmarks.

Audit follow-up (R-9 tooling). The 100K/500K/1M perf gates in CI measure
end-to-end query wall time at scale (scripts/emit_perf_latest.py +
scripts/perf_gate.sh). Those catch regressions in the integration of
the pipeline. This file complements them by measuring per-call cost on
the pure-Python helpers those queries build on:

  - HyperLogLog: add / count / merge / to_bytes / from_bytes. The
    cross-hour distinct-IP query path serialises sketches per hour to
    parquet and merges on read; a regression here scales linearly
    with hour-count.
  - _safe_table_name: invoked on every connection prepare path and
    every QueryRunner construction. Cheap by design — the bench pins
    it stays that way.
  - escape_sql_literal: invoked on every interpolated user/object-key
    value in ingest SQL. Same shape — pins the cheap-path contract.

Run with:

    uv run pytest tests/perf/test_benchmarks_micro.py --benchmark-only

Not timed by the default suite: pytest-benchmark auto-disables under the
`-n 4` xdist addopts (parallel timing is unreliable), and the resulting
startup warning is suppressed by message in pyproject `filterwarnings`. So
`uv run pytest` collects these as ordinary (un-timed) tests and stays under
wall-clock budget; `--benchmark-only` (which forces serial execution) is the
way to actually time them. pytest-benchmark can auto-detect regressions in a
CI run by comparing against a saved baseline (`--benchmark-autosave` +
`--benchmark-compare`); the project hasn't wired that pipeline yet — the file
lands first so the benches exist + can be invoked manually before the
autosave + gate ship.

Why this file isn't in tests/core or tests/utils:
  - Co-located with the existing perf harness (tests/perf/baseline.json,
    tests/perf/__init__.py) so the perf tooling story is in one place.
  - Marked with @pytest.mark.benchmark via the conftest below so a
    future ``-m "not benchmark"`` filter can exclude it without
    touching the file body.
"""

from __future__ import annotations

import pytest

from backend.core.duckdb import _safe_table_name
from backend.utils.hll import HyperLogLog, merge_sketches
from backend.utils.sql_validator import escape_sql_literal

pytestmark = pytest.mark.benchmark


# ── HyperLogLog (cross-hour distinct-IP query) ──────────────────────────────


def test_bench_hll_add_1k_items(benchmark):
    """Insert 1000 IPv4-shaped strings into a fresh sketch.

    Represents one cron-tick's worth of distinct-IP recording for a
    busy hour. Regression target: a 2× slowdown here would make the
    sync cron's HLL build the bottleneck on large hours.
    """
    items = [f"203.0.113.{i % 256}" for i in range(1000)]

    def _go():
        sketch = HyperLogLog()
        for item in items:
            sketch.add(item)
        return sketch

    result = benchmark(_go)
    assert result.count() > 0


def test_bench_hll_count_after_100k_adds(benchmark):
    """Estimate cardinality on a fully-populated sketch.

    The dashboard's distinct-IP card queries this on every render.
    Pins the estimator's per-call cost at ``count()`` time, separate
    from the ``add()`` cost above.
    """
    sketch = HyperLogLog()
    for i in range(100_000):
        sketch.add(f"203.0.113.{i % 256}.{i // 256}")

    estimate = benchmark(sketch.count)
    assert estimate > 0


def test_bench_hll_merge_24_hours(benchmark):
    """Merge 24 per-hour sketches into a daily roll-up.

    This is the cross-hour aggregate path: each hourly Iceberg partition
    carries a sketch; the daily distinct-IP query merges 24 of them.
    Bench pins per-merge cost so a regression at the registry width
    or per-bucket max formula surfaces.
    """
    sketches = []
    for hour in range(24):
        s = HyperLogLog()
        for i in range(5_000):
            s.add(f"hour{hour}:198.51.100.{i % 256}.{i // 256}")
        sketches.append(s)

    def _go():
        return merge_sketches(sketches)

    merged = benchmark(_go)
    assert merged is not None
    assert merged.count() > 0


def test_bench_hll_serialize_roundtrip(benchmark):
    """to_bytes → from_bytes round-trip on a populated sketch.

    The parquet-stored sketches go through this on every read. Cheap
    today (~10 µs); a future format change that adds a checksum or
    bit-packing tweak should keep it cheap.
    """
    sketch = HyperLogLog()
    for i in range(10_000):
        sketch.add(f"192.0.2.{i % 256}.{i // 256}")
    blob = sketch.to_bytes()

    def _roundtrip():
        return HyperLogLog.from_bytes(blob).to_bytes()

    out = benchmark(_roundtrip)
    assert out == blob


# ── SQL utility hot paths ───────────────────────────────────────────────────


def test_bench_safe_table_name(benchmark):
    """``_safe_table_name`` is called on every QueryRunner construction
    and every per-service view rebind. The contract is "fast, regex-
    based, no allocation surprises" — bench pins per-call cost so a
    future "audit-log every call" or "validate against catalog" change
    lands deliberately.
    """
    name = "svc-test-account-name-with-dashes-and-some-numbers-42"
    result = benchmark(_safe_table_name, name)
    assert result.startswith("logs_")


def test_bench_escape_sql_literal_typical(benchmark):
    """Escape a typical FOS object key. Hot on every ingest SQL build —
    one call per file per chunk. The fast path here is the no-quote
    case (no replacements needed).
    """
    key = "raw/2026-05-15/12/2026-05-15T12-34-56.svc-name.gz"
    result = benchmark(escape_sql_literal, key)
    assert result == key  # no single quotes → returned unchanged


def test_bench_escape_sql_literal_with_quotes(benchmark):
    """Escape a key containing single quotes — the worst-case branch.
    Should still be cheap (single str.replace). Pins the no-allocation-
    spike contract for the slow branch."""
    bad_key = "raw/2026-05-15/O'Brien's-bucket/file.gz"
    result = benchmark(escape_sql_literal, bad_key)
    assert "''" in result  # quotes doubled per SQL standard
