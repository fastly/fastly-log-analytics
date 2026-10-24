#!/usr/bin/env python3
"""Reproducible local profiler for `get_insights` (Step 1 of Insights optimization).

Generates a deterministic 30-day (720h baseline + 1h window) synthetic dataset
using `scripts/lib/log_factory.py`, writes partitioned Parquet files backing the
service view, and measures per-phase wall-clock (`perf_counter`) and CPU
(`process_time` / `thread_time`) latencies across repeated runs:
  - history_and_schema (`MIN(timestamp)` + `DESCRIBE`)
  - scratch_table_build (`CREATE TABLE _insights_scratch.logs AS SELECT ...`)
  - window_total_count (`SELECT COUNT(*) ...`)
  - each coalesced aggregate (`coalesced:waf`, `coalesced:city`, `coalesced:url`, `coalesced:ip`)
  - each registered insight detection (`insight:<id>`)
  - end-to-end cold vs warm `get_insights` wall-clock and process CPU time
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.core.iceberg import get_arrow_schema
from backend.repositories._base import _safe_table
from backend.repositories.insights import registry
from backend.repositories.insights import repository as insights_repo
from scripts.lib.log_factory import cols_to_arrow_table, generate_columns


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] * (c - k) + s[c] * (k - f)


def build_synthetic_30d_dataset(
    con: duckdb.DuckDBPyConnection,
    parquet_dir: Path,
    service_name: str = "profile-svc",
    total_rows: int = 500_000,
    num_files: int = 10,
    seed: int = 42,
) -> dict[str, Any]:
    """Generate a deterministic 30-day synthetic dataset across `num_files` Parquet
    files using `scripts/lib/log_factory.py` and create the DuckDB view.
    """
    arrow_schema = get_arrow_schema()
    schema_names = set(arrow_schema.names)
    extra_str_cols = [c for c in ("ja3", "ja4", "region", "waf_sig", "cookie_session") if c not in schema_names]

    now = datetime.now(UTC)
    window_rows = max(1, total_rows // 20)  # 5% in the 1h window, 95% across 720h baseline
    baseline_rows = max(1, total_rows - window_rows)

    parquet_dir.mkdir(parents=True, exist_ok=True)
    n_base_files = max(1, num_files - 1)
    rows_per_file = max(1, baseline_rows // n_base_files)

    base_start_ms = int((now - timedelta(hours=720, minutes=50)).timestamp() * 1000)
    base_end_ms = int((now - timedelta(hours=1, minutes=1)).timestamp() * 1000)
    span_per_file = max(1, (base_end_ms - base_start_ms) // n_base_files)

    for file_idx in range(n_base_files):
        n = baseline_rows - rows_per_file * file_idx if file_idx == n_base_files - 1 else rows_per_file
        if n <= 0:
            continue
        f_start_ms = base_start_ms + file_idx * span_per_file
        f_end_ms = base_end_ms if file_idx == n_base_files - 1 else (f_start_ms + span_per_file)
        rng = np.random.default_rng(seed + file_idx)
        cols = generate_columns(n, f_start_ms, f_end_ms, "diurnal", rng)
        tbl = cols_to_arrow_table(cols, arrow_schema)
        for col_name in extra_str_cols:
            tbl = tbl.append_column(col_name, pa.array([""] * n, type=pa.string()))
        pq.write_table(tbl, parquet_dir / f"baseline_{file_idx:03d}.parquet")

    # Window file (last 1h)
    w_rng = np.random.default_rng(seed + 9999)
    w_start_ms = int((now - timedelta(minutes=58)).timestamp() * 1000)
    w_end_ms = int((now - timedelta(seconds=5)).timestamp() * 1000)
    w_cols = generate_columns(window_rows, w_start_ms, w_end_ms, "diurnal", w_rng)
    w_tbl = cols_to_arrow_table(w_cols, arrow_schema)
    for col_name in extra_str_cols:
        w_tbl = w_tbl.append_column(col_name, pa.array([""] * window_rows, type=pa.string()))
    pq.write_table(w_tbl, parquet_dir / "window_000.parquet")

    view_name = _safe_table(service_name)
    glob_path = str(parquet_dir / "*.parquet").replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * REPLACE (CAST("timestamp" AS TIMESTAMPTZ) AS "timestamp")
        FROM read_parquet('{glob_path}')
        """
    )
    return {
        "service_name": service_name,
        "view_name": view_name,
        "total_rows": baseline_rows + window_rows,
        "baseline_rows": baseline_rows,
        "window_rows": window_rows,
        "num_files": num_files,
    }


@contextlib.contextmanager
def _instrument_insights_phases(run_metrics: dict[str, Any]):
    """Wrap internal `con.cursor()` and insight definitions in `repository.py`
    to record per-phase wall-clock (`perf_counter`) and CPU (`thread_time`) ms
    without altering `get_insights` behavior.
    """
    phase_wall_ms: dict[str, float] = {}
    phase_cpu_ms: dict[str, float] = {}

    orig_definitions = list(registry.get_all())
    wrapped_definitions = []

    for d in orig_definitions:
        orig_proc = d.row_processor

        def _make_wrapped_proc(defn_id: str, proc_fn):
            def _wrapped_proc(rows: list[Any], ctx: dict[str, Any]):
                t0_w = time.perf_counter()
                t0_c = time.thread_time()
                try:
                    return proc_fn(rows, ctx)
                finally:
                    key = f"proc:{defn_id}"
                    phase_wall_ms[key] = phase_wall_ms.get(key, 0.0) + (time.perf_counter() - t0_w) * 1000.0
                    phase_cpu_ms[key] = phase_cpu_ms.get(key, 0.0) + (time.thread_time() - t0_c) * 1000.0

            return _wrapped_proc

        updates: dict[str, Any] = {"row_processor": _make_wrapped_proc(d.id, orig_proc)}
        if d.sql_template:
            updates["sql_template"] = f"/* insight:{d.id} */\n{d.sql_template}"
        d_copy = d.model_copy(update=updates)
        wrapped_definitions.append(d_copy)

    def _wrap_coalesced(key: str, fn):
        def _wrapped(*args: Any, **kwargs: Any):
            t0_w = time.perf_counter()
            t0_c = time.thread_time()
            try:
                return fn(*args, **kwargs)
            finally:
                phase_wall_ms[key] = phase_wall_ms.get(key, 0.0) + (time.perf_counter() - t0_w) * 1000.0
                phase_cpu_ms[key] = phase_cpu_ms.get(key, 0.0) + (time.thread_time() - t0_c) * 1000.0

        return _wrapped

    with (
        patch.object(registry, "get_all", return_value=wrapped_definitions),
        patch.object(
            insights_repo,
            "_coalesced_city_aggregates",
            _wrap_coalesced("coalesced:city", insights_repo._coalesced_city_aggregates),
        ),
        patch.object(
            insights_repo,
            "_coalesced_url_aggregates",
            _wrap_coalesced("coalesced:url", insights_repo._coalesced_url_aggregates),
        ),
        patch.object(
            insights_repo,
            "_coalesced_ip_security_aggregates",
            _wrap_coalesced("coalesced:ip", insights_repo._coalesced_ip_security_aggregates),
        ),
    ):
        yield phase_wall_ms, phase_cpu_ms

    run_metrics["phase_wall_ms"] = phase_wall_ms
    run_metrics["phase_cpu_ms"] = phase_cpu_ms


class _ProfilingConnProxy:
    """Thin proxy around a DuckDB connection that classifies and times every
    SQL execution on both the main connection and `.cursor()` child connections.
    """

    def __init__(
        self,
        raw_con: duckdb.DuckDBPyConnection,
        sql_timings_wall: dict[str, float],
        sql_timings_cpu: dict[str, float],
    ) -> None:
        self._con = raw_con
        self._wall = sql_timings_wall
        self._cpu = sql_timings_cpu

    def _classify_sql(self, sql: str) -> str:
        s = " ".join(sql.strip().split())
        if s.startswith("/* insight:") and "*/" in s:
            defn_id = s.split("/* insight:", 1)[1].split("*/", 1)[0].strip()
            return f"sql:{defn_id}"
        s_lower = s.lower()
        if "select min(timestamp) from" in s_lower:
            return "phase:min_timestamp"
        if s_lower.startswith("describe "):
            return "phase:describe_schema"
        if s_lower.startswith("attach ':memory:' as insights_scratch_"):
            return "phase:scratch_attach"
        if "create table insights_scratch_" in s_lower and ".insights_temp_" in s_lower:
            return "phase:scratch_table_build"
        if "create table insights_scratch_" in s_lower and ".insights_waf_" in s_lower:
            return "coalesced:waf"
        if "select count(*) from insights_scratch_" in s_lower:
            return "phase:window_total_count"
        if s_lower.startswith("detach insights_scratch_"):
            return "phase:scratch_detach"
        return "sql:other"

    def execute(self, sql: str, *args: Any, **kwargs: Any):
        label = self._classify_sql(sql)
        t0_w = time.perf_counter()
        t0_c = time.thread_time()
        try:
            return self._con.execute(sql, *args, **kwargs)
        finally:
            self._wall[label] = self._wall.get(label, 0.0) + (time.perf_counter() - t0_w) * 1000.0
            self._cpu[label] = self._cpu.get(label, 0.0) + (time.thread_time() - t0_c) * 1000.0

    def cursor(self) -> _ProfilingConnProxy:
        return _ProfilingConnProxy(self._con.cursor(), self._wall, self._cpu)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)


def run_profile(
    total_rows: int = 500_000,
    runs: int = 5,
    num_files: int = 10,
    window_hours: float = 1.0,
    baseline_hours: float = 720.0,
) -> dict[str, Any]:
    """Profile `get_insights` across `runs` cold runs and `runs` warm runs."""
    all_defns = registry.get_all()

    with tempfile.TemporaryDirectory(prefix="insights_profile_") as tmpdir:
        con = duckdb.connect(":memory:")
        dataset_meta = build_synthetic_30d_dataset(
            con,
            Path(tmpdir),
            service_name="profile-svc",
            total_rows=total_rows,
            num_files=num_files,
        )
        src = {"name": "profile-svc", "service_id": "svc-profile"}

        cold_wall_ms: list[float] = []
        cold_cpu_ms: list[float] = []
        warm_wall_ms: list[float] = []
        warm_cpu_ms: list[float] = []

        per_op_wall_runs: dict[str, list[float]] = {}
        per_op_cpu_runs: dict[str, list[float]] = {}

        mock_pop = {"IAD": (38.9531, -77.4565), "LAX": (33.9416, -118.4085), "JFK": (40.6413, -73.7781)}

        with (
            patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=mock_pop),
            patch("backend.core.duckdb.get_asn_names", return_value={}),
            patch("backend.core.duckdb._get_dma_map", return_value={}),
        ):
            for _run_idx in range(runs):
                sql_wall: dict[str, float] = {}
                sql_cpu: dict[str, float] = {}
                run_meta: dict[str, Any] = {}
                proxy_con = _ProfilingConnProxy(con, sql_wall, sql_cpu)

                with _instrument_insights_phases(run_meta) as (proc_wall, proc_cpu):
                    t0_w = time.perf_counter()
                    t0_c = time.process_time()
                    insights_repo.get_insights(
                        proxy_con,  # type: ignore[arg-type]
                        src,
                        window_hours=window_hours,
                        baseline_hours=baseline_hours,
                        service_id="svc-profile",
                        force_refresh=True,
                    )
                    cold_wall_ms.append((time.perf_counter() - t0_w) * 1000.0)
                    cold_cpu_ms.append((time.process_time() - t0_c) * 1000.0)

                # Measure warm cache hit immediately after cold populate
                t0_warm_w = time.perf_counter()
                t0_warm_c = time.process_time()
                insights_repo.get_insights(
                    con,
                    src,
                    window_hours=window_hours,
                    baseline_hours=baseline_hours,
                    service_id="svc-profile",
                    force_refresh=False,
                )
                warm_wall_ms.append((time.perf_counter() - t0_warm_w) * 1000.0)
                warm_cpu_ms.append((time.process_time() - t0_warm_c) * 1000.0)

                # Combine SQL + row_processor timings per insight and phase
                combined_wall: dict[str, float] = {}
                combined_cpu: dict[str, float] = {}

                for phase_key in (
                    "phase:min_timestamp",
                    "phase:describe_schema",
                    "phase:scratch_attach",
                    "phase:scratch_table_build",
                    "phase:window_total_count",
                    "phase:scratch_detach",
                    "coalesced:waf",
                    "coalesced:city",
                    "coalesced:url",
                    "coalesced:ip",
                ):
                    combined_wall[phase_key] = sql_wall.get(phase_key, 0.0) + proc_wall.get(phase_key, 0.0)
                    combined_cpu[phase_key] = sql_cpu.get(phase_key, 0.0) + proc_cpu.get(phase_key, 0.0)

                for d in all_defns:
                    op_key = f"insight:{d.id}"
                    combined_wall[op_key] = sql_wall.get(f"sql:{d.id}", 0.0) + proc_wall.get(f"proc:{d.id}", 0.0)
                    combined_cpu[op_key] = sql_cpu.get(f"sql:{d.id}", 0.0) + proc_cpu.get(f"proc:{d.id}", 0.0)

                for k, v in combined_wall.items():
                    per_op_wall_runs.setdefault(k, []).append(v)
                for k, v in combined_cpu.items():
                    per_op_cpu_runs.setdefault(k, []).append(v)

    op_summaries: list[dict[str, Any]] = []
    for k, w_vals in per_op_wall_runs.items():
        c_vals = per_op_cpu_runs.get(k, [0.0] * len(w_vals))
        op_summaries.append(
            {
                "operation": k,
                "wall_p50_ms": round(_percentile(w_vals, 50), 2),
                "wall_p95_ms": round(_percentile(w_vals, 95), 2),
                "cpu_p50_ms": round(_percentile(c_vals, 50), 2),
                "cpu_p95_ms": round(_percentile(c_vals, 95), 2),
            }
        )

    op_summaries_by_wall = sorted(op_summaries, key=lambda x: x["wall_p50_ms"], reverse=True)
    op_summaries_by_cpu = sorted(op_summaries, key=lambda x: x["cpu_p50_ms"], reverse=True)

    return {
        "dataset": dataset_meta,
        "runs": runs,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "cold_total": {
            "wall_p50_ms": round(_percentile(cold_wall_ms, 50), 2),
            "wall_p95_ms": round(_percentile(cold_wall_ms, 95), 2),
            "cpu_p50_ms": round(_percentile(cold_cpu_ms, 50), 2),
            "cpu_p95_ms": round(_percentile(cold_cpu_ms, 95), 2),
            "raw_wall_ms": [round(x, 2) for x in cold_wall_ms],
            "raw_cpu_ms": [round(x, 2) for x in cold_cpu_ms],
        },
        "warm_total": {
            "wall_p50_ms": round(_percentile(warm_wall_ms, 50), 2),
            "wall_p95_ms": round(_percentile(warm_wall_ms, 95), 2),
            "cpu_p50_ms": round(_percentile(warm_cpu_ms, 50), 2),
            "cpu_p95_ms": round(_percentile(warm_cpu_ms, 95), 2),
        },
        "top_10_by_wall_p50": op_summaries_by_wall[:10],
        "top_10_by_cpu_p50": op_summaries_by_cpu[:10],
        "all_operations": op_summaries_by_wall,
    }


def format_markdown_report(result: dict[str, Any]) -> str:
    ds = result["dataset"]
    cold = result["cold_total"]
    warm = result["warm_total"]
    lines = [
        "# Insights Performance Profile (Local Synthetic 30-Day Dataset)",
        "",
        "## Dataset & Methodology",
        f"- **Total Rows**: {ds['total_rows']:,} ({ds['baseline_rows']:,} across {result['baseline_hours']}h baseline + {ds['window_rows']:,} in {result['window_hours']}h window)",
        f"- **Parquet Files**: {ds['num_files']}",
        f"- **Repeated Runs**: {result['runs']}",
        "",
        "## End-to-End `get_insights` Summary",
        "| Mode | Wall p50 (ms) | Wall p95 (ms) | Process CPU p50 (ms) | Process CPU p95 (ms) |",
        "|---|---:|---:|---:|---:|",
        f"| **Cold (`force_refresh=True`)** | {cold['wall_p50_ms']:.2f} | {cold['wall_p95_ms']:.2f} | {cold['cpu_p50_ms']:.2f} | {cold['cpu_p95_ms']:.2f} |",
        f"| **Warm (5m TTL cache hit)** | {warm['wall_p50_ms']:.2f} | {warm['wall_p95_ms']:.2f} | {warm['cpu_p50_ms']:.2f} | {warm['cpu_p95_ms']:.2f} |",
        "",
        "## Top 10 Costs by Wall-Clock p50",
        "| Rank | Operation | Wall p50 (ms) | Wall p95 (ms) | Thread CPU p50 (ms) | Thread CPU p95 (ms) |",
        "|---:|---|---:|---:|---:|---:|",
    ]
    for idx, op in enumerate(result["top_10_by_wall_p50"], start=1):
        lines.append(
            f"| {idx} | `{op['operation']}` | {op['wall_p50_ms']:.2f} | {op['wall_p95_ms']:.2f} | {op['cpu_p50_ms']:.2f} | {op['cpu_p95_ms']:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Full Per-Phase & Per-Detection Breakdown",
            "| Operation | Wall p50 (ms) | Wall p95 (ms) | Thread CPU p50 (ms) | Thread CPU p95 (ms) |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for op in result["all_operations"]:
        lines.append(
            f"| `{op['operation']}` | {op['wall_p50_ms']:.2f} | {op['wall_p95_ms']:.2f} | {op['cpu_p50_ms']:.2f} | {op['cpu_p95_ms']:.2f} |"
        )

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Profile get_insights locally against synthetic 30d data.")
    parser.add_argument("--rows", type=int, default=500_000, help="Total synthetic rows to generate (default: 500000)")
    parser.add_argument("--runs", type=int, default=5, help="Number of cold/warm runs (default: 5)")
    parser.add_argument("--files", type=int, default=10, help="Number of Parquet files (default: 10)")
    parser.add_argument("--output", type=str, default="", help="Optional path to write Markdown report")
    parser.add_argument("--json-output", type=str, default="", help="Optional path to write JSON report")
    args = parser.parse_args(argv)

    res = run_profile(total_rows=args.rows, runs=args.runs, num_files=args.files)
    md = format_markdown_report(res)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(md)
    if args.json_output:
        json_path = Path(args.json_output)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(res, indent=2) + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
