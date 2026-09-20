#!/usr/bin/env python3
"""scripts/benchmark_dashboard.py.

Multi-Iteration Statistical Benchmark Suite for Fastly Log Analytics.
Measures API latency distributions (mean, median, p90, p95, p99, stddev, RPS)
across dashboard endpoints to ensure high-scale serving performance.

Usage:
  python scripts/benchmark_dashboard.py --url http://localhost:8000 --service-id <service-id> \
      --iterations 15 --warmup 3 --output artifacts/benchmarks/benchmark.json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore


@dataclass
class EndpointBenchmarkResult:
    route: str
    method: str
    iterations: int
    successful_requests: int
    failed_requests: int
    error_rate_pct: float
    total_duration_s: float
    requests_per_second: float
    mean_ms: float
    median_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    stddev_ms: float
    avg_bytes: int


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    k = (len(data) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    d0 = data[int(f)] * (c - k)
    d1 = data[int(c)] * (k - f)
    return d0 + d1


def benchmark_endpoint(
    client: httpx.Client,
    base_url: str,
    endpoint: str,
    params: dict[str, Any],
    iterations: int,
    warmup: int,
    concurrency: int,
    timeout: float,
) -> EndpointBenchmarkResult:
    url = f"{base_url.rstrip('/')}{endpoint}"
    print(f"\nBenchmarking [{endpoint}] (iterations={iterations}, warmup={warmup}, concurrency={concurrency})...")

    # Warmup phase
    for _ in range(warmup):
        try:
            client.get(url, params=params, timeout=timeout)
        except Exception:
            pass

    latencies_ms: list[float] = []
    payload_sizes: list[int] = []
    failures = 0
    t0 = time.perf_counter()

    def _make_request() -> tuple[bool, float, int]:
        req_start = time.perf_counter()
        try:
            resp = client.get(url, params=params, timeout=timeout)
            dur = (time.perf_counter() - req_start) * 1000.0
            if resp.status_code < 400:
                return True, dur, len(resp.content)
            return False, dur, len(resp.content)
        except Exception:
            dur = (time.perf_counter() - req_start) * 1000.0
            return False, dur, 0

    if concurrency <= 1:
        for _ in range(iterations):
            success, dur, size = _make_request()
            if success:
                latencies_ms.append(dur)
                payload_sizes.append(size)
            else:
                failures += 1
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(_make_request) for _ in range(iterations)]
            for fut in as_completed(futures):
                success, dur, size = fut.result()
                if success:
                    latencies_ms.append(dur)
                    payload_sizes.append(size)
                else:
                    failures += 1

    total_time = time.perf_counter() - t0
    sorted_latencies = sorted(latencies_ms)
    successful = len(sorted_latencies)
    rps = (successful + failures) / total_time if total_time > 0 else 0.0

    mean_ms = statistics.mean(sorted_latencies) if sorted_latencies else 0.0
    median_ms = statistics.median(sorted_latencies) if sorted_latencies else 0.0
    stddev_ms = statistics.stdev(sorted_latencies) if len(sorted_latencies) > 1 else 0.0
    min_ms = sorted_latencies[0] if sorted_latencies else 0.0
    max_ms = sorted_latencies[-1] if sorted_latencies else 0.0
    p90_ms = percentile(sorted_latencies, 90)
    p95_ms = percentile(sorted_latencies, 95)
    p99_ms = percentile(sorted_latencies, 99)
    avg_bytes = int(statistics.mean(payload_sizes)) if payload_sizes else 0
    err_rate = (failures / (successful + failures)) * 100.0 if (successful + failures) > 0 else 0.0

    print(
        f"  ✓ Mean: {mean_ms:.1f}ms | p50: {median_ms:.1f}ms | p95: {p95_ms:.1f}ms | p99: {p99_ms:.1f}ms | RPS: {rps:.1f}"
    )

    return EndpointBenchmarkResult(
        route=endpoint,
        method="GET",
        iterations=iterations,
        successful_requests=successful,
        failed_requests=failures,
        error_rate_pct=err_rate,
        total_duration_s=total_time,
        requests_per_second=rps,
        mean_ms=mean_ms,
        median_ms=median_ms,
        p90_ms=p90_ms,
        p95_ms=p95_ms,
        p99_ms=p99_ms,
        min_ms=min_ms,
        max_ms=max_ms,
        stddev_ms=stddev_ms,
        avg_bytes=avg_bytes,
    )


def format_markdown_table(results: list[EndpointBenchmarkResult], meta: dict[str, Any]) -> str:
    lines = [
        f"# Statistical Benchmark Report: {meta.get('env', 'Production')}",
        "",
        f"- **Timestamp**: `{meta.get('timestamp')}`",
        f"- **Base URL**: `{meta.get('base_url')}`",
        f"- **Service ID**: `{meta.get('service_id')}`",
        f"- **Iterations**: `{meta.get('iterations')}` (Warmup: `{meta.get('warmup')}`)",
        f"- **Concurrency**: `{meta.get('concurrency')}`",
        "",
        "| Endpoint | Success / Total | RPS | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | StdDev | Avg Payload |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    for r in results:
        lines.append(
            f"| `{r.route}` | {r.successful_requests}/{r.iterations} | {r.requests_per_second:.1f} | "
            f"{r.mean_ms:.1f} | {r.median_ms:.1f} | {r.p95_ms:.1f} | {r.p99_ms:.1f} | "
            f"±{r.stddev_ms:.1f} | {(r.avg_bytes / 1024):.1f} KB |"
        )

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8000", help="Base backend API URL.")
    parser.add_argument("--service-id", help="Target service ID for queries.")
    parser.add_argument("--env", default="elevation-high-scale", help="Environment label.")
    parser.add_argument("--iterations", type=int, default=15, help="Number of benchmark iterations per route.")
    parser.add_argument("--warmup", type=int, default=3, help="Warmup iterations before recording.")
    parser.add_argument("--concurrency", type=int, default=1, help="Concurrent client workers.")
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request HTTP timeout in seconds.")
    parser.add_argument("--output", help="Path to save JSON benchmark results.")
    parser.add_argument("--markdown-output", help="Path to save Markdown benchmark report.")

    args = parser.parse_args()

    if httpx is None:
        print("ERROR: httpx library is required. Run 'uv pip install httpx' or use 'uv run'.", file=sys.stderr)
        return 1

    service_id = args.service_id
    if not service_id:
        from backend.config import list_configs

        configs = list_configs()
        if configs:
            service_id = configs[0].get("service_id")
            print(f"Using default discovered service: {service_id}")

    endpoints_to_test = [
        ("/api/health", {}),
        ("/api/bootstrap", {}),
    ]

    if service_id:
        endpoints_to_test.extend(
            [
                ("/api/services/status", {"service_id": service_id}),
                ("/api/services/schema", {"service_id": service_id}),
                ("/api/dashboard", {"service_id": service_id, "sections": "core,topten", "chart_metric": "requests"}),
                ("/api/security", {"service_id": service_id}),
                ("/api/network", {"service_id": service_id}),
                ("/api/origin", {"service_id": service_id}),
                ("/api/performance", {"service_id": service_id}),
            ]
        )

    client = httpx.Client(timeout=args.timeout, verify=False)
    results: list[EndpointBenchmarkResult] = []

    print("\n============================================================")
    print("Fastly Log Analytics Dashboard Benchmark")
    print(f"  Target:      {args.url}")
    print(f"  Environment: {args.env}")
    print(f"  Service:     {service_id}")
    print(f"  Iterations:  {args.iterations} (Warmup: {args.warmup})")
    print("============================================================")

    for route, params in endpoints_to_test:
        res = benchmark_endpoint(
            client=client,
            base_url=args.url,
            endpoint=route,
            params=params,
            iterations=args.iterations,
            warmup=args.warmup,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
        results.append(res)

    meta = {
        "timestamp": datetime.now(UTC).isoformat(),
        "env": args.env,
        "base_url": args.url,
        "service_id": service_id,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "concurrency": args.concurrency,
    }

    report_data = {
        "metadata": meta,
        "results": [asdict(r) for r in results],
    }

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)
        print(f"\nSaved JSON benchmark report to: {out_path}")

    md_content = format_markdown_table(results, meta)
    if args.markdown_output:
        md_path = Path(args.markdown_output)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"Saved Markdown benchmark report to: {md_path}")

    print("\n" + md_content)
    return 0


if __name__ == "__main__":
    sys.exit(main())
