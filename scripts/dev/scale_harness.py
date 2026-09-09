#!/usr/bin/env python3
"""Bounded edge-load runner with FOS freshness and serving checkpoints."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import aiohttp


@dataclass(frozen=True)
class StageConfig:
    target_rps: float
    duration_s: float
    max_in_flight: int = 64


def parse_stages(value: str, *, allow_high_rate: bool = False) -> list[StageConfig]:
    stages: list[StageConfig] = []
    for item in value.split(","):
        parts = item.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"stage must be RPS:seconds, got {item!r}")
        rps, duration = float(parts[0]), float(parts[1])
        if rps <= 0 or duration <= 0:
            raise ValueError("stage RPS and duration must be positive")
        if rps > 1000 and not allow_high_rate:
            raise ValueError("stages above 1000 RPS require --allow-high-rate")
        stages.append(StageConfig(rps, duration))
    if not stages:
        raise ValueError("at least one stage is required")
    return stages


async def _request(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
) -> tuple[int, int]:
    started = time.perf_counter()
    try:
        async with session.get(url, headers=headers) as response:
            await response.read()
            return response.status, round((time.perf_counter() - started) * 1000)
    except (aiohttp.ClientError, TimeoutError):
        return 0, round((time.perf_counter() - started) * 1000)


async def run_stage(
    config: StageConfig,
    session: aiohttp.ClientSession,
    request_url: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    deadline = started + config.duration_s
    interval = 1.0 / config.target_rps
    pending: set[asyncio.Task[tuple[int, int]]] = set()
    statuses: Counter[str] = Counter()
    latencies: list[int] = []
    launched = 0

    while time.perf_counter() < deadline:
        due = started + launched * interval
        await asyncio.sleep(max(0.0, due - time.perf_counter()))
        while len(pending) >= config.max_in_flight:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                status, latency = task.result()
                statuses[str(status)] += 1
                latencies.append(latency)
        pending.add(asyncio.create_task(_request(session, request_url, headers)))
        launched += 1

    if pending:
        results = await asyncio.gather(*pending)
        for status, latency in results:
            statuses[str(status)] += 1
            latencies.append(latency)

    elapsed = max(time.perf_counter() - started, 0.001)
    successful = sum(count for status, count in statuses.items() if status.startswith("2"))
    ordered = sorted(latencies)
    return {
        "started_at": started_at.isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "target_rps": config.target_rps,
        "duration_s": config.duration_s,
        "max_in_flight": config.max_in_flight,
        "launched": launched,
        "completed": sum(statuses.values()),
        "successful": successful,
        "achieved_rps": round(successful / elapsed, 2),
        "status_counts": dict(statuses),
        "latency_ms": _percentiles(ordered),
    }


def _percentiles(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    return {
        "p50": values[min(len(values) - 1, math.ceil(len(values) * 0.50) - 1)],
        "p95": values[min(len(values) - 1, math.ceil(len(values) * 0.95) - 1)],
        "p99": values[min(len(values) - 1, math.ceil(len(values) * 0.99) - 1)],
        "max": values[-1],
    }


def _json_get(url: str, headers: dict[str, str]) -> dict[str, Any] | None:
    try:
        request = Request(url, headers=headers, method="GET")
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read())
        return payload if isinstance(payload, dict) else None
    except (OSError, URLError, TimeoutError, json.JSONDecodeError):
        return None


def _prometheus_value(base_url: str, query: str) -> float | None:
    payload = _json_get(
        f"{base_url.rstrip('/')}/api/v1/query?query={quote(query)}",
        {},
    )
    try:
        return float(payload["data"]["result"][0]["value"][1]) if payload else None
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def freshness_lag_seconds(latest_log_at: str | None, now: datetime | None = None) -> float | None:
    latest = _parse_timestamp(latest_log_at)
    current = now or datetime.now(UTC)
    if latest is None:
        return None
    return round(max(0.0, (current - latest).total_seconds()), 1)


def sample_checkpoint(
    backend: str,
    service_id: str,
    prometheus: str,
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    headers = {"x-fastly-service-id": service_id}
    health = _json_get(f"{backend.rstrip('/')}/api/admin/health-snapshot", headers)
    extents = _json_get(f"{backend.rstrip('/')}/api/log-extents", headers)
    checkpoint: dict[str, Any] = {
        "sampled_at": datetime.now(UTC).isoformat(),
        "health": health,
        "extents": extents,
        "freshness_lag_s": freshness_lag_seconds(extents.get("latest_log_at") if extents else None),
        "otel": {
            "http_p95_ms": _prometheus_value(
                prometheus,
                "histogram_quantile(0.95,sum by (le)(rate(http_server_duration_milliseconds_bucket[5m])))",
            ),
            "query_p95_ms": _prometheus_value(
                prometheus,
                "histogram_quantile(0.95,sum by (le)(rate(app_query_duration_ms_milliseconds_bucket[5m])))",
            ),
            "pool_wait_p95_ms": _prometheus_value(
                prometheus,
                "histogram_quantile(0.95,sum by (le)(rate(app_thread_wait_ms_milliseconds_bucket[5m])))",
            ),
        },
    }
    if start_time and end_time:
        checkpoint["analytics"] = probe_analytics_endpoints(backend, service_id, start_time, end_time)
    return checkpoint


def probe_analytics_endpoints(backend: str, service_id: str, start_time: str, end_time: str) -> list[dict[str, Any]]:
    probes = [
        ("/api/dashboard/aggregates", {"start_time": start_time, "end_time": end_time, "filters": {}}),
        (
            "/api/dashboard/field-values",
            {"start_time": start_time, "end_time": end_time, "field": "country", "limit": 100},
        ),
        ("/api/security/aggregates", {"start_time": start_time, "end_time": end_time, "filters": {}}),
        (
            "/api/network-health",
            {
                "start_time": start_time,
                "end_time": end_time,
                "filters": {},
                "metric": "health_score",
                "bucket_seconds": 60,
                "top_n": 30,
            },
        ),
        (
            "/api/origin/timeseries",
            {"start_time": start_time, "end_time": end_time, "filters": {}, "timeseries_percentile": "p95"},
        ),
        (
            "/api/origin/slow-urls",
            {
                "start_time": start_time,
                "end_time": end_time,
                "filters": {},
                "slow_urls_limit": 50,
                "slow_urls_min_requests": 10,
            },
        ),
        ("/api/performance/aggregates", {"start_time": start_time, "end_time": end_time, "filters": {}}),
    ]
    results = []
    for path, body in probes:
        request = Request(
            f"{backend.rstrip('/')}{path}",
            data=json.dumps(body).encode(),
            headers={"content-type": "application/json", "x-fastly-service-id": service_id},
            method="POST",
        )
        started = time.perf_counter()
        status = 0
        try:
            with urlopen(request, timeout=60) as response:
                response.read()
                status = response.status
        except HTTPError as exc:
            status = exc.code
        except (OSError, URLError, TimeoutError):
            status = 0
        results.append({"path": path, "status": status, "latency_ms": round((time.perf_counter() - started) * 1000)})
    return results


def render_report(result: dict[str, Any]) -> str:
    lines = [
        "# FOS Scale Harness Report",
        "",
        f"Run started: {result.get('started_at', 'unknown')}",
        "",
        "| Stage | Target RPS | Achieved RPS | Completed | 2xx | p95 ms | Freshness lag |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for stage in result.get("stages", []):
        stage_result = stage["result"]
        checkpoint = stage.get("checkpoint") or {}
        lag = checkpoint.get("freshness_lag_s")
        lines.append(
            f"| {stage['index']} | {stage_result['target_rps']:.0f} | "
            f"{stage_result['achieved_rps']:.2f} | {stage_result['completed']} | "
            f"{stage_result['successful']} | {stage_result['latency_ms']['p95'] or '-'} | "
            f"{f'{lag:.1f}s' if lag is not None else '-'} |"
        )
    lines.extend(
        [
            "",
            f"Final checkpoint freshness lag: {result.get('final_checkpoint', {}).get('freshness_lag_s', '-')} seconds",
            "Missing values indicate that the corresponding local telemetry endpoint was unavailable; they are not zero.",
        ]
    )
    return "\n".join(lines) + "\n"


async def run(args: argparse.Namespace) -> dict[str, Any]:
    stages = [
        StageConfig(stage.target_rps, stage.duration_s, args.max_in_flight)
        for stage in parse_stages(args.stages, allow_high_rate=args.allow_high_rate)
    ]
    headers = {"User-Agent": args.user_agent, "X-Load-Test": args.marker}
    report: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "url": args.url,
        "marker": args.marker,
        "stages": [],
    }
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=max(args.max_in_flight, 1), ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for index, stage in enumerate(stages, 1):
            result = await run_stage(stage, session, args.url, headers)
            checkpoint = sample_checkpoint(
                args.backend,
                args.service_id,
                args.prometheus,
                args.start_time,
                args.end_time,
            )
            report["stages"].append({"index": index, "result": result, "checkpoint": checkpoint})
    if args.settle_seconds:
        await asyncio.sleep(args.settle_seconds)
    report["final_checkpoint"] = sample_checkpoint(
        args.backend,
        args.service_id,
        args.prometheus,
        args.start_time,
        args.end_time,
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--url", required=True)
    run_parser.add_argument("--service-id", required=True)
    run_parser.add_argument("--backend", default="http://127.0.0.1:18002")
    run_parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    run_parser.add_argument("--stages", default="100:10,500:10,1000:10")
    run_parser.add_argument("--max-in-flight", type=int, default=64)
    run_parser.add_argument("--marker", default="fla-scale-harness")
    run_parser.add_argument("--user-agent", default="FlaScaleHarness/1.0")
    run_parser.add_argument("--start-time")
    run_parser.add_argument("--end-time")
    run_parser.add_argument("--allow-high-rate", action="store_true")
    run_parser.add_argument(
        "--settle-seconds",
        type=float,
        default=60,
        help="wait for delayed FOS delivery before the final checkpoint",
    )
    run_parser.add_argument("--output", type=Path, default=Path("performance-report/scale-harness.json"))
    args = parser.parse_args()
    if args.max_in_flight < 1:
        parser.error("--max-in-flight must be positive")
    report = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.output.with_suffix(".md").write_text(render_report(report), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
