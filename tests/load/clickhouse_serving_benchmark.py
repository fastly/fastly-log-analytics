"""Opt-in loopback Docker benchmark and historical hybrid evidence reader.

The live hybrid route was removed by ADR-20. New hybrid endpoint runs are
refused; historical report parsing does not reactivate dashboard dispatch.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

import httpx
from pydantic import Field, model_validator

from tests.load.clickhouse_serving_contract import (
    BenchmarkReport,
    Count,
    Freshness,
    Nonnegative,
    _StrictReport,
)

START = "2026-09-01T00:00:00Z"
END = "2026-09-02T00:00:00Z"
REQUEST: dict[str, Any] = {
    "start_time": START,
    "end_time": END,
    "fields": ["country"],
    "sections": ["core", "topten"],
    "chart_interval": "1 hour",
    "chart_metric": "requests",
    "filters": {},
}
HOURS = {
    3: 7907,
    4: 17903,
    5: 18416,
    6: 18091,
    7: 18254,
    8: 18187,
    9: 17752,
    10: 18469,
    11: 18081,
    12: 18252,
    13: 4095,
    15: 5374,
    16: 3593,
}
SOURCE_SQL = """
SELECT count(*) AS rows, min(timestamp) AS earliest, max(timestamp) AS latest,
count(*) FILTER (WHERE timestamp >= TIMESTAMPTZ '2026-09-01T00:00:00Z'
 AND timestamp <= TIMESTAMPTZ '2026-09-02T00:00:00Z'
 AND ip IS NOT NULL AND ip != '' AND url != '/rum-beacon'
 AND url NOT LIKE '/rum-beacon' || chr(63) || '%') AS eligible_rows,
count(*) FILTER (WHERE timestamp >= TIMESTAMPTZ '2026-09-06T00:00:00Z'
 AND timestamp <= TIMESTAMPTZ '2026-09-07T00:00:00Z') AS empty_rows
FROM logs
"""
Kind = Literal["bundle", "top_n", "time_series", "empty"]
Engine = Literal["ducklake", "clickhouse_hybrid"]
StageStatus = Literal["complete", "deadline", "memory_abort", "monitor_error", "warmup_failed"]


class Sample(_StrictReport):
    kind: Kind
    duration_ms: Nonnegative
    status_code: Annotated[int, Field(ge=100, le=599)] | None
    error: Literal["http", "timeout", "transport", "semantic", "engine", "telemetry"] | None
    dispatch: Literal["ducklake", "disabled", "hybrid", "unsupported_sections", "outside_coverage", "other", "unknown"]
    response_cached: bool | None
    duckdb_queries: Count
    clickhouse_calls: Count
    clickhouse_errors: Count
    rows_read: Count | None
    bytes_read: Count | None
    readiness_ms: Nonnegative | None
    semantic_digest: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")] | None


class Stage(_StrictReport):
    concurrency: Annotated[int, Field(ge=1, le=64)]
    requested_attempts: Annotated[int, Field(ge=30, le=256)]
    peak_in_flight: Count
    status: StageStatus
    elapsed_seconds: Nonnegative
    max_ms: Nonnegative
    backend_peak_memory_bytes: Count
    warmups: list[Sample]
    samples: list[Sample]
    report: BenchmarkReport | None

    @model_validator(mode="after")
    def consistency(self):
        if len(self.samples) > self.requested_attempts or self.peak_in_flight > self.concurrency:
            raise ValueError("attempt/admission budget exceeded")
        if self.status == "complete" and len(self.samples) != self.requested_attempts:
            raise ValueError("complete stage requires all attempts")
        if self.max_ms != max((s.duration_ms for s in self.samples), default=0.0):
            raise ValueError("maximum must include every attempt")
        if self.report is not None:
            if self.report.measured_requests != len(self.samples):
                raise ValueError("sample count differs from report")
            if self.report.error_count != sum(s.error is not None for s in self.samples):
                raise ValueError("sample errors differ from report")
            if self.report.elapsed_seconds != self.elapsed_seconds or self.report.concurrency != self.concurrency:
                raise ValueError("stage timing/concurrency differs from report")
            for percentile, observed in (
                (0.5, self.report.p50_ms),
                (0.95, self.report.p95_ms),
                (0.99, self.report.p99_ms),
            ):
                if observed != quantile([s.duration_ms for s in self.samples], percentile):
                    raise ValueError("quantiles must include every attempt")
        return self


class SourceCounts(_StrictReport):
    rows: Count
    earliest: Annotated[str, Field(pattern=r"^\d{4}-\d\d-\d\dT[\d:.+-]+$")]
    latest: Annotated[str, Field(pattern=r"^\d{4}-\d\d-\d\dT[\d:.+-]+$")]
    eligible_rows: Count
    empty_rows: Count


class RunEvidence(_StrictReport):
    fixture_alias: Literal["local-durable-one-service"]
    engine: Engine
    image_digest: Annotated[str, Field(pattern=r"^sha256:[a-f0-9]{64}$")]
    source_before: SourceCounts
    source_after: SourceCounts | None
    source_stable: bool
    request_sha256: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    started_at: str
    ended_at: str
    stages: list[Stage]
    corpus: list[Sample]
    backend_cgroup_limit_bytes: Count
    memory_abort_bytes: Count
    request_timeout_seconds: Nonnegative
    stage_admission_seconds: Nonnegative
    cache_ownership: Literal["response-off; warmed storage/OS; hybrid readiness uncached"]
    duplicate_visible_rows: Count | None
    replayed_artifacts: Count | None
    rebuild_seconds: Nonnegative | None

    @model_validator(mode="after")
    def consistency(self):
        if self.source_stable != (self.source_before == self.source_after):
            raise ValueError("source stability must reflect independent measurements")
        if any(s.report and s.report.engine != self.engine for s in self.stages):
            raise ValueError("stage engine differs from runtime")
        return self


class RecoveryEvidence(_StrictReport):
    rebuild_seconds: Nonnegative
    rebuild_rows: Count
    replayed_artifacts: Count
    published_artifacts: Count
    artifact_bytes: Count
    source_snapshot: Count
    source_catalog_unset: bool
    new_generation: bool
    canonical_digest_matches: bool
    duplicate_visible_rows: Count
    ch_success_without_ducklake_marker_deleted: Count
    fixture_rows: Count
    stopped_clickhouse_publication_seconds: Nonnegative
    publication_error_kind: Literal["ClickHouseError"]
    outage_state: Literal["pending", "failed"]
    false_publication: bool
    fixture_ducklake_marker_authorized_raw_deletes: Count
    retained_artifact_survives_raw_delete: bool
    outage_resume_seconds: Nonnegative
    outage_resume_published: Count
    outage_resume_activated: bool
    outage_resume_rows: Count
    replayed_duplicate_rows: Count
    physical_rows_before_merges: Count
    logical_rows_before_merges: Count
    outage_resume_digest_matches: bool
    fixture_cleanup_complete: bool


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--allow-prototype", action="store_true")
    p.add_argument("--base-url", default=None)
    p.add_argument("--backend-container", default=None)
    p.add_argument("--engine", choices=("ducklake", "clickhouse_hybrid"), required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--attempts", type=int, default=64)
    p.add_argument("--timeout", type=float, default=45)
    p.add_argument("--stage-seconds", type=float, default=180)
    p.add_argument("--memory-abort-mb", type=int, default=1536)
    p.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32, 64])
    return p


def validate_args(args) -> None:
    if args.engine == "clickhouse_hybrid":
        raise ValueError("hybrid dashboard route is archived (ADR-20); no live endpoint benchmark is available")
    if not args.apply or not args.allow_prototype or not args.base_url or not args.backend_container:
        raise ValueError("explicit apply, prototype, loopback URL and backend container required")
    url = urlsplit(args.base_url)
    if (
        url.scheme != "http"
        or url.hostname not in ("127.0.0.1", "::1")
        or url.username
        or url.password
        or url.query
        or url.fragment
        or url.path not in ("", "/")
    ):
        raise ValueError("only literal loopback HTTP origins allowed")
    if (
        not 30 <= args.attempts <= 256
        or not 1 <= args.timeout <= 60
        or not 1 <= args.stage_seconds <= 600
        or not 256 <= args.memory_abort_mb <= 2048
        or not args.concurrency
        or len(set(args.concurrency)) != len(args.concurrency)
        or any(c not in (1, 8, 32, 64) or c > args.attempts for c in args.concurrency)
    ):
        raise ValueError("invalid bounded load budget")
    output = args.output.resolve()
    local = Path("local-docs").resolve()
    if not output.is_relative_to(local) or output.suffix != ".json":
        raise ValueError("reports belong in gitignored local-docs JSON files")
    if subprocess.run(["git", "check-ignore", "-q", str(output)], check=False).returncode:
        raise ValueError("report output must be gitignored")


def runtime(container: str) -> tuple[str, int]:
    data = json.loads(subprocess.check_output(["docker", "inspect", container], timeout=10))[0]
    env = dict(item.split("=", 1) for item in data["Config"]["Env"])
    files = data["Config"]["Labels"].get("com.docker.compose.project.config_files", "")
    if (
        not all(
            name in files
            for name in (
                "docker-compose.multipod.yml",
                "docker-compose.observability.yml",
                "docker-compose.clickhouse-prototype.yml",
            )
        )
        or env.get("INGEST_MODE") != "celery"
        or env.get("SERVING_MODE") != "durable"
        or env.get("CLICKHOUSE_ENABLED") not in ("true", "false")
        or not env.get("METADATA_DSN", "").startswith("postgres")
        or not env.get("DUCKLAKE_CATALOG", "").startswith("postgres")
        or not env.get("CLICKHOUSE_HOST")
    ):
        raise ValueError("backend is not the explicitly configured prototype")
    return data["Image"], data["HostConfig"]["Memory"]


async def memory_bytes(container: str) -> int:
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "exec",
        container,
        "cat",
        "/sys/fs/cgroup/memory.current",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), 5)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise
    if proc.returncode:
        raise RuntimeError("memory monitor failed")
    return int(stdout)


def quantile(samples: list[float], p: float) -> float:
    values = sorted(samples)
    index = (len(values) - 1) * p
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (values[hi] - values[lo]) * (index - lo)


def request_for(kind: Kind) -> tuple[str, dict]:
    body = dict(REQUEST)
    endpoint = "/api/dashboard/bundle"
    if kind == "top_n":
        endpoint, body["sections"] = "/api/dashboard/aggregates", ["topten"]
    elif kind == "time_series":
        endpoint = "/api/dashboard/aggregates"
        body.pop("sections")
        body.update(include_time_series=True, include_conn_requests=False, include_map_data=False)
    elif kind == "empty":
        body.update(start_time="2026-09-06T00:00:00Z", end_time="2026-09-07T00:00:00Z")
    return endpoint, body


def check_semantics(payload: dict, kind: Kind) -> str:
    a = payload["aggregates"] if kind in ("bundle", "empty") else payload
    total = 0 if kind == "empty" else 184374
    assert a["total_rows"] == total
    assert a["interval"] == ("1 minute" if kind == "top_n" else "1 hour") and a["metric"] == "requests"
    if kind != "empty":
        assert a["data"]["country"]["total"] == total
        assert a["data"]["country"]["top"] == [{"value": "US", "count": total, "label": None}]
    else:
        assert not a["map_data"] and not a["time_series"]
    if kind in ("bundle", "empty"):
        assert payload["top_bots"] is None
    if kind == "bundle":
        assert a["data"]["conn_requests"]["top"] == [{"value": "1", "count": total, "label": None}]
        assert a["data"]["conn_requests"]["total"] == total
        assert a["map_data"] == [{"country": "US", "count": total}]
    if kind in ("bundle", "time_series"):
        points = a["time_series"]
        assert len(points) == len(HOURS)
        for point, (hour, count) in zip(points, HOURS.items(), strict=True):
            assert datetime.fromisoformat(point["time"]) == datetime(2026, 9, 1, hour, tzinfo=UTC)
            assert point["value"] == count and point["category"] is None and point["baseline"] is None
    semantic = {k: a[k] for k in ("total_rows", "data", "time_series", "map_data", "interval", "metric")}
    return hashlib.sha256(json.dumps(semantic, sort_keys=True).encode()).hexdigest()


def summarize_payload(payload: dict, kind: Kind, engine: Engine) -> dict:
    a = payload.get("aggregates", payload)
    sections = a.get("_section_timings", [])
    dispatch = next((s["section"][7:] for s in sections if s["section"].startswith("engine:")), "unknown")
    calls = [json.loads(c["details"]) for c in payload.get("_debug_calls", []) if c.get("service") == "ClickHouse"]
    if engine == "ducklake" and dispatch == "unknown" and payload.get("_debug_queries") and not calls:
        dispatch = "ducklake"
    result = {
        "dispatch": dispatch
        if dispatch in ("ducklake", "disabled", "hybrid", "unsupported_sections", "outside_coverage")
        else "other",
        "response_cached": payload.get("_is_cached"),
        "duckdb_queries": len(payload.get("_debug_queries", [])),
        "clickhouse_calls": len(calls),
        "clickhouse_errors": sum(c["outcome"] != "success" for c in calls),
        "rows_read": sum(c["rows_read"] for c in calls),
        "bytes_read": sum(c["bytes_read"] for c in calls),
        "readiness_ms": sum(s["time_ms"] for s in sections if s["section"] == "clickhouse:readiness"),
        "semantic_digest": None,
        "error": None,
    }
    try:
        result["semantic_digest"] = check_semantics(payload, kind)
    except (AssertionError, KeyError, TypeError, ValueError):
        result["error"] = "semantic"
    expected = (
        ("ducklake" if dispatch == "ducklake" else "disabled")
        if engine == "ducklake"
        else (
            "hybrid"
            if kind == "bundle"
            else "unsupported_sections"
            if kind in ("top_n", "time_series")
            else "outside_coverage"
        )
    )
    if dispatch != expected or (dispatch != "hybrid" and calls):
        result["error"] = "engine"
    if (
        result["clickhouse_errors"]
        or payload.get("_is_cached") is not False
        or not sections
        or (dispatch == "hybrid" and (len(calls) < 3 or result["readiness_ms"] <= 0))
    ):
        result["error"] = "telemetry"
    return result


async def attempt(client: httpx.AsyncClient, service: str, kind: Kind, engine: Engine, timeout: float) -> Sample:
    started = time.perf_counter()
    values: dict[str, Any] = dict(
        kind=kind,
        status_code=None,
        error=None,
        dispatch="unknown",
        response_cached=None,
        duckdb_queries=0,
        clickhouse_calls=0,
        clickhouse_errors=0,
        rows_read=None,
        bytes_read=None,
        readiness_ms=None,
        semantic_digest=None,
    )
    try:
        endpoint, body = request_for(kind)
        # wait_for bounds the entire response, not just each socket read.
        async with asyncio.timeout(timeout):
            response = await client.post(endpoint, params={"service_id": service}, json=body)
            values["status_code"] = response.status_code
            if response.status_code != 200:
                values["error"] = "http"
            else:
                values.update(summarize_payload(response.json(), kind, engine))
    except (TimeoutError, httpx.TimeoutException):
        values["error"] = "timeout"
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        values["error"] = "transport"
    return Sample(duration_ms=(time.perf_counter() - started) * 1000, **values)


async def stage(client, service, args, concurrency: int, freshness: Freshness, ingest_lag: float) -> Stage:
    peak_memory = await memory_bytes(args.backend_container)
    if peak_memory >= args.memory_abort_mb * 1024**2:
        return Stage(
            concurrency=concurrency,
            requested_attempts=args.attempts,
            peak_in_flight=0,
            status="memory_abort",
            elapsed_seconds=0.0,
            max_ms=0.0,
            backend_peak_memory_bytes=peak_memory,
            warmups=[],
            samples=[],
            report=None,
        )
    warmups = [await attempt(client, service, "bundle", args.engine, args.timeout) for _ in range(3)]
    samples: list[Sample] = []
    status: StageStatus = "complete" if all(s.error is None for s in warmups) else "warmup_failed"
    in_flight = peak = admitted = 0
    peak_memory = max(peak_memory, await memory_bytes(args.backend_container))
    if peak_memory >= args.memory_abort_mb * 1024**2:
        status = "memory_abort"
    started = time.perf_counter()
    done = asyncio.Event()

    async def monitor():
        nonlocal status, peak_memory
        while not done.is_set():
            try:
                peak_memory = max(peak_memory, await memory_bytes(args.backend_container))
                if peak_memory >= args.memory_abort_mb * 1024**2:
                    status = "memory_abort"
            except (TimeoutError, RuntimeError, ValueError):
                status = "monitor_error"
            try:
                await asyncio.wait_for(done.wait(), 1)
            except TimeoutError:
                pass

    async def worker():
        nonlocal in_flight, peak, admitted, status
        while admitted < args.attempts and status == "complete":
            if time.perf_counter() - started >= args.stage_seconds:
                status = "deadline"
                break
            admitted += 1
            in_flight += 1
            peak = max(peak, in_flight)
            samples.append(await attempt(client, service, "bundle", args.engine, args.timeout))
            in_flight -= 1

    watcher = asyncio.create_task(monitor())
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - started
    done.set()
    await watcher
    errors = sum(s.error is not None for s in samples)
    report = None
    if len(samples) >= 30:
        latencies = [s.duration_ms for s in samples]
        report = BenchmarkReport(
            schema_version=1,
            engine=args.engine,
            dataset_size_rows=190334,
            eligible_window_rows=184374,
            concurrency=concurrency,
            warmup_requests=3,
            measured_requests=len(samples),
            elapsed_seconds=elapsed,
            achieved_throughput_rps=(len(samples) - errors) / elapsed,
            p50_ms=quantile(latencies, 0.5),
            p95_ms=quantile(latencies, 0.95),
            p99_ms=quantile(latencies, 0.99),
            ingest_lag_seconds=ingest_lag,
            error_count=errors,
            freshness=freshness,
            clickhouse_ingest_lag_seconds=None,
            clickhouse_rebuild_seconds=None,
        )
    return Stage(
        concurrency=concurrency,
        requested_attempts=args.attempts,
        peak_in_flight=peak,
        status=status,
        elapsed_seconds=elapsed,
        max_ms=max((s.duration_ms for s in samples), default=0.0),
        backend_peak_memory_bytes=peak_memory,
        warmups=warmups,
        samples=samples,
        report=report,
    )


def ledger(container: str) -> tuple[Freshness, float]:
    # Values never enter argv or output: sole service is resolved in-container.
    script = """
import json,os,psycopg
from backend import config
service=config.list_configs()[0]['service_id']
with psycopg.connect(os.environ['METADATA_DSN']) as c:
 r=c.execute('SELECT max(committed_at-discovered_at),max(committed_at),max(published_at) FROM ingest_ledger WHERE service_id=%s AND status=%s',(service,'committed')).fetchone()
print(json.dumps(r,default=str))
"""
    lag, commit, publication = json.loads(
        subprocess.check_output(
            ["docker", "exec", container, "python", "-c", script],
            timeout=20,
        )
    )

    def timestamp(value):
        return (
            datetime.fromtimestamp(float(value), UTC)
            if isinstance(value, (int, float))
            else datetime.fromisoformat(value)
        )

    return Freshness(
        observed_at=datetime.now(UTC),
        event_watermark=datetime(2026, 9, 7, 19, 41, 25, tzinfo=UTC),
        window_start=datetime.fromisoformat(START),
        window_end=datetime.fromisoformat(END),
        last_commit_at=timestamp(commit),
        last_publication_at=timestamp(publication),
    ), float(lag)


async def source(client, service) -> SourceCounts:
    response = await client.post(
        "/api/query", params={"service_id": service}, json={"sql": SOURCE_SQL, "dataset": "logs"}
    )
    response.raise_for_status()
    return SourceCounts.model_validate(response.json()["data"][0])


async def run(args) -> RunEvidence:
    validate_args(args)
    image, limit = runtime(args.backend_container)
    freshness, lag = ledger(args.backend_container)
    started = datetime.now(UTC).isoformat()
    async with httpx.AsyncClient(
        base_url=args.base_url,
        trust_env=False,
        follow_redirects=False,
        timeout=args.timeout,
        limits=httpx.Limits(max_connections=64, max_keepalive_connections=0),
        headers={"x-debug-responses": "1"},
    ) as client:
        services = (await client.get("/api/services")).json()["services"]
        if len(services) != 1:
            raise ValueError("benchmark requires exactly one local fixture")
        service = services[0]["service_id"]
        before = await source(client, service)
        if before.rows != 190334 or before.eligible_rows != 184374 or before.empty_rows != 0:
            raise ValueError("frozen dataset changed")
        if datetime.fromisoformat(before.latest) != freshness.event_watermark:
            raise ValueError("frozen event watermark changed")
        evidence = RunEvidence(
            fixture_alias="local-durable-one-service",
            engine=args.engine,
            image_digest=image,
            source_before=before,
            source_after=None,
            source_stable=False,
            request_sha256=hashlib.sha256(json.dumps(REQUEST, sort_keys=True).encode()).hexdigest(),
            started_at=started,
            ended_at=datetime.now(UTC).isoformat(),
            stages=[],
            corpus=[],
            backend_cgroup_limit_bytes=limit,
            memory_abort_bytes=args.memory_abort_mb * 1024**2,
            request_timeout_seconds=args.timeout,
            stage_admission_seconds=args.stage_seconds,
            cache_ownership="response-off; warmed storage/OS; hybrid readiness uncached",
            duplicate_visible_rows=None,
            replayed_artifacts=None,
            rebuild_seconds=None,
        )
        try:
            for concurrency in args.concurrency:
                measured = await stage(client, service, args, concurrency, freshness, lag)
                evidence.stages.append(measured)
                args.output.write_text(evidence.model_dump_json(indent=2) + "\n")
                if measured.status != "complete":
                    break
            if evidence.stages[-1].status == "complete":
                for kind in ("bundle", "top_n", "time_series", "empty"):
                    evidence.corpus.append(await attempt(client, service, kind, args.engine, args.timeout))
            evidence.source_after = await source(client, service)
            evidence.source_stable = before == evidence.source_after
        finally:
            evidence.ended_at = datetime.now(UTC).isoformat()
            args.output.write_text(evidence.model_dump_json(indent=2) + "\n")
    return evidence


def main() -> int:
    try:
        result = asyncio.run(run(parser().parse_args()))
        for measured in result.stages:
            r = measured.report
            print(
                json.dumps(
                    {
                        "engine": result.engine,
                        "concurrency": measured.concurrency,
                        "status": measured.status,
                        "attempts": len(measured.samples),
                        "errors": sum(s.error is not None for s in measured.samples),
                        "rps": r.achieved_throughput_rps if r else None,
                        "p50_ms": r.p50_ms if r else None,
                        "p95_ms": r.p95_ms if r else None,
                        "p99_ms": r.p99_ms if r else None,
                    }
                )
            )
        return int(
            not result.source_stable
            or any(s.status != "complete" or any(x.error for x in s.samples) for s in result.stages)
            or any(s.error for s in result.corpus)
        )
    except Exception as exc:
        # Never print HTTP bodies, Docker environments, exception messages or SQL.
        print(json.dumps({"benchmark_error": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
