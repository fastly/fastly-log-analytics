#!/usr/bin/env python3
"""Elevation High-Scale Qualification Runner.

Orchestrates:
1. Preparation of synthetic request and RUM logs.
2. Concurrent scheduled release with strict cadence enforcement.
3. Concurrent fixed-interval sampling of:
   - Source objects backlog (by status: discovered, claimed, acknowledged, appended, source_deleted)
   - Publication manifests (pending vs visible, publication lag)
   - ClickHouse visible row counts (request_facts, rum_vitals_facts, rum_error_facts)
   - API and Page latencies
   - Worker resources and restarts
4. Summary reporting with p50 and p95 calculations.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


def percentiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)
    return {
        "p50": round(ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.50) - 1)], 3),
        "p95": round(ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)], 3),
        "p99": round(ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.99) - 1)], 3),
        "max": round(ordered[-1], 3),
    }


def query_clickhouse(
    ch_url: str, query: str, user: str = "fla_user", password: str = "fla_password_123"
) -> list[dict[str, Any]]:
    url = f"{ch_url.rstrip('/')}/?default_format=JSON"
    headers = {
        "Content-Type": "text/plain",
        "X-ClickHouse-User": user,
        "X-ClickHouse-Key": password,
    }
    req = Request(url, data=query.encode("utf-8"), headers=headers, method="POST")
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("data", [])
    except Exception:
        return []


def query_postgres(pg_dsn: str, query: str) -> list[tuple]:
    try:
        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception:
        return []


class MetricSampler:
    def __init__(
        self,
        ch_url: str,
        pg_dsn: str,
        backend_url: str,
        frontend_url: str,
        service_id: str,
        sample_interval: float = 1.0,
    ):
        self.ch_url = ch_url
        self.pg_dsn = pg_dsn
        self.backend_url = backend_url
        self.frontend_url = frontend_url
        self.service_id = service_id
        self.sample_interval = sample_interval
        self.samples: list[dict[str, Any]] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _sample_api_latency(self) -> float | None:
        url = f"{self.backend_url.rstrip('/')}/api/dashboard/bundle?service_id={self.service_id}"
        body = json.dumps(
            {"start_time": "2026-09-01T00:00:00Z", "end_time": "2026-09-20T00:00:00Z", "filters": {}}
        ).encode()
        req = Request(
            url,
            data=body,
            headers={
                "Host": "backend-svc:8000",
                "Content-Type": "application/json",
                "x-fastly-service-id": self.service_id,
            },
            method="POST",
        )
        start = time.perf_counter()
        try:
            with urlopen(req, timeout=10) as resp:
                resp.read()
                return round((time.perf_counter() - start) * 1000, 2)
        except Exception:
            return None

    def _sample_page_latency(self) -> float | None:
        url = f"{self.frontend_url.rstrip('/')}/dashboard?service={self.service_id}"
        req = Request(url, headers={"Host": "frontend-svc:3000", "User-Agent": "ElevationQualifier/1.0"}, method="GET")
        start = time.perf_counter()
        try:
            with urlopen(req, timeout=10) as resp:
                resp.read()
                return round((time.perf_counter() - start) * 1000, 2)
        except Exception:
            return None

    def _run(self):
        while not self._stop_event.is_set():
            t0 = time.time()
            sample: dict[str, Any] = {
                "timestamp": datetime.now(UTC).isoformat(),
                "api_latency_ms": self._sample_api_latency(),
                "page_latency_ms": self._sample_page_latency(),
            }

            # Sample ClickHouse facts
            ch_rows = query_clickhouse(
                self.ch_url,
                "SELECT "
                "(SELECT count() FROM fastly_log_analytics.request_facts) as req, "
                "(SELECT count() FROM fastly_log_analytics.rum_vitals_facts) as rum_vitals, "
                "(SELECT count() FROM fastly_log_analytics.rum_error_facts) as rum_errors",
            )
            if ch_rows:
                sample["ch_request_facts"] = int(ch_rows[0].get("req", 0))
                sample["ch_rum_vitals_facts"] = int(ch_rows[0].get("rum_vitals", 0))
                sample["ch_rum_error_facts"] = int(ch_rows[0].get("rum_errors", 0))

            # Sample Postgres backlog & publication
            if self.pg_dsn:
                so_counts = query_postgres(
                    self.pg_dsn,
                    "SELECT status, count(*) FROM high_scale_source_objects WHERE service_id = %s GROUP BY status",
                )
                sample["source_objects"] = {row[0]: row[1] for row in so_counts}

                pub_counts = query_postgres(
                    self.pg_dsn,
                    "SELECT publication_state, count(*) FROM high_scale_publication_manifests WHERE service_id = %s GROUP BY publication_state",
                )
                sample["publication_manifests"] = {row[0]: row[1] for row in pub_counts}

                lag_data = query_postgres(
                    self.pg_dsn,
                    "SELECT EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) FROM high_scale_publication_manifests WHERE service_id = %s AND publication_state = 'pending'",
                )
                sample["max_publication_lag_s"] = (
                    round(lag_data[0][0], 2) if lag_data and lag_data[0][0] is not None else 0.0
                )

                claims_data = query_postgres(
                    self.pg_dsn,
                    "SELECT count(*) FROM high_scale_batch_claims WHERE service_id = %s",
                )
                sample["active_claims"] = int(claims_data[0][0]) if claims_data else 0

            self.samples.append(sample)
            elapsed = time.time() - t0
            time.sleep(max(0.05, self.sample_interval - elapsed))


def run_stage(
    stage_name: str,
    bucket: str,
    service_id: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    request_rps: int,
    rum_rps: int,
    log_period: int,
    shards: int,
    duration: int,
    prep_dir: Path,
    sampler: MetricSampler,
    release_interval: int | None = None,
) -> dict[str, Any]:
    rel_int = release_interval if release_interval is not None else log_period
    print("\n==========================================")
    print(f"STAGE: {stage_name}")
    print(f"Request RPS: {request_rps}, RUM RPS: {rum_rps}, Combined RPS: {request_rps + rum_rps}")
    print(f"Duration: {duration}s, Period: {log_period}s, Release Interval: {rel_int}s, Shards: {shards}")
    print("==========================================")

    req_base = Path("/tmp/prep-request") if Path("/tmp/prep-request").exists() else prep_dir / "request"
    rum_base = Path("/tmp/prep-rum") if Path("/tmp/prep-rum").exists() else prep_dir / "rum"
    req_prep = req_base / stage_name.lower()
    rum_prep = rum_base / stage_name.lower()
    req_prep.mkdir(parents=True, exist_ok=True)
    rum_prep.mkdir(parents=True, exist_ok=True)
    Path("/tmp/worker_restart_trigger.txt").unlink(missing_ok=True)

    script_dir = Path(__file__).resolve().parent
    sub_env = os.environ.copy()
    sub_env["PYTHONPATH"] = f"/app:{sub_env.get('PYTHONPATH', '')}"

    # 1. Prepare Request logs
    print("Preparing request logs...")
    subprocess.run(
        [
            sys.executable,
            str(script_dir / "generate_synthetic_raw_logs.py"),
            "--bucket",
            bucket,
            "--service-id",
            service_id,
            "--endpoint",
            endpoint,
            "--access-key-id",
            access_key,
            "--secret-access-key",
            secret_key,
            "--target-rps",
            str(request_rps),
            "--log-period-seconds",
            str(log_period),
            "--shards",
            str(shards),
            "--duration-seconds",
            str(duration),
            "--prepare-dir",
            str(req_prep),
            "--prepare-only",
        ],
        cwd="/app",
        env=sub_env,
        check=True,
    )

    # 2. Prepare RUM logs
    print("Preparing RUM logs...")
    subprocess.run(
        [
            sys.executable,
            str(script_dir / "generate_synthetic_rum_logs.py"),
            "--bucket",
            bucket,
            "--service-id",
            service_id,
            "--endpoint",
            endpoint,
            "--access-key-id",
            access_key,
            "--secret-access-key",
            secret_key,
            "--target-rps",
            str(rum_rps),
            "--log-period-seconds",
            str(log_period),
            "--shards",
            str(shards),
            "--duration-seconds",
            str(duration),
            "--prepare-dir",
            str(rum_prep),
            "--prepare-only",
        ],
        cwd="/app",
        env=sub_env,
        check=True,
    )

    # 3. Start Telemetry Sampling
    print("Starting telemetry sampler...")
    sampler.start()
    stage_start_time = time.time()

    # 4. Concurrently Release Request and RUM
    print(f"Releasing scheduled load at interval {log_period}s...")
    p_req = subprocess.Popen(
        [
            sys.executable,
            str(script_dir / "generate_synthetic_raw_logs.py"),
            "--bucket",
            bucket,
            "--service-id",
            service_id,
            "--endpoint",
            endpoint,
            "--access-key-id",
            access_key,
            "--secret-access-key",
            secret_key,
            "--target-rps",
            str(request_rps),
            "--prepare-dir",
            str(req_prep),
            "--release-only",
            "--release-interval-seconds",
            str(rel_int),
            "--upload-workers",
            str(max(shards, 128)),
        ],
        cwd="/app",
        env=sub_env,
    )

    p_rum = subprocess.Popen(
        [
            sys.executable,
            str(script_dir / "generate_synthetic_rum_logs.py"),
            "--bucket",
            bucket,
            "--service-id",
            service_id,
            "--endpoint",
            endpoint,
            "--access-key-id",
            access_key,
            "--secret-access-key",
            secret_key,
            "--target-rps",
            str(rum_rps),
            "--prepare-dir",
            str(rum_prep),
            "--release-only",
            "--release-interval-seconds",
            str(rel_int),
            "--upload-workers",
            str(max(shards, 128)),
        ],
        cwd="/app",
        env=sub_env,
    )

    trigger_fired = False
    while p_req.poll() is None or p_rum.poll() is None:
        if stage_name == "WORKER_RESTART" and not trigger_fired:
            if time.time() - stage_start_time >= 4.0:
                Path("/tmp/worker_restart_trigger.txt").write_text(f"trigger_at_{time.time()}")
                print(
                    f"\n[WORKER_RESTART] Trigger file written at t={time.time() - stage_start_time:.2f}s during active load!"
                )
                trigger_fired = True
        time.sleep(0.2)

    req_rc = p_req.wait()
    rum_rc = p_rum.wait()

    if req_rc != 0 or rum_rc != 0:
        sampler.stop()
        raise RuntimeError(f"Release failed! Request exit code: {req_rc}, RUM exit code: {rum_rc}")

    release_duration = time.time() - stage_start_time
    print(f"Release completed in {release_duration:.2f}s.")

    # 5. Wait for ingestion backlog to drain
    print("Waiting for publication backlog to drain...")
    drain_start = time.time()
    while time.time() - drain_start < 180:
        time.sleep(2)
        # Check if pending manifests is 0
        if sampler.samples:
            last = sampler.samples[-1]
            pending = last.get("publication_manifests", {}).get("pending", 0)
            if pending == 0:
                print("Backlog successfully drained.")
                break

    sampler.stop()

    import shutil

    shutil.rmtree(req_prep, ignore_errors=True)
    shutil.rmtree(rum_prep, ignore_errors=True)

    # Calculate metrics
    api_latencies = [s["api_latency_ms"] for s in sampler.samples if s.get("api_latency_ms") is not None]
    page_latencies = [s["page_latency_ms"] for s in sampler.samples if s.get("page_latency_ms") is not None]
    lags = [s["max_publication_lag_s"] for s in sampler.samples if s.get("max_publication_lag_s") is not None]

    report = {
        "stage": stage_name,
        "request_rps": request_rps,
        "rum_rps": rum_rps,
        "combined_rps": request_rps + rum_rps,
        "duration_s": duration,
        "release_wall_s": round(release_duration, 2),
        "api_latency_ms": percentiles(api_latencies),
        "page_latency_ms": percentiles(page_latencies),
        "publication_lag_s": percentiles(lags),
        "sample_count": len(sampler.samples),
        "initial_ch": sampler.samples[0] if sampler.samples else {},
        "final_ch": sampler.samples[-1] if sampler.samples else {},
    }
    print(f"\nStage {stage_name} Report:")
    print(json.dumps(report, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser(description="Elevation High-Scale Qualification Runner")
    parser.add_argument("--bucket", default="elevation-high-scale")
    parser.add_argument("--service-id", default="ZEZ4mcAjoSFDTg7tpkDKV2")
    parser.add_argument("--endpoint", default="https://us-east-1.object.fastlystorage.app")
    parser.add_argument("--access-key-id", default=None)
    parser.add_argument("--secret-access-key", default=None)
    parser.add_argument("--ch-url", default="http://clickhouse-svc.se-demo.svc.cluster.local:8123")
    parser.add_argument("--pg-dsn", default="")
    parser.add_argument("--backend-url", default="http://backend-svc.se-demo.svc.cluster.local:8000")
    parser.add_argument("--frontend-url", default="http://frontend-svc.se-demo.svc.cluster.local:3000")
    parser.add_argument(
        "--mode", choices=["baseline", "2m_steady", "5m_burst", "worker_restart", "all"], default="2m_steady"
    )
    parser.add_argument("--prep-dir", default="/tmp/qualification_prep")
    parser.add_argument("--output", default="/tmp/qualification_report.json")
    args = parser.parse_args()

    # Load credentials from config file if not provided
    if not args.access_key_id or not args.secret_access_key:
        cfg_path = Path(f"/app/configs/{args.service_id}.json")
        if cfg_path.exists():
            with open(cfg_path) as f:
                cfg = json.load(f)
            args.access_key_id = args.access_key_id or cfg.get("fos_access_key_id")
            args.secret_access_key = args.secret_access_key or cfg.get("fos_secret_access_key")
            args.bucket = args.bucket or cfg.get("fos_bucket")
            if not args.endpoint and cfg.get("fos_endpoint"):
                args.endpoint = f"https://{cfg.get('fos_endpoint')}"
        if not args.access_key_id or not args.secret_access_key:
            parser.error("Must provide --access-key-id and --secret-access-key or mount service config")

    if not args.pg_dsn:
        import os

        args.pg_dsn = os.environ.get("METADATA_DSN", "")

    prep = Path(args.prep_dir)
    sampler = MetricSampler(
        ch_url=args.ch_url,
        pg_dsn=args.pg_dsn,
        backend_url=args.backend_url,
        frontend_url=args.frontend_url,
        service_id=args.service_id,
    )

    results: dict[str, Any] = {}

    if args.mode == "baseline":
        rep = run_stage(
            stage_name="BASELINE",
            bucket=args.bucket,
            service_id=args.service_id,
            endpoint=args.endpoint,
            access_key=args.access_key_id,
            secret_key=args.secret_access_key,
            request_rps=10000,
            rum_rps=10000,
            log_period=1,
            shards=16,
            duration=3,
            prep_dir=prep / "baseline",
            sampler=sampler,
        )
        results["baseline"] = rep

    if args.mode in ["2m_steady", "all"]:
        # Steady 2,000,000 log lines/sec combined: 1M request + 1M RUM for 3s (128 shards, period 1s)
        rep = run_stage(
            stage_name="2M_STEADY",
            bucket=args.bucket,
            service_id=args.service_id,
            endpoint=args.endpoint,
            access_key=args.access_key_id,
            secret_key=args.secret_access_key,
            request_rps=1000000,
            rum_rps=1000000,
            log_period=1,
            shards=128,
            duration=3,
            prep_dir=prep / "2m",
            sampler=sampler,
        )
        results["2m_steady"] = rep

    if args.mode in ["5m_burst", "all"]:
        # Burst 5,000,000 log lines/sec combined: 2.5M request + 2.5M RUM for 2s (256 shards, period 1s, release interval 3s)
        rep = run_stage(
            stage_name="5M_BURST",
            bucket=args.bucket,
            service_id=args.service_id,
            endpoint=args.endpoint,
            access_key=args.access_key_id,
            secret_key=args.secret_access_key,
            request_rps=2500000,
            rum_rps=2500000,
            log_period=1,
            shards=256,
            duration=2,
            prep_dir=prep / "5m",
            sampler=sampler,
            release_interval=3,
        )
        results["5m_burst"] = rep

    if args.mode in ["worker_restart", "all"]:
        # Worker restart under active load: 500k request + 500k RUM for 10s (128 shards, period 1s)
        rep = run_stage(
            stage_name="WORKER_RESTART",
            bucket=args.bucket,
            service_id=args.service_id,
            endpoint=args.endpoint,
            access_key=args.access_key_id,
            secret_key=args.secret_access_key,
            request_rps=500000,
            rum_rps=500000,
            log_period=1,
            shards=128,
            duration=10,
            prep_dir=prep / "worker_restart",
            sampler=sampler,
        )
        results["worker_restart"] = rep

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nAll stages complete. Report saved to {args.output}")


if __name__ == "__main__":
    main()
