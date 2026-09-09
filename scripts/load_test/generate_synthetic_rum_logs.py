#!/usr/bin/env python3
"""Synthetic RUM-beacon generator for ingest-side load testing.

Companion to generate_synthetic_raw_logs.py: same upload mechanics
(gzipped NDJSON, direct-to-FOS, no live edge traffic), but shaped like the
RUM beacon log stream (``raw/rum/...``, see backend/provision/log_paths.py)
instead of the main request-log stream. Real traffic generates both request
logs and RUM beacons concurrently, so a realistic ingest-load test needs
both streams landing at the same time -- this fills the RUM half.

Each line carries a `rum_body` field holding a Faro-shaped payload with one
web-vitals measurement, matching what `extract_metrics_from_faro_payload`
(backend/core/rum_ingest.py) expects so ingest actually produces
client_vitals rows, not just parses without effect.

Usage:
    uv run python scripts/load_test/generate_synthetic_rum_logs.py \\
        --bucket my-test-bucket --service-id svc-abc123 \\
        --endpoint https://<region>.object.fastlystorage.app \\
        --access-key-id "$FOS_KEY" --secret-access-key "$FOS_SECRET" \\
        --target-rps 50000 --log-period-seconds 10 --shards 20 \\
        --duration-seconds 60

Pass --dry-run to generate and gzip locally without uploading, to sanity
check file counts/sizes before spending real FOS API calls.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import random
import sys
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

sys.path.insert(0, ".")

from backend.provision.log_paths import rum_log_path  # noqa: E402

COUNTRIES = ["US", "GB", "DE", "JP", "BR", "IN", "AU", "FR", "CA", "NL"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
OSES = ["macOS", "Windows", "Linux", "iOS", "Android"]
PATHS = ["/", "/dashboard", "/pricing", "/docs", "/checkout"]
VITALS = ["LCP", "CLS", "FID", "TTFB", "INP"]
VITAL_RANGES = {
    "LCP": (800, 4500),
    "CLS": (0, 0.4),
    "FID": (5, 300),
    "TTFB": (50, 1200),
    "INP": (20, 500),
}
RATINGS = ["good", "good", "good", "needs-improvement", "poor"]


def _synthetic_rum_line(ts: datetime, service_id: str) -> dict:
    browser = random.choice(BROWSERS)
    os_name = random.choice(OSES)
    mobile = os_name in ("iOS", "Android")
    path = random.choice(PATHS)
    vital = random.choice(VITALS)
    lo, hi = VITAL_RANGES[vital]
    value = random.uniform(lo, hi)
    rum_body = {
        "meta": {
            "browser": {"name": browser, "mobile": mobile},
            "os": {"name": os_name},
            "page": {"url": f"https://example.com{path}"},
        },
        "page": {"url": f"https://example.com{path}"},
        "measurements": [
            {
                "type": "web-vitals",
                "values": {vital: value},
                "context": {"rating": random.choice(RATINGS)},
            }
        ],
    }
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "service_id": service_id,
        "city": "San Francisco",
        "region": "CA",
        "country": random.choice(COUNTRIES),
        "pop": "SJC",
        "tls": "TLSv1.3",
        "ttfb": random.uniform(20, 300),
        "rum_cid": uuid.uuid4().hex[:16],
        "fastly_req_id": uuid.uuid4().hex,
        "rum_body": json.dumps(rum_body),
    }


def _build_gz_ndjson(lines: list[dict]) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write("\n".join(json.dumps(line) for line in lines).encode("utf-8"))
    return buf.getvalue()


def _object_key(minute_dt: datetime, shard: int) -> str:
    base = rum_log_path().rstrip("/").lstrip("/")
    minute_dir = minute_dt.strftime(base)
    unique = f"{minute_dt.strftime('%Y-%m-%dT%H:%M:%S')}.000-{shard:03d}-{uuid.uuid4().hex[:8]}"
    return f"{minute_dir}/rum_log_{unique}.json.gz"


@dataclass
class TickResult:
    period_start: datetime
    files: int
    lines: int
    bytes_uploaded: int
    upload_seconds: float
    errors: int


def _generate_shard(
    period_start: datetime, service_id: str, lines_per_shard: int, shard: int
) -> tuple[str, bytes, int]:
    """Top-level (picklable) so ProcessPoolExecutor can run it on a real
    core -- same rationale as generate_synthetic_raw_logs.py's version.
    """
    lines = [
        _synthetic_rum_line(period_start + timedelta(seconds=random.uniform(0, 1)), service_id)
        for _ in range(lines_per_shard)
    ]
    body = _build_gz_ndjson(lines)
    key = _object_key(period_start, shard)
    return key, body, len(lines)


def _run_one_period(
    fos_client,
    bucket: str,
    service_id: str,
    period_start: datetime,
    lines_per_shard: int,
    shards: int,
    dry_run: bool,
    pool: ProcessPoolExecutor,
) -> TickResult:
    def _upload(key: str, body: bytes) -> tuple[int, bool]:
        if dry_run:
            return len(body), False
        try:
            fos_client.put_object(Bucket=bucket, Key=key, Body=body)
            return len(body), False
        except Exception as e:
            print(f"  [error] uploading {key}: {e}", file=sys.stderr)
            return 0, True

    t0 = time.monotonic()
    total_bytes = total_lines = errors = 0
    gen_futures = [pool.submit(_generate_shard, period_start, service_id, lines_per_shard, s) for s in range(shards)]
    with ThreadPoolExecutor(max_workers=min(shards, 64), thread_name_prefix="synthgen-rum-upload") as upload_ex:
        upload_futures = []
        for fut in as_completed(gen_futures):
            key, body, n_lines = fut.result()
            total_lines += n_lines
            upload_futures.append(upload_ex.submit(_upload, key, body))
        for fut in as_completed(upload_futures):
            b, err = fut.result()
            total_bytes += b
            errors += int(err)
    elapsed = time.monotonic() - t0
    return TickResult(period_start, shards - errors, total_lines, total_bytes, elapsed, errors)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True)
    p.add_argument("--service-id", required=True)
    p.add_argument("--endpoint", default="")
    p.add_argument("--access-key-id", default="")
    p.add_argument("--secret-access-key", default="")
    p.add_argument("--target-rps", type=int, required=True)
    p.add_argument("--log-period-seconds", type=int, default=10, help="Matches the service's configured log_period.")
    p.add_argument(
        "--shards",
        type=int,
        default=20,
        help="Concurrent writer shards per period (simulates per-POP/per-node fan-out).",
    )
    p.add_argument("--duration-seconds", type=int, default=60)
    p.add_argument(
        "--gen-workers",
        type=int,
        default=None,
        help="Process pool size for line generation/gzip. Defaults to os.cpu_count().",
    )
    p.add_argument("--dry-run", action="store_true", help="Generate/gzip locally; skip the FOS upload.")
    args = p.parse_args()

    service_id = args.service_id
    lines_per_period = args.target_rps * args.log_period_seconds
    lines_per_shard = max(1, lines_per_period // args.shards)

    fos_client = None
    if not args.dry_run:
        import boto3
        from botocore.config import Config

        fos_client = boto3.client(
            "s3",
            endpoint_url=args.endpoint,
            aws_access_key_id=args.access_key_id,
            aws_secret_access_key=args.secret_access_key,
            config=Config(
                retries={"max_attempts": 3, "mode": "adaptive"},
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    print(
        f"target_rps={args.target_rps} log_period={args.log_period_seconds}s shards={args.shards} "
        f"-> {lines_per_period} lines/period, {lines_per_shard} lines/shard/file, "
        f"{args.duration_seconds // args.log_period_seconds} periods over {args.duration_seconds}s"
        f"{' [DRY RUN]' if args.dry_run else ''}"
    )

    start = datetime.now(UTC).replace(microsecond=0)
    n_periods = max(1, args.duration_seconds // args.log_period_seconds)
    results: list[TickResult] = []
    with ProcessPoolExecutor(max_workers=args.gen_workers) as pool:
        for i in range(n_periods):
            period_start = start + timedelta(seconds=i * args.log_period_seconds)
            tick_t0 = time.monotonic()
            result = _run_one_period(
                fos_client,
                args.bucket,
                service_id,
                period_start,
                lines_per_shard,
                args.shards,
                args.dry_run,
                pool,
            )
            results.append(result)
            print(
                f"  period {i + 1}/{n_periods}: {result.files} files, {result.lines} lines, "
                f"{result.bytes_uploaded / 1e6:.2f} MB, upload {result.upload_seconds:.2f}s, errors={result.errors}"
            )
            sleep_for = args.log_period_seconds - (time.monotonic() - tick_t0)
            if sleep_for > 0 and i < n_periods - 1:
                time.sleep(sleep_for)

    total_files = sum(r.files for r in results)
    total_lines = sum(r.lines for r in results)
    total_bytes = sum(r.bytes_uploaded for r in results)
    total_errors = sum(r.errors for r in results)
    print(
        f"\nDone: {total_files} files, {total_lines} lines ({total_lines / args.duration_seconds:.0f} lines/s "
        f"avg over wall time), {total_bytes / 1e6:.1f} MB, {total_errors} shard errors"
    )
    return 1 if total_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
