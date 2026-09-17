#!/usr/bin/env python3
"""Synthetic raw-log generator for ingest-side load testing.

Uploads gzipped NDJSON files shaped like Fastly's log-delivery output
directly to FOS at a controlled arrival rate, instead of driving real edge
traffic through a live Fastly service (which would cost real Fastly
delivery/bandwidth and be far less reproducible). Mirrors the layout
``backend/provision/log_paths.py`` defines so ledger discovery
(``discover_prefix`` / ``minute_list_prefix``) treats these exactly like
real delivered files.

This intentionally does NOT reproduce every production field — it targets
ingest throughput/catalog-commit measurement (files/sec, Postgres commit
rate, FOS call volume), not analytics-correctness, which ADR-16/20's
existing fixtures already cover.

Usage:
    uv run python scripts/load_test/generate_synthetic_raw_logs.py \\
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
from pathlib import Path

for p in [Path.cwd(), Path("/app"), Path(__file__).resolve().parent.parent.parent]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from backend.provision.log_paths import analytics_log_path  # noqa: E402
from scripts.load_test.release_schedule import ReleaseReport, release_schedule  # noqa: E402

COUNTRIES = ["US", "GB", "DE", "JP", "BR", "IN", "AU", "FR", "CA", "NL"]
HOSTS = ["www.example.com", "api.example.com", "img.example.com"]
BACKENDS = ["origin-primary", "origin-secondary"]
POPS = ["IAD", "LHR", "NRT", "SYD", "SFO"]
METHODS = ["GET", "GET", "GET", "GET", "POST", "HEAD"]
PROTOS = ["2.0", "2.0", "3.0", "1.1"]
STATUSES = [200, 200, 200, 200, 304, 404, 500]
CACHE_STATUSES = ["HIT", "HIT", "HIT", "MISS", "PASS"]
TRANSPORTS = ["tcp", "tcp", "quic"]
DIGESTS = ["digest-a", "digest-b", "digest-c"]
TLS_FINGERPRINTS = ["ja3-synthetic-a", "ja3-synthetic-b"]
ORIGIN_IPS = ["203.0.113.10", "203.0.113.11"]
IMAGE_FORMATS = ["jpeg", "webp", "avif"]
USER_AGENTS = [
    "Mozilla/5.0 (synthetic Chrome)",
    "Mozilla/5.0 (synthetic Safari)",
    "synthetic-monitor/1.0",
]
REFERERS = ["https://www.example.com/", "https://search.example.com/", "https://news.example.com/"]
REGIONS = ["CA", "NY", "TX", "ON", "BE"]
PROXY_TYPES = ["VPN", "VPN", "DCH", "DCH"]
PROXY_DESCRIPTIONS = ["synthetic-vpn", "synthetic-vpn", "synthetic-datacenter", "synthetic-datacenter"]
CONTENT_ENCODINGS = ["br", "gzip", "identity"]
SERVER_REGIONS = ["NA", "EU", "APAC"]
CONNECTION_SPEEDS = ["broadband", "cable", "mobile"]
CONNECTION_TYPES = ["residential", "commercial", "cellular"]


def _synthetic_line(ts: datetime, service_id: str) -> dict:
    status = random.choice(STATUSES)
    method = random.choice(METHODS)
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ip": f"198.51.100.{random.randint(1, 254)}",
        "host": random.choice(HOSTS),
        "url": f"/synthetic/{random.randint(1, 5000)}",
        "method": method,
        "proto": random.choice(PROTOS),
        "ua": random.choice(USER_AGENTS),
        "referer": random.choice(REFERERS),
        "status": status,
        "country": random.choice(COUNTRIES),
        "city": random.choice(["San Francisco", "New York", "Toronto", "London", "Tokyo"]),
        "region": random.choice(REGIONS),
        "cache": random.choice(CACHE_STATUSES),
        "ttl": random.randint(60, 86_400),
        "age": random.randint(0, 3_600),
        "hits": random.randint(1, 10_000),
        "digest": random.choice(DIGESTS),
        "backend": random.choice(BACKENDS),
        "edge": random.choice([True, True, True, False]),
        "pop": random.choice(POPS),
        "server_region": random.choice(SERVER_REGIONS),
        "tls": random.choice(["1.2", "1.3"]),
        "is_ipv6": random.choice([False, False, False, True]),
        "conn_requests": random.randint(1, 32),
        "lat": round(random.uniform(-60, 60), 4),
        "lon": round(random.uniform(-150, 150), 4),
        "metro": random.randint(500, 900),
        "asn": random.randint(1_000, 65_000),
        "tcp_rtt": random.randint(8, 180_000),
        "transport": random.choice(TRANSPORTS),
        "ploss": round(random.uniform(0, 0.03), 6),
        "rtt_min": random.randint(5, 120_000),
        "rtt_var": random.randint(1, 20_000),
        "retrans": random.randint(0, 5),
        "bw": random.randint(1_000_000, 100_000_000),
        "elapsed": random.randint(2_000, 450_000),
        "ttfb": round(random.uniform(0.002, 0.45), 6),
        "req_bytes": 0 if method in {"GET", "HEAD"} else random.randint(64, 16_384),
        "req_header_bytes": random.randint(300, 2_500),
        "resp_bytes": random.randint(200, 150_000),
        "resp_header_content_encoding": random.choice(CONTENT_ENCODINGS),
        "p_type": random.choice(PROXY_TYPES),
        "p_desc": random.choice(PROXY_DESCRIPTIONS),
        "c_speed": random.choice(CONNECTION_SPEEDS),
        "c_type": random.choice(CONNECTION_TYPES),
        "delivery_rate": random.randint(500_000, 80_000_000),
        "data_segs_out": random.randint(10, 10_000),
        "ja3": random.choice(TLS_FINGERPRINTS),
        "ja4": random.choice(["ja4-synthetic-a", "ja4-synthetic-b"]),
        "tls_ciphers_sha": random.choice(["cipher-synthetic-a", "cipher-synthetic-b"]),
        "cookie_session": f"session-{random.randint(1, 10_000)}",
        "waf": False,
        "waf_resp": 200,
        "waf_ms": 0,
        "waf_req_id": f"waf-{random.randint(1, 10_000)}",
        "q_rtt": random.randint(8, 180_000),
        "q_rtt_var": random.randint(1, 20_000),
        "q_lost": random.randint(0, 3),
        "q_cwnd": random.randint(10_000, 2_000_000),
        "ottfb": random.randint(1_000, 300_000),
        "ottlb": random.randint(2_000, 500_000),
        "oconnect_ms": random.randint(1, 100),
        "ost": status,
        "obytes": random.randint(200, 150_000),
        "oip": random.choice(ORIGIN_IPS),
        "oretries": random.randint(0, 2),
        "rid": f"rid-{random.randint(1, 10_000)}",
        "prid": f"prid-{random.randint(1, 10_000)}",
        "io_input_bytes": random.randint(10_000, 500_000),
        "io_output_bytes": random.randint(5_000, 400_000),
        "io_input_format": random.choice(IMAGE_FORMATS),
        "io_output_format": random.choice(IMAGE_FORMATS),
        "service_id": service_id,
        "cmcd": {"v": "1", "sid": f"sid-{random.randint(1, 1000)}", "br": "3000", "d": "4000"},
        "waf_sig": "VERIFIED-BOT",
    }


def _build_gz_ndjson(lines: list[dict]) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write("\n".join(json.dumps(line) for line in lines).encode("utf-8"))
    return buf.getvalue()


def _object_key(minute_dt: datetime, shard: int) -> str:
    base = analytics_log_path().rstrip("/").lstrip("/")
    minute_dir = minute_dt.strftime(base)
    unique = f"{minute_dt.strftime('%Y-%m-%dT%H:%M:%S')}.000-{shard:03d}-{uuid.uuid4().hex[:8]}"
    return f"{minute_dir}/analytics_log_{unique}.json.gz"


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
    core. json.dumps + string-join is CPU-bound pure-Python bytecode that
    holds the GIL, so a thread pool for this step tops out on ~1 core no
    matter how many threads -- confirmed by the initial dry run spending
    half of a 10s log_period generating 500k lines on one core. gzip's
    actual compress() calls release the GIL, but that alone wasn't enough
    to keep up; process-level parallelism fixes the json.dumps part too.
    """
    lines = [
        _synthetic_line(period_start + timedelta(seconds=random.uniform(0, 1)), service_id)
        for _ in range(lines_per_shard)
    ]
    if any(not line["ip"] for line in lines):
        raise ValueError("synthetic request logs must include a client IP for dashboard analytics")
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
    # Generation (CPU-bound, multi-process) and upload (I/O-bound, threaded)
    # overlap: each shard uploads as soon as ITS generation finishes rather
    # than waiting for all `shards` to complete first.
    with ThreadPoolExecutor(max_workers=min(shards, 64), thread_name_prefix="synthgen-upload") as upload_ex:
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


def _write_prepared_period(
    prepare_dir: Path,
    index: int,
    generated: list[tuple[str, bytes, int]],
) -> dict:
    period_dir = prepare_dir / f"period-{index:06d}"
    period_dir.mkdir(parents=True, exist_ok=True)
    shards = []
    for shard, (key, body, lines) in enumerate(generated):
        filename = f"shard-{shard:06d}.json.gz"
        (period_dir / filename).write_bytes(body)
        shards.append({"key": key, "filename": filename, "lines": lines, "bytes": len(body)})
    return {"shards": shards}


def _write_manifest(prepare_dir: Path, periods: list[dict], args: argparse.Namespace) -> None:
    (prepare_dir / "manifest.json").write_text(
        json.dumps(
            {
                "target_rps": args.target_rps,
                "period_seconds": args.log_period_seconds,
                "duration_seconds": args.duration_seconds,
                "shards": args.shards,
                "periods": periods,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _load_manifest(prepare_dir: Path) -> dict:
    try:
        return json.loads((prepare_dir / "manifest.json").read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"prepared directory has no manifest: {prepare_dir}") from exc


def _release_prepared(
    *,
    prepare_dir: Path,
    manifest: dict,
    fos_client,
    bucket: str,
    dry_run: bool,
    schedule: tuple[datetime, ...],
    upload_workers: int = 128,
) -> list[ReleaseReport]:
    reports = []

    def _upload_shard(shard_data: tuple[dict, bytes]) -> tuple[int, int, datetime | None, datetime | None, bool]:
        shard, body = shard_data
        try:
            if not dry_run:
                fos_client.put_object(Bucket=bucket, Key=shard["key"], Body=body)
            uploaded_at = datetime.now(UTC)
            return len(body), shard["lines"], uploaded_at, uploaded_at, False
        except Exception as exc:
            print(f"  [error] uploading {shard['key']}: {exc}", file=sys.stderr)
            return 0, 0, None, None, True

    # Pre-load all shard files into memory to avoid I/O blocking during scheduled releases
    all_periods_data = []
    for index, period in enumerate(manifest["periods"]):
        period_dir = prepare_dir / f"period-{index:06d}"
        all_periods_data.append([(shard, (period_dir / shard["filename"]).read_bytes()) for shard in period["shards"]])

    with ThreadPoolExecutor(max_workers=max(1, upload_workers)) as upload_ex:
        if not dry_run and schedule:
            warm_futures = [
                upload_ex.submit(lambda: fos_client.list_objects_v2(Bucket=bucket, MaxKeys=1))
                for _ in range(upload_workers)
            ]
            for f in as_completed(warm_futures):
                try:
                    f.result()
                except Exception:
                    pass
            actual_start = datetime.now(UTC)
            offset = (actual_start + timedelta(milliseconds=500)) - schedule[0]
            schedule = tuple(t + offset for t in schedule)

        for index, (period, scheduled_at) in enumerate(zip(manifest["periods"], schedule, strict=True)):
            shards_with_bodies = all_periods_data[index]
            while True:
                wait = (scheduled_at - datetime.now(UTC)).total_seconds()
                if wait <= 0:
                    break
                time.sleep(min(wait, 0.05))

            started = time.monotonic()
            upload_futures = [upload_ex.submit(_upload_shard, item) for item in shards_with_bodies]
            results = [future.result() for future in as_completed(upload_futures)]
            del shards_with_bodies
            total_bytes = sum(result[0] for result in results)
            total_lines = sum(result[1] for result in results)
            upload_times = [time for result in results for time in result[2:4] if time is not None]
            first_upload = min(upload_times, default=None)
            last_upload = max(upload_times, default=None)
            errors = sum(result[4] for result in results)
            files = len(results) - errors
            report = ReleaseReport(
                scheduled_at,
                first_upload,
                last_upload,
                total_bytes,
                total_lines,
                time.monotonic() - started,
                files,
                errors,
            )
            reports.append(report)
            print(
                f"  release {index + 1}/{len(schedule)} scheduled={scheduled_at.isoformat()} "
                f"first={first_upload.isoformat() if first_upload else '-'} "
                f"last={last_upload.isoformat() if last_upload else '-'} "
                f"{total_lines} lines, {total_bytes / 1e6:.2f} MB, wall={report.wall_seconds:.2f}s"
            )
            cadence = (
                (schedule[1] - schedule[0]).total_seconds()
                if len(schedule) > 1
                else float(manifest.get("period_seconds", 1.0))
            )
            max_allowed_wall = max(cadence * 1.15, cadence + 0.25)
            if report.wall_seconds > max_allowed_wall:
                raise RuntimeError(
                    f"Release {index + 1}/{len(schedule)} wall time {report.wall_seconds:.2f}s "
                    f"exceeded requested cadence {cadence:.2f}s (max allowed: {max_allowed_wall:.2f}s); schedule violated"
                )
    return reports


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True)
    p.add_argument("--service-id", required=True)
    p.add_argument("--endpoint", default="")
    p.add_argument("--access-key-id", default="")
    p.add_argument("--secret-access-key", default="")
    p.add_argument("--target-rps", type=int, required=True)
    p.add_argument("--log-period-seconds", type=int, default=10)
    p.add_argument("--shards", type=int, default=20)
    p.add_argument("--duration-seconds", type=int, default=60)
    p.add_argument("--gen-workers", type=int, default=None)
    p.add_argument("--upload-workers", type=int, default=128)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--prepare-dir", type=Path)
    p.add_argument("--prepare-only", action="store_true")
    p.add_argument("--release-only", action="store_true")
    p.add_argument("--release-interval-seconds", type=int)
    return p


def main() -> int:
    p = build_parser()
    args = p.parse_args()
    if args.release_only and args.prepare_dir is None:
        p.error("--release-only requires --prepare-dir")
    if args.release_interval_seconds is not None and args.release_interval_seconds <= 0:
        p.error("--release-interval-seconds must be positive")
    if args.prepare_dir is not None and args.release_only:
        manifest = _load_manifest(args.prepare_dir)
        args.target_rps = manifest["target_rps"]
        args.log_period_seconds = manifest["period_seconds"]
        args.duration_seconds = manifest["duration_seconds"]
        args.shards = manifest["shards"]

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
                # botocore >=1.36's default of adding a trailing CRC32 request
                # checksum breaks PutObject against FOS ("InvalidRequest" with
                # no further detail) -- confirmed by testing a bare put_object
                # with/without this. FOS isn't AWS S3; don't send AWS-specific
                # integrity extensions it doesn't support unless required.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                max_pool_connections=max(args.upload_workers or 128, 128),
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
    if args.prepare_dir is not None:
        args.prepare_dir.mkdir(parents=True, exist_ok=True)
        if not args.release_only:
            periods = []
            with ProcessPoolExecutor(max_workers=args.gen_workers) as pool:
                for i in range(n_periods):
                    period_start = start + timedelta(seconds=i * args.log_period_seconds)
                    futures = [
                        pool.submit(_generate_shard, period_start, service_id, lines_per_shard, s)
                        for s in range(args.shards)
                    ]
                    periods.append(_write_prepared_period(args.prepare_dir, i, [future.result() for future in futures]))
            _write_manifest(args.prepare_dir, periods, args)
            if args.prepare_only:
                print(f"Preparation complete. Wrote {len(periods)} periods to {args.prepare_dir}")
                return 0
        release_started_at = datetime.now(UTC)
        manifest = _load_manifest(args.prepare_dir)
        schedule = release_schedule(
            target_rps=args.target_rps,
            period_seconds=args.log_period_seconds,
            duration_seconds=args.duration_seconds,
            started_at=release_started_at,
            release_interval_seconds=args.release_interval_seconds,
        )
        reports = _release_prepared(
            prepare_dir=args.prepare_dir,
            manifest=manifest,
            fos_client=fos_client,
            bucket=args.bucket,
            dry_run=args.dry_run,
            schedule=schedule,
            upload_workers=args.upload_workers,
        )
        total_errors = sum(report.errors for report in reports)
        print(
            f"\nDone: {sum(r.files for r in reports)} files, {sum(r.lines for r in reports)} lines, {sum(r.bytes_uploaded for r in reports) / 1e6:.1f} MB, {total_errors} shard errors"
        )
        return 1 if total_errors else 0

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
