#!/usr/bin/env python3
"""Unified Multi-Tier Synthetic Traffic Generator.

Supports both:
  1. --target local: Writes synthetic Parquet batches directly into
     ``cache/{bucket}/buffer/`` and optionally calls ``commit_buffer``
     to materialize DuckLake snapshots. Instant, zero S3 API cost,
     and 100% reproducible for local testing and CI.
  2. --target fos: Generates gzipped NDJSON chunks conforming to
     ``backend/provision/log_paths.py`` and uploads them to FOS object
     storage to test the full discovery, ledger, and commit crons.
  3. --target clickhouse / --with-clickhouse: Directly seeds ClickHouse
     fact tables (``request_facts``) and registers publication batches
     for high-scale serving verification.

Scenario Profiles:
  - diurnal: Standard 24h day/night curve with normal CDN performance (~82% CHR).
  - ddos-spike: 15-minute attack spike with concentrated IPs, JA3s, and 429/503 errors.
  - origin-5xx-outage: 30-minute backend database outage (502/503/504 MISSes, high OTTFB).
  - bot-scrape: High-volume bot crawls across search engines, SEO tools, and scrapers.
  - slow-network: High RTT, elevated TLS handshakes, and slow mobile connections.

Usage:
  # Local fast fixture (100k rows, 24 hours):
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target local --scenario diurnal --rows 100000 --commit

  # Direct ClickHouse seeding for high-scale:
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target clickhouse --scenario diurnal --rows 50000

  # FOS upload + direct ClickHouse seeding:
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target fos --with-clickhouse --scenario diurnal --rows 50000

  # High-throughput rate pacing (50k sustained, 100k burst to FOS):
  uv run python scripts/load_test/generate_synthetic_traffic.py \\
      --target fos --rows 200000 --rate-rps 50000 --burst-rps 100000 --upload-workers 8
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

# Add repo root to sys.path
for p in [Path.cwd(), Path("/app"), Path(__file__).resolve().parent.parent.parent]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from backend.config import list_configs, load_config  # noqa: E402
from backend.core.iceberg import (  # noqa: E402
    _buffer_dir,
    commit_buffer,
    get_arrow_schema,
)

# ---------------------------------------------------------------------------
# Dimension Pools, Columnar Generator & Arrow Builder
#
# The canonical request-log schema, dimension pools, and columnar generator are
# owned by scripts/lib/log_factory.py so tests/utils/mock_data.py shares the same
# source of truth. See AGENTS.md (synthetic traffic generation).
# ---------------------------------------------------------------------------
from scripts.lib.log_factory import cols_to_arrow_table as _cols_to_arrow_table  # noqa: E402
from scripts.lib.log_factory import generate_columns as _generate_batch_data  # noqa: E402

# ---------------------------------------------------------------------------
# Target Runners: Local Parquet vs FOS S3 Upload
# ---------------------------------------------------------------------------


def run_target_local(
    src: dict[str, Any],
    scenario: str,
    rows: int,
    start_dt: datetime,
    end_dt: datetime,
    batch_size: int,
    file_rows: int,
    seed: int,
    commit: bool,
    clean: bool,
    dry_run: bool,
    rate_rps: int = 0,
    burst_rps: int = 0,
    burst_duration: int = 30,
    burst_interval: int = 120,
) -> int:
    """Writes Parquet directly to local buffer and optionally commits to DuckLake."""
    buf_dir = _buffer_dir(src)
    os.makedirs(buf_dir, exist_ok=True)

    if clean:
        print(f"Cleaning existing buffer files in {buf_dir}...")
        for f in Path(buf_dir).glob("*.parquet"):
            try:
                f.unlink()
            except Exception as e:
                print(f"  warning: failed to delete {f.name}: {e}")

    schema = get_arrow_schema(src.get("log_fields", {}))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    t0 = time.monotonic()
    rows_remaining = rows
    file_idx = 0
    total_rows = 0

    pacing_desc = (
        f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
        if rate_rps > 0
        else "Unthrottled (maximum throughput)"
    )

    print(
        f"Generating {rows:,} rows [{scenario}] for service '{src.get('service_id')}'\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Target: Local buffer ({buf_dir})\n"
        f"  Pacing: {pacing_desc}"
    )

    if dry_run:
        print("[dry-run] Sample batch generated successfully. Exiting without writing.")
        return 0

    while rows_remaining > 0:
        rows_this_file = min(file_rows, rows_remaining)
        fname = f"synth_{scenario}_{int(time.time())}_{file_idx:04d}.parquet"
        fpath = os.path.join(buf_dir, fname)
        writer = pq.ParquetWriter(fpath, schema, compression="zstd", compression_level=1)

        rows_in_this_file = 0
        while rows_in_this_file < rows_this_file:
            b_t0 = time.monotonic()
            elapsed_total = b_t0 - t0
            if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
                target_rps = burst_rps
            elif rate_rps > 0:
                target_rps = rate_rps
            else:
                target_rps = 0

            n = min(batch_size, rows_this_file - rows_in_this_file)
            cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)
            tbl = _cols_to_arrow_table(cols, schema)
            tbl = tbl.sort_by([("timestamp", "ascending"), ("ip", "ascending")])
            writer.write_table(tbl)
            rows_in_this_file += n

            if target_rps > 0:
                target_batch_sec = n / target_rps
                b_dur = time.monotonic() - b_t0
                if target_batch_sec > b_dur:
                    time.sleep(target_batch_sec - b_dur)

        writer.close()
        rows_remaining -= rows_this_file
        total_rows += rows_this_file
        file_idx += 1
        elapsed = time.monotonic() - t0
        rate = total_rows / max(elapsed, 0.001)
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        print(
            f"  wrote {fname}: {rows_this_file:,} rows ({size_mb:.2f} MB) | "
            f"total: {total_rows:,}/{rows:,} ({100 * total_rows / rows:.1f}%) | "
            f"{rate:,.0f} rows/s",
            flush=True,
        )

    total_elapsed = time.monotonic() - t0
    print(
        f"\nSUCCESS: Generated {total_rows:,} rows in {total_elapsed:.2f}s ({total_rows / total_elapsed:,.0f} rows/s)."
    )

    if commit:
        print("\nCommitting buffer to DuckLake table...", flush=True)
        t_commit = time.monotonic()
        res = commit_buffer(src)
        print(
            f"COMMITTED: {res.get('rows_committed', 0):,} rows in "
            f"{res.get('files_committed', 0)} files in "
            f"{time.monotonic() - t_commit:.2f}s."
        )

    return 0


def run_target_fos(
    src: dict[str, Any],
    scenario: str,
    rows: int,
    start_dt: datetime,
    end_dt: datetime,
    batch_size: int,
    seed: int,
    dry_run: bool,
    rate_rps: int = 0,
    burst_rps: int = 0,
    burst_duration: int = 30,
    burst_interval: int = 120,
    upload_workers: int = 8,
) -> int:
    """Generates gzipped NDJSON and uploads directly to FOS object storage with rate pacing & background uploads."""
    from backend.provision.log_paths import minute_list_prefix

    bucket = src.get("s3_bucket") or src.get("fos_bucket")
    if not bucket:
        print("ERROR: Service config does not specify an s3_bucket / fos_bucket", file=sys.stderr)
        return 1

    import boto3
    from botocore.config import Config

    endpoint = src.get("s3_endpoint") or src.get("fos_endpoint")
    if endpoint and not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    key_id = src.get("s3_access_key") or src.get("fos_access_key_id")
    secret_key = src.get("s3_secret_key") or src.get("fos_secret_access_key")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            max_pool_connections=max(25, upload_workers * 2),
            # botocore >=1.36 defaults to sending a streaming trailer checksum
            # (x-amz-checksum-crc32) on every PutObject. Real Fastly Object
            # Storage's S3-compatible endpoint rejects that trailer with a bare
            # "InvalidRequest" (no further detail in the response body), so
            # every upload fails until this is forced back to the pre-1.36
            # opt-in behavior.
            request_checksum_calculation="when_required",
        ),
    )

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    pacing_desc = (
        f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
        if rate_rps > 0
        else "Unthrottled (maximum throughput)"
    )

    print(
        f"Generating and uploading {rows:,} raw log records [{scenario}] to FOS\n"
        f"  Bucket: {bucket}\n"
        f"  Service: {src.get('service_id')}\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Upload Workers: {upload_workers}\n"
        f"  Pacing: {pacing_desc}"
    )

    rows_remaining = rows
    file_idx = 0
    t0 = time.monotonic()
    total_bytes_uploaded = 0
    total_rows_emitted = 0

    upload_pool = ThreadPoolExecutor(max_workers=upload_workers) if not dry_run else None
    pending_uploads = set()

    def _upload_task(bucket_name: str, key_name: str, payload: bytes) -> int:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=payload,
            ContentType="application/gzip",
        )
        return len(payload)

    try:
        while rows_remaining > 0:
            b_t0 = time.monotonic()
            elapsed_total = b_t0 - t0

            if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
                target_rps = burst_rps
                in_burst = True
            elif rate_rps > 0:
                target_rps = rate_rps
                in_burst = False
            else:
                target_rps = 0
                in_burst = False

            n = min(batch_size, rows_remaining)
            cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)

            # Fast NDJSON formatting with pre-converted lists
            cols_py = {k: cols[k].tolist() if hasattr(cols[k], "tolist") else cols[k] for k in cols}
            lines = []
            keys = [k for k in cols_py if k != "_source_file"]
            ts_list = cols_py["timestamp"]
            for i in range(n):
                record = {k: cols_py[k][i] for k in keys}
                record["timestamp"] = datetime.fromtimestamp(ts_list[i] / 1_000_000, tz=UTC).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                lines.append(json.dumps(record, default=str))

            ndjson_bytes = "\n".join(lines).encode("utf-8")
            gz_buf = io.BytesIO()
            with gzip.GzipFile(fileobj=gz_buf, mode="wb", compresslevel=1) as gz:
                gz.write(ndjson_bytes)
            gz_bytes = gz_buf.getvalue()

            now_utc = datetime.now(UTC)
            prefix = (src.get("s3_prefix") or src.get("fos_prefix") or "").strip("/")
            min_prefix = minute_list_prefix(now_utc)
            base_dir = f"{prefix}/{min_prefix}" if prefix else min_prefix
            s3_key = f"{base_dir}{src.get('service_id')}_{now_utc.strftime('%Y%m%dT%H%M%SZ')}_{file_idx:04d}.log.gz"

            mode_str = (
                f"BURST {burst_rps:,} rps" if in_burst else (f"NORMAL {rate_rps:,} rps" if rate_rps > 0 else "MAX")
            )

            if dry_run:
                total_bytes_uploaded += len(gz_bytes)
                print(
                    f"  [dry-run] [{mode_str}] File {file_idx:04d}: {n:,} rows -> "
                    f"s3://{bucket}/{s3_key} ({len(gz_bytes):,} bytes)"
                )
            else:
                assert upload_pool is not None
                if len(pending_uploads) >= upload_workers * 2:
                    done, pending_uploads = wait(pending_uploads, return_when=FIRST_COMPLETED)
                    for fut in done:
                        total_bytes_uploaded += fut.result()

                fut = upload_pool.submit(_upload_task, bucket, s3_key, gz_bytes)
                pending_uploads.add(fut)
                print(
                    f"  queued [{mode_str}] s3://{bucket}/{s3_key} ({len(gz_bytes):,} bytes, {n:,} rows)",
                    flush=True,
                )

            total_rows_emitted += n
            rows_remaining -= n
            file_idx += 1

            if target_rps > 0:
                target_batch_sec = n / target_rps
                b_dur = time.monotonic() - b_t0
                if target_batch_sec > b_dur:
                    time.sleep(target_batch_sec - b_dur)

        if upload_pool and pending_uploads:
            print(f"Waiting for {len(pending_uploads)} pending S3 uploads to complete...")
            done, _ = wait(pending_uploads)
            for fut in done:
                total_bytes_uploaded += fut.result()

    finally:
        if upload_pool:
            upload_pool.shutdown(wait=True)

    elapsed = time.monotonic() - t0
    rate = total_rows_emitted / max(elapsed, 0.001)
    print(
        f"\nSUCCESS: Finished FOS upload of {total_rows_emitted:,} rows ({total_bytes_uploaded / (1024 * 1024):.2f} MB compressed) "
        f"in {elapsed:.2f}s ({rate:,.0f} rows/s)."
    )
    return 0


def run_target_clickhouse(
    src: dict[str, Any],
    scenario: str,
    rows: int,
    start_dt: datetime,
    end_dt: datetime,
    batch_size: int,
    seed: int,
    dry_run: bool,
    clickhouse_host: str | None = None,
    clickhouse_port: int | None = None,
    clickhouse_database: str | None = None,
    clickhouse_user: str | None = None,
    clickhouse_password: str | None = None,
    rate_rps: int = 0,
    burst_rps: int = 0,
    burst_duration: int = 30,
    burst_interval: int = 120,
) -> int:
    """Generates synthetic log records and inserts them directly into ClickHouse."""
    from uuid import uuid4

    from backend.core.clickhouse_client import (
        ClickHouseClient,
        get_clickhouse_client,
    )
    from backend.high_scale.publication import (
        ClickHouseBatchAdapter,
        HighScaleBatch,
    )
    from backend.high_scale.schema import install_clickhouse_schema

    # Apply configuration overrides to environment
    if clickhouse_host:
        os.environ["CLICKHOUSE_HOST"] = clickhouse_host
    if clickhouse_port:
        os.environ["CLICKHOUSE_PORT"] = str(clickhouse_port)
    if clickhouse_database:
        os.environ["CLICKHOUSE_DATABASE"] = clickhouse_database
    if clickhouse_user:
        os.environ["CLICKHOUSE_USER"] = clickhouse_user
    if clickhouse_password is not None:
        os.environ["CLICKHOUSE_PASSWORD"] = clickhouse_password

    os.environ.setdefault("CLICKHOUSE_ENABLED", "true")
    os.environ.setdefault("CLICKHOUSE_HOST", "localhost")
    os.environ.setdefault("CLICKHOUSE_DATABASE", "default")
    os.environ.setdefault("CLICKHOUSE_USER", "default")
    os.environ.setdefault("CLICKHOUSE_SECURE", "false")

    client: ClickHouseClient | None = None
    if not dry_run:
        try:
            client = get_clickhouse_client()
            if client is None:
                from backend.config import load_clickhouse_config

                cfg = load_clickhouse_config()
                if cfg:
                    client = ClickHouseClient(cfg)
            if client is None:
                print("ERROR: Could not initialize ClickHouse client. Check CLICKHOUSE_* settings.", file=sys.stderr)
                return 1
            client.health()
            install_clickhouse_schema()
        except Exception as exc:
            print(f"ERROR: ClickHouse connection/schema initialization failed: {exc}", file=sys.stderr)
            return 1

    service_id = src.get("service_id", "default_service")
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    pacing_desc = (
        f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
        if rate_rps > 0
        else "Unthrottled (maximum throughput)"
    )

    print(
        f"Generating and inserting {rows:,} log records [{scenario}] into ClickHouse\n"
        f"  Service: {service_id}\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Pacing: {pacing_desc}"
    )

    rows_remaining = rows
    batch_idx = 0
    t0 = time.monotonic()
    adapter = ClickHouseBatchAdapter(client) if client else None
    total_rows_emitted = 0

    while rows_remaining > 0:
        b_t0 = time.monotonic()
        elapsed_total = b_t0 - t0

        if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
            target_rps = burst_rps
            in_burst = True
        elif rate_rps > 0:
            target_rps = rate_rps
            in_burst = False
        else:
            target_rps = 0
            in_burst = False

        n = min(batch_size, rows_remaining)
        cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)
        cols_py = {k: cols[k].tolist() if hasattr(cols[k], "tolist") else cols[k] for k in cols}

        batch_rows = []
        batch_uid = uuid4().hex[:8]
        ts_list = cols_py["timestamp"]
        for i in range(n):
            row_dict = {k: cols_py[k][i] for k in cols_py if k != "_source_file"}
            ts_us = ts_list[i]
            row_dt = datetime.fromtimestamp(ts_us / 1_000_000, tz=UTC)
            row_dict["timestamp"] = row_dt.isoformat()
            row_dict["event_id"] = f"syn_{batch_idx}_{i}_{batch_uid}"
            row_dict["source_object_key"] = f"synthetic://{service_id}/{batch_idx}"
            row_dict["source_object_version"] = "1"
            row_dict["line_ordinal"] = i
            row_dict["transform_version"] = "v1"
            row_dict["client_ip"] = str(row_dict.get("ip", ""))
            batch_rows.append(row_dict)

        batch_id = f"batch_{service_id}_{batch_idx}_{batch_uid}"
        batch = HighScaleBatch(
            batch_id=batch_id,
            service_id=service_id,
            domain="request",
            generation="1",
            rows=tuple(batch_rows),
        )

        mode_str = f"BURST {burst_rps:,} rps" if in_burst else (f"NORMAL {rate_rps:,} rps" if rate_rps > 0 else "MAX")

        if dry_run:
            print(
                f"  [dry-run] [{mode_str}] Would insert ClickHouse batch {batch_id}: {n:,} rows ({batch.digest[:16]}...)"
            )
        else:
            assert adapter is not None
            receipt = adapter.insert(batch)
            print(
                f"  inserted [{mode_str}] ClickHouse batch {receipt.batch_id}: "
                f"{receipt.rows_inserted:,} rows ({receipt.digest[:16]}...)"
            )

        total_rows_emitted += n
        rows_remaining -= n
        batch_idx += 1

        if target_rps > 0:
            target_batch_sec = n / target_rps
            b_dur = time.monotonic() - b_t0
            if target_batch_sec > b_dur:
                time.sleep(target_batch_sec - b_dur)

    elapsed = time.monotonic() - t0
    rate = total_rows_emitted / max(elapsed, 0.001)
    print(
        f"\nSUCCESS: Finished ClickHouse insertion of {total_rows_emitted:,} rows in {elapsed:.2f}s ({rate:,.0f} rows/s)."
    )
    return 0


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--service-id",
        help="Service ID to populate. Defaults to first available config.",
    )
    parser.add_argument(
        "--config",
        help="Path to service config JSON file.",
    )
    parser.add_argument(
        "--target",
        choices=["local", "fos", "clickhouse"],
        default="local",
        help="Target data tier: 'local' (direct DuckLake Parquet), 'fos' (raw gzipped S3 upload), or 'clickhouse'.",
    )
    parser.add_argument(
        "--with-clickhouse",
        action="store_true",
        help="Also seed generated batches directly to ClickHouse (works alongside --target local or --target fos).",
    )
    parser.add_argument(
        "--clickhouse-host",
        help="ClickHouse host (defaults to CLICKHOUSE_HOST env or 'localhost').",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        help="ClickHouse HTTP port (defaults to CLICKHOUSE_PORT env or 8123).",
    )
    parser.add_argument(
        "--clickhouse-database",
        help="ClickHouse database (defaults to CLICKHOUSE_DATABASE env or 'default').",
    )
    parser.add_argument(
        "--clickhouse-user",
        help="ClickHouse user (defaults to CLICKHOUSE_USER env or 'default').",
    )
    parser.add_argument(
        "--clickhouse-password",
        help="ClickHouse password (defaults to CLICKHOUSE_PASSWORD env or '').",
    )
    parser.add_argument(
        "--scenario",
        "--profile",
        dest="scenario",
        choices=["diurnal", "ddos-spike", "origin-5xx-outage", "bot-scrape", "slow-network"],
        default="diurnal",
        help="Incident/traffic pattern scenario.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Total rows to generate (default: 100,000).",
    )
    parser.add_argument(
        "--span-hours",
        type=int,
        default=24,
        help="Time window span in hours (default: 24).",
    )
    parser.add_argument(
        "--span-days",
        type=int,
        help="Time window span in days (overrides --span-hours).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25_000,
        help="Rows per generation batch.",
    )
    parser.add_argument(
        "--file-rows",
        type=int,
        default=50_000,
        help="Rows per output Parquet file.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for reproducibility.",
    )
    parser.add_argument(
        "--commit",
        dest="commit",
        action="store_true",
        default=True,
        help="Commit buffer to DuckLake table after writing (default: True).",
    )
    parser.add_argument(
        "--no-commit",
        dest="commit",
        action="store_false",
        help="Skip committing buffer to DuckLake table.",
    )
    parser.add_argument(
        "--rate-rps",
        type=int,
        default=0,
        help="Sustained rate pacing in requests per second (e.g. 50000). 0 disables rate pacing (max throughput).",
    )
    parser.add_argument(
        "--burst-rps",
        type=int,
        default=0,
        help="Burst rate pacing in requests per second (e.g. 100000). 0 disables burst modulation.",
    )
    parser.add_argument(
        "--burst-duration",
        type=int,
        default=30,
        help="Duration of each burst in seconds (default: 30s).",
    )
    parser.add_argument(
        "--burst-interval",
        type=int,
        default=120,
        help="Interval between bursts in seconds (default: 120s).",
    )
    parser.add_argument(
        "--upload-workers",
        type=int,
        default=8,
        help="Concurrent S3 upload worker threads for --target fos (default: 8).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Purge existing buffer files before generating.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview generation without writing files or uploading.",
    )

    args = parser.parse_args()

    # Resolve service configuration
    src = None
    if args.config:
        with open(args.config) as f:
            src = json.load(f)
    elif args.service_id:
        src = load_config(args.service_id)
    else:
        configs = list_configs()
        if configs:
            src = configs[0]
            print(f"Using default active service: {src.get('service_id')}")
        else:
            print("ERROR: No configured services found in configs/", file=sys.stderr)
            return 2

    if not src:
        print(f"ERROR: Could not load configuration for service {args.service_id!r}", file=sys.stderr)
        return 2

    span_hours = args.span_days * 24 if args.span_days else args.span_hours
    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(hours=span_hours)

    res = 0
    if args.target == "local":
        res = run_target_local(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            file_rows=args.file_rows,
            seed=args.seed,
            commit=args.commit,
            clean=args.clean,
            dry_run=args.dry_run,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
        )
    elif args.target == "fos":
        res = run_target_fos(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            seed=args.seed,
            dry_run=args.dry_run,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
            upload_workers=args.upload_workers,
        )
    elif args.target == "clickhouse":
        res = run_target_clickhouse(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            seed=args.seed,
            dry_run=args.dry_run,
            clickhouse_host=args.clickhouse_host,
            clickhouse_port=args.clickhouse_port,
            clickhouse_database=args.clickhouse_database,
            clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
        )

    if res == 0 and args.with_clickhouse and args.target != "clickhouse":
        res = run_target_clickhouse(
            src=src,
            scenario=args.scenario,
            rows=args.rows,
            start_dt=start_dt,
            end_dt=end_dt,
            batch_size=args.batch_size,
            seed=args.seed,
            dry_run=args.dry_run,
            clickhouse_host=args.clickhouse_host,
            clickhouse_port=args.clickhouse_port,
            clickhouse_database=args.clickhouse_database,
            clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password,
            rate_rps=args.rate_rps,
            burst_rps=args.burst_rps,
            burst_duration=args.burst_duration,
            burst_interval=args.burst_interval,
        )

    return res


if __name__ == "__main__":
    sys.exit(main())
