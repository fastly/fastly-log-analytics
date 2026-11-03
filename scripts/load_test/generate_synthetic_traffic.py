#!/usr/bin/env python3
"""Unified Multi-Tier Synthetic Traffic Generator.

Supports:
  1. --target local: Writes synthetic Parquet batches into `cache/{bucket}/buffer/`
     and optionally calls `commit_buffer` to materialize DuckLake snapshots.
  2. --target fos: Generates gzipped NDJSON chunks conforming to
     `backend/provision/cli.py` and uploads them to FOS object storage.
  3. --target clickhouse / --with-clickhouse: Directly seeds ClickHouse
     fact tables (`request_facts`) and registers publication batches.

Scenario Profiles: diurnal, ddos-spike, origin-5xx-outage, bot-scrape, slow-network.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import os
import random
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None  # type: ignore[assignment]

for p in [Path.cwd(), Path("/app"), Path(__file__).resolve().parent.parent.parent]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from backend.config import list_configs, load_config  # noqa: E402
from backend.core.storage import (  # noqa: E402
    _buffer_dir,
    commit_buffer,
    get_arrow_schema,
)
from scripts.lib.log_factory import (  # noqa: E402
    build_pop_wave_weights,
    generate_rum_records,
)
from scripts.lib.log_factory import (
    cols_to_arrow_table as _cols_to_arrow_table,
)
from scripts.lib.log_factory import (
    generate_columns as _generate_batch_data,
)

# fmt: off


def _pacing_desc(rate_rps: int, burst_rps: int, burst_duration: int, burst_interval: int) -> str:
    if rate_rps > 0:
        return f"{rate_rps:,} rps (burst: {burst_rps:,} rps for {burst_duration}s every {burst_interval}s)"
    return "Unthrottled (maximum throughput)"


def _step_pacing(
    elapsed_total: float, rate_rps: int, burst_rps: int, burst_duration: int, burst_interval: int
) -> tuple[int, str]:
    if burst_rps > 0 and (elapsed_total % burst_interval) < burst_duration:
        return burst_rps, f"BURST {burst_rps:,} rps"
    if rate_rps > 0:
        return rate_rps, f"NORMAL {rate_rps:,} rps"
    return 0, "MAX"


def _wave_window_ms(now_ms: int, log_period: int, live_timestamps: bool, run_window: tuple[int, int]) -> tuple[int, int]:
    """Event-time window (ms) for one POP wave.

    A live feed stamps events inside the last ``log_period`` seconds, as real Fastly delivery does, so only the current
    hour is touched. The default keeps the run window stretched up to now, which suits backfills.
    """
    if live_timestamps:
        return now_ms - log_period * 1000, now_ms
    return run_window[0], max(run_window[1], now_ms)


def _sleep_for_rate(n: int, target_rps: int, b_t0: float) -> None:
    if target_rps > 0:
        target_batch_sec = n / target_rps
        b_dur = time.monotonic() - b_t0
        if target_batch_sec > b_dur:
            time.sleep(target_batch_sec - b_dur)


def _validate_generation_args(
    *,
    rows: int,
    batch_size: int,
    file_rows: int | None = None,
    compression: str = "zstd",
    pop_count: int = 20,
    log_period: int = 10,
    gen_workers: int = 1,
    upload_workers: int = 8,
    rum_ratio: float = 0.0,
) -> None:
    if rows < 0:
        raise ValueError(f"rows must be >= 0 (got {rows})")
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0 (got {batch_size})")
    if file_rows is not None and file_rows <= 0:
        raise ValueError(f"file_rows must be > 0 (got {file_rows})")
    if compression not in {"zstd", "gzip"}:
        raise ValueError(f"compression must be 'zstd' or 'gzip' (got {compression!r})")
    if pop_count <= 0:
        raise ValueError(f"pop_count must be > 0 (got {pop_count})")
    if log_period <= 0:
        raise ValueError(f"log_period must be > 0 (got {log_period})")
    if gen_workers <= 0:
        raise ValueError(f"gen_workers must be > 0 (got {gen_workers})")
    if upload_workers <= 0:
        raise ValueError(f"upload_workers must be > 0 (got {upload_workers})")
    if not (0.0 <= rum_ratio <= 1.0):
        raise ValueError(f"rum_ratio must be between 0.0 and 1.0 (got {rum_ratio})")


def _compress_ndjson(raw_bytes: bytes, compression: str) -> tuple[bytes, str, str]:
    """Compress raw NDJSON bytes and return ``(compressed_bytes, ext, content_type)``."""
    if compression == "zstd":
        import zstandard as zstd

        cctx = zstd.ZstdCompressor(level=1)
        return cctx.compress(raw_bytes), "zst", "application/zstd"
    if compression == "gzip":
        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb", compresslevel=1) as gz:
            gz.write(raw_bytes)
        return gz_buf.getvalue(), "gz", "application/gzip"
    raise ValueError(f"Unsupported compression {compression!r}")


def _serialize_record_bytes(record: dict[str, Any]) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(record)
    return json.dumps(record, default=str, separators=(",", ":")).encode("utf-8")


def _build_fos_chunk_worker(
    n: int,
    start_ms: int,
    end_ms: int,
    scenario: str,
    seed: int,
    compression: str = "zstd",
    pop_override: str | None = None,
) -> tuple[bytes, int, str, str]:
    """Generate and compress a single FOS request-log NDJSON chunk (picklable for ProcessPoolExecutor)."""
    rng = np.random.default_rng(seed)
    cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng, pop_override=pop_override)
    ts_iso = np.datetime_as_string(
        (cols["timestamp"] // 1_000_000).astype("datetime64[s]"),
        unit="s",
        timezone="UTC",
    ).tolist()
    keys = [k for k in cols if k not in ("_source_file", "timestamp")]
    cols_py = {k: cols[k].tolist() if hasattr(cols[k], "tolist") else cols[k] for k in keys}
    line_buffers = []
    for i in range(n):
        rec = {k: cols_py[k][i] for k in keys}
        rec["timestamp"] = ts_iso[i]
        line_buffers.append(_serialize_record_bytes(rec))
    raw_bytes = b"\n".join(line_buffers)
    compressed, ext, content_type = _compress_ndjson(raw_bytes, compression)
    return compressed, n, ext, content_type


def _build_fos_rum_chunk_worker(
    service_id: str,
    n: int,
    now_iso: str,
    seed: int,
    compression: str = "zstd",
) -> tuple[bytes, int, str, str]:
    """Generate and compress a single FOS RUM-beacon NDJSON chunk."""
    now_dt = datetime.fromisoformat(now_iso)
    py_rng = random.Random(seed)
    records = generate_rum_records(service_id, n, now_dt, rng=py_rng)
    raw_bytes = b"\n".join(_serialize_record_bytes(rec) for rec in records)
    compressed, ext, content_type = _compress_ndjson(raw_bytes, compression)
    return compressed, n, ext, content_type


def _allocate_pop_wave_chunks(
    total_rows: int,
    pop_count: int,
    max_chunk_rows: int,
) -> list[tuple[str, int]]:
    """Distribute ``total_rows`` across ``pop_count`` POPs with Zipf weights, splitting chunks above ``max_chunk_rows``."""
    pops, weights = build_pop_wave_weights(pop_count)
    exact = [total_rows * w for w in weights]
    counts = [int(math.floor(x)) for x in exact]
    remainder = total_rows - sum(counts)
    if remainder > 0:
        order = sorted(range(pop_count), key=lambda idx: exact[idx] - counts[idx], reverse=True)
        for idx in order[:remainder]:
            counts[idx] += 1
    chunks: list[tuple[str, int]] = []
    for pop, pop_rows in zip(pops, counts, strict=True):
        rem = pop_rows
        while rem > 0:
            take = min(max_chunk_rows, rem)
            chunks.append((pop, take))
            rem -= take
    chunks.sort(key=lambda item: item[0])
    return chunks


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
    _validate_generation_args(rows=rows, batch_size=batch_size, file_rows=file_rows)
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
    start_ms, end_ms = int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)
    t0 = time.monotonic()
    rows_remaining, file_idx, total_rows = rows, 0, 0

    print(
        f"Generating {rows:,} rows [{scenario}] for service '{src.get('service_id')}'\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Target: Local buffer ({buf_dir})\n"
        f"  Pacing: {_pacing_desc(rate_rps, burst_rps, burst_duration, burst_interval)}"
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
            target_rps, _ = _step_pacing(b_t0 - t0, rate_rps, burst_rps, burst_duration, burst_interval)
            n = min(batch_size, rows_this_file - rows_in_this_file)
            cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)
            tbl = _cols_to_arrow_table(cols, schema).sort_by([("timestamp", "ascending"), ("ip", "ascending")])
            writer.write_table(tbl)
            rows_in_this_file += n
            _sleep_for_rate(n, target_rps, b_t0)

        writer.close()
        rows_remaining -= rows_this_file
        total_rows += rows_this_file
        file_idx += 1
        elapsed = time.monotonic() - t0
        rate = total_rows / max(elapsed, 0.001)
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        print(
            f"  wrote {fname}: {rows_this_file:,} rows ({size_mb:.2f} MB) | "
            f"total: {total_rows:,}/{rows:,} ({100 * total_rows / rows:.1f}%) | {rate:,.0f} rows/s",
            flush=True,
        )

    total_elapsed = time.monotonic() - t0
    print(f"\nSUCCESS: Generated {total_rows:,} rows in {total_elapsed:.2f}s ({total_rows / total_elapsed:,.0f} rows/s).")

    if commit:
        print("\nCommitting buffer to DuckLake table...", flush=True)
        t_commit = time.monotonic()
        res = commit_buffer(src)
        print(
            f"COMMITTED: {res.get('rows_committed', 0):,} rows in "
            f"{res.get('files_committed', 0)} files in {time.monotonic() - t_commit:.2f}s."
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
    compression: str = "zstd",
    gen_workers: int = 1,
    simulate_pops: bool = False,
    pop_count: int = 20,
    log_period: int = 10,
    rum_ratio: float = 0.0,
    live_timestamps: bool = False,
) -> int:
    """Generates compressed NDJSON (.log.zst or .log.gz) and uploads to FOS with optional POP-wave simulation."""
    _validate_generation_args(
        rows=rows,
        batch_size=batch_size,
        compression=compression,
        pop_count=pop_count,
        log_period=log_period,
        gen_workers=gen_workers,
        upload_workers=upload_workers,
        rum_ratio=rum_ratio,
    )
    from backend.provision.cli import minute_list_prefix, rum_minute_list_prefix

    bucket = src.get("s3_bucket") or src.get("fos_bucket")
    if not bucket:
        print("ERROR: Service config does not specify an s3_bucket / fos_bucket", file=sys.stderr)
        return 1

    s3_client: Any = None
    if not dry_run:
        import boto3
        from botocore.config import Config

        endpoint = src.get("s3_endpoint") or src.get("fos_endpoint")
        if endpoint and not endpoint.startswith(("http://", "https://")):
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
                # botocore >=1.36 streaming trailer checksums are rejected by FOS
                request_checksum_calculation="when_required",
            ),
        )

    service_id = str(src.get("service_id") or "default_service")
    start_ms, end_ms = int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)

    print(
        f"Generating and uploading {rows:,} raw log records [{scenario}] to FOS\n"
        f"  Bucket: {bucket}\n"
        f"  Service: {service_id}\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Compression: {compression} | Gen Workers: {gen_workers} | Upload Workers: {upload_workers}\n"
        f"  POP Simulation: {'enabled' if simulate_pops else 'disabled'} "
        f"(pops={pop_count}, period={log_period}s, rum_ratio={rum_ratio})\n"
        f"  Pacing: {_pacing_desc(rate_rps, burst_rps, burst_duration, burst_interval)}"
    )

    rows_remaining, file_idx, wave_idx = rows, 0, 0
    total_bytes_uploaded, total_rows_emitted, total_rum_rows_emitted = 0, 0, 0
    t0 = time.monotonic()
    upload_pool = ThreadPoolExecutor(max_workers=upload_workers) if not dry_run else None
    gen_pool = ProcessPoolExecutor(max_workers=gen_workers) if gen_workers > 1 else None
    pending_uploads: set[Any] = set()

    def _upload_task(bucket_name: str, key_name: str, payload: bytes, content_type: str) -> int:
        assert s3_client is not None
        s3_client.put_object(Bucket=bucket_name, Key=key_name, Body=payload, ContentType=content_type)
        return len(payload)

    def _enqueue_upload(s3_key: str, payload: bytes, content_type: str, row_count: int, mode_str: str) -> None:
        nonlocal pending_uploads, total_bytes_uploaded
        if dry_run:
            total_bytes_uploaded += len(payload)
            print(
                f"  [dry-run] [{mode_str}] {row_count:,} rows -> s3://{bucket}/{s3_key} ({len(payload):,} bytes)"
            )
            return
        assert upload_pool is not None
        if len(pending_uploads) >= upload_workers * 3:
            done, pending_uploads = wait(pending_uploads, return_when=FIRST_COMPLETED)
            for fut in done:
                total_bytes_uploaded += fut.result()
        pending_uploads.add(upload_pool.submit(_upload_task, bucket, s3_key, payload, content_type))

    try:
        while rows_remaining > 0:
            b_t0 = time.monotonic()
            target_rps, mode_str = _step_pacing(b_t0 - t0, rate_rps, burst_rps, burst_duration, burst_interval)
            now_utc = datetime.now(UTC)
            wave_start_ms, wave_end_ms = _wave_window_ms(
                int(now_utc.timestamp() * 1000), log_period, live_timestamps, (start_ms, end_ms)
            )
            prefix = (src.get("s3_prefix") or src.get("fos_prefix") or "").strip("/")
            min_prefix = minute_list_prefix(now_utc)
            base_dir = f"{prefix}/{min_prefix}" if prefix else min_prefix
            ts_slug = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000")

            if simulate_pops:
                if target_rps > 0:
                    wave_rows = min(rows_remaining, max(1, target_rps * log_period))
                else:
                    wave_rows = min(rows_remaining, max(batch_size, batch_size * min(pop_count, max(1, gen_workers))))
                pop_chunks = _allocate_pop_wave_chunks(wave_rows, pop_count, batch_size)
                specs = [
                    (
                        chunk_rows,
                        wave_start_ms,
                        wave_end_ms,
                        scenario,
                        seed + file_idx + idx + 1,
                        compression,
                        pop_code,
                    )
                    for idx, (pop_code, chunk_rows) in enumerate(pop_chunks)
                ]
            else:
                num_chunks = min(max(1, gen_workers), math.ceil(rows_remaining / batch_size))
                specs = []
                rem_loop = rows_remaining
                for idx in range(num_chunks):
                    take = min(batch_size, rem_loop)
                    if take <= 0:
                        break
                    specs.append((take, wave_start_ms, wave_end_ms, scenario, seed + file_idx + idx + 1, compression, None))
                    rem_loop -= take

            if gen_pool is not None and len(specs) > 1:
                gen_futures = [gen_pool.submit(_build_fos_chunk_worker, *sp) for sp in specs]
                results = [fut.result() for fut in gen_futures]
            else:
                results = [_build_fos_chunk_worker(*sp) for sp in specs]

            step_rows = 0
            step_bytes = 0
            for sp, (payload, n_chunk, ext, content_type) in zip(specs, results, strict=True):
                pop_label = sp[6] or "EDGE"
                s3_key = f"{base_dir}{ts_slug}-{service_id}-{pop_label}-{file_idx:05d}.log.{ext}"
                _enqueue_upload(s3_key, payload, content_type, n_chunk, mode_str)
                step_rows += n_chunk
                step_bytes += len(payload)
                file_idx += 1

            if rum_ratio > 0.0 and step_rows > 0:
                rum_rows = max(1, int(round(step_rows * rum_ratio)))
                rum_payload, rum_n, rum_ext, rum_ct = _build_fos_rum_chunk_worker(
                    service_id,
                    rum_rows,
                    now_utc.isoformat(),
                    seed + 1_000_000 + wave_idx,
                    compression,
                )
                rum_min_prefix = rum_minute_list_prefix(now_utc)
                rum_base_dir = f"{prefix}/{rum_min_prefix}" if prefix else rum_min_prefix
                rum_key = f"{rum_base_dir}{ts_slug}-{service_id}-rum-{wave_idx:05d}.log.{rum_ext}"
                _enqueue_upload(rum_key, rum_payload, rum_ct, rum_n, f"{mode_str}/RUM")
                total_rum_rows_emitted += rum_n
                step_bytes += len(rum_payload)

            total_rows_emitted += step_rows
            rows_remaining -= step_rows
            wave_idx += 1
            elapsed_so_far = max(time.monotonic() - t0, 0.001)
            eff_rps = total_rows_emitted / elapsed_so_far
            print(
                f"  wave {wave_idx:03d} [{mode_str}] files={len(specs)} rows={step_rows:,} "
                f"bytes={step_bytes:,} | total={total_rows_emitted:,}/{rows:,} ({eff_rps:,.0f} rows/s)",
                flush=True,
            )
            _sleep_for_rate(step_rows, target_rps, b_t0)

        if upload_pool and pending_uploads:
            print(f"Waiting for {len(pending_uploads)} pending S3 uploads to complete...")
            done, _ = wait(pending_uploads)
            for fut in done:
                total_bytes_uploaded += fut.result()
    finally:
        if gen_pool:
            gen_pool.shutdown(wait=True)
        if upload_pool:
            upload_pool.shutdown(wait=True)

    elapsed = time.monotonic() - t0
    rate = total_rows_emitted / max(elapsed, 0.001)
    rum_suffix = f" (+ {total_rum_rows_emitted:,} RUM beacons)" if total_rum_rows_emitted else ""
    print(
        f"\nSUCCESS: Finished FOS upload of {total_rows_emitted:,} rows{rum_suffix} "
        f"({total_bytes_uploaded / (1024 * 1024):.2f} MB compressed) in {elapsed:.2f}s ({rate:,.0f} rows/s)."
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
    _validate_generation_args(rows=rows, batch_size=batch_size)
    from uuid import uuid4

    from backend.core.clickhouse import ClickHouseClient, get_clickhouse_client
    from backend.high_scale.archive import ClickHouseBatchAdapter, HighScaleBatch, install_clickhouse_schema

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
    start_ms, end_ms = int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)
    rng = np.random.default_rng(seed)

    print(
        f"Generating and inserting {rows:,} log records [{scenario}] into ClickHouse\n"
        f"  Service: {service_id}\n"
        f"  Window: {start_dt.isoformat()} -> {end_dt.isoformat()}\n"
        f"  Pacing: {_pacing_desc(rate_rps, burst_rps, burst_duration, burst_interval)}"
    )

    rows_remaining, batch_idx, total_rows_emitted = rows, 0, 0
    t0 = time.monotonic()
    adapter = ClickHouseBatchAdapter(client) if client else None

    while rows_remaining > 0:
        b_t0 = time.monotonic()
        target_rps, mode_str = _step_pacing(b_t0 - t0, rate_rps, burst_rps, burst_duration, burst_interval)
        n = min(batch_size, rows_remaining)
        cols = _generate_batch_data(n, start_ms, end_ms, scenario, rng)
        cols_py = {k: cols[k].tolist() if hasattr(cols[k], "tolist") else cols[k] for k in cols}

        batch_rows = []
        batch_uid = uuid4().hex[:8]
        ts_list = cols_py["timestamp"]
        for i in range(n):
            row_dict = {k: cols_py[k][i] for k in cols_py if k != "_source_file"}
            row_dt = datetime.fromtimestamp(ts_list[i] / 1_000_000, tz=UTC)
            row_dict["timestamp"] = row_dt.isoformat()
            row_dict["event_id"] = f"syn_{batch_idx}_{i}_{batch_uid}"
            row_dict["source_object_key"] = f"synthetic://{service_id}/{batch_idx}"
            row_dict["source_object_version"] = "1"
            row_dict["line_ordinal"] = i
            row_dict["transform_version"] = "v1"
            row_dict["client_ip"] = str(row_dict.get("ip", ""))
            batch_rows.append(row_dict)

        batch_id = f"batch_{service_id}_{batch_idx}_{batch_uid}"
        batch = HighScaleBatch(batch_id=batch_id, service_id=service_id, domain="request", generation="1", rows=tuple(batch_rows))

        if dry_run:
            print(f"  [dry-run] [{mode_str}] Would insert ClickHouse batch {batch_id}: {n:,} rows ({batch.digest[:16]}...)")
        else:
            assert adapter is not None
            receipt = adapter.insert(batch)
            print(f"  inserted [{mode_str}] ClickHouse batch {receipt.batch_id}: {receipt.rows_inserted:,} rows ({receipt.digest[:16]}...)")

        total_rows_emitted += n
        rows_remaining -= n
        batch_idx += 1
        _sleep_for_rate(n, target_rps, b_t0)

    elapsed = time.monotonic() - t0
    rate = total_rows_emitted / max(elapsed, 0.001)
    print(f"\nSUCCESS: Finished ClickHouse insertion of {total_rows_emitted:,} rows in {elapsed:.2f}s ({rate:,.0f} rows/s).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--service-id", help="Service ID to populate. Defaults to first available config.")
    parser.add_argument("--config", help="Path to service config JSON file.")
    parser.add_argument("--target", choices=["local", "fos", "clickhouse"], default="local", help="Target data tier: 'local', 'fos', or 'clickhouse'.")
    parser.add_argument("--with-clickhouse", action="store_true", help="Also seed generated batches directly to ClickHouse.")
    parser.add_argument("--clickhouse-host", help="ClickHouse host (defaults to CLICKHOUSE_HOST env or 'localhost').")
    parser.add_argument("--clickhouse-port", type=int, help="ClickHouse HTTP port (defaults to CLICKHOUSE_PORT env or 8123).")
    parser.add_argument("--clickhouse-database", help="ClickHouse database (defaults to CLICKHOUSE_DATABASE env or 'default').")
    parser.add_argument("--clickhouse-user", help="ClickHouse user (defaults to CLICKHOUSE_USER env or 'default').")
    parser.add_argument("--clickhouse-password", help="ClickHouse password (defaults to CLICKHOUSE_PASSWORD env or '').")
    parser.add_argument(
        "--scenario", "--profile", dest="scenario",
        choices=["diurnal", "ddos-spike", "origin-5xx-outage", "bot-scrape", "slow-network"],
        default="diurnal", help="Incident/traffic pattern scenario.",
    )
    parser.add_argument("--rows", type=int, default=100_000, help="Total rows to generate (default: 100,000).")
    parser.add_argument("--span-hours", type=int, default=24, help="Time window span in hours (default: 24).")
    parser.add_argument("--span-days", type=int, help="Time window span in days (overrides --span-hours).")
    parser.add_argument("--batch-size", type=int, default=25_000, help="Rows per generation batch.")
    parser.add_argument("--file-rows", type=int, default=50_000, help="Rows per output Parquet file.")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility.")
    parser.add_argument("--commit", dest="commit", action="store_true", default=True, help="Commit buffer to DuckLake table after writing (default: True).")
    parser.add_argument("--no-commit", dest="commit", action="store_false", help="Skip committing buffer to DuckLake table.")
    parser.add_argument("--rate-rps", type=int, default=0, help="Sustained rate pacing in requests per second (0 disables).")
    parser.add_argument("--burst-rps", type=int, default=0, help="Burst rate pacing in requests per second (0 disables).")
    parser.add_argument("--burst-duration", type=int, default=30, help="Duration of each burst in seconds (default: 30s).")
    parser.add_argument("--burst-interval", type=int, default=120, help="Interval between bursts in seconds (default: 120s).")
    parser.add_argument("--upload-workers", type=int, default=8, help="Concurrent S3 upload worker threads for --target fos (default: 8).")
    parser.add_argument("--gen-workers", type=int, default=1, help="Parallel generation worker processes for --target fos (default: 1).")
    parser.add_argument("--compression", choices=["zstd", "gzip"], default="zstd", help="Compression format for --target fos: 'zstd' (.log.zst, default) or 'gzip' (.log.gz).")
    parser.add_argument("--simulate-pops", action="store_true", help="Simulate Fastly edge POP-wave log batching across --pop-count POPs every --log-period seconds.")
    parser.add_argument("--live-timestamps", action="store_true", help="For --target fos: stamp each wave's events inside the last --log-period seconds (a realistic live feed) instead of spreading them across the run window.")
    parser.add_argument("--pop-count", type=int, default=20, help="Number of simulated edge POPs when --simulate-pops is enabled (default: 20).")
    parser.add_argument("--log-period", type=int, default=10, help="Fastly edge log flush period in seconds when --simulate-pops is enabled (default: 10s).")
    parser.add_argument("--rum-ratio", type=float, default=0.0, help="Optional fraction of RUM beacon events to emit alongside request logs for --target fos (0.0 to 1.0).")
    parser.add_argument("--clean", action="store_true", help="Purge existing buffer files before generating.")
    parser.add_argument("--dry-run", action="store_true", help="Preview generation without writing files or uploading.")

    args = parser.parse_args()
    try:
        _validate_generation_args(
            rows=args.rows,
            batch_size=args.batch_size,
            file_rows=args.file_rows if args.target == "local" else None,
            compression=args.compression,
            pop_count=args.pop_count,
            log_period=args.log_period,
            gen_workers=args.gen_workers,
            upload_workers=args.upload_workers,
            rum_ratio=args.rum_ratio,
        )
    except ValueError as exc:
        parser.error(str(exc))

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
            src=src, scenario=args.scenario, rows=args.rows, start_dt=start_dt, end_dt=end_dt,
            batch_size=args.batch_size, file_rows=args.file_rows, seed=args.seed, commit=args.commit,
            clean=args.clean, dry_run=args.dry_run, rate_rps=args.rate_rps, burst_rps=args.burst_rps,
            burst_duration=args.burst_duration, burst_interval=args.burst_interval,
        )
    elif args.target == "fos":
        res = run_target_fos(
            src=src, scenario=args.scenario, rows=args.rows, start_dt=start_dt, end_dt=end_dt,
            batch_size=args.batch_size, seed=args.seed, dry_run=args.dry_run, rate_rps=args.rate_rps,
            burst_rps=args.burst_rps, burst_duration=args.burst_duration, burst_interval=args.burst_interval,
            upload_workers=args.upload_workers, compression=args.compression, gen_workers=args.gen_workers,
            simulate_pops=args.simulate_pops, pop_count=args.pop_count, log_period=args.log_period,
            rum_ratio=args.rum_ratio, live_timestamps=args.live_timestamps,
        )
    elif args.target == "clickhouse":
        res = run_target_clickhouse(
            src=src, scenario=args.scenario, rows=args.rows, start_dt=start_dt, end_dt=end_dt,
            batch_size=args.batch_size, seed=args.seed, dry_run=args.dry_run,
            clickhouse_host=args.clickhouse_host, clickhouse_port=args.clickhouse_port,
            clickhouse_database=args.clickhouse_database, clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password, rate_rps=args.rate_rps, burst_rps=args.burst_rps,
            burst_duration=args.burst_duration, burst_interval=args.burst_interval,
        )

    if res == 0 and args.with_clickhouse and args.target != "clickhouse":
        res = run_target_clickhouse(
            src=src, scenario=args.scenario, rows=args.rows, start_dt=start_dt, end_dt=end_dt,
            batch_size=args.batch_size, seed=args.seed, dry_run=args.dry_run,
            clickhouse_host=args.clickhouse_host, clickhouse_port=args.clickhouse_port,
            clickhouse_database=args.clickhouse_database, clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password, rate_rps=args.rate_rps, burst_rps=args.burst_rps,
            burst_duration=args.burst_duration, burst_interval=args.burst_interval,
        )

    return res


if __name__ == "__main__":
    sys.exit(main())
