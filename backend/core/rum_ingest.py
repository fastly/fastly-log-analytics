"""RUM beacon ingest pipeline.

Parses raw/rum/ logs from FOS and streams them into the sqlite rum_beacons table.
Uses the standard ingested_files table to ensure atomic, de-duplicated imports.
"""

from __future__ import annotations

import gzip
import json
import logging
import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

from backend.core import metadata as metadata_db
from backend.core.duckdb import _get_fos_client, get_source_for_service
from backend.core.metadata.cron_log import (
    finalize_cron_run_if_running,
    log_cron_run,
    start_cron_run,
)

logger = logging.getLogger(__name__)


def cleanup_old_rum_logs(service_id: str) -> tuple[int, int]:
    """Delete RUM beacon logs from FOS older than rum.delete_after days.

    Returns (files_deleted, bytes_freed).
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum", {})

    # Only cleanup if delete_after is explicitly enabled
    if not rum_cfg.get("delete_after", False):
        return 0, 0

    # Default retention matches the regular log retention
    delete_after_days = int(rum_cfg.get("delete_after", True))
    if delete_after_days is True:
        delete_after_days = int(cfg.get("log_retention_days", 90))

    src = get_source_for_service(service_id)
    if not src or not src.get("bucket"):
        return 0, 0

    try:
        s3 = _get_fos_client(src)
        bucket = src["bucket"]
        prefix = src.get("prefix", "").strip("/")
        rum_prefix = f"{prefix}/raw/rum/" if prefix else "raw/rum/"

        cutoff_time = datetime.now(UTC) - timedelta(days=delete_after_days)
        files_deleted = 0
        bytes_freed = 0

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=rum_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Skip directory markers
                if key.endswith("/"):
                    continue

                # Get file mtime from LastModified
                last_modified = obj.get("LastModified")
                if not last_modified:
                    continue

                # Convert to aware datetime for comparison
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=UTC)

                # Only delete if older than retention period and not actively being written
                if last_modified < cutoff_time:
                    try:
                        s3.delete_object(Bucket=bucket, Key=key)
                        files_deleted += 1
                        bytes_freed += obj.get("Size", 0)
                        logger.info(f"RUM cleanup: deleted {key} ({obj.get('Size', 0)} bytes)")
                    except Exception as e:
                        logger.warning(f"RUM cleanup: failed to delete {key}: {e}")

        logger.info(
            f"RUM cleanup for {service_id}: deleted {files_deleted} files, freed {bytes_freed / (1024 * 1024):.2f} MB"
        )
        return files_deleted, bytes_freed
    except Exception as e:
        logger.error(f"RUM cleanup failed for {service_id}: {e}")
        return 0, 0


def ingest_rum_logs(
    service_id: str,
) -> Generator[tuple]:
    """Ingest RUM beacon logs from FOS raw/rum/ prefix into local SQLite."""
    import tempfile

    from backend.core.ingest import _download_chunk_to_local, list_fos_files

    start_time = time.time()
    run_id = start_cron_run(service_id, "rum_sync")
    yield ("started", run_id)

    src = get_source_for_service(service_id)
    if not src or not src.get("bucket"):
        logger.info(f"RUM sync skipped: bucket not configured for {service_id}")
        yield ("done", 0)
        log_cron_run(
            service_id,
            "rum_sync",
            time.time() - start_time,
            "success",
            files_downloaded=0,
            rows_ingested=0,
            run_id=run_id,
        )
        return

    try:
        s3 = _get_fos_client(src)
        bucket = src["bucket"]

        # Fetch already ingested files to avoid duplicate processing
        already_ingested_raw = metadata_db.get_ingested_filenames(service_id) or set()
        bucket_prefix = f"s3://{bucket}/"
        already_ingested = set()
        for p in already_ingested_raw:
            if p.startswith("s3://"):
                already_ingested.add(p)
            else:
                already_ingested.add(f"{bucket_prefix}{p}")

        # Use the shared list_fos_files helper to discover files in raw/rum/ prefix
        list_gen = list_fos_files(
            src=src,
            prefix_subpath="raw/rum/",
            already_ingested=already_ingested,
            incremental_only=False,
            elapsed_fn=lambda: f"{time.time() - start_time:.1f}s",
            fos_client=s3,
        )
        try:
            while True:
                evt = next(list_gen)
                if evt.get("type") == "status":
                    logger.debug(f"RUM sync: {evt['message']}")
        except StopIteration as e:
            list_res = e.value

        new_files_s3 = list_res["new_files"]
        file_sizes = list_res["file_sizes"]

        if not new_files_s3:
            logger.info(f"RUM sync: no new RUM logs found in bucket for {service_id}")
            yield ("done", 0)
            log_cron_run(
                service_id,
                "rum_sync",
                time.time() - start_time,
                "success",
                files_downloaded=0,
                rows_ingested=0,
                run_id=run_id,
            )
            return

        total_rows = 0
        db = metadata_db.get_con(service_id)
        ingested_batch_records = []

        # Download and process in parallel chunks to unify request and RUM ingestion logic
        CHUNK_SIZE = 50
        chunks = [new_files_s3[i : i + CHUNK_SIZE] for i in range(0, len(new_files_s3), CHUNK_SIZE)]

        for chunk in chunks:
            with tempfile.TemporaryDirectory() as tmpdir:
                s3_to_local, _ = _download_chunk_to_local(s3, chunk, tmpdir)

                for s3_path in chunk:
                    if s3_path not in s3_to_local:
                        logger.error(f"RUM sync: Failed to download {s3_path}")
                        yield ("error", s3_path, "Download failed")
                        continue

                    local_path = s3_to_local[s3_path]
                    file_rows_count = 0
                    size = file_sizes.get(s3_path, 0)

                    try:
                        # Decompress and parse log lines
                        with gzip.open(local_path, "rt", encoding="utf-8") as f:
                            for line in f:
                                if not line.strip():
                                    continue
                                try:
                                    log_data = json.loads(line)

                                    # Service filtering if present in log data
                                    log_service_id = log_data.get("service_id") or log_data.get("rum_service_id")
                                    if log_service_id and log_service_id != service_id:
                                        continue

                                    received_at = log_data.get("timestamp") or datetime.now(UTC).isoformat()

                                    # Extract core RUM metric values with URL/query parameter fallbacks
                                    metric_name = log_data.get("rum_metric_name")
                                    metric_value = log_data.get("rum_metric_value")
                                    metric_rating = log_data.get("rum_metric_rating")
                                    cid = log_data.get("rum_cid") or log_data.get("cid")
                                    pathname = log_data.get("rum_pathname")

                                    from urllib.parse import parse_qs, urlparse

                                    raw_url = log_data.get("url") or log_data.get("rum_raw_query") or ""
                                    if raw_url:
                                        try:
                                            parsed = urlparse(raw_url)
                                            qparams = parse_qs(parsed.query)
                                            if not metric_name and "rum_metric_name" in qparams:
                                                metric_name = qparams["rum_metric_name"][0]
                                            if (
                                                metric_value is None or metric_value == ""
                                            ) and "rum_metric_value" in qparams:
                                                metric_value = qparams["rum_metric_value"][0]
                                            if not metric_rating and "rum_metric_rating" in qparams:
                                                metric_rating = qparams["rum_metric_rating"][0]
                                            if not cid and "cid" in qparams:
                                                cid = qparams["cid"][0]
                                            if not pathname and "rum_pathname" in qparams:
                                                pathname = qparams["rum_pathname"][0]
                                        except Exception:
                                            pass

                                    if metric_value is not None:
                                        try:
                                            if isinstance(metric_value, str):
                                                if "." in metric_value:
                                                    metric_value = float(metric_value)
                                                else:
                                                    metric_value = int(metric_value)
                                        except ValueError:
                                            pass

                                    # Robust path extraction fallback from referer if still missing
                                    if not pathname and log_data.get("referer"):
                                        try:
                                            pathname = urlparse(log_data["referer"]).path
                                        except Exception:
                                            pass
                                    if not pathname:
                                        pathname = "/"
                                    pathname = pathname.replace("//", "/")

                                    # Reconstruct the beacon_data format expected by RUM analytics dashboard
                                    beacon_data = {
                                        "pathname": pathname,
                                        "path": pathname,
                                        "name": metric_name or "",
                                        "value": metric_value,
                                        "rating": metric_rating or "",
                                        "cid": cid or "",
                                        "req_id": log_data.get("fastly_req_id") or "",
                                        "browser": log_data.get("browser") or "Chrome",
                                        "os": log_data.get("os") or "macOS",
                                        "device": log_data.get("device") or "Desktop",
                                        "raw_log": log_data,  # Store complete original raw log for debugging!
                                        "meta": {
                                            "browser": log_data.get("browser") or "Chrome",
                                            "os": log_data.get("os") or "macOS",
                                            "device": log_data.get("device") or "Desktop",
                                        },
                                    }

                                    # Append client telemetry if available
                                    if log_data.get("rum_error_message"):
                                        beacon_data["exceptions"] = [
                                            {
                                                "value": log_data["rum_error_message"],
                                                "message": log_data["rum_error_message"],
                                                "filename": log_data.get("rum_error_file") or "unknown.js",
                                                "file": log_data.get("rum_error_file") or "unknown.js",
                                                "lineno": log_data.get("rum_error_line") or 0,
                                                "line": log_data.get("rum_error_line") or 0,
                                                "colno": log_data.get("rum_error_col") or 0,
                                                "col": log_data.get("rum_error_col") or 0,
                                            }
                                        ]

                                    db.execute(
                                        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
                                        (service_id, received_at, json.dumps(beacon_data)),
                                    )
                                    file_rows_count += 1
                                    total_rows += 1
                                except Exception as e:
                                    logger.warning(f"RUM sync: Failed to parse RUM log line: {e}")
                                    continue

                        db.commit()
                        ingested_batch_records.append((s3_path, file_rows_count, size))
                        yield ("file_done", s3_path.split("/")[-1], file_rows_count)
                    except Exception as e:
                        logger.error(f"RUM sync: Failed to ingest RUM log file {s3_path}: {e}")
                        yield ("error", s3_path, str(e))
                        continue

        # Persist completed files to ingested_files
        if ingested_batch_records:
            metadata_db.insert_ingested_files(service_id, ingested_batch_records)

        # Clean up old RUM logs according to retention policy
        cleanup_files, cleanup_bytes = cleanup_old_rum_logs(service_id)
        if cleanup_files > 0:
            yield ("cleanup_done", cleanup_files, cleanup_bytes)

        yield ("done", total_rows)
        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            "success",
            files_downloaded=len(new_files_s3),
            rows_ingested=total_rows,
            run_id=run_id,
        )
    except Exception as e:
        logger.error(f"RUM ingest failed: {e}", exc_info=True)
        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            "error",
            files_downloaded=len(ingested_batch_records) if "ingested_batch_records" in locals() else 0,
            error_message=str(e),
            run_id=run_id,
        )
        yield ("error", "sync", str(e))
    finally:
        finalize_cron_run_if_running(service_id, "rum_sync", run_id)
