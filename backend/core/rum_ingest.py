"""RUM beacon ingest pipeline.

Parses raw_rum/ logs from FOS and streams them into local Parquet buffers
which are then committed to Apache Iceberg tables.
Uses the standard ingested_files table to ensure atomic, de-duplicated imports.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from backend.core import iceberg
from backend.core import metadata as metadata_db
from backend.core.duckdb import _get_fos_client, get_source_for_service
from backend.core.metadata.cron_log import (
    finalize_cron_run_if_running,
    log_cron_run,
    start_cron_run,
)

logger = logging.getLogger(__name__)


def safe_int(val, default: int = 0) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def safe_float(val, default: float | None = None) -> float | None:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except Exception:
        return default


def cleanup_old_rum_logs(service_id: str) -> tuple[int, int]:
    """Delete RUM beacon logs from FOS older than rum.delete_after days.

    Stands down unconditionally when
    ``provisioning.cron_sync.high_scale_shared_source`` is set — a high-scale
    consumer owns raw-deletion authority for this service's stream, RUM
    included. See ``backend.config.resolve_raw_delete_after``.

    Returns (files_deleted, bytes_freed).
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    if svcconfig.high_scale_shared_source_enabled(cfg):
        return 0, 0
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


def extract_metrics_from_faro_payload(payload: dict, log_data: dict) -> list[dict]:
    """Extract all distinct metrics, timings, and exceptions from a raw Faro payload."""
    extracted = []

    # Extract common context
    meta = payload.get("meta") or {}
    browser_meta = meta.get("browser") or {}
    os_meta = meta.get("os") or {}
    page_meta = payload.get("page") or meta.get("page") or {}

    browser_name = browser_meta.get("name") or log_data.get("browser") or "Chrome"
    os_name = os_meta.get("name") or log_data.get("os") or "macOS"
    device_type = "Desktop"
    if browser_meta.get("mobile"):
        device_type = "Mobile"

    url_str = page_meta.get("url") or log_data.get("url") or ""
    from urllib.parse import urlparse

    pathname = "/"
    if url_str:
        try:
            pathname = urlparse(url_str).path
        except Exception:
            pass

    cid = log_data.get("rum_cid") or log_data.get("cid") or ""

    # 1. Web Vitals
    measurements = payload.get("measurements") or []
    if isinstance(measurements, list):
        for m in measurements:
            if not isinstance(m, dict):
                continue
            if m.get("type") == "web-vitals":
                values = m.get("values") or {}
                rating = m.get("context", {}).get("rating") or (m.get("meta") or {}).get("rating") or ""
                for k, v in values.items():
                    if k == "delta":
                        continue
                    extracted.append(
                        {
                            "metric_name": k,
                            "metric_value": v,
                            "metric_rating": rating,
                            "pathname": pathname,
                            "cid": cid,
                            "browser": browser_name,
                            "os": os_name,
                            "device": device_type,
                        }
                    )

    # 2. Performance Navigation Timing and Custom Events (Interactions)
    events = payload.get("events") or []
    if isinstance(events, list):
        for e in events:
            if not isinstance(e, dict):
                continue
            name = e.get("name")
            if not name:
                continue
            if name == "faro.performance.navigation":
                attrs = e.get("attributes") or e.get("values") or {}
                for k, v in attrs.items():
                    extracted.append(
                        {
                            "metric_name": k,
                            "metric_value": v,
                            "metric_rating": "",
                            "pathname": pathname,
                            "cid": cid,
                            "browser": browser_name,
                            "os": os_name,
                            "device": device_type,
                        }
                    )
            else:
                # Custom/interaction event (like user click)
                extracted.append(
                    {
                        "metric_name": f"event_{name}",
                        "metric_value": 1.0,
                        "metric_rating": "",
                        "pathname": pathname,
                        "cid": cid,
                        "browser": browser_name,
                        "os": os_name,
                        "device": device_type,
                    }
                )

    # 3. Custom logs
    logs = payload.get("logs") or []
    if isinstance(logs, list):
        for log_item in logs:
            if not isinstance(log_item, dict):
                continue
            extracted.append(
                {
                    "metric_name": "log",
                    "metric_value": 1.0,
                    "metric_rating": log_item.get("level") or "info",
                    "pathname": pathname,
                    "cid": cid,
                    "browser": browser_name,
                    "os": os_name,
                    "device": device_type,
                }
            )

    # 3. Exceptions
    exceptions = payload.get("exceptions") or []
    if isinstance(exceptions, list):
        for exc in exceptions:
            if not isinstance(exc, dict):
                continue
            err_msg = exc.get("value") or exc.get("message") or "Unknown error"
            err_file = "unknown.js"
            err_line = 0
            err_col = 0
            stack = exc.get("stacktrace") or {}
            frames = stack.get("frames") or []
            if isinstance(frames, list) and len(frames) > 0:
                frame = frames[0]
                err_file = frame.get("filename") or "unknown.js"
                err_line = frame.get("lineno") or 0
                err_col = frame.get("colno") or 0

            extracted.append(
                {
                    "metric_name": "exception",
                    "metric_value": None,
                    "metric_rating": "",
                    "pathname": pathname,
                    "cid": cid,
                    "browser": browser_name,
                    "os": os_name,
                    "device": device_type,
                    "error_message": err_msg,
                    "error_file": err_file,
                    "error_line": err_line,
                    "error_col": err_col,
                }
            )

    if not extracted:
        # Fallback pageview entry if we got a payload but no specific vitals/events were extracted
        extracted.append(
            {
                "metric_name": "pageview",
                "metric_value": 1.0,
                "metric_rating": "",
                "pathname": pathname,
                "cid": cid,
                "browser": browser_name,
                "os": os_name,
                "device": device_type,
            }
        )

    return extracted


def ingest_rum_logs(
    service_id: str,
    max_seconds: int | None = None,
    run_id: int | None = None,
) -> Generator[tuple]:
    """Ingest RUM beacon logs from FOS raw_rum/ prefix into local Parquet buffers & DuckDB Iceberg views."""
    import tempfile

    from backend.config import load_config, resolve_raw_delete_after
    from backend.core.iceberg.rum_schema import (
        CLIENT_ERRORS_ARROW_SCHEMA,
        CLIENT_VITALS_ARROW_SCHEMA,
    )
    from backend.core.ingest import (
        _capture_corrupt_container,
        _corrupt_gzip_error,
        _delete_objects_robust_with_failures,
        _deterministic_buffer_name,
        _download_chunk_to_local,
        _new_object_outcome,
        _parse_rum_beacon_file,
        _quarantine_rum_corrupt_lines,
        _recover_in_flight,
        list_fos_files,
    )

    start_time = time.time()
    if run_id is None:
        run_id = start_cron_run(service_id, "rum_sync")
    outcome_counters = _new_object_outcome(processed=0)
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
            outcome_counters=outcome_counters,
            run_id=run_id,
        )
        return

    cfg = load_config(service_id)
    raw_delete_after = resolve_raw_delete_after(cfg)

    try:
        s3 = _get_fos_client(src)
        bucket = src["bucket"]

        # Run crash recovery for BOTH RUM tables
        _recover_in_flight(src, table_name="client_vitals")
        _recover_in_flight(src, table_name="client_errors")

        # Fetch already ingested files to avoid duplicate processing
        vitals_ingested = metadata_db.get_ingested_filenames(service_id, table_name="client_vitals") or set()
        errors_ingested = metadata_db.get_ingested_filenames(service_id, table_name="client_errors") or set()
        already_ingested_raw = vitals_ingested.union(errors_ingested)

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
            incremental_only=True,
            delete_after=raw_delete_after,
            elapsed_fn=lambda: f"{time.time() - start_time:.1f}s",
            fos_client=s3,
        )
        list_error = None
        try:
            while True:
                evt = next(list_gen)
                if evt.get("type") == "status":
                    logger.debug(f"RUM sync: {evt['message']}")
                elif evt.get("type") == "error":
                    list_error = evt.get("message", "FOS list error")
                    logger.error(f"RUM sync: {list_error}")
                    yield ("error", "list", list_error)
        except StopIteration as e:
            list_res = e.value

        if list_error:
            log_cron_run(
                service_id,
                "rum_sync",
                time.time() - start_time,
                "error",
                files_downloaded=0,
                rows_ingested=0,
                error_message=list_error,
                run_id=run_id,
            )
            return

        new_files_s3 = list_res["new_files"]
        file_sizes = list_res["file_sizes"]
        stranded_already = list_res.get("stranded_already", [])

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

        total_vitals_rows = 0
        total_errors_rows = 0
        error_count = 0
        failed_paths: set[str] = set()
        capture_failed_paths: set[str] = set()
        partial_paths: set[str] = set()
        delete_failed_paths: set[str] = set()
        deleted = 0
        reclaimed = 0

        # Download and process in parallel chunks to unify request and RUM ingestion logic
        CHUNK_SIZE = 50
        chunks = [new_files_s3[i : i + CHUNK_SIZE] for i in range(0, len(new_files_s3), CHUNK_SIZE)]

        for chunk_idx, chunk in enumerate(chunks):
            # Time limit check — Trap #41: ALWAYS allow the first chunk (chunk_idx == 0)
            # to run even if discovery consumed the full budget.
            if chunk_idx > 0 and max_seconds and (time.time() - start_time) > max_seconds:
                logger.warning(
                    "RUM sync: Reached time limit (%ds) for %s, stopping early after %d/%d files",
                    max_seconds,
                    service_id,
                    chunk_idx * CHUNK_SIZE,
                    len(new_files_s3),
                )
                yield ("status", "budget", f"Time limit of {max_seconds}s reached. Stopping batch early.")
                break

            chunk_vitals_rows: list[dict[str, object]] = []
            chunk_errors_rows: list[dict[str, object]] = []
            vitals_batch_records = []
            errors_batch_records = []

            with tempfile.TemporaryDirectory() as tmpdir:
                s3_to_local, _ = _download_chunk_to_local(s3, chunk, tmpdir)

                for s3_path in chunk:
                    outcome_counters["objects_processed"] += 1
                    if s3_path not in s3_to_local:
                        error_count += 1
                        failed_paths.add(s3_path)
                        logger.error(f"RUM sync: Failed to download {s3_path}")
                        yield ("error", s3_path, "Download failed")
                        continue

                    local_path = s3_to_local[s3_path]
                    size = file_sizes.get(s3_path, 0)

                    try:
                        file_vitals, file_errors, corrupt_lines = _parse_rum_beacon_file(local_path, service_id)
                    except Exception as e:
                        error_count += 1
                        failed_paths.add(s3_path)
                        failed_gzip = _corrupt_gzip_error(local_path)
                        if failed_gzip is not None:
                            q_res = _capture_corrupt_container(
                                service_id,
                                "rum",
                                s3_path.removeprefix(f"s3://{bucket}/"),
                                local_path,
                                failed_gzip,
                            )
                            outcome_counters["corrupt_containers"] += q_res["corrupt_containers"]
                            outcome_counters["cap_evictions"] += q_res["cap_evictions"]
                            outcome_counters["quarantine_capture_failures"] += q_res["quarantine_capture_failures"]
                            if q_res["quarantine_capture_failures"] > 0:
                                capture_failed_paths.add(s3_path)
                        logger.error(f"RUM sync: Failed to ingest RUM log file {s3_path}: {e}")
                        yield ("error", s3_path, str(e))
                        continue

                    valid_for_file = len(file_vitals) + len(file_errors)
                    outcome_counters["valid_records"] += valid_for_file

                    if corrupt_lines:
                        logger.warning(
                            "RUM sync: %s has %d malformed line(s) quarantined",
                            s3_path,
                            len(corrupt_lines),
                        )
                        try:
                            quarantine_result = _quarantine_rum_corrupt_lines(
                                s3,
                                src,
                                s3_path.removeprefix(f"s3://{bucket}/"),
                                corrupt_lines,
                                valid_for_file,
                                local_file=local_path,
                            )
                        except Exception:
                            logger.exception("RUM sync: exact-byte quarantine failed for %s", s3_path)
                            quarantine_result = {
                                "malformed_records": len(corrupt_lines),
                                "cap_evictions": 0,
                                "quarantine_capture_failures": len(corrupt_lines),
                            }
                        outcome_counters["malformed_records"] += quarantine_result["malformed_records"]
                        outcome_counters["cap_evictions"] += quarantine_result["cap_evictions"]
                        outcome_counters["quarantine_capture_failures"] += quarantine_result[
                            "quarantine_capture_failures"
                        ]
                        if quarantine_result["quarantine_capture_failures"]:
                            capture_failed_paths.add(s3_path)
                        elif valid_for_file > 0:
                            partial_paths.add(s3_path)

                    chunk_vitals_rows.extend(file_vitals)
                    chunk_errors_rows.extend(file_errors)
                    total_vitals_rows += len(file_vitals)
                    total_errors_rows += len(file_errors)

                    # Bookkeeping: record durable row counts (0 for files that produced no rows)
                    vitals_batch_records.append((s3_path, len(file_vitals), size))
                    errors_batch_records.append((s3_path, len(file_errors), size))
                    yield ("file_done", s3_path.split("/")[-1], valid_for_file)

            # End of chunk: Write tables to buffers
            buf_filename = _deterministic_buffer_name(chunk)
            if vitals_batch_records:
                if chunk_vitals_rows:
                    metadata_db.record_in_flight(
                        service_id, buf_filename, vitals_batch_records, table_name="client_vitals"
                    )
                    vitals_table = pa.Table.from_pylist(chunk_vitals_rows, schema=CLIENT_VITALS_ARROW_SCHEMA)
                    iceberg.write_to_buffer(src, vitals_table, buf_filename, table_name="client_vitals")
                    metadata_db.insert_ingested_files(service_id, vitals_batch_records, table_name="client_vitals")
                    metadata_db.clear_in_flight(service_id, buf_filename, table_name="client_vitals")
                else:
                    metadata_db.insert_ingested_files(service_id, vitals_batch_records, table_name="client_vitals")

            if errors_batch_records:
                if chunk_errors_rows:
                    metadata_db.record_in_flight(
                        service_id, buf_filename, errors_batch_records, table_name="client_errors"
                    )
                    errors_table = pa.Table.from_pylist(chunk_errors_rows, schema=CLIENT_ERRORS_ARROW_SCHEMA)
                    iceberg.write_to_buffer(src, errors_table, buf_filename, table_name="client_errors")
                    metadata_db.insert_ingested_files(service_id, errors_batch_records, table_name="client_errors")
                    metadata_db.clear_in_flight(service_id, buf_filename, table_name="client_errors")
                else:
                    metadata_db.insert_ingested_files(service_id, errors_batch_records, table_name="client_errors")

            # Inline deletion per chunk (Gap 2)
            if raw_delete_after:
                exclude_from_delete = failed_paths | capture_failed_paths
                chunk_keys = [
                    f.removeprefix(f"s3://{bucket}/")
                    for f in chunk
                    if f.startswith(f"s3://{bucket}/") and f not in exclude_from_delete
                ]
                if chunk_keys:
                    try:
                        chunk_deleted, chunk_failed_keys = _delete_objects_robust_with_failures(s3, bucket, chunk_keys)
                        deleted += chunk_deleted
                        delete_failed_paths.update(f"s3://{bucket}/{k}" for k in chunk_failed_keys)
                    except Exception as del_err:
                        delete_failed_paths.update(f"s3://{bucket}/{k}" for k in chunk_keys)
                        logger.error("RUM sync: failed to delete %d raw files: %s", len(chunk_keys), del_err)

        # Reclaim stranded already-ingested objects (Gap 2)
        if raw_delete_after and stranded_already:
            stranded_keys = [
                f.removeprefix(f"s3://{bucket}/") for f in stranded_already if f.startswith(f"s3://{bucket}/")
            ]
            if stranded_keys:
                try:
                    reclaim_del, reclaim_failures = _delete_objects_robust_with_failures(s3, bucket, stranded_keys)
                    reclaimed += reclaim_del
                    deleted += reclaim_del
                    delete_failed_paths.update(f"s3://{bucket}/{k}" for k in reclaim_failures)
                except Exception as rec_err:
                    delete_failed_paths.update(f"s3://{bucket}/{k}" for k in stranded_keys)
                    logger.error("RUM sync: failed to reclaim stranded raw files: %s", rec_err)

        # Clean up old RUM logs according to retention policy
        cleanup_files, cleanup_bytes = cleanup_old_rum_logs(service_id)
        if cleanup_files > 0:
            yield ("cleanup_done", cleanup_files, cleanup_bytes)

        yield ("done", total_vitals_rows + total_errors_rows)
        duration_s = time.time() - start_time

        # Outcome counters calculation matching ingest()
        outcome_counters["source_delete_failures"] = len(delete_failed_paths)
        failed_objects = failed_paths | capture_failed_paths | delete_failed_paths
        partial_objects = partial_paths - failed_objects
        outcome_counters["objects_failed"] = len(failed_objects)
        outcome_counters["objects_partial"] = len(partial_objects)
        outcome_counters["objects_successful"] = max(
            0,
            outcome_counters["objects_processed"]
            - outcome_counters["objects_failed"]
            - outcome_counters["objects_partial"],
        )
        had_errors = any(
            (
                error_count,
                outcome_counters["malformed_records"],
                outcome_counters["corrupt_containers"],
                outcome_counters["quarantine_capture_failures"],
                outcome_counters["source_delete_failures"],
                outcome_counters["objects_failed"],
            )
        )
        run_status = "error" if had_errors else "success"
        summary_msg = (
            f"Ingested {total_vitals_rows + total_errors_rows} RUM rows ({error_count} file error(s))"
            if had_errors
            else f"Ingested {total_vitals_rows + total_errors_rows} RUM rows"
        )
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            run_status,
            files_downloaded=len(new_files_s3),
            rows_ingested=total_vitals_rows + total_errors_rows,
            run_id=run_id,
            summary=summary_msg,
            outcome_counters=outcome_counters,
        )
    except Exception as e:
        logger.error(f"RUM ingest failed: {e}", exc_info=True)
        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            "error",
            files_downloaded=len(new_files_s3) if "new_files_s3" in locals() else 0,
            error_message=str(e),
            outcome_counters=outcome_counters,
            run_id=run_id,
        )
        yield ("error", "sync", str(e))
    finally:
        finalize_cron_run_if_running(service_id, "rum_sync", run_id)
