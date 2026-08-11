"""RUM beacon ingest pipeline.

Parses raw_rum/ logs from FOS and streams them into local Parquet buffers
which are then committed to Apache Iceberg tables.
Uses the standard ingested_files table to ensure atomic, de-duplicated imports.
"""

from __future__ import annotations

import gzip
import json
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
        rum_prefix = f"{prefix}/raw_rum/" if prefix else "raw_rum/"

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

    # 2. Performance Navigation Timing events
    events = payload.get("events") or []
    if isinstance(events, list):
        for e in events:
            if not isinstance(e, dict):
                continue
            if e.get("name") == "faro.performance.navigation":
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

    return extracted


def ingest_rum_logs(
    service_id: str,
) -> Generator[tuple]:
    """Ingest RUM beacon logs from FOS raw_rum/ prefix into local Parquet buffers & DuckDB Iceberg views."""
    import tempfile

    from backend.core.iceberg.rum_schema import (
        CLIENT_ERRORS_ARROW_SCHEMA,
        CLIENT_VITALS_ARROW_SCHEMA,
    )
    from backend.core.ingest import (
        _deterministic_buffer_name,
        _download_chunk_to_local,
        _recover_in_flight,
        list_fos_files,
    )

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

        # Use the shared list_fos_files helper to discover files in raw_rum/ prefix
        list_gen = list_fos_files(
            src=src,
            prefix_subpath="raw_rum/",
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

        total_vitals_rows = 0
        total_errors_rows = 0

        # Download and process in parallel chunks to unify request and RUM ingestion logic
        CHUNK_SIZE = 50
        chunks = [new_files_s3[i : i + CHUNK_SIZE] for i in range(0, len(new_files_s3), CHUNK_SIZE)]

        for chunk in chunks:
            buf_filename = _deterministic_buffer_name(chunk)
            vitals_rows = []
            errors_rows = []
            vitals_batch_records = []
            errors_batch_records = []

            with tempfile.TemporaryDirectory() as tmpdir:
                s3_to_local, _ = _download_chunk_to_local(s3, chunk, tmpdir)

                for s3_path in chunk:
                    if s3_path not in s3_to_local:
                        logger.error(f"RUM sync: Failed to download {s3_path}")
                        yield ("error", s3_path, "Download failed")
                        continue

                    local_path = s3_to_local[s3_path]
                    vitals_count_for_file = 0
                    errors_count_for_file = 0
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
                                    try:
                                        dt = datetime.fromisoformat(received_at)
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=UTC)
                                        else:
                                            dt = dt.astimezone(UTC)
                                    except Exception:
                                        dt = datetime.now(UTC)

                                    # Try to extract metrics/exceptions from raw JSON in rum_body first
                                    raw_body = log_data.get("rum_body")
                                    extracted_metrics = []
                                    if raw_body:
                                        try:
                                            # It could be json-escaped or doubly-stringified
                                            payload = json.loads(raw_body)
                                            if isinstance(payload, str):
                                                payload = json.loads(payload)
                                            if isinstance(payload, dict):
                                                extracted_metrics = extract_metrics_from_faro_payload(payload, log_data)
                                        except Exception as e:
                                            logger.warning(f"RUM sync: Failed to parse rum_body JSON: {e}")

                                    if extracted_metrics:
                                        # Ingest each extracted metric as a separate beacon row
                                        for metric in extracted_metrics:
                                            # Common fields
                                            browser_val = metric.get("browser") or log_data.get("browser") or "Chrome"
                                            os_val = metric.get("os") or log_data.get("os") or "macOS"
                                            device_val = metric.get("device") or log_data.get("device") or "Desktop"
                                            cid_val = (
                                                metric.get("cid")
                                                or log_data.get("rum_cid")
                                                or log_data.get("cid")
                                                or ""
                                            )
                                            req_id_val = log_data.get("fastly_req_id") or ""
                                            pathname_val = metric.get("pathname") or "/"

                                            is_exception = (metric.get("metric_name") == "exception") or metric.get(
                                                "error_message"
                                            )

                                            if is_exception:
                                                # client_errors row
                                                errors_rows.append(
                                                    {
                                                        "timestamp": dt,
                                                        "error_message": metric.get("error_message") or "Unknown error",
                                                        "error_file": metric.get("error_file") or "unknown.js",
                                                        "error_line": safe_int(metric.get("error_line")),
                                                        "error_col": safe_int(metric.get("error_col")),
                                                        "pathname": pathname_val,
                                                        "browser": browser_val,
                                                        "os": os_val,
                                                        "device": device_val,
                                                        "cid": cid_val,
                                                        "req_id": req_id_val,
                                                    }
                                                )
                                                errors_count_for_file += 1
                                                total_errors_rows += 1
                                            else:
                                                # client_vitals row
                                                vitals_rows.append(
                                                    {
                                                        "timestamp": dt,
                                                        "metric_name": metric.get("metric_name") or "",
                                                        "metric_value": safe_float(metric.get("metric_value")),
                                                        "metric_rating": metric.get("metric_rating") or "",
                                                        "pathname": pathname_val,
                                                        "browser": browser_val,
                                                        "os": os_val,
                                                        "device": device_val,
                                                        "cid": cid_val,
                                                        "req_id": req_id_val,
                                                    }
                                                )
                                                vitals_count_for_file += 1
                                                total_vitals_rows += 1
                                    else:
                                        # Extract core RUM metric values with URL/query parameter fallbacks
                                        metric_name = log_data.get("rum_metric_name")
                                        metric_value = log_data.get("rum_metric_value")
                                        metric_rating = log_data.get("rum_metric_rating")
                                        cid_val = log_data.get("rum_cid") or log_data.get("cid") or ""
                                        pathname_val = log_data.get("rum_pathname")

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
                                                if not cid_val and "cid" in qparams:
                                                    cid_val = qparams["cid"][0]
                                                if not pathname_val and "rum_pathname" in qparams:
                                                    pathname_val = qparams["rum_pathname"][0]
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
                                        if not pathname_val and log_data.get("referer"):
                                            try:
                                                pathname_val = urlparse(log_data["referer"]).path
                                            except Exception:
                                                pass
                                        if not pathname_val:
                                            pathname_val = "/"
                                        pathname_val = pathname_val.replace("//", "/")

                                        browser_val = log_data.get("browser") or "Chrome"
                                        os_val = log_data.get("os") or "macOS"
                                        device_val = log_data.get("device") or "Desktop"
                                        req_id_val = log_data.get("fastly_req_id") or ""

                                        # Append client telemetry if available
                                        is_exception = log_data.get("rum_error_message") is not None
                                        if is_exception:
                                            errors_rows.append(
                                                {
                                                    "timestamp": dt,
                                                    "error_message": log_data["rum_error_message"] or "Unknown error",
                                                    "error_file": log_data.get("rum_error_file") or "unknown.js",
                                                    "error_line": safe_int(log_data.get("rum_error_line")),
                                                    "error_col": safe_int(log_data.get("rum_error_col")),
                                                    "pathname": pathname_val,
                                                    "browser": browser_val,
                                                    "os": os_val,
                                                    "device": device_val,
                                                    "cid": cid_val,
                                                    "req_id": req_id_val,
                                                }
                                            )
                                            errors_count_for_file += 1
                                            total_errors_rows += 1
                                        else:
                                            vitals_rows.append(
                                                {
                                                    "timestamp": dt,
                                                    "metric_name": metric_name or "",
                                                    "metric_value": safe_float(metric_value),
                                                    "metric_rating": metric_rating or "",
                                                    "pathname": pathname_val,
                                                    "browser": browser_val,
                                                    "os": os_val,
                                                    "device": device_val,
                                                    "cid": cid_val,
                                                    "req_id": req_id_val,
                                                }
                                            )
                                            vitals_count_for_file += 1
                                            total_vitals_rows += 1
                                except Exception as e:
                                    logger.warning(f"RUM sync: Failed to parse RUM log line: {e}")
                                    continue

                        vitals_batch_records.append((s3_path, vitals_count_for_file, size))
                        errors_batch_records.append((s3_path, errors_count_for_file, size))
                        yield ("file_done", s3_path.split("/")[-1], vitals_count_for_file + errors_count_for_file)
                    except Exception as e:
                        logger.error(f"RUM sync: Failed to ingest RUM log file {s3_path}: {e}")
                        yield ("error", s3_path, str(e))
                        continue

            # End of chunk: Write tables to buffers
            if vitals_batch_records:
                if vitals_rows:
                    metadata_db.record_in_flight(
                        service_id, buf_filename, vitals_batch_records, table_name="client_vitals"
                    )
                    vitals_table = pa.Table.from_pylist(vitals_rows, schema=CLIENT_VITALS_ARROW_SCHEMA)
                    iceberg.write_to_buffer(src, vitals_table, buf_filename, table_name="client_vitals")
                    metadata_db.insert_ingested_files(service_id, vitals_batch_records, table_name="client_vitals")
                    metadata_db.clear_in_flight(service_id, buf_filename, table_name="client_vitals")
                else:
                    metadata_db.insert_ingested_files(service_id, vitals_batch_records, table_name="client_vitals")

            if errors_batch_records:
                if errors_rows:
                    metadata_db.record_in_flight(
                        service_id, buf_filename, errors_batch_records, table_name="client_errors"
                    )
                    errors_table = pa.Table.from_pylist(errors_rows, schema=CLIENT_ERRORS_ARROW_SCHEMA)
                    iceberg.write_to_buffer(src, errors_table, buf_filename, table_name="client_errors")
                    metadata_db.insert_ingested_files(service_id, errors_batch_records, table_name="client_errors")
                    metadata_db.clear_in_flight(service_id, buf_filename, table_name="client_errors")
                else:
                    metadata_db.insert_ingested_files(service_id, errors_batch_records, table_name="client_errors")

        # Clean up old RUM logs according to retention policy
        cleanup_files, cleanup_bytes = cleanup_old_rum_logs(service_id)
        if cleanup_files > 0:
            yield ("cleanup_done", cleanup_files, cleanup_bytes)

        yield ("done", total_vitals_rows + total_errors_rows)
        duration_s = time.time() - start_time
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            "success",
            files_downloaded=len(new_files_s3),
            rows_ingested=total_vitals_rows + total_errors_rows,
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
            files_downloaded=len(new_files_s3) if "new_files_s3" in locals() else 0,
            error_message=str(e),
            run_id=run_id,
        )
        yield ("error", "sync", str(e))
    finally:
        finalize_cron_run_if_running(service_id, "rum_sync", run_id)
