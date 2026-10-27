"""RUM beacon discovery, ingestion, conversion, quarantine, and DuckLake RUM table helpers."""

from __future__ import annotations

import gzip
import json
import logging
import os
import sys as _sys
import tempfile
import time
from collections.abc import Generator
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa

from backend.celery_app import app
from backend.core import metadata as metadata_db
from backend.core.duckdb import escape_sql_literal, get_source_for_service
from backend.core.faro_versions import DEFAULT_FARO_VERSION  # noqa: F401
from backend.core.ingest import (
    _celery_ingest_scope,
    _discover_prefix_common,
    _ducklake_replace_source_rows,
    _ducklake_table_exists,
    _finalize_object_outcome,
    _get_fos_client,
    _new_object_outcome,
    _run_batch_convert_common,
    _sum_evidence_counts,
    _sweep_ledger_common,
)
from backend.core.metadata.usage_log import finalize_cron_run_if_running, log_cron_run, start_cron_run
from backend.core.rollups.recompute import backfill_missing_rum_aggregates, recompute_rum_aggregates  # noqa: F401
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)


def _resolve_ingest_hook(attr_name: str, current: Any, orig: Any) -> Any:
    if current is not orig:
        return current
    ingest_mod = _sys.modules.get("backend.core.ingest")
    if ingest_mod is not None:
        other_val = ingest_mod.__dict__.get(attr_name, orig)
        if other_val is not orig:
            return other_val
    return current


def safe_int(val: Any, default: int = 0) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def safe_float(val: Any, default: float | None = None) -> float | None:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except Exception:
        return default


def extract_metrics_from_faro_payload(payload: dict, log_data: dict) -> list[dict]:
    """Extract all distinct metrics, timings, and exceptions from a raw Faro payload."""
    extracted: list[dict] = []
    meta = payload.get("meta") or {}
    browser_meta = meta.get("browser") or {}
    os_meta = meta.get("os") or {}
    page_meta = payload.get("page") or meta.get("page") or {}
    browser_name = browser_meta.get("name") or log_data.get("browser") or "Chrome"
    os_name = os_meta.get("name") or log_data.get("os") or "macOS"
    device_type = "Mobile" if browser_meta.get("mobile") else "Desktop"
    url_str = page_meta.get("url") or log_data.get("url") or ""
    pathname = "/"
    if url_str:
        try:
            pathname = urlparse(url_str).path
        except Exception:
            if log_once_per("extract_metrics_from_faro_payload: urlparse failed (ignored)", 60):
                logger.debug("extract_metrics_from_faro_payload: urlparse failed (ignored)", exc_info=True)
    cid = log_data.get("rum_cid") or log_data.get("cid") or ""

    def _entry(metric_name: str, metric_value: Any, metric_rating: str = "", **extra: Any) -> dict[str, Any]:
        return {
            "metric_name": metric_name,
            "metric_value": metric_value,
            "metric_rating": metric_rating,
            "pathname": pathname,
            "cid": cid,
            "browser": browser_name,
            "os": os_name,
            "device": device_type,
            **extra,
        }

    measurements = payload.get("measurements") or []
    if isinstance(measurements, list):
        for m in measurements:
            if isinstance(m, dict) and m.get("type") == "web-vitals":
                rating = m.get("context", {}).get("rating") or (m.get("meta") or {}).get("rating") or ""
                for k, v in (m.get("values") or {}).items():
                    if k != "delta":
                        extracted.append(_entry(k, v, rating))

    events = payload.get("events") or []
    if isinstance(events, list):
        for e in events:
            if not isinstance(e, dict) or not (name := e.get("name")):
                continue
            if name == "faro.performance.navigation":
                for k, v in (e.get("attributes") or e.get("values") or {}).items():
                    extracted.append(_entry(k, v))
            else:
                extracted.append(_entry(f"event_{name}", 1.0))

    logs = payload.get("logs") or []
    if isinstance(logs, list):
        for log_item in logs:
            if isinstance(log_item, dict):
                extracted.append(_entry("log", 1.0, log_item.get("level") or "info"))

    exceptions = payload.get("exceptions") or []
    if isinstance(exceptions, list):
        for exc in exceptions:
            if not isinstance(exc, dict):
                continue
            err_msg = exc.get("value") or exc.get("message") or "Unknown error"
            frames = (exc.get("stacktrace") or {}).get("frames") or []
            frame = frames[0] if isinstance(frames, list) and frames else {}
            extracted.append(
                _entry(
                    "exception",
                    None,
                    error_message=err_msg,
                    error_file=frame.get("filename") or "unknown.js",
                    error_line=frame.get("lineno") or 0,
                    error_col=frame.get("colno") or 0,
                )
            )

    if not extracted:
        extracted.append(_entry("pageview", 1.0))
    return extracted


def _parse_rum_line(log_data: dict, service_id: str) -> tuple[list[dict], list[dict]] | None:
    log_service_id = log_data.get("service_id") or log_data.get("rum_service_id")
    if log_service_id and log_service_id != service_id:
        return None
    if (
        log_data.get("method") == "OPTIONS"
        or log_data.get("is_healthcheck")
        or log_data.get("path") in ("/health", "/ping", "/favicon.ico")
    ):
        return None
    received_at = log_data.get("timestamp")
    if not received_at:
        raise ValueError("missing_timestamp")
    dt = parse_iso_utc(received_at) if isinstance(received_at, str) else None
    if dt is None:
        raise ValueError(f"invalid_timestamp: {received_at!r}")
    city_val = log_data.get("city") or log_data.get("geo_city") or ""
    region_val = log_data.get("region") or log_data.get("geo_region") or ""
    country_val = log_data.get("country") or log_data.get("geo_country_code") or ""
    pop_val = log_data.get("pop") or log_data.get("server_pop") or ""
    tls_val = log_data.get("tls") or log_data.get("tls_version") or ""
    ttfb_val = safe_float(log_data.get("ttfb") or log_data.get("time_to_first_byte"))

    def _common(pathname, browser, os_name, device, cid, req_id) -> dict:
        return {
            "pathname": pathname,
            "browser": browser,
            "os": os_name,
            "device": device,
            "cid": cid,
            "req_id": req_id,
            "city": city_val,
            "region": region_val,
            "country": country_val,
            "pop": pop_val,
            "tls": tls_val,
            "ttfb": ttfb_val,
        }

    vitals_rows: list[dict] = []
    errors_rows: list[dict] = []

    def _append_rum_row(
        *,
        is_error: bool,
        err_msg: Any,
        err_file: Any,
        err_line: Any,
        err_col: Any,
        metric_name: Any,
        metric_value: Any,
        metric_rating: Any,
        common: dict,
    ) -> None:
        if is_error:
            errors_rows.append(
                {
                    "timestamp": dt,
                    "error_message": err_msg or "Unknown error",
                    "error_file": err_file or "unknown.js",
                    "error_line": safe_int(err_line),
                    "error_col": safe_int(err_col),
                    **common,
                }
            )
        else:
            val = safe_float(metric_value)
            vitals_rows.append(
                {
                    "timestamp": dt,
                    "metric_name": metric_name or "unknown",
                    "metric_value": val if val is not None else 0.0,
                    "metric_rating": metric_rating or "",
                    **common,
                }
            )

    raw_body = log_data.get("rum_body")
    extracted_metrics: list[dict] = []
    if raw_body:
        try:
            payload = json.loads(raw_body)
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                extracted_metrics = extract_metrics_from_faro_payload(payload, log_data)
        except Exception:
            extracted_metrics = []
    if extracted_metrics:
        for metric in extracted_metrics:
            common = _common(
                metric.get("pathname") or "/",
                metric.get("browser") or log_data.get("browser") or "Chrome",
                metric.get("os") or log_data.get("os") or "macOS",
                metric.get("device") or log_data.get("device") or "Desktop",
                metric.get("cid") or log_data.get("rum_cid") or log_data.get("cid") or "",
                log_data.get("fastly_req_id") or "",
            )
            _append_rum_row(
                is_error=bool((metric.get("metric_name") == "exception") or metric.get("error_message")),
                err_msg=metric.get("error_message"),
                err_file=metric.get("error_file"),
                err_line=metric.get("error_line"),
                err_col=metric.get("error_col"),
                metric_name=metric.get("metric_name"),
                metric_value=metric.get("metric_value"),
                metric_rating=metric.get("metric_rating"),
                common=common,
            )
    else:
        metric_name = log_data.get("rum_metric_name")
        metric_value = log_data.get("rum_metric_value")
        metric_rating = log_data.get("rum_metric_rating")
        cid_val = log_data.get("rum_cid") or log_data.get("cid") or ""
        pathname_val = log_data.get("rum_pathname")
        raw_url = log_data.get("url") or log_data.get("rum_raw_query") or ""
        if raw_url:
            try:
                qparams = parse_qs(urlparse(raw_url).query)
                metric_name = metric_name or qparams.get("rum_metric_name", [metric_name])[0]
                if metric_value is None or metric_value == "":
                    metric_value = qparams.get("rum_metric_value", [metric_value])[0]
                metric_rating = metric_rating or qparams.get("rum_metric_rating", [metric_rating])[0]
                cid_val = cid_val or qparams.get("cid", [cid_val])[0]
                pathname_val = pathname_val or qparams.get("rum_pathname", [pathname_val])[0]
            except Exception:
                if log_once_per("_parse_rum_line: step failed (ignored)", 60):
                    logger.debug("_parse_rum_line: step failed (ignored)", exc_info=True)
        if metric_value is not None and isinstance(metric_value, str):
            try:
                metric_value = float(metric_value) if "." in metric_value else int(metric_value)
            except ValueError:
                pass
        if not pathname_val and log_data.get("referer"):
            try:
                pathname_val = urlparse(log_data["referer"]).path
            except Exception:
                if log_once_per("_parse_rum_line: urlparse failed (ignored)", 60):
                    logger.debug("_parse_rum_line: urlparse failed (ignored)", exc_info=True)
        pathname_val = (pathname_val or "/").replace("//", "/")
        common = _common(
            pathname_val,
            log_data.get("browser") or "Chrome",
            log_data.get("os") or "macOS",
            log_data.get("device") or "Desktop",
            cid_val,
            log_data.get("fastly_req_id") or "",
        )
        _append_rum_row(
            is_error=bool(log_data.get("rum_error_message")),
            err_msg=log_data.get("rum_error_message"),
            err_file=log_data.get("rum_error_file"),
            err_line=log_data.get("rum_error_line"),
            err_col=log_data.get("rum_error_col"),
            metric_name=metric_name,
            metric_value=metric_value,
            metric_rating=metric_rating,
            common=common,
        )
    return vitals_rows, errors_rows


def _parse_rum_beacon_file(local_path: str, service_id: str) -> tuple[list[dict], list[dict], list[tuple[str, str]]]:
    vitals_rows: list[dict] = []
    errors_rows: list[dict] = []
    corrupt_lines: list[tuple[str, str]] = []
    with gzip.open(local_path, "rt", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                log_data = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                corrupt_lines.append((stripped, "invalid_json"))
                continue
            try:
                parsed = _parse_rum_line(log_data, service_id)
            except Exception as e:
                msg = str(e)
                if msg == "missing_timestamp":
                    corrupt_lines.append((stripped, "missing_timestamp"))
                elif msg.startswith("invalid_timestamp"):
                    corrupt_lines.append(
                        (stripped, f"invalid_timestamp: {msg.removeprefix('invalid_timestamp: ')}"[:200])
                    )
                else:
                    corrupt_lines.append((stripped, f"parse_error: {e}"[:200]))
                continue
            if parsed is None:
                continue
            v_rows, e_rows = parsed
            if not v_rows and not e_rows:
                corrupt_lines.append((stripped, "empty_beacon_payload"))
                continue
            vitals_rows.extend(v_rows)
            errors_rows.extend(e_rows)
    return vitals_rows, errors_rows, corrupt_lines


_orig_parse_rum_beacon_file = _parse_rum_beacon_file


def _quarantine_rum_corrupt_lines(
    fos_client,
    src: dict,
    object_key: str,
    corrupt_lines: list[tuple[str, str]],
    valid_count: int,
    *,
    local_file: str,
) -> dict[str, int]:
    from backend.core.metadata.ingest_log import capture_evidence

    service_id = src.get("service_id") or src.get("name", "")
    prefix_path = src.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/raw/rum/" if prefix_path else "raw/rum/"
    if not object_key.startswith(raw_prefix):
        return {
            "valid_records": valid_count,
            "malformed_records": len(corrupt_lines),
            "cap_evictions": 0,
            "quarantine_capture_failures": len(corrupt_lines),
        }
    occurrences: dict[str, list[str]] = {}
    for line, reason in corrupt_lines:
        occurrences.setdefault(line, []).append(reason)
    offsets: dict[str, int] = {}
    evidence_results = []
    byte_offset = 0
    with gzip.open(local_file, "rb") as raw:
        for ordinal, raw_line in enumerate(raw, start=1):
            stripped = raw_line.strip().decode("utf-8", errors="replace")
            current_offset = byte_offset
            byte_offset += len(raw_line)
            if not occurrences.get(stripped):
                continue
            reason = occurrences[stripped][offsets.get(stripped, 0)]
            offsets[stripped] = offsets.get(stripped, 0) + 1
            evidence_results.append(
                capture_evidence(
                    service_id,
                    "rum",
                    object_key,
                    raw_line,
                    line_ordinal=ordinal,
                    byte_offset=current_offset,
                    error_category=reason.partition(":")[0],
                    error_text=reason,
                )
            )
    counts = _sum_evidence_counts(evidence_results, extra_failures=len(corrupt_lines) - len(evidence_results))
    if counts["quarantine_capture_failures"]:
        logger.error(
            "[ledger] %s: RUM evidence capture failed for %s (%d line(s))",
            service_id,
            object_key,
            counts["quarantine_capture_failures"],
        )
    return {"valid_records": valid_count, "malformed_records": len(corrupt_lines), **counts}


_orig_quarantine_rum_corrupt_lines = _quarantine_rum_corrupt_lines


def _write_rum_arrow_table_batch(
    duckdb_con: Any,
    table_name: str,
    rows: list[dict],
    sources: list[str],
    source_files_sql: str,
    schema: Any,
) -> None:
    if not rows:
        return
    arrow_tbl = pa.Table.from_pylist(rows, schema=schema).append_column(
        "_source_file", pa.array(sources, type=pa.string())
    )
    duckdb_con.register("_rum_stage", arrow_tbl)
    try:
        if not _ducklake_table_exists(duckdb_con, table_name):
            duckdb_con.execute(f"CREATE TABLE lake.{table_name} AS SELECT * FROM _rum_stage")
        else:
            existing_cols = {r[0] for r in duckdb_con.execute(f"DESCRIBE lake.{table_name}").fetchall()}
            if "_source_file" not in existing_cols:
                duckdb_con.execute(f'ALTER TABLE lake.{table_name} ADD COLUMN "_source_file" VARCHAR')
            _ducklake_replace_source_rows(
                duckdb_con, table_name, source_files_sql, "SELECT * FROM _rum_stage", "_write_rum_arrow_table_batch"
            )
    finally:
        duckdb_con.unregister("_rum_stage")


def convert_batch_rum_objects(service_id: str, object_keys: list[str], worker_id: str) -> dict:
    from backend.core.storage._core import CLIENT_ERRORS_ARROW_SCHEMA, CLIENT_VITALS_ARROW_SCHEMA, ducklake_table_name

    parse_fn = _resolve_ingest_hook("_parse_rum_beacon_file", _parse_rum_beacon_file, _orig_parse_rum_beacon_file)
    quarantine_fn = _resolve_ingest_hook(
        "_quarantine_rum_corrupt_lines", _quarantine_rum_corrupt_lines, _orig_quarantine_rum_corrupt_lines
    )

    def _process_rum_batch(*, src, fos, duckdb_con, files, outcomes, enter_write_admission) -> None:
        vitals_table = ducklake_table_name(src, table_name="client_vitals")
        errors_table = ducklake_table_name(src, table_name="client_errors")
        all_vitals: list[dict] = []
        vitals_sources: list[str] = []
        all_errors: list[dict] = []
        errors_sources: list[str] = []
        source_files: list[str] = []
        file_sizes: dict[str, int] = {}
        per_file_counts: dict[str, tuple[int, int]] = {}
        for local_file, s3_path, object_key in files:
            vitals_rows, errors_rows, corrupt_lines = parse_fn(local_file, service_id)
            all_vitals.extend(vitals_rows)
            vitals_sources.extend([s3_path] * len(vitals_rows))
            all_errors.extend(errors_rows)
            errors_sources.extend([s3_path] * len(errors_rows))
            source_files.append(s3_path)
            file_sizes[object_key] = os.path.getsize(local_file)
            per_file_counts[object_key] = (len(vitals_rows), len(errors_rows))
            outcome = _new_object_outcome()
            outcome["valid_records"] = len(vitals_rows) + len(errors_rows)
            if corrupt_lines:
                try:
                    outcome.update(
                        quarantine_fn(
                            fos,
                            src,
                            object_key,
                            corrupt_lines,
                            len(vitals_rows) + len(errors_rows),
                            local_file=local_file,
                        )
                    )
                except Exception:
                    logger.exception("[ledger] %s: RUM evidence processing failed for %s", service_id, object_key)
                    outcome["malformed_records"] = len(corrupt_lines)
                    outcome["quarantine_capture_failures"] = len(corrupt_lines)
            outcomes[object_key] = _finalize_object_outcome(outcome)
        source_files_sql = ", ".join(f"'{escape_sql_literal(s)}'" for s in source_files)
        enter_write_admission()
        _write_rum_arrow_table_batch(
            duckdb_con, vitals_table, all_vitals, vitals_sources, source_files_sql, CLIENT_VITALS_ARROW_SCHEMA
        )
        _write_rum_arrow_table_batch(
            duckdb_con, errors_table, all_errors, errors_sources, source_files_sql, CLIENT_ERRORS_ARROW_SCHEMA
        )
        try:
            for count_idx, table_name in ((0, "client_vitals"), (1, "client_errors")):
                metadata_db.insert_ingested_files(
                    service_id,
                    [
                        (object_key, per_file_counts.get(object_key, (0, 0))[count_idx], file_sizes.get(object_key, 0))
                        for _lf, _sp, object_key in files
                    ],
                    table_name=table_name,
                )
        except Exception as ie:
            logger.warning(
                "[ledger] %s: RUM ingested_files bookkeeping failed for %d file(s): %s", service_id, len(files), ie
            )

    return _run_batch_convert_common(
        service_id,
        object_keys,
        worker_id,
        cleanup_label="convert_batch_rum_objects",
        log_label="RUM ",
        process_batch_fn=_process_rum_batch,
    )


@app.task(name="backend.core.ingest.convert_batch_rum_files", bind=True)
def convert_batch_rum_files(self, service_id: str, object_keys: list[str]):
    with _celery_ingest_scope("ledger_rum_convert_batch", service_id):
        return convert_batch_rum_objects(service_id, object_keys, self.request.id or "rum-worker")


_orig_convert_batch_rum_files = convert_batch_rum_files


def discover_rum_prefix(
    service_id: str,
    prefix_subpath: str | None = None,
    start_time: str | None = None,
    run_id: int | None = None,
    originating_task: str = "rum_discovery",
) -> int:
    task_obj = _resolve_ingest_hook("convert_batch_rum_files", convert_batch_rum_files, _orig_convert_batch_rum_files)
    return _discover_prefix_common(
        service_id,
        list_kwargs={"start_time": start_time, "prefix_subpath": prefix_subpath or "raw/rum/"},
        prefix_subpath=prefix_subpath,
        dispatch_fn=task_obj.delay,
        originating_task=originating_task,
        run_id=run_id,
        log_label="RUM ",
    )


def sweep_rum_ledger_once(service_id: str, lookback_hours: int = 4, run_id: int | None = None) -> dict:
    return _sweep_ledger_common(service_id, lookback_hours, run_id, is_rum=True)


def ingest_rum_logs(service_id: str, max_seconds: int | None = None, run_id: int | None = None) -> Generator[tuple]:
    """Ingest RUM beacon logs from FOS raw_rum/ prefix into local Parquet buffers & DuckDB DuckLake views."""
    from backend.config import load_config, resolve_raw_delete_after
    from backend.core.ingest import (
        _RECONCILE_LEDGER_EPOCH,
        _capture_corrupt_container,
        _corrupt_gzip_error,
        _delete_objects_robust_with_failures,
        _deterministic_buffer_name,
        _download_chunk_to_local,
        _finalize_sync_outcome_counters,
        _recover_in_flight,
        _write_buffer_with_in_flight,
        list_fos_files,
    )
    from backend.core.storage._core import CLIENT_ERRORS_ARROW_SCHEMA, CLIENT_VITALS_ARROW_SCHEMA

    parse_fn = _resolve_ingest_hook("_parse_rum_beacon_file", _parse_rum_beacon_file, _orig_parse_rum_beacon_file)
    quarantine_fn = _resolve_ingest_hook(
        "_quarantine_rum_corrupt_lines", _quarantine_rum_corrupt_lines, _orig_quarantine_rum_corrupt_lines
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

    raw_delete_after = resolve_raw_delete_after(load_config(service_id))
    try:
        s3 = _get_fos_client(src)
        bucket = src["bucket"]
        bucket_prefix = f"s3://{bucket}/"
        _recover_in_flight(src, table_name="client_vitals")
        _recover_in_flight(src, table_name="client_errors")

        vitals_ingested = (
            metadata_db.get_ingested_filenames(service_id, limit=50_000, table_name="client_vitals") or set()
        )
        errors_ingested = (
            metadata_db.get_ingested_filenames(service_id, limit=50_000, table_name="client_errors") or set()
        )
        already_ingested = {
            p if p.startswith("s3://") else f"{bucket_prefix}{p}" for p in vitals_ingested | errors_ingested
        }

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

        new_files_s3, file_sizes = list_res["new_files"], list_res["file_sizes"]
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

        total_vitals_rows = total_errors_rows = error_count = deleted = reclaimed = 0
        failed_paths: set[str] = set()
        capture_failed_paths: set[str] = set()
        partial_paths: set[str] = set()
        delete_failed_paths: set[str] = set()
        CHUNK_SIZE = 50
        chunks = [new_files_s3[i : i + CHUNK_SIZE] for i in range(0, len(new_files_s3), CHUNK_SIZE)]

        for chunk_idx, chunk in enumerate(chunks):
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
            vitals_batch_records, errors_batch_records = [], []

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
                        file_vitals, file_errors, corrupt_lines = parse_fn(local_path, service_id)
                    except Exception as e:
                        error_count += 1
                        failed_paths.add(s3_path)
                        failed_gzip = _corrupt_gzip_error(local_path)
                        if failed_gzip is not None:
                            q_res = _capture_corrupt_container(
                                service_id, "rum", s3_path.removeprefix(bucket_prefix), local_path, failed_gzip
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
                        logger.warning("RUM sync: %s has %d malformed line(s) quarantined", s3_path, len(corrupt_lines))
                        try:
                            quarantine_result = quarantine_fn(
                                s3,
                                src,
                                s3_path.removeprefix(bucket_prefix),
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
                    vitals_batch_records.append((s3_path, len(file_vitals), size))
                    errors_batch_records.append((s3_path, len(file_errors), size))
                    yield ("file_done", s3_path.split("/")[-1], valid_for_file)

            buf_filename = _deterministic_buffer_name(chunk)
            for table_name, batch_records, chunk_rows, schema in (
                ("client_vitals", vitals_batch_records, chunk_vitals_rows, CLIENT_VITALS_ARROW_SCHEMA),
                ("client_errors", errors_batch_records, chunk_errors_rows, CLIENT_ERRORS_ARROW_SCHEMA),
            ):
                if batch_records:
                    tbl = pa.Table.from_pylist(chunk_rows, schema=schema) if chunk_rows else None
                    _write_buffer_with_in_flight(
                        src, service_id, buf_filename, tbl, batch_records, table_name=table_name
                    )

            if raw_delete_after:
                exclude_from_delete = failed_paths | capture_failed_paths
                chunk_keys = [
                    f.removeprefix(bucket_prefix)
                    for f in chunk
                    if f.startswith(bucket_prefix) and f not in exclude_from_delete
                ]
                if chunk_keys:
                    try:
                        chunk_deleted, chunk_failed_keys = _delete_objects_robust_with_failures(s3, bucket, chunk_keys)
                        deleted += chunk_deleted
                        delete_failed_paths.update(f"{bucket_prefix}{k}" for k in chunk_failed_keys)
                    except Exception as del_err:
                        delete_failed_paths.update(f"{bucket_prefix}{k}" for k in chunk_keys)
                        logger.error("RUM sync: failed to delete %d raw files: %s", len(chunk_keys), del_err)

        if raw_delete_after and stranded_already:
            try:
                deletable = metadata_db.get_reclaimable_strand_filenames(
                    service_id, set(stranded_already), _RECONCILE_LEDGER_EPOCH
                )
            except Exception as classify_err:
                logger.warning(
                    "RUM sync: could not classify reclaimable strands for %s (%s) — skipping reclaim this run",
                    service_id,
                    classify_err,
                )
                deletable = set()
            stranded_keys = [
                f.removeprefix(bucket_prefix)
                for f in stranded_already
                if f in deletable and f.startswith(bucket_prefix)
            ]
            if stranded_keys:
                try:
                    reclaim_del, reclaim_failures = _delete_objects_robust_with_failures(s3, bucket, stranded_keys)
                    reclaimed += reclaim_del
                    deleted += reclaim_del
                    delete_failed_paths.update(f"{bucket_prefix}{k}" for k in reclaim_failures)
                except Exception as rec_err:
                    delete_failed_paths.update(f"{bucket_prefix}{k}" for k in stranded_keys)
                    logger.error("RUM sync: failed to reclaim stranded raw files: %s", rec_err)

        total_rows = total_vitals_rows + total_errors_rows
        yield ("done", total_rows)
        duration_s = time.time() - start_time
        _finalize_sync_outcome_counters(
            outcome_counters, failed_paths, capture_failed_paths, delete_failed_paths, partial_paths
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
        summary_msg = (
            f"Ingested {total_rows} RUM rows ({error_count} file error(s))"
            if had_errors
            else f"Ingested {total_rows} RUM rows"
        )
        log_cron_run(
            service_id,
            "rum_sync",
            duration_s,
            "error" if had_errors else "success",
            files_downloaded=len(new_files_s3),
            rows_ingested=total_rows,
            run_id=run_id,
            summary=summary_msg,
            outcome_counters=outcome_counters,
        )
    except Exception as e:
        logger.error(f"RUM ingest failed: {e}", exc_info=True)
        log_cron_run(
            service_id,
            "rum_sync",
            time.time() - start_time,
            "error",
            files_downloaded=len(new_files_s3) if "new_files_s3" in locals() else 0,
            error_message=str(e),
            outcome_counters=outcome_counters,
            run_id=run_id,
        )
        yield ("error", "sync", str(e))
    finally:
        finalize_cron_run_if_running(service_id, "rum_sync", run_id)
