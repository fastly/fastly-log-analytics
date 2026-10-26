import concurrent.futures
import gzip
import hashlib
import itertools
import json
import logging
import math
import os
import random
import re
import tempfile
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import backend.core as _core_pkg
from backend.core import field_registry as fc
from backend.core import iceberg
from backend.core import metadata as metadata_db
from backend.core.duckdb import (
    _DEFAULT_SOURCE,
    INGEST_CHUNK_SIZE,
    _configure_fos,  # noqa: F401  re-exported for test monkey-patching
    _ensure_source_registered,
    _execute_query_with_retry,
    _get_fos_client,
    _load_httpfs,  # noqa: F401  re-exported for test monkey-patching
)
from backend.core.field_registry import LOG_FIELD_CATALOG
from backend.core.metadata import ingest_log as _quarantine_mod
from backend.utils.active_requests import yield_to_api
from backend.utils.date_utils import ensure_utc
from backend.utils.sql_validator import escape_sql_literal
from backend.utils.telemetry import log_once_per

_core_pkg.quarantine = _quarantine_mod  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)

_INGEST_OUTCOME_COUNTER_KEYS = (
    "valid_records", "malformed_records", "corrupt_containers", "quarantine_capture_failures",
    "source_delete_failures", "cap_evictions", "objects_processed", "objects_successful",
    "objects_partial", "objects_failed",
)  # fmt: skip


def _new_object_outcome(*, processed: int = 1) -> dict[str, int]:
    return {**dict.fromkeys(_INGEST_OUTCOME_COUNTER_KEYS, 0), "objects_processed": processed}


def _record_ledger_object_outcome(service_id: str, object_key: str, counters: dict[str, int]) -> None:
    from backend.core.metadata.cron_log import record_ingest_object_outcome

    record_ingest_object_outcome(service_id, object_key, counters)


def _record_ledger_batch_outcomes(service_id: str, outcomes_by_key: dict[str, dict[str, int]]) -> None:
    if not outcomes_by_key:
        return
    from backend.core.metadata.cron_log import record_ingest_batch_outcomes

    record_ingest_batch_outcomes(service_id, outcomes_by_key)


def _mark_object_failed(counters: dict[str, int]) -> dict[str, int]:
    counters["objects_failed"] = 1
    counters["objects_successful"] = 0
    counters["objects_partial"] = 0
    return counters


def _failed_object_outcome() -> dict[str, int]:
    return _mark_object_failed(_new_object_outcome())


def _finalize_object_outcome(outcome: dict[str, int]) -> dict[str, int]:
    if outcome["quarantine_capture_failures"]:
        _mark_object_failed(outcome)
    elif outcome["malformed_records"] or outcome["corrupt_containers"]:
        outcome["objects_partial"] = 1
    else:
        outcome["objects_successful"] = 1
    return outcome


def _sum_evidence_counts(evidence_results: list[dict[str, Any]], extra_failures: int = 0) -> dict[str, int]:
    return {
        "cap_evictions": sum(int(r.get("cap_evictions", 0)) for r in evidence_results),
        "quarantine_capture_failures": extra_failures
        + sum(int(r.get("quarantine_capture_failures", 0)) for r in evidence_results),
    }


def _is_permission_error(err: Any) -> bool:
    s = str(err or "")
    return "AccessDenied" in s or "UnauthorizedAccess" in s


def _parse_time_bounds(start_time: str | None, end_time: str | None) -> tuple[datetime | None, datetime | None]:
    from backend.utils.date_utils import parse_iso_utc

    return (parse_iso_utc(start_time) if start_time else None, parse_iso_utc(end_time) if end_time else None)


def _consume_generator(gen: Any) -> Any:
    try:
        while True:
            next(gen)
    except StopIteration as e:
        return e.value


def _read_json_auto_sql(paths_expr: str, columns_sql: str, *, filename: bool = False) -> str:
    fn_clause = ", filename=true" if filename else ""
    return (
        f"read_json_auto({paths_expr}, format='newline_delimited', "
        f"records='auto'{fn_clause}, columns={columns_sql}, ignore_errors=true)"
    )


def _export_staging_arrow(mem_con: Any) -> Any:
    fetched = mem_con.execute("SELECT * FROM _ingest_staging WHERE timestamp IS NOT NULL").to_arrow_table()
    return fetched.read_all() if hasattr(fetched, "read_all") else fetched


def _ducklake_table_exists(duckdb_con: Any, table_name: str) -> bool:
    return bool(
        duckdb_con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
    )


def _ducklake_replace_source_rows(
    duckdb_con: Any, table_name: str, source_in_sql: str, select_sql: str, caller_name: str
) -> None:
    duckdb_con.execute("BEGIN TRANSACTION")
    try:
        duckdb_con.execute(f"DELETE FROM lake.{table_name} WHERE _source_file IN ({source_in_sql})")
        duckdb_con.execute(f"INSERT INTO lake.{table_name} BY NAME {select_sql}")
        duckdb_con.execute("COMMIT")
    except Exception:
        try:
            duckdb_con.execute("ROLLBACK")
        except Exception:
            logger.debug("%s: duckdb_con.execute failed (ignored)", caller_name, exc_info=True)
        raise


def _record_raw_delete_failure(service_id: str, object_key: str) -> None:
    from backend.core.metadata.base import get_con
    from backend.core.metadata.cron_log import OUTCOME_COUNTER_KEYS

    con = get_con(service_id)
    row = con.execute(
        "SELECT outcome_counters FROM ingest_ledger WHERE service_id = ? AND object_key = ?",
        (service_id, object_key),
    ).fetchone()
    counters = json.loads(row["outcome_counters"] or "{}") if row else {}
    outcome = {key: int(counters.get(key, 0)) for key in OUTCOME_COUNTER_KEYS}
    outcome["objects_processed"] = max(1, outcome["objects_processed"])
    outcome["source_delete_failures"] += 1
    _mark_object_failed(outcome)
    _record_ledger_object_outcome(service_id, object_key, outcome)


def _capture_corrupt_container(
    service_id: str,
    source_type: str,
    object_key: str,
    local_file: str,
    error: BaseException,
    *,
    full_outcome: bool = True,
) -> dict[str, int]:
    from backend.core.quarantine import capture_evidence

    try:
        with open(local_file, "rb") as raw:
            payload = raw.read()
        result = capture_evidence(
            service_id, source_type, object_key, payload,
            line_ordinal=None, byte_offset=None, error_category="corrupt_container", error_text=str(error)[:2_000],
        )  # fmt: skip
    except OSError:
        result = {"cap_evictions": 0, "quarantine_capture_failures": 1}
    cap_evictions = int(result.get("cap_evictions", 0))
    capture_failures = int(result.get("quarantine_capture_failures", 0))
    if not full_outcome:
        return {
            "valid_records": 0,
            "malformed_records": 0,
            "corrupt_containers": 1,
            "cap_evictions": cap_evictions,
            "quarantine_capture_failures": capture_failures,
        }
    counters = _failed_object_outcome()
    counters["corrupt_containers"] = 1
    counters["cap_evictions"] = cap_evictions
    counters["quarantine_capture_failures"] = capture_failures
    return counters


def _corrupt_gzip_error(local_file: str) -> BaseException | None:
    try:
        with gzip.open(local_file, "rb") as raw:
            while raw.read(64 * 1024):
                pass
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        return exc
    return None


_STRANDED_DELETE_CAP = 10_000
_RECONCILE_MIN_INTERVAL_S = 300.0
_reconcile_last_attempt: dict[str, float] = {}
_RECONCILE_LEDGER_EPOCH = "2026-06-21 00:00:00"
_INGEST_EXCLUDED_GROUPS = ("METRICS", "VIRTUAL", "INTERNAL")


def get_ingest_type_hints(log_fields_config: dict | None = None) -> dict[str, str]:
    hints = {
        f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"])
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in _INGEST_EXCLUDED_GROUPS and f.get("vcl") is not None
    }
    if log_fields_config:
        for cf in log_fields_config.get("custom_fields", []):
            if cf.get("enabled", True):
                hints[cf["name"]] = cf.get("duckdb_type", "VARCHAR")
    return hints


def get_catalog_field_ids(log_fields_config: dict | None = None) -> list[str]:
    ids = [
        f["id"] for f in LOG_FIELD_CATALOG if f.get("group") not in _INGEST_EXCLUDED_GROUPS and f.get("vcl") is not None
    ]
    if log_fields_config:
        for cf in sorted(log_fields_config.get("custom_fields", []), key=lambda x: x["name"]):
            if cf.get("enabled", True):
                ids.append(cf["name"])
    return ids


def get_ingest_columns_sql(log_fields_config: dict | None = None) -> str:
    hints = get_ingest_type_hints(log_fields_config)
    return "{" + ", ".join(f"'{escape_sql_literal(k)}': '{escape_sql_literal(v)}'" for k, v in hints.items()) + "}"


_DECODE_EXPRS: dict[str, str] = {
    "c_speed": fc.duckdb_decode_case("c_speed", fc.CONN_SPEED_ENCODE),
    "p_type": fc.duckdb_decode_case("p_type", fc.PROXY_TYPE_ENCODE),
    "p_desc": fc.duckdb_decode_case("p_desc", fc.PROXY_DESC_ENCODE),
}


def _build_column_select_exprs(
    src: dict | None,
    log_fields_config: dict | None = None,
    *,
    source_file_expr: str | None = None,
) -> list[str]:
    """Build the canonical column SELECT expression list for raw-JSON ingest."""
    catalog_field_ids = get_catalog_field_ids(log_fields_config)
    svc_id = (src or {}).get("logging_service_id") or (src or {}).get("name", "")
    if svc_id:
        escaped = escape_sql_literal(svc_id)
        backend_expr = f"regexp_replace(\"backend\", '^{escaped}--', '') AS \"backend\""
    else:
        backend_expr = '"backend"'

    field_selects: list[str] = []
    for fid in catalog_field_ids:
        if fid in ("backend", "_source_file"):
            continue
        safe_fid = fid.replace('"', '""')
        field_selects.append(f'{_DECODE_EXPRS[fid]} AS "{safe_fid}"' if fid in _DECODE_EXPRS else f'"{safe_fid}"')
    if source_file_expr is not None:
        field_selects.append(f"{source_file_expr} AS _source_file")
    field_selects.append(backend_expr)
    return field_selects


_FASTLY_FNAME_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}[-:]\d{2}[-:]\d{2})")
_EMPTY_VALUE_RE = re.compile(r":(?=[,}])")


def _parse_fastly_filename_dt(fname: str) -> datetime | None:
    """Parse the leading ``YYYY-MM-DDTHH-MM-SS`` of a Fastly raw log filename into a UTC datetime."""
    m = _FASTLY_FNAME_TS_RE.match(fname)
    if not m:
        return None
    try:
        date_part, time_part = m.group(1).split("T", 1)
        return datetime.fromisoformat(f"{date_part}T{time_part.replace('-', ':')}").replace(tzinfo=UTC)
    except ValueError:
        return None


_V3_MINUTE_KEY_RE = re.compile(
    r"(?:^|/)raw/(?:request|rum)/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/"
)
_MINUTE_KEY_RE = re.compile(r"/raw/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/")
_SLASHED_KEY_RE = re.compile(r"/raw/(\d{4})/(\d{2})/(\d{2})/(\d{2})/[^/]*?_(\d{2})\.json\.gz")
_DASH_KEY_RE = re.compile(r"/raw/(\d{4}-\d{2}-\d{2})/(\d{2})/")


def _parse_key_layout_dt(key: str) -> tuple[str, datetime] | None:
    """Classify a raw-log key's layout and extract its UTC timestamp."""
    for layout, pattern in (
        ("v3", _V3_MINUTE_KEY_RE),
        ("minute", _MINUTE_KEY_RE),
        ("slashed", _SLASHED_KEY_RE),
    ):
        m = pattern.search(key)
        if m:
            try:
                y, mo, d, h, mi = (int(g) for g in m.groups())
                return (layout, datetime(y, mo, d, h, mi, tzinfo=UTC))
            except ValueError:
                return None
    m = _DASH_KEY_RE.search(key)
    if m:
        fname_dt = _parse_fastly_filename_dt(key.split("/")[-1])
        if fname_dt is not None:
            return ("dash", fname_dt)
        try:
            return ("dash", datetime.fromisoformat(m.group(1)).replace(hour=int(m.group(2)), tzinfo=UTC))
        except ValueError:
            return None
    return None


def _compute_incremental_start_after(
    already: set[str], lookback_hours: int = 4, prefix_subpath: str = "raw/request/"
) -> str | None:
    """Derive an S3 ``StartAfter`` key from previously-ingested filenames."""
    norm_subpath = prefix_subpath.strip("/")
    parsed = [p for f in already if norm_subpath in f and (p := _parse_key_layout_dt(f)) is not None] or [
        p for f in already if "/raw/" in f and (p := _parse_key_layout_dt(f)) is not None
    ]
    if not parsed:
        return None
    lookback = max(dt for _, dt in parsed) - timedelta(hours=lookback_hours)
    layouts = {layout for layout, _ in parsed}
    if "v3" in layouts:
        return lookback.strftime(f"{norm_subpath}/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
    if "dash" in layouts:
        return lookback.strftime("raw/%Y-%m-%d/%H/")
    if "slashed" in layouts:
        return lookback.strftime("raw/%Y/%m/%d/%H/")
    return lookback.strftime("raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")


def _delete_objects_robust_with_failures(fos_client, bucket: str, keys: list[str]) -> tuple[int, set[str]]:
    """Delete objects and return the exact keys that could not be acknowledged."""
    if not keys:
        return 0, set()

    failed_keys: set[str] = set()
    try:
        batch_size, total_deleted = 500, 0
        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            response = fos_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
            )
            errors = response.get("Errors", [])
            batch_failures = {err.get("Key") for err in errors if err.get("Key") in batch}
            if any(err.get("Key") not in batch for err in errors):
                batch_failures = set(batch)
            if errors and _is_permission_error(errors[0].get("Code", "")):
                logger.warning(
                    "Bulk delete skipped due to missing permissions (%s). Disabling further delete attempts for this batch.",
                    errors[0].get("Code"),
                )
                total_deleted += len(batch) - len(batch_failures)
                return total_deleted, failed_keys | batch_failures | set(keys[i + len(batch) :])
            total_deleted += len(batch) - len(batch_failures)
            failed_keys.update(batch_failures)
        return total_deleted, failed_keys
    except Exception as e:
        err_str = str(e)
        if _is_permission_error(err_str):
            logger.warning("Delete failed due to missing permissions: %s", err_str.split(":", 1)[-1].strip() or err_str)
            return 0, set(keys)

        logger.warning("Bulk delete failed, falling back to individual", exc_info=True)
        deleted_count, individual_failures = 0, set()
        if len(keys) <= 4:
            for index, k in enumerate(keys):
                try:
                    fos_client.delete_object(Bucket=bucket, Key=k)
                    deleted_count += 1
                except Exception as individual_err:
                    ind_err_str = str(individual_err)
                    if _is_permission_error(ind_err_str):
                        logger.warning(
                            "Individual delete failed due to missing permissions: %s. Stopping further deletes.",
                            ind_err_str,
                        )
                        individual_failures.update(keys[index:])
                        break
                    logger.warning("Failed to delete object %s", k, exc_info=True)
                    individual_failures.add(k)
            return deleted_count, individual_failures

        access_denied_event = threading.Event()

        def _delete_single_key(k: str) -> tuple[str, bool, str | None]:
            if access_denied_event.is_set():
                return k, False, "AccessDenied_aborted"
            try:
                fos_client.delete_object(Bucket=bucket, Key=k)
                return k, True, None
            except Exception as individual_err:
                ind_err_str = str(individual_err)
                if _is_permission_error(ind_err_str):
                    access_denied_event.set()
                return k, False, ind_err_str

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(32, len(keys)), thread_name_prefix="fos_del"
        ) as del_ex:
            for fut in concurrent.futures.as_completed({del_ex.submit(_delete_single_key, k): k for k in keys}):
                k, ok, err_s = fut.result()
                if ok:
                    deleted_count += 1
                else:
                    msg = (
                        "Individual delete permission error on %s: %s"
                        if _is_permission_error(err_s)
                        else "Failed to delete object %s: %s"
                    )
                    logger.warning(msg, k, err_s)
                    individual_failures.add(k)
        return deleted_count, individual_failures


def _delete_objects_robust(fos_client, bucket: str, keys: list[str]) -> int:
    """Delete multiple objects, retaining the historical deleted-count result."""
    return _delete_objects_robust_with_failures(fos_client, bucket, keys)[0]


def _scan_invalid_gzip_lines(paths: list[str]) -> list[tuple[str, str]]:
    """Find malformed request rows from downloaded gzip files without DuckDB."""
    invalid_rows: list[tuple[str, str]] = []
    for path in paths:
        with gzip.open(path, "rb") as compressed:
            for raw_line in compressed:
                if not raw_line.strip():
                    continue
                try:
                    row = json.loads(raw_line)
                    is_valid = isinstance(row, dict) and bool(row.get("timestamp"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    is_valid = False
                if not is_valid:
                    invalid_rows.append((path, raw_line.decode("utf-8", errors="replace").rstrip("\r\n")))
    return invalid_rows


def _download_chunk_to_local(fos_client, s3_paths: list[str], tmpdir: str) -> tuple[dict[str, str], dict[str, str]]:
    """Download each ``s3://bucket/key`` once into ``tmpdir``, returning ``(s3_to_local, local_to_s3)``."""
    from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

    s3_to_local: dict[str, str] = {}
    local_to_s3: dict[str, str] = {}

    def _download_one(s3_path: str) -> tuple[str, str]:
        bucket, key = s3_path.removeprefix("s3://").split("/", 1)
        local_path = os.path.join(tmpdir, f"{abs(hash(s3_path)):x}_{key.rsplit('/', 1)[-1]}")
        token = _BOTO3_CALLER_HINT.set("ingest_download")
        try:
            resp = fos_client.get_object(Bucket=bucket, Key=key)
            with open(local_path, "wb") as f:
                f.write(resp["Body"].read())
        finally:
            _BOTO3_CALLER_HINT.reset(token)
        return s3_path, local_path

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(64, max(16, len(s3_paths))), thread_name_prefix="ingest_dl"
    ) as ex:
        futures = {ex.submit(_download_one, p): p for p in s3_paths}
        for fut in concurrent.futures.as_completed(futures):
            try:
                s3, local = fut.result()
                s3_to_local[s3], local_to_s3[local] = local, s3
            except Exception as e:
                p, err_str = futures[fut], str(e)
                is_transient = "(502)" in err_str or "(503)" in err_str
                (logger.debug if is_transient else logger.warning)(
                    "[ingest] transient FOS error downloading %s: %s"
                    if is_transient
                    else "[ingest] failed to download %s: %s",
                    p,
                    err_str,
                )
    return s3_to_local, local_to_s3


def _deterministic_buffer_name(chunk: list[str]) -> str:
    """Return ``batch_{sha256(sorted_chunk)[:16]}.parquet``."""
    return f"batch_{hashlib.sha256('\n'.join(sorted(chunk)).encode('utf-8')).hexdigest()[:16]}.parquet"


def _recover_in_flight(source: dict, table_name: str = "logs") -> dict:
    """Reconcile ``ingest_in_flight`` rows with on-disk buffer state."""
    source_name = source["name"]
    pending = metadata_db.list_in_flight(source_name, table_name=table_name)
    if not pending:
        return {"promoted": 0, "dropped": 0, "rows_recovered": 0}

    buf_dir = iceberg._buffer_dir(source, table_name=table_name)  # type: ignore[attr-defined]
    promoted = dropped = rows_recovered = 0
    for buffer_filename, file_rows in pending:
        if os.path.isfile(os.path.join(buf_dir, buffer_filename)) and file_rows:
            row_sum = sum(rc for (_, rc, _) in file_rows if rc)
            metadata_db.insert_ingested_files(source_name, file_rows, table_name=table_name)
            metadata_db.clear_in_flight(source_name, buffer_filename, table_name=table_name)
            promoted += 1
            rows_recovered += row_sum
            logger.info(
                "[ingest] %s: recovered in_flight buffer %s (%s) — promoted %d files (%d rows)",
                source_name, buffer_filename, table_name, len(file_rows), row_sum,
            )  # fmt: skip
        else:
            metadata_db.clear_in_flight(source_name, buffer_filename, table_name=table_name)
            dropped += 1
            logger.info(
                "[ingest] %s: dropped stale in_flight row for missing buffer %s (%s) (%d files will re-ingest)",
                source_name, buffer_filename, table_name, len(file_rows),
            )  # fmt: skip
    return {"promoted": promoted, "dropped": dropped, "rows_recovered": rows_recovered}


def list_fos_files(
    src: dict,
    prefix_subpath: str = "raw/request/",
    exclude_prefix_subpath: str | None = None,
    already_ingested: set[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    incremental_only: bool = False,
    max_files: int | None = None,
    elapsed_fn=None,
    delete_after: bool = False,
    fos_client=None,
):
    """List and discover new files in Fastly Object Storage."""
    if elapsed_fn is None:
        elapsed_fn = lambda: ""  # noqa: E731

    st_dt, et_dt = _parse_time_bounds(start_time, end_time)
    already = already_ingested or set()
    start_after_key = None

    def _resolve_start_after_key() -> str | None:
        nonlocal start_after_key
        if start_after_key is not None:
            return start_after_key
        if st_dt and not already:
            start_after_key = st_dt.strftime(f"{prefix_subpath.strip('/')}/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
            logger.info(
                "[ingest] %s: Using requested start_time to bound FOS scan: %s", src.get("name"), start_after_key
            )
        elif incremental_only and already:
            try:
                start_after_key = _compute_incremental_start_after(
                    already, lookback_hours=4, prefix_subpath=prefix_subpath
                )
            except Exception as e:
                logger.warning(
                    "[ingest] %s: Failed to calculate lookback marker, scanning full bucket: %s", src.get("name"), e
                )
        return start_after_key

    fos_client = fos_client or _get_fos_client(src)
    file_sizes: dict[str, int] = {}
    new_files: list[str] = []
    skipped_already = total_listed = 0
    stranded_already: list[str] = []

    try:
        prefix_path = src.get("prefix", "").strip("/")
        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
        raw_prefix = f"{prefix_path}/{prefix_subpath}" if prefix_path else prefix_subpath
        yield {"type": "status", "message": f"{elapsed_fn()} Discovering new files in Fastly Object Storage..."}

        from backend.provision.log_paths import minute_list_prefix, rum_minute_list_prefix

        now_utc, norm_subpath = datetime.now(UTC), prefix_subpath.strip("/")
        has_v3_files = bool(already) and any("year=" in f for f in already)
        use_minute_prefixes = (
            incremental_only and not st_dt and not et_dt and norm_subpath in ("raw/request", "raw/rum") and has_v3_files
        )

        scan_batches: list[dict[str, Any]] = []
        if use_minute_prefixes:
            prefix_fn = rum_minute_list_prefix if norm_subpath == "raw/rum" else minute_list_prefix
            for i in range(5):
                min_sub = prefix_fn(now_utc - timedelta(minutes=i))
                scan_batches.append(
                    {"Bucket": src["bucket"], "Prefix": f"{prefix_path}/{min_sub}" if prefix_path else min_sub}
                )

        if not scan_batches:
            sak = _resolve_start_after_key()
            kwargs = {"Bucket": src["bucket"], "Prefix": raw_prefix}
            if sak:
                kwargs["StartAfter"] = f"{prefix_path}/{sak}" if prefix_path else sak
            scan_batches.append(kwargs)

        seen_keys: set[str] = set()
        exclude_prefix = (
            (f"{prefix_path}/{exclude_prefix_subpath}" if prefix_path else exclude_prefix_subpath)
            if exclude_prefix_subpath
            else None
        )
        et_bound = (et_dt + timedelta(hours=1)) if et_dt else None
        st_bound = (st_dt - timedelta(hours=1)) if st_dt else None
        s3_bucket_prefix = f"s3://{src['bucket']}/"

        def _process_contents(contents: list[dict[str, Any]]):
            nonlocal total_listed, skipped_already
            for obj in contents:
                key = obj["Key"]
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                if not key.endswith(".gz") or (exclude_prefix and key.startswith(exclude_prefix)):
                    continue
                total_listed += 1
                if total_listed % 10000 == 0:
                    yield {"type": "status", "message": f"{elapsed_fn()} Discovered {total_listed:,} new files..."}
                if et_bound is not None or st_bound is not None:
                    file_dt = _parse_fastly_filename_dt(key.rsplit("/", 1)[-1])
                    if file_dt is not None and (
                        (et_bound is not None and file_dt > et_bound) or (st_bound is not None and file_dt < st_bound)
                    ):
                        continue
                full_path = f"{s3_bucket_prefix}{key}"
                if full_path not in already:
                    new_files.append(full_path)
                    file_sizes[full_path] = obj["Size"]
                else:
                    skipped_already += 1
                    if delete_after and len(stranded_already) < _STRANDED_DELETE_CAP:
                        stranded_already.append(full_path)

        for scan_kw in scan_batches:
            for page in paginator.paginate(**scan_kw):
                yield from _process_contents(page.get("Contents", []))
                if max_files and len(new_files) >= max_files:
                    break
            if max_files and len(new_files) >= max_files:
                break
        if max_files and len(new_files) > max_files:
            new_files = new_files[:max_files]
    except Exception as e:
        logger.exception("[ingest] Could not list FOS objects: %s", e)
        yield {"type": "error", "message": f"Could not list FOS objects: {e}"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    return {
        "new_files": new_files,
        "file_sizes": file_sizes,
        "skipped_already": skipped_already,
        "stranded_already": stranded_already,
    }


def ingest(
    source: dict | None = None,
    delete_after: bool = False,
    max_files: int | None = None,
    max_seconds: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    incremental_only: bool = False,
):
    """Ingest new raw log files from FOS into the local Parquet buffer."""
    start_time_exec = time.time()

    def elapsed() -> str:
        s = time.time() - start_time_exec
        return f"{int(s // 60)}m{int(s % 60):02d}s" if s >= 60 else f"{s:.1f}s"

    src = source or _DEFAULT_SOURCE
    if src.get("access_level") == "read_only":
        yield {"type": "error", "message": "Write operations are disabled in read-only mode."}
        return
    svc_id, source_name = src.get("service_id", "unknown"), src["name"]
    svc_name = src.get("service_name") or source_name
    display_name = f"{svc_name} ({svc_id})" if svc_name != svc_id else svc_id
    _ensure_source_registered(src)

    if not src.get("bucket"):
        yield {"type": "error", "message": "Fastly Object Storage bucket is not configured for this service."}
        return

    fos_client = _get_fos_client(src)
    try:
        recovery = _recover_in_flight(src)
        if recovery["promoted"] or recovery["dropped"]:
            yield {
                "type": "status",
                "message": (
                    f"Crash recovery: promoted {recovery['promoted']} buffer(s) "
                    f"({recovery['rows_recovered']:,} rows), dropped {recovery['dropped']} stale row(s)."
                ),
            }
    except Exception as e:
        logger.warning("[ingest] %s: in_flight recovery sweep failed: %s", display_name, e)

    _parse_time_bounds(start_time, end_time)
    yield {"type": "status", "message": f"{elapsed()} Fetching ingestion history..."}
    already = metadata_db.get_ingested_filenames(source_name, limit=20_000 if incremental_only else None)

    list_res = yield from list_fos_files(
        src=src,
        prefix_subpath="raw/request/",
        already_ingested=already,
        start_time=start_time,
        end_time=end_time,
        incremental_only=incremental_only,
        max_files=max_files,
        elapsed_fn=elapsed,
        delete_after=delete_after,
        fos_client=fos_client,
    )

    new_files, file_sizes = list_res["new_files"], list_res["file_sizes"]
    skipped_already, stranded_already = list_res["skipped_already"], list_res["stranded_already"]
    outcome_counters = _new_object_outcome(processed=0)

    reclaimed = 0
    if delete_after and stranded_already:
        throttled = False
        if incremental_only:
            now = time.time()
            if now - _reconcile_last_attempt.get(source_name, 0.0) < _RECONCILE_MIN_INTERVAL_S:
                throttled = True
            else:
                _reconcile_last_attempt[source_name] = now

        if not throttled:
            try:
                deletable = metadata_db.get_reclaimable_strand_filenames(
                    source_name, set(stranded_already), _RECONCILE_LEDGER_EPOCH
                )
            except Exception as classify_err:
                logger.warning(
                    "[ingest] %s: could not classify reclaimable strands (%s) — skipping reclaim this run",
                    display_name, classify_err,
                )  # fmt: skip
                deletable = set()

            s3_pfx = f"s3://{src['bucket']}/"
            stranded_keys = [p[len(s3_pfx) :] for p in stranded_already if p in deletable and p.startswith(s3_pfx)]
            if stranded_keys:
                yield {
                    "type": "status",
                    "message": (
                        f"{elapsed()} Reclaiming {len(stranded_keys)} raw file(s) "
                        "ingested but not deleted by an interrupted prior run..."
                    ),
                }
                try:
                    reclaimed = _delete_objects_robust(fos_client, src["bucket"], stranded_keys)
                except Exception as recl_err:
                    logger.warning(
                        "[ingest] %s: stranded-delete reconcile failed (%s) — will retry next run",
                        display_name, recl_err,
                    )  # fmt: skip

    if not new_files:
        msg = (
            f"No new files; reclaimed {reclaimed} raw file(s) left by an interrupted prior run."
            if reclaimed
            else "Already up to date — no new files to ingest."
        )
        yield {
            "type": "done",
            "new_files": 0,
            "skipped_files": skipped_already,
            "rows_inserted": 0,
            "deleted_files": reclaimed,
            "outcome_counters": outcome_counters,
            "message": msg,
        }
        return

    chunk_size = INGEST_CHUNK_SIZE
    total_inserted = total_corrupt = total_quarantined = processed_count = deleted = 0
    corrupt_containers = quarantine_capture_failures = cap_evictions = 0
    total_corrupt_details: list[str] = []
    successfully_processed_files: list[str] = []
    delete_failed_paths: set[str] = set()
    partial_paths: set[str] = set()
    capture_failed_paths: set[str] = set()
    touched_hours: set[str] = set()

    mem_con = None
    _delete_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="ingest_delete")
    _pending_deletes: list[tuple[concurrent.futures.Future, set[str]]] = []

    def _record_delete_result(future: concurrent.futures.Future, keys: set[str]) -> None:
        nonlocal deleted
        try:
            deleted_count, failed_keys = future.result()
            deleted += deleted_count
            delete_failed_paths.update(f"s3://{src['bucket']}/{key}" for key in failed_keys)
        except Exception as delete_error:
            delete_failed_paths.update(f"s3://{src['bucket']}/{key}" for key in keys)
            logger.warning("[ingest] %s: async delete error: %s", source_name, delete_error)

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) if source else None
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    columns_sql = get_ingest_columns_sql(log_fields_config)

    try:
        from backend.core.duckdb import get_memory_connection

        mem_con = get_memory_connection(src)
        mem_con.execute(f"SET threads = {os.cpu_count() or 4};")
        try:
            mem_con.execute("SET force_download = true;")
        except Exception as _fd_err:
            logger.debug("force_download not supported on this DuckDB build: %s", _fd_err)

        failed_paths: set[str] = set()
        for chunk_start in range(0, len(new_files), chunk_size):
            yield_to_api()
            if chunk_start > 0 and max_seconds and (time.time() - start_time_exec) > max_seconds:
                yield {
                    "type": "status",
                    "message": f"{elapsed()} Time limit of {max_seconds}s reached. Stopping batch early.",
                }
                break

            chunk = new_files[chunk_start : chunk_start + chunk_size]
            outcome_counters["objects_processed"] += len(chunk)
            chunk_num = (chunk_start // chunk_size) + 1
            total_chunks = math.ceil(len(new_files) / chunk_size)
            yield {
                "type": "status",
                "message": f"{elapsed()} Processing {len(chunk)} files in batch {chunk_num}/{total_chunks} ({len(new_files):,} files total)...",
            }

            count_map: dict[str, int] = {}
            chunk_tmpdir_obj = tempfile.TemporaryDirectory(prefix="ingest_chunk_")
            try:
                s3_to_local, local_to_s3 = _download_chunk_to_local(fos_client, list(chunk), chunk_tmpdir_obj.name)
                failed_paths.update(p for p in chunk if p not in s3_to_local)
                read_paths_s3 = [p for p in chunk if p in s3_to_local]
                if not read_paths_s3:
                    yield {"type": "status", "message": "All files in batch failed to download."}
                    continue
                read_paths = [s3_to_local[p] for p in read_paths_s3]

                mem_con.execute("DROP TABLE IF EXISTS _ingest_staging")
                paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in read_paths)

                try:
                    _execute_query_with_retry(
                        mem_con,
                        f"CREATE TEMP TABLE _ingest_staging AS SELECT * FROM {_read_json_auto_sql(f'[{paths_sql}]', columns_sql, filename=True)}",
                    )
                except Exception as batch_err:
                    logger.error(f"Batch read failed: {batch_err}")
                    yield {"type": "status", "message": "Batch read failed, isolating problematic files..."}

                    valid_paths = []
                    for i, read_path in enumerate(read_paths):
                        try:
                            safe_read_path = escape_sql_literal(read_path)
                            _execute_query_with_retry(
                                mem_con,
                                f"SELECT 1 FROM read_json_auto('{safe_read_path}', sample_size=1) LIMIT 1",
                                max_retries=2,
                            )
                            valid_paths.append(f"'{safe_read_path}'")
                        except Exception as file_err:
                            f_name = read_paths_s3[i].split("/")[-1]
                            err_msg = str(file_err)
                            logger.error(f"Ingest isolation failed for {f_name}: {err_msg}")
                            err_msg = err_msg.split(":", 1)[-1].strip()
                            failed_paths.add(read_paths_s3[i])
                            failed_gzip = _corrupt_gzip_error(read_path)
                            if failed_gzip is not None:
                                object_key = read_paths_s3[i].removeprefix(f"s3://{src['bucket']}/")
                                result = _capture_corrupt_container(
                                    src.get("service_id") or src.get("name", ""),
                                    "request",
                                    object_key,
                                    read_path,
                                    failed_gzip,
                                )
                                corrupt_containers += result["corrupt_containers"]
                                cap_evictions += result["cap_evictions"]
                                quarantine_capture_failures += result["quarantine_capture_failures"]
                                if result["quarantine_capture_failures"]:
                                    capture_failed_paths.add(read_paths_s3[i])
                            yield {"type": "status", "message": f"Skipping unreadable file {f_name}: {err_msg}"}

                    if not valid_paths:
                        yield {"type": "status", "message": "All files in batch are unreadable."}
                        continue

                    try:
                        _execute_query_with_retry(
                            mem_con,
                            f"CREATE TEMP TABLE _ingest_staging AS SELECT * FROM {_read_json_auto_sql(f'[{", ".join(valid_paths)}]', columns_sql, filename=True)}",
                        )
                    except Exception as retry_err:
                        failed_paths.update(read_paths_s3)
                        yield {"type": "status", "message": f"Retry failed: {retry_err}"}
                        continue

                if local_to_s3:
                    path_map_rows = ", ".join(
                        f"('{escape_sql_literal(local)}', '{escape_sql_literal(s3)}')"
                        for local, s3 in local_to_s3.items()
                    )
                    mem_con.execute("DROP TABLE IF EXISTS _ingest_path_map")
                    mem_con.execute(
                        f"CREATE TEMP TABLE _ingest_path_map AS SELECT * FROM (VALUES {path_map_rows}) AS t(local, s3)"
                    )
                    mem_con.execute(
                        "UPDATE _ingest_staging SET filename = COALESCE("
                        "(SELECT s3 FROM _ingest_path_map WHERE local = _ingest_staging.filename), filename)"
                    )

                for fpath, rcount in mem_con.execute(
                    "SELECT filename, count(*) FROM _ingest_staging GROUP BY 1"
                ).fetchall():
                    count_map[fpath] = rcount

                field_selects = _build_column_select_exprs(src, log_fields_config, source_file_expr='"filename"')
                where_clauses, where_params = [], []
                if start_time:
                    where_clauses.append("timestamp >= ?::TIMESTAMPTZ")
                    where_params.append(start_time)
                if end_time:
                    where_clauses.append("timestamp <= ?::TIMESTAMPTZ")
                    where_params.append(end_time)
                where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

                mem_con.execute(
                    f"CREATE TEMP TABLE _ingest_typed AS SELECT {', '.join(field_selects)} FROM _ingest_staging{where_sql}",
                    where_params,
                )
                mem_con.execute("DROP TABLE _ingest_staging")
                mem_con.execute("ALTER TABLE _ingest_typed RENAME TO _ingest_staging")

                yield {
                    "type": "status",
                    "message": f"{elapsed()} Batch {chunk_num}: Exporting to PyArrow and writing buffer...",
                }

                arrow_table = _export_staging_arrow(mem_con)
                valid_rows = len(arrow_table)
                if valid_rows > 0:
                    touched_hours.update(
                        r[0]
                        for r in mem_con.execute(
                            "SELECT DISTINCT strftime(timestamp, '%Y-%m-%d-%H') FROM _ingest_staging WHERE timestamp IS NOT NULL"
                        ).fetchall()
                        if r[0] is not None
                    )

                _row = mem_con.execute("SELECT count(*) FROM _ingest_staging").fetchone()
                total_rows_batch = _row[0] if _row else 0
                corrupt_in_batch = total_rows_batch - valid_rows

                repairs_made = False
                _chunk_corrupt_s3_paths: list[str] = []
                _chunk_truly_corrupt: list = []
                if corrupt_in_batch > 0:
                    try:
                        corrupt_read_paths, corrupt_s3_paths = read_paths, read_paths_s3
                        if corrupt_read_paths:
                            paths_sql_str = ", ".join(f"'{escape_sql_literal(p)}'" for p in corrupt_read_paths)
                            q = (
                                f"SELECT filename, column0 FROM read_csv([{paths_sql_str}], header=false, sep='', "
                                "quote='', escape='', columns={'column0': 'VARCHAR'}, filename=true) "
                                "WHERE NOT json_valid(column0) OR json_extract(column0, '$.timestamp') IS NULL"
                            )
                            try:
                                bad_rows = _execute_query_with_retry(mem_con, q).fetchall()
                            except Exception as diagnostic_error:
                                logger.warning(
                                    "[sync] %s: DuckDB corrupt-line scan failed; scanning downloaded gzip files directly: %s",
                                    source_name, diagnostic_error,
                                )  # fmt: skip
                                bad_rows = _scan_invalid_gzip_lines(corrupt_read_paths)

                            repaired_by_fname: dict[str, list] = {}
                            truly_corrupt: list[tuple[str, str, str]] = []
                            for fname, raw_line in bad_rows:
                                fname = local_to_s3.get(fname, fname)
                                if raw_line is None:
                                    continue
                                repaired = _EMPTY_VALUE_RE.sub(":null", raw_line.strip())
                                try:
                                    parsed = json.loads(repaired)
                                    if parsed.get("timestamp"):
                                        repaired_by_fname.setdefault(fname, []).append(repaired)
                                        continue
                                    truly_corrupt.append((fname, raw_line, "missing_timestamp"))
                                except (json.JSONDecodeError, ValueError):
                                    truly_corrupt.append((fname, raw_line, "invalid_json"))

                            partial_paths.update(fname for fname, _raw_line, _reason in truly_corrupt)
                            if repaired_by_fname:
                                repairs_made = True
                                for fname, lines in repaired_by_fname.items():
                                    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
                                    try:
                                        for line in lines:
                                            tmp.write(line + "\n")
                                        tmp.flush()
                                        tmp_path = tmp.name
                                        tmp.close()
                                        safe_fname = escape_sql_literal(fname)
                                        mem_con.execute(
                                            f"INSERT INTO _ingest_staging BY NAME SELECT {', '.join(field_selects)} "
                                            f"FROM (SELECT *, '{safe_fname}' AS filename "
                                            f"FROM {_read_json_auto_sql(f'{tmp_path!r}', columns_sql)}){where_sql}",
                                            where_params,
                                        )
                                        corrupt_in_batch -= len(lines)
                                        valid_rows += len(lines)
                                    finally:
                                        try:
                                            os.unlink(tmp_path)
                                        except OSError:
                                            pass

                            if len(total_corrupt_details) < 100:
                                for fname, raw_line, _reason in truly_corrupt[: 100 - len(total_corrupt_details)]:
                                    short_name = fname.split("?")[0].split("/")[-1]
                                    total_corrupt_details.append(f"[{short_name}] {raw_line.strip()[:2000]}")
                            _chunk_corrupt_s3_paths, _chunk_truly_corrupt = corrupt_s3_paths, truly_corrupt
                    except Exception as e:
                        err_str = str(e)
                        if any(
                            kw in err_str.lower()
                            for kw in (
                                "could not resolve hostname",
                                "connection refused",
                                "ssl",
                                "http",
                                "no such file",
                            )
                        ):
                            logger.warning(
                                "[sync] Local re-read failed during corrupt-line extraction. Rolling back partial data for %d files to allow retry: %s",
                                len(corrupt_s3_paths), err_str,
                            )  # fmt: skip
                            if corrupt_s3_paths:
                                placeholders = ", ".join("?" * len(corrupt_s3_paths))
                                mem_con.execute(
                                    f"DELETE FROM _ingest_staging WHERE _source_file IN ({placeholders})",
                                    corrupt_s3_paths,
                                )
                                failed_paths.update(corrupt_s3_paths)
                                repairs_made = True
                                _valid_row = mem_con.execute(
                                    "SELECT count(*) FROM _ingest_staging WHERE timestamp IS NOT NULL"
                                ).fetchone()
                                valid_rows = _valid_row[0] if _valid_row else 0
                                _corrupt_row = mem_con.execute(
                                    "SELECT count(*) FROM _ingest_staging WHERE timestamp IS NULL"
                                ).fetchone()
                                corrupt_in_batch = _corrupt_row[0] if _corrupt_row else 0
                        else:
                            total_corrupt_details.append(f"[Error extracting lines: {e}]")

                total_corrupt += corrupt_in_batch

                for corrupt_path in _chunk_corrupt_s3_paths:
                    local_file = s3_to_local.get(corrupt_path)
                    if local_file is None:
                        quarantine_capture_failures += 1
                        capture_failed_paths.add(corrupt_path)
                        continue
                    object_key = corrupt_path.removeprefix(f"s3://{src['bucket']}/")
                    corrupt_read_expr = _read_json_auto_sql(f"'{escape_sql_literal(local_file)}'", columns_sql)
                    try:
                        quarantine_result = _quarantine_convert_corrupt_lines(
                            mem_con, fos_client, src, corrupt_read_expr, local_file, object_key
                        )
                    except Exception:
                        logger.exception("[ingest] %s: exact-byte quarantine failed for %s", source_name, object_key)
                        quarantine_result = {
                            "malformed_records": sum(
                                1 for fname, _line, _reason in _chunk_truly_corrupt if fname == corrupt_path
                            ),
                            "cap_evictions": 0,
                            "quarantine_capture_failures": 1,
                        }
                    outcome_counters["malformed_records"] += quarantine_result["malformed_records"]
                    cap_evictions += quarantine_result["cap_evictions"]
                    quarantine_capture_failures += quarantine_result["quarantine_capture_failures"]
                    if quarantine_result["quarantine_capture_failures"]:
                        capture_failed_paths.add(corrupt_path)
                    if quarantine_result["malformed_records"]:
                        partial_paths.add(corrupt_path)
                        total_quarantined += 1
            finally:
                chunk_tmpdir_obj.cleanup()

            good_files = [f for f in chunk if f not in failed_paths]
            try:
                durable_files = {
                    r[0]
                    for r in mem_con.execute(
                        "SELECT DISTINCT _source_file FROM _ingest_staging WHERE timestamp IS NOT NULL"
                    ).fetchall()
                    if r[0] is not None
                }
            except Exception:
                durable_files = set()
            rows_to_track = [
                (f, count_map.get(f, 0) if f in durable_files else 0, file_sizes.get(f)) for f in good_files
            ]

            if valid_rows > 0:
                if repairs_made:
                    arrow_table = _export_staging_arrow(mem_con)
                buf_filename = _deterministic_buffer_name(good_files)
                if rows_to_track:
                    metadata_db.record_in_flight(source_name, buf_filename, rows_to_track)
                iceberg.write_to_buffer(src, arrow_table, buf_filename)
                if rows_to_track:
                    metadata_db.insert_ingested_files(source_name, rows_to_track)
                    metadata_db.clear_in_flight(source_name, buf_filename)
            elif rows_to_track:
                metadata_db.insert_ingested_files(source_name, rows_to_track)
            successfully_processed_files.extend(good_files)

            if delete_after:
                remaining_deletes = []
                for future, keys in _pending_deletes:
                    if future.done():
                        _record_delete_result(future, keys)
                    else:
                        remaining_deletes.append((future, keys))
                _pending_deletes = remaining_deletes

                s3_pfx = f"s3://{src['bucket']}/"
                chunk_keys = [f[len(s3_pfx) :] for f in chunk if f.startswith(s3_pfx) and f not in failed_paths]
                if chunk_keys:
                    try:
                        future = _delete_executor.submit(
                            _delete_objects_robust_with_failures, fos_client, src["bucket"], chunk_keys
                        )
                        _pending_deletes.append((future, set(chunk_keys)))
                        yield {
                            "type": "status",
                            "message": f"{elapsed()} Batch {chunk_num}: Submitted deletion of {len(chunk_keys)} raw files (async)...",
                        }
                    except RuntimeError as submit_err:
                        logger.warning(
                            "[ingest] %s: delete executor submit failed (%s) — falling back to inline delete",
                            source_name, submit_err,
                        )  # fmt: skip
                        try:
                            inline_deleted, inline_failures = _delete_objects_robust_with_failures(
                                fos_client, src["bucket"], chunk_keys
                            )
                            deleted += inline_deleted
                            delete_failed_paths.update(f"{s3_pfx}{key}" for key in inline_failures)
                            yield {
                                "type": "status",
                                "message": f"{elapsed()} Batch {chunk_num}: Deleted {inline_deleted} raw files inline (executor unavailable).",
                            }
                        except Exception as inline_err:
                            delete_failed_paths.update(f"{s3_pfx}{key}" for key in chunk_keys)
                            logger.error(
                                "[ingest] %s: inline delete also failed for batch %d (%d files): %s",
                                source_name, chunk_num, len(chunk_keys), inline_err,
                            )  # fmt: skip
                            yield {
                                "type": "status",
                                "message": f"{elapsed()} Batch {chunk_num}: WARN — failed to delete {len(chunk_keys)} raw files: {inline_err}",
                            }

            total_inserted += valid_rows
            for file_path in chunk:
                processed_count += 1
                yield {
                    "type": "file_done",
                    "current": processed_count,
                    "total": len(new_files),
                    "file_name": file_path.split("/")[-1],
                    "row_count": count_map.get(file_path, 0),
                    "total_inserted": total_inserted,
                    "total_corrupt": total_corrupt,
                }
    finally:
        pending_by_future = {future: keys for future, keys in _pending_deletes}
        try:
            for f in concurrent.futures.as_completed(pending_by_future, timeout=300):
                _record_delete_result(f, pending_by_future[f])
        except concurrent.futures.TimeoutError:
            for future, keys in _pending_deletes:
                if not future.done():
                    delete_failed_paths.update(f"s3://{src['bucket']}/{key}" for key in keys)
            logger.warning(
                "[ingest] %s: timed out waiting for all async deletions to complete. Some files may still be deleting in the background.",
                source_name,
            )

        _delete_executor.shutdown(wait=False)
        try:
            del arrow_table
        except NameError:
            pass
        if mem_con:
            try:
                mem_con.close()
            except Exception:
                logger.debug("ingest: mem_con.close failed (ignored)", exc_info=True)

    total_deleted = deleted + reclaimed
    outcome_counters.update(
        {
            "valid_records": total_inserted,
            "corrupt_containers": corrupt_containers,
            "quarantine_capture_failures": quarantine_capture_failures,
            "source_delete_failures": len(delete_failed_paths),
            "cap_evictions": cap_evictions,
        }
    )
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
    reclaimed_note = f" (incl. {reclaimed} reclaimed from an interrupted prior run)" if reclaimed else ""
    yield {
        "type": "done",
        "new_files": processed_count,
        "skipped_files": skipped_already,
        "rows_inserted": total_inserted,
        "corrupt_rows": total_corrupt,
        "corrupt_details": total_corrupt_details,
        "deleted_files": total_deleted,
        "quarantined_files": total_quarantined,
        "outcome_counters": outcome_counters,
        "message": (
            f"Successfully ingested {processed_count} new files ({total_inserted} rows) "
            f"and deleted {total_deleted} raw files{reclaimed_note}."
        ),
        "touched_hours": list(touched_hours),
    }


# ─── Celery ingest data plane (v3) ────────────────────────────────────────────

from backend.celery_app import app

LEDGER_RECLAIM_AFTER_S = 20 * 60
LEDGER_REDISPATCH_AFTER_S = 10 * 60
LEDGER_MAX_ATTEMPTS = 5
LEDGER_RETRY_BASE_S = 30
LEDGER_RETRY_MAX_S = 60 * 60
LEDGER_RETRY_JITTER_RATIO = 0.25
LEDGER_CONVERT_BATCH_SIZE = int(os.getenv("LEDGER_CONVERT_BATCH_SIZE", "50"))
_LEDGER_KEY_SLICE = 400


def _iter_key_slices(keys: list[str]):
    """Yield ``(key_slice, placeholders)`` in ``_LEDGER_KEY_SLICE``-sized chunks."""
    for key_slice in itertools.batched(keys, _LEDGER_KEY_SLICE):
        yield list(key_slice), ",".join("?" * len(key_slice))


def _celery_ingest_scope(kind: str, service_id: str):
    """Telemetry envelope for ledger tasks: process-context tagging, FOS call tracking, and usage-log flush."""
    from contextlib import contextmanager

    from backend.utils.telemetry import flush_usage_log, process_context_scope, start_call_tracking

    @contextmanager
    def _scope():
        with process_context_scope(f"cron:{kind}:{service_id}"):
            start_call_tracking()
            try:
                yield
            finally:
                flush_usage_log(service_id)

    return _scope()


def _discover_prefix_common(
    service_id: str,
    *,
    list_kwargs: dict[str, Any],
    prefix_subpath: str | None,
    dispatch_fn: Callable[[str, list[str]], object],
    originating_task: str,
    run_id: int | None,
    log_label: str = "",
) -> int:
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return 0
    src = get_source_for_service(service_id)
    if src is None:
        return 0

    fos_result = _consume_generator(list_fos_files(src, **list_kwargs))
    if not fos_result:
        logger.warning("[ledger] %s: %sFOS LIST returned nothing (prefix=%s)", service_id, log_label, prefix_subpath)
        return 0
    new_files = fos_result.get("new_files") or []
    if not new_files:
        return 0

    bucket = src.get("bucket", "")
    s3_prefix = f"s3://{bucket}/"

    con = get_con(service_id)
    cur = con.cursor()
    newly_discovered: list[str] = []
    file_sizes = fos_result.get("file_sizes", {})
    now_ts = time.time()
    orig_task = originating_task if run_id is not None else None
    for chunk_files in itertools.batched(new_files, _LEDGER_KEY_SLICE):
        values_sql = ", ".join(["(?, ?, ?, 'discovered', ?, ?, ?)"] * len(chunk_files))
        params: list[Any] = []
        for f_path in chunk_files:
            object_key = f_path[len(s3_prefix) :] if f_path.startswith(s3_prefix) else f_path
            params.extend((service_id, object_key, file_sizes.get(f_path, 0), now_ts, orig_task, run_id))
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, size_bytes, status, discovered_at, originating_task, originating_run_id) "
            f"VALUES {values_sql} ON CONFLICT DO NOTHING RETURNING object_key",
            tuple(params),
        )
        newly_discovered.extend(r[0] for r in cur.fetchall())
    con.commit()

    batches = 0
    for batch in itertools.batched(newly_discovered, LEDGER_CONVERT_BATCH_SIZE):
        dispatch_fn(service_id, list(batch))
        batches += 1
    if newly_discovered:
        logger.info(
            "[ledger] %s: discovered %d new %sfile(s), dispatched %d convert batch(es)",
            service_id,
            len(newly_discovered),
            log_label,
            batches,
        )
    return len(newly_discovered)


def discover_prefix(
    service_id: str,
    prefix_subpath: str | None = None,
    start_time: str | None = None,
    run_id: int | None = None,
    originating_task: str = "log_discovery",
) -> int:
    """Discover new request-log keys in FOS, insert into ``ingest_ledger``, and dispatch convert batches."""
    list_kwargs: dict[str, Any] = {"start_time": start_time}
    if prefix_subpath is not None:
        list_kwargs["prefix_subpath"] = prefix_subpath
    return _discover_prefix_common(
        service_id,
        list_kwargs=list_kwargs,
        prefix_subpath=prefix_subpath,
        dispatch_fn=convert_batch_files.delay,
        originating_task=originating_task,
        run_id=run_id,
    )


def _ledger_record_failure(
    con,
    service_id: str,
    object_key: str,
    error: str,
    expected_generation: int | None = None,
    error_kind: str = "transient",
    *,
    commit: bool = True,
) -> str:
    """Record a convert failure and either requeue ('discovered') or quarantine after LEDGER_MAX_ATTEMPTS."""
    cur = con.cursor()
    row = con.execute(
        "SELECT attempts, status, lease_generation FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    if row is None:
        return "unknown"
    if expected_generation is not None and int(row[2] or 0) != expected_generation:
        return "stale"
    next_attempts = int(row[0] or 0) + 1
    terminal = next_attempts >= LEDGER_MAX_ATTEMPTS
    next_attempt_at = None
    if not terminal:
        delay = min(LEDGER_RETRY_MAX_S, LEDGER_RETRY_BASE_S * (2 ** max(0, next_attempts - 1)))
        next_attempt_at = time.time() + delay + random.uniform(0, delay * LEDGER_RETRY_JITTER_RATIO)
    generation_clause = " AND status = 'claimed' AND lease_generation = ?" if expected_generation is not None else ""
    params: tuple = (error[:500], error_kind, next_attempt_at, LEDGER_MAX_ATTEMPTS, service_id, object_key)
    if expected_generation is not None:
        params += (expected_generation,)
    cur.execute(
        "UPDATE ingest_ledger SET attempts = attempts + 1, last_error = ?, error_kind=?, next_attempt_at=?, "
        "status = CASE WHEN attempts + 1 >= ? THEN 'quarantined' ELSE 'discovered' END, "
        "claimed_by = NULL, claimed_at = NULL WHERE service_id = ? AND object_key = ?" + generation_clause,
        params,
    )
    if cur.rowcount == 0 and expected_generation is not None:
        return "stale"
    if commit:
        con.commit()
    if terminal:
        logger.error(
            "[ledger] %s: %s QUARANTINED after %s failed convert attempts: %s",
            service_id,
            object_key,
            next_attempts,
            error,
        )
        return "quarantined"
    logger.warning(
        "[ledger] %s: convert failed for %s (attempt %s; retry at %.3f): %s",
        service_id,
        object_key,
        next_attempts,
        next_attempt_at or 0,
        error,
    )
    return "discovered"


def _ledger_mark_committed_batch(
    con, service_id: str, object_keys: list[str], claimed_generations: dict[str, int]
) -> set[str]:
    """Mark claimed ledger rows as committed grouped by lease generation."""
    if not object_keys:
        return set()
    cur = con.cursor()
    now = time.time()
    by_gen: dict[int, list[str]] = defaultdict(list)
    for k in object_keys:
        by_gen[claimed_generations[k]].append(k)
    committed_keys: set[str] = set()
    for gen, keys in by_gen.items():
        for key_slice, placeholders in _iter_key_slices(keys):
            cur.execute(
                "UPDATE ingest_ledger SET status='committed', committed_at=? "
                f"WHERE service_id=? AND status='claimed' AND lease_generation=? AND object_key IN ({placeholders}) "
                "RETURNING object_key",
                (now, service_id, gen, *key_slice),
            )
            committed_keys.update(r[0] for r in cur.fetchall())
    con.commit()
    return committed_keys


def _is_permanent_s3_missing(fos: Any, bucket: str, object_key: str) -> bool:
    """Return True if head_object confirms the object is permanently missing (404/NoSuchKey)."""
    try:
        fos.head_object(Bucket=bucket, Key=object_key)
    except Exception as head_err:
        err = str(head_err)
        return "404" in err or "Not Found" in err or "NoSuchKey" in err
    return False


def _ledger_mark_dead_letter_missing(
    con_meta: Any, service_id: str, object_key: str, lease_generation: int | None
) -> int:
    """Mark a claimed ledger row as dead_letter when the object is missing from FOS."""
    cur = con_meta.cursor()
    cur.execute(
        "UPDATE ingest_ledger SET status='dead_letter', "
        "last_error='object missing from FOS (already ingested+deleted, or expired)', "
        "error_kind='object_missing', next_attempt_at=NULL "
        "WHERE service_id=? AND object_key=? AND status='claimed' AND lease_generation=?",
        (service_id, object_key, lease_generation),
    )
    con_meta.commit()
    return int(cur.rowcount or 0)


def _ledger_claim_batch(con, service_id: str, object_keys: list[str], worker_id: str) -> dict[str, int]:
    """Claim every still-``discovered`` key in ``object_keys`` and return ``{key: lease_generation}``."""
    cur = con.cursor()
    now = time.time()
    won: dict[str, int] = {}
    for key_slice, placeholders in _iter_key_slices(object_keys):
        cur.execute(
            "UPDATE ingest_ledger SET status='claimed', claimed_by=?, claimed_at=?, next_attempt_at=NULL, "
            "lease_generation=lease_generation + 1 "
            f"WHERE service_id=? AND object_key IN ({placeholders}) AND status='discovered' "
            "RETURNING object_key, lease_generation",
            (worker_id, now, service_id, *key_slice),
        )
        won.update({r[0]: int(r[1]) for r in cur.fetchall()})
    con.commit()
    return won


def _claim_and_resolve_batch_source(
    service_id: str, object_keys: list[str], worker_id: str
) -> tuple[dict[str, int], Any, dict[str, Any] | None, dict[str, int], list[str]]:
    """Initialize batch summary, claim keys in ``ingest_ledger``, and resolve the service source."""
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    summary: dict[str, int] = {
        "requested": len(object_keys),
        "claimed": 0,
        "not_claimed": 0,
        "committed": 0,
        "dead_letter": 0,
        "failed": 0,
    }
    if not object_keys:
        return summary, None, None, {}, []
    con_meta = get_con(service_id)
    claimed_generations = _ledger_claim_batch(con_meta, service_id, object_keys, worker_id)
    claimed_keys = list(claimed_generations)
    summary["claimed"] = len(claimed_generations)
    summary["not_claimed"] = len(object_keys) - len(claimed_generations)
    if not claimed_generations:
        return summary, con_meta, None, claimed_generations, []

    src = get_source_for_service(service_id)
    if src is None:
        for object_key in claimed_keys:
            _ledger_record_failure(
                con_meta, service_id, object_key, "no source registered for service", claimed_generations[object_key]
            )
        summary["failed"] = len(claimed_keys)
        return summary, con_meta, None, claimed_generations, []
    return summary, con_meta, src, claimed_generations, claimed_keys


def _download_and_classify_batch_files(
    fos: Any,
    con_meta: Any,
    service_id: str,
    bucket: str,
    claimed_keys: list[str],
    claimed_generations: dict[str, int],
    tmpdir: str,
    active: list[str],
    summary: dict[str, int],
    batch_outcomes: dict[str, dict[str, int]],
    log_label: str = "",
) -> list[tuple[str, str, str]]:
    """Download claimed keys to ``tmpdir`` and classify missing/transient failures."""
    s3_to_local, _ = _download_chunk_to_local(fos, [f"s3://{bucket}/{k}" for k in claimed_keys], tmpdir)
    files: list[tuple[str, str, str]] = []
    for object_key in claimed_keys:
        s3_path = f"s3://{bucket}/{object_key}"
        local_file = s3_to_local.get(s3_path)
        if local_file:
            files.append((local_file, s3_path, object_key))
            continue
        active.remove(object_key)
        if _is_permanent_s3_missing(fos, bucket, object_key):
            if _ledger_mark_dead_letter_missing(con_meta, service_id, object_key, claimed_generations[object_key]):
                summary["dead_letter"] += 1
                batch_outcomes[object_key] = _failed_object_outcome()
            logger.info("[ledger] %s: %s%s gone from FOS — dead_letter", service_id, log_label, object_key)
        else:
            _ledger_record_failure(
                con_meta,
                service_id,
                object_key,
                "download failed (object still exists — transient)",
                claimed_generations[object_key],
            )
            summary["failed"] += 1
            batch_outcomes[object_key] = _failed_object_outcome()
    return files


def _handle_batch_convert_exception(
    con_meta: Any,
    service_id: str,
    active: list[str],
    claimed_generations: dict[str, int],
    batch_outcomes: dict[str, dict[str, int]],
    summary: dict[str, int],
    exc: Exception,
) -> dict[str, int]:
    """Record retryable failure on all still-active keys when a batch transaction fails."""
    for object_key in active:
        _ledger_record_failure(
            con_meta, service_id, object_key, str(exc), claimed_generations[object_key], commit=False
        )
        batch_outcomes[object_key] = _failed_object_outcome()
    con_meta.commit()
    _record_ledger_batch_outcomes(service_id, batch_outcomes)
    summary["failed"] += len(active)
    return summary


def _cleanup_batch_resources(
    admission_cm: Any, admission_entered: bool, duckdb_con: Any, service_id: str, caller_name: str
) -> None:
    """Release DuckLake write admission lock and close in-memory DuckDB connection."""
    from backend.core.iceberg._ducklake import _ducklake_detach

    if admission_entered:
        admission_cm.__exit__(None, None, None)
    if duckdb_con is not None:
        try:
            _ducklake_detach(duckdb_con, service_id=service_id)
        except Exception:
            logger.debug("%s: _ducklake_detach failed (ignored)", caller_name, exc_info=True)
        duckdb_con.close()


def _finalize_batch_convert(
    con_meta: Any,
    service_id: str,
    active: list[str],
    claimed_generations: dict[str, int],
    outcomes: dict[str, dict[str, int]],
    batch_outcomes: dict[str, dict[str, int]],
    summary: dict[str, int],
) -> dict[str, int]:
    """Mark active keys committed in ``ingest_ledger`` and record batch outcomes."""
    committed_keys = _ledger_mark_committed_batch(con_meta, service_id, active, claimed_generations)
    for object_key in active:
        if object_key in committed_keys:
            summary["committed"] += 1
            batch_outcomes[object_key] = outcomes[object_key]
        else:
            batch_outcomes[object_key] = _mark_object_failed(outcomes[object_key])
    _record_ledger_batch_outcomes(service_id, batch_outcomes)
    return summary


def _run_batch_convert_common(
    service_id: str,
    object_keys: list[str],
    worker_id: str,
    *,
    cleanup_label: str,
    log_label: str = "",
    process_batch_fn: Any,
) -> dict[str, int]:
    """Shared claim -> download -> process -> finalize scaffolding for request and RUM batch conversion."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_memory_connection
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_write_admission

    summary, con_meta, src, claimed_generations, claimed_keys = _claim_and_resolve_batch_source(
        service_id, object_keys, worker_id
    )
    if not claimed_keys or src is None:
        return summary

    bucket = src.get("bucket", "")
    active: list[str] = list(claimed_keys)
    outcomes: dict[str, dict[str, int]] = {}
    batch_outcomes: dict[str, dict[str, int]] = {}
    duckdb_con = None
    admission_cm = ducklake_write_admission(service_id)
    admission_entered = False

    def _enter_write_admission() -> None:
        nonlocal admission_entered
        admission_cm.__enter__()
        admission_entered = True
        if not _ducklake_attach(duckdb_con, src, read_only=False):
            raise RuntimeError("DuckLake read-write attach failed")

    try:
        fos = boto3_client_hot() if svcconfig.HOT_S3_ENDPOINT else _get_fos_client(src)
        duckdb_con = get_memory_connection(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            files = _download_and_classify_batch_files(
                fos,
                con_meta,
                service_id,
                bucket,
                claimed_keys,
                claimed_generations,
                tmpdir,
                active,
                summary,
                batch_outcomes,
                log_label=log_label,
            )
            if not files:
                _record_ledger_batch_outcomes(service_id, batch_outcomes)
                return summary
            process_batch_fn(
                src=src,
                fos=fos,
                duckdb_con=duckdb_con,
                files=files,
                outcomes=outcomes,
                enter_write_admission=_enter_write_admission,
            )
    except Exception as e:
        return _handle_batch_convert_exception(
            con_meta, service_id, active, claimed_generations, batch_outcomes, summary, e
        )
    finally:
        _cleanup_batch_resources(admission_cm, admission_entered, duckdb_con, service_id, cleanup_label)

    return _finalize_batch_convert(con_meta, service_id, active, claimed_generations, outcomes, batch_outcomes, summary)


def convert_batch_objects(service_id: str, object_keys: list[str], worker_id: str) -> dict:
    """Claim N ledger rows, download them, and land every row in one DuckLake transaction."""
    from backend import config as svcconfig
    from backend.core.iceberg._ducklake import ducklake_table_name

    def _process_request_batch(*, src, fos, duckdb_con, files, outcomes, enter_write_admission) -> None:
        table = ducklake_table_name(src)
        enter_write_admission()
        log_fields_config = (svcconfig.load_config(service_id) or {}).get("log_fields")
        columns_sql = get_ingest_columns_sql(log_fields_config)
        local_files = [lf for lf, _s3, _k in files]
        file_list_sql = "[" + ", ".join(f"'{escape_sql_literal(lf)}'" for lf in local_files) + "]"
        read_expr = _read_json_auto_sql(file_list_sql, columns_sql, filename=True)

        duckdb_con.execute("DROP TABLE IF EXISTS temp.srcmap")
        duckdb_con.execute("CREATE TEMP TABLE srcmap(local VARCHAR, s3 VARCHAR)")
        duckdb_con.execute(
            "INSERT INTO temp.srcmap VALUES "
            + ", ".join(f"('{escape_sql_literal(lf)}', '{escape_sql_literal(s3)}')" for lf, s3, _k in files)
        )

        stats = duckdb_con.execute(
            "SELECT filename, count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) "
            f"FROM {read_expr} GROUP BY filename"
        ).fetchall()
        valid_by_local = {r[0]: r[1] for r in stats}
        corrupt_by_local = {r[0]: r[2] for r in stats}
        unmapped = set(valid_by_local) - set(local_files)
        if unmapped:
            raise RuntimeError(f"read_json_auto returned unmapped filename(s): {sorted(unmapped)[:3]}")

        clean_batch_expr = (
            f"(SELECT {', '.join(_build_column_select_exprs(src, log_fields_config))}, filename "
            f"FROM {read_expr} WHERE timestamp IS NOT NULL)"
        )
        select_sql = (
            "SELECT r.* EXCLUDE (filename), m.s3 AS _source_file "
            f"FROM {clean_batch_expr} r JOIN temp.srcmap m ON m.local = r.filename"
        )

        needs_insert = True
        if not _ducklake_table_exists(duckdb_con, table):
            try:
                duckdb_con.execute(f"CREATE TABLE lake.{table} AS {select_sql}")
                needs_insert = False
            except Exception as e:
                if "already exists" not in str(e):
                    raise

        if needs_insert:
            try:
                existing_cols = {r[0] for r in duckdb_con.execute(f"DESCRIBE lake.{table}").fetchall()}
                for p_col, p_type, *_ in duckdb_con.execute(f"DESCRIBE SELECT * FROM {read_expr} LIMIT 0").fetchall():
                    if p_col != "filename" and p_col not in existing_cols:
                        duckdb_con.execute(
                            f'ALTER TABLE lake.{table} ADD COLUMN "{p_col.replace(chr(34), chr(34) * 2)}" {p_type}'
                        )
            except Exception as e:
                logger.warning("[ledger] %s: batch schema sync failed (%d file(s)): %s", service_id, len(files), e)
            _ducklake_replace_source_rows(
                duckdb_con, table, "SELECT s3 FROM temp.srcmap", select_sql, "convert_batch_objects"
            )

        file_sizes_by_local = {local_file: os.path.getsize(local_file) for local_file, _s3_path, _k in files}
        for local_file, _s3_path, object_key in files:
            outcome = _new_object_outcome()
            outcome["valid_records"] = int(valid_by_local.get(local_file, 0))
            try:
                if corrupt_by_local.get(local_file):
                    single_read_expr = _read_json_auto_sql(f"'{escape_sql_literal(local_file)}'", columns_sql)
                    outcome.update(
                        _quarantine_convert_corrupt_lines(
                            duckdb_con, fos, src, single_read_expr, local_file, object_key
                        )
                    )
            except Exception:
                logger.exception("[ledger] %s: quarantine check failed for %s", service_id, object_key)
                outcome["quarantine_capture_failures"] = max(1, outcome["quarantine_capture_failures"])
            outcomes[object_key] = _finalize_object_outcome(outcome)

        try:
            metadata_db.insert_ingested_files(
                service_id,
                [
                    (object_key, valid_by_local.get(local_file, 0), file_sizes_by_local[local_file])
                    for local_file, _s3_path, object_key in files
                ],
            )
        except Exception as ie:
            logger.warning(
                "[ledger] %s: ingested_files bookkeeping failed for %d file(s): %s", service_id, len(files), ie
            )

    return _run_batch_convert_common(
        service_id,
        object_keys,
        worker_id,
        cleanup_label="convert_batch_objects",
        process_batch_fn=_process_request_batch,
    )


def _quarantine_convert_corrupt_lines(
    duckdb_con,
    fos_client,
    src: dict,
    read_expr: str,
    local_file: str,
    object_key: str,
) -> dict[str, int]:
    """Capture malformed request lines as exact-byte local evidence items."""
    from backend.core.quarantine import capture_evidence

    valid_count, corrupt_count = duckdb_con.execute(
        f"SELECT count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) FROM {read_expr}"
    ).fetchone()
    if not corrupt_count:
        return {
            "valid_records": int(valid_count or 0),
            "malformed_records": 0,
            "cap_evictions": 0,
            "quarantine_capture_failures": 0,
        }

    service_id = src.get("service_id") or src.get("name", "")
    prefix_path = src.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/raw/request/" if prefix_path else "raw/request/"
    if not object_key.startswith(raw_prefix):
        logger.warning(
            "[ledger] %s: refusing request quarantine for key outside raw prefix: %s", service_id, object_key
        )
        return {
            "valid_records": int(valid_count or 0),
            "malformed_records": int(corrupt_count or 0),
            "cap_evictions": 0,
            "quarantine_capture_failures": int(corrupt_count or 0),
        }

    open_file = gzip.open if local_file.endswith(".gz") else open
    evidence_results = []
    offset = 0
    try:
        with open_file(local_file, "rb") as raw:
            for ordinal, line in enumerate(raw, start=1):
                current_offset = offset
                offset += len(line)
                stripped = line.strip()
                if not stripped:
                    continue
                category = None
                error_text = ""
                try:
                    parsed = json.loads(stripped)
                    if not isinstance(parsed, dict) or not parsed.get("timestamp"):
                        category, error_text = "missing_timestamp", "record has no timestamp"
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                    category, error_text = "invalid_json", str(exc)[:2000]
                if category is not None:
                    evidence_results.append(
                        capture_evidence(
                            service_id,
                            "request",
                            object_key,
                            line,
                            line_ordinal=ordinal,
                            byte_offset=current_offset,
                            error_category=category,
                            error_text=error_text,
                        )
                    )
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        logger.warning("[ledger] %s: malformed request gzip %s: %s", service_id, object_key, exc)
        return _capture_corrupt_container(service_id, "request", object_key, local_file, exc, full_outcome=False)

    if len(evidence_results) != int(corrupt_count or 0):
        logger.error(
            "[ledger] %s: DuckDB/raw-line corruption count mismatch for %s (%d vs %d)",
            service_id,
            object_key,
            corrupt_count,
            len(evidence_results),
        )
        return {
            "valid_records": int(valid_count or 0),
            "malformed_records": len(evidence_results),
            **_sum_evidence_counts(evidence_results, extra_failures=1),
        }

    for result in evidence_results:
        if int(result.get("quarantine_capture_failures", 0)):
            logger.error("[ledger] %s: request quarantine capture failed for %s", service_id, object_key)
    return {
        "valid_records": int(valid_count or 0),
        "malformed_records": len(evidence_results),
        **_sum_evidence_counts(evidence_results),
    }


def boto3_client_hot():
    """S3 client for the hot-tier endpoint override (HOT_S3_*)."""
    import boto3

    from backend import config as svcconfig

    return boto3.client(
        "s3",
        endpoint_url=svcconfig.HOT_S3_ENDPOINT,
        aws_access_key_id=svcconfig.HOT_S3_KEY,
        aws_secret_access_key=svcconfig.HOT_S3_SECRET,
    )


def _redispatch_pending(
    service_id: str,
    reclaimed: list[str],
    stuck: list[str],
    dispatch_fn: Callable[[str, list[str]], object],
    label: str,
    queue_depths_fn: Callable[[], tuple[dict[str, int], bool]] | None = None,
) -> tuple[int, bool]:
    """Re-dispatch reclaimed + stuck ledger keys in batches when queue depth indicates lost messages."""
    pending = len(stuck) + len(reclaimed)
    if not pending:
        return 0, True
    pending_batches = math.ceil(pending / LEDGER_CONVERT_BATCH_SIZE)
    broker_ok = False
    try:
        import sys as _sys

        from backend import celery_app as _cq

        if queue_depths_fn is not None:
            depth_fn = queue_depths_fn
        else:
            cs_mod = _sys.modules.get("backend.celery_status")
            cs_fn = getattr(cs_mod, "celery_queue_depths", None) if cs_mod is not None else None
            depth_fn = cs_fn if cs_fn is not None and cs_fn is not _cq.celery_queue_depths else _cq.celery_queue_depths
        queues, broker_ok = depth_fn()
        queue_depth = queues.get("q.ingest", 0) if broker_ok else 0
    except Exception:
        queue_depth = 0
    if not broker_ok:
        logger.warning(
            "[ledger] %s: %sq.ingest depth unavailable; skipping redispatch of %d pending object(s)",
            service_id,
            label,
            pending,
        )
        return 0, False
    if queue_depth >= pending_batches:
        logger.info(
            "[ledger] %s: %d %spending object(s) require %d batch message(s), "
            "but q.ingest already holds %d message(s) — skipping re-dispatch",
            service_id,
            pending,
            label,
            pending_batches,
            queue_depth,
        )
        return 0, True
    for batch in itertools.batched(reclaimed + stuck, LEDGER_CONVERT_BATCH_SIZE):
        dispatch_fn(service_id, list(batch))
    logger.info(
        "[ledger] %s: %ssweeper re-dispatched %d pending row(s) (batches=%d, reclaimed=%d, queue_depth=%d)",
        service_id,
        label,
        pending,
        pending_batches,
        len(reclaimed),
        queue_depth,
    )
    return pending, True


def _sweep_ledger_common(
    service_id: str, lookback_hours: int = 4, run_id: int | None = None, *, is_rum: bool = False
) -> dict:
    """Shared implementation for ``sweep_ledger_once`` and ``sweep_rum_ledger_once``."""
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    cur = con.cursor()
    now = time.time()

    src = get_source_for_service(service_id)
    prefix_path = (src.get("prefix", "") if src else "").strip("/")
    rum_like_pattern = f"{prefix_path}/raw/rum/%" if prefix_path else "raw/rum/%"
    like_op = "LIKE" if is_rum else "NOT LIKE"

    cur.execute(
        "UPDATE ingest_ledger SET status='discovered', claimed_by=NULL, claimed_at=NULL, next_attempt_at=? "
        f"WHERE service_id=? AND status='claimed' AND claimed_at < ? AND object_key {like_op} ? "
        "RETURNING object_key",
        (now, service_id, now - LEDGER_RECLAIM_AFTER_S, rum_like_pattern),
    )
    reclaimed = [r[0] for r in cur.fetchall()]
    con.commit()

    stale_cutoff = now - LEDGER_REDISPATCH_AFTER_S
    reclaimed_set = set(reclaimed)
    stuck = [
        r[0]
        for r in con.execute(
            "SELECT object_key FROM ingest_ledger "
            f"WHERE service_id=? AND status='discovered' AND object_key {like_op} ? "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "AND (claimed_at IS NULL OR claimed_at < ?) LIMIT 5000",
            (service_id, rum_like_pattern, now, stale_cutoff),
        ).fetchall()
        if r[0] not in reclaimed_set
    ]

    dispatch_fn = convert_batch_rum_files.delay if is_rum else convert_batch_files.delay
    label = "RUM " if is_rum else ""
    redispatched, broker_ok = _redispatch_pending(service_id, reclaimed, stuck, dispatch_fn, label)

    st = (datetime.now(UTC) - timedelta(hours=lookback_hours)).isoformat()
    discover_fn = discover_rum_prefix if is_rum else discover_prefix
    discovered = discover_fn(
        service_id, start_time=st, run_id=run_id, originating_task="ledger_rum_sweep" if is_rum else "ledger_sweep"
    )

    dead_letter_count = 0
    try:
        dead_letter_row = con.execute(
            "SELECT COUNT(*) FROM ingest_ledger WHERE service_id=? AND status IN ('quarantined', 'dead_letter') "
            f"AND object_key {like_op} ?",
            (service_id, rum_like_pattern),
        ).fetchone()
        dead_letter_count = int(dead_letter_row[0]) if dead_letter_row else 0
    except Exception:
        dead_letter_count = 0

    return {
        "reclaimed": len(reclaimed),
        "redispatched": redispatched,
        "discovered": discovered,
        "broker_ok": broker_ok,
        "dead_letter": dead_letter_count,
    }


def sweep_ledger_once(service_id: str, lookback_hours: int = 4, run_id: int | None = None) -> dict:
    """Reclaim stale claims, re-dispatch stuck request-log keys, and run lookback discovery."""
    return _sweep_ledger_common(service_id, lookback_hours, run_id, is_rum=False)


# ── RUM ledger pipeline ──────────────────────────────────────────────────────


def discover_rum_prefix(
    service_id: str,
    prefix_subpath: str | None = None,
    start_time: str | None = None,
    run_id: int | None = None,
    originating_task: str = "rum_discovery",
) -> int:
    """RUM counterpart of ``discover_prefix``, scoped to ``raw/rum/``."""
    return _discover_prefix_common(
        service_id,
        list_kwargs={"start_time": start_time, "prefix_subpath": prefix_subpath or "raw/rum/"},
        prefix_subpath=prefix_subpath,
        dispatch_fn=convert_batch_rum_files.delay,
        originating_task=originating_task,
        run_id=run_id,
        log_label="RUM ",
    )


def _parse_rum_line(log_data: dict, service_id: str) -> tuple[list[dict], list[dict]] | None:
    """Parse one decoded RUM beacon JSON object into ``(vitals_rows, errors_rows)``."""
    from backend.core.rum_ingest import extract_metrics_from_faro_payload, safe_float, safe_int

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
    try:
        dt = ensure_utc(datetime.fromisoformat(received_at))
    except Exception as e:
        raise ValueError(f"invalid_timestamp: {e}") from e

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
    """Decode and parse one ``raw/rum/*.gz`` beacon file into ``(vitals_rows, errors_rows, corrupt_lines)``."""
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


def _quarantine_rum_corrupt_lines(
    fos_client,
    src: dict,
    object_key: str,
    corrupt_lines: list[tuple[str, str]],
    valid_count: int,
    *,
    local_file: str,
) -> dict[str, int]:
    """Capture malformed RUM lines as exact-byte local evidence items."""
    from backend.core.quarantine import capture_evidence

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


def _write_rum_arrow_table_batch(
    duckdb_con: Any,
    table_name: str,
    rows: list[dict],
    sources: list[str],
    source_files_sql: str,
    schema: Any,
) -> None:
    """Stage Arrow rows into DuckDB and upsert into ``lake.{table_name}``."""
    if not rows:
        return
    import pyarrow as pa

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
    """Claim N RUM ledger rows, download them, and land every row in one DuckLake transaction per table."""
    from backend.core.iceberg._core import CLIENT_ERRORS_ARROW_SCHEMA, CLIENT_VITALS_ARROW_SCHEMA
    from backend.core.iceberg._ducklake import ducklake_table_name

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
            vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(local_file, service_id)
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
                        _quarantine_rum_corrupt_lines(
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


def sweep_rum_ledger_once(service_id: str, lookback_hours: int = 4, run_id: int | None = None) -> dict:
    """RUM counterpart of ``sweep_ledger_once``, scoped to ``raw/rum/`` object keys."""
    return _sweep_ledger_common(service_id, lookback_hours, run_id, is_rum=True)


# ── Celery task wrappers ──────────────────────────────────────────────────────


@app.task(name="backend.core.ingest.convert_batch_files", bind=True)
def convert_batch_files(self, service_id: str, object_keys: list[str]):
    """Batched convert — one DuckLake catalog commit for the whole batch."""
    with _celery_ingest_scope("ledger_convert_batch", service_id):
        return convert_batch_objects(service_id, object_keys, self.request.id or "sync-worker")


@app.task(name="backend.core.ingest.convert_batch_rum_files", bind=True)
def convert_batch_rum_files(self, service_id: str, object_keys: list[str]):
    with _celery_ingest_scope("ledger_rum_convert_batch", service_id):
        return convert_batch_rum_objects(service_id, object_keys, self.request.id or "rum-worker")


RAW_DELETE_GRACE_S = 10 * 60
RAW_DELETE_CLAIM_LEASE_S = 15 * 60


def _mark_ledger_published(service_id: str, *, rum: bool = False) -> int:
    """Record that DuckLake has durably published the committed ledger rows."""
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    prefix_clause = (
        "(object_key LIKE ? OR object_key LIKE ? OR object_key LIKE ?)"
        if rum
        else "(object_key NOT LIKE ? AND object_key NOT LIKE ? AND object_key NOT LIKE ?)"
    )
    cur = con.cursor()
    cur.execute(
        "UPDATE ingest_ledger SET published_at=? "
        "WHERE service_id=? AND status='committed' AND published_at IS NULL AND " + prefix_clause,
        (time.time(), service_id, "%/raw/rum/%", "raw/rum/%", "rum/%"),
    )
    con.commit()
    return cur.rowcount


def finalize_committed_raw(service_id: str, batch_size: int = 10_000) -> dict:
    """Delete raw .gz files whose rows are durably committed to the lake."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    cfg = svcconfig.load_config(service_id) or {}
    delete_after = svcconfig.resolve_raw_delete_after(cfg)

    con = get_con(service_id)
    cutoff = time.time() - RAW_DELETE_GRACE_S
    claim_expired = time.time() - RAW_DELETE_CLAIM_LEASE_S
    eligible = [
        r[0]
        for r in con.execute(
            "SELECT object_key FROM ingest_ledger "
            "WHERE service_id = ? AND status = 'committed' AND published_at IS NOT NULL "
            "AND raw_deleted_at IS NULL AND committed_at < ? "
            "AND (raw_delete_claim_token IS NULL OR raw_delete_claimed_at < ?) LIMIT ?",
            (service_id, cutoff, claim_expired, batch_size),
        ).fetchall()
    ]
    if not delete_after or not eligible:
        return {"deleted": 0, "eligible": len(eligible), "delete_after": bool(delete_after)}

    claim_token = uuid.uuid4().hex
    claimed_at = time.time()
    claimed: list[str] = []
    cur = con.cursor()
    for key_slice, placeholders in _iter_key_slices(eligible):
        cur.execute(
            "UPDATE ingest_ledger SET raw_delete_claim_token=?, raw_delete_claimed_at=? "
            f"WHERE service_id=? AND object_key IN ({placeholders}) AND status='committed' "
            "AND published_at IS NOT NULL AND raw_deleted_at IS NULL AND committed_at < ? "
            "AND (raw_delete_claim_token IS NULL OR raw_delete_claimed_at < ?) RETURNING object_key",
            (claim_token, claimed_at, service_id, *key_slice, cutoff, claim_expired),
        )
        claimed.extend(r[0] for r in cur.fetchall())
    con.commit()
    eligible = claimed
    if not eligible:
        return {"deleted": 0, "eligible": 0, "delete_after": True}

    src = get_source_for_service(service_id)
    if src is None:
        return {"deleted": 0, "eligible": len(eligible), "delete_after": True}
    fos = _get_fos_client(src)
    bucket = src.get("bucket", "")

    deleted = 0
    for chunk_tuple in itertools.batched(eligible, 1000):
        chunk = list(chunk_tuple)
        try:
            resp = fos.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True})
        except Exception as e:
            logger.warning("[ledger] %s: raw-file batch delete failed: %s", service_id, e)
            for key in chunk:
                _record_raw_delete_failure(service_id, key)
            continue
        failed_keys = {err.get("Key") for err in (resp.get("Errors") or [])}
        now = time.time()
        cur = con.cursor()
        failed_list = [k for k in chunk if k in failed_keys]
        succeeded_list = [k for k in chunk if k not in failed_keys]
        for f_slice, placeholders in _iter_key_slices(failed_list):
            cur.execute(
                "UPDATE ingest_ledger SET raw_delete_claim_token=NULL, raw_delete_claimed_at=NULL "
                f"WHERE service_id=? AND object_key IN ({placeholders}) AND raw_delete_claim_token=? AND raw_deleted_at IS NULL",
                (service_id, *f_slice, claim_token),
            )
        for s_slice, placeholders in _iter_key_slices(succeeded_list):
            cur.execute(
                "UPDATE ingest_ledger SET raw_deleted_at=?, raw_delete_claim_token=NULL, raw_delete_claimed_at=NULL "
                f"WHERE service_id=? AND object_key IN ({placeholders}) AND raw_delete_claim_token=? AND raw_deleted_at IS NULL",
                (now, service_id, *s_slice, claim_token),
            )
            deleted += int(cur.rowcount or 0)
        con.commit()
        for key in failed_list:
            _record_raw_delete_failure(service_id, key)
        if failed_keys:
            logger.warning("[ledger] %s: %d raw delete(s) failed in batch", service_id, len(failed_keys))
    return {"deleted": deleted, "eligible": len(eligible), "delete_after": True}


def merge_lake_files(service_id: str) -> None:
    """Flush inlined rows to parquet and compact adjacent small DuckLake data files for one service."""
    import duckdb as _duckdb

    from backend.core.duckdb import get_memory_connection, get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_write_admission

    src = get_source_for_service(service_id)
    if src is None:
        raise RuntimeError(f"no source registered for {service_id}")

    con = get_memory_connection(src)
    try:
        with ducklake_write_admission(service_id):
            if not _ducklake_attach(con, src, read_only=False):
                raise RuntimeError("DuckLake read-write attach failed")
            con.execute("CALL ducklake_flush_inlined_data('lake')")
            try:
                con.execute("CALL ducklake_merge_adjacent_files('lake')")
            except _duckdb.Error as exc:
                if "schema mismatch in glob" not in str(exc):
                    raise
                logger.warning(
                    "[ledger] %s: skipping DuckLake adjacent-file compaction after schema mismatch; "
                    "inlined rows were flushed and remain durable: %s",
                    service_id,
                    exc,
                )
            _mark_ledger_published(service_id)
    finally:
        con.close()
