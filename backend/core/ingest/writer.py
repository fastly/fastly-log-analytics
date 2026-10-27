import concurrent.futures
import hashlib
import itertools
import json
import logging
import math
import os
import random
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.celery_app import app
from backend.core import metadata as metadata_db
from backend.core import storage
from backend.core.duckdb import (
    _DEFAULT_SOURCE,
    INGEST_CHUNK_SIZE,
    _ensure_source_registered,
    _execute_query_with_retry,
    _get_fos_client,
    escape_sql_literal,
)
from backend.core.ingest.discovery import (
    _RECONCILE_LEDGER_EPOCH,
    _RECONCILE_MIN_INTERVAL_S,
    LEDGER_CONVERT_BATCH_SIZE,
    LEDGER_MAX_ATTEMPTS,
    LEDGER_RECLAIM_AFTER_S,
    LEDGER_REDISPATCH_AFTER_S,
    LEDGER_RETRY_BASE_S,
    LEDGER_RETRY_JITTER_RATIO,
    LEDGER_RETRY_MAX_S,
    RAW_DELETE_CLAIM_LEASE_S,
    RAW_DELETE_GRACE_S,
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    _download_chunk_to_local,
    _is_permanent_s3_missing,
    _iter_key_slices,
    _parse_time_bounds,
    _reconcile_last_attempt,
    boto3_client_hot,
    discover_prefix,
    list_fos_files,
)
from backend.core.ingest.parser import (
    _EMPTY_VALUE_RE,
    _build_column_select_exprs,
    _capture_corrupt_container,
    _corrupt_gzip_error,
    _failed_object_outcome,
    _finalize_object_outcome,
    _finalize_sync_outcome_counters,
    _mark_object_failed,
    _new_object_outcome,
    _quarantine_convert_corrupt_lines,
    _read_json_auto_sql,
    _scan_invalid_gzip_lines,
    get_ingest_columns_sql,
)
from backend.utils.telemetry import yield_to_api

logger = logging.getLogger("backend.core.ingest")


def _record_ledger_object_outcome(service_id: str, object_key: str, counters: dict[str, int]) -> None:
    from backend.core.metadata.usage_log import record_ingest_object_outcome

    record_ingest_object_outcome(service_id, object_key, counters)


def _record_ledger_batch_outcomes(service_id: str, outcomes_by_key: dict[str, dict[str, int]]) -> None:
    if not outcomes_by_key:
        return
    from backend.core.metadata.usage_log import record_ingest_batch_outcomes

    record_ingest_batch_outcomes(service_id, outcomes_by_key)


def _export_staging_arrow(mem_con: Any) -> Any:
    fetched = mem_con.execute("SELECT * FROM _ingest_staging WHERE timestamp IS NOT NULL").to_arrow_table()
    return fetched.read_all() if hasattr(fetched, "read_all") else fetched


def _ducklake_table_exists(duckdb_con: Any, table_name: str) -> bool:
    q = "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1"
    return bool(duckdb_con.execute(q, (table_name,)).fetchone())


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
    from backend.core.metadata.usage_log import OUTCOME_COUNTER_KEYS

    q = "SELECT outcome_counters FROM ingest_ledger WHERE service_id = ? AND object_key = ?"
    row = get_con(service_id).execute(q, (service_id, object_key)).fetchone()
    counters = json.loads(row["outcome_counters"] or "{}") if row else {}
    outcome = {key: int(counters.get(key, 0)) for key in OUTCOME_COUNTER_KEYS}
    outcome["objects_processed"] = max(1, outcome["objects_processed"])
    outcome["source_delete_failures"] += 1
    _record_ledger_object_outcome(service_id, object_key, _mark_object_failed(outcome))


def _deterministic_buffer_name(chunk: list[str]) -> str:
    return f"batch_{hashlib.sha256('\n'.join(sorted(chunk)).encode('utf-8')).hexdigest()[:16]}.parquet"


def _recover_in_flight(source: dict, table_name: str = "logs") -> dict:
    source_name = source["name"]
    pending = metadata_db.list_in_flight(source_name, table_name=table_name)
    if not pending:
        return {"promoted": 0, "dropped": 0, "rows_recovered": 0}
    buf_dir = storage._buffer_dir(source, table_name=table_name)  # type: ignore[attr-defined]
    promoted = dropped = rows_recovered = 0
    for buffer_filename, file_rows in pending:
        if os.path.isfile(os.path.join(buf_dir, buffer_filename)) and file_rows:
            row_sum = sum(rc for (_, rc, _) in file_rows if rc)
            metadata_db.insert_ingested_files(source_name, file_rows, table_name=table_name)
            metadata_db.clear_in_flight(source_name, buffer_filename, table_name=table_name)
            promoted += 1
            rows_recovered += row_sum
            logger.info("[ingest] %s: recovered in_flight buffer %s (%s) — promoted %d files (%d rows)", source_name, buffer_filename, table_name, len(file_rows), row_sum,)  # fmt: skip
        else:
            metadata_db.clear_in_flight(source_name, buffer_filename, table_name=table_name)
            dropped += 1
            logger.info( "[ingest] %s: dropped stale in_flight row for missing buffer %s (%s) (%d files will re-ingest)",
                source_name, buffer_filename, table_name, len(file_rows),
            )  # fmt: skip
    return {"promoted": promoted, "dropped": dropped, "rows_recovered": rows_recovered}


def _write_buffer_with_in_flight(
    src: dict,
    source_name: str,
    buf_filename: str,
    arrow_table: Any | None,
    rows_to_track: list,
    table_name: str = "logs",
) -> None:
    kw = {} if table_name == "logs" else {"table_name": table_name}
    if arrow_table is not None:
        if rows_to_track:
            metadata_db.record_in_flight(source_name, buf_filename, rows_to_track, **kw)
        storage.write_to_buffer(src, arrow_table, buf_filename, **kw)
        if rows_to_track:
            metadata_db.insert_ingested_files(source_name, rows_to_track, **kw)
            metadata_db.clear_in_flight(source_name, buf_filename, **kw)
    elif rows_to_track:
        metadata_db.insert_ingested_files(source_name, rows_to_track, **kw)


def ingest(  # fmt: skip
    source: dict | None = None,
    delete_after: bool = False,
    max_files: int | None = None,
    max_seconds: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    incremental_only: bool = False,
):
    start_time_exec = time.time()

    def elapsed() -> str:
        s = time.time() - start_time_exec
        return f"{int(s // 60)}m{int(s % 60):02d}s" if s >= 60 else f"{s:.1f}s"

    src = source or _DEFAULT_SOURCE
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
            msg = f"Crash recovery: promoted {recovery['promoted']} buffer(s) ({recovery['rows_recovered']:} rows), dropped {recovery['dropped']} stale row(s)."
            yield {"type": "status", "message": msg}
    except Exception as e:
        logger.warning("[ingest] %s: in_flight recovery sweep failed: %s", display_name, e)
    _parse_time_bounds(start_time, end_time)
    yield {"type": "status", "message": f"{elapsed()} Fetching ingestion history..."}
    already = metadata_db.get_ingested_filenames(source_name, limit=20_000 if incremental_only else None)
    list_res = yield from list_fos_files(  # fmt: skip
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
                    display_name,
                    classify_err,
                )
                deletable = set()
            s3_pfx = f"s3://{src['bucket']}/"
            stranded_keys = [p[len(s3_pfx) :] for p in stranded_already if p in deletable and p.startswith(s3_pfx)]
            if stranded_keys:
                yield {
                    "type": "status",
                    "message": f"{elapsed()} Reclaiming {len(stranded_keys)} raw file(s) ingested but not deleted by an interrupted prior run...",
                }
                try:
                    reclaimed = _delete_objects_robust(fos_client, src["bucket"], stranded_keys)
                except Exception as recl_err:
                    logger.warning(
                        "[ingest] %s: stranded-delete reconcile failed (%s) — will retry next run",
                        display_name,
                        recl_err,
                    )
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
            chunk_num, total_chunks = (chunk_start // chunk_size) + 1, math.ceil(len(new_files) / chunk_size)
            yield {
                "type": "status",
                "message": f"{elapsed()} Processing {len(chunk)} files in batch {chunk_num}/{total_chunks} ({len(new_files):} files total)...",
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
                            f_name, err_msg = read_paths_s3[i].split("/")[-1], str(file_err)
                            logger.error(f"Ingest isolation failed for {f_name}: {err_msg}")
                            err_msg = err_msg.split(":", 1)[-1].strip()
                            failed_paths.add(read_paths_s3[i])
                            if (failed_gzip := _corrupt_gzip_error(read_path)) is not None:
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
                        "UPDATE _ingest_staging SET filename = COALESCE((SELECT s3 FROM _ingest_path_map WHERE local = _ingest_staging.filename), filename)"
                    )
                count_map.update(
                    dict(mem_con.execute("SELECT filename, count(*) FROM _ingest_staging GROUP BY 1").fetchall())
                )
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
                    q_hrs = "SELECT DISTINCT strftime(timestamp, '%Y-%m-%d-%H') FROM _ingest_staging WHERE timestamp IS NOT NULL"
                    touched_hours.update(r[0] for r in mem_con.execute(q_hrs).fetchall() if r[0] is not None)
                _row = mem_con.execute("SELECT count(*) FROM _ingest_staging").fetchone()
                corrupt_in_batch = (_row[0] if _row else 0) - valid_rows
                repairs_made = False
                _chunk_corrupt_s3_paths, _chunk_truly_corrupt = [], []
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
                                    source_name,
                                    diagnostic_error,
                                )
                                bad_rows = _scan_invalid_gzip_lines(corrupt_read_paths)
                            repaired_by_fname: dict[str, list] = {}
                            truly_corrupt: list[tuple[str, str, str]] = []
                            for fname, raw_line in bad_rows:
                                fname = local_to_s3.get(fname, fname)
                                if raw_line is None:
                                    continue
                                repaired = _EMPTY_VALUE_RE.sub(":null", raw_line.strip())
                                try:
                                    if json.loads(repaired).get("timestamp"):
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
                                    tmp_path = tmp.name
                                    try:
                                        tmp.write("\n".join(lines) + "\n")
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
                            for fname, raw_line, _reason in truly_corrupt[: max(0, 100 - len(total_corrupt_details))]:
                                total_corrupt_details.append(
                                    f"[{fname.split('?')[0].split('/')[-1]}] {raw_line.strip()[:2000]}"
                                )
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
                                len(corrupt_s3_paths),
                                err_str,
                            )
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
                q_dur = "SELECT DISTINCT _source_file FROM _ingest_staging WHERE timestamp IS NOT NULL"
                durable_files = {r[0] for r in mem_con.execute(q_dur).fetchall() if r[0] is not None}
            except Exception:
                durable_files = set()
            rows_to_track = [
                (f, count_map.get(f, 0) if f in durable_files else 0, file_sizes.get(f)) for f in good_files
            ]
            if valid_rows > 0:
                if repairs_made:
                    arrow_table = _export_staging_arrow(mem_con)
                _write_buffer_with_in_flight(
                    src, source_name, _deterministic_buffer_name(good_files), arrow_table, rows_to_track
                )
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
                            source_name,
                            submit_err,
                        )
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
                                source_name,
                                chunk_num,
                                len(chunk_keys),
                                inline_err,
                            )
                            yield {
                                "type": "status",
                                "message": f"{elapsed()} Batch {chunk_num}: WARN — failed to delete {len(chunk_keys)} raw files: {inline_err}",
                            }
            total_inserted += valid_rows
            for file_path in chunk:
                processed_count += 1
                yield {  # fmt: skip
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
    _finalize_sync_outcome_counters(
        outcome_counters,
        valid_records=total_inserted,
        corrupt_containers=corrupt_containers,
        quarantine_capture_failures=quarantine_capture_failures,
        cap_evictions=cap_evictions,
        failed_paths=failed_paths,
        capture_failed_paths=capture_failed_paths,
        delete_failed_paths=delete_failed_paths,
        partial_paths=partial_paths,
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
        "message": f"Successfully ingested {processed_count} new files ({total_inserted} rows) and deleted {total_deleted} raw files{reclaimed_note}.",
        "touched_hours": list(touched_hours),
    }


def _celery_ingest_scope(kind: str, service_id: str):
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


def _ledger_mark_dead_letter_missing(
    con_meta: Any, service_id: str, object_key: str, lease_generation: int | None
) -> int:
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
    from backend.core.storage._core import _ducklake_detach

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
    from backend import config as svcconfig
    from backend.core.duckdb import get_memory_connection
    from backend.core.storage._core import _ducklake_attach, ducklake_write_admission

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
    from backend import config as svcconfig
    from backend.core.storage._core import ducklake_table_name

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


def _redispatch_pending(
    service_id: str,
    reclaimed: list[str],
    stuck: list[str],
    dispatch_fn: Callable[[str, list[str]], object],
    label: str,
    queue_depths_fn: Callable[[], tuple[dict[str, int], bool]] | None = None,
) -> tuple[int, bool]:
    pending = len(stuck) + len(reclaimed)
    if not pending:
        return 0, True
    pending_batches = math.ceil(pending / LEDGER_CONVERT_BATCH_SIZE)
    broker_ok = False
    try:
        from backend import celery_app as _cq

        depth_fn = queue_depths_fn if queue_depths_fn is not None else _cq.celery_queue_depths
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
    _self_mod = sys.modules["backend.core.ingest"]
    dispatch_fn = _self_mod.convert_batch_rum_files.delay if is_rum else convert_batch_files.delay
    label = "RUM " if is_rum else ""
    redispatched, broker_ok = _redispatch_pending(service_id, reclaimed, stuck, dispatch_fn, label)
    st = (datetime.now(UTC) - timedelta(hours=lookback_hours)).isoformat()
    discover_fn = _self_mod.discover_rum_prefix if is_rum else discover_prefix
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
    return _sweep_ledger_common(service_id, lookback_hours, run_id, is_rum=False)


@app.task(name="backend.core.ingest.convert_batch_files", bind=True)
def convert_batch_files(self, service_id: str, object_keys: list[str]):
    with _celery_ingest_scope("ledger_convert_batch", service_id):
        return convert_batch_objects(service_id, object_keys, self.request.id or "sync-worker")


def _mark_ledger_published(service_id: str, *, rum: bool = False) -> int:
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
    import duckdb as _duckdb

    from backend.core.duckdb import get_memory_connection, get_source_for_service
    from backend.core.storage._core import _ducklake_attach, ducklake_write_admission

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
