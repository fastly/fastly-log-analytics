import concurrent.futures
import contextvars
import hashlib
import logging
import math
import os
import tempfile
import time
from typing import Any

import orjson
from filelock import FileLock

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
from backend.core.ingest import batch_worker
from backend.core.ingest.batch_worker import (
    _celery_ingest_scope,
    _claim_and_resolve_batch_source,
    _cleanup_batch_resources,
    _download_and_classify_batch_files,
    _ducklake_replace_source_rows,
    _ducklake_table_exists,
    _finalize_batch_convert,
    _handle_batch_convert_exception,
    _ledger_claim_batch,
    _ledger_mark_committed_batch,
    _ledger_mark_dead_letter_missing,
    _ledger_record_failure,
    _record_ledger_batch_outcomes,
    _record_ledger_object_outcome,
    _record_raw_delete_failure,
    _redispatch_pending,
    _run_batch_convert_common,
    _sweep_ledger_common,
    convert_batch_files,
    convert_batch_objects,
    finalize_committed_raw,
    sweep_ledger_once,
)
from backend.core.ingest.discovery import (
    _RECONCILE_LEDGER_EPOCH,
    _RECONCILE_MIN_INTERVAL_S,
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    _download_chunk_to_local,
    _parse_time_bounds,
    _reconcile_last_attempt,
    list_fos_files,
)
from backend.core.ingest.parser import (
    _EMPTY_VALUE_RE,
    _build_column_select_exprs,
    _capture_corrupt_container,
    _corrupt_gzip_error,
    _finalize_sync_outcome_counters,
    _new_object_outcome,
    _quarantine_convert_corrupt_lines,
    _read_json_auto_sql,
    _scan_invalid_gzip_lines,
    get_ingest_columns_sql,
)
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import yield_to_api

logger = logging.getLogger("backend.core.ingest")

_REWRITE_STAGING_FILENAME_SQL = (
    "UPDATE _ingest_staging SET filename = m.s3 FROM _ingest_path_map AS m WHERE _ingest_staging.filename = m.local"
)


def _export_staging_arrow(mem_con: Any) -> Any:
    fetched = mem_con.execute("SELECT * FROM _ingest_staging WHERE timestamp IS NOT NULL").to_arrow_table()
    return fetched.read_all() if hasattr(fetched, "read_all") else fetched


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
    from backend.core.duckdb import _cache_dir

    src = source or _DEFAULT_SOURCE
    cache_dir = _cache_dir(src)
    os.makedirs(cache_dir, exist_ok=True)
    wait_started = time.monotonic()
    # Cron leases are per task; discovery and full sweeps share this source ledger.
    with FileLock(os.path.join(cache_dir, ".request-ingest.lock"), thread_local=False):
        logger.info(
            "[ingest] %s: acquired request-ingest fence after %.3fs",
            src["name"],
            time.monotonic() - wait_started,
        )
        yield from _ingest_unlocked(
            source=src,
            delete_after=delete_after,
            max_files=max_files,
            max_seconds=max_seconds,
            start_time=start_time,
            end_time=end_time,
            incremental_only=incremental_only,
        )


def _ingest_unlocked(  # fmt: skip
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
                    mem_con.execute(_REWRITE_STAGING_FILENAME_SQL)
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
                            corrupt_s3_files: set[str] = set()
                            for fname, raw_line in bad_rows:
                                fname = local_to_s3.get(fname, fname)
                                corrupt_s3_files.add(fname)
                                if raw_line is None:
                                    continue
                                repaired = _EMPTY_VALUE_RE.sub(":null", raw_line.strip())
                                try:
                                    if orjson.loads(repaired).get("timestamp"):
                                        repaired_by_fname.setdefault(fname, []).append(repaired)
                                        continue
                                    truly_corrupt.append((fname, raw_line, "missing_timestamp"))
                                except (orjson.JSONDecodeError, ValueError):
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
                            _chunk_corrupt_s3_paths = (
                                [p for p in corrupt_s3_paths if p in corrupt_s3_files]
                                if corrupt_s3_files
                                else list(corrupt_s3_paths)
                            )
                            _chunk_truly_corrupt = truly_corrupt
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
                            contextvars.copy_context().run,
                            _delete_objects_robust_with_failures,
                            fos_client,
                            src["bucket"],
                            chunk_keys,
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


__all__ = [
    "_REWRITE_STAGING_FILENAME_SQL",
    "_celery_ingest_scope",
    "_claim_and_resolve_batch_source",
    "_cleanup_batch_resources",
    "_deterministic_buffer_name",
    "_download_and_classify_batch_files",
    "_ducklake_replace_source_rows",
    "_ducklake_table_exists",
    "_export_staging_arrow",
    "_finalize_batch_convert",
    "_handle_batch_convert_exception",
    "_ledger_claim_batch",
    "_ledger_mark_committed_batch",
    "_ledger_mark_dead_letter_missing",
    "_ledger_record_failure",
    "_mark_ledger_published",
    "_record_ledger_batch_outcomes",
    "_record_ledger_object_outcome",
    "_record_raw_delete_failure",
    "_recover_in_flight",
    "_redispatch_pending",
    "_run_batch_convert_common",
    "_sweep_ledger_common",
    "_write_buffer_with_in_flight",
    "convert_batch_files",
    "convert_batch_objects",
    "finalize_committed_raw",
    "ingest",
    "merge_lake_files",
    "sweep_ledger_once",
]

install_shim_module(__name__, (batch_worker,), bidirectional_seed=True)
