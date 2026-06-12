import concurrent.futures
import hashlib
import json
import logging
import math
import os
import re
import tempfile
import time
from datetime import UTC, datetime, timedelta

from backend.core import iceberg, metadata_db
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
from backend.utils import field_codes as fc
from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)


def get_ingest_type_hints(log_fields_config: dict | None = None) -> dict[str, str]:
    hints = {
        f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"])
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in ("METRICS", "VIRTUAL", "INTERNAL") and f.get("vcl") is not None
    }
    if log_fields_config:
        for cf in log_fields_config.get("custom_fields", []):
            if cf.get("enabled", True):
                hints[cf["name"]] = cf.get("duckdb_type", "VARCHAR")
    return hints


def get_catalog_field_ids(log_fields_config: dict | None = None) -> list[str]:
    ids = [
        f["id"]
        for f in LOG_FIELD_CATALOG
        if f.get("group") not in ("METRICS", "VIRTUAL", "INTERNAL") and f.get("vcl") is not None
    ]
    if log_fields_config:
        for cf in sorted(log_fields_config.get("custom_fields", []), key=lambda x: x["name"]):
            if cf.get("enabled", True):
                ids.append(cf["name"])
    return ids


def get_ingest_columns_sql(log_fields_config: dict | None = None) -> str:
    hints = get_ingest_type_hints(log_fields_config)
    return "{" + ", ".join(f"{fid}: '{dtype}'" for fid, dtype in hints.items()) + "}"


_FASTLY_FNAME_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}[-:]\d{2}[-:]\d{2})")


def _parse_fastly_filename_dt(fname: str) -> datetime | None:
    """Parse the leading ``YYYY-MM-DDTHH-MM-SS`` of a Fastly raw log filename.

    Returns a UTC-aware datetime, or None if the filename doesn't match
    (e.g. an unrelated key that ends in ``.gz``). Used by the ingest
    discovery loop for both per-file range filtering and end-of-page
    early-stop.

    Example: ``2026-05-04T18-30-00.svc.gz`` → 2026-05-04 18:30:00+00:00

    Historical note: the previous inline implementation built the ISO
    string with ``.replace("-", ":", 2)`` which replaced dashes in the
    DATE half (``2026:05:04T18-30-00``), not the time half. The result
    failed ``fromisoformat`` and was silently swallowed by an outer
    ``try/except Exception``. The range-filter code path that depended
    on this never actually filtered. We split on ``T`` so only the time
    half's dashes become colons.
    """
    m = _FASTLY_FNAME_TS_RE.match(fname)
    if not m:
        return None
    try:
        date_part, time_part = m.group(1).split("T", 1)
        return datetime.fromisoformat(f"{date_part}T{time_part.replace('-', ':')}").replace(tzinfo=UTC)
    except ValueError:
        return None


def _compute_incremental_start_after(already: set[str], lookback_hours: int = 4) -> str | None:
    """Derive an S3 ``StartAfter`` key from previously-ingested filenames.

    The cron's incremental mode skips most of the bucket by passing
    ``StartAfter=raw/YYYY-MM-DD/HH/`` derived from the most recent file we
    already have, minus a small lookback to catch late-arriving POP logs.
    Without this, every cron run would scan the entire bucket from epoch.

    Returns None when ``already`` is empty or when no file matches the
    Fastly filename pattern (in which case the caller falls back to a full
    scan).
    """
    candidates = [f.split("/")[-1] for f in already if "/raw/" in f]
    if not candidates:
        return None
    latest = _parse_fastly_filename_dt(max(candidates))
    if latest is None:
        return None
    lookback = latest - timedelta(hours=lookback_hours)
    return lookback.strftime("raw/%Y-%m-%d/%H/")


def _delete_objects_robust(fos_client, bucket: str, keys: list[str]) -> int:
    """Delete multiple objects from S3 in bulk with a fallback to individual calls on failure."""
    if not keys:
        return 0

    try:
        # S3 bulk delete limit is 1000, we use 500 for safety with varied implementations
        batch_size = 500
        total_deleted = 0
        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            delete_payload = {"Objects": [{"Key": k} for k in batch], "Quiet": True}

            response = fos_client.delete_objects(Bucket=bucket, Delete=delete_payload)
            # Fastly Object Storage delete_objects returns success but doesn't always
            # populate the 'Deleted' array in the response like AWS S3 does.
            # If no errors were returned, assume all were deleted.
            errors = response.get("Errors", [])
            if errors:
                for error in errors[:1]:  # Log the first error
                    if "AccessDenied" in error.get("Code", "") or "UnauthorizedAccess" in error.get("Code", ""):
                        print(
                            f"Warning: Bulk delete skipped due to missing permissions ({error.get('Code')}). Disabling further delete attempts for this batch."
                        )
                        return total_deleted
            total_deleted += len(batch) - len(errors)
        return total_deleted
    except Exception as e:
        err_str = str(e)
        if "AccessDenied" in err_str or "UnauthorizedAccess" in err_str:
            print(f"Warning: Delete failed due to missing permissions: {err_str.split(':', 1)[-1].strip() or err_str}")
            return 0

        # Fallback to individual deletion if bulk is not supported or fails
        print(f"Bulk delete failed, falling back to individual: {e}")
        deleted_count = 0
        for k in keys:
            try:
                fos_client.delete_object(Bucket=bucket, Key=k)
                deleted_count += 1
            except Exception as individual_err:
                ind_err_str = str(individual_err)
                if "AccessDenied" in ind_err_str or "UnauthorizedAccess" in ind_err_str:
                    print(
                        f"Warning: Individual delete failed due to missing permissions: {ind_err_str}. Stopping further deletes."
                    )
                    break
                print(f"Warning: Failed to delete object {k}: {individual_err}")
        return deleted_count


def _download_chunk_to_local(fos_client, s3_paths: list[str], tmpdir: str) -> tuple[dict[str, str], dict[str, str]]:
    """Download each ``s3://bucket/key`` once into ``tmpdir``.

    Returns ``(s3_to_local, local_to_s3)``. Paths that fail to download are
    absent from both dicts; the caller marks them as failed.

    Every GET is tagged ``X-Telemetry-Caller: ingest_download`` via the FOS
    proxy hook, so it shows up correctly in ``usage_log``.
    """
    from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

    s3_to_local: dict[str, str] = {}
    local_to_s3: dict[str, str] = {}

    def _download_one(s3_path: str) -> tuple[str, str]:
        without_scheme = s3_path[5:] if s3_path.startswith("s3://") else s3_path
        slash = without_scheme.find("/")
        bucket = without_scheme[:slash]
        key = without_scheme[slash + 1 :]
        local_name = f"{abs(hash(s3_path)):x}_{key.rsplit('/', 1)[-1]}"
        local_path = os.path.join(tmpdir, local_name)
        token = _BOTO3_CALLER_HINT.set("ingest_download")
        try:
            resp = fos_client.get_object(Bucket=bucket, Key=key)
            with open(local_path, "wb") as f:
                for piece in iter(lambda: resp["Body"].read(65536), b""):
                    f.write(piece)
        finally:
            _BOTO3_CALLER_HINT.reset(token)
        return s3_path, local_path

    with concurrent.futures.ThreadPoolExecutor(max_workers=32, thread_name_prefix="ingest_dl") as ex:
        futures = {ex.submit(_download_one, p): p for p in s3_paths}
        for fut in concurrent.futures.as_completed(futures):
            try:
                s3, local = fut.result()
                s3_to_local[s3] = local
                local_to_s3[local] = s3
            except Exception as e:
                p = futures[fut]
                logger.warning("[ingest] failed to download %s: %s", p, e)

    return s3_to_local, local_to_s3


def _deterministic_buffer_name(chunk: list[str]) -> str:
    """Return ``batch_{sha256(sorted_chunk)[:16]}.parquet``.

    The hash is stable across runs of the same input set, so a crash between
    ``write_to_buffer`` and ``insert_ingested_files`` cannot produce two
    differently-named buffers for the same files (which the commit cron
    would otherwise both push to Iceberg, double-committing rows).
    """
    payload = "\n".join(sorted(chunk)).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return f"batch_{digest}.parquet"


def _recover_in_flight(source: dict) -> dict:
    """Reconcile ``ingest_in_flight`` rows with on-disk buffer state.

    Called at the start of every ingest tick. For each in_flight row:
      * If the buffer Parquet exists, the crash happened AFTER
        write_to_buffer succeeded but BEFORE insert_ingested_files. Promote
        the files into ``ingested_files`` and drop the in_flight row — the
        commit cron will pick up the buffer in its next run.
      * If the buffer Parquet is missing, the crash happened DURING or
        BEFORE write_to_buffer. Drop the in_flight row without touching
        ``ingested_files`` — those files will be re-LISTed on the next tick
        and re-ingested cleanly.

    Returns a summary dict ``{promoted, dropped, rows_recovered}`` for the
    cron logger.
    """
    source_name = source["name"]
    pending = metadata_db.list_in_flight(source_name)
    if not pending:
        return {"promoted": 0, "dropped": 0, "rows_recovered": 0}

    buf_dir = iceberg._buffer_dir(source)  # type: ignore[attr-defined]
    promoted = 0
    dropped = 0
    rows_recovered = 0
    for buffer_filename, file_rows in pending:
        buf_path = os.path.join(buf_dir, buffer_filename)
        if os.path.isfile(buf_path) and file_rows:
            metadata_db.insert_ingested_files(source_name, file_rows)
            metadata_db.clear_in_flight(source_name, buffer_filename)
            promoted += 1
            rows_recovered += sum(rc for (_, rc, _) in file_rows if rc)
            logger.info(
                "[ingest] %s: recovered in_flight buffer %s — promoted %d files (%d rows)",
                source_name,
                buffer_filename,
                len(file_rows),
                sum(rc for (_, rc, _) in file_rows if rc),
            )
        else:
            metadata_db.clear_in_flight(source_name, buffer_filename)
            dropped += 1
            logger.info(
                "[ingest] %s: dropped stale in_flight row for missing buffer %s (%d files will re-ingest)",
                source_name,
                buffer_filename,
                len(file_rows),
            )
    return {"promoted": promoted, "dropped": dropped, "rows_recovered": rows_recovered}


def ingest(
    source: dict | None = None,
    delete_after: bool = False,
    max_files: int | None = None,
    max_seconds: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    incremental_only: bool = False,
):
    """Ingest new raw log files from FOS into the local Iceberg buffer.

    Downloads .gz log files from the raw/ prefix, parses JSON, casts types,
    and writes each batch as a Parquet file to the local buffer directory.
    The scheduler's commit_buffer job flushes the buffer to the Iceberg table
    every few minutes.

    Yields progress dicts for SSE streaming.
    """
    start_time_exec = time.time()

    def elapsed() -> str:
        s = time.time() - start_time_exec
        return f"{int(s // 60)}m{int(s % 60):02d}s" if s >= 60 else f"{s:.1f}s"

    src = source or _DEFAULT_SOURCE
    if src.get("access_level") == "read_only":
        yield {"type": "error", "message": "Write operations are disabled in read-only mode."}
        return
    svc_id = src.get("service_id", "unknown")
    source_name = src["name"]
    svc_name = src.get("service_name") or source_name
    display_name = f"{svc_name} ({svc_id})" if svc_name != svc_id else svc_id
    _ensure_source_registered(src)

    if not src.get("bucket"):
        yield {"type": "error", "message": "Fastly Object Storage bucket is not configured for this service."}
        return

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

    # Normalize input range to UTC datetimes
    st_dt = None
    et_dt = None
    from backend.utils.date_utils import parse_iso_utc

    if start_time:
        st_dt = parse_iso_utc(start_time)
    if end_time:
        et_dt = parse_iso_utc(end_time)

    # Fetch already ingested files first.
    # ``incremental_only=True`` is the cron hot path: LIST is bounded by
    # ``StartAfter`` (or, for delete_after=True, the bucket itself only holds
    # un-ingested files) so dedup membership only needs the most recent rows.
    # Capping the fetch to 200k rows turns this from a ~4 s full-table
    # fetchall into ~600 ms on services with >1 M ingested_files. Full sweeps
    # (incremental_only=False) keep the unbounded load because they LIST the
    # entire bucket and may match arbitrarily old files. The user-facing
    # "skipped" count comes from the rollup summary so we still report the
    # accurate total even when the dedup set itself is bounded.
    yield {"type": "status", "message": f"{elapsed()} Fetching ingestion history..."}

    dedup_limit: int | None = 200_000 if incremental_only else None
    already = metadata_db.get_ingested_filenames(source_name, limit=dedup_limit)
    if dedup_limit is not None:
        try:
            skipped = metadata_db.get_ingested_files_status_summary(source_name)["file_count"]
        except Exception:
            skipped = len(already)
    else:
        skipped = len(already)

    # Determine StartAfter marker for incremental discovery to avoid scanning the entire bucket.
    start_after_key = None
    if st_dt and not already:
        # Use the configured start to bound the FOS list only on the very first import
        # (no previously ingested files). On subsequent cron runs `already` is non-empty,
        # so we fall through to the incremental lookback — scanning only the last 4 hours
        # instead of the entire bucket from the original import start date.
        start_after_key = st_dt.strftime("raw/%Y-%m-%d/%H/")
        logger.info("[ingest] %s: Using requested start_time to bound FOS scan: %s", display_name, start_after_key)
    elif incremental_only and already:
        # Incremental cron mode: scan only from 4 hours before the latest
        # ingested file to avoid listing the entire bucket on every run.
        # Previously gated on `not delete_after`, but the gate was wrong:
        # Fastly writes raw keys lexicographically by timestamp, so
        # StartAfter from our latest-known key still surfaces any newer
        # arrivals regardless of whether earlier files got deleted post-
        # ingest. With delete_after=True (the common case) we were doing
        # a full-bucket LIST every cron tick — one Class A call per 1000
        # files — and Fastly's running tally grows forever, so this was
        # the dominant LIST cost. Now bounded to ~1 Class A call/tick.
        # Manual imports still skip this branch (incremental_only=False)
        # so they scan the full bucket as before.
        try:
            start_after_key = _compute_incremental_start_after(already, lookback_hours=4)
        except Exception as e:
            logger.warning(
                "[ingest] %s: Failed to calculate lookback marker, scanning full bucket: %s", display_name, e
            )

    fos_client = _get_fos_client(src)
    file_sizes: dict[str, int] = {}
    new_files: list[str] = []
    total_listed = 0

    try:
        prefix_path = src["prefix"].strip("/")
        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
        raw_prefix = f"{prefix_path}/raw/" if prefix_path else "raw/"

        kwargs = {"Bucket": src["bucket"], "Prefix": raw_prefix}
        if start_after_key:
            kwargs["StartAfter"] = start_after_key

        yield {"type": "status", "message": f"{elapsed()} Discovering new files in Fastly Object Storage..."}

        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".gz"):
                    continue

                total_listed += 1
                if total_listed % 10000 == 0:
                    msg = f"{elapsed()} Discovered {total_listed:,} new files..."
                    yield {"type": "status", "message": msg}

                # Filename-based filtering to avoid downloading files completely outside our range
                # Fastly format: raw/YYYY-MM-DD/HH/YYYY-MM-DDTHH-MM-SS.xxx.gz
                fname = key.split("/")[-1]
                file_dt = _parse_fastly_filename_dt(fname)
                if file_dt is not None:
                    # Files are listed lexicographically. If we have an end_time and
                    # we've passed it by more than an hour (to account for ragged edges),
                    # we can stop listing entirely.
                    if et_dt and file_dt > (et_dt + timedelta(hours=1)):
                        break

                    # If the file is strictly before our start_time (minus some buffer), skip it
                    if st_dt and file_dt < (st_dt - timedelta(hours=1)):
                        continue

                full_path = f"s3://{src['bucket']}/{key}"
                # Even with StartAfter, double check it's not in 'already' just in case
                if full_path not in already:
                    new_files.append(full_path)
                    file_sizes[full_path] = obj["Size"]

            if et_dt and total_listed > 0:
                # Check the last key in the page to see if we can stop listing
                last_key = page.get("Contents", [])[-1]["Key"]
                last_dt = _parse_fastly_filename_dt(last_key.split("/")[-1])
                if last_dt is not None and last_dt > (et_dt + timedelta(hours=1)):
                    break

            # Break early if we've found enough new files
            if max_files and len(new_files) >= max_files:
                new_files = new_files[:max_files]
                break

    except Exception as e:
        yield {"type": "error", "message": f"Could not list FOS objects: {e}"}
        return

    if not new_files:
        yield {
            "type": "done",
            "new_files": 0,
            "skipped_files": skipped,
            "rows_inserted": 0,
            "deleted_files": 0,
            "message": f"Already up to date. {skipped} files previously processed.",
        }
        return

    chunk_size = INGEST_CHUNK_SIZE
    total_inserted = 0
    total_corrupt = 0
    total_corrupt_details: list[str] = []
    processed_count = 0
    deleted = 0
    successfully_processed_files: list[str] = []
    touched_hours: set[str] = set()

    mem_con = None
    # Increase parallelism for S3 deletions
    _delete_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="ingest_delete")
    _pending_deletes: list = []
    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) if source else None
    log_fields_config = cfg.get("log_fields", {}) if cfg else None

    columns_sql = get_ingest_columns_sql(log_fields_config)
    catalog_field_ids = get_catalog_field_ids(log_fields_config)

    # Pre-built SQL CASE expressions that decode single-char field codes back to
    # human-readable values. Handles both new logs (encoded) and old logs (full
    # strings) transparently. Unknown values pass through via ELSE.
    _DECODE_EXPRS: dict[str, str] = {
        "c_speed": fc.duckdb_decode_case("c_speed", fc.CONN_SPEED_ENCODE),
        "p_type": fc.duckdb_decode_case("p_type", fc.PROXY_TYPE_ENCODE),
        "p_desc": fc.duckdb_decode_case("p_desc", fc.PROXY_DESC_ENCODE),
    }

    try:
        from backend.core.duckdb import get_memory_connection

        mem_con = get_memory_connection(src)

        # Increase parallelism for DuckDB reads
        _cpus = os.cpu_count() or 4
        mem_con.execute(f"SET threads = {_cpus};")

        # Skip per-file HEAD-before-GET probes on .log.gz reads. DuckDB's default
        # httpfs path issues HEAD to plan ranged GETs, but gzip isn't seekable
        # so we always read the whole file anyway — the HEAD adds a round trip
        # with zero benefit. force_download=true tells httpfs to stream the
        # whole compressed object into a temp file in one GET. Files are
        # ~1–10 MB compressed, well within memory/disk headroom.
        # Scoped to mem_con (ingest-only); other connections still use ranged
        # reads where they matter (e.g., parquet column pruning).
        try:
            mem_con.execute("SET force_download = true;")
        except Exception as _fd_err:
            logger.debug("force_download not supported on this DuckDB build: %s", _fd_err)

        failed_paths = set()
        for chunk_start in range(0, len(new_files), chunk_size):
            if max_seconds and (time.time() - start_time_exec) > max_seconds:
                yield {
                    "type": "status",
                    "message": f"{elapsed()} Time limit of {max_seconds}s reached. Stopping batch early.",
                }
                break

            chunk = new_files[chunk_start : chunk_start + chunk_size]
            chunk_num = (chunk_start // chunk_size) + 1
            total_chunks = math.ceil(len(new_files) / chunk_size)
            msg = f"{elapsed()} Processing {len(chunk)} files in batch {chunk_num}/{total_chunks} ({len(new_files):,} files total)..."
            yield {"type": "status", "message": msg}

            count_map: dict[str, int] = {}

            # Download once, ingest from local paths. The previous version
            # passed s3:// URLs directly to DuckDB and relied on
            # `force_download=true` to stream each file into a per-query
            # tempfile. That fast path was fine — but the isolation retry
            # then refetched every file, and the corruption-repair branch's
            # `read_csv(sep='')` triggered DuckDB's sniffer for ~5 extra GETs
            # per file. Pre-downloading via boto3 (tagged
            # X-Telemetry-Caller=ingest_download in usage_log) bounds per-
            # file GETs at exactly 1 regardless of which branches run.
            chunk_tmpdir_obj = tempfile.TemporaryDirectory(prefix="ingest_chunk_")
            try:
                s3_to_local, local_to_s3 = _download_chunk_to_local(fos_client, list(chunk), chunk_tmpdir_obj.name)
                for p in chunk:
                    if p not in s3_to_local:
                        failed_paths.add(p)
                read_paths_s3 = [p for p in chunk if p in s3_to_local]
                if not read_paths_s3:
                    yield {"type": "status", "message": "All files in batch failed to download."}
                    continue
                read_paths = [s3_to_local[p] for p in read_paths_s3]

                mem_con.execute("DROP TABLE IF EXISTS _ingest_staging")
                # Security: escape single quotes in each local path before
                # interpolating into the SQL literal. The local paths inherit
                # their basename from the attacker-controllable S3 object key,
                # so a key like ``raw/'); ATTACH '...; --`` would otherwise
                # break out of the literal and execute arbitrary DuckDB SQL.
                paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in read_paths)

                try:
                    _execute_query_with_retry(
                        mem_con,
                        f"""
                        CREATE TEMP TABLE _ingest_staging AS
                        SELECT * FROM read_json_auto([{paths_sql}], format='newline_delimited',
                            records='auto', filename=true, columns={columns_sql}, ignore_errors=true)
                    """,
                    )
                except Exception:
                    yield {"type": "status", "message": "Batch read failed, isolating problematic files..."}

                    valid_paths = []
                    for i, read_path in enumerate(read_paths):
                        try:
                            # Security: per-file isolation read also needs escaping.
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
                            yield {"type": "status", "message": f"Skipping unreadable file {f_name}: {err_msg}"}

                    if not valid_paths:
                        yield {"type": "status", "message": "All files in batch are unreadable."}
                        continue

                    paths_sql_retry = ", ".join(valid_paths)
                    try:
                        _execute_query_with_retry(
                            mem_con,
                            f"""
                            CREATE TEMP TABLE _ingest_staging AS
                            SELECT * FROM read_json_auto([{paths_sql_retry}], format='newline_delimited',
                                records='auto', filename=true, columns={columns_sql}, ignore_errors=true)
                        """,
                        )
                    except Exception as retry_err:
                        yield {"type": "status", "message": f"Retry failed: {retry_err}"}
                        continue

                # Translate filename column from local→s3 so downstream
                # count_map / _source_file / file_sizes all key on s3://.
                if local_to_s3:
                    # Security: same escaping treatment for the
                    # local→s3 mapping table — both halves originate from
                    # attacker-controllable object keys.
                    path_map_rows = ", ".join(
                        f"('{escape_sql_literal(local)}', '{escape_sql_literal(s3)}')"
                        for local, s3 in local_to_s3.items()
                    )
                    mem_con.execute("DROP TABLE IF EXISTS _ingest_path_map")
                    mem_con.execute(
                        f"CREATE TEMP TABLE _ingest_path_map AS SELECT * FROM (VALUES {path_map_rows}) AS t(local, s3)"
                    )
                    mem_con.execute(
                        "UPDATE _ingest_staging "
                        "SET filename = COALESCE("
                        "  (SELECT s3 FROM _ingest_path_map WHERE local = _ingest_staging.filename), "
                        "  filename"
                        ")"
                    )

                for fpath, rcount in mem_con.execute(
                    "SELECT filename, count(*) FROM _ingest_staging GROUP BY 1"
                ).fetchall():
                    count_map[fpath] = rcount

                # ── Transform: backend prefix strip ──
                # Types are already correct from columns= above — no TRY_CAST needed.
                svc_id = src.get("logging_service_id") or src.get("name", "")

                filename_expr = '"filename"'

                if svc_id:
                    # Security: consistent escape helper across the
                    # ingest path. Functionally identical to the inline
                    # .replace but routes through escape_sql_literal so
                    # any future hardening on the canonical helper
                    # (e.g., extra char classes) flows here.
                    escaped = escape_sql_literal(svc_id)
                    backend_expr = f"regexp_replace(\"backend\", '^{escaped}--', '') AS \"backend\""
                else:
                    backend_expr = '"backend"'

                field_selects = []
                for fid in catalog_field_ids:
                    if fid in ("backend", "_source_file"):
                        continue
                    elif fid in _DECODE_EXPRS:
                        field_selects.append(f'{_DECODE_EXPRS[fid]} AS "{fid}"')
                    else:
                        field_selects.append(f'"{fid}"')
                field_selects.append(f"{filename_expr} AS _source_file")
                field_selects.append(backend_expr)

                where_clauses = []
                where_params = []
                if start_time:
                    where_clauses.append("timestamp >= ?::TIMESTAMPTZ")
                    where_params.append(start_time)
                if end_time:
                    where_clauses.append("timestamp <= ?::TIMESTAMPTZ")
                    where_params.append(end_time)

                where_sql = ""
                if where_clauses:
                    where_sql = f" WHERE {' AND '.join(where_clauses)}"

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

                _fetched = mem_con.execute("SELECT * FROM _ingest_staging WHERE timestamp IS NOT NULL").to_arrow_table()
                arrow_table = _fetched.read_all() if hasattr(_fetched, "read_all") else _fetched
                valid_rows = len(arrow_table)

                if valid_rows > 0:
                    chunk_hours = {
                        r[0]
                        for r in mem_con.execute(
                            "SELECT DISTINCT strftime(timestamp, '%Y-%m-%d-%H') FROM _ingest_staging WHERE timestamp IS NOT NULL"
                        ).fetchall()
                        if r[0] is not None
                    }
                    touched_hours.update(chunk_hours)

                _row = mem_con.execute("SELECT count(*) FROM _ingest_staging").fetchone()
                total_rows_batch = _row[0] if _row else 0
                corrupt_in_batch = total_rows_batch - valid_rows

                repairs_made = False
                if corrupt_in_batch > 0:
                    try:
                        valid_counts = dict(
                            mem_con.execute(
                                "SELECT _source_file, count(*) FROM _ingest_staging WHERE timestamp IS NOT NULL GROUP BY 1"
                            ).fetchall()
                        )
                        corrupt_read_paths = []
                        corrupt_s3_paths = []
                        for i, s3_path in enumerate(read_paths_s3):
                            expected = count_map.get(s3_path, 0)
                            actual = valid_counts.get(s3_path, 0)
                            if actual < expected:
                                corrupt_read_paths.append(read_paths[i])
                                corrupt_s3_paths.append(s3_path)

                        if corrupt_read_paths:
                            # Security: corrupt-file diagnostic path
                            # also needs escaping. Same vector as above.
                            paths_sql_str = ", ".join(f"'{escape_sql_literal(p)}'" for p in corrupt_read_paths)
                            q = f"""
                                SELECT filename, column0 FROM read_csv([{paths_sql_str}], header=false, sep='', quote='', escape='', columns={{'column0': 'VARCHAR'}}, filename=true)
                                WHERE NOT json_valid(column0) OR json_extract(column0, '$.timestamp') IS NULL
                            """
                            bad_rows = _execute_query_with_retry(mem_con, q).fetchall()

                            _EMPTY_VALUE_RE = re.compile(r":(?=[,}])")
                            repaired_by_fname: dict[str, list] = {}
                            truly_corrupt: list = []
                            for fname, raw_line in bad_rows:
                                # DuckDB filenames here are local paths; translate
                                # back so all downstream attribution stays s3://.
                                fname = local_to_s3.get(fname, fname)
                                if raw_line is None:
                                    continue
                                repaired = _EMPTY_VALUE_RE.sub(":null", raw_line.strip())
                                try:
                                    parsed = json.loads(repaired)
                                    if parsed.get("timestamp"):
                                        repaired_by_fname.setdefault(fname, []).append(repaired)
                                        continue
                                except (json.JSONDecodeError, ValueError):
                                    pass
                                truly_corrupt.append((fname, raw_line))

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

                                        # Apply the same transformation (decoding, filename
                                        # normalization) and inject the original s3:// fname
                                        # so attribution stays consistent. Security:
                                        # escape via the shared helper.
                                        safe_fname = escape_sql_literal(fname)
                                        mem_con.execute(
                                            f"""
                                            INSERT INTO _ingest_staging BY NAME
                                            SELECT {", ".join(field_selects)} FROM (
                                                SELECT *, '{safe_fname}' AS filename
                                                FROM read_json_auto(
                                                    '{tmp_path}',
                                                    format='newline_delimited',
                                                    records='auto',
                                                    columns={columns_sql},
                                                    ignore_errors=true
                                                )
                                            ){where_sql}
                                        """,
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
                                for fname, raw_line in truly_corrupt[: 100 - len(total_corrupt_details)]:
                                    short_name = fname.split("?")[0].split("/")[-1]
                                    total_corrupt_details.append(f"[{short_name}] {raw_line.strip()[:2000]}")
                    except Exception as e:
                        err_str = str(e)
                        # Network/disk failures here mean the local re-read failed —
                        # the main batch ingest already succeeded, but if we continue
                        # we will have partially ingested files. Roll back data for
                        # these files to allow them to be fully retried later.
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
                                for p in corrupt_s3_paths:
                                    failed_paths.add(p)

                                # Force re-calculation of counts and re-fetch of arrow_table
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
            finally:
                chunk_tmpdir_obj.cleanup()

            rows_to_track = [(f, count_map.get(f, 0), file_sizes.get(f)) for f in chunk if f not in failed_paths]

            if valid_rows > 0:
                if repairs_made:
                    _fetched = mem_con.execute(
                        "SELECT * FROM _ingest_staging WHERE timestamp IS NOT NULL"
                    ).to_arrow_table()
                    arrow_table = _fetched.read_all() if hasattr(_fetched, "read_all") else _fetched
                # Deterministic name + mark-before-write: the in_flight row
                # lands BEFORE the Parquet so a crash leaves the file rows
                # discoverable via list_in_flight() on next startup. The
                # buffer filename is hashed from the chunk's sorted source
                # filenames so a re-ingest of the same chunk overwrites the
                # same buffer file — Iceberg can't double-commit it.
                buf_filename = _deterministic_buffer_name([f for f in chunk if f not in failed_paths])
                if rows_to_track:
                    metadata_db.record_in_flight(source_name, buf_filename, rows_to_track)
                iceberg.write_to_buffer(src, arrow_table, buf_filename)
                if rows_to_track:
                    metadata_db.insert_ingested_files(source_name, rows_to_track)
                    metadata_db.clear_in_flight(source_name, buf_filename)
            elif rows_to_track:
                # No buffer produced (all rows outside the timestamp filter),
                # but we still need to mark these files ingested so we don't
                # re-LIST them every tick.
                metadata_db.insert_ingested_files(source_name, rows_to_track)
            successfully_processed_files.extend([f for f in chunk if f not in failed_paths])

            # Per-file ingest downloads are captured by the telemetry proxy on
            # every duckdb.httpfs GET/HEAD; recording them again here would
            # double-count CDN GETs in the usage log. The cron's
            # process_context tags this work as `cron:sync:*`.

            if delete_after:
                # Clean up completed futures to avoid unbounded list growth
                _pending_deletes = [f for f in _pending_deletes if not f.done()]

                chunk_keys = [
                    f[len(f"s3://{src['bucket']}/") :]
                    for f in chunk
                    if f.startswith(f"s3://{src['bucket']}/") and f not in failed_paths
                ]

                if chunk_keys:
                    # delete_objects batches 500 keys per API call. The telemetry
                    # proxy records the actual POST per batch (function_name=
                    # boto3.deleteobjects), so we don't fire a manual record_call
                    # here — doing so produced a 1:1 duplicate row per batch.

                    def _do_delete(keys, bucket, client):
                        return _delete_objects_robust(client, bucket, keys)

                    future = _delete_executor.submit(_do_delete, chunk_keys, src["bucket"], fos_client)
                    _pending_deletes.append(future)
                    yield {
                        "type": "status",
                        "message": f"{elapsed()} Batch {chunk_num}: Submitted deletion of {len(chunk_keys)} raw files (async)...",
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
        # Wait for all in-flight S3 deletions
        try:
            for f in concurrent.futures.as_completed(_pending_deletes, timeout=300):
                try:
                    deleted += f.result()
                except Exception as _de:
                    logger.warning("[ingest] %s: async delete error: %s", source_name, _de)
        except concurrent.futures.TimeoutError:
            logger.warning(
                "[ingest] %s: timed out waiting for all async deletions to complete. Some files may still be deleting in the background.",
                source_name,
            )

        _delete_executor.shutdown(wait=False)
        if mem_con:
            try:
                mem_con.close()
            except Exception:
                pass

    yield {
        "type": "done",
        "new_files": processed_count,
        "skipped_files": skipped,
        "rows_inserted": total_inserted,
        "corrupt_rows": total_corrupt,
        "corrupt_details": total_corrupt_details,
        "deleted_files": deleted,
        "message": f"Successfully ingested {processed_count} new files ({total_inserted} rows) and deleted {deleted} raw files.",
        "touched_hours": list(touched_hours),
    }
