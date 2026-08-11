import concurrent.futures
import hashlib
import json
import logging
import math
import os
import re
import tempfile
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

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
from backend.utils import field_codes as fc
from backend.utils.active_requests import yield_to_api
from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

# When delete_after is on, files seen in the LIST that are already in the dedup
# ledger are strands: ingested but never deleted (a restart landed between the
# ledger write and the FOS delete). We re-delete them to make deletion
# self-healing. Cap how many we collect per run so a large backlog can't balloon
# memory; whatever exceeds the cap is reclaimed on subsequent ticks/full_syncs.
_STRANDED_DELETE_CAP = 10_000

# Throttle the strand reconcile on the incremental cron path so a strand whose
# delete keeps FAILING (e.g. a FOS permissions outage) can't drive a delete API
# call every tick (the real-time tier fires every ~5s). The happy path clears a
# strand in one successful delete, so this only bounds the pathological loop.
# full_sync (incremental_only=False) ignores this and always reconciles — it is
# the backstop for old/over-cap strands. In-process state, so a restart re-arms
# at most one extra attempt; that is fine.
_RECONCILE_MIN_INTERVAL_S = 300.0
_reconcile_last_attempt: dict[str, float] = {}

# Durability epoch for the strand reconcile. Ledger rows written BEFORE this fix
# shipped can carry a NON-zero row_count for a file with NO durable data (the
# pre-fix code stored the PRE-filter count for fully-filtered / all-corrupt files),
# so row_count is not a trustworthy "is this durable?" signal for them. The
# reconcile therefore only deletes strands ingested at/after this boundary, where
# the new code guarantees a no-data file is recorded with row_count 0. Pre-epoch
# strands drain via the existing 1-day ledger trim instead. Format matches the
# SQLite ``ingested_at`` column (``datetime('now')``: "YYYY-MM-DD HH:MM:SS", UTC).
_RECONCILE_LEDGER_EPOCH = "2026-06-21 00:00:00"


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
    return (
        "{"
        + ", ".join(f"'{escape_sql_literal(fid)}': '{escape_sql_literal(dtype)}'" for fid, dtype in hints.items())
        + "}"
    )


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
                        logger.warning(
                            "Bulk delete skipped due to missing permissions (%s). Disabling further delete attempts for this batch.",
                            error.get("Code"),
                        )
                        return total_deleted
            total_deleted += len(batch) - len(errors)
        return total_deleted
    except Exception as e:
        err_str = str(e)
        if "AccessDenied" in err_str or "UnauthorizedAccess" in err_str:
            logger.warning(
                "Delete failed due to missing permissions: %s",
                err_str.split(":", 1)[-1].strip() or err_str,
            )
            return 0

        # Fallback to individual deletion if bulk is not supported or fails
        logger.warning("Bulk delete failed, falling back to individual", exc_info=True)
        deleted_count = 0
        for k in keys:
            try:
                fos_client.delete_object(Bucket=bucket, Key=k)
                deleted_count += 1
            except Exception as individual_err:
                ind_err_str = str(individual_err)
                if "AccessDenied" in ind_err_str or "UnauthorizedAccess" in ind_err_str:
                    logger.warning(
                        "Individual delete failed due to missing permissions: %s. Stopping further deletes.",
                        ind_err_str,
                    )
                    break
                logger.warning("Failed to delete object %s", k, exc_info=True)
        return deleted_count


def _quarantine_corrupt_files(
    fos_client,
    bucket: str,
    source: dict,
    corrupt_s3_paths: list[str],
    truly_corrupt: list[tuple[str, str, str]],
    count_map: dict[str, int],
    valid_counts: dict[str, int],
    file_sizes: Mapping[str, int | None],
    source_name: str,
) -> int:
    """Write corrupt lines to ``errors/`` prefix in FOS with a sidecar ``.meta.json``.

    Only the bad lines are written — not the entire raw file (valid rows are
    already ingested). Best-effort: per-file failures are logged but never
    block the ingest pipeline. Returns the number of files successfully quarantined.
    """
    prefix_path = source.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/raw/" if prefix_path else "raw/"
    errors_prefix = f"{prefix_path}/errors/" if prefix_path else "errors/"
    quarantined = 0

    corrupt_by_file: dict[str, list[str]] = {}
    reason_counts_by_file: dict[str, dict[str, int]] = {}
    for fname, raw_line, reason in truly_corrupt:
        corrupt_by_file.setdefault(fname, []).append(raw_line.strip())
        rc = reason_counts_by_file.setdefault(fname, {})
        rc[reason] = rc.get(reason, 0) + 1

    for s3_path in corrupt_s3_paths:
        bad_lines = corrupt_by_file.get(s3_path, [])
        if not bad_lines:
            continue
        try:
            original_key = s3_path[len(f"s3://{bucket}/") :]
            if not original_key.startswith(raw_prefix):
                continue
            error_key = errors_prefix + original_key[len(raw_prefix) :].replace(".gz", ".bad.jsonl")
            meta_key = error_key + ".meta.json"
            file_name = original_key.rsplit("/", 1)[-1]

            error_body = "\n".join(bad_lines).encode()
            fos_client.put_object(
                Bucket=bucket,
                Key=error_key,
                Body=error_body,
                ContentType="application/x-ndjson",
            )

            file_valid = valid_counts.get(s3_path, 0)
            file_total = count_map.get(s3_path, 0)
            file_corrupt = len(bad_lines)
            samples = [line[:2000] for line in bad_lines[:5]]
            file_reason_counts = reason_counts_by_file.get(s3_path, {})

            meta = {
                "original_key": original_key,
                "quarantined_at": datetime.now(UTC).isoformat(),
                "valid_rows": file_valid,
                "corrupt_rows": file_corrupt,
                "total_rows": file_total,
                "file_size_bytes": file_sizes.get(s3_path),
                "corrupt_samples": samples,
                "reason_counts": file_reason_counts,
                "source_name": source_name,
            }
            fos_client.put_object(
                Bucket=bucket,
                Key=meta_key,
                Body=json.dumps(meta).encode(),
                ContentType="application/json",
            )

            metadata_db.insert_quarantined_file(
                service_id=source.get("service_id") or source.get("name", ""),
                file_name=file_name,
                source_name=source_name,
                fos_key=original_key,
                error_key=error_key,
                meta_key=meta_key,
                valid_rows=file_valid,
                corrupt_rows=file_corrupt,
                file_size_bytes=file_sizes.get(s3_path),
                corrupt_samples=samples,
                reason_counts=file_reason_counts,
                error_size_bytes=len(error_body),
            )
            quarantined += 1
        except Exception as qe:
            logger.warning("[ingest] %s: failed to quarantine %s: %s", source_name, s3_path, qe)

    return quarantined


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


def _recover_in_flight(source: dict, table_name: str = "logs") -> dict:
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
    pending = metadata_db.list_in_flight(source_name, table_name=table_name)
    if not pending:
        return {"promoted": 0, "dropped": 0, "rows_recovered": 0}

    buf_dir = iceberg._buffer_dir(source, table_name=table_name)  # type: ignore[attr-defined]
    promoted = 0
    dropped = 0
    rows_recovered = 0
    for buffer_filename, file_rows in pending:
        buf_path = os.path.join(buf_dir, buffer_filename)
        if os.path.isfile(buf_path) and file_rows:
            metadata_db.insert_ingested_files(source_name, file_rows, table_name=table_name)
            metadata_db.clear_in_flight(source_name, buffer_filename, table_name=table_name)
            promoted += 1
            rows_recovered += sum(rc for (_, rc, _) in file_rows if rc)
            logger.info(
                "[ingest] %s: recovered in_flight buffer %s (%s) — promoted %d files (%d rows)",
                source_name,
                buffer_filename,
                table_name,
                len(file_rows),
                sum(rc for (_, rc, _) in file_rows if rc),
            )
        else:
            metadata_db.clear_in_flight(source_name, buffer_filename, table_name=table_name)
            dropped += 1
            logger.info(
                "[ingest] %s: dropped stale in_flight row for missing buffer %s (%s) (%d files will re-ingest)",
                source_name,
                buffer_filename,
                table_name,
                len(file_rows),
            )
    return {"promoted": promoted, "dropped": dropped, "rows_recovered": rows_recovered}


def list_fos_files(
    src: dict,
    prefix_subpath: str = "raw/",
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
    """List and discover new files in Fastly Object Storage.

    Yields progress dicts with format {"type": "status", "message": msg}
    or {"type": "error", "message": msg}.

    Returns a dict with:
    {
        "new_files": list[str],
        "file_sizes": dict[str, int],
        "skipped_already": int,
        "stranded_already": list[str]
    }
    """
    if elapsed_fn is None:

        def default_elapsed():
            return ""

        elapsed_fn = default_elapsed

    st_dt = None
    et_dt = None
    from backend.utils.date_utils import parse_iso_utc

    if start_time:
        st_dt = parse_iso_utc(start_time)
    if end_time:
        et_dt = parse_iso_utc(end_time)

    already = already_ingested or set()

    # Determine StartAfter marker for incremental discovery to avoid scanning the entire bucket.
    start_after_key = None
    if st_dt and not already:
        # Use the configured start to bound the FOS list only on the very first import
        # (no previously ingested files). On subsequent cron runs `already` is non-empty,
        # so we fall through to the incremental lookback — scanning only the last 4 hours
        # instead of the entire bucket from the original import start date.
        start_after_key = st_dt.strftime("raw/%Y-%m-%d/%H/")
        logger.info("[ingest] %s: Using requested start_time to bound FOS scan: %s", src.get("name"), start_after_key)
    elif incremental_only and already:
        try:
            start_after_key = _compute_incremental_start_after(already, lookback_hours=4)
        except Exception as e:
            logger.warning(
                "[ingest] %s: Failed to calculate lookback marker, scanning full bucket: %s", src.get("name"), e
            )

    from backend.core.fastly.mock_fixtures import is_mock_mode

    if fos_client is None:
        fos_client = _get_fos_client(src)
    file_sizes: dict[str, int] = {}
    new_files: list[str] = []
    skipped_already = 0
    stranded_already: list[str] = []
    total_listed = 0

    try:
        prefix_path = src.get("prefix", "").strip("/")
        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
        raw_prefix = f"{prefix_path}/{prefix_subpath}" if prefix_path else prefix_subpath

        kwargs = {"Bucket": src["bucket"], "Prefix": raw_prefix}
        if start_after_key:
            kwargs["StartAfter"] = start_after_key

        yield {"type": "status", "message": f"{elapsed_fn()} Discovering new files in Fastly Object Storage..."}

        pages = [] if is_mock_mode() else paginator.paginate(**kwargs)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".gz"):
                    continue

                if exclude_prefix_subpath:
                    exclude_prefix = (
                        f"{prefix_path}/{exclude_prefix_subpath}" if prefix_path else exclude_prefix_subpath
                    )
                    if key.startswith(exclude_prefix):
                        continue

                total_listed += 1
                if total_listed % 10000 == 0:
                    msg = f"{elapsed_fn()} Discovered {total_listed:,} new files..."
                    yield {"type": "status", "message": msg}

                fname = key.split("/")[-1]
                file_dt = _parse_fastly_filename_dt(fname)
                if file_dt is not None:
                    if et_dt and file_dt > (et_dt + timedelta(hours=1)):
                        break
                    if st_dt and file_dt < (st_dt - timedelta(hours=1)):
                        continue

                full_path = f"s3://{src['bucket']}/{key}"
                if full_path not in already:
                    new_files.append(full_path)
                    file_sizes[full_path] = obj["Size"]
                else:
                    skipped_already += 1
                    if delete_after and len(stranded_already) < _STRANDED_DELETE_CAP:
                        stranded_already.append(full_path)

            if et_dt and total_listed > 0:
                last_key = page.get("Contents", [])[-1]["Key"]
                last_dt = _parse_fastly_filename_dt(last_key.split("/")[-1])
                if last_dt is not None and last_dt > (et_dt + timedelta(hours=1)):
                    break

            if max_files and len(new_files) >= max_files:
                new_files = new_files[:max_files]
                break

    except Exception as e:
        yield {"type": "error", "message": f"Could not list FOS objects: {e}"}
        return {
            "new_files": [],
            "file_sizes": {},
            "skipped_already": 0,
            "stranded_already": [],
        }

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
    # entire bucket and may match arbitrarily old files.
    yield {"type": "status", "message": f"{elapsed()} Fetching ingestion history..."}

    dedup_limit: int | None = 200_000 if incremental_only else None
    already = metadata_db.get_ingested_filenames(source_name, limit=dedup_limit)

    # Use list_fos_files generator
    list_gen = list_fos_files(
        src=src,
        prefix_subpath="raw/",
        already_ingested=already,
        start_time=start_time,
        end_time=end_time,
        incremental_only=incremental_only,
        max_files=max_files,
        elapsed_fn=elapsed,
        delete_after=delete_after,
        fos_client=fos_client,
    )
    try:
        while True:
            evt = next(list_gen)
            yield evt
    except StopIteration as e:
        list_res = e.value

    new_files = list_res["new_files"]
    file_sizes = list_res["file_sizes"]
    skipped_already = list_res["skipped_already"]
    stranded_already = list_res["stranded_already"]

    # Reconcile interrupted deletes. Any file we LISTed that is already in the
    # ledger is, by the delete_after contract, one we ingested but never finished
    # deleting (a restart hit between the ledger write and the FOS delete). Re-issue
    # the delete now. This makes deletion self-healing: every sync re-derives the
    # strand from (bucket LIST ∩ ledger), so a delete interrupted at any point is
    # retried on the next tick — idempotently (deleting an absent key is a no-op),
    # with no schema, no extra LIST, and no wait for the 1-day ledger trim.
    # full_sync LISTs the whole bucket so it catches strands of any age; the
    # incremental sync's 4h lookback catches recent ones within a tick.
    reclaimed = 0
    if delete_after and stranded_already:
        # Throttle the high-frequency incremental path so a persistently-failing
        # delete can't hammer FOS every tick; full_sync always runs (it is the
        # backstop for old/over-cap strands).
        throttled = False
        if incremental_only:
            now = time.time()
            last = _reconcile_last_attempt.get(source_name, 0.0)
            if now - last < _RECONCILE_MIN_INTERVAL_S:
                throttled = True
            else:
                _reconcile_last_attempt[source_name] = now

        if not throttled:
            # Default-deny: only delete strands we can POSITIVELY prove are safe —
            # durable data (row_count>0) AND ingested at/after the durability epoch
            # (so row_count is trustworthy). This excludes both no-data markers
            # (row_count==0; their raw .gz may be the only copy) and pre-fix ledger
            # rows whose row_count can't be trusted. Anything not proven safe is
            # left for the normal 1-day ledger trim.
            try:
                deletable = metadata_db.get_reclaimable_strand_filenames(
                    source_name, set(stranded_already), _RECONCILE_LEDGER_EPOCH
                )
            except Exception as classify_err:
                # Fail SAFE: if we can't classify, reclaim nothing this run.
                logger.warning(
                    "[ingest] %s: could not classify reclaimable strands (%s) — skipping reclaim this run",
                    display_name,
                    classify_err,
                )
                deletable = set()

            stranded_keys = [
                p[len(f"s3://{src['bucket']}/") :]
                for p in stranded_already
                if p in deletable and p.startswith(f"s3://{src['bucket']}/")
            ]
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
                        display_name,
                        recl_err,
                    )

    if not new_files:
        msg = "Already up to date — no new files to ingest."
        if reclaimed:
            msg = f"No new files; reclaimed {reclaimed} raw file(s) left by an interrupted prior run."
        yield {
            "type": "done",
            "new_files": 0,
            "skipped_files": skipped_already,
            "rows_inserted": 0,
            "deleted_files": reclaimed,
            "message": msg,
        }
        return

    chunk_size = INGEST_CHUNK_SIZE
    total_inserted = 0
    total_corrupt = 0
    total_corrupt_details: list[str] = []
    total_quarantined = 0
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
            # Cooperative yield: if API requests came in mid-ingest, pause
            # briefly so the dashboard query gets CPU before the next chunk's
            # CREATE TEMP TABLE + Arrow export burns it. The 30s starvation
            # guard at the top-of-tick gate is the backstop for sustained
            # API load — this helper just smooths the per-chunk contention.
            yield_to_api()
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
                    safe_fid = fid.replace('"', '""')
                    if fid in _DECODE_EXPRS:
                        field_selects.append(f'{_DECODE_EXPRS[fid]} AS "{safe_fid}"')
                    else:
                        field_selects.append(f'"{safe_fid}"')
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
                _chunk_corrupt_s3_paths: list[str] = []
                _chunk_truly_corrupt: list = []
                _chunk_valid_counts: dict[str, int] = {}
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
                            truly_corrupt: list[tuple[str, str, str]] = []
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
                                    truly_corrupt.append((fname, raw_line, "missing_timestamp"))
                                except (json.JSONDecodeError, ValueError):
                                    truly_corrupt.append((fname, raw_line, "invalid_json"))

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
                                for fname, raw_line, _reason in truly_corrupt[: 100 - len(total_corrupt_details)]:
                                    short_name = fname.split("?")[0].split("/")[-1]
                                    total_corrupt_details.append(f"[{short_name}] {raw_line.strip()[:2000]}")

                            _chunk_corrupt_s3_paths = corrupt_s3_paths
                            _chunk_truly_corrupt = truly_corrupt
                            _chunk_valid_counts = valid_counts
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

                if _chunk_corrupt_s3_paths and delete_after:
                    try:
                        total_quarantined += _quarantine_corrupt_files(
                            fos_client,
                            src["bucket"],
                            src,
                            _chunk_corrupt_s3_paths,
                            _chunk_truly_corrupt,
                            count_map,
                            _chunk_valid_counts,
                            file_sizes,
                            source_name,
                        )
                    except Exception as qe:
                        logger.warning("[ingest] %s: quarantine failed: %s", source_name, qe)
            finally:
                chunk_tmpdir_obj.cleanup()

            good_files = [f for f in chunk if f not in failed_paths]
            # Files that actually contributed DURABLE rows (survived the time-range
            # WHERE filter and have a non-null timestamp = rows really written to the
            # buffer/Iceberg). A file with zero durable rows produced NO stored data;
            # we still ledger it to suppress re-LIST, but record row_count 0 so the
            # later strand RECONCILE can tell it apart from a genuine interrupted
            # delete and skip it (its raw .gz may be the only copy). Note this guards
            # only the reconcile: on THIS run the per-chunk delete below still removes
            # the raw under delete_after=True — that is the configured "delete after
            # processing" contract for out-of-range / corrupt files, unchanged here.
            # Per-file: a buffered chunk can still hold a file whose own rows were all
            # filtered out, and count_map holds PRE-filter counts, so durability can't
            # be inferred from count_map alone.
            try:
                durable_files = {
                    r[0]
                    for r in mem_con.execute(
                        "SELECT DISTINCT _source_file FROM _ingest_staging WHERE timestamp IS NOT NULL"
                    ).fetchall()
                    if r[0] is not None
                }
            except Exception:
                # Staging table unavailable (chunk errored earlier). Fail toward NO
                # DATA LOSS: treat every file as a zero-row marker so the reconcile
                # will never delete its raw .gz. Worst case is a benign raw-file leak
                # (and a small summary under-count) on a path that, in practice, never
                # fires once a chunk has been processed — never an erroneous delete.
                durable_files = set()
            rows_to_track = [
                (f, count_map.get(f, 0) if f in durable_files else 0, file_sizes.get(f)) for f in good_files
            ]

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
                buf_filename = _deterministic_buffer_name(good_files)
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
            successfully_processed_files.extend(good_files)

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

                    try:
                        future = _delete_executor.submit(_do_delete, chunk_keys, src["bucket"], fos_client)
                        _pending_deletes.append(future)
                        yield {
                            "type": "status",
                            "message": f"{elapsed()} Batch {chunk_num}: Submitted deletion of {len(chunk_keys)} raw files (async)...",
                        }
                    except RuntimeError as submit_err:
                        # Python's concurrent.futures.thread module-global
                        # ``_shutdown`` flag has been flipped (its atexit
                        # cleanup fires once and is process-wide), so every
                        # subsequent ``.submit()`` raises "cannot schedule
                        # new futures after interpreter shutdown" — even on
                        # a fresh ThreadPoolExecutor. The cause has been
                        # observed under uvicorn worker recycling and some
                        # multiprocessing-using libraries.
                        #
                        # Without this fallback the ingest loop crashes here
                        # AFTER the chunk's rows are already written to the
                        # buffer + recorded in ``ingested_files``, so the
                        # .gz files stay in FOS forever as orphans (the dedup
                        # check makes them invisible to every future sync —
                        # 2026-06-16 incident: 167 orphans across 4 hours).
                        # Fall back to a synchronous inline delete: slower
                        # per chunk, but no orphans leaked.
                        logger.warning(
                            "[ingest] %s: delete executor submit failed (%s) — falling back to inline delete",
                            source_name,
                            submit_err,
                        )
                        try:
                            inline_deleted = _delete_objects_robust(fos_client, src["bucket"], chunk_keys)
                            deleted += inline_deleted
                            yield {
                                "type": "status",
                                "message": f"{elapsed()} Batch {chunk_num}: Deleted {inline_deleted} raw files inline (executor unavailable).",
                            }
                        except Exception as inline_err:
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

    total_deleted = deleted + reclaimed
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
        "message": (
            f"Successfully ingested {processed_count} new files ({total_inserted} rows) "
            f"and deleted {total_deleted} raw files{reclaimed_note}."
        ),
        "touched_hours": list(touched_hours),
    }
