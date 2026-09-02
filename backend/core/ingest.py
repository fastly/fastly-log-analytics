import concurrent.futures
import gzip
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


_MINUTE_KEY_RE = re.compile(r"/raw/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/")
# Slashed layout written by the canonical template (see
# backend/provision/log_paths.py): raw/%Y/%m/%d/%H/analytics_log_%M.json.gz
# plus a Fastly-appended "<ISO-with-colons>-<unique>.log.gz" suffix.
_SLASHED_KEY_RE = re.compile(r"/raw/(\d{4})/(\d{2})/(\d{2})/(\d{2})/[^/]*?_(\d{2})\.json\.gz")
# Old dash layout: raw/%Y-%m-%d/%H/<timestamped filename>
_DASH_KEY_RE = re.compile(r"/raw/(\d{4}-\d{2}-\d{2})/(\d{2})/")


def _parse_key_layout_dt(key: str) -> tuple[str, datetime] | None:
    """Classify a raw-log key's layout and extract its timestamp.

    Returns ``("minute"|"slashed"|"dash", dt)`` or None for keys in no known layout.
    Minute precision comes from the filename where available; the dash
    layout falls back to the leading filename timestamp (hour dir otherwise).
    """
    m = _MINUTE_KEY_RE.search(key)
    if m:
        try:
            y, mo, d, h, mi = (int(g) for g in m.groups())
            return ("minute", datetime(y, mo, d, h, mi, tzinfo=UTC))
        except ValueError:
            return None
    m = _SLASHED_KEY_RE.search(key)
    if m:
        try:
            y, mo, d, h, mi = (int(g) for g in m.groups())
            return ("slashed", datetime(y, mo, d, h, mi, tzinfo=UTC))
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


def _compute_incremental_start_after(already: set[str], lookback_hours: int = 4) -> str | None:
    """Derive an S3 ``StartAfter`` key from previously-ingested filenames.

    The cron's incremental mode skips most of the bucket by passing a
    ``StartAfter`` bound derived from the most recent file we already
    have, minus a small lookback to catch late-arriving POP logs. Without
    this, every cron run would scan the entire bucket from epoch.

    Layout-aware: the bound is formatted in the bucket's key layout. The
    layout of the latest file determines the StartAfter format, as old
    layouts are static and new files only arrive in the current layout.

    Returns None when ``already`` is empty or no key matches either layout
    (the caller then falls back to a full scan).
    """
    parsed = [p for f in already if "/raw/" in f and (p := _parse_key_layout_dt(f)) is not None]
    if not parsed:
        return None
    latest_dt = max(dt for _, dt in parsed)
    lookback = latest_dt - timedelta(hours=lookback_hours)

    layouts = {layout for layout, _ in parsed}
    if "dash" in layouts:
        return lookback.strftime("raw/%Y-%m-%d/%H/")
    elif "slashed" in layouts:
        return lookback.strftime("raw/%Y/%m/%d/%H/")
    else:
        return lookback.strftime("raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")


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
                err_str = str(e)
                if "(502)" in err_str or "(503)" in err_str:
                    logger.debug("[ingest] transient FOS error downloading %s: %s", p, err_str)
                else:
                    logger.warning("[ingest] failed to download %s: %s", p, err_str)

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
            # StartAfter must live in the same keyspace as Prefix: with a
            # non-empty fos_prefix, a bare "raw/…" bound sorts after every
            # "<prefix>/raw/…" key and the listing would return nothing.
            if prefix_path:
                start_after_key = f"{prefix_path}/{start_after_key}"
            kwargs["StartAfter"] = start_after_key

        yield {"type": "status", "message": f"{elapsed_fn()} Discovering new files in Fastly Object Storage..."}

        pages = paginator.paginate(**kwargs)
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
    queue_only: bool = False,
    specific_files: list[str] | None = None,
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
        try:
            del _fetched
        except NameError:
            pass
        try:
            del arrow_table
        except NameError:
            pass
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


# ─── Celery ingest data plane (v3) ────────────────────────────────────────────
# Discovery (cron, inline on a worker) LISTs recent minute prefixes and inserts
# new object keys into the per-service ``ingest_ledger``
# (discovered → claimed → committed | quarantined), then fans out
# ``convert_batch_files`` tasks of LEDGER_CONVERT_BATCH_SIZE keys each — one
# DuckLake catalog commit per BATCH, since the catalog transaction (one
# snapshot row per commit), not celery dispatch, is what limits throughput.
# ``convert``/``convert_batch_files`` are idempotent (delete-then-insert
# keyed on ``_source_file`` inside one DuckLake transaction), so at-least-once
# delivery (acks_late) and sweeper reclaims are safe. ``sweep_ledger`` is the
# crash net: reclaims stale claims, re-dispatches stuck rows, and diffs a
# lookback LIST against the ledger.

from backend.celery_app import app

# A claim older than this is presumed dead and reclaimed by the sweeper.
# Must exceed celery's task_time_limit so a still-running convert isn't
# reclaimed mid-flight (see backend/celery_app.py).
LEDGER_RECLAIM_AFTER_S = 20 * 60
# Rows still 'discovered' after this long had their convert message lost
# (broker restart, dispatch-before-crash) — re-dispatch them.
LEDGER_REDISPATCH_AFTER_S = 10 * 60
# Give up on a file after this many failed convert attempts.
LEDGER_MAX_ATTEMPTS = 5
# Files per ``convert_batch_files`` task. The DuckLake catalog commit — one
# Postgres transaction and one snapshot ROW per commit — is the ingest
# bottleneck, not celery dispatch: a live test service showed 27,613
# committed files (avg 665 bytes, ~7 log lines each) against 27,615
# snapshots, i.e. exactly one snapshot per file, growing the snapshot table
# linearly with file count forever. Batching the DELETE+INSERT for N files
# into ONE transaction collapses that to one snapshot per batch. 50 matches
# the sync path's chunk precedent and lands near one minute of arrivals at
# the measured ~44 files/min per service.
LEDGER_CONVERT_BATCH_SIZE = int(os.getenv("LEDGER_CONVERT_BATCH_SIZE", "50"))
# Bound parameters per claim/commit statement. SQLite caps them per
# statement (999 on older builds), so a caller handing in more keys than one
# batch is sliced rather than raising.
_LEDGER_KEY_SLICE = 400


def _drain_generator(gen):
    """Exhaust a generator, returning its StopIteration ``return`` value."""
    while True:
        try:
            next(gen)
        except StopIteration as e:
            return e.value


def _celery_ingest_scope(kind: str, service_id: str):
    """Telemetry envelope for ledger tasks: process-context tagging + FOS
    call tracking + usage-log flush — the same accounting the ``@cron_task``
    decorator gives scheduler jobs. Without it every LIST/GET these tasks
    perform is invisible to the Class A/B cost accounting."""
    from contextlib import contextmanager

    from backend.utils.telemetry import process_context_scope, start_call_tracking
    from backend.utils.usage_logger import flush_usage_log

    @contextmanager
    def _scope():
        with process_context_scope(f"cron:{kind}:{service_id}"):
            start_call_tracking()
            try:
                yield
            finally:
                flush_usage_log(service_id)

    return _scope()


def discover_prefix(service_id: str, prefix_subpath: str | None = None, start_time: str | None = None) -> int:
    """LIST one FOS prefix (or a start_time-bounded range), insert unseen keys
    into ``ingest_ledger`` as ``discovered``, and dispatch the new keys in
    ``LEDGER_CONVERT_BATCH_SIZE``-sized ``convert_batch_files`` batches (one
    DuckLake catalog commit each). Returns the number of newly discovered files.

    The ledger insert is committed BEFORE any dispatch so a worker's claim
    UPDATE can never race an uncommitted row (claim matches 0 rows → the file
    would strand in 'discovered' with its message already consumed).
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return 0
    src = get_source_for_service(service_id)
    if src is None:
        return 0

    list_kwargs: dict = {"start_time": start_time}
    if prefix_subpath is not None:
        list_kwargs["prefix_subpath"] = prefix_subpath
    fos_result = _drain_generator(list_fos_files(src, **list_kwargs))
    if not fos_result:
        logger.warning("[ledger] %s: FOS LIST returned nothing (prefix=%s)", service_id, prefix_subpath)
        return 0
    new_files = fos_result.get("new_files") or []
    if not new_files:
        return 0

    bucket = src.get("bucket", "")
    s3_prefix = f"s3://{bucket}/"

    con = get_con(service_id)
    cur = con.cursor()
    newly_discovered: list[str] = []
    for f_path in new_files:
        f_size = fos_result.get("file_sizes", {}).get(f_path, 0)
        object_key = f_path[len(s3_prefix) :] if f_path.startswith(s3_prefix) else f_path
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, discovered_at) "
            "VALUES (?, ?, ?, 'discovered', ?) ON CONFLICT DO NOTHING",
            (service_id, object_key, f_size, time.time()),
        )
        if cur.rowcount > 0:
            newly_discovered.append(object_key)
    con.commit()

    batches = 0
    for i in range(0, len(newly_discovered), LEDGER_CONVERT_BATCH_SIZE):
        convert_batch_files.delay(service_id, newly_discovered[i : i + LEDGER_CONVERT_BATCH_SIZE])
        batches += 1
    if newly_discovered:
        logger.info(
            "[ledger] %s: discovered %d new file(s), dispatched %d convert batch(es)",
            service_id,
            len(newly_discovered),
            batches,
        )
    return len(newly_discovered)


def _ledger_record_failure(con, service_id: str, object_key: str, error: str) -> str:
    """Record a convert failure: bump attempts, keep the error, and either
    requeue ('discovered') or give up ('quarantined') after LEDGER_MAX_ATTEMPTS.
    Returns the new status."""
    cur = con.cursor()
    cur.execute(
        "UPDATE ingest_ledger SET attempts = attempts + 1, last_error = ?, "
        "status = CASE WHEN attempts + 1 >= ? THEN 'quarantined' ELSE 'discovered' END, "
        "claimed_by = NULL, claimed_at = NULL "
        "WHERE service_id = ? AND object_key = ?",
        (error[:500], LEDGER_MAX_ATTEMPTS, service_id, object_key),
    )
    con.commit()
    row = con.execute(
        "SELECT status, attempts FROM ingest_ledger WHERE service_id = ? AND object_key = ?",
        (service_id, object_key),
    ).fetchone()
    status = row[0] if row else "unknown"
    if status == "quarantined":
        logger.error(
            "[ledger] %s: %s QUARANTINED after %s failed convert attempts: %s", service_id, object_key, row[1], error
        )
    else:
        logger.warning("[ledger] %s: convert failed for %s (attempt %s): %s", service_id, object_key, row[1], error)
    return status


def convert_object(service_id: str, object_key: str, worker_id: str) -> str:
    """Claim one ledger row, download the file, and insert its rows into the
    per-service DuckLake table. Returns the terminal ledger status.

    Idempotent: rows for this file are DELETEd (keyed on ``_source_file``)
    inside the same DuckLake transaction as the INSERT, so redelivery or a
    sweeper reclaim after a crash-between-insert-and-ack cannot duplicate data.
    """
    import duckdb as _duckdb

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
    from backend.core.metadata.base import get_con

    con_meta = get_con(service_id)
    cur = con_meta.cursor()

    cur.execute(
        "UPDATE ingest_ledger SET status='claimed', claimed_by=?, claimed_at=? "
        "WHERE service_id=? AND object_key=? AND status='discovered'",
        (worker_id, time.time(), service_id, object_key),
    )
    claimed = cur.rowcount > 0
    con_meta.commit()
    if not claimed:
        return "not_claimed"

    src = get_source_for_service(service_id)
    if src is None:
        _ledger_record_failure(con_meta, service_id, object_key, "no source registered for service")
        return "error"

    bucket = src.get("bucket", "")
    s3_path = f"s3://{bucket}/{object_key}"
    safe_source_file = escape_sql_literal(s3_path)

    duckdb_con = None
    try:
        fos = boto3_client_hot() if svcconfig.HOT_S3_ENDPOINT else _get_fos_client(src)
        # Workers must NOT open the per-service .duckdb FILE: DuckDB is
        # single-writer-per-file across processes, so a convert holding it
        # starves the backend's dashboard readers ("Database is locked by
        # another process" 503s). An in-memory connection with FOS creds +
        # the transactional DuckLake catalog is all a convert needs.
        duckdb_con = _duckdb.connect()
        _configure_fos(duckdb_con, src)
        if not _ducklake_attach(duckdb_con, src, read_only=False):
            raise RuntimeError("DuckLake read-write attach failed")
        table = ducklake_table_name(src)

        with tempfile.TemporaryDirectory() as tmpdir:
            s3_to_local, _ = _download_chunk_to_local(fos, [s3_path], tmpdir)
            local_file = s3_to_local.get(s3_path)
            if not local_file:
                # Distinguish "object is gone" (terminal — e.g. a ledger row
                # resurrected for a raw file the old pipeline already ingested
                # and deleted) from a transient download failure (retryable).
                # Without this, every dead key burns LEDGER_MAX_ATTEMPTS GETs.
                try:
                    fos.head_object(Bucket=bucket, Key=object_key)
                except Exception as head_err:
                    if "404" in str(head_err) or "Not Found" in str(head_err) or "NoSuchKey" in str(head_err):
                        cur.execute(
                            "UPDATE ingest_ledger SET status='dead_letter', "
                            "last_error='object missing from FOS (already ingested+deleted, or expired)' "
                            "WHERE service_id=? AND object_key=?",
                            (service_id, object_key),
                        )
                        con_meta.commit()
                        logger.info("[ledger] %s: %s gone from FOS — dead_letter", service_id, object_key)
                        return "dead_letter"
                raise RuntimeError("download failed (object still exists — transient)")

            log_fields_config = (svcconfig.load_config(service_id) or {}).get("log_fields")
            columns_sql = get_ingest_columns_sql(log_fields_config)
            safe_local = escape_sql_literal(local_file)
            read_expr = (
                f"read_json_auto('{safe_local}', format='newline_delimited', "
                f"records='auto', columns={columns_sql}, ignore_errors=true)"
            )
            # ignore_errors=true does NOT drop a malformed line — it inserts
            # it with every field NULL (verified empirically: a truncated
            # JSON object comes back as a row of NULLs, not a skipped row).
            # Without this filter that garbage row lands straight in the
            # queryable lake table. WHERE timestamp IS NOT NULL is the same
            # invariant the sync path enforces before writing its buffer
            # (ingest.py's `_ingest_staging` reads, e.g. the one behind
            # `_fetched = ... WHERE timestamp IS NOT NULL`).
            clean_read_expr = f"(SELECT * FROM {read_expr} WHERE timestamp IS NOT NULL)"

            table_exists = bool(
                duckdb_con.execute(
                    "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
                    (table,),
                ).fetchone()
            )

            needs_insert = True
            if not table_exists:
                try:
                    duckdb_con.execute(
                        f"CREATE TABLE lake.{table} AS "
                        f"SELECT *, '{safe_source_file}' AS _source_file FROM {clean_read_expr}"
                    )
                    needs_insert = False  # created + inserted in one step
                except Exception as e:
                    if "already exists" not in str(e):
                        raise
                    # Lost a create race — fall through to the insert path.

            if needs_insert:
                # Widen the table for any new (custom-field) columns first.
                try:
                    existing_cols = {r[0] for r in duckdb_con.execute(f"DESCRIBE lake.{table}").fetchall()}
                    for p_col, p_type, *_ in duckdb_con.execute(
                        f"DESCRIBE SELECT * FROM {read_expr} LIMIT 0"
                    ).fetchall():
                        if p_col not in existing_cols:
                            safe_col = p_col.replace('"', '""')
                            duckdb_con.execute(f'ALTER TABLE lake.{table} ADD COLUMN "{safe_col}" {p_type}')
                except Exception as e:
                    logger.warning("[ledger] %s: schema sync failed for %s: %s", service_id, object_key, e)

                duckdb_con.execute("BEGIN TRANSACTION")
                try:
                    duckdb_con.execute(f"DELETE FROM lake.{table} WHERE _source_file = '{safe_source_file}'")
                    duckdb_con.execute(
                        f"INSERT INTO lake.{table} BY NAME "
                        f"SELECT *, '{safe_source_file}' AS _source_file FROM {clean_read_expr}"
                    )
                    duckdb_con.execute("COMMIT")
                except Exception:
                    try:
                        duckdb_con.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

            # A NULL-timestamp row (see clean_read_expr above) is what
            # ignore_errors=true produces for a malformed line — check for
            # that now rather than let it disappear with zero trace once
            # excluded from the insert. Best-effort: a failure here must
            # never fail the convert itself (the valid rows are already
            # durably committed).
            try:
                _quarantine_convert_corrupt_lines(duckdb_con, fos, src, read_expr, local_file, object_key)
            except Exception as qe:
                logger.warning("[ledger] %s: quarantine check failed for %s: %s", service_id, object_key, qe)

            # Mirror the sync path's ingested_files bookkeeping so every
            # existing reader (Usage Log / log-line-accounting
            # reconciliation, the admin ingested-files list, the historical
            # dedup path) keeps working unchanged for celery-mode services
            # instead of silently reading zero. Best-effort: never fails
            # the convert — the ledger remains the authoritative record.
            try:
                count_row = duckdb_con.execute(
                    f"SELECT count(*) FROM {read_expr} WHERE timestamp IS NOT NULL"
                ).fetchone()
                row_count = count_row[0] if count_row else 0
                metadata_db.insert_ingested_files(service_id, [(object_key, row_count, os.path.getsize(local_file))])
            except Exception as ie:
                logger.warning("[ledger] %s: ingested_files bookkeeping failed for %s: %s", service_id, object_key, ie)
    except Exception as e:
        return _ledger_record_failure(con_meta, service_id, object_key, str(e))
    finally:
        if duckdb_con is not None:
            duckdb_con.close()

    cur.execute(
        "UPDATE ingest_ledger SET status='committed', committed_at=? "
        "WHERE service_id=? AND object_key=? AND status='claimed'",
        (time.time(), service_id, object_key),
    )
    con_meta.commit()
    return "committed"


def _ledger_claim_batch(con, service_id: str, object_keys: list[str], worker_id: str) -> list[str]:
    """Claim every still-``discovered`` key in ``object_keys`` with one
    UPDATE per ``_LEDGER_KEY_SLICE`` slice, returning the keys actually won.

    Keys NOT returned were claimed by another worker or already committed —
    the batch's equivalent of ``convert_object``'s ``"not_claimed"``. The
    claim is committed before the caller downloads anything, same rationale
    as the single-key claim: a worker's claim must never race an
    uncommitted row.
    """
    cur = con.cursor()
    now = time.time()
    won: list[str] = []
    for i in range(0, len(object_keys), _LEDGER_KEY_SLICE):
        key_slice = object_keys[i : i + _LEDGER_KEY_SLICE]
        placeholders = ",".join("?" * len(key_slice))
        cur.execute(
            "UPDATE ingest_ledger SET status='claimed', claimed_by=?, claimed_at=? "
            f"WHERE service_id=? AND object_key IN ({placeholders}) AND status='discovered' "
            "RETURNING object_key",
            (worker_id, now, service_id, *key_slice),
        )
        won.extend(r[0] for r in cur.fetchall())
    con.commit()
    return won


def convert_batch_objects(service_id: str, object_keys: list[str], worker_id: str) -> dict:
    """Batched sibling of ``convert_object``: claim N ledger rows, download
    them all, and land every row in ONE DuckLake transaction — one catalog
    snapshot for the whole batch instead of one per file.

    Per-row ``_source_file`` attribution is preserved (idempotency's
    DELETE-by-``_source_file`` and the ledger both depend on it) by reading
    the whole batch through a single ``read_json_auto([...], filename=true)``
    and joining that ``filename`` against a per-connection TEMP table
    mapping local temp path -> ``s3://`` key.

    Failure isolation is per file where it can be: a key that is gone from
    FOS is dead-lettered and DROPPED from the batch so it cannot strand its
    healthy siblings. A failure of the shared transaction, by contrast,
    routes EVERY still-claimed key through ``_ledger_record_failure`` so
    none is left stranded in ``claimed``.

    Returns a summary dict of per-outcome counts.
    """
    import duckdb as _duckdb

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
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
        return summary

    con_meta = get_con(service_id)
    cur = con_meta.cursor()

    claimed_keys = _ledger_claim_batch(con_meta, service_id, object_keys, worker_id)
    summary["claimed"] = len(claimed_keys)
    summary["not_claimed"] = len(object_keys) - len(claimed_keys)
    if not claimed_keys:
        return summary

    src = get_source_for_service(service_id)
    if src is None:
        for object_key in claimed_keys:
            _ledger_record_failure(con_meta, service_id, object_key, "no source registered for service")
        summary["failed"] = len(claimed_keys)
        return summary

    bucket = src.get("bucket", "")
    # Keys this batch still owns. A key removed here has already been given
    # its own terminal ledger status, so the batch-failure path below must
    # not touch it again.
    active: list[str] = list(claimed_keys)
    duckdb_con = None
    try:
        fos = boto3_client_hot() if svcconfig.HOT_S3_ENDPOINT else _get_fos_client(src)
        # Same single-writer-per-file rationale as convert_object: a worker
        # holding the per-service .duckdb FILE starves the backend's
        # dashboard readers into 503s. In-memory connection only.
        duckdb_con = _duckdb.connect()
        _configure_fos(duckdb_con, src)
        if not _ducklake_attach(duckdb_con, src, read_only=False):
            raise RuntimeError("DuckLake read-write attach failed")
        table = ducklake_table_name(src)

        with tempfile.TemporaryDirectory() as tmpdir:
            s3_to_local, _ = _download_chunk_to_local(fos, [f"s3://{bucket}/{k}" for k in claimed_keys], tmpdir)

            # (local_path, s3_path, object_key) for every key that actually
            # downloaded. One dead key must not strand its 49 siblings, so
            # each undownloadable key gets convert_object's dead-vs-transient
            # split here and is then excluded from the batch.
            files: list[tuple[str, str, str]] = []
            for object_key in claimed_keys:
                s3_path = f"s3://{bucket}/{object_key}"
                local_file = s3_to_local.get(s3_path)
                if local_file:
                    files.append((local_file, s3_path, object_key))
                    continue
                active.remove(object_key)
                gone = False
                try:
                    fos.head_object(Bucket=bucket, Key=object_key)
                except Exception as head_err:
                    err = str(head_err)
                    gone = "404" in err or "Not Found" in err or "NoSuchKey" in err
                if gone:
                    cur.execute(
                        "UPDATE ingest_ledger SET status='dead_letter', "
                        "last_error='object missing from FOS (already ingested+deleted, or expired)' "
                        "WHERE service_id=? AND object_key=?",
                        (service_id, object_key),
                    )
                    con_meta.commit()
                    summary["dead_letter"] += 1
                    logger.info("[ledger] %s: %s gone from FOS — dead_letter", service_id, object_key)
                else:
                    _ledger_record_failure(
                        con_meta, service_id, object_key, "download failed (object still exists — transient)"
                    )
                    summary["failed"] += 1

            if not files:
                return summary

            log_fields_config = (svcconfig.load_config(service_id) or {}).get("log_fields")
            columns_sql = get_ingest_columns_sql(log_fields_config)
            local_files = [lf for lf, _s3, _k in files]
            file_list_sql = "[" + ", ".join(f"'{escape_sql_literal(lf)}'" for lf in local_files) + "]"
            # filename=true is the linchpin: it gives per-ROW source
            # attribution from a single statement over the whole file list,
            # which is what makes one transaction for N files possible.
            read_expr = (
                f"read_json_auto({file_list_sql}, format='newline_delimited', "
                f"records='auto', columns={columns_sql}, ignore_errors=true, filename=true)"
            )

            # TEMP tables are per-connection so this cannot collide across
            # workers, but a reused connection could still carry a stale one.
            duckdb_con.execute("DROP TABLE IF EXISTS temp.srcmap")
            duckdb_con.execute("CREATE TEMP TABLE srcmap(local VARCHAR, s3 VARCHAR)")
            duckdb_con.execute(
                "INSERT INTO temp.srcmap VALUES "
                + ", ".join(f"('{escape_sql_literal(lf)}', '{escape_sql_literal(s3)}')" for lf, s3, _k in files)
            )

            # Per-file valid/corrupt counts, computed BEFORE the transaction
            # so the same numbers drive both the ingested_files bookkeeping
            # (which must be per file, not the batch total, or log-line
            # accounting reconciliation goes wrong) and the per-file
            # quarantine pass. Reading the same immutable local files twice
            # yields the same answer.
            stats = duckdb_con.execute(
                "SELECT filename, count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) "
                f"FROM {read_expr} GROUP BY filename"
            ).fetchall()
            valid_by_local = {r[0]: r[1] for r in stats}
            corrupt_by_local = {r[0]: r[2] for r in stats}
            unmapped = set(valid_by_local) - set(local_files)
            if unmapped:
                # DuckDB attributed rows to a path absent from srcmap, so
                # the JOIN below would silently drop them while their ledger
                # rows went 'committed'. Fail the batch instead of losing
                # rows without a trace.
                raise RuntimeError(f"read_json_auto returned unmapped filename(s): {sorted(unmapped)[:3]}")

            # WHERE timestamp IS NOT NULL: ignore_errors=true does NOT drop a
            # malformed line, it inserts a row with every field NULL, and
            # that garbage would land straight in the queryable lake table.
            select_sql = (
                "SELECT r.* EXCLUDE (filename), m.s3 AS _source_file "
                f"FROM {read_expr} r JOIN temp.srcmap m ON m.local = r.filename "
                "WHERE r.timestamp IS NOT NULL"
            )

            table_exists = bool(
                duckdb_con.execute(
                    "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
                    (table,),
                ).fetchone()
            )

            needs_insert = True
            if not table_exists:
                try:
                    duckdb_con.execute(f"CREATE TABLE lake.{table} AS {select_sql}")
                    needs_insert = False  # created + inserted in one step
                except Exception as e:
                    if "already exists" not in str(e):
                        raise
                    # Lost a create race — fall through to the insert path.

            if needs_insert:
                # Widen the table for any new (custom-field) columns first,
                # over the UNION of columns across the WHOLE batch so a
                # custom field present in only one file still widens.
                try:
                    existing_cols = {r[0] for r in duckdb_con.execute(f"DESCRIBE lake.{table}").fetchall()}
                    for p_col, p_type, *_ in duckdb_con.execute(
                        f"DESCRIBE SELECT * FROM {read_expr} LIMIT 0"
                    ).fetchall():
                        if p_col == "filename" or p_col in existing_cols:
                            continue  # filename is synthetic — never a lake column
                        safe_col = p_col.replace('"', '""')
                        duckdb_con.execute(f'ALTER TABLE lake.{table} ADD COLUMN "{safe_col}" {p_type}')
                except Exception as e:
                    logger.warning("[ledger] %s: batch schema sync failed (%d file(s)): %s", service_id, len(files), e)

                duckdb_con.execute("BEGIN TRANSACTION")
                try:
                    duckdb_con.execute(f"DELETE FROM lake.{table} WHERE _source_file IN (SELECT s3 FROM temp.srcmap)")
                    duckdb_con.execute(f"INSERT INTO lake.{table} BY NAME {select_sql}")
                    duckdb_con.execute("COMMIT")
                except Exception:
                    try:
                        duckdb_con.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

            # Quarantine per ORIGINATING file. The NULL-timestamp rows are
            # already excluded from the insert above, but they must still be
            # reported against the right object key — so re-read only the
            # files that actually contain them (rare) through the
            # single-file helper. Best-effort: the valid rows are already
            # durably committed and must never be failed by a reporting
            # problem.
            for local_file, _s3_path, object_key in files:
                if not corrupt_by_local.get(local_file):
                    continue
                try:
                    single_read_expr = (
                        f"read_json_auto('{escape_sql_literal(local_file)}', format='newline_delimited', "
                        f"records='auto', columns={columns_sql}, ignore_errors=true)"
                    )
                    _quarantine_convert_corrupt_lines(duckdb_con, fos, src, single_read_expr, local_file, object_key)
                except Exception as qe:
                    logger.warning("[ledger] %s: quarantine check failed for %s: %s", service_id, object_key, qe)

            # Mirror convert_object's ingested_files bookkeeping — one bulk
            # call, but with PER-FILE row counts (the batch total would
            # break Usage Log / log-line-accounting reconciliation).
            # Best-effort: the ledger remains the authoritative record.
            try:
                metadata_db.insert_ingested_files(
                    service_id,
                    [
                        (object_key, valid_by_local.get(local_file, 0), os.path.getsize(local_file))
                        for local_file, _s3_path, object_key in files
                    ],
                )
            except Exception as ie:
                logger.warning(
                    "[ledger] %s: ingested_files bookkeeping failed for %d file(s): %s", service_id, len(files), ie
                )
    except Exception as e:
        # The transaction is shared, so its failure is the whole batch's:
        # every key still claimed becomes retryable rather than stranded.
        for object_key in active:
            _ledger_record_failure(con_meta, service_id, object_key, str(e))
        summary["failed"] += len(active)
        return summary
    finally:
        if duckdb_con is not None:
            duckdb_con.close()

    for i in range(0, len(active), _LEDGER_KEY_SLICE):
        key_slice = active[i : i + _LEDGER_KEY_SLICE]
        placeholders = ",".join("?" * len(key_slice))
        cur.execute(
            "UPDATE ingest_ledger SET status='committed', committed_at=? "
            f"WHERE service_id=? AND object_key IN ({placeholders}) AND status='claimed'",
            (time.time(), service_id, *key_slice),
        )
    con_meta.commit()
    summary["committed"] = len(active)
    return summary


def _quarantine_convert_corrupt_lines(
    duckdb_con,
    fos_client,
    src: dict,
    read_expr: str,
    local_file: str,
    object_key: str,
) -> None:
    """Detect lines ``convert_object``'s ``ignore_errors=true`` read turned
    into an all-NULL row (verified empirically: DuckDB does NOT drop a
    malformed newline-delimited-JSON line under ``ignore_errors=true`` —
    it inserts a row of NULLs), and quarantine them — mirroring the sync
    path's ``_quarantine_corrupt_files`` protocol (upload the bad lines +
    a ``.meta.json`` sidecar to the ``errors/`` FOS prefix, register in
    ``quarantined_files``) so corrupt data is visible and recoverable
    instead of vanishing with zero trace. The caller excludes these same
    NULL-timestamp rows from the actual lake INSERT (see ``clean_read_expr``
    at the call site) — this function only handles reporting them.

    Single-file scoped — the sync path's version also auto-repairs a
    specific empty-value malformation across a whole ingest batch; that
    repair pass isn't reproduced here, so a repairable line quarantines
    instead of being fixed. Silence, not silent data loss with no fix
    available, is the bar this closes.
    """
    service_id = src.get("service_id") or src.get("name", "")
    source_name = src.get("name", service_id)
    bucket = src.get("bucket", "")

    valid_count, corrupt_count = duckdb_con.execute(
        f"SELECT count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) FROM {read_expr}"
    ).fetchone()
    if not corrupt_count:
        return

    safe_local = escape_sql_literal(local_file)
    bad_rows = duckdb_con.execute(
        f"SELECT column0 FROM read_csv('{safe_local}', header=false, sep='', quote='', escape='', "
        f"columns={{'column0': 'VARCHAR'}}) "
        "WHERE NOT json_valid(column0) OR json_extract(column0, '$.timestamp') IS NULL"
    ).fetchall()
    bad_lines = [r[0].strip() for r in bad_rows if r[0] is not None]
    if not bad_lines:
        # DuckDB's row-level NULL count and this raw-line-level scan
        # disagree — different corruption detectors, kept independent on
        # purpose so a widened schema doesn't silently blind one of them.
        # Nothing concrete to attach to the FOS upload; log and move on.
        logger.warning(
            "[ledger] %s: %d NULL row(s) from %s but the raw-line scan found none",
            service_id,
            corrupt_count,
            object_key,
        )
        return

    prefix_path = src.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/raw/" if prefix_path else "raw/"
    errors_prefix = f"{prefix_path}/errors/" if prefix_path else "errors/"
    if not object_key.startswith(raw_prefix):
        return
    error_key = errors_prefix + object_key[len(raw_prefix) :].replace(".gz", ".bad.jsonl")
    meta_key = error_key + ".meta.json"
    file_name = object_key.rsplit("/", 1)[-1]

    reason_counts: dict[str, int] = {}
    for line in bad_lines:
        try:
            parsed = json.loads(line)
            reason = "missing_timestamp" if not parsed.get("timestamp") else "unknown"
        except (json.JSONDecodeError, ValueError):
            reason = "invalid_json"
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    error_body = "\n".join(bad_lines).encode()
    fos_client.put_object(Bucket=bucket, Key=error_key, Body=error_body, ContentType="application/x-ndjson")

    meta = {
        "original_key": object_key,
        "quarantined_at": datetime.now(UTC).isoformat(),
        "valid_rows": valid_count,
        "corrupt_rows": len(bad_lines),
        "total_rows": valid_count + corrupt_count,
        "corrupt_samples": [line[:2000] for line in bad_lines[:5]],
        "reason_counts": reason_counts,
        "source_name": source_name,
    }
    fos_client.put_object(Bucket=bucket, Key=meta_key, Body=json.dumps(meta).encode(), ContentType="application/json")

    metadata_db.insert_quarantined_file(
        service_id=service_id,
        file_name=file_name,
        source_name=source_name,
        fos_key=object_key,
        error_key=error_key,
        meta_key=meta_key,
        valid_rows=valid_count,
        corrupt_rows=len(bad_lines),
        file_size_bytes=None,
        corrupt_samples=[line[:2000] for line in bad_lines[:5]],
        reason_counts=reason_counts,
        error_size_bytes=len(error_body),
    )
    logger.warning(
        "[ledger] %s: quarantined %d corrupt row(s) from %s -> %s", service_id, len(bad_lines), object_key, error_key
    )


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


def sweep_ledger_once(service_id: str, lookback_hours: int = 4) -> dict:
    """Crash net for the ledger pipeline. Returns a summary dict.

    1. Reclaims rows stuck in 'claimed' past LEDGER_RECLAIM_AFTER_S (dead
       worker) back to 'discovered' — committed BEFORE re-dispatch so the
       worker's claim can't race the uncommitted reset.
    2. Re-dispatches rows stuck in 'discovered' past LEDGER_REDISPATCH_AFTER_S
       (their convert message was lost — broker restart, crash after insert).
    3. Diffs a lookback-window FOS LIST against the ledger for files discovery
       never saw.

    Excludes ``rum/raw/`` object keys — those are ``sweep_rum_ledger_once``'s
    job. Without this exclusion, a stale RUM row could get reclaimed/
    redispatched here first and misrouted to ``convert_batch_files.delay``
    (the regular-log parser) instead of ``convert_rum.delay``, corrupting the
    ``logs`` table with misparsed beacon data.
    """
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    cur = con.cursor()
    now = time.time()

    src = get_source_for_service(service_id)
    prefix_path = (src.get("prefix", "") if src else "").strip("/")
    rum_like_pattern = f"{prefix_path}/rum/raw/%" if prefix_path else "rum/raw/%"

    cur.execute(
        "UPDATE ingest_ledger SET status='discovered', claimed_by=NULL, claimed_at=NULL "
        "WHERE service_id=? AND status='claimed' AND claimed_at < ? AND object_key NOT LIKE ? "
        "RETURNING object_key",
        (service_id, now - LEDGER_RECLAIM_AFTER_S, rum_like_pattern),
    )
    reclaimed = [r[0] for r in cur.fetchall()]
    con.commit()

    stale_cutoff = now - LEDGER_REDISPATCH_AFTER_S
    stuck = [
        r[0]
        for r in con.execute(
            "SELECT object_key FROM ingest_ledger "
            "WHERE service_id=? AND status='discovered' AND object_key NOT LIKE ? "
            "AND (claimed_at IS NULL OR claimed_at < ?) LIMIT 5000",
            (service_id, rum_like_pattern, stale_cutoff),
        ).fetchall()
        if r[0] not in reclaimed
    ]

    # LOST-MESSAGE guard: re-dispatch exists for messages that vanished
    # (broker restart, crash before enqueue). If the ingest queue already
    # holds at least as many messages as the pending backlog, nothing is
    # lost — the workers just haven't gotten there yet. Re-dispatching
    # anyway multiplies duplicate messages every sweep during a long drain
    # (observed live: 5k pending → 25k queued after a few sweeps). Converts
    # are idempotent so duplicates are safe, but the queue bloat wastes
    # worker throughput and makes depth metrics meaningless. Dead-worker
    # redelivery is separately covered by acks_late + visibility_timeout.
    pending = len(stuck) + len(reclaimed)
    redispatched = 0
    if pending:
        try:
            from backend.celery_status import celery_queue_depths

            queues, broker_ok = celery_queue_depths()
            queue_depth = queues.get("q.ingest", 0) if broker_ok else 0
        except Exception:
            queue_depth = 0
        if queue_depth < pending:
            # Batched like discovery: the sweeper can re-dispatch up to 5000
            # keys, and one convert per key would mean 5000 DuckLake catalog
            # snapshots during a drain — exactly the cost this batching
            # exists to remove. The rum/raw/ exclusion lives on the queries
            # above, so batching the dispatch cannot leak a RUM key into the
            # regular-log parser.
            pending_keys = reclaimed + stuck
            for i in range(0, pending, LEDGER_CONVERT_BATCH_SIZE):
                convert_batch_files.delay(service_id, pending_keys[i : i + LEDGER_CONVERT_BATCH_SIZE])
            redispatched = pending
            logger.info(
                "[ledger] %s: sweeper re-dispatched %d pending row(s) (reclaimed=%d, queue_depth=%d)",
                service_id,
                pending,
                len(reclaimed),
                queue_depth,
            )
        else:
            logger.info(
                "[ledger] %s: %d pending row(s) but q.ingest already holds %d message(s) — nothing lost, skipping re-dispatch",
                service_id,
                pending,
                queue_depth,
            )

    st = (datetime.now(UTC) - timedelta(hours=lookback_hours)).isoformat()
    discovered = discover_prefix(service_id, start_time=st)

    return {"reclaimed": len(reclaimed), "redispatched": redispatched, "discovered": discovered}


# ── RUM ledger pipeline ──────────────────────────────────────────────────────
#
# RUM beacon counterpart of the regular-log ledger pipeline above. Reuses the
# SAME ``ingest_ledger`` table — RUM object keys live under ``rum/raw/...``
# and regular-log keys under ``raw/...`` (see backend/core/rum_ingest.py's
# ``ingest_rum_logs``, which LISTs ``prefix_subpath="rum/raw/"``), so the two
# keyspaces can never collide and no schema change is needed.
#
# Kept as standalone twins of discover_prefix/convert_object/sweep_ledger_once
# rather than parameters on them: those are the hottest path in the system
# (every regular-log file goes through them) and must not carry RUM
# branching. The v2 (non-celery) RUM path — backend/core/rum_ingest.py,
# backend/cron/jobs/rum_commit.py, and the rum_sync_{id}/rum_commit_{id}
# APScheduler jobs — is untouched and keeps working unchanged for
# non-celery deployments; this section only reuses its already-public pure
# helpers (extract_metrics_from_faro_payload, safe_int, safe_float) to avoid
# duplicating the Faro-payload extraction logic.


def discover_rum_prefix(service_id: str, prefix_subpath: str | None = None, start_time: str | None = None) -> int:
    """RUM counterpart of ``discover_prefix``: LIST one ``rum/raw/`` prefix
    (or a caller-supplied minute-scoped subpath under it), insert unseen keys
    into ``ingest_ledger`` as ``discovered``, and dispatch one ``convert_rum``
    per NEW row.

    ``prefix_subpath`` defaults to the RUM root (``rum/raw/``) rather than
    relying on ``list_fos_files``'s own default (``raw/``, the regular-log
    root) — a caller must never accidentally fall through to scanning the
    regular-log tree.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return 0
    src = get_source_for_service(service_id)
    if src is None:
        return 0

    list_kwargs: dict = {"start_time": start_time, "prefix_subpath": prefix_subpath or "rum/raw/"}
    fos_result = _drain_generator(list_fos_files(src, **list_kwargs))
    if not fos_result:
        logger.warning("[ledger] %s: RUM FOS LIST returned nothing (prefix=%s)", service_id, prefix_subpath)
        return 0
    new_files = fos_result.get("new_files") or []
    if not new_files:
        return 0

    bucket = src.get("bucket", "")
    s3_prefix = f"s3://{bucket}/"

    con = get_con(service_id)
    cur = con.cursor()
    newly_discovered: list[str] = []
    for f_path in new_files:
        f_size = fos_result.get("file_sizes", {}).get(f_path, 0)
        object_key = f_path[len(s3_prefix) :] if f_path.startswith(s3_prefix) else f_path
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, size_bytes, status, discovered_at) "
            "VALUES (?, ?, ?, 'discovered', ?) ON CONFLICT DO NOTHING",
            (service_id, object_key, f_size, time.time()),
        )
        if cur.rowcount > 0:
            newly_discovered.append(object_key)
    con.commit()

    for object_key in newly_discovered:
        convert_rum.delay(service_id, object_key)
    if newly_discovered:
        logger.info(
            "[ledger] %s: discovered %d new RUM file(s), dispatched convert_rum", service_id, len(newly_discovered)
        )
    return len(newly_discovered)


def _parse_rum_line(log_data: dict, service_id: str) -> tuple[list[dict], list[dict]] | None:
    """Parse one decoded RUM beacon JSON object into ``(vitals_rows, errors_rows)``.

    Returns ``None`` when the line belongs to a different service (a
    multi-tenant bucket) — not corruption, just routing, so the caller must
    not count it as either a valid or a corrupt row.

    A close copy of the per-line body in ``ingest_rum_logs``
    (backend/core/rum_ingest.py) — kept independent rather than extracted
    into a shared helper per the "don't touch the v2 RUM path" constraint.
    Reuses that module's already-public pure helpers
    (``extract_metrics_from_faro_payload``, ``safe_int``, ``safe_float``)
    to avoid duplicating the Faro-payload extraction logic itself.
    """
    from urllib.parse import parse_qs, urlparse

    from backend.core.rum_ingest import extract_metrics_from_faro_payload, safe_float, safe_int

    log_service_id = log_data.get("service_id") or log_data.get("rum_service_id")
    if log_service_id and log_service_id != service_id:
        return None

    received_at = log_data.get("timestamp") or datetime.now(UTC).isoformat()
    try:
        dt = datetime.fromisoformat(received_at)
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    except Exception:
        dt = datetime.now(UTC)

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
            browser_val = metric.get("browser") or log_data.get("browser") or "Chrome"
            os_val = metric.get("os") or log_data.get("os") or "macOS"
            device_val = metric.get("device") or log_data.get("device") or "Desktop"
            cid_val = metric.get("cid") or log_data.get("rum_cid") or log_data.get("cid") or ""
            req_id_val = log_data.get("fastly_req_id") or ""
            pathname_val = metric.get("pathname") or "/"
            common = _common(pathname_val, browser_val, os_val, device_val, cid_val, req_id_val)

            is_exception = (metric.get("metric_name") == "exception") or metric.get("error_message")
            if is_exception:
                errors_rows.append(
                    {
                        "timestamp": dt,
                        "error_message": metric.get("error_message") or "Unknown error",
                        "error_file": metric.get("error_file") or "unknown.js",
                        "error_line": safe_int(metric.get("error_line")),
                        "error_col": safe_int(metric.get("error_col")),
                        **common,
                    }
                )
            else:
                val = safe_float(metric.get("metric_value"))
                vitals_rows.append(
                    {
                        "timestamp": dt,
                        "metric_name": metric.get("metric_name") or "unknown",
                        "metric_value": val if val is not None else 0.0,
                        "metric_rating": metric.get("metric_rating") or "",
                        **common,
                    }
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
                parsed = urlparse(raw_url)
                qparams = parse_qs(parsed.query)
                if not metric_name and "rum_metric_name" in qparams:
                    metric_name = qparams["rum_metric_name"][0]
                if (metric_value is None or metric_value == "") and "rum_metric_value" in qparams:
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
                    metric_value = float(metric_value) if "." in metric_value else int(metric_value)
            except ValueError:
                pass

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
        common = _common(pathname_val, browser_val, os_val, device_val, cid_val, req_id_val)

        if log_data.get("rum_error_message"):
            errors_rows.append(
                {
                    "timestamp": dt,
                    "error_message": log_data.get("rum_error_message") or "Unknown error",
                    "error_file": log_data.get("rum_error_file") or "unknown.js",
                    "error_line": safe_int(log_data.get("rum_error_line")),
                    "error_col": safe_int(log_data.get("rum_error_col")),
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

    return vitals_rows, errors_rows


def _parse_rum_beacon_file(local_path: str, service_id: str) -> tuple[list[dict], list[dict], list[tuple[str, str]]]:
    """Decode+parse one downloaded ``rum/raw/*.gz`` beacon file.

    Returns ``(vitals_rows, errors_rows, corrupt_lines)`` where
    ``corrupt_lines`` is a list of ``(raw_line, reason)`` for lines that
    failed to parse. The caller (``convert_rum_object``) quarantines these
    via the same sidecar format ``_quarantine_convert_corrupt_lines`` uses
    for the regular-log path, instead of the v2 path's silent log-and-skip
    (``ingest_rum_logs``'s per-line ``except Exception: continue``).
    """
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
                corrupt_lines.append((stripped, f"parse_error: {e}"[:200]))
                continue
            if parsed is None:
                continue
            v_rows, e_rows = parsed
            vitals_rows.extend(v_rows)
            errors_rows.extend(e_rows)

    return vitals_rows, errors_rows, corrupt_lines


def _quarantine_rum_corrupt_lines(
    fos_client,
    src: dict,
    object_key: str,
    corrupt_lines: list[tuple[str, str]],
    valid_count: int,
) -> None:
    """Upload malformed RUM beacon lines to the ``errors/`` FOS prefix with a
    ``.meta.json`` sidecar — the RUM counterpart of
    ``_quarantine_convert_corrupt_lines``. Best-effort: the caller wraps this
    in a try/except so a quarantine-upload failure never fails the convert
    itself (the valid rows are already durably committed by the time this
    runs)."""
    service_id = src.get("service_id") or src.get("name", "")
    source_name = src.get("name", service_id)
    bucket = src.get("bucket", "")

    prefix_path = src.get("prefix", "").strip("/")
    raw_prefix = f"{prefix_path}/rum/raw/" if prefix_path else "rum/raw/"
    errors_prefix = f"{prefix_path}/errors/" if prefix_path else "errors/"
    if not object_key.startswith(raw_prefix):
        return
    error_key = errors_prefix + object_key[len(raw_prefix) :].replace(".gz", ".bad.jsonl")
    meta_key = error_key + ".meta.json"
    file_name = object_key.rsplit("/", 1)[-1]

    bad_lines = [line for line, _ in corrupt_lines]
    reason_counts: dict[str, int] = {}
    for _, reason in corrupt_lines:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    error_body = "\n".join(bad_lines).encode()
    fos_client.put_object(Bucket=bucket, Key=error_key, Body=error_body, ContentType="application/x-ndjson")

    meta = {
        "original_key": object_key,
        "quarantined_at": datetime.now(UTC).isoformat(),
        "valid_rows": valid_count,
        "corrupt_rows": len(bad_lines),
        "total_rows": valid_count + len(bad_lines),
        "corrupt_samples": [line[:2000] for line in bad_lines[:5]],
        "reason_counts": reason_counts,
        "source_name": source_name,
    }
    fos_client.put_object(Bucket=bucket, Key=meta_key, Body=json.dumps(meta).encode(), ContentType="application/json")

    metadata_db.insert_quarantined_file(
        service_id=service_id,
        file_name=file_name,
        source_name=source_name,
        fos_key=object_key,
        error_key=error_key,
        meta_key=meta_key,
        valid_rows=valid_count,
        corrupt_rows=len(bad_lines),
        file_size_bytes=None,
        corrupt_samples=[line[:2000] for line in bad_lines[:5]],
        reason_counts=reason_counts,
        error_size_bytes=len(error_body),
    )
    logger.warning(
        "[ledger] %s: quarantined %d corrupt RUM line(s) from %s -> %s",
        service_id,
        len(bad_lines),
        object_key,
        error_key,
    )


def convert_rum_object(service_id: str, object_key: str, worker_id: str) -> str:
    """RUM counterpart of ``convert_object``: claim one ``rum/raw/`` ledger
    row, parse the beacon file, and insert its rows into BOTH per-service
    DuckLake tables (``client_vitals``, ``client_errors``) — one raw file
    produces rows for both. Returns the terminal ledger status.

    Dual-table commit atomicity: each table is written in its own
    DELETE-by-``_source_file``-then-INSERT transaction (idempotent under
    redelivery, mirroring ``convert_object``'s single-table version of the
    same pattern). The ledger row is only flipped to ``committed`` after
    BOTH tables have been written without raising — if the second table's
    write fails after the first succeeded, the exception propagates to
    ``_ledger_record_failure`` and the row stays retryable (``discovered``
    or, after LEDGER_MAX_ATTEMPTS, ``quarantined``); it is never marked
    committed on a partial write. A retry re-runs both DELETE+INSERTs,
    which is a no-op for the table that already succeeded (same source
    file, same rows) and completes the table that didn't.
    """
    import duckdb as _duckdb
    import pyarrow as pa

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name

    # Schema catalog only — CLIENT_VITALS_ARROW_SCHEMA/CLIENT_ERRORS_ARROW_SCHEMA
    # are plain, static pa.Schema definitions (no iceberg pipeline logic), the
    # same shared source of truth v2's commit_buffer aligns buffer files to
    # (backend/core/iceberg/buffer.py's get_arrow_schema). Using the same
    # schema here — rather than letting pyarrow infer types from the parsed
    # dicts — is what keeps this path and v2's physically type-compatible on
    # the SAME DuckLake table (ducklake_table_name is shared naming), so
    # whichever writer creates the table first, the other's inserts don't
    # hit a type mismatch.
    from backend.core.iceberg.rum_schema import CLIENT_ERRORS_ARROW_SCHEMA, CLIENT_VITALS_ARROW_SCHEMA
    from backend.core.metadata.base import get_con

    con_meta = get_con(service_id)
    cur = con_meta.cursor()

    cur.execute(
        "UPDATE ingest_ledger SET status='claimed', claimed_by=?, claimed_at=? "
        "WHERE service_id=? AND object_key=? AND status='discovered'",
        (worker_id, time.time(), service_id, object_key),
    )
    claimed = cur.rowcount > 0
    con_meta.commit()
    if not claimed:
        return "not_claimed"

    src = get_source_for_service(service_id)
    if src is None:
        _ledger_record_failure(con_meta, service_id, object_key, "no source registered for service")
        return "error"

    bucket = src.get("bucket", "")
    s3_path = f"s3://{bucket}/{object_key}"
    safe_source_file = escape_sql_literal(s3_path)

    duckdb_con = None
    try:
        fos = boto3_client_hot() if svcconfig.HOT_S3_ENDPOINT else _get_fos_client(src)
        # Same single-writer-file rationale as convert_object: an in-memory
        # connection with FOS creds + the transactional DuckLake catalog,
        # never the per-service .duckdb file.
        duckdb_con = _duckdb.connect()
        _configure_fos(duckdb_con, src)
        if not _ducklake_attach(duckdb_con, src, read_only=False):
            raise RuntimeError("DuckLake read-write attach failed")

        vitals_table = ducklake_table_name(src, table_name="client_vitals")
        errors_table = ducklake_table_name(src, table_name="client_errors")

        with tempfile.TemporaryDirectory() as tmpdir:
            s3_to_local, _ = _download_chunk_to_local(fos, [s3_path], tmpdir)
            local_file = s3_to_local.get(s3_path)
            if not local_file:
                # Same dead-vs-transient split as convert_object: distinguish
                # "object is gone" (terminal) from a transient download blip.
                try:
                    fos.head_object(Bucket=bucket, Key=object_key)
                except Exception as head_err:
                    if "404" in str(head_err) or "Not Found" in str(head_err) or "NoSuchKey" in str(head_err):
                        cur.execute(
                            "UPDATE ingest_ledger SET status='dead_letter', "
                            "last_error='object missing from FOS (already ingested+deleted, or expired)' "
                            "WHERE service_id=? AND object_key=?",
                            (service_id, object_key),
                        )
                        con_meta.commit()
                        logger.info("[ledger] %s: RUM %s gone from FOS — dead_letter", service_id, object_key)
                        return "dead_letter"
                raise RuntimeError("download failed (object still exists — transient)")

            vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(local_file, service_id)

            if corrupt_lines:
                try:
                    _quarantine_rum_corrupt_lines(
                        fos, src, object_key, corrupt_lines, len(vitals_rows) + len(errors_rows)
                    )
                except Exception as qe:
                    logger.warning("[ledger] %s: RUM quarantine failed for %s: %s", service_id, object_key, qe)

            def _write_table(table_name: str, rows: list[dict], schema) -> None:
                if not rows:
                    # Nothing to delete a duplicate of on retry either — a
                    # deterministic re-parse of the same bytes always
                    # produces the same (possibly empty) row set.
                    return
                arrow_tbl = pa.Table.from_pylist(rows, schema=schema)
                duckdb_con.register("_rum_stage", arrow_tbl)
                try:
                    table_exists = bool(
                        duckdb_con.execute(
                            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
                            (table_name,),
                        ).fetchone()
                    )
                    if not table_exists:
                        duckdb_con.execute(
                            f"CREATE TABLE lake.{table_name} AS "
                            f"SELECT *, '{safe_source_file}' AS _source_file FROM _rum_stage"
                        )
                    else:
                        existing_cols = {r[0] for r in duckdb_con.execute(f"DESCRIBE lake.{table_name}").fetchall()}
                        if "_source_file" not in existing_cols:
                            duckdb_con.execute(f'ALTER TABLE lake.{table_name} ADD COLUMN "_source_file" VARCHAR')
                        duckdb_con.execute("BEGIN TRANSACTION")
                        try:
                            duckdb_con.execute(
                                f"DELETE FROM lake.{table_name} WHERE _source_file = '{safe_source_file}'"
                            )
                            duckdb_con.execute(
                                f"INSERT INTO lake.{table_name} BY NAME "
                                f"SELECT *, '{safe_source_file}' AS _source_file FROM _rum_stage"
                            )
                            duckdb_con.execute("COMMIT")
                        except Exception:
                            try:
                                duckdb_con.execute("ROLLBACK")
                            except Exception:
                                pass
                            raise
                finally:
                    duckdb_con.unregister("_rum_stage")

            _write_table(vitals_table, vitals_rows, CLIENT_VITALS_ARROW_SCHEMA)
            _write_table(errors_table, errors_rows, CLIENT_ERRORS_ARROW_SCHEMA)

            # Mirror convert_object's ingested_files bookkeeping so existing
            # readers (Usage Log, admin ingested-files list) keep working.
            # Best-effort: never fails the convert — the ledger is
            # authoritative.
            try:
                metadata_db.insert_ingested_files(
                    service_id,
                    [(object_key, len(vitals_rows), os.path.getsize(local_file))],
                    table_name="client_vitals",
                )
                metadata_db.insert_ingested_files(
                    service_id,
                    [(object_key, len(errors_rows), os.path.getsize(local_file))],
                    table_name="client_errors",
                )
            except Exception as ie:
                logger.warning(
                    "[ledger] %s: RUM ingested_files bookkeeping failed for %s: %s", service_id, object_key, ie
                )
    except Exception as e:
        return _ledger_record_failure(con_meta, service_id, object_key, str(e))
    finally:
        if duckdb_con is not None:
            duckdb_con.close()

    cur.execute(
        "UPDATE ingest_ledger SET status='committed', committed_at=? "
        "WHERE service_id=? AND object_key=? AND status='claimed'",
        (time.time(), service_id, object_key),
    )
    con_meta.commit()
    return "committed"


def sweep_rum_ledger_once(service_id: str, lookback_hours: int = 4) -> dict:
    """RUM counterpart of ``sweep_ledger_once``, scoped to ``rum/raw/``
    object keys via an explicit ``LIKE`` filter.

    A standalone twin rather than an extension of ``sweep_ledger_once``:
    its own hot path (regular-log reclaim/redispatch) must stay untouched.
    Scoping this copy's queries to the RUM keyspace — and the mirrored
    ``NOT LIKE`` exclusion on ``sweep_ledger_once``'s own queries — keeps
    each sweep's redispatch calling the correct task (``convert_rum.delay``
    here, ``convert_batch_files.delay`` there) for every row it touches, so a stale RUM
    row can never get picked up and misparsed by the regular-log sweep.
    """
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    cur = con.cursor()
    now = time.time()

    src = get_source_for_service(service_id)
    prefix_path = (src.get("prefix", "") if src else "").strip("/")
    rum_prefix = f"{prefix_path}/rum/raw/" if prefix_path else "rum/raw/"
    like_pattern = rum_prefix + "%"

    cur.execute(
        "UPDATE ingest_ledger SET status='discovered', claimed_by=NULL, claimed_at=NULL "
        "WHERE service_id=? AND status='claimed' AND claimed_at < ? AND object_key LIKE ? RETURNING object_key",
        (service_id, now - LEDGER_RECLAIM_AFTER_S, like_pattern),
    )
    reclaimed = [r[0] for r in cur.fetchall()]
    con.commit()

    stale_cutoff = now - LEDGER_REDISPATCH_AFTER_S
    stuck = [
        r[0]
        for r in con.execute(
            "SELECT object_key FROM ingest_ledger "
            "WHERE service_id=? AND status='discovered' AND object_key LIKE ? "
            "AND (claimed_at IS NULL OR claimed_at < ?) LIMIT 5000",
            (service_id, like_pattern, stale_cutoff),
        ).fetchall()
        if r[0] not in reclaimed
    ]

    # Same lost-message guard as sweep_ledger_once (see its docstring) —
    # shares the q.ingest depth check since RUM converts route to the same
    # queue as regular-log converts.
    pending = len(stuck) + len(reclaimed)
    redispatched = 0
    if pending:
        try:
            from backend.celery_status import celery_queue_depths

            queues, broker_ok = celery_queue_depths()
            queue_depth = queues.get("q.ingest", 0) if broker_ok else 0
        except Exception:
            queue_depth = 0
        if queue_depth < pending:
            for object_key in reclaimed:
                convert_rum.delay(service_id, object_key)
            for object_key in stuck:
                convert_rum.delay(service_id, object_key)
            redispatched = pending
            logger.info(
                "[ledger] %s: RUM sweeper re-dispatched %d pending row(s) (reclaimed=%d, queue_depth=%d)",
                service_id,
                pending,
                len(reclaimed),
                queue_depth,
            )
        else:
            logger.info(
                "[ledger] %s: %d RUM pending row(s) but q.ingest already holds %d message(s) — "
                "nothing lost, skipping re-dispatch",
                service_id,
                pending,
                queue_depth,
            )

    st = (datetime.now(UTC) - timedelta(hours=lookback_hours)).isoformat()
    discovered = discover_rum_prefix(service_id, start_time=st)

    return {"reclaimed": len(reclaimed), "redispatched": redispatched, "discovered": discovered}


# ── Celery task wrappers ──────────────────────────────────────────────────────


@app.task(name="backend.core.ingest.dispatch_minute", bind=True)
def dispatch_minute(self, service_id: str, minute_prefix: str):
    with _celery_ingest_scope("ledger_discover", service_id):
        return discover_prefix(service_id, prefix_subpath=minute_prefix)


@app.task(name="backend.core.ingest.convert", bind=True)
def convert(self, service_id: str, object_key: str):
    with _celery_ingest_scope("ledger_convert", service_id):
        return convert_object(service_id, object_key, self.request.id or "sync-worker")


@app.task(name="backend.core.ingest.convert_batch_files", bind=True)
def convert_batch_files(self, service_id: str, object_keys: list[str]):
    """Batched convert — one DuckLake catalog commit for the whole batch.
    The per-key ``convert`` task above stays: the sweeper's dead-key retries
    and any single-key message still in flight across a deploy need it."""
    with _celery_ingest_scope("ledger_convert_batch", service_id):
        return convert_batch_objects(service_id, object_keys, self.request.id or "sync-worker")


@app.task(name="backend.core.ingest.sweep_ledger", bind=True)
def sweep_ledger(self, service_id: str, lookback_hours: int = 4):
    with _celery_ingest_scope("ledger_sweep", service_id):
        return sweep_ledger_once(service_id, lookback_hours=lookback_hours)


@app.task(name="backend.core.ingest.dispatch_rum_minute", bind=True)
def dispatch_rum_minute(self, service_id: str, minute_prefix: str):
    with _celery_ingest_scope("ledger_rum_discover", service_id):
        return discover_rum_prefix(service_id, prefix_subpath=minute_prefix)


@app.task(name="backend.core.ingest.convert_rum", bind=True)
def convert_rum(self, service_id: str, object_key: str):
    with _celery_ingest_scope("ledger_rum_convert", service_id):
        return convert_rum_object(service_id, object_key, self.request.id or "rum-worker")


@app.task(name="backend.core.ingest.sweep_rum_ledger", bind=True)
def sweep_rum_ledger(self, service_id: str, lookback_hours: int = 4):
    with _celery_ingest_scope("ledger_rum_sweep", service_id):
        return sweep_rum_ledger_once(service_id, lookback_hours=lookback_hours)


# Only delete a raw file once its rows have been durably committed for this
# long — protects any reader still holding the pre-commit view of that file.
RAW_DELETE_GRACE_S = 10 * 60


def finalize_committed_raw(service_id: str, batch_size: int = 10_000) -> dict:
    """Delete raw .gz files whose rows are durably committed to the lake.

    Celery-mode counterpart of the sync path's ``delete_after`` handling —
    without it, FOS raw storage grows without bound. Honors the service's
    ``provisioning.cron_sync.delete_after`` (default True). Idempotent:
    each ledger row is stamped ``raw_deleted_at`` on successful delete.
    Returns ``{"deleted": n, "eligible": m, "delete_after": bool}``.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata.base import get_con

    cfg = svcconfig.load_config(service_id) or {}
    delete_after = cfg.get("provisioning", {}).get("cron_sync", {}).get("delete_after", True)

    con = get_con(service_id)
    cutoff = time.time() - RAW_DELETE_GRACE_S
    eligible = [
        r[0]
        for r in con.execute(
            "SELECT object_key FROM ingest_ledger "
            "WHERE service_id = ? AND status = 'committed' AND raw_deleted_at IS NULL "
            "AND committed_at < ? LIMIT ?",
            (service_id, cutoff, batch_size),
        ).fetchall()
    ]
    if not delete_after or not eligible:
        return {"deleted": 0, "eligible": len(eligible), "delete_after": bool(delete_after)}

    src = get_source_for_service(service_id)
    if src is None:
        return {"deleted": 0, "eligible": len(eligible), "delete_after": True}
    fos = _get_fos_client(src)
    bucket = src.get("bucket", "")

    deleted = 0
    for i in range(0, len(eligible), 1000):
        chunk = eligible[i : i + 1000]
        try:
            resp = fos.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
        except Exception as e:
            logger.warning("[ledger] %s: raw-file batch delete failed: %s", service_id, e)
            continue
        failed_keys = {err.get("Key") for err in (resp.get("Errors") or [])}
        now = time.time()
        cur = con.cursor()
        for key in chunk:
            if key in failed_keys:
                continue
            cur.execute(
                "UPDATE ingest_ledger SET raw_deleted_at = ? WHERE service_id = ? AND object_key = ?",
                (now, service_id, key),
            )
            deleted += 1
        con.commit()
        if failed_keys:
            logger.warning("[ledger] %s: %d raw delete(s) failed in batch", service_id, len(failed_keys))
    return {"deleted": deleted, "eligible": len(eligible), "delete_after": True}


def merge_lake_files(service_id: str) -> None:
    """Flush inlined rows to parquet, then compact adjacent small DuckLake
    data files for one service. Raises on failure — callers record the
    outcome in cron_runs.

    The flush is load-bearing for DURABILITY, not just for file layout.
    DuckLake "inlines" small inserts directly into the metadata catalog
    (Postgres, or the .ducklake file) instead of writing parquet, and
    NEITHER ``ducklake_merge_adjacent_files`` nor
    ``ducklake_rewrite_data_files`` promotes inlined rows — both operate on
    already-materialized files, so a table whose every commit was inlined
    stays at ``file_count = 0`` forever no matter how often compaction
    runs (verified empirically). Since ``finalize_committed_raw`` deletes
    the raw .gz once its ledger row has been committed for
    RAW_DELETE_GRACE_S, an unflushed catalog means the ONLY copy of the
    data is the catalog itself — no FOS parquet, no raw file to re-ingest.
    ``ducklake_flush_inlined_data`` is the only primitive that promotes
    them, and it must run BEFORE the merge (the merge needs real files to
    have anything to compact) and before raw deletion in the same tick —
    which is the order ``cron/jobs/commit.py`` calls them in.
    """
    import duckdb as _duckdb

    from backend.core.duckdb import get_source_for_service
    from backend.core.iceberg._ducklake import _ducklake_attach

    src = get_source_for_service(service_id)
    if src is None:
        raise RuntimeError(f"no source registered for {service_id}")
    # In-memory connection: same single-writer-file-lock rationale as
    # convert_object — never open the per-service .duckdb from a worker.
    con = _duckdb.connect()
    try:
        _configure_fos(con, src)
        if not _ducklake_attach(con, src, read_only=False):
            raise RuntimeError("DuckLake read-write attach failed")
        con.execute("CALL ducklake_flush_inlined_data('lake')")
        con.execute("CALL ducklake_merge_adjacent_files('lake')")
    finally:
        con.close()


@app.task(name="backend.core.ingest.commit_batch", bind=True)
def commit_batch(self, service_id: str):
    with _celery_ingest_scope("ledger_merge", service_id):
        merge_lake_files(service_id)
