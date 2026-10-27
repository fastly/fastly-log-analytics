import concurrent.futures
import itertools
import logging
import os
import re
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.duckdb import _get_fos_client

logger = logging.getLogger("backend.core.ingest")

_STRANDED_DELETE_CAP = 10_000
_RECONCILE_MIN_INTERVAL_S = 300.0
_reconcile_last_attempt: dict[str, float] = {}
_RECONCILE_LEDGER_EPOCH = "2026-06-21 00:00:00"

LEDGER_RECLAIM_AFTER_S = 20 * 60
LEDGER_REDISPATCH_AFTER_S = 10 * 60
LEDGER_MAX_ATTEMPTS = 5
LEDGER_RETRY_BASE_S = 30
LEDGER_RETRY_MAX_S = 60 * 60
LEDGER_RETRY_JITTER_RATIO = 0.25
LEDGER_CONVERT_BATCH_SIZE = int(os.getenv("LEDGER_CONVERT_BATCH_SIZE", "50"))
_LEDGER_KEY_SLICE = 400
RAW_DELETE_GRACE_S = 10 * 60
RAW_DELETE_CLAIM_LEASE_S = 15 * 60

_FASTLY_FNAME_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}[-:]\d{2}[-:]\d{2})")
_V3_MINUTE_KEY_RE = re.compile(
    r"(?:^|/)raw/(?:request|rum)/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/"
)
_MINUTE_KEY_RE = re.compile(r"/raw/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/")
_SLASHED_KEY_RE = re.compile(r"/raw/(\d{4})/(\d{2})/(\d{2})/(\d{2})/[^/]*?_(\d{2})\.json\.gz")
_DASH_KEY_RE = re.compile(r"/raw/(\d{4}-\d{2}-\d{2})/(\d{2})/")


def _is_permission_error(err: Any) -> bool:
    s = str(err or "")
    return "AccessDenied" in s or "UnauthorizedAccess" in s


def _parse_time_bounds(start_time: str | None, end_time: str | None) -> tuple[datetime | None, datetime | None]:
    from backend.utils.telemetry import parse_iso_utc

    return (parse_iso_utc(start_time) if start_time else None, parse_iso_utc(end_time) if end_time else None)


def _consume_generator(gen: Any) -> Any:
    try:
        while True:
            next(gen)
    except StopIteration as e:
        return e.value


def _parse_fastly_filename_dt(fname: str) -> datetime | None:
    m = _FASTLY_FNAME_TS_RE.match(fname)
    if not m:
        return None
    try:
        date_part, time_part = m.group(1).split("T", 1)
        return datetime.fromisoformat(f"{date_part}T{time_part.replace('-', ':')}").replace(tzinfo=UTC)
    except ValueError:
        return None


def _parse_key_layout_dt(key: str) -> tuple[str, datetime] | None:
    for layout, pattern in (("v3", _V3_MINUTE_KEY_RE), ("minute", _MINUTE_KEY_RE), ("slashed", _SLASHED_KEY_RE)):
        if m := pattern.search(key):
            try:
                y, mo, d, h, mi = (int(g) for g in m.groups())
                return (layout, datetime(y, mo, d, h, mi, tzinfo=UTC))
            except ValueError:
                return None
    if m := _DASH_KEY_RE.search(key):
        if (fname_dt := _parse_fastly_filename_dt(key.split("/")[-1])) is not None:
            return ("dash", fname_dt)
        try:
            return ("dash", datetime.fromisoformat(m.group(1)).replace(hour=int(m.group(2)), tzinfo=UTC))
        except ValueError:
            return None
    return None


def _compute_incremental_start_after(
    already: set[str], lookback_hours: int = 4, prefix_subpath: str = "raw/request/"
) -> str | None:
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
            batch_failures = (
                set(batch)
                if any(err.get("Key") not in batch for err in errors)
                else {err.get("Key") for err in errors if err.get("Key") in batch}
            )
            if errors and _is_permission_error(errors[0].get("Code", "")):
                logger.warning(
                    "Bulk delete skipped due to missing permissions (%s). Disabling further delete attempts for this batch.",
                    errors[0].get("Code"),
                )
                return total_deleted + len(batch) - len(batch_failures), failed_keys | batch_failures | set(
                    keys[i + len(batch) :]
                )
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
    return _delete_objects_robust_with_failures(fos_client, bucket, keys)[0]


def _download_chunk_to_local(fos_client, s3_paths: list[str], tmpdir: str) -> tuple[dict[str, str], dict[str, str]]:
    from backend.utils.telemetry import _BOTO3_CALLER_HINT

    s3_to_local, local_to_s3 = {}, {}

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
                msg = (
                    "[ingest] transient FOS error downloading %s: %s"
                    if is_transient
                    else "[ingest] failed to download %s: %s"
                )
                (logger.debug if is_transient else logger.warning)(msg, p, err_str)
    return s3_to_local, local_to_s3


def list_fos_files(  # fmt: skip
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
    elapsed_fn = elapsed_fn or (lambda: "")
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
    stranded_already: list[str] = []
    skipped_already = total_listed = 0
    try:
        prefix_path = src.get("prefix", "").strip("/")
        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
        raw_prefix = f"{prefix_path}/{prefix_subpath}" if prefix_path else prefix_subpath
        yield {"type": "status", "message": f"{elapsed_fn()} Discovering new files in Fastly Object Storage..."}
        from backend.provision.cli import minute_list_prefix, rum_minute_list_prefix

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


def _iter_key_slices(keys: list[str]):
    for key_slice in itertools.batched(keys, _LEDGER_KEY_SLICE):
        yield list(key_slice), ",".join("?" * len(key_slice))


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
    from backend.core import ingest as _ingest_pkg

    list_kwargs: dict[str, Any] = {"start_time": start_time}
    if prefix_subpath is not None:
        list_kwargs["prefix_subpath"] = prefix_subpath
    return _discover_prefix_common(
        service_id,
        list_kwargs=list_kwargs,
        prefix_subpath=prefix_subpath,
        dispatch_fn=_ingest_pkg.convert_batch_files.delay,
        originating_task=originating_task,
        run_id=run_id,
    )


def _is_permanent_s3_missing(fos: Any, bucket: str, object_key: str) -> bool:
    try:
        fos.head_object(Bucket=bucket, Key=object_key)
    except Exception as head_err:
        err = str(head_err)
        return "404" in err or "Not Found" in err or "NoSuchKey" in err
    return False


def boto3_client_hot():
    import boto3

    from backend import config as svcconfig

    return boto3.client(
        "s3",
        endpoint_url=svcconfig.HOT_S3_ENDPOINT,
        aws_access_key_id=svcconfig.HOT_S3_KEY,
        aws_secret_access_key=svcconfig.HOT_S3_SECRET,
    )
