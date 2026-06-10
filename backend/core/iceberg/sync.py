"""Iceberg sync_data orchestrator.

Carved out of ``backend/core/iceberg/_core.py`` (v2.0 file-size sweep
part 4/4). Holds the single huge ``sync_data`` function (~450 lines):
the FOS-to-local download orchestrator that the scheduler's sync cron
calls on every tick. It walks the Iceberg manifest, identifies files
that exist in the table snapshot but not on local disk, downloads
them, and updates the per-service ingested_files SQLite metadata.

Re-exported back into ``backend.core.iceberg._core`` at the bottom of
that module so existing call sites
(``backend.core.iceberg.sync_data(...)``) keep resolving. Test
monkeypatches on ``backend.core.iceberg.sync_data`` flow through the
package proxy → _core's binding → this module via the re-export.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import UTC, datetime, timedelta, timezone

import duckdb

logger = logging.getLogger("backend.core.iceberg._core")

# Library imports the carved function references.
from backend.utils.sql_validator import escape_sql_literal

# Late-bind helpers from the main _core module.
from backend.core.iceberg import _core as _core_mod


def __getattr__(name: str):
    return getattr(_core_mod, name)


def sync_data(source: dict, progress_callback=None, start_time: str | None = None, end_time: str | None = None) -> dict:
    """Download data files from FOS that are present in the Iceberg table but missing locally.

    If start_time and end_time (ISO strings) are provided, only files matching that range
    are considered for download. Files already present locally but outside this range
    are NOT deleted if a range is specified (to allow incremental multi-range imports).
    """
    source_key = source.get("name", "default")

    # Phase 1: Brief lock just for catalog init — table object is captured, then lock released.
    # The manifest scan (plan_files) runs outside the lock so dashboard queries are not blocked.
    try:
        with _core_mod._get_service_lock(source_key):
            catalog = _core_mod._get_catalog(source)
            identifier = _core_mod._table_identifier(source)
            _core_mod._refresh_local_catalog_metadata(catalog, source, identifier)
            try:
                table = _core_mod._load_table_cached(source, identifier, catalog)
            except Exception:
                table = _core_mod._try_register_from_fos(catalog, source, identifier)
                if table is None:
                    return {
                        "error": "Iceberg table not found in FOS — the admin may not have committed any data yet.",
                        "files_downloaded": 0,
                    }
    except Exception as e:
        return {"error": f"Could not load table: {e}", "files_downloaded": 0}

    # Phase 2: Manifest scan — runs without the service lock so the dashboard is never blocked.
    from backend.core.duckdb import _cache_dir

    cache_dir = os.path.join(_cache_dir(source), "data")
    os.makedirs(cache_dir, exist_ok=True)

    # 1. Map cloud paths to local paths
    cloud_files: dict[str, tuple[str, int]] = {}  # cloud_uri -> (local_path, record_count)

    # Fast path: when no time filter is requested and the snapshot cache is
    # fresh (commit_buffer's delta update kept it aligned with this
    # metadata_loc), use the cached file list instead of doing another full
    # tbl.scan().plan_files() — that scan would re-read every immutable
    # manifest just to discover that nothing has changed. record_count
    # is not stored in the cache; downloaded-rows reporting falls back to 0
    # for delta-tracked files, which is fine for steady-state cron runs.
    cached_snapshot = _core_mod._snapshot_files_cache.get(source_key)
    fast_path_used = False
    # Pre-fetch the set of basenames that local_compaction has intentionally
    # removed (merged into a bigger local file). Without this exclusion, the
    # missing_local check below treats them as "lost — re-download" and
    # forces the slow path on every tick.
    compacted_basenames: set[str] = set()
    try:
        from backend.core import metadata_db as _meta

        compacted_basenames = _meta.get_locally_compacted_basenames(
            source.get("service_id") or source.get("name") or ""
        )
    except Exception:
        pass

    if not start_time and not end_time and cached_snapshot and cached_snapshot[0] == table.metadata_location:
        try:
            cached_files = cached_snapshot[3]
            # A local-path entry in the cache means "this file was previously
            # downloaded". If any of those files are now missing on disk we
            # cannot use the fast path UNLESS local_compaction merged them
            # away (in which case "missing" is the desired state).
            missing_local = next(
                (
                    p
                    for p in cached_files
                    if not p.startswith("s3://")
                    and not os.path.exists(p)
                    and os.path.basename(p) not in compacted_basenames
                ),
                None,
            )
            if missing_local is not None:
                logger.warning(
                    "%s %s: snapshot cache references missing local file %s — falling back to full plan_files scan to recover",
                    _core_mod._SYNC,
                    source.get("name"),
                    missing_local,
                )
            else:
                for entry in cached_files:
                    if entry.startswith("s3://"):
                        uri = entry
                        rel_path = uri.split("/data/")[-1] if "/data/" in uri else uri.split("/")[-1]
                        local_path = os.path.abspath(os.path.join(cache_dir, rel_path))
                        if not local_path.startswith(os.path.abspath(cache_dir) + os.sep):
                            continue
                        cloud_files[uri] = (local_path, 0)
                    else:
                        # Already-downloaded entry. Must populate cloud_files
                        # so the orphan-cleanup loop below sees its local_path
                        # in ``active_paths`` and does NOT delete it. Without
                        # this, once _core_mod._reconcile_snapshot_cache_after_sync has
                        # converted every s3:// to a local path, cloud_files /
                        # active_paths would be empty and the cleanup loop
                        # would nuke the entire local cache — leaving only the
                        # next commit's freshly-arrived file. Safe because we
                        # confirmed above that every local-path entry exists
                        # on disk (so files_to_download won't try to fetch
                        # using a local path as a fake s3 key).
                        cloud_files[entry] = (entry, 0)
                fast_path_used = True
                logger.info(
                    "%s %s: sync_data using snapshot cache (%d total files, all locally present)",
                    _core_mod._SYNC,
                    source.get("name"),
                    len(cached_files),
                )
        except Exception as e:
            logger.warning("[sync_data] %s: cache fast-path failed (%s) — falling back to full scan", source_key, e)
            cloud_files = {}
            fast_path_used = False

    if not fast_path_used:
        try:
            import dateutil.parser
            from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

            scan = table.scan()

            # Helper to normalize ISO strings to datetime for comparison
            def _parse_ts(ts_str: str) -> datetime:
                dt = dateutil.parser.isoparse(ts_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt

            st_dt = _parse_ts(start_time) if start_time else None
            et_dt = _parse_ts(end_time) if end_time else None

            if st_dt and et_dt and st_dt > et_dt:
                logger.warning(
                    "[sync_data] %s: Start time (%s) is after end time (%s). No files will be matched.",
                    source.get("name"),
                    start_time,
                    end_time,
                )
                return {"files_downloaded": 0, "rows_downloaded": 0, "message": "Invalid time range: start after end."}

            if start_time:
                scan = scan.filter(GreaterThanOrEqual("timestamp", st_dt.isoformat()))
            if end_time:
                scan = scan.filter(LessThanOrEqual("timestamp", et_dt.isoformat()))

            for f in scan.plan_files():
                uri = f.file.file_path
                record_count = getattr(f.file, "record_count", 0)
                # Preserve the partition folder structure for Hive partition pruning
                # PyIceberg writes to .../data/timestamp_hour=.../file.parquet
                if "/data/" in uri:
                    rel_path = uri.split("/data/")[-1]
                else:
                    rel_path = uri.split("/")[-1]

                local_path = os.path.abspath(os.path.join(cache_dir, rel_path))
                if not local_path.startswith(os.path.abspath(cache_dir) + os.sep):
                    continue
                cloud_files[uri] = (local_path, record_count)
        except Exception as e:
            return {"error": f"Metadata scan failed: {e}", "files_downloaded": 0}

    # Phase 3: File downloads — no lock held

    # 2. Download missing files
    downloaded = 0
    rows_downloaded = 0
    bytes_downloaded = 0

    # Pre-count so the callback can report X/total progress
    total_to_download = sum(1 for local_path, _ in cloud_files.values() if not os.path.exists(local_path))
    already_cached = sum(1 for local_path, _ in cloud_files.values() if os.path.exists(local_path))

    from backend.core.duckdb import _get_fos_client

    s3 = _get_fos_client(source)
    bucket = source["bucket"]
    cdn_url = (source.get("cdn_url") or "").rstrip("/")
    cdn_secret = source.get("cdn_secret") or ""

    import concurrent.futures
    import shutil

    download_lock = threading.Lock()

    def _download_file(uri, local_path, record_count):
        nonlocal downloaded, rows_downloaded, bytes_downloaded
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        key = uri.replace(f"s3://{bucket}/", "").lstrip("/")
        # Thread-safe temp file name
        tmp_path = local_path + f".tmp.{threading.get_ident()}"

        try:
            success = False
            if cdn_url:
                import urllib.parse

                # Check if the secret is provided. The CDN might expect it as a query parameter
                # 'key' (as seen in the working curl command) or as a header. We will append it
                # to the URL if a secret is configured.
                if cdn_secret:
                    # Parse the cdn_url to see if it already has query params
                    url_parts = urllib.parse.urlparse(cdn_url)
                    query = urllib.parse.parse_qs(url_parts.query)
                    query["key"] = [cdn_secret]
                    new_query = urllib.parse.urlencode(query, doseq=True)

                    # Append the key to the path so it comes before the query string
                    safe_key = urllib.parse.quote(key, safe="/=")
                    new_path = url_parts.path.rstrip("/") + "/" + safe_key

                    download_url = urllib.parse.urlunparse(
                        (url_parts.scheme, url_parts.netloc, new_path, url_parts.params, new_query, url_parts.fragment)
                    )
                else:
                    download_url = f"{cdn_url}/{urllib.parse.quote(key, safe='/=')}"

                req = urllib.request.Request(download_url)
                if cdn_secret:
                    req.add_header("x-fastly-key", cdn_secret)

                last_err = None
                cdn_headers = None
                # Measure wall-clock of the successful attempt only so the
                # usage_log row's elapsed reflects actual CDN service time,
                # not the cumulative cost of retries.
                cdn_elapsed_ms = 0.0
                for attempt in range(3):
                    try:
                        t0 = time.time()
                        with urllib.request.urlopen(req, timeout=30) as response, open(tmp_path, "wb") as out_file:
                            cdn_headers = response.headers
                            shutil.copyfileobj(response, out_file)
                        cdn_elapsed_ms = round((time.time() - t0) * 1000, 2)
                        success = True
                        break
                    except urllib.error.HTTPError as e:
                        last_err = e
                        if e.code in (401, 403):
                            # Don't retry on auth errors
                            break
                        if attempt < 2:
                            time.sleep(1)
                    except Exception as e:
                        last_err = e
                        if attempt < 2:
                            time.sleep(1)

                if not success:
                    raise RuntimeError(
                        f"CDN download failed for {key}: {last_err}. Check CDN URL, secret, and VCL configuration. URL attempted: {download_url.split('?')[0]}?key=***"
                    )
            else:
                s3.download_file(bucket, key, tmp_path)
                success = True

            os.rename(tmp_path, local_path)

            if cdn_url:
                try:
                    from backend.utils.telemetry import record_cdn_call

                    record_cdn_call(
                        "GET",
                        key,
                        cdn_elapsed_ms,
                        headers=cdn_headers,
                        bytes_count=os.path.getsize(local_path),
                        caller="sync_data_files",
                    )
                except Exception:
                    pass

            with download_lock:
                downloaded += 1
                rows_downloaded += record_count
                bytes_downloaded += os.path.getsize(local_path)
                curr_dl = downloaded

            if progress_callback:
                progress_callback(curr_dl, total_to_download, os.path.basename(local_path), record_count)

        except Exception as e:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            raise e

    # Skip files whose basename is in the local-compacted registry: they
    # were intentionally deleted by local_compaction after being merged
    # into a larger local file. Without this filter the slow-path
    # download loop pulls them right back, starting the cycle over.
    files_to_download = [
        (u, p, c)
        for u, (p, c) in cloud_files.items()
        if not os.path.exists(p) and os.path.basename(p) not in compacted_basenames
    ]

    # 10 concurrent connections is a good balance between speed and avoiding rate limits/socket exhaustion
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_download_file, u, p, c) for u, p, c in files_to_download]
        # Iterate over as_completed to bubble up exceptions immediately
        for f in concurrent.futures.as_completed(futures):
            f.result()

    # 3. Clean up orphaned local files (not in current snapshot)
    # We skip this if a range was specified to avoid deleting files outside the range
    # that are still part of the table snapshot.
    #
    # Local-compaction writes merged rollups in two places:
    #   • <cache>/data/daily/ and <cache>/data/weekly/   (multi-day tier)
    #   • <cache>/data/timestamp_hour=*/compacted_*.parquet  (intra-hour tier)
    # Both kinds are LOCAL-ONLY — they're not part of the iceberg snapshot, so
    # they never appear in ``active_paths``. Without the skip, every sync
    # deletes them and the next sync's registry-filter blocks the iceberg
    # source files from being re-downloaded — silently dropping rows from the
    # view (production hit ~31k missing rows on 2026-06-01). Restrict the scan
    # to ``timestamp_hour=*`` dirs AND ignore ``compacted_*.parquet`` outputs.
    deleted = 0
    if not start_time and not end_time:
        active_paths = {p for p, _ in cloud_files.values()}
        try:
            data_root = os.path.join(cache_dir, "data")
            scan_root = data_root if os.path.isdir(data_root) else cache_dir
            for entry in os.listdir(scan_root) if os.path.isdir(scan_root) else []:
                if not entry.startswith("timestamp_hour="):
                    continue  # skip daily/ weekly/ and any other local-only dirs
                part_dir = os.path.join(scan_root, entry)
                for root, _, files in os.walk(part_dir):
                    for file in files:
                        if not file.endswith(".parquet"):
                            continue
                        if file.startswith("compacted_"):
                            continue  # hourly-tier compaction output (local-only)
                        local_path = os.path.abspath(os.path.join(root, file))
                        if local_path not in active_paths:
                            os.remove(local_path)
                            deleted += 1
            _core_mod._prune_empty_dirs(cache_dir)
        except Exception as e:
            logger.warning(f"[iceberg] Failed to cleanup orphaned files: {e}")

    # 4. Update the resolved files cache so the next dashboard load uses the local paths
    #
    # FOS occasionally returns "[Errno 16] Reduce your request rate" right after
    # a heavy sync — the catalog reload + manifest scan piles more reads onto
    # an already-busy bucket. We retry rate-limit errors only (with backoff);
    # other failures bubble straight to the warning so they stay visible.
    import time as _time

    _MAX_RETRIES = 3

    def _is_rate_limited(err: Exception) -> bool:
        msg = str(err).lower()
        return any(
            tok in msg for tok in ("reduce your request rate", "errno 16", "slowdown", "throttl", "too many requests")
        )

    for attempt in range(_MAX_RETRIES):
        try:
            source_key = source.get("name", "default")
            with _core_mod._get_service_lock(source_key):
                # Fast path: if commit_buffer's snapshot-delta update kept
                # _core_mod._snapshot_files_cache aligned with the table we loaded in
                # Phase 1, we can skip the catalog reload + full plan_files()
                # scan entirely. Just flip any s3:// entries to local paths
                # for files we just downloaded.
                cached = _core_mod._snapshot_files_cache.get(source_key)
                if cached and cached[0] == table.metadata_location:
                    _core_mod._reconcile_snapshot_cache_after_sync(source)
                    _core_mod._view_cache.pop(source_key, None)
                    break

                # Slow path: cache miss/stale — re-resolve via catalog scan.
                catalog = _core_mod._get_catalog(source)
                table = _core_mod._load_table_cached(source, _core_mod._table_identifier(source), catalog)
                snap = table.current_snapshot()
                snapshot_id = snap.snapshot_id if snap else None

                from backend.core.duckdb import _cache_dir

                data_dir = os.path.join(_cache_dir(source), "data")

                resolved_files = []
                for f in table.scan().plan_files():
                    uri = f.file.file_path
                    if "/data/" in uri:
                        rel_path = uri.split("/data/")[-1]
                    else:
                        rel_path = uri.split("/")[-1]

                    local_path = os.path.abspath(os.path.join(data_dir, rel_path))
                    if not local_path.startswith(os.path.abspath(data_dir) + os.sep):
                        continue
                    if os.path.exists(local_path):
                        resolved_files.append(local_path)
                    else:
                        resolved_files.append(uri)

                _core_mod._snapshot_files_cache[source_key] = (
                    table.metadata_location,
                    snapshot_id,
                    table.location(),
                    resolved_files,
                )
                _core_mod._save_persistent_cache(source)

                # Invalidate the view SQL cache so it generates a new union with local paths
                _core_mod._view_cache.pop(source_key, None)
            break  # success
        except Exception as e:
            if _is_rate_limited(e) and attempt < _MAX_RETRIES - 1:
                backoff_s = 0.5 * (2**attempt)  # 0.5s, 1s, 2s
                logger.info("[iceberg] FOS rate-limited during cache update, retrying in %.1fs", backoff_s)
                _time.sleep(backoff_s)
                continue
            logger.warning("[iceberg] Failed to update cache after sync: %s", e)
            break

    return {
        "files_downloaded": downloaded,
        "rows_downloaded": rows_downloaded,
        "bytes_downloaded": bytes_downloaded,
        "files_removed": deleted,
        "files_skipped": already_cached,
    }


# Cache for UI metadata scans which are very slow on large tables
# source_key -> (metadata_location, (data_files, size_bytes, calendar))
_ui_metadata_cache: dict[str, tuple] = {}
_ui_metadata_scan_locks: dict[str, threading.Lock] = {}
_ui_metadata_scan_locks_lock = threading.Lock()

# Per-manifest aggregate cache: manifest_path -> (calendar, min_ts, max_ts, files, size).
# Iceberg manifests are immutable once written — a given manifest's entries (and
# therefore its calendar/min/max contribution) never change. This cache lets
# `_get_cached_or_scan_metadata` skip re-fetching every manifest after each
# commit; only manifests new to the current snapshot trigger an .avro GET.
# Persisted to disk per-service so restarts don't pay a ~1250-manifest cold
# scan (~12 MB FOS GETs) on the first cron_compact tick.

