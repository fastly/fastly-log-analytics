"""Per-hour bundling: combine per-field hour parquets into
``rollups/hour_bundled/hour=H/all_fields.parquet``, sweep the per-field
sources after the bundle is published, and a backfill driver for
historical hours.
"""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from datetime import UTC, datetime

from ._common import (
    IP_SPREAD_BUNDLE_FILENAME,
    _hour_bundled_root,
    _ip_spread_root,
    _rollups_root,
    parse_hour_token,
    quote_path_list,
)

logger = logging.getLogger(__name__)


def bundle_hours(service_id: str, source: dict, hours: list[str]) -> int:
    """Combine per-field hour parquets into one bundled parquet per hour.

    For each hour token, reads every per-field parquet under
    rollups/hour/field=*/hour=H/*.parquet and writes a single bundled file
    at rollups/hour_bundled/hour=H/all_fields.parquet.

    Skips hours where:
      - No per-field files exist (nothing to bundle).
      - A bundled file already exists and is fresh enough to skip rebuild
        (per-field mtime <= bundle mtime).

    Returns the count of hours that were rebuilt.

    Skip the active hour — bundles for in-progress hours would race the
    sync's per-field rebuilds. The active hour is served live anyway.
    """
    if not hours:
        return 0

    import duckdb

    from backend.core.iceberg.view import _get_service_lock

    # _rollups_root already returns <cache>/rollups/hour — it's the
    # per-field per-hour tree root, not the rollups/ parent.
    hour_per_field_root = _rollups_root(source)
    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    rebuilt = 0
    # Use :memory: DuckDB to avoid contending with uvicorn's RW connection
    # on the per-service .duckdb file (mirrors compact_closed_days_to_daily —
    # see the 2026-06-06 incident comment in that function). The bundling
    # COPY only needs to read existing parquets and write a new one; it
    # doesn't need any per-service catalog state.
    con = duckdb.connect(":memory:")
    try:
        for hour in hours:
            if hour == active_hour:
                continue
            # Validate hour token format defensively — string lands in
            # filesystem paths and SQL string literals below.
            if parse_hour_token(hour) is None:
                continue

            # Enumerate per-field parquets for this hour.
            per_field_paths: list[str] = []
            max_src_mtime = 0.0
            try:
                for field_entry in os.listdir(hour_per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    hour_dir = os.path.join(hour_per_field_root, field_entry, f"hour={hour}")
                    if not os.path.isdir(hour_dir):
                        continue
                    for fname in os.listdir(hour_dir):
                        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                            continue
                        p = os.path.join(hour_dir, fname)
                        per_field_paths.append(p)
                        try:
                            mt = os.path.getmtime(p)
                            if mt > max_src_mtime:
                                max_src_mtime = mt
                        except OSError:
                            pass
            except OSError:
                continue

            if not per_field_paths:
                continue

            # Skip if bundle is already up-to-date — but still run the
            # per-field cleanup against this hour, because:
            #   (a) backlog from before the cleanup pass shipped means
            #       many already-bundled hours still carry stale per-
            #       field copies on disk; this branch is how they get
            #       reaped without forcing an explicit one-shot job;
            #   (b) the cleanup is a no-op if there's nothing to delete
            #       (no per-field dirs for the hour), so the cost is
            #       one os.listdir.
            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            bundle_path = os.path.join(bundle_dir, "all_fields.parquet")
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        _cleanup_per_field_after_bundle(
                            hour_per_field_root,
                            hour,
                            bundle_path,
                            service_id,
                        )
                        continue
                except OSError:
                    pass

            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = quote_path_list(per_field_paths)
            # Read the per-field parquets (each has columns field/value/count)
            # and write to a single bundled parquet. Use COPY for atomicity
            # via the tmp + rename pattern.
            query = (
                f"COPY (SELECT field, value, CAST(count AS BIGINT) AS count "
                f"FROM read_parquet([{paths_sql}])) "
                f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: bundle COPY failed for hour=%s: %s", service_id, hour, e)
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            with _get_service_lock(lock_key):
                # Atomic publish — os.replace is atomic on POSIX.
                os.replace(tmp_path, bundle_path)
                # Now that the bundle for this hour is on disk and at least
                # as new as every per-field source we just read, the per-
                # field per-hour files are redundant — the reader prefers
                # the bundled path. Sweep them so the active-day query
                # window stops opening N×72 small parquets when N hours
                # have already been bundled. The active hour is skipped
                # above, so we only ever clean closed hours.
                # Guarded by ROLLUP_CLEANUP_DRY_RUN=1 for the first-deploy
                # log-only audit before the actual unlinks ship.
                _cleanup_per_field_after_bundle(
                    hour_per_field_root,
                    hour,
                    bundle_path,
                    service_id,
                )
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


def stamp_empty_hour_sentinels(service_id: str, source: dict, hours: list[str]) -> int:
    """Write an EMPTY sentinel bundle for closed hours that have NO rows.

    A zero-row closed hour can never acquire rollup coverage through the
    data-driven writers: the per-field COPY's PARTITION_BY emits no file
    for an empty partition, ``bundle_hours`` skips hours with no per-field
    files, and ``backfill_missing_hour_bundles`` discovers candidate hours
    via ``HAVING n > 0``. Without a marker, the reader's missing-hour live
    heal classifies every quiet closed hour as a writer gap and live-scans
    it on EVERY request — permanent hot-path cost on exactly the bursty /
    low-traffic services the heal exists for, plus a misleading operator
    warning. An empty ``all_fields.parquet`` (same field/value/count
    schema as a real bundle) marks the hour as VERIFIED EMPTY: the reader
    counts it as covered, and it contributes zero rows to the UNION ALL.

    Late data is safe: if rows stamped in a sentinel'd hour arrive later,
    the per-sync recompute writes newer per-field files and
    ``bundle_hours``' mtime check rebuilds the real bundle over the
    sentinel.

    Returns the count of sentinels written. Idempotent — hours with any
    existing bundle (real or sentinel) are skipped.
    """
    if not hours:
        return 0

    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.core.iceberg.view import _get_service_lock

    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    # Same column shape as a real bundle (value is CAST(... AS VARCHAR) in
    # the per-field COPY), so the reader's UNION ALL types line up.
    schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
    empty_table = pa.table({"field": [], "value": [], "count": []}, schema=schema)

    stamped = 0
    for hour in hours:
        if hour >= active_hour:
            # Active (or future) hour — still receiving writes; served live.
            continue
        if parse_hour_token(hour) is None:
            continue
        bundle_dir = os.path.join(bundled_root, f"hour={hour}")
        bundle_path = os.path.join(bundle_dir, "all_fields.parquet")
        if os.path.exists(bundle_path):
            continue
        os.makedirs(bundle_dir, exist_ok=True)
        tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
        try:
            pq.write_table(empty_table, tmp_path)
            with _get_service_lock(lock_key):
                os.replace(tmp_path, bundle_path)
            stamped += 1
        except OSError as e:
            logger.warning("[rollups] %s: empty-hour sentinel write failed for hour=%s: %s", service_id, hour, e)
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    return stamped


def bundle_hours_ip_spread(service_id: str, source: dict, hours: list[str]) -> int:
    """Combine per-field IP-spread hour parquets into one bundled parquet per hour.

    Companion to :func:`bundle_hours` for the new IP-spread tree. For
    each hour token, reads every per-field IP-spread parquet under
    ``rollups/hour_ip_spread/field=*/hour=H/*.parquet`` and writes a
    single bundled file at
    ``rollups/hour_bundled/hour=H/all_fields_ip.parquet`` (sibling to
    ``all_fields.parquet`` produced by :func:`bundle_hours`). Skips
    hours with no per-field IP-spread files and hours where the
    bundle is already up-to-date relative to source mtimes.

    Skip the active hour for the same reason :func:`bundle_hours`
    does — bundling an in-progress hour would race the writer's
    per-field rebuilds.

    Returns the count of hours rebuilt. Does NOT sweep the per-field
    source files after publish — the per-field IP-spread tree is
    quite small (one parquet per (field, hour) at most), so the
    storage saving wouldn't justify the read-path coordination that
    the count-side cleanup carries (see
    :func:`_cleanup_per_field_after_bundle`). Lifecycle for those
    files is handled by the existing ``cleanup_old_rollups``
    retention path on the same age cutoff.
    """
    if not hours:
        return 0

    import duckdb

    from backend.core.iceberg.view import _get_service_lock

    ip_spread_per_field_root = _ip_spread_root(source)
    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    if not os.path.isdir(ip_spread_per_field_root):
        # Cold pool: writer hasn't produced any IP-spread parquets yet.
        # Bundling has nothing to do until the first writer tick lands.
        return 0

    rebuilt = 0
    con = duckdb.connect(":memory:")
    try:
        for hour in hours:
            if hour == active_hour:
                continue
            if parse_hour_token(hour) is None:
                continue

            per_field_paths: list[str] = []
            max_src_mtime = 0.0
            try:
                for field_entry in os.listdir(ip_spread_per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    hour_dir = os.path.join(ip_spread_per_field_root, field_entry, f"hour={hour}")
                    if not os.path.isdir(hour_dir):
                        continue
                    for fname in os.listdir(hour_dir):
                        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                            continue
                        p = os.path.join(hour_dir, fname)
                        per_field_paths.append(p)
                        try:
                            mt = os.path.getmtime(p)
                            if mt > max_src_mtime:
                                max_src_mtime = mt
                        except OSError:
                            pass
            except OSError:
                continue

            if not per_field_paths:
                continue

            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            bundle_path = os.path.join(bundle_dir, IP_SPREAD_BUNDLE_FILENAME)
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        # Bundle already current — nothing to rebuild.
                        # Skip the per-field cleanup branch the count-side
                        # bundler runs here; see this function's docstring.
                        continue
                except OSError:
                    pass

            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = quote_path_list(per_field_paths)
            # Read every per-field IP-spread parquet for this hour and
            # write one bundled file. The HLL ``ip_sketch`` BLOB column
            # passes through verbatim — DuckDB doesn't introspect BLOB
            # contents, so the sketches arrive at the reader byte-
            # identical to what the writer produced.
            query = (
                "COPY (SELECT field, value, ip_sketch, "
                "CAST(ip_count_observed AS BIGINT) AS ip_count_observed, "
                "sample_capped "
                f"FROM read_parquet([{paths_sql}])) "
                f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning(
                    "[rollups] %s: ip_spread bundle COPY failed for hour=%s: %s",
                    service_id,
                    hour,
                    e,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            with _get_service_lock(lock_key):
                os.replace(tmp_path, bundle_path)
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


def _cleanup_per_field_after_bundle(
    hour_per_field_root: str,
    hour: str,
    bundle_path: str,
    service_id: str,
) -> None:
    """Sweep the per-field per-hour parquet directories for ``hour`` after
    a fresh hour bundle has been published.

    Safety checks (any failure → log and bail, do NOT unlink):
    - ``hour_bundled/.../all_fields.parquet`` exists on disk.
    - Bundle mtime ≥ max per-field mtime under hour=HOUR (i.e. the bundle
      includes everything that's currently in the per-field tree).

    Reader fallback at backend/repositories/_base.py:937-1003 prefers the
    bundled file, so dropping per-field for a bundled hour is safe; if a
    bundle ever gets deleted, ``backfill_rollups`` or the next sync tick
    rebuilds per-field from the base data. Loss of dual-storage redundancy
    is the trade for the file-count win.

    Gated on ROLLUP_CLEANUP_DRY_RUN=1: when set, log "would delete N
    files" instead of unlinking. First prod tick should run with this to
    confirm the math, then unset.
    """
    if not os.path.exists(bundle_path):
        return
    try:
        bundle_mtime = os.path.getmtime(bundle_path)
    except OSError:
        return

    dry_run = os.environ.get("ROLLUP_CLEANUP_DRY_RUN") == "1"
    candidate_dirs: list[str] = []
    file_count = 0
    try:
        for field_entry in os.listdir(hour_per_field_root):
            if not field_entry.startswith("field="):
                continue
            hour_dir = os.path.join(hour_per_field_root, field_entry, f"hour={hour}")
            if not os.path.isdir(hour_dir):
                continue
            # Bundle must be at least as new as every per-field file in
            # the dir, otherwise we'd lose data published since the
            # bundle ran. (Belt-and-suspenders — bundle_hours already
            # verifies max_src_mtime ≤ bundle mtime before reusing an
            # existing bundle, but a concurrent recompute could have
            # rewritten a per-field file between the bundle COPY and
            # this sweep.)
            ok = True
            count_here = 0
            try:
                for fname in os.listdir(hour_dir):
                    if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                        continue
                    p = os.path.join(hour_dir, fname)
                    try:
                        if os.path.getmtime(p) > bundle_mtime:
                            ok = False
                            break
                        count_here += 1
                    except OSError:
                        ok = False
                        break
            except OSError:
                ok = False
            if ok and count_here > 0:
                candidate_dirs.append(hour_dir)
                file_count += count_here
    except OSError:
        return

    if not candidate_dirs:
        return

    if dry_run:
        logger.info(
            "[rollups] %s: ROLLUP_CLEANUP_DRY_RUN — would delete %d per-field parquets across %d field dirs for hour=%s",
            service_id,
            file_count,
            len(candidate_dirs),
            hour,
        )
        return

    deleted_files = 0
    deleted_dirs = 0
    for hour_dir in candidate_dirs:
        try:
            shutil.rmtree(hour_dir)
            deleted_dirs += 1
            deleted_files += 1  # underestimate; we don't recount post-delete
        except OSError as e:
            logger.warning("[rollups] %s: cleanup failed for %s: %s", service_id, hour_dir, e)
    logger.debug(
        "[rollups] %s: cleaned %d per-field dirs (~%d parquets) for bundled hour=%s",
        service_id,
        deleted_dirs,
        file_count,
        hour,
    )
