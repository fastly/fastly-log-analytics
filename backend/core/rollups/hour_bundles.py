"""Per-hour bundling: combine per-field hour parquets into
``rollups/hour_bundled/hour=H/all_fields.parquet``, sweep the per-field
sources after the bundle is published, and a backfill driver for
historical hours.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from ._common import (
    IP_SPREAD_BUNDLE_FILENAME,
    _hour_bundled_root,
    _ip_spread_root,
    _rollups_root,
    cleanup_per_field_after_bundle,
    parquet_zstd_target,
    parse_hour_token,
    quote_path_list,
    unlink_quiet,
)

logger = logging.getLogger(__name__)


def _bundle_per_field_hours_impl(
    service_id: str,
    source: dict,
    hours: list[str],
    *,
    per_field_root: str,
    bundle_filename: str,
    select_columns_sql: str,
    cleanup_fn: Callable[[str, str, str, str], None],
    err_label: str,
    require_root_exists: bool = False,
) -> int:
    if not hours:
        return 0

    import duckdb

    from backend.core.iceberg.view import _get_service_lock

    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    if require_root_exists and not os.path.isdir(per_field_root):
        return 0

    rebuilt = 0
    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        for hour in hours:
            if hour == active_hour:
                continue
            if parse_hour_token(hour) is None:
                continue

            per_field_paths: list[str] = []
            max_src_mtime = 0.0
            try:
                for field_entry in os.listdir(per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    hour_dir = os.path.join(per_field_root, field_entry, f"hour={hour}")
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
            bundle_path = os.path.join(bundle_dir, bundle_filename)
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        cleanup_fn(per_field_root, hour, bundle_path, service_id)
                        continue
                except OSError:
                    pass

            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = quote_path_list(per_field_paths)
            query = f"COPY ({select_columns_sql} FROM read_parquet([{paths_sql}])) {parquet_zstd_target(tmp_path)}"
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: %s COPY failed for hour=%s: %s", service_id, err_label, hour, e)
                unlink_quiet(tmp_path)
                continue

            with _get_service_lock(lock_key):
                os.replace(tmp_path, bundle_path)
                cleanup_fn(per_field_root, hour, bundle_path, service_id)
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


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
    return _bundle_per_field_hours_impl(
        service_id,
        source,
        hours,
        per_field_root=_rollups_root(source),
        bundle_filename="all_fields.parquet",
        select_columns_sql="SELECT field, value, CAST(count AS BIGINT) AS count",
        cleanup_fn=lambda root, hr, bpath, sid: _cleanup_per_field_after_bundle(root, hr, bpath, sid),
        err_label="bundle",
        require_root_exists=False,
    )


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
            unlink_quiet(tmp_path)
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
    """
    return _bundle_per_field_hours_impl(
        service_id,
        source,
        hours,
        per_field_root=_ip_spread_root(source),
        bundle_filename=IP_SPREAD_BUNDLE_FILENAME,
        select_columns_sql=(
            "SELECT field, value, ip_sketch, CAST(ip_count_observed AS BIGINT) AS ip_count_observed, sample_capped"
        ),
        cleanup_fn=lambda root, hr, bpath, sid: _cleanup_ip_spread_per_field_after_bundle(root, hr, bpath, sid),
        err_label="ip_spread bundle",
        require_root_exists=True,
    )


def _cleanup_per_field_after_bundle(
    hour_per_field_root: str,
    hour: str,
    bundle_path: str,
    service_id: str,
) -> None:
    """Sweep the per-field per-hour parquet directories for ``hour`` after
    a fresh hour bundle has been published.
    """
    cleanup_per_field_after_bundle(
        hour_per_field_root,
        f"hour={hour}",
        bundle_path,
        service_id,
        file_kind_label="per-field",
        dir_kind_label="per-field",
        cleanup_label="cleanup",
        logger=logger,
    )


def _cleanup_ip_spread_per_field_after_bundle(
    ip_spread_per_field_root: str,
    hour: str,
    bundle_path: str,
    service_id: str,
) -> None:
    """Sweep the per-field IP-spread parquet directories for ``hour`` after
    a fresh hour bundle has been published.
    """
    cleanup_per_field_after_bundle(
        ip_spread_per_field_root,
        f"hour={hour}",
        bundle_path,
        service_id,
        file_kind_label="per-field ip_spread",
        dir_kind_label="per-field ip_spread",
        cleanup_label="ip_spread cleanup",
        logger=logger,
    )
