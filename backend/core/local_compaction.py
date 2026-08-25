"""Local-only parquet compaction.

Unlike ``iceberg.optimize_table`` which routes through PyIceberg's
``table.overwrite()`` (writes back to FOS, triggering the 30-day minimum
billing penalty on rewritten files), this module operates *only* on the
local cache. The Iceberg catalog on FOS is untouched.

Why this works:

* The dashboard's DuckDB view reads parquet files via a glob pattern
  (``read_parquet('cache/.../data/**/*.parquet')``) that is re-evaluated
  at every query — not from the Iceberg manifest. So merging files in
  the cache directory is immediately visible to queries; no catalog
  refresh required.
* The hour-partition column is COMPUTED from the timestamp at query
  time (``strftime(timestamp, '%Y-%m-%d-%H') as timestamp_hour``), not
  extracted from the file path — so files can move to a different
  directory layout (e.g., a single ``daily/`` dir holding cross-hour
  merges) without breaking partition filtering.
* FOS still holds the original raw 5-minute files, untouched. The next
  ``sync_data`` from FOS pulls only files we haven't downloaded yet
  (tracked by the local snapshot-files cache), so a compacted local
  parquet doesn't get re-fetched as raw small files.

Trade-offs:

* The Iceberg catalog's per-file metadata becomes lightly stale (it
  still references the small original files). This is fine for query
  serving (we don't read the catalog) but means an Iceberg-native
  consumer of the catalog would see "missing" files. Local cache only.
* If the catalog cache is ever wiped and re-pulled from FOS, the
  small-file metadata returns — but a subsequent local compaction
  pass will re-merge them.

Use this for hot-tier compaction every few minutes. Use the
``optimize_table`` path when you want compaction reflected in FOS too
(e.g., for an external Iceberg reader).
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

# Don't merge a partition whose total parquet size already exceeds this — we'd
# just produce one absurdly huge file that destroys query parallelism on the
# next scan. Tune via env var if a hot site needs different ergonomics.
_MAX_PARTITION_BYTES = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256")) * 1024 * 1024

# Partitions older than this become eligible for cross-hour daily compaction.
# Recent hours stay hourly so the dashboard's time-range pruning stays tight
# at the file level (each scan opens one daily file vs 24 hourly files).
_DAILY_TIER_AGE_DAYS = int(os.environ.get("LOCAL_COMPACT_DAILY_TIER_DAYS", "1"))

# Daily files older than this become eligible for cross-day weekly compaction.
# Only useful when log_retention_days > this; otherwise daily files just age
# out of the cache before any weekly rollup could happen. Default 30 matches
# the common retention window — a no-op for shorter retentions.
_WEEKLY_TIER_AGE_DAYS = int(os.environ.get("LOCAL_COMPACT_WEEKLY_TIER_DAYS", "30"))

# Directories under cache/<bucket>/data/ that hold cross-hour merged files.
# The dashboard's view-glob is data/**/*.parquet so sibling dirs are fine.
_DAILY_DIR = "daily"
_WEEKLY_DIR = "weekly"

_HOUR_PART_RE = re.compile(r"^timestamp_hour=(\d{4}-\d{2}-\d{2})-(\d{2})$")
# Daily file naming from _compact_daily_tier: daily_YYYY-MM-DD_<hex>.parquet
_DAILY_FILE_RE = re.compile(r"^daily_(\d{4}-\d{2}-\d{2})_[0-9a-f]+\.parquet$")


def _build_merge_select_sql(paths_sql: str, cols_to_strip: list[str], has_rid: bool) -> str:
    """SELECT clause for the COPY that produces a merged parquet.

    When the schema has a ``rid`` column, dedupe by ``rid`` keeping the
    earliest-timestamp occurrence. Without this, the buffer-commit ↔
    tombstone window (``buffer.py:463-477`` — table.append succeeded but
    tombstone_buffer_files crashed before running) causes the same buffer
    file's rows to be committed twice on the retry tick → every row in
    that batch counted twice in every dashboard query. The 2026-06-12
    audit found ~12 days of ~2× duplication from exactly this race.

    ``rid`` is Fastly's per-request id and is unique per logical request,
    so it's the right key. NULL-rid rows pass through unchanged — the row
    is preserved without a uniqueness guarantee. In practice prod data
    has zero NULL rids (verified 2026-06-12), so this branch is defensive.

    When the schema has no ``rid`` column (older sources, test fixtures),
    fall through to a plain SELECT — no dedup, original behaviour.
    """
    exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
    if not has_rid:
        return f"SELECT *{exclude_clause} FROM read_parquet([{paths_sql}], union_by_name=true)"
    # Add _dup_rn to the EXCLUDE list so the helper column doesn't bleed
    # into the output parquet schema.
    inner_exclude_clause = f" EXCLUDE ({', '.join([*cols_to_strip, '_dup_rn'])})"
    return (
        # Non-NULL rid: dedupe, keep earliest occurrence.
        f"SELECT *{inner_exclude_clause} FROM ("
        f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY timestamp) AS _dup_rn"
        f"  FROM read_parquet([{paths_sql}], union_by_name=true)"
        f"  WHERE rid IS NOT NULL"
        f") WHERE _dup_rn = 1"
        f" UNION ALL BY NAME "
        # NULL rid: pass through.
        f"SELECT *{exclude_clause} FROM read_parquet([{paths_sql}], union_by_name=true)"
        f" WHERE rid IS NULL"
    )


def _bin_pack_files(file_paths: list[str], max_bin_size_bytes: int) -> list[list[str]]:
    """Group file_paths into bins such that the sum of file sizes in each bin
    does not exceed max_bin_size_bytes. Preserves the original file order.
    If any single file exceeds max_bin_size_bytes, it goes in its own bin.
    """
    bins: list[list[str]] = []
    current_bin: list[str] = []
    current_size = 0

    for path in file_paths:
        try:
            file_size = os.path.getsize(path)
        except OSError:
            continue

        if current_size + file_size > max_bin_size_bytes:
            if current_bin:
                bins.append(current_bin)
                current_bin = []
                current_size = 0

            if file_size >= max_bin_size_bytes:
                bins.append([path])
            else:
                current_bin.append(path)
                current_size = file_size
        else:
            current_bin.append(path)
            current_size += file_size

    if current_bin:
        bins.append(current_bin)

    return bins


def compact_local_partitions(
    source: dict, min_files_per_partition: int = 1, dry_run: bool = False, table_name: str = "logs"
) -> dict[str, Any]:
    """Merge small parquet files within each hour-partition directory into
    a single larger file. Additionally rolls partitions older than
    ``_DAILY_TIER_AGE_DAYS`` into per-day merged files.

    Args:
        source: service source dict (used to resolve cache path)
        min_files_per_partition: only partitions with strictly more than
            this many files are touched. Default 1 — every multi-file
            partition is eligible. This is required for the dedup-on-merge
            pass (see ``_build_merge_select_sql``) to clean up the
            orphan-file dup pattern: a partition with exactly 3 files (one
            ``compacted_*`` + a 2-split ``00000-N-*`` orphan pair from a
            buffer-commit replay) needs ``> 1`` to be considered, not the
            previous ``> 3``. Without this, the historic 12 days of ~2×
            duplication would never self-heal.
        dry_run: if True, report what would be done without writing.
        table_name: table name identifier ("logs", "client_vitals", "client_errors")

    Returns:
        Result dict — see implementation for fields.
    """
    from backend.core.duckdb import _cache_dir

    t0 = time.time()
    cache_root = _cache_dir(source)
    data_dir = os.path.join(cache_root, "data" if table_name == "logs" else f"data_{table_name}")
    result: dict[str, Any] = {
        "partitions_scanned": 0,
        "partitions_compacted": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "daily_rollups": 0,
        "weekly_rollups": 0,
        "active_hour_skipped": False,
        "stale_tmp_removed": 0,
        "errors": [],
        "duration_ms": 0,
        "dry_run": dry_run,
    }

    if not os.path.isdir(data_dir):
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    # Acquire the per-service RLock around the file-system mutation
    # phase so concurrent dashboard queries via the view-build path
    # don't race with our delete-then-rename and hit FileNotFoundError /
    # IO Error mid-glob. Architecture-review Finding #3.
    from backend.core.iceberg.view import _get_service_lock

    service_key = source.get("name", "default")
    publish_lock = _get_service_lock(service_key)

    # ── Cleanup pass: remove orphaned .tmp_ files from previous crashed
    # runs. Safe because the publish step renames .tmp_<name> → <name>;
    # any leftover .tmp_ is by definition incomplete. The dashboard glob
    # matches *.parquet so leftovers don't pollute queries, but they
    # do waste disk and confuse the file-count metric.
    if not dry_run:
        with publish_lock:
            result["stale_tmp_removed"] = _cleanup_stale_tmp(data_dir)

    # ── Active-hour guard: do NOT compact the current UTC hour. The sync
    # cron may be flushing buffer files into this partition mid-pass; our
    # delete-then-rename is atomic per-file but a half-second window after
    # we listdir() and before we delete is enough for a freshly-arrived
    # file to be in the listing of one operation and gone from the other.
    # Skipping the active hour is cheap and removes the race entirely.
    active_hour = datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
    result["active_hour_skipped"] = os.path.isdir(os.path.join(data_dir, active_hour))

    # Accumulate every basename we delete across all three tiers so we
    # can register them in one SQLite write at the end (vs N small writes).
    removed_basenames: list[str] = []

    # ── Hourly tier: walk each partition dir, merge if multi-file.
    for entry in sorted(os.listdir(data_dir)):
        if entry == active_hour:
            continue
        if entry in (_DAILY_DIR, _WEEKLY_DIR):
            continue  # daily/weekly rollup dirs handled in subsequent passes
        part_dir = os.path.join(data_dir, entry)
        if not os.path.isdir(part_dir):
            continue
        parquets = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
        if len(parquets) <= min_files_per_partition:
            continue

        # Sort files alphabetically for deterministic sequential binning
        parquets_sorted = sorted(parquets)
        full_paths = [os.path.join(part_dir, f) for f in parquets_sorted]
        bins = _bin_pack_files(full_paths, _MAX_PARTITION_BYTES)

        # In normal compaction (``min_files_per_partition >= 1``) a single-
        # file bin is a no-op — there's nothing to merge. In force-rewrite
        # mode (``== 0``, one-shot dedup pass) we DO want to rewrite even
        # singletons so intra-file dups in long-stable partitions get the
        # dedup-by-rid pass.
        if min_files_per_partition == 0:
            eligible_bins = bins
        else:
            eligible_bins = [b for b in bins if len(b) > 1]
        if not eligible_bins:
            continue

        result["partitions_scanned"] += 1
        partition_compacted = False

        for bin_paths in eligible_bins:
            bin_basenames = [os.path.basename(p) for p in bin_paths]
            try:
                # Lock held only during the actual file-system mutation (delete +
                # rename) inside _compact_single_partition; the parquet COPY
                # write happens before that on an in-memory DuckDB connection and
                # doesn't need the lock. Holding the lock during the COPY would
                # block dashboard reads for ~1s per partition.
                with publish_lock:
                    r = _compact_single_partition(part_dir, bin_basenames, dry_run=dry_run)
                partition_compacted = True
                result["files_merged"] += r["files_merged"]
                result["files_removed"] += r["files_removed"]
                result["bytes_before"] += r["bytes_before"]
                result["bytes_after"] += r["bytes_after"]
                removed_basenames.extend(r.get("removed_basenames", []))
            except Exception as e:
                msg = f"{part_dir} (bin): {type(e).__name__}: {e}"
                logger.warning("[local-compact] %s", msg)
                result["errors"].append(msg)

        if partition_compacted:
            result["partitions_compacted"] += 1

    # ── Daily tier: roll up hour-partitions older than threshold into one
    # daily file. After this, the partition's hour dirs are removed.
    try:
        # Same RLock as the hourly path — file-system mutation phase only.
        with publish_lock:
            r = _compact_daily_tier(data_dir, dry_run=dry_run)
        result["daily_rollups"] = r["daily_rollups"]
        result["files_merged"] += r["files_merged"]
        result["files_removed"] += r["files_removed"]
        result["bytes_before"] += r["bytes_before"]
        result["bytes_after"] += r["bytes_after"]
        removed_basenames.extend(r.get("removed_basenames", []))
    except Exception as e:
        msg = f"daily-tier: {type(e).__name__}: {e}"
        logger.warning("[local-compact] %s", msg)
        result["errors"].append(msg)

    # ── Weekly tier: roll up daily files older than threshold into one
    # weekly file. Only does work when local retention extends past
    # _WEEKLY_TIER_AGE_DAYS (default 30); for shorter retentions the
    # daily files age out before becoming weekly-eligible.
    try:
        with publish_lock:
            r = _compact_weekly_tier(data_dir, dry_run=dry_run)
        result["weekly_rollups"] = r["weekly_rollups"]
        result["files_merged"] += r["files_merged"]
        result["files_removed"] += r["files_removed"]
        result["bytes_before"] += r["bytes_before"]
        result["bytes_after"] += r["bytes_after"]
        removed_basenames.extend(r.get("removed_basenames", []))
    except Exception as e:
        msg = f"weekly-tier: {type(e).__name__}: {e}"
        logger.warning("[local-compact] %s", msg)
        result["errors"].append(msg)

    # ── Register deletions so sync_data won't re-fetch them. Without this,
    # every local_compact pass invalidates sync_data's fast path and forces
    # a full re-download of every file we just deleted.
    service_id = source.get("service_id") or source.get("name")
    if removed_basenames and service_id and not dry_run:
        try:
            from backend.core import metadata as _meta

            _meta.register_locally_compacted(service_id, removed_basenames)
        except Exception as e:
            logger.warning("[local-compact] failed to register compacted basenames: %s", e)

    result["duration_ms"] = int((time.time() - t0) * 1000)
    logger.info(
        "🧹 [local-compact] %s: hourly=%d/%d merged=%d removed=%d daily=%d weekly=%d tmp_cleaned=%d in %dms",
        source.get("name"),
        result["partitions_compacted"],
        result["partitions_scanned"],
        result["files_merged"],
        result["files_removed"],
        result["daily_rollups"],
        result["weekly_rollups"],
        result["stale_tmp_removed"],
        result["duration_ms"],
    )
    return result


def _cleanup_stale_tmp(data_dir: str) -> int:
    """Walk data_dir and rm any *.parquet.tmp left over from crashed runs.

    Also cleans up the OLD naming convention (.tmp_*.parquet) from the
    earlier version of this module so a deploy doesn't leave orphans
    that the parquet glob picks up (causing view-build errors).
    """
    n = 0
    for root, _, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".parquet.tmp") or (f.startswith(".tmp_") and f.endswith(".parquet")):
                try:
                    os.remove(os.path.join(root, f))
                    n += 1
                except OSError:
                    pass
    return n


def _rollup_bins(
    bins: list[list[str]],
    out_root: str,
    name_prefix: str,
    token: str,
    rollup_key: str,
    result: dict[str, Any],
    *,
    dry_run: bool,
    log_migrate,
    log_migrate_fail,
    log_merge,
    log_merge_fail,
) -> None:
    """Roll up each pre-packed bin into one output parquet under ``out_root``.

    Shared inner loop of the daily and weekly tiers. A single-file bin is
    migrated via ``os.rename``; a multi-file bin is merged via a DuckDB COPY
    into a tmp file then atomically renamed, with originals deleted into
    ``result['removed_basenames']`` and the tmp cleaned on failure. ``token``
    is the per-group date key embedded in the output filename (day_str /
    week_key); ``rollup_key`` is the result counter to bump (daily_rollups /
    weekly_rollups). The four log callables let each tier keep its exact log
    strings (emoji + wording) unchanged.
    """
    for bin_paths in bins:
        bytes_before = sum(os.path.getsize(p) for p in bin_paths)
        if dry_run:
            result[rollup_key] += 1
            result["files_merged"] += len(bin_paths)
            result["bytes_before"] += bytes_before
            continue

        if len(bin_paths) == 1:
            # Migrate single-file bin to the tier folder to retire the source.
            old_path = bin_paths[0]
            old_name = os.path.basename(old_path)
            out_name = f"{name_prefix}{token}_{uuid.uuid4().hex[:8]}.parquet"
            out_path = os.path.join(out_root, out_name)
            try:
                os.rename(old_path, out_path)
                result["files_removed"] += 1
                result.setdefault("removed_basenames", []).append(old_name)
                result[rollup_key] += 1
                result["files_merged"] += 1
                result["bytes_before"] += bytes_before
                result["bytes_after"] += bytes_before
                log_migrate(old_name, out_name)
            except Exception as e:
                log_migrate_fail(old_path, e)
        else:
            out_name = f"{name_prefix}{token}_{uuid.uuid4().hex[:8]}.parquet"
            tmp_path = os.path.join(out_root, f"{out_name}.tmp")
            out_path = os.path.join(out_root, out_name)
            try:
                con = duckdb.connect(":memory:")
                try:
                    paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in bin_paths)
                    probe = (
                        con.execute(
                            f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true) LIMIT 0"
                        ).description
                        or []
                    )
                    col_names = {d[0] for d in probe}
                    cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in col_names)
                    select_sql = _build_merge_select_sql(paths_sql, cols_to_strip, "rid" in col_names)
                    order_by = "ORDER BY timestamp"
                    if "ip" in col_names:
                        order_by += ", ip"
                    con.execute(
                        f"COPY ({select_sql} {order_by}) "
                        f"TO '{escape_sql_literal(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                finally:
                    con.close()
                for p in bin_paths:
                    try:
                        os.remove(p)
                        result["files_removed"] += 1
                        result.setdefault("removed_basenames", []).append(os.path.basename(p))
                    except OSError as e:
                        logger.warning("[local-compact] failed to remove %s: %s", p, e)
                os.rename(tmp_path, out_path)
                bytes_after = os.path.getsize(out_path)
                result[rollup_key] += 1
                result["files_merged"] += len(bin_paths)
                result["bytes_before"] += bytes_before
                result["bytes_after"] += bytes_after
                log_merge(token, len(bin_paths))
            except Exception as e:
                # Clean the tmp on failure so we don't leak.
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except OSError:
                    pass
                log_merge_fail(token, e)


def _compact_daily_tier(data_dir: str, dry_run: bool = False) -> dict[str, Any]:
    """Group hour-partitions older than _DAILY_TIER_AGE_DAYS by day, merge
    each day's parquets into size-capped daily files under data/daily/, and
    remove the now-empty hour partition dirs.

    Returns {daily_rollups, files_merged, files_removed, bytes_before, bytes_after}.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=_DAILY_TIER_AGE_DAYS)).date()
    daily_root = os.path.join(data_dir, _DAILY_DIR)

    # day_str -> [(hour_part_dir, [parquet_paths])]
    by_day: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)
    for entry in os.listdir(data_dir):
        m = _HOUR_PART_RE.match(entry)
        if not m:
            continue
        day_str = m.group(1)
        try:
            day = datetime.strptime(day_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if day >= cutoff:
            continue
        part_dir = os.path.join(data_dir, entry)
        parquets = [
            os.path.join(part_dir, f)
            for f in os.listdir(part_dir)
            if f.endswith(".parquet") and not f.startswith(".tmp_")
        ]
        if not parquets:
            continue
        by_day[day_str].append((part_dir, parquets))

    result: dict[str, Any] = {
        "daily_rollups": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "removed_basenames": [],
    }
    if not by_day:
        return result

    if not dry_run:
        os.makedirs(daily_root, exist_ok=True)

    for day_str, parts in by_day.items():
        all_paths: list[str] = []
        for _, paths in parts:
            all_paths.extend(paths)

        # Sort files alphabetically/chronologically for deterministic sequential binning
        all_paths = sorted(all_paths)
        bins = _bin_pack_files(all_paths, _MAX_PARTITION_BYTES)

        _rollup_bins(
            bins,
            daily_root,
            "daily_",
            day_str,
            "daily_rollups",
            result,
            dry_run=dry_run,
            log_migrate=lambda old, out: logger.info("🚚 [local-compact] migrated single-file bin %s to %s", old, out),
            log_migrate_fail=lambda old, e: logger.warning(
                "[local-compact] failed to migrate single-file %s: %s", old, e
            ),
            log_merge=lambda tok, n: logger.info("📦 [local-compact] daily bin rollup %s: %d files → 1", tok, n),
            log_merge_fail=lambda tok, e: logger.warning("[local-compact] daily bin rollup %s failed: %s", tok, e),
        )

        # Try to rmdir the now-empty hour partition dirs.
        if not dry_run:
            for part_dir, _ in parts:
                try:
                    os.rmdir(part_dir)
                except OSError:
                    pass  # dir not empty (concurrent write) — leave it

    return result


def _compact_weekly_tier(data_dir: str, dry_run: bool = False) -> dict[str, Any]:
    """Group daily files older than _WEEKLY_TIER_AGE_DAYS by ISO week, merge
    each week's parquets into size-capped weekly files under data/weekly/,
    and delete originals.

    Operates on files in data/daily/ produced by _compact_daily_tier. The
    daily filenames embed YYYY-MM-DD (the rollup date), which we parse with
    _DAILY_FILE_RE to derive ISO week → group.

    Returns {weekly_rollups, files_merged, files_removed, bytes_before, bytes_after}.
    """
    daily_root = os.path.join(data_dir, _DAILY_DIR)
    weekly_root = os.path.join(data_dir, _WEEKLY_DIR)
    result: dict[str, Any] = {
        "weekly_rollups": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "removed_basenames": [],
    }
    if not os.path.isdir(daily_root):
        return result

    from datetime import date as _date

    cutoff = (datetime.now(UTC) - timedelta(days=_WEEKLY_TIER_AGE_DAYS)).date()
    # week_key → [(path, date)]
    by_week: dict[str, list[tuple[str, _date]]] = defaultdict(list)
    for fname in os.listdir(daily_root):
        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
            continue
        m = _DAILY_FILE_RE.match(fname)
        if not m:
            continue
        try:
            day = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        if day >= cutoff:
            continue
        # ISO week key like "2026-W22". Days in the same calendar week
        # share the key; year-week handles year boundaries (W52→W01).
        iso = day.isocalendar()
        week_key = f"{iso[0]:04d}-W{iso[1]:02d}"
        by_week[week_key].append((os.path.join(daily_root, fname), day))

    if not by_week:
        return result

    if not dry_run:
        os.makedirs(weekly_root, exist_ok=True)

    for week_key, items in by_week.items():
        if len(items) < 2:
            continue  # nothing to merge for a single-day week

        # Sort daily files alphabetically/chronologically for deterministic sequential binning
        items_sorted = sorted(items, key=lambda x: x[0])
        all_paths = [p for p, _ in items_sorted]
        bins = _bin_pack_files(all_paths, _MAX_PARTITION_BYTES)

        _rollup_bins(
            bins,
            weekly_root,
            "weekly_",
            week_key,
            "weekly_rollups",
            result,
            dry_run=dry_run,
            log_migrate=lambda old, out: logger.info(
                "🚚 [local-compact] migrated single-file weekly bin %s to %s", old, out
            ),
            log_migrate_fail=lambda old, e: logger.warning(
                "[local-compact] failed to migrate single-file weekly bin %s: %s", old, e
            ),
            log_merge=lambda tok, n: logger.info(
                "🗓️  [local-compact] weekly bin rollup %s: %d daily file(s) → 1", tok, n
            ),
            log_merge_fail=lambda tok, e: logger.warning("[local-compact] weekly bin rollup %s failed: %s", tok, e),
        )

    return result


def _compact_single_partition(part_dir: str, parquets: list[str], dry_run: bool = False) -> dict[str, Any]:
    """Merge `parquets` (relative names) in `part_dir` into one new parquet.

    Uses DuckDB COPY to read+write since it's already in the dep tree and
    handles the union-by-name semantics the view uses.
    """
    paths = [os.path.join(part_dir, p) for p in parquets]
    bytes_before = sum(os.path.getsize(p) for p in paths)

    if dry_run:
        return {
            "files_merged": len(parquets),
            "files_removed": 0,
            "bytes_before": bytes_before,
            "bytes_after": 0,
        }

    # Write to a temp file in the same directory so the atomic rename
    # below stays within one filesystem (rename across filesystems is
    # NOT atomic on POSIX).
    out_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
    tmp_name = f"{out_name}.tmp"
    tmp_path = os.path.join(part_dir, tmp_name)
    out_path = os.path.join(part_dir, out_name)

    # Use in-memory DuckDB so we don't contend with the per-service writer
    # lock. read_parquet with explicit list + union_by_name matches the
    # view's semantics so the resulting file is query-compatible.
    con = duckdb.connect(":memory:")
    try:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in paths)
        # Strip the computed `timestamp_hour` / `dt` columns from output
        # if they exist in input files. Iceberg's view-build re-adds them
        # via `SELECT *, strftime(...) AS timestamp_hour` and would error
        # with a duplicate-column UNION ALL BY NAME on a merged file
        # that already contained them.
        probe = con.execute(f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true) LIMIT 0").description or []
        col_names = {d[0] for d in probe}
        cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in col_names)
        select_sql = _build_merge_select_sql(paths_sql, cols_to_strip, "rid" in col_names)
        order_by = "ORDER BY timestamp"
        if "ip" in col_names:
            order_by += ", ip"
        # zstd compression matches Fastly's parquet output and the
        # buffer-commit writer; keeps decompression cost stable.
        con.execute(
            f"COPY ({select_sql} {order_by}) TO '{escape_sql_literal(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()

    # Atomic publish: delete originals BEFORE rename so a crash leaves
    # only the tmp (which the dashboard glob ignores via the .parquet.tmp
    # suffix). Worst case: cleanup pass next run removes the orphaned tmp.
    files_removed = 0
    removed_basenames: list[str] = []
    for p in paths:
        try:
            os.remove(p)
            files_removed += 1
            removed_basenames.append(os.path.basename(p))
        except OSError as e:
            logger.warning("[local-compact] failed to remove %s: %s", p, e)
    os.rename(tmp_path, out_path)
    bytes_after = os.path.getsize(out_path)

    return {
        "files_merged": len(parquets),
        "files_removed": files_removed,
        "removed_basenames": removed_basenames,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
    }


# Short-TTL memo for compaction_stats so the 5 s health-snapshot poll
# in the admin UI doesn't redo a fan-out os.listdir per service per
# tick. The cron's actual local-compact runs every 2 min, so even a
# 5 s lag is well inside the staleness budget the dashboard already
# tolerates.
_COMPACTION_STATS_TTL = 5.0
_COMPACTION_STATS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def compaction_stats(source: dict, table_name: str = "logs") -> dict[str, Any]:
    """Snapshot of file-count distribution across local cache partitions.

    Returns counts that downstream metrics / health endpoints can graph
    to spot small-file regressions (e.g., if the cron stops running and
    files start accumulating, ``partitions_above_threshold`` climbs).

    Results are memoised per cache_root + table_name for ``_COMPACTION_STATS_TTL`` s
    so the admin health-snapshot poll doesn't re-walk the data dir on
    every tick.
    """
    from backend.core.duckdb import _cache_dir

    cache_root = _cache_dir(source)
    now = time.monotonic()
    cache_key = f"{cache_root}:{table_name}"
    cached = _COMPACTION_STATS_CACHE.get(cache_key)
    if cached is not None and (now - cached[0]) < _COMPACTION_STATS_TTL:
        return cached[1]

    data_dir = os.path.join(cache_root, "data" if table_name == "logs" else f"data_{table_name}")
    total_files = 0
    partitions = 0
    above_3 = 0
    above_10 = 0
    daily_files = 0
    weekly_files = 0
    if not os.path.isdir(data_dir):
        result: dict[str, Any] = {
            "total_files": 0,
            "partitions": 0,
            "partitions_above_3": 0,
            "partitions_above_10": 0,
            "daily_files": 0,
            "weekly_files": 0,
            "avg_files_per_partition": 0.0,
        }
        _COMPACTION_STATS_CACHE[cache_key] = (now, result)
        return result
    for entry in os.listdir(data_dir):
        full = os.path.join(data_dir, entry)
        if not os.path.isdir(full):
            continue
        n = sum(1 for f in os.listdir(full) if f.endswith(".parquet") and not f.startswith(".tmp_"))
        if entry == _DAILY_DIR:
            daily_files += n
        elif entry == _WEEKLY_DIR:
            weekly_files += n
        else:
            partitions += 1
            total_files += n
            if n > 3:
                above_3 += 1
            if n > 10:
                above_10 += 1
    result = {
        "total_files": total_files + daily_files + weekly_files,
        "partitions": partitions,
        "partitions_above_3": above_3,
        "partitions_above_10": above_10,
        "daily_files": daily_files,
        "weekly_files": weekly_files,
        "avg_files_per_partition": (total_files / partitions) if partitions else 0.0,
    }
    _COMPACTION_STATS_CACHE[cache_key] = (now, result)
    return result


# R-1: drain the per-service compaction stats TTL cache between tests.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("core.local_compaction._COMPACTION_STATS_CACHE", _COMPACTION_STATS_CACHE)
