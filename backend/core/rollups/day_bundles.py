"""Per-day bundling: combine per-field day parquets into
``rollups/day_bundled/day=D/all_fields.parquet``, plus the closed-day
compactor that builds the per-field day parquets in the first place.
"""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from backend.utils.date_utils import parse_day_key_strict
from backend.utils.sql_validator import escape_sql_literal

from ._common import (
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_FILENAME,
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    ORIGIN_DIMS_BUNDLE_TOP_K,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    PERF_LATENCY_BUNDLE_TOP_K,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    POP_HEALTH_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_TOP_K,
    TIME_SERIES_BUNDLE_FILENAME,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    _day_bundled_root,
    _day_rollups_root,
    _hour_bundled_root,
    _is_safe_ident,
    _rollups_root,
    cleanup_per_field_after_bundle,
    compact_closed_days,
    parquet_zstd_target,
    publish_temp_parquet,
    quote_path_list,
    unlink_quiet,
    wavg,
)

logger = logging.getLogger(__name__)


def _link_day_bundle_alias(src_path: str, dst_path: str) -> None:
    """Create a hardlink alias from src_path to dst_path, falling back to copy.

    The alias is staged at ``dst_path + ".tmp"`` and renamed into place, so a
    reader never sees ``dst_path`` missing and a failure leaves any existing
    alias untouched.
    """
    tmp_path = dst_path + ".tmp"
    Path(tmp_path).unlink(missing_ok=True)
    try:
        try:
            os.link(src_path, tmp_path)
        except OSError:
            shutil.copyfile(src_path, tmp_path)
        os.replace(tmp_path, dst_path)
    except OSError as e:
        logger.warning("[rollups] Failed to create alias %s -> %s: %s", src_path, dst_path, e)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except OSError:
            pass


def bundle_days(service_id: str, source: dict, days: list[str]) -> int:
    """Combine per-field day parquets into one bundled parquet per day.

    For each day token, reads every per-field parquet under
    ``rollups/day/field=*/day=DAY/*.parquet`` and writes a single
    bundled file at
    ``rollups/day_bundled/day=DAY/all_fields.parquet``.

    Skips days where:
      - No per-field files exist (nothing to bundle).
      - A bundled file already exists and is fresh enough to skip
        rebuild (per-field mtime <= bundle mtime).

    Returns the count of days that were rebuilt.

    Skip the active day — per-field day files for in-progress days
    don't exist yet (compact_closed_days_to_daily skips them too).
    Mirrors :func:`bundle_hours` in structure / lock semantics.
    """
    if not days:
        return 0

    import duckdb

    day_per_field_root = _day_rollups_root(source)
    bundled_root = _day_bundled_root(source)
    if not os.path.isdir(day_per_field_root):
        return 0
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_day = datetime.now(UTC).strftime("%Y-%m-%d")

    rebuilt = 0
    # :memory: DuckDB — see bundle_hours for the rationale (avoid
    # contention on the per-service .duckdb file held by uvicorn).
    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        for day in days:
            if day == active_day:
                continue
            # Defensive: validate day token format.
            try:
                parse_day_key_strict(day)
            except ValueError:
                continue

            per_field_paths: list[str] = []
            updated_fields: set[str] = set()
            max_src_mtime = 0.0
            try:
                for field_entry in os.listdir(day_per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    field_name = field_entry[len("field=") :]
                    if not _is_safe_ident(field_name):
                        continue
                    day_dir = os.path.join(day_per_field_root, field_entry, f"day={day}")
                    if not os.path.isdir(day_dir):
                        continue
                    field_has_files = False
                    for fname in os.listdir(day_dir):
                        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                            continue
                        p = os.path.join(day_dir, fname)
                        per_field_paths.append(p)
                        field_has_files = True
                        try:
                            mt = os.path.getmtime(p)
                            if mt > max_src_mtime:
                                max_src_mtime = mt
                        except OSError:
                            pass
                    if field_has_files:
                        updated_fields.add(field_name)
            except OSError:
                continue

            if not per_field_paths:
                continue

            bundle_dir = os.path.join(bundled_root, f"day={day}")
            bundle_path = os.path.join(bundle_dir, DAY_BUNDLE_FILENAME)
            alias_dir_path = os.path.join(bundle_dir, f"day_bundle_{day}.parquet")
            alias_root_path = os.path.join(bundled_root, f"day_bundle_{day}.parquet")
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        if not os.path.exists(alias_dir_path):
                            _link_day_bundle_alias(bundle_path, alias_dir_path)
                        if not os.path.exists(alias_root_path):
                            _link_day_bundle_alias(bundle_path, alias_root_path)
                        _cleanup_per_field_day_after_bundle(day_per_field_root, day, bundle_path, service_id)
                        continue
                except OSError:
                    pass

            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = quote_path_list(per_field_paths)

            # Truncate to top-K per field at bundle-write time, plus an
            # ``__other__`` synthetic row that aggregates everything
            # beyond the cut. The dashboard top-N panel renders 10
            # values; keeping top-K per (field, day) gives generous
            # headroom for the global top-10 across a 30-day window
            # while cutting bundle row count — most of the
            # ``top_n_rollups:rolled_res`` cost on prod 30d.
            #
            # __other__ keeps ``field_totals[field]`` correct (the
            # dashboard derives it via SUM across all rollup rows for
            # the field; without __other__ the dashboard's "total"
            # would undercount by ~90% for high-cardinality fields).
            # The reader filters ``value = '__other__'`` from the
            # displayed top-N rows but includes its count in the
            # field totals — see execute_top_n_rollups.
            #
            # Also preserve existing bundle rows for fields NOT in
            # ``updated_fields`` — because ``_cleanup_per_field_day_after_bundle``
            # sweeps per-field files after bundling, a partial per-field
            # update would otherwise drop every untouched field from the
            # bundle.
            def _build_day_bundle_query(src_union_sql: str = "") -> str:
                return (
                    f"COPY ("
                    f"  WITH src AS (SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"               FROM read_parquet([{paths_sql}]){src_union_sql}), "
                    f"       ranked AS (SELECT field, value, count, "
                    f"                  ROW_NUMBER() OVER (PARTITION BY field ORDER BY count DESC) AS rn "
                    f"                  FROM src) "
                    f"  SELECT field, value, count FROM ranked WHERE rn <= {DAY_BUNDLE_TOP_K} "
                    f"  UNION ALL "
                    f"  SELECT field, '__other__' AS value, SUM(count) AS count "
                    f"  FROM ranked WHERE rn > {DAY_BUNDLE_TOP_K} "
                    f"  GROUP BY field "
                    f"  HAVING SUM(count) > 0"
                    f") "
                    f"{parquet_zstd_target(tmp_path)}"
                )

            plain_query = _build_day_bundle_query()
            if os.path.isfile(bundle_path) and updated_fields:
                esc_bundle = escape_sql_literal(bundle_path)
                safe_in = ", ".join(f"'{escape_sql_literal(f)}'" for f in sorted(updated_fields))
                preserve_sql = (
                    f" UNION ALL SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet('{esc_bundle}', hive_partitioning=0) "
                    f"WHERE field NOT IN ({safe_in})"
                )
                query = _build_day_bundle_query(preserve_sql)
            else:
                query = plain_query
            try:
                con.execute(query)
            except duckdb.Error as e:
                if query != plain_query:
                    unlink_quiet(tmp_path)
                    try:
                        con.execute(plain_query)
                    except duckdb.Error as e2:
                        logger.warning("[rollups] %s: day-bundle COPY failed for day=%s: %s", service_id, day, e2)
                        unlink_quiet(tmp_path)
                        continue
                else:
                    logger.warning("[rollups] %s: day-bundle COPY failed for day=%s: %s", service_id, day, e)
                    unlink_quiet(tmp_path)
                    continue

            def _link_aliases(
                _bp: str = bundle_path,
                _ad: str = alias_dir_path,
                _ar: str = alias_root_path,
            ) -> None:
                _link_day_bundle_alias(_bp, _ad)
                _link_day_bundle_alias(_bp, _ar)

            publish_temp_parquet(
                tmp_path,
                bundle_path,
                lock_key,
                after_replace_under_lock=_link_aliases,
            )
            _cleanup_per_field_day_after_bundle(day_per_field_root, day, bundle_path, service_id)
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


def backfill_day_bundles(
    service_id: str,
    source: dict,
    max_days: int | None = None,
    lookback_days: int | None = None,
) -> int:
    """One-shot bulk bundling for all closed days that don't yet have a
    per-day bundled file (or whose bundle is older than its source per-
    field files).

    Walks ``rollups/day/field=*/day=*/`` to discover candidate days and
    calls :func:`bundle_days` on the subset that needs rebuilding.
    Idempotent — bundle_days skips up-to-date days via mtime comparison.
    """
    day_per_field_root = _day_rollups_root(source)
    bundled_root = _day_bundled_root(source)
    if not os.path.isdir(day_per_field_root):
        return 0

    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
    cutoff_day: str | None = None
    if lookback_days is not None:
        from datetime import timedelta

        cutoff_day = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    all_days: set[str] = set()
    try:
        for field_entry in os.listdir(day_per_field_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(day_per_field_root, field_entry)
            try:
                for day_entry in os.listdir(field_dir):
                    if not day_entry.startswith("day="):
                        continue
                    day = day_entry[len("day=") :]
                    if day >= active_day:
                        continue
                    if cutoff_day is not None and day < cutoff_day:
                        continue
                    all_days.add(day)
            except OSError:
                continue
    except OSError:
        return 0

    to_bundle: list[str] = []
    for day in sorted(all_days):
        if cutoff_day is not None and day < cutoff_day:
            continue
        bundle_path = os.path.join(bundled_root, f"day={day}", DAY_BUNDLE_FILENAME)
        if not os.path.exists(bundle_path):
            to_bundle.append(day)
        if max_days and len(to_bundle) >= max_days:
            break

    if not to_bundle:
        return 0
    return bundle_days(service_id, source, to_bundle)


def _cleanup_per_field_day_after_bundle(
    day_per_field_root: str,
    day: str,
    bundle_path: str,
    service_id: str,
) -> None:
    """Sweep per-field day parquet directories for ``day`` after
    the day bundle has been published.
    """
    cleanup_per_field_after_bundle(
        day_per_field_root,
        f"day={day}",
        bundle_path,
        service_id,
        file_kind_label="per-field day",
        dir_kind_label="per-field day",
        cleanup_label="cleanup",
        logger=logger,
    )


# ── Closed-day compaction (item 17 / RC-9) ──────────────────────────────────


def compact_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour rollup parquet into per-day parquet.

    For each (field, closed-day) tuple where either (a) no per-day parquet
    exists, or (b) some constituent per-hour parquet has a newer mtime
    than the per-day parquet, rebuild the per-day parquet by summing the
    24 hour parquets into one. Active (current UTC) day is always skipped
    — it's still being written.

    The per-day file is written via DuckDB COPY to a temp path and
    renamed into place under the per-service iceberg lock so concurrent
    `execute_top_n_rollups` readers never see a half-written file. On
    failure the per-day file is left in its previous state and the
    reader transparently falls back to per-hour parquet.

    Returns the count of (field, day) tuples that were rebuilt.

    Operators can call this from a maintenance script or wire it into a
    daily cron. The reader works whether or not this has ever run — when
    a per-day file is missing, `execute_top_n_rollups` reads the source
    per-hour files. When present, it reads ONE file per closed day per
    field instead of 24, slashing the file-open overhead that dominates
    dashboard cold-load wall time on 7-day queries (1,512 → 30-some
    files per the local audit).
    """
    import duckdb

    hour_root = _rollups_root(source)
    day_root = _day_rollups_root(source)
    # Either source-of-truth is enough: per-field hour tree (live, pre-
    # bundle) OR bundled-hour tree (post-bundle, after the per-field
    # sweep). A service with neither has nothing to compact yet.
    if not os.path.isdir(hour_root) and not os.path.isdir(_hour_bundled_root(source)):
        return 0

    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
    cutoff_day: str | None = None
    if lookback_days is not None:
        from datetime import timedelta

        cutoff_day = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    lock_key = source.get("name", "default")
    rebuilt = 0

    # In-memory DuckDB — we only need it to run COPY against parquet files
    # on the local filesystem. Opening the per-service ``.duckdb`` file
    # would contend with uvicorn's RW connection on the SAME file (held
    # for view rebuilds), since DuckDB does not allow mixed RW+RO from
    # one path. On the 2026-06-06 prod incident an RO ``get_connection``
    # blocked 5+ minutes on that lock and the compaction never produced
    # any per-day files. ``:memory:`` sidesteps the contention entirely
    # — the compaction reads + writes parquet via DuckDB's I/O layer,
    # never touching any persistent DuckDB database.
    # Pre-pass: enumerate every closed hour that ``hour_bundled`` covers,
    # bucketed by day. The bundler's ``_cleanup_per_field_after_bundle``
    # sweep deletes ``rollups/hour/field=*/hour=H/`` once an hour-bundle
    # is published, so the per-field per-hour tree the loop below walks
    # is the WRONG source-of-truth for closed bundled hours — the data
    # is in ``hour_bundled/hour=H/all_fields.parquet`` instead. Without
    # this fallback the compactor wrote per-field-per-day files holding
    # only the small sliver of hours that hadn't yet been bundled-and-
    # cleaned-up, and the reader's day_covered_by_any_field check then
    # treated the partial day as authoritative — silently dropping the
    # bundled hours for every field whose per-field-per-day was thinner
    # than the bundle. Surfaced 2026-06-15 as a 47k-row POST undercount
    # on the dashboard's method panel.
    bundled_hour_root_path = _hour_bundled_root(source)
    hour_bundled_by_day: dict[str, list[str]] = {}
    if os.path.isdir(bundled_hour_root_path):
        try:
            for hour_entry in os.listdir(bundled_hour_root_path):
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                if len(hour) < 13:
                    continue
                day = hour[:10]
                if day == active_day:
                    continue
                if cutoff_day is not None and day < cutoff_day:
                    continue
                bundle_path = os.path.join(bundled_hour_root_path, hour_entry, "all_fields.parquet")
                if os.path.isfile(bundle_path):
                    hour_bundled_by_day.setdefault(day, []).append(bundle_path)
        except OSError:
            pass

    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        # Field set spans both the per-field hour tree (live, not-yet-bundled
        # hours) AND any field that appears in a bundled-hour file (which is
        # every field with data on a closed-and-bundled day, even if its
        # per-field hour dir has since been cleaned up). Union the two so the
        # compactor doesn't miss fields whose entire window is bundled.
        field_set: set[str] = set()
        try:
            for entry in os.listdir(hour_root):
                if entry.startswith("field="):
                    f = entry[len("field=") :]
                    if _is_safe_ident(f):
                        field_set.add(f)
        except OSError:
            pass
        # Discover fields present in any bundled-hour file via a single
        # SELECT DISTINCT — cheap relative to the rest of the daily job.
        if hour_bundled_by_day:
            all_bundle_paths = sorted({p for paths in hour_bundled_by_day.values() for p in paths})
            try:
                paths_sql = quote_path_list(all_bundle_paths)
                for (f,) in con.execute(f"SELECT DISTINCT field FROM read_parquet([{paths_sql}])").fetchall():
                    if isinstance(f, str) and _is_safe_ident(f):
                        field_set.add(f)
            except duckdb.Error:
                pass

        for field in sorted(field_set):
            field_entry = f"field={field}"
            field_hour_dir = os.path.join(hour_root, field_entry)
            # Bucket hour-dirs by their YYYY-MM-DD prefix.
            by_day: dict[str, list[str]] = {}
            try:
                hour_entries = os.listdir(field_hour_dir) if os.path.isdir(field_hour_dir) else []
            except OSError:
                hour_entries = []
            for hour_entry in hour_entries:
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                # hour shape: YYYY-MM-DD-HH — first 10 chars are the day.
                if len(hour) < 13:
                    continue
                day = hour[:10]
                if day == active_day:
                    continue
                if cutoff_day is not None and day < cutoff_day:
                    continue
                hour_dir = os.path.join(field_hour_dir, hour_entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet"):
                            by_day.setdefault(day, []).append(os.path.join(hour_dir, fname))
                except OSError:
                    continue

            # Days touched by either per-field hour OR bundled-hour data.
            all_days = set(by_day.keys()) | set(hour_bundled_by_day.keys())
            for day in all_days:
                if cutoff_day is not None and day < cutoff_day:
                    continue
                hour_paths = by_day.get(day, [])
                bundled_paths = hour_bundled_by_day.get(day, [])
                if not hour_paths and not bundled_paths:
                    continue
                day_dir = os.path.join(day_root, field_entry, f"day={day}")
                day_file = os.path.join(day_dir, "compacted.parquet")
                all_source_paths = hour_paths + bundled_paths
                # Skip if the per-day file is newer than every source hour
                # parquet — already up to date.
                try:
                    day_mtime = os.path.getmtime(day_file)
                    max_hour_mtime = max(os.path.getmtime(p) for p in all_source_paths)
                    if day_mtime >= max_hour_mtime:
                        continue
                except OSError:
                    pass  # day file missing → rebuild

                tmp_file = os.path.join(day_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
                os.makedirs(day_dir, exist_ok=True)
                # Per-field hour parquets have ``field`` in the hive path
                # (hive_partitioning=1 supplies it); bundled-hour parquets
                # have ``field`` as a regular column (hive_partitioning=0).
                # SQL branches per source so the schemas match in UNION ALL.
                # The outer WHERE filters bundled-hour rows down to JUST
                # this field — the file contains every field's data.
                branches = []
                if hour_paths:
                    paths_sql = quote_path_list(hour_paths)
                    branches.append(
                        f"SELECT field, value, CAST(count AS BIGINT) AS count "
                        f"FROM read_parquet([{paths_sql}], hive_partitioning=1)"
                    )
                if bundled_paths:
                    paths_sql = quote_path_list(bundled_paths)
                    # Single-quote-escape the field name for the inline literal.
                    safe_field_sql = escape_sql_literal(field)
                    branches.append(
                        f"SELECT field, value, CAST(count AS BIGINT) AS count "
                        f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                        f"WHERE field = '{safe_field_sql}'"
                    )
                # CAST to BIGINT so the per-day file's count column matches
                # the per-hour files (which are BIGINT). The reader's
                # UNION ALL of day + hour requires matching column types
                # per column; without this CAST, the day file lands as
                # DOUBLE and the union breaks (and the dashboard top-N
                # tabs go blank — 2026-06-06 incident).
                copy_sql = (
                    f"COPY ("
                    f"SELECT field, value, CAST(SUM(count) AS BIGINT) AS count "
                    f"FROM ({' UNION ALL '.join(branches)}) "
                    f"GROUP BY field, value"
                    f") {parquet_zstd_target(tmp_file)}"
                )
                try:
                    con.execute(copy_sql)
                except duckdb.Error as e:
                    logger.warning(
                        "[rollups] %s: day-compact COPY failed for %s/%s: %s",
                        service_id,
                        field,
                        day,
                        e,
                    )
                    unlink_quiet(tmp_file)
                    continue

                try:
                    publish_temp_parquet(tmp_file, day_file, lock_key)
                    rebuilt += 1
                except OSError as e:
                    logger.warning("[rollups] %s: rename to %s failed: %s", service_id, day_file, e)
    finally:
        con.close()

    return rebuilt


# ── origin_summary closed-day compaction ────────────────────────────────────


def compact_origin_summary_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour origin_summary parquets into per-day files.

    For each closed UTC day where:
      - All present hours under ``hour_bundled/hour=YYYY-MM-DD-HH/origin_summary.parquet``
        belong to a day that is strictly before today (the active day is
        still being written and would change underneath us).
      - The per-day file is missing OR any constituent hour parquet has
        a newer mtime than the per-day file.

    ...read every hour file for the day, weighted-average percentile
    columns + SUM count columns, write a single
    ``day_bundled/day=YYYY-MM-DD/origin_summary.parquet`` with the
    same schema as the hour file.

    Same atomic-tmp + rename + per-service iceberg lock as
    :func:`compact_closed_days_to_daily`. Same in-memory DuckDB to
    sidestep the per-service .duckdb RW lock contention surfaced in the
    2026-06-06 prod incident — origin_summary inputs are parquet on
    disk, no per-service catalog state needed.

    Mathematically equivalent to reading 24 per-hour rows directly,
    because the reader's request-weighted-average SQL is associative
    when weights are carried (``lat_us_count``, ``ottlb_count``,
    ``cdn_ovh_count``, ``ost_total_count``, ``obytes_count``). Counts
    are exact SUMs.

    The hour files are NOT deleted — the reader prefers day-bundled
    when present and silently falls back to per-hour otherwise, so
    leaving the hour files keeps mid-read deletions impossible and
    makes the compaction freely idempotent.

    Returns the number of (closed-day) origin_summary files rebuilt.
    """

    # Mirrors try_origin_summary_from_rollup's reader SQL exactly: SUMs for
    # counts (associative), request-weighted average for percentile columns
    # using the per-hour count as the weight. Reading the resulting day file
    # with the same weighted-average SQL across day rows is mathematically
    # identical to reading all 24 hour files directly.
    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST(SUM(total_misses) AS BIGINT) AS total_misses, "
            f"    CAST(SUM(total_passes) AS BIGINT) AS total_passes, "
            f"    CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
            f"    CAST({wavg('ottfb_p50_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p50_us, "
            f"    CAST({wavg('ottfb_p75_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p75_us, "
            f"    CAST({wavg('ottfb_p95_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p95_us, "
            f"    CAST({wavg('ottfb_p99_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p99_us, "
            f"    CAST(SUM(ottlb_count) AS BIGINT) AS ottlb_count, "
            f"    CAST({wavg('ottlb_p50_us', 'ottlb_count')} AS DOUBLE) AS ottlb_p50_us, "
            f"    CAST({wavg('ottlb_p95_us', 'ottlb_count')} AS DOUBLE) AS ottlb_p95_us, "
            f"    CAST(SUM(cdn_ovh_count) AS BIGINT) AS cdn_ovh_count, "
            f"    CAST({wavg('cdn_ovh_p50_us', 'cdn_ovh_count')} AS DOUBLE) AS cdn_ovh_p50_us, "
            f"    CAST(SUM(ost_5xx_count) AS BIGINT) AS ost_5xx_count, "
            f"    CAST(SUM(ost_total_count) AS BIGINT) AS ost_total_count, "
            f"    CAST(SUM(obytes_count) AS BIGINT) AS obytes_count, "
            f"    CAST({wavg('obytes_p50', 'obytes_count')} AS DOUBLE) AS obytes_p50 "
            f"  FROM read_parquet([{paths_sql}])"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(ORIGIN_SUMMARY_BUNDLE_FILENAME, ".tmp_os_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── network_rtt closed-day compaction ───────────────────────────────────


def compact_network_rtt_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour network_rtt parquets into per-day
    files at ``day_bundled/day=YYYY-MM-DD/network_rtt.parquet``.

    Mirrors :func:`compact_origin_summary_closed_days_to_daily` exactly,
    only swapping the filename + aggregation columns. Per-day row keeps
    the same schema as per-hour (asn, requests, rtt_count, p95_us,
    p99_us) so the reader treats both interchangeably — the math is
    associative because percentiles are request-weighted-averaged via
    the rtt_count column carried alongside.

    Top-K cap per day is the SAME as per hour (100). The day file is
    the GROUP BY asn of all per-hour rows; no extra ranking step
    needed because every ASN that hit top-100 in any hour is already
    in scope. Returns the number of (closed-day) files rebuilt.
    """

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT asn, "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST(SUM(rtt_count) AS BIGINT) AS rtt_count, "
            f"    CAST({wavg('p95_us', 'rtt_count')} AS DOUBLE) AS p95_us, "
            f"    CAST({wavg('p99_us', 'rtt_count')} AS DOUBLE) AS p99_us "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY asn"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(NETWORK_RTT_BUNDLE_FILENAME, ".tmp_nr_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── network_speed closed-day compaction ─────────────────────────────────


def compact_network_speed_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour network_speed parquets into
    per-day files at ``day_bundled/day=YYYY-MM-DD/network_speed.parquet``.

    Same shape as :func:`compact_network_rtt_closed_days_to_daily` but
    the aggregation is a pure GROUP BY (asn, c_speed) + SUM(count) —
    no weighted-average step because the math is exact for integer
    counts.
    """

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT asn, c_speed, CAST(SUM(count) AS BIGINT) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY asn, c_speed"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(NETWORK_SPEED_BUNDLE_FILENAME, ".tmp_ns_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


def compact_ngwaf_bots_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour ngwaf_bots parquets into per-day
    files at ``day_bundled/day=YYYY-MM-DD/ngwaf_bots.parquet``.

    Same shape as :func:`compact_network_speed_closed_days_to_daily`:
    pure GROUP BY (bot_name, wellknown_bot_name, category) + SUM(count) —
    exact for integer counts, no re-cap needed (bot cardinality is tens per day).
    Purges pre-wellknown_bot_name 3-column hour/day files first (Trap #24).
    """
    from .ngwaf_bots import _purge_stale_ngwaf_bots_parquets

    _purge_stale_ngwaf_bots_parquets(source)

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bot_name, wellknown_bot_name, category, "
            f"         CAST(SUM(count) AS BIGINT) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bot_name, wellknown_bot_name, category"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(NGWAF_BOTS_BUNDLE_FILENAME, ".tmp_nb_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


def compact_ngwaf_bots_ts_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour ngwaf_bots_ts parquets into per-day
    files at ``day_bundled/day=YYYY-MM-DD/ngwaf_bots_ts.parquet``.

    Preserves minute granularity (``bucket_ts``) so the reader can re-bucket
    across any window — same shape as :func:`compact_verified_bots_ts_closed_days_to_daily`.
    """

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bucket_ts, bot_name, CAST(SUM(count) AS BIGINT) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket_ts, bot_name"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(NGWAF_BOTS_TS_BUNDLE_FILENAME, ".tmp_nbts_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── verified_bots_ts closed-day compaction ──────────────────────────────


def compact_verified_bots_ts_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour verified_bots_ts parquets into
    per-day files at ``day_bundled/day=YYYY-MM-DD/verified_bots_ts.parquet``.

    Unlike :func:`compact_network_speed_closed_days_to_daily` (which
    collapses the time dimension for a leaderboard panel) this PRESERVES
    minute granularity — verified_bots_ts is a time series, so the reader
    must still be able to re-bucket across the window. The aggregation is
    ``GROUP BY (bucket_ts, bot_type) SUM(count)``, a no-op merge (each
    minute lives in exactly one source hour file) that's harmless and
    idempotent.

    Same ``:memory:`` DuckDB + mtime-gated + active-day-skip posture as
    the other rollup day-compactors (per the 2026-06-06 incident lesson).
    """

    # Preserve the minute (bucket_ts) dimension — this is a time series, not
    # a leaderboard. GROUP BY (bucket_ts, bot_type) is a no-op merge (a minute
    # lives in exactly one source hour file).
    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bucket_ts, bot_type, CAST(SUM(count) AS BIGINT) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket_ts, bot_type"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(VERIFIED_BOTS_TS_BUNDLE_FILENAME, ".tmp_vbts_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── origin_latency_ts closed-day compaction ─────────────────────────────


def compact_origin_latency_ts_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour origin_latency_ts parquets into per-day
    files at ``day_bundled/day=YYYY-MM-DD/origin_latency_ts.parquet``.

    Like :func:`compact_verified_bots_ts_closed_days_to_daily` this PRESERVES
    the minute (``bucket_ts``) dimension — origin_latency_ts is a time series,
    so the reader must still be able to re-bucket across the window. Minutes
    are DISJOINT across the 24 source hour files (each minute lives in exactly
    one hour), so the day file is the union of the 24 hour files: do NOT
    collapse to one row and do NOT apply any top-K cap.

    The ``GROUP BY bucket_ts`` is therefore a no-op merge — ``SUM(count)`` and
    the request-weighted percentile (``SUM(p*_us * count)/SUM(count)``) both
    reduce to identity for the single source row per bucket, but the
    request-weighted form is written for uniformity with the reader's
    cross-file weighting (and harmlessly composes if two hour files ever share
    a minute).

    Same ``:memory:`` DuckDB + mtime-gated + active-day-skip posture as the
    other rollup day-compactors. Returns the number of (closed-day) files
    rebuilt.
    """

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bucket_ts, "
            f"    CAST(SUM(ttfb_count) AS BIGINT) AS ttfb_count, "
            f"    {wavg('ttfb_p50_us', 'ttfb_count')} AS ttfb_p50_us, "
            f"    {wavg('ttfb_p95_us', 'ttfb_count')} AS ttfb_p95_us, "
            f"    {wavg('ttfb_p99_us', 'ttfb_count')} AS ttfb_p99_us, "
            f"    CAST(SUM(ttlb_count) AS BIGINT) AS ttlb_count, "
            f"    {wavg('ttlb_p50_us', 'ttlb_count')} AS ttlb_p50_us, "
            f"    {wavg('ttlb_p95_us', 'ttlb_count')} AS ttlb_p95_us, "
            f"    {wavg('ttlb_p99_us', 'ttlb_count')} AS ttlb_p99_us "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket_ts"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(ORIGIN_LATENCY_TS_BUNDLE_FILENAME, ".tmp_olts_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── perf_latency closed-day compaction ──────────────────────────────────


def compact_perf_latency_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour perf_top_urls / perf_top_asns parquets
    into per-day files at ``day_bundled/day=YYYY-MM-DD/<name>.parquet``.

    Mirrors :func:`compact_network_rtt_closed_days_to_daily` (request-weighted
    percentile merge) for BOTH perf dimensions, but re-caps the day file to the
    top-K by p99 because URL cardinality is high (the union of 24 hours'
    top-100 can be large). The weighted-avg composes exactly with the reader's
    cross-file weighting. Returns the number of (closed-day) files rebuilt.
    """

    # Identical aggregation for both dimensions — each per-hour row keys on a
    # ``value`` column (url or asn); the request-weighted-average composes with
    # the reader's cross-file weighting, then re-cap to top-K by p99.
    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT value, requests, elapsed_count, elapsed_sum, p50_us, p95_us, p99_us FROM ("
            f"    SELECT value, "
            f"      CAST(SUM(requests) AS BIGINT) AS requests, "
            f"      CAST(SUM(elapsed_count) AS BIGINT) AS elapsed_count, "
            f"      CAST(SUM(elapsed_sum) AS DOUBLE) AS elapsed_sum, "
            f"      CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
            f"      CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us, "
            f"      CAST({wavg('p99_us', 'requests')} AS DOUBLE) AS p99_us, "
            f"      ROW_NUMBER() OVER ("
            f"        ORDER BY {wavg('p99_us', 'requests')} DESC"
            f"      ) AS rn "
            f"    FROM read_parquet([{paths_sql}]) "
            f"    GROUP BY value"
            f"  ) WHERE rn <= {PERF_LATENCY_BUNDLE_TOP_K}"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[
            (PERF_TOP_URLS_BUNDLE_FILENAME, ".tmp_pl_", _copy_sql),
            (PERF_TOP_ASNS_BUNDLE_FILENAME, ".tmp_pl_", _copy_sql),
        ],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── origin_dims (pop / oip / edge) closed-day compaction ────────────────────


def compact_origin_dims_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour origin_pop / origin_ip / origin_path
    parquets into per-day files at ``day_bundled/day=YYYY-MM-DD/<name>.parquet``.

    Mirrors :func:`compact_perf_latency_closed_days_to_daily` (request-weighted
    percentile merge) for all three origin-dimension bundles. The per-day file
    request-weights the percentiles (``SUM(p*_us * requests) / SUM(requests)``)
    and SUMs the counts; pop/oip re-cap the top-K at day grain (the union of 24
    hours' top-100 can exceed 100 distinct keys), and oip additionally SUMs the
    exact ``ost_5xx_count`` + ``ost_total_count`` so the reader's error_pct
    stays exact. edge has no top-K (only 2 keys). The weighted-avg composes
    exactly with the reader's cross-file weighting.

    Same ``:memory:`` DuckDB + mtime-gated + active-day-skip posture as the
    other rollup day-compactors. Returns the number of (closed-day) files
    rebuilt across all three jobs.
    """

    # pop: per-day GROUP BY pop, request-weighted percentiles, re-cap top-K by
    # requests (pop cardinality is small but the cap keeps the shape uniform).
    def _pop_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT pop, requests, lat_us_count, lat_us_sum, p50_us, p95_us FROM ("
            f"    SELECT pop, "
            f"      CAST(SUM(requests) AS BIGINT) AS requests, "
            f"      CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
            f"      CAST(SUM(lat_us_sum) AS DOUBLE) AS lat_us_sum, "
            f"      CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
            f"      CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us, "
            f"      ROW_NUMBER() OVER (ORDER BY SUM(requests) DESC) AS rn "
            f"    FROM read_parquet([{paths_sql}]) "
            f"    GROUP BY pop"
            f"  ) WHERE rn <= {ORIGIN_DIMS_BUNDLE_TOP_K}"
            f") {parquet_zstd_target(tmp_file)}"
        )

    # oip: per-day GROUP BY oip, request-weighted percentiles, SUM the exact
    # 5xx/total counts, re-cap top-K by requests.
    def _ip_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT oip, requests, lat_us_count, lat_us_sum, p50_us, p95_us, "
            f"         ost_5xx_count, ost_total_count FROM ("
            f"    SELECT oip, "
            f"      CAST(SUM(requests) AS BIGINT) AS requests, "
            f"      CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
            f"      CAST(SUM(lat_us_sum) AS DOUBLE) AS lat_us_sum, "
            f"      CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
            f"      CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us, "
            f"      CAST(SUM(ost_5xx_count) AS BIGINT) AS ost_5xx_count, "
            f"      CAST(SUM(ost_total_count) AS BIGINT) AS ost_total_count, "
            f"      ROW_NUMBER() OVER (ORDER BY SUM(requests) DESC) AS rn "
            f"    FROM read_parquet([{paths_sql}]) "
            f"    GROUP BY oip"
            f"  ) WHERE rn <= {ORIGIN_DIMS_BUNDLE_TOP_K}"
            f") {parquet_zstd_target(tmp_file)}"
        )

    # edge: per-day GROUP BY edge, request-weighted percentiles. NO top-K (2
    # rows per day at most).
    def _path_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT edge, "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
            f"    CAST(SUM(lat_us_sum) AS DOUBLE) AS lat_us_sum, "
            f"    CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
            f"    CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY edge"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[
            (ORIGIN_POP_BUNDLE_FILENAME, ".tmp_od_", _pop_copy_sql),
            (ORIGIN_IP_BUNDLE_FILENAME, ".tmp_od_", _ip_copy_sql),
            (ORIGIN_PATH_BUNDLE_FILENAME, ".tmp_od_", _path_copy_sql),
        ],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── security_dims (req_size / conn_reuse / topips / cov) closed-day compaction ─


def compact_security_dims_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour security_req_size / security_conn_reuse /
    security_topips / security_cov parquets into per-day files at
    ``day_bundled/day=YYYY-MM-DD/<name>.parquet``.

    All four merges are EXACT (no request-weighted percentile step):

      - req_size / conn_reuse: ``GROUP BY bucket`` with ``SUM(count)`` and
        ``MIN(min_val)`` (min-of-mins keeps the cross-hour bucket ordering the
        reader's ``ORDER BY MIN(min_val)`` relies on).
      - topips: ``GROUP BY ip`` with ``MAX(max_header)`` (MAX-of-MAX — NOT SUM;
        the origin precedent in ``_pop_copy_sql`` orders by ``SUM(requests)``,
        which is the WRONG merge for a per-ip MAX leaderboard), re-capped to the
        same top-500 per-day so the cross-day reader still re-ranks correctly.
      - cov: collapse to ONE row per day with ``SUM(total_rows)`` +
        ``SUM(tls_populated)``.

    Same ``:memory:`` DuckDB + mtime-gated + active-day-skip posture as the
    other rollup day-compactors (per the 2026-06-06 incident lesson — parquet
    inputs only, no per-service catalog state). Returns the number of
    (closed-day) files rebuilt across all four jobs.
    """

    # req_size / conn_reuse: GROUP BY bucket, SUM(count), MIN(min_val).
    def _bucket_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bucket, "
            f"    CAST(SUM(count) AS BIGINT) AS count, "
            f"    CAST(MIN(min_val) AS BIGINT) AS min_val "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket"
            f") {parquet_zstd_target(tmp_file)}"
        )

    # topips: GROUP BY ip, MAX(max_header) (MAX-of-MAX), re-cap top-K by max.
    def _topips_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT ip, max_header FROM ("
            f"    SELECT ip, CAST(MAX(max_header) AS BIGINT) AS max_header "
            f"    FROM read_parquet([{paths_sql}]) "
            f"    GROUP BY ip "
            f"    ORDER BY max_header DESC "
            f"    LIMIT {SECURITY_TOPIPS_BUNDLE_TOP_K}"
            f"  )"
            f") {parquet_zstd_target(tmp_file)}"
        )

    # cov: collapse to one row per day — SUM both counts.
    def _cov_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT "
            f"    CAST(SUM(total_rows) AS BIGINT) AS total_rows, "
            f"    CAST(SUM(tls_populated) AS BIGINT) AS tls_populated "
            f"  FROM read_parquet([{paths_sql}])"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[
            (SECURITY_REQ_SIZE_BUNDLE_FILENAME, ".tmp_sd_", _bucket_copy_sql),
            (SECURITY_CONN_REUSE_BUNDLE_FILENAME, ".tmp_sd_", _bucket_copy_sql),
            (SECURITY_TOPIPS_BUNDLE_FILENAME, ".tmp_sd_", _topips_copy_sql),
            (SECURITY_COV_BUNDLE_FILENAME, ".tmp_sd_", _cov_copy_sql),
        ],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── security_proxies (proxy_quality / proxy_flagged / bot_eff) closed-day compaction ─


def compact_security_proxies_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour security_proxy_quality /
    security_proxy_flagged / security_bot_eff parquets into per-day files at
    ``day_bundled/day=YYYY-MM-DD/<name>.parquet``.

      - proxy_quality: ``GROUP BY type`` with ``SUM(count)`` (exact).
      - proxy_flagged: ``GROUP BY`` the client tuple with ``SUM(cnt)`` (exact; the
        reader counts distinct IPs over these rows, so the tuple must survive).
      - bot_eff: ``GROUP BY kind, key`` with ``SUM(cnt)`` / ``SUM(resp_bytes)``;
        the per-UA rows are re-capped to the same top-K by bytes. The ``ref``
        count stays a SUM of per-hour distinct IPs (approximate, see
        :mod:`.security_proxies`).

    Same ``:memory:`` DuckDB + mtime-gated + active-day-skip posture as the other
    rollup day-compactors. Returns the number of day files rebuilt.
    """

    def _quality_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT type, CAST(SUM(count) AS BIGINT) AS count "
            f"  FROM read_parquet([{paths_sql}]) GROUP BY type"
            f") {parquet_zstd_target(tmp_file)}"
        )

    def _flagged_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city, CAST(SUM(cnt) AS BIGINT) AS cnt "
            f"  FROM read_parquet([{paths_sql}]) GROUP BY ALL"
            f") {parquet_zstd_target(tmp_file)}"
        )

    def _bot_eff_copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  WITH merged AS ("
            f"    SELECT kind, key, CAST(SUM(cnt) AS BIGINT) AS cnt, CAST(SUM(resp_bytes) AS BIGINT) AS resp_bytes "
            f"    FROM read_parquet([{paths_sql}]) GROUP BY kind, key"
            f"  ), "
            f"  ranked AS ("
            f"    SELECT *, ROW_NUMBER() OVER (PARTITION BY kind ORDER BY resp_bytes DESC, cnt DESC) AS rn FROM merged"
            f"  ) "
            f"  SELECT kind, key, cnt, resp_bytes FROM ranked "
            f"  WHERE kind != 'ua' OR rn <= {SECURITY_BOT_EFF_UA_TOP_K}"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[
            (SECURITY_PROXY_QUALITY_BUNDLE_FILENAME, ".tmp_sp_", _quality_copy_sql),
            (SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME, ".tmp_sp_", _flagged_copy_sql),
            (SECURITY_BOT_EFF_BUNDLE_FILENAME, ".tmp_sp_", _bot_eff_copy_sql),
        ],
        lookback_days=lookback_days,
        logger=logger,
    )


# ── perf_dims (ttl_dist) closed-day compaction ───────────────────────────────


def compact_perf_dims_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour perf_ttl_dist parquets into per-day files
    at ``day_bundled/day=YYYY-MM-DD/perf_ttl_dist.parquet``.

    The merge is EXACT (no request-weighted percentile step): ``GROUP BY bucket``
    with ``SUM(count)`` and ``MIN(min_ttl)`` (min-of-mins keeps the cross-hour
    bucket ordering the reader's ``ORDER BY MIN(min_ttl)`` relies on) — the same
    posture as security_dims' req_size/conn_reuse merge.

    Same ``:memory:`` DuckDB + mtime-gated + active-day-skip posture as the other
    rollup day-compactors (per the 2026-06-06 incident lesson — parquet inputs
    only, no per-service catalog state). Returns the number of (closed-day) files
    rebuilt.
    """

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bucket, "
            f"    CAST(SUM(count) AS BIGINT) AS count, "
            f"    CAST(MIN(min_ttl) AS BIGINT) AS min_ttl "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(PERF_TTL_DIST_BUNDLE_FILENAME, ".tmp_pd_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


def compact_overview_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour overview parquets into per-day files
    at ``day_bundled/day=YYYY-MM-DD/overview.parquet``.

    Preserves ``hour_start`` granularity (24 rows per day file) so the
    reader can still re-bucket to sub-day chart intervals. All metric
    columns are SUM-aggregatable — the ``GROUP BY hour_start`` is a no-op
    merge (each hour lives in exactly one source file).
    """

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT hour_start, "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST(SUM(hit_requests) AS BIGINT) AS hit_requests, "
            f"    CAST(SUM(miss_requests) AS BIGINT) AS miss_requests, "
            f"    CAST(SUM(pass_requests) AS BIGINT) AS pass_requests, "
            f"    CAST(SUM(synth_requests) AS BIGINT) AS synth_requests, "
            f"    CAST(SUM(origin_requests) AS BIGINT) AS origin_requests, "
            f"    CAST(SUM(bandwidth_saved_bytes) AS BIGINT) AS bandwidth_saved_bytes, "
            f"    CAST(SUM(total_bandwidth_bytes) AS BIGINT) AS total_bandwidth_bytes, "
            f"    CAST(SUM(shield_hit_requests) AS BIGINT) AS shield_hit_requests, "
            f"    CAST(SUM(shield_total_requests) AS BIGINT) AS shield_total_requests, "
            f"    CAST(SUM(threats_blocked) AS BIGINT) AS threats_blocked, "
            f"    CAST(SUM(hit_elapsed_sum) AS DOUBLE) AS hit_elapsed_sum, "
            f"    CAST(SUM(hit_elapsed_count) AS BIGINT) AS hit_elapsed_count, "
            f"    CAST(SUM(miss_elapsed_sum) AS DOUBLE) AS miss_elapsed_sum, "
            f"    CAST(SUM(miss_elapsed_count) AS BIGINT) AS miss_elapsed_count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY hour_start"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(OVERVIEW_BUNDLE_FILENAME, ".tmp_ov_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


def compact_time_series_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour time_series parquets into per-day files."""

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT "
            f"    bucket, "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST(SUM(status_4xx) AS BIGINT) AS status_4xx, "
            f"    CAST(SUM(status_5xx) AS BIGINT) AS status_5xx, "
            f"    CAST(SUM(hits) AS BIGINT) AS hits, "
            f"    CAST(SUM(cache_total) AS BIGINT) AS cache_total, "
            f"    CAST(SUM(resp_bytes_sum) AS BIGINT) AS resp_bytes_sum, "
            f"    CAST(SUM(ttfb_sum) AS DOUBLE) AS ttfb_sum, "
            f"    CAST(SUM(ttfb_count) AS BIGINT) AS ttfb_count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket "
            f"  ORDER BY bucket"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(TIME_SERIES_BUNDLE_FILENAME, ".tmp_ts_day_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


def compact_pop_health_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    """Consolidate closed-day per-hour pop_health parquets into per-day files."""

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT "
            f"      CAST(pop AS VARCHAR) AS pop, "
            f"      CAST(SUM(requests) AS BIGINT) AS requests, "
            f"      CAST(SUM(errors) AS BIGINT) AS errors, "
            f"      CAST(SUM(cache_hits) AS BIGINT) AS cache_hits, "
            f"      CAST(SUM(bandwidth_bytes) AS BIGINT) AS bandwidth_bytes, "
            f"      CAST({wavg('p50_rtt_us', 'requests')} AS DOUBLE) AS p50_rtt_us, "
            f"      CAST({wavg('p95_ttfb_ms', 'requests')} AS DOUBLE) AS p95_ttfb_ms "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY pop"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[(POP_HEALTH_BUNDLE_FILENAME, ".tmp_ph_", _copy_sql)],
        lookback_days=lookback_days,
        logger=logger,
    )


import typing


def compact_network_quality_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    from ._common import (
        NETWORK_QUALITY_ASN_FILENAME,
        NETWORK_QUALITY_COUNTRY_FILENAME,
        NETWORK_QUALITY_POP_FILENAME,
        NETWORK_QUALITY_REGION_FILENAME,
    )

    def _build_sql(dim_col: str, include_country: bool) -> typing.Callable[[str, str], str]:
        country_select = ", country" if include_country else ""
        return lambda paths_sql, tmp_file: (
            f"COPY ("
            f"  SELECT dim_val {country_select}, "
            f"    CAST(SUM(requests) AS BIGINT) AS requests, "
            f"    CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY dim_val {country_select} "
            f"  ORDER BY requests DESC LIMIT 100"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return compact_closed_days(
        service_id,
        source,
        jobs=[
            (NETWORK_QUALITY_COUNTRY_FILENAME, ".tmp_nq_", _build_sql("country", False)),
            (NETWORK_QUALITY_ASN_FILENAME, ".tmp_nq_", _build_sql("asn", False)),
            (NETWORK_QUALITY_REGION_FILENAME, ".tmp_nq_", _build_sql("region", True)),
            (NETWORK_QUALITY_POP_FILENAME, ".tmp_nq_", _build_sql("pop", False)),
        ],
        lookback_days=lookback_days,
        logger=logger,
    )


def retire_compacted_hour_bundles(
    service_id: str,
    source: dict,
    lookback_days: int | None = 30,
) -> tuple[int, int]:
    """Safely unlink the 24 hourly bundle files once the day bundle is verified.

    For closed calendar days strictly before today and within lookback_days,
    checks if a valid day bundle exists and passes read validation (non-zero row count).
    If valid, safely unlinks constituent hour bundle files (all_fields.parquet)
    under hour_bundled/hour=YYYY-MM-DD-HH/ and cleans up empty hour dirs.

    Returns:
        tuple[int, int]: (retired_files_count, retired_bytes_count)
    """
    from backend.core.duckdb import get_memory_connection
    from backend.core.iceberg.view import _get_service_lock

    from ._common import (
        DAY_BUNDLE_FILENAME,
        IP_SPREAD_BUNDLE_FILENAME,
        _day_bundled_root,
        _hour_bundled_root,
    )

    bundled_root = _day_bundled_root(source)
    hour_bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root) or not os.path.isdir(hour_bundled_root):
        return 0, 0

    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
    cutoff_day: str | None = None
    if lookback_days is not None:
        from datetime import timedelta

        cutoff_day = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    retired_files = 0
    retired_bytes = 0
    lock_key = source.get("name", "default")
    dry_run = os.environ.get("ROLLUP_CLEANUP_DRY_RUN") == "1"

    candidate_days: set[str] = set()
    try:
        for entry in os.listdir(bundled_root):
            if entry.startswith("day="):
                d = entry[len("day=") :]
                if d < active_day and (cutoff_day is None or d >= cutoff_day):
                    candidate_days.add(d)
            elif entry.startswith("day_bundle_") and entry.endswith(".parquet"):
                d = entry[len("day_bundle_") : -len(".parquet")]
                if len(d) == 10 and d < active_day and (cutoff_day is None or d >= cutoff_day):
                    candidate_days.add(d)
    except OSError:
        return 0, 0

    con = get_memory_connection()
    try:
        for day in sorted(candidate_days):
            bundle_path = os.path.join(bundled_root, f"day={day}", DAY_BUNDLE_FILENAME)
            alt_bundle_path = os.path.join(bundled_root, f"day={day}", f"day_bundle_{day}.parquet")
            flat_bundle_path = os.path.join(bundled_root, f"day_bundle_{day}.parquet")
            valid_path = (
                bundle_path
                if os.path.isfile(bundle_path)
                else (
                    alt_bundle_path
                    if os.path.isfile(alt_bundle_path)
                    else (flat_bundle_path if os.path.isfile(flat_bundle_path) else None)
                )
            )
            if not valid_path:
                continue

            # Read validation: verify day bundle can be read and has rows
            try:
                res = con.execute(f"SELECT count(*) FROM read_parquet('{valid_path}')").fetchone()
                row_cnt = res[0] if res else 0
                if not row_cnt or row_cnt <= 0:
                    continue
            except Exception as e:
                logger.warning(
                    "[rollups] %s: day bundle read validation failed for %s: %s",
                    service_id,
                    valid_path,
                    e,
                )
                continue

            # Candidate constituent hours for this day
            day_ip_spread_exists = os.path.isfile(os.path.join(bundled_root, f"day={day}", IP_SPREAD_BUNDLE_FILENAME))
            target_filenames = (
                (DAY_BUNDLE_FILENAME, IP_SPREAD_BUNDLE_FILENAME) if day_ip_spread_exists else (DAY_BUNDLE_FILENAME,)
            )
            for hour_idx in range(24):
                hour_str = f"{day}-{hour_idx:02d}"
                hour_dir = os.path.join(hour_bundled_root, f"hour={hour_str}")
                if not os.path.isdir(hour_dir):
                    continue
                for target_filename in target_filenames:
                    target_file = os.path.join(hour_dir, target_filename)
                    if os.path.isfile(target_file):
                        try:
                            fsize = os.path.getsize(target_file)
                            if not dry_run:
                                with _get_service_lock(lock_key):
                                    os.remove(target_file)
                            retired_files += 1
                            retired_bytes += fsize
                        except OSError as e:
                            logger.warning(
                                "[rollups] %s: failed to unlink %s: %s",
                                service_id,
                                target_file,
                                e,
                            )

                if not dry_run and os.path.isdir(hour_dir):
                    try:
                        os.rmdir(hour_dir)
                    except OSError:
                        pass
    finally:
        con.close()

    return retired_files, retired_bytes
