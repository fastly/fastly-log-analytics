"""Per-day bundling: combine per-field day parquets into
``rollups/day_bundled/day=D/all_fields.parquet``, plus the closed-day
compactor that builds the per-field day parquets in the first place.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime

from ._common import (
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    _day_bundled_root,
    _day_rollups_root,
    _is_safe_ident,
    _rollups_root,
)

logger = logging.getLogger(__name__)


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

    from backend.core.iceberg.view import _get_service_lock

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
    con = duckdb.connect(":memory:")
    try:
        for day in days:
            if day == active_day:
                continue
            # Defensive: validate day token format.
            try:
                datetime.strptime(day, "%Y-%m-%d")
            except ValueError:
                continue

            per_field_paths: list[str] = []
            max_src_mtime = 0.0
            try:
                for field_entry in os.listdir(day_per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    day_dir = os.path.join(day_per_field_root, field_entry, f"day={day}")
                    if not os.path.isdir(day_dir):
                        continue
                    for fname in os.listdir(day_dir):
                        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                            continue
                        p = os.path.join(day_dir, fname)
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

            bundle_dir = os.path.join(bundled_root, f"day={day}")
            bundle_path = os.path.join(bundle_dir, DAY_BUNDLE_FILENAME)
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        continue
                except OSError:
                    pass

            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in per_field_paths)
            # Truncate to top-K per field at bundle-write time, plus an
            # ``__other__`` synthetic row that aggregates everything
            # beyond the cut. The dashboard top-N panel renders 10
            # values; keeping top-100 per (field, day) gives generous
            # headroom for the global top-10 across a 30-day window
            # while cutting bundle row count by ~10x — most of the
            # ``top_n_rollups:rolled_res`` cost on prod 30d.
            #
            # __other__ keeps ``field_totals[field]`` correct (the
            # dashboard derives it via SUM across all rollup rows for
            # the field; without __other__ the dashboard's "total"
            # would undercount by ~90% for high-cardinality fields).
            # The reader filters ``value = '__other__'`` from the
            # displayed top-N rows but includes its count in the
            # field totals — see execute_top_n_rollups.
            query = (
                f"COPY ("
                f"  WITH src AS (SELECT field, value, CAST(count AS BIGINT) AS count "
                f"               FROM read_parquet([{paths_sql}])), "
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
                f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: day-bundle COPY failed for day=%s: %s", service_id, day, e)
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


def backfill_day_bundles(service_id: str, source: dict, max_days: int | None = None) -> int:
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
                    all_days.add(day)
            except OSError:
                continue
    except OSError:
        return 0

    to_bundle: list[str] = []
    for day in sorted(all_days):
        bundle_path = os.path.join(bundled_root, f"day={day}", DAY_BUNDLE_FILENAME)
        if not os.path.exists(bundle_path):
            to_bundle.append(day)
        if max_days and len(to_bundle) >= max_days:
            break

    if not to_bundle:
        return 0
    return bundle_days(service_id, source, to_bundle)


# ── Closed-day compaction (item 17 / RC-9) ──────────────────────────────────


def compact_closed_days_to_daily(service_id: str, source: dict) -> int:
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

    from backend.core.iceberg.view import _get_service_lock

    hour_root = _rollups_root(source)
    day_root = _day_rollups_root(source)
    if not os.path.isdir(hour_root):
        return 0

    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
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
    con = duckdb.connect(":memory:")
    try:
        for field_entry in sorted(os.listdir(hour_root)):
            if not field_entry.startswith("field="):
                continue
            field = field_entry[len("field=") :]
            if not _is_safe_ident(field):
                continue
            field_hour_dir = os.path.join(hour_root, field_entry)
            # Bucket hour-dirs by their YYYY-MM-DD prefix.
            by_day: dict[str, list[str]] = {}
            try:
                hour_entries = os.listdir(field_hour_dir)
            except OSError:
                continue
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
                hour_dir = os.path.join(field_hour_dir, hour_entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet"):
                            by_day.setdefault(day, []).append(os.path.join(hour_dir, fname))
                except OSError:
                    continue

            for day, hour_paths in by_day.items():
                if not hour_paths:
                    continue
                day_dir = os.path.join(day_root, field_entry, f"day={day}")
                day_file = os.path.join(day_dir, "compacted.parquet")
                # Skip if the per-day file is newer than every source hour
                # parquet — already up to date.
                try:
                    day_mtime = os.path.getmtime(day_file)
                    max_hour_mtime = max(os.path.getmtime(p) for p in hour_paths)
                    if day_mtime >= max_hour_mtime:
                        continue
                except OSError:
                    pass  # day file missing → rebuild

                tmp_file = os.path.join(day_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
                os.makedirs(day_dir, exist_ok=True)
                paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in hour_paths)
                # CAST to BIGINT so the per-day file's count column matches
                # the per-hour files (which are BIGINT). The reader's
                # UNION ALL of day + hour requires matching column types
                # per column; without this CAST, the day file lands as
                # DOUBLE and the union breaks (and the dashboard top-N
                # tabs go blank — 2026-06-06 incident).
                copy_sql = f"""
                    COPY (
                        SELECT field, value, CAST(SUM(count) AS BIGINT) AS count
                        FROM read_parquet([{paths_sql}], hive_partitioning=1)
                        GROUP BY field, value
                    ) TO '{tmp_file}'
                    (FORMAT PARQUET, COMPRESSION ZSTD)
                """
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
                    try:
                        os.remove(tmp_file)
                    except OSError:
                        pass
                    continue

                with _get_service_lock(lock_key):
                    try:
                        os.replace(tmp_file, day_file)
                        rebuilt += 1
                    except OSError as e:
                        logger.warning("[rollups] %s: rename to %s failed: %s", service_id, day_file, e)
                        try:
                            os.remove(tmp_file)
                        except OSError:
                            pass
    finally:
        con.close()

    return rebuilt
