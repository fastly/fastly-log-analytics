"""Shared per-hour and per-day bundle writer/compactor drivers and DuckDB session helpers."""

from __future__ import annotations

import contextlib
import contextvars
import logging
import os
import shutil
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from backend.utils.telemetry import parse_hour_key_strict

from ._common import (
    TIME_SERIES_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
    _is_safe_ident,
    _safe_table_for,
    parquet_zstd_target,
    parse_hour_token,
    quote_path_list,
    unlink_quiet,
)

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


def publish_temp_parquet(
    tmp_path: str,
    dst_path: str,
    lock_key: str,
    *,
    after_replace_under_lock: Callable[[], None] | None = None,
    before_replace_under_lock: Callable[[], bool] | None = None,
) -> bool:
    """Atomically replace ``tmp_path`` onto ``dst_path`` under the service view lock."""
    from backend.core.storage._core import _get_service_lock

    try:
        with _get_service_lock(lock_key):
            if before_replace_under_lock is not None and not before_replace_under_lock():
                unlink_quiet(tmp_path)
                return False
            os.replace(tmp_path, dst_path)
            if after_replace_under_lock is not None:
                after_replace_under_lock()
            return True
    except BaseException:
        unlink_quiet(tmp_path)
        raise


def _publish_field_partitions(
    tmp_field_dir: str,
    dst_root: str,
    field: str,
    *,
    hours: list[str] | None = None,
) -> int:
    """Move per-hour parquet files from a temp PARTITION_BY tree into the
    canonical ``rollups/hour/field=X/hour=Y/`` layout.

    The publish order is RENAME-then-UNLINK to close the race window where
    a concurrent dashboard read could observe an empty hour directory.
    Worst case after this change: a dashboard read briefly sees BOTH the
    new and old parquet for the same hour and double-counts that hour
    until the unlink lands — which is bounded and self-corrects on the
    next refresh. Pre-fix, the dashboard could observe ZERO files for the
    hour (undercount), which was indistinguishable from a real traffic dip.

    Caller MUST hold the per-service view lock around the whole call.
    Returns the number of hour-dirs published.
    """
    field_dir = os.path.join(tmp_field_dir, f"field={field}")
    if hours is None and not os.path.isdir(field_dir):
        return 0

    published = 0
    hour_dirnames = (
        [entry for entry in os.listdir(field_dir) if entry.startswith("hour=")]
        if hours is None
        else [f"hour={hour}" for hour in hours]
    )
    for hour_dirname in hour_dirnames:
        if not hour_dirname.startswith("hour="):
            continue
        src_hour_dir = os.path.join(field_dir, hour_dirname)
        dst_hour_dir = os.path.join(dst_root, f"field={field}", hour_dirname)
        if hours is None and not os.path.isdir(src_hour_dir):
            continue
        if hours is not None:
            os.makedirs(src_hour_dir, exist_ok=True)
        os.makedirs(dst_hour_dir, exist_ok=True)

        # 1. Rename new files into place first (overcounting window OK).
        new_names: set[str] = set()
        if os.path.isdir(src_hour_dir):
            for fname in os.listdir(src_hour_dir):
                if not fname.endswith(".parquet"):
                    continue
                new_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
                os.rename(os.path.join(src_hour_dir, fname), os.path.join(dst_hour_dir, new_name))
                new_names.add(new_name)
        if hours is not None and not new_names:
            import pyarrow as pa
            import pyarrow.parquet as pq

            marker = os.path.join(src_hour_dir, f"empty_{uuid.uuid4().hex[:12]}.parquet")
            pq.write_table(
                pa.table({"value": pa.array([], type=pa.string()), "count": pa.array([], type=pa.int64())}),
                marker,
            )
            new_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
            os.rename(marker, os.path.join(dst_hour_dir, new_name))
            new_names.add(new_name)

        # 2. Now unlink any pre-existing files that we didn't just write.
        existing_parquets = [existing for existing in os.listdir(dst_hour_dir) if existing.endswith(".parquet")]
        if new_names:
            for existing in existing_parquets:
                if existing.endswith(".parquet") and existing not in new_names:
                    try:
                        os.remove(os.path.join(dst_hour_dir, existing))
                    except OSError as e:
                        logger.warning("[rollups] could not unlink stale %s: %s", existing, e)
            published += 1

    return published


def backfill_missing_bundles(
    service_id: str,
    source: dict[str, Any],
    *,
    bundle_filename: str,
    label: str,
    builder: Callable[[str, dict[str, Any], list[str]], int],
    logger: logging.Logger = logger,
) -> int:
    """Self-heal driver shared by the per-feature hour-bundle backfillers.

    Walks every closed hour under the hour-bundled root that already has an
    ``all_fields.parquet`` (the signal the hour was touched by the recompute
    pipeline at least once) but is missing ``bundle_filename``, and hands
    those hours to ``builder`` to (re-)materialize. Skipping hours without
    the per-field rollup avoids writing bundles for ingest gaps.

    Idempotent — ``builder`` itself skips the active hour and atomic-renames
    its output. ``label`` is the feature name used in the info log; pass each
    caller's module ``logger`` so the log record keeps its origin module.
    Returns the number of bundles ``builder`` reports writing.
    """
    bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root):
        return 0

    hours_to_build: list[str] = []
    try:
        for entry in sorted(os.listdir(bundled_root)):
            if not entry.startswith("hour="):
                continue
            hour = entry[len("hour=") :]
            if parse_hour_token(hour) is None:
                continue
            if not (
                os.path.isfile(os.path.join(bundled_root, entry, "all_fields.parquet"))
                or os.path.isfile(os.path.join(bundled_root, entry, TIME_SERIES_BUNDLE_FILENAME))
            ):
                continue
            if os.path.isfile(os.path.join(bundled_root, entry, bundle_filename)) or os.path.isfile(
                os.path.join(bundled_root, entry, f"{bundle_filename}.done")
            ):
                continue
            hours_to_build.append(hour)
    except OSError:
        return 0

    if not hours_to_build:
        return 0

    logger.info(
        "[rollups] %s: backfilling %s for %d closed hour(s)",
        service_id,
        label,
        len(hours_to_build),
    )
    return builder(service_id, source, hours_to_build)


def _closed_day_window(lookback_days: int | None = None) -> tuple[str, str | None]:
    """Return ``(active_day, cutoff_day)`` in ``YYYY-MM-DD`` UTC format."""
    now = datetime.now(UTC)
    active_day = now.strftime("%Y-%m-%d")
    cutoff_day: str | None = None
    if lookback_days is not None:
        cutoff_day = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    return active_day, cutoff_day


def collect_per_field_partition_parquets(
    per_field_root: str,
    partition_subdir: str,
) -> tuple[list[str], set[str], float] | None:
    """Scan ``field=*`` under ``per_field_root`` for ``<partition_subdir>/*.parquet``.

    Returns ``(per_field_paths, updated_fields, max_src_mtime)`` or ``None`` if
    listing ``per_field_root`` raises ``OSError``.
    """
    per_field_paths: list[str] = []
    updated_fields: set[str] = set()
    max_src_mtime = 0.0
    try:
        for field_entry in os.listdir(per_field_root):
            if not field_entry.startswith("field="):
                continue
            field_name = field_entry[len("field=") :]
            if not _is_safe_ident(field_name):
                continue
            part_dir = os.path.join(per_field_root, field_entry, partition_subdir)
            if not os.path.isdir(part_dir):
                continue
            field_has_files = False
            for fname in os.listdir(part_dir):
                if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                    continue
                p = os.path.join(part_dir, fname)
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
        return None
    return per_field_paths, updated_fields, max_src_mtime


def _sum_count_copy_sql(group_cols: str) -> Callable[[str, str], str]:
    """Build a ``COPY (SELECT <group_cols>, CAST(SUM(count) AS BIGINT) AS count ...)`` callback."""

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT {group_cols}, CAST(SUM(count) AS BIGINT) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY {group_cols}"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return _copy_sql


def _bucket_min_copy_sql(min_col: str) -> Callable[[str, str], str]:
    """Build a ``COPY (SELECT bucket, SUM(count), MIN(<min_col>) ... GROUP BY bucket)`` callback."""

    def _copy_sql(paths_sql: str, tmp_file: str) -> str:
        return (
            f"COPY ("
            f"  SELECT bucket, "
            f"    CAST(SUM(count) AS BIGINT) AS count, "
            f"    CAST(MIN({min_col}) AS BIGINT) AS {min_col} "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  GROUP BY bucket"
            f") {parquet_zstd_target(tmp_file)}"
        )

    return _copy_sql


def _time_series_replacement_inputs(
    con: Any,
    hour_root: str,
    day_file: str,
    day: str,
    input_paths: list[str],
) -> dict[str, os.stat_result]:
    """Prove time-series inputs preserve the saved day's positive minute counts.

    The inputs are themselves the count references, not independently published
    conditional feature files. Require all 24 hours, including explicit empty
    Parquets; a newly empty input cannot erase a saved positive minute.
    """
    from backend.utils.telemetry import parse_day_key_strict

    from .rollup_coverage import BundleProbe, CoverageIndex, validated_daily_reference_minutes

    start = parse_day_key_strict(day)
    index = CoverageIndex.build(start, start + timedelta(days=1))
    if len(index.closed_hours) != 24:
        raise RuntimeError("day is not fully closed")
    probe = BundleProbe(hour_root)
    reference_paths = [probe.path(hour, TIME_SERIES_BUNDLE_FILENAME) for hour in index.closed_hours]
    if set(reference_paths) != set(input_paths):
        raise RuntimeError("hourly time-series input coverage incomplete")
    generations = {p: os.stat(p) for p in [day_file, *input_paths]}
    saved = validated_daily_reference_minutes(con, day_file, start)
    candidate: dict[datetime, int] = {}
    for hour, path in zip(index.closed_hours, reference_paths, strict=True):
        minutes = (
            {}
            if probe.is_empty(hour, TIME_SERIES_BUNDLE_FILENAME)
            else validated_daily_reference_minutes(con, path, start)
        )
        if any(bucket.strftime("%Y-%m-%d-%H") != hour for bucket in minutes):
            raise RuntimeError("hourly count reference contains another hour")
        candidate.update(minutes)
    if any(candidate.get(bucket, 0) < requests for bucket, requests in saved.items()):
        raise RuntimeError("hourly count references lose saved positive minutes")
    return generations


def compact_closed_days(
    service_id: str,
    source: dict[str, Any],
    *,
    jobs: list[tuple[str, str, Callable[[str, str], str]]],
    lookback_days: int | None = None,
    logger: logging.Logger = logger,
) -> int:
    """Consolidate closed-day per-hour bundle parquets into per-day files.

    Shared driver for the per-feature day compactors. ``jobs`` is a list of
    ``(bundle_filename, tmp_prefix, build_copy_sql)`` tuples. For each job the
    driver:

      - walks ``hour_bundled/hour=YYYY-MM-DD-HH/<bundle_filename>``, grouping
        files by their ``YYYY-MM-DD`` prefix and skipping the active day (still
        being written);
      - bounds historical days to ``lookback_days`` when specified (e.g. 30 days
        for nightly compaction);
      - for each closed day, skips when the per-day file is already newer than
        every constituent hour file (mtime gate);
      - preserves existing feature dailies: hourly feature files/empty markers
        have no persisted source-population proof linking them to count references;
      - replaces existing time-series dailies only when their actual hourly
        inputs reconcile with saved positive minutes;
      - otherwise writes ``build_copy_sql(paths_sql, tmp_file)`` to a temp file
        and atomically renames it into place under the per-service view
        lock, cleaning up the temp file on any failure.

    ``build_copy_sql`` receives the SQL-escaped ``read_parquet([...])`` source
    list and the temp target path and returns the full ``COPY (...) TO ...``
    statement — the per-feature aggregation (and any top-K re-cap) lives there,
    byte-for-byte as before. A single in-memory DuckDB connection is shared
    across all jobs and days (matches the 2026-06-06 incident posture: parquet
    inputs only, no per-service catalog state). Returns the total number of
    per-day files rebuilt across all jobs.
    """
    import duckdb

    hour_root = _hour_bundled_root(source)
    day_root = _day_bundled_root(source)
    if not os.path.isdir(hour_root):
        return 0

    active_day, cutoff_day = _closed_day_window(lookback_days)

    lock_key = str(source.get("name", "default"))

    rebuilt = 0
    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        for bundle_filename, tmp_prefix, build_copy_sql in jobs:
            hours_by_day: dict[str, list[str]] = {}
            try:
                for hour_entry in os.listdir(hour_root):
                    if not hour_entry.startswith("hour="):
                        continue
                    hour_tok = hour_entry[len("hour=") :]
                    if len(hour_tok) < 13:
                        continue
                    day = hour_tok[:10]
                    if day == active_day:
                        continue
                    if cutoff_day is not None and day < cutoff_day:
                        continue
                    p = os.path.join(hour_root, hour_entry, bundle_filename)
                    if os.path.isfile(p):
                        hours_by_day.setdefault(day, []).append(p)
            except OSError:
                continue

            for day in sorted(hours_by_day):
                if cutoff_day is not None and day < cutoff_day:
                    continue
                input_paths = sorted(hours_by_day[day])
                if not input_paths:
                    continue
                day_dir = os.path.join(day_root, f"day={day}")
                day_file = os.path.join(day_dir, bundle_filename)
                day_stat = None
                try:
                    day_stat = os.stat(day_file)
                    day_mtime = day_stat.st_mtime
                    max_hour_mtime = max(os.path.getmtime(p) for p in input_paths)
                    if day_mtime >= max_hour_mtime:
                        continue
                except FileNotFoundError:
                    pass  # day file missing → rebuild
                except OSError as e:
                    logger.warning(
                        "[rollups] %s: %s day=%s preserved: cannot inspect compaction inputs: %s",
                        service_id,
                        bundle_filename,
                        day,
                        e,
                    )
                    continue

                generations: dict[str, os.stat_result] = {}
                if day_stat is not None:
                    if bundle_filename != TIME_SERIES_BUNDLE_FILENAME:
                        # A feature may be empty despite positive request counts.
                        # Independent backfills can also recreate it from partial
                        # raw while retaining an older complete count reference.
                        # Neither file presence nor mtime/empty markers proves
                        # that both were generated from the same population.
                        logger.warning(
                            "[rollups] %s: %s day=%s preserved: feature input provenance unavailable",
                            service_id,
                            bundle_filename,
                            day,
                        )
                        continue
                    try:
                        generations = _time_series_replacement_inputs(con, hour_root, day_file, day, input_paths)
                    except (duckdb.Error, OSError, RuntimeError, ValueError) as e:
                        logger.warning(
                            "[rollups] %s: %s day=%s preserved: replacement coverage unverified: %s",
                            service_id,
                            bundle_filename,
                            day,
                            e,
                        )
                        continue

                os.makedirs(day_dir, exist_ok=True)
                tmp_file = os.path.join(day_dir, f"{tmp_prefix}{uuid.uuid4().hex[:12]}.parquet")
                paths_sql = quote_path_list(input_paths)
                copy_sql = build_copy_sql(paths_sql, tmp_file)
                try:
                    con.execute(copy_sql)
                except duckdb.Error as e:
                    logger.warning(
                        "[rollups] %s: %s day-compact COPY failed for day=%s: %s",
                        service_id,
                        bundle_filename,
                        day,
                        e,
                    )
                    unlink_quiet(tmp_file)
                    continue

                try:

                    def day_unchanged() -> bool:
                        # A repair/another compactor may publish while COPY runs.
                        # Compare under the same lock as replacement, not before it.
                        try:
                            current = os.stat(day_file)
                        except FileNotFoundError:
                            return day_stat is None
                        if day_stat is None or (current.st_ino, current.st_mtime_ns) != (
                            day_stat.st_ino,
                            day_stat.st_mtime_ns,
                        ):
                            return False
                        for path, original in generations.items():
                            try:
                                current = os.stat(path)
                            except OSError:
                                break
                            if (current.st_ino, current.st_mtime_ns) != (original.st_ino, original.st_mtime_ns):
                                break
                        else:
                            return True
                        logger.warning(
                            "[rollups] %s: %s day=%s preserved: replacement coverage generation changed",
                            service_id,
                            bundle_filename,
                            day,
                        )
                        return False

                    if publish_temp_parquet(tmp_file, day_file, lock_key, before_replace_under_lock=day_unchanged):
                        rebuilt += 1
                except OSError as e:
                    logger.warning(
                        "[rollups] %s: %s day rename to %s failed: %s",
                        service_id,
                        bundle_filename,
                        day_file,
                        e,
                    )
    finally:
        con.close()

    return rebuilt


def build_per_hour_bundles(
    service_id: str,
    source: dict[str, Any],
    hours: list[str],
    *,
    bundle_filename: str,
    tmp_prefix: str,
    label: str,
    eligibility: Callable[[set[str], str], Any],
    build_copy_sql: Callable[[Any, str, str, str, str], str],
    describe_label: str | None = None,
    logger: logging.Logger = logger,
    output_root: str | None = None,
) -> int:
    """Shared per-hour bundle writer — the writer-side mirror of
    :func:`compact_closed_days`.

    Owns the scaffold every per-feature ``build_*_bundles`` writer repeats:
    drop the active hour + malformed tokens, resolve the service view, open a
    read-only connection, describe its columns, then for each closed hour
    write a temp parquet via ``build_copy_sql`` and atomically rename it into
    ``hour=H/<bundle_filename>`` under the per-service view lock (cleaning
    up the temp file on any failure). ``output_root`` optionally directs all
    outputs to private staging without changing the source or its connection.

    Per-feature logic lives in two callbacks:

      - ``eligibility(cols, table_ident)`` returns ``None`` to skip this
        service entirely (a required column is absent) — emitting any
        feature-specific skip log itself — or an opaque context object that is
        passed straight through to ``build_copy_sql``.
      - ``build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path)``
        returns the full ``COPY (...) TO '<tmp_path>' (...)`` statement,
        byte-for-byte as the inline writers built it.

    ``label`` names the feature in the COPY/publish warnings; ``describe_label``
    overrides it for the ``describe_columns`` log label only (defaults to
    ``label``) so a writer that drives this once per dimension can keep a
    single un-suffixed describe label. Pass each caller's module ``logger`` so
    log records keep their origin module. Returns the number of bundles
    written this call.
    """
    if not hours:
        return 0

    import duckdb

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    target_hours: list[str] = []
    for h in hours:
        if h == active_hour:
            continue
        if parse_hour_token(h) is None:
            logger.warning("[rollups] skipping malformed hour token: %r", h)
            continue
        target_hours.append(h)
    if not target_hours:
        return 0

    table_ident = _safe_table_for(source)
    if not table_ident:
        return 0

    bundled_root = output_root if output_root is not None else _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = str(source.get("name", "default"))

    con, cols, owns_con = acquire_rollup_connection_and_cols(
        source,
        table_ident,
        logger=logger,
        log_label=f"cannot describe {describe_label or label} bundle",
    )
    try:
        if cols is None:
            return 0

        session = _SHARED_ROLLUP_SESSION.get()
        if not owns_con and session is not None and session.source is source:
            from .rollup_coverage import heal_where_clause

            table_ident, _ = session.materialize_slice(
                table_ident,
                heal_where_clause(sorted(set(target_hours)), None, None),
                logger=logger,
            )

        ctx = eligibility(cols, table_ident)
        if ctx is None:
            return 0

        rebuilt = 0
        for hour in target_hours:
            hour_dt = parse_hour_key_strict(hour)
            start_iso = hour_dt.isoformat()
            end_iso = (hour_dt + timedelta(hours=1)).isoformat()

            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            os.makedirs(bundle_dir, exist_ok=True)
            bundle_path = os.path.join(bundle_dir, bundle_filename)

            tmp_path = os.path.join(bundle_dir, f"{tmp_prefix}{uuid.uuid4().hex[:12]}.parquet")
            query = build_copy_sql(ctx, table_ident, start_iso, end_iso, tmp_path)
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning(
                    "[rollups] %s: %s COPY failed for hour=%s: %s",
                    service_id,
                    label,
                    hour,
                    e,
                )
                unlink_quiet(tmp_path)
                continue

            try:
                publish_temp_parquet(tmp_path, bundle_path, lock_key)
                rebuilt += 1
            except OSError as e:
                logger.warning(
                    "[rollups] %s: could not publish %s for hour=%s: %s",
                    service_id,
                    label,
                    hour,
                    e,
                )

        return rebuilt
    finally:
        if owns_con:
            con.close()


class _SharedRollupSession:
    """Lazy per-tick cache for the read-only DuckDB connection, DESCRIBE columns,
    and materialized touched-hours temp slices."""

    def __init__(self, source: dict[str, Any]) -> None:
        self.source = source
        self.con: duckdb.DuckDBPyConnection | None = None
        self.cols_by_table: dict[str, set[str] | None] = {}
        self.slices_by_key: dict[tuple[str, str], tuple[str, str]] = {}
        self.ngwaf_bots_alias: str | None = None
        self.ngwaf_bots_ready = False

    def acquire(
        self,
        table_ident: str,
        *,
        logger: logging.Logger,
        log_label: str,
    ) -> tuple[duckdb.DuckDBPyConnection, set[str] | None]:
        from backend.core.duckdb import get_connection

        if self.con is None:
            self.con = get_connection(source=self.source, read_only=True)
        if table_ident not in self.cols_by_table:
            self.cols_by_table[table_ident] = describe_columns(
                self.con,
                self.source,
                table_ident,
                logger=logger,
                log_label=log_label,
            )
        return self.con, self.cols_by_table[table_ident]

    def materialize_slice(
        self,
        table_ident: str,
        where_sql: str,
        *,
        logger: logging.Logger,
    ) -> tuple[str, str]:
        """Materialize ``SELECT * FROM {table_ident} WHERE {where_sql}`` into a
        session-scoped temp table once so per-field rollup writers scan the
        in-memory slice instead of re-scanning the underlying view 40×."""
        key = (table_ident, where_sql)
        if key in self.slices_by_key:
            return self.slices_by_key[key]
        if self.con is None:
            return table_ident, where_sql
        from backend.core.storage import execute_with_stale_view_retry

        slice_ident = f"_rollup_slice_{len(self.slices_by_key)}"
        try:
            execute_with_stale_view_retry(
                self.con,
                self.source,
                lambda c: c.execute(
                    f"CREATE OR REPLACE TEMP TABLE {slice_ident} AS SELECT * FROM {table_ident} WHERE {where_sql}"
                ),
            )
            res = (slice_ident, "1=1")
        except Exception as e:  # noqa: BLE001
            logger.debug("[rollups] temp slice materialization skipped (%s); scanning %s directly", e, table_ident)
            res = (table_ident, where_sql)
        self.slices_by_key[key] = res
        return res

    def materialize_ngwaf_bots(self) -> str | None:
        from backend.repositories._base import ensure_ngwaf_bots_materialized

        if self.con is None:
            raise RuntimeError("NGWAF metadata requires an acquired rollup connection")
        started = time.perf_counter()
        reused = self.ngwaf_bots_ready
        if self.ngwaf_bots_alias is None:
            self.ngwaf_bots_alias = f"_rollup_ngwaf_{uuid.uuid4().hex}"
        if not reused:
            self.ngwaf_bots_ready = ensure_ngwaf_bots_materialized(self.con, self.ngwaf_bots_alias)
        logger.debug(
            "[rollups] ngwaf_cache ready=%s reused=%s duration_ms=%.2f",
            self.ngwaf_bots_ready,
            reused,
            (time.perf_counter() - started) * 1000,
        )
        return f"{self.ngwaf_bots_alias}_ngwaf_bots" if self.ngwaf_bots_ready else None

    def close(self) -> None:
        if self.con is not None:
            try:
                for slice_ident, _ in self.slices_by_key.values():
                    if slice_ident.startswith("_rollup_slice_"):
                        with contextlib.suppress(Exception):
                            self.con.execute(f"DROP TABLE IF EXISTS temp.main.{slice_ident}")
                if self.ngwaf_bots_alias is not None:
                    try:
                        self.con.execute(f"DROP TABLE IF EXISTS temp.main.{self.ngwaf_bots_alias}_ngwaf_bots")
                    finally:
                        self.con.unregister(f"temp_arrow_{self.ngwaf_bots_alias}")
            finally:
                try:
                    self.con.close()
                finally:
                    self.con = None
                    self.slices_by_key.clear()
                    self.ngwaf_bots_alias = None
                    self.ngwaf_bots_ready = False


_SHARED_ROLLUP_SESSION: contextvars.ContextVar[_SharedRollupSession | None] = contextvars.ContextVar(
    "_SHARED_ROLLUP_SESSION", default=None
)


@contextlib.contextmanager
def shared_rollup_session(source: dict[str, Any]) -> Iterator[_SharedRollupSession]:
    """Share a single lazy read-only DuckDB connection and DESCRIBE column set
    across all writers within a recompute tick."""
    existing = _SHARED_ROLLUP_SESSION.get()
    if existing is not None and existing.source is source:
        yield existing
        return

    session = _SharedRollupSession(source)
    token = _SHARED_ROLLUP_SESSION.set(session)
    try:
        yield session
    finally:
        _SHARED_ROLLUP_SESSION.reset(token)
        session.close()


def acquire_rollup_connection_and_cols(
    source: dict[str, Any],
    table_ident: str,
    *,
    logger: logging.Logger,
    log_label: str,
) -> tuple[duckdb.DuckDBPyConnection, set[str] | None, bool]:
    """Return ``(con, cols, owns_con)`` using the active :func:`shared_rollup_session`
    when available, or opening a standalone connection otherwise."""
    from backend.core.duckdb import get_connection

    session = _SHARED_ROLLUP_SESSION.get()
    if session is not None and session.source is source:
        con, cols = session.acquire(table_ident, logger=logger, log_label=log_label)
        return con, cols, False

    con = get_connection(source=source, read_only=True)
    cols = describe_columns(con, source, table_ident, logger=logger, log_label=log_label)
    return con, cols, True


MultiBundleSpec = tuple[
    str,
    str,
    tuple[str, ...],
    Callable[[Any, str, str, str, str], str],
]


def build_multi_spec_bundles(
    service_id: str,
    source: dict[str, Any],
    hours: list[str],
    *,
    specs: Sequence[MultiBundleSpec],
    group_label: str,
    tmp_prefix: str,
    logger: logging.Logger = logger,
    context_fn: Callable[[set[str], str], Any] | None = None,
    driver: Callable[..., int] | None = None,
) -> int:
    """Shared multi-bundle per-hour writer for modules that emit multiple
    dimension parquet files per closed hour."""
    run_driver = driver or build_per_hour_bundles
    written = 0
    for sub_label, filename, req_cols, build_copy_sql in specs:

        def eligibility(cols: set[str], table_ident: str, _req_cols: tuple[str, ...] = req_cols) -> object | None:
            ctx = context_fn(cols, table_ident) if context_fn is not None else True
            if ctx is None:
                return None
            for c in _req_cols:
                if c not in cols:
                    return None
            return ctx

        written += run_driver(
            service_id,
            source,
            hours,
            bundle_filename=filename,
            tmp_prefix=tmp_prefix,
            label=f"{group_label}({sub_label})",
            describe_label=group_label,
            eligibility=eligibility,
            build_copy_sql=build_copy_sql,
            logger=logger,
        )
    return written


def backfill_multi_spec_bundles(
    service_id: str,
    source: dict[str, Any],
    *,
    specs: Sequence[MultiBundleSpec],
    group_label: str,
    tmp_prefix: str,
    logger: logging.Logger = logger,
    context_fn: Callable[[set[str], str], Any] | None = None,
    driver: Callable[..., int] | None = None,
) -> int:
    """Self-heal pass for multi-bundle rollup modules: walks each bundle
    filename independently so an interrupted partial run never strands the
    remaining bundles behind a single sentinel."""
    written = 0
    for spec in specs:
        sub_label, filename, _, _ = spec

        def _one_bundle_builder(
            sid: str,
            src: dict[str, Any],
            hours: list[str],
            _spec: MultiBundleSpec = spec,
        ) -> int:
            return build_multi_spec_bundles(
                sid,
                src,
                hours,
                specs=(_spec,),
                group_label=group_label,
                tmp_prefix=tmp_prefix,
                logger=logger,
                context_fn=context_fn,
                driver=driver,
            )

        written += backfill_missing_bundles(
            service_id,
            source,
            bundle_filename=filename,
            label=f"{group_label}({sub_label})",
            builder=_one_bundle_builder,
            logger=logger,
        )
    return written


def cleanup_per_field_after_bundle(
    per_field_root: str,
    partition_subdir: str,
    bundle_path: str,
    service_id: str,
    *,
    file_kind_label: str = "per-field",
    dir_kind_label: str = "per-field",
    cleanup_label: str = "cleanup",
    logger: logging.Logger = logger,
) -> None:
    """Sweep per-field parquet directories for ``partition_subdir`` (e.g.
    ``hour=YYYY-MM-DD-HH`` or ``day=YYYY-MM-DD``) after its bundled file
    has been published and verified to be at least as new as every source
    per-field parquet."""
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
        for field_entry in os.listdir(per_field_root):
            if not field_entry.startswith("field="):
                continue
            target_dir = os.path.join(per_field_root, field_entry, partition_subdir)
            if not os.path.isdir(target_dir):
                continue
            ok = True
            count_here = 0
            try:
                for fname in os.listdir(target_dir):
                    if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                        continue
                    p = os.path.join(target_dir, fname)
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
                candidate_dirs.append(target_dir)
                file_count += count_here
    except OSError:
        return

    if not candidate_dirs:
        return

    if dry_run:
        logger.info(
            "[rollups] %s: ROLLUP_CLEANUP_DRY_RUN — would delete %d %s parquets across %d field dirs for %s",
            service_id,
            file_count,
            file_kind_label,
            len(candidate_dirs),
            partition_subdir,
        )
        return

    deleted_dirs = 0
    for target_dir in candidate_dirs:
        try:
            shutil.rmtree(target_dir)
            deleted_dirs += 1
        except OSError as e:
            logger.warning("[rollups] %s: %s failed for %s: %s", service_id, cleanup_label, target_dir, e)
    logger.debug(
        "[rollups] %s: cleaned %d %s dirs (~%d parquets) for bundled %s",
        service_id,
        deleted_dirs,
        dir_kind_label,
        file_count,
        partition_subdir,
    )


def describe_columns(
    con: duckdb.DuckDBPyConnection,
    source: dict[str, Any],
    table_ident: str,
    *,
    logger: logging.Logger | None = None,
    log_label: str = "",
) -> set[str] | None:
    """Run ``DESCRIBE <table_ident>`` against ``con`` with the standard
    stale-view-retry hop, returning the set of column names. Returns
    ``None`` and (optionally) warns through ``logger`` if DuckDB raises —
    callers treat that as "view not ready, skip this round".
    """
    from backend.core.storage import execute_with_stale_view_retry

    try:
        rows = execute_with_stale_view_retry(
            con,
            source,
            lambda c: c.execute(f"DESCRIBE {table_ident}").fetchall(),
        )
    except Exception as e:  # noqa: BLE001 — DuckDB raises typed errors but the stale-view retry hop may wrap them
        if logger is not None:
            service_id = source.get("name", "default")
            label = f"{log_label}: " if log_label else ""
            logger.warning("[rollups] %s: %s%s: %s", service_id, label, table_ident, e)
        return None
    return {row[0] for row in rows}
