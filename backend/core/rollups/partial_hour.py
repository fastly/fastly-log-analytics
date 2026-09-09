"""Incremental partial-hour rollup — the "speed layer" for the currently-
open hour.

Deliberately a SEPARATE tree from ``rollups/hour``/``rollups/hour_bundled``
(see docs/superpowers/specs/2026-09-09-partial-hour-speed-layer-design.md
Part 2): readers only ever consult ``rollups/partial_hour/hour=<H>`` while
``H`` is the current UTC hour. The moment the wall clock rolls over, that
tree is simply abandoned — no coordination with ``recompute_touched_hours``
/ ``bundle_hours``/``_run_rollup_hour_heal`` is needed, because this module
never writes into their trees and they never read this one.

Storage per hour: ``all_fields.parquet`` (schema ``field, value, count`` —
identical to the real closed-hour bundle, so it reads with the exact same
merge helpers) plus a ``.watermark`` file holding a single float epoch
timestamp: the mtime of the newest source file already folded in. A tick
that dies before its atomic rename leaves both files untouched, so the next
tick just re-includes the same "new" files — merges are idempotent because
they always compute the SAME output from the SAME (existing rollup + new
files) input, never an accumulate-in-place counter.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from datetime import UTC, datetime, timedelta

from ._common import TOP_K, _build_copy_query, _is_safe_ident, parse_hour_token

logger = logging.getLogger(__name__)


def partial_hour_root(source: dict) -> str:
    # Imported lazily (matches _common.py's _rollups_root et al.) so tests
    # that monkeypatch backend.core.duckdb._cache_dir as a module attribute
    # take effect — a module-level `from ... import _cache_dir` would bind
    # the original function at import time and never observe the patch.
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "partial_hour")


def partial_hour_dir(source: dict, hour: str) -> str:
    return os.path.join(partial_hour_root(source), f"hour={hour}")


def _watermark_path(source: dict, hour: str) -> str:
    return os.path.join(partial_hour_dir(source, hour), ".watermark")


def _all_fields_path(source: dict, hour: str) -> str:
    return os.path.join(partial_hour_dir(source, hour), "all_fields.parquet")


def read_partial_hour_watermark(source: dict, hour: str) -> float:
    path = _watermark_path(source, hour)
    try:
        with open(path) as f:
            return float(f.read().strip())
    except (OSError, ValueError):
        return 0.0


def _write_partial_hour_watermark(source: dict, hour: str, value: float) -> None:
    d = partial_hour_dir(source, hour)
    os.makedirs(d, exist_ok=True)
    path = _watermark_path(source, hour)
    tmp = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
    with open(tmp, "w") as f:
        f.write(repr(value))
    os.replace(tmp, path)


def read_partial_hour_all_fields(source: dict, hour: str) -> list[tuple[str, str, int]]:
    path = _all_fields_path(source, hour)
    if not os.path.isfile(path):
        return []
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(f"SELECT field, value, count FROM read_parquet('{path}')").fetchall()
        return [(r[0], r[1], int(r[2])) for r in rows]
    finally:
        con.close()


def _list_new_source_files(source: dict, hour: str, watermark: float) -> list[str]:
    """Buffer + active-hour-partition parquets newer than ``watermark``.

    Mirrors the mtime-floor pruning ``_create_active_hour_temp_direct``
    (backend/repositories/_base.py) already uses — a file finalized at or
    before the watermark cannot hold a row this module hasn't already
    folded in, so re-reading it would double-count.
    """
    from backend.core.duckdb import _cache_dir

    cache_dir = _cache_dir(source)
    dirs = [
        os.path.join(cache_dir, "buffer"),
        os.path.join(cache_dir, "data", f"timestamp_hour={hour}"),
    ]
    try:
        from backend.core.iceberg.buffer import _tombstoned_parquet_paths

        tombstoned = _tombstoned_parquet_paths(os.path.join(cache_dir, "buffer"))
    except Exception:
        tombstoned = set()

    out: list[str] = []
    for d in dirs:
        try:
            with os.scandir(d) as it:
                for e in it:
                    if not e.name.endswith(".parquet") or e.name.startswith(".tmp_"):
                        continue
                    if e.path in tombstoned:
                        continue
                    try:
                        if e.stat().st_mtime <= watermark:
                            continue
                    except OSError:
                        continue
                    out.append(e.path)
        except OSError:
            continue
    return out


def merge_partial_hour(service_id: str, source: dict, fields: list[str]) -> dict:
    """One incremental merge tick for the currently-active hour.

    Returns ``{"hour": str, "new_files": int, "duration_ms": float}`` for
    the caller (the cron job) to log/summarize.
    """
    t0 = time.perf_counter()
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    watermark = read_partial_hour_watermark(source, active_hour)
    new_files = _list_new_source_files(source, active_hour, watermark)
    if not new_files:
        return {"hour": active_hour, "new_files": 0, "duration_ms": (time.perf_counter() - t0) * 1000}

    safe_fields = [f for f in fields if _is_safe_ident(f)]
    if not safe_fields:
        return {"hour": active_hour, "new_files": 0, "duration_ms": (time.perf_counter() - t0) * 1000}

    import duckdb

    from ._common import quote_path_list

    con = duckdb.connect(":memory:")
    try:
        # DuckDB's session TimeZone defaults to the OS locale, not UTC (see
        # backend/core/duckdb.py's connection setup) — without pinning it,
        # the TIMESTAMP literal in where_sql below would be interpreted
        # against the host machine's zone instead of the UTC hour boundary
        # this module computes, silently dropping rows on any non-UTC host.
        con.execute("SET TimeZone='UTC';")
        paths_sql = quote_path_list(new_files)
        table_ident = f"read_parquet([{paths_sql}], union_by_name=true)"

        # A buffer parquet is included whenever its mtime is newer than the
        # watermark (see _list_new_source_files), but a buffer file isn't
        # guaranteed to hold ONLY rows stamped inside the active hour — one
        # finalized right at the boundary can straddle it. _build_copy_query
        # groups by a derived `hour` column internally but does not filter
        # by it, so an unfiltered WHERE would let a stray previous-hour row
        # inflate this hour's counts. Pin the predicate to the active hour's
        # exact [start, end) range so straddling rows outside it are
        # dropped, matching the same half-open convention
        # _create_active_hour_temp_direct's callers already rely on.
        hour_start = datetime.strptime(active_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        hour_end = hour_start + timedelta(hours=1)
        where_sql = (
            f"timestamp >= TIMESTAMP '{hour_start.isoformat()}' AND timestamp < TIMESTAMP '{hour_end.isoformat()}'"
        )

        per_field_selects = [_build_copy_query(table_ident, f, where_sql) for f in safe_fields]
        increment_sql = " UNION ALL ".join(f"({s})" for s in per_field_selects)

        existing_rows = read_partial_hour_all_fields(source, active_hour)
        existing_sql = ""
        if existing_rows:
            values_sql = ", ".join(
                f"('{f.replace(chr(39), chr(39) * 2)}', '{v.replace(chr(39), chr(39) * 2)}', {c})"
                for f, v, c in existing_rows
            )
            existing_sql = f" UNION ALL SELECT * FROM (VALUES {values_sql}) AS t(field, value, count)"

        merged_sql = f"""
            SELECT field, value, CAST(count AS BIGINT) AS count FROM (
                SELECT field, value, SUM(count) AS count,
                       ROW_NUMBER() OVER (PARTITION BY field ORDER BY SUM(count) DESC) AS rn
                FROM (
                    SELECT field, value, count FROM ({increment_sql}){existing_sql}
                )
                GROUP BY field, value
            ) WHERE rn <= {TOP_K}
        """

        d = partial_hour_dir(source, active_hour)
        os.makedirs(d, exist_ok=True)
        tmp_path = os.path.join(d, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
        con.execute(f"COPY ({merged_sql}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        os.replace(tmp_path, _all_fields_path(source, active_hour))
    finally:
        con.close()

    max_mtime = max((os.path.getmtime(p) for p in new_files), default=watermark)
    _write_partial_hour_watermark(source, active_hour, max_mtime)

    return {
        "hour": active_hour,
        "new_files": len(new_files),
        "duration_ms": (time.perf_counter() - t0) * 1000,
    }


def gc_stale_partial_hours(source: dict, max_age_hours: int = 2) -> int:
    """Delete partial-hour dirs older than ``max_age_hours``.

    Safe unconditionally: by the time an hour is this stale, the real
    closed-hour rollup (built by the completely separate
    recompute_touched_hours / rollup_hour_heal machinery) has taken over —
    this module's readers only ever address the CURRENT active hour's
    directory, so a removed old one is never consulted again regardless.
    """
    root = partial_hour_root(source)
    if not os.path.isdir(root):
        return 0
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
    removed = 0
    try:
        entries = os.listdir(root)
    except OSError:
        return 0
    for entry in entries:
        if not entry.startswith("hour="):
            continue
        hour = entry[len("hour=") :]
        if hour == active_hour:
            continue
        parsed = parse_hour_token(hour)
        if parsed is None or parsed >= cutoff:
            continue
        try:
            shutil.rmtree(os.path.join(root, entry))
            removed += 1
        except OSError as e:
            logger.warning("[partial_hour] failed to GC stale hour=%s: %s", hour, e)
    return removed
