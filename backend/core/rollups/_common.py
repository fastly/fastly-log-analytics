"""Shared primitives for the rollups package.

Constants, ident validators, path helpers, atomic marker IO, COPY query
builders, and the virtual-field backing map — everything every other
sub-module needs.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)

# How many top values per (field, hour) we persist. Dashboards render
# 10-25 at a time; 500 gives generous headroom for filter overlays and
# the long-tail "Other" rollup.
TOP_K = 500

# SQL identifier safelist. Field names land verbatim inside ``"..."``
# quoted identifiers and inside SELECT projections; service names land
# in the table identifier ``logs_<name>``. Both come from cfg / DuckDB
# schema and are PROBABLY already validated upstream — but a single
# stray double-quote or backtick in either would break the query in a
# way that's both a correctness bug and a privilege boundary (the
# fields are derived from admin-controlled custom_field entries).
# Defense in depth: this module reject anything not matching the
# pattern with a logged warning.
_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_ident(name: str) -> bool:
    return bool(name) and bool(_SAFE_IDENT_RE.match(name))


def _safe_table_for(source: dict) -> str | None:
    """Return the DuckDB view name for this service, or ``None`` if no slug.

    Slugifies the same way the dashboard's view-builder does
    (``backend.core.duckdb._safe_table_name``: non-alphanumerics to ``_``,
    lowercased, ``logs_`` prefix) so the rollup COPY/SELECT targets the
    same view name the dashboard creates. Reads ``service_id`` first (the
    canonical slug in normalized source dicts) and falls back to ``name``
    for callers that pass a raw on-disk config — both cases pass through
    the slugifier identically.
    """
    raw = source.get("service_id") or source.get("name") or ""
    if not raw:
        logger.warning("[rollups] no service_id/name in source dict; skipping rollup")
        return None
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(raw)


def _get_fields(src: dict) -> list[str]:
    """Return the dashboard fields eligible for rollup.

    Custom-field names are validated against ``_SAFE_IDENT_RE`` — anything
    failing the check is skipped with a warning rather than fed into SQL.

    Includes virtual fields (waf_sig_ind, edge_score_reason_ind) — those
    used to be excluded because they require unnesting a CSV column, but
    we now have a dedicated SQL builder (``_build_virtual_field_copy_query``)
    that does the unnest at write time so the dashboard reader doesn't
    have to rescan + unnest the raw window at query time.
    """
    from backend.repositories.dashboard import _VIRTUAL_FIELDS, FIELDS

    lf_config = src.get("log_fields") or {}
    custom_field_names: list[str] = []
    for cf in lf_config.get("custom_fields", []):
        if not cf.get("enabled", True) or not cf.get("show_in_dashboard", True):
            continue
        name = cf.get("name") or ""
        if not _is_safe_ident(name):
            logger.warning("[rollups] skipping custom field with unsafe name: %r", name)
            continue
        custom_field_names.append(name)
    actual_fields = [f for f in FIELDS if f not in _VIRTUAL_FIELDS and _is_safe_ident(f)]
    virtual_fields = [f for f in _VIRTUAL_FIELDS if f in _VIRTUAL_FIELD_BACKING and _is_safe_ident(f)]
    return actual_fields + virtual_fields + custom_field_names


def _rollups_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour")


def _day_rollups_root(source: dict) -> str:
    """Per-day compacted rollups directory.

    Companion to `_rollups_root` (which holds per-hour rollups). Populated
    by `compact_closed_days_to_daily` — each (field, closed-day) becomes
    a single parquet file aggregating its 24 source hour parquets. The
    reader (`execute_top_n_rollups`) prefers per-day files for closed
    days and falls back to per-hour for the active trailing window.
    Item 17 / RC-9.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "day")


def _markers_path(source: dict) -> str:
    """JSON file tracking which fields have been backfilled.

    Replaces the prior single ``.backfill_done`` marker which couldn't
    distinguish "fully backfilled" from "backfilled before a new custom
    field was added". Shape: ``{"field": "ISO timestamp", ...}``.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "backfill_markers.json")


def _load_markers(source: dict) -> dict[str, str]:
    path = _markers_path(source)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("[rollups] could not read markers at %s: %s", path, e)
        return {}


def _save_markers(source: dict, markers: dict[str, str]) -> None:
    path = _markers_path(source)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Atomic write so a crash mid-write doesn't truncate the file.
    tmp_path = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp_path, "w") as f:
            json.dump(markers, f)
        os.replace(tmp_path, path)
    except OSError as e:
        logger.warning("[rollups] could not write markers to %s: %s", path, e)
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def _publish_field_partitions(tmp_field_dir: str, dst_root: str, field: str) -> int:
    """Move per-hour parquet files from a temp PARTITION_BY tree into the
    canonical ``rollups/hour/field=X/hour=Y/`` layout.

    The publish order is RENAME-then-UNLINK to close the race window where
    a concurrent dashboard read could observe an empty hour directory.
    Worst case after this change: a dashboard read briefly sees BOTH the
    new and old parquet for the same hour and double-counts that hour
    until the unlink lands — which is bounded and self-corrects on the
    next refresh. Pre-fix, the dashboard could observe ZERO files for the
    hour (undercount), which was indistinguishable from a real traffic dip.

    Caller MUST hold the per-service iceberg lock around the whole call.
    Returns the number of hour-dirs published.
    """
    field_dir = os.path.join(tmp_field_dir, f"field={field}")
    if not os.path.isdir(field_dir):
        return 0

    published = 0
    for hour_dirname in os.listdir(field_dir):
        if not hour_dirname.startswith("hour="):
            continue
        src_hour_dir = os.path.join(field_dir, hour_dirname)
        dst_hour_dir = os.path.join(dst_root, f"field={field}", hour_dirname)
        os.makedirs(dst_hour_dir, exist_ok=True)

        # 1. Rename new files into place first (overcounting window OK).
        new_names: set[str] = set()
        for fname in os.listdir(src_hour_dir):
            if not fname.endswith(".parquet"):
                continue
            new_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
            os.rename(os.path.join(src_hour_dir, fname), os.path.join(dst_hour_dir, new_name))
            new_names.add(new_name)

        # 2. Now unlink any pre-existing files that we didn't just write.
        if new_names:
            for existing in os.listdir(dst_hour_dir):
                if existing.endswith(".parquet") and existing not in new_names:
                    try:
                        os.remove(os.path.join(dst_hour_dir, existing))
                    except OSError as e:
                        logger.warning("[rollups] could not unlink stale %s: %s", existing, e)
            published += 1

    return published


def _build_copy_query(table_ident: str, field: str, where_sql: str) -> str:
    """Return the COPY ... TO <tmp> PARTITION_BY (field, hour) SQL for one field.

    Inputs must already be validated — this function does NO escaping.
    Callers (recompute_touched_hours / backfill_rollups) gate via
    ``_is_safe_ident`` and ``_safe_table_for``.
    """
    return f"""
        SELECT field, hour, value, count FROM (
            SELECT
                '{field}' AS field,
                strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                CAST("{field}" AS VARCHAR) AS value,
                COUNT(*) AS count,
                ROW_NUMBER() OVER (
                    PARTITION BY strftime(timestamp, '%Y-%m-%d-%H')
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM {table_ident}
            WHERE {where_sql}
            GROUP BY 1, 2, 3
        ) WHERE rn <= {TOP_K}
    """


# Virtual fields are dashboard panels whose values come from
# unnesting a comma-separated CSV column at query time
# (``backend.repositories.dashboard._VIRTUAL_FIELDS``). Pre-aggregating
# them into the rollup tree eliminates the runtime-unnest cost that
# dominates dashboard 30d (per the perf audit: waf_sig_ind_explode
# ~1.2 s + edge_score_reason_ind_explode ~0.7 s on prod 30d).
#
# Map: <virtual_field_name> → <backing_column_name>.
# Mirrors the call sites in dashboard.py:_exploded_top_n.
_VIRTUAL_FIELD_BACKING: dict[str, str] = {
    "waf_sig_ind": "waf_sig",
    "edge_score_reason_ind": "edge_score_reason",
}


def _build_virtual_field_copy_query(table_ident: str, virtual_field: str, backing_col: str, where_sql: str) -> str:
    """COPY SQL for a virtual (unnest-based) field rollup.

    Same output shape as :func:`_build_copy_query` (field/hour/value/count)
    so the per-field rollup tree, hour bundling, day bundling, and
    reader path all work unchanged. The only difference is the inner
    SELECT does the CSV unnest before grouping.

    Same input-validation contract: callers gate via ``_is_safe_ident``
    on both the virtual field name and the backing column name.
    """
    return f"""
        SELECT field, hour, value, count FROM (
            SELECT
                '{virtual_field}' AS field,
                hour,
                value,
                count,
                ROW_NUMBER() OVER (
                    PARTITION BY hour
                    ORDER BY count DESC
                ) AS rn
            FROM (
                SELECT
                    strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                    trim(signal) AS value,
                    COUNT(*) AS count
                FROM (
                    SELECT timestamp, unnest(string_split("{backing_col}", ',')) AS signal
                    FROM {table_ident}
                    WHERE {where_sql}
                      AND "{backing_col}" IS NOT NULL
                      AND "{backing_col}" != ''
                )
                WHERE trim(signal) != ''
                GROUP BY 1, 2
            )
        ) WHERE rn <= {TOP_K}
    """


def _hour_bundled_root(source: dict) -> str:
    """Return the per-hour bundled rollup root.

    Layout: cache/<svc>/rollups/hour_bundled/hour=YYYY-MM-DD-HH/all_fields.parquet
    Each bundle contains rows for ALL fields for that hour with the same
    (field, value, count) schema as the per-field hour parquets. Reading
    one bundle replaces opening ~40+ per-field files for that hour.

    The same hour directory also holds ``time_series.parquet`` — see
    :func:`build_time_series_bundles` for the schema.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour_bundled")


def _day_bundled_root(source: dict) -> str:
    """Return the per-day bundled rollup root.

    Layout: cache/<svc>/rollups/day_bundled/day=YYYY-MM-DD/all_fields.parquet
    Each bundle contains rows for ALL fields for that day with the same
    (field, value, count) schema as the per-field day parquets. Reading
    one bundle replaces opening ~40 per-field files for that day; on a
    30-day window this cuts file opens from ~1,200 to ~30. Per the perf
    audit, ``top_n_rollups:rolled_res`` was the dominant cost
    (4 s on prod 30d) entirely because of per-file open overhead on
    the per-field-day tree.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "day_bundled")


# Filename for the per-day bundled rollup (same as the per-hour
# bundled). Kept identical so future tooling can treat the two trees
# uniformly when needed.
DAY_BUNDLE_FILENAME = "all_fields.parquet"

# Per-(field, day) row cap inside the bundled-day parquet. The
# dashboard top-N panel renders 10 values; 100 gives generous headroom
# for the global top-10 to be visible in at least one day across a
# 30-day window. Anything beyond rank 100 in a single day is
# aggregated into a single synthetic ``__other__`` row so
# field totals stay correct.
DAY_BUNDLE_TOP_K = 100


# Filename for the per-hour 1-minute time-series rollup. Kept as a constant
# so the writer + reader can never drift on the name.
TIME_SERIES_BUNDLE_FILENAME = "time_series.parquet"

# Filename for the per-hour per-(ip, ja4) sessions rollup. Stored
# alongside time_series.parquet so the same reader can enumerate both
# in one directory walk.
SESSIONS_BUNDLE_FILENAME = "sessions.parquet"


def _time_series_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", TIME_SERIES_BUNDLE_FILENAME)


def _sessions_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SESSIONS_BUNDLE_FILENAME)


def parse_hour_token(h: str) -> datetime | None:
    """Parse a rollup hour partition token (``"YYYY-MM-DD-HH"``) to a
    tz-aware UTC datetime, or ``None`` if the string doesn't match."""
    try:
        return datetime.strptime(h, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    except ValueError:
        return None


def discover_closed_hours(source: dict) -> set[str]:
    """Return every ``"YYYY-MM-DD-HH"`` partition that exists under
    ``_rollups_root(source)`` and is strictly before the active hour.

    Skips field directories that don't begin with ``"field="``; tolerates
    missing roots and unreadable sub-directories by treating them as
    empty (the rollups jobs already handle the "no data yet" case).
    """
    hour_root = _rollups_root(source)
    if not os.path.isdir(hour_root):
        return set()

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    all_hours: set[str] = set()
    try:
        for field_entry in os.listdir(hour_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(hour_root, field_entry)
            try:
                for hour_entry in os.listdir(field_dir):
                    if not hour_entry.startswith("hour="):
                        continue
                    hour = hour_entry[len("hour=") :]
                    if hour >= active_hour:
                        continue
                    all_hours.add(hour)
            except OSError:
                continue
    except OSError:
        return set()
    return all_hours


def describe_columns(
    con: duckdb.DuckDBPyConnection,
    source: dict,
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
    from backend.core.iceberg import execute_with_stale_view_retry

    try:
        rows = execute_with_stale_view_retry(
            con,
            source,
            lambda c: c.execute(f"DESCRIBE {table_ident}").fetchall(),
        )
    except Exception as e:  # noqa: BLE001 — DuckDB raises typed errors but iceberg may wrap them
        if logger is not None:
            service_id = source.get("name", "default")
            label = f"{log_label}: " if log_label else ""
            logger.warning("[rollups] %s: %s%s: %s", service_id, label, table_ident, e)
        return None
    return {row[0] for row in rows}
