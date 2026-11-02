"""Explicit, fail-closed repair of selected daily derivatives from retained raw."""

from __future__ import annotations

import logging
import os
import re
import shutil
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypedDict

import duckdb

from backend.core.duckdb import get_connection
from backend.core.metric_expressions import sum_or_zero
from backend.core.storage import update_storage_view
from backend.core.storage._core import _get_service_lock
from backend.utils.date_utils import parse_day_key_strict

from ._common import (
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    TIME_SERIES_BUNDLE_FILENAME,
    _build_copy_query,
    _day_bundled_root,
    _hour_bundled_root,
    _safe_table_for,
    parquet_zstd_target,
    quote_path_list,
)
from .bundle_builder import publish_temp_parquet
from .day_bundles import _link_day_bundle_alias
from .rollup_coverage import _day_bundle_file, validated_daily_reference_minutes

logger = logging.getLogger(__name__)
_REPAIR_FIELDS = frozenset({"country", "status", "method", "pop"})


class _DayRepairReceipt(TypedDict):
    day: str
    snapshot_id: int
    raw_requests: int
    hourly_requests: list[int]
    raw_first_event: str | None
    raw_last_event: str | None


class CountryDayRepairResult(_DayRepairReceipt):
    country_counts: dict[str, int]


class DailyFieldsRepairResult(_DayRepairReceipt):
    field_counts: dict[str, dict[str, int]]


def _pinned_source_sql(con: Any, source: dict[str, Any]) -> tuple[str, int]:
    """Freeze the canonical view's SELECT, including its commit version and buffers."""
    table = _safe_table_for(source)
    if table is None:
        raise RuntimeError("invalid country repair source")
    update_storage_view(con, source, force=True)
    with _get_service_lock(str(source.get("name", "default"))):
        row = con.execute(
            "SELECT sql FROM duckdb_views() WHERE view_name = ? ORDER BY temporary DESC LIMIT 1", [table]
        ).fetchone()
    sql = row[0] if row else ""
    select = re.search(r"\bAS\s+(SELECT\b.*)", sql, re.IGNORECASE | re.DOTALL)
    versions = re.findall(r"\bAT\s*\(\s*VERSION\s*=>\s*(\d+)\s*\)", sql, re.IGNORECASE)
    if select is None or len(set(versions)) != 1:
        raise RuntimeError("country repair requires a verified pinned source snapshot")
    return select[1].rstrip().rstrip(";"), int(versions[0])


def _stamp(path: Path) -> tuple[int, int, int, int]:
    st = path.stat()
    return st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def _daily_reference_hours(con: Any, reference: str, start: datetime) -> list[int]:
    """Validate a whole canonical minute-grain day; use it only as count evidence."""
    minutes = validated_daily_reference_minutes(con, reference, start)
    hourly = [0] * 24
    for bucket, requests in minutes.items():
        hourly[bucket.hour] += requests
    # Schema/alignment alone cannot distinguish a collapsed hourly summary
    # masquerading as minutes. Reconcile positive minute buckets with the
    # already pinned raw materialization; no country data comes from references.
    raw_minutes = dict(
        con.execute(
            "SELECT time_bucket(INTERVAL '1 minute', timestamp), count(*) FROM _country_repair_raw GROUP BY 1"
        ).fetchall()
    )
    if {bucket: requests for bucket, requests in minutes.items() if requests} != raw_minutes:
        raise RuntimeError("retained raw/daily minute count mismatch")
    return hourly


def rebuild_daily_fields(
    service_id: str,
    source: dict[str, Any],
    day: str,
    *,
    expected_requests: int,
    expected_fields: dict[str, dict[str, int]],
) -> DailyFieldsRepairResult:
    """Reconcile all 24 retained raw hours, then atomically replace selected fields.

    The operator's independent raw oracle and all 24 retained time-series hours
    must agree (validated daily minutes may evidence retired hourly files).
    Empty hours/minutes are legal, not a traffic assumption.
    Official hourly inputs are never rewritten or used to compute field vectors.
    Null/empty values are excluded, exactly as in the canonical hourly writer.
    """
    start = parse_day_key_strict(day)
    if start.strftime("%Y-%m-%d") != day or start >= datetime.now(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    ):
        raise ValueError("daily repair requires a closed UTC day")
    if (
        type(expected_requests) is not int
        or expected_requests <= 0
        or not isinstance(expected_fields, dict)
        or not expected_fields
        or any(
            field not in _REPAIR_FIELDS
            or not isinstance(vector, dict)
            or any(
                not isinstance(value, str) or not value or type(count) is not int or count < 0
                for value, count in vector.items()
            )
            for field, vector in expected_fields.items()
        )
    ):
        raise ValueError("daily repair requires allowed fields and an exact retained raw oracle")
    fields = sorted(expected_fields)
    field_names_sql = ", ".join(f"'{field}'" for field in fields)
    columns_sql = ", ".join(f'"{field}"' for field in fields)
    end = start + timedelta(days=1)
    day_file = Path(_day_bundled_root(source)) / f"day={day}" / DAY_BUNDLE_FILENAME
    lock_key = str(source.get("name", "default"))
    logger.info(
        "daily_fields_repair_start service_id=%s day=%s expected_requests=%d fields=%s",
        service_id,
        day,
        expected_requests,
        fields,
    )
    try:
        with TemporaryDirectory(prefix=".tmp_country_repair_", dir=day_file.parent) as tmp:
            staging = Path(tmp)
            with _get_service_lock(lock_key):
                initial_stamp = _stamp(day_file)
                backup = staging / "original.parquet"
                shutil.copyfile(day_file, backup)
            with closing(get_connection(source=source, read_only=True)) as con:
                source_sql, snapshot = _pinned_source_sql(con, source)
                # Materialize once: later queries cannot rebind the shared view or
                # mix a later commit with its earlier fixed buffer list.
                con.execute(
                    "CREATE TEMP TABLE _country_repair_raw AS "
                    f"SELECT timestamp, {columns_sql} FROM ({source_sql}) AS pinned "
                    "WHERE timestamp >= ? AND timestamp < ?",
                    [start, end],
                )
                extent = con.execute(
                    "SELECT count(*), min(timestamp), max(timestamp) FROM _country_repair_raw"
                ).fetchone()
                if extent is None:
                    raise RuntimeError("retained raw extent unavailable")
                count, first, last = extent
                hourly = [0] * 24
                for hour, requests in con.execute(
                    "SELECT date_part('hour', timestamp AT TIME ZONE 'UTC'), count(*) "
                    "FROM _country_repair_raw GROUP BY 1"
                ).fetchall():
                    hourly[int(hour)] = int(requests)
                field_counts = {
                    field: dict(
                        con.execute(
                            f'SELECT CAST("{field}" AS VARCHAR), count(*) FROM _country_repair_raw '
                            f'WHERE "{field}" IS NOT NULL AND CAST("{field}" AS VARCHAR) != \'\' GROUP BY 1'
                        ).fetchall()
                    )
                    for field in fields
                }
                if count != expected_requests or field_counts != expected_fields:
                    raise RuntimeError("retained raw oracle changed; existing daily fields preserved")
                if any(len(vector) > DAY_BUNDLE_TOP_K for vector in field_counts.values()):
                    raise RuntimeError("field vector exceeds exact daily bundle capacity")
                paths: list[str] = []
                daily_hours: list[int] | None = None
                for hour in range(24):
                    hour_start = start + timedelta(hours=hour)
                    reference = (
                        Path(_hour_bundled_root(source)) / f"hour={day}-{hour:02d}" / TIME_SERIES_BUNDLE_FILENAME
                    )
                    if reference.is_file():
                        reference_row = con.execute(
                            f"SELECT {sum_or_zero('requests')} "
                            f"FROM read_parquet([{quote_path_list([str(reference)])}]) WHERE bucket >= ? AND bucket < ?",
                            [hour_start, hour_start + timedelta(hours=1)],
                        ).fetchone()
                        if reference_row is None or reference_row[0] != hourly[hour]:
                            raise RuntimeError("retained raw/hourly count mismatch")
                    else:
                        if daily_hours is None:
                            daily_file = _day_bundle_file(_day_bundled_root(source), day, TIME_SERIES_BUNDLE_FILENAME)
                            if daily_file is None:
                                raise RuntimeError("missing retained hourly/daily count reference")
                            daily_hours = _daily_reference_hours(con, daily_file, start)
                            if daily_hours != hourly:
                                raise RuntimeError("retained raw/daily hourly count mismatch")
                            logger.info(
                                "daily_fields_repair_daily_reference service_id=%s day=%s hours=24", service_id, day
                            )
                    hour_dir = staging / f"hour={day}-{hour:02d}"
                    hour_dir.mkdir()
                    hour_path = hour_dir / DAY_BUNDLE_FILENAME
                    where = (
                        f"timestamp >= TIMESTAMPTZ '{hour_start.isoformat()}' AND "
                        f"timestamp < TIMESTAMPTZ '{(hour_start + timedelta(hours=1)).isoformat()}'"
                    )
                    query = " UNION ALL ".join(
                        f"({_build_copy_query('_country_repair_raw', field, where)})" for field in fields
                    )
                    con.execute(f"COPY ({query}) {parquet_zstd_target(str(hour_path))}")
                    paths.append(str(hour_path))
                staged_vectors: dict[str, dict[str, int]] = {field: {} for field in fields}
                for field, value, requests in con.execute(
                    f"SELECT field, value, CAST(sum(count) AS BIGINT) FROM read_parquet([{quote_path_list(paths)}], "
                    "hive_partitioning=false) GROUP BY field, value"
                ).fetchall():
                    staged_vectors[field][value] = requests
                if staged_vectors != field_counts:
                    raise RuntimeError("incomplete staged field vectors")
                output = staging / DAY_BUNDLE_FILENAME
                con.execute(
                    "COPY (SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet([{quote_path_list([str(backup)])}], hive_partitioning=false) "
                    f"WHERE field NOT IN ({field_names_sql}) UNION ALL "
                    "SELECT field, value, CAST(sum(count) AS BIGINT) AS count "
                    f"FROM read_parquet([{quote_path_list(paths)}], hive_partitioning=false) GROUP BY field, value) "
                    f"{parquet_zstd_target(str(output))}"
                )
                result: DailyFieldsRepairResult = {
                    "day": day,
                    "snapshot_id": snapshot,
                    "raw_requests": int(count),
                    "hourly_requests": hourly,
                    "field_counts": field_counts,
                    "raw_first_event": first.astimezone(UTC).isoformat() if first else None,
                    "raw_last_event": last.astimezone(UTC).isoformat() if last else None,
                }
            aliases = (
                day_file.parent / f"day_bundle_{day}.parquet",
                day_file.parent.parent / f"day_bundle_{day}.parquet",
            )

            def refresh_aliases() -> None:
                try:
                    for alias in aliases:
                        _link_day_bundle_alias(str(day_file), str(alias))
                        if alias.read_bytes() != day_file.read_bytes():
                            raise OSError("daily fields repair alias refresh failed")
                except OSError:
                    os.replace(backup, day_file)
                    for alias in aliases:
                        _link_day_bundle_alias(str(day_file), str(alias))
                    raise

            if not publish_temp_parquet(
                str(output),
                str(day_file),
                lock_key,
                before_replace_under_lock=lambda: _stamp(day_file) == initial_stamp,
                after_replace_under_lock=refresh_aliases,
            ):
                raise RuntimeError("daily bundle changed during repair; retry from fresh evidence")
    except RuntimeError:
        logger.warning("daily_fields_repair_refused service_id=%s day=%s", service_id, day, exc_info=True)
        raise
    except (duckdb.Error, OSError) as exc:
        logger.warning("daily_fields_repair_failed service_id=%s day=%s", service_id, day, exc_info=True)
        raise RuntimeError("daily fields repair failed; existing day preserved") from exc
    logger.info(
        "daily_fields_repair_complete service_id=%s day=%s snapshot_id=%d raw_requests=%d fields=%s",
        service_id,
        day,
        snapshot,
        result["raw_requests"],
        fields,
    )
    return result


def rebuild_country_day(
    service_id: str,
    source: dict[str, Any],
    day: str,
    *,
    expected_requests: int,
    expected_countries: dict[str, int],
) -> CountryDayRepairResult:
    """Keep the original country-only contract on the shared daily repair engine."""
    if not expected_countries:
        raise ValueError("country repair requires a retained raw oracle")
    result = rebuild_daily_fields(
        service_id, source, day, expected_requests=expected_requests, expected_fields={"country": expected_countries}
    )
    return {
        "day": result["day"],
        "snapshot_id": result["snapshot_id"],
        "raw_requests": result["raw_requests"],
        "hourly_requests": result["hourly_requests"],
        "country_counts": result["field_counts"]["country"],
        "raw_first_event": result["raw_first_event"],
        "raw_last_event": result["raw_last_event"],
    }
