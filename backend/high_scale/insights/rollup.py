"""Hourly per-batch rollup of request_facts that lets Insights baselines skip raw fact scans.

``request_insight_hourly`` holds one row per (hour, shape, batch, publication state, key values)
with an exact request count. Each shape is the set of request fields some detections group or
filter on. Rows keep ``batch_id`` and ``publication_state`` so readers apply the same publication
fence as ``request_facts``: a rollup row is visible exactly when its source batch is.

A detection reads the rollup only for baseline hours whose per-hour row count and batch checksum
match ``request_facts`` (see :func:`covered_span`); every other hour, and the whole window, is
read raw, so results stay identical to the raw-only query.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta

from backend.high_scale._common import visible_fact_clauses

from .sql_queries import COLUMNS, Detection

ROLLUP_TABLE = "request_insight_hourly"

# Every rollup key column, in table order. ``url`` and ``country`` are request_facts columns;
# the rest are ``custom_fields`` keys stored as ``cf_<key>``.
ROLLUP_FIELDS: tuple[str, ...] = (
    "url",
    "country",
    "status",
    "cache",
    "transport",
    "ost",
    "p_type",
    "method",
    "c_type",
    "c_speed",
    "city",
    "region",
    "asn",
    "ua",
    "referer",
    "waf_sig",
)

ROLLUP_SHAPES: dict[str, tuple[str, ...]] = {
    "url_status": ("url", "status"),
    "url_cache": ("url", "cache"),
    "misc": ("cache", "transport", "ost", "p_type", "method", "c_type", "c_speed"),
    "geo": ("country", "city", "region", "status"),
    "asn": ("asn", "p_type"),
    "ua": ("ua",),
    "referer": ("referer",),
    "waf": ("waf_sig",),
}

# Detections whose baseline only counts requests per key, so summing hourly counts is exact.
ROLLUP_DETECTIONS: dict[str, str] = {
    "error_spikes": "url_status",
    "new_probe_urls": "url_status",
    "cache_collapse": "url_cache",
    "cacheability_regression": "url_cache",
    "cache_hit_cliff": "misc",
    "http3_fallback": "misc",
    "origin_error_rate": "misc",
    "proxy_surge": "misc",
    "method_drift": "misc",
    "connection_type_mix": "misc",
    "new_country_traffic": "geo",
    "city_surges": "geo",
    "city_error_spikes": "geo",
    "new_city_traffic": "geo",
    "asn_concentration": "asn",
    "new_asn_traffic": "asn",
    "asn_hosting_shift": "asn",
    "ua_monoculture": "ua",
    "referer_monoculture": "referer",
    "waf_signal_spikes": "waf",
}

_FACT_COLUMNS = frozenset({"url", "country"})
_CF_KEY = re.compile(r"custom_fields\['([a-z0-9_]+)'\]")
_NON_ROLLUP_SOURCES = re.compile(r"\b(custom_fields|event_timestamp|client_ip|cmcd|event_id|ingest_timestamp)\b")
# Aggregates the rollup cannot reproduce: anything that is not a plain request count. ``avgIf`` is
# allowed only over window rows, which are always read raw.
_UNSAFE_AGGREGATE = re.compile(
    r"\b(uniq\w*|quantile\w*|median\w*|avg\w*|min\w*|max\w*|any\w*|group\w*|arg\w*|stddev\w*|var\w*|top\w*)\s*\("
)
_WINDOW_AVG = re.compile(r"\bavgIf\((?:[^()]|\([^()]*\))*, is_w\)")
# A sum is a request count only when it sums the weight ``n`` or an inner request count.
_NON_WEIGHT_SUM = re.compile(r"\bsum(?:If)?\((?!n[,)]|sum)")


def rollup_column(field: str) -> str:
    return field if field in _FACT_COLUMNS else f"cf_{field}"


def _source_expr(field: str) -> str:
    if field == "url":
        return "url"
    if field == "country":
        return "CAST(country AS String)"
    return f"custom_fields['{field}']"


def rollup_select_sql(where: str = "") -> str:
    """The SELECT feeding ``request_insight_hourly`` (materialized view body and backfill source)."""
    tuples = ", ".join(
        "(" + ", ".join([f"'{shape}'"] + [_source_expr(f) if f in fields else "''" for f in ROLLUP_FIELDS]) + ")"
        for shape, fields in ROLLUP_SHAPES.items()
    )
    keys = ", ".join(f"k.{i + 2} AS {rollup_column(f)}" for i, f in enumerate(ROLLUP_FIELDS))
    group = ", ".join(rollup_column(f) for f in ROLLUP_FIELDS)
    where_clause = f" WHERE {where}" if where else ""
    return (
        f"SELECT service_id, bucket_start, k.1 AS shape, batch_id, publication_state, {keys}, "
        "count() AS request_count "
        "FROM (SELECT service_id, toStartOfHour(event_timestamp) AS bucket_start, batch_id, publication_state, "
        f"[{tuples}] AS keys FROM request_facts{where_clause}) "
        f"ARRAY JOIN keys AS k GROUP BY service_id, bucket_start, shape, batch_id, publication_state, {group}"
    )


ROLLUP_INSERT_COLUMNS = ", ".join(
    ["service_id", "bucket_start", "shape", "batch_id", "publication_state"]
    + [rollup_column(f) for f in ROLLUP_FIELDS]
    + ["request_count"]
)


def _to_rollup(expr: str) -> str:
    return _CF_KEY.sub(lambda m: f"cf_{m.group(1)}", expr)


def fields_read(detection: Detection) -> set[str]:
    """Request fields a detection's base projection and filters read."""
    fields: set[str] = set()
    for column in detection.columns:
        fields.update(_CF_KEY.findall(COLUMNS[column]))
        if column in _FACT_COLUMNS:
            fields.add(column)
    for text in (detection.row_filter, detection.prewhere):
        fields.update(_CF_KEY.findall(text))
        fields.update(name for name in _FACT_COLUMNS if re.search(rf"\b{name}\b", text))
    return fields


def count_to_sum(sql: str) -> str:
    """Rewrite request counts as sums of the per-row weight ``n`` (1 for raw rows, the hourly count otherwise)."""
    return sql.replace("countIf(", "sumIf(n, ").replace("count()", "sum(n)")


def rollup_problems(detection: Detection, shape: str) -> list[str]:
    """Why ``detection`` cannot read ``shape`` exactly; empty when it can."""
    problems: list[str] = []
    if detection.window_only or not detection.needs_baseline:
        problems.append("reads no baseline")
    missing = fields_read(detection) - set(ROLLUP_SHAPES[shape])
    if missing:
        problems.append(f"fields {sorted(missing)} are not in shape {shape}")
    for text in [COLUMNS[c] for c in detection.columns] + [detection.row_filter, detection.prewhere]:
        if _NON_ROLLUP_SOURCES.search(_to_rollup(text)):
            problems.append(f"reads a raw-only column: {text}")
    body = _WINDOW_AVG.sub("", count_to_sum(detection.sql))
    for pattern in (re.compile(r"\bcount\w*\s*\("), _UNSAFE_AGGREGATE, _NON_WEIGHT_SUM):
        if bad := pattern.search(body):
            problems.append(f"aggregate is not a request count: {bad.group(0)}")
    return problems


def rollup_base_sql(detection: Detection, insight_id: str, shape: str) -> str:
    """Like ``_base_sql`` but baseline hours in [rollup_start, rollup_end) come from the rollup."""
    if problems := rollup_problems(detection, shape):
        raise ValueError(f"{insight_id} cannot use the {shape} rollup: {'; '.join(problems)}")
    raw_projection = ", ".join(f"{COLUMNS[c]} AS {c}" for c in detection.columns)
    rollup_projection = ", ".join(f"{_to_rollup(COLUMNS[c])} AS {c}" for c in detection.columns)
    row_filter = f" AND ({detection.row_filter})" if detection.row_filter else ""
    raw_prewhere = f" PREWHERE {detection.prewhere}" if detection.prewhere else ""
    rollup_prewhere = f" PREWHERE {_to_rollup(detection.prewhere)}" if detection.prewhere else ""
    scope = " AND ".join(visible_fact_clauses("request"))
    raw = (
        f"SELECT {raw_projection}, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
        "event_timestamp >= {window_start:DateTime64(3)} AS is_w, toUInt64(1) AS n "
        f"FROM request_facts{raw_prewhere} WHERE {scope} AND event_timestamp >= {{baseline_start:DateTime64(3)}} "
        f"AND event_timestamp <= {{window_end:DateTime64(3)}}{row_filter} "
        "AND (event_timestamp < {rollup_start:DateTime64(3)} OR event_timestamp >= {rollup_end:DateTime64(3)})"
    )
    rollup = (
        f"SELECT {rollup_projection}, toUInt8(1) AS is_b, toUInt8(0) AS is_w, request_count AS n "
        f"FROM {ROLLUP_TABLE}{rollup_prewhere} WHERE {scope} AND shape = '{shape}' "
        "AND bucket_start >= {rollup_start:DateTime64(3)} AND bucket_start < {rollup_end:DateTime64(3)}"
        f"{_to_rollup(row_filter)}"
    )
    return f"/* insight:{insight_id} */ WITH base AS ({raw} UNION ALL {rollup}) {count_to_sum(detection.sql)}"


def coverage_sql() -> str:
    """Per shape and hour: visible rollup request count and batch checksum (matches the history query's)."""
    scope = " AND ".join(visible_fact_clauses("request"))
    return (
        "/* insight:rollup_coverage */ SELECT shape, toUInt32(toUnixTimestamp(bucket_start)) AS hour, "
        "sum(request_count) AS row_count, sum(cityHash64(batch_id) * request_count) AS batch_checksum "
        f"FROM {ROLLUP_TABLE} WHERE {scope} "
        "AND bucket_start >= {coverage_start:DateTime64(3)} AND bucket_start < {coverage_end:DateTime64(3)} "
        "GROUP BY shape, hour"
    )


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def baseline_hours_span(baseline_start: datetime, window_start: datetime) -> tuple[datetime, datetime]:
    """Whole hours inside [baseline_start, window_start): the only hours the rollup may replace."""
    start, end = _utc(baseline_start), _utc(window_start)
    floor_start = start.replace(minute=0, second=0, microsecond=0)
    first = floor_start if floor_start == start else floor_start + timedelta(hours=1)
    return first, end.replace(minute=0, second=0, microsecond=0)


HourTotals = Mapping[int, tuple[int, int]]


def hour_totals(rows: Iterable[tuple[object, object, object]]) -> dict[int, tuple[int, int]]:
    return {int(str(hour)): (int(str(count)), int(str(checksum))) for hour, count, checksum in rows}


def covered_span(
    baseline_start: datetime, window_start: datetime, raw: HourTotals, rollup: HourTotals
) -> tuple[datetime, datetime] | None:
    """Longest run of whole baseline hours whose rollup totals equal the raw totals, if it holds any rows."""
    first, end = baseline_hours_span(baseline_start, window_start)
    best: tuple[int, int, int] | None = None
    run_start: int | None = None
    run_rows = 0
    hour = int(first.timestamp())
    stop = int(end.timestamp())
    while hour <= stop:
        totals = raw.get(hour, (0, 0))
        if hour < stop and totals == rollup.get(hour, (0, 0)):
            if run_start is None:
                run_start, run_rows = hour, 0
            run_rows += totals[0]
        else:
            if run_start is not None and run_rows and (best is None or hour - run_start > best[1] - best[0]):
                best = (run_start, hour, run_rows)
            run_start = None
        hour += 3600
    if best is None:
        return None
    return datetime.fromtimestamp(best[0], UTC), datetime.fromtimestamp(best[1], UTC)
