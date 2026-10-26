from __future__ import annotations

import contextvars
import hashlib
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._common import format_iso_utc_or_empty as _iso
from backend.high_scale._common import (
    parse_time_window,
    range_value,
    visible_fact_clauses,
    visible_publication_fence_sql,
)
from backend.high_scale.archive_models import ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.publication import (
    _DOMAIN_COLUMNS,
    _DOMAIN_TABLES,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    PartialInsert,
    _batch_uuid,
    _rows_for_domain,
    row_matches_serving_domain,
)
from backend.high_scale.query_service import decode_source_object
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.replay import _read_manifest_rows
from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key

# Columns present on BOTH rum fact tables; a filter on anything else cannot apply to every beacon kind.
_FILTER_COLUMNS = frozenset({"browser", "os", "device", "country", "pathname", "city", "region", "pop", "tls"})
_BEACON_IDENTITY = """
        coalesce(nullIf(toString(request_event_id), ''),
                 concat(client_id, '_', toString(toInt64(round(toUnixTimestamp64Milli(event_timestamp) / 1000.0)))))
    """


def parse_rum_filters(raw: str | None) -> dict[str, Any]:
    """Decode the ``filters`` query parameter (a JSON object of ``{column: {mode, values}}``)."""
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("filters must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("filters must be a JSON object")
    return payload


def _filter_clauses(filters: Mapping[str, Any] | None) -> tuple[list[str], dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    unsupported: list[str] = []
    for i, (raw_key, spec) in enumerate((filters or {}).items()):
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if value is not None and not (isinstance(value, str) and value.strip() == "")
        ]
        if not values:
            continue
        column = normalize_filter_key(raw_key)
        if column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        names: list[str] = []
        for j, value in enumerate(values):
            name = f"flt_{i}_{j}"
            params[name] = str(value)
            names.append(f"{{{name}:String}}")
        operator = "NOT IN" if str(filter_spec_attr(spec, "mode") or "include") == "exclude" else "IN"
        clauses.append(f"{column} {operator} ({', '.join(names)})")
    if unsupported:
        raise ValueError(f"high-scale RUM cannot filter on: {', '.join(sorted(unsupported))}")
    return clauses, params


def rum_beacon_health(
    service: HighScaleService,
    *,
    source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(source, dict) and (
        (isinstance(source.get("rum"), dict) and source["rum"].get("enabled") is False)
        or source.get("rum_enabled") is False
    ):
        return {
            "enabled": False,
            "has_data": False,
            "beacons": 0,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": "RUM is not enabled for this service.",
        }

    try:
        res = service.client.execute(
            f"SELECT sum(event_count) as c, max(bucket_start) as last_ts FROM rum_vitals_aggregates WHERE service_id = {{service_id:String}} AND dimension = 'metric_name' AND {visible_publication_fence_sql('rum_vitals_aggregate')}",
            {"service_id": service.service_id},
        )
        row = res[0] if res else {}
        beacons = int(row.get("c") or 0)
        last_ts = row.get("last_ts") if beacons > 0 else None
    except Exception:
        res = service.client.execute(
            f"SELECT count() as c, max(event_timestamp) as last_ts FROM rum_vitals_facts WHERE service_id = {{service_id:String}} AND {visible_publication_fence_sql('rum_vitals')}",
            {"service_id": service.service_id},
        )
        row = res[0] if res else {}
        beacons = int(row.get("c") or 0)
        last_ts = row.get("last_ts") if beacons > 0 else None
    has_data = beacons > 0
    last_beacon_time = (_iso(last_ts) or None) if last_ts is not None else None
    return {
        "enabled": True,
        "has_data": has_data,
        "beacons": beacons,
        "beacon_fire_rate": beacons,
        "recent_beacons": beacons,
        "last_beacon_time": last_beacon_time,
        "setup_complete": has_data,
        "message": (
            f"RUM is working! Received {beacons} beacon(s) in the last 15 minutes."
            if has_data
            else "No beacons received in the last 15 minutes. Ensure the RUM script is installed and your site is receiving traffic."
        ),
    }


def rum_analytics(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Serve the standard RUM wire contract from visible, time-filtered facts.

    Counts use beacon identity, not metric-row count. All SQL goes through the
    instrumented client; dependency failures must not become empty chart data.
    """
    start, end = parse_time_window(start_time, end_time, utc=True, now=datetime.now(UTC))
    if end < start:
        raise ValueError("RUM end time precedes start time")
    filter_clauses, filter_params = _filter_clauses(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}

    def scope(domain: str) -> str:
        return " AND ".join(
            [
                *visible_fact_clauses(domain),
                "event_timestamp >= {start:DateTime64(3, 'UTC')}",
                "event_timestamp <= {end:DateTime64(3, 'UTC')}",
                *filter_clauses,
            ]
        )

    where_vitals = scope("rum_vitals")
    where_errors = scope("rum_errors")
    # Same fallback as the standard raw producer's req_id / cid + epoch-second
    # identity. Do not count separate LCP/CLS rows as separate pageviews.
    identity = _BEACON_IDENTITY
    events = f"""
        WITH events AS (
            SELECT event_timestamp, {identity} AS beacon_id, 'vitals' AS src,
                   metric_name, metric_value, pathname
            FROM rum_vitals_facts WHERE {where_vitals}
            UNION ALL
            SELECT event_timestamp, {identity} AS beacon_id, 'errors' AS src,
                   '' AS metric_name, CAST(NULL AS Nullable(Float64)) AS metric_value, pathname
            FROM rum_error_facts WHERE {where_errors}
        )
    """
    counts = service.client.execute(
        events
        + """
        /* rum_analytics_counts */
        SELECT uniqExact(beacon_id) AS beacons,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name NOT LIKE 'event_%') AS pageviews,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name LIKE 'event_%') AS interactions,
               uniqExactIf(beacon_id, src = 'errors') AS errors
        FROM events
        """,
        params,
    )[0]
    no_data = False
    if not counts["beacons"]:
        # Distinguish an empty selected range from a never-configured service.
        any_vitals = " AND ".join(visible_fact_clauses("rum_vitals"))
        any_errors = " AND ".join(visible_fact_clauses("rum_errors"))
        any_data = service.client.execute(
            f"""
            /* rum_analytics_any_data */
            SELECT count() AS has_data FROM (
                (SELECT event_id FROM rum_vitals_facts WHERE {any_vitals} LIMIT 1)
                UNION ALL
                (SELECT event_id FROM rum_error_facts WHERE {any_errors} LIMIT 1)
            )
            """,
            {"service_id": service.service_id},
        )
        no_data = not any_data[0]["has_data"]

    hourly = end - start <= timedelta(hours=48)
    # Fixed internal expression only; user values remain typed parameters.
    bucket_sql = "toStartOfHour(event_timestamp, 'UTC')" if hourly else "toStartOfDay(event_timestamp, 'UTC')"

    vitals_sql = f"""
        /* rum_analytics_vitals */
        SELECT lower(metric_name) AS metric_name,
               quantileExactInclusiveOrNull(0.75)(metric_value) as p75,
               count() as total,
               countIf(metric_rating = 'good') as good,
               countIf(metric_rating IN ('needs-improvement', 'needs_improvement')) as ni,
               countIf(metric_rating = 'poor') as poor
        FROM rum_vitals_facts
        WHERE {where_vitals}
        GROUP BY metric_name
        """
    environments_sql = f"""
        /* rum_analytics_environments */
        SELECT if(browser = '', 'Unknown', browser) AS env_browser,
               if(os = '', 'Unknown', os) AS env_os,
               if(device = '', 'Unknown', device) AS env_device,
               uniqExact({identity}) AS beacons,
               grouping(browser) AS g_browser, grouping(os) AS g_os, grouping(device) AS g_device
        FROM rum_vitals_facts
        WHERE {where_vitals}
        GROUP BY GROUPING SETS ((browser), (os), (device))
        """
    trends_sql = (
        events
        + f"""
        /* rum_analytics_trends */
        SELECT {bucket_sql} AS bucket,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls,
               uniqExactIf(beacon_id, src = 'vitals') AS views,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name NOT LIKE 'event_%') AS pageviews,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name LIKE 'event_%') AS interactions,
               uniqExactIf(beacon_id, src = 'errors') AS errors
        FROM events GROUP BY bucket ORDER BY bucket
        """
    )
    pages_sql = (
        events
        + """
        /* rum_analytics_pages */
        SELECT pathname AS path,
               uniqExactIf(beacon_id, src = 'vitals') AS views,
               avgOrNullIf(metric_value, metric_name IN
                   ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')) AS avg_load,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls,
               uniqExactIf(beacon_id, src = 'errors') * 100.0
                   / nullIf(views + uniqExactIf(beacon_id, src = 'errors'), 0) AS error_rate
        FROM events GROUP BY pathname HAVING views > 0
        ORDER BY error_rate DESC, avg_load DESC, path LIMIT 5
        """
    )
    exceptions_sql = f"""
        /* rum_analytics_exceptions */
        SELECT error_message, error_file, error_line, error_col, count() AS count
        FROM rum_error_facts WHERE {where_errors}
        GROUP BY error_message, error_file, error_line, error_col
        ORDER BY count DESC, error_message, error_file, error_line, error_col LIMIT 3
        """

    with ThreadPoolExecutor(max_workers=5) as pool:
        f_vitals = pool.submit(contextvars.copy_context().run, service.client.execute, vitals_sql, params)
        f_env = pool.submit(contextvars.copy_context().run, service.client.execute, environments_sql, params)
        f_trends = pool.submit(contextvars.copy_context().run, service.client.execute, trends_sql, params)
        f_pages = pool.submit(contextvars.copy_context().run, service.client.execute, pages_sql, params)
        f_errors = pool.submit(contextvars.copy_context().run, service.client.execute, exceptions_sql, params)
        rows = f_vitals.result()
        environment_rows = f_env.result()
        trend_rows = f_trends.result()
        page_rows = f_pages.result()
        error_rows = f_errors.result()

    browsers: dict[str, int] = {}
    operating_systems: dict[str, int] = {}
    devices: dict[str, int] = {}
    for row in environment_rows:
        if not row["g_browser"]:
            browsers[row["env_browser"]] = int(row["beacons"])
        elif not row["g_os"]:
            operating_systems[row["env_os"]] = int(row["beacons"])
        elif not row["g_device"]:
            devices[row["env_device"]] = int(row["beacons"])

    vitals: dict[str, Any] = {
        "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "fid": {"p75": None, "fcp": None, "ttfb": None},
    }

    def seconds(value: float | None) -> float | None:
        return round(value / 1000.0 if value > 20 else value, 2) if value is not None else None

    for r in rows:
        m = r["metric_name"].lower()
        p75 = r["p75"]
        if m in ("lcp", "cls", "inp"):
            vitals[m]["p75"] = (
                (seconds(p75) if m == "lcp" else (round(p75, 3) if m == "cls" else int(p75)))
                if p75 is not None
                else None
            )
            t = r["total"]
            if t:
                good = int(r["good"] * 100 / t)
                poor = int(r["poor"] * 100 / t)
                vitals[m]["distribution"] = {"good": good, "poor": poor, "needs_improvement": max(0, 100 - good - poor)}
        elif m == "fid":
            vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
        elif m in ("fcp", "ttfb"):
            vitals["fid"][m] = seconds(p75)

    trend_by_bucket = {}
    for row in trend_rows:
        ts = row["bucket"]
        dt = ts if isinstance(ts, datetime) else range_value(ts)
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        trend_by_bucket[dt] = row

    trends: dict[str, list[Any]] = {
        "timestamps": [],
        "lcp": [],
        "cls": [],
        "error_rate": [],
        "pageviews": [],
        "interactions": [],
        "errors": [],
    }
    curr = start.replace(minute=0, second=0, microsecond=0)
    target = end.replace(minute=0, second=0, microsecond=0)
    step = timedelta(hours=1) if hourly else timedelta(days=1)
    if not hourly:
        curr = curr.replace(hour=0)
        target = target.replace(hour=0)
    while not no_data and curr <= target:
        bucket_row = trend_by_bucket.get(curr)
        trends["timestamps"].append(curr.isoformat())
        trends["lcp"].append(seconds(bucket_row["lcp"]) if bucket_row else None)
        trends["cls"].append(round(bucket_row["cls"], 3) if bucket_row and bucket_row["cls"] is not None else None)
        denominator = bucket_row["views"] + bucket_row["errors"] if bucket_row else 0
        trends["error_rate"].append(
            round(bucket_row["errors"] * 100.0 / denominator, 2) if bucket_row and denominator else None
        )
        for field in ("pageviews", "interactions", "errors"):
            trends[field].append(int(bucket_row[field]) if bucket_row else 0)
        curr += step

    worst_pages = [
        {
            "path": row["path"],
            "views": row["views"],
            "avg_load_time": seconds(row["avg_load"] or 0.0),
            "lcp_p75": seconds(row["lcp"]),
            "cls_p75": round(row["cls"], 3) if row["cls"] is not None else None,
            "error_rate": round(row["error_rate"], 2) if row["error_rate"] is not None else 0.0,
        }
        for row in page_rows
    ]
    result = {
        "is_mock": False,
        "no_data": no_data,
        "beacon_count": int(counts["beacons"]),
        "pageview_count": int(counts["pageviews"]),
        "interaction_count": int(counts["interactions"]),
        "error_count": int(counts["errors"]),
        "vitals": vitals,
        "worst_pages": worst_pages,
        "errors": [
            {
                "message": row["error_message"],
                "file": row["error_file"] or "unknown.js",
                "line": int(row.get("error_line") or 0),
                "col": int(row.get("error_col") or 0),
                "count": row["count"],
            }
            for row in error_rows
        ],
        "trends": trends,
        "environments": {"browsers": browsers, "os": operating_systems, "devices": devices},
    }
    if no_data:
        result["message"] = "Waiting for real-time RUM user events..."
        for metric in ("lcp", "cls", "inp"):
            vitals[metric]["distribution"] = None
    return result


def _ttfb_ms(value: Any) -> int | None:
    """Beacons report TTFB in seconds or milliseconds; the live feed always shows whole milliseconds."""
    if value is None:
        return None
    number = float(value)
    return int(number * 1000) if number < 15 else int(number)


def rum_live_events(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    limit: int,
    filters: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    start_dt, end_dt = parse_time_window(start_time, end_time, now=datetime.now(UTC))
    filter_clauses, filter_params = _filter_clauses(filters)
    window = ["event_timestamp >= {start_time:DateTime64(3)}", "event_timestamp <= {end_time:DateTime64(3)}"]

    def scope(domain: str) -> str:
        return " AND ".join([*visible_fact_clauses(domain), *window, *filter_clauses])

    # Push ORDER BY + LIMIT into each branch before sorting the union.
    query = f"""
    /* rum_live_events */
    SELECT * FROM (
    SELECT * FROM (
        SELECT
            event_timestamp AS timestamp,
            'pageview' AS type,
            pathname,
            metric_name,
            metric_value,
            metric_rating,
            browser, os, device,
            client_id AS cid,
            toString(request_event_id) AS req_id,
            '' AS error_message,
            city, region, country, pop, tls, ttfb
        FROM rum_vitals_facts
        WHERE {scope("rum_vitals")}
        ORDER BY event_timestamp DESC
        LIMIT {{limit:UInt32}}
    )

    UNION ALL

    SELECT * FROM (
        SELECT
            event_timestamp AS timestamp,
            'error' AS type,
            pathname,
            '' AS metric_name,
            CAST(NULL AS Nullable(Float64)) AS metric_value,
            '' AS metric_rating,
            browser, os, device,
            client_id AS cid,
            toString(request_event_id) AS req_id,
            error_message,
            city, region, country, pop, tls, ttfb
        FROM rum_error_facts
        WHERE {scope("rum_errors")}
        ORDER BY event_timestamp DESC
        LIMIT {{limit:UInt32}}
    )
    )
    ORDER BY timestamp DESC
    LIMIT {{limit:UInt32}}
    """
    res = service.client.execute(
        query,
        {
            "service_id": service.service_id,
            "start_time": start_dt,
            "end_time": end_dt,
            "limit": limit,
            **filter_params,
        },
    )

    events: list[dict[str, Any]] = []
    for d in res:
        etype = d.get("type", "pageview")
        mname = d.get("metric_name")
        mrating = d.get("metric_rating")
        mval = d.get("metric_value")
        emsg = d.get("error_message")
        path = d.get("pathname") or "/"
        browser = d.get("browser") or "Unknown"
        os_name = d.get("os") or "Unknown"
        meta = {
            "browser": {"name": browser},
            "os": {"name": os_name},
            "device": {"type": d.get("device") or "Unknown"},
            "page": {"url": path},
        }
        context = {
            "cid": d.get("cid"),
            "req_id": d.get("req_id"),
            "city": d.get("city") or "Unknown",
            "region": d.get("region") or "Unknown",
            "country": d.get("country") or "Unknown",
            "pop": d.get("pop") or "Unknown",
            "tls": d.get("tls") or "Unknown",
            "ttfb": _ttfb_ms(d.get("ttfb")),
        }
        if etype == "error":
            desc = emsg or "JS Error"
            raw_log: dict[str, Any] = {
                "meta": meta,
                "measurements": [],
                "exceptions": [{"type": "Error", "value": desc}],
                **context,
            }
        else:
            desc = "Page loaded successfully"
            if mname:
                desc = f"Metric {mname.upper()}: {float(mval)}" if mval is not None else f"Metric {mname.upper()}"
                if mrating:
                    desc += f" ({mrating.upper()})"
            raw_log = {
                "meta": meta,
                "measurements": [
                    {
                        "type": "web-vitals",
                        "values": {mname: mval} if mname else {},
                        "context": {"rating": mrating or ""},
                    }
                ]
                if mname
                else [],
                "exceptions": [],
                **context,
            }
        events.append(
            {
                "time": _iso(d.get("timestamp")),
                "type": etype,
                "path": path,
                "desc": desc,
                "browser": browser,
                "os": os_name,
                "raw_log": raw_log,
            }
        )
    return events


# ── Bounded RUM Recovery Planning & Execution (folded from rum_recovery.py) ───

_DOMAINS = ("rum_vitals", "rum_errors")
_IGNORED_CONTENT = {"event_id", "transform_version", "ingest_timestamp", "batch_id", "publication_state"}
_READ_LIMITS = {
    "max_rows_to_read": "100000",
    "read_overflow_mode": "throw",
    "max_result_rows": "1001",
    "result_overflow_mode": "throw",
}


class RumRecoveryBatchAdapter(ClickHouseBatchAdapter):
    """Recovery-only GLOBAL batch verification without one unbounded scan."""

    def _inventory(self, table: str) -> dict[str, int]:
        rows = self._client.execute(
            "SELECT partition_id,sum(rows) AS partition_rows FROM system.parts "
            "WHERE database=currentDatabase() AND table={table:String} AND active "
            "GROUP BY partition_id LIMIT 33",
            {"table": table},
            settings=_READ_LIMITS,
        )
        if len(rows) > 32:
            raise ValueError("RUM repair global partition inventory exceeds bound")
        result: dict[str, int] = {}
        for row in rows:
            key, weight = row["partition_id"], row["partition_rows"]
            if (
                not isinstance(key, str)
                or not key
                or key in result
                or not isinstance(weight, int)
                or isinstance(weight, bool)
                or not 0 <= weight <= 90_000
            ):
                raise ValueError("RUM repair partition cannot be proved under read cap")
            result[key] = weight
        return result

    def _count_rows(self, table: str, batch: HighScaleBatch, batch_uuid: str) -> int:
        if batch.domain not in _DOMAINS or table != _DOMAIN_TABLES[batch.domain] or not 0 < len(batch.rows) <= 1000:
            raise ValueError("RUM repair batch verification scope is invalid")
        inventory = self._inventory(table)
        groups: list[tuple[list[str], int]] = []
        for key, weight in sorted(inventory.items(), key=lambda item: (-item[1], item[0])):
            for index, (keys, total) in enumerate(groups):
                if total + weight <= 90_000:
                    keys.append(key)
                    groups[index] = (keys, total + weight)
                    break
            else:
                groups.append(([key], weight))
        if len(groups) > 6:
            raise ValueError("RUM repair global partition read groups exceed bound")
        expected = [
            dict(zip(_DOMAIN_COLUMNS[batch.domain], values, strict=True))
            for values in _rows_for_domain(batch, batch_uuid)
        ]
        expected_by_id = {str(row["event_id"]): row for row in expected}
        if len(expected_by_id) != len(expected):
            raise ValueError("RUM repair batch candidate identities are ambiguous")
        present: list[dict[str, Any]] = []
        for keys, _ in groups:
            placeholders = ",".join("{part" + str(index) + ":String}" for index in range(len(keys)))
            rows = self._client.execute(
                f"SELECT {','.join(_DOMAIN_COLUMNS[batch.domain])} FROM {table} "
                "WHERE service_id={service_id:String} AND batch_id={batch_id:UUID} "
                "AND _partition_id IN (" + placeholders + ") LIMIT 1001",
                {
                    "service_id": batch.service_id,
                    "batch_id": batch_uuid,
                    **{f"part{index}": key for index, key in enumerate(keys)},
                },
                settings=_READ_LIMITS,
            )
            present.extend(rows)
            if len(present) > 1000:
                raise ValueError("RUM repair global batch facts exceed row bound")
        if set(self._inventory(table)) != set(inventory):
            raise ValueError("RUM repair global partition inventory changed")
        seen: set[str] = set()
        for row in present:
            identity = str(row["event_id"])
            wanted = expected_by_id.get(identity)
            if (
                identity in seen
                or wanted is None
                or str(row["batch_id"]) != batch_uuid
                or _content(row, batch.domain) != _content(wanted, batch.domain)
                or row["transform_version"] != wanted["transform_version"]
                or row["publication_state"] != "visible"
            ):
                raise ValueError("RUM repair existing batch facts are duplicate, conflicting or out of scope")
            seen.add(identity)
        if len(present) not in (0, len(expected)):
            raise PartialInsert(len(present))
        return len(present)

    def inspect_batch(self, batch: HighScaleBatch) -> tuple[int, dict[str, Any] | None]:
        """Read-only preflight: exact global physical proof plus full-key receipt."""
        batch_uuid = _batch_uuid(batch)
        count = self._count_rows(_DOMAIN_TABLES[batch.domain], batch, batch_uuid)
        existing = super()._existing_publication(batch, batch_uuid)
        if existing is not None:
            self._validate_publication(existing, batch)
            if existing["publication_state"] == "visible" and count != len(batch.rows):
                raise PartialInsert(count)
        return count, existing

    def _existing_publication(self, batch: HighScaleBatch, batch_uuid: str) -> dict[str, Any] | None:
        return self.inspect_batch(batch)[1]


@dataclass(frozen=True)
class RumRecoveryPlan:
    plan_digest: str
    candidate_counts: dict[str, int]
    missing_counts: dict[str, int]
    exact_identity_counts: dict[str, int]
    equivalent_identity_counts: dict[str, int]
    batches: tuple[HighScaleBatch, ...]
    expected_facts: tuple[dict[str, Any], ...]
    source_count: int
    raw_line_count: int
    quarantined_lines: int

    def summary(self) -> dict[str, Any]:
        """Allowlisted operational evidence: no service/source keys or beacons."""
        return {
            "plan_digest": self.plan_digest,
            "candidate_counts": self.candidate_counts,
            "missing_counts": self.missing_counts,
            "exact_identity_counts": self.exact_identity_counts,
            "equivalent_identity_counts": self.equivalent_identity_counts,
            "batches": len(self.batches),
            "sources": self.source_count,
            "raw_lines": self.raw_line_count,
            "quarantined_lines": self.quarantined_lines,
        }


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds")
    return value


def _content(row: Mapping[str, Any], domain: str) -> str:
    values = {}
    for name in _DOMAIN_COLUMNS[domain]:
        if name in _IGNORED_CONTENT:
            continue
        value = row.get(name)
        if name == "event_timestamp" and isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        if name in {"metric_value", "ttfb"} and isinstance(value, (int, float)) and not isinstance(value, bool):
            value = float(value)
        values[name] = _canonical(value)
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def plan_rum_recovery(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    *,
    owner_epoch: int,
    read_facts: Callable[[str, tuple[str, ...]], Sequence[Mapping[str, Any]]],
    now: datetime | None = None,
    max_rows: int = 1000,
) -> RumRecoveryPlan:
    """Reconstruct each original source line once, preserving ordinals/version."""
    if manifest.domain not in _DOMAINS or owner_epoch < 1 or not 0 < max_rows <= 1000:
        raise ValueError("invalid bounded RUM repair scope")
    sources = manifest.sources
    if not sources or len(sources) > 100 or len({s.key for s in sources}) != len(sources):
        raise ValueError("RUM repair requires unique bounded manifest sources")
    rows = _read_manifest_rows(manifest, archive)
    if len(rows) > max_rows:
        raise ValueError("RUM repair archive exceeds row bound")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    candidates: dict[str, list[dict[str, Any]]] = {domain: [] for domain in _DOMAINS}
    raw_lines = 0
    quarantined: set[tuple[str, int]] = set()
    covered_offsets: set[int] = set()
    for source in sources:
        end = source.row_start + source.row_count
        if source.row_start < 0 or end > len(rows):
            raise ValueError("RUM repair source slice is invalid")
        offsets = set(range(source.row_start, end))
        if covered_offsets & offsets:
            raise ValueError("RUM repair source slices overlap")
        covered_offsets.update(offsets)
        lines: dict[int, str] = {}
        version = source.version or source.checksum
        for row in rows[source.row_start : end]:
            if row.get("_record_kind", "event") != "event":
                raise ValueError("RUM repair requires reviewed event-only source slices")
            ordinal = row.get("line_ordinal")
            raw = row.get("raw_json")
            if (
                row.get("service_id") != manifest.service_id
                or row.get("source_object_key") != source.key
                or row.get("source_object_version") != version
                or row.get("transform_version") != manifest.artifact.transform_version
                or not isinstance(ordinal, int)
                or not 0 <= ordinal < 10000
                or not isinstance(raw, str)
                or "\n" in raw
                or "\r" in raw
            ):
                raise ValueError("RUM repair source provenance is invalid")
            if ordinal in lines and lines[ordinal] != raw:
                raise ValueError("RUM repair source line is ambiguous")
            lines[ordinal] = raw
        raw_lines += len(lines)
        if not lines:
            continue
        payload = "\n".join(lines.get(i, "") for i in range(max(lines) + 1)).encode()
        descriptor = ArchiveSourceObject(
            manifest.service_id, manifest.domain, source.key, source.checksum, source.size_bytes, source.version
        )
        for domain in _DOMAINS:
            decoded = decode_source_object(
                descriptor, payload, domain=domain, transform_version=manifest.artifact.transform_version
            )
            quarantined.update((source.key, item.line_ordinal) for item in decoded.dead_letters)
            for event in decoded.events:
                if row_matches_serving_domain(domain, event):
                    candidates[domain].append(event)
    if covered_offsets != set(range(len(rows))):
        raise ValueError("RUM repair source slices do not cover archive")
    if quarantined:
        raise ValueError("RUM repair found quarantined source lines; review exact archive separately")
    if sum(map(len, candidates.values())) > max_rows:
        raise ValueError("RUM repair expansion exceeds row bound")

    expected: list[dict[str, Any]] = []
    batches: list[HighScaleBatch] = []
    missing_counts: dict[str, int] = {}
    exact_counts = {domain: 0 for domain in _DOMAINS}
    equivalent_counts = {domain: 0 for domain in _DOMAINS}
    for domain in _DOMAINS:
        events = candidates[domain]
        batch = HighScaleBatch("plan", manifest.service_id, domain, str(owner_epoch), tuple(events))
        mapped = _rows_for_domain(batch, "00000000-0000-0000-0000-000000000000")
        facts = [dict(zip(_DOMAIN_COLUMNS[domain], row, strict=True), _repair_domain=domain) for row in mapped]
        ids = [fact["event_id"] for fact in facts]
        if len(ids) != len(set(ids)):
            raise ValueError("RUM repair candidate identities are ambiguous")
        for fact in facts:
            timestamp = fact["event_timestamp"]
            if timestamp < observed - timedelta(days=30) or timestamp > observed:
                raise ValueError("RUM repair event is outside retained fact window")
        expected.extend(facts)
        present = [
            row
            for row in read_facts(domain, tuple(s.key for s in sources))
            if row.get("_repair_domain", domain) == domain
        ]
        if len(present) > max_rows:
            raise ValueError("RUM repair existing facts exceed row bound")
        present_ids = [str(row.get("event_id")) for row in present]
        if len(present_ids) != len(set(present_ids)):
            raise ValueError("RUM repair existing identity is ambiguous or unpublished")
        missing: list[dict[str, Any]] = []
        for event, fact in zip(events, facts, strict=True):
            matches = [
                row
                for row in present
                if str(row.get("event_id")) == fact["event_id"] or _content(row, domain) == _content(fact, domain)
            ]
            if not matches:
                missing.append(event)
            elif (
                len(matches) != 1
                or not matches[0].get("repair_visible")
                or _content(matches[0], domain) != _content(fact, domain)
            ):
                raise ValueError("RUM repair existing identity is ambiguous or unpublished")
            elif str(matches[0]["event_id"]) == fact["event_id"]:
                exact_counts[domain] += 1
            else:
                equivalent_counts[domain] += 1
        missing_counts[domain] = len(missing)
        if missing:
            token = hashlib.sha256("|".join(sorted(event["event_id"] for event in missing)).encode()).hexdigest()
            batches.append(
                HighScaleBatch(
                    f"rum-repair-v1:{manifest.manifest_id}:{domain}:{token}",
                    manifest.service_id,
                    domain,
                    str(owner_epoch),
                    tuple(missing),
                )
            )
    approval = {
        "version": "rum-repair-v1",
        "manifest": manifest.manifest_id,
        "artifact": manifest.artifact.checksum,
        "owner_epoch": owner_epoch,
        "facts": [
            {
                **fact,
                "event_timestamp": _canonical(fact["event_timestamp"]),
                "ingest_timestamp": _canonical(fact["ingest_timestamp"]),
            }
            for fact in expected
        ],
    }
    digest = hashlib.sha256(json.dumps(approval, sort_keys=True, default=str).encode()).hexdigest()
    return RumRecoveryPlan(
        digest,
        {domain: len(candidates[domain]) for domain in _DOMAINS},
        missing_counts,
        exact_counts,
        equivalent_counts,
        tuple(batches),
        tuple(expected),
        len(sources),
        raw_lines,
        len(quarantined),
    )


def execute_rum_recovery(
    make_plan: Callable[[], RumRecoveryPlan],
    publication: ClickHousePublication,
    *,
    apply: bool = False,
    approve_digest: str | None = None,
) -> dict[str, Any]:
    """Caller holds the terminal-source/owner fence for this entire operation."""
    plan = make_plan()
    report = {"applied": False, **plan.summary()}
    if not apply:
        return report
    if approve_digest != plan.plan_digest:
        raise ValueError("RUM repair requires the exact reviewed plan digest")
    for batch in plan.batches:
        publication.publish(batch)
    verified = make_plan()
    if verified.plan_digest != plan.plan_digest or any(verified.missing_counts.values()):
        raise ValueError("RUM repair post-apply identity verification failed")
    return {**report, "applied": True, "remaining_counts": verified.missing_counts}


_self_mod = sys.modules[__name__]
for _alias_name in ("rum_recovery", "rum_ingest"):
    sys.modules[f"backend.high_scale.{_alias_name}"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    for _alias_name in ("rum_recovery", "rum_ingest"):
        setattr(_hs_pkg, _alias_name, _self_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    for _alias_name in ("rum_recovery", "rum_ingest"):
        _backend_pkg._MODULE_ALIASES[f"backend.high_scale.{_alias_name}"] = __name__
