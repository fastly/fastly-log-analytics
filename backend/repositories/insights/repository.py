"""Insights repository — anomaly detection queries, no HTTP imports."""

from __future__ import annotations

import threading
import time
import urllib.parse
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.repositories._base import QueryRunner, _safe_table

from .registry import registry

# ── Caches ────────────────────────────────────────────────────────────────────

INSIGHTS_CACHE_TTL = 300  # seconds
# Bounded + lazy-reaped. Pre-migration this was a plain dict; entries
# were time-bucketed by ``int(time.time() / TTL)`` so each TTL window
# minted distinct keys but old buckets were never removed. Across hours
# of admin use the bucket-count grew linearly. 500 entries × insights
# payload (~100KB) caps this around ~50MB.
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_insights_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=500, ttl_seconds=INSIGHTS_CACHE_TTL)
_insights_cache_lock = threading.Lock()


def get_insights(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    window_hours: float,
    baseline_hours: float,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)

    now = datetime.now(UTC)
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)

    now_s = now.isoformat()
    window_start_s = window_start.isoformat()
    baseline_start_s = baseline_start.isoformat()

    cache_bucket = int(time.time() / max(INSIGHTS_CACHE_TTL, 1))
    cache_key = f"{source_name}:{window_hours}:{baseline_hours}:{cache_bucket}"
    if INSIGHTS_CACHE_TTL > 0:
        with _insights_cache_lock:
            entry = _insights_cache.get(cache_key)
        if entry is not None:
            cached = entry[1].copy()
            cached["_is_cached"] = True
            return cached

    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

    empty_resp = {
        "insights": [],
        "window_start": window_start_s,
        "window_end": now_s,
        "baseline_start": baseline_start_s,
        "baseline_end": window_start_s,
        "computed_at": now_s,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "window_total_requests": 0,
        **runner.telemetry(),
    }
    if not actual_cols:
        return empty_resp

    # ── Materialize relevant window into temp table ───────────────────────────
    # This is the single most important optimization: avoid globbing/metadata parsing 30+ times.
    temp_table = f"insights_temp_{int(time.time())}"

    # Derive needed_cols from every registered insight's `required_fields` so
    # we never project a temp table that's missing a column some insight's SQL
    # references. (Previously a hard-coded list silently dropped columns like
    # `metro` / `tls_ciphers_sha` / `oretries` / `conn_requests` — the matching
    # insights then 500'd with "Referenced column not found in FROM clause".)
    needed_cols_set: set[str] = {"timestamp"}
    for d in registry.get_all():
        needed_cols_set.update(d.required_fields)
    # Also include support cols that processors read from context but aren't in
    # required_fields (e.g. ja3/ja4 fingerprint selection in botnet_grouping).
    # Geo columns are referenced via build_geo_select_clause when present
    # in the source schema, even though no insight lists `region` directly
    # in its required_fields.
    needed_cols_set.update({"ja3", "ja4", "region"})
    needed_cols = sorted(needed_cols_set)
    cols_sql = ", ".join(f'"{c}"' for c in needed_cols if c in actual_cols)
    if not cols_sql:
        cols_sql = "*"

    create_q = f"CREATE TEMP TABLE {temp_table} AS SELECT {cols_sql} FROM {table_name} WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)"
    if not runner.create_temp_table(create_q, [baseline_start_s, now_s]):
        temp_table = table_name  # Fallback

    # Available history
    try:
        earliest_ts = runner.execute(f"SELECT min(timestamp) FROM {temp_table}").fetchone()[0]
        if earliest_ts:
            if isinstance(earliest_ts, str):
                from backend.utils.date_utils import parse_iso_utc

                earliest_ts = parse_iso_utc(earliest_ts) or earliest_ts
            elif hasattr(earliest_ts, "tzinfo"):
                # DuckDB returns TIMESTAMPTZ in the *server's* local zone, not UTC.
                # `.replace(tzinfo=UTC)` would re-label the local wall clock as
                # UTC and shift the instant by the local offset — turning a
                # 23h-old row into a 29h-old one on MDT, or vice versa. Use
                # astimezone so the instant is preserved across zones (and
                # handle the rare naive case by assuming UTC).
                if earliest_ts.tzinfo is None:
                    earliest_ts = earliest_ts.replace(tzinfo=UTC)
                else:
                    earliest_ts = earliest_ts.astimezone(UTC)
            available_history_hours = (now - earliest_ts).total_seconds() / 3600.0
        else:
            available_history_hours = 0.0
    except Exception:
        available_history_hours = 0.0

    # Insight definitions
    try:
        from backend.core.log_fields import INSIGHT_DEFINITIONS as _defs

        defs_map = {d["id"]: d for d in _defs}
    except Exception:
        defs_map = {}

    def _def(insight_id: str) -> dict:
        return defs_map.get(insight_id, {})

    def check_baseline(insight_id: str) -> dict | None:
        if available_history_hours < baseline_hours:
            d = _def(insight_id)
            avail = max(0.1, round(available_history_hours, 1))
            return {
                "id": insight_id,
                "title": d.get("title", insight_id.replace("_", " ").title()),
                "description": d.get("description", ""),
                "severity": "info",
                "summary": f"Requires {int(baseline_hours)}h of historical data (only {avail}h available)",
                "items": [],
            }
        return None

    try:
        w_total = runner.execute(
            f"SELECT count(*) FROM {temp_table} WHERE timestamp >= CAST(? AS TIMESTAMPTZ)", [window_start_s]
        ).fetchone()[0]
    except Exception:
        w_total = 0

    table_name = temp_table

    def make_investigate_url(filters: dict | None = None) -> str:
        p = [("start", window_start_s), ("end", now_s)]
        for col, val in (filters or {}).items():
            if val is not None:
                p.append((f"filter_{col}", str(val)))
        return "/dashboard?" + urllib.parse.urlencode(p)

    def _sev(items: list, crit_key: bool = False) -> str:
        if not items:
            return "clean"
        if crit_key and any(i.get("severity") == "critical" for i in items):
            return "critical"
        return "warning"

    tasks: list[Callable[[], dict | None]] = []

    # ── Registered Dynamic Insights ───────────────────────────────────────────
    from backend.repositories.utils.filters import build_geo_select_clause

    loc_cols, label_expr, country_sel, region_sel = build_geo_select_clause(actual_cols)
    # Bare expression (no leading comma, no trailing alias) so templates can
    # write ``, {ua_mobile_sel} AS mobile_ratio`` consistently. Returning
    # ``, ... AS mobile_ratio`` here produced ``avg_kb, , ... AS mobile_ratio
    # AS mobile_ratio`` — a syntax error around the double comma.
    ua_mobile_sel = "0"
    if "ua" in actual_cols:
        ua_mobile_sel = "SUM(CASE WHEN \"ua\" ILIKE '%Mobi%' OR \"ua\" ILIKE '%Android%' OR \"ua\" ILIKE '%iPhone%' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)"
    url_col = '"url"' if "url" in actual_cols else "NULL"
    q_col = '"url"' if "url" in actual_cols else ('"digest"' if "digest" in actual_cols else "'(unknown)'")

    for definition in registry.get_all():
        # Check if all required fields are present
        if not all(col in actual_cols for col in definition.required_fields):
            continue

        def _make_task(d=definition):
            def compute_insight() -> dict | None:
                # Hydrate template
                fp_col = "ja4" if "ja4" in actual_cols else "ja3"

                # Special hydration for specific insights
                extra_args = {}
                if d.id == "impossible_distance":
                    from backend.utils.pop_utils import get_pop_lat_lon_map

                    pop_map = get_pop_lat_lon_map()
                    if not pop_map:
                        return None
                    extra_args["pop_values"] = ", ".join(
                        f"('{code}', {float(lat)}::DOUBLE, {float(lon)}::DOUBLE)"
                        for code, (lat, lon) in pop_map.items()
                        if lat is not None and lon is not None
                    )
                    extra_args["edge_filter"] = 'AND t."edge" = true' if "edge" in actual_cols else ""

                if d.id != "impossible_distance":
                    r = check_baseline(d.id)
                    if r:
                        return r

                try:
                    sql = d.sql_template.format(
                        table_name=table_name,
                        window_hours=window_hours,
                        baseline_hours=baseline_hours,
                        fp_col=fp_col,
                        loc_cols=loc_cols,
                        label_expr=label_expr,
                        country_sel=country_sel,
                        region_sel=region_sel,
                        ua_mobile_sel=ua_mobile_sel,
                        url_col=url_col,
                        q_col=q_col,
                        **extra_args,
                    )
                except KeyError:
                    # If hydration fails due to missing keys (e.g. pop_values), skip this insight
                    return None

                param_count = sql.count("?")
                params = [window_start_s] * param_count

                rows = runner.execute(sql, params).fetchall()
                items = []
                if d.row_processor:
                    # Build context for processors
                    context = {
                        "window_hours": window_hours,
                        "baseline_hours": baseline_hours,
                        "fp_col": fp_col,
                        "actual_cols": actual_cols,
                    }

                    # Lazy load maps if needed by processors
                    if any(p in d.id for p in ["asn", "metro"]):
                        from backend.core import duckdb as _db_core

                        context["asn_names"] = _db_core.get_asn_names(src["name"], [r[0] for r in rows if r])
                        if "metro" in actual_cols:
                            context["dma_map"] = _db_core._get_dma_map()

                    for row in rows:
                        try:
                            item = d.row_processor(row, d, context)
                            if "investigate_url" not in item:
                                filters = item.get("meta", {}).get("filters", {})
                                item["investigate_url"] = make_investigate_url(filters)
                            items.append(item)
                        except Exception:
                            continue

                severity = "clean"
                if items:
                    if d.severity_logic:
                        severity = d.severity_logic(items)
                    else:
                        severity = _sev(items, crit_key=True)

                summary = ""
                if items:
                    if d.id == "error_spikes":
                        summary = f"{len(items)} URLs with elevated server error rates"
                    elif d.id == "botnet_grouping":
                        summary = f"{len(items)} fingerprints with suspicious IP spread"
                    else:
                        summary = f"{len(items)} anomalies detected"
                else:
                    summary = f"No {d.title.lower()} detected"

                return {
                    "id": d.id,
                    "title": d.title,
                    "description": d.description,
                    "severity": severity,
                    "summary": summary,
                    "items": items,
                }

            # Tag the closure with the insight id+title so the error-path
            # below can report which insight failed. Without these, every
            # task closes over the same `compute_insight` name and the
            # error path emits duplicate `id="insight"` entries — which
            # React then warns about as duplicate keys.
            compute_insight._insight_id = d.id  # type: ignore[attr-defined]
            compute_insight._insight_title = d.title  # type: ignore[attr-defined]
            return compute_insight

        tasks.append(_make_task())

    insights_list: list[dict] = []
    for fn in tasks:
        try:
            res = fn()
            if res:
                insights_list.append(res)
        except Exception as e:
            insight_id = getattr(fn, "_insight_id", "unknown")
            insight_title = getattr(fn, "_insight_title", insight_id.replace("_", " ").title())
            insights_list.append(
                {
                    "id": insight_id,
                    "title": insight_title,
                    "severity": "error",
                    "summary": f"Query failed: {str(e)}",
                    "description": "",
                    "items": [],
                }
            )

    payload: dict[str, Any] = {
        "insights": insights_list,
        "window_start": window_start_s,
        "window_end": now_s,
        "baseline_start": baseline_start_s,
        "baseline_end": window_start_s,
        "computed_at": now_s,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "window_total_requests": w_total,
        **runner.telemetry(),
    }
    if INSIGHTS_CACHE_TTL > 0:
        with _insights_cache_lock:
            _insights_cache[cache_key] = (now_s, payload)
    return payload
