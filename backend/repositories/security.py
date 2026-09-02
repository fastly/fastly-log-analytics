"""Security repository — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

import logging
import os
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
    ensure_ngwaf_bots_materialized,
    safe_iso,
    time_bucket_select,
)
from backend.repositories._sql import security as SQL
from backend.repositories.utils.filters import build_where_clause

_logger = logging.getLogger(__name__)

# Rollup-served paths only beat the live-SQL-on-temp path on windows
# wide enough that the closed-hour parquet-read cost amortises better
# than the live column scan. Measured 2026-06-15 on prod: 24 h was a
# net regression (+110 ms), 7 d and 30 d were wins (−107 ms / −453 ms).
# Gate at 3 days as a conservative break-even — 24 h and shorter
# windows skip rollup routing and serve from the live SQL on the
# already-materialised catalog temp (which keeps is_ipv6 + p_type in
# its projection on the small-window path).
_ROLLUP_MIN_WINDOW_SECONDS = 3 * 86400

# The maximum number of top distinct User Agents (UAs) retrieved from the database
# (via rollups or live temp tables) to pass to the regex-based classifier.
# Lowering this from 50,000 to 2,000 reduces CPU-bound regex evaluation times
# by up to 25x, preventing Python GIL starvation and drastically speeding up
# the home dashboard bundle load.
_BOT_UA_CLASSIFY_LIMIT = int(os.getenv("BOT_UA_CLASSIFY_LIMIT", "2000"))

# Canonical projection order for the catalog temp. The temp materializes
# ONLY the columns the live (rollup-missed) sections actually touch — see
# the per-section column-needs in _build_security_response. asn / req_bytes
# / ja3 / ja4 are never consumed by any section so they never appear.
_TEMP_COL_ORDER = (
    "timestamp",
    "ip",
    "tls_ciphers_sha",
    "req_header_bytes",
    "is_ipv6",
    "p_type",
    "conn_requests",
    "waf_sig",
    "ua",
    "waf_req_id",
)

# (section name, is_list) — the empty default each section carries when it
# was requested but produced no value (e.g. the temp build failed). Lists
# for table/series sections; dict for fingerprint_coverage.
_SECTION_DEFAULTS = (
    ("verified_bots_ts", True),
    ("ngwaf_verified_bots", True),
    ("ngwaf_verified_bots_ts", True),
    ("wellknown_bots", True),
    ("tls_fingerprints", True),
    ("fingerprint_coverage", False),
    ("req_size_dist", True),
    ("top_ips_header", True),
    ("ipv6_adoption", True),
    ("proxy_dist", True),
    ("conn_reuse_dist", True),
)


def _window_eligible_for_rollup(start_time: str | None, end_time: str | None) -> bool:
    """Return True when the request window is wide enough to make the
    closed-hour rollup-read cost worth paying.

    Windows narrower than :data:`_ROLLUP_MIN_WINDOW_SECONDS` skip the
    rollup-served paths in :func:`_build_security_response` and let the
    live SQL serve those aggregates instead — the live path scans
    ``is_ipv6`` / ``p_type`` from the already-materialised catalog temp
    which is cheaper than opening N closed-hour parquet files on small
    windows. Returns False when bounds are missing or unparseable so
    open-ended requests fall through to live SQL.
    """
    if not start_time or not end_time:
        return False
    from backend.utils.date_utils import parse_iso_utc

    try:
        st_dt = parse_iso_utc(start_time)
        et_dt = parse_iso_utc(end_time)
    except Exception:
        return False
    if st_dt is None or et_dt is None or et_dt <= st_dt:
        return False
    return (et_dt - st_dt).total_seconds() >= _ROLLUP_MIN_WINDOW_SECONDS


def _has_rollup_coverage(src: dict, field: str, start_time: str | None, end_time: str | None) -> bool:
    """Cheap directory-stat check for in-window closed-hour rollup data.

    Returns ``True`` when the per-(field, hour) count rollup has at least
    one in-window closed-hour parquet on disk for ``field`` — either as a
    bundled ``all_fields.parquet`` row or as a per-field parquet.

    Used by :func:`get_security_aggregates` to decide BEFORE the catalog
    temp materializes whether the corresponding column can be dropped
    from the temp's projection (the rollup-served aggregate won't need
    it). Pre-checking avoids a re-materialize race: when the temp is
    built without ``is_ipv6`` / ``p_type``, the live-SQL fallback in
    ``_build_security_response`` would BinderException — so we MUST
    only drop the column when the rollup serve is virtually guaranteed.
    """
    from datetime import UTC, datetime, timedelta

    from backend.core.duckdb import _cache_dir
    from backend.core.rollups._common import _is_safe_ident
    from backend.utils.date_utils import parse_iso_utc

    if not _is_safe_ident(field):
        return False
    if not start_time or not end_time:
        return False
    try:
        st_dt = parse_iso_utc(start_time)
        et_dt = parse_iso_utc(end_time)
    except Exception:
        return False
    if st_dt is None or et_dt is None or et_dt <= st_dt:
        return False

    active_str = datetime.now(UTC).replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d-%H")
    st_str = st_dt.strftime("%Y-%m-%d-%H")
    et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")

    cache_dir = _cache_dir(src)
    bundled_root = os.path.join(cache_dir, "rollups", "hour_bundled")
    per_field_root = os.path.join(cache_dir, "rollups", "hour", f"field={field}")

    def _in_window(hour: str) -> bool:
        return st_str <= hour <= et_inclusive and hour < active_str

    # Bundled-hour tree: one all_fields.parquet per closed hour.
    if os.path.isdir(bundled_root):
        try:
            for entry in os.listdir(bundled_root):
                if not entry.startswith("hour=") or not _in_window(entry[5:]):
                    continue
                if os.path.isfile(os.path.join(bundled_root, entry, "all_fields.parquet")):
                    return True
        except OSError:
            pass

    # Per-field tree: any closed-hour parquet (skip tmp_ artifacts).
    if os.path.isdir(per_field_root):
        try:
            for entry in os.listdir(per_field_root):
                if not entry.startswith("hour=") or not _in_window(entry[5:]):
                    continue
                hour_dir = os.path.join(per_field_root, entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            return True
                except OSError:
                    continue
        except OSError:
            pass

    return False


def _ipv6_per_hour_from_rollups(
    runner: QueryRunner, src: dict, start_time: str | None, end_time: str | None
) -> list[dict] | None:
    """Compute the IPv6 adoption time series from per-hour count rollups.

    Returns the same shape as the live ``IPV6_ADOPTION_TS`` query —
    ``[{"time": iso, "pct": float}, ...]`` sorted by hour ascending.
    Reads ``(value, count)`` per (is_ipv6, hour) from the bundled +
    per-field count rollups, then merges the active-hour slice via a
    direct base-table query so the chart's right edge isn't missing.
    Returns ``None`` when start/end is missing, no in-window closed-
    hour rollups exist, or a read raises (caller falls back to live SQL).
    """
    if not start_time or not end_time:
        return None

    from datetime import UTC, datetime, timedelta

    from backend.core.duckdb import _cache_dir
    from backend.core.rollups._common import _is_safe_ident, _safe_table_for
    from backend.utils.date_utils import parse_iso_utc

    if not _is_safe_ident("is_ipv6"):
        return None
    try:
        st_dt = parse_iso_utc(start_time)
        et_dt = parse_iso_utc(end_time)
    except Exception:
        return None
    if st_dt is None or et_dt is None or et_dt <= st_dt:
        return None

    cache_dir = _cache_dir(src)
    bundled_root = os.path.join(cache_dir, "rollups", "hour_bundled")
    per_field_root = os.path.join(cache_dir, "rollups", "hour", "field=is_ipv6")

    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_end_dt = active_dt + timedelta(hours=1)
    active_str = active_dt.strftime("%Y-%m-%d-%H")
    st_str = st_dt.strftime("%Y-%m-%d-%H")
    et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")

    def _in_window(hour: str) -> bool:
        return st_str <= hour <= et_inclusive and hour < active_str

    bundled_paths: list[str] = []
    bundled_hours: set[str] = set()
    if os.path.isdir(bundled_root):
        try:
            for entry in os.listdir(bundled_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[5:]
                if not _in_window(hour):
                    continue
                p = os.path.join(bundled_root, entry, "all_fields.parquet")
                if os.path.isfile(p):
                    bundled_paths.append(p)
                    bundled_hours.add(hour)
        except OSError:
            pass

    per_field_paths: list[str] = []
    if os.path.isdir(per_field_root):
        try:
            for entry in os.listdir(per_field_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[5:]
                if not _in_window(hour) or hour in bundled_hours:
                    continue
                hour_dir = os.path.join(per_field_root, entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            per_field_paths.append(os.path.join(hour_dir, fname))
                except OSError:
                    continue
        except OSError:
            pass

    if not bundled_paths and not per_field_paths:
        return None

    branches: list[str] = []
    if bundled_paths:
        # hive_partitioning=1 is load-bearing: the bundled all_fields.parquet
        # body carries only (field, value, count) — `hour` lives ONLY in
        # the path segment ``hour=YYYY-MM-DD-HH``. With hive_partitioning=0,
        # selecting `hour` raises BinderException, the try/except swallows
        # it, and the IPv6 card renders empty on every closed-hour-bundled
        # service. Caught by session-9 adversarial review and pinned by
        # ``test_security_aggregates_ipv6_serves_from_bundled_hour_when_per_field_swept``.
        ps = ", ".join("'" + p.replace("'", "''") + "'" for p in bundled_paths)
        branches.append(
            f"SELECT hour, value, count FROM read_parquet([{ps}], hive_partitioning=1) WHERE field = 'is_ipv6'"
        )
    if per_field_paths:
        ps = ", ".join("'" + p.replace("'", "''") + "'" for p in per_field_paths)
        branches.append(
            f"SELECT hour, value, count FROM read_parquet([{ps}], hive_partitioning=1) WHERE field = 'is_ipv6'"
        )

    q = "SELECT hour, value, SUM(count) AS c FROM (" + " UNION ALL ".join(branches) + ") GROUP BY hour, value"
    try:
        rows = runner.execute(q).fetchall()
    except Exception:
        return None

    # Per-hour bucketing: writer stores `is_ipv6` cast as VARCHAR, so
    # values land as 'true' / 'false' (DuckDB's boolean→VARCHAR is
    # lowercase). NULL is_ipv6 rows land as NULL value and contribute
    # to the denominator only — matching the live query's
    # `count(*)`-over-all denominator with `CASE WHEN is_ipv6` numerator.
    per_hour: dict[str, dict[str, int]] = {}
    for hour, value, count in rows:
        b = per_hour.setdefault(hour, {"true": 0, "total": 0})
        b["total"] += int(count)
        if value == "true":
            b["true"] += int(count)

    if not per_hour:
        return None

    # Active-hour slice (closed-hour rollups skip active by contract).
    # Run a focused base-table query for the in-window active-hour
    # interval so the chart's right edge isn't blank.
    if et_dt > active_dt:
        live_start = max(active_dt, st_dt)
        live_end = min(active_end_dt, et_dt)
        if live_end > live_start:
            base_table = _safe_table_for(src)
            if base_table:
                try:
                    q_live = (
                        f"SELECT strftime(timestamp, '%Y-%m-%d-%H') AS hour, "
                        f"SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) AS true_c, "
                        f'count(*) AS total_c FROM "{base_table}" '
                        f"WHERE timestamp >= '{live_start.isoformat()}' "
                        f"AND timestamp < '{live_end.isoformat()}' GROUP BY 1"
                    )
                    for hour, true_c, total_c in runner.execute(q_live).fetchall():
                        b = per_hour.setdefault(hour, {"true": 0, "total": 0})
                        b["true"] += int(true_c) if true_c is not None else 0
                        b["total"] += int(total_c) if total_c is not None else 0
                except Exception:
                    # Active-hour merge failed — keep rollup data; chart's
                    # latest point will be missing but closed-hour data is
                    # still correct. Cheaper than failing the whole card.
                    _logger.debug("[security] ipv6 active-hour merge failed", exc_info=True)

    # Match the live IPV6_ADOPTION_TS output format: tz-aware datetime
    # routed through safe_iso so the `time` field is `+00:00`-suffixed
    # (not `Z`). The FE date parser distinguishes the two — diverging
    # would surface as broken sorting or invalid-date toasts.
    result: list[dict] = []
    for hour in sorted(per_hour):
        b = per_hour[hour]
        pct = (b["true"] / b["total"] * 100.0) if b["total"] else 0.0
        dt = datetime(int(hour[:4]), int(hour[5:7]), int(hour[8:10]), int(hour[11:13]), 0, 0, tzinfo=UTC)
        result.append({"time": safe_iso(dt), "pct": pct})
    return result


def _proxy_dist_from_rollups(runner: QueryRunner, start_time: str | None, end_time: str | None) -> list[dict] | None:
    """Compute PROXY_TYPE_DIST totals from the per-hour count rollup.

    ``execute_top_n_rollups`` already merges across closed hours AND
    queries the live active hour internally, so the totals match what
    the live SQL would have produced over the temp table. Returns
    ``None`` when the reader yields no rows for ``p_type`` (cold pool
    or all rows had empty / __other__ values).
    """
    try:
        rolled, _ = runner.execute_top_n_rollups(
            ["p_type"],
            start_time,
            end_time,
            limit=500,
        )
    except Exception:
        return None
    if not rolled:
        return None
    by_val: dict[str, int] = {}
    for _field, value, count in rolled:
        # Live query filters `p_type != ''` — mirror that, plus skip
        # __other__ (the synthetic day-bundle catch-all) and any
        # non-string values (DuckDB NULL surfaces as None here).
        if not isinstance(value, str) or value == "" or value == "__other__":
            continue
        by_val[value] = by_val.get(value, 0) + int(count)
    if not by_val:
        return None
    return [{"type": v, "count": c} for v, c in sorted(by_val.items(), key=lambda kv: kv[1], reverse=True)]


def get_top_bots(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    n: int = 10,
) -> dict:
    """Return top N bots from UA matching and (if available) the NGWAF bot cache."""
    import logging
    import time as _time

    timer = SectionTimer()
    section_timings = timer.entries

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("top_bots:get_schema_cols", _t)
    if not actual_cols:
        return empty_schema_response(bots=[], ngwaf_bots=[], **runner.telemetry())

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("top_bots:build_where_clause", _t)

    arcjet_bots: list[dict] = []
    ngwaf_bots: list[dict] = []

    use_rollups = not filters

    # ── Arcjet UA matching ──────────────────────────────────────────
    # Rollup-served when no filters apply. The hour bundles already
    # carry top-500 UAs per hour; UNION + GROUP-BY across the window
    # is sub-second even on 30d (vs ~1.1s for the ua column scan via
    # the iceberg view on prod). Real bots send enough traffic that
    # their UAs almost always land in top-500 for at least some hours,
    # so the rollup gives equivalent arcjet matches to the raw scan.
    # Filtered requests bypass this path (rollup is filter-free) and
    # fall through to the temp-scan branch below.
    ua_rollup_rows: list[tuple[str, int]] | None = None
    if use_rollups and "ua" in actual_cols:
        try:
            _t = _time.perf_counter()
            rolled, _ = runner.execute_top_n_rollups(
                ["ua"],
                start_time,
                end_time,
                limit=_BOT_UA_CLASSIFY_LIMIT,
                per_field_limits={"ua": _BOT_UA_CLASSIFY_LIMIT},
            )
            timer.mark("top_bots:ua_rollup_query", _t)
            ua_rollup_rows = [(v, int(c)) for _f, v, c in rolled if v and v != "__other__"]
            if not ua_rollup_rows:
                # Rollup is empty (cold service, no backfill yet) —
                # fall back to the raw temp scan so we still produce
                # bot matches on first dashboard load.
                ua_rollup_rows = None
        except Exception as e:
            logging.getLogger(__name__).warning("[security] UA rollup read failed, falling back: %s", e)
            ua_rollup_rows = None

    def _classify(rows: list[tuple[str, int]]) -> list[dict]:
        from backend.utils.bot_sources import build_matcher

        match_ua = build_matcher()
        bot_counts: dict[str, dict] = {}
        for ua_val, cnt in rows:
            for entry in match_ua(ua_val):
                bot_id = entry.get("id", "unknown")
                if bot_id not in bot_counts:
                    cats = entry.get("categories", [])
                    bot_counts[bot_id] = {
                        "id": bot_id,
                        "name": bot_id.replace("-", " ").title(),
                        "category": cats[0] if cats else "unknown",
                        "request_count": 0,
                    }
                bot_counts[bot_id]["request_count"] += cnt
        return sorted(bot_counts.values(), key=lambda x: x["request_count"], reverse=True)[:n]

    if ua_rollup_rows is not None:
        _t = _time.perf_counter()
        try:
            arcjet_bots = _classify(ua_rollup_rows)
        except Exception as e:
            logging.getLogger(__name__).error("[security] arcjet rollup match failed: %s", e)
        timer.mark("top_bots:arcjet_match", _t)

    # ── NGWAF cache bot names + filtered-UA fallback ────────────────
    # NGWAF JOIN needs raw waf_req_id (high-cardinality, no rollup),
    # so it still builds a temp. When the rollup-served UA path
    # didn't run (filters present, or "ua" not in schema), the temp
    # also carries `ua` so the filtered-UA branch can scan it.
    ngwaf_attached = False
    if "waf_req_id" in actual_cols:
        try:
            ngwaf_attached = ensure_ngwaf_bots_materialized(con, "ngwaf_top")
        except Exception:
            pass  # materialization failed — fall back gracefully

    needs_filtered_ua_scan = ua_rollup_rows is None and "ua" in actual_cols
    cols_needed: list[str] = []
    if needs_filtered_ua_scan:
        cols_needed.append("ua")
    if ngwaf_attached and "waf_req_id" in actual_cols:
        cols_needed.append("waf_req_id")

    if cols_needed == ["waf_req_id"]:
        # The UA branch was rollup-served, so the NGWAF join would be the
        # temp's ONLY consumer — materializing the window's waf_req_ids
        # (318ms of the 2026-07-06 24h trace) to probe them once is pure
        # overhead.
        #
        # Rollup fast path first: the per-hour ngwaf_bots parquets carry the
        # write-time (waf_req_id ⨝ bot cache) aggregation, so eligible
        # unfiltered windows skip even the single direct join (~115ms on
        # prod 24h). Falls back to the direct base-table join on any miss —
        # one scan, no materialization, same result (the IS NOT NULL floor
        # in the template matches the INNER JOIN semantics).
        rolled_ngwaf: list[dict] | None = None
        if use_rollups:
            _t = _time.perf_counter()
            try:
                rolled_ngwaf = runner.try_ngwaf_top_bots_from_rollup(
                    start_time, end_time, has_filters=bool(filters), n=n
                )
            except Exception as e:
                logging.getLogger(__name__).warning("[security] ngwaf_bots rollup read failed, falling back: %s", e)
                rolled_ngwaf = None
            timer.mark("top_bots:ngwaf_rollup", _t)
        if rolled_ngwaf is not None:
            ngwaf_bots = rolled_ngwaf
        else:
            try:
                _t = _time.perf_counter()
                q = SQL.NGWAF_TOP_BOTS_JOIN_DIRECT.format(table_name=table_name, where_clause=where_clause, n=n)
                res = runner.execute(q, params).fetchall()
                ngwaf_bots = [{"name": r[0], "category": r[1], "request_count": r[2]} for r in res]
                timer.mark("top_bots:ngwaf_join_direct", _t)
            except Exception as e:
                logging.getLogger(__name__).error("[security] NGWAF top bots failed: %s", e)
    elif cols_needed:
        _t = _time.perf_counter()
        with runner.temp_table(cols_needed, actual_cols, table_name, where_clause, params) as temp_table:
            timer.mark("top_bots:temp_table_create", _t)
            if temp_table is None:
                return {
                    "bots": arcjet_bots,
                    "ngwaf_bots": ngwaf_bots,
                    "section_timings": section_timings,
                    **runner.telemetry(),
                }
            if needs_filtered_ua_scan:
                try:
                    _t = _time.perf_counter()
                    q = SQL.TOP_UAS_BY_COUNT.format(temp_table=temp_table, limit=_BOT_UA_CLASSIFY_LIMIT)
                    rows = runner.execute(q).fetchall()
                    timer.mark("top_bots:top_uas_query", _t)
                    _t = _time.perf_counter()
                    arcjet_bots = _classify(rows)
                    timer.mark("top_bots:arcjet_match", _t)
                except Exception as e:
                    logging.getLogger(__name__).error("[security] arcjet top bots failed: %s", e)

            if ngwaf_attached:
                try:
                    _t = _time.perf_counter()
                    q = SQL.NGWAF_TOP_BOTS_JOIN.format(temp_table=temp_table, n=n)
                    res = runner.execute(q).fetchall()
                    ngwaf_bots = [{"name": r[0], "category": r[1], "request_count": r[2]} for r in res]
                    timer.mark("top_bots:ngwaf_join", _t)
                except Exception as e:
                    logging.getLogger(__name__).error("[security] NGWAF top bots failed: %s", e)

    return {
        "bots": arcjet_bots,
        "ngwaf_bots": ngwaf_bots,
        "section_timings": section_timings,
        **runner.telemetry(),
    }


def get_security_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
    sections: set[str] | None = None,
) -> dict:
    import time as _time

    # Per-phase timings for /api/security/aggregates so the perf
    # harness can attribute wall time across the ~14 sub-queries
    # _build_security_response runs without ad-hoc instrumentation.
    timer = SectionTimer()
    section_timings = timer.entries

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t)
    if not actual_cols:
        return {
            "tls_fingerprints": [],
            "req_size_dist": [],
            "ipv6_adoption": [],
            "proxy_dist": [],
            "conn_reuse_dist": [],
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t)

    # The catalog temp is built section-aware inside _build_security_response:
    # every section that can serve from a parquet rollup is served WITHOUT
    # the temp, and the temp is then materialized for ONLY the columns the
    # remaining live sections touch (or skipped entirely). This is the
    # per-column form of the all-or-nothing materialize the origin/aggregates
    # fix removed — so a 30d unfiltered request collapses the shared
    # ``temp_table_create`` to the tiny NGWAF-flagged subset (or nothing).
    return _build_security_response(
        runner,
        src,
        con,
        actual_cols,
        table_name,
        where_clause,
        params,
        bucket_seconds,
        section_timings,
        start_time=start_time,
        end_time=end_time,
        filters=filters,
        sections=sections,
    )


def _build_security_response(
    runner: QueryRunner,
    src: dict,
    con: duckdb.DuckDBPyConnection,
    actual_cols: list[str],
    table_name: str,
    where_clause: str,
    params: list | None,
    bucket_seconds: int,
    section_timings: list[dict] | None = None,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    filters: FiltersDict | None = None,
    sections: set[str] | None = None,
) -> dict:
    import time as _time

    timer = SectionTimer(section_timings)
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    results = {**runner.telemetry()}

    # ── Rollup eligibility (cheap directory-stat pre-checks; no parquet
    # reads). is_ipv6 / p_type serve from their count rollups when their
    # in-window closed-hour data exists, the request is unfiltered, and the
    # window clears the 3-day break-even. Same invariant as before: a column
    # is dropped from the temp ONLY when its rollup serve is virtually
    # guaranteed, so the live fallback never references a missing column. ──
    rollup_eligible = (
        not filters
        and start_time is not None
        and end_time is not None
        and _window_eligible_for_rollup(start_time, end_time)
    )
    use_ipv6_rollup = (
        rollup_eligible and "is_ipv6" in actual_cols and _has_rollup_coverage(src, "is_ipv6", start_time, end_time)
    )
    use_proxy_rollup = (
        rollup_eligible and "p_type" in actual_cols and _has_rollup_coverage(src, "p_type", start_time, end_time)
    )

    # ── Pass 1: serve everything that can come from a parquet rollup WITHOUT
    # the catalog temp, and record which sections still need a live scan plus
    # the exact temp columns they touch. The temp is then built for ONLY that
    # column set (or skipped). This is the per-column form of the
    # all-or-nothing materialize the origin/aggregates fix removed. ──
    needed_cols: set[str] = set()
    live_sections: set[str] = set()

    def _need(section: str, cols: set[str]) -> None:
        live_sections.add(section)
        needed_cols.update(cols)

    # Surface whether NGWAF is configured so the frontend can distinguish
    # "not configured" from "configured but no detections yet". Bundled
    # with the NGWAF bot section gate.
    _want_ngwaf_bots = _want("ngwaf_verified_bots") or _want("ngwaf_verified_bots_ts")
    if _want_ngwaf_bots:
        try:
            from backend import config as svcconfig

            results["ngwaf_configured"] = bool(svcconfig.get_ngwaf_workspace_id(src.get("service_id", "")))
        except Exception:
            results["ngwaf_configured"] = False

    # Materialize the NGWAF bot cache once per connection if it exists and
    # waf_req_id is in schema (see ensure_ngwaf_bots_materialized — reuses
    # an already-materialized alias on this connection, so this is cheap
    # after the first call). The bot JOIN needs raw waf_req_id
    # (high-cardinality UUID, never rolled up), so the NGWAF pair always
    # live-scans a (tiny) temp when materialized.
    _ngwaf_attached = False
    if _want_ngwaf_bots and "waf_req_id" in actual_cols:
        try:
            _ngwaf_attached = ensure_ngwaf_bots_materialized(con, "ngwaf_cache")
        except Exception:
            pass  # materialization failed — fall back gracefully
    if _ngwaf_attached:
        # The pair runs together (shared ATTACH); both need the temp.
        _need("ngwaf_verified_bots", {"waf_req_id"})
        _need("ngwaf_verified_bots_ts", {"timestamp", "waf_req_id"})
    elif _want_ngwaf_bots:
        results["ngwaf_verified_bots"] = []
        results["ngwaf_verified_bots_ts"] = []

    # Verified bots TS: the rollup reader fills the active hour FROM the temp,
    # so requesting this section always needs {timestamp, waf_sig} in the
    # temp (whether the rollup hits or not).
    if _want("verified_bots_ts"):
        if "waf_sig" in actual_cols:
            _need("verified_bots_ts", {"timestamp", "waf_sig"})
        else:
            results["verified_bots_ts"] = []

    # Fingerprint cards: TLS only. Each card returns top-20 + a coverage
    # fraction (populated rows / total rows) so the FE can render a low-
    # coverage hint when a leaderboard is legitimately sparse for the current
    # traffic mix (e.g. TLS fingerprints on a mostly-shielded service).
    #
    # The (column → result-key) map below keeps the result keying explicit
    # (the key isn't derivable from the column name by suffix manipulation)
    # and leaves room to re-add sibling fingerprint columns under the same
    # shared top-N template without touching the loop.
    _FP_RESULT_KEYS = (("tls_ciphers_sha", "tls_fingerprints"),)
    # Per-card opt-in. Coupling at the router boundary forces requesting
    # any FP card to also enable fingerprint_coverage, so this set drives
    # both the rollup fast-path eligibility AND the live SQL loop below.
    _wanted_fp_keys: set[str] = {result_key for _col, result_key in _FP_RESULT_KEYS if _want(result_key)}
    coverage_cols: list[str] = []
    schema_eligible_fp_cols = [
        c for c, rk in _FP_RESULT_KEYS if c in actual_cols and "ip" in actual_cols and rk in _wanted_fp_keys
    ]

    # Rollup fast-path (no temp): when no per-request filters apply, the
    # top-N fingerprints + their cross-window distinct-IP counts can be
    # served from the per-(field, hour) count rollup + the HLL ip_spread
    # rollup instead of the live FINGERPRINT_TOP_N scan over the catalog
    # temp. On 30-day windows this trades a count(DISTINCT ip) full-temp
    # scan for two parquet reads + an in-Python HLL merge.
    #
    # Falls back to the live SQL loop below when:
    #   * filters are non-empty (rollups don't carry per-request filter
    #     state — the live path is the only correct one)
    #   * no fingerprint fields are in the schema OR ip is missing
    #   * the count rollup returns no rows for any fingerprint field
    #     (cold pool / writer hasn't run for this service yet)
    #   * either rollup read raises
    rollup_served_fp_cols: set[str] = set()
    if not filters and schema_eligible_fp_cols:
        try:
            _t = _time.perf_counter()
            top_n_rows, _ = runner.execute_top_n_rollups(
                schema_eligible_fp_cols,
                start_time,
                end_time,
                limit=20,
                per_field_limits={c: 20 for c in schema_eligible_fp_cols},
            )
            timer.mark("fingerprints_rollup_top_n", _t)

            # Group top-N by field so we can render per-card lists.
            # __other__ rows (the day-bundle synthetic catch-all) are
            # skipped — they're a totals-preserving artefact, not a
            # real fingerprint value.
            by_field: dict[str, list[tuple[str, int]]] = {}
            for field, value, count in top_n_rows:
                if value == "__other__":
                    continue
                if not isinstance(value, str):
                    continue
                by_field.setdefault(field, []).append((value, int(count)))

            if by_field:
                _t = _time.perf_counter()
                ip_spread, _spread_meta = runner.execute_ip_spread_rollups(
                    schema_eligible_fp_cols,
                    start_time,
                    end_time,
                )
                timer.mark("fingerprints_rollup_ip_spread", _t)

                for col, result_key in _FP_RESULT_KEYS:
                    if col not in by_field:
                        continue
                    # Per-(field, value) ip_count comes from the merged
                    # HLL. If the writer hasn't populated ip_spread for
                    # THIS field yet (cold pool, backfill in progress,
                    # this commit shipped before the daily compact's
                    # self-heal ran), every lookup returns 0 — we MUST
                    # fall through to the live FINGERPRINT_TOP_N SQL
                    # so the FE shows real IP counts rather than a sea
                    # of zeros. Test on "at least one non-zero" rather
                    # than "non-empty dict" because the merge can
                    # produce 0 for a (field, value) tuple that isn't
                    # in the spread tree even when the tree DOES have
                    # data for other fields in the same call.
                    candidate_rows: list[dict] = []
                    has_nonzero_ip_count = False
                    for value, count in by_field[col]:
                        ip_count = int(ip_spread.get((col, value), 0))
                        if ip_count > 0:
                            has_nonzero_ip_count = True
                        candidate_rows.append(
                            {
                                "fingerprint": value,
                                "ip_count": ip_count,
                                "request_count": count,
                            }
                        )
                    if not has_nonzero_ip_count:
                        # ip_spread is cold for this field — skip
                        # marking it served so the live SQL fallback
                        # below runs. backfill_missing_hour_ip_spread
                        # on the next daily compact tick will fill the
                        # spread tree in and let the rollup path take
                        # over from there.
                        continue
                    results[result_key] = candidate_rows
                    coverage_cols.append(col)
                    rollup_served_fp_cols.add(col)
        except Exception:
            # Any error in the rollup path → fall through to the live
            # FINGERPRINT_TOP_N loop below. Logged at debug — the
            # caller produces a working result either way.
            import logging as _logging

            _logging.getLogger(__name__).debug(
                "[security] fingerprint rollup path failed; falling back to live SQL",
                exc_info=True,
            )

    # Fingerprint cols the rollup didn't serve need the live FINGERPRINT_TOP_N
    # over the temp ({col, ip}). Coverage scans the temp for EVERY fingerprint
    # col that ran (served OR live), so it pins those cols into the temp even
    # on a rollup hit.
    for col, result_key in _FP_RESULT_KEYS:
        if result_key not in _wanted_fp_keys:
            continue
        if col in rollup_served_fp_cols:
            continue
        if col in actual_cols and "ip" in actual_cols:
            _need("tls_fingerprints", {col, "ip"})
            coverage_cols.append(col)
        else:
            results[result_key] = []
    if coverage_cols and _want("fingerprint_coverage"):
        # Coverage scans the temp for every fingerprint col that ran. When a
        # fingerprint col is ALREADY in the temp for the live FINGERPRINT_TOP_N
        # (rollup missed), reuse that temp scan. But when every fingerprint
        # card served from a rollup (no fp col in the temp), serve coverage
        # from the per-hour security_cov counts so tls_ciphers_sha can drop —
        # the last column blocking the NGWAF-only temp narrowing. The cov
        # rollup carries tls_ciphers_sha's populated count specifically, so it
        # only applies to the single-col tls case.
        fp_cols_in_temp = any(c in needed_cols for c in coverage_cols)
        if not fp_cols_in_temp and coverage_cols == ["tls_ciphers_sha"]:
            _t = _time.perf_counter()
            cov = runner.try_security_coverage_from_rollup(start_time, end_time, has_filters=bool(filters))
            if cov is not None:
                timer.mark("fingerprint_coverage_rollup", _t)
                total_rows, tls_populated = cov
                results["fingerprint_coverage"] = {
                    "tls_ciphers_sha": (tls_populated / total_rows if total_rows > 0 else 0.0)
                }
            else:
                _need("fingerprint_coverage", set(coverage_cols))
        else:
            _need("fingerprint_coverage", set(coverage_cols))

    # Request Header Size Distribution: per-hour req_header_bytes histogram
    # rollup (exact SUM of bucket counts); live fallback scans the temp.
    if _want("req_size_dist"):
        if "req_header_bytes" in actual_cols:
            _t = _time.perf_counter()
            rolled = runner.try_security_req_size_from_rollup(start_time, end_time, has_filters=bool(filters))
            if rolled is not None:
                timer.mark("req_size_dist_rollup", _t)
                results["req_size_dist"] = rolled
            else:
                _need("req_size_dist", {"req_header_bytes"})
        else:
            results["req_size_dist"] = []

    # Top IPs by Max Header Size: per-hour top-K (ip, MAX) rollup (exact
    # MAX-of-MAX merge); live fallback scans the temp.
    if _want("top_ips_header"):
        if "req_header_bytes" in actual_cols:
            _t = _time.perf_counter()
            rolled = runner.try_security_top_ips_from_rollup(start_time, end_time, has_filters=bool(filters))
            if rolled is not None:
                timer.mark("top_ips_by_header_rollup", _t)
                results["top_ips_header"] = rolled
            else:
                _need("top_ips_header", {"ip", "req_header_bytes"})
        else:
            results["top_ips_header"] = []

    # Connection Reuse Distribution: per-hour conn_requests histogram rollup
    # (exact SUM of bucket counts); live fallback scans the temp.
    if _want("conn_reuse_dist"):
        if "conn_requests" in actual_cols:
            _t = _time.perf_counter()
            rolled = runner.try_security_conn_reuse_from_rollup(start_time, end_time, has_filters=bool(filters))
            if rolled is not None:
                timer.mark("conn_reuse_dist_rollup", _t)
                results["conn_reuse_dist"] = rolled
            else:
                _need("conn_reuse_dist", {"conn_requests"})
        else:
            results["conn_reuse_dist"] = []

    # IPv6 Adoption: rollup-served (no temp) when the pre-check passed; else
    # live, which needs {timestamp, is_ipv6}.
    if _want("ipv6_adoption") and "is_ipv6" in actual_cols:
        if use_ipv6_rollup:
            _t = _time.perf_counter()
            rolled = _ipv6_per_hour_from_rollups(runner, src, start_time, end_time)
            timer.mark("ipv6_adoption_rollup", _t)
            if rolled is None:
                _logger.warning(
                    "[security] ipv6 rollup pre-check passed but per-hour read returned None; rendering empty card"
                )
                results["ipv6_adoption"] = []
            else:
                results["ipv6_adoption"] = rolled
        else:
            _need("ipv6_adoption", {"timestamp", "is_ipv6"})
    elif _want("ipv6_adoption"):
        results["ipv6_adoption"] = []

    # Proxy/Anonymizer Breakdown: rollup-served (no temp) when the pre-check
    # passed; else live, which needs {p_type}.
    if _want("proxy_dist") and "p_type" in actual_cols:
        if use_proxy_rollup:
            _t = _time.perf_counter()
            rolled = _proxy_dist_from_rollups(runner, start_time, end_time)
            timer.mark("proxy_dist_rollup", _t)
            if rolled is None:
                _logger.warning(
                    "[security] proxy rollup pre-check passed but reader returned None; rendering empty card"
                )
                results["proxy_dist"] = []
            else:
                results["proxy_dist"] = rolled
        else:
            _need("proxy_dist", {"p_type"})
    elif _want("proxy_dist"):
        results["proxy_dist"] = []

    # Well-Known Bots: pre-materialised (ua, ip, count) rollup (no temp);
    # falls back to a live regex prefilter over the temp ({ua, ip}) on a
    # rollup miss (active hour, missing file, stale pattern_set_version).
    _wk_active = False
    _wk_rows: list | None = None
    if _want("wellknown_bots") and "ua" in actual_cols and "ip" in actual_cols:
        _wk_active = True
        if start_time and end_time:
            _t = _time.perf_counter()
            try:
                from backend.core.rollups import read_wellknown_bots_rollup

                _wk_rows = read_wellknown_bots_rollup(src, start_time, end_time)
            except Exception:
                _wk_rows = None
            if _wk_rows is not None:
                timer.mark("wellknown_bots_rollup_read", _t)
        if _wk_rows is None:
            _need("wellknown_bots", {"ua", "ip"})
    elif _want("wellknown_bots"):
        results["wellknown_bots"] = []

    # ── Build the (narrowed / NGWAF-only / skipped) catalog temp ──
    temp_table: str | None = None
    if needed_cols:
        # When the ONLY live sections are the NGWAF bot pair, restrict the
        # temp to NGWAF-flagged rows — both bot templates INNER JOIN on a
        # non-null waf_req_id anyway, so this is semantically identical and
        # collapses a full-window scan to the tiny flagged subset. Safe to
        # append a literal predicate: inline_params means ``params`` is
        # empty, so it can't desync bind parameters.
        ngwaf_pair = {"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        ngwaf_only = bool(live_sections) and live_sections <= ngwaf_pair and "waf_req_id" in actual_cols
        wc = where_clause
        if ngwaf_only and where_clause.strip():
            wc = f"{where_clause} AND waf_req_id IS NOT NULL"
        cols = [c for c in _TEMP_COL_ORDER if c in needed_cols]
        _t = _time.perf_counter()
        temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, wc, params)
        timer.mark("temp_table_create", _t)
        if ngwaf_only:
            timer.mark("security:temp_ngwaf_narrowed", _time.perf_counter())
    else:
        # Every requested section served from a rollup — no temp at all.
        timer.mark("security:temp_skipped", _time.perf_counter())

    # ── Pass 2: live SQL for the missed sections, against the narrowed temp ──
    try:
        if "verified_bots_ts" in live_sections and temp_table:
            _t = _time.perf_counter()
            rolled_vbts = runner.try_verified_bots_ts_from_rollup(
                start_time,
                end_time,
                temp_table=temp_table,
                bucket_seconds=bucket_seconds,
                has_filters=bool(filters),
            )
            if rolled_vbts is not None:
                timer.mark("verified_bots_ts_rollup", _t)
                results["verified_bots_ts"] = [
                    {"time": safe_iso(r[0]), "bot_type": r[1], "count": r[2]} for r in rolled_vbts
                ]
            else:
                q = SQL.VERIFIED_BOTS_TS.format(bucket_seconds=bucket_seconds, temp_table=temp_table)
                res = runner.execute(q).fetchall()
                timer.mark("verified_bots_ts", _t)
                results["verified_bots_ts"] = [{"time": safe_iso(r[0]), "bot_type": r[1], "count": r[2]} for r in res]

        if _ngwaf_attached and temp_table:
            try:
                # Table: group by bot_name + wellknown_bot_name + category
                q = SQL.NGWAF_VERIFIED_BOTS.format(temp_table=temp_table)
                _t = _time.perf_counter()
                res = runner.execute(q).fetchall()
                timer.mark("ngwaf_verified_bots", _t)
                results["ngwaf_verified_bots"] = [
                    {
                        "bot_name": r[0],
                        "wellknown_bot_name": r[1],
                        "category": r[2],
                        "request_count": r[3],
                    }
                    for r in res
                ]

                # Time series: bucketed counts by bot_name
                q = SQL.NGWAF_VERIFIED_BOTS_TS.format(bucket_seconds=bucket_seconds, temp_table=temp_table)
                _t = _time.perf_counter()
                res = runner.execute(q).fetchall()
                timer.mark("ngwaf_verified_bots_ts", _t)
                results["ngwaf_verified_bots_ts"] = [
                    {"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]} for r in res
                ]
            except Exception as e:
                import logging

                logging.getLogger(__name__).error("[security] NGWAF bot join failed: %s", e)
                results["ngwaf_verified_bots"] = []
                results["ngwaf_verified_bots_ts"] = []

        # Live FINGERPRINT_TOP_N for any fingerprint col the rollup didn't
        # serve (cols absent from the schema were already defaulted above).
        for col, result_key in _FP_RESULT_KEYS:
            if result_key not in _wanted_fp_keys:
                continue
            if col in rollup_served_fp_cols:
                continue
            if temp_table and col in actual_cols and "ip" in actual_cols:
                q = SQL.FINGERPRINT_TOP_N.format(col=col, temp_table=temp_table)
                _t = _time.perf_counter()
                res = runner.execute(q).fetchall()
                timer.mark(result_key, _t)
                results[result_key] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]

        # One scan of the temp populates coverage for every fingerprint card
        # that ran live (rollup-served coverage was already set from the
        # security_cov counts in pass 1, so it's not in live_sections here).
        # Column names come from the _FP_RESULT_KEYS safelist; no untrusted
        # input reaches the aggregate.
        if "fingerprint_coverage" in live_sections and temp_table:
            fingerprint_coverage: dict[str, float] = {}
            agg_cols = ", ".join(
                f'count(*) FILTER (WHERE "{c}" IS NOT NULL AND "{c}" != \'\') AS pop_{i}'
                for i, c in enumerate(coverage_cols)
            )
            try:
                _t = _time.perf_counter()
                row = runner.execute(
                    SQL.FINGERPRINT_COVERAGE_BULK.format(agg_cols=agg_cols, temp_table=temp_table)
                ).fetchone()
                timer.mark("fingerprint_coverage", _t)
                if row:
                    total = float(row[0]) if row[0] else 0.0
                    for i, c in enumerate(coverage_cols):
                        populated = float(row[i + 1]) if row[i + 1] is not None else 0.0
                        fingerprint_coverage[c] = populated / total if total > 0 else 0.0
            except Exception:
                # FE treats 0.0 as "no signal, show the existing emptyMessage".
                for c in coverage_cols:
                    fingerprint_coverage[c] = 0.0
            results["fingerprint_coverage"] = fingerprint_coverage
        elif _want("fingerprint_coverage") and "fingerprint_coverage" not in results:
            results["fingerprint_coverage"] = {}

        if "req_size_dist" in live_sections and temp_table:
            q = SQL.REQ_HEADER_SIZE_DIST.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("req_size_dist", _t)
            results["req_size_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]

        if "top_ips_header" in live_sections and temp_table:
            q = SQL.TOP_IPS_BY_MAX_HEADER.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("top_ips_by_header", _t)
            results["top_ips_header"] = [{"ip": r[0], "max_header": r[1]} for r in res]

        if "conn_reuse_dist" in live_sections and temp_table:
            q = SQL.CONN_REUSE_DIST.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("conn_reuse_dist", _t)
            results["conn_reuse_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]

        if "ipv6_adoption" in live_sections and temp_table:
            q = SQL.IPV6_ADOPTION_TS.format(
                time_bucket_select=time_bucket_select("1 hour"),
                temp_table=temp_table,
            )
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("ipv6_adoption", _t)
            results["ipv6_adoption"] = [{"time": safe_iso(r[0]), "pct": r[1]} for r in res]

        if "proxy_dist" in live_sections and temp_table:
            q = SQL.PROXY_TYPE_DIST.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("proxy_dist", _t)
            results["proxy_dist"] = [{"type": r[0], "count": r[1]} for r in res]

        # Well-Known Bots (UA matching + FCrDNS verification). Rows come from
        # the rollup (hit) or a live temp scan (miss); the Python enrichment
        # below is identical either way.
        if _wk_active:
            try:
                if _wk_rows is None and temp_table:
                    from backend.utils.bot_sources import get_bot_regex_pattern

                    _t = _time.perf_counter()
                    pattern = get_bot_regex_pattern(500)
                    if pattern:
                        pattern_sql = pattern.replace("'", "''")
                        prefilter = f"WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '{pattern_sql}')"
                    else:
                        prefilter = "WHERE ua IS NOT NULL AND ip IS NOT NULL"
                    q = SQL.WELLKNOWN_BOTS_UA_IP.format(temp_table=temp_table, prefilter=prefilter)
                    _wk_rows = runner.execute(q).fetchall()
                    timer.mark("wellknown_bots_query", _t)

                if _wk_rows is None:
                    results["wellknown_bots"] = []
                else:
                    from backend.utils.bot_sources import build_matcher
                    from backend.utils.rdns_cache import classify, enqueue, get_hostnames

                    match_ua = build_matcher()
                    bot_agg: dict[str, dict] = {}
                    new_ips: list[str] = []

                    # Batch-resolve every distinct IP in one SELECT instead of
                    # opening a fresh SQLite connection per (ua, ip) row.
                    hostnames = get_hostnames([ip for _, ip, _ in _wk_rows if ip])

                    for ua_val, ip_val, cnt in _wk_rows:
                        matches = match_ua(ua_val)
                        for entry in matches:
                            bot_id = entry.get("id", "unknown")
                            hostname, status, fcrdns_verified = hostnames.get(ip_val, (None, "pending", False))
                            if status == "pending":
                                new_ips.append(ip_val)
                            verification = entry.get("verification", {})
                            verification_domains = verification.get("domains", [])
                            verification_cidrs = verification.get("cidrs", [])
                            state = classify(
                                ip_val, hostname, status, fcrdns_verified, verification_domains, verification_cidrs
                            )

                            if bot_id not in bot_agg:
                                cats = entry.get("categories", [])
                                bot_agg[bot_id] = {
                                    "id": bot_id,
                                    "name": bot_id.replace("-", " ").title(),
                                    "category": cats[0] if cats else "unknown",
                                    "request_count": 0,
                                    "verified_count": 0,
                                    "impersonator_count": 0,
                                    "unverified_count": 0,
                                    "pending_count": 0,
                                }
                            agg = bot_agg[bot_id]
                            agg["request_count"] += cnt
                            if state == "verified":
                                agg["verified_count"] += cnt
                            elif state == "impersonator":
                                agg["impersonator_count"] += cnt
                            elif state == "unverified_pending":
                                agg["pending_count"] += cnt
                            else:
                                agg["unverified_count"] += cnt

                    # Enqueue any pending IPs we encountered for future enrichment
                    if new_ips:
                        enqueue(list(set(new_ips)))

                    # Sort by request count, return top 50, annotate coverage
                    sorted_bots = sorted(bot_agg.values(), key=lambda x: x["request_count"], reverse=True)[:50]
                    for b in sorted_bots:
                        total = b["request_count"]
                        covered = b["verified_count"] + b["impersonator_count"]
                        b["verification_coverage"] = round(covered / total, 3) if total else 0.0

                    results["wellknown_bots"] = sorted_bots
            except Exception as e:
                import logging

                logging.getLogger(__name__).error("[security] well-known bots query failed: %s", e)
                results["wellknown_bots"] = []
    finally:
        if temp_table:
            try:
                runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except Exception:
                pass

    # Any requested section left unset (e.g. the temp build failed) falls
    # back to its empty default so the response stays well-formed. Only
    # wanted sections get a key — the selector contract.
    for name, is_list in _SECTION_DEFAULTS:
        if _want(name) and name not in results:
            results[name] = [] if is_list else {}

    results["section_timings"] = section_timings
    return results


def get_security_proxies(
    con: duckdb.DuckDBPyConnection,
    src: dict[str, Any],
    start_time: Any,
    end_time: Any,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from backend.repositories._base import QueryRunner, _safe_table
    from backend.repositories._sql import security as SQL
    from backend.repositories.utils.filters import build_where_clause

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    actual_cols = runner.get_schema_cols()
    required = ["ip", "pop", "rtt_min", "tcp_rtt", "lat", "lon", "asn"]
    if not actual_cols or not all(col in actual_cols for col in required):
        return {
            "active_proxies_count": 0,
            "tunnel_requests_count": 0,
            "distance_mismatches_count": 0,
            "traffic_quality": [],
            "suspicious_isps": [],
            "active_clients": [],
        }

    params, where_clause = build_where_clause(start_time, end_time, filters or {}, actual_cols, inline_params=True)

    optional = ["country", "city"]
    cols_needed = [c for c in required + optional if c in actual_cols]

    with runner.temp_table(cols_needed, actual_cols, table_name, where_clause, params) as temp_table:
        if temp_table is None:
            return {
                "active_proxies_count": 0,
                "tunnel_requests_count": 0,
                "distance_mismatches_count": 0,
                "traffic_quality": [],
                "suspicious_isps": [],
                "active_clients": [],
            }

        # Fetch stats
        stats_df = runner.execute(SQL.GET_PROXY_STATS.format(temp_table=temp_table)).fetchdf()
        stats = stats_df.to_dict(orient="records")[0] if not stats_df.empty else {}

        # Fetch traffic quality segments
        quality_df = runner.execute(SQL.GET_TRAFFIC_QUALITY.format(temp_table=temp_table)).fetchdf()
        traffic_quality = []
        if not quality_df.empty:
            total_count = int(quality_df["count"].sum())
            if total_count > 0:
                for row in quality_df.to_dict(orient="records"):
                    label = str(row.get("type", "Unknown"))
                    item_count = float(row.get("count", 0))
                    value = round((item_count / total_count) * 100, 1)
                    traffic_quality.append({"label": label, "value": value})

        # Fetch top suspicious networks
        isps_df = runner.execute(SQL.GET_SUSPICIOUS_ISPS.format(temp_table=temp_table)).fetchdf()
        raw_isps = isps_df.to_dict(orient="records") if not isps_df.empty else []

        # Fetch active clients
        select_country_city_inner = ""
        if "country" in cols_needed:
            select_country_city_inner += ",\n            country"
        else:
            select_country_city_inner += ",\n            CAST(NULL AS VARCHAR) AS country"
        if "city" in cols_needed:
            select_country_city_inner += ",\n            city"
        else:
            select_country_city_inner += ",\n            CAST(NULL AS VARCHAR) AS city"

        select_country_city_outer = ""
        select_country_city_outer += ",\n        country"
        select_country_city_outer += ",\n        city"

        clients_df = runner.execute(
            SQL.GET_ACTIVE_PROXY_CLIENTS.format(
                temp_table=temp_table,
                select_country_city_inner=select_country_city_inner,
                select_country_city_outer=select_country_city_outer,
            )
        ).fetchdf()
        raw_clients = clients_df.to_dict(orient="records") if not clients_df.empty else []

        # Gather all distinct ASNs to resolve
        asns_to_resolve = set()
        for row in raw_isps:
            asn_val = row.get("asn")
            if asn_val is not None:
                try:
                    asns_to_resolve.add(int(asn_val))
                except (ValueError, TypeError):
                    pass
        for row in raw_clients:
            asn_val = row.get("asn")
            if asn_val is not None:
                try:
                    asns_to_resolve.add(int(asn_val))
                except (ValueError, TypeError):
                    pass

        # Resolve ASN names in batch
        from backend.core.duckdb import get_asn_names

        asn_names = get_asn_names(source_name, list(asns_to_resolve))

        # Format suspicious ISPs
        suspicious_isps = []
        for row in raw_isps:
            asn_val = row.get("asn")
            try:
                asn_int = int(asn_val) if asn_val is not None else None
            except (ValueError, TypeError):
                asn_int = None
            name = asn_names.get(asn_int) or (f"AS{asn_int}" if asn_int else "Unknown ISP")
            suspicious_isps.append(
                {
                    "isp": name,
                    "asn": asn_int,
                    "count": int(row.get("count", 0)),
                }
            )

        # Format active clients
        active_clients = []
        for row in raw_clients:
            asn_val = row.get("asn")
            try:
                asn_int = int(asn_val) if asn_val is not None else None
            except (ValueError, TypeError):
                asn_int = None
            name = asn_names.get(asn_int) or (f"AS{asn_int}" if asn_int else "Unknown ISP")
            client_item = {**row}
            client_item["asn_name"] = name
            if "asn" in client_item:
                del client_item["asn"]
            active_clients.append(client_item)

    return {
        "active_proxies_count": int(stats.get("active_proxies_count", 0)),
        "tunnel_requests_count": int(stats.get("total_requests_count", 0)),
        "distance_mismatches_count": int(stats.get("distance_mismatches_count", 0)),
        "traffic_quality": traffic_quality,
        "suspicious_isps": suspicious_isps,
        "active_clients": active_clients,
    }
